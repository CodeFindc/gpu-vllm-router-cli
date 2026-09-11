package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"gpu-vllm-router/pkg/config"
	"gpu-vllm-router/pkg/dashboard"
	"gpu-vllm-router/pkg/gpustack"
	"gpu-vllm-router/pkg/router"
)

func TestMultiModelRouting(t *testing.T) {
	s := NewServer(ServerConfig{Policy: router.PolicyRoundRobin}, nil)

	targetA1, _ := url.Parse("http://model-a-1:8000")
	targetA2, _ := url.Parse("http://model-a-2:8000")
	targetB1, _ := url.Parse("http://model-b-1:8000")

	cluster := &gpustack.ClusterEndpoints{
		ModelsEndpoints: map[string][]gpustack.WorkerEndpoint{
			"DeepSeek-V4-Flash-0731-w8a8": {
				{ModelName: "DeepSeek-V4-Flash-0731-w8a8", URL: "http://model-a-1:8000"},
				{ModelName: "DeepSeek-V4-Flash-0731-w8a8", URL: "http://model-a-2:8000"},
			},
			"Qwen3.6-27B": {
				{ModelName: "Qwen3.6-27B", URL: "http://model-b-1:8000"},
			},
		},
		Models: map[string]gpustack.ModelPublic{
			"DeepSeek-V4-Flash-0731-w8a8": {Name: "DeepSeek-V4-Flash-0731-w8a8"},
			"Qwen3.6-27B":                 {Name: "Qwen3.6-27B"},
		},
	}
	s.updateClusterEndpoints(cluster)

	// Test 1: Extract model from body for Model A
	bodyA := []byte(`{"model": "DeepSeek-V4-Flash-0731-w8a8", "messages": [{"role": "user", "content": "hi"}]}`)
	reqA, _ := http.NewRequest(http.MethodPost, "http://localhost:8000/v1/chat/completions", bytes.NewReader(bodyA))

	mNameA, _ := extractModelFromRequest(reqA)
	if mNameA != "DeepSeek-V4-Flash-0731-w8a8" {
		t.Fatalf("expected extracted model DeepSeek-V4-Flash-0731-w8a8, got %q", mNameA)
	}

	poolA, err := s.findModelPool(mNameA)
	if err != nil {
		t.Fatalf("failed to find pool for Model A: %v", err)
	}
	if len(poolA.Targets) != 2 {
		t.Errorf("expected 2 targets for Model A, got %d", len(poolA.Targets))
	}

	// Test 2: Extract model from body for Model B (case-insensitive)
	bodyB := []byte(`{"model": "qwen3.6-27b", "messages": []}`)
	reqB, _ := http.NewRequest(http.MethodPost, "http://localhost:8000/v1/chat/completions", bytes.NewReader(bodyB))

	mNameB, _ := extractModelFromRequest(reqB)
	poolB, err := s.findModelPool(mNameB)
	if err != nil {
		t.Fatalf("failed to find pool for Model B: %v", err)
	}
	if poolB.ModelName != "Qwen3.6-27B" {
		t.Errorf("expected Qwen3.6-27B, got %s", poolB.ModelName)
	}

	// Test 3: Model does not exist
	_, errNonExistent := s.findModelPool("non-existent-model")
	if errNonExistent == nil {
		t.Error("expected error for non-existent model")
	}

	// Test 4: Verify /v1/models response
	rw := httptest.NewRecorder()
	reqModels, _ := http.NewRequest(http.MethodGet, "http://localhost:8000/v1/models", nil)
	s.handleModels(rw, reqModels)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 for /v1/models, got %d", rw.Code)
	}

	var resp struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rw.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse /v1/models response: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 models in /v1/models response, got %d", len(resp.Data))
	}

	_ = context.Background()
	_ = targetA1
	_ = targetA2
	_ = targetB1
}

func TestProxyEndpointsAndTopology(t *testing.T) {
	s := NewServer(ServerConfig{Policy: router.PolicyConsistentHash}, nil)

	// 1. Test Standby Health Checks (0 models)
	for _, path := range []string{"/health", "/healthz", "/livez", "/ping"} {
		rw := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		s.handleHealth(rw, req)
		if rw.Code != http.StatusOK {
			t.Errorf("expected 200 OK for %s in standby, got %d", path, rw.Code)
		}
	}

	// 2. Test /readyz returns 503 when 0 models
	rwReady := httptest.NewRecorder()
	reqReady, _ := http.NewRequest(http.MethodGet, "/readyz", nil)
	s.handleHealth(rwReady, reqReady)
	if rwReady.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable for /readyz in standby, got %d", rwReady.Code)
	}

	// 3. Test /admin/stats
	rwStats := httptest.NewRecorder()
	reqStats, _ := http.NewRequest(http.MethodGet, "/admin/stats", nil)
	s.handleStats(rwStats, reqStats)
	if rwStats.Code != http.StatusOK {
		t.Errorf("expected 200 OK for /admin/stats, got %d", rwStats.Code)
	}

	// 4. Setup target and verify CircuitBreaker GetLastProbeAndError in GetTopology
	u, _ := url.Parse("http://worker-test:8000")
	cb := NewCircuitBreaker("http://worker-test:8000", DefaultCircuitBreakerConfig())
	cb.RecordFailure(http.ErrHandlerTimeout)
	probeTime, lastErr := cb.GetLastProbeAndError()
	if lastErr == "" {
		t.Errorf("expected lastError to be recorded, got empty")
	}
	_ = probeTime

	target := &BackendTarget{
		URL:            u,
		URLString:      "http://worker-test:8000",
		Healthy:        true,
		CircuitBreaker: cb,
	}
	s.allTargets = []*BackendTarget{target}
	s.modelPools["test-model"] = &ModelPool{
		ModelName: "test-model",
		Balancer:  NewBalancer(router.PolicyConsistentHash, []*BackendTarget{target}),
		Targets:   []*BackendTarget{target},
	}

	topo, err := s.GetTopology(context.Background())
	if err != nil {
		t.Fatalf("GetTopology failed: %v", err)
	}
	if len(topo.Models) != 1 || len(topo.Models[0].Workers) != 1 {
		t.Fatalf("expected 1 worker in topology, got %v", topo)
	}
	workerTopo := topo.Models[0].Workers[0]
	if workerTopo.LastErr == "" {
		t.Errorf("expected worker last_err to be populated in topology, got empty")
	}

	// 5. Test ProbeWorker and ResetBreaker with trailing slash normalization
	_ = s.ResetBreaker(context.Background(), "http://worker-test:8000/")
	_, resetErr := cb.GetLastProbeAndError()
	if resetErr != "" {
		t.Errorf("expected lastErr to be cleared after reset, got %s", resetErr)
	}

	// 6. Test /api/config via dashHandler
	dashHandler := dashboard.NewHandler(s)
	rwCfg := httptest.NewRecorder()
	reqCfg, _ := http.NewRequest(http.MethodGet, "/api/config", nil)
	dashHandler.ServeHTTP(rwCfg, reqCfg)
	if rwCfg.Code != http.StatusOK {
		t.Errorf("expected 200 OK for /api/config, got %d", rwCfg.Code)
	}
	if !strings.Contains(rwCfg.Body.String(), `"mode":"proxy"`) {
		t.Errorf("expected mode proxy in /api/config, got %s", rwCfg.Body.String())
	}
}

func TestProxyConfigManager(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "config.yaml")

	srv := NewServer(ServerConfig{
		Host:           "127.0.0.1",
		Port:           8080,
		Policy:         router.PolicyConsistentHash,
		ConfigFilePath: cfgPath,
	}, nil)

	u, _ := url.Parse("http://worker-1:8000")
	target := &BackendTarget{
		URL:       u,
		URLString: "http://worker-1:8000",
		Healthy:   true,
	}
	srv.modelPools["deepseek"] = &ModelPool{
		ModelName: "deepseek",
		Mode:      "proxy",
		Policy:    router.PolicyConsistentHash,
		Balancer:  NewBalancer(router.PolicyConsistentHash, []*BackendTarget{target}),
		Targets:   []*BackendTarget{target},
	}

	ctx := context.Background()

	// 1. GetConfig
	cfg, err := srv.GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if cfg.Mode != "proxy" || cfg.Policy != "consistent_hash" {
		t.Errorf("unexpected initial config: %+v", cfg)
	}
	if len(cfg.Models) != 1 || cfg.Models[0].ModelName != "deepseek" {
		t.Errorf("expected model deepseek, got %+v", cfg.Models)
	}

	// 2. UpdateModelRule
	updated, err := srv.UpdateModelRule(ctx, dashboard.ModelRuleUpdateRequest{
		ModelName: "deepseek",
		Mode:      "proxy",
		Policy:    "round_robin",
	})
	if err != nil {
		t.Fatalf("UpdateModelRule failed: %v", err)
	}
	if len(updated.Models) != 1 || updated.Models[0].Policy != "round_robin" {
		t.Errorf("expected policy updated to round_robin: %+v", updated.Models)
	}
	if srv.modelPools["deepseek"].Policy != "round_robin" {
		t.Errorf("pool policy not updated in memory: %s", srv.modelPools["deepseek"].Policy)
	}

	// 3. UpdateConfig (global)
	newPolicy := "random"
	newDowntime := true
	updated2, err := srv.UpdateConfig(ctx, dashboard.ConfigUpdateRequest{
		Policy:       &newPolicy,
		ZeroDowntime: &newDowntime,
	})
	if err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}
	if updated2.Policy != "random" || updated2.ZeroDowntime != true {
		t.Errorf("global config update mismatch: %+v", updated2)
	}

	// 4. Verify disk persistence
	loaded, err := config.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("failed to load saved config: %v", err)
	}
	if loaded.Target.Policy != "random" {
		t.Errorf("saved config policy mismatch: got %s", loaded.Target.Policy)
	}
	if len(loaded.Models) != 1 || loaded.Models[0].Policy != "round_robin" {
		t.Errorf("saved config models mismatch: %+v", loaded.Models)
	}
}

