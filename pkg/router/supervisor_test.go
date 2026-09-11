package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gpu-vllm-router/pkg/config"
	"gpu-vllm-router/pkg/dashboard"
)

func TestGetFreePort(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	if port <= 1024 || port > 65535 {
		t.Errorf("unexpected port number: %d", port)
	}
}

func TestSupervisorHealthCheck(t *testing.T) {
	// Mock backend that starts unhealthy then becomes healthy
	isHealthy := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if isHealthy {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"ok"}`))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	portStr := u.Port()
	port, _ := strconv.Atoi(portStr)

	sup := &Supervisor{}

	// Initially unhealthy, should timeout quickly
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := sup.waitForHealth(ctx, port, 400*time.Millisecond)
	if err == nil {
		t.Error("expected health check to fail when unhealthy")
	}

	// Now make it healthy
	isHealthy = true
	ctx2 := context.Background()
	err2 := sup.waitForHealth(ctx2, port, 1*time.Second)
	if err2 != nil {
		t.Errorf("expected health check to pass, got: %v", err2)
	}
}

func TestZeroDowntimeTargetSwitch(t *testing.T) {
	sup := &Supervisor{}

	targetA, _ := url.Parse("http://127.0.0.1:18001")
	targetB, _ := url.Parse("http://127.0.0.1:18002")

	sup.activeTarget = targetA

	req, _ := http.NewRequest(http.MethodGet, "http://0.0.0.0:8000/v1/models", nil)

	// Simulate reverse proxy director reading target
	sup.mu.RLock()
	target := sup.activeTarget
	sup.mu.RUnlock()

	req.URL.Host = target.Host
	if !strings.Contains(req.URL.Host, "18001") {
		t.Errorf("expected host 18001, got %s", req.URL.Host)
	}

	// Atomic switch to targetB
	sup.mu.Lock()
	sup.activeTarget = targetB
	sup.mu.Unlock()

	sup.mu.RLock()
	newTarget := sup.activeTarget
	sup.mu.RUnlock()

	req.URL.Host = newTarget.Host
	if !strings.Contains(req.URL.Host, "18002") {
		t.Errorf("expected host 18002, got %s", req.URL.Host)
	}
}

func TestSupervisorMultiModelRouting(t *testing.T) {
	// 1. Setup Mock Router Processes for Model A and Model B
	serverAHits := 0
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverAHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"reply":"from_model_a"}`))
	}))
	defer serverA.Close()

	serverBHits := 0
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverBHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"reply":"from_model_b"}`))
	}))
	defer serverB.Close()

	targetA, _ := url.Parse(serverA.URL)
	targetB, _ := url.Parse(serverB.URL)

	sup := &Supervisor{
		runners: map[string]*ModelRunner{
			"DeepSeek-V4-Flash-0731-w8a8": {
				ModelName:    "DeepSeek-V4-Flash-0731-w8a8",
				ActiveTarget: targetA,
				ActiveURLs:   []string{"http://192.168.1.10:40039"},
			},
			"Qwen3.6-27B": {
				ModelName:    "Qwen3.6-27B",
				ActiveTarget: targetB,
				ActiveURLs:   []string{"http://192.168.1.11:40039"},
			},
		},
	}

	// 2. Test /health
	rwHealth := httptest.NewRecorder()
	reqHealth, _ := http.NewRequest(http.MethodGet, "/health", nil)
	sup.handleHealth(rwHealth, reqHealth)
	if rwHealth.Code != http.StatusOK {
		t.Errorf("expected /health 200, got %d", rwHealth.Code)
	}

	// 3. Test /v1/models
	rwModels := httptest.NewRecorder()
	reqModels, _ := http.NewRequest(http.MethodGet, "/v1/models", nil)
	sup.handleModels(rwModels, reqModels)
	if rwModels.Code != http.StatusOK {
		t.Errorf("expected /v1/models 200, got %d", rwModels.Code)
	}
	modelsBody := rwModels.Body.String()
	if !strings.Contains(modelsBody, "DeepSeek-V4-Flash") || !strings.Contains(modelsBody, "Qwen3.6-27B") {
		t.Errorf("expected both models in /v1/models, got: %s", modelsBody)
	}

	// 4. Test Routing to Model A
	rwReqA := httptest.NewRecorder()
	reqBodyA := strings.NewReader(`{"model":"DeepSeek-V4-Flash-0731-w8a8","messages":[{"role":"user","content":"hi"}]}`)
	reqA, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", reqBodyA)
	sup.handleProxy(rwReqA, reqA)
	if rwReqA.Code != http.StatusOK {
		t.Errorf("expected 200 for model A, got %d (body: %s)", rwReqA.Code, rwReqA.Body.String())
	}
	if serverAHits != 1 || serverBHits != 0 {
		t.Errorf("expected serverA hit=1, got A=%d, B=%d", serverAHits, serverBHits)
	}

	// 5. Test Routing to Model B (case-insensitive substring)
	rwReqB := httptest.NewRecorder()
	reqBodyB := strings.NewReader(`{"model":"qwen3.6","messages":[{"role":"user","content":"hi"}]}`)
	reqB, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", reqBodyB)
	sup.handleProxy(rwReqB, reqB)
	if rwReqB.Code != http.StatusOK {
		t.Errorf("expected 200 for model B, got %d", rwReqB.Code)
	}
	if serverBHits != 1 {
		t.Errorf("expected serverB hit=1, got %d", serverBHits)
	}

	// 6. Test Unknown Model Returns 404
	rwUnknown := httptest.NewRecorder()
	reqBodyUnknown := strings.NewReader(`{"model":"NonExistentModel"}`)
	reqUnknown, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", reqBodyUnknown)
	sup.handleProxy(rwUnknown, reqUnknown)
	if rwUnknown.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown model, got %d", rwUnknown.Code)
	}
}

func TestSupervisorEndpointsAndRedirect(t *testing.T) {
	sup := &Supervisor{
		runners:      make(map[string]*ModelRunner),
		workerStates: make(map[string]*WorkerBreaker),
	}

	// 1. Test Root GET / returns service status JSON
	rwRoot := httptest.NewRecorder()
	reqRoot, _ := http.NewRequest(http.MethodGet, "/", nil)
	sup.handleProxy(rwRoot, reqRoot)
	if rwRoot.Code != http.StatusOK {
		t.Errorf("expected 200 OK for GET /, got %d", rwRoot.Code)
	}
	if !strings.Contains(rwRoot.Body.String(), "gpu-vllm-router-cli") {
		t.Errorf("expected body to contain gpu-vllm-router-cli, got %s", rwRoot.Body.String())
	}

	// 2. Test Standby Health Checks (0 models)
	for _, path := range []string{"/health", "/healthz", "/livez", "/ping"} {
		rw := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		sup.handleHealth(rw, req)
		if rw.Code != http.StatusOK {
			t.Errorf("expected 200 OK for %s in standby, got %d", path, rw.Code)
		}
		if !strings.Contains(rw.Body.String(), `"standby":true`) {
			t.Errorf("expected standby:true in %s response, got %s", path, rw.Body.String())
		}
	}

	// 3. Test /readyz returns 503 when 0 models
	rwReady := httptest.NewRecorder()
	reqReady, _ := http.NewRequest(http.MethodGet, "/readyz", nil)
	sup.handleHealth(rwReady, reqReady)
	if rwReady.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable for /readyz with 0 models, got %d", rwReady.Code)
	}

	// 4. Test /admin/supervisor and /admin/stats
	rwStats := httptest.NewRecorder()
	reqStats, _ := http.NewRequest(http.MethodGet, "/admin/stats", nil)
	sup.handleSupervisorStatus(rwStats, reqStats)
	if rwStats.Code != http.StatusOK {
		t.Errorf("expected 200 OK for /admin/stats, got %d", rwStats.Code)
	}
	if !strings.Contains(rwStats.Body.String(), `"mode":"supervisor_run_mode"`) {
		t.Errorf("expected supervisor_run_mode in stats JSON, got %s", rwStats.Body.String())
	}

	// 5. Test GetTopology with active connections
	sup.runners["test-model"] = &ModelRunner{
		ModelName:   "test-model",
		AllURLs:     []string{"http://worker-1:8000"},
		activeConns: 3,
	}
	sup.totalActiveConns = 3
	topo, err := sup.GetTopology(context.Background())
	if err != nil {
		t.Fatalf("GetTopology failed: %v", err)
	}
	if topo.TotalActiveConns != 3 {
		t.Errorf("expected TotalActiveConns=3, got %d", topo.TotalActiveConns)
	}
	if len(topo.Models) != 1 || topo.Models[0].ActiveConns != 3 {
		t.Errorf("expected model ActiveConns=3, got %v", topo.Models)
	}

	// 6. Test /api/config via handleProxy safeguard
	rwCfg := httptest.NewRecorder()
	reqCfg, _ := http.NewRequest(http.MethodGet, "/api/config", nil)
	sup.handleProxy(rwCfg, reqCfg)
	if rwCfg.Code != http.StatusOK {
		t.Errorf("expected 200 OK for /api/config, got %d (body: %s)", rwCfg.Code, rwCfg.Body.String())
	}
	if !strings.Contains(rwCfg.Body.String(), `"mode":"run"`) {
		t.Errorf("expected mode run in /api/config response, got %s", rwCfg.Body.String())
	}
}

func TestSupervisorConfigManager(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "config.yaml")

	sup := NewSupervisor(nil, "", SupervisorConfig{
		PublicHost:     "127.0.0.1",
		PublicPort:     9000,
		ConfigFilePath: cfgPath,
		RouterCfg: Config{
			Policy: PolicyConsistentHash,
		},
		ZeroDowntime:  true,
		DrainTimeout: 45 * time.Second,
		WatchInterval: 10 * time.Second,
	})

	sup.runners["qwen-7b"] = &ModelRunner{
		ModelName: "qwen-7b",
		Mode:      "run",
		Policy:    PolicyConsistentHash,
		AllURLs:   []string{"http://127.0.0.1:8001"},
	}

	ctx := context.Background()

	// 1. GetConfig
	cfg, err := sup.GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if cfg.Mode != "run" || cfg.Policy != "consistent_hash" {
		t.Errorf("unexpected initial config: %+v", cfg)
	}
	if len(cfg.Models) != 1 || cfg.Models[0].ModelName != "qwen-7b" {
		t.Errorf("expected model qwen-7b in config models, got %+v", cfg.Models)
	}

	// 2. UpdateModelRule
	updated, err := sup.UpdateModelRule(ctx, dashboard.ModelRuleUpdateRequest{
		ModelName: "qwen-7b",
		Mode:      "proxy",
		Policy:    "round_robin",
	})
	if err != nil {
		t.Fatalf("UpdateModelRule failed: %v", err)
	}
	if len(updated.Models) != 1 || updated.Models[0].Mode != "proxy" || updated.Models[0].Policy != "round_robin" {
		t.Errorf("expected qwen-7b updated to proxy/round_robin, got: %+v", updated.Models)
	}
	if sup.runners["qwen-7b"].Mode != "proxy" {
		t.Errorf("runner mode not updated in memory: got %s", sup.runners["qwen-7b"].Mode)
	}

	// 3. UpdateConfig (global)
	newPolicy := "round_robin"
	newDowntime := false
	updated2, err := sup.UpdateConfig(ctx, dashboard.ConfigUpdateRequest{
		Policy:       &newPolicy,
		ZeroDowntime: &newDowntime,
	})
	if err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}
	if updated2.Policy != "round_robin" || updated2.ZeroDowntime != false {
		t.Errorf("global config not updated: %+v", updated2)
	}

	// 4. Verify persistence to disk
	loaded, err := config.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("failed to load saved config from disk: %v", err)
	}
	if loaded.Target.Policy != "round_robin" {
		t.Errorf("saved config target policy mismatch: got %s", loaded.Target.Policy)
	}
	if len(loaded.Models) != 1 || loaded.Models[0].Mode != "proxy" {
		t.Errorf("saved config model rule mismatch: got %+v", loaded.Models)
	}
}

func TestSupervisor_ClientContextCanceledDoesNotTriggerProbe(t *testing.T) {
	var probeCalled int32

	sup := &Supervisor{
		modelName: "test-model",
		runners:   make(map[string]*ModelRunner),
		probeWorkerFunc: func(ctx context.Context, rawURL string) (bool, error) {
			atomic.AddInt32(&probeCalled, 1)
			return true, nil
		},
		workerStates: make(map[string]*WorkerBreaker),
	}

	// Backend server that blocks until client cancels
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer backend.Close()

	targetURL, _ := url.Parse(backend.URL)
	runner := &ModelRunner{
		ModelName:    "test-model",
		Mode:         "run",
		ActiveTarget: targetURL,
		ActiveURLs:   []string{backend.URL},
		AllURLs:      []string{backend.URL},
	}
	sup.runners["test-model"] = runner

	// Create request with cancelable context
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8000/v1/chat/completions", strings.NewReader(`{"model":"test-model"}`))
	req = req.WithContext(ctx)

	// Cancel context immediately to simulate client aborting during queueing
	cancel()

	rr := httptest.NewRecorder()
	sup.handleProxy(rr, req)

	// Give any background fastProbeRunner a moment if it were mistakenly spawned
	time.Sleep(100 * time.Millisecond)

	if calls := atomic.LoadInt32(&probeCalled); calls > 0 {
		t.Errorf("expected 0 probe calls when client cancels, got %d", calls)
	}
	if rr.Code == http.StatusBadGateway {
		t.Errorf("expected no 502 Bad Gateway written for client-side cancellation, got status %d", rr.Code)
	}
}


