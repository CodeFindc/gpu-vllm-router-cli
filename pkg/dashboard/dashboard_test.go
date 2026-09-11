package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockProvider struct {
	data     *TopologyData
	probeOK  bool
	probeErr error
	resetErr error
}

func (m *mockProvider) GetTopology(ctx context.Context) (*TopologyData, error) {
	return m.data, nil
}

func (m *mockProvider) ProbeWorker(ctx context.Context, workerURL string) (bool, error) {
	return m.probeOK, m.probeErr
}

func (m *mockProvider) ResetBreaker(ctx context.Context, workerURL string) error {
	return m.resetErr
}

func (m *mockProvider) GetConfig(ctx context.Context) (*ConfigSnapshot, error) {
	return &ConfigSnapshot{
		Mode:              "proxy",
		Policy:            "consistent_hash",
		WatchIntervalSecs: 10,
		MaxFailures:       3,
		AvailablePolicies: []string{"consistent_hash", "round_robin"},
		AvailableModes:    []string{"proxy", "run"},
		Models: []ModelRuleDTO{
			{ModelName: "DeepSeek-V4", Mode: "proxy", Policy: "consistent_hash"},
		},
	}, nil
}

func (m *mockProvider) UpdateConfig(ctx context.Context, req ConfigUpdateRequest) (*ConfigSnapshot, error) {
	return &ConfigSnapshot{
		Mode:   "run",
		Policy: "power_of_two",
	}, nil
}

func (m *mockProvider) UpdateModelRule(ctx context.Context, req ModelRuleUpdateRequest) (*ConfigSnapshot, error) {
	return &ConfigSnapshot{
		Models: []ModelRuleDTO{
			{ModelName: req.ModelName, Mode: req.Mode, Policy: req.Policy},
		},
	}, nil
}

func TestDashboardUIHandler(t *testing.T) {
	mock := &mockProvider{
		data: &TopologyData{
			Mode:          "proxy",
			Policy:        "consistent_hash",
			PublicAddr:    "0.0.0.0:8000",
			ClusterHealth: "healthy",
			TotalModels:   1,
			TotalWorkers:  2,
		},
	}
	h := NewHandler(mock)

	// 1. Test /dashboard
	rw := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/dashboard", nil)
	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /dashboard, got %d", rw.Code)
	}
	if !strings.Contains(rw.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected application/json content type, got %s", rw.Header().Get("Content-Type"))
	}
	body := rw.Body.String()
	if !strings.Contains(body, "gpu-vllm-router-cli") {
		t.Errorf("expected response to contain gpu-vllm-router-cli, got %s", body)
	}
	if !strings.Contains(body, "disabled") {
		t.Errorf("expected response to indicate disabled UI, got %s", body)
	}

	// 2. Test /ui
	rwUI := httptest.NewRecorder()
	reqUI, _ := http.NewRequest(http.MethodGet, "/ui", nil)
	h.ServeHTTP(rwUI, reqUI)

	if rwUI.Code != http.StatusOK {
		t.Errorf("expected 200 OK for /ui, got %d", rwUI.Code)
	}
	if !strings.Contains(rwUI.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected application/json content type, got %s", rwUI.Header().Get("Content-Type"))
	}
}

func TestDashboardTopologyAPI(t *testing.T) {
	mock := &mockProvider{
		data: &TopologyData{
			Mode:             "proxy",
			Policy:           "consistent_hash",
			PublicAddr:       "127.0.0.1:8000",
			ClusterHealth:    "healthy",
			TotalModels:      1,
			TotalWorkers:     2,
			HealthyWorkers:   2,
			TotalActiveConns: 5,
			Models: []ModelTopology{
				{
					ModelName:    "DeepSeek-V4",
					Policy:       "consistent_hash",
					WorkerCount:  2,
					HealthyCount: 2,
					ActiveConns:  5,
					Workers: []WorkerTopology{
						{
							URL:                 "http://worker-1:8000",
							ActiveConns:         3,
							Healthy:             true,
							CircuitState:        "CLOSED",
							ConsecutiveFailures: 0,
						},
						{
							URL:                 "http://worker-2:8000",
							ActiveConns:         2,
							Healthy:             true,
							CircuitState:        "CLOSED",
							ConsecutiveFailures: 0,
						},
					},
				},
			},
		},
		probeOK: true,
	}

	h := NewHandler(mock)

	// 1. Test GET /api/topology
	rw := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/topology", nil)
	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /api/topology, got %d", rw.Code)
	}

	var topo TopologyData
	if err := json.Unmarshal(rw.Body.Bytes(), &topo); err != nil {
		t.Fatalf("failed to parse topology JSON: %v", err)
	}
	if topo.TotalModels != 1 || len(topo.Models) != 1 {
		t.Errorf("expected 1 model, got %d", topo.TotalModels)
	}
	if topo.Models[0].ModelName != "DeepSeek-V4" {
		t.Errorf("expected model name DeepSeek-V4, got %s", topo.Models[0].ModelName)
	}

	// 2. Test POST /api/probe
	probeBody, _ := json.Marshal(ProbeRequest{URL: "http://worker-1:8000"})
	rwProbe := httptest.NewRecorder()
	reqProbe, _ := http.NewRequest(http.MethodPost, "/api/probe", bytes.NewReader(probeBody))
	h.ServeHTTP(rwProbe, reqProbe)

	if rwProbe.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /api/probe, got %d", rwProbe.Code)
	}
	var probeResp ProbeResponse
	if err := json.Unmarshal(rwProbe.Body.Bytes(), &probeResp); err != nil {
		t.Fatalf("failed to decode probe response: %v", err)
	}
	if !probeResp.Success {
		t.Errorf("expected probe success")
	}

	// 3. Test POST /api/reset-breaker
	resetBody, _ := json.Marshal(ResetBreakerRequest{URL: "http://worker-1:8000"})
	rwReset := httptest.NewRecorder()
	reqReset, _ := http.NewRequest(http.MethodPost, "/api/reset-breaker", bytes.NewReader(resetBody))
	h.ServeHTTP(rwReset, reqReset)

	if rwReset.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /api/reset-breaker, got %d", rwReset.Code)
	}
	var resetResp ResetBreakerResponse
	if err := json.Unmarshal(rwReset.Body.Bytes(), &resetResp); err != nil {
		t.Fatalf("failed to decode reset response: %v", err)
	}
	if !resetResp.Success {
		t.Errorf("expected reset success")
	}
}

func TestDashboardConfigAPI(t *testing.T) {
	mock := &mockProvider{}
	h := NewHandler(mock)

	// 1. Test GET /api/config
	rwGet := httptest.NewRecorder()
	reqGet, _ := http.NewRequest(http.MethodGet, "/api/config", nil)
	h.ServeHTTP(rwGet, reqGet)

	if rwGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /api/config, got %d", rwGet.Code)
	}
	var snap ConfigSnapshot
	if err := json.Unmarshal(rwGet.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to decode config snapshot: %v", err)
	}
	if snap.Mode != "proxy" || snap.Policy != "consistent_hash" {
		t.Errorf("unexpected config snapshot: %+v", snap)
	}

	// 2. Test POST /api/config
	newMode := "run"
	newPolicy := "power_of_two"
	updateBody, _ := json.Marshal(ConfigUpdateRequest{
		Mode:   &newMode,
		Policy: &newPolicy,
	})
	rwPost := httptest.NewRecorder()
	reqPost, _ := http.NewRequest(http.MethodPost, "/api/config", bytes.NewReader(updateBody))
	h.ServeHTTP(rwPost, reqPost)

	if rwPost.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for POST /api/config, got %d", rwPost.Code)
	}
	var updateResp ConfigUpdateResponse
	if err := json.Unmarshal(rwPost.Body.Bytes(), &updateResp); err != nil {
		t.Fatalf("failed to decode update response: %v", err)
	}
	if !updateResp.Success || updateResp.Config.Mode != "run" {
		t.Errorf("unexpected update response: %+v", updateResp)
	}

	// 3. Test POST /api/models/rule
	modelBody, _ := json.Marshal(ModelRuleUpdateRequest{
		ModelName: "Qwen3.6-27B",
		Mode:      "run",
		Policy:    "power_of_two",
	})
	rwModel := httptest.NewRecorder()
	reqModel, _ := http.NewRequest(http.MethodPost, "/api/models/rule", bytes.NewReader(modelBody))
	h.ServeHTTP(rwModel, reqModel)

	if rwModel.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for POST /api/models/rule, got %d", rwModel.Code)
	}
	var modelResp ConfigUpdateResponse
	if err := json.Unmarshal(rwModel.Body.Bytes(), &modelResp); err != nil {
		t.Fatalf("failed to decode model response: %v", err)
	}
	if !modelResp.Success || len(modelResp.Config.Models) == 0 || modelResp.Config.Models[0].ModelName != "Qwen3.6-27B" {
		t.Errorf("unexpected model rule response: %+v", modelResp)
	}
}

