package gpustack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGPUStackClient(t *testing.T) {
	// Mock GPUStack server
	loggedIn := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/login":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			username := r.FormValue("username")
			password := r.FormValue("password")
			if username == "admin" && password == "mock_password_123" {
				loggedIn = true
				http.SetCookie(w, &http.Cookie{
					Name:  "gpustack_session",
					Value: "mock-session-token",
					Path:  "/",
				})
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"ok"}`))
			} else {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"code":401,"reason":"Unauthorized"}`))
			}
		case "/v2/models":
			if !loggedIn && r.Header.Get("Authorization") == "" {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"code":401,"reason":"Unauthorized"}`))
				return
			}
			models := PaginatedList[ModelPublic]{
				Items: []ModelPublic{
					{
						ID:       13,
						Name:     "DeepSeek-V4-Flash-0731-w8a8",
						Replicas: 1,
						Backend:  "vLLM",
					},
					{
						ID:       8,
						Name:     "Qwen3.6-27B",
						Replicas: 0,
						Backend:  "vLLM",
					},
				},
				Pagination: Pagination{Page: 1, PerPage: 100, Total: 2, TotalPage: 1},
			}
			json.NewEncoder(w).Encode(models)
		case "/v2/models/13/instances":
			if !loggedIn && r.Header.Get("Authorization") == "" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			port := 40039
			instances := PaginatedList[ModelInstancePublic]{
				Items: []ModelInstancePublic{
					{
						ID:                     189,
						Name:                   "instance-1",
						ModelID:                13,
						ModelName:              "DeepSeek-V4-Flash-0731-w8a8",
						WorkerName:             "npu-35-240",
						WorkerAdvertiseAddress: "192.168.1.10",
						WorkerIP:               "192.168.1.10",
						Port:                   &port,
						Ports:                  []int{40039},
						State:                  "running",
						Backend:                "vLLM",
					},
					{
						ID:        190,
						Name:      "instance-2-stopped",
						ModelID:   13,
						ModelName: "DeepSeek-V4-Flash-0731-w8a8",
						Port:      &port,
						State:     "stopped",
					},
				},
				Pagination: Pagination{Page: 1, PerPage: 100, Total: 2, TotalPage: 1},
			}
			json.NewEncoder(w).Encode(instances)
		case "/v2/model-instances":
			if !loggedIn && r.Header.Get("Authorization") == "" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			port1 := 40039
			port2 := 40040
			instances := PaginatedList[ModelInstancePublic]{
				Items: []ModelInstancePublic{
					{
						ID:                     189,
						Name:                   "instance-1",
						ModelID:                13,
						ModelName:              "DeepSeek-V4-Flash-0731-w8a8",
						WorkerName:             "npu-35-240",
						WorkerAdvertiseAddress: "192.168.1.10",
						Port:                   &port1,
						State:                  "running",
						Backend:                "vLLM",
					},
					{
						ID:                     191,
						Name:                   "instance-qwen",
						ModelID:                8,
						ModelName:              "Qwen3.6-27B",
						WorkerName:             "npu-35-241",
						WorkerAdvertiseAddress: "192.168.1.11",
						Port:                   &port2,
						State:                  "running",
						Backend:                "vLLM",
					},
				},
				Pagination: Pagination{Page: 1, PerPage: 100, Total: 2, TotalPage: 1},
			}
			json.NewEncoder(w).Encode(instances)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx := context.Background()
	cfg := Config{
		BaseURL:  ts.URL,
		Username: "admin",
		Password: "mock_password_123",
		Timeout:  2 * time.Second,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test GetRunningWorkerEndpoints
	endpoints, model, err := client.GetRunningWorkerEndpoints(ctx, "DeepSeek-V4-Flash-0731-w8a8")
	if err != nil {
		t.Fatalf("GetRunningWorkerEndpoints error: %v", err)
	}

	if model == nil || model.ID != 13 {
		t.Fatalf("Expected model ID 13, got %v", model)
	}

	if len(endpoints) != 1 {
		t.Fatalf("Expected 1 running endpoint, got %d", len(endpoints))
	}

	expectedURL := "http://192.168.1.10:40039"
	if endpoints[0].URL != expectedURL {
		t.Errorf("Expected URL %q, got %q", expectedURL, endpoints[0].URL)
	}

	// Test GetAllRunningWorkerEndpoints (Multi-model discovery)
	cluster, err := client.GetAllRunningWorkerEndpoints(ctx)
	if err != nil {
		t.Fatalf("GetAllRunningWorkerEndpoints error: %v", err)
	}

	if cluster.ModelCount != 2 {
		t.Errorf("Expected 2 models in cluster, got %d", cluster.ModelCount)
	}
	if cluster.InstanceCount != 2 {
		t.Errorf("Expected 2 running instances, got %d", cluster.InstanceCount)
	}
	if len(cluster.ModelsEndpoints["DeepSeek-V4-Flash-0731-w8a8"]) != 1 {
		t.Errorf("Expected 1 DeepSeek instance")
	}
	if len(cluster.ModelsEndpoints["Qwen3.6-27B"]) != 1 {
		t.Errorf("Expected 1 Qwen instance")
	}
}
