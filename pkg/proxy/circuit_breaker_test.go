package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gpu-vllm-router/pkg/router"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	cfg := CircuitBreakerConfig{
		MaxFailures: 3,
		Cooldown:    50 * time.Millisecond,
		MaxRetries:  2,
	}
	cb := NewCircuitBreaker("http://mock-worker:8000", cfg)

	if !cb.CanExecute() {
		t.Fatalf("Expected initial state to allow execution")
	}
	state, fails, _ := cb.GetStatus()
	if state != StateClosed || fails != 0 {
		t.Fatalf("Expected StateClosed with 0 failures, got %s with %d fails", state, fails)
	}

	// 1st failure
	cb.RecordFailure(errors.New("dial error 1"))
	state, fails, _ = cb.GetStatus()
	if state != StateClosed || fails != 1 {
		t.Fatalf("Expected StateClosed with 1 failure, got %s, %d", state, fails)
	}

	// 2nd failure
	cb.RecordFailure(errors.New("dial error 2"))
	state, fails, _ = cb.GetStatus()
	if state != StateClosed || fails != 2 {
		t.Fatalf("Expected StateClosed with 2 failures, got %s, %d", state, fails)
	}

	// 3rd failure -> should TRIP OPEN!
	cb.RecordFailure(errors.New("dial error 3"))
	state, fails, _ = cb.GetStatus()
	if state != StateOpen {
		t.Fatalf("Expected StateOpen after 3 failures, got %s", state)
	}
	if cb.CanExecute() {
		t.Fatalf("Expected CanExecute() to return false while in StateOpen during cooldown")
	}

	// Wait for cooldown
	time.Sleep(70 * time.Millisecond)

	// Now CanExecute should enter HalfOpen and allow trial request
	if !cb.CanExecute() {
		t.Fatalf("Expected CanExecute() to return true after cooldown (transitioning to HalfOpen)")
	}
	state, _, _ = cb.GetStatus()
	if state != StateHalfOpen {
		t.Fatalf("Expected StateHalfOpen, got %s", state)
	}

	// Successful trial request -> should restore to CLOSED
	cb.RecordSuccess()
	state, fails, _ = cb.GetStatus()
	if state != StateClosed || fails != 0 {
		t.Fatalf("Expected restored to StateClosed with 0 failures, got %s, %d", state, fails)
	}
}

func TestCircuitBreakerActiveProbe(t *testing.T) {
	var healthy int32 = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&healthy) == 1 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	cfg := CircuitBreakerConfig{
		MaxFailures: 1,
		Cooldown:    10 * time.Second,
	}
	cb := NewCircuitBreaker(server.URL, cfg)
	cb.RecordFailure(errors.New("initial failure"))

	state, _, _ := cb.GetStatus()
	if state != StateOpen {
		t.Fatalf("Expected StateOpen, got %s", state)
	}

	// First probe when unhealthy
	if cb.Probe() {
		t.Fatalf("Expected probe to fail when server is unhealthy")
	}

	// Now make server healthy
	atomic.StoreInt32(&healthy, 1)

	// Probe again -> should succeed and restore to CLOSED
	if !cb.Probe() {
		t.Fatalf("Expected probe to succeed when server is healthy")
	}

	state, _, _ = cb.GetStatus()
	if state != StateClosed {
		t.Fatalf("Expected StateClosed after successful probe, got %s", state)
	}
}

func TestBalancerCircuitBreakerFiltering(t *testing.T) {
	uA, _ := url.Parse("http://worker-a:8000")
	uB, _ := url.Parse("http://worker-b:8000")

	cbA := NewCircuitBreaker(uA.String(), CircuitBreakerConfig{MaxFailures: 1, Cooldown: 10 * time.Second})
	cbB := NewCircuitBreaker(uB.String(), CircuitBreakerConfig{MaxFailures: 1, Cooldown: 10 * time.Second})

	targetA := &BackendTarget{URL: uA, URLString: uA.String(), CircuitBreaker: cbA}
	targetB := &BackendTarget{URL: uB, URLString: uB.String(), CircuitBreaker: cbB}

	// Test RoundRobin
	rr := NewRoundRobinBalancer([]*BackendTarget{targetA, targetB})
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)

	// Trip targetA
	cbA.RecordFailure(errors.New("node A down"))

	// All selections should exclusively return targetB
	for i := 0; i < 5; i++ {
		target, err := rr.SelectTarget(req)
		if err != nil {
			t.Fatalf("SelectTarget error: %v", err)
		}
		if target.URLString != uB.String() {
			t.Fatalf("Expected targetB, got %s", target.URLString)
		}
	}

	// Test ConsistentHash
	ch := NewConsistentHashBalancer([]*BackendTarget{targetA, targetB}, 100)
	for i := 0; i < 5; i++ {
		target, err := ch.SelectTarget(req)
		if err != nil {
			t.Fatalf("ConsistentHash SelectTarget error: %v", err)
		}
		if target.URLString != uB.String() {
			t.Fatalf("Expected targetB, got %s", target.URLString)
		}
	}

	// Trip targetB as well
	cbB.RecordFailure(errors.New("node B down"))
	_, err := rr.SelectTarget(req)
	if err == nil || !strings.Contains(err.Error(), "circuit breaker") {
		t.Fatalf("Expected circuit breaker open error when all backends down, got: %v", err)
	}
}

func TestProxyFailoverTransparentRetry(t *testing.T) {
	// 1. Allocate a closed listener port to simulate crashed / offline worker
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	deadURL := "http://" + l.Addr().String()
	l.Close() // Immediately close to guarantee connection refused!

	// 2. Start healthy Mock Backend
	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "test-question") {
			t.Errorf("Expected body to contain 'test-question', got: %s", string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"choices":[{"message":{"content":"success from healthy backend"}}]}`))
	}))
	defer healthyServer.Close()

	// Parse URLs
	deadParsed, _ := url.Parse(deadURL)
	healthyParsed, _ := url.Parse(healthyServer.URL)

	cbDead := NewCircuitBreaker(deadURL, CircuitBreakerConfig{MaxFailures: 2, Cooldown: 10 * time.Second, MaxRetries: 2})
	cbHealthy := NewCircuitBreaker(healthyServer.URL, CircuitBreakerConfig{MaxFailures: 2, Cooldown: 10 * time.Second, MaxRetries: 2})

	targetDead := &BackendTarget{URL: deadParsed, URLString: deadURL, CircuitBreaker: cbDead}
	targetHealthy := &BackendTarget{URL: healthyParsed, URLString: healthyServer.URL, CircuitBreaker: cbHealthy}

	// Setup Server manually with dead target first
	serverCfg := ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		Policy: router.PolicyRoundRobin,
		CircuitBreaker: CircuitBreakerConfig{
			MaxFailures: 2,
			Cooldown:    10 * time.Second,
			MaxRetries:  2,
		},
	}
	srv := NewServer(serverCfg, nil)

	// Inject model pool with healthy target first, so counter=1 selects targetDead on initial try
	targets := []*BackendTarget{targetHealthy, targetDead}
	pool := &ModelPool{
		ModelName: "Qwen3.6-27B",
		Balancer:  NewRoundRobinBalancer(targets),
		Targets:   targets,
	}
	srv.modelPools["Qwen3.6-27B"] = pool
	srv.allTargets = targets

	// Send POST chat completion request with JSON body
	reqBody := `{"model":"Qwen3.6-27B","messages":[{"role":"user","content":"test-question"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	// Execute through reverseProxy
	srv.reverseProxy.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected HTTP 200 via transparent failover, got %d: %s", resp.StatusCode, string(respBytes))
	}

	bodyResp, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(bodyResp), "success from healthy backend") {
		t.Fatalf("Expected response from healthy backend, got: %s", string(bodyResp))
	}

	// Verify header shows routed target was healthyServer
	if routedTarget := resp.Header.Get("X-Routed-Target"); routedTarget != healthyServer.URL {
		t.Errorf("Expected X-Routed-Target %s, got %s", healthyServer.URL, routedTarget)
	}

	// Verify dead target has recorded failure
	_, deadFails, _ := cbDead.GetStatus()
	if deadFails < 1 {
		t.Errorf("Expected dead target to have at least 1 failure, got %d", deadFails)
	}

	// Test Prometheus /metrics endpoint
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	srv.handleMetrics(metricsRec, metricsReq)
	metricsResp := metricsRec.Result()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected HTTP 200 from /metrics, got %d", metricsResp.StatusCode)
	}
	metricsBody, _ := io.ReadAll(metricsResp.Body)
	metricsText := string(metricsBody)
	if !strings.Contains(metricsText, "gpu_router_models_total") || !strings.Contains(metricsText, "gpu_router_backend_healthy") {
		t.Errorf("Expected Prometheus metrics to contain router stats, got:\n%s", metricsText)
	}

	// Assert official vllm-project/router metrics compatibility
	expectedStandardMetrics := []string{
		"vllm_router_processed_requests_total",
		"vllm_router_running_requests",
		"vllm_router_cb_outcomes_total",
		"outcome=\"success\"",
		"outcome=\"failure\"",
	}
	for _, m := range expectedStandardMetrics {
		if !strings.Contains(metricsText, m) {
			t.Errorf("Expected Prometheus metrics to contain official standard metric %q, got:\n%s", m, metricsText)
		}
	}
}

func TestProxy_PrometheusMetrics(t *testing.T) {
	srv := NewServer(ServerConfig{
		Host:        "127.0.0.1",
		Port:        8000,
		MetricsPort: 29000,
	}, nil)

	u, _ := url.Parse("http://10.0.0.1:8000")
	cb := NewCircuitBreaker(u.String(), DefaultCircuitBreakerConfig())
	cb.RecordSuccess()
	cb.RecordFailure(errors.New("connection reset"))

	target := &BackendTarget{
		URL:            u,
		URLString:      u.String(),
		CircuitBreaker: cb,
	}
	atomic.StoreInt64(&target.ActiveConns, 5)

	srv.modelPools["deepseek-r1"] = &ModelPool{
		ModelName: "deepseek-r1",
		Targets:   []*BackendTarget{target},
	}
	srv.allTargets = []*BackendTarget{target}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	srv.handleMetrics(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	expected := []string{
		"vllm_router_processed_requests_total{model=\"deepseek-r1\",worker=\"http://10.0.0.1:8000\"} 1",
		"vllm_router_running_requests{model=\"deepseek-r1\",worker=\"http://10.0.0.1:8000\"} 5",
		"vllm_router_cb_outcomes_total{model=\"deepseek-r1\",outcome=\"success\",worker=\"http://10.0.0.1:8000\"} 1",
		"vllm_router_cb_outcomes_total{model=\"deepseek-r1\",outcome=\"failure\",worker=\"http://10.0.0.1:8000\"} 1",
		"gpu_router_models_total 1",
		"gpu_router_backend_healthy",
	}

	for _, exp := range expected {
		if !strings.Contains(text, exp) {
			t.Errorf("expected metrics to contain %q, but got:\n%s", exp, text)
		}
	}
}
