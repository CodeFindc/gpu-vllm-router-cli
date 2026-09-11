package gpustack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"gpu-vllm-router/pkg/config"
)

func TestAutoHealer_ShouldRestart(t *testing.T) {
	cfg := config.AutoHealConfig{
		Enabled:          true,
		UnhealthyTimeout: 1 * time.Minute,
		FatalKeywords: []string{
			"out of memory",
			"nccl",
			"cuda error",
			"engine is dead",
		},
	}

	healer := NewAutoHealer(nil, cfg)

	// 1. Transient error within timeout -> false
	should, _ := healer.ShouldRestart(time.Now().Add(-10*time.Second), "connection refused")
	if should {
		t.Errorf("Expected false for transient connection error within timeout, got true")
	}

	// 2. Fatal CUDA OOM error -> true immediately
	should, reason := healer.ShouldRestart(time.Now().Add(-5*time.Second), "RuntimeError: CUDA out of memory. Tried to allocate 2.00 GiB")
	if !should {
		t.Errorf("Expected true for CUDA OOM, got false")
	}
	if !strings.Contains(reason, "out of memory") {
		t.Errorf("Expected reason to mention out of memory, got %q", reason)
	}

	// 3. Fatal NCCL error -> true immediately
	should, reason = healer.ShouldRestart(time.Time{}, "RuntimeError: NCCL error: unhandled system error / broken pipe")
	if !should {
		t.Errorf("Expected true for NCCL error, got false")
	}
	if !strings.Contains(reason, "nccl") {
		t.Errorf("Expected reason to mention nccl, got %q", reason)
	}

	// 4. Timeout exceeded -> true
	should, reason = healer.ShouldRestart(time.Now().Add(-2*time.Minute), "i/o timeout")
	if !should {
		t.Errorf("Expected true for timeout exceeded, got false")
	}
	if !strings.Contains(reason, "Unhealthy timeout exceeded") {
		t.Errorf("Expected reason to mention timeout exceeded, got %q", reason)
	}

	// 5. When disabled -> false
	disabledHealer := NewAutoHealer(nil, config.AutoHealConfig{Enabled: false})
	should, _ = disabledHealer.ShouldRestart(time.Now().Add(-10*time.Minute), "CUDA out of memory")
	if should {
		t.Errorf("Expected false when auto-heal is disabled")
	}
}

func TestAutoHealer_TriggerIncidentRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "crash_logs_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var logRequested bool
	var restartRequested bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v2/model-instances/101/logs"):
			logRequested = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("2026-09-11 12:00:00 [ERROR] vllm.engine.async_llm_engine: Engine is dead!\n2026-09-11 12:00:01 [FATAL] CUDA out of memory.\n"))
		case r.URL.Path == "/v2/model-instances/101" && r.Method == http.MethodDelete:
			restartRequested = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"deleted"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client, err := NewClient(Config{
		BaseURL: ts.URL,
		APIKey:  "test-key",
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	cfg := config.AutoHealConfig{
		Enabled:            true,
		UnhealthyTimeout:   1 * time.Minute,
		CrashLogDir:        tmpDir,
		MaxRestartAttempts: 2,
		RestartCooldown:    500 * time.Millisecond,
	}
	healer := NewAutoHealer(client, cfg)

	ep := WorkerEndpoint{
		ModelName:    "deepseek-v3",
		InstanceName: "deepseek-worker-1",
		InstanceID:   101,
		WorkerName:   "node-gpu-0",
		URL:          "http://192.168.1.50:8001",
	}

	// 1. Trigger first restart
	ctx := context.Background()
	crashPath, err := healer.TriggerIncidentRestart(ctx, ep, "CUDA OOM detected", "RuntimeError: CUDA out of memory")
	if err != nil {
		t.Fatalf("Unexpected error triggering restart: %v", err)
	}
	if !logRequested {
		t.Errorf("Expected logs to be requested from GPUStack API")
	}
	if !restartRequested {
		t.Errorf("Expected restart API (DELETE /v2/model-instances/101) to be called")
	}
	if crashPath == "" {
		t.Fatalf("Expected crashPath to be returned")
	}

	// Verify file on disk
	content, err := os.ReadFile(crashPath)
	if err != nil {
		t.Fatalf("Failed to read generated crash log file: %v", err)
	}
	strContent := string(content)
	if !strings.Contains(strContent, "INCIDENT CRASH AUDIT REPORT") {
		t.Errorf("Expected audit report header, got: %s", strContent)
	}
	if !strings.Contains(strContent, "deepseek-v3") || !strings.Contains(strContent, "101") {
		t.Errorf("Expected model and instance metadata in report")
	}
	if !strings.Contains(strContent, "CUDA out of memory") {
		t.Errorf("Expected container log content in report")
	}

	// 2. Immediate second call should be skipped due to cooldown
	skipPath, skipErr := healer.TriggerIncidentRestart(ctx, ep, "CUDA OOM detected", "CUDA out of memory")
	if skipErr != nil {
		t.Fatalf("Expected no error on cooldown skip: %v", skipErr)
	}
	if skipPath != "" {
		t.Errorf("Expected empty path on cooldown skip, got %s", skipPath)
	}

	// 3. Wait for cooldown to expire
	time.Sleep(550 * time.Millisecond)

	// Second valid restart attempt
	secondPath, secondErr := healer.TriggerIncidentRestart(ctx, ep, "Second failure", "Engine is dead")
	if secondErr != nil {
		t.Fatalf("Second restart failed: %v", secondErr)
	}
	if secondPath == "" {
		t.Fatalf("Expected second restart to succeed")
	}

	// 4. Wait for cooldown to expire again
	time.Sleep(550 * time.Millisecond)

	// Third attempt should exceed MaxRestartAttempts (2)
	thirdPath, thirdErr := healer.TriggerIncidentRestart(ctx, ep, "Third failure", "Engine is dead")
	if thirdErr == nil || !strings.Contains(thirdErr.Error(), "exceeded maximum restart attempts") {
		t.Fatalf("Expected max restart attempts error, got: %v", thirdErr)
	}
	if thirdPath != "" {
		t.Errorf("Expected third restart to be refused because MaxRestartAttempts was reached")
	}

	// 5. Test ResetAttempts: should allow new restart
	healer.ResetAttempts(101)
	rec, ok := healer.GetRecord(101)
	if !ok || rec.Attempts != 0 {
		t.Errorf("Expected attempts to reset to 0, got %d", rec.Attempts)
	}
}

func TestAutoHealer_BootingGuardAndStateMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "crash_logs_booting_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var currentInstanceState string
	var currentStateMessage string
	var restartDeleted bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/model-instances/202" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(ModelInstancePublic{
				ID:           202,
				Name:         "deepseek-booting-1",
				ModelName:    "deepseek-v3",
				State:        currentInstanceState,
				StateMessage: currentStateMessage,
			})
		case strings.HasPrefix(r.URL.Path, "/v2/model-instances/202/logs"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("container logs sample"))
		case r.URL.Path == "/v2/model-instances/202" && r.Method == http.MethodDelete:
			restartDeleted = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"deleted"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client, err := NewClient(Config{
		BaseURL: ts.URL,
		APIKey:  "test-key",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	cfg := config.AutoHealConfig{
		Enabled:            true,
		UnhealthyTimeout:   1 * time.Minute,
		CrashLogDir:        tmpDir,
		MaxRestartAttempts: 3,
		RestartCooldown:    100 * time.Millisecond,
		FatalKeywords:      []string{"out of memory", "nccl"},
	}
	healer := NewAutoHealer(client, cfg)

	ep := WorkerEndpoint{
		ModelName:    "deepseek-v3",
		InstanceName: "deepseek-booting-1",
		InstanceID:   202,
		URL:          "http://192.168.1.50:8002",
	}

	// Case 1: Booting Guard - instance is currently "downloading"
	currentInstanceState = "downloading"
	ctx := context.Background()
	path, err := healer.EvaluateAndTrigger(ctx, ep, time.Now().Add(-5*time.Minute), "connection refused")
	if err != nil {
		t.Fatalf("EvaluateAndTrigger returned error: %v", err)
	}
	if path != "" || restartDeleted {
		t.Errorf("Booting Guard failed: instance in downloading state should NOT be restarted")
	}

	// Case 2: Booting Guard - instance is "starting"
	currentInstanceState = "starting"
	path, _ = healer.EvaluateAndTrigger(ctx, ep, time.Now().Add(-5*time.Minute), "connection refused")
	if path != "" || restartDeleted {
		t.Errorf("Booting Guard failed: instance in starting state should NOT be restarted")
	}

	// Case 3: Authoritative StateMessage check - probe only sees "connection refused",
	// but GPUStack reports State="error" and StateMessage="RuntimeError: CUDA out of memory"
	currentInstanceState = "error"
	currentStateMessage = "RuntimeError: CUDA out of memory. Tried to allocate 4.00 GiB"
	path, err = healer.EvaluateAndTrigger(ctx, ep, time.Now().Add(-5*time.Second), "connectex: connection refused")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !restartDeleted {
		t.Errorf("Expected restart to be triggered for GPUStack StateMessage with CUDA OOM")
	}
	if path == "" {
		t.Errorf("Expected crash report path to be generated")
	}
}
