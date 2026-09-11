package gpustack

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
		case r.URL.Path == "/v2/model-instances/101/restart" && r.Method == http.MethodPost:
			restartRequested = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"restarting"}`))
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
		t.Errorf("Expected restart API to be called")
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

	// Ensure crash folder structure is sanitized
	modelDir := filepath.Join(tmpDir, "deepseek-v3")
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s to exist", modelDir)
	}
}
