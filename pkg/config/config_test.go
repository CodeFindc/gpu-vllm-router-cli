package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	yamlContent := `
gpustack:
  base_url: "http://192.168.1.100:8200"
  username: "admin"
  password: "password123"
  api_key: "test-key"
  timeout: 5s

target:
  model_name: "test-model"
  policy: "consistent_hash"

router:
  mode: "run"
  host: "0.0.0.0"
  port: 8080
  router_bin: "vllm-router"
  watch_interval: 15s
  data_parallel_size: 2
  zero_downtime: true
  drain_timeout: 45s
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.GPUStack.BaseURL != "http://192.168.1.100:8200" {
		t.Errorf("expected base_url http://192.168.1.100:8200, got %s", cfg.GPUStack.BaseURL)
	}
	if cfg.GPUStack.Username != "admin" {
		t.Errorf("expected username admin, got %s", cfg.GPUStack.Username)
	}
	if cfg.GPUStack.Timeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", cfg.GPUStack.Timeout)
	}
	if cfg.Target.ModelName != "test-model" {
		t.Errorf("expected model test-model, got %s", cfg.Target.ModelName)
	}
	if cfg.Router.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Router.Port)
	}
	if cfg.Router.ZeroDowntime == nil || !*cfg.Router.ZeroDowntime {
		t.Errorf("expected zero_downtime true, got %v", cfg.Router.ZeroDowntime)
	}
	if cfg.Router.DrainTimeout != 45*time.Second {
		t.Errorf("expected drain_timeout 45s, got %v", cfg.Router.DrainTimeout)
	}
}

func TestSaveAndLoadConfigWithModels(t *testing.T) {
	tmpDir := t.TempDir()
	cfgFile := filepath.Join(tmpDir, "config.yaml")

	zeroDown := true
	cfg := &FileConfig{
		GPUStack: GPUStackConfig{
			BaseURL: "http://127.0.0.1:8200",
		},
		Target: TargetConfig{
			Policy: "consistent_hash",
		},
		Router: RouterConfig{
			Mode:         "proxy",
			Host:         "0.0.0.0",
			Port:         8000,
			ZeroDowntime: &zeroDown,
		},
		CircuitBreaker: CircuitBreakerConfig{
			MaxFailures: 3,
			Cooldown:    10 * time.Second,
		},
		Models: []ModelRule{
			{ModelName: "DeepSeek-V4", Mode: "proxy", Policy: "consistent_hash"},
			{ModelName: "Qwen3.6-27B", Mode: "run", Policy: "power_of_two"},
		},
	}

	if err := SaveConfig(cfgFile, cfg); err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	loaded, err := LoadConfig(cfgFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if loaded.Router.Mode != "proxy" {
		t.Errorf("expected router.mode proxy, got %s", loaded.Router.Mode)
	}
	if len(loaded.Models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(loaded.Models))
	}
	if loaded.Models[0].ModelName != "DeepSeek-V4" || loaded.Models[0].Mode != "proxy" {
		t.Errorf("unexpected model 0: %+v", loaded.Models[0])
	}
	if loaded.Models[1].ModelName != "Qwen3.6-27B" || loaded.Models[1].Mode != "run" {
		t.Errorf("unexpected model 1: %+v", loaded.Models[1])
	}
}

func TestLoadConfigWithTuningParameters(t *testing.T) {
	yamlContent := `
target:
  policy: "cache_aware"

router:
  mode: "run"
  balance_abs_threshold: 4
  balance_rel_threshold: 1.1
  cache_threshold: 0.6
  extra_args:
    - "--request-timeout"
    - "60"

models:
  - model_name: "custom-cache-model"
    mode: "run"
    policy: "rendezvous_hash"
    balance_abs_threshold: 8
    balance_rel_threshold: 1.2
    cache_threshold: 0.8
    extra_args: ["--max-num-seqs", "256"]
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Target.Policy != "cache_aware" {
		t.Errorf("expected target.policy cache_aware, got %s", cfg.Target.Policy)
	}
	if cfg.Router.BalanceAbsThreshold == nil || *cfg.Router.BalanceAbsThreshold != 4 {
		t.Errorf("expected balance_abs_threshold 4, got %v", cfg.Router.BalanceAbsThreshold)
	}
	if cfg.Router.BalanceRelThreshold == nil || *cfg.Router.BalanceRelThreshold != 1.1 {
		t.Errorf("expected balance_rel_threshold 1.1, got %v", cfg.Router.BalanceRelThreshold)
	}
	if cfg.Router.CacheThreshold == nil || *cfg.Router.CacheThreshold != 0.6 {
		t.Errorf("expected cache_threshold 0.6, got %v", cfg.Router.CacheThreshold)
	}
	if len(cfg.Router.ExtraArgs) != 2 || cfg.Router.ExtraArgs[0] != "--request-timeout" {
		t.Errorf("unexpected router extra_args: %v", cfg.Router.ExtraArgs)
	}

	if len(cfg.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(cfg.Models))
	}
	m := cfg.Models[0]
	if m.Policy != "rendezvous_hash" {
		t.Errorf("expected model policy rendezvous_hash, got %s", m.Policy)
	}
	if m.BalanceAbsThreshold == nil || *m.BalanceAbsThreshold != 8 {
		t.Errorf("expected model balance_abs_threshold 8, got %v", m.BalanceAbsThreshold)
	}
	if m.BalanceRelThreshold == nil || *m.BalanceRelThreshold != 1.2 {
		t.Errorf("expected model balance_rel_threshold 1.2, got %v", m.BalanceRelThreshold)
	}
	if m.CacheThreshold == nil || *m.CacheThreshold != 0.8 {
		t.Errorf("expected model cache_threshold 0.8, got %v", m.CacheThreshold)
	}
	if len(m.ExtraArgs) != 2 || m.ExtraArgs[1] != "256" {
		t.Errorf("unexpected model extra_args: %v", m.ExtraArgs)
	}
}

func TestLoadConfigExtendedRouterOptions(t *testing.T) {
	yamlContent := `
router:
  mode: "run"
  backend: "sglang"
  log_level: "debug"
  log_dir: "/var/log/router"
  eviction_interval: 240
  max_tree_size: 1048576
  max_payload_size: 104857600
  worker_startup_timeout_secs: 300
  worker_startup_check_interval: 15
  vllm_pd_disaggregation: true
  vllm_discovery_address: "127.0.0.1:30001"
  prefill_policy: "cache_aware"
  decode_policy: "power_of_two"
  prefill_urls:
    - "http://10.0.0.1:8000"
  decode_urls:
    - "http://10.0.0.2:8000"
  api_key: "secret-key-123"
  api_key_validation_urls:
    - "http://auth.local/verify"

circuit_breaker:
  health_failure_threshold: 4
  health_success_threshold: 3
  health_check_endpoint: "/v1/health"
  disable_circuit_breaker: true
  disable_retries: true
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Router.Backend != "sglang" {
		t.Errorf("expected backend sglang, got %s", cfg.Router.Backend)
	}
	if cfg.Router.LogLevel != "debug" {
		t.Errorf("expected log_level debug, got %s", cfg.Router.LogLevel)
	}
	if cfg.Router.LogDir != "/var/log/router" {
		t.Errorf("expected log_dir /var/log/router, got %s", cfg.Router.LogDir)
	}
	if cfg.Router.EvictionInterval == nil || *cfg.Router.EvictionInterval != 240 {
		t.Errorf("expected eviction_interval 240, got %v", cfg.Router.EvictionInterval)
	}
	if cfg.Router.MaxTreeSize == nil || *cfg.Router.MaxTreeSize != 1048576 {
		t.Errorf("expected max_tree_size 1048576, got %v", cfg.Router.MaxTreeSize)
	}
	if cfg.Router.MaxPayloadSize == nil || *cfg.Router.MaxPayloadSize != 104857600 {
		t.Errorf("expected max_payload_size 104857600, got %v", cfg.Router.MaxPayloadSize)
	}
	if cfg.Router.WorkerStartupTimeoutSecs == nil || *cfg.Router.WorkerStartupTimeoutSecs != 300 {
		t.Errorf("expected worker_startup_timeout_secs 300, got %v", cfg.Router.WorkerStartupTimeoutSecs)
	}
	if cfg.Router.WorkerStartupCheckInterval == nil || *cfg.Router.WorkerStartupCheckInterval != 15 {
		t.Errorf("expected worker_startup_check_interval 15, got %v", cfg.Router.WorkerStartupCheckInterval)
	}
	if cfg.Router.VllmPDDisagg == nil || !*cfg.Router.VllmPDDisagg {
		t.Errorf("expected vllm_pd_disaggregation true, got %v", cfg.Router.VllmPDDisagg)
	}
	if cfg.Router.VllmDiscoveryAddress != "127.0.0.1:30001" {
		t.Errorf("expected vllm_discovery_address 127.0.0.1:30001, got %s", cfg.Router.VllmDiscoveryAddress)
	}
	if cfg.Router.PrefillPolicy != "cache_aware" {
		t.Errorf("expected prefill_policy cache_aware, got %s", cfg.Router.PrefillPolicy)
	}
	if cfg.Router.DecodePolicy != "power_of_two" {
		t.Errorf("expected decode_policy power_of_two, got %s", cfg.Router.DecodePolicy)
	}
	if len(cfg.Router.PrefillURLs) != 1 || cfg.Router.PrefillURLs[0] != "http://10.0.0.1:8000" {
		t.Errorf("unexpected prefill_urls: %v", cfg.Router.PrefillURLs)
	}
	if len(cfg.Router.DecodeURLs) != 1 || cfg.Router.DecodeURLs[0] != "http://10.0.0.2:8000" {
		t.Errorf("unexpected decode_urls: %v", cfg.Router.DecodeURLs)
	}
	if cfg.Router.APIKey != "secret-key-123" {
		t.Errorf("expected api_key secret-key-123, got %s", cfg.Router.APIKey)
	}
	if len(cfg.Router.APIKeyValidationURLs) != 1 || cfg.Router.APIKeyValidationURLs[0] != "http://auth.local/verify" {
		t.Errorf("unexpected api_key_validation_urls: %v", cfg.Router.APIKeyValidationURLs)
	}

	if cfg.CircuitBreaker.HealthFailureThreshold != 4 {
		t.Errorf("expected health_failure_threshold 4, got %d", cfg.CircuitBreaker.HealthFailureThreshold)
	}
	if cfg.CircuitBreaker.HealthSuccessThreshold != 3 {
		t.Errorf("expected health_success_threshold 3, got %d", cfg.CircuitBreaker.HealthSuccessThreshold)
	}
	if cfg.CircuitBreaker.HealthCheckEndpoint != "/v1/health" {
		t.Errorf("expected health_check_endpoint /v1/health, got %s", cfg.CircuitBreaker.HealthCheckEndpoint)
	}
	if cfg.CircuitBreaker.DisableCircuitBreaker == nil || !*cfg.CircuitBreaker.DisableCircuitBreaker {
		t.Errorf("expected disable_circuit_breaker true, got %v", cfg.CircuitBreaker.DisableCircuitBreaker)
	}
	if cfg.CircuitBreaker.DisableRetries == nil || !*cfg.CircuitBreaker.DisableRetries {
		t.Errorf("expected disable_retries true, got %v", cfg.CircuitBreaker.DisableRetries)
	}
}

func TestLoadConfigExampleYaml(t *testing.T) {
	// Verify that the actual config.example.yaml in the project root loads cleanly
	examplePath := filepath.Join("..", "..", "config.example.yaml")
	cfg, err := LoadConfig(examplePath)
	if err != nil {
		t.Fatalf("failed to load config.example.yaml: %v", err)
	}

	if cfg.Router.Backend != "vllm" {
		t.Errorf("expected default backend vllm, got %s", cfg.Router.Backend)
	}
	if cfg.Router.BalanceAbsThreshold == nil || *cfg.Router.BalanceAbsThreshold != 64 {
		t.Errorf("expected balance_abs_threshold 64, got %v", cfg.Router.BalanceAbsThreshold)
	}
	if cfg.Router.BalanceRelThreshold == nil || *cfg.Router.BalanceRelThreshold != 1.5 {
		t.Errorf("expected balance_rel_threshold 1.5, got %v", cfg.Router.BalanceRelThreshold)
	}
	if cfg.Router.CacheThreshold == nil || *cfg.Router.CacheThreshold != 0.3 {
		t.Errorf("expected cache_threshold 0.3, got %v", cfg.Router.CacheThreshold)
	}
	if cfg.Router.EvictionInterval == nil || *cfg.Router.EvictionInterval != 120 {
		t.Errorf("expected eviction_interval 120, got %v", cfg.Router.EvictionInterval)
	}
	if cfg.CircuitBreaker.HealthCheckEndpoint != "/health" {
		t.Errorf("expected health_check_endpoint /health, got %s", cfg.CircuitBreaker.HealthCheckEndpoint)
	}
}

