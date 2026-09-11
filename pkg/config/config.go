package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// GPUStackConfig holds settings for connecting to GPUStack.
type GPUStackConfig struct {
	BaseURL  string        `yaml:"base_url"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	APIKey   string        `yaml:"api_key"`
	Timeout  time.Duration `yaml:"timeout"`
}

// TargetConfig holds settings for the model to be routed.
type TargetConfig struct {
	ModelName string `yaml:"model_name"`
	Policy    string `yaml:"policy"`
}

// RouterConfig holds settings for the router service.
type RouterConfig struct {
	Mode                       string        `yaml:"mode"`
	Host                       string        `yaml:"host"`
	Port                       int           `yaml:"port"`
	RouterBin                  string        `yaml:"router_bin"`
	Backend                    string        `yaml:"backend,omitempty"`
	LogLevel                   string        `yaml:"log_level,omitempty"`
	LogDir                     string        `yaml:"log_dir,omitempty"`
	WatchInterval              time.Duration `yaml:"watch_interval"`
	DataParallelSize           int           `yaml:"data_parallel_size"`
	ZeroDowntime               *bool         `yaml:"zero_downtime"`
	DrainTimeout               time.Duration `yaml:"drain_timeout"`
	BalanceAbsThreshold        *int          `yaml:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold        *float64      `yaml:"balance_rel_threshold,omitempty"`
	CacheThreshold             *float64      `yaml:"cache_threshold,omitempty"`
	EvictionInterval           *int          `yaml:"eviction_interval,omitempty"`
	MaxTreeSize                *int          `yaml:"max_tree_size,omitempty"`
	MaxPayloadSize             *int          `yaml:"max_payload_size,omitempty"`
	WorkerStartupTimeoutSecs   *int          `yaml:"worker_startup_timeout_secs,omitempty"`
	WorkerStartupCheckInterval *int          `yaml:"worker_startup_check_interval,omitempty"`
	VllmPDDisagg               *bool         `yaml:"vllm_pd_disaggregation,omitempty"`
	VllmDiscoveryAddress       string        `yaml:"vllm_discovery_address,omitempty"`
	PrefillPolicy              string        `yaml:"prefill_policy,omitempty"`
	DecodePolicy               string        `yaml:"decode_policy,omitempty"`
	PrefillURLs                []string      `yaml:"prefill_urls,omitempty"`
	DecodeURLs                 []string      `yaml:"decode_urls,omitempty"`
	APIKey                     string        `yaml:"api_key,omitempty"`
	APIKeyValidationURLs       []string      `yaml:"api_key_validation_urls,omitempty"`
	ExtraArgs                  []string      `yaml:"extra_args,omitempty"`
}

// CircuitBreakerConfig holds settings for circuit breaker and failover.
type CircuitBreakerConfig struct {
	Enabled                *bool         `yaml:"enabled"`
	MaxFailures            int           `yaml:"max_failures"`
	SuccessThreshold       int           `yaml:"success_threshold"`
	Cooldown               time.Duration `yaml:"cooldown"`
	WindowDuration         time.Duration `yaml:"window_duration"`
	MaxRetries             int           `yaml:"max_retries"`
	RetryInitialBackoff    time.Duration `yaml:"retry_initial_backoff"`
	HealthCheckInterval    time.Duration `yaml:"health_check_interval"`
	HealthCheckTimeout     time.Duration `yaml:"health_check_timeout"`
	HealthFailureThreshold int           `yaml:"health_failure_threshold,omitempty"`
	HealthSuccessThreshold int           `yaml:"health_success_threshold,omitempty"`
	HealthCheckEndpoint    string        `yaml:"health_check_endpoint,omitempty"`
	DisableCircuitBreaker  *bool         `yaml:"disable_circuit_breaker,omitempty"`
	DisableRetries         *bool         `yaml:"disable_retries,omitempty"`
}

// AutoHealConfig holds settings for automated instance restart and crash log preservation upon fatal failures.
type AutoHealConfig struct {
	Enabled            bool          `yaml:"enabled" json:"enabled"`
	UnhealthyTimeout   time.Duration `yaml:"unhealthy_timeout" json:"unhealthy_timeout"`
	FatalKeywords      []string      `yaml:"fatal_keywords" json:"fatal_keywords"`
	CrashLogDir        string        `yaml:"crash_log_dir" json:"crash_log_dir"`
	MaxRestartAttempts int           `yaml:"max_restart_attempts" json:"max_restart_attempts"`
	RestartCooldown    time.Duration `yaml:"restart_cooldown" json:"restart_cooldown"`
}

// DefaultAutoHealConfig provides sensible production defaults.
func DefaultAutoHealConfig() AutoHealConfig {
	return AutoHealConfig{
		Enabled:          false,
		UnhealthyTimeout: 3 * time.Minute,
		FatalKeywords: []string{
			"CUDA out of memory",
			"CUDA error",
			"an illegal memory access",
			"NCCL timeout",
			"NCCL error",
			"Engine is dead",
			"RayActorError",
		},
		CrashLogDir:        "logs/crashes",
		MaxRestartAttempts: 3,
		RestartCooldown:    5 * time.Minute,
	}
}

// ModelRule holds per-model routing override settings.
type ModelRule struct {
	ModelName           string   `yaml:"model_name" json:"model_name"`
	Mode                string   `yaml:"mode,omitempty" json:"mode,omitempty"`     // "proxy" or "run" (empty = inherit global)
	Policy              string   `yaml:"policy,omitempty" json:"policy,omitempty"` // "consistent_hash", etc. (empty = inherit global)
	BalanceAbsThreshold *int     `yaml:"balance_abs_threshold,omitempty" json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold *float64 `yaml:"balance_rel_threshold,omitempty" json:"balance_rel_threshold,omitempty"`
	CacheThreshold      *float64 `yaml:"cache_threshold,omitempty" json:"cache_threshold,omitempty"`
	ExtraArgs           []string `yaml:"extra_args,omitempty" json:"extra_args,omitempty"`
}

// FileConfig represents the full structure of config.yaml.
type FileConfig struct {
	GPUStack       GPUStackConfig       `yaml:"gpustack"`
	Target         TargetConfig         `yaml:"target"`
	Router         RouterConfig         `yaml:"router"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	AutoHeal       AutoHealConfig       `yaml:"auto_heal"`
	Models         []ModelRule          `yaml:"models,omitempty" json:"models,omitempty"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var cfg FileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config file %q: %w", path, err)
	}

	return &cfg, nil
}

// SaveConfig safely writes the FileConfig to the specified path.
func SaveConfig(path string, cfg *FileConfig) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML config: %w", err)
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary config file: %w", err)
	}

	// Windows-safe rename with remove fallback
	_ = os.Remove(path)
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		if writeErr := os.WriteFile(path, data, 0644); writeErr != nil {
			return fmt.Errorf("failed to save config file %q: %w", path, writeErr)
		}
	}

	return nil
}
