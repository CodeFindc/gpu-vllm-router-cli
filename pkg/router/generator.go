package router

import (
	"fmt"
	"strconv"
	"strings"
)

// Config holds the configuration to construct vllm-router command.
type Config struct {
	RouterBin         string   `json:"router_bin"` // e.g. "vllm-router"
	Host              string   `json:"host"`       // e.g. "0.0.0.0"
	Port              int      `json:"port"`       // e.g. 8000
	Policy            Policy   `json:"policy"`     // e.g. PolicyConsistentHash
	WorkerURLs        []string `json:"worker_urls"`
	DataParallelSize  int      `json:"data_parallel_size"` // default 1
	Backend           string   `json:"backend"`            // default "vllm"
	LogLevel          string   `json:"log_level"`          // default "info"
	LogDir            string   `json:"log_dir,omitempty"`
	VllmPDDisagg      bool     `json:"vllm_pd_disaggregation"`
	VllmDiscoveryAddress string `json:"vllm_discovery_address,omitempty"`
	PrefillPolicy     string   `json:"prefill_policy,omitempty"`
	DecodePolicy      string   `json:"decode_policy,omitempty"`
	PrefillURLs       []string `json:"prefill_urls"`
	DecodeURLs        []string `json:"decode_urls"`
	ExtraArgs         []string `json:"extra_args"`

	// Tuning parameters for official vllm-router
	BalanceAbsThreshold int     `json:"balance_abs_threshold,omitempty"` // --balance-abs-threshold
	BalanceRelThreshold float64 `json:"balance_rel_threshold,omitempty"` // --balance-rel-threshold
	CacheThreshold      float64 `json:"cache_threshold,omitempty"`       // --cache-threshold
	EvictionInterval    int     `json:"eviction_interval,omitempty"`     // --eviction-interval
	MaxTreeSize         int     `json:"max_tree_size,omitempty"`         // --max-tree-size
	MaxPayloadSize      int     `json:"max_payload_size,omitempty"`      // --max-payload-size

	// Worker startup flags
	WorkerStartupTimeoutSecs   int `json:"worker_startup_timeout_secs,omitempty"`   // --worker-startup-timeout-secs
	WorkerStartupCheckInterval int `json:"worker_startup_check_interval,omitempty"` // --worker-startup-check-interval

	// Authentication & Security
	APIKey               string   `json:"api_key,omitempty"`                 // --api-key
	APIKeyValidationURLs []string `json:"api_key_validation_urls,omitempty"` // --api-key-validation-urls

	// Built-in Circuit Breaker options for official vllm-router
	CbFailureThreshold    int  `json:"cb_failure_threshold,omitempty"`     // --cb-failure-threshold
	CbSuccessThreshold    int  `json:"cb_success_threshold,omitempty"`     // --cb-success-threshold
	CbTimeoutDurationSecs int  `json:"cb_timeout_duration_secs,omitempty"` // --cb-timeout-duration-secs
	CbWindowDurationSecs  int  `json:"cb_window_duration_secs,omitempty"`  // --cb-window-duration-secs
	DisableCircuitBreaker bool `json:"disable_circuit_breaker,omitempty"`  // --disable-circuit-breaker

	// Built-in Retry options for official vllm-router
	RetryMaxRetries       int  `json:"retry_max_retries,omitempty"`        // --retry-max-retries
	RetryInitialBackoffMs int  `json:"retry_initial_backoff_ms,omitempty"` // --retry-initial-backoff-ms
	DisableRetries        bool `json:"disable_retries,omitempty"`          // --disable-retries

	// Built-in Health Check options for official vllm-router
	HealthFailureThreshold  int    `json:"health_failure_threshold,omitempty"`   // --health-failure-threshold
	HealthSuccessThreshold  int    `json:"health_success_threshold,omitempty"`   // --health-success-threshold
	HealthCheckTimeoutSecs  int    `json:"health_check_timeout_secs,omitempty"`  // --health-check-timeout-secs
	HealthCheckIntervalSecs int    `json:"health_check_interval_secs,omitempty"` // --health-check-interval-secs
	HealthCheckEndpoint     string `json:"health_check_endpoint,omitempty"`      // --health-check-endpoint
}

// BuildArgs constructs the slice of command line arguments for vllm-router.
func BuildArgs(cfg Config) []string {
	var args []string

	host := cfg.Host
	if host == "" {
		host = "0.0.0.0"
	}
	args = append(args, "--host", host)

	port := cfg.Port
	if port <= 0 {
		port = 8000
	}
	args = append(args, "--port", fmt.Sprintf("%d", port))

	policy := cfg.Policy
	if policy == "" {
		policy = PolicyConsistentHash
	}
	args = append(args, "--policy", string(policy))

	if cfg.DataParallelSize > 1 {
		args = append(args, "--intra-node-data-parallel-size", fmt.Sprintf("%d", cfg.DataParallelSize))
	}

	if cfg.Backend != "" && cfg.Backend != "vllm" {
		args = append(args, "--backend", cfg.Backend)
	}

	if cfg.LogLevel != "" {
		args = append(args, "--log-level", cfg.LogLevel)
	}

	if cfg.LogDir != "" {
		args = append(args, "--log-dir", cfg.LogDir)
	}

	if cfg.VllmPDDisagg {
		args = append(args, "--vllm-pd-disaggregation")
		if cfg.VllmDiscoveryAddress != "" {
			args = append(args, "--vllm-discovery-address", cfg.VllmDiscoveryAddress)
		}
		if cfg.PrefillPolicy != "" {
			args = append(args, "--prefill-policy", cfg.PrefillPolicy)
		}
		if cfg.DecodePolicy != "" {
			args = append(args, "--decode-policy", cfg.DecodePolicy)
		}
		for _, u := range cfg.PrefillURLs {
			args = append(args, "--prefill", u)
		}
		for _, u := range cfg.DecodeURLs {
			args = append(args, "--decode", u)
		}
	} else if len(cfg.WorkerURLs) > 0 {
		args = append(args, "--worker-urls")
		args = append(args, cfg.WorkerURLs...)
	}

	// Worker startup flags
	if cfg.WorkerStartupTimeoutSecs > 0 {
		args = append(args, "--worker-startup-timeout-secs", strconv.Itoa(cfg.WorkerStartupTimeoutSecs))
	}
	if cfg.WorkerStartupCheckInterval > 0 {
		args = append(args, "--worker-startup-check-interval", strconv.Itoa(cfg.WorkerStartupCheckInterval))
	}

	// Load balancing threshold & cache tuning flags for official vllm-router
	if cfg.BalanceAbsThreshold > 0 {
		args = append(args, "--balance-abs-threshold", strconv.Itoa(cfg.BalanceAbsThreshold))
	}
	if cfg.BalanceRelThreshold > 0 {
		args = append(args, "--balance-rel-threshold", strconv.FormatFloat(cfg.BalanceRelThreshold, 'f', -1, 64))
	}
	if cfg.CacheThreshold > 0 {
		args = append(args, "--cache-threshold", strconv.FormatFloat(cfg.CacheThreshold, 'f', -1, 64))
	}
	if cfg.EvictionInterval > 0 {
		args = append(args, "--eviction-interval", strconv.Itoa(cfg.EvictionInterval))
	}
	if cfg.MaxTreeSize > 0 {
		args = append(args, "--max-tree-size", strconv.Itoa(cfg.MaxTreeSize))
	}
	if cfg.MaxPayloadSize > 0 {
		args = append(args, "--max-payload-size", strconv.Itoa(cfg.MaxPayloadSize))
	}

	// Authentication & Security
	if cfg.APIKey != "" {
		args = append(args, "--api-key", cfg.APIKey)
	}
	if len(cfg.APIKeyValidationURLs) > 0 {
		args = append(args, "--api-key-validation-urls")
		args = append(args, cfg.APIKeyValidationURLs...)
	}

	// Circuit Breaker CLI flags
	if cfg.CbFailureThreshold > 0 {
		args = append(args, "--cb-failure-threshold", fmt.Sprintf("%d", cfg.CbFailureThreshold))
	}
	if cfg.CbSuccessThreshold > 0 {
		args = append(args, "--cb-success-threshold", fmt.Sprintf("%d", cfg.CbSuccessThreshold))
	}
	if cfg.CbTimeoutDurationSecs > 0 {
		args = append(args, "--cb-timeout-duration-secs", fmt.Sprintf("%d", cfg.CbTimeoutDurationSecs))
	}
	if cfg.CbWindowDurationSecs > 0 {
		args = append(args, "--cb-window-duration-secs", fmt.Sprintf("%d", cfg.CbWindowDurationSecs))
	}
	if cfg.DisableCircuitBreaker {
		args = append(args, "--disable-circuit-breaker")
	}

	// Retry CLI flags
	if cfg.RetryMaxRetries > 0 {
		args = append(args, "--retry-max-retries", fmt.Sprintf("%d", cfg.RetryMaxRetries))
	}
	if cfg.RetryInitialBackoffMs > 0 {
		args = append(args, "--retry-initial-backoff-ms", fmt.Sprintf("%d", cfg.RetryInitialBackoffMs))
	}
	if cfg.DisableRetries {
		args = append(args, "--disable-retries")
	}

	// Health Check CLI flags
	if cfg.HealthFailureThreshold > 0 {
		args = append(args, "--health-failure-threshold", fmt.Sprintf("%d", cfg.HealthFailureThreshold))
	}
	if cfg.HealthSuccessThreshold > 0 {
		args = append(args, "--health-success-threshold", fmt.Sprintf("%d", cfg.HealthSuccessThreshold))
	}
	if cfg.HealthCheckTimeoutSecs > 0 {
		args = append(args, "--health-check-timeout-secs", fmt.Sprintf("%d", cfg.HealthCheckTimeoutSecs))
	}
	if cfg.HealthCheckIntervalSecs > 0 {
		args = append(args, "--health-check-interval-secs", fmt.Sprintf("%d", cfg.HealthCheckIntervalSecs))
	}
	if cfg.HealthCheckEndpoint != "" {
		args = append(args, "--health-check-endpoint", cfg.HealthCheckEndpoint)
	}

	if len(cfg.ExtraArgs) > 0 {
		args = append(args, cfg.ExtraArgs...)
	}

	return args
}

// GenerateBashCommand formats the vllm-router command for bash shells.
func GenerateBashCommand(cfg Config) string {
	bin := cfg.RouterBin
	if bin == "" {
		bin = "vllm-router"
	}

	args := BuildArgs(cfg)
	var sb strings.Builder
	sb.WriteString(bin)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			sb.WriteString(" \\\n    ")
			sb.WriteString(arg)
		} else {
			sb.WriteString(" ")
			sb.WriteString(arg)
		}
	}
	return sb.String()
}

// GeneratePowerShellCommand formats the vllm-router command for PowerShell.
func GeneratePowerShellCommand(cfg Config) string {
	bin := cfg.RouterBin
	if bin == "" {
		bin = "vllm-router"
	}

	args := BuildArgs(cfg)
	var sb strings.Builder
	sb.WriteString(bin)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			sb.WriteString(" `\n    ")
			sb.WriteString(arg)
		} else {
			sb.WriteString(" ")
			sb.WriteString(arg)
		}
	}
	return sb.String()
}

// GenerateDockerCommand formats a docker run command for vllm-router.
func GenerateDockerCommand(cfg Config, imageName string) string {
	if imageName == "" {
		imageName = "vllm/vllm-router:latest"
	}
	port := cfg.Port
	if port <= 0 {
		port = 8000
	}

	args := BuildArgs(cfg)
	return fmt.Sprintf("docker run --rm -it --network host -p %d:%d %s %s",
		port, port, imageName, strings.Join(args, " "))
}
