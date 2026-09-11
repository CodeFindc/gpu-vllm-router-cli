package dashboard

import "context"

// TopologyData represents the complete system topology, model routing pools, and worker health.
type TopologyData struct {
	Mode             string          `json:"mode"`               // "proxy" or "run"
	Policy           string          `json:"policy"`             // "consistent_hash", "round_robin", etc.
	PublicAddr       string          `json:"public_addr"`        // e.g. "0.0.0.0:8000"
	ClusterHealth    string          `json:"cluster_health"`     // "healthy", "degraded", "unhealthy"
	TotalModels      int             `json:"total_models"`       // Total active models
	TotalWorkers     int             `json:"total_workers"`      // Total worker instances
	HealthyWorkers   int             `json:"healthy_workers"`    // Healthy worker instances
	TotalActiveConns int64           `json:"total_active_conns"` // In-flight active connections across cluster
	Models           []ModelTopology `json:"models"`             // Per-model routing topology
	ServerTimeUTC    string          `json:"server_time_utc"`    // RFC3339 timestamp
}

// ModelTopology represents the load and worker topology for a specific model pool.
type ModelTopology struct {
	ModelName    string           `json:"model_name"`
	Mode         string           `json:"mode"` // "proxy" or "run"
	Policy       string           `json:"policy"`
	WorkerCount  int              `json:"worker_count"`
	HealthyCount int              `json:"healthy_count"`
	ActiveConns  int64            `json:"active_conns"`
	Workers      []WorkerTopology `json:"workers"`
}

// WorkerTopology represents the real-time load, health, and circuit breaker status of a backend worker.
type WorkerTopology struct {
	URL                 string `json:"url"`
	ActiveConns         int64  `json:"active_conns"`
	Healthy             bool   `json:"healthy"`
	CircuitState        string `json:"circuit_state"` // "CLOSED", "OPEN", "HALF_OPEN"
	ConsecutiveFailures int    `json:"consecutive_failures"`
	LastErr             string `json:"last_error,omitempty"`
	LastProbe           string `json:"last_probe,omitempty"`
}

// ProbeRequest represents the payload for triggering a manual worker probe.
type ProbeRequest struct {
	URL string `json:"url"`
}

// ProbeResponse represents the result of a manual worker probe.
type ProbeResponse struct {
	URL     string `json:"url"`
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// ResetBreakerRequest represents the payload for resetting a circuit breaker.
type ResetBreakerRequest struct {
	URL string `json:"url"`
}

// ResetBreakerResponse represents the result of resetting a circuit breaker.
type ResetBreakerResponse struct {
	URL     string `json:"url"`
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// ModelRuleDTO represents a model rule transferred over REST API.
type ModelRuleDTO struct {
	ModelName           string   `json:"model_name"`
	Mode                string   `json:"mode,omitempty"`   // "proxy", "run" or "" (inherit)
	Policy              string   `json:"policy,omitempty"` // "consistent_hash", etc. or "" (inherit)
	BalanceAbsThreshold *int     `json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold *float64 `json:"balance_rel_threshold,omitempty"`
	CacheThreshold      *float64 `json:"cache_threshold,omitempty"`
	ExtraArgs           []string `json:"extra_args,omitempty"`
}

// ConfigSnapshot represents the current active system configuration for dashboard viewing and editing.
type ConfigSnapshot struct {
	Mode                    string         `json:"mode"`                       // Global mode: "proxy" or "run"
	Policy                  string         `json:"policy"`                     // Global policy
	WatchIntervalSecs       int            `json:"watch_interval_secs"`        // e.g. 10
	ZeroDowntime            bool           `json:"zero_downtime"`              // true/false
	DrainTimeoutSecs        int            `json:"drain_timeout_secs"`         // e.g. 60
	BalanceAbsThreshold     *int           `json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold     *float64       `json:"balance_rel_threshold,omitempty"`
	CacheThreshold          *float64       `json:"cache_threshold,omitempty"`
	ExtraArgs               []string       `json:"extra_args,omitempty"`
	CircuitBreakerEnabled   bool           `json:"circuit_breaker_enabled"`    // true/false
	MaxFailures             int            `json:"max_failures"`               // e.g. 3
	CooldownSecs            int            `json:"cooldown_secs"`              // e.g. 10
	MaxRetries              int            `json:"max_retries"`                // e.g. 2
	HealthCheckIntervalSecs int            `json:"health_check_interval_secs"` // e.g. 3
	SuccessThreshold        int            `json:"success_threshold"`          // e.g. 2
	Models                  []ModelRuleDTO `json:"models"`                     // Custom per-model rules
	AvailablePolicies       []string       `json:"available_policies"`         // 6 supported policies
	AvailableModes          []string       `json:"available_modes"`            // ["proxy", "run"]
	ConfigFilePath          string         `json:"config_file_path"`           // e.g. "config.yaml"
}

// ConfigUpdateRequest contains fields to update system configuration.
type ConfigUpdateRequest struct {
	Mode                    *string        `json:"mode,omitempty"`
	Policy                  *string        `json:"policy,omitempty"`
	WatchIntervalSecs       *int           `json:"watch_interval_secs,omitempty"`
	ZeroDowntime            *bool          `json:"zero_downtime,omitempty"`
	DrainTimeoutSecs        *int           `json:"drain_timeout_secs,omitempty"`
	BalanceAbsThreshold     *int           `json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold     *float64       `json:"balance_rel_threshold,omitempty"`
	CacheThreshold          *float64       `json:"cache_threshold,omitempty"`
	ExtraArgs               []string       `json:"extra_args,omitempty"`
	CircuitBreakerEnabled   *bool          `json:"circuit_breaker_enabled,omitempty"`
	MaxFailures             *int           `json:"max_failures,omitempty"`
	CooldownSecs            *int           `json:"cooldown_secs,omitempty"`
	MaxRetries              *int           `json:"max_retries,omitempty"`
	HealthCheckIntervalSecs *int           `json:"health_check_interval_secs,omitempty"`
	SuccessThreshold        *int           `json:"success_threshold,omitempty"`
	Models                  []ModelRuleDTO `json:"models,omitempty"`
}

// ModelRuleUpdateRequest updates mode and/or policy for a specific model.
type ModelRuleUpdateRequest struct {
	ModelName           string   `json:"model_name"`
	Mode                string   `json:"mode"`   // "proxy", "run", or "" (reset to inherit)
	Policy              string   `json:"policy"` // "consistent_hash", etc. or "" (reset to inherit)
	BalanceAbsThreshold *int     `json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold *float64 `json:"balance_rel_threshold,omitempty"`
	CacheThreshold      *float64 `json:"cache_threshold,omitempty"`
	ExtraArgs           []string `json:"extra_args,omitempty"`
}

// ConfigUpdateResponse represents the response after saving configuration.
type ConfigUpdateResponse struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Config  *ConfigSnapshot `json:"config,omitempty"`
}

// TopologyProvider is implemented by both Proxy Server (mode: proxy) and Supervisor (mode: run).
type TopologyProvider interface {
	GetTopology(ctx context.Context) (*TopologyData, error)
	ProbeWorker(ctx context.Context, workerURL string) (bool, error)
	ResetBreaker(ctx context.Context, workerURL string) error
}

// ConfigManager is an optional interface for providers that support live configuration management.
type ConfigManager interface {
	GetConfig(ctx context.Context) (*ConfigSnapshot, error)
	UpdateConfig(ctx context.Context, req ConfigUpdateRequest) (*ConfigSnapshot, error)
	UpdateModelRule(ctx context.Context, req ModelRuleUpdateRequest) (*ConfigSnapshot, error)
}
