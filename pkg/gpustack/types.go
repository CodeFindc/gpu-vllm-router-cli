package gpustack

import "time"

// ModelPublic represents a model in GPUStack.
type ModelPublic struct {
	ID                int       `json:"id"`
	Name              string    `json:"name"`
	Description       string    `json:"description,omitempty"`
	Replicas          int       `json:"replicas"`
	ReadyReplicas     int       `json:"ready_replicas"`
	Backend           string    `json:"backend,omitempty"`
	BackendVersion    string    `json:"backend_version,omitempty"`
	LocalPath         string    `json:"local_path,omitempty"`
	HuggingFaceRepoID string    `json:"huggingface_repo_id,omitempty"`
	ModelScopeModelID string    `json:"model_scope_model_id,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// ModelInstancePublic represents a running instance of a model.
type ModelInstancePublic struct {
	ID                     int       `json:"id"`
	Name                   string    `json:"name"`
	ModelID                int       `json:"model_id"`
	ModelName              string    `json:"model_name"`
	WorkerID               *int      `json:"worker_id,omitempty"`
	WorkerName             string    `json:"worker_name,omitempty"`
	WorkerIP               string    `json:"worker_ip,omitempty"`
	WorkerAdvertiseAddress string    `json:"worker_advertise_address,omitempty"`
	WorkerIfname           string    `json:"worker_ifname,omitempty"`
	PID                    *int      `json:"pid,omitempty"`
	Port                   *int      `json:"port,omitempty"`
	Ports                  []int     `json:"ports,omitempty"`
	DownloadProgress       float64   `json:"download_progress,omitempty"`
	State                  string    `json:"state"` // "running", "starting", "downloading", "error", etc.
	StateMessage           string    `json:"state_message,omitempty"`
	Backend                string    `json:"backend,omitempty"`
	GPUType                string    `json:"gpu_type,omitempty"`
	GPUIndexes             []int     `json:"gpu_indexes,omitempty"`
	InjectedBackendParams  []string  `json:"injected_backend_parameters,omitempty"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

// Pagination metadata in GPUStack response.
type Pagination struct {
	Page      int `json:"page"`
	PerPage   int `json:"perPage"`
	Total     int `json:"total"`
	TotalPage int `json:"totalPage"`
}

// PaginatedList represents a generic paginated response.
type PaginatedList[T any] struct {
	Items      []T        `json:"items"`
	Pagination Pagination `json:"pagination"`
}

// WorkerEndpoint contains the resolved URL and metadata of an instance endpoint.
type WorkerEndpoint struct {
	InstanceID   int    `json:"instance_id"`
	InstanceName string `json:"instance_name"`
	ModelID      int    `json:"model_id"`
	ModelName    string `json:"model_name"`
	WorkerName   string `json:"worker_name"`
	IP           string `json:"ip"`
	Port         int    `json:"port"`
	URL          string `json:"url"` // e.g. "http://192.168.1.100:8000"
	State        string `json:"state"`
	Backend      string `json:"backend"`
}

// ClusterEndpoints holds all active worker endpoints across all models in GPUStack.
type ClusterEndpoints struct {
	ModelsEndpoints map[string][]WorkerEndpoint `json:"models_endpoints"` // key: model name
	AllEndpoints    []WorkerEndpoint            `json:"all_endpoints"`
	Models          map[string]ModelPublic      `json:"models"`           // key: model name
	ModelCount      int                         `json:"model_count"`
	InstanceCount   int                         `json:"instance_count"`
}

