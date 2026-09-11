package gpustack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Config configures the GPUStack API client.
type Config struct {
	BaseURL  string        `json:"base_url"`
	APIKey   string        `json:"api_key"`
	Username string        `json:"username"`
	Password string        `json:"password"`
	Timeout  time.Duration `json:"timeout"`
}

// Client is a client for the GPUStack API.
type Client struct {
	cfg        Config
	httpClient *http.Client
	mu         sync.Mutex
	isLoggedIn bool
}

// NewClient creates a new GPUStack API client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "http://127.0.0.1:8200"
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")

	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}

	httpClient := &http.Client{
		Timeout: cfg.Timeout,
		Jar:     jar,
	}

	return &Client{
		cfg:        cfg,
		httpClient: httpClient,
	}, nil
}

// Login logs in to GPUStack using username and password.
func (c *Client) Login(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cfg.Username == "" {
		return errors.New("username is required for login")
	}

	loginURL := fmt.Sprintf("%s/auth/login", c.cfg.BaseURL)
	formData := url.Values{
		"username": {c.cfg.Username},
		"password": {c.cfg.Password},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("login failed with HTTP %d: %s", resp.StatusCode, string(body))
	}

	c.isLoggedIn = true
	return nil
}

// doRequest performs an HTTP request with auth headers and handles re-login if needed.
func (c *Client) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	reqURL := fmt.Sprintf("%s%s", c.cfg.BaseURL, path)

	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, err
	}

	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
		req.Header.Set("X-API-Key", c.cfg.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	// If unauthorized and username/password available, try logging in once and retry
	if resp.StatusCode == http.StatusUnauthorized && c.cfg.Username != "" && c.cfg.Password != "" {
		resp.Body.Close()
		if err := c.Login(ctx); err != nil {
			return nil, fmt.Errorf("auto-login failed: %w", err)
		}

		// Recreate request with new session cookie
		req2, err := http.NewRequestWithContext(ctx, method, reqURL, body)
		if err != nil {
			return nil, err
		}
		if c.cfg.APIKey != "" {
			req2.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
			req2.Header.Set("X-API-Key", c.cfg.APIKey)
		}
		return c.httpClient.Do(req2)
	}

	return resp, nil
}

// GetModels lists models from GPUStack.
func (c *Client) GetModels(ctx context.Context) ([]ModelPublic, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/v2/models?perPage=100", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetch models failed with HTTP %d: %s", resp.StatusCode, string(body))
	}

	var list PaginatedList[ModelPublic]
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("failed to decode models response: %w", err)
	}

	return list.Items, nil
}

// FindModelByName looks up a model by its name.
func (c *Client) FindModelByName(ctx context.Context, modelName string) (*ModelPublic, error) {
	models, err := c.GetModels(ctx)
	if err != nil {
		return nil, err
	}

	// 1. Try exact match
	for _, m := range models {
		if m.Name == modelName {
			return &m, nil
		}
	}

	// 2. Try case-insensitive match
	for _, m := range models {
		if strings.EqualFold(m.Name, modelName) {
			return &m, nil
		}
	}

	// 3. Try partial substring match
	for _, m := range models {
		if strings.Contains(strings.ToLower(m.Name), strings.ToLower(modelName)) {
			return &m, nil
		}
	}

	return nil, fmt.Errorf("model %q not found in GPUStack (found %d models)", modelName, len(models))
}

// GetModelInstances retrieves instances for a given model ID.
func (c *Client) GetModelInstances(ctx context.Context, modelID int) ([]ModelInstancePublic, error) {
	path := fmt.Sprintf("/v2/models/%d/instances?perPage=100", modelID)
	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch model %d instances: %w", modelID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetch instances failed with HTTP %d: %s", resp.StatusCode, string(body))
	}

	var list PaginatedList[ModelInstancePublic]
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("failed to decode instances response: %w", err)
	}

	return list.Items, nil
}

// GetRunningWorkerEndpoints discovers running worker endpoints for the specified model name.
func (c *Client) GetRunningWorkerEndpoints(ctx context.Context, modelName string) ([]WorkerEndpoint, *ModelPublic, error) {
	model, err := c.FindModelByName(ctx, modelName)
	if err != nil {
		return nil, nil, err
	}

	instances, err := c.GetModelInstances(ctx, model.ID)
	if err != nil {
		return nil, model, err
	}

	var endpoints []WorkerEndpoint
	for _, inst := range instances {
		// Only select running instances
		if !strings.EqualFold(inst.State, "running") {
			continue
		}

		// Resolve IP
		ip := inst.WorkerAdvertiseAddress
		if ip == "" {
			ip = inst.WorkerIP
		}
		if ip == "" {
			continue
		}

		// Resolve Port
		port := 0
		if inst.Port != nil && *inst.Port > 0 {
			port = *inst.Port
		} else if len(inst.Ports) > 0 {
			port = inst.Ports[0]
		}
		if port <= 0 {
			continue
		}

		urlStr := fmt.Sprintf("http://%s:%d", ip, port)
		if strings.HasPrefix(ip, "http://") || strings.HasPrefix(ip, "https://") {
			urlStr = fmt.Sprintf("%s:%d", ip, port)
		}

		endpoints = append(endpoints, WorkerEndpoint{
			InstanceID:   inst.ID,
			InstanceName: inst.Name,
			ModelID:      model.ID,
			ModelName:    model.Name,
			WorkerName:   inst.WorkerName,
			IP:           ip,
			Port:         port,
			URL:          urlStr,
			State:        inst.State,
			Backend:      inst.Backend,
		})
	}

	return endpoints, model, nil
}

// GetAllModelInstances retrieves all instances across all models in GPUStack.
func (c *Client) GetAllModelInstances(ctx context.Context) ([]ModelInstancePublic, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/v2/model-instances?perPage=100", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch all model instances: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetch all model instances failed with HTTP %d: %s", resp.StatusCode, string(body))
	}

	var list PaginatedList[ModelInstancePublic]
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("failed to decode instances response: %w", err)
	}

	return list.Items, nil
}

// GetAllRunningWorkerEndpoints discovers running worker endpoints for ALL models in GPUStack.
func (c *Client) GetAllRunningWorkerEndpoints(ctx context.Context) (*ClusterEndpoints, error) {
	// 1. Fetch all models for metadata
	models, err := c.GetModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch models list: %w", err)
	}

	modelMap := make(map[string]ModelPublic)
	modelIDMap := make(map[int]ModelPublic)
	for _, m := range models {
		modelMap[m.Name] = m
		modelIDMap[m.ID] = m
	}

	// 2. Fetch all instances across the cluster
	instances, err := c.GetAllModelInstances(ctx)
	if err != nil {
		return nil, err
	}

	result := &ClusterEndpoints{
		ModelsEndpoints: make(map[string][]WorkerEndpoint),
		Models:          modelMap,
	}

	for _, inst := range instances {
		if !strings.EqualFold(inst.State, "running") {
			continue
		}

		mName := inst.ModelName
		if mName == "" {
			if m, ok := modelIDMap[inst.ModelID]; ok {
				mName = m.Name
			}
		}
		if mName == "" {
			continue
		}

		// Resolve IP
		ip := inst.WorkerAdvertiseAddress
		if ip == "" {
			ip = inst.WorkerIP
		}
		if ip == "" {
			continue
		}

		// Resolve Port
		port := 0
		if inst.Port != nil && *inst.Port > 0 {
			port = *inst.Port
		} else if len(inst.Ports) > 0 {
			port = inst.Ports[0]
		}
		if port <= 0 {
			continue
		}

		urlStr := fmt.Sprintf("http://%s:%d", ip, port)
		if strings.HasPrefix(ip, "http://") || strings.HasPrefix(ip, "https://") {
			urlStr = fmt.Sprintf("%s:%d", ip, port)
		}

		ep := WorkerEndpoint{
			InstanceID:   inst.ID,
			InstanceName: inst.Name,
			ModelID:      inst.ModelID,
			ModelName:    mName,
			WorkerName:   inst.WorkerName,
			IP:           ip,
			Port:         port,
			URL:          urlStr,
			State:        inst.State,
			Backend:      inst.Backend,
		}

		result.ModelsEndpoints[mName] = append(result.ModelsEndpoints[mName], ep)
		result.AllEndpoints = append(result.AllEndpoints, ep)
	}

	result.ModelCount = len(result.ModelsEndpoints)
	result.InstanceCount = len(result.AllEndpoints)

	return result, nil
}

// GetInstanceLogs retrieves recent container logs (stdout/stderr) for a specific model instance.
func (c *Client) GetInstanceLogs(ctx context.Context, instanceID int, tailLines int) (string, error) {
	if tailLines <= 0 {
		tailLines = 1000
	}
	path := fmt.Sprintf("/v2/model-instances/%d/logs?tail=%d", instanceID, tailLines)
	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", fmt.Errorf("failed to fetch logs for instance %d: %w", instanceID, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read logs response for instance %d: %w", instanceID, err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch instance %d logs failed with HTTP %d: %s", instanceID, resp.StatusCode, string(body))
	}

	return string(body), nil
}

// GetInstance fetches the latest details and state of a model instance from GPUStack.
func (c *Client) GetInstance(ctx context.Context, instanceID int) (*ModelInstancePublic, error) {
	path := fmt.Sprintf("/v2/model-instances/%d", instanceID)
	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance %d: %w", instanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get instance %d failed with HTTP %d: %s", instanceID, resp.StatusCode, string(body))
	}

	var inst ModelInstancePublic
	if err := json.NewDecoder(resp.Body).Decode(&inst); err != nil {
		return nil, fmt.Errorf("failed to decode instance %d: %w", instanceID, err)
	}
	return &inst, nil
}

// DeleteInstance deletes a model instance in GPUStack.
// In GPUStack's controller architecture, deleting an instance causes the scheduler
// to immediately provision and boot a fresh replacement instance to satisfy desired replicas.
func (c *Client) DeleteInstance(ctx context.Context, instanceID int) error {
	path := fmt.Sprintf("/v2/model-instances/%d", instanceID)
	resp, err := c.doRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return fmt.Errorf("failed to delete instance %d: %w", instanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete instance %d failed with HTTP %d: %s", instanceID, resp.StatusCode, string(body))
	}
	return nil
}

// RestartInstance restarts an instance by deleting it via GPUStack API,
// prompting the GPUStack controller to spawn a clean replacement instance.
func (c *Client) RestartInstance(ctx context.Context, instanceID int) error {
	return c.DeleteInstance(ctx, instanceID)
}


