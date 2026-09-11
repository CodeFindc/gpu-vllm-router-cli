package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gpu-vllm-router/pkg/config"
	"gpu-vllm-router/pkg/dashboard"
	"gpu-vllm-router/pkg/gpustack"
	"gpu-vllm-router/pkg/router"
	"gpu-vllm-router/pkg/swagger"
)

// ServerConfig configures the native Go reverse proxy server.
type ServerConfig struct {
	Host                string
	Port                int
	Policy              router.Policy
	ModelName           string // If empty, operates in full-cluster multi-model mode
	WatchInterval       time.Duration
	CircuitBreaker      CircuitBreakerConfig
	ConfigFilePath      string
	ModelRules          []config.ModelRule
	ZeroDowntime        bool
	DrainTimeout        time.Duration
	BalanceAbsThreshold *int
	BalanceRelThreshold *float64
	CacheThreshold      *float64
	ExtraArgs           []string
}

// ModelPool manages load balancing and active targets for a specific model.
type ModelPool struct {
	ModelName           string
	Mode                string        // "proxy" (default) or "run"
	Policy              router.Policy // per-model policy
	Balancer            Balancer
	Targets             []*BackendTarget
	RunnerCmd           *exec.Cmd
	RunnerTarget        *url.URL
	RunnerPort          int
	BalanceAbsThreshold *int
	BalanceRelThreshold *float64
	CacheThreshold      *float64
	ExtraArgs           []string
}

// routeState maintains in-flight routing state across retries and failovers.
type routeState struct {
	target   *BackendTarget
	balancer Balancer
	pool     *ModelPool
}

// retryTransport wraps http.RoundTripper with transparent failover and circuit breaker tracking.
type retryTransport struct {
	server *Server
	base   http.RoundTripper
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	state, _ := ctx.Value("route_state").(*routeState)
	if state == nil || state.target == nil || state.pool == nil || state.balancer == nil {
		return t.base.RoundTrip(req)
	}

	maxRetries := t.server.cfg.CircuitBreaker.MaxRetries
	excluded := make(map[string]bool)

	var lastResp *http.Response
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Reset body reader for this attempt if GetBody is available
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err == nil {
				req.Body = body
			}
		}

		resp, err := t.base.RoundTrip(req)
		if err == nil && resp.StatusCode < 500 {
			if state.target.CircuitBreaker != nil {
				state.target.CircuitBreaker.RecordSuccess()
			}
			resp.Header.Set("X-Routed-Target", state.target.URLString)
			return resp, nil
		}

		// If client canceled or timed out, do not penalize the backend worker or retry
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return resp, err
		}

		lastResp = resp
		lastErr = err
		if state.target.CircuitBreaker != nil {
			state.target.CircuitBreaker.RecordFailure(err)
		}

		if attempt >= maxRetries {
			break
		}

		// Exclude current failing target
		excluded[state.target.URLString] = true

		// Try to select alternative healthy target for this model
		nextTarget, selectErr := state.pool.Balancer.SelectTargetExcluding(req, excluded)
		if selectErr != nil {
			log.Printf("[Proxy:Failover] No alternative healthy backends available for model %s: %v", state.pool.ModelName, selectErr)
			break
		}

		// Update active connections
		state.balancer.RecordRequestEnd(state.target)
		state.balancer.RecordRequestStart(nextTarget)

		log.Printf("[Proxy:Failover] ⚡ Target %s failed (%v). Retrying on alternative target %s (attempt %d/%d)...",
			state.target.URLString, err, nextTarget.URLString, attempt+1, maxRetries)

		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		state.target = nextTarget
		req.URL.Scheme = state.target.URL.Scheme
		req.URL.Host = state.target.URL.Host
		req.Host = state.target.URL.Host
	}

	return lastResp, lastErr
}

// Server is the built-in HTTP reverse proxy server with multi-model dynamic load balancing.
type Server struct {
	cfg            ServerConfig
	client         *gpustack.Client
	mu             sync.RWMutex
	modelPools     map[string]*ModelPool  // Key: model name
	modelsList     []gpustack.ModelPublic // All models metadata
	allTargets     []*BackendTarget
	activeURLs     []string
	configFilePath string
	modelRules     map[string]config.ModelRule
	httpServer     *http.Server
	reverseProxy   *httputil.ReverseProxy
	stopCh         chan struct{}
}

// NewServer creates a new reverse proxy Server.
func NewServer(cfg ServerConfig, client *gpustack.Client) *Server {
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port <= 0 {
		cfg.Port = 8000
	}
	if cfg.WatchInterval <= 0 {
		cfg.WatchInterval = 10 * time.Second
	}
	if cfg.CircuitBreaker.MaxFailures <= 0 {
		cfg.CircuitBreaker = DefaultCircuitBreakerConfig()
	}

	rulesMap := make(map[string]config.ModelRule)
	for _, r := range cfg.ModelRules {
		rulesMap[r.ModelName] = r
	}

	s := &Server{
		cfg:            cfg,
		client:         client,
		modelPools:     make(map[string]*ModelPool),
		stopCh:         make(chan struct{}),
		configFilePath: cfg.ConfigFilePath,
		modelRules:     rulesMap,
	}

	// Custom ReverseProxy with RetryTransport
	proxy := &httputil.ReverseProxy{
		Director:       s.director,
		ModifyResponse: s.modifyResponse,
		ErrorHandler:   s.errorHandler,
		FlushInterval:  10 * time.Millisecond, // Instant flush for LLM streaming SSE
		Transport:      &retryTransport{server: s, base: http.DefaultTransport},
	}
	s.reverseProxy = proxy

	return s
}

// extractModelFromRequest inspects query param and JSON body to determine the requested model.
func extractModelFromRequest(req *http.Request) (string, []byte) {
	// 1. Check query parameter e.g. ?model=xxx
	if m := req.URL.Query().Get("model"); m != "" {
		return m, nil
	}

	// 2. Peek into JSON body for POST/PUT requests
	if req.Body != nil && (req.Method == http.MethodPost || req.Method == http.MethodPut) {
		bodyBytes, err := io.ReadAll(req.Body)
		if err == nil {
			// Restore request body and set GetBody for transparent retries
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			req.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bodyBytes)), nil
			}

			var peek struct {
				Model string `json:"model"`
			}
			if err := json.Unmarshal(bodyBytes, &peek); err == nil && peek.Model != "" {
				return peek.Model, bodyBytes
			}
			return "", bodyBytes
		}
	}
	return "", nil
}

func (s *Server) findModelPool(modelName string) (*ModelPool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.modelPools) == 0 {
		return nil, errors.New("no running model instances available in GPUStack cluster")
	}

	// If no model was specified:
	if modelName == "" {
		// If single-model mode was configured, use it
		if s.cfg.ModelName != "" {
			if pool, ok := s.modelPools[s.cfg.ModelName]; ok {
				return pool, nil
			}
		}
		// If only 1 model pool exists in cluster, route to it automatically
		if len(s.modelPools) == 1 {
			for _, pool := range s.modelPools {
				return pool, nil
			}
		}
		var available []string
		for k := range s.modelPools {
			available = append(available, k)
		}
		return nil, fmt.Errorf("request did not specify 'model'. Available models in cluster: %v", available)
	}

	// 1. Exact match
	if pool, ok := s.modelPools[modelName]; ok {
		return pool, nil
	}

	// 2. Case-insensitive match
	for k, pool := range s.modelPools {
		if strings.EqualFold(k, modelName) {
			return pool, nil
		}
	}

	// 3. Substring / Prefix match
	for k, pool := range s.modelPools {
		if strings.Contains(strings.ToLower(k), strings.ToLower(modelName)) ||
			strings.Contains(strings.ToLower(modelName), strings.ToLower(k)) {
			return pool, nil
		}
	}

	var available []string
	for k := range s.modelPools {
		available = append(available, k)
	}
	return nil, fmt.Errorf("model %q does not exist or has no healthy instances in cluster. Available: %v", modelName, available)
}

func (s *Server) director(req *http.Request) {
	modelName, _ := extractModelFromRequest(req)
	pool, err := s.findModelPool(modelName)
	if err != nil {
		log.Printf("[Proxy] Model routing error: %v (Request Path: %s)", err, req.URL.Path)
		ctx := context.WithValue(req.Context(), "route_error", err)
		*req = *req.WithContext(ctx)
		return
	}

	if pool.Mode == "run" && pool.RunnerTarget != nil {
		req.URL.Scheme = pool.RunnerTarget.Scheme
		req.URL.Host = pool.RunnerTarget.Host
		req.Host = pool.RunnerTarget.Host
		if _, ok := req.Header["User-Agent"]; !ok {
			req.Header.Set("User-Agent", "gpu-vllm-router-hybrid/1.0")
		}
		ctx := context.WithValue(req.Context(), "routed_model", pool.ModelName)
		*req = *req.WithContext(ctx)
		return
	}

	target, err := pool.Balancer.SelectTarget(req)
	if err != nil {
		log.Printf("[Proxy] Balancer error for model %s: %v", pool.ModelName, err)
		ctx := context.WithValue(req.Context(), "route_error", err)
		*req = *req.WithContext(ctx)
		return
	}

	// Record start of request for active connection tracking
	pool.Balancer.RecordRequestStart(target)

	// Save routeState in context for retryTransport, modifyResponse, and errorHandler
	state := &routeState{
		target:   target,
		balancer: pool.Balancer,
		pool:     pool,
	}
	ctx := context.WithValue(req.Context(), "route_state", state)
	ctx = context.WithValue(ctx, "routed_model", pool.ModelName)
	*req = *req.WithContext(ctx)

	// Rewrite URL
	req.URL.Scheme = target.URL.Scheme
	req.URL.Host = target.URL.Host
	req.Host = target.URL.Host
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", "gpu-vllm-router/1.0")
	}
}

func (s *Server) modifyResponse(resp *http.Response) error {
	ctx := resp.Request.Context()
	state, _ := ctx.Value("route_state").(*routeState)
	if state != nil && state.target != nil && state.balancer != nil {
		state.balancer.RecordRequestEnd(state.target)
	}

	// Add router indicators
	resp.Header.Set("X-Router-Policy", string(s.cfg.Policy))
	if m, ok := ctx.Value("routed_model").(string); ok && m != "" {
		resp.Header.Set("X-Routed-Model", m)
	}
	return nil
}

func (s *Server) errorHandler(w http.ResponseWriter, req *http.Request, err error) {
	ctx := req.Context()
	state, _ := ctx.Value("route_state").(*routeState)
	if state != nil && state.target != nil && state.balancer != nil {
		state.balancer.RecordRequestEnd(state.target)
	}

	// If route_error occurred (model not found)
	if routeErr, ok := ctx.Value("route_error").(error); ok && routeErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"message": routeErr.Error(),
				"type":    "invalid_request_error",
				"param":   "model",
				"code":    "model_not_found",
			},
		})
		return
	}

	// If client canceled/aborted or timed out while waiting in queue, do not write 502
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		log.Printf("[Proxy] Client canceled/disconnected request for %s (Queue timeout or User abort)", req.URL.String())
		return
	}

	log.Printf("[Proxy] Forwarding error to %s: %v", req.URL.String(), err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"message": fmt.Sprintf("GPU router backend error: %v", err),
			"type":    "router_bad_gateway",
			"code":    502,
		},
	})
}

// Start boots the proxy server and watches GPUStack instances.
func (s *Server) Start(ctx context.Context) error {
	// Initial endpoint discovery
	if s.client != nil {
		if s.cfg.ModelName != "" {
			// Single Model Mode
			endpoints, model, err := s.client.GetRunningWorkerEndpoints(ctx, s.cfg.ModelName)
			if err != nil {
				log.Printf("[Proxy] ⚠️ Initial discovery warning for model %q: %v. Entering STANDBY mode...", s.cfg.ModelName, err)
			} else if len(endpoints) == 0 {
				log.Printf("[Proxy] ⚠️ Warning: no running instances found for model %q in GPUStack at startup. Entering STANDBY mode...", s.cfg.ModelName)
			} else {
				log.Printf("[Proxy] Discovered %d running instance(s) for model %s (ID: %d):", len(endpoints), model.Name, model.ID)
				s.updateSingleModelEndpoints(s.cfg.ModelName, endpoints)
			}
		} else {
			// Full Cluster Multi-Model Mode
			cluster, err := s.client.GetAllRunningWorkerEndpoints(ctx)
			if err != nil {
				log.Printf("[Proxy] ⚠️ Cluster-wide model discovery warning: %v. Entering STANDBY mode...", err)
			} else if cluster == nil || cluster.InstanceCount == 0 {
				log.Printf("[Proxy] ⚠️ Warning: no running model instances found across entire GPUStack cluster at startup. Entering STANDBY mode...")
			} else {
				log.Printf("[Proxy] [Multi-Model] Discovered %d active model(s) and %d running instance(s) in cluster:",
					cluster.ModelCount, cluster.InstanceCount)
				s.updateClusterEndpoints(cluster)
			}
		}
	} else {
		log.Printf("[Proxy] Standby mode: running without active GPUStack client.")
	}

	// Setup HTTP handler multiplexer
	mux := http.NewServeMux()

	// Health & liveness probes (K8s & unified convention)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/livez", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleHealth)
	mux.HandleFunc("/ping", s.handleHealth)

	// Admin stats & supervisor status (Unified dual-mode support)
	mux.HandleFunc("/admin/stats", s.handleStats)
	mux.HandleFunc("/admin/supervisor", s.handleStats)

	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/swagger/", swagger.Handler)
	mux.HandleFunc("/swagger/doc.json", swagger.DocJSONHandler)
	mux.HandleFunc("/openapi.json", swagger.DocJSONHandler)
	mux.HandleFunc("/docs", swagger.DocsRedirectHandler)
	mux.HandleFunc("/redoc", swagger.RedocHandler)
	dashHandler := dashboard.NewHandler(s)
	mux.Handle("/dashboard/", dashHandler)
	mux.Handle("/dashboard", dashHandler)
	mux.Handle("/ui/", dashHandler)
	mux.Handle("/ui", dashHandler)
	mux.Handle("/api/", dashHandler)
	mux.Handle("/api/topology", dashHandler)
	mux.Handle("/api/probe", dashHandler)
	mux.Handle("/api/reset-breaker", dashHandler)
	mux.Handle("/api/config", dashHandler)
	mux.Handle("/api/models/rule", dashHandler)
	mux.Handle("/api/health", dashHandler)

	// Root handler returning service JSON status or forwarding to proxy
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet && r.URL.Query().Get("model") == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"service": "gpu-vllm-router-cli",
				"status":  "running",
				"mode":    "proxy",
				"docs":    "/docs",
				"openapi": "/openapi.json",
				"health":  "/health",
				"models":  "/v1/models",
			})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			dashHandler.ServeHTTP(w, r)
			return
		}
		s.reverseProxy.ServeHTTP(w, r)
	})
	mux.Handle("/", rootHandler)

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start background watch loop
	go s.watchLoop(ctx)

	// Start background circuit breaker active health probe loop
	go s.probeLoop(ctx)

	log.Printf("[Proxy] Native Multi-Model Load Balancer running on http://%s (Policy: %s, MaxRetries: %d)",
		addr, s.cfg.Policy, s.cfg.CircuitBreaker.MaxRetries)
	log.Printf("[Proxy] Swagger UI documentation: http://%s/docs (OpenAPI spec: /openapi.json)", addr)

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("proxy server failed: %w", err)
	}

	return nil
}

// Stop gracefully shuts down the proxy server.
func (s *Server) Stop(ctx context.Context) error {
	close(s.stopCh)
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) findExistingTarget(urlStr string) *BackendTarget {
	for _, t := range s.allTargets {
		if t.URLString == urlStr {
			return t
		}
	}
	return nil
}

func (s *Server) updateSingleModelEndpoints(modelName string, endpoints []gpustack.WorkerEndpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var targets []*BackendTarget
	var urls []string

	for _, ep := range endpoints {
		u, err := url.Parse(ep.URL)
		if err != nil {
			continue
		}

		existing := s.findExistingTarget(ep.URL)
		var cb *CircuitBreaker
		if existing != nil && existing.CircuitBreaker != nil {
			cb = existing.CircuitBreaker
		} else {
			cb = NewCircuitBreaker(ep.URL, s.cfg.CircuitBreaker)
		}

		targets = append(targets, &BackendTarget{
			URL:            u,
			URLString:      ep.URL,
			Healthy:        true,
			CircuitBreaker: cb,
		})
		urls = append(urls, ep.URL)
		log.Printf("  -> Model: %-25s Backend: %s (Worker: %s)", ep.ModelName, ep.URL, ep.WorkerName)
	}

	sort.Strings(urls)
	s.allTargets = targets
	s.activeURLs = urls

	poolMode := "proxy"
	poolPolicy := s.cfg.Policy
	if rule, ok := s.modelRules[modelName]; ok {
		if rule.Mode != "" {
			poolMode = rule.Mode
		}
		if rule.Policy != "" {
			if parsedP, err := router.ParsePolicy(rule.Policy); err == nil {
				poolPolicy = parsedP
			}
		}
	}

	pool := &ModelPool{
		ModelName: modelName,
		Mode:      poolMode,
		Policy:    poolPolicy,
		Balancer:  NewBalancer(poolPolicy, targets),
		Targets:   targets,
	}
	s.modelPools[modelName] = pool
}

func (s *Server) updateClusterEndpoints(cluster *gpustack.ClusterEndpoints) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newPools := make(map[string]*ModelPool)
	var allTargets []*BackendTarget
	var allURLs []string
	var modelsList []gpustack.ModelPublic

	for mName, endpoints := range cluster.ModelsEndpoints {
		var targets []*BackendTarget
		for _, ep := range endpoints {
			u, err := url.Parse(ep.URL)
			if err != nil {
				continue
			}

			existing := s.findExistingTarget(ep.URL)
			var cb *CircuitBreaker
			if existing != nil && existing.CircuitBreaker != nil {
				cb = existing.CircuitBreaker
			} else {
				cb = NewCircuitBreaker(ep.URL, s.cfg.CircuitBreaker)
			}

			t := &BackendTarget{
				URL:            u,
				URLString:      ep.URL,
				Healthy:        true,
				CircuitBreaker: cb,
			}
			targets = append(targets, t)
			allTargets = append(allTargets, t)
			allURLs = append(allURLs, ep.URL)
			log.Printf("  -> [%s] Worker: %-12s Endpoint: %s", ep.ModelName, ep.WorkerName, ep.URL)
		}

		poolMode := "proxy"
		poolPolicy := s.cfg.Policy
		if rule, ok := s.modelRules[mName]; ok {
			if rule.Mode != "" {
				poolMode = rule.Mode
			}
			if rule.Policy != "" {
				if parsedP, err := router.ParsePolicy(rule.Policy); err == nil {
					poolPolicy = parsedP
				}
			}
		}

		pool := &ModelPool{
			ModelName: mName,
			Mode:      poolMode,
			Policy:    poolPolicy,
			Balancer:  NewBalancer(poolPolicy, targets),
			Targets:   targets,
		}

		// Preserve runner if previous pool had one
		if existingPool, ok := s.modelPools[mName]; ok && existingPool.RunnerCmd != nil {
			pool.RunnerCmd = existingPool.RunnerCmd
			pool.RunnerPort = existingPool.RunnerPort
			pool.RunnerTarget = existingPool.RunnerTarget
		}

		newPools[mName] = pool

		if mInfo, ok := cluster.Models[mName]; ok {
			modelsList = append(modelsList, mInfo)
		} else {
			modelsList = append(modelsList, gpustack.ModelPublic{Name: mName})
		}
	}

	sort.Strings(allURLs)
	s.modelPools = newPools
	s.allTargets = allTargets
	s.activeURLs = allURLs
	s.modelsList = modelsList
}

func (s *Server) probeLoop(ctx context.Context) {
	interval := s.cfg.CircuitBreaker.HealthCheckInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.RLock()
			targets := make([]*BackendTarget, len(s.allTargets))
			copy(targets, s.allTargets)
			s.mu.RUnlock()

			for _, t := range targets {
				if t.CircuitBreaker == nil {
					continue
				}
				state, _, _ := t.CircuitBreaker.GetStatus()
				if state == StateOpen || state == StateHalfOpen {
					go func(target *BackendTarget) {
						if target.CircuitBreaker.Probe() {
							log.Printf("[Proxy:Probe] 🟢 Target %s self-healing probe succeeded! Restored to CLOSED", target.URLString)
						}
					}(t)
				}
			}
		}
	}
}

func (s *Server) watchLoop(ctx context.Context) {
	if s.client == nil {
		return
	}
	ticker := time.NewTicker(s.cfg.WatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.cfg.ModelName != "" {
				// Single model refresh
				endpoints, _, err := s.client.GetRunningWorkerEndpoints(ctx, s.cfg.ModelName)
				if err != nil {
					log.Printf("[Proxy] Warning: failed to refresh instances: %v", err)
					continue
				}
				var newURLs []string
				for _, ep := range endpoints {
					newURLs = append(newURLs, ep.URL)
				}
				sort.Strings(newURLs)

				s.mu.RLock()
				currentURLs := s.activeURLs
				s.mu.RUnlock()

				if strings.Join(currentURLs, ",") != strings.Join(newURLs, ",") {
					log.Printf("[Proxy] Model %s instances changed! Updating...", s.cfg.ModelName)
					s.updateSingleModelEndpoints(s.cfg.ModelName, endpoints)
				}
			} else {
				// Cluster-wide all models refresh
				cluster, err := s.client.GetAllRunningWorkerEndpoints(ctx)
				if err != nil {
					log.Printf("[Proxy] Warning: failed to refresh cluster models: %v", err)
					continue
				}

				var newURLs []string
				for _, ep := range cluster.AllEndpoints {
					newURLs = append(newURLs, ep.URL)
				}
				sort.Strings(newURLs)

				s.mu.RLock()
				currentURLs := s.activeURLs
				s.mu.RUnlock()

				if strings.Join(currentURLs, ",") != strings.Join(newURLs, ",") {
					log.Printf("[Proxy] Cluster-wide instances or models changed! Updating routing table...")
					log.Printf("  Previous URLs: %v", currentURLs)
					log.Printf("  New URLs:      %v", newURLs)
					s.updateClusterEndpoints(cluster)
				}
			}
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	count := len(s.modelPools)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/readyz" && count == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"standby","message":"waiting for model instances to be ready"}`))
		return
	}

	w.WriteHeader(http.StatusOK)
	if count == 0 {
		_, _ = w.Write([]byte(`{"status":"ok","standby":true,"models":0}`))
		return
	}
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"ok","models":%d}`, count)))
}

// handleModels returns OpenAI-compatible /v1/models response containing all available models.
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	modelsList := s.modelsList
	pools := s.modelPools
	s.mu.RUnlock()

	type openAIModel struct {
		ID         string                   `json:"id"`
		Object     string                   `json:"object"`
		Created    int64                    `json:"created"`
		OwnedBy    string                   `json:"owned_by"`
		Permission []map[string]interface{} `json:"permission"`
	}

	var data []openAIModel
	for _, m := range modelsList {
		if _, ok := pools[m.Name]; ok {
			createdTime := m.CreatedAt.Unix()
			if createdTime <= 0 {
				createdTime = time.Now().Unix()
			}
			data = append(data, openAIModel{
				ID:         m.Name,
				Object:     "model",
				Created:    createdTime,
				OwnedBy:    "gpustack",
				Permission: []map[string]interface{}{{"id": "modelperm-default", "allow_sampling": true}},
			})
		}
	}

	// If modelsList was empty, populate from pool names
	if len(data) == 0 {
		for mName := range pools {
			data = append(data, openAIModel{
				ID:         mName,
				Object:     "model",
				Created:    time.Now().Unix(),
				OwnedBy:    "gpustack",
				Permission: []map[string]interface{}{{"id": "modelperm-default", "allow_sampling": true}},
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"object": "list",
		"data":   data,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	pools := s.modelPools
	totalTargets := len(s.allTargets)
	s.mu.RUnlock()

	type targetStat struct {
		URL                 string       `json:"url"`
		ActiveConns         int64        `json:"active_conns"`
		Healthy             bool         `json:"healthy"`
		CircuitState        CircuitState `json:"circuit_state"`
		ConsecutiveFailures int          `json:"consecutive_failures"`
	}

	type modelStat struct {
		ModelName    string       `json:"model_name"`
		BackendCount int          `json:"backend_count"`
		Backends     []targetStat `json:"backends"`
	}

	modelsStats := make(map[string]modelStat)
	for mName, pool := range pools {
		var bStats []targetStat
		for _, t := range pool.Targets {
			state := StateClosed
			fails := 0
			healthy := true
			if t.CircuitBreaker != nil {
				state, fails, _ = t.CircuitBreaker.GetStatus()
				healthy = t.CircuitBreaker.CanExecute()
			}
			bStats = append(bStats, targetStat{
				URL:                 t.URLString,
				ActiveConns:         atomic.LoadInt64(&t.ActiveConns),
				Healthy:             healthy,
				CircuitState:        state,
				ConsecutiveFailures: fails,
			})
		}
		modelsStats[mName] = modelStat{
			ModelName:    mName,
			BackendCount: len(pool.Targets),
			Backends:     bStats,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"mode":                   "multi_model_auto_sync",
		"policy":                 s.cfg.Policy,
		"model_count":            len(pools),
		"total_backends":         totalTargets,
		"models":                 modelsStats,
		"circuit_breaker_config": s.cfg.CircuitBreaker,
		"server_time_utc":        time.Now().UTC().Format(time.RFC3339),
	})
}

// handleMetrics exposes Prometheus-formatted metrics for Grafana monitoring.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	pools := s.modelPools
	totalTargets := len(s.allTargets)
	s.mu.RUnlock()

	var buf bytes.Buffer

	buf.WriteString("# HELP gpu_router_models_total Total number of registered active models\n")
	buf.WriteString("# TYPE gpu_router_models_total gauge\n")
	fmt.Fprintf(&buf, "gpu_router_models_total %d\n\n", len(pools))

	buf.WriteString("# HELP gpu_router_backends_total Total number of backends\n")
	buf.WriteString("# TYPE gpu_router_backends_total gauge\n")
	fmt.Fprintf(&buf, "gpu_router_backends_total %d\n\n", totalTargets)

	buf.WriteString("# HELP gpu_router_backend_active_connections Number of in-flight active connections per backend\n")
	buf.WriteString("# TYPE gpu_router_backend_active_connections gauge\n")

	buf.WriteString("# HELP gpu_router_backend_healthy Backend healthy state (1 = healthy, 0 = isolated)\n")
	buf.WriteString("# TYPE gpu_router_backend_healthy gauge\n")

	buf.WriteString("# HELP gpu_router_backend_consecutive_failures Consecutive failure count\n")
	buf.WriteString("# TYPE gpu_router_backend_consecutive_failures gauge\n")

	buf.WriteString("# HELP gpu_router_backend_requests_success_total Total successful requests\n")
	buf.WriteString("# TYPE gpu_router_backend_requests_success_total counter\n")

	buf.WriteString("# HELP gpu_router_backend_requests_failed_total Total failed requests\n")
	buf.WriteString("# TYPE gpu_router_backend_requests_failed_total counter\n")

	for mName, pool := range pools {
		for _, t := range pool.Targets {
			active := atomic.LoadInt64(&t.ActiveConns)
			fmt.Fprintf(&buf, "gpu_router_backend_active_connections{model=%q,target=%q} %d\n", mName, t.URLString, active)

			healthyVal := 0
			consecFails := 0
			var succ, fails int64
			if t.CircuitBreaker != nil {
				_, consecFails, succ, fails = t.CircuitBreaker.GetMetrics()
				if t.CircuitBreaker.CanExecute() {
					healthyVal = 1
				}
			}
			fmt.Fprintf(&buf, "gpu_router_backend_healthy{model=%q,target=%q} %d\n", mName, t.URLString, healthyVal)
			fmt.Fprintf(&buf, "gpu_router_backend_consecutive_failures{model=%q,target=%q} %d\n", mName, t.URLString, consecFails)
			fmt.Fprintf(&buf, "gpu_router_backend_requests_success_total{model=%q,target=%q} %d\n", mName, t.URLString, succ)
			fmt.Fprintf(&buf, "gpu_router_backend_requests_failed_total{model=%q,target=%q} %d\n", mName, t.URLString, fails)
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// GetTopology satisfies dashboard.TopologyProvider interface for Mode: Proxy.
func (s *Server) GetTopology(ctx context.Context) (*dashboard.TopologyData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalConns int64
	healthyWorkers := 0
	totalWorkers := len(s.allTargets)

	var modelsTopo []dashboard.ModelTopology

	for mName, pool := range s.modelPools {
		var workersTopo []dashboard.WorkerTopology
		var modelConns int64
		modelHealthy := 0

		for _, t := range pool.Targets {
			conns := atomic.LoadInt64(&t.ActiveConns)
			modelConns += conns
			totalConns += conns

			state := StateClosed
			fails := 0
			healthy := true
			lastProbeStr := ""
			lastErr := ""
			if t.CircuitBreaker != nil {
				state, fails, _ = t.CircuitBreaker.GetStatus()
				healthy = t.CircuitBreaker.CanExecute()
				probeTime, errStr := t.CircuitBreaker.GetLastProbeAndError()
				if !probeTime.IsZero() {
					lastProbeStr = probeTime.Format(time.RFC3339)
				}
				lastErr = errStr
			}
			if healthy {
				modelHealthy++
			}

			workersTopo = append(workersTopo, dashboard.WorkerTopology{
				URL:                 t.URLString,
				ActiveConns:         conns,
				Healthy:             healthy,
				CircuitState:        string(state),
				ConsecutiveFailures: fails,
				LastErr:             lastErr,
				LastProbe:           lastProbeStr,
			})
		}

		mode := pool.Mode
		if mode == "" {
			mode = "proxy"
		}
		policy := pool.Policy
		if policy == "" {
			policy = s.cfg.Policy
		}

		modelsTopo = append(modelsTopo, dashboard.ModelTopology{
			ModelName:    mName,
			Mode:         string(mode),
			Policy:       string(policy),
			WorkerCount:  len(pool.Targets),
			HealthyCount: modelHealthy,
			ActiveConns:  modelConns,
			Workers:      workersTopo,
		})
	}

	for _, t := range s.allTargets {
		if t.CircuitBreaker != nil && t.CircuitBreaker.CanExecute() {
			healthyWorkers++
		} else if t.CircuitBreaker == nil && t.Healthy {
			healthyWorkers++
		}
	}

	clusterHealth := "healthy"
	if healthyWorkers == 0 && totalWorkers > 0 {
		clusterHealth = "unhealthy"
	} else if healthyWorkers < totalWorkers {
		clusterHealth = "degraded"
	}

	return &dashboard.TopologyData{
		Mode:             "proxy",
		Policy:           string(s.cfg.Policy),
		PublicAddr:       fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port),
		ClusterHealth:    clusterHealth,
		TotalModels:      len(s.modelPools),
		TotalWorkers:     totalWorkers,
		HealthyWorkers:   healthyWorkers,
		TotalActiveConns: totalConns,
		Models:           modelsTopo,
		ServerTimeUTC:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// ProbeWorker triggers an active probe on a target backend worker.
func (s *Server) ProbeWorker(ctx context.Context, workerURL string) (bool, error) {
	s.mu.RLock()
	var foundTarget *BackendTarget
	trimmed := strings.TrimRight(workerURL, "/")
	for _, t := range s.allTargets {
		if strings.TrimRight(t.URLString, "/") == trimmed {
			foundTarget = t
			break
		}
	}
	s.mu.RUnlock()

	if foundTarget == nil {
		return false, fmt.Errorf("worker %s not found", workerURL)
	}

	if foundTarget.CircuitBreaker != nil {
		ok := foundTarget.CircuitBreaker.Probe()
		return ok, nil
	}
	return true, nil
}

// ResetBreaker resets the circuit breaker for a worker.
func (s *Server) ResetBreaker(ctx context.Context, workerURL string) error {
	s.mu.RLock()
	var foundTarget *BackendTarget
	trimmed := strings.TrimRight(workerURL, "/")
	for _, t := range s.allTargets {
		if strings.TrimRight(t.URLString, "/") == trimmed {
			foundTarget = t
			break
		}
	}
	s.mu.RUnlock()

	if foundTarget == nil {
		return fmt.Errorf("worker %s not found", workerURL)
	}

	if foundTarget.CircuitBreaker != nil {
		foundTarget.CircuitBreaker.Reset()
	}
	return nil
}

// GetConfig returns the active configuration snapshot.
func (s *Server) GetConfig(ctx context.Context) (*dashboard.ConfigSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getConfigLocked(), nil
}

// UpdateConfig updates runtime configuration parameters and persists to config.yaml if available.
func (s *Server) UpdateConfig(ctx context.Context, req dashboard.ConfigUpdateRequest) (*dashboard.ConfigSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Policy != nil && *req.Policy != "" {
		p, err := router.ParsePolicy(*req.Policy)
		if err != nil {
			return nil, fmt.Errorf("invalid policy: %w", err)
		}
		s.cfg.Policy = p
		for _, pool := range s.modelPools {
			if pool.Policy == "" {
				pool.Balancer = NewBalancer(p, pool.Targets)
			}
		}
	}
	if req.ZeroDowntime != nil {
		s.cfg.ZeroDowntime = *req.ZeroDowntime
	}
	if req.DrainTimeoutSecs != nil && *req.DrainTimeoutSecs > 0 {
		s.cfg.DrainTimeout = time.Duration(*req.DrainTimeoutSecs) * time.Second
	}
	if req.WatchIntervalSecs != nil && *req.WatchIntervalSecs > 0 {
		s.cfg.WatchInterval = time.Duration(*req.WatchIntervalSecs) * time.Second
	}
	if req.CircuitBreakerEnabled != nil {
		s.cfg.CircuitBreaker.Enabled = req.CircuitBreakerEnabled
	}
	if req.MaxFailures != nil && *req.MaxFailures > 0 {
		s.cfg.CircuitBreaker.MaxFailures = *req.MaxFailures
	}
	if req.CooldownSecs != nil && *req.CooldownSecs > 0 {
		s.cfg.CircuitBreaker.Cooldown = time.Duration(*req.CooldownSecs) * time.Second
	}
	if req.MaxRetries != nil && *req.MaxRetries >= 0 {
		s.cfg.CircuitBreaker.MaxRetries = *req.MaxRetries
	}
	if req.HealthCheckIntervalSecs != nil && *req.HealthCheckIntervalSecs > 0 {
		s.cfg.CircuitBreaker.HealthCheckInterval = time.Duration(*req.HealthCheckIntervalSecs) * time.Second
	}
	if req.SuccessThreshold != nil && *req.SuccessThreshold > 0 {
		s.cfg.CircuitBreaker.SuccessThreshold = *req.SuccessThreshold
	}
	if req.BalanceAbsThreshold != nil {
		s.cfg.BalanceAbsThreshold = req.BalanceAbsThreshold
	}
	if req.BalanceRelThreshold != nil {
		s.cfg.BalanceRelThreshold = req.BalanceRelThreshold
	}
	if req.CacheThreshold != nil {
		s.cfg.CacheThreshold = req.CacheThreshold
	}
	if req.ExtraArgs != nil {
		s.cfg.ExtraArgs = req.ExtraArgs
	}

	if req.Models != nil {
		for _, m := range req.Models {
			rule := config.ModelRule{
				ModelName:           m.ModelName,
				Mode:                m.Mode,
				Policy:              m.Policy,
				BalanceAbsThreshold: m.BalanceAbsThreshold,
				BalanceRelThreshold: m.BalanceRelThreshold,
				CacheThreshold:      m.CacheThreshold,
				ExtraArgs:           m.ExtraArgs,
			}
			s.modelRules[m.ModelName] = rule
			s.applyModelRuleLocked(ctx, rule)
		}
	}

	_ = s.saveConfigToFileLocked()
	return s.getConfigLocked(), nil
}

// UpdateModelRule updates rule for a single model and hot-applies immediately.
func (s *Server) UpdateModelRule(ctx context.Context, req dashboard.ModelRuleUpdateRequest) (*dashboard.ConfigSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rule := config.ModelRule{
		ModelName:           req.ModelName,
		Mode:                req.Mode,
		Policy:              req.Policy,
		BalanceAbsThreshold: req.BalanceAbsThreshold,
		BalanceRelThreshold: req.BalanceRelThreshold,
		CacheThreshold:      req.CacheThreshold,
		ExtraArgs:           req.ExtraArgs,
	}
	s.modelRules[req.ModelName] = rule
	s.applyModelRuleLocked(ctx, rule)

	_ = s.saveConfigToFileLocked()
	return s.getConfigLocked(), nil
}

func (s *Server) applyModelRuleLocked(ctx context.Context, rule config.ModelRule) {
	pool, ok := s.modelPools[rule.ModelName]
	if !ok || pool == nil {
		return
	}

	targetMode := rule.Mode
	if targetMode == "" {
		targetMode = "proxy"
	}
	prevMode := pool.Mode
	pool.Mode = targetMode

	if rule.Policy != "" {
		if p, err := router.ParsePolicy(rule.Policy); err == nil {
			pool.Policy = p
			pool.Balancer = NewBalancer(p, pool.Targets)
		}
	} else {
		pool.Policy = s.cfg.Policy
		pool.Balancer = NewBalancer(s.cfg.Policy, pool.Targets)
	}

	pool.BalanceAbsThreshold = rule.BalanceAbsThreshold
	pool.BalanceRelThreshold = rule.BalanceRelThreshold
	pool.CacheThreshold = rule.CacheThreshold
	pool.ExtraArgs = rule.ExtraArgs

	// Mode run: spawn runner if needed
	if targetMode == "run" && prevMode != "run" && len(pool.Targets) > 0 {
		var urls []string
		for _, t := range pool.Targets {
			urls = append(urls, t.URLString)
		}
		log.Printf("[Proxy] Model [%s] switched to run mode. Spawning vllm-router runner...", pool.ModelName)
		go func(p *ModelPool, u []string) {
			_ = s.startRunnerForPool(p, u)
		}(pool, urls)
	}

	// Mode proxy: terminate runner if exists
	if targetMode == "proxy" && prevMode == "run" {
		if pool.RunnerCmd != nil && pool.RunnerCmd.Process != nil {
			log.Printf("[Proxy] Model [%s] switched to proxy mode. Stopping child runner...", pool.ModelName)
			_ = pool.RunnerCmd.Process.Kill()
			pool.RunnerCmd = nil
			pool.RunnerTarget = nil
		}
	}
}

func (s *Server) startRunnerForPool(pool *ModelPool, urls []string) error {
	freePort, err := router.GetFreePort()
	if err != nil {
		return fmt.Errorf("failed to get free port: %w", err)
	}

	balAbs := 0
	if pool.BalanceAbsThreshold != nil && *pool.BalanceAbsThreshold > 0 {
		balAbs = *pool.BalanceAbsThreshold
	} else if s.cfg.BalanceAbsThreshold != nil && *s.cfg.BalanceAbsThreshold > 0 {
		balAbs = *s.cfg.BalanceAbsThreshold
	}

	balRel := 0.0
	if pool.BalanceRelThreshold != nil && *pool.BalanceRelThreshold > 0 {
		balRel = *pool.BalanceRelThreshold
	} else if s.cfg.BalanceRelThreshold != nil && *s.cfg.BalanceRelThreshold > 0 {
		balRel = *s.cfg.BalanceRelThreshold
	}

	cacheThresh := 0.0
	if pool.CacheThreshold != nil && *pool.CacheThreshold > 0 {
		cacheThresh = *pool.CacheThreshold
	} else if s.cfg.CacheThreshold != nil && *s.cfg.CacheThreshold > 0 {
		cacheThresh = *s.cfg.CacheThreshold
	}

	extraArgs := pool.ExtraArgs
	if len(extraArgs) == 0 {
		extraArgs = s.cfg.ExtraArgs
	}

	cfg := router.Config{
		RouterBin:           "vllm-router",
		Host:                "127.0.0.1",
		Port:                freePort,
		Policy:              pool.Policy,
		WorkerURLs:          urls,
		Backend:             "vllm",
		LogLevel:            "info",
		BalanceAbsThreshold: balAbs,
		BalanceRelThreshold: balRel,
		CacheThreshold:      cacheThresh,
		ExtraArgs:           extraArgs,
	}
	args := router.BuildArgs(cfg)
	cmd := exec.Command("vllm-router", args...)
	if err := cmd.Start(); err != nil {
		log.Printf("[Proxy] Failed to start vllm-router for [%s]: %v", pool.ModelName, err)
		return err
	}
	s.mu.Lock()
	pool.RunnerCmd = cmd
	pool.RunnerPort = freePort
	pool.RunnerTarget, _ = url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePort))
	s.mu.Unlock()
	return nil
}

func (s *Server) saveConfigToFileLocked() error {
	if s.configFilePath == "" {
		s.configFilePath = "config.yaml"
	}

	cbEnabled := true
	if s.cfg.CircuitBreaker.Enabled != nil {
		cbEnabled = *s.cfg.CircuitBreaker.Enabled
	}

	cfgToSave := &config.FileConfig{
		Router: config.RouterConfig{
			Mode:                "proxy",
			Host:                s.cfg.Host,
			Port:                s.cfg.Port,
			WatchInterval:       s.cfg.WatchInterval,
			ZeroDowntime:        &s.cfg.ZeroDowntime,
			DrainTimeout:        s.cfg.DrainTimeout,
			BalanceAbsThreshold: s.cfg.BalanceAbsThreshold,
			BalanceRelThreshold: s.cfg.BalanceRelThreshold,
			CacheThreshold:      s.cfg.CacheThreshold,
			ExtraArgs:           s.cfg.ExtraArgs,
		},
		Target: config.TargetConfig{
			ModelName: s.cfg.ModelName,
			Policy:    string(s.cfg.Policy),
		},
		CircuitBreaker: config.CircuitBreakerConfig{
			Enabled:             &cbEnabled,
			MaxFailures:         s.cfg.CircuitBreaker.MaxFailures,
			SuccessThreshold:    s.cfg.CircuitBreaker.SuccessThreshold,
			Cooldown:            s.cfg.CircuitBreaker.Cooldown,
			WindowDuration:      10 * time.Second,
			MaxRetries:          s.cfg.CircuitBreaker.MaxRetries,
			HealthCheckInterval: s.cfg.CircuitBreaker.HealthCheckInterval,
		},
	}

	for _, r := range s.modelRules {
		cfgToSave.Models = append(cfgToSave.Models, r)
	}

	return config.SaveConfig(s.configFilePath, cfgToSave)
}

func (s *Server) getConfigLocked() *dashboard.ConfigSnapshot {
	var models []dashboard.ModelRuleDTO
	for mName, r := range s.modelRules {
		models = append(models, dashboard.ModelRuleDTO{
			ModelName:           mName,
			Mode:                r.Mode,
			Policy:              r.Policy,
			BalanceAbsThreshold: r.BalanceAbsThreshold,
			BalanceRelThreshold: r.BalanceRelThreshold,
			CacheThreshold:      r.CacheThreshold,
			ExtraArgs:           r.ExtraArgs,
		})
	}
	existing := make(map[string]bool)
	for _, m := range models {
		existing[m.ModelName] = true
	}
	for mName, pool := range s.modelPools {
		if !existing[mName] {
			mode := pool.Mode
			if mode == "" {
				mode = "proxy"
			}
			policy := pool.Policy
			if policy == "" {
				policy = s.cfg.Policy
			}
			models = append(models, dashboard.ModelRuleDTO{
				ModelName: mName,
				Mode:      string(mode),
				Policy:    string(policy),
			})
			existing[mName] = true
		}
	}
	sort.Slice(models, func(i, j int) bool {
		return models[i].ModelName < models[j].ModelName
	})

	cbEnabled := true
	if s.cfg.CircuitBreaker.Enabled != nil {
		cbEnabled = *s.cfg.CircuitBreaker.Enabled
	}

	return &dashboard.ConfigSnapshot{
		Mode:                    "proxy",
		Policy:                  string(s.cfg.Policy),
		WatchIntervalSecs:       int(s.cfg.WatchInterval.Seconds()),
		ZeroDowntime:            s.cfg.ZeroDowntime,
		DrainTimeoutSecs:        int(s.cfg.DrainTimeout.Seconds()),
		BalanceAbsThreshold:     s.cfg.BalanceAbsThreshold,
		BalanceRelThreshold:     s.cfg.BalanceRelThreshold,
		CacheThreshold:          s.cfg.CacheThreshold,
		ExtraArgs:               s.cfg.ExtraArgs,
		CircuitBreakerEnabled:   cbEnabled,
		MaxFailures:             s.cfg.CircuitBreaker.MaxFailures,
		CooldownSecs:            int(s.cfg.CircuitBreaker.Cooldown.Seconds()),
		MaxRetries:              s.cfg.CircuitBreaker.MaxRetries,
		HealthCheckIntervalSecs: int(s.cfg.CircuitBreaker.HealthCheckInterval.Seconds()),
		SuccessThreshold:        s.cfg.CircuitBreaker.SuccessThreshold,
		Models:                  models,
		AvailablePolicies:       []string{"consistent_hash", "cache_aware", "rendezvous_hash", "round_robin", "power_of_two", "random"},
		AvailableModes:          []string{"proxy", "run"},
		ConfigFilePath:          s.configFilePath,
	}
}


