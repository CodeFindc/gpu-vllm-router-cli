package router

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gpu-vllm-router/pkg/config"
	"gpu-vllm-router/pkg/dashboard"
	"gpu-vllm-router/pkg/gpustack"
	"gpu-vllm-router/pkg/swagger"
)

// WorkerBreaker tracks health and circuit breaker status of a backend worker instance in mode: run.
type WorkerBreaker struct {
	URL              string    `json:"url"`
	ConsecutiveFails int       `json:"consecutive_fails"`
	LastProbe        time.Time `json:"last_probe"`
	LastErr          string    `json:"last_error,omitempty"`
	Healthy          bool      `json:"healthy"`
	OpenSince        time.Time `json:"open_since,omitempty"`
}

// SupervisorConfig defines configuration for the router supervisor.
type SupervisorConfig struct {
	ZeroDowntime        bool
	PublicHost          string
	PublicPort          int
	DrainTimeout        time.Duration
	HealthCheckTimeout  time.Duration
	WatchInterval       time.Duration
	WorkerProbeInterval time.Duration
	WorkerMaxFailures   int

	RouterCfg      Config
	ConfigFilePath string
	ModelRules     []config.ModelRule
}

type runningProcess struct {
	cmd       *exec.Cmd
	port      int
	targetURL *url.URL
}

// ModelRunner manages a dedicated vllm-router instance for a specific model.
type ModelRunner struct {
	ModelName    string
	Mode         string // "run" (default) or "proxy"
	Policy       Policy // routing policy for this model
	mu           sync.RWMutex
	CurrentProc  *runningProcess
	ActiveTarget *url.URL
	ActiveURLs   []string
	AllURLs      []string
	activeConns  int64
	reloading    bool
}

// Supervisor oversees vllm-router instances with zero-downtime rolling reload.
// In single-model mode (-model <name>), it manages 1 official vllm-router process.
// In multi-model mode (model == ""), it manages a pool of official vllm-router processes (1 per model).
type Supervisor struct {
	client    *gpustack.Client
	modelName string
	cfg       SupervisorConfig

	mu               sync.RWMutex
	runners          map[string]*ModelRunner
	totalActiveConns int64
	currentProc      *runningProcess // for backward compatibility & single-model direct access
	activeTarget     *url.URL         // for backward compatibility & single-model direct access
	activeURLs       []string         // for backward compatibility & single-model direct access

	configFilePath string
	modelRules     map[string]config.ModelRule

	workerMu     sync.RWMutex
	workerStates map[string]*WorkerBreaker

	frontServer *http.Server
	stopCh      chan struct{}

	spawnProcessFunc  func(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error)
	probeWorkerFunc   func(ctx context.Context, rawURL string) (bool, error)
	waitForHealthFunc func(ctx context.Context, port int, timeout time.Duration) error
}

// NewSupervisor creates a new Supervisor instance.
func NewSupervisor(client *gpustack.Client, modelName string, cfg SupervisorConfig) *Supervisor {
	if cfg.PublicHost == "" {
		cfg.PublicHost = "0.0.0.0"
	}
	if cfg.PublicPort <= 0 {
		cfg.PublicPort = 8000
	}
	if cfg.WatchInterval <= 0 {
		cfg.WatchInterval = 10 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 60 * time.Second
	}
	if cfg.HealthCheckTimeout <= 0 {
		cfg.HealthCheckTimeout = 30 * time.Second
	}
	if cfg.WorkerProbeInterval <= 0 {
		cfg.WorkerProbeInterval = 3 * time.Second
	}
	if cfg.WorkerMaxFailures <= 0 {
		cfg.WorkerMaxFailures = 3
	}

	rulesMap := make(map[string]config.ModelRule)
	for _, r := range cfg.ModelRules {
		rulesMap[r.ModelName] = r
	}

	return &Supervisor{
		client:         client,
		modelName:      modelName,
		cfg:            cfg,
		runners:        make(map[string]*ModelRunner),
		workerStates:   make(map[string]*WorkerBreaker),
		stopCh:         make(chan struct{}),
		configFilePath: cfg.ConfigFilePath,
		modelRules:     rulesMap,
	}
}

func (s *Supervisor) initWorkers(urls []string) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	for _, u := range urls {
		if _, exists := s.workerStates[u]; !exists {
			s.workerStates[u] = &WorkerBreaker{
				URL:       u,
				Healthy:   true,
				LastProbe: time.Now(),
			}
		}
	}
}

// syncWorkers ensures all activeURLs are tracked in workerStates and prunes obsolete/dead URLs.
func (s *Supervisor) syncWorkers(activeURLs []string) {
	if len(activeURLs) == 0 {
		return
	}
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	activeSet := make(map[string]bool, len(activeURLs))
	for _, u := range activeURLs {
		activeSet[u] = true
		if _, exists := s.workerStates[u]; !exists {
			s.workerStates[u] = &WorkerBreaker{
				URL:       u,
				Healthy:   true,
				LastProbe: time.Now(),
			}
		}
	}

	for u := range s.workerStates {
		if !activeSet[u] {
			log.Printf("[Supervisor] Pruning obsolete worker state: %s", u)
			delete(s.workerStates, u)
		}
	}
}

// GetFreePort finds an available unprivileged TCP port on 127.0.0.1.
func GetFreePort() (int, error) {
	return getFreePort()
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// inspectModelFromRequest peeks at the incoming request to determine the target model name.
func inspectModelFromRequest(req *http.Request) (string, []byte) {
	if m := req.URL.Query().Get("model"); m != "" {
		return m, nil
	}

	if req.Body != nil && (req.Method == http.MethodPost || req.Method == http.MethodPut) {
		bodyBytes, err := io.ReadAll(req.Body)
		if err == nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))

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

// findRunner matches the requested model name against active model runners.
func (s *Supervisor) findRunner(modelName string) (*ModelRunner, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.runners) == 0 {
		return nil, fmt.Errorf("no active vllm-router instances in cluster")
	}

	if modelName == "" {
		if s.modelName != "" {
			if r, ok := s.runners[s.modelName]; ok {
				return r, nil
			}
		}
		if len(s.runners) == 1 {
			for _, r := range s.runners {
				return r, nil
			}
		}
		var available []string
		for k := range s.runners {
			available = append(available, k)
		}
		return nil, fmt.Errorf("request did not specify 'model'. Available models in cluster: %v", available)
	}

	// 1. Exact match
	if r, ok := s.runners[modelName]; ok {
		return r, nil
	}

	// 2. Case-insensitive match
	for k, r := range s.runners {
		if strings.EqualFold(k, modelName) {
			return r, nil
		}
	}

	// 3. Substring / Prefix match
	for k, r := range s.runners {
		if strings.Contains(strings.ToLower(k), strings.ToLower(modelName)) ||
			strings.Contains(strings.ToLower(modelName), strings.ToLower(k)) {
			return r, nil
		}
	}

	var available []string
	for k := range s.runners {
		available = append(available, k)
	}
	return nil, fmt.Errorf("model %q not found. Available models: %v", modelName, available)
}

// Start launches the initial vllm-router instances and the front gateway.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	s.runners = make(map[string]*ModelRunner)
	s.mu.Unlock()

	if s.client != nil {
		if s.modelName != "" {
			// Single Model Mode
			endpoints, model, err := s.client.GetRunningWorkerEndpoints(ctx, s.modelName)
			if err != nil {
				log.Printf("[Supervisor] ⚠️ Initial discovery warning for model %q: %v. Entering STANDBY mode...", s.modelName, err)
			} else if len(endpoints) == 0 {
				log.Printf("[Supervisor] ⚠️ Warning: no running instances found for model %q in GPUStack at startup. Entering STANDBY mode...", s.modelName)
			} else {
				var urls []string
				log.Printf("[Supervisor] Discovered %d running instance(s) for model %s (ID: %d):", len(endpoints), model.Name, model.ID)
				for _, ep := range endpoints {
					log.Printf("  -> Instance: %-25s Worker: %-15s Endpoint: %s", ep.InstanceName, ep.WorkerName, ep.URL)
					urls = append(urls, ep.URL)
				}
				sort.Strings(urls)

				runner, err := s.startModelRunner(ctx, model.Name, urls)
				if err != nil {
					return fmt.Errorf("failed to start router for model %s: %w", model.Name, err)
				}
				runner.AllURLs = urls
				s.syncWorkers(urls)

				s.mu.Lock()
				s.runners[model.Name] = runner
				s.currentProc = runner.CurrentProc
				s.activeTarget = runner.ActiveTarget
				s.activeURLs = urls
				s.mu.Unlock()
			}
		} else {
			// Cluster-wide Multi-Model Pool Mode
			cluster, err := s.client.GetAllRunningWorkerEndpoints(ctx)
			if err != nil {
				log.Printf("[Supervisor] ⚠️ Cluster-wide model discovery warning: %v. Entering STANDBY mode...", err)
			} else if cluster == nil || cluster.InstanceCount == 0 {
				log.Printf("[Supervisor] ⚠️ Warning: no running model instances found across entire GPUStack cluster at startup. Entering STANDBY mode...")
			} else {
				log.Printf("[Supervisor] [Multi-Model Router Pool] Discovered %d model(s) and %d running instance(s) in cluster:",
					cluster.ModelCount, cluster.InstanceCount)

				var allClusterURLs []string
				for mName, eps := range cluster.ModelsEndpoints {
					var urls []string
					for _, ep := range eps {
						urls = append(urls, ep.URL)
						allClusterURLs = append(allClusterURLs, ep.URL)
					}
					sort.Strings(urls)
					log.Printf("[Supervisor] Initializing vllm-router process for model %q with %d worker(s)...", mName, len(urls))

					runner, err := s.startModelRunner(ctx, mName, urls)
					if err != nil {
						log.Printf("[Supervisor] Warning: failed to start router for model %q: %v", mName, err)
						continue
					}
					runner.AllURLs = urls

					s.mu.Lock()
					s.runners[mName] = runner
					if s.activeTarget == nil {
						s.activeTarget = runner.ActiveTarget
						s.currentProc = runner.CurrentProc
						s.activeURLs = urls
					}
					s.mu.Unlock()
				}
				s.syncWorkers(allClusterURLs)
			}
		}
	} else {
		log.Printf("[Supervisor] Standby mode: running without active GPUStack client.")
	}

	// Start Front Gateway Server on PublicHost:PublicPort
	mux := http.NewServeMux()

	// Health & liveness probes (K8s & unified convention)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/livez", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleHealth)
	mux.HandleFunc("/ping", s.handleHealth)

	// Admin stats & supervisor status (Unified dual-mode support)
	mux.HandleFunc("/admin/supervisor", s.handleSupervisorStatus)
	mux.HandleFunc("/admin/stats", s.handleSupervisorStatus)

	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/metrics", s.handleMetrics)
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
	mux.HandleFunc("/", s.handleProxy)

	addr := fmt.Sprintf("%s:%d", s.cfg.PublicHost, s.cfg.PublicPort)
	s.frontServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("[Supervisor] Front Gateway listening on http://%s (Managing %d model router processes, Zero-Downtime=%t)",
			addr, len(s.runners), s.cfg.ZeroDowntime)
		log.Printf("[Supervisor] Swagger UI documentation: http://%s/docs (OpenAPI spec: /openapi.json)", addr)
		if err := s.frontServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Supervisor] Front proxy server error: %v", err)
		}
	}()

	// Start dynamic watch loop and proactive worker probe loop
	go s.watchLoop(ctx)
	go s.workerProbeLoop(ctx)
	return nil
}

func (s *Supervisor) doSpawn(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
	return s.doSpawnForModel(ctx, "", host, port, workerURLs)
}

func (s *Supervisor) doSpawnForModel(ctx context.Context, modelName string, host string, port int, workerURLs []string) (*runningProcess, error) {
	if s.spawnProcessFunc != nil {
		return s.spawnProcessFunc(ctx, host, port, workerURLs)
	}
	return s.spawnProcessForModel(ctx, modelName, host, port, workerURLs)
}

func (s *Supervisor) startModelRunner(ctx context.Context, modelName string, workerURLs []string) (*ModelRunner, error) {
	ruleMode := "run"
	rulePolicy := s.cfg.RouterCfg.Policy
	s.mu.RLock()
	if rule, ok := s.modelRules[modelName]; ok {
		if rule.Mode != "" {
			ruleMode = rule.Mode
		}
		if rule.Policy != "" {
			rulePolicy = Policy(rule.Policy)
		}
	}
	s.mu.RUnlock()

	if ruleMode == "proxy" {
		log.Printf("[Supervisor] Launching model [%s] in direct 'proxy' mode (Workers: %d, Policy: %s)...",
			modelName, len(workerURLs), rulePolicy)
		return &ModelRunner{
			ModelName:  modelName,
			Mode:       "proxy",
			Policy:     rulePolicy,
			ActiveURLs: workerURLs,
			AllURLs:    workerURLs,
		}, nil
	}

	internalPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate internal port for %s: %w", modelName, err)
	}

	log.Printf("[Supervisor] Launching vllm-router process for [%s] on internal port %d (Workers: %d)...",
		modelName, internalPort, len(workerURLs))
	proc, err := s.doSpawnForModel(ctx, modelName, "127.0.0.1", internalPort, workerURLs)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn router process for %s: %w", modelName, err)
	}

	// Wait for process readiness
	if err := s.checkHealth(ctx, internalPort, s.cfg.HealthCheckTimeout); err != nil {
		if proc != nil && proc.cmd != nil && proc.cmd.Process != nil {
			_ = proc.cmd.Process.Kill()
		}
		return nil, fmt.Errorf("health check failed on internal port %d for model %s: %w", internalPort, modelName, err)
	}

	return &ModelRunner{
		ModelName:    modelName,
		Mode:         "run",
		Policy:       rulePolicy,
		CurrentProc:  proc,
		ActiveTarget: proc.targetURL,
		ActiveURLs:   workerURLs,
		AllURLs:      workerURLs,
	}, nil
}

func (s *Supervisor) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	count := len(s.runners)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/readyz" && count == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"standby","message":"waiting for model routers to be ready"}`))
		return
	}

	w.WriteHeader(http.StatusOK)
	if count == 0 {
		w.Write([]byte(`{"status":"ok","standby":true,"models":0}`))
		return
	}
	w.Write([]byte(fmt.Sprintf(`{"status":"ok","models":%d}`, count)))
}

func (s *Supervisor) handleModels(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	var modelCards []map[string]interface{}
	now := time.Now().Unix()
	for name := range s.runners {
		modelCards = append(modelCards, map[string]interface{}{
			"id":       name,
			"object":   "model",
			"created":  now,
			"owned_by": "gpustack-supervisor",
		})
	}
	s.mu.RUnlock()

	resp := map[string]interface{}{
		"object": "list",
		"data":   modelCards,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Supervisor) handleSupervisorStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	modelsMap := make(map[string]interface{})
	for name, runner := range s.runners {
		runner.mu.RLock()
		port := 0
		if runner.CurrentProc != nil {
			port = runner.CurrentProc.port
		}
		targetStr := ""
		if runner.ActiveTarget != nil {
			targetStr = runner.ActiveTarget.String()
		}
		modelsMap[name] = map[string]interface{}{
			"internal_port": port,
			"target":        targetStr,
			"worker_count":  len(runner.ActiveURLs),
			"worker_urls":   runner.ActiveURLs,
			"all_urls":      runner.AllURLs,
			"reloading":     runner.reloading,
		}
		runner.mu.RUnlock()
	}

	s.workerMu.RLock()
	workersMap := make(map[string]interface{})
	for u, wb := range s.workerStates {
		openSinceStr := ""
		if !wb.OpenSince.IsZero() {
			openSinceStr = wb.OpenSince.Format(time.RFC3339)
		}
		workersMap[u] = map[string]interface{}{
			"healthy":           wb.Healthy,
			"consecutive_fails": wb.ConsecutiveFails,
			"last_probe":        wb.LastProbe.Format(time.RFC3339),
			"last_error":        wb.LastErr,
			"open_since":        openSinceStr,
		}
	}
	s.workerMu.RUnlock()

	resp := map[string]interface{}{
		"mode":          "supervisor_run_mode",
		"multi_model":   s.modelName == "",
		"public_port":   s.cfg.PublicPort,
		"model_count":   len(s.runners),
		"drain_timeout": s.cfg.DrainTimeout.String(),
		"zero_downtime": s.cfg.ZeroDowntime,
		"probe_config": map[string]interface{}{
			"interval":     s.cfg.WorkerProbeInterval.String(),
			"max_failures": s.cfg.WorkerMaxFailures,
		},
		"models":  modelsMap,
		"workers": workersMap,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleMetrics provides Prometheus-formatted metrics for Supervisor in mode: run.
func (s *Supervisor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	modelCount := len(s.runners)
	runners := make([]*ModelRunner, 0, modelCount)
	for _, r := range s.runners {
		runners = append(runners, r)
	}
	s.mu.RUnlock()

	s.workerMu.RLock()
	workers := make(map[string]WorkerBreaker, len(s.workerStates))
	for u, wb := range s.workerStates {
		workers[u] = *wb
	}
	s.workerMu.RUnlock()

	var buf bytes.Buffer

	buf.WriteString("# HELP gpu_router_models_total Total number of registered active models\n")
	buf.WriteString("# TYPE gpu_router_models_total gauge\n")
	fmt.Fprintf(&buf, "gpu_router_models_total %d\n\n", modelCount)

	buf.WriteString("# HELP gpu_router_workers_total Total number of backend workers tracked by supervisor\n")
	buf.WriteString("# TYPE gpu_router_workers_total gauge\n")
	fmt.Fprintf(&buf, "gpu_router_workers_total %d\n\n", len(workers))

	buf.WriteString("# HELP gpu_router_worker_health Health status of worker (1 = healthy, 0 = unhealthy/circuit open)\n")
	buf.WriteString("# TYPE gpu_router_worker_health gauge\n")
	for u, wb := range workers {
		hVal := 0
		if wb.Healthy {
			hVal = 1
		}
		fmt.Fprintf(&buf, "gpu_router_worker_health{worker=%q} %d\n", u, hVal)
	}
	buf.WriteString("\n")

	buf.WriteString("# HELP gpu_router_worker_consecutive_failures Consecutive probe failure count\n")
	buf.WriteString("# TYPE gpu_router_worker_consecutive_failures gauge\n")
	for u, wb := range workers {
		fmt.Fprintf(&buf, "gpu_router_worker_consecutive_failures{worker=%q} %d\n", u, wb.ConsecutiveFails)
	}
	buf.WriteString("\n")

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write(buf.Bytes())
}

func (s *Supervisor) handleProxy(w http.ResponseWriter, req *http.Request) {
	// Return status JSON for root GET requests
	if req.URL.Path == "/" && req.Method == http.MethodGet && req.URL.Query().Get("model") == "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "gpu-vllm-router-cli",
			"status":  "running",
			"mode":    "run",
			"docs":    "/docs",
			"openapi": "/openapi.json",
			"health":  "/health",
			"models":  "/v1/models",
		})
		return
	}

	// Safeguard for internal dashboard REST APIs
	if strings.HasPrefix(req.URL.Path, "/api/") {
		dashHandler := dashboard.NewHandler(s)
		dashHandler.ServeHTTP(w, req)
		return
	}

	modelName, _ := inspectModelFromRequest(req)
	runner, err := s.findRunner(modelName)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		resp := map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("The model %q does not exist or has no healthy backends in GPUStack cluster. Details: %v", modelName, err),
				"type":    "invalid_request_error",
				"param":   "model",
				"code":    "model_not_found",
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	runner.mu.RLock()
	mode := runner.Mode
	target := runner.ActiveTarget
	activeURLs := append([]string(nil), runner.ActiveURLs...)
	runner.mu.RUnlock()

	if mode == "proxy" {
		if len(activeURLs) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":{"message":"No active backends for model %s","type":"bad_gateway"}}`, runner.ModelName)))
			return
		}

		var healthyURLs []string
		s.workerMu.RLock()
		for _, u := range activeURLs {
			if wb, ok := s.workerStates[u]; ok && wb.Healthy {
				healthyURLs = append(healthyURLs, u)
			}
		}
		s.workerMu.RUnlock()

		if len(healthyURLs) == 0 {
			healthyURLs = activeURLs
		}

		idx := int(atomic.AddInt64(&runner.activeConns, 1)) % len(healthyURLs)
		if idx < 0 {
			idx = -idx
		}
		chosenURL := healthyURLs[idx]
		destURL, parseErr := url.Parse(chosenURL)
		if parseErr != nil {
			atomic.AddInt64(&runner.activeConns, -1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":{"message":"Invalid worker URL %q: %v","type":"bad_gateway"}}`, chosenURL, parseErr)))
			return
		}

		atomic.AddInt64(&s.totalActiveConns, 1)
		defer atomic.AddInt64(&s.totalActiveConns, -1)
		defer atomic.AddInt64(&runner.activeConns, -1)

		proxy := &httputil.ReverseProxy{
			Director: func(r *http.Request) {
				r.URL.Scheme = destURL.Scheme
				r.URL.Host = destURL.Host
				r.Host = destURL.Host
				if _, ok := r.Header["User-Agent"]; !ok {
					r.Header.Set("User-Agent", "gpu-vllm-router-supervisor-direct/1.0")
				}
			},
			FlushInterval: 10 * time.Millisecond,
			ErrorHandler: func(rw http.ResponseWriter, r *http.Request, err error) {
				log.Printf("[Supervisor:DirectProxy] Forwarding error for model %s to %s: %v", runner.ModelName, chosenURL, err)
				rw.WriteHeader(http.StatusBadGateway)
				_, _ = rw.Write([]byte(fmt.Sprintf(`{"error":{"message":"Router direct proxy error for model %s: %v","type":"bad_gateway"}}`, runner.ModelName, err)))
			},
		}
		proxy.ServeHTTP(w, req)
		return
	}

	if target == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":{"message":"Router process for model is initializing, please retry shortly","type":"service_unavailable"}}`))
		return
	}

	// Track in-flight concurrency
	atomic.AddInt64(&s.totalActiveConns, 1)
	defer atomic.AddInt64(&s.totalActiveConns, -1)
	atomic.AddInt64(&runner.activeConns, 1)
	defer atomic.AddInt64(&runner.activeConns, -1)

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = target.Scheme
			r.URL.Host = target.Host
			r.Host = target.Host
			if _, ok := r.Header["User-Agent"]; !ok {
				r.Header.Set("User-Agent", "gpu-vllm-router-supervisor/1.0")
			}
		},
		FlushInterval: 10 * time.Millisecond,
		ErrorHandler: func(rw http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[Supervisor:Proxy] Forwarding error for model %s: %v", runner.ModelName, err)
			go s.fastProbeRunner(runner)
			rw.WriteHeader(http.StatusBadGateway)
			_, _ = rw.Write([]byte(fmt.Sprintf(`{"error":{"message":"Router gateway error for model %s: %v","type":"bad_gateway"}}`, runner.ModelName, err)))
		},
	}

	proxy.ServeHTTP(w, req)
}

func (s *Supervisor) spawnProcess(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
	return s.spawnProcessForModel(ctx, "", host, port, workerURLs)
}

func (s *Supervisor) spawnProcessForModel(ctx context.Context, modelName string, host string, port int, workerURLs []string) (*runningProcess, error) {
	cfg := s.cfg.RouterCfg
	cfg.Host = host
	cfg.Port = port
	cfg.WorkerURLs = workerURLs

	if modelName != "" {
		s.mu.RLock()
		rule, ok := s.modelRules[modelName]
		s.mu.RUnlock()
		if ok {
			if rule.Policy != "" {
				if p, err := ParsePolicy(rule.Policy); err == nil {
					cfg.Policy = p
				}
			}
			if rule.BalanceAbsThreshold != nil && *rule.BalanceAbsThreshold > 0 {
				cfg.BalanceAbsThreshold = *rule.BalanceAbsThreshold
			}
			if rule.BalanceRelThreshold != nil && *rule.BalanceRelThreshold > 0 {
				cfg.BalanceRelThreshold = *rule.BalanceRelThreshold
			}
			if rule.CacheThreshold != nil && *rule.CacheThreshold > 0 {
				cfg.CacheThreshold = *rule.CacheThreshold
			}
			if len(rule.ExtraArgs) > 0 {
				cfg.ExtraArgs = rule.ExtraArgs
			}
		}
	}

	args := BuildArgs(cfg)
	bin := cfg.RouterBin
	if bin == "" {
		bin = "vllm-router"
	}

	log.Printf("[Supervisor] Spawning %s on %s:%d (Model: %q, Workers: %d, Policy: %s, Tuning: Abs=%d, Rel=%.2f, Cache=%.2f, Extra=%v)",
		bin, host, port, modelName, len(workerURLs), cfg.Policy, cfg.BalanceAbsThreshold, cfg.BalanceRelThreshold, cfg.CacheThreshold, cfg.ExtraArgs)

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start %s: %w", bin, err)
	}

	prefix := fmt.Sprintf("[vllm-router:%d]", port)
	go streamPipe(stdout, prefix)
	go streamPipe(stderr, prefix)

	targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))

	proc := &runningProcess{
		cmd:       cmd,
		port:      port,
		targetURL: targetURL,
	}

	go func() {
		waitErr := cmd.Wait()
		log.Printf("%s Process exited: %v", prefix, waitErr)
		s.handleProcessExit(proc, waitErr)
	}()

	return proc, nil
}

func (s *Supervisor) handleProcessExit(proc *runningProcess, waitErr error) {
	select {
	case <-s.stopCh:
		// Supervisor is shutting down, normal termination
		return
	default:
	}

	s.mu.RLock()
	var crashedRunner *ModelRunner
	for _, r := range s.runners {
		r.mu.RLock()
		if r.CurrentProc == proc {
			crashedRunner = r
		}
		r.mu.RUnlock()
		if crashedRunner != nil {
			break
		}
	}
	s.mu.RUnlock()

	if crashedRunner != nil {
		log.Printf("[Supervisor:Guard] 🚨 Active vllm-router process on port %d exited unexpectedly (%v)! Initiating emergency auto-respawn for [%s]...",
			proc.port, waitErr, crashedRunner.ModelName)
		crashedRunner.mu.RLock()
		targetURLs := make([]string, len(crashedRunner.ActiveURLs))
		copy(targetURLs, crashedRunner.ActiveURLs)
		crashedRunner.mu.RUnlock()

		if len(targetURLs) > 0 {
			go s.performZeroDowntimeReloadForRunner(context.Background(), crashedRunner, targetURLs)
		}
	}
}

func (s *Supervisor) probeWorker(ctx context.Context, rawURL string) (bool, error) {
	client := &http.Client{Timeout: 1500 * time.Millisecond}

	// 1. Try /health
	healthURL := strings.TrimRight(rawURL, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err == nil {
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true, nil
			}
		}
	}

	// 2. Fallback /v1/models
	modelsURL := strings.TrimRight(rawURL, "/") + "/v1/models"
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, modelsURL, nil)
	if err == nil {
		resp, err := client.Do(req2)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true, nil
			}
			return false, fmt.Errorf("HTTP status %d", resp.StatusCode)
		}
		return false, err
	}
	return false, err
}

func (s *Supervisor) probeRunnerWorkers(ctx context.Context, runner *ModelRunner) {
	runner.mu.RLock()
	allURLs := make([]string, len(runner.AllURLs))
	copy(allURLs, runner.AllURLs)
	activeURLs := make([]string, len(runner.ActiveURLs))
	copy(activeURLs, runner.ActiveURLs)
	runner.mu.RUnlock()

	if len(allURLs) == 0 {
		return
	}

	type probeResult struct {
		url     string
		healthy bool
		err     error
	}

	results := make(chan probeResult, len(allURLs))
	var wg sync.WaitGroup
	for _, u := range allURLs {
		wg.Add(1)
		go func(targetURL string) {
			defer wg.Done()
			var ok bool
			var err error
			if s.probeWorkerFunc != nil {
				ok, err = s.probeWorkerFunc(ctx, targetURL)
			} else {
				ok, err = s.probeWorker(ctx, targetURL)
			}
			results <- probeResult{url: targetURL, healthy: ok, err: err}
		}(u)
	}
	wg.Wait()
	close(results)

	s.workerMu.Lock()
	for res := range results {
		wb, exists := s.workerStates[res.url]
		if !exists {
			wb = &WorkerBreaker{URL: res.url, Healthy: true}
			s.workerStates[res.url] = wb
		}
		wb.LastProbe = time.Now()
		if res.healthy {
			if !wb.Healthy {
				log.Printf("[Supervisor:Breaker] 🟢 Worker %s RECOVERED! Resetting circuit breaker.", res.url)
			}
			wb.ConsecutiveFails = 0
			wb.Healthy = true
			wb.LastErr = ""
			wb.OpenSince = time.Time{}
		} else {
			wb.ConsecutiveFails++
			wb.LastErr = fmt.Sprintf("%v", res.err)
			maxFails := s.cfg.WorkerMaxFailures
			if maxFails <= 0 {
				maxFails = 3
			}
			if wb.Healthy && wb.ConsecutiveFails >= maxFails {
				wb.Healthy = false
				wb.OpenSince = time.Now()
				log.Printf("[Supervisor:Breaker] 🔴 CIRCUIT BREAKER TRIPPED for worker %s! (%d consecutive failures, last error: %v)",
					res.url, wb.ConsecutiveFails, res.err)
			}
		}
	}

	var healthyURLs []string
	for _, u := range allURLs {
		if wb := s.workerStates[u]; wb != nil && wb.Healthy {
			healthyURLs = append(healthyURLs, u)
		}
	}
	s.workerMu.Unlock()

	sort.Strings(healthyURLs)
	sort.Strings(activeURLs)

	if !reflect.DeepEqual(healthyURLs, activeURLs) {
		if len(healthyURLs) == 0 {
			log.Printf("[Supervisor:Breaker] ⚠️ CRITICAL: All workers for model %q are unhealthy! Keeping current active router to avoid complete service drop.", runner.ModelName)
		} else {
			log.Printf("[Supervisor:Breaker] ⚡ Healthy worker pool changed for model %q! (Active: %d -> Healthy: %d). Triggering immediate zero-downtime rolling reload...",
				runner.ModelName, len(activeURLs), len(healthyURLs))
			go s.performZeroDowntimeReloadForRunner(ctx, runner, healthyURLs)
		}
	}
}

func (s *Supervisor) probeAllWorkers(ctx context.Context) {
	s.mu.RLock()
	runners := make([]*ModelRunner, 0, len(s.runners))
	for _, r := range s.runners {
		runners = append(runners, r)
	}
	s.mu.RUnlock()

	for _, r := range runners {
		s.probeRunnerWorkers(ctx, r)
	}
}

func (s *Supervisor) workerProbeLoop(ctx context.Context) {
	interval := s.cfg.WorkerProbeInterval
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
			s.probeAllWorkers(ctx)
		}
	}
}

func (s *Supervisor) fastProbeRunner(runner *ModelRunner) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.probeRunnerWorkers(ctx, runner)
}

func streamPipe(r io.Reader, prefix string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		log.Printf("%s %s", prefix, scanner.Text())
	}
}

func (s *Supervisor) checkHealth(ctx context.Context, port int, timeout time.Duration) error {
	if s.waitForHealthFunc != nil {
		return s.waitForHealthFunc(ctx, port, timeout)
	}
	return s.waitForHealth(ctx, port, timeout)
}

func (s *Supervisor) waitForHealth(ctx context.Context, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	modelsURL := fmt.Sprintf("http://127.0.0.1:%d/v1/models", port)

	client := &http.Client{Timeout: 1 * time.Second}

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
			// Check /health first
			resp, err := client.Get(healthURL)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					log.Printf("[Supervisor] Health check passed on port %d via /health", port)
					return nil
				}
			}

			// Fallback check /v1/models
			resp2, err2 := client.Get(modelsURL)
			if err2 == nil {
				resp2.Body.Close()
				if resp2.StatusCode == http.StatusOK {
					log.Printf("[Supervisor] Health check passed on port %d via /v1/models", port)
					return nil
				}
			}
		}
	}

	return fmt.Errorf("health check timed out after %v on port %d", timeout, port)
}

// watchLoop periodically checks GPUStack for changes in model instances.
func (s *Supervisor) watchLoop(ctx context.Context) {
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
			if s.modelName != "" {
				// Single model watch
				endpoints, _, err := s.client.GetRunningWorkerEndpoints(ctx, s.modelName)
				if err != nil {
					log.Printf("[Supervisor] Warning: failed to query GPUStack instances for %s: %v", s.modelName, err)
					continue
				}
				var newURLs []string
				for _, ep := range endpoints {
					newURLs = append(newURLs, ep.URL)
				}
				sort.Strings(newURLs)

				s.mu.RLock()
				runner := s.runners[s.modelName]
				s.mu.RUnlock()

				if runner != nil {
					runner.mu.Lock()
					runner.AllURLs = newURLs
					runner.mu.Unlock()
					s.syncWorkers(newURLs)

					// Compute healthy subset among newURLs
					s.workerMu.RLock()
					var healthyNewURLs []string
					for _, u := range newURLs {
						wb, ok := s.workerStates[u]
						if !ok || wb.Healthy {
							healthyNewURLs = append(healthyNewURLs, u)
						}
					}
					s.workerMu.RUnlock()

					if len(healthyNewURLs) == 0 {
						healthyNewURLs = newURLs
					}
					sort.Strings(healthyNewURLs)

					runner.mu.RLock()
					currURLs := runner.ActiveURLs
					runner.mu.RUnlock()

					if !reflect.DeepEqual(currURLs, healthyNewURLs) {
						log.Printf("[Supervisor] [Watch] Detected topology change for model %s!", s.modelName)
						log.Printf("  Previous URLs: %v", currURLs)
						log.Printf("  New URLs:      %v", healthyNewURLs)
						if len(healthyNewURLs) > 0 {
							s.performZeroDowntimeReloadForRunner(ctx, runner, healthyNewURLs)
						}
					}
				}
			} else {
				// Cluster-wide multi-model watch
				cluster, err := s.client.GetAllRunningWorkerEndpoints(ctx)
				if err != nil {
					log.Printf("[Supervisor] Warning: failed to query GPUStack cluster: %v", err)
					continue
				}

				// 1. Sync all active cluster worker states and prune obsolete endpoints
				var allClusterURLs []string
				for _, eps := range cluster.ModelsEndpoints {
					for _, ep := range eps {
						allClusterURLs = append(allClusterURLs, ep.URL)
					}
				}
				s.syncWorkers(allClusterURLs)

				// 2. Check existing and new models
				for mName, eps := range cluster.ModelsEndpoints {
					var newURLs []string
					for _, ep := range eps {
						newURLs = append(newURLs, ep.URL)
					}
					sort.Strings(newURLs)

					s.mu.RLock()
					runner, exists := s.runners[mName]
					s.mu.RUnlock()

					if !exists {
						log.Printf("[Supervisor] [Watch] Detected new model %q with %d instance(s) in GPUStack! Spawning router...", mName, len(newURLs))
						newRunner, err := s.startModelRunner(ctx, mName, newURLs)
						if err != nil {
							log.Printf("[Supervisor] Failed to spawn router for new model %q: %v", mName, err)
							continue
						}
						newRunner.AllURLs = newURLs
						s.mu.Lock()
						s.runners[mName] = newRunner
						s.mu.Unlock()
					} else {
						runner.mu.Lock()
						runner.AllURLs = newURLs
						runner.mu.Unlock()

						s.workerMu.RLock()
						var healthyNewURLs []string
						for _, u := range newURLs {
							wb, ok := s.workerStates[u]
							if !ok || wb.Healthy {
								healthyNewURLs = append(healthyNewURLs, u)
							}
						}
						s.workerMu.RUnlock()

						if len(healthyNewURLs) == 0 {
							healthyNewURLs = newURLs
						}
						sort.Strings(healthyNewURLs)

						runner.mu.RLock()
						currURLs := runner.ActiveURLs
						runner.mu.RUnlock()

						if !reflect.DeepEqual(currURLs, healthyNewURLs) {
							log.Printf("[Supervisor] [Watch] Detected topology change for model %q!", mName)
							log.Printf("  Previous URLs: %v", currURLs)
							log.Printf("  New URLs:      %v", healthyNewURLs)
							if len(healthyNewURLs) > 0 {
								s.performZeroDowntimeReloadForRunner(ctx, runner, healthyNewURLs)
							}
						}
					}
				}

				// 2. Check removed/stopped models
				s.mu.RLock()
				var modelsToRemove []string
				for mName := range s.runners {
					if _, stillActive := cluster.ModelsEndpoints[mName]; !stillActive {
						modelsToRemove = append(modelsToRemove, mName)
					}
				}
				s.mu.RUnlock()

				for _, mName := range modelsToRemove {
					log.Printf("[Supervisor] [Watch] Model %q has no running instances in GPUStack! Draining and stopping router...", mName)
					s.mu.Lock()
					runner := s.runners[mName]
					delete(s.runners, mName)
					s.mu.Unlock()

					if runner != nil && runner.CurrentProc != nil && runner.CurrentProc.cmd != nil && runner.CurrentProc.cmd.Process != nil {
						_ = runner.CurrentProc.cmd.Process.Kill()
					}
				}
			}
		}
	}
}

// performZeroDowntimeReloadForRunner executes blue-green rolling reload for a specific model runner.
func (s *Supervisor) performZeroDowntimeReloadForRunner(ctx context.Context, runner *ModelRunner, newURLs []string) {
	runner.mu.Lock()
	if runner.Mode == "proxy" {
		runner.ActiveURLs = newURLs
		runner.AllURLs = newURLs
		runner.mu.Unlock()
		log.Printf("[Supervisor] [%s] In proxy mode, updated active workers directly (%d backends)", runner.ModelName, len(newURLs))
		return
	}
	if runner.reloading {
		runner.mu.Unlock()
		log.Printf("[Supervisor] Reload already in progress for [%s], skipping duplicate trigger...", runner.ModelName)
		return
	}
	runner.reloading = true
	runner.mu.Unlock()

	defer func() {
		runner.mu.Lock()
		runner.reloading = false
		runner.mu.Unlock()
	}()

	nextPort, err := getFreePort()
	if err != nil {
		log.Printf("[Supervisor] Error allocating port for [%s] reload: %v", runner.ModelName, err)
		return
	}

	log.Printf("[Supervisor] [Zero-Downtime Reload] [%s] Step 1/4: Launching candidate on internal port %d (New Workers: %d)...",
		runner.ModelName, nextPort, len(newURLs))
	newProc, err := s.doSpawnForModel(ctx, runner.ModelName, "127.0.0.1", nextPort, newURLs)
	if err != nil {
		log.Printf("[Supervisor] Failed to spawn candidate for [%s]: %v", runner.ModelName, err)
		return
	}

	log.Printf("[Supervisor] [Zero-Downtime Reload] [%s] Step 2/4: Probing candidate health on port %d...", runner.ModelName, nextPort)
	if err := s.checkHealth(ctx, nextPort, s.cfg.HealthCheckTimeout); err != nil {
		log.Printf("[Supervisor] Candidate health check failed for [%s] on port %d: %v. Aborting reload!", runner.ModelName, nextPort, err)
		if newProc != nil && newProc.cmd != nil && newProc.cmd.Process != nil {
			_ = newProc.cmd.Process.Kill()
		}
		return
	}

	log.Printf("[Supervisor] [Zero-Downtime Reload] [%s] Step 3/4: Candidate healthy! Atomically switching traffic to port %d...", runner.ModelName, nextPort)
	runner.mu.Lock()
	oldProc := runner.CurrentProc
	runner.CurrentProc = newProc
	runner.ActiveTarget = newProc.targetURL
	runner.ActiveURLs = newURLs
	runner.mu.Unlock()

	s.mu.Lock()
	if s.modelName == runner.ModelName || s.modelName != "" {
		s.activeTarget = newProc.targetURL
		s.activeURLs = newURLs
		s.currentProc = newProc
	}
	s.mu.Unlock()

	if oldProc != nil && oldProc.cmd != nil && oldProc.cmd.Process != nil {
		log.Printf("[Supervisor] [Zero-Downtime Reload] [%s] Step 4/4: Traffic switched! Draining old router (PID %d on port %d) for %v...",
			runner.ModelName, oldProc.cmd.Process.Pid, oldProc.port, s.cfg.DrainTimeout)

		go func(proc *runningProcess, drainDuration time.Duration, mName string) {
			time.Sleep(drainDuration)
			log.Printf("[Supervisor] Drain period completed for [%s] on port %d. Terminating old process.", mName, proc.port)
			if proc.cmd != nil && proc.cmd.Process != nil {
				_ = proc.cmd.Process.Kill()
			}
		}(oldProc, s.cfg.DrainTimeout, runner.ModelName)
	} else {
		log.Printf("[Supervisor] [Zero-Downtime Reload] [%s] Step 4/4: Traffic switched (no previous running process to drain).", runner.ModelName)
	}
}

// Stop cleanly terminates all processes and servers.
func (s *Supervisor) Stop() {
	close(s.stopCh)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.frontServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.frontServer.Shutdown(ctx)
	}

	for mName, runner := range s.runners {
		runner.mu.Lock()
		if runner.CurrentProc != nil && runner.CurrentProc.cmd != nil && runner.CurrentProc.cmd.Process != nil {
			log.Printf("[Supervisor] Terminating router process for [%s] on port %d (PID %d)...",
				mName, runner.CurrentProc.port, runner.CurrentProc.cmd.Process.Pid)
			_ = runner.CurrentProc.cmd.Process.Kill()
		}
		runner.mu.Unlock()
	}
}

// GetTopology satisfies dashboard.TopologyProvider interface for Mode: Run.
func (s *Supervisor) GetTopology(ctx context.Context) (*dashboard.TopologyData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.workerMu.RLock()
	defer s.workerMu.RUnlock()

	var modelsTopo []dashboard.ModelTopology
	totalWorkers := len(s.workerStates)
	healthyWorkers := 0

	for mName, runner := range s.runners {
		runner.mu.RLock()
		var workersTopo []dashboard.WorkerTopology
		modelHealthy := 0

		for _, u := range runner.AllURLs {
			wb := s.workerStates[u]
			isHealthy := true
			state := "CLOSED"
			fails := 0
			lastErr := ""
			lastProbeStr := ""

			if wb != nil {
				isHealthy = wb.Healthy
				if !isHealthy {
					state = "OPEN"
				}
				fails = wb.ConsecutiveFails
				lastErr = wb.LastErr
				if !wb.LastProbe.IsZero() {
					lastProbeStr = wb.LastProbe.Format(time.RFC3339)
				}
			}

			if isHealthy {
				modelHealthy++
			}

			workersTopo = append(workersTopo, dashboard.WorkerTopology{
				URL:                 u,
				ActiveConns:         0,
				Healthy:             isHealthy,
				CircuitState:        state,
				ConsecutiveFailures: fails,
				LastErr:             lastErr,
				LastProbe:           lastProbeStr,
			})
		}
		runner.mu.RUnlock()

		mode := runner.Mode
		if mode == "" {
			mode = "run"
		}
		policy := runner.Policy
		if policy == "" {
			policy = s.cfg.RouterCfg.Policy
		}

		modelsTopo = append(modelsTopo, dashboard.ModelTopology{
			ModelName:    mName,
			Mode:         string(mode),
			Policy:       string(policy),
			WorkerCount:  len(runner.AllURLs),
			HealthyCount: modelHealthy,
			ActiveConns:  atomic.LoadInt64(&runner.activeConns),
			Workers:      workersTopo,
		})
	}

	for _, wb := range s.workerStates {
		if wb.Healthy {
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
		Mode:             "run",
		Policy:           string(s.cfg.RouterCfg.Policy),
		PublicAddr:       fmt.Sprintf("%s:%d", s.cfg.PublicHost, s.cfg.PublicPort),
		ClusterHealth:    clusterHealth,
		TotalModels:      len(s.runners),
		TotalWorkers:     totalWorkers,
		HealthyWorkers:   healthyWorkers,
		TotalActiveConns: atomic.LoadInt64(&s.totalActiveConns),
		Models:           modelsTopo,
		ServerTimeUTC:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// ProbeWorker triggers an active health probe on a target backend worker.
func (s *Supervisor) ProbeWorker(ctx context.Context, workerURL string) (bool, error) {
	var ok bool
	var err error
	if s.probeWorkerFunc != nil {
		ok, err = s.probeWorkerFunc(ctx, workerURL)
	} else {
		ok, err = s.probeWorker(ctx, workerURL)
	}

	trimmed := strings.TrimRight(workerURL, "/")
	s.workerMu.Lock()
	var wb *WorkerBreaker
	for u, state := range s.workerStates {
		if strings.TrimRight(u, "/") == trimmed {
			wb = state
			break
		}
	}
	if wb == nil {
		wb = &WorkerBreaker{URL: workerURL, Healthy: true}
		s.workerStates[workerURL] = wb
	}
	wb.LastProbe = time.Now()
	if ok {
		wb.Healthy = true
		wb.ConsecutiveFails = 0
		wb.LastErr = ""
	} else {
		wb.ConsecutiveFails++
		wb.LastErr = fmt.Sprintf("%v", err)
	}
	s.workerMu.Unlock()

	return ok, err
}

// ResetBreaker manually restores a worker breaker to healthy state.
func (s *Supervisor) ResetBreaker(ctx context.Context, workerURL string) error {
	trimmed := strings.TrimRight(workerURL, "/")
	s.workerMu.Lock()
	for u, wb := range s.workerStates {
		if strings.TrimRight(u, "/") == trimmed {
			wb.Healthy = true
			wb.ConsecutiveFails = 0
			wb.LastErr = ""
			wb.OpenSince = time.Time{}
			break
		}
	}
	s.workerMu.Unlock()
	return nil
}

// GetConfig returns the active configuration snapshot.
func (s *Supervisor) GetConfig(ctx context.Context) (*dashboard.ConfigSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getConfigLocked(), nil
}

// UpdateConfig updates runtime configuration parameters and persists to config.yaml if available.
func (s *Supervisor) UpdateConfig(ctx context.Context, req dashboard.ConfigUpdateRequest) (*dashboard.ConfigSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Policy != nil && *req.Policy != "" {
		p, err := ParsePolicy(*req.Policy)
		if err != nil {
			return nil, fmt.Errorf("invalid policy: %w", err)
		}
		s.cfg.RouterCfg.Policy = p
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
	if req.MaxFailures != nil && *req.MaxFailures > 0 {
		s.cfg.WorkerMaxFailures = *req.MaxFailures
	}
	if req.HealthCheckIntervalSecs != nil && *req.HealthCheckIntervalSecs > 0 {
		s.cfg.WorkerProbeInterval = time.Duration(*req.HealthCheckIntervalSecs) * time.Second
	}
	if req.BalanceAbsThreshold != nil {
		s.cfg.RouterCfg.BalanceAbsThreshold = *req.BalanceAbsThreshold
	}
	if req.BalanceRelThreshold != nil {
		s.cfg.RouterCfg.BalanceRelThreshold = *req.BalanceRelThreshold
	}
	if req.CacheThreshold != nil {
		s.cfg.RouterCfg.CacheThreshold = *req.CacheThreshold
	}
	if req.ExtraArgs != nil {
		s.cfg.RouterCfg.ExtraArgs = req.ExtraArgs
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
func (s *Supervisor) UpdateModelRule(ctx context.Context, req dashboard.ModelRuleUpdateRequest) (*dashboard.ConfigSnapshot, error) {
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

func (s *Supervisor) applyModelRuleLocked(ctx context.Context, rule config.ModelRule) {
	runner, ok := s.runners[rule.ModelName]
	if !ok || runner == nil {
		return
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()

	targetMode := rule.Mode
	if targetMode == "" {
		targetMode = "run"
	}
	targetPolicy := Policy(rule.Policy)
	if targetPolicy == "" {
		targetPolicy = s.cfg.RouterCfg.Policy
	}

	prevMode := runner.Mode
	prevPolicy := runner.Policy
	runner.Mode = targetMode
	runner.Policy = targetPolicy

	// Transition from run -> proxy: Kill child process if exists
	if targetMode == "proxy" && prevMode == "run" {
		if runner.CurrentProc != nil && runner.CurrentProc.cmd != nil && runner.CurrentProc.cmd.Process != nil {
			log.Printf("[Supervisor] Switching [%s] to proxy mode. Terminating child router process...", runner.ModelName)
			_ = runner.CurrentProc.cmd.Process.Kill()
			runner.CurrentProc = nil
			runner.ActiveTarget = nil
		}
	}

	// Transition from proxy -> run: Spawn child process if backends available
	if targetMode == "run" && prevMode != "run" && len(runner.AllURLs) > 0 {
		log.Printf("[Supervisor] Switching [%s] to run mode. Spawning dedicated router process...", runner.ModelName)
		go func(mName string, urls []string) {
			r, err := s.startModelRunner(context.Background(), mName, urls)
			if err != nil {
				log.Printf("[Supervisor] Error starting runner for [%s] in run mode: %v", mName, err)
				return
			}
			runner.mu.Lock()
			runner.CurrentProc = r.CurrentProc
			runner.ActiveTarget = r.ActiveTarget
			runner.mu.Unlock()
		}(runner.ModelName, append([]string(nil), runner.AllURLs...))
	} else if targetMode == "run" && prevMode == "run" && (prevPolicy != targetPolicy || rule.BalanceAbsThreshold != nil || rule.BalanceRelThreshold != nil || rule.CacheThreshold != nil || len(rule.ExtraArgs) > 0) && len(runner.ActiveURLs) > 0 {
		// Policy or tuning parameter changed while in run mode: trigger zero-downtime rolling reload
		log.Printf("[Supervisor] Policy/tuning parameters changed for [%s] in run mode. Triggering zero-downtime rolling reload...", runner.ModelName)
		go s.performZeroDowntimeReloadForRunner(context.Background(), runner, append([]string(nil), runner.ActiveURLs...))
	}
}

func (s *Supervisor) saveConfigToFileLocked() error {
	if s.configFilePath == "" {
		s.configFilePath = "config.yaml"
	}

	var balAbs *int
	if s.cfg.RouterCfg.BalanceAbsThreshold > 0 {
		v := s.cfg.RouterCfg.BalanceAbsThreshold
		balAbs = &v
	}
	var balRel *float64
	if s.cfg.RouterCfg.BalanceRelThreshold > 0 {
		v := s.cfg.RouterCfg.BalanceRelThreshold
		balRel = &v
	}
	var cacheThresh *float64
	if s.cfg.RouterCfg.CacheThreshold > 0 {
		v := s.cfg.RouterCfg.CacheThreshold
		cacheThresh = &v
	}

	cfgToSave := &config.FileConfig{
		Router: config.RouterConfig{
			Mode:                "run",
			Host:                s.cfg.PublicHost,
			Port:                s.cfg.PublicPort,
			WatchInterval:       s.cfg.WatchInterval,
			ZeroDowntime:        &s.cfg.ZeroDowntime,
			DrainTimeout:        s.cfg.DrainTimeout,
			BalanceAbsThreshold: balAbs,
			BalanceRelThreshold: balRel,
			CacheThreshold:      cacheThresh,
			ExtraArgs:           s.cfg.RouterCfg.ExtraArgs,
		},
		Target: config.TargetConfig{
			ModelName: s.modelName,
			Policy:    string(s.cfg.RouterCfg.Policy),
		},
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxFailures:         s.cfg.WorkerMaxFailures,
			HealthCheckInterval: s.cfg.WorkerProbeInterval,
		},
	}

	for _, r := range s.modelRules {
		cfgToSave.Models = append(cfgToSave.Models, r)
	}

	return config.SaveConfig(s.configFilePath, cfgToSave)
}

func (s *Supervisor) getConfigLocked() *dashboard.ConfigSnapshot {
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
	for mName, runner := range s.runners {
		if !existing[mName] {
			runner.mu.RLock()
			mode := runner.Mode
			if mode == "" {
				mode = "run"
			}
			policy := runner.Policy
			if policy == "" {
				policy = s.cfg.RouterCfg.Policy
			}
			runner.mu.RUnlock()

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

	var balAbs *int
	if s.cfg.RouterCfg.BalanceAbsThreshold > 0 {
		v := s.cfg.RouterCfg.BalanceAbsThreshold
		balAbs = &v
	}
	var balRel *float64
	if s.cfg.RouterCfg.BalanceRelThreshold > 0 {
		v := s.cfg.RouterCfg.BalanceRelThreshold
		balRel = &v
	}
	var cacheThresh *float64
	if s.cfg.RouterCfg.CacheThreshold > 0 {
		v := s.cfg.RouterCfg.CacheThreshold
		cacheThresh = &v
	}

	return &dashboard.ConfigSnapshot{
		Mode:                    "run",
		Policy:                  string(s.cfg.RouterCfg.Policy),
		WatchIntervalSecs:       int(s.cfg.WatchInterval.Seconds()),
		ZeroDowntime:            s.cfg.ZeroDowntime,
		DrainTimeoutSecs:        int(s.cfg.DrainTimeout.Seconds()),
		BalanceAbsThreshold:     balAbs,
		BalanceRelThreshold:     balRel,
		CacheThreshold:          cacheThresh,
		ExtraArgs:               s.cfg.RouterCfg.ExtraArgs,
		CircuitBreakerEnabled:   true,
		MaxFailures:             s.cfg.WorkerMaxFailures,
		CooldownSecs:            10,
		MaxRetries:              2,
		HealthCheckIntervalSecs: int(s.cfg.WorkerProbeInterval.Seconds()),
		SuccessThreshold:        2,
		Models:                  models,
		AvailablePolicies:       []string{"consistent_hash", "cache_aware", "rendezvous_hash", "round_robin", "power_of_two", "random"},
		AvailableModes:          []string{"proxy", "run"},
		ConfigFilePath:          s.configFilePath,
	}
}

