package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDynamicScaling_9To17_ModelLoadingThenReady tests the exact user scenario:
// 1. Initial state: 9 workers running and ready (health: 9, active: 9).
// 2. Scale-up: 8 new workers discovered while loading model weights (/health 200, /v1/models 503 or empty data).
//    Readiness probe must keep new workers Healthy=false, runner ActiveURLs must remain 9.
// 3. Ready: 8 new workers finish loading weights (/v1/models returns 200 with model data).
//    Readiness probe marks them Healthy=true, zero-downtime reload triggers, active atomically switches to 17.
func TestDynamicScaling_9To17_ModelLoadingThenReady(t *testing.T) {
	// Create 9 initial workers
	initialWorkers := make([]*httptest.Server, 9)
	initialURLs := make([]string, 9)
	for i := 0; i < 9; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/v1/models" {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"object": "list",
					"data": []map[string]interface{}{
						{"id": "DeepSeek-V4-Flash", "object": "model"},
					},
				})
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()
		initialWorkers[i] = srv
		initialURLs[i] = srv.URL
	}
	sort.Strings(initialURLs)

	// Create 8 new workers that start in "loading" state
	newWorkers := make([]*httptest.Server, 8)
	newURLs := make([]string, 8)
	var newWorkersLoaded int32 // 0 = loading, 1 = loaded

	for i := 0; i < 8; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/v1/models" {
				if atomic.LoadInt32(&newWorkersLoaded) == 0 {
					// Simulating "当前加载模型实例" -> 503 Service Unavailable or empty data
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]interface{}{
						"error": "model weights currently loading",
					})
					return
				}
				// Model loaded
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"object": "list",
					"data": []map[string]interface{}{
						{"id": "DeepSeek-V4-Flash", "object": "model"},
					},
				})
				return
			}
			// /health returns 200 OK even while loading (common liveness behavior)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer srv.Close()
		newWorkers[i] = srv
		newURLs[i] = srv.URL
	}
	sort.Strings(newURLs)

	var spawnMu sync.Mutex
	var lastSpawnedURLs []string
	var spawnCount int

	spawnFunc := func(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
		spawnMu.Lock()
		spawnCount++
		lastSpawnedURLs = append([]string{}, workerURLs...)
		spawnMu.Unlock()

		targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		return &runningProcess{
			port:      port,
			targetURL: targetURL,
		}, nil
	}

	sup := &Supervisor{
		modelName: "DeepSeek-V4-Flash",
		cfg: SupervisorConfig{
			WorkerMaxFailures:   2,
			WorkerProbeInterval: 50 * time.Millisecond,
			HealthCheckTimeout:  100 * time.Millisecond,
			DrainTimeout:        50 * time.Millisecond,
		},
		runners:          make(map[string]*ModelRunner),
		workerStates:     make(map[string]*WorkerBreaker),
		stopCh:           make(chan struct{}),
		spawnProcessFunc:  spawnFunc,
		waitForHealthFunc: func(ctx context.Context, port int, timeout time.Duration) error { return nil },
	}

	initialTarget, _ := url.Parse("http://127.0.0.1:18001")
	runner := &ModelRunner{
		ModelName:    "DeepSeek-V4-Flash",
		ActiveTarget: initialTarget,
		ActiveURLs:   initialURLs,
		AllURLs:      initialURLs,
		CurrentProc: &runningProcess{
			port:      18001,
			targetURL: initialTarget,
		},
	}
	sup.runners["DeepSeek-V4-Flash"] = runner
	sup.initWorkers(initialURLs)

	// Step 1: Verify Initial 9 Workers
	ctx := context.Background()
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	healthyCount := 0
	for _, u := range initialURLs {
		if wb := sup.workerStates[u]; wb != nil && wb.Healthy {
			healthyCount++
		}
	}
	sup.workerMu.RUnlock()

	if healthyCount != 9 {
		t.Fatalf("expected 9 healthy initial workers, got %d", healthyCount)
	}
	if len(runner.ActiveURLs) != 9 {
		t.Fatalf("expected 9 active URLs on runner, got %d", len(runner.ActiveURLs))
	}

	// Step 2: GPUStack scales to 17 workers, but 8 are "当前加载模型实例"
	all17URLs := append([]string{}, initialURLs...)
	all17URLs = append(all17URLs, newURLs...)
	sort.Strings(all17URLs)

	runner.mu.Lock()
	runner.AllURLs = all17URLs
	runner.mu.Unlock()

	sup.syncWorkers(all17URLs)

	// Probe while 8 new workers are still loading model
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	totalHealthy := 0
	for _, u := range all17URLs {
		if wb := sup.workerStates[u]; wb != nil && wb.Healthy {
			totalHealthy++
		}
	}
	sup.workerMu.RUnlock()

	// New 8 workers MUST NOT be marked healthy! Healthy count must remain 9!
	if totalHealthy != 9 {
		t.Fatalf("expected healthy count to remain 9 while 8 workers are loading, but got %d", totalHealthy)
	}

	runner.mu.RLock()
	activeCount := len(runner.ActiveURLs)
	runner.mu.RUnlock()

	if activeCount != 9 {
		t.Fatalf("expected active count to remain 9 while 8 workers are loading, but got %d", activeCount)
	}

	spawnMu.Lock()
	spawnsWhileLoading := spawnCount
	spawnMu.Unlock()
	if spawnsWhileLoading != 0 {
		t.Fatalf("expected 0 candidate router spawns while new workers are loading, got %d", spawnsWhileLoading)
	}

	// Step 3: 8 new workers finish loading model weights!
	atomic.StoreInt32(&newWorkersLoaded, 1)

	// Probe again -> readiness probe should now succeed for all 17 workers
	sup.probeRunnerWorkers(ctx, runner)

	// Allow goroutine reload to settle
	time.Sleep(100 * time.Millisecond)

	sup.workerMu.RLock()
	totalHealthyAfter := 0
	for _, u := range all17URLs {
		if wb := sup.workerStates[u]; wb != nil && wb.Healthy {
			totalHealthyAfter++
		}
	}
	sup.workerMu.RUnlock()

	if totalHealthyAfter != 17 {
		t.Fatalf("expected all 17 workers healthy after model loading finished, got %d", totalHealthyAfter)
	}

	runner.mu.RLock()
	activeAfter := len(runner.ActiveURLs)
	activeURLsAfter := append([]string{}, runner.ActiveURLs...)
	runner.mu.RUnlock()

	if activeAfter != 17 {
		t.Fatalf("expected active URLs to update to 17 after model ready, got %d", activeAfter)
	}
	sort.Strings(activeURLsAfter)
	if !reflect.DeepEqual(activeURLsAfter, all17URLs) {
		t.Fatalf("expected active URLs %v, got %v", all17URLs, activeURLsAfter)
	}

	spawnMu.Lock()
	finalSpawns := spawnCount
	spawnedURLs := append([]string{}, lastSpawnedURLs...)
	spawnMu.Unlock()

	if finalSpawns != 1 {
		t.Fatalf("expected exactly 1 candidate router spawned for rolling reload, got %d", finalSpawns)
	}
	sort.Strings(spawnedURLs)
	if !reflect.DeepEqual(spawnedURLs, all17URLs) {
		t.Fatalf("expected candidate spawned with 17 URLs, got %v", spawnedURLs)
	}
}

// TestFastProbeContextIndependence verifies that fastProbeRunner (which has a 5-second context)
// does NOT terminate the candidate process spawned by rolling reload.
func TestFastProbeContextIndependence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/models" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"object": "list",
				"data":   []map[string]interface{}{{"id": "m1"}},
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	initialWorker := srv.URL
	initialTarget, _ := url.Parse("http://127.0.0.1:18001")

	var spawnContextCancelled int32
	spawnFunc := func(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
		go func() {
			<-ctx.Done()
			atomic.StoreInt32(&spawnContextCancelled, 1)
		}()
		targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		return &runningProcess{
			port:      port,
			targetURL: targetURL,
		}, nil
	}

	sup := &Supervisor{
		modelName: "m1",
		cfg: SupervisorConfig{
			WorkerMaxFailures:   2,
			WorkerProbeInterval: 50 * time.Millisecond,
			HealthCheckTimeout:  100 * time.Millisecond,
			DrainTimeout:        50 * time.Millisecond,
		},
		runners:          make(map[string]*ModelRunner),
		workerStates:     make(map[string]*WorkerBreaker),
		stopCh:           make(chan struct{}),
		spawnProcessFunc:  spawnFunc,
		waitForHealthFunc: func(ctx context.Context, port int, timeout time.Duration) error { return nil },
	}

	runner := &ModelRunner{
		ModelName:    "m1",
		ActiveTarget: initialTarget,
		ActiveURLs:   []string{"http://old-worker:8000"},
		AllURLs:      []string{initialWorker},
		CurrentProc: &runningProcess{
			port:      18001,
			targetURL: initialTarget,
		},
	}
	sup.runners["m1"] = runner
	sup.initWorkers([]string{initialWorker})

	// Run fastProbeRunner which sets 5s context with defer cancel()
	sup.fastProbeRunner(runner)

	time.Sleep(100 * time.Millisecond)

	// Verify that the candidate process context was NOT cancelled by fastProbeRunner context
	if atomic.LoadInt32(&spawnContextCancelled) == 1 {
		t.Fatalf("candidate process context was prematurely cancelled by fastProbeRunner context!")
	}

	runner.mu.RLock()
	activeCount := len(runner.ActiveURLs)
	runner.mu.RUnlock()

	if activeCount != 1 || runner.ActiveURLs[0] != initialWorker {
		t.Fatalf("expected runner ActiveURLs updated to %s, got %v", initialWorker, runner.ActiveURLs)
	}
}
