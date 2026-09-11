package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSupervisorWorkerProbeAndCircuitBreaker(t *testing.T) {
	workerA := "http://10.0.0.1:8000"
	workerB := "http://10.0.0.2:8000"

	var workerBHealthy int32 = 1 // 1 = healthy, 0 = broken

	probeFunc := func(ctx context.Context, rawURL string) (bool, error) {
		if rawURL == workerA {
			return true, nil
		}
		if rawURL == workerB {
			if atomic.LoadInt32(&workerBHealthy) == 1 {
				return true, nil
			}
			return false, fmt.Errorf("connection refused to %s", rawURL)
		}
		return false, fmt.Errorf("unknown worker %s", rawURL)
	}

	spawnCount := 0
	var lastSpawnedURLs []string
	var spawnMu sync.Mutex

	// Mock process spawner that simulates launching a candidate router process
	spawnFunc := func(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
		spawnMu.Lock()
		spawnCount++
		lastSpawnedURLs = append([]string{}, workerURLs...)
		spawnMu.Unlock()

		targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		return &runningProcess{
			cmd:       nil,
			port:      port,
			targetURL: targetURL,
		}, nil
	}

	sup := &Supervisor{
		modelName: "test-model",
		cfg: SupervisorConfig{
			WorkerMaxFailures:   2,
			WorkerProbeInterval: 100 * time.Millisecond,
			HealthCheckTimeout:  100 * time.Millisecond,
			DrainTimeout:        100 * time.Millisecond,
		},
		runners:          make(map[string]*ModelRunner),
		workerStates:     make(map[string]*WorkerBreaker),
		stopCh:           make(chan struct{}),
		spawnProcessFunc:  spawnFunc,
		probeWorkerFunc:   probeFunc,
		waitForHealthFunc: func(ctx context.Context, port int, timeout time.Duration) error { return nil },
	}

	initialURLs := []string{workerA, workerB}
	initialTarget, _ := url.Parse("http://127.0.0.1:18001")
	runner := &ModelRunner{
		ModelName:    "test-model",
		ActiveTarget: initialTarget,
		ActiveURLs:   initialURLs,
		AllURLs:      initialURLs,
		CurrentProc: &runningProcess{
			port:      18001,
			targetURL: initialTarget,
		},
	}
	sup.runners["test-model"] = runner
	sup.initWorkers(initialURLs)

	ctx := context.Background()

	// 1. Initial probe - both healthy
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	wbA := sup.workerStates[workerA]
	wbB := sup.workerStates[workerB]
	sup.workerMu.RUnlock()

	if wbA == nil || !wbA.Healthy || wbA.ConsecutiveFails != 0 {
		t.Fatalf("expected workerA healthy with 0 fails, got %+v", wbA)
	}
	if wbB == nil || !wbB.Healthy || wbB.ConsecutiveFails != 0 {
		t.Fatalf("expected workerB healthy with 0 fails, got %+v", wbB)
	}

	// 2. WorkerB fails once
	atomic.StoreInt32(&workerBHealthy, 0)
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	wbB = sup.workerStates[workerB]
	sup.workerMu.RUnlock()

	if !wbB.Healthy {
		t.Errorf("workerB should still be healthy after 1 failure (maxFailures=2)")
	}
	if wbB.ConsecutiveFails != 1 {
		t.Errorf("expected consecutive fails = 1, got %d", wbB.ConsecutiveFails)
	}

	// 3. WorkerB fails second time -> Trips Circuit Breaker!
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	wbB = sup.workerStates[workerB]
	sup.workerMu.RUnlock()

	if wbB.Healthy {
		t.Errorf("expected workerB to be unhealthy (circuit breaker open), got healthy")
	}
	if wbB.ConsecutiveFails != 2 {
		t.Errorf("expected consecutive fails = 2, got %d", wbB.ConsecutiveFails)
	}
	if wbB.OpenSince.IsZero() {
		t.Errorf("expected OpenSince to be set on breaker trip")
	}

	// Wait briefly for asynchronous performZeroDowntimeReloadForRunner to complete
	time.Sleep(200 * time.Millisecond)

	spawnMu.Lock()
	count := spawnCount
	spawnedURLs := lastSpawnedURLs
	spawnMu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 reload candidate spawned, got %d", count)
	}
	expectedURLs := []string{workerA}
	if !reflect.DeepEqual(spawnedURLs, expectedURLs) {
		t.Errorf("expected candidate spawned with %v, got %v", expectedURLs, spawnedURLs)
	}

	runner.mu.RLock()
	activeURLs := runner.ActiveURLs
	runner.mu.RUnlock()
	if !reflect.DeepEqual(activeURLs, expectedURLs) {
		t.Errorf("expected runner ActiveURLs updated to %v, got %v", expectedURLs, activeURLs)
	}

	// 4. Verify /admin/supervisor status response
	rwStatus := httptest.NewRecorder()
	reqStatus, _ := http.NewRequest(http.MethodGet, "/admin/supervisor", nil)
	sup.handleSupervisorStatus(rwStatus, reqStatus)
	if rwStatus.Code != http.StatusOK {
		t.Fatalf("expected /admin/supervisor 200, got %d", rwStatus.Code)
	}
	bodyStr := rwStatus.Body.String()
	if !strings.Contains(bodyStr, `"healthy":false`) {
		t.Errorf("expected status JSON to contain healthy:false for broken worker, got: %s", bodyStr)
	}

	// 5. Verify /metrics output
	rwMetrics := httptest.NewRecorder()
	reqMetrics, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
	sup.handleMetrics(rwMetrics, reqMetrics)
	if rwMetrics.Code != http.StatusOK {
		t.Fatalf("expected /metrics 200, got %d", rwMetrics.Code)
	}
	metricsStr := rwMetrics.Body.String()
	if !strings.Contains(metricsStr, fmt.Sprintf("gpu_router_worker_health{worker=%q} 0", workerB)) {
		t.Errorf("expected workerB health metric 0, got:\n%s", metricsStr)
	}
	if !strings.Contains(metricsStr, fmt.Sprintf("gpu_router_worker_health{worker=%q} 1", workerA)) {
		t.Errorf("expected workerA health metric 1, got:\n%s", metricsStr)
	}

	// 6. Test Recovery: WorkerB recovers
	atomic.StoreInt32(&workerBHealthy, 1)
	sup.probeRunnerWorkers(ctx, runner)

	sup.workerMu.RLock()
	wbB = sup.workerStates[workerB]
	sup.workerMu.RUnlock()

	if !wbB.Healthy {
		t.Errorf("expected workerB to recover to healthy, got unhealthy")
	}
	if wbB.ConsecutiveFails != 0 {
		t.Errorf("expected consecutive fails reset to 0, got %d", wbB.ConsecutiveFails)
	}

	// Wait for reload
	time.Sleep(200 * time.Millisecond)

	spawnMu.Lock()
	count2 := spawnCount
	spawnedURLs2 := lastSpawnedURLs
	spawnMu.Unlock()

	if count2 != 2 {
		t.Errorf("expected 2 total reload candidates spawned after recovery, got %d", count2)
	}
	bothURLs := []string{workerA, workerB}
	if !reflect.DeepEqual(spawnedURLs2, bothURLs) {
		t.Errorf("expected candidate spawned with %v after recovery, got %v", bothURLs, spawnedURLs2)
	}
}

func TestSupervisorProcessGuardCrashRespawn(t *testing.T) {
	spawnCount := 0
	var spawnMu sync.Mutex

	spawnFunc := func(ctx context.Context, host string, port int, workerURLs []string) (*runningProcess, error) {
		spawnMu.Lock()
		spawnCount++
		spawnMu.Unlock()

		targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		return &runningProcess{
			cmd:       nil,
			port:      port,
			targetURL: targetURL,
		}, nil
	}

	sup := &Supervisor{
		modelName: "crash-test-model",
		cfg: SupervisorConfig{
			HealthCheckTimeout: 100 * time.Millisecond,
			DrainTimeout:       100 * time.Millisecond,
		},
		runners:          make(map[string]*ModelRunner),
		workerStates:     make(map[string]*WorkerBreaker),
		stopCh:           make(chan struct{}),
		spawnProcessFunc: spawnFunc,
		waitForHealthFunc: func(ctx context.Context, port int, timeout time.Duration) error { return nil },
	}

	initialTarget, _ := url.Parse("http://127.0.0.1:18001")
	initialProc := &runningProcess{
		port:      18001,
		targetURL: initialTarget,
	}

	runner := &ModelRunner{
		ModelName:    "crash-test-model",
		ActiveTarget: initialTarget,
		ActiveURLs:   []string{"http://10.0.0.1:8000"},
		AllURLs:      []string{"http://10.0.0.1:8000"},
		CurrentProc:  initialProc,
	}
	sup.runners["crash-test-model"] = runner

	// Simulate unexpected crash of the active router process
	sup.handleProcessExit(initialProc, fmt.Errorf("signal: killed (OOM)"))

	time.Sleep(200 * time.Millisecond)

	spawnMu.Lock()
	count := spawnCount
	spawnMu.Unlock()

	if count != 1 {
		t.Fatalf("expected emergency respawn triggered (count = 1), got %d", count)
	}

	runner.mu.RLock()
	newProc := runner.CurrentProc
	runner.mu.RUnlock()

	if newProc == initialProc {
		t.Errorf("expected runner CurrentProc to be updated to new process, but still old process")
	}
}

func TestSupervisor_WorkerPortChangeDynamicReload(t *testing.T) {
	oldWorker := "http://10.0.0.1:8000"
	newWorker := "http://10.0.0.1:8001"

	var spawnCount int
	var lastSpawnedURLs []string
	var spawnMu sync.Mutex

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
		modelName: "test-model",
		cfg: SupervisorConfig{
			WorkerMaxFailures:   2,
			WorkerProbeInterval: 50 * time.Millisecond,
			HealthCheckTimeout:  50 * time.Millisecond,
			DrainTimeout:        50 * time.Millisecond,
		},
		runners:          make(map[string]*ModelRunner),
		workerStates:     make(map[string]*WorkerBreaker),
		stopCh:           make(chan struct{}),
		spawnProcessFunc:  spawnFunc,
		waitForHealthFunc: func(ctx context.Context, port int, timeout time.Duration) error { return nil },
	}

	initialURLs := []string{oldWorker}
	initialTarget, _ := url.Parse("http://127.0.0.1:18001")
	runner := &ModelRunner{
		ModelName:    "test-model",
		ActiveTarget: initialTarget,
		ActiveURLs:   initialURLs,
		AllURLs:      initialURLs,
		CurrentProc: &runningProcess{
			port:      18001,
			targetURL: initialTarget,
		},
	}
	sup.runners["test-model"] = runner
	sup.syncWorkers(initialURLs)

	// Verify old worker is tracked
	sup.workerMu.RLock()
	if _, exists := sup.workerStates[oldWorker]; !exists {
		t.Fatalf("expected oldWorker to be in workerStates")
	}
	sup.workerMu.RUnlock()

	// Simulate worker restart with port change: oldWorker is down, newWorker is up
	// GPUStack discovery finds newWorker
	newURLs := []string{newWorker}
	runner.mu.Lock()
	runner.AllURLs = newURLs
	runner.mu.Unlock()

	// syncWorkers should register newWorker and PRUNE oldWorker
	sup.syncWorkers(newURLs)

	sup.workerMu.RLock()
	if _, exists := sup.workerStates[oldWorker]; exists {
		t.Errorf("expected oldWorker %s to be pruned from workerStates after port change", oldWorker)
	}
	wbNew, exists := sup.workerStates[newWorker]
	if !exists || !wbNew.Healthy {
		t.Errorf("expected newWorker %s to be registered and healthy in workerStates", newWorker)
	}
	sup.workerMu.RUnlock()

	// Execute rolling reload to newWorker
	ctx := context.Background()
	sup.performZeroDowntimeReloadForRunner(ctx, runner, newURLs)

	spawnMu.Lock()
	count := spawnCount
	spawnedURLs := lastSpawnedURLs
	spawnMu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 candidate spawned for rolling reload, got %d", count)
	}
	if !reflect.DeepEqual(spawnedURLs, []string{newWorker}) {
		t.Errorf("expected candidate spawned with newWorker %v, got %v", []string{newWorker}, spawnedURLs)
	}

	runner.mu.RLock()
	activeURLs := runner.ActiveURLs
	runner.mu.RUnlock()

	if !reflect.DeepEqual(activeURLs, []string{newWorker}) {
		t.Errorf("expected runner ActiveURLs updated to %v, got %v", []string{newWorker}, activeURLs)
	}
}
