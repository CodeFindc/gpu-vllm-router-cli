package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gpu-vllm-router/pkg/router"
	"gpu-vllm-router/pkg/workermetrics"
)

func makeTargets(urls ...string) []*BackendTarget {
	var targets []*BackendTarget
	for _, uStr := range urls {
		u, _ := url.Parse(uStr)
		targets = append(targets, &BackendTarget{
			URL:       u,
			URLString: uStr,
			Healthy:   true,
		})
	}
	return targets
}

func TestRoundRobinBalancer(t *testing.T) {
	targets := makeTargets("http://worker1:8000", "http://worker2:8000", "http://worker3:8000")
	b := NewRoundRobinBalancer(targets)

	req, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/models", nil)

	seen := make(map[string]int)
	for i := 0; i < 9; i++ {
		target, err := b.SelectTarget(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen[target.URLString]++
	}

	for _, u := range targets {
		if seen[u.URLString] != 3 {
			t.Errorf("expected 3 hits for %s, got %d", u.URLString, seen[u.URLString])
		}
	}
}

func TestPowerOfTwoBalancer(t *testing.T) {
	targets := makeTargets("http://busy:8000", "http://idle:8000")
	// Simulate load on busy target
	atomic.StoreInt64(&targets[0].ActiveConns, 10)
	atomic.StoreInt64(&targets[1].ActiveConns, 0)

	b := NewPowerOfTwoBalancer(targets)
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/models", nil)

	// Since there are only 2 targets, P2C always compares both and must pick the idle one
	for i := 0; i < 10; i++ {
		target, err := b.SelectTarget(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if target.URLString != "http://idle:8000" {
			t.Errorf("expected idle target, got %s", target.URLString)
		}
	}
}

func TestConsistentHashBalancer(t *testing.T) {
	targets := makeTargets("http://worker1:8000", "http://worker2:8000", "http://worker3:8000")
	b := NewConsistentHashBalancer(targets, 100)

	reqSessionA, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", nil)
	reqSessionA.Header.Set("X-Session-ID", "session-user-alice")

	reqSessionB, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", nil)
	reqSessionB.Header.Set("X-Session-ID", "session-user-bob")

	targetA1, err := b.SelectTarget(reqSessionA)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// Repeated requests for Alice should route to the exact same worker!
	for i := 0; i < 20; i++ {
		targetA2, err := b.SelectTarget(reqSessionA)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if targetA1.URLString != targetA2.URLString {
			t.Fatalf("consistent hash broken! Got %s then %s for Alice", targetA1.URLString, targetA2.URLString)
		}
	}

	// Repeated requests for Bob should also consistently route to Bob's worker
	targetB1, _ := b.SelectTarget(reqSessionB)
	for i := 0; i < 20; i++ {
		targetB2, _ := b.SelectTarget(reqSessionB)
		if targetB1.URLString != targetB2.URLString {
			t.Fatalf("consistent hash broken for Bob! Got %s then %s", targetB1.URLString, targetB2.URLString)
		}
	}
}

func TestFactoryNewBalancer(t *testing.T) {
	targets := makeTargets("http://worker1:8000")
	if b := NewBalancer(router.PolicyRoundRobin, targets); b == nil {
		t.Error("expected balancer")
	}
	if b := NewBalancer(router.PolicyConsistentHash, targets); b == nil {
		t.Error("expected balancer")
	}
	if b := NewBalancer(router.PolicyPowerOfTwo, targets); b == nil {
		t.Error("expected balancer")
	}
	if b := NewBalancer(router.PolicyCacheAware, targets); b == nil {
		t.Error("expected balancer")
	}
}

func TestCacheAwareBalancer(t *testing.T) {
	targets := makeTargets("http://worker-cache-1:8000", "http://worker-cache-2:8000", "http://worker-cache-3:8000")
	b := NewCacheAwareBalancer(targets, 100)

	// 1. Test chat completions prefix matching consistency
	bodyChatA1 := `{"model": "qwen", "messages": [{"role": "system", "content": "You are a code analyzer"}, {"role": "user", "content": "Fix bug in line 10"}]}`
	reqA1, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", strings.NewReader(bodyChatA1))

	targetA1, err := b.SelectTarget(reqA1)
	if err != nil {
		t.Fatalf("CacheAware SelectTarget failed: %v", err)
	}

	// Repeated requests with the exact same prefix should route to the same worker!
	for i := 0; i < 10; i++ {
		bodyChatA2 := `{"model": "qwen", "messages": [{"role": "system", "content": "You are a code analyzer"}, {"role": "user", "content": "Different user follow up question"}]}`
		reqA2, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", strings.NewReader(bodyChatA2))
		targetA2, err := b.SelectTarget(reqA2)
		if err != nil {
			t.Fatalf("CacheAware SelectTarget iteration %d failed: %v", i, err)
		}
		if targetA1.URLString != targetA2.URLString {
			t.Fatalf("CacheAware routing failed! Shared system prefix should hit same worker %s, got %s", targetA1.URLString, targetA2.URLString)
		}
	}

	// 2. Test completions prompt matching
	bodyPrompt := `{"model": "deepseek", "prompt": "Once upon a time in a faraway galaxy"}`
	reqPrompt, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/completions", strings.NewReader(bodyPrompt))
	targetP1, err := b.SelectTarget(reqPrompt)
	if err != nil {
		t.Fatalf("Prompt routing failed: %v", err)
	}
	reqPrompt2, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/completions", strings.NewReader(bodyPrompt))
	targetP2, err := b.SelectTarget(reqPrompt2)
	if err != nil {
		t.Fatalf("Prompt routing 2 failed: %v", err)
	}
	if targetP1.URLString != targetP2.URLString {
		t.Fatalf("Prompt routing consistency broken: %s vs %s", targetP1.URLString, targetP2.URLString)
	}

	// 3. Test fallback to Session ID when no body
	reqHeader, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/models", nil)
	reqHeader.Header.Set("X-Session-ID", "custom-session-12345")
	targetH1, err := b.SelectTarget(reqHeader)
	if err != nil {
		t.Fatalf("Header fallback routing failed: %v", err)
	}
	targetH2, _ := b.SelectTarget(reqHeader)
	if targetH1.URLString != targetH2.URLString {
		t.Fatalf("Header fallback consistency broken: %s vs %s", targetH1.URLString, targetH2.URLString)
	}
}

func newMockWorkerMetricsServer(running, waiting *int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			fmt.Fprintf(w, "vllm:num_requests_running %d\nvllm:num_requests_waiting %d\n",
				atomic.LoadInt64(running), atomic.LoadInt64(waiting))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
}

func TestLeastWaitingBalancer(t *testing.T) {
	var r1, w1 int64 = 12, 20 // Heavily overloaded worker (running=12, waiting=20)
	var r2, w2 int64 = 0, 0   // Idle
	var r3, w3 int64 = 2, 0   // Slightly busy running, 0 waiting
	var r4, w4 int64 = 0, 0   // Idle

	s1 := newMockWorkerMetricsServer(&r1, &w1)
	defer s1.Close()
	s2 := newMockWorkerMetricsServer(&r2, &w2)
	defer s2.Close()
	s3 := newMockWorkerMetricsServer(&r3, &w3)
	defer s3.Close()
	s4 := newMockWorkerMetricsServer(&r4, &w4)
	defer s4.Close()

	store := workermetrics.NewStore(workermetrics.Config{
		ScrapeInterval: 50 * time.Millisecond,
		ScrapeTimeout:  time.Second,
	})
	urls := []string{s1.URL, s2.URL, s3.URL, s4.URL}
	store.SetWorkerURLs(urls)
	store.ScrapeOnce(context.Background())

	targets := makeTargets(urls...)
	for _, tgt := range targets {
		tgt.MetricsStore = store
		tgt.SnapshotActiveAtScrape()
	}

	b := NewLeastWaitingBalancer(targets)
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", nil)

	// 1. First request must pick one of the idle workers (s2 or s4, both waiting=0 running=0), never s1 or s3
	chosen1, err := b.SelectTarget(req)
	if err != nil {
		t.Fatalf("SelectTarget error: %v", err)
	}
	if chosen1.URLString != s2.URL && chosen1.URLString != s4.URL {
		t.Errorf("expected idle worker s2 or s4, got %s", chosen1.URLString)
	}

	// 2. Simulate burst between scrapes: RecordRequestStart increments ActiveConns -> delta prevents piling onto same node
	b.RecordRequestStart(chosen1)
	chosen2, err := b.SelectTarget(req)
	if err != nil {
		t.Fatalf("SelectTarget 2 error: %v", err)
	}
	if chosen2.URLString == chosen1.URLString {
		t.Errorf("expected burst anti-herding to pick the other idle worker, got %s again", chosen2.URLString)
	}
	if chosen2.URLString != s2.URL && chosen2.URLString != s4.URL {
		t.Errorf("expected the other idle worker, got %s", chosen2.URLString)
	}

	// 3. Occupy s2 and s4 locally; next request should go to s3 (waiting=0, running=2), NOT s1 (waiting=20)
	b.RecordRequestStart(chosen2)
	for i := 0; i < 4; i++ {
		// Make s2 and s4 have effective waiting=3 (0 + 3 delta) so s3 (waiting=0, running=2) is strictly better
		b.RecordRequestStart(targets[1])
		b.RecordRequestStart(targets[3])
	}
	chosen3, err := b.SelectTarget(req)
	if err != nil {
		t.Fatalf("SelectTarget 3 error: %v", err)
	}
	if chosen3.URLString != s3.URL {
		t.Errorf("expected s3 (waiting=0, running=2), got %s", chosen3.URLString)
	}
	if chosen3.URLString == s1.URL {
		t.Errorf("must never route to overloaded s1 (waiting=20)")
	}
}

func TestLeastWaitingBalancer_FallbackWithoutMetrics(t *testing.T) {
	targets := makeTargets("http://w1:8000", "http://w2:8000")
	atomic.StoreInt64(&targets[0].ActiveConns, 5)
	atomic.StoreInt64(&targets[1].ActiveConns, 1)

	b := NewLeastWaitingBalancer(targets)
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/models", nil)

	chosen, err := b.SelectTarget(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chosen.URLString != "http://w2:8000" {
		t.Errorf("expected fallback to lower ActiveConns w2, got %s", chosen.URLString)
	}
}

func TestConsistentHashAndCacheAwareWaitingDiversion(t *testing.T) {
	var r1, w1 int64 = 12, 20
	var r2, w2 int64 = 0, 0
	var r3, w3 int64 = 5, 20
	var r4, w4 int64 = 8, 20

	s1 := newMockWorkerMetricsServer(&r1, &w1)
	defer s1.Close()
	s2 := newMockWorkerMetricsServer(&r2, &w2)
	defer s2.Close()
	s3 := newMockWorkerMetricsServer(&r3, &w3)
	defer s3.Close()
	s4 := newMockWorkerMetricsServer(&r4, &w4)
	defer s4.Close()

	store := workermetrics.NewStore(workermetrics.Config{ScrapeInterval: 50 * time.Millisecond})
	urls := []string{s1.URL, s2.URL, s3.URL, s4.URL}
	store.SetWorkerURLs(urls)
	store.ScrapeOnce(context.Background())

	targets := makeTargets(urls...)
	for _, tgt := range targets {
		tgt.MetricsStore = store
	}

	// ConsistentHash with threshold=4: any overloaded primary (waiting=20 >= 4) must divert to idle s2 (waiting=0)
	ch := NewConsistentHashBalancer(targets, 100)
	ch.SetWaitingThreshold(4)

	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", nil)
	req.Header.Set("X-Session-ID", "heavy-session-1")
	chosen, err := ch.SelectTarget(req)
	if err != nil {
		t.Fatalf("ConsistentHash SelectTarget failed: %v", err)
	}
	if chosen.URLString != s2.URL {
		t.Errorf("expected diversion to idle s2, got %s", chosen.URLString)
	}

	// CacheAware with threshold=4: overloaded primary must divert to idle s2
	ca := NewCacheAwareBalancer(targets, 100)
	ca.SetWaitingThreshold(4)
	body := `{"model": "qwen", "messages": [{"role": "user", "content": "shared prefix overload test"}]}`
	reqCA, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", strings.NewReader(body))
	chosenCA, err := ca.SelectTarget(reqCA)
	if err != nil {
		t.Fatalf("CacheAware SelectTarget failed: %v", err)
	}
	if chosenCA.URLString != s2.URL {
		t.Errorf("expected CacheAware diversion to idle s2, got %s", chosenCA.URLString)
	}

	// When all nodes have waiting=20 (none less loaded), preserve primary affinity (no needless jitter)
	atomic.StoreInt64(&w2, 20)
	store.ScrapeOnce(context.Background())
	ca.SetWaitingThreshold(0) // find primary without diversion
	primaryReq, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", strings.NewReader(body))
	expectedPrimary, _ := ca.SelectTarget(primaryReq)

	ca.SetWaitingThreshold(4)
	reqAllBusy, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/chat/completions", strings.NewReader(body))
	chosenAllBusy, _ := ca.SelectTarget(reqAllBusy)
	if chosenAllBusy.URLString != expectedPrimary.URLString {
		t.Errorf("when all workers equally busy (waiting=20), expected affinity to %s, got %s", expectedPrimary.URLString, chosenAllBusy.URLString)
	}
}
