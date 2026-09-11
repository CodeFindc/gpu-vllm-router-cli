package proxy

import (
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"gpu-vllm-router/pkg/router"
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
