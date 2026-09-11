package proxy

import (
	"net/http"
	"net/url"
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
}
