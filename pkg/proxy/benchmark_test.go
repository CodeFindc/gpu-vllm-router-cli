package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gpu-vllm-router/pkg/router"
)

// BenchmarkCircuitBreakerCanExecuteParallel measures concurrent CanExecute throughput across goroutines.
func BenchmarkCircuitBreakerCanExecuteParallel(b *testing.B) {
	cb := NewCircuitBreaker("http://127.0.0.1:8000", DefaultCircuitBreakerConfig())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !cb.CanExecute() {
				b.Fatal("expected CanExecute to return true in closed state")
			}
		}
	})
}

// BenchmarkGetOrReadRequestBodyCached measures the performance of 4 passes of cached request body retrieval.
func BenchmarkGetOrReadRequestBodyCached(b *testing.B) {
	largePrompt := strings.Repeat("User: Explain quantum computing in detail. Assistant: Certainly! ", 200)
	payload := []byte(`{"model":"qwen2.5","prompt":"` + largePrompt + `"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "/v1/completions", bytes.NewReader(payload))
		for p := 0; p < 4; p++ {
			bBytes := getOrReadRequestBody(req)
			if len(bBytes) == 0 {
				b.Fatal("expected body")
			}
		}
	}
}

// BenchmarkGetOrReadRequestBodyUncached measures 4 repeated uncached io.ReadAll passes for comparison.
func BenchmarkGetOrReadRequestBodyUncached(b *testing.B) {
	largePrompt := strings.Repeat("User: Explain quantum computing in detail. Assistant: Certainly! ", 200)
	payload := []byte(`{"model":"qwen2.5","prompt":"` + largePrompt + `"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "/v1/completions", bytes.NewReader(payload))
		for p := 0; p < 4; p++ {
			bodyBytes, err := io.ReadAll(req.Body)
			if err != nil {
				b.Fatal(err)
			}
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
	}
}

// BenchmarkOptimizedTransportPool measures concurrent request throughput with connection pooling.
func BenchmarkOptimizedTransportPool(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := &http.Client{
		Transport: router.NewOptimizedTransport(),
		Timeout:   5 * time.Second,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err != nil {
				b.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	})
}
