package workermetrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParsePrometheusMetrics_VLLM(t *testing.T) {
	body := `
# HELP vllm:num_requests_running Number of requests currently running on GPU.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="DeepSeek-V4 {test}",engine="0"} 12.0
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="DeepSeek-V4 {test}",engine="0"} 20.0 1730000000000
# HELP vllm:gpu_cache_usage_perc GPU KV-cache usage.
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{model_name="DeepSeek-V4 {test}",engine="0"} 0.75
`
	running, waiting, kv, found, err := ParsePrometheusMetrics(strings.NewReader(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if running != 12 {
		t.Errorf("expected running=12, got %v", running)
	}
	if waiting != 20 {
		t.Errorf("expected waiting=20, got %v", waiting)
	}
	if kv != 0.75 {
		t.Errorf("expected kv=0.75, got %v", kv)
	}
}

func TestParsePrometheusMetrics_MultiEngineSumAndSGLang(t *testing.T) {
	vllmMulti := `
vllm_num_requests_running{engine="0"} 4
vllm_num_requests_running{engine="1"} 6
vllm_num_requests_waiting{engine="0"} 3
vllm_num_requests_waiting{engine="1"} 5
`
	running, waiting, _, found, err := ParsePrometheusMetrics(strings.NewReader(vllmMulti))
	if err != nil || !found {
		t.Fatalf("unexpected parse failure: found=%v err=%v", found, err)
	}
	if running != 10 || waiting != 8 {
		t.Errorf("expected running=10 waiting=8, got running=%v waiting=%v", running, waiting)
	}

	sglang := `
sglang:num_running_reqs{model="qwen"} 7
sglang:num_queue_reqs{model="qwen"} 14
sglang:token_usage{model="qwen"} 0.42
`
	running, waiting, kv, found, err := ParsePrometheusMetrics(strings.NewReader(sglang))
	if err != nil || !found {
		t.Fatalf("unexpected sglang parse failure: found=%v err=%v", found, err)
	}
	if running != 7 || waiting != 14 || kv != 0.42 {
		t.Errorf("unexpected sglang metrics: running=%v waiting=%v kv=%v", running, waiting, kv)
	}
}

func TestStoreScrapeAndStale(t *testing.T) {
	var waitingVal int64 = 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		v := atomic.LoadInt64(&waitingVal)
		_, _ = w.Write([]byte("vllm:num_requests_running 3\nvllm:num_requests_waiting " + strconvItoa(int(v)) + "\n"))
	}))
	defer srv.Close()

	var callbackCalled int64
	store := NewStore(Config{
		ScrapeInterval: 50 * time.Millisecond,
		ScrapeTimeout:  500 * time.Millisecond,
		StaleTimeout:   150 * time.Millisecond,
	})
	store.SetOnScrape(func(url string, m WorkerMetrics) {
		atomic.AddInt64(&callbackCalled, 1)
	})
	store.SetWorkerURLs([]string{srv.URL + "/", "http://127.0.0.1:1"})

	store.ScrapeOnce(context.Background())

	m, ok := store.Get(srv.URL)
	if !ok {
		t.Fatal("expected valid scraped metrics")
	}
	if m.Running != 3 || m.Waiting != 5 {
		t.Errorf("expected running=3 waiting=5, got running=%v waiting=%v", m.Running, m.Waiting)
	}
	if atomic.LoadInt64(&callbackCalled) != 1 {
		t.Errorf("expected callbackCalled=1, got %d", callbackCalled)
	}

	if _, ok := store.Get("http://127.0.0.1:1"); ok {
		t.Error("expected unreachable worker to return ok=false")
	}

	// Wait past StaleTimeout
	time.Sleep(180 * time.Millisecond)
	if _, ok := store.Get(srv.URL); ok {
		t.Error("expected metrics to expire after StaleTimeout")
	}
}

func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
