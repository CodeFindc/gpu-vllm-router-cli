package workermetrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultScrapeInterval = 1 * time.Second
	defaultScrapeTimeout  = 1500 * time.Millisecond
	defaultMetricsPath    = "/metrics"
	maxMetricsBodyBytes   = 2 * 1024 * 1024
	maxConcurrentScrapes  = 16
)

// WorkerMetrics holds the latest Prometheus queue and execution metrics scraped from a worker instance.
type WorkerMetrics struct {
	URL          string    `json:"url"`
	Running      float64   `json:"running"`
	Waiting      float64   `json:"waiting"`
	KVCacheUsage float64   `json:"kv_cache_usage"`
	ScrapedAt    time.Time `json:"scraped_at"`
	Available    bool      `json:"available"`
	LastErr      string    `json:"last_error,omitempty"`
}

// Config configures the worker Prometheus metrics scraper store.
type Config struct {
	ScrapeInterval time.Duration
	ScrapeTimeout  time.Duration
	MetricsPath    string
	StaleTimeout   time.Duration
}

func (c Config) withDefaults() Config {
	if c.ScrapeInterval <= 0 {
		c.ScrapeInterval = defaultScrapeInterval
	}
	if c.ScrapeTimeout <= 0 {
		c.ScrapeTimeout = defaultScrapeTimeout
	}
	if c.MetricsPath == "" {
		c.MetricsPath = defaultMetricsPath
	}
	if !strings.HasPrefix(c.MetricsPath, "/") {
		c.MetricsPath = "/" + c.MetricsPath
	}
	if c.StaleTimeout <= 0 {
		c.StaleTimeout = 5 * c.ScrapeInterval
		if c.StaleTimeout < 5*time.Second {
			c.StaleTimeout = 5 * time.Second
		}
	}
	return c
}

// Store periodically scrapes /metrics from registered backend workers and caches their latest load state.
type Store struct {
	cfg        Config
	httpClient *http.Client

	mu       sync.RWMutex
	urls     []string
	metrics  map[string]WorkerMetrics
	onScrape func(url string, m WorkerMetrics)
}

// NewStore creates a new worker metrics store with connection-pooled HTTP scraping.
func NewStore(cfg Config) *Store {
	cfg = cfg.withDefaults()
	return &Store{
		cfg:     cfg,
		metrics: make(map[string]WorkerMetrics),
		httpClient: &http.Client{
			Timeout: cfg.ScrapeTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

// SetOnScrape registers a callback invoked whenever a worker scrape succeeds.
func (s *Store) SetOnScrape(fn func(url string, m WorkerMetrics)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onScrape = fn
}

// SetWorkerURLs replaces the set of backend worker URLs to scrape.
func (s *Store) SetWorkerURLs(urls []string) {
	seen := make(map[string]struct{}, len(urls))
	norm := make([]string, 0, len(urls))
	for _, raw := range urls {
		u := strings.TrimRight(strings.TrimSpace(raw), "/")
		if u == "" {
			continue
		}
		if _, exists := seen[u]; !exists {
			seen[u] = struct{}{}
			norm = append(norm, u)
		}
	}

	s.mu.Lock()
	s.urls = norm
	for existing := range s.metrics {
		if _, ok := seen[existing]; !ok {
			delete(s.metrics, existing)
		}
	}
	s.mu.Unlock()
}

// Get returns the latest valid, non-stale metrics for a worker URL.
func (s *Store) Get(workerURL string) (WorkerMetrics, bool) {
	key := strings.TrimRight(strings.TrimSpace(workerURL), "/")
	s.mu.RLock()
	m, ok := s.metrics[key]
	staleTimeout := s.cfg.StaleTimeout
	s.mu.RUnlock()

	if !ok || !m.Available || m.ScrapedAt.IsZero() {
		return WorkerMetrics{}, false
	}
	if time.Since(m.ScrapedAt) > staleTimeout {
		return WorkerMetrics{}, false
	}
	return m, true
}

// GetRaw returns the latest stored metrics record (even if unavailable or stale) for observability.
func (s *Store) GetRaw(workerURL string) (WorkerMetrics, bool) {
	key := strings.TrimRight(strings.TrimSpace(workerURL), "/")
	s.mu.RLock()
	m, ok := s.metrics[key]
	s.mu.RUnlock()
	return m, ok
}

// GetAll returns a snapshot copy of all stored worker metrics.
func (s *Store) GetAll() map[string]WorkerMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]WorkerMetrics, len(s.metrics))
	for k, v := range s.metrics {
		out[k] = v
	}
	return out
}

// Run starts the background periodic scraping loop until ctx is canceled or stopCh closes.
func (s *Store) Run(ctx context.Context, stopCh <-chan struct{}) {
	s.ScrapeOnce(ctx)

	ticker := time.NewTicker(s.cfg.ScrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			s.ScrapeOnce(ctx)
		}
	}
}

// ScrapeOnce scrapes all currently registered worker URLs concurrently.
func (s *Store) ScrapeOnce(ctx context.Context) {
	s.mu.RLock()
	urls := make([]string, len(s.urls))
	copy(urls, s.urls)
	s.mu.RUnlock()

	if len(urls) == 0 {
		return
	}

	sem := make(chan struct{}, maxConcurrentScrapes)
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go func(targetURL string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			_, _ = s.ScrapeWorker(ctx, targetURL)
		}(u)
	}
	wg.Wait()
}

// ScrapeWorker scrapes metrics from a single worker URL and updates the store.
func (s *Store) ScrapeWorker(ctx context.Context, workerURL string) (WorkerMetrics, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(workerURL), "/")
	if baseURL == "" {
		return WorkerMetrics{}, fmt.Errorf("empty worker URL")
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.ScrapeTimeout)
	defer cancel()

	metricsURL := baseURL + s.cfg.MetricsPath
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, metricsURL, nil)
	if err != nil {
		m := WorkerMetrics{URL: baseURL, Available: false, LastErr: err.Error()}
		s.storeFailure(baseURL, err.Error())
		return m, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		m := WorkerMetrics{URL: baseURL, Available: false, LastErr: err.Error()}
		s.storeFailure(baseURL, err.Error())
		return m, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("HTTP %d from %s", resp.StatusCode, metricsURL)
		m := WorkerMetrics{URL: baseURL, Available: false, LastErr: errMsg}
		s.storeFailure(baseURL, errMsg)
		return m, fmt.Errorf("%s", errMsg)
	}

	running, waiting, kvCache, found, err := ParsePrometheusMetrics(resp.Body)
	if err != nil {
		m := WorkerMetrics{URL: baseURL, Available: false, LastErr: err.Error()}
		s.storeFailure(baseURL, err.Error())
		return m, err
	}
	if !found {
		errMsg := "no supported waiting/running metrics found"
		m := WorkerMetrics{URL: baseURL, Available: false, LastErr: errMsg}
		s.storeFailure(baseURL, errMsg)
		return m, fmt.Errorf("%s", errMsg)
	}

	now := time.Now()
	m := WorkerMetrics{
		URL:          baseURL,
		Running:      running,
		Waiting:      waiting,
		KVCacheUsage: kvCache,
		ScrapedAt:    now,
		Available:    true,
	}

	s.mu.Lock()
	s.metrics[baseURL] = m
	cb := s.onScrape
	s.mu.Unlock()

	if cb != nil {
		cb(baseURL, m)
	}
	return m, nil
}

func (s *Store) storeFailure(baseURL, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, exists := s.metrics[baseURL]
	if exists && !prev.ScrapedAt.IsZero() && time.Since(prev.ScrapedAt) <= s.cfg.StaleTimeout {
		// Preserve last valid reading while within StaleTimeout, only record LastErr
		prev.LastErr = errMsg
		s.metrics[baseURL] = prev
		return
	}
	s.metrics[baseURL] = WorkerMetrics{
		URL:       baseURL,
		Available: false,
		LastErr:   errMsg,
	}
}

type metricKind int

const (
	kindNone metricKind = iota
	kindWaiting
	kindRunning
	kindKVCache
)

func classifyMetricName(name string) (metricKind, string) {
	switch name {
	case "vllm:num_requests_waiting", "vllm_num_requests_waiting",
		"sglang:num_queue_reqs", "sglang_num_queue_reqs":
		return kindWaiting, name
	case "vllm:num_requests_running", "vllm_num_requests_running",
		"sglang:num_running_reqs", "sglang_num_running_reqs":
		return kindRunning, name
	case "vllm:gpu_cache_usage_perc", "vllm_gpu_cache_usage_perc",
		"vllm:kv_cache_usage_perc", "vllm_kv_cache_usage_perc",
		"sglang:token_usage", "sglang_token_usage":
		return kindKVCache, name
	default:
		return kindNone, ""
	}
}

// ParsePrometheusMetrics parses Prometheus exposition text for vLLM and SGLang running/waiting/KV-cache gauges.
// Multiple label series for the same metric name (e.g. DP engines) are summed.
func ParsePrometheusMetrics(r io.Reader) (running, waiting, kvCache float64, found bool, err error) {
	scanner := bufio.NewScanner(io.LimitReader(r, maxMetricsBodyBytes))
	// Allow long label lines up to 256KB
	scanner.Buffer(make([]byte, 0, 16*1024), 256*1024)

	sums := make(map[string]float64)
	counts := make(map[string]int)
	var waitingFamily, runningFamily, kvFamily string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := parsePrometheusSampleLine(line)
		if !ok {
			continue
		}
		kind, family := classifyMetricName(name)
		switch kind {
		case kindWaiting:
			if waitingFamily == "" {
				waitingFamily = family
			}
			if family == waitingFamily {
				sums[family] += val
				counts[family]++
				found = true
			}
		case kindRunning:
			if runningFamily == "" {
				runningFamily = family
			}
			if family == runningFamily {
				sums[family] += val
				counts[family]++
				found = true
			}
		case kindKVCache:
			if kvFamily == "" {
				kvFamily = family
			}
			if family == kvFamily {
				sums[family] += val
				counts[family]++
			}
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return 0, 0, 0, false, scanErr
	}

	if waitingFamily != "" {
		waiting = sums[waitingFamily]
	}
	if runningFamily != "" {
		running = sums[runningFamily]
	}
	if kvFamily != "" && counts[kvFamily] > 0 {
		kvCache = sums[kvFamily] / float64(counts[kvFamily])
	}
	return running, waiting, kvCache, found, nil
}

// parsePrometheusSampleLine extracts the metric name and numeric value from a Prometheus sample line.
// Format: metric_name[{labels}] value [timestamp]
func parsePrometheusSampleLine(line string) (string, float64, bool) {
	var name, rest string
	if openIdx := strings.IndexByte(line, '{'); openIdx != -1 {
		name = strings.TrimSpace(line[:openIdx])
		closeIdx := findClosingBrace(line, openIdx+1)
		if closeIdx == -1 {
			return "", 0, false
		}
		rest = strings.TrimSpace(line[closeIdx+1:])
	} else {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return "", 0, false
		}
		name = fields[0]
		rest = fields[1]
	}

	if name == "" || rest == "" {
		return "", 0, false
	}

	valFields := strings.Fields(rest)
	if len(valFields) == 0 {
		return "", 0, false
	}
	val, err := strconv.ParseFloat(valFields[0], 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return "", 0, false
	}
	return name, val, true
}

func findClosingBrace(s string, start int) int {
	inQuotes := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inQuotes {
			escaped = true
			continue
		}
		if c == '"' {
			inQuotes = !inQuotes
			continue
		}
		if c == '}' && !inQuotes {
			return i
		}
	}
	return -1
}
