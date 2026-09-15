package router

import (
	"net"
	"net/http"
	"time"
)

// NewOptimizedTransport returns an http.Transport configured for high-concurrency LLM routing.
// It eliminates socket exhaustion and TIME_WAIT pileup by optimizing connection pooling thresholds.
func NewOptimizedTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   2048,
		MaxConnsPerHost:       0, // Unlimited active concurrent conns
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0,    // LLM streaming generation can have long TTFT
		DisableCompression:    true, // Avoid compression CPU overhead and preserve raw SSE streams
	}
}
