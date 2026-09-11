package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"gpu-vllm-router/pkg/logger"
	"gpu-vllm-router/pkg/router"
)

// BackendTarget represents a single worker node endpoint.
type BackendTarget struct {
	URL            *url.URL
	URLString      string
	ActiveConns    int64
	Healthy        bool
	CircuitBreaker *CircuitBreaker
}

// Balancer is the interface for load balancing algorithms.
type Balancer interface {
	SelectTarget(r *http.Request) (*BackendTarget, error)
	SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error)
	SetTargets(targets []*BackendTarget)
	GetTargets() []*BackendTarget
	RecordRequestStart(target *BackendTarget)
	RecordRequestEnd(target *BackendTarget)
}

// BaseBalancer provides common target management.
type BaseBalancer struct {
	mu      sync.RWMutex
	targets []*BackendTarget
}

func (b *BaseBalancer) SetTargets(targets []*BackendTarget) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.targets = targets
}

func (b *BaseBalancer) GetTargets() []*BackendTarget {
	b.mu.RLock()
	defer b.mu.RUnlock()
	res := make([]*BackendTarget, len(b.targets))
	copy(res, b.targets)
	return res
}

func (b *BaseBalancer) RecordRequestStart(target *BackendTarget) {
	atomic.AddInt64(&target.ActiveConns, 1)
}

func (b *BaseBalancer) RecordRequestEnd(target *BackendTarget) {
	atomic.AddInt64(&target.ActiveConns, -1)
}

func (b *BaseBalancer) getAvailableTargets(excluded map[string]bool) []*BackendTarget {
	var available []*BackendTarget
	for _, t := range b.targets {
		if excluded != nil && excluded[t.URLString] {
			continue
		}
		if t.CircuitBreaker == nil || t.CircuitBreaker.CanExecute() {
			available = append(available, t)
		}
	}
	return available
}

// --- Round Robin Balancer ---

type RoundRobinBalancer struct {
	BaseBalancer
	counter uint64
}

func NewRoundRobinBalancer(targets []*BackendTarget) *RoundRobinBalancer {
	b := &RoundRobinBalancer{}
	b.SetTargets(targets)
	return b
}

func (b *RoundRobinBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *RoundRobinBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	available := b.getAvailableTargets(excluded)
	if len(available) == 0 {
		if len(excluded) > 0 {
			return nil, errors.New("no alternative healthy backends available for failover")
		}
		return nil, errors.New("all backends are currently isolated by circuit breaker (OPEN)")
	}

	idx := atomic.AddUint64(&b.counter, 1) % uint64(len(available))
	return available[idx], nil
}

// --- Random Balancer ---

type RandomBalancer struct {
	BaseBalancer
}

func NewRandomBalancer(targets []*BackendTarget) *RandomBalancer {
	b := &RandomBalancer{}
	b.SetTargets(targets)
	return b
}

func (b *RandomBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *RandomBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	available := b.getAvailableTargets(excluded)
	if len(available) == 0 {
		if len(excluded) > 0 {
			return nil, errors.New("no alternative healthy backends available for failover")
		}
		return nil, errors.New("all backends are currently isolated by circuit breaker (OPEN)")
	}

	idx := rand.Intn(len(available))
	return available[idx], nil
}

// --- Power of Two Choices (P2C) Balancer ---

type PowerOfTwoBalancer struct {
	BaseBalancer
}

func NewPowerOfTwoBalancer(targets []*BackendTarget) *PowerOfTwoBalancer {
	b := &PowerOfTwoBalancer{}
	b.SetTargets(targets)
	return b
}

func (b *PowerOfTwoBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *PowerOfTwoBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	available := b.getAvailableTargets(excluded)
	n := len(available)
	if n == 0 {
		if len(excluded) > 0 {
			return nil, errors.New("no alternative healthy backends available for failover")
		}
		return nil, errors.New("all backends are currently isolated by circuit breaker (OPEN)")
	}
	if n == 1 {
		return available[0], nil
	}

	// Pick two distinct random indices
	i1 := rand.Intn(n)
	i2 := rand.Intn(n - 1)
	if i2 >= i1 {
		i2++
	}

	t1 := available[i1]
	t2 := available[i2]

	// Choose the one with fewer active connections
	if atomic.LoadInt64(&t1.ActiveConns) <= atomic.LoadInt64(&t2.ActiveConns) {
		return t1, nil
	}
	return t2, nil
}

// --- Consistent Hash Balancer ---

type ConsistentHashBalancer struct {
	BaseBalancer
	virtualNodes int
	ring         []uint32
	ringMap      map[uint32]*BackendTarget
}

func NewConsistentHashBalancer(targets []*BackendTarget, virtualNodes int) *ConsistentHashBalancer {
	if virtualNodes <= 0 {
		virtualNodes = 100
	}
	b := &ConsistentHashBalancer{
		virtualNodes: virtualNodes,
		ringMap:      make(map[uint32]*BackendTarget),
	}
	b.SetTargets(targets)
	return b
}

func (b *ConsistentHashBalancer) SetTargets(targets []*BackendTarget) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.targets = targets
	b.ring = nil
	b.ringMap = make(map[uint32]*BackendTarget)

	for _, t := range targets {
		for i := 0; i < b.virtualNodes; i++ {
			vKey := t.URLString + "#" + strconv.Itoa(i)
			h := hashKey(vKey)
			b.ring = append(b.ring, h)
			b.ringMap[h] = t
		}
	}
	sort.Slice(b.ring, func(i, j int) bool { return b.ring[i] < b.ring[j] })
}

func (b *ConsistentHashBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *ConsistentHashBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.targets) == 0 || len(b.ring) == 0 {
		return nil, errors.New("no backends available in pool")
	}

	key := extractSessionKey(r)
	h := hashKey(key)

	// Binary search for first ring node >= h
	startIdx := sort.Search(len(b.ring), func(i int) bool { return b.ring[i] >= h })
	if startIdx >= len(b.ring) {
		startIdx = 0
	}

	// Walk the hash ring to find first target that is not excluded and can execute
	for i := 0; i < len(b.ring); i++ {
		idx := (startIdx + i) % len(b.ring)
		target := b.ringMap[b.ring[idx]]
		if target == nil {
			continue
		}
		if excluded != nil && excluded[target.URLString] {
			continue
		}
		if target.CircuitBreaker == nil || target.CircuitBreaker.CanExecute() {
			log.Printf("[Balancer:ConsistentHash] 🎯 Routed to worker %s (Active: %d) | SessionKey: %q (hash=0x%08x)",
				target.URLString, atomic.LoadInt64(&target.ActiveConns), key, h)
			return target, nil
		}
	}

	if len(excluded) > 0 {
		return nil, errors.New("no alternative healthy backends available for failover on hash ring")
	}
	return nil, errors.New("all backends on hash ring are currently isolated by circuit breaker (OPEN)")
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// extractSessionKey extracts session/user identifier matching vllm-router priority.
func extractSessionKey(r *http.Request) string {
	// Priority 1-4: Standard Headers
	if val := r.Header.Get("X-Session-ID"); val != "" {
		return val
	}
	if val := r.Header.Get("X-User-ID"); val != "" {
		return val
	}
	if val := r.Header.Get("X-Tenant-ID"); val != "" {
		return val
	}
	if val := r.Header.Get("X-Request-ID"); val != "" {
		return val
	}

	// Priority 5: If JSON body is present, attempt to extract user or session_params
	if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err == nil {
			// Restore request body for downstream handler
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			var reqMap map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &reqMap); err == nil {
				if user, ok := reqMap["user"].(string); ok && user != "" {
					return user
				}
				if sParams, ok := reqMap["session_params"].(map[string]interface{}); ok {
					if sid, ok := sParams["session_id"].(string); ok && sid != "" {
						return sid
					}
				}
				if sid, ok := reqMap["session_id"].(string); ok && sid != "" {
					return sid
				}
			}
		}
	}

	// Fallback: Remote address
	return r.RemoteAddr
}

// --- Rendezvous (HRW) Hash Balancer ---

type RendezvousHashBalancer struct {
	BaseBalancer
}

func NewRendezvousHashBalancer(targets []*BackendTarget) *RendezvousHashBalancer {
	b := &RendezvousHashBalancer{}
	b.SetTargets(targets)
	return b
}

func (b *RendezvousHashBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *RendezvousHashBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	available := b.getAvailableTargets(excluded)
	if len(available) == 0 {
		if len(excluded) > 0 {
			return nil, errors.New("no alternative healthy backends available for failover on rendezvous hash")
		}
		return nil, errors.New("all backends are currently isolated by circuit breaker (OPEN)")
	}

	key := extractSessionKey(r)

	// Highest Random Weight (HRW) algorithm
	var bestTarget *BackendTarget
	var maxWeight uint32

	for _, target := range available {
		weight := hashKey(key + "#" + target.URLString)
		if bestTarget == nil || weight > maxWeight {
			bestTarget = target
			maxWeight = weight
		}
	}

	return bestTarget, nil
}

// --- Cache Aware Balancer ---

type CacheAwareBalancer struct {
	ConsistentHashBalancer
}

func NewCacheAwareBalancer(targets []*BackendTarget, virtualNodes int) *CacheAwareBalancer {
	ch := NewConsistentHashBalancer(targets, virtualNodes)
	return &CacheAwareBalancer{*ch}
}

func (b *CacheAwareBalancer) SelectTarget(r *http.Request) (*BackendTarget, error) {
	return b.SelectTargetExcluding(r, nil)
}

func (b *CacheAwareBalancer) SelectTargetExcluding(r *http.Request, excluded map[string]bool) (*BackendTarget, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.targets) == 0 || len(b.ring) == 0 {
		return nil, errors.New("no backends available in pool")
	}

	info := extractCacheAwareKeyInfo(r)
	h := info.Hash

	startIdx := sort.Search(len(b.ring), func(i int) bool { return b.ring[i] >= h })
	if startIdx >= len(b.ring) {
		startIdx = 0
	}

	for i := 0; i < len(b.ring); i++ {
		idx := (startIdx + i) % len(b.ring)
		target := b.ringMap[b.ring[idx]]
		if target == nil {
			continue
		}
		if excluded != nil && excluded[target.URLString] {
			continue
		}
		if target.CircuitBreaker == nil || target.CircuitBreaker.CanExecute() {
			logger.Debugf("[Balancer:CacheAware] 🎯 Routed to %s (Active: %d) | Key: [%s] hash=0x%08x len=%d sample=%q",
				target.URLString, atomic.LoadInt64(&target.ActiveConns), info.Source, info.Hash, info.KeyLength, info.KeyPreview)
			return target, nil
		}
	}

	if len(excluded) > 0 {
		return nil, errors.New("no alternative healthy backends available for failover on cache-aware ring")
	}
	return nil, errors.New("all backends on cache-aware ring are currently isolated by circuit breaker (OPEN)")
}

// CacheAwareKeyInfo holds detailed cache matching information for logging and routing analysis.
type CacheAwareKeyInfo struct {
	Source     string // "prompt", "prompt_list", "chat_prefix", "header:X-Session-ID", "json_session", "remote_addr"
	RawKey     string
	KeyPreview string // Short preview for log output
	KeyLength  int    // Original text length
	Hash       uint32
}

func extractCacheAwareKeyInfo(r *http.Request) CacheAwareKeyInfo {
	if r == nil {
		return CacheAwareKeyInfo{Source: "none", RawKey: "none", KeyPreview: "none", Hash: hashKey("none")}
	}

	// Try to extract prompt prefix from JSON body if present
	if r.Body != nil && (r.Method == http.MethodPost || r.Method == http.MethodPut) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err == nil {
			// Restore request body for downstream handlers
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			r.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bodyBytes)), nil
			}

			var reqMap map[string]interface{}
			if err := json.Unmarshal(bodyBytes, &reqMap); err == nil {
				// 1. Check prompt string or prompt array
				if promptStr, ok := reqMap["prompt"].(string); ok && promptStr != "" {
					origLen := len(promptStr)
					preview := promptStr
					if len(preview) > 60 {
						preview = preview[:60] + "..."
					}
					prefix := promptStr
					if len(prefix) > 256 {
						prefix = prefix[:256]
					}
					rawKey := "prompt:" + prefix
					return CacheAwareKeyInfo{
						Source:     "prompt",
						RawKey:     rawKey,
						KeyPreview: preview,
						KeyLength:  origLen,
						Hash:       hashKey(rawKey),
					}
				} else if promptList, ok := reqMap["prompt"].([]interface{}); ok && len(promptList) > 0 {
					if firstP, ok := promptList[0].(string); ok && firstP != "" {
						origLen := len(firstP)
						preview := firstP
						if len(preview) > 60 {
							preview = preview[:60] + "..."
						}
						prefix := firstP
						if len(prefix) > 256 {
							prefix = prefix[:256]
						}
						rawKey := "prompt:" + prefix
						return CacheAwareKeyInfo{
							Source:     "prompt_list",
							RawKey:     rawKey,
							KeyPreview: preview,
							KeyLength:  origLen,
							Hash:       hashKey(rawKey),
						}
					}
				}

				// 2. Check messages array for chat completions
				if msgs, ok := reqMap["messages"].([]interface{}); ok && len(msgs) > 0 {
					var prefixBuf strings.Builder
					var totalLen int
					for i := 0; i < len(msgs) && i < 2; i++ {
						msgMap, ok := msgs[i].(map[string]interface{})
						if !ok {
							continue
						}
						role, _ := msgMap["role"].(string)
						var text string
						if contentStr, ok := msgMap["content"].(string); ok {
							text = contentStr
						} else if parts, ok := msgMap["content"].([]interface{}); ok {
							for _, p := range parts {
								if pMap, ok := p.(map[string]interface{}); ok {
									if pType, _ := pMap["type"].(string); pType == "text" {
										if t, ok := pMap["text"].(string); ok {
											text += t
										}
									}
								}
							}
						}
						if text != "" {
							totalLen += len(text)
							if prefixBuf.Len() > 0 {
								prefixBuf.WriteString(" | ")
							}
							prefixBuf.WriteString(role + ":" + text)
						}
						if role == "user" {
							break
						}
					}

					fullPrefix := prefixBuf.String()
					if fullPrefix != "" {
						preview := fullPrefix
						if len(preview) > 60 {
							preview = preview[:60] + "..."
						}
						prefixKey := fullPrefix
						if len(prefixKey) > 256 {
							prefixKey = prefixKey[:256]
						}
						rawKey := "chat_prefix:" + prefixKey
						return CacheAwareKeyInfo{
							Source:     "chat_prefix",
							RawKey:     rawKey,
							KeyPreview: preview,
							KeyLength:  totalLen,
							Hash:       hashKey(rawKey),
						}
					}
				}
			}
		}
	}

	// 3. Fallback to session headers
	if val := r.Header.Get("X-Session-ID"); val != "" {
		return CacheAwareKeyInfo{Source: "header:X-Session-ID", RawKey: val, KeyPreview: val, KeyLength: len(val), Hash: hashKey(val)}
	}
	if val := r.Header.Get("X-User-ID"); val != "" {
		return CacheAwareKeyInfo{Source: "header:X-User-ID", RawKey: val, KeyPreview: val, KeyLength: len(val), Hash: hashKey(val)}
	}
	if val := r.Header.Get("X-Tenant-ID"); val != "" {
		return CacheAwareKeyInfo{Source: "header:X-Tenant-ID", RawKey: val, KeyPreview: val, KeyLength: len(val), Hash: hashKey(val)}
	}
	if val := r.Header.Get("X-Request-ID"); val != "" {
		return CacheAwareKeyInfo{Source: "header:X-Request-ID", RawKey: val, KeyPreview: val, KeyLength: len(val), Hash: hashKey(val)}
	}

	// 4. Remote Address fallback
	remote := r.RemoteAddr
	return CacheAwareKeyInfo{Source: "remote_addr", RawKey: remote, KeyPreview: remote, KeyLength: len(remote), Hash: hashKey(remote)}
}

func extractCacheAwareKey(r *http.Request) string {
	return extractCacheAwareKeyInfo(r).RawKey
}

// NewBalancer creates a Balancer based on Policy.
func NewBalancer(policy router.Policy, targets []*BackendTarget) Balancer {
	switch policy {
	case router.PolicyRoundRobin:
		return NewRoundRobinBalancer(targets)
	case router.PolicyRandom:
		return NewRandomBalancer(targets)
	case router.PolicyPowerOfTwo:
		return NewPowerOfTwoBalancer(targets)
	case router.PolicyConsistentHash:
		return NewConsistentHashBalancer(targets, 100)
	case router.PolicyCacheAware:
		return NewCacheAwareBalancer(targets, 100)
	case router.PolicyRendezvousHash:
		return NewRendezvousHashBalancer(targets)
	default:
		return NewRoundRobinBalancer(targets)
	}
}

