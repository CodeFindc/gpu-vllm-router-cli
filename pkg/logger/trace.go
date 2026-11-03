package logger

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var reqSeq uint64

// CallTrace tracks the full lifecycle of an API/inference request across reception,
// routing, retries/failovers, first-byte response (TTFB), and stream/body completion.
type CallTrace struct {
	mu           sync.Mutex
	ReqID        string
	ClientIP     string
	Method       string
	Path         string
	Model        string
	RoutedModel  string
	Policy       string
	Mode         string
	Stream       bool
	SessionKey   string
	ReqBodySize  int
	ReqSnippet   string
	TargetURL    string
	WorkerName   string
	InstanceName string

	StartTime     time.Time
	RouteTime     time.Time
	FirstByteTime time.Time
	RetryCount    int
	doneLogged    atomic.Bool
}

// NewCallTrace initializes a new CallTrace from an incoming HTTP request and extracted body metadata.
func NewCallTrace(r *http.Request, model string, stream bool, sessionKey string, bodyBytes []byte, mode string) *CallTrace {
	now := time.Now()
	reqID := ExtractOrGenerateReqID(r)
	clientIP := ExtractClientIP(r)
	path := "/"
	method := "GET"
	if r != nil {
		method = r.Method
		if r.URL != nil {
			path = r.URL.Path
			if r.URL.RawQuery != "" {
				path = path + "?" + r.URL.RawQuery
			}
		}
	}

	return &CallTrace{
		ReqID:       reqID,
		ClientIP:    clientIP,
		Method:      method,
		Path:        path,
		Model:       model,
		Mode:        mode,
		Stream:      stream,
		SessionKey:  sessionKey,
		ReqBodySize: len(bodyBytes),
		ReqSnippet:  SanitizeSnippet(bodyBytes, 512),
		StartTime:   now,
	}
}

// ExtractOrGenerateReqID extracts a request/trace ID from standard headers or generates a unique one.
func ExtractOrGenerateReqID(r *http.Request) string {
	if r != nil {
		for _, h := range []string{"X-Request-ID", "Request-Id", "X-Trace-ID", "X-Correlation-ID"} {
			if val := strings.TrimSpace(r.Header.Get(h)); val != "" {
				return val
			}
		}
	}
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		seq := atomic.AddUint64(&reqSeq, 1)
		return fmt.Sprintf("req-%06d-%08x", time.Now().UnixMilli()%1000000, seq)
	}
	return fmt.Sprintf("req-%06d-%s", time.Now().UnixMilli()%1000000, hex.EncodeToString(rnd[:]))
}

// ExtractClientIP extracts the real client IP address from headers or RemoteAddr.
func ExtractClientIP(r *http.Request) string {
	if r == nil {
		return "unknown"
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

// SanitizeSnippet collapses newlines/tabs into single spaces and safely truncates UTF-8 text to maxLen bytes.
func SanitizeSnippet(b []byte, maxLen int) string {
	if len(b) == 0 {
		return ""
	}
	if maxLen <= 0 {
		maxLen = 512
	}
	limit := len(b)
	truncated := false
	if limit > maxLen*2 {
		limit = maxLen * 2
		truncated = true
	}
	s := string(b[:limit])
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.TrimSpace(s)

	if len(s) > maxLen {
		s = s[:maxLen]
		for len(s) > 0 && !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		truncated = true
	}
	if truncated {
		return s + "...(truncated)"
	}
	return s
}

// PeekErrorResponseBody non-destructively reads up to maxBytes from resp.Body and restores resp.Body
// so downstream clients still receive the exact, complete response stream.
func PeekErrorResponseBody(resp *http.Response, maxBytes int) string {
	if resp == nil || resp.Body == nil {
		return ""
	}
	if maxBytes <= 0 {
		maxBytes = 1024
	}
	origBody := resp.Body
	buf, err := io.ReadAll(io.LimitReader(origBody, int64(maxBytes)))
	if len(buf) > 0 {
		resp.Body = &restoredReadCloser{
			Reader: io.MultiReader(bytes.NewReader(buf), origBody),
			Closer: origBody,
		}
	}
	if err != nil && len(buf) == 0 {
		return fmt.Sprintf("<read error: %v>", err)
	}
	return SanitizeSnippet(buf, maxBytes)
}

type restoredReadCloser struct {
	io.Reader
	io.Closer
}

// SetTarget updates the selected routing target and worker metadata on the trace.
func (t *CallTrace) SetTarget(routedModel, policy, targetURL, workerName, instanceName string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if routedModel != "" {
		t.RoutedModel = routedModel
	}
	if policy != "" {
		t.Policy = policy
	}
	t.TargetURL = targetURL
	t.WorkerName = workerName
	t.InstanceName = instanceName
	if t.RouteTime.IsZero() {
		t.RouteTime = time.Now()
	}
}

func (t *CallTrace) workerDisplayLocked() string {
	target := t.TargetURL
	if target == "" {
		target = "unassigned"
	}
	if t.WorkerName != "" && t.InstanceName != "" {
		return fmt.Sprintf("%s (Worker: %s, Instance: %s)", target, t.WorkerName, t.InstanceName)
	}
	if t.WorkerName != "" {
		return fmt.Sprintf("%s (Worker: %s)", target, t.WorkerName)
	}
	return target
}

func (t *CallTrace) modelDisplayLocked() string {
	if t.RoutedModel != "" {
		return t.RoutedModel
	}
	if t.Model != "" {
		return t.Model
	}
	return "<unspecified>"
}

// LogRecv logs Stage 1: Incoming Request Reception at INFO level (plus payload snippet at DEBUG level).
func (t *CallTrace) LogRecv() {
	if t == nil {
		return
	}
	t.mu.Lock()
	reqID := t.ReqID
	clientIP := t.ClientIP
	method := t.Method
	path := t.Path
	model := t.Model
	if model == "" {
		model = "<auto>"
	}
	stream := t.Stream
	bodySize := t.ReqBodySize
	session := t.SessionKey
	if session == "" {
		session = "-"
	}
	snippet := t.ReqSnippet
	t.mu.Unlock()

	Infof("[Call:RECV] [ReqID: %s] 📥 收到调用请求 | Client: %s | %s %s | Model: %q | Stream: %t | BodySize: %dB | Session: %q",
		reqID, clientIP, method, path, model, stream, bodySize, session)

	if snippet != "" {
		Debugf("[Call:PAYLOAD] [ReqID: %s] 🔍 请求报文摘要: %s", reqID, snippet)
	}
}

// LogRoute logs Stage 2: Successful Routing Decision at INFO level.
func (t *CallTrace) LogRoute(activeConns int64, keyInfo string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.RouteTime.IsZero() {
		t.RouteTime = time.Now()
	}
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	policy := t.Policy
	if policy == "" {
		policy = "default"
	}
	workerDesc := t.workerDisplayLocked()
	if keyInfo == "" {
		keyInfo = "-"
	}
	t.mu.Unlock()

	Infof("[Call:ROUTE] [ReqID: %s] 🎯 路由命中 | Model: %s | Policy: %s | Target: %s | ActiveConns: %d | Key: %s",
		reqID, model, policy, workerDesc, activeConns, keyInfo)
}

// LogRouteError logs Stage 2b: Routing or Balancer Failure at ERROR level with request snippet.
func (t *CallTrace) LogRouteError(err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	path := t.Path
	snippet := t.ReqSnippet
	if snippet == "" {
		snippet = "<empty>"
	}
	t.mu.Unlock()

	Errorf("[Call:ROUTE_ERR] [ReqID: %s] ❌ 路由失败 | Model: %q | Path: %s | Error: %v | ReqSnippet: %s",
		reqID, model, path, err, snippet)
}

// LogRetry logs Stage 3 (Conditional): Failover & Transparent Retry at WARN level.
func (t *CallTrace) LogRetry(attempt, maxRetries int, failedTarget string, reason error, attemptCost time.Duration, nextTarget, nextWorkerName, nextInstanceName string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.RetryCount = attempt
	t.TargetURL = nextTarget
	t.WorkerName = nextWorkerName
	t.InstanceName = nextInstanceName
	reqID := t.ReqID
	nextDesc := t.workerDisplayLocked()
	t.mu.Unlock()

	Warnf("[Call:RETRY] [ReqID: %s] ⚡ 节点调用异常，触发故障转移 (Attempt %d/%d) | FailedTarget: %s | Cost: %v | Reason: %v -> NextTarget: %s",
		reqID, attempt, maxRetries, failedTarget, FormatDuration(attemptCost), reason, nextDesc)
}

// LogRetryExhausted logs when failover cannot find any healthy alternative target.
func (t *CallTrace) LogRetryExhausted(attempt int, reason error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	failedTarget := t.TargetURL
	t.mu.Unlock()

	Warnf("[Call:RETRY_EXHAUSTED] [ReqID: %s] ⚠️ 故障转移终止 (已尝试 %d 次, 无可用健康备选节点) | Model: %s | LastTarget: %s | Reason: %v",
		reqID, attempt, model, failedTarget, reason)
}

// LogFirstByte logs Stage 4: Backend First-Byte Response (TTFB) and HTTP Status Code.
// If statusCode >= 400, it automatically logs the request and error response snippets at WARN/ERROR level.
func (t *CallTrace) LogFirstByte(statusCode int, contentType string, errRespSnippet string) {
	if t == nil {
		return
	}
	now := time.Now()
	t.mu.Lock()
	t.FirstByteTime = now
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	workerDesc := t.workerDisplayLocked()
	ttfb := now.Sub(t.StartTime)
	retries := t.RetryCount
	reqSnippet := t.ReqSnippet
	if reqSnippet == "" {
		reqSnippet = "<empty>"
	}
	t.mu.Unlock()

	if contentType == "" {
		contentType = "-"
	}

	if statusCode >= 500 {
		Errorf("[Call:RESP_ERR] [ReqID: %s] ❌ 后端返回服务端错误 | Status: %d | Model: %s | Worker: %s | TTFB: %s | Retries: %d | ReqSnippet: %s | RespError: %s",
			reqID, statusCode, model, workerDesc, FormatDuration(ttfb), retries, reqSnippet, errRespSnippet)
		return
	}
	if statusCode >= 400 {
		Warnf("[Call:RESP_ERR] [ReqID: %s] ⚠️ 后端返回异常状态码 | Status: %d | Model: %s | Worker: %s | TTFB: %s | Retries: %d | ReqSnippet: %s | RespError: %s",
			reqID, statusCode, model, workerDesc, FormatDuration(ttfb), retries, reqSnippet, errRespSnippet)
		return
	}

	Infof("[Call:RESP] [ReqID: %s] 📡 后端首包响应 | Status: %d | Model: %s | Worker: %s | TTFB: %s | Type: %s | Retries: %d",
		reqID, statusCode, model, workerDesc, FormatDuration(ttfb), contentType, retries)
}

// LogDone logs Stage 5: Final Completion or Mid-Stream Client Abort.
// Guaranteed idempotent via atomic flag so it only logs once per request lifecycle.
func (t *CallTrace) LogDone(statusCode int, bytesSent int64, streamErr error, ctxErr error) {
	if t == nil || !t.doneLogged.CompareAndSwap(false, true) {
		return
	}
	now := time.Now()
	t.mu.Lock()
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	workerDesc := t.workerDisplayLocked()
	totalCost := now.Sub(t.StartTime)
	var ttfbCost, transferCost time.Duration
	if !t.FirstByteTime.IsZero() {
		ttfbCost = t.FirstByteTime.Sub(t.StartTime)
		transferCost = now.Sub(t.FirstByteTime)
	} else {
		ttfbCost = totalCost
	}
	stream := t.Stream
	retries := t.RetryCount
	t.mu.Unlock()

	if isCanceledErr(streamErr) || isCanceledErr(ctxErr) {
		reason := streamErr
		if reason == nil {
			reason = ctxErr
		}
		Warnf("[Call:ABORT] [ReqID: %s] 🚫 客户端中断连接 | Status: %d | Model: %s | Worker: %s | Elapsed: %s (TTFB: %s) | BytesSent: %dB | Stream: %t | Reason: %v",
			reqID, statusCode, model, workerDesc, FormatDuration(totalCost), FormatDuration(ttfbCost), bytesSent, stream, reason)
		return
	}

	if streamErr != nil && !errors.Is(streamErr, io.EOF) {
		Errorf("[Call:FAIL] [ReqID: %s] ❌ 响应流传输异常中断 | Status: %d | Model: %s | Worker: %s | Elapsed: %s (TTFB: %s, Transfer: %s) | BytesSent: %dB | Stream: %t | Error: %v",
			reqID, statusCode, model, workerDesc, FormatDuration(totalCost), FormatDuration(ttfbCost), FormatDuration(transferCost), bytesSent, stream, streamErr)
		return
	}

	if statusCode >= 500 {
		Errorf("[Call:DONE] [ReqID: %s] ❌ 调用结束(服务端错误) | Status: %d | Model: %s | Worker: %s | TotalCost: %s (TTFB: %s, Transfer: %s) | Bytes: %dB | Stream: %t | Retries: %d",
			reqID, statusCode, model, workerDesc, FormatDuration(totalCost), FormatDuration(ttfbCost), FormatDuration(transferCost), bytesSent, stream, retries)
		return
	}

	if statusCode >= 400 {
		Warnf("[Call:DONE] [ReqID: %s] ⚠️ 调用结束(状态码异常) | Status: %d | Model: %s | Worker: %s | TotalCost: %s (TTFB: %s, Transfer: %s) | Bytes: %dB | Stream: %t | Retries: %d",
			reqID, statusCode, model, workerDesc, FormatDuration(totalCost), FormatDuration(ttfbCost), FormatDuration(transferCost), bytesSent, stream, retries)
		return
	}

	Infof("[Call:DONE] [ReqID: %s] ✅ 调用完成 | Status: %d | Model: %s | Worker: %s | TotalCost: %s (TTFB: %s, Transfer: %s) | Bytes: %dB | Stream: %t | Retries: %d",
		reqID, statusCode, model, workerDesc, FormatDuration(totalCost), FormatDuration(ttfbCost), FormatDuration(transferCost), bytesSent, stream, retries)
}

// LogGatewayFail logs Stage 5b: Gateway-level failure when no response could be obtained from backend (e.g. 502 / 404 / client canceled before response).
func (t *CallTrace) LogGatewayFail(statusCode int, err error, ctxErr error) {
	if t == nil || !t.doneLogged.CompareAndSwap(false, true) {
		return
	}
	now := time.Now()
	t.mu.Lock()
	reqID := t.ReqID
	model := t.modelDisplayLocked()
	workerDesc := t.workerDisplayLocked()
	elapsed := now.Sub(t.StartTime)
	retries := t.RetryCount
	reqSnippet := t.ReqSnippet
	if reqSnippet == "" {
		reqSnippet = "<empty>"
	}
	t.mu.Unlock()

	if isCanceledErr(err) || isCanceledErr(ctxErr) {
		reason := err
		if reason == nil {
			reason = ctxErr
		}
		Warnf("[Call:ABORT] [ReqID: %s] 🚫 客户端取消/断开请求 (未完成响应) | Model: %s | Worker: %s | Elapsed: %s | Retries: %d | Reason: %v",
			reqID, model, workerDesc, FormatDuration(elapsed), retries, reason)
		return
	}

	Errorf("[Call:FAIL] [ReqID: %s] ❌ 调用失败 | Status: %d | Model: %s | Worker: %s | Elapsed: %s | Retries: %d | Error: %v | ReqSnippet: %s",
		reqID, statusCode, model, workerDesc, FormatDuration(elapsed), retries, err, reqSnippet)
}

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "context canceled") || strings.Contains(s, "client disconnected")
}

// FormatDuration formats a time.Duration into a concise, human-friendly string.
func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// WrapResponseBody wraps an http.Response.Body to count bytes transferred and invoke onClose
// exactly once when the body is closed by ReverseProxy after streaming/copying finishes.
func WrapResponseBody(body io.ReadCloser, onClose func(bytesRead int64, readErr error)) io.ReadCloser {
	if body == nil {
		if onClose != nil {
			onClose(0, nil)
		}
		return nil
	}
	return &trackedBodyReader{
		rc:      body,
		onClose: onClose,
	}
}

type trackedBodyReader struct {
	rc      io.ReadCloser
	bytes   int64
	lastErr error
	closed  atomic.Bool
	onClose func(bytesRead int64, readErr error)
}

func (r *trackedBodyReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		atomic.AddInt64(&r.bytes, int64(n))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.lastErr = err
	}
	return n, err
}

func (r *trackedBodyReader) Close() error {
	err := r.rc.Close()
	if r.closed.CompareAndSwap(false, true) && r.onClose != nil {
		r.onClose(atomic.LoadInt64(&r.bytes), r.lastErr)
	}
	return err
}
