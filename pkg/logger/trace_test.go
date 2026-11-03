package logger

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExtractOrGenerateReqID(t *testing.T) {
	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req1.Header.Set("X-Request-ID", "custom-req-999")
	if got := ExtractOrGenerateReqID(req1); got != "custom-req-999" {
		t.Errorf("expected custom-req-999, got %q", got)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gen1 := ExtractOrGenerateReqID(req2)
	gen2 := ExtractOrGenerateReqID(req2)
	if !strings.HasPrefix(gen1, "req-") || gen1 == gen2 {
		t.Errorf("expected unique generated req- IDs, got %q and %q", gen1, gen2)
	}
}

func TestPeekErrorResponseBodyPreservesStream(t *testing.T) {
	errJSON := `{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":400}}`
	resp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader(errJSON)),
	}

	snippet := PeekErrorResponseBody(resp, 1024)
	if !strings.Contains(snippet, "maximum context length exceeded") {
		t.Fatalf("expected snippet to contain error message, got %q", snippet)
	}

	// Verify downstream client still reads the full untouched body
	restoredBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read restored body: %v", err)
	}
	if string(restoredBytes) != errJSON {
		t.Errorf("restored body mismatch:\ngot:  %s\nwant: %s", string(restoredBytes), errJSON)
	}
}

func TestCallTraceFullLifecycleAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	origLevel := GetLevel()
	SetOutput(&buf)
	SetLevel(LevelInfo)
	defer func() {
		SetOutput(os.Stderr)
		SetLevel(origLevel)
	}()

	body := []byte(`{"model":"DeepSeek-V4","stream":true,"messages":[{"role":"user","content":"hello"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.RemoteAddr = "192.168.10.25:54321"
	req.Header.Set("X-Request-ID", "trace-test-001")

	trace := NewCallTrace(req, "DeepSeek-V4", true, "sess-abc", body, "proxy")
	trace.LogRecv()

	trace.SetTarget("DeepSeek-V4", "consistent_hash", "http://10.0.0.1:8000", "gpu-worker-1", "deepseek-inst-1")
	trace.LogRoute(2, `session="sess-abc" (hash=0x1234)`)

	trace.LogRetry(1, 2, "http://10.0.0.1:8000", errors.New("connection refused"), 15*time.Millisecond, "http://10.0.0.2:8000", "gpu-worker-2", "deepseek-inst-2")

	trace.LogFirstByte(http.StatusOK, "text/event-stream", "")

	// Simulate streaming response body reading and closing
	streamData := "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]\n\n"
	wrapped := WrapResponseBody(io.NopCloser(strings.NewReader(streamData)), func(bytesRead int64, readErr error) {
		trace.LogDone(http.StatusOK, bytesRead, readErr, nil)
	})
	readBytes, _ := io.ReadAll(wrapped)
	_ = wrapped.Close()

	if len(readBytes) != len(streamData) {
		t.Fatalf("expected %d bytes read, got %d", len(streamData), len(readBytes))
	}

	logs := buf.String()
	expectedSubstrings := []string{
		"[INFO] [Call:RECV] [ReqID: trace-test-001]",
		"Client: 192.168.10.25",
		"Model: \"DeepSeek-V4\" | Stream: true",
		"[INFO] [Call:ROUTE] [ReqID: trace-test-001]",
		"Target: http://10.0.0.1:8000 (Worker: gpu-worker-1, Instance: deepseek-inst-1)",
		"[WARN] [Call:RETRY] [ReqID: trace-test-001]",
		"Attempt 1/2",
		"NextTarget: http://10.0.0.2:8000 (Worker: gpu-worker-2, Instance: deepseek-inst-2)",
		"[INFO] [Call:RESP] [ReqID: trace-test-001]",
		"Status: 200",
		"[INFO] [Call:DONE] [ReqID: trace-test-001]",
		"Retries: 1",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(logs, exp) {
			t.Errorf("expected log output to contain %q, got:\n%s", exp, logs)
		}
	}

	// At INFO level, [Call:PAYLOAD] should not appear unless error occurs
	if strings.Contains(logs, "[Call:PAYLOAD]") {
		t.Errorf("did not expect [Call:PAYLOAD] at INFO level for 200 OK request, got:\n%s", logs)
	}
}

func TestCallTraceErrorAndCancelLogging(t *testing.T) {
	var buf bytes.Buffer
	origLevel := GetLevel()
	SetOutput(&buf)
	SetLevel(LevelInfo)
	defer func() {
		SetOutput(os.Stderr)
		SetLevel(origLevel)
	}()

	// 1. Test 400 Bad Request logs both ReqSnippet and RespError at WARN
	body := []byte(`{"model":"Qwen3.6","max_tokens":999999}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("X-Request-ID", "trace-err-400")

	trace400 := NewCallTrace(req, "Qwen3.6", false, "user-1", body, "proxy")
	trace400.SetTarget("Qwen3.6", "round_robin", "http://10.0.0.5:8000", "worker-5", "qwen-5")
	trace400.LogFirstByte(http.StatusBadRequest, "application/json", `{"error":"max_tokens too large"}`)
	trace400.LogDone(http.StatusBadRequest, 32, nil, nil)

	logs := buf.String()
	if !strings.Contains(logs, "[WARN] [Call:RESP_ERR] [ReqID: trace-err-400]") ||
		!strings.Contains(logs, `"max_tokens":999999`) ||
		!strings.Contains(logs, `{"error":"max_tokens too large"}`) {
		t.Errorf("expected 400 error log to contain ReqSnippet and RespError, got:\n%s", logs)
	}

	// 2. Test Client Abort logs [Call:ABORT]
	buf.Reset()
	traceAbort := NewCallTrace(req, "Qwen3.6", true, "user-1", body, "proxy")
	traceAbort.SetTarget("Qwen3.6", "round_robin", "http://10.0.0.5:8000", "worker-5", "qwen-5")
	traceAbort.LogGatewayFail(0, context.Canceled, context.Canceled)

	if !strings.Contains(buf.String(), "[WARN] [Call:ABORT]") {
		t.Errorf("expected client cancel to log [Call:ABORT], got:\n%s", buf.String())
	}
}
