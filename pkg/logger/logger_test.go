package logger

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"warning", LevelWarn},
		{"WARN", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"unknown", LevelInfo},
		{"", LevelInfo},
	}

	for _, tt := range tests {
		got := ParseLevel(tt.input)
		if got != tt.expected {
			t.Errorf("ParseLevel(%q) = %v; want %v", tt.input, got, tt.expected)
		}
	}
}

func TestLoggerFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, LevelWarn)

	// DEBUG and INFO should be suppressed
	l.Logf(LevelDebug, "debug message: %d", 1)
	l.Logf(LevelInfo, "info message: %d", 2)
	if buf.Len() > 0 {
		t.Errorf("expected empty buffer for debug/info at warn level, got: %s", buf.String())
	}

	// WARN and ERROR should be recorded
	l.Logf(LevelWarn, "warn message: %d", 3)
	if !strings.Contains(buf.String(), "[WARN] warn message: 3") {
		t.Errorf("expected WARN log, got: %s", buf.String())
	}
	buf.Reset()

	l.Logf(LevelError, "error message: %d", 4)
	if !strings.Contains(buf.String(), "[ERROR] error message: 4") {
		t.Errorf("expected ERROR log, got: %s", buf.String())
	}

	// Change level to DEBUG
	l.SetLevel(LevelDebug)
	buf.Reset()
	l.Logf(LevelDebug, "now debug is allowed")
	if !strings.Contains(buf.String(), "[DEBUG] now debug is allowed") {
		t.Errorf("expected DEBUG log after SetLevel, got: %s", buf.String())
	}
}

func TestLogLineDetection(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, LevelInfo)

	// Debug line should be filtered out at LevelInfo
	l.LogLine("[DEBUG] cache hit detail")
	l.LogLine("worker DEBUG trace")
	if buf.Len() > 0 {
		t.Errorf("expected debug line to be filtered out at LevelInfo, got: %s", buf.String())
	}

	// Info line
	l.LogLine("normal operational log")
	if !strings.Contains(buf.String(), "[INFO] normal operational log") {
		t.Errorf("expected [INFO] prefix, got: %s", buf.String())
	}
	buf.Reset()

	// Warn line with warning keyword or emoji
	l.LogLine("⚠️ worker probe slow")
	if !strings.Contains(buf.String(), "[WARN] ⚠️ worker probe slow") {
		t.Errorf("expected [WARN] prefix, got: %s", buf.String())
	}
	buf.Reset()

	// Error line
	l.LogLine("failed to connect to upstream worker")
	if !strings.Contains(buf.String(), "[ERROR] failed to connect to upstream worker") {
		t.Errorf("expected [ERROR] prefix, got: %s", buf.String())
	}
}

func TestStandardLogBridge(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, LevelWarn)

	bridge := l.StandardLogBridge()
	customLog := log.New(bridge, "", 0)

	// standard Printf at info level should be suppressed
	customLog.Printf("regular info line via std log")
	if buf.Len() > 0 {
		t.Errorf("expected std log info to be suppressed at WARN level, got: %s", buf.String())
	}

	// standard Printf with Warning keyword should pass
	customLog.Printf("Warning: connection degraded")
	if !strings.Contains(buf.String(), "[WARN] Warning: connection degraded") {
		t.Errorf("expected std log warning to pass, got: %s", buf.String())
	}
}

func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, LevelDebug)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			l.Logf(LevelInfo, "concurrent info %d", id)
			l.LogLine("[DEBUG] concurrent line")
			_ = l.GetLevel()
		}(i)
	}
	wg.Wait()

	if buf.Len() == 0 {
		t.Errorf("expected concurrent output to buffer")
	}
}
