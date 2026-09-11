package logger

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// Level represents the severity level of a log message.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}

// ParseLevel parses a level string into a Level. Defaults to LevelInfo if unknown.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "info":
		return LevelInfo
	default:
		return LevelInfo
	}
}

// Logger provides leveled and synchronized logging.
type Logger struct {
	mu     sync.RWMutex
	level  Level
	writer io.Writer
	std    *log.Logger
}

var globalLogger = New(os.Stderr, LevelInfo)

// New creates a new Logger writing to w at the specified initial level.
func New(w io.Writer, level Level) *Logger {
	if w == nil {
		w = os.Stderr
	}
	return &Logger{
		level:  level,
		writer: w,
		std:    log.New(w, "", log.LstdFlags),
	}
}

// SetOutput sets the destination writer for the global logger.
func SetOutput(w io.Writer) {
	globalLogger.SetOutput(w)
}

// SetLevel sets the minimum log level for the global logger.
func SetLevel(l Level) {
	globalLogger.SetLevel(l)
}

// SetLevelString sets the minimum log level by string for the global logger.
func SetLevelString(s string) {
	globalLogger.SetLevel(ParseLevel(s))
}

// GetLevel returns the current minimum log level of the global logger.
func GetLevel() Level {
	return globalLogger.GetLevel()
}

// SetOutput sets the destination writer for this Logger.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writer = w
	l.std.SetOutput(w)
}

// SetLevel sets the minimum log level for this Logger.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current minimum log level of this Logger.
func (l *Logger) GetLevel() Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

// Logf formats and outputs a log message if level >= minimum configured level.
func (l *Logger) Logf(level Level, format string, v ...interface{}) {
	l.mu.RLock()
	curLevel := l.level
	l.mu.RUnlock()

	if level < curLevel {
		return
	}

	msg := fmt.Sprintf(format, v...)
	l.std.Printf("[%s] %s", level.String(), msg)
}

// LogLine logs a pre-formatted string line, inferring its level from keywords if not explicit.
func (l *Logger) LogLine(line string) {
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return
	}

	level := DetectLevel(line)

	l.mu.RLock()
	curLevel := l.level
	l.mu.RUnlock()

	if level < curLevel {
		return
	}

	// Avoid duplicate level tag if the line already starts with [LEVEL]
	prefix := fmt.Sprintf("[%s]", level.String())
	if strings.HasPrefix(line, prefix) {
		l.std.Println(line)
	} else {
		l.std.Printf("[%s] %s", level.String(), line)
	}
}

// DetectLevel inspects a log string and infers its severity level.
func DetectLevel(line string) Level {
	upper := strings.ToUpper(line)
	if strings.Contains(upper, "[DEBUG]") || strings.Contains(upper, " DEBUG ") || strings.Contains(upper, " DEBUG:") {
		return LevelDebug
	}
	if strings.Contains(upper, "[ERROR]") || strings.Contains(upper, " ERROR ") || strings.Contains(upper, " ERROR:") ||
		strings.Contains(upper, "FAILED TO") || strings.Contains(upper, "FATAL") {
		return LevelError
	}
	if strings.Contains(upper, "[WARN]") || strings.Contains(upper, " WARN ") || strings.Contains(upper, " WARN:") ||
		strings.Contains(upper, "WARNING") || strings.Contains(line, "⚠️") {
		return LevelWarn
	}
	return LevelInfo
}

// Debugf logs a debug message using the global logger.
func Debugf(format string, v ...interface{}) {
	globalLogger.Logf(LevelDebug, format, v...)
}

// Infof logs an info message using the global logger.
func Infof(format string, v ...interface{}) {
	globalLogger.Logf(LevelInfo, format, v...)
}

// Warnf logs a warning message using the global logger.
func Warnf(format string, v ...interface{}) {
	globalLogger.Logf(LevelWarn, format, v...)
}

// Errorf logs an error message using the global logger.
func Errorf(format string, v ...interface{}) {
	globalLogger.Logf(LevelError, format, v...)
}

// Fatalf logs an error message and calls os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	globalLogger.Logf(LevelError, format, v...)
	os.Exit(1)
}

// LogLine logs a single line using the global logger with automatic level detection and filtering.
func LogLine(line string) {
	globalLogger.LogLine(line)
}

type bridgeWriter struct {
	logger *Logger
	buf    bytes.Buffer
	mu     sync.Mutex
}

func (b *bridgeWriter) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n = len(p)
	b.buf.Write(p)

	for {
		line, err := b.buf.ReadString('\n')
		if err != nil {
			// Line is incomplete, put back any remaining bytes
			if len(line) > 0 {
				b.buf.WriteString(line)
			}
			break
		}
		b.logger.LogLine(strings.TrimRight(line, "\r\n"))
	}
	return n, nil
}

// StandardLogBridge returns an io.Writer that routes Go standard library log output
// through this Logger with level detection and filtering.
func (l *Logger) StandardLogBridge() io.Writer {
	return &bridgeWriter{logger: l}
}

// StandardLogBridge returns an io.Writer routing to the global logger.
func StandardLogBridge() io.Writer {
	return globalLogger.StandardLogBridge()
}

// BridgeStandardLog redirects the Go standard library 'log' package to the global logger.
// It sets standard log flags to 0 so timestamps are cleanly formatted by Logger.
func BridgeStandardLog() {
	log.SetFlags(0)
	log.SetOutput(StandardLogBridge())
}
