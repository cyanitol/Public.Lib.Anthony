// Package observability provides structured logging and observability features
// for the SQLite driver implementation.
package observability

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// LogLevel represents the severity level of a log message.
type LogLevel int

const (
	// LevelTrace is the most verbose level, for very detailed diagnostic information.
	LevelTrace LogLevel = iota
	// LevelDebug is for debugging information useful during development.
	LevelDebug
	// LevelInfo is for informational messages about normal operation.
	LevelInfo
	// LevelWarn is for warning messages about potentially problematic situations.
	LevelWarn
	// LevelError is for error messages about failures that may be recoverable.
	LevelError
	// LevelFatal is for critical errors that require immediate attention.
	LevelFatal
	// LevelNone disables all logging.
	LevelNone
)

// String returns the string representation of a log level.
func (l LogLevel) String() string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	case LevelNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

// Logger provides structured logging with multiple severity levels.
// It is safe for concurrent use by multiple goroutines.
type Logger struct {
	mu       sync.Mutex
	output   io.Writer
	level    LogLevel
	prefix   string
	flags    int
	enabled  bool
	fileMode bool // true if logging to a file
}

// LogEntry represents a single log entry with structured fields.
type LogEntry struct {
	Timestamp time.Time
	Level     LogLevel
	Message   string
	Fields    map[string]interface{}
}

// NewLogger creates a new logger with the specified output and level.
// By default, it writes to stderr with LevelInfo.
func NewLogger(output io.Writer, level LogLevel) *Logger {
	if output == nil {
		output = os.Stderr
	}

	return &Logger{
		output:  output,
		level:   level,
		flags:   log.LstdFlags | log.Lmicroseconds,
		enabled: true,
	}
}

// NewFileLogger creates a logger that writes to the specified file.
func NewFileLogger(filename string, level LogLevel) (*Logger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	logger := NewLogger(file, level)
	logger.fileMode = true
	return logger, nil
}

// DefaultLogger returns the default logger instance (logs to stderr at Info level).
var defaultLogger = NewLogger(os.Stderr, LevelInfo)

// SetDefaultLogger sets the default logger instance.
func SetDefaultLogger(logger *Logger) {
	defaultLogger = logger
}

// GetDefaultLogger returns the default logger instance.
func GetDefaultLogger() *Logger {
	return defaultLogger
}

// SetLevel sets the minimum log level. Messages below this level are ignored.
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current log level.
func (l *Logger) GetLevel() LogLevel {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// SetOutput sets the output destination for the logger.
func (l *Logger) SetOutput(output io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = output
}

// SetPrefix sets the prefix for all log messages.
func (l *Logger) SetPrefix(prefix string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prefix = prefix
}

// Enable enables logging.
func (l *Logger) Enable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = true
}

// Disable disables logging.
func (l *Logger) Disable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = false
}

// IsEnabled returns true if logging is enabled.
func (l *Logger) IsEnabled() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.enabled
}

// shouldLog returns true if a message at the given level should be logged.
func (l *Logger) shouldLog(level LogLevel) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.enabled && level >= l.level
}

// log writes a log message at the specified level.
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if !l.shouldLog(level) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006/01/02 15:04:05.000000")
	message := fmt.Sprintf(format, args...)

	var output string
	if l.prefix != "" {
		output = fmt.Sprintf("%s [%s] %s: %s\n", timestamp, level, l.prefix, message)
	} else {
		output = fmt.Sprintf("%s [%s] %s\n", timestamp, level, message)
	}

	fmt.Fprint(l.output, output)
}

// logWithFields writes a log message with structured fields.
func (l *Logger) logWithFields(level LogLevel, message string, fields map[string]interface{}) {
	if !l.shouldLog(level) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006/01/02 15:04:05.000000")

	var output string
	if l.prefix != "" {
		output = fmt.Sprintf("%s [%s] %s: %s", timestamp, level, l.prefix, message)
	} else {
		output = fmt.Sprintf("%s [%s] %s", timestamp, level, message)
	}

	// Add fields
	if len(fields) > 0 {
		output += " |"
		for key, value := range fields {
			output += fmt.Sprintf(" %s=%v", key, value)
		}
	}
	output += "\n"

	fmt.Fprint(l.output, output)
}

// Trace logs a message at TRACE level (most verbose).
func (l *Logger) Trace(format string, args ...interface{}) {
	l.log(LevelTrace, format, args...)
}

// TraceFields logs a message with fields at TRACE level.
func (l *Logger) TraceFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelTrace, message, fields)
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

// DebugFields logs a message with fields at DEBUG level.
func (l *Logger) DebugFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelDebug, message, fields)
}

// Info logs a message at INFO level.
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

// InfoFields logs a message with fields at INFO level.
func (l *Logger) InfoFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelInfo, message, fields)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(LevelWarn, format, args...)
}

// WarnFields logs a message with fields at WARN level.
func (l *Logger) WarnFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelWarn, message, fields)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

// ErrorFields logs a message with fields at ERROR level.
func (l *Logger) ErrorFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelError, message, fields)
}

// Fatal logs a message at FATAL level and exits the program.
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(LevelFatal, format, args...)
	os.Exit(1)
}

// FatalFields logs a message with fields at FATAL level and exits the program.
func (l *Logger) FatalFields(message string, fields map[string]interface{}) {
	l.logWithFields(LevelFatal, message, fields)
	os.Exit(1)
}

// Close closes the logger's output if it's a file.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.fileMode {
		if closer, ok := l.output.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
}

// Package-level convenience functions that use the default logger

// SetLevel sets the log level for the default logger.
func SetLevel(level LogLevel) {
	defaultLogger.SetLevel(level)
}

// Trace logs a trace message using the default logger.
func Trace(format string, args ...interface{}) {
	defaultLogger.Trace(format, args...)
}

// Debug logs a debug message using the default logger.
func Debug(format string, args ...interface{}) {
	defaultLogger.Debug(format, args...)
}

// Info logs an info message using the default logger.
func Info(format string, args ...interface{}) {
	defaultLogger.Info(format, args...)
}

// Warn logs a warning message using the default logger.
func Warn(format string, args ...interface{}) {
	defaultLogger.Warn(format, args...)
}

// Error logs an error message using the default logger.
func Error(format string, args ...interface{}) {
	defaultLogger.Error(format, args...)
}

// Fatal logs a fatal message using the default logger and exits.
func Fatal(format string, args ...interface{}) {
	defaultLogger.Fatal(format, args...)
}
