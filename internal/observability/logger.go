// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package observability provides structured logging and observability features
// for the SQLite driver implementation.
package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents the severity level of a log entry.
type Level int

const (
	// TraceLevel is the most verbose logging level, for very detailed debugging.
	TraceLevel Level = iota
	// DebugLevel is for development and debugging information.
	DebugLevel
	// InfoLevel is for operational information.
	InfoLevel
	// WarnLevel is for important issues that should be noted.
	WarnLevel
	// ErrorLevel is for errors that should always be logged.
	ErrorLevel
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case TraceLevel:
		return "TRACE"
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Fields represents structured logging fields as key-value pairs.
type Fields map[string]interface{}

// Logger is the interface for structured logging with multiple severity levels.
type Logger interface {
	// Error logs an error-level message with optional structured fields.
	Error(msg string, fields ...Fields)
	// Warn logs a warning-level message with optional structured fields.
	Warn(msg string, fields ...Fields)
	// Info logs an info-level message with optional structured fields.
	Info(msg string, fields ...Fields)
	// Debug logs a debug-level message with optional structured fields.
	Debug(msg string, fields ...Fields)
	// Trace logs a trace-level message with optional structured fields.
	Trace(msg string, fields ...Fields)

	// SetLevel sets the minimum log level threshold.
	SetLevel(level Level)
	// GetLevel returns the current log level threshold.
	GetLevel() Level
	// SetOutput sets the output writer for log messages.
	SetOutput(w io.Writer)
	// SetFormat sets the output format (text or json).
	SetFormat(format OutputFormat)
}

// OutputFormat specifies how log entries should be formatted.
type OutputFormat int

const (
	// TextFormat outputs human-readable text logs.
	TextFormat OutputFormat = iota
	// JSONFormat outputs structured JSON logs.
	JSONFormat
)

// defaultLogger is the default implementation of the Logger interface.
type defaultLogger struct {
	mu     sync.Mutex
	level  Level
	output io.Writer
	format OutputFormat
}

// NewLogger creates a new Logger instance with the specified configuration.
func NewLogger(level Level, output io.Writer, format OutputFormat) Logger {
	if output == nil {
		output = os.Stderr
	}
	return &defaultLogger{
		level:  level,
		output: output,
		format: format,
	}
}

// Error logs an error-level message.
func (l *defaultLogger) Error(msg string, fields ...Fields) {
	l.log(ErrorLevel, msg, fields...)
}

// Warn logs a warning-level message.
func (l *defaultLogger) Warn(msg string, fields ...Fields) {
	l.log(WarnLevel, msg, fields...)
}

// Info logs an info-level message.
func (l *defaultLogger) Info(msg string, fields ...Fields) {
	l.log(InfoLevel, msg, fields...)
}

// Debug logs a debug-level message.
func (l *defaultLogger) Debug(msg string, fields ...Fields) {
	l.log(DebugLevel, msg, fields...)
}

// Trace logs a trace-level message.
func (l *defaultLogger) Trace(msg string, fields ...Fields) {
	l.log(TraceLevel, msg, fields...)
}

// SetLevel sets the minimum log level threshold.
func (l *defaultLogger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current log level threshold.
func (l *defaultLogger) GetLevel() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// SetOutput sets the output writer for log messages.
func (l *defaultLogger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w == nil {
		w = os.Stderr
	}
	l.output = w
}

// SetFormat sets the output format.
func (l *defaultLogger) SetFormat(format OutputFormat) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.format = format
}

// log is the internal logging function that handles level checking and formatting.
func (l *defaultLogger) log(level Level, msg string, fields ...Fields) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if this message should be logged based on level threshold
	if level < l.level {
		return
	}

	// Merge all fields into one map
	mergedFields := make(Fields)
	for _, f := range fields {
		for k, v := range f {
			mergedFields[k] = v
		}
	}

	var output string
	switch l.format {
	case JSONFormat:
		output = l.formatJSON(level, msg, mergedFields)
	default:
		output = l.formatText(level, msg, mergedFields)
	}

	fmt.Fprintln(l.output, output)
}

// formatText formats a log entry as human-readable text.
func (l *defaultLogger) formatText(level Level, msg string, fields Fields) string {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z07:00")
	result := fmt.Sprintf("[%s] %s: %s", timestamp, level.String(), msg)

	if len(fields) > 0 {
		result += " |"
		for k, v := range fields {
			result += fmt.Sprintf(" %s=%v", k, v)
		}
	}

	return result
}

// formatJSON formats a log entry as JSON.
func (l *defaultLogger) formatJSON(level Level, msg string, fields Fields) string {
	entry := map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"level":     level.String(),
		"message":   msg,
	}

	// Add all structured fields to the entry
	for k, v := range fields {
		entry[k] = v
	}

	data, err := json.Marshal(entry)
	if err != nil {
		// Fallback to text format if JSON marshaling fails
		return l.formatText(level, msg, fields)
	}

	return string(data)
}

// Global logger instance
var (
	globalLogger     Logger
	globalLoggerOnce sync.Once
)

// initGlobalLogger initializes the global logger with default settings.
func initGlobalLogger() {
	globalLogger = NewLogger(InfoLevel, os.Stderr, TextFormat)
}

// GetLogger returns the global logger instance.
func GetLogger() Logger {
	globalLoggerOnce.Do(initGlobalLogger)
	return globalLogger
}

// SetLogger sets the global logger instance.
func SetLogger(logger Logger) {
	globalLoggerOnce.Do(func() {}) // Ensure initialization happens
	globalLogger = logger
}

// Convenience functions that use the global logger

// Error logs an error-level message using the global logger.
func Error(msg string, fields ...Fields) {
	GetLogger().Error(msg, fields...)
}

// Warn logs a warning-level message using the global logger.
func Warn(msg string, fields ...Fields) {
	GetLogger().Warn(msg, fields...)
}

// Info logs an info-level message using the global logger.
func Info(msg string, fields ...Fields) {
	GetLogger().Info(msg, fields...)
}

// Debug logs a debug-level message using the global logger.
func Debug(msg string, fields ...Fields) {
	GetLogger().Debug(msg, fields...)
}

// Trace logs a trace-level message using the global logger.
func Trace(msg string, fields ...Fields) {
	GetLogger().Trace(msg, fields...)
}

// SetLevel sets the log level for the global logger.
func SetLevel(level Level) {
	GetLogger().SetLevel(level)
}

// SetOutput sets the output writer for the global logger.
func SetOutput(w io.Writer) {
	GetLogger().SetOutput(w)
}

// SetFormat sets the output format for the global logger.
func SetFormat(format OutputFormat) {
	GetLogger().SetFormat(format)
}
