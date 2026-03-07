// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// TestLevelString verifies that Level.String() returns correct values.
func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{TraceLevel, "TRACE"},
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{Level(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("Level.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNewLogger verifies that NewLogger creates a logger with correct defaults.
func TestNewLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, TextFormat)

	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}

	if logger.GetLevel() != InfoLevel {
		t.Errorf("NewLogger level = %v, want %v", logger.GetLevel(), InfoLevel)
	}
}

// TestNewLoggerNilOutput verifies that NewLogger handles nil output.
func TestNewLoggerNilOutput(t *testing.T) {
	logger := NewLogger(InfoLevel, nil, TextFormat)
	if logger == nil {
		t.Fatal("NewLogger with nil output returned nil")
	}

	// Should not panic when logging
	logger.Info("test message")
}

// TestLogLevels verifies that log level filtering works correctly.
func TestLogLevels(t *testing.T) {
	tests := []struct {
		name      string
		setLevel  Level
		logLevel  Level
		shouldLog bool
	}{
		{"Trace with Trace level", TraceLevel, TraceLevel, true},
		{"Debug with Trace level", TraceLevel, DebugLevel, true},
		{"Info with Trace level", TraceLevel, InfoLevel, true},
		{"Trace with Info level", InfoLevel, TraceLevel, false},
		{"Debug with Info level", InfoLevel, DebugLevel, false},
		{"Info with Info level", InfoLevel, InfoLevel, true},
		{"Warn with Info level", InfoLevel, WarnLevel, true},
		{"Error with Info level", InfoLevel, ErrorLevel, true},
		{"Info with Error level", ErrorLevel, InfoLevel, false},
		{"Error with Error level", ErrorLevel, ErrorLevel, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := NewLogger(tt.setLevel, buf, TextFormat)

			// Log based on the log level
			switch tt.logLevel {
			case TraceLevel:
				logger.Trace("test message")
			case DebugLevel:
				logger.Debug("test message")
			case InfoLevel:
				logger.Info("test message")
			case WarnLevel:
				logger.Warn("test message")
			case ErrorLevel:
				logger.Error("test message")
			}

			hasOutput := buf.Len() > 0
			if hasOutput != tt.shouldLog {
				t.Errorf("Log output present = %v, want %v", hasOutput, tt.shouldLog)
			}
		})
	}
}

// TestTextFormat verifies that text format output is correct.
func TestTextFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, TextFormat)

	logger.Info("test message")
	output := buf.String()

	if !strings.Contains(output, "INFO") {
		t.Errorf("Output missing INFO level: %s", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("Output missing message: %s", output)
	}
}

// TestTextFormatWithFields verifies that text format includes structured fields.
func TestTextFormatWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, TextFormat)

	logger.Info("test message", Fields{
		"key1": "value1",
		"key2": 42,
	})
	output := buf.String()

	if !strings.Contains(output, "test message") {
		t.Errorf("Output missing message: %s", output)
	}
	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Output missing key1 field: %s", output)
	}
	if !strings.Contains(output, "key2=42") {
		t.Errorf("Output missing key2 field: %s", output)
	}
}

// TestJSONFormat verifies that JSON format output is valid.
func TestJSONFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, JSONFormat)

	logger.Info("test message")
	output := strings.TrimSpace(buf.String())

	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse JSON output: %v\nOutput: %s", err, output)
	}

	if entry["level"] != "INFO" {
		t.Errorf("JSON level = %v, want INFO", entry["level"])
	}
	if entry["message"] != "test message" {
		t.Errorf("JSON message = %v, want 'test message'", entry["message"])
	}
	if _, ok := entry["timestamp"]; !ok {
		t.Error("JSON missing timestamp field")
	}
}

// TestJSONFormatWithFields verifies that JSON format includes structured fields.
func TestJSONFormatWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, JSONFormat)

	logger.Info("test message", Fields{
		"key1": "value1",
		"key2": float64(42), // JSON numbers are floats
		"key3": true,
	})
	output := strings.TrimSpace(buf.String())

	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	if entry["message"] != "test message" {
		t.Errorf("JSON message = %v, want 'test message'", entry["message"])
	}
	if entry["key1"] != "value1" {
		t.Errorf("JSON key1 = %v, want 'value1'", entry["key1"])
	}
	if entry["key2"] != float64(42) {
		t.Errorf("JSON key2 = %v, want 42", entry["key2"])
	}
	if entry["key3"] != true {
		t.Errorf("JSON key3 = %v, want true", entry["key3"])
	}
}

// TestMultipleFields verifies that multiple Fields maps are merged.
func TestMultipleFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, TextFormat)

	logger.Info("test message",
		Fields{"key1": "value1"},
		Fields{"key2": "value2"},
		Fields{"key3": "value3"},
	)
	output := buf.String()

	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Output missing key1: %s", output)
	}
	if !strings.Contains(output, "key2=value2") {
		t.Errorf("Output missing key2: %s", output)
	}
	if !strings.Contains(output, "key3=value3") {
		t.Errorf("Output missing key3: %s", output)
	}
}

// TestSetLevel verifies that SetLevel updates the log level.
func TestSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, TextFormat)

	// Should not log at Debug level initially
	logger.Debug("debug message")
	if buf.Len() > 0 {
		t.Error("Debug message logged when level is Info")
	}

	// Change to Debug level
	logger.SetLevel(DebugLevel)
	logger.Debug("debug message")
	if buf.Len() == 0 {
		t.Error("Debug message not logged when level is Debug")
	}
}

// TestGetLevel verifies that GetLevel returns the current level.
func TestGetLevel(t *testing.T) {
	logger := NewLogger(WarnLevel, nil, TextFormat)
	if logger.GetLevel() != WarnLevel {
		t.Errorf("GetLevel() = %v, want %v", logger.GetLevel(), WarnLevel)
	}

	logger.SetLevel(DebugLevel)
	if logger.GetLevel() != DebugLevel {
		t.Errorf("GetLevel() after SetLevel = %v, want %v", logger.GetLevel(), DebugLevel)
	}
}

// TestSetOutput verifies that SetOutput changes the output writer.
func TestSetOutput(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	logger := NewLogger(InfoLevel, buf1, TextFormat)
	logger.Info("message 1")

	if buf1.Len() == 0 {
		t.Error("First buffer should have output")
	}

	logger.SetOutput(buf2)
	logger.Info("message 2")

	if !strings.Contains(buf1.String(), "message 1") {
		t.Error("First buffer missing 'message 1'")
	}
	if strings.Contains(buf1.String(), "message 2") {
		t.Error("First buffer should not contain 'message 2'")
	}
	if !strings.Contains(buf2.String(), "message 2") {
		t.Error("Second buffer missing 'message 2'")
	}
}

// TestSetOutputNil verifies that SetOutput handles nil gracefully.
func TestSetOutputNil(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, TextFormat)
	logger.SetOutput(nil)

	// Should not panic
	logger.Info("test message")
}

// TestSetFormat verifies that SetFormat changes the output format.
func TestSetFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, TextFormat)

	logger.Info("message 1")
	output1 := buf.String()

	// Should be text format
	if strings.HasPrefix(output1, "{") {
		t.Error("First message should not be JSON")
	}

	buf.Reset()
	logger.SetFormat(JSONFormat)
	logger.Info("message 2")
	output2 := strings.TrimSpace(buf.String())

	// Should be JSON format
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(output2), &entry); err != nil {
		t.Errorf("Second message should be valid JSON: %v", err)
	}
}

// TestAllLevels verifies that all log levels work correctly.
func TestAllLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, TextFormat)

	logger.Trace("trace message")
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	output := buf.String()
	expectedSubstrings := []string{
		"TRACE", "trace message",
		"DEBUG", "debug message",
		"INFO", "info message",
		"WARN", "warn message",
		"ERROR", "error message",
	}

	for _, substr := range expectedSubstrings {
		if !strings.Contains(output, substr) {
			t.Errorf("Output missing '%s':\n%s", substr, output)
		}
	}
}

// TestConcurrency verifies that the logger is safe for concurrent use.
func TestConcurrency(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(TraceLevel, buf, TextFormat)

	const numGoroutines = 100
	const numMessages = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				logger.Info("concurrent message", Fields{
					"goroutine": id,
					"message":   j,
				})
			}
		}(i)
	}

	wg.Wait()

	// Verify we got the expected number of lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	expectedLines := numGoroutines * numMessages
	if len(lines) != expectedLines {
		t.Errorf("Got %d log lines, want %d", len(lines), expectedLines)
	}
}

// TestGlobalLogger verifies that the global logger functions work.
func TestGlobalLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	customLogger := NewLogger(TraceLevel, buf, TextFormat)
	SetLogger(customLogger)

	// Use global functions
	Info("global info message")
	Debug("global debug message")

	output := buf.String()
	if !strings.Contains(output, "global info message") {
		t.Error("Global Info() did not log message")
	}
	if !strings.Contains(output, "global debug message") {
		t.Error("Global Debug() did not log message")
	}
}

// TestGlobalLoggerFunctions verifies all global logger functions.
func TestGlobalLoggerFunctions(t *testing.T) {
	buf := &bytes.Buffer{}
	customLogger := NewLogger(TraceLevel, buf, TextFormat)
	SetLogger(customLogger)

	Trace("trace msg")
	Debug("debug msg")
	Info("info msg")
	Warn("warn msg")
	Error("error msg")

	output := buf.String()
	messages := []string{"trace msg", "debug msg", "info msg", "warn msg", "error msg"}
	for _, msg := range messages {
		if !strings.Contains(output, msg) {
			t.Errorf("Global functions missing '%s'", msg)
		}
	}
}

// TestGlobalSetLevel verifies that global SetLevel works.
func TestGlobalSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	customLogger := NewLogger(InfoLevel, buf, TextFormat)
	SetLogger(customLogger)

	Debug("should not appear")
	if buf.Len() > 0 {
		t.Error("Debug message logged when level is Info")
	}

	SetLevel(DebugLevel)
	Debug("should appear")
	if buf.Len() == 0 {
		t.Error("Debug message not logged after SetLevel(DebugLevel)")
	}
}

// TestGlobalSetOutput verifies that global SetOutput works.
func TestGlobalSetOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	customLogger := NewLogger(InfoLevel, &bytes.Buffer{}, TextFormat)
	SetLogger(customLogger)

	SetOutput(buf)
	Info("test message")

	if !strings.Contains(buf.String(), "test message") {
		t.Error("Global SetOutput did not redirect output")
	}
}

// TestGlobalSetFormat verifies that global SetFormat works.
func TestGlobalSetFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	customLogger := NewLogger(InfoLevel, buf, TextFormat)
	SetLogger(customLogger)

	SetFormat(JSONFormat)
	Info("json message")

	var entry map[string]interface{}
	output := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Errorf("Global SetFormat did not produce JSON: %v", err)
	}
}

// TestGetLogger verifies that GetLogger returns a valid logger.
func TestGetLogger(t *testing.T) {
	logger := GetLogger()
	if logger == nil {
		t.Fatal("GetLogger() returned nil")
	}

	// Should be usable
	buf := &bytes.Buffer{}
	logger.SetOutput(buf)
	logger.Info("test")

	if buf.Len() == 0 {
		t.Error("GetLogger() returned non-functional logger")
	}
}

// TestEmptyFields verifies that logging with empty fields works.
func TestEmptyFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, TextFormat)

	logger.Info("message with no fields")
	output := buf.String()

	if !strings.Contains(output, "message with no fields") {
		t.Error("Message without fields not logged correctly")
	}
}

// TestFieldsOverwrite verifies that later fields overwrite earlier ones.
func TestFieldsOverwrite(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(InfoLevel, buf, JSONFormat)

	logger.Info("test",
		Fields{"key": "value1"},
		Fields{"key": "value2"},
	)

	var entry map[string]interface{}
	output := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(output), &entry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if entry["key"] != "value2" {
		t.Errorf("Field not overwritten: got %v, want 'value2'", entry["key"])
	}
}
