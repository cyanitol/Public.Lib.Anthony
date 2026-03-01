package observability_test

import (
	"bytes"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/observability"
)

// ExampleNewLogger demonstrates creating a custom logger.
func ExampleNewLogger() {
	// Create a logger with Debug level, writing to a buffer in text format
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.DebugLevel,
		buf,
		observability.TextFormat,
	)

	logger.Debug("Debug message")
	logger.Info("Info message")
	// Both messages will be logged since the level is set to Debug
}

// ExampleLogger_Error demonstrates logging an error with structured fields.
func ExampleLogger_Error() {
	// Create a logger that outputs errors with structured fields
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.ErrorLevel,
		buf,
		observability.TextFormat,
	)

	logger.Error("Database connection failed", observability.Fields{
		"host":    "localhost",
		"port":    5432,
		"timeout": "5s",
	})
	// Output will contain: ERROR: Database connection failed | host=localhost port=5432 timeout=5s
}

// ExampleLogger_SetLevel demonstrates changing the log level.
func ExampleLogger_SetLevel() {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.TextFormat,
	)

	// This will not be logged (below Info level)
	logger.Debug("Debug message")

	// Change to Debug level
	logger.SetLevel(observability.DebugLevel)

	// Now this will be logged
	logger.Debug("Debug message now visible")
}

// ExampleLogger_SetFormat demonstrates switching between text and JSON formats.
func ExampleLogger_SetFormat() {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.TextFormat,
	)

	// Log in text format
	logger.Info("Text format message")

	// Switch to JSON format
	logger.SetFormat(observability.JSONFormat)
	logger.Info("JSON format message")
	// Output will show both formats (text then JSON)
}

// ExampleGetLogger demonstrates using the global logger.
func ExampleGetLogger() {
	// Get the global logger instance
	logger := observability.GetLogger()

	// Configure it
	logger.SetLevel(observability.DebugLevel)

	// Use it
	logger.Info("Using the global logger")
}

// ExampleSetLogger demonstrates setting a custom global logger.
func ExampleSetLogger() {
	// Create a custom logger with JSON output
	buf := &bytes.Buffer{}
	customLogger := observability.NewLogger(
		observability.TraceLevel,
		buf,
		observability.JSONFormat,
	)

	// Set it as the global logger
	observability.SetLogger(customLogger)

	// Now all global logging functions use the custom logger
	observability.Info("This will be logged as JSON")
}

// Example demonstrates the most common usage pattern.
func Example() {
	// Create a logger for your application
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.TextFormat,
	)

	// Log at different levels
	logger.Trace("Very detailed trace information")   // Won't be logged (below Info)
	logger.Debug("Debug information")                 // Won't be logged (below Info)
	logger.Info("Application started")                // Will be logged
	logger.Warn("Configuration file not found")       // Will be logged
	logger.Error("Failed to connect to database")     // Will be logged

	// Log with structured fields
	logger.Info("User logged in", observability.Fields{
		"user_id":  12345,
		"username": "john_doe",
		"ip":       "192.168.1.1",
	})

	// Change log level dynamically
	logger.SetLevel(observability.DebugLevel)
	logger.Debug("Now debug messages are visible")

	// Switch to JSON format for structured logging
	logger.SetFormat(observability.JSONFormat)
	logger.Info("This will be output as JSON", observability.Fields{
		"event": "user_action",
		"action": "file_upload",
	})
}

// ExampleFields demonstrates working with structured fields.
func ExampleFields() {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.TextFormat,
	)

	// Single fields map
	logger.Info("Processing request", observability.Fields{
		"method": "GET",
		"path":   "/api/users",
		"status": 200,
	})

	// Multiple fields maps (they will be merged)
	requestFields := observability.Fields{
		"method": "POST",
		"path":   "/api/users",
	}
	responseFields := observability.Fields{
		"status":   201,
		"duration": "45ms",
	}
	logger.Info("Request completed", requestFields, responseFields)
	// Output will contain both log entries with their respective fields
}

// ExampleLevel demonstrates the log level hierarchy.
func ExampleLevel() {
	// Log levels from most verbose to least verbose:
	fmt.Println("TraceLevel:", observability.TraceLevel)
	fmt.Println("DebugLevel:", observability.DebugLevel)
	fmt.Println("InfoLevel:", observability.InfoLevel)
	fmt.Println("WarnLevel:", observability.WarnLevel)
	fmt.Println("ErrorLevel:", observability.ErrorLevel)
	// Output:
	// TraceLevel: TRACE
	// DebugLevel: DEBUG
	// InfoLevel: INFO
	// WarnLevel: WARN
	// ErrorLevel: ERROR
}

// ExampleTextFormat demonstrates text format output.
// Text format produces human-readable logs with timestamps, levels, and optional fields.
func ExampleTextFormat() {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.TextFormat,
	)

	logger.Info("Simple message")
	logger.Warn("Warning message", observability.Fields{
		"code": 1001,
		"detail": "Something needs attention",
	})

	// Output format: [timestamp] LEVEL: message | key=value key=value
	// Example: [2026-02-28T10:30:45.123-05:00] INFO: Simple message
	// Example: [2026-02-28T10:30:45.124-05:00] WARN: Warning message | code=1001 detail=Something needs attention
}

// ExampleJSONFormat demonstrates JSON format output.
// JSON format produces structured logs suitable for log aggregation systems.
// The output will be a JSON object with timestamp, level, message, and custom fields.
// Example: {"timestamp":"2026-02-28T10:30:45.123-05:00","level":"INFO","message":"Operation completed","operation":"database_backup","duration_ms":1234,"success":true}
func ExampleJSONFormat() {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(
		observability.InfoLevel,
		buf,
		observability.JSONFormat,
	)

	logger.Info("Operation completed", observability.Fields{
		"operation": "database_backup",
		"duration_ms": 1234,
		"success": true,
	})
}
