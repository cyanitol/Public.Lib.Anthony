// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package observability provides structured logging capabilities with multiple
// severity levels and configurable output formats.
//
// The package implements a leveled logging system with support for structured
// fields (key-value pairs) and two output formats: human-readable text and JSON.
//
// # Log Levels
//
// The package supports five log levels, from most verbose to least verbose:
//
//   - TraceLevel: Very detailed debugging information
//   - DebugLevel: Development and debugging information
//   - InfoLevel: Operational information about normal application behavior
//   - WarnLevel: Important issues that should be noted
//   - ErrorLevel: Errors that should always be logged
//
// When you set a log level, only messages at that level or higher will be output.
// For example, if you set InfoLevel, only Info, Warn, and Error messages will
// be logged; Trace and Debug messages will be suppressed.
//
// # Basic Usage
//
// Create a logger with desired configuration:
//
//	logger := observability.NewLogger(
//	    observability.InfoLevel,
//	    os.Stderr,
//	    observability.TextFormat,
//	)
//
// Log messages at different levels:
//
//	logger.Trace("Detailed trace information")
//	logger.Debug("Debug information")
//	logger.Info("Application started")
//	logger.Warn("Configuration file not found")
//	logger.Error("Failed to connect to database")
//
// # Structured Fields
//
// Add structured key-value pairs to log messages for better searchability
// and analysis:
//
//	logger.Info("User logged in", observability.Fields{
//	    "user_id": 12345,
//	    "username": "john_doe",
//	    "ip": "192.168.1.1",
//	})
//
// You can pass multiple Fields maps, and they will be merged:
//
//	requestFields := observability.Fields{"method": "POST", "path": "/api/users"}
//	responseFields := observability.Fields{"status": 201, "duration": "45ms"}
//	logger.Info("Request completed", requestFields, responseFields)
//
// # Output Formats
//
// The package supports two output formats:
//
// TextFormat produces human-readable output:
//
//	[2026-02-28T10:30:45.123-05:00] INFO: Application started | version=1.0 env=production
//
// JSONFormat produces structured JSON logs suitable for log aggregation systems:
//
//	{"timestamp":"2026-02-28T10:30:45.123456789-05:00","level":"INFO","message":"Application started","version":"1.0","env":"production"}
//
// Switch formats dynamically:
//
//	logger.SetFormat(observability.JSONFormat)
//
// # Global Logger
//
// For convenience, the package provides a global logger instance that can be
// accessed and configured using package-level functions:
//
//	// Configure the global logger
//	observability.SetLevel(observability.DebugLevel)
//	observability.SetOutput(os.Stdout)
//	observability.SetFormat(observability.JSONFormat)
//
//	// Use the global logger
//	observability.Info("Using global logger")
//	observability.Error("An error occurred", observability.Fields{
//	    "error": err.Error(),
//	    "component": "database",
//	})
//
// Or create your own logger and set it as the global instance:
//
//	customLogger := observability.NewLogger(
//	    observability.TraceLevel,
//	    logFile,
//	    observability.JSONFormat,
//	)
//	observability.SetLogger(customLogger)
//
// # Thread Safety
//
// All logger methods are safe for concurrent use by multiple goroutines.
// The logger uses internal synchronization to ensure thread-safe operation.
//
// # Dynamic Configuration
//
// Logger settings can be changed at runtime:
//
//	// Change the minimum log level
//	logger.SetLevel(observability.DebugLevel)
//
//	// Change the output destination
//	logger.SetOutput(newWriter)
//
//	// Change the output format
//	logger.SetFormat(observability.JSONFormat)
//
// # Performance Considerations
//
// Messages below the configured log level are filtered early and incur minimal
// overhead. The structured fields are only processed if the message will actually
// be logged. This makes it safe to include detailed logging calls throughout your
// code without significant performance impact when running at higher log levels.
//
// # Integration Example
//
// Here's a complete example of integrating the logger into an application:
//
//	package main
//
//	import (
//	    "os"
//	    "github.com/JuniperBible/Public.Lib.Anthony/internal/observability"
//	)
//
//	func main() {
//	    // Configure the global logger
//	    observability.SetLevel(observability.InfoLevel)
//	    observability.SetFormat(observability.TextFormat)
//
//	    observability.Info("Application starting")
//
//	    if err := runApp(); err != nil {
//	        observability.Error("Application error", observability.Fields{
//	            "error": err.Error(),
//	        })
//	        os.Exit(1)
//	    }
//
//	    observability.Info("Application shutdown complete")
//	}
//
//	func runApp() error {
//	    observability.Debug("Initializing components")
//	    // ... application logic ...
//	    return nil
//	}
package observability
