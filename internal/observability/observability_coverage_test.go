// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package observability

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// TestFormatJSON_MarshalError covers the json.Marshal fallback branch.
// json.Marshal fails on values that contain a chan, func, etc.
// We can trigger this by setting a field containing an un-marshalable value
// indirectly via the Fields map on a logger with a custom writer.
func TestFormatJSON_MarshalError(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(TraceLevel, &buf, JSONFormat).(*defaultLogger)

	// Pass a channel as a field value — json.Marshal will error on this.
	ch := make(chan int)
	l.log(InfoLevel, "test message", Fields{"bad_field": ch})

	output := buf.String()
	if !strings.Contains(output, "json_error=") {
		t.Errorf("expected json_error fallback in output, got: %q", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("expected original message in fallback output, got: %q", output)
	}
}

// TestInitGlobalLogger covers the initGlobalLogger function which is called
// lazily through sync.Once. We reset the global state to allow it to be
// exercised again.
func TestInitGlobalLogger(t *testing.T) {
	// Save and restore global state around the test
	origLogger := globalLogger
	origOnce := globalLoggerOnce
	defer func() {
		globalLogger = origLogger
		globalLoggerOnce = origOnce
	}()

	// Reset so initGlobalLogger runs again
	globalLogger = nil
	globalLoggerOnce = sync.Once{}

	// GetLogger triggers initGlobalLogger via sync.Once
	lg := GetLogger()
	if lg == nil {
		t.Fatal("GetLogger() returned nil after reset")
	}
	if lg.GetLevel() != InfoLevel {
		t.Errorf("expected InfoLevel default, got %v", lg.GetLevel())
	}
}
