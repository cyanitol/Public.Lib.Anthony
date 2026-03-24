// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package observability

import (
	"bytes"
	"testing"
)

// TestMCDC_LogLevelFiltering exercises the compound condition in log():
//
//	if level < l.level { return }
//
// This is a single boolean condition (not compound), but it is the critical
// decision point. MC/DC requires covering both outcomes with independent
// influence. We vary `level` and `l.level` independently so that each
// sub-expression alone determines whether logging is suppressed.
//
// Condition A: level < l.level
//
//	case 1 (A=false): level >= l.level  → message IS logged
//	case 2 (A=true):  level <  l.level  → message is NOT logged
//
// To reach the complete MC/DC matrix for the level comparison we also cover
// the exact-equal boundary, where `level == l.level` must produce a log.
func TestMCDC_LogLevelFiltering(t *testing.T) {
	tests := []struct {
		name      string
		setLevel  Level
		logLevel  Level
		shouldLog bool
	}{
		// Condition A = false  (level >= l.level) → log
		{name: "MCDC_A_false_equal_boundary", setLevel: InfoLevel, logLevel: InfoLevel, shouldLog: true},
		{name: "MCDC_A_false_level_above_threshold", setLevel: InfoLevel, logLevel: WarnLevel, shouldLog: true},
		{name: "MCDC_A_false_error_above_info", setLevel: InfoLevel, logLevel: ErrorLevel, shouldLog: true},
		// Condition A = true  (level < l.level) → suppress
		{name: "MCDC_A_true_level_below_threshold", setLevel: InfoLevel, logLevel: DebugLevel, shouldLog: false},
		{name: "MCDC_A_true_trace_below_info", setLevel: InfoLevel, logLevel: TraceLevel, shouldLog: false},
		// Additional boundary: lowest threshold allows all
		{name: "MCDC_A_false_trace_threshold_trace_msg", setLevel: TraceLevel, logLevel: TraceLevel, shouldLog: true},
		// Highest threshold suppresses everything below error
		{name: "MCDC_A_true_info_below_error_threshold", setLevel: ErrorLevel, logLevel: InfoLevel, shouldLog: false},
		{name: "MCDC_A_false_error_at_error_threshold", setLevel: ErrorLevel, logLevel: ErrorLevel, shouldLog: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := NewLogger(tt.setLevel, buf, TextFormat)

			switch tt.logLevel {
			case TraceLevel:
				logger.Trace("mcdc message")
			case DebugLevel:
				logger.Debug("mcdc message")
			case InfoLevel:
				logger.Info("mcdc message")
			case WarnLevel:
				logger.Warn("mcdc message")
			case ErrorLevel:
				logger.Error("mcdc message")
			}

			logged := buf.Len() > 0
			if logged != tt.shouldLog {
				t.Errorf("logged=%v want=%v (setLevel=%v logLevel=%v)",
					logged, tt.shouldLog, tt.setLevel, tt.logLevel)
			}
		})
	}
}

// TestMCDC_FormatTextFieldsGuard exercises the condition in formatText():
//
//	if len(fields) > 0 { ... append fields ... }
//
// Condition A: len(fields) > 0
//
//	case 1 (A=false): empty fields map  → output has no " |" separator
//	case 2 (A=true):  non-empty fields  → output contains " |" separator
func TestMCDC_FormatTextFieldsGuard(t *testing.T) {
	tests := []struct {
		name     string
		fields   Fields
		wantPipe bool // whether " |" separator appears in output
	}{
		// A = false: no fields
		{name: "MCDC_A_false_no_fields", fields: Fields{}, wantPipe: false},
		{name: "MCDC_A_false_nil_fields", fields: nil, wantPipe: false},
		// A = true: at least one field
		{name: "MCDC_A_true_one_field", fields: Fields{"k": "v"}, wantPipe: true},
		{name: "MCDC_A_true_multiple_fields", fields: Fields{"k1": "v1", "k2": 2}, wantPipe: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := NewLogger(TraceLevel, buf, TextFormat)

			if tt.fields == nil {
				logger.Info("mcdc msg")
			} else {
				logger.Info("mcdc msg", tt.fields)
			}

			output := buf.String()
			hasPipe := len(output) > 0 && containsFieldSeparator(output)
			if hasPipe != tt.wantPipe {
				t.Errorf("hasPipe=%v want=%v output=%q", hasPipe, tt.wantPipe, output)
			}
		})
	}
}

// containsFieldSeparator returns true when the text-format " |" separator is present.
func containsFieldSeparator(s string) bool {
	for i := 0; i < len(s)-1; i++ {
		if s[i] == ' ' && s[i+1] == '|' {
			return true
		}
	}
	return false
}

// TestMCDC_NewLoggerNilOutputGuard exercises the nil-output guard in NewLogger():
//
//	if output == nil { output = os.Stderr }
//
// Condition A: output == nil
//
//	case 1 (A=false): non-nil writer → logger uses supplied writer
//	case 2 (A=true):  nil writer     → logger falls back to os.Stderr (no panic)
func TestMCDC_NewLoggerNilOutputGuard(t *testing.T) {
	tests := []struct {
		name      string
		useNil    bool
		wantLevel Level
	}{
		{name: "MCDC_A_false_non_nil_output", useNil: false, wantLevel: DebugLevel},
		{name: "MCDC_A_true_nil_output", useNil: true, wantLevel: DebugLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logger Logger
			if tt.useNil {
				logger = NewLogger(tt.wantLevel, nil, TextFormat)
			} else {
				buf := &bytes.Buffer{}
				logger = NewLogger(tt.wantLevel, buf, TextFormat)
			}
			if logger == nil {
				t.Fatal("NewLogger returned nil")
			}
			if got := logger.GetLevel(); got != tt.wantLevel {
				t.Errorf("GetLevel()=%v want=%v", got, tt.wantLevel)
			}
			// Must not panic when logging after nil output
			logger.Info("mcdc probe")
		})
	}
}

// TestMCDC_SetOutputNilGuard exercises the nil-writer guard in SetOutput():
//
//	if w == nil { w = os.Stderr }
//
// Condition A: w == nil
//
//	case 1 (A=false): non-nil writer supplied → subsequent log written to that writer
//	case 2 (A=true):  nil writer supplied     → falls back to os.Stderr, no panic
func TestMCDC_SetOutputNilGuard(t *testing.T) {
	tests := []struct {
		name   string
		useNil bool
	}{
		{name: "MCDC_A_false_non_nil_writer", useNil: false},
		{name: "MCDC_A_true_nil_writer", useNil: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := NewLogger(InfoLevel, buf, TextFormat)

			if tt.useNil {
				logger.SetOutput(nil)
			} else {
				newBuf := &bytes.Buffer{}
				logger.SetOutput(newBuf)
				// Confirm log goes to newBuf, not original buf
				logger.Info("mcdc probe")
				if newBuf.Len() == 0 {
					t.Error("expected output in newBuf after SetOutput(newBuf)")
				}
				return
			}
			// nil path: must not panic
			logger.Info("mcdc probe after nil SetOutput")
		})
	}
}
