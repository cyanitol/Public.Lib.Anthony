// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package security

import (
	"strings"
	"testing"
)

func TestIsSafePragma(t *testing.T) {
	tests := []struct {
		name     string
		pragma   string
		expected bool
	}{
		// Safe pragmas
		{"table_info is safe", "table_info", true},
		{"index_list is safe", "index_list", true},
		{"foreign_key_list is safe", "foreign_key_list", true},
		{"database_list is safe", "database_list", true},
		{"compile_options is safe", "compile_options", true},
		{"schema_version is safe", "schema_version", true},
		{"user_version is safe", "user_version", true},
		{"application_id is safe", "application_id", true},
		{"encoding is safe", "encoding", true},
		{"page_size is safe", "page_size", true},
		{"page_count is safe", "page_count", true},
		{"freelist_count is safe", "freelist_count", true},
		{"cache_size is safe", "cache_size", true},
		{"foreign_keys is safe", "foreign_keys", true},
		{"case_sensitive_like is safe", "case_sensitive_like", true},
		{"recursive_triggers is safe", "recursive_triggers", true},

		// Case insensitive
		{"TABLE_INFO is safe (uppercase)", "TABLE_INFO", true},
		{"Table_Info is safe (mixed case)", "Table_Info", true},

		// Operational pragmas (now safe)
		{"journal_mode is safe", "journal_mode", true},
		{"synchronous is safe", "synchronous", true},
		{"wal_autocheckpoint is safe", "wal_autocheckpoint", true},
		{"temp_store is safe", "temp_store", true},

		// Actually unsafe pragmas (not in whitelist)
		{"writable_schema is unsafe", "writable_schema", false},
		{"trusted_schema is unsafe", "trusted_schema", false},
		{"unknown pragma is unsafe", "some_unknown_pragma", false},
		{"empty string is unsafe", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSafePragma(tt.pragma)
			if result != tt.expected {
				t.Errorf("IsSafePragma(%q) = %v, want %v", tt.pragma, result, tt.expected)
			}
		})
	}
}

func TestSecurityLimitsConstants(t *testing.T) {
	// Verify that security limits are set to reasonable values
	tests := []struct {
		name     string
		value    int
		minValue int
		maxValue int
	}{
		{"MaxSQLLength", MaxSQLLength, 100_000, 10_000_000},
		{"MaxTokens", MaxTokens, 1_000, 100_000},
		{"MaxExprDepth", MaxExprDepth, 50, 1000},
		{"MaxQueryTables", MaxQueryTables, 10, 1000},
		{"MaxParameters", MaxParameters, 1_000, 100_000},
		{"MaxMemoryDBPages", MaxMemoryDBPages, 10_000, 1_000_000},
		{"MaxRecordSize", MaxRecordSize, 1_000_000, 2_000_000_000},
		{"MaxBlobSize", MaxBlobSize, 1_000_000, 2_000_000_000},
		{"MaxAttachedDBs", MaxAttachedDBs, 1, 100},
		{"MaxTriggerDepth", MaxTriggerDepth, 8, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value < tt.minValue {
				t.Errorf("%s = %d is too small (minimum: %d)", tt.name, tt.value, tt.minValue)
			}
			if tt.value > tt.maxValue {
				t.Errorf("%s = %d is too large (maximum: %d)", tt.name, tt.value, tt.maxValue)
			}
		})
	}
}

func TestSafePragmasCompleteness(t *testing.T) {
	// Ensure all pragmas in the whitelist are lowercase for consistency
	for pragma := range SafePragmas {
		if pragma != strings.ToLower(pragma) {
			t.Errorf("Pragma %q in SafePragmas should be lowercase", pragma)
		}
	}

	// Ensure we have a reasonable number of safe pragmas
	if len(SafePragmas) < 5 {
		t.Errorf("SafePragmas has too few entries (%d), expected at least 5", len(SafePragmas))
	}
	if len(SafePragmas) > 100 {
		t.Errorf("SafePragmas has too many entries (%d), expected at most 100", len(SafePragmas))
	}
}
