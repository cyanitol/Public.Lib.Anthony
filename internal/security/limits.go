// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package security

import "strings"

const (
	// SQL parsing limits
	MaxSQLLength   = 1_000_000 // 1MB
	MaxTokens      = 10_000
	MaxExprDepth   = 100
	MaxQueryTables = 64
	MaxParameters  = 32_767

	// Memory limits
	MaxMemoryDBPages = 100_000       // ~400MB at 4KB pages
	MaxRecordSize    = 1_000_000_000 // 1GB
	MaxBlobSize      = 1_000_000_000 // 1GB

	// Operation limits
	MaxAttachedDBs  = 10
	MaxTriggerDepth = 32
)

// SafePragmas is the whitelist of allowed PRAGMA statements.
// This includes both read-only informational pragmas and common
// operational pragmas that are safe for normal use.
var SafePragmas = map[string]bool{
	// Informational (read-only)
	"table_info":        true,
	"index_list":        true,
	"index_info":        true,
	"index_xinfo":       true,
	"foreign_key_list":  true,
	"foreign_key_check": true,
	"database_list":     true,
	"compile_options":   true,
	"collation_list":    true,
	"data_version":      true,
	"integrity_check":   true,
	"quick_check":       true,
	"table_list":        true,
	"function_list":     true,
	"module_list":       true,
	"pragma_list":       true,
	// Configuration (safe to modify)
	"schema_version":      true,
	"user_version":        true,
	"application_id":      true,
	"encoding":            true,
	"page_size":           true,
	"page_count":          true,
	"freelist_count":      true,
	"cache_size":          true,
	"cache_spill":         true,
	"foreign_keys":        true,
	"case_sensitive_like": true,
	"recursive_triggers":  true,
	"journal_mode":        true,
	"synchronous":         true,
	"temp_store":          true,
	"busy_timeout":        true,
	"auto_vacuum":         true,
	"secure_delete":       true,
	"wal_checkpoint":      true,
	"wal_autocheckpoint":  true,
	"mmap_size":           true,
	"max_page_count":      true,
	"locking_mode":        true,
	"query_only":          true,
	"read_uncommitted":    true,
	"cell_size_check":     true,
	// Note: Dangerous pragmas NOT included:
	// - writable_schema (allows schema corruption)
	// - ignore_check_constraints (bypasses constraints)
	// - legacy_file_format (compatibility issues)
	// - trusted_schema (security bypass)
}

// IsSafePragma checks if a PRAGMA name is in the whitelist.
// This prevents dangerous or write operations via PRAGMA.
func IsSafePragma(name string) bool {
	return SafePragmas[strings.ToLower(name)]
}
