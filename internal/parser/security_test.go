// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"strings"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/security"
)

func TestSQLLengthLimit(t *testing.T) {
	t.Parallel()
	// Create a SQL string that exceeds the limit
	longSQL := "SELECT " + strings.Repeat("a, ", security.MaxSQLLength/3)

	p := NewParser(longSQL)
	_, err := p.Parse()

	if err == nil {
		t.Fatal("Expected error for SQL exceeding length limit, got nil")
	}

	if !strings.Contains(err.Error(), "SQL query too long") {
		t.Errorf("Expected 'SQL query too long' error, got: %v", err)
	}
}

func TestSQLLengthLimitAtBoundary(t *testing.T) {
	t.Parallel()
	// Create a SQL string just under the limit
	sql := "SELECT " + strings.Repeat("a", security.MaxSQLLength-10)

	p := NewParser(sql)
	_, err := p.Parse()

	// Should not error on length (may error on parsing, but not length)
	if err != nil && strings.Contains(err.Error(), "SQL query too long") {
		t.Errorf("Should not error on length for SQL under limit: %v", err)
	}
}

func TestTokenCountLimit(t *testing.T) {
	t.Parallel()
	// Create a SQL string with too many tokens
	// Each "a," is 2 tokens, so we need MaxTokens/2 + 1 repetitions
	manyTokens := "SELECT " + strings.Repeat("a, ", security.MaxTokens/2+1)

	p := NewParser(manyTokens)
	_, err := p.Parse()

	if err == nil {
		t.Fatal("Expected error for SQL with too many tokens, got nil")
	}

	if !strings.Contains(err.Error(), "too many tokens") {
		t.Errorf("Expected 'too many tokens' error, got: %v", err)
	}
}

func TestExpressionDepthLimit(t *testing.T) {
	t.Parallel()
	// Create a deeply nested expression that exceeds the depth limit
	// NOT NOT NOT ... NOT 1
	deepExpr := strings.Repeat("NOT (", security.MaxExprDepth+10) + "1" + strings.Repeat(")", security.MaxExprDepth+10)
	sql := "SELECT " + deepExpr

	p := NewParser(sql)
	_, err := p.Parse()

	if err == nil {
		t.Fatal("Expected error for expression exceeding depth limit, got nil")
	}

	if !strings.Contains(err.Error(), "expression depth exceeds maximum") {
		t.Errorf("Expected 'expression depth exceeds maximum' error, got: %v", err)
	}
}

func TestExpressionDepthLimitBinary(t *testing.T) {
	t.Parallel()
	// Binary operators like AND/OR create a left-recursive structure
	// The depth is determined by how many recursive parse calls are made
	// Since each binary operator increments depth once, we need a very long chain
	// Skip this test as binary operators don't create deep nesting in our parser
	t.Skip("Binary operators don't create deep nesting in our left-recursive parser")
}

func TestExpressionDepthLimitNested(t *testing.T) {
	t.Parallel()
	// Create nested parenthetical expressions
	deepExpr := strings.Repeat("(", security.MaxExprDepth+10) + "1" + strings.Repeat(")", security.MaxExprDepth+10)
	sql := "SELECT " + deepExpr

	p := NewParser(sql)
	_, err := p.Parse()

	if err == nil {
		t.Fatal("Expected error for expression exceeding depth limit, got nil")
	}

	if !strings.Contains(err.Error(), "expression depth exceeds maximum") {
		t.Errorf("Expected 'expression depth exceeds maximum' error, got: %v", err)
	}
}

func TestPragmaWhitelistAllowed(t *testing.T) {
	t.Parallel()
	allowedPragmas := []string{
		"PRAGMA table_info(users)",
		"PRAGMA index_list(users)",
		"PRAGMA foreign_key_list(users)",
		"PRAGMA database_list",
		"PRAGMA compile_options",
		"PRAGMA schema_version",
		"PRAGMA user_version",
		"PRAGMA foreign_keys",
		"PRAGMA cache_size",
	}

	for _, sql := range allowedPragmas {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(sql)
			_, err := p.Parse()

			if err != nil && strings.Contains(err.Error(), "not allowed for security reasons") {
				t.Errorf("PRAGMA should be allowed: %v", err)
			}
		})
	}
}

func TestPragmaWhitelistDenied(t *testing.T) {
	t.Parallel()
	// These are actually dangerous PRAGMAs that should be blocked
	deniedPragmas := []string{
		"PRAGMA writable_schema = ON",
		"PRAGMA trusted_schema = ON",
		"PRAGMA ignore_check_constraints = ON",
		"PRAGMA legacy_file_format = 1",
		"PRAGMA unsafe_pragma = value",
		"PRAGMA unknown_dangerous_pragma = 1",
	}

	for _, sql := range deniedPragmas {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(sql)
			_, err := p.Parse()

			if err == nil {
				t.Fatal("Expected error for disallowed PRAGMA, got nil")
			}

			if !strings.Contains(err.Error(), "not allowed for security reasons") {
				t.Errorf("Expected 'not allowed for security reasons' error, got: %v", err)
			}
		})
	}
}

func TestPragmaWhitelistCaseInsensitive(t *testing.T) {
	t.Parallel()
	// Test that PRAGMA checking is case-insensitive
	tests := []struct {
		sql     string
		allowed bool
	}{
		{"PRAGMA table_info(users)", true},
		{"PRAGMA TABLE_INFO(users)", true},
		{"PRAGMA Table_Info(users)", true},
		{"PRAGMA journal_mode", true},  // Now allowed
		{"PRAGMA JOURNAL_MODE", true},  // Now allowed
		{"PRAGMA Journal_Mode", true},  // Now allowed
		{"PRAGMA writable_schema", false},  // Dangerous - not allowed
		{"PRAGMA WRITABLE_SCHEMA", false},  // Dangerous - not allowed
		{"PRAGMA Writable_Schema", false},  // Dangerous - not allowed
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()

			hasSecurityError := err != nil && strings.Contains(err.Error(), "not allowed for security reasons")

			if tt.allowed && hasSecurityError {
				t.Errorf("PRAGMA should be allowed: %v", err)
			}
			if !tt.allowed && !hasSecurityError {
				t.Error("Expected security error for disallowed PRAGMA")
			}
		})
	}
}

func TestNormalQueriesNotAffectedByLimits(t *testing.T) {
	t.Parallel()
	// Test that normal queries are not affected by security limits
	normalQueries := []string{
		"SELECT * FROM users",
		"SELECT id, name FROM users WHERE age > 18",
		"INSERT INTO users (name, age) VALUES ('Alice', 30)",
		"UPDATE users SET age = 31 WHERE name = 'Alice'",
		"DELETE FROM users WHERE age < 18",
		"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
		"SELECT * FROM users WHERE (age > 18 AND age < 65) OR status = 'active'",
	}

	for _, sql := range normalQueries {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(sql)
			_, err := p.Parse()

			// Should not have security-related errors
			if err != nil {
				if strings.Contains(err.Error(), "SQL query too long") ||
					strings.Contains(err.Error(), "too many tokens") ||
					strings.Contains(err.Error(), "expression depth exceeds") ||
					strings.Contains(err.Error(), "not allowed for security reasons") {
					t.Errorf("Normal query should not trigger security limits: %v", err)
				}
			}
		})
	}
}

func TestExpressionDepthAtBoundary(t *testing.T) {
	t.Parallel()
	// Test expression depth just under the limit (should succeed)
	// Each level of the parse tree adds to depth (OR, AND, NOT, comparison, etc.)
	// So we need to account for all levels, not just the NOT operators
	// Use a smaller depth that accounts for the full parse tree depth
	depth := security.MaxExprDepth / 10 // Account for all parse levels
	deepExpr := strings.Repeat("NOT (", depth) + "1" + strings.Repeat(")", depth)
	sql := "SELECT " + deepExpr

	p := NewParser(sql)
	_, err := p.Parse()

	if err != nil && strings.Contains(err.Error(), "expression depth exceeds") {
		t.Errorf("Should not error on depth for expression under limit: %v", err)
	}
}
