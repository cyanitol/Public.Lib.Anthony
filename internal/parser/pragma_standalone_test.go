// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestPragmaASTNode verifies that PragmaStmt implements the Statement interface
func TestPragmaASTNode(t *testing.T) {
	t.Parallel()
	var _ Statement = (*PragmaStmt)(nil)
}

// TestPragmaStmtFields verifies that PragmaStmt has the correct fields
func TestPragmaStmtFields(t *testing.T) {
	t.Parallel()
	stmt := &PragmaStmt{
		Schema: "main",
		Name:   "cache_size",
		Value:  &LiteralExpr{Type: LiteralInteger, Value: "10000"},
	}

	if stmt.Schema != "main" {
		t.Errorf("expected Schema 'main', got '%s'", stmt.Schema)
	}

	if stmt.Name != "cache_size" {
		t.Errorf("expected Name 'cache_size', got '%s'", stmt.Name)
	}

	if stmt.Value == nil {
		t.Error("expected Value to be non-nil")
	}

	if stmt.String() != "PRAGMA" {
		t.Errorf("expected String() to return 'PRAGMA', got '%s'", stmt.String())
	}
}

// TestPragmaStmtNoValue verifies PragmaStmt with no value
func TestPragmaStmtNoValue(t *testing.T) {
	t.Parallel()
	stmt := &PragmaStmt{
		Name: "user_version",
	}

	if stmt.Schema != "" {
		t.Errorf("expected Schema to be empty, got '%s'", stmt.Schema)
	}

	if stmt.Name != "user_version" {
		t.Errorf("expected Name 'user_version', got '%s'", stmt.Name)
	}

	if stmt.Value != nil {
		t.Errorf("expected Value to be nil, got %v", stmt.Value)
	}
}
