// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func parseSinglePragma(t *testing.T, sql string) *PragmaStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*PragmaStmt)
	if !ok {
		t.Fatalf("expected PragmaStmt, got %T", stmts[0])
	}
	return stmt
}

func assertPragmaFields(t *testing.T, stmt *PragmaStmt, wantSchema, wantName string, wantValue bool) {
	t.Helper()
	if stmt.Schema != wantSchema {
		t.Errorf("expected schema %q, got %q", wantSchema, stmt.Schema)
	}
	if stmt.Name != wantName {
		t.Errorf("expected name %q, got %q", wantName, stmt.Name)
	}
	if wantValue && stmt.Value == nil {
		t.Errorf("expected value to be present, got nil")
	}
	if !wantValue && stmt.Value != nil {
		t.Errorf("expected no value, got %v", stmt.Value)
	}
}

func TestParsePragma(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantSchema string
		wantName   string
		wantValue  bool
	}{
		{"simple pragma", "PRAGMA cache_size", false, "", "cache_size", false},
		{"pragma with equals value", "PRAGMA cache_size = 10000", false, "", "cache_size", true},
		{"pragma with function syntax", "PRAGMA cache_size(10000)", false, "", "cache_size", true},
		{"pragma with schema", "PRAGMA main.cache_size", false, "main", "cache_size", false},
		{"pragma with schema and equals value", "PRAGMA main.cache_size = 10000", false, "main", "cache_size", true},
		{"pragma with schema and function syntax", "PRAGMA main.cache_size(10000)", false, "main", "cache_size", true},
		{"pragma with string value", "PRAGMA journal_mode = 'WAL'", false, "", "journal_mode", true},
		{"pragma with negative value", "PRAGMA cache_size = -2000", false, "", "cache_size", true},
		{"pragma user_version", "PRAGMA user_version", false, "", "user_version", false},
		{"pragma user_version with value", "PRAGMA user_version = 123", false, "", "user_version", true},
		{"pragma table_info", "PRAGMA table_info(users)", false, "", "table_info", true},
		{"pragma with temp schema", "PRAGMA temp.cache_size = 5000", false, "temp", "cache_size", true},
		{"pragma without name - error", "PRAGMA", true, "", "", false},
		{"pragma with incomplete schema - error", "PRAGMA main.", true, "", "", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			stmt := parseSinglePragma(t, tt.sql)
			assertPragmaFields(t, stmt, tt.wantSchema, tt.wantName, tt.wantValue)
		})
	}
}

// assertParsePragmaNames parses the SQL and checks that the resulting PragmaStmt names match.
func assertParsePragmaNames(t *testing.T, sql string, wantNames []string) {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != len(wantNames) {
		t.Fatalf("expected %d statements, got %d", len(wantNames), len(stmts))
	}
	for i, stmt := range stmts {
		pragmaStmt, ok := stmt.(*PragmaStmt)
		if !ok {
			t.Errorf("statement %d: expected PragmaStmt, got %T", i, stmt)
			continue
		}
		if pragmaStmt.Name != wantNames[i] {
			t.Errorf("statement %d: expected name %q, got %q", i, wantNames[i], pragmaStmt.Name)
		}
	}
}

func TestParsePragmaMultiple(t *testing.T) {
	t.Parallel()
	t.Run("multiple pragma statements", func(t *testing.T) {
		t.Parallel()
		assertParsePragmaNames(t,
			"PRAGMA cache_size = 10000; PRAGMA journal_mode = 'WAL'; PRAGMA synchronous = FULL",
			[]string{"cache_size", "journal_mode", "synchronous"})
	})
	t.Run("pragma with semicolons", func(t *testing.T) {
		t.Parallel()
		assertParsePragmaNames(t,
			"PRAGMA user_version; PRAGMA schema_version;",
			[]string{"user_version", "schema_version"})
	})
}

func parsePragmaValue(t *testing.T, sql string) Expression {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*PragmaStmt)
	if !ok {
		t.Fatalf("expected PragmaStmt, got %T", stmts[0])
	}
	if stmt.Value == nil {
		t.Fatal("expected value to be present, got nil")
	}
	return stmt.Value
}

func assertLiteralValue(t *testing.T, expr Expression, wantType LiteralType, wantVal string) {
	t.Helper()
	lit, ok := expr.(*LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr, got %T", expr)
	}
	if lit.Type != wantType {
		t.Errorf("expected literal type %v, got %v", wantType, lit.Type)
	}
	if lit.Value != wantVal {
		t.Errorf("expected value %q, got %q", wantVal, lit.Value)
	}
}

func TestParsePragmaValueTypes(t *testing.T) {
	t.Parallel()

	t.Run("pragma with integer value", func(t *testing.T) {
		t.Parallel()
		expr := parsePragmaValue(t, "PRAGMA cache_size = 10000")
		assertLiteralValue(t, expr, LiteralInteger, "10000")
	})

	t.Run("pragma with string value", func(t *testing.T) {
		t.Parallel()
		expr := parsePragmaValue(t, "PRAGMA journal_mode = 'WAL'")
		assertLiteralValue(t, expr, LiteralString, "WAL")
	})

	t.Run("pragma with identifier value", func(t *testing.T) {
		t.Parallel()
		expr := parsePragmaValue(t, "PRAGMA synchronous = FULL")
		ident, ok := expr.(*IdentExpr)
		if !ok {
			t.Fatalf("expected IdentExpr, got %T", expr)
		}
		if ident.Name != "FULL" {
			t.Errorf("expected name 'FULL', got %q", ident.Name)
		}
	})
}
