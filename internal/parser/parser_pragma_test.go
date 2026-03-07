// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

func TestParsePragma(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantSchema string
		wantName   string
		wantValue  bool // whether a value should be present
	}{
		{
			name:       "simple pragma",
			sql:        "PRAGMA cache_size",
			wantErr:    false,
			wantSchema: "",
			wantName:   "cache_size",
			wantValue:  false,
		},
		{
			name:       "pragma with equals value",
			sql:        "PRAGMA cache_size = 10000",
			wantErr:    false,
			wantSchema: "",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:       "pragma with function syntax",
			sql:        "PRAGMA cache_size(10000)",
			wantErr:    false,
			wantSchema: "",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:       "pragma with schema",
			sql:        "PRAGMA main.cache_size",
			wantErr:    false,
			wantSchema: "main",
			wantName:   "cache_size",
			wantValue:  false,
		},
		{
			name:       "pragma with schema and equals value",
			sql:        "PRAGMA main.cache_size = 10000",
			wantErr:    false,
			wantSchema: "main",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:       "pragma with schema and function syntax",
			sql:        "PRAGMA main.cache_size(10000)",
			wantErr:    false,
			wantSchema: "main",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:       "pragma with string value",
			sql:        "PRAGMA journal_mode = 'WAL'",
			wantErr:    false,
			wantSchema: "",
			wantName:   "journal_mode",
			wantValue:  true,
		},
		{
			name:       "pragma with negative value",
			sql:        "PRAGMA cache_size = -2000",
			wantErr:    false,
			wantSchema: "",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:       "pragma user_version",
			sql:        "PRAGMA user_version",
			wantErr:    false,
			wantSchema: "",
			wantName:   "user_version",
			wantValue:  false,
		},
		{
			name:       "pragma user_version with value",
			sql:        "PRAGMA user_version = 123",
			wantErr:    false,
			wantSchema: "",
			wantName:   "user_version",
			wantValue:  true,
		},
		{
			name:       "pragma table_info",
			sql:        "PRAGMA table_info(users)",
			wantErr:    false,
			wantSchema: "",
			wantName:   "table_info",
			wantValue:  true,
		},
		{
			name:       "pragma with temp schema",
			sql:        "PRAGMA temp.cache_size = 5000",
			wantErr:    false,
			wantSchema: "temp",
			wantName:   "cache_size",
			wantValue:  true,
		},
		{
			name:    "pragma without name - error",
			sql:     "PRAGMA",
			wantErr: true,
		},
		{
			name:    "pragma with incomplete schema - error",
			sql:     "PRAGMA main.",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*PragmaStmt)
			if !ok {
				t.Errorf("expected PragmaStmt, got %T", stmts[0])
				return
			}

			if stmt.Schema != tt.wantSchema {
				t.Errorf("expected schema %q, got %q", tt.wantSchema, stmt.Schema)
			}

			if stmt.Name != tt.wantName {
				t.Errorf("expected name %q, got %q", tt.wantName, stmt.Name)
			}

			if tt.wantValue {
				if stmt.Value == nil {
					t.Errorf("expected value to be present, got nil")
				}
			} else {
				if stmt.Value != nil {
					t.Errorf("expected no value, got %v", stmt.Value)
				}
			}
		})
	}
}

func TestParsePragmaMultiple(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		wantCount int
		wantNames []string
	}{
		{
			name:      "multiple pragma statements",
			sql:       "PRAGMA cache_size = 10000; PRAGMA journal_mode = 'WAL'; PRAGMA synchronous = FULL",
			wantErr:   false,
			wantCount: 3,
			wantNames: []string{"cache_size", "journal_mode", "synchronous"},
		},
		{
			name:      "pragma with semicolons",
			sql:       "PRAGMA user_version; PRAGMA schema_version;",
			wantErr:   false,
			wantCount: 2,
			wantNames: []string{"user_version", "schema_version"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if len(stmts) != tt.wantCount {
				t.Errorf("expected %d statements, got %d", tt.wantCount, len(stmts))
				return
			}

			for i, stmt := range stmts {
				pragmaStmt, ok := stmt.(*PragmaStmt)
				if !ok {
					t.Errorf("statement %d: expected PragmaStmt, got %T", i, stmt)
					continue
				}

				if i < len(tt.wantNames) && pragmaStmt.Name != tt.wantNames[i] {
					t.Errorf("statement %d: expected name %q, got %q", i, tt.wantNames[i], pragmaStmt.Name)
				}
			}
		})
	}
}

func TestParsePragmaValueTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		checkFunc func(*testing.T, Expression)
	}{
		{
			name:    "pragma with integer value",
			sql:     "PRAGMA cache_size = 10000",
			wantErr: false,
			checkFunc: func(t *testing.T, expr Expression) {
				lit, ok := expr.(*LiteralExpr)
				if !ok {
					t.Errorf("expected LiteralExpr, got %T", expr)
					return
				}
				if lit.Type != LiteralInteger {
					t.Errorf("expected LiteralInteger, got %v", lit.Type)
				}
				if lit.Value != "10000" {
					t.Errorf("expected value '10000', got '%s'", lit.Value)
				}
			},
		},
		{
			name:    "pragma with string value",
			sql:     "PRAGMA journal_mode = 'WAL'",
			wantErr: false,
			checkFunc: func(t *testing.T, expr Expression) {
				lit, ok := expr.(*LiteralExpr)
				if !ok {
					t.Errorf("expected LiteralExpr, got %T", expr)
					return
				}
				if lit.Type != LiteralString {
					t.Errorf("expected LiteralString, got %v", lit.Type)
				}
				if lit.Value != "WAL" {
					t.Errorf("expected value 'WAL', got '%s'", lit.Value)
				}
			},
		},
		{
			name:    "pragma with identifier value",
			sql:     "PRAGMA synchronous = FULL",
			wantErr: false,
			checkFunc: func(t *testing.T, expr Expression) {
				ident, ok := expr.(*IdentExpr)
				if !ok {
					t.Errorf("expected IdentExpr, got %T", expr)
					return
				}
				if ident.Name != "FULL" {
					t.Errorf("expected name 'FULL', got '%s'", ident.Name)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*PragmaStmt)
			if !ok {
				t.Errorf("expected PragmaStmt, got %T", stmts[0])
				return
			}

			if stmt.Value == nil {
				t.Errorf("expected value to be present, got nil")
				return
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, stmt.Value)
			}
		})
	}
}
