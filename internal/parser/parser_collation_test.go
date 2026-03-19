// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Helper functions to reduce cyclomatic complexity

func parseCreateTableStmt(t *testing.T, sql string) *CreateTableStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmts[0])
	}
	return stmt
}

func parseSelectStmt(t *testing.T, sql string) *SelectStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	return stmt
}

func runCollateColumnSubtest(t *testing.T, name, sql string, check func(*testing.T, *CreateTableStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseCreateTableStmt(t, sql)
		check(t, stmt)
	})
}

func runCollateOrderBySubtest(t *testing.T, name, sql string, check func(*testing.T, *SelectStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseSelectStmt(t, sql)
		check(t, stmt)
	})
}

func TestParseCollateInColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		checkTable func(*testing.T, *CreateTableStmt)
	}{
		{
			name:    "column with COLLATE NOCASE",
			sql:     "CREATE TABLE users (name TEXT COLLATE NOCASE)",
			wantErr: false,
			checkTable: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
				}
				// Check that the COLLATE constraint was parsed
				found := false
				for _, c := range stmt.Columns[0].Constraints {
					if c.Type == ConstraintCollate && c.Collate == "NOCASE" {
						found = true
						break
					}
				}
				if !found {
					t.Error("COLLATE NOCASE constraint not found in column")
				}
			},
		},
		{
			name:    "column with COLLATE BINARY",
			sql:     "CREATE TABLE items (code TEXT COLLATE BINARY)",
			wantErr: false,
			checkTable: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
				}
				found := false
				for _, c := range stmt.Columns[0].Constraints {
					if c.Type == ConstraintCollate && c.Collate == "BINARY" {
						found = true
						break
					}
				}
				if !found {
					t.Error("COLLATE BINARY constraint not found in column")
				}
			},
		},
		{
			name:    "column with COLLATE RTRIM",
			sql:     "CREATE TABLE data (value TEXT COLLATE RTRIM)",
			wantErr: false,
			checkTable: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
				}
				found := false
				for _, c := range stmt.Columns[0].Constraints {
					if c.Type == ConstraintCollate && c.Collate == "RTRIM" {
						found = true
						break
					}
				}
				if !found {
					t.Error("COLLATE RTRIM constraint not found in column")
				}
			},
		},
		{
			name:    "multiple columns with different collations",
			sql:     "CREATE TABLE mixed (name TEXT COLLATE NOCASE, code TEXT COLLATE BINARY, value TEXT)",
			wantErr: false,
			checkTable: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Columns) != 3 {
					t.Fatalf("expected 3 columns, got %d", len(stmt.Columns))
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		runCollateColumnSubtest(t, tt.name, tt.sql, tt.checkTable)
	}
}

// Prefix: coll_
type collateOrderByTestCase struct {
	name           string
	sql            string
	wantErr        bool
	wantNumTerms   int
	term0Collation string
	term0Asc       bool
	term1Collation string
	term1Asc       bool
	term2Collation string
}

func coll_checkTerm(t *testing.T, term OrderingTerm, idx int, wantCollation string, wantAsc bool) {
	t.Helper()
	if term.Collation != wantCollation {
		t.Errorf("term %d: expected collation %q, got %q", idx, wantCollation, term.Collation)
	}
	if term.Asc != wantAsc {
		t.Errorf("term %d: expected Asc=%v, got %v", idx, wantAsc, term.Asc)
	}
}

func coll_checkOrderBy(t *testing.T, stmt *SelectStmt, tc collateOrderByTestCase) {
	t.Helper()
	if len(stmt.OrderBy) != tc.wantNumTerms {
		t.Fatalf("expected %d ORDER BY terms, got %d", tc.wantNumTerms, len(stmt.OrderBy))
	}
	if tc.wantNumTerms >= 1 {
		coll_checkTerm(t, stmt.OrderBy[0], 0, tc.term0Collation, tc.term0Asc)
	}
	if tc.wantNumTerms >= 2 {
		coll_checkTerm(t, stmt.OrderBy[1], 1, tc.term1Collation, tc.term1Asc)
	}
	if tc.wantNumTerms >= 3 {
		coll_checkTerm(t, stmt.OrderBy[2], 2, tc.term2Collation, true)
	}
}

func TestParseCollateInOrderBy(t *testing.T) {
	t.Parallel()
	tests := []collateOrderByTestCase{
		{
			name:         "ORDER BY with COLLATE NOCASE",
			sql:          "SELECT name FROM users ORDER BY name COLLATE NOCASE",
			wantNumTerms: 1, term0Collation: "NOCASE", term0Asc: true,
		},
		{
			name:         "ORDER BY with COLLATE BINARY DESC",
			sql:          "SELECT code FROM items ORDER BY code COLLATE BINARY DESC",
			wantNumTerms: 1, term0Collation: "BINARY", term0Asc: false,
		},
		{
			name:         "ORDER BY with COLLATE RTRIM ASC",
			sql:          "SELECT value FROM data ORDER BY value COLLATE RTRIM ASC",
			wantNumTerms: 1, term0Collation: "RTRIM", term0Asc: true,
		},
		{
			name:         "ORDER BY multiple columns with different collations",
			sql:          "SELECT * FROM users ORDER BY lastname COLLATE NOCASE, firstname COLLATE BINARY DESC",
			wantNumTerms: 2, term0Collation: "NOCASE", term0Asc: true, term1Collation: "BINARY", term1Asc: false,
		},
		{
			name:         "ORDER BY without COLLATE",
			sql:          "SELECT name FROM users ORDER BY name",
			wantNumTerms: 1, term0Collation: "", term0Asc: true,
		},
		{
			name:         "ORDER BY mixed - some with COLLATE, some without",
			sql:          "SELECT * FROM users ORDER BY name COLLATE NOCASE, age, email COLLATE BINARY",
			wantNumTerms: 3, term0Collation: "NOCASE", term0Asc: true, term1Collation: "", term1Asc: true, term2Collation: "BINARY",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmt := parseSelectStmt(t, tc.sql)
			coll_checkOrderBy(t, stmt, tc)
		})
	}
}

func TestParseCollateInExpression(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "COLLATE in WHERE clause",
			sql:     "SELECT * FROM users WHERE name COLLATE NOCASE = 'john'",
			wantErr: false,
		},
		{
			name:    "COLLATE in comparison",
			sql:     "SELECT * FROM users WHERE name COLLATE BINARY > 'A'",
			wantErr: false,
		},
		{
			name:    "COLLATE with LIKE",
			sql:     "SELECT * FROM users WHERE name COLLATE NOCASE LIKE 'john%'",
			wantErr: false,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Fatalf("expected 1 statement, got %d", len(stmts))
				}
				stmt, ok := stmts[0].(*SelectStmt)
				if !ok {
					t.Fatalf("expected SelectStmt, got %T", stmts[0])
				}
				if stmt.Where == nil {
					t.Error("expected WHERE clause")
				}
				// Check that the WHERE clause contains a CollateExpr
				hasCollate := checkExprForCollate(stmt.Where)
				if !hasCollate {
					t.Error("expected CollateExpr in WHERE clause")
				}
			}
		})
	}
}

// Helper function to recursively check if an expression tree contains a CollateExpr
func checkExprForCollate(expr Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *CollateExpr:
		return true
	case *BinaryExpr:
		return checkExprForCollate(e.Left) || checkExprForCollate(e.Right)
	case *UnaryExpr:
		return checkExprForCollate(e.Expr)
	case *ParenExpr:
		return checkExprForCollate(e.Expr)
	case *FunctionExpr:
		for _, arg := range e.Args {
			if checkExprForCollate(arg) {
				return true
			}
		}
	}
	return false
}

func TestParseUpdateWithCollate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "UPDATE with ORDER BY COLLATE",
			sql:     "UPDATE users SET name = 'John' WHERE id > 0 ORDER BY name COLLATE NOCASE LIMIT 10",
			wantErr: false,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Fatalf("expected 1 statement, got %d", len(stmts))
				}
				stmt, ok := stmts[0].(*UpdateStmt)
				if !ok {
					t.Fatalf("expected UpdateStmt, got %T", stmts[0])
				}
				if len(stmt.OrderBy) > 0 && stmt.OrderBy[0].Collation != "NOCASE" {
					t.Errorf("expected COLLATE NOCASE in ORDER BY, got %q", stmt.OrderBy[0].Collation)
				}
			}
		})
	}
}

func TestParseDeleteWithCollate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "DELETE with ORDER BY COLLATE",
			sql:     "DELETE FROM users WHERE id > 0 ORDER BY name COLLATE RTRIM LIMIT 5",
			wantErr: false,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Fatalf("expected 1 statement, got %d", len(stmts))
				}
				stmt, ok := stmts[0].(*DeleteStmt)
				if !ok {
					t.Fatalf("expected DeleteStmt, got %T", stmts[0])
				}
				if len(stmt.OrderBy) > 0 && stmt.OrderBy[0].Collation != "RTRIM" {
					t.Errorf("expected COLLATE RTRIM in ORDER BY, got %q", stmt.OrderBy[0].Collation)
				}
			}
		})
	}
}
