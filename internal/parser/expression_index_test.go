// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Helper functions to reduce cyclomatic complexity

func parseCreateIndexStmt(t *testing.T, sql string) *CreateIndexStmt {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected CreateIndexStmt, got %T", stmts[0])
	}
	return stmt
}

func validateBasicIndexProperties(t *testing.T, stmt *CreateIndexStmt, name, table string) {
	t.Helper()
	if stmt.Name != name {
		t.Errorf("Expected index name '%s', got '%s'", name, stmt.Name)
	}
	if stmt.Table != table {
		t.Errorf("Expected table '%s', got '%s'", table, stmt.Table)
	}
}

func validateColumnCount(t *testing.T, stmt *CreateIndexStmt, expected int) {
	t.Helper()
	if len(stmt.Columns) != expected {
		t.Fatalf("Expected %d column(s), got %d", expected, len(stmt.Columns))
	}
}

func validateHasExpression(t *testing.T, col *IndexedColumn) {
	t.Helper()
	if col.Expr == nil {
		t.Error("Expected expression to be set")
	}
}

func validateFunctionExpr(t *testing.T, expr Expression, expectedFuncName string) {
	t.Helper()
	call, ok := expr.(*FunctionExpr)
	if !ok {
		t.Errorf("Expected FunctionExpr, got %T", expr)
		return
	}
	if call.Name != expectedFuncName {
		t.Errorf("Expected %s function, got %s", expectedFuncName, call.Name)
	}
}

func validateBinaryExpr(t *testing.T, expr Expression, expectedOp BinaryOp) {
	t.Helper()
	binExpr, ok := expr.(*BinaryExpr)
	if !ok {
		t.Errorf("Expected BinaryExpr, got %T", expr)
		return
	}
	if binExpr.Op != expectedOp {
		t.Errorf("Expected %v, got %v", expectedOp, binExpr.Op)
	}
}

// Helper to run subtests for expression index validation
func runExpressionIndexSubtest(t *testing.T, name, sql string, validate func(*testing.T, *CreateIndexStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseCreateIndexStmt(t, sql)
		validate(t, stmt)
	})
}

// TestParseExpressionIndex tests parsing of expression-based indexes
func TestParseExpressionIndex(t *testing.T) {
	t.Parallel()

	runExpressionIndexSubtest(t, "simple function expression - LOWER",
		"CREATE INDEX idx ON users(LOWER(name))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateBasicIndexProperties(t, stmt, "idx", "users")
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
			validateFunctionExpr(t, stmt.Columns[0].Expr, "LOWER")
		})

	runExpressionIndexSubtest(t, "simple function expression - UPPER",
		"CREATE INDEX idx ON products(UPPER(name))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateFunctionExpr(t, stmt.Columns[0].Expr, "UPPER")
		})

	runExpressionIndexSubtest(t, "arithmetic expression - addition",
		"CREATE INDEX idx ON sales(price + tax)",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
			validateBinaryExpr(t, stmt.Columns[0].Expr, OpPlus)
		})

	runExpressionIndexSubtest(t, "multiple expressions",
		"CREATE INDEX idx ON people(LOWER(last_name), LOWER(first_name))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 2)
			for i := range stmt.Columns {
				validateHasExpression(t, &stmt.Columns[i])
			}
		})

	runExpressionIndexSubtest(t, "mixed: expression and simple column",
		"CREATE INDEX idx ON people(LOWER(name), age)",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 2)
			validateHasExpression(t, &stmt.Columns[0])
			if stmt.Columns[1].Column != "age" {
				t.Errorf("Second column should be 'age', got '%s'", stmt.Columns[1].Column)
			}
		})

	runExpressionIndexSubtest(t, "expression with DESC",
		"CREATE INDEX idx ON users(LOWER(name) DESC)",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			if stmt.Columns[0].Order != SortDesc {
				t.Errorf("Expected DESC order, got %v", stmt.Columns[0].Order)
			}
			validateHasExpression(t, &stmt.Columns[0])
		})

	runExpressionIndexSubtest(t, "nested function calls",
		"CREATE INDEX idx ON texts(LOWER(TRIM(content)))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
			call, ok := stmt.Columns[0].Expr.(*FunctionExpr)
			if !ok {
				return
			}
			if call.Name != "LOWER" {
				t.Errorf("Expected LOWER function, got %s", call.Name)
			}
			if len(call.Args) != 1 {
				t.Errorf("Expected 1 argument, got %d", len(call.Args))
			}
		})

	runExpressionIndexSubtest(t, "string concatenation",
		"CREATE INDEX idx ON names(last || ', ' || first)",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
		})

	runExpressionIndexSubtest(t, "CAST expression",
		"CREATE INDEX idx ON data(CAST(text_num AS INTEGER))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
		})

	runExpressionIndexSubtest(t, "SUBSTR function",
		"CREATE INDEX idx ON codes(SUBSTR(code, 1, 3))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateFunctionExpr(t, stmt.Columns[0].Expr, "SUBSTR")
		})

	runExpressionIndexSubtest(t, "expression with partial index (WHERE)",
		"CREATE INDEX idx ON items(LOWER(name)) WHERE active = 1",
		func(t *testing.T, stmt *CreateIndexStmt) {
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
			if stmt.Where == nil {
				t.Error("Expected WHERE clause to be set")
			}
		})

	runExpressionIndexSubtest(t, "UNIQUE expression index",
		"CREATE UNIQUE INDEX idx ON emails(LOWER(email))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			if !stmt.Unique {
				t.Error("Expected index to be UNIQUE")
			}
			validateColumnCount(t, stmt, 1)
			validateHasExpression(t, &stmt.Columns[0])
		})

	runExpressionIndexSubtest(t, "expression index with IF NOT EXISTS",
		"CREATE INDEX IF NOT EXISTS idx ON users(LOWER(name))",
		func(t *testing.T, stmt *CreateIndexStmt) {
			if !stmt.IfNotExists {
				t.Error("Expected IF NOT EXISTS to be set")
			}
			validateHasExpression(t, &stmt.Columns[0])
		})
}

// TestExpressionIndexNameExtraction tests the extractExpressionName helper
func TestExpressionIndexNameExtraction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple identifier",
			sql:      "CREATE INDEX idx ON t(id)",
			expected: "id",
		},
		{
			name:     "function with identifier",
			sql:      "CREATE INDEX idx ON t(LOWER(name))",
			expected: "LOWER(name)",
		},
		{
			name:     "binary expression",
			sql:      "CREATE INDEX idx ON t(a + b)",
			expected: "a + b",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			stmt := stmts[0].(*CreateIndexStmt)
			if len(stmt.Columns) == 0 {
				t.Fatal("No columns in index")
			}

			got := stmt.Columns[0].Column
			if got != tt.expected {
				t.Errorf("Expected column name '%s', got '%s'", tt.expected, got)
			}
		})
	}
}
