package parser

import (
	"testing"
)

// TestParseExpressionIndex tests parsing of expression-based indexes
func TestParseExpressionIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, *CreateIndexStmt)
	}{
		{
			name:    "simple function expression - LOWER",
			sql:     "CREATE INDEX idx ON users(LOWER(name))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if stmt.Name != "idx" {
					t.Errorf("Expected index name 'idx', got '%s'", stmt.Name)
				}
				if stmt.Table != "users" {
					t.Errorf("Expected table 'users', got '%s'", stmt.Table)
				}
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
				// Check it's a function call
				if call, ok := stmt.Columns[0].Expr.(*FunctionExpr); ok {
					if call.Name != "LOWER" {
						t.Errorf("Expected LOWER function, got %s", call.Name)
					}
				} else {
					t.Errorf("Expected FunctionExpr, got %T", stmt.Columns[0].Expr)
				}
			},
		},
		{
			name:    "simple function expression - UPPER",
			sql:     "CREATE INDEX idx ON products(UPPER(name))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if call, ok := stmt.Columns[0].Expr.(*FunctionExpr); ok {
					if call.Name != "UPPER" {
						t.Errorf("Expected UPPER function, got %s", call.Name)
					}
				}
			},
		},
		{
			name:    "arithmetic expression - addition",
			sql:     "CREATE INDEX idx ON sales(price + tax)",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
				// Check it's a binary expression
				if binExpr, ok := stmt.Columns[0].Expr.(*BinaryExpr); ok {
					if binExpr.Op != OpPlus {
						t.Errorf("Expected OpPlus, got %v", binExpr.Op)
					}
				} else {
					t.Errorf("Expected BinaryExpr, got %T", stmt.Columns[0].Expr)
				}
			},
		},
		{
			name:    "multiple expressions",
			sql:     "CREATE INDEX idx ON people(LOWER(last_name), LOWER(first_name))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 2 {
					t.Fatalf("Expected 2 columns, got %d", len(stmt.Columns))
				}
				for i, col := range stmt.Columns {
					if col.Expr == nil {
						t.Errorf("Column %d: expected expression to be set", i)
					}
				}
			},
		},
		{
			name:    "mixed: expression and simple column",
			sql:     "CREATE INDEX idx ON people(LOWER(name), age)",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 2 {
					t.Fatalf("Expected 2 columns, got %d", len(stmt.Columns))
				}
				// First should be expression
				if stmt.Columns[0].Expr == nil {
					t.Error("First column should have expression")
				}
				// Second should be simple column (IdentExpr is also an expression)
				if stmt.Columns[1].Column != "age" {
					t.Errorf("Second column should be 'age', got '%s'", stmt.Columns[1].Column)
				}
			},
		},
		{
			name:    "expression with DESC",
			sql:     "CREATE INDEX idx ON users(LOWER(name) DESC)",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Order != SortDesc {
					t.Errorf("Expected DESC order, got %v", stmt.Columns[0].Order)
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
			},
		},
		{
			name:    "nested function calls",
			sql:     "CREATE INDEX idx ON texts(LOWER(TRIM(content)))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
				// Should be LOWER call with TRIM as argument
				if call, ok := stmt.Columns[0].Expr.(*FunctionExpr); ok {
					if call.Name != "LOWER" {
						t.Errorf("Expected LOWER function, got %s", call.Name)
					}
					if len(call.Args) != 1 {
						t.Errorf("Expected 1 argument, got %d", len(call.Args))
					}
				}
			},
		},
		{
			name:    "string concatenation",
			sql:     "CREATE INDEX idx ON names(last || ', ' || first)",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
			},
		},
		{
			name:    "CAST expression",
			sql:     "CREATE INDEX idx ON data(CAST(text_num AS INTEGER))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
			},
		},
		{
			name:    "SUBSTR function",
			sql:     "CREATE INDEX idx ON codes(SUBSTR(code, 1, 3))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if call, ok := stmt.Columns[0].Expr.(*FunctionExpr); ok {
					if call.Name != "SUBSTR" {
						t.Errorf("Expected SUBSTR function, got %s", call.Name)
					}
				}
			},
		},
		{
			name:    "expression with partial index (WHERE)",
			sql:     "CREATE INDEX idx ON items(LOWER(name)) WHERE active = 1",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
				if stmt.Where == nil {
					t.Error("Expected WHERE clause to be set")
				}
			},
		},
		{
			name:    "UNIQUE expression index",
			sql:     "CREATE UNIQUE INDEX idx ON emails(LOWER(email))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if !stmt.Unique {
					t.Error("Expected index to be UNIQUE")
				}
				if len(stmt.Columns) != 1 {
					t.Fatalf("Expected 1 column, got %d", len(stmt.Columns))
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
			},
		},
		{
			name:    "expression index with IF NOT EXISTS",
			sql:     "CREATE INDEX IF NOT EXISTS idx ON users(LOWER(name))",
			wantErr: false,
			validate: func(t *testing.T, stmt *CreateIndexStmt) {
				if !stmt.IfNotExists {
					t.Error("Expected IF NOT EXISTS to be set")
				}
				if stmt.Columns[0].Expr == nil {
					t.Error("Expected expression to be set")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			stmts, err := p.Parse()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

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

			if tt.validate != nil {
				tt.validate(t, stmt)
			}
		})
	}
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
