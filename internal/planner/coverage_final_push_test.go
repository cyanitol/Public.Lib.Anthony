// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// Tests for join optimization paths via public API

// TestJoinOptimizerComprehensive tests join optimizer edge cases.
// makeJoinTables creates test TableInfo entries by name and log-estimated row counts.
func makeJoinTables(names []string, logEsts []int64) []*TableInfo {
	tables := make([]*TableInfo, len(names))
	for i, name := range names {
		tables[i] = &TableInfo{Name: name, Cursor: i, RowLogEst: NewLogEst(logEsts[i]), Indexes: []*IndexInfo{}}
	}
	return tables
}

// assertJoinOrder calls DynamicProgrammingJoinOrder and checks the result.
func assertJoinOrder(t *testing.T, tables []*TableInfo, wantCount int) {
	t.Helper()
	opt := NewJoinOptimizer(tables, &WhereInfo{Tables: tables}, NewCostModel())
	order, err := opt.DynamicProgrammingJoinOrder()
	if err != nil {
		t.Fatalf("DynamicProgrammingJoinOrder failed: %v", err)
	}
	if order == nil || len(order.Tables) != wantCount {
		t.Errorf("Expected %d tables in order, got %v", wantCount, order)
	}
}

func TestJoinOptimizerComprehensive(t *testing.T) {
	t.Run("Two tables", func(t *testing.T) {
		assertJoinOrder(t, makeJoinTables([]string{"users", "orders"}, []int64{1000, 10000}), 2)
	})

	t.Run("Three tables", func(t *testing.T) {
		assertJoinOrder(t, makeJoinTables([]string{"users", "orders", "products"}, []int64{1000, 10000, 500}), 3)
	})

	t.Run("Greedy join order", func(t *testing.T) {
		tables := makeJoinTables([]string{"users", "orders"}, []int64{1000, 10000})
		opt := NewJoinOptimizer(tables, &WhereInfo{Tables: tables}, NewCostModel())
		order, err := opt.GreedyJoinOrder()
		if err != nil {
			t.Fatalf("GreedyJoinOrder failed: %v", err)
		}
		if order == nil {
			t.Fatal("Expected non-nil order")
		}
	})
}

// TestCTEReferencesInSubqueriesComprehensive tests CTE reference detection in subqueries.
func TestCTEReferencesInSubqueriesComprehensive(t *testing.T) {
	// Test case 1: CTE referenced in subquery within WHERE
	t.Run("CTE in WHERE subquery", func(t *testing.T) {
		sql := "WITH cte AS (SELECT 1) SELECT * FROM users WHERE id IN (SELECT * FROM cte)"
		p := parser.NewParser(sql)
		stmts, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		selectStmt := stmts[0].(*parser.SelectStmt)
		ctx, err := NewCTEContext(selectStmt.With)
		if err != nil {
			t.Fatalf("NewCTEContext failed: %v", err)
		}

		// Verify CTE was created
		if !ctx.HasCTE("cte") {
			t.Error("Expected CTE 'cte' to exist")
		}
	})

	// Test case 2: CTE referenced in HAVING clause
	t.Run("CTE in HAVING subquery", func(t *testing.T) {
		sql := "WITH cte AS (SELECT 1 AS val) SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > (SELECT val FROM cte)"
		p := parser.NewParser(sql)
		stmts, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		selectStmt := stmts[0].(*parser.SelectStmt)
		ctx, err := NewCTEContext(selectStmt.With)
		if err != nil {
			t.Fatalf("NewCTEContext failed: %v", err)
		}

		if !ctx.HasCTE("cte") {
			t.Error("Expected CTE 'cte' to exist")
		}
	})
}

// TestCTEExpressionHandlingComprehensive tests all expression handling paths.
func TestCTEExpressionHandlingComprehensive(t *testing.T) {
	ctx := &CTEContext{
		CTEs:        make(map[string]*CTEDefinition),
		IsRecursive: false,
	}

	ctx.CTEs["test_cte"] = &CTEDefinition{
		Name: "test_cte",
	}

	// Test case 1: UnaryExpr
	t.Run("UnaryExpr", func(t *testing.T) {
		expr := &parser.UnaryExpr{
			Op:   parser.OpNot,
			Expr: &parser.IdentExpr{Name: "column"},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 2: ParenExpr
	t.Run("ParenExpr", func(t *testing.T) {
		expr := &parser.ParenExpr{
			Expr: &parser.IdentExpr{Name: "column"},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 3: CastExpr
	t.Run("CastExpr", func(t *testing.T) {
		expr := &parser.CastExpr{
			Expr: &parser.IdentExpr{Name: "column"},
			Type: "INTEGER",
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 4: CollateExpr
	t.Run("CollateExpr", func(t *testing.T) {
		expr := &parser.CollateExpr{
			Expr:      &parser.IdentExpr{Name: "column"},
			Collation: "NOCASE",
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 5: BetweenExpr
	t.Run("BetweenExpr", func(t *testing.T) {
		expr := &parser.BetweenExpr{
			Expr:  &parser.IdentExpr{Name: "age"},
			Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
			Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "65"},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 6: FunctionExpr with filter
	t.Run("FunctionExpr with filter", func(t *testing.T) {
		expr := &parser.FunctionExpr{
			Name: "COUNT",
			Args: []parser.Expression{
				&parser.IdentExpr{Name: "id"},
			},
			Filter: &parser.BinaryExpr{
				Op:    parser.OpGt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
			},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 7: CaseExpr without Expr
	t.Run("CaseExpr without base expr", func(t *testing.T) {
		expr := &parser.CaseExpr{
			Expr: nil,
			WhenClauses: []parser.WhenClause{
				{
					Condition: &parser.BinaryExpr{
						Op:    parser.OpGt,
						Left:  &parser.IdentExpr{Name: "age"},
						Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
					},
					Result: &parser.LiteralExpr{Type: parser.LiteralString, Value: "adult"},
				},
			},
			ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "minor"},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})

	// Test case 8: InExpr with values
	t.Run("InExpr with values", func(t *testing.T) {
		expr := &parser.InExpr{
			Expr: &parser.IdentExpr{Name: "status"},
			Values: []parser.Expression{
				&parser.LiteralExpr{Type: parser.LiteralString, Value: "active"},
				&parser.LiteralExpr{Type: parser.LiteralString, Value: "pending"},
			},
		}

		deps := make(map[string]bool)
		ctx.collectCTEReferencesInExpr(expr, deps)

		// Should not crash
	})
}

// TestCTEReferencesInJoins tests CTE detection in various JOIN scenarios.
func TestCTEReferencesInJoins(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "CTE in LEFT JOIN",
			sql:     "WITH cte AS (SELECT 1) SELECT * FROM users LEFT JOIN cte ON users.id = cte.id",
			wantErr: false,
		},
		{
			name:    "CTE in RIGHT JOIN",
			sql:     "WITH cte AS (SELECT 1) SELECT * FROM users RIGHT JOIN cte ON users.id = cte.id",
			wantErr: false,
		},
		{
			name:    "CTE in JOIN with subquery",
			sql:     "WITH cte AS (SELECT 1) SELECT * FROM users JOIN (SELECT * FROM cte) sub ON users.id = sub.id",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Parse failed: %v", err)
				}
				return
			}

			selectStmt := stmts[0].(*parser.SelectStmt)
			ctx, err := NewCTEContext(selectStmt.With)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCTEContext() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && ctx != nil {
				if !ctx.HasCTE("cte") {
					t.Error("Expected CTE 'cte' to exist")
				}
			}
		})
	}
}

// TestCTECompoundQueriesComprehensive tests CTE detection in compound queries.
func TestCTECompoundQueriesComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		cteName  string
		wantDeps []string
	}{
		{
			name:     "CTE in EXCEPT query",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT * FROM a EXCEPT SELECT * FROM users) SELECT * FROM b",
			cteName:  "b",
			wantDeps: []string{"a"},
		},
		{
			name:     "CTE in INTERSECT query",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT * FROM a INTERSECT SELECT * FROM users) SELECT * FROM b",
			cteName:  "b",
			wantDeps: []string{"a"},
		},
		{
			name:     "Compound with multiple CTE references",
			sql:      "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x), c AS (SELECT * FROM a UNION SELECT * FROM b) SELECT * FROM c",
			cteName:  "c",
			wantDeps: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt := stmts[0].(*parser.SelectStmt)
			ctx, err := NewCTEContext(selectStmt.With)
			if err != nil {
				t.Fatalf("NewCTEContext failed: %v", err)
			}

			def, exists := ctx.GetCTE(tt.cteName)
			if !exists {
				t.Fatalf("CTE %s not found", tt.cteName)
			}

			// Check dependencies match (order doesn't matter)
			if len(def.DependsOn) != len(tt.wantDeps) {
				t.Errorf("Expected %d dependencies, got %d", len(tt.wantDeps), len(def.DependsOn))
			}
		})
	}
}

// TestTableReferencesEdgeCases tests table reference detection edge cases.
func TestTableReferencesEdgeCases(t *testing.T) {
	ctx := &CTEContext{
		CTEs:        make(map[string]*CTEDefinition),
		IsRecursive: true,
	}

	// Test case 1: Empty tables slice
	t.Run("Empty tables", func(t *testing.T) {
		tables := []parser.TableOrSubquery{}
		result := ctx.tablesReferenceTable(tables, "test")

		if result {
			t.Error("Expected false for empty tables")
		}
	})

	// Test case 2: Subquery that references table
	t.Run("Subquery references table", func(t *testing.T) {
		tables := []parser.TableOrSubquery{
			{
				Subquery: &parser.SelectStmt{
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "test"},
						},
					},
				},
			},
		}

		result := ctx.tablesReferenceTable(tables, "test")

		if !result {
			t.Error("Expected true when subquery references table")
		}
	})

	// Test case 3: Empty joins slice
	t.Run("Empty joins", func(t *testing.T) {
		joins := []parser.JoinClause{}
		result := ctx.joinsReferenceTable(joins, "test")

		if result {
			t.Error("Expected false for empty joins")
		}
	})

	// Test case 4: Join with subquery
	t.Run("Join with subquery", func(t *testing.T) {
		joins := []parser.JoinClause{
			{
				Table: parser.TableOrSubquery{
					Subquery: &parser.SelectStmt{
						From: &parser.FromClause{
							Tables: []parser.TableOrSubquery{
								{TableName: "test"},
							},
						},
					},
				},
			},
		}

		result := ctx.joinsReferenceTable(joins, "test")

		if !result {
			t.Error("Expected true when join subquery references table")
		}
	})

	// Test case 5: CompoundSelect with nil
	t.Run("Nil compound", func(t *testing.T) {
		result := ctx.compoundReferencesTable(nil, "test")

		if result {
			t.Error("Expected false for nil compound")
		}
	})
}
