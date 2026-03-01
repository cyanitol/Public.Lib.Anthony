// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// TestCTEExpressionHandling tests expression handling in CTEs.
func TestCTEExpressionHandling(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		cteName string
		wantErr bool
	}{
		{
			name:    "subquery in WHERE",
			sql:     "WITH cte AS (SELECT * FROM users WHERE id IN (SELECT user_id FROM posts)) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "CASE expression",
			sql:     "WITH cte AS (SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END AS status FROM users) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "BETWEEN expression",
			sql:     "WITH cte AS (SELECT * FROM users WHERE age BETWEEN 18 AND 65) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "function expression",
			sql:     "WITH cte AS (SELECT COUNT(*) AS total FROM users) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "nested subqueries",
			sql:     "WITH cte AS (SELECT * FROM users WHERE id IN (SELECT user_id FROM posts WHERE post_id IN (SELECT id FROM categories))) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
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

			if (err != nil) != tt.wantErr {
				t.Errorf("NewCTEContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if ctx == nil {
					t.Fatal("expected CTEContext, got nil")
				}

				def, exists := ctx.GetCTE(tt.cteName)
				if !exists {
					t.Fatalf("CTE %s not found", tt.cteName)
				}

				if def == nil {
					t.Fatal("CTE definition is nil")
				}
			}
		})
	}
}

// TestCTEWithJoins tests CTEs with JOIN clauses.
func TestCTEWithJoins(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		cteName  string
		wantDeps int
	}{
		{
			name:     "CTE with inner join",
			sql:      "WITH cte AS (SELECT * FROM users JOIN posts ON users.id = posts.user_id) SELECT * FROM cte",
			cteName:  "cte",
			wantDeps: 0,
		},
		{
			name:     "CTE depending on another via join",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT * FROM users JOIN a ON users.id = a.id) SELECT * FROM b",
			cteName:  "b",
			wantDeps: 1,
		},
		{
			name:     "multiple joins with CTE references",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a JOIN b) SELECT * FROM c",
			cteName:  "c",
			wantDeps: 2,
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

			if len(def.DependsOn) != tt.wantDeps {
				t.Errorf("Expected %d dependencies, got %d", tt.wantDeps, len(def.DependsOn))
			}
		})
	}
}

// TestCTEWithCompoundQueries tests CTEs with UNION/EXCEPT/INTERSECT.
func TestCTEWithCompoundQueries(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		cteName string
		wantErr bool
	}{
		{
			name:    "UNION in CTE",
			sql:     "WITH cte AS (SELECT id FROM users UNION SELECT id FROM posts) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "UNION ALL in CTE",
			sql:     "WITH cte AS (SELECT id FROM users UNION ALL SELECT id FROM posts) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "EXCEPT in CTE",
			sql:     "WITH cte AS (SELECT id FROM users EXCEPT SELECT id FROM deleted_users) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
		},
		{
			name:    "INTERSECT in CTE",
			sql:     "WITH cte AS (SELECT id FROM users INTERSECT SELECT id FROM active_users) SELECT * FROM cte",
			cteName: "cte",
			wantErr: false,
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

			if (err != nil) != tt.wantErr {
				t.Errorf("NewCTEContext() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && ctx != nil {
				_, exists := ctx.GetCTE(tt.cteName)
				if !exists {
					t.Errorf("CTE %s not found", tt.cteName)
				}
			}
		})
	}
}

// TestCTECircularDependencies tests circular dependency detection.
func TestCTECircularDependencies(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "no circular dependency",
			sql:     "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b",
			wantErr: false,
		},
		{
			name:    "recursive CTE allowed",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
			wantErr: false,
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
			if err != nil && !tt.wantErr {
				t.Fatalf("NewCTEContext failed: %v", err)
			}

			if ctx != nil {
				err = ctx.ValidateCTEs()
				if (err != nil) != tt.wantErr {
					t.Errorf("ValidateCTEs() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

// TestCTEDependencyLevels tests calculating dependency levels.
func TestCTEDependencyLevels(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b), d AS (SELECT * FROM c) SELECT * FROM d"

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

	// Check levels are strictly increasing
	levels := make(map[string]int)
	for name, def := range ctx.CTEs {
		levels[name] = def.Level
	}

	if levels["a"] >= levels["b"] {
		t.Errorf("Level of 'a' (%d) should be less than 'b' (%d)", levels["a"], levels["b"])
	}
	if levels["b"] >= levels["c"] {
		t.Errorf("Level of 'b' (%d) should be less than 'c' (%d)", levels["b"], levels["c"])
	}
	if levels["c"] >= levels["d"] {
		t.Errorf("Level of 'c' (%d) should be less than 'd' (%d)", levels["c"], levels["d"])
	}
}

// TestMaterializeRecursiveCTE tests recursive CTE materialization.
func TestMaterializeRecursiveCTE(t *testing.T) {
	sql := "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"

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

	mat, err := ctx.MaterializeCTE("cte")
	if err != nil {
		t.Fatalf("MaterializeCTE failed: %v", err)
	}

	if !mat.IsRecursive {
		t.Error("Expected IsRecursive = true")
	}

	if mat.Iterations <= 0 {
		t.Error("Expected positive iteration count")
	}
}

// TestEstimateRecursiveCTERows tests recursive CTE row estimation.
func TestEstimateRecursiveCTERows(t *testing.T) {
	sql := "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM cte"

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

	def, exists := ctx.GetCTE("cte")
	if !exists {
		t.Fatal("CTE not found")
	}

	rows := ctx.EstimateRecursiveCTERows(def)
	if rows <= 0 {
		t.Error("Expected positive row estimate")
	}
}

// TestCTEWithDependencyMaterialization tests materializing CTEs with dependencies.
func TestCTEWithDependencyMaterialization(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c"

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

	// Materialize 'c' which depends on 'b' which depends on 'a'
	mat, err := ctx.MaterializeCTE("c")
	if err != nil {
		t.Fatalf("MaterializeCTE failed: %v", err)
	}

	if mat == nil {
		t.Fatal("Expected MaterializedCTE, got nil")
	}

	// Check that dependencies were also materialized
	if _, exists := ctx.MaterializedCTEs["a"]; !exists {
		t.Error("Dependency 'a' should be materialized")
	}
	if _, exists := ctx.MaterializedCTEs["b"]; !exists {
		t.Error("Dependency 'b' should be materialized")
	}
}

// TestCTEExpandWithMaterialization tests expanding a materialized CTE.
func TestCTEExpandWithMaterialization(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM users) SELECT * FROM cte"

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

	// First materialize the CTE
	mat, err := ctx.MaterializeCTE("cte")
	if err != nil {
		t.Fatalf("MaterializeCTE failed: %v", err)
	}

	// Then expand it - should use materialized version
	table, err := ctx.ExpandCTE("cte", 0)
	if err != nil {
		t.Fatalf("ExpandCTE failed: %v", err)
	}

	if table.Name != mat.TempTable {
		t.Errorf("Expected table name %s (materialized), got %s", mat.TempTable, table.Name)
	}
}

// TestCTEUnionStructureValidation tests UNION structure validation.
func TestCTEUnionStructureValidation(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "valid UNION ALL",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "valid UNION",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION SELECT 2) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "invalid no UNION",
			sql:     "WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte",
			wantErr: true,
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

			err = ctx.ValidateCTEs()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCTEs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCTEInferColumnNameEdgeCases tests edge cases in column name inference.
func TestCTEInferColumnNameEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		cteName      string
		wantColNames []string
	}{
		{
			name:         "star column",
			sql:          "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
			cteName:      "cte",
			wantColNames: []string{"*"},
		},
		{
			name:         "expression without alias",
			sql:          "WITH cte AS (SELECT 1 + 2) SELECT * FROM cte",
			cteName:      "cte",
			wantColNames: []string{"column_0"},
		},
		{
			name:         "mixed columns",
			sql:          "WITH cte AS (SELECT id, 'test', name AS user_name FROM users) SELECT * FROM cte",
			cteName:      "cte",
			wantColNames: []string{"id", "column_1", "user_name"},
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

			columns := ctx.inferColumns(def)
			if len(columns) != len(tt.wantColNames) {
				t.Errorf("Expected %d columns, got %d", len(tt.wantColNames), len(columns))
			}

			for i, wantName := range tt.wantColNames {
				if i < len(columns) && columns[i].Name != wantName {
					t.Errorf("Column %d: expected name %s, got %s", i, wantName, columns[i].Name)
				}
			}
		})
	}
}

// TestCTERewriteWithNonCTETables tests rewriting with mixed CTE and regular tables.
func TestCTERewriteWithNonCTETables(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM users) SELECT * FROM cte, posts"

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

	tables := []*TableInfo{
		{Name: "cte", Cursor: 0},
		{Name: "posts", Cursor: 1},
	}

	rewritten, err := ctx.RewriteQueryWithCTEs(tables)
	if err != nil {
		t.Fatalf("RewriteQueryWithCTEs failed: %v", err)
	}

	if len(rewritten) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(rewritten))
	}

	// First should be expanded CTE
	if rewritten[0].Name != "cte" {
		t.Errorf("Expected first table to be 'cte', got %s", rewritten[0].Name)
	}

	// Second should be unchanged
	if rewritten[1].Name != "posts" {
		t.Errorf("Expected second table to be 'posts', got %s", rewritten[1].Name)
	}
}

// TestCTEWithNullContext tests that nil context is handled properly.
func TestCTEWithNullContext(t *testing.T) {
	var ctx *CTEContext

	// RewriteQueryWithCTEs should pass through with nil context
	tables := []*TableInfo{{Name: "test"}}
	rewritten, err := ctx.RewriteQueryWithCTEs(tables)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(rewritten) != len(tables) {
		t.Error("nil context should pass through tables unchanged")
	}

	// ValidateCTEs should succeed with nil context
	if err := ctx.ValidateCTEs(); err != nil {
		t.Errorf("nil context validation should succeed, got error: %v", err)
	}

	// GetCTE and HasCTE don't have nil guards, so we skip testing them
	// as they would panic - which is acceptable behavior for nil receivers
}

// TestCTEExpandUndefined tests expanding an undefined CTE.
func TestCTEExpandUndefined(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"

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

	_, err = ctx.ExpandCTE("undefined_cte", 0)
	if err == nil {
		t.Error("Expected error when expanding undefined CTE")
	}
}

// TestCTEMaterializeUndefined tests materializing an undefined CTE.
func TestCTEMaterializeUndefined(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"

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

	_, err = ctx.MaterializeCTE("undefined_cte")
	if err == nil {
		t.Error("Expected error when materializing undefined CTE")
	}
}

// TestCTEWithHavingClause tests CTEs with HAVING clause.
func TestCTEWithHavingClause(t *testing.T) {
	sql := "WITH cte AS (SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1) SELECT * FROM cte"

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

	_, exists := ctx.GetCTE("cte")
	if !exists {
		t.Error("CTE with HAVING clause should be parsed")
	}
}

// TestCTECheckIfRecursiveNonRecursiveContext tests checkIfRecursive with non-recursive context.
func TestCTECheckIfRecursiveNonRecursiveContext(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM cte) SELECT * FROM cte"

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

	def, exists := ctx.GetCTE("cte")
	if !exists {
		t.Fatal("CTE not found")
	}

	// Should not be recursive because context is not recursive
	if def.IsRecursive {
		t.Error("CTE should not be marked recursive in non-recursive context")
	}
}

// TestCTEComplexDependencyGraph tests a complex dependency graph.
func TestCTEComplexDependencyGraph(t *testing.T) {
	sql := `WITH
		a AS (SELECT 1),
		b AS (SELECT 2),
		c AS (SELECT * FROM a),
		d AS (SELECT * FROM b),
		e AS (SELECT * FROM c UNION SELECT * FROM d)
		SELECT * FROM e`

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

	// Verify dependency structure
	e, exists := ctx.GetCTE("e")
	if !exists {
		t.Fatal("CTE 'e' not found")
	}

	// 'e' should depend on 'c' and 'd'
	expectedDeps := map[string]bool{"c": true, "d": true}
	for _, dep := range e.DependsOn {
		if !expectedDeps[dep] {
			t.Errorf("Unexpected dependency: %s", dep)
		}
		delete(expectedDeps, dep)
	}
	if len(expectedDeps) > 0 {
		t.Errorf("Missing dependencies: %v", expectedDeps)
	}
}
