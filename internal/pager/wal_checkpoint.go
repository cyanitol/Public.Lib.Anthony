// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// TestNewCTEContext tests creating a CTE context from a WITH clause.
func TestNewCTEContext(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantCTEs  int
		wantRecur bool
		wantErr   bool
	}{
		{
			name:      "simple CTE",
			sql:       "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
			wantCTEs:  1,
			wantRecur: false,
			wantErr:   false,
		},
		{
			name:      "multiple CTEs",
			sql:       "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
			wantCTEs:  2,
			wantRecur: false,
			wantErr:   false,
		},
		{
			name:      "recursive CTE",
			sql:       "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM cte",
			wantCTEs:  1,
			wantRecur: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			selectStmt, ok := stmts[0].(*parser.SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmts[0])
			}

			ctx, err := NewCTEContext(selectStmt.With)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCTEContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if ctx == nil {
					t.Fatal("expected CTEContext, got nil")
				}

				if len(ctx.CTEs) != tt.wantCTEs {
					t.Errorf("expected %d CTEs, got %d", tt.wantCTEs, len(ctx.CTEs))
				}

				if ctx.IsRecursive != tt.wantRecur {
					t.Errorf("expected IsRecursive = %v, got %v", tt.wantRecur, ctx.IsRecursive)
				}
			}
		})
	}
}

// TestCTEDependencies tests CTE dependency detection.
func TestCTEDependencies(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		cteName  string
		wantDeps []string
	}{
		{
			name:     "no dependencies",
			sql:      "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
			cteName:  "cte",
			wantDeps: []string{},
		},
		{
			name:     "one dependency",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b",
			cteName:  "b",
			wantDeps: []string{"a"},
		},
		{
			name:     "multiple dependencies",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a JOIN b) SELECT * FROM c",
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

			if len(def.DependsOn) != len(tt.wantDeps) {
				t.Errorf("expected %d dependencies, got %d", len(tt.wantDeps), len(def.DependsOn))
				t.Logf("  want: %v", tt.wantDeps)
				t.Logf("  got:  %v", def.DependsOn)
			}
		})
	}
}

// TestCTEDependencyOrder tests topological sorting of CTEs.
func TestCTEDependencyOrder(t *testing.T) {
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

	// Check that order is correct (a before b, b before c)
	order := ctx.CTEOrder
	if len(order) != 3 {
		t.Fatalf("expected 3 CTEs in order, got %d", len(order))
	}

	aIdx, bIdx, cIdx := -1, -1, -1
	for i, name := range order {
		switch name {
		case "a":
			aIdx = i
		case "b":
			bIdx = i
		case "c":
			cIdx = i
		}
	}

	if aIdx == -1 || bIdx == -1 || cIdx == -1 {
		t.Fatal("not all CTEs found in order")
	}

	if aIdx >= bIdx || bIdx >= cIdx {
		t.Errorf("CTE order incorrect: a=%d, b=%d, c=%d", aIdx, bIdx, cIdx)
	}
}

// TestRecursiveCTEDetection tests detecting recursive CTEs.
func TestRecursiveCTEDetection(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		cteName       string
		wantRecursive bool
	}{
		{
			name:          "non-recursive",
			sql:           "WITH cte AS (SELECT 1) SELECT * FROM cte",
			cteName:       "cte",
			wantRecursive: false,
		},
		{
			name:          "recursive self-reference",
			sql:           "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte) SELECT * FROM cte",
			cteName:       "cte",
			wantRecursive: true,
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

			if def.IsRecursive != tt.wantRecursive {
				t.Errorf("expected IsRecursive = %v, got %v", tt.wantRecursive, def.IsRecursive)
			}
		})
	}
}

// TestExpandCTE tests expanding a CTE to a TableInfo.
func TestExpandCTE(t *testing.T) {
	sql := "WITH users_cte(id, name) AS (SELECT id, name FROM users) SELECT * FROM users_cte"

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

	// Expand the CTE
	table, err := ctx.ExpandCTE("users_cte", 0)
	if err != nil {
		t.Fatalf("ExpandCTE failed: %v", err)
	}

	if table == nil {
		t.Fatal("expected TableInfo, got nil")
	}

	if table.Name != "users_cte" {
		t.Errorf("expected Name = 'users_cte', got '%s'", table.Name)
	}

	if len(table.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(table.Columns))
	}

	if table.Cursor != 0 {
		t.Errorf("expected Cursor = 0, got %d", table.Cursor)
	}
}

// TestCTEColumnInference tests column inference from SELECT.
func TestCTEColumnInference(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		cteName     string
		wantColumns int
	}{
		{
			name:        "explicit column list",
			sql:         "WITH cte(a, b, c) AS (SELECT 1, 2, 3) SELECT * FROM cte",
			cteName:     "cte",
			wantColumns: 3,
		},
		{
			name:        "inferred from SELECT",
			sql:         "WITH cte AS (SELECT id, name, email FROM users) SELECT * FROM cte",
			cteName:     "cte",
			wantColumns: 3,
		},
		{
			name:        "with aliases",
			sql:         "WITH cte AS (SELECT id AS user_id, name AS user_name FROM users) SELECT * FROM cte",
			cteName:     "cte",
			wantColumns: 2,
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
			if len(columns) != tt.wantColumns {
				t.Errorf("expected %d columns, got %d", tt.wantColumns, len(columns))
			}
		})
	}
}

// TestCTEValidation tests CTE validation.
func TestCTEValidation(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "valid simple CTE",
			sql:     "WITH cte AS (SELECT 1) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "valid recursive CTE",
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

// TestRewriteQueryWithCTEs tests rewriting queries with CTEs.
func TestRewriteQueryWithCTEs(t *testing.T) {
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

	// Create a mock table reference to CTE
	tables := []*TableInfo{
		{
			Name:   "cte",
			Cursor: 0,
		},
	}

	// Rewrite
	rewritten, err := ctx.RewriteQueryWithCTEs(tables)
	if err != nil {
		t.Fatalf("RewriteQueryWithCTEs failed: %v", err)
	}

	if len(rewritten) != 1 {
		t.Fatalf("expected 1 table, got %d", len(rewritten))
	}

	// The table should now have columns from CTE
	if rewritten[0].Name != "cte" {
		t.Errorf("expected table name 'cte', got '%s'", rewritten[0].Name)
	}
}

// TestPlannerWithCTEs tests integrating CTEs with the planner.
func TestPlannerWithCTEs(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"

	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := NewCTEContext(selectStmt.With)
	if err != nil {
		t.Fatalf("NewCTEContext failed: %v", err)
	}

	// Create planner
	planner := NewPlanner()
	planner.SetCTEContext(ctx)

	// Create tables list (referencing CTE)
	tables := []*TableInfo{
		{
			Name:   "cte",
			Cursor: 0,
		},
	}

	// Plan the query
	info, err := planner.PlanQuery(tables, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info == nil {
		t.Fatal("expected WhereInfo, got nil")
	}

	if len(info.Tables) == 0 {
		t.Fatal("expected at least one table in plan")
	}
}

// TestMultipleCTEs tests planning with multiple CTEs.
func TestMultipleCTEs(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a JOIN b) SELECT * FROM c"

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

	if len(ctx.CTEs) != 3 {
		t.Errorf("expected 3 CTEs, got %d", len(ctx.CTEs))
	}

	// Check order: a and b should come before c
	order := ctx.CTEOrder
	cIdx := -1
	for i, name := range order {
		if name == "c" {
			cIdx = i
		}
	}

	if cIdx == -1 {
		t.Fatal("CTE 'c' not found in order")
	}

	// Both a and b should appear before c
	for i := cIdx; i < len(order); i++ {
		if order[i] == "a" || order[i] == "b" {
			t.Errorf("CTE '%s' appears after 'c' in order", order[i])
		}
	}
}

// TestRecursiveCTEStructure tests recursive CTE structure validation.
func TestRecursiveCTEStructure(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "valid recursive with UNION ALL",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "valid recursive with UNION",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION SELECT 2) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "recursive without UNION",
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

// TestMaterializeCTE tests CTE materialization.
func TestMaterializeCTE(t *testing.T) {
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

	// Materialize the CTE
	mat, err := ctx.MaterializeCTE("cte")
	if err != nil {
		t.Fatalf("MaterializeCTE failed: %v", err)
	}

	if mat == nil {
		t.Fatal("expected MaterializedCTE, got nil")
	}

	if mat.Name != "cte" {
		t.Errorf("expected Name = 'cte', got '%s'", mat.Name)
	}

	if mat.TempTable == "" {
		t.Error("expected non-empty TempTable")
	}

	// Check that it's cached
	mat2, err := ctx.MaterializeCTE("cte")
	if err != nil {
		t.Fatalf("second MaterializeCTE failed: %v", err)
	}

	if mat != mat2 {
		t.Error("expected same MaterializedCTE instance")
	}
}

// TestCTEsInSubqueries tests CTEs referenced in subqueries.
func TestCTEsInSubqueries(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id IN (SELECT id FROM cte)"

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

	if len(ctx.CTEs) != 1 {
		t.Errorf("expected 1 CTE, got %d", len(ctx.CTEs))
	}

	// The CTE should be expandable multiple times
	table1, err := ctx.ExpandCTE("cte", 0)
	if err != nil {
		t.Fatalf("first ExpandCTE failed: %v", err)
	}

	table2, err := ctx.ExpandCTE("cte", 1)
	if err != nil {
		t.Fatalf("second ExpandCTE failed: %v", err)
	}

	if table1.Name != table2.Name {
		t.Error("expected same CTE name for both expansions")
	}

	if table1.Cursor == table2.Cursor {
		t.Error("expected different cursor numbers for each expansion")
	}
}
