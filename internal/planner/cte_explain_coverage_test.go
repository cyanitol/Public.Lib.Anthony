// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustParseCTECtx(t *testing.T, sql string) *CTEContext {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmts[0].(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	ctx, err := NewCTEContext(sel.With)
	if err != nil {
		t.Fatalf("NewCTEContext: %v", err)
	}
	return ctx
}

func newSchemaWithIndex(tableName, colName, idxName string, unique bool) *schema.Schema {
	s := schema.NewSchema()
	s.AddTableDirect(&schema.Table{
		Name: tableName,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: colName, Type: "TEXT"},
		},
	})
	s.AddIndexDirect(&schema.Index{
		Name:    idxName,
		Table:   tableName,
		Columns: []string{colName},
		Unique:  unique,
	})
	return s
}

// ---------------------------------------------------------------------------
// CTE: calculateAllLevels / calculateLevel
// ---------------------------------------------------------------------------

// TestCTEExplain_CalculateLevels exercises calculateAllLevels and calculateLevel
// by building a chain of three dependent CTEs.
func TestCTEExplain_CalculateLevels(t *testing.T) {
	t.Parallel()
	ctx := mustParseCTECtx(t,
		`WITH a AS (SELECT 1),
		      b AS (SELECT * FROM a),
		      c AS (SELECT * FROM b)
		 SELECT * FROM c`)

	aLevel := ctx.CTEs["a"].Level
	bLevel := ctx.CTEs["b"].Level
	cLevel := ctx.CTEs["c"].Level

	if aLevel >= bLevel {
		t.Errorf("expected level(a) < level(b), got a=%d b=%d", aLevel, bLevel)
	}
	if bLevel >= cLevel {
		t.Errorf("expected level(b) < level(c), got b=%d c=%d", bLevel, cLevel)
	}
}

// TestCTEExplain_CalculateLevelRecursive exercises the already-calculated (Level>0)
// early-return path and the recursive-self-reference skip inside calculateMaxDependencyLevel.
func TestCTEExplain_CalculateLevelRecursive(t *testing.T) {
	t.Parallel()
	ctx := mustParseCTECtx(t,
		`WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5)
		 SELECT * FROM t`)

	def, ok := ctx.CTEs["t"]
	if !ok {
		t.Fatal("CTE 't' not found")
	}
	if def.Level < 1 {
		t.Errorf("recursive CTE level should be >= 1, got %d", def.Level)
	}
}

// ---------------------------------------------------------------------------
// CTE: calculateMaxDependencyLevel – non-recursive self-reference error
// ---------------------------------------------------------------------------

// TestCTEExplain_NonRecursiveSelfRef verifies that a non-recursive CTE that
// references itself triggers the "non-recursive CTE cannot reference itself"
// error inside calculateMaxDependencyLevel.
func TestCTEExplain_NonRecursiveSelfRef(t *testing.T) {
	t.Parallel()
	// Build a CTEContext manually so we can inject the self-referencing dep
	// without RECURSIVE keyword (parser would normally reject this parse).
	ctx := &CTEContext{
		CTEs:             make(map[string]*CTEDefinition),
		IsRecursive:      false,
		MaterializedCTEs: make(map[string]*MaterializedCTE),
	}
	ctx.CTEs["x"] = &CTEDefinition{
		Name:        "x",
		IsRecursive: false,
		DependsOn:   []string{"x"},
		Select:      &parser.SelectStmt{},
	}

	err := ctx.calculateAllLevels()
	if err == nil {
		t.Fatal("expected error for non-recursive self-referencing CTE, got nil")
	}
	if !strings.Contains(err.Error(), "non-recursive") {
		t.Errorf("unexpected error text: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CTE: materializeDependencies
// ---------------------------------------------------------------------------

// TestCTEExplain_MaterializeDependencies exercises materializeDependencies by
// materializing a CTE whose dependency must be materialized first.
func TestCTEExplain_MaterializeDependencies(t *testing.T) {
	t.Parallel()
	ctx := mustParseCTECtx(t,
		`WITH base AS (SELECT 1 AS n),
		      derived AS (SELECT * FROM base)
		 SELECT * FROM derived`)

	mat, err := ctx.MaterializeCTE("derived")
	if err != nil {
		t.Fatalf("MaterializeCTE(derived): %v", err)
	}
	if mat == nil {
		t.Fatal("expected non-nil MaterializedCTE")
	}
	// base must have been materialized as a dependency
	if _, ok := ctx.MaterializedCTEs["base"]; !ok {
		t.Error("expected 'base' to be materialized as dependency")
	}
	// Self-skip: materialise a recursive CTE with self-dep
	ctxR := mustParseCTECtx(t,
		`WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3)
		 SELECT * FROM r`)
	matR, err := ctxR.MaterializeCTE("r")
	if err != nil {
		t.Fatalf("MaterializeCTE(r): %v", err)
	}
	if matR == nil {
		t.Fatal("expected non-nil MaterializedCTE for recursive")
	}
}

// ---------------------------------------------------------------------------
// CTE: checkVisitingCycle / checkDependencies / validateDependency / validateSelfReference
// ---------------------------------------------------------------------------

// TestCTEExplain_CheckVisitingCycle exercises checkVisitingCycle.
// For a recursive CTE the cycle is allowed; for non-recursive it errors.
func TestCTEExplain_CheckVisitingCycle(t *testing.T) {
	t.Parallel()

	t.Run("recursive_cycle_ok", func(t *testing.T) {
		t.Parallel()
		ctx := &CTEContext{CTEs: make(map[string]*CTEDefinition)}
		ctx.CTEs["r"] = &CTEDefinition{Name: "r", IsRecursive: true}
		visiting := map[string]bool{"r": true}
		if err := ctx.checkVisitingCycle("r", visiting); err != nil {
			t.Errorf("expected nil for recursive cycle, got %v", err)
		}
	})

	t.Run("non_recursive_cycle_error", func(t *testing.T) {
		t.Parallel()
		ctx := &CTEContext{CTEs: make(map[string]*CTEDefinition)}
		ctx.CTEs["nr"] = &CTEDefinition{Name: "nr", IsRecursive: false}
		visiting := map[string]bool{"nr": true}
		err := ctx.checkVisitingCycle("nr", visiting)
		if err == nil {
			t.Fatal("expected error for non-recursive cycle")
		}
		if !strings.Contains(err.Error(), "circular dependency") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("not_visiting_ok", func(t *testing.T) {
		t.Parallel()
		ctx := &CTEContext{CTEs: make(map[string]*CTEDefinition)}
		ctx.CTEs["x"] = &CTEDefinition{Name: "x", IsRecursive: false}
		if err := ctx.checkVisitingCycle("x", map[string]bool{}); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})
}

// TestCTEExplain_ValidateSelfReference exercises validateSelfReference directly.
func TestCTEExplain_ValidateSelfReference(t *testing.T) {
	t.Parallel()

	ctx := &CTEContext{}

	t.Run("recursive_self_ref_ok", func(t *testing.T) {
		t.Parallel()
		if err := ctx.validateSelfReference("r", true); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})

	t.Run("non_recursive_self_ref_error", func(t *testing.T) {
		t.Parallel()
		err := ctx.validateSelfReference("nr", false)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "non-recursive CTE cannot reference itself") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestCTEExplain_CheckDependencies exercises checkDependencies and validateDependency
// through ValidateCTEs on a non-trivial dependency graph.
func TestCTEExplain_CheckDependencies(t *testing.T) {
	t.Parallel()

	t.Run("valid_chain", func(t *testing.T) {
		t.Parallel()
		ctx := mustParseCTECtx(t,
			`WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b`)
		if err := ctx.ValidateCTEs(); err != nil {
			t.Errorf("unexpected validation error: %v", err)
		}
	})

	t.Run("valid_recursive", func(t *testing.T) {
		t.Parallel()
		ctx := mustParseCTECtx(t,
			`WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5)
			 SELECT * FROM r`)
		if err := ctx.ValidateCTEs(); err != nil {
			t.Errorf("unexpected validation error: %v", err)
		}
	})

	t.Run("non_recursive_self_dep_error", func(t *testing.T) {
		t.Parallel()
		ctx := &CTEContext{
			CTEs:             make(map[string]*CTEDefinition),
			IsRecursive:      false,
			MaterializedCTEs: make(map[string]*MaterializedCTE),
		}
		ctx.CTEs["bad"] = &CTEDefinition{
			Name:        "bad",
			IsRecursive: false,
			DependsOn:   []string{"bad"},
			Select:      &parser.SelectStmt{},
		}
		err := ctx.ValidateCTEs()
		if err == nil {
			t.Fatal("expected validation error")
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: AddNodeWithCost
// ---------------------------------------------------------------------------

// TestCTEExplain_AddNodeWithCost exercises AddNodeWithCost.
func TestCTEExplain_AddNodeWithCost(t *testing.T) {
	t.Parallel()

	plan := NewExplainPlan()
	root := plan.AddNodeWithCost(nil, "ROOT", 1000, 42.5)
	if root == nil {
		t.Fatal("expected non-nil node")
	}
	if root.EstimatedRows != 1000 {
		t.Errorf("EstimatedRows: want 1000, got %d", root.EstimatedRows)
	}
	if root.EstimatedCost != 42.5 {
		t.Errorf("EstimatedCost: want 42.5, got %f", root.EstimatedCost)
	}

	child := plan.AddNodeWithCost(root, "CHILD", 50, 3.14)
	if child.Parent != root.ID {
		t.Errorf("child.Parent: want %d, got %d", root.ID, child.Parent)
	}
	if child.EstimatedCost != 3.14 {
		t.Errorf("child.EstimatedCost: want 3.14, got %f", child.EstimatedCost)
	}
}

// ---------------------------------------------------------------------------
// Explain: explainCompound (UNION / UNION ALL)
// ---------------------------------------------------------------------------

// TestCTEExplain_ExplainCompound exercises explainCompound via GenerateExplain.
func TestCTEExplain_ExplainCompound(t *testing.T) {
	t.Parallel()

	stmt := &parser.SelectStmt{
		Compound: &parser.CompoundSelect{
			Op: parser.CompoundUnionAll,
			Left: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "t1"}},
				},
			},
			Right: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "t2"}},
				},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain: %v", err)
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "COMPOUND") {
		t.Errorf("expected COMPOUND in plan text, got:\n%s", text)
	}
	if !strings.Contains(text, "t1") || !strings.Contains(text, "t2") {
		t.Errorf("expected t1 and t2 in plan text, got:\n%s", text)
	}
}

// TestCTEExplain_ExplainUnion exercises UNION (not ALL).
func TestCTEExplain_ExplainUnion(t *testing.T) {
	t.Parallel()

	stmt := &parser.SelectStmt{
		Compound: &parser.CompoundSelect{
			Op: parser.CompoundUnion,
			Left: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "a"}},
				},
			},
			Right: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "b"}},
				},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain: %v", err)
	}
	if len(plan.Roots) == 0 {
		t.Fatal("expected at least one root node")
	}
}

// ---------------------------------------------------------------------------
// Explain: explainSelect (simple + compound path)
// ---------------------------------------------------------------------------

// TestCTEExplain_ExplainSelect covers explainSelect routing to compound and simple.
func TestCTEExplain_ExplainSelect(t *testing.T) {
	t.Parallel()

	t.Run("simple_scan", func(t *testing.T) {
		t.Parallel()
		plan, err := GenerateExplain(&parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "users"}},
			},
		})
		if err != nil {
			t.Fatalf("GenerateExplain: %v", err)
		}
		if !strings.Contains(plan.FormatAsText(), "users") {
			t.Error("expected 'users' in plan")
		}
	})

	t.Run("compound_select", func(t *testing.T) {
		t.Parallel()
		plan, err := GenerateExplain(&parser.SelectStmt{
			Compound: &parser.CompoundSelect{
				Op:    parser.CompoundUnionAll,
				Left:  &parser.SelectStmt{From: &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "p"}}}},
				Right: &parser.SelectStmt{From: &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "q"}}}},
			},
		})
		if err != nil {
			t.Fatalf("GenerateExplain: %v", err)
		}
		text := plan.FormatAsText()
		if !strings.Contains(text, "COMPOUND") {
			t.Errorf("expected COMPOUND, got:\n%s", text)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: emitJoinScanNode
// ---------------------------------------------------------------------------

// TestCTEExplain_EmitJoinScanNode exercises emitJoinScanNode for both a named
// table join and a subquery join.
func TestCTEExplain_EmitJoinScanNode(t *testing.T) {
	t.Parallel()

	t.Run("named_table_join", func(t *testing.T) {
		t.Parallel()
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "orders"}},
				Joins: []parser.JoinClause{
					{
						Type:  parser.JoinLeft,
						Table: parser.TableOrSubquery{TableName: "items"},
						Condition: parser.JoinCondition{
							On: &parser.BinaryExpr{
								Op:    parser.OpEq,
								Left:  &parser.IdentExpr{Name: "order_id"},
								Right: &parser.IdentExpr{Name: "id"},
							},
						},
					},
				},
			},
		}
		plan, err := GenerateExplain(stmt)
		if err != nil {
			t.Fatalf("GenerateExplain: %v", err)
		}
		text := plan.FormatAsText()
		if !strings.Contains(text, "items") {
			t.Errorf("expected 'items' in plan, got:\n%s", text)
		}
	})

	t.Run("subquery_join", func(t *testing.T) {
		t.Parallel()
		// Call emitJoinScanNode directly to exercise the subquery branch,
		// since hasFromSubqueries routes away when joins contain subqueries.
		plan := NewExplainPlan()
		root := plan.AddNode(nil, "ROOT")
		c := &explainCtx{plan: plan}
		join := parser.JoinClause{
			Type: parser.JoinInner,
			Table: parser.TableOrSubquery{
				Subquery: &parser.SelectStmt{
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{{TableName: "sub_tbl"}},
					},
				},
			},
		}
		c.emitJoinScanNode(root, join)
		text := plan.FormatAsText()
		if !strings.Contains(text, "SUBQUERY") {
			t.Errorf("expected SUBQUERY in plan, got:\n%s", text)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: formatScanDetail
// ---------------------------------------------------------------------------

// TestCTEExplain_FormatScanDetail exercises all branches of formatScanDetail:
// nil WHERE, rowid lookup, indexed column, and plain scan.
func TestCTEExplain_FormatScanDetail(t *testing.T) {
	t.Parallel()

	s := newSchemaWithIndex("products", "sku", "idx_products_sku", false)

	// also add an INTEGER PRIMARY KEY column
	s.AddTableDirect(&schema.Table{
		Name: "catalog",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
	})

	c := &explainCtx{plan: NewExplainPlan(), schema: s}

	t.Run("nil_where_scan", func(t *testing.T) {
		t.Parallel()
		lc := &explainCtx{plan: NewExplainPlan(), schema: s}
		detail := lc.formatScanDetail("products", nil)
		if !strings.HasPrefix(detail, "SCAN TABLE") {
			t.Errorf("want SCAN TABLE, got: %s", detail)
		}
	})

	t.Run("rowid_alias_search", func(t *testing.T) {
		t.Parallel()
		lc := &explainCtx{plan: NewExplainPlan(), schema: s}
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "rowid"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
		detail := lc.formatScanDetail("products", where)
		if !strings.Contains(detail, "INTEGER PRIMARY KEY") {
			t.Errorf("want INTEGER PRIMARY KEY detail, got: %s", detail)
		}
	})

	t.Run("indexed_column_equality", func(t *testing.T) {
		t.Parallel()
		lc := &explainCtx{plan: NewExplainPlan(), schema: s}
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "sku"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "ABC"},
		}
		detail := lc.formatScanDetail("products", where)
		if !strings.Contains(detail, "USING INDEX") {
			t.Errorf("want USING INDEX, got: %s", detail)
		}
	})

	t.Run("unindexed_column_scan", func(t *testing.T) {
		t.Parallel()
		lc := &explainCtx{plan: c.plan, schema: s}
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "unindexed_col"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "x"},
		}
		detail := lc.formatScanDetail("products", where)
		if !strings.HasPrefix(detail, "SCAN TABLE") {
			t.Errorf("want SCAN TABLE, got: %s", detail)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: isRowidLookup
// ---------------------------------------------------------------------------

// TestCTEExplain_IsRowidLookup covers the rowid-alias path and the schema
// INTEGER PRIMARY KEY path.
// newExplainCtxForRowid creates an explainCtx for rowid lookup tests.
func newExplainCtxForRowid(s *schema.Schema) *explainCtx {
	return &explainCtx{plan: NewExplainPlan(), schema: s}
}

// assertRowidDetected checks that isRowidLookup returns the expected isRowid flag.
func assertRowidDetected(t *testing.T, s *schema.Schema, table string, where parser.Expression, wantRowid bool) {
	t.Helper()
	col, isRowid := newExplainCtxForRowid(s).isRowidLookup(table, where)
	if isRowid != wantRowid {
		t.Errorf("isRowidLookup: isRowid=%v, want %v (col=%q)", isRowid, wantRowid, col)
	}
}

func TestCTEExplain_IsRowidLookup(t *testing.T) {
	t.Parallel()

	s := schema.NewSchema()
	s.AddTableDirect(&schema.Table{
		Name: "things",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "label", Type: "TEXT"},
		},
	})

	eqWhere := func(col string) parser.Expression {
		return &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: col},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
	}

	t.Run("rowid_alias", func(t *testing.T) {
		t.Parallel()
		assertRowidDetected(t, s, "things", eqWhere("rowid"), true)
	})

	t.Run("oid_alias", func(t *testing.T) {
		t.Parallel()
		assertRowidDetected(t, s, "things", eqWhere("oid"), true)
	})

	t.Run("integer_pk_via_schema", func(t *testing.T) {
		t.Parallel()
		assertRowidDetected(t, s, "things", eqWhere("id"), true)
	})

	t.Run("non_pk_column", func(t *testing.T) {
		t.Parallel()
		assertRowidDetected(t, s, "things", eqWhere("label"), false)
	})

	t.Run("no_schema", func(t *testing.T) {
		t.Parallel()
		assertRowidDetected(t, nil, "things", eqWhere("id"), false)
	})

	t.Run("non_binary_expr", func(t *testing.T) {
		t.Parallel()
		col, isRowid := newExplainCtxForRowid(s).isRowidLookup("things", &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"})
		if col != "" || isRowid {
			t.Errorf("expected empty col and false isRowid, got col=%q isRowid=%v", col, isRowid)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: findIndexForColumn
// ---------------------------------------------------------------------------

// TestCTEExplain_FindIndexForColumn exercises findIndexForColumn: nil schema,
// no matching index, and a match on the first column.
func TestCTEExplain_FindIndexForColumn(t *testing.T) {
	t.Parallel()

	s := newSchemaWithIndex("emp", "dept", "idx_emp_dept", false)

	t.Run("nil_schema", func(t *testing.T) {
		t.Parallel()
		c := &explainCtx{plan: NewExplainPlan(), schema: nil}
		idx := c.findIndexForColumn("emp", "dept")
		if idx != nil {
			t.Error("expected nil with no schema")
		}
	})

	t.Run("no_match", func(t *testing.T) {
		t.Parallel()
		c := &explainCtx{plan: NewExplainPlan(), schema: s}
		idx := c.findIndexForColumn("emp", "salary")
		if idx != nil {
			t.Error("expected nil for unindexed column")
		}
	})

	t.Run("match", func(t *testing.T) {
		t.Parallel()
		c := &explainCtx{plan: NewExplainPlan(), schema: s}
		idx := c.findIndexForColumn("emp", "dept")
		if idx == nil {
			t.Fatal("expected non-nil index")
		}
		if idx.Name != "idx_emp_dept" {
			t.Errorf("unexpected index name: %s", idx.Name)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: formatIndexDetail
// ---------------------------------------------------------------------------

// TestCTEExplain_FormatIndexDetail exercises both equality and range branches.
func TestCTEExplain_FormatIndexDetail(t *testing.T) {
	t.Parallel()

	idx := &schema.Index{Name: "idx_foo", Table: "foo", Columns: []string{"bar"}}
	c := &explainCtx{plan: NewExplainPlan()}

	t.Run("equality", func(t *testing.T) {
		t.Parallel()
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "bar"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "v"},
		}
		detail := c.formatIndexDetail("foo", idx, "bar", where)
		if !strings.Contains(detail, "bar=?") {
			t.Errorf("expected equality format, got: %s", detail)
		}
	})

	t.Run("range", func(t *testing.T) {
		t.Parallel()
		where := &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Name: "bar"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		}
		detail := c.formatIndexDetail("foo", idx, "bar", where)
		if !strings.Contains(detail, "bar>?") {
			t.Errorf("expected range format, got: %s", detail)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: walkExprForSubqueries
// ---------------------------------------------------------------------------

// TestCTEExplain_WalkExprForSubqueries exercises all handled expression types
// in walkExprForSubqueries.
// countWalkSubqueries walks the given expressions and counts SUBQUERY children.
func countWalkSubqueries(exprs ...parser.Expression) int {
	plan := NewExplainPlan()
	root := plan.AddNode(nil, "ROOT")
	c := &explainCtx{plan: plan}
	for _, e := range exprs {
		c.walkExprForSubqueries(root, e)
	}
	n := 0
	for _, ch := range root.Children {
		if strings.Contains(ch.Detail, "SUBQUERY") {
			n++
		}
	}
	return n
}

func TestCTEExplain_WalkExprForSubqueries(t *testing.T) {
	t.Parallel()

	innerSel := &parser.SelectStmt{
		From: &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "inner"}}},
	}

	t.Run("SubqueryExpr", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.SubqueryExpr{Select: innerSel}); n != 1 {
			t.Errorf("expected 1 subquery node, got %d", n)
		}
	})

	t.Run("ExistsExpr", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.ExistsExpr{Select: innerSel}); n != 1 {
			t.Errorf("expected 1 subquery node, got %d", n)
		}
	})

	t.Run("InExpr_with_select", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.InExpr{Expr: &parser.IdentExpr{Name: "id"}, Select: innerSel}); n != 1 {
			t.Errorf("expected 1 subquery node, got %d", n)
		}
	})

	t.Run("InExpr_no_select", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.InExpr{Expr: &parser.IdentExpr{Name: "id"}, Values: []parser.Expression{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}}}); n != 0 {
			t.Errorf("expected 0 subquery nodes, got %d", n)
		}
	})

	t.Run("BinaryExpr_with_subquery_child", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.BinaryExpr{Op: parser.OpEq, Left: &parser.SubqueryExpr{Select: innerSel}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}}); n != 1 {
			t.Errorf("expected 1 subquery node, got %d", n)
		}
	})

	t.Run("UnaryAndParenExpr_with_subquery", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(&parser.UnaryExpr{Op: parser.OpNot, Expr: &parser.SubqueryExpr{Select: innerSel}}); n != 1 {
			t.Errorf("UnaryExpr: expected 1 subquery node, got %d", n)
		}
		if n := countWalkSubqueries(&parser.ParenExpr{Expr: &parser.SubqueryExpr{Select: innerSel}}); n != 1 {
			t.Errorf("ParenExpr: expected 1 subquery node, got %d", n)
		}
	})

	t.Run("nil_expr", func(t *testing.T) {
		t.Parallel()
		if n := countWalkSubqueries(nil); n != 0 {
			t.Errorf("expected 0 for nil, got %d", n)
		}
	})
}

// ---------------------------------------------------------------------------
// Explain: isEqualityWhere
// ---------------------------------------------------------------------------

// TestCTEExplain_IsEqualityWhere exercises isEqualityWhere.
func TestCTEExplain_IsEqualityWhere(t *testing.T) {
	t.Parallel()

	t.Run("equality", func(t *testing.T) {
		t.Parallel()
		where := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "x"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		}
		if !isEqualityWhere(where) {
			t.Error("expected true for OpEq")
		}
	})

	t.Run("greater_than", func(t *testing.T) {
		t.Parallel()
		where := &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Name: "x"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		}
		if isEqualityWhere(where) {
			t.Error("expected false for OpGt")
		}
	})

	t.Run("non_binary", func(t *testing.T) {
		t.Parallel()
		if isEqualityWhere(&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}) {
			t.Error("expected false for non-BinaryExpr")
		}
	})
}
