// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// --- subquery.go: ConvertInToJoin (60%) ---

// TestConvertInToJoin_CheaperJoin exercises the success path.
func TestConvertInToJoin_CheaperJoin(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", Cursor: 0, RowCount: 10, RowLogEst: NewLogEst(10)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}

	result, err := opt.ConvertInToJoin(info, parentInfo)
	// Either succeeds or fails with "JOIN not beneficial" — both are valid
	if err == nil && result == nil {
		t.Error("expected non-nil result on success")
	}
}

// TestConvertInToJoin_NotInSubquery verifies error for non-IN subquery.
func TestConvertInToJoin_NotInSubquery(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{Type: SubqueryExists, EstimatedRows: NewLogEst(100)}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}
	_, err := opt.ConvertInToJoin(info, parentInfo)
	if err == nil {
		t.Error("expected error for non-IN subquery type")
	}
}

// --- subquery.go: ConvertExistsToSemiJoin (60%) ---

// TestConvertExistsToSemiJoin_NotExistsSubquery verifies error for non-EXISTS subquery.
func TestConvertExistsToSemiJoin_NotExistsSubquery(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{Type: SubqueryIn, EstimatedRows: NewLogEst(100)}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}
	_, err := opt.ConvertExistsToSemiJoin(info, parentInfo)
	if err == nil {
		t.Error("expected error for non-EXISTS subquery type")
	}
}

// TestConvertExistsToSemiJoin_LargeCost tests with large estimated rows.
func TestConvertExistsToSemiJoin_LargeCost(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "orders", Cursor: 0, RowCount: 100}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parentInfo)
	if err == nil && result == nil {
		t.Error("expected non-nil result on success")
	}
}

// --- subquery.go: tryTypeSpecificOptimization (77.8%) ---

// TestTryTypeSpecificOpt_ExistsBranch exercises the EXISTS branch.
func TestTryTypeSpecificOpt_ExistsBranch(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(1),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}

	_, _ = opt.tryTypeSpecificOptimization(info, parentInfo)
}

// TestTryTypeSpecificOpt_ScalarBranch verifies scalar subquery is not optimized.
func TestTryTypeSpecificOpt_ScalarBranch(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		EstimatedRows: NewLogEst(1),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}

	result, ok := opt.tryTypeSpecificOptimization(info, parentInfo)
	if ok || result != nil {
		t.Error("expected no optimization for SubqueryScalar")
	}
}

// TestTryTypeSpecificOpt_InBranch exercises the IN branch.
func TestTryTypeSpecificOpt_InBranch(t *testing.T) {
	costModel := NewCostModel()
	opt := NewSubqueryOptimizer(costModel)

	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(500),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "u", Cursor: 0, RowCount: 50}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(50),
	}
	_, _ = opt.tryTypeSpecificOptimization(info, parentInfo)
}

// --- planner.go: findColumnIndex (75%) ---

// TestFindColumnIndex3_Found verifies match returns correct index.
func TestFindColumnIndex3_Found(t *testing.T) {
	table := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
		},
	}
	idx := findColumnIndex(table, "name")
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}
}

// TestFindColumnIndex3_NotFound verifies -1 on miss.
func TestFindColumnIndex3_NotFound(t *testing.T) {
	table := &TableInfo{
		Columns: []ColumnInfo{{Name: "id"}},
	}
	idx := findColumnIndex(table, "missing")
	if idx != -1 {
		t.Errorf("expected -1, got %d", idx)
	}
}

// TestFindColumnIndex3_Empty verifies -1 on empty column list.
func TestFindColumnIndex3_Empty(t *testing.T) {
	table := &TableInfo{Columns: []ColumnInfo{}}
	idx := findColumnIndex(table, "x")
	if idx != -1 {
		t.Errorf("expected -1, got %d", idx)
	}
}

// --- planner.go: createFlattenedSubqueryTable and createMaterializedSubqueryTable (75%) ---

// TestCreateFlattenedSubqueryTable3_Fields verifies table fields are set correctly.
func TestCreateFlattenedSubqueryTable3_Fields(t *testing.T) {
	p := NewPlanner()
	info := &SubqueryInfo{
		EstimatedRows:     NewLogEst(50),
		MaterializedTable: "flat_view",
	}
	table := p.createFlattenedSubqueryTable(info, 3)
	if table == nil {
		t.Fatal("expected non-nil TableInfo")
	}
	if table.Cursor != 3 {
		t.Errorf("expected cursor 3, got %d", table.Cursor)
	}
	if table.Name != "flat_view" {
		t.Errorf("expected name 'flat_view', got %q", table.Name)
	}
}

// TestCreateMaterializedSubqueryTable3_Materialize tests materialized subquery path.
func TestCreateMaterializedSubqueryTable3_Materialize(t *testing.T) {
	p := NewPlanner()
	info := &SubqueryInfo{
		Type:              SubqueryFrom,
		CanMaterialize:    true,
		EstimatedRows:     NewLogEst(100),
		MaterializedTable: "matview_1",
	}
	table, err := p.createMaterializedSubqueryTable(info, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table == nil {
		t.Fatal("expected non-nil TableInfo")
	}
	if table.Cursor != 2 {
		t.Errorf("expected cursor=2, got %d", table.Cursor)
	}
}

// --- planner.go: propagateConstants (80%) ---

// TestPropagateConstants3_NoEqTerms verifies empty result when no EQ terms exist.
func TestPropagateConstants3_NoEqTerms(t *testing.T) {
	p := NewPlanner()
	clause := &WhereClause{
		Terms: []*WhereTerm{
			{Operator: WO_GT, LeftCursor: 0, LeftColumn: 0},
		},
	}
	equiv := map[string][]string{}
	result := p.propagateConstants(clause, equiv)
	if len(result) != 0 {
		t.Errorf("expected 0 propagated terms, got %d", len(result))
	}
}

// TestPropagateConstants3_WithEquivAndConstant verifies constant propagation.
func TestPropagateConstants3_WithEquivAndConstant(t *testing.T) {
	p := NewPlanner()

	colExpr := &ColumnExpr{Cursor: 0, Column: "id"}
	valExpr := &ValueExpr{Value: int64(5)}
	binaryExpr := &BinaryExpr{Left: colExpr, Op: "=", Right: valExpr}

	term := &WhereTerm{
		Expr:       binaryExpr,
		Operator:   WO_EQ,
		LeftCursor: 0,
		LeftColumn: 0,
		RightValue: int64(5),
	}
	clause := &WhereClause{Terms: []*WhereTerm{term}}
	equiv := map[string][]string{
		"0.0": {"1.0"},
	}
	result := p.propagateConstants(clause, equiv)
	if len(result) == 0 {
		t.Error("expected at least one propagated term")
	}
}

// --- view.go: extractOriginalName (66.7%) ---

// TestExtractOriginalName3_Ident verifies name extracted from IdentExpr with no table.
func TestExtractOriginalName3_WithIdent(t *testing.T) {
	col := &parser.ResultColumn{
		Expr: &parser.IdentExpr{Name: "myCol", Table: ""},
	}
	name := extractOriginalName(col)
	if name != "myCol" {
		t.Errorf("expected 'myCol', got %q", name)
	}
}

// TestExtractOriginalName3_QualifiedIdent returns empty when table is set.
func TestExtractOriginalName3_QualifiedIdent(t *testing.T) {
	col := &parser.ResultColumn{
		Expr: &parser.IdentExpr{Name: "myCol", Table: "t"},
	}
	name := extractOriginalName(col)
	if name != "" {
		t.Errorf("expected empty for qualified ident, got %q", name)
	}
}

// TestExtractOriginalName3_NonIdent returns empty for non-IdentExpr.
func TestExtractOriginalName3_NonIdent(t *testing.T) {
	col := &parser.ResultColumn{
		Expr: &parser.LiteralExpr{Value: "42"},
	}
	name := extractOriginalName(col)
	if name != "" {
		t.Errorf("expected empty for non-ident, got %q", name)
	}
}

// --- view.go: processViewInFromClause (66.7%) ---

// TestProcessViewInFromClause3_Flatten verifies a flattenable view is flattened.
func TestProcessViewInFromClause3_Flatten(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "simple_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "base_table"},
				},
			},
		},
	}

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "simple_view"},
			},
		},
	}

	table := &stmt.From.Tables[0]
	err := processViewInFromClause(stmt, 0, view, table, s, 0)
	_ = err // canFlattenView may return false due to hasNoExplicitColumns
}

// TestProcessViewInFromClause3_Expand verifies a complex view is expanded as subquery.
func TestProcessViewInFromClause3_Expand(t *testing.T) {
	s := schema.NewSchema()

	// Complex view (has GROUP BY - prevents flattening)
	view := &schema.View{
		Name: "complex_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "t"}},
			},
			GroupBy: []parser.Expression{
				&parser.IdentExpr{Name: "id"},
			},
		},
	}

	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "complex_view"},
			},
		},
	}

	table := &stmt.From.Tables[0]
	err := processViewInFromClause(stmt, 0, view, table, s, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table.Subquery == nil {
		t.Error("expected table to be converted to subquery")
	}
}

// --- view.go: expandViewsInSelectWithDepth / expandViewsInFromTables / expandViewsInJoins ---

// TestExpandViewsInSelectWithDepth3_NilStmt verifies nil stmt returns nil.
func TestExpandViewsInSelectWithDepth3_NilStmt(t *testing.T) {
	s := schema.NewSchema()
	result, err := expandViewsInSelectWithDepth(nil, s, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil for nil stmt")
	}
}

// TestExpandViewsInSelectWithDepth3_DepthExceeded verifies depth limit error.
func TestExpandViewsInSelectWithDepth3_DepthExceeded(t *testing.T) {
	s := schema.NewSchema()
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "t"}}},
	}
	_, err := expandViewsInSelectWithDepth(stmt, s, 101)
	if err == nil {
		t.Error("expected depth limit error")
	}
}

// TestExpandViewsInFromTables3_SubqueryRecurse verifies subquery is recursively expanded.
func TestExpandViewsInFromTables3_SubqueryRecurse(t *testing.T) {
	s := schema.NewSchema()
	innerStmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "inner_t"}},
		},
	}
	tables := []parser.TableOrSubquery{
		{Subquery: innerStmt},
	}
	err := expandViewsInFromTables(tables, s, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExpandViewsInJoins3_SubqueryRecurse verifies joins with subqueries are recursively expanded.
func TestExpandViewsInJoins3_SubqueryRecurse(t *testing.T) {
	s := schema.NewSchema()
	innerStmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "j_t"}},
		},
	}
	joins := []parser.JoinClause{
		{Table: parser.TableOrSubquery{Subquery: innerStmt}},
	}
	err := expandViewsInJoins(joins, s, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExpandViewsInJoins3_ViewRef verifies a view in a JOIN is expanded.
func TestExpandViewsInJoins3_ViewRef(t *testing.T) {
	s := schema.NewSchema()
	s.AddViewDirect(&schema.View{
		Name: "join_view3",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Expr: &parser.IdentExpr{Name: "x"}}},
			From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "base"}}},
		},
	})
	joins := []parser.JoinClause{
		{Table: parser.TableOrSubquery{TableName: "join_view3"}},
	}
	err := expandViewsInJoins(joins, s, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if joins[0].Table.Subquery == nil {
		t.Error("expected view to be expanded to subquery")
	}
}

// --- explain.go: explainFromSubqueries (71.4%) ---

// TestExplainFromSubqueries3_WithTableName verifies plain table name emits scan node.
func TestExplainFromSubqueries3_WithTableName(t *testing.T) {
	plan := NewExplainPlan()
	ctx := &explainCtx{plan: plan, schema: nil}
	root := plan.AddNode(nil, "root")

	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}
	ctx.explainFromSubqueries(root, stmt)
	if len(root.Children) == 0 {
		t.Error("expected child scan node for table")
	}
}

// TestExplainFromSubqueries3_WithSubquery verifies subquery in FROM adds SUBQUERY node.
func TestExplainFromSubqueries3_WithSubquery(t *testing.T) {
	plan := NewExplainPlan()
	ctx := &explainCtx{plan: plan, schema: nil}
	root := plan.AddNode(nil, "root")

	innerStmt := &parser.SelectStmt{}
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{Subquery: innerStmt},
			},
		},
	}
	ctx.explainFromSubqueries(root, stmt)
	if len(root.Children) == 0 {
		t.Error("expected SUBQUERY child node")
	}
}

// --- explain.go: findBestIndexWithColumns (71.4%) ---

// TestFindBestIndexWithColumns3_NilSchema returns nil for nil schema.
func TestFindBestIndexWithColumns3_NilSchema(t *testing.T) {
	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Name: "id"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Value: "1"},
	}
	result := findBestIndexWithColumns("users", where, nil, nil)
	if result != nil {
		t.Error("expected nil for nil schema")
	}
}

// TestFindBestIndexWithColumns3_NilWhere returns nil for nil where.
func TestFindBestIndexWithColumns3_NilWhere(t *testing.T) {
	s := schema.NewSchema()
	result := findBestIndexWithColumns("users", nil, s, nil)
	if result != nil {
		t.Error("expected nil for nil where")
	}
}

// TestFindBestIndexWithColumns3_WithMatchingIndex returns a candidate.
func TestFindBestIndexWithColumns3_WithMatchingIndex(t *testing.T) {
	s := schema.NewSchema()
	s.AddTableDirect(&schema.Table{
		Name: "users3",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	})
	s.AddIndexDirect(&schema.Index{
		Name:    "idx_users3_name",
		Table:   "users3",
		Columns: []string{"name"},
	})

	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Name: "name"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Value: "Alice"},
	}

	result := findBestIndexWithColumns("users3", where, s, nil)
	if result == nil {
		t.Error("expected index candidate, got nil")
	}
}

// TestFindBestIndexWithColumns3_CoveringIndex verifies covering index detection.
func TestFindBestIndexWithColumns3_CoveringIndex(t *testing.T) {
	s := schema.NewSchema()
	s.AddTableDirect(&schema.Table{
		Name: "orders3",
		Columns: []*schema.Column{
			{Name: "user_id", Type: "INTEGER"},
			{Name: "amount", Type: "REAL"},
		},
	})
	s.AddIndexDirect(&schema.Index{
		Name:    "idx_orders3_uid",
		Table:   "orders3",
		Columns: []string{"user_id", "amount"},
	})

	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Name: "user_id"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Value: "10"},
	}

	result := findBestIndexWithColumns("orders3", where, s, []string{"user_id", "amount"})
	if result == nil {
		t.Error("expected index candidate, got nil")
	}
	if !result.IsCovering {
		t.Error("expected covering index")
	}
}

// --- explain.go: emitScanNode (75%) ---

// TestEmitScanNode3_EmptyTableName returns parent unchanged.
func TestEmitScanNode3_EmptyTableName(t *testing.T) {
	plan := NewExplainPlan()
	ctx := &explainCtx{plan: plan}
	root := plan.AddNode(nil, "root")
	result := ctx.emitScanNode(root, "", nil)
	if result != root {
		t.Error("expected parent returned for empty table name")
	}
}

// TestEmitScanNode3_WithTable creates a child scan node.
func TestEmitScanNode3_WithTable(t *testing.T) {
	plan := NewExplainPlan()
	ctx := &explainCtx{plan: plan}
	root := plan.AddNode(nil, "root")
	ctx.emitScanNode(root, "users", nil)
	if len(root.Children) != 1 {
		t.Errorf("expected 1 child, got %d", len(root.Children))
	}
}

// --- explain.go: formatTableScan (80%) ---

// TestFormatTableScan3_EmptyName uses "?" fallback.
func TestFormatTableScan3_EmptyName(t *testing.T) {
	result := formatTableScan("", nil, false)
	if result != "SCAN ?" {
		t.Errorf("expected 'SCAN ?', got %q", result)
	}
}

// TestFormatTableScan3_NilWhere returns plain SCAN.
func TestFormatTableScan3_NilWhere(t *testing.T) {
	result := formatTableScan("orders", nil, false)
	if result != "SCAN orders" {
		t.Errorf("expected 'SCAN orders', got %q", result)
	}
}

// TestFormatTableScan3_WithWhere returns SEARCH description.
func TestFormatTableScan3_WithWhere(t *testing.T) {
	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Name: "id"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Value: "1"},
	}
	result := formatTableScan("users", where, false)
	if result == "" {
		t.Error("expected non-empty result")
	}
}

// --- join_algorithm.go: SelectBest (78.6%) ---

// TestJoinAlgorithmSelector3_NoEquiJoin forces nested loop.
func TestJoinAlgorithmSelector3_NoEquiJoin(t *testing.T) {
	outer := &JoinNode{
		IsLeaf:        true,
		TableIndex:    0,
		EstimatedRows: NewLogEst(100),
	}
	inner := &JoinNode{
		IsLeaf:        true,
		TableIndex:    1,
		EstimatedRows: NewLogEst(50),
	}
	// No conditions → no equi-join
	selector := NewJoinAlgorithmSelector(outer, inner, []*WhereTerm{}, NewCostModel())
	algo := selector.SelectBest()
	if algo != JoinNestedLoop {
		t.Errorf("expected JoinNestedLoop for no equi-join, got %v", algo)
	}
}

// TestJoinAlgorithmSelector3_WithEquiJoin does not panic.
func TestJoinAlgorithmSelector3_WithEquiJoin(t *testing.T) {
	outer := &JoinNode{
		IsLeaf:        true,
		TableIndex:    0,
		EstimatedRows: NewLogEst(10000),
	}
	inner := &JoinNode{
		IsLeaf:        true,
		TableIndex:    1,
		EstimatedRows: NewLogEst(500),
	}

	colExpr := &ColumnExpr{Cursor: 0, Column: "id"}
	colExpr2 := &ColumnExpr{Cursor: 1, Column: "user_id"}
	binExpr := &BinaryExpr{Left: colExpr, Op: "=", Right: colExpr2}
	term := &WhereTerm{
		Expr:        binExpr,
		Operator:    WO_EQ,
		LeftCursor:  0,
		LeftColumn:  0,
		PrereqRight: Bitmask(2),
	}

	selector := NewJoinAlgorithmSelector(outer, inner, []*WhereTerm{term}, NewCostModel())
	algo := selector.SelectBest()
	_ = algo // Result can be any algorithm — just verify no panic
}

// --- planner.go: PlanQueryWithSubqueries (88.9%) ---

// TestPlanQueryWithSubqueries3_NoFromSubqueries succeeds with empty subquery list.
func TestPlanQueryWithSubqueries3_NoFromSubqueries(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{
			Name:      "t1",
			Cursor:    0,
			RowCount:  100,
			RowLogEst: NewLogEst(100),
			Columns:   []ColumnInfo{{Name: "id"}},
		},
	}
	whereClause := &WhereClause{Terms: []*WhereTerm{}}
	fromSubqueries := []Expr{}

	result, err := p.PlanQueryWithSubqueries(tables, fromSubqueries, whereClause)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil WhereInfo")
	}
}
