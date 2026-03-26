// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// --- CostEstimator coverage ---

func TestNewCostEstimatorWithSchema(t *testing.T) {
	sch := schema.NewSchema()
	ce := NewCostEstimatorWithSchema(sch)
	if ce == nil {
		t.Fatal("expected non-nil CostEstimator")
	}
	if ce.schemaInfo != sch {
		t.Error("schema not set correctly")
	}
}

func TestCostEstimator_getTableRowCount_NoSchema(t *testing.T) {
	ce := NewCostEstimator()
	rows := ce.getTableRowCount("nonexistent")
	if rows != ce.defaultTableRows {
		t.Errorf("expected %d, got %d", ce.defaultTableRows, rows)
	}
}

func TestCostEstimator_getTableRowCount_WithSchema(t *testing.T) {
	sch := schema.NewSchema()
	ce := NewCostEstimatorWithSchema(sch)
	// Table doesn't exist in schema, should return default
	rows := ce.getTableRowCount("missing_table")
	if rows != ce.defaultTableRows {
		t.Errorf("expected %d, got %d", ce.defaultTableRows, rows)
	}
}

func TestCostEstimator_EstimateTableScan(t *testing.T) {
	ce := NewCostEstimator()
	rows, cost := ce.EstimateTableScan("t", false)
	if rows != ce.defaultTableRows {
		t.Errorf("no-where: expected %d rows, got %d", ce.defaultTableRows, rows)
	}
	if cost <= 0 {
		t.Error("expected positive cost")
	}

	rowsWhere, _ := ce.EstimateTableScan("t", true)
	if rowsWhere >= rows {
		t.Error("with WHERE should produce fewer rows")
	}
}

func TestCostEstimator_EstimateIndexScan(t *testing.T) {
	ce := NewCostEstimator()

	// Unique + equality → 1 row
	rows, cost := ce.EstimateIndexScan("idx", true, false, true)
	if rows != 1 {
		t.Errorf("unique equality: expected 1 row, got %d", rows)
	}
	if cost != 10.0 {
		t.Errorf("expected cost 10, got %f", cost)
	}

	// Non-unique, equality, covering
	rows2, cost2 := ce.EstimateIndexScan("idx", false, true, true)
	// Non-unique, equality, non-covering
	_, cost3 := ce.EstimateIndexScan("idx", false, false, true)
	if cost2 >= cost3 {
		t.Error("covering index should be cheaper")
	}
	if rows2 <= 0 {
		t.Error("expected positive row count")
	}

	// Range scan (no equality)
	rows4, _ := ce.EstimateIndexScan("idx", false, false, false)
	if rows4 <= 0 {
		t.Error("expected positive rows for range scan")
	}
}

func TestCostEstimator_EstimateJoinCost(t *testing.T) {
	ce := NewCostEstimator()
	tests := []struct {
		joinType parser.JoinType
	}{
		{parser.JoinCross},
		{parser.JoinInner},
		{parser.JoinLeft},
		{parser.JoinRight},
		{parser.JoinFull},
		{parser.JoinType(99)}, // default case
	}
	for _, tt := range tests {
		rows, cost := ce.EstimateJoinCost(100, 200, tt.joinType, false)
		if rows < 0 || cost < 0 {
			t.Errorf("join type %v: negative result rows=%d cost=%f", tt.joinType, rows, cost)
		}
	}
	// With index
	_, costIdx := ce.EstimateJoinCost(100, 200, parser.JoinInner, true)
	_, costNoIdx := ce.EstimateJoinCost(100, 200, parser.JoinInner, false)
	if costIdx >= costNoIdx {
		t.Error("index join should be cheaper for large tables")
	}
}

func TestCostEstimator_EstimateAggregateCost(t *testing.T) {
	ce := NewCostEstimator()
	// numGroups == 0 triggers estimation
	rows, cost := ce.EstimateAggregateCost(50000, 0)
	if rows <= 0 || cost <= 0 {
		t.Error("expected positive estimates")
	}
	// numGroups provided
	rows2, cost2 := ce.EstimateAggregateCost(1000, 100)
	if rows2 != 100 {
		t.Errorf("expected 100 groups, got %d", rows2)
	}
	if cost2 <= 0 {
		t.Error("expected positive cost")
	}
	// Small input with numGroups==0
	rows3, _ := ce.EstimateAggregateCost(100, 0)
	if rows3 <= 0 {
		t.Error("expected positive groups for small input")
	}
}

func TestCostEstimator_EstimateSortCost(t *testing.T) {
	ce := NewCostEstimator()
	// Zero input
	rows, cost := ce.EstimateSortCost(0)
	if rows != 0 || cost != 0 {
		t.Error("zero input should yield zero cost")
	}
	// Negative input
	rows2, cost2 := ce.EstimateSortCost(-5)
	if rows2 != 0 || cost2 != 0 {
		t.Error("negative input should yield zero cost")
	}
	// Positive
	rows3, cost3 := ce.EstimateSortCost(100)
	if rows3 != 100 || cost3 <= 0 {
		t.Error("positive input should yield positive cost")
	}
}

// --- mergeSubplan coverage ---

func TestMergeSubplan(t *testing.T) {
	plan := NewExplainPlan()
	parent := plan.AddNode(nil, "parent")
	child := &ExplainNode{
		ID:       99,
		Detail:   "subchild",
		Children: make([]*ExplainNode, 0),
	}
	mergeSubplan(parent, child, plan)
	if len(parent.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(parent.Children))
	}
	if parent.Children[0].Detail != "subchild" {
		t.Error("child detail mismatch")
	}
	// With nested children
	nested := &ExplainNode{
		ID:     100,
		Detail: "nested",
		Children: []*ExplainNode{
			{ID: 101, Detail: "deep", Children: make([]*ExplainNode, 0)},
		},
	}
	mergeSubplan(parent, nested, plan)
	if len(parent.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(parent.Children))
	}
}

// --- termMatchesExpression / extractLeftExprText / normalizeExpr coverage ---

func TestTermMatchesExpression(t *testing.T) {
	// Nil term.Expr
	term := &WhereTerm{}
	if termMatchesExpression(term, "x") {
		t.Error("nil expr should return false")
	}

	// Non-binary expr
	term2 := &WhereTerm{
		Expr: &ValueExpr{Value: 42},
	}
	if termMatchesExpression(term2, "x") {
		t.Error("non-binary expr should return false")
	}

	// Binary expr with nil left
	term3 := &WhereTerm{
		Expr: &BinaryExpr{
			Op:    "=",
			Left:  nil,
			Right: &ValueExpr{Value: 1},
		},
	}
	if termMatchesExpression(term3, "x") {
		t.Error("nil left should return false")
	}

	// Match
	term4 := &WhereTerm{
		Expr: &BinaryExpr{
			Op:    "=",
			Left:  &ColumnExpr{Column: "age"},
			Right: &ValueExpr{Value: 25},
		},
	}
	if !termMatchesExpression(term4, "age") {
		t.Error("expected match")
	}
	if !termMatchesExpression(term4, "AGE") {
		t.Error("expected case-insensitive match")
	}
	if termMatchesExpression(term4, "name") {
		t.Error("should not match different column")
	}
}

func TestExtractLeftExprText(t *testing.T) {
	// Non-binary returns ""
	result := extractLeftExprText(&ValueExpr{Value: 1})
	if result != "" {
		t.Errorf("expected empty, got %q", result)
	}

	// Binary with nil left returns ""
	result2 := extractLeftExprText(&BinaryExpr{Op: "=", Left: nil, Right: &ValueExpr{Value: 1}})
	if result2 != "" {
		t.Errorf("expected empty, got %q", result2)
	}

	// Binary with valid left
	result3 := extractLeftExprText(&BinaryExpr{
		Op:    "=",
		Left:  &ColumnExpr{Column: "id"},
		Right: &ValueExpr{Value: 1},
	})
	if result3 == "" {
		t.Error("expected non-empty result")
	}
}

func TestNormalizeExpr(t *testing.T) {
	result := normalizeExpr("  age  + 1 ")
	for _, ch := range result {
		if ch == ' ' || ch == '\t' || ch == '\n' {
			t.Error("whitespace not removed")
		}
	}
	// Check uppercase
	if normalizeExpr("age") != "AGE" {
		t.Error("expected uppercase")
	}
}

// --- estimateSelectivity / estimateTermSelectivity coverage ---

func TestPlannerEstimateSelectivity(t *testing.T) {
	p := NewPlanner()
	// No terms → 1.0
	loop := &WhereLoop{Terms: nil}
	s := p.estimateSelectivity(loop)
	if s != 1.0 {
		t.Errorf("expected 1.0, got %f", s)
	}

	// With EQ term
	loop2 := &WhereLoop{
		Terms: []*WhereTerm{
			{Operator: WO_EQ},
		},
	}
	s2 := p.estimateSelectivity(loop2)
	if s2 <= 0 || s2 > 1 {
		t.Errorf("expected (0,1], got %f", s2)
	}
}

func TestPlannerEstimateTermSelectivity(t *testing.T) {
	p := NewPlanner()
	tests := []struct {
		op   WhereOperator
		desc string
	}{
		{WO_EQ, "equality"},
		{WO_LT, "less-than"},
		{WO_LE, "less-than-or-equal"},
		{WO_GT, "greater-than"},
		{WO_GE, "greater-than-or-equal"},
		{WO_IN, "in"},
		{WO_IS, "is"},
		{WO_NOOP, "default"},
	}
	for _, tt := range tests {
		term := &WhereTerm{Operator: tt.op}
		s := p.estimateTermSelectivity(term)
		if s <= 0 || s > 1 {
			t.Errorf("%s: selectivity out of (0,1]: %f", tt.desc, s)
		}
	}
}

// --- optimizeWhereSubqueries / detectSubquery coverage ---

func TestOptimizeWhereSubqueries_NilClause(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{
		Tables: []*TableInfo{},
	}
	result, err := p.optimizeWhereSubqueries(info)
	if err != nil {
		t.Fatal(err)
	}
	if result != info {
		t.Error("expected same info returned when no clause")
	}
}

func TestDetectSubquery_Nil(t *testing.T) {
	p := NewPlanner()
	result := p.detectSubquery(nil)
	if result != nil {
		t.Error("expected nil for nil expr")
	}
}

func TestDetectSubquery_SubqueryExpr(t *testing.T) {
	p := NewPlanner()
	inner := &ValueExpr{Value: 1}
	expr := &SubqueryExpr{
		Type:  SubqueryScalar,
		Query: inner,
	}
	result := p.detectSubquery(expr)
	if result == nil {
		t.Fatal("expected SubqueryInfo")
	}
	if result.Type != SubqueryScalar {
		t.Error("wrong type")
	}
}

func TestDetectSubquery_BinaryIN(t *testing.T) {
	p := NewPlanner()
	bin := &BinaryExpr{
		Op:    "IN",
		Left:  &ColumnExpr{Column: "id"},
		Right: &ValueExpr{Value: 1},
	}
	result := p.detectSubquery(bin)
	if result == nil {
		t.Fatal("expected SubqueryInfo for IN")
	}
	if result.Type != SubqueryIn {
		t.Error("expected SubqueryIn type")
	}
}

func TestDetectSubquery_BinaryRecursive(t *testing.T) {
	p := NewPlanner()
	inner := &SubqueryExpr{Type: SubqueryExists, Query: &ValueExpr{Value: 1}}
	bin := &BinaryExpr{
		Op:   "AND",
		Left: inner,
	}
	result := p.detectSubquery(bin)
	if result == nil {
		t.Fatal("expected SubqueryInfo from left child")
	}
}

func TestDetectSubquery_BinaryRightRecursive(t *testing.T) {
	p := NewPlanner()
	inner := &SubqueryExpr{Type: SubqueryExists, Query: &ValueExpr{Value: 1}}
	bin := &BinaryExpr{
		Op:    "AND",
		Left:  &ValueExpr{Value: 1},
		Right: inner,
	}
	result := p.detectSubquery(bin)
	if result == nil {
		t.Fatal("expected SubqueryInfo from right child")
	}
}

// --- ColumnExpr.String() with and without table ---

func TestColumnExprString(t *testing.T) {
	withTable := &ColumnExpr{Table: "users", Column: "id"}
	if withTable.String() != "users.id" {
		t.Errorf("expected 'users.id', got %q", withTable.String())
	}
	withoutTable := &ColumnExpr{Column: "id"}
	if withoutTable.String() != "id" {
		t.Errorf("expected 'id', got %q", withoutTable.String())
	}
}

// --- SubqueryType.String() coverage (unique name to avoid redeclaration) ---

func TestSubqueryTypeStringCoverage(t *testing.T) {
	cases := []struct {
		tp   SubqueryType
		want string
	}{
		{SubqueryScalar, "SCALAR"},
		{SubqueryExists, "EXISTS"},
		{SubqueryIn, "IN"},
		{SubqueryFrom, "FROM"},
		{SubqueryType(99), "UNKNOWN"},
	}
	for _, c := range cases {
		if c.tp.String() != c.want {
			t.Errorf("got %q, want %q", c.tp.String(), c.want)
		}
	}
}

// --- ConvertInToJoin / ConvertExistsToSemiJoin ---

func TestConvertInToJoin_WrongType(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{Type: SubqueryScalar}
	wi := &WhereInfo{Tables: []*TableInfo{}}
	_, err := o.ConvertInToJoin(info, wi)
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestConvertInToJoin_JoinNotBeneficial(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	// Set up so join cost >= in cost
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		EstimatedRows:  NewLogEst(1000000),
		ExecutionCount: NewLogEst(1),
	}
	wi := &WhereInfo{
		NOut:   NewLogEst(10),
		Tables: []*TableInfo{},
	}
	_, err := o.ConvertInToJoin(info, wi)
	// May or may not succeed depending on costs; just verify no panic
	_ = err
}

func TestConvertInToJoin_Success(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		EstimatedRows:  NewLogEst(1),
		ExecutionCount: NewLogEst(1),
	}
	wi := &WhereInfo{
		NOut:     NewLogEst(10000),
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
	}
	_, _ = o.ConvertInToJoin(info, wi)
}

func TestConvertExistsToSemiJoin_WrongType(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{Type: SubqueryIn}
	wi := &WhereInfo{Tables: []*TableInfo{}}
	_, err := o.ConvertExistsToSemiJoin(info, wi)
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestConvertExistsToSemiJoin_Success(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryExists,
		EstimatedRows:  NewLogEst(1),
		ExecutionCount: NewLogEst(1),
	}
	wi := &WhereInfo{
		NOut:     NewLogEst(10000),
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
	}
	_, _ = o.ConvertExistsToSemiJoin(info, wi)
}

// --- BuildJoinTree / JoinAlgorithmSelector.SelectBest ---

func TestBuildJoinTree_SingleTable(t *testing.T) {
	order := &JoinOrder{Tables: []int{0}, Cost: NewLogEst(1000), RowCount: NewLogEst(100)}
	tables := []*TableInfo{{Name: "t", RowLogEst: NewLogEst(100)}}
	node := BuildJoinTree(order, tables, &WhereInfo{Tables: tables})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if !node.IsLeaf {
		t.Error("single table should be leaf")
	}
}

func TestBuildJoinTree_MultiTable(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", RowLogEst: NewLogEst(100)},
		{Name: "t2", RowLogEst: NewLogEst(200)},
	}
	order := &JoinOrder{Tables: []int{0, 1}, Cost: NewLogEst(500), RowCount: NewLogEst(50)}
	node := BuildJoinTree(order, tables, &WhereInfo{Tables: tables})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.IsLeaf {
		t.Error("multi-table should not be leaf")
	}
}

func TestJoinAlgorithmSelector_SelectBest_NoEquiJoin(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(100)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(100)}
	// No equi-join conditions
	jas := NewJoinAlgorithmSelector(outer, inner, []*WhereTerm{}, NewCostModel())
	alg := jas.SelectBest()
	if alg != JoinNestedLoop {
		t.Errorf("expected NestedLoop without equi-join, got %v", alg)
	}
}

func TestJoinAlgorithmSelector_SelectBest_WithEquiJoin(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(100)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(200)}
	conds := []*WhereTerm{
		{Operator: WO_EQ, LeftColumn: 0},
	}
	jas := NewJoinAlgorithmSelector(outer, inner, conds, NewCostModel())
	alg := jas.SelectBest()
	// Should return one of the valid algorithms
	validAlgs := map[JoinAlgorithm]bool{
		JoinNestedLoop: true,
		JoinHash:       true,
		JoinMerge:      true,
	}
	if !validAlgs[alg] {
		t.Errorf("unexpected algorithm: %v", alg)
	}
}

func TestJoinAlgorithmSelector_SelectBest_OuterSmallerThanInner(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(10)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(1000)}
	conds := []*WhereTerm{
		{Operator: WO_EQ, LeftColumn: 0},
	}
	jas := NewJoinAlgorithmSelector(outer, inner, conds, NewCostModel())
	_ = jas.SelectBest()
}

// --- HashJoinPlanner / MergeJoinPlanner Plan() errors ---

func TestHashJoinPlanner_Plan_NoKeys(t *testing.T) {
	left := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	right := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	hj := &HashJoinPlanner{
		Build:     left,
		Probe:     right,
		JoinKeys:  []int{},
		CostModel: NewCostModel(),
	}
	_, err := hj.Plan()
	if err == nil {
		t.Error("expected error for empty join keys")
	}
}

func TestHashJoinPlanner_Plan_WithKeys(t *testing.T) {
	left := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	right := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	hj := &HashJoinPlanner{
		Build:     left,
		Probe:     right,
		JoinKeys:  []int{0},
		CostModel: NewCostModel(),
	}
	plan, err := hj.Plan()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.HashTableSize <= 0 {
		t.Error("expected positive hash table size")
	}
	_ = plan.Execute()
}

func TestMergeJoinPlanner_Plan_NoKeys(t *testing.T) {
	left := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	right := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	mj := &MergeJoinPlanner{
		Left:      left,
		Right:     right,
		JoinKeys:  []int{},
		CostModel: NewCostModel(),
	}
	_, err := mj.Plan()
	if err == nil {
		t.Error("expected error for empty join keys")
	}
}

func TestMergeJoinPlanner_Plan_WithSort(t *testing.T) {
	left := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	right := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	mj := &MergeJoinPlanner{
		Left:        left,
		Right:       right,
		JoinKeys:    []int{0},
		LeftSorted:  false,
		RightSorted: false,
		CostModel:   NewCostModel(),
	}
	plan, err := mj.Plan()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	exec := plan.Execute()
	if exec == "" {
		t.Error("expected non-empty execute string")
	}
}

func TestMergeJoinPlan_Execute_Sorted(t *testing.T) {
	left := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	right := &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)}
	plan := &MergeJoinPlan{
		Left:        left,
		Right:       right,
		JoinKeys:    []int{0},
		LeftSorted:  true,
		RightSorted: true,
	}
	exec := plan.Execute()
	if exec == "" {
		t.Error("expected non-empty execute string")
	}
}

// --- NestedLoopJoinPlan.Execute ---

func TestNestedLoopJoinPlan_Execute(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1}
	plan := &NestedLoopJoinPlan{Outer: outer, Inner: inner}
	exec := plan.Execute()
	if exec == "" {
		t.Error("expected non-empty execute string")
	}
}

// --- JoinNode.String ---

func TestJoinNodeString(t *testing.T) {
	leaf := &JoinNode{IsLeaf: true, TableIndex: 5}
	s := leaf.String()
	if s == "" {
		t.Error("expected non-empty string")
	}
	inner := &JoinNode{
		IsLeaf:    false,
		Algorithm: JoinHash,
		Left:      &JoinNode{IsLeaf: true, TableIndex: 0},
		Right:     &JoinNode{IsLeaf: true, TableIndex: 1},
	}
	s2 := inner.String()
	if s2 == "" {
		t.Error("expected non-empty string")
	}
}

// --- PlanQueryWithSubqueries paths ---

func TestPlanQueryWithSubqueries_NoSubqueries(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
	}
	result, err := p.PlanQueryWithSubqueries(tables, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Error("expected non-nil WhereInfo")
	}
}

// --- explainLoopDetailed via ExplainInfo ---

func TestExplainLoopDetailed_FullScan(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{Name: "users", Cursor: 0, RowLogEst: NewLogEst(1000), RowCount: 1000, Indexes: []*IndexInfo{}},
	}
	loop := &WhereLoop{TabIndex: 0, Index: nil, NOut: NewLogEst(100), Run: NewLogEst(1000)}
	info := &WhereInfo{
		Tables:   tables,
		BestPath: &WherePath{Loops: []*WhereLoop{loop}},
	}
	// Test single loop (isLast=true)
	detail := p.explainLoopDetailed(info, info.BestPath.Loops[0], 0, 1)
	if detail == "" {
		t.Error("expected non-empty detail")
	}

	// Test non-last loop
	detail2 := p.explainLoopDetailed(info, info.BestPath.Loops[0], 0, 2)
	if detail2 == "" {
		t.Error("expected non-empty detail for non-last")
	}
}

func TestExplainLoopDetailed_IndexScan(t *testing.T) {
	p := NewPlanner()
	idx := &IndexInfo{
		Name:    "idx_age",
		Unique:  false,
		Columns: []IndexColumn{{Index: 0, Name: "age"}},
	}
	tables := []*TableInfo{
		{Name: "users", Cursor: 0, RowLogEst: NewLogEst(1000), RowCount: 1000, Indexes: []*IndexInfo{idx}},
	}
	loop := &WhereLoop{TabIndex: 0, Index: idx, NOut: NewLogEst(10), Run: NewLogEst(50), Terms: []*WhereTerm{}}
	info := &WhereInfo{
		Tables:   tables,
		BestPath: &WherePath{Loops: []*WhereLoop{loop}},
	}
	detail := p.explainLoopDetailed(info, info.BestPath.Loops[0], 0, 1)
	if detail == "" {
		t.Error("expected non-empty detail")
	}

	// Unique index
	idxUniq := &IndexInfo{
		Name:    "idx_id",
		Unique:  true,
		Columns: []IndexColumn{{Index: 0, Name: "id"}},
	}
	loop2 := &WhereLoop{TabIndex: 0, Index: idxUniq, NOut: NewLogEst(1), Run: NewLogEst(10), Terms: []*WhereTerm{}}
	info2 := &WhereInfo{
		Tables:   tables,
		BestPath: &WherePath{Loops: []*WhereLoop{loop2}},
	}
	detail2 := p.explainLoopDetailed(info2, info2.BestPath.Loops[0], 0, 1)
	if detail2 == "" {
		t.Error("expected non-empty detail for unique index")
	}
}

// --- extractRightValue ---

func TestExtractRightValue_ValueExpr(t *testing.T) {
	expr := &ValueExpr{Value: int64(42)}
	result := extractRightValue(expr)
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestExtractRightValue_NonValueExpr(t *testing.T) {
	expr := &ColumnExpr{Column: "id"}
	result := extractRightValue(expr)
	if result != nil {
		t.Errorf("expected nil for non-ValueExpr, got %v", result)
	}
}

// --- determineCombinedSortOrder ---

func TestDetermineCombinedSortOrder_Empty(t *testing.T) {
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{},
		CostModel: NewCostModel(),
	}
	order := jo.determineCombinedSortOrder(&JoinOrder{}, &JoinOrder{}, JoinNestedLoop)
	_ = order // just test no panic
}

func TestDetermineCombinedSortOrder_Hash(t *testing.T) {
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{},
		CostModel: NewCostModel(),
	}
	order := jo.determineCombinedSortOrder(&JoinOrder{}, &JoinOrder{}, JoinHash)
	_ = order
}

// --- combineJoinOrders ---

func TestCombineJoinOrders(t *testing.T) {
	jo := &JoinOptimizer{
		Tables: []*TableInfo{
			{Name: "a", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
			{Name: "b", Cursor: 1, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
		},
		CostModel: NewCostModel(),
	}
	outer := &JoinOrder{Tables: []int{0}, Cost: NewLogEst(100), RowCount: NewLogEst(100)}
	inner := &JoinOrder{Tables: []int{1}, Cost: NewLogEst(100), RowCount: NewLogEst(100)}
	combined := jo.combineJoinOrders(outer, inner, JoinNestedLoop, NewLogEst(100), NewLogEst(100))
	if combined == nil {
		t.Error("expected non-nil combined order")
	}
}

// --- PlanQueryWithSubqueries with FROM subqueries ---

func TestPlanQueryWithSubqueries_WithFromSubquery(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
	}
	// FROM subquery as ValueExpr (simple expression)
	fromSubquery := &ValueExpr{Value: "SELECT * FROM x"}
	result, err := p.PlanQueryWithSubqueries(tables, []Expr{fromSubquery}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- optimizeFromSubquery branches ---

func TestOptimizeFromSubquery_CanFlatten(t *testing.T) {
	p := NewPlanner()
	// SubqueryExpr of type FROM - AnalyzeSubquery will set CanFlatten=true for FROM subqueries
	subExpr := &SubqueryExpr{Type: SubqueryFrom, Query: &ValueExpr{Value: "q"}}
	result, err := p.optimizeFromSubquery(subExpr, []*TableInfo{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- SubqueryOptimizer.OptimizeSubquery branches ---

func TestOptimizeSubquery_FlattenBranch(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:       SubqueryFrom,
		CanFlatten: true,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
	}
	result, err := o.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestOptimizeSubquery_MaterializeBranch(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		CanMaterialize: true,
		IsCorrelated:   false,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{},
		AllLoops: []*WhereLoop{},
	}
	result, err := o.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- estimateSelectivity ---

func TestEstimateSelectivity_AllOps(t *testing.T) {
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{},
		CostModel: NewCostModel(),
	}

	// estimateSelectivity takes []*WhereTerm (slice), not a single term
	terms := []*WhereTerm{
		{Operator: WO_EQ},
	}
	sel := jo.estimateSelectivity(terms)
	_ = sel

	// Range terms
	rangeTerms := []*WhereTerm{
		{Operator: WO_LT},
	}
	sel2 := jo.estimateSelectivity(rangeTerms)
	_ = sel2

	// Empty
	sel3 := jo.estimateSelectivity([]*WhereTerm{})
	_ = sel3
}

// --- Statistics.loadTableStats branches ---

func TestStatistics_LoadTableStats_Valid(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadTableStats("users", "500")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStatistics_LoadTableStats_EmptyString(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadTableStats("users", "")
	if err == nil {
		t.Error("expected error for empty stat string")
	}
}

func TestStatistics_LoadTableStats_InvalidInt(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadTableStats("users", "not-a-number")
	if err == nil {
		t.Error("expected error for invalid row count")
	}
}

// --- getTableRowCount with actual table ---

func TestCostEstimator_GetTableRowCount_WithStats(t *testing.T) {
	sch := schema.NewSchema()
	sch.Tables["users"] = &schema.Table{
		Name:    "users",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
		Stats:   &schema.TableStats{RowCount: 500},
	}
	ce := NewCostEstimatorWithSchema(sch)
	rows := ce.getTableRowCount("users")
	if rows <= 0 {
		t.Error("expected positive row count for existing table")
	}
}

// --- tryMaterialize branches ---

func TestTryMaterialize_Correlated(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:         SubqueryScalar,
		IsCorrelated: true,
	}
	parentInfo := &WhereInfo{}
	result, ok := o.tryMaterialize(info, parentInfo)
	_ = ok
	_ = result
}

func TestTryMaterialize_Uncorrelated(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		IsCorrelated:  false,
		EstimatedRows: NewLogEst(100),
	}
	parentInfo := &WhereInfo{}
	result, ok := o.tryMaterialize(info, parentInfo)
	_ = ok
	_ = result
}

// --- JoinAlgorithmSelector.SelectBest ---

func TestJoinAlgorithmSelector_SelectBest_NoLoops(t *testing.T) {
	jas := &JoinAlgorithmSelector{
		CostModel: NewCostModel(),
	}
	alg := jas.SelectBest()
	_ = alg
}

// --- optimizeWhereSubqueries additional coverage ---

func TestOptimizeWhereSubqueries_WithSubqueryTerm(t *testing.T) {
	p := NewPlanner()
	subExpr := &SubqueryExpr{
		Type:  SubqueryIn,
		Query: &ValueExpr{Value: "subquery"},
	}
	term := &WhereTerm{Expr: subExpr}
	info := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{term}},
		Tables: []*TableInfo{
			{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
		},
	}
	result, err := p.optimizeWhereSubqueries(info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestOptimizeWhereSubqueries_BinaryINTerm(t *testing.T) {
	p := NewPlanner()
	binExpr := &BinaryExpr{
		Op:    "IN",
		Left:  &ColumnExpr{Column: "id"},
		Right: &ValueExpr{Value: "list"},
	}
	term := &WhereTerm{Expr: binExpr}
	info := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{term}},
		Tables: []*TableInfo{},
	}
	result, err := p.optimizeWhereSubqueries(info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestOptimizeWhereSubqueries_NilTermExpr(t *testing.T) {
	p := NewPlanner()
	term := &WhereTerm{Expr: nil}
	info := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{term}},
		Tables: []*TableInfo{},
	}
	result, err := p.optimizeWhereSubqueries(info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- optimizeFromSubquery additional paths ---

func TestOptimizeFromSubquery_CanMaterialize(t *testing.T) {
	p := NewPlanner()
	// SubqueryExpr of type scalar - will set CanMaterialize = true
	subExpr := &SubqueryExpr{
		Type:  SubqueryScalar,
		Query: &ValueExpr{Value: "q"},
	}
	result, err := p.optimizeFromSubquery(subExpr, []*TableInfo{}, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

func TestOptimizeFromSubquery_NoOptimization(t *testing.T) {
	p := NewPlanner()
	// A nil expr - AnalyzeSubquery will still work but won't be flatten/materialize
	result, err := p.optimizeFromSubquery(nil, []*TableInfo{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- determineCombinedSortOrder with outer sort ---

func TestDetermineCombinedSortOrder_WithOuterSort(t *testing.T) {
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{},
		CostModel: NewCostModel(),
	}
	outerOrder := &JoinOrder{
		Tables:    []int{0},
		SortOrder: []SortColumn{{Column: "id", Ascending: true}},
	}
	innerOrder := &JoinOrder{Tables: []int{1}}
	order := jo.determineCombinedSortOrder(outerOrder, innerOrder, JoinNestedLoop)
	_ = order
}

// --- extractBestPlan ---

func TestExtractBestPlan_Empty(t *testing.T) {
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{},
		CostModel: NewCostModel(),
	}
	plans := make(map[uint64]*JoinOrder)
	result, err := jo.extractBestPlan(2, plans)
	if err == nil && result != nil {
		// empty plans may return nil without error
	}
	_ = err
}

func TestExtractBestPlan_WithPlans(t *testing.T) {
	jo := &JoinOptimizer{
		Tables: []*TableInfo{
			{Name: "a", Cursor: 0},
			{Name: "b", Cursor: 1},
		},
		CostModel: NewCostModel(),
	}
	plans := map[uint64]*JoinOrder{
		3: {Tables: []int{0, 1}, Cost: NewLogEst(100), RowCount: NewLogEst(100)},
	}
	result, err := jo.extractBestPlan(2, plans)
	_ = err
	_ = result
}

// --- estimateSingleTableCost ---

func TestEstimateSingleTableCost_WithWhereInfo(t *testing.T) {
	loop := &WhereLoop{TabIndex: 0, NOut: NewLogEst(100), Run: NewLogEst(1000)}
	table := &TableInfo{Name: "t", Cursor: 0, RowLogEst: NewLogEst(1000), Indexes: []*IndexInfo{}}
	whereInfo := &WhereInfo{
		AllLoops: []*WhereLoop{loop},
		Tables:   []*TableInfo{table},
	}
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{table},
		CostModel: NewCostModel(),
		WhereInfo: whereInfo,
	}
	cost := jo.estimateSingleTableCost(0)
	_ = cost
}

func TestEstimateSingleTableCost_NoWhereInfo(t *testing.T) {
	table := &TableInfo{Name: "t", Cursor: 0, RowLogEst: NewLogEst(1000), Indexes: []*IndexInfo{}}
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{table},
		CostModel: NewCostModel(),
	}
	cost := jo.estimateSingleTableCost(0)
	_ = cost
}

// --- createMaterializedSubqueryTable2 ---

func TestCreateMaterializedSubqueryTable2(t *testing.T) {
	p := NewPlanner()
	info := &SubqueryInfo{
		Type:              SubqueryFrom,
		CanMaterialize:    true,
		MaterializedTable: "_temp_1",
		EstimatedRows:     NewLogEst(100),
	}
	table, err := p.createMaterializedSubqueryTable(info, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = table
}

// --- PlanQueryWithSubqueries with subquery error ---

func TestPlanQueryWithSubqueries_MultipleSubqueries(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
	}
	subquery1 := &SubqueryExpr{Type: SubqueryFrom, Query: &ValueExpr{Value: "s1"}}
	subquery2 := &SubqueryExpr{Type: SubqueryScalar, Query: &ValueExpr{Value: "s2"}}
	result, err := p.PlanQueryWithSubqueries(tables, []Expr{subquery1, subquery2}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- GreedyJoinOrder branches ---

func TestGreedyJoinOrder2_SingleTable(t *testing.T) {
	table := &TableInfo{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}}
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{table},
		CostModel: NewCostModel(),
	}
	result, err := jo.GreedyJoinOrder()
	_ = err
	if result == nil {
		t.Error("expected non-nil result for single table")
	}
}

func TestGreedyJoinOrder2_MultipleTables(t *testing.T) {
	tables := []*TableInfo{
		{Name: "a", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
		{Name: "b", Cursor: 1, RowLogEst: NewLogEst(200), Indexes: []*IndexInfo{}},
		{Name: "c", Cursor: 2, RowLogEst: NewLogEst(50), Indexes: []*IndexInfo{}},
	}
	jo := &JoinOptimizer{
		Tables:    tables,
		CostModel: NewCostModel(),
	}
	result, err := jo.GreedyJoinOrder()
	_ = err
	_ = result
}

// --- OptimizeJoinOrder ---

func TestOptimizeJoinOrder2_SingleTable(t *testing.T) {
	table := &TableInfo{Name: "t", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}}
	jo := &JoinOptimizer{
		Tables:    []*TableInfo{table},
		CostModel: NewCostModel(),
	}
	result, err := jo.OptimizeJoinOrder()
	_ = err
	if result == nil {
		t.Error("expected non-nil result for single table")
	}
}

func TestOptimizeJoinOrder2_FewTables(t *testing.T) {
	tables := []*TableInfo{
		{Name: "a", Cursor: 0, RowLogEst: NewLogEst(100), Indexes: []*IndexInfo{}},
		{Name: "b", Cursor: 1, RowLogEst: NewLogEst(200), Indexes: []*IndexInfo{}},
	}
	jo := &JoinOptimizer{
		Tables:    tables,
		CostModel: NewCostModel(),
	}
	result, err := jo.OptimizeJoinOrder()
	_ = err
	_ = result
}

// --- optimizeFromSubquery: CanMaterialize branch ---

func TestOptimizeFromSubquery_CanMaterializeCorrelated(t *testing.T) {
	p := NewPlanner()
	// ColumnExpr with cursor=1 has UsedTables() = {1} -> non-zero -> IsCorrelated=true
	// type is SubqueryIn -> CanFlatten=false
	// With IsCorrelated and appropriate EstimatedRows, CanMaterialize might be true
	outerColExpr := &ColumnExpr{Column: "user_id", Cursor: 1}
	subExpr := &SubqueryExpr{
		Type:        SubqueryIn,
		Query:       outerColExpr,
		OuterColumn: outerColExpr,
	}
	outerTable := &TableInfo{Name: "outer_t", Cursor: 1, RowLogEst: NewLogEst(1000), Indexes: []*IndexInfo{}}
	result, err := p.optimizeFromSubquery(subExpr, []*TableInfo{outerTable}, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = result
}

// --- ConvertInToJoin success path ---

func TestConvertInToJoin2_LargeSubquery(t *testing.T) {
	o := NewSubqueryOptimizer(NewCostModel())
	// Create a large IN subquery where JOIN should be beneficial
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(1), // Very small = low IN cost?
	}
	// Create parent with a very large loop so JOIN seems cheap
	loop := &WhereLoop{TabIndex: 0, NOut: NewLogEst(1), Run: NewLogEst(1)}
	table := &TableInfo{Name: "t", Cursor: 0, RowLogEst: NewLogEst(1), Indexes: []*IndexInfo{}}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{table},
		AllLoops: []*WhereLoop{loop},
		Clause:   &WhereClause{Terms: []*WhereTerm{}},
	}
	_, err := o.ConvertInToJoin(info, parentInfo)
	_ = err // accept either outcome
}

// --- materializeDependencies with self-dep ---

func TestMaterializeDependencies_SelfDep(t *testing.T) {
	ctx := &CTEContext{
		CTEs:             map[string]*CTEDefinition{},
		MaterializedCTEs: map[string]*MaterializedCTE{},
		CTEOrder:         []string{},
	}
	ctx.CTEs["mycte"] = &CTEDefinition{
		Name:        "mycte",
		IsRecursive: false,
	}
	err := ctx.materializeDependencies("mycte", []string{"mycte"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMaterializeDependencies_WithDep(t *testing.T) {
	ctx := &CTEContext{
		CTEs:             map[string]*CTEDefinition{},
		MaterializedCTEs: map[string]*MaterializedCTE{},
		CTEOrder:         []string{},
	}
	ctx.CTEs["dep"] = &CTEDefinition{
		Name:          "dep",
		IsRecursive:   false,
		EstimatedRows: NewLogEst(100),
	}
	ctx.CTEs["main"] = &CTEDefinition{
		Name:          "main",
		IsRecursive:   false,
		EstimatedRows: NewLogEst(100),
	}
	err := ctx.materializeDependencies("main", []string{"dep"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- statistics branches ---

func TestStatistics_LoadIndexStats(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadIndexStats("users", "idx_age", "1000 200 50")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStatistics_LoadIndexStats_Empty(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadIndexStats("users", "idx_age", "")
	if err == nil {
		t.Error("expected error for empty stat string")
	}
}

func TestStatistics_LoadIndexStats_InvalidInt(t *testing.T) {
	stats := NewStatistics()
	err := stats.loadIndexStats("users", "idx_age", "not-a-number")
	if err == nil {
		t.Error("expected error for invalid row count")
	}
}
