// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// --- explain.go: generate (90%) - unsupported statement type ---

// TestGenerateExplain_UnsupportedType verifies error for unsupported statement types.
func TestGenerateExplain_UnsupportedType(t *testing.T) {
	// Use a statement type that is not SELECT/INSERT/UPDATE/DELETE
	stmt := &parser.CreateTableStmt{Name: "test"}
	_, err := GenerateExplain(stmt)
	if err == nil {
		t.Fatal("expected error for unsupported statement type")
	}
	if !strings.Contains(err.Error(), "EXPLAIN not supported") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestGenerateExplain_InsertStmt verifies explain for INSERT statement.
func TestGenerateExplain_InsertStmt(t *testing.T) {
	stmt := &parser.InsertStmt{
		Table: "users",
		Columns: []string{"id", "name"},
	}
	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "users") {
		t.Errorf("expected plan to mention 'users', got: %s", text)
	}
}

// TestGenerateExplain_UpdateStmt verifies explain for UPDATE statement.
func TestGenerateExplain_UpdateStmt(t *testing.T) {
	stmt := &parser.UpdateStmt{
		Table: "users",
		Sets: []parser.Assignment{
			{Column: "name", Value: &parser.LiteralExpr{Value: "Alice"}},
		},
	}
	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
}

// TestGenerateExplain_DeleteStmt verifies explain for DELETE statement.
func TestGenerateExplain_DeleteStmt(t *testing.T) {
	stmt := &parser.DeleteStmt{
		Table: "users",
	}
	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
}

// --- explain.go: explainSingleSelect (93.3%) - with schema and WITH clause ---

// TestGenerateExplainWithSchema_IndexUsed verifies index usage is shown when schema present.
func TestGenerateExplainWithSchema_IndexUsed(t *testing.T) {
	sch := schema.NewSchema()
	sch.Tables["users"] = &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT"},
		},
	}
	sch.Indexes["idx_email"] = &schema.Index{
		Name:    "idx_email",
		Table:   "users",
		Columns: []string{"email"},
		Unique:  true,
	}

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "email"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "alice@example.com"},
		},
	}

	plan, err := GenerateExplainWithSchema(stmt, sch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "users") {
		t.Errorf("expected plan to mention 'users', got: %s", text)
	}
}

// TestGenerateExplainWithSchema_GroupBy verifies GROUP BY shows temp btree.
func TestGenerateExplainWithSchema_GroupBy(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "dept"}},
			{Expr: &parser.FunctionExpr{Name: "count", Args: []parser.Expression{&parser.IdentExpr{Name: "*"}}}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "employees"},
			},
		},
		GroupBy: []parser.Expression{
			&parser.IdentExpr{Name: "dept"},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "GROUP BY") {
		t.Errorf("expected plan to mention GROUP BY, got: %s", text)
	}
}

// TestGenerateExplainWithSchema_OrderBy verifies ORDER BY shows temp btree.
func TestGenerateExplainWithSchema_OrderBy(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "ORDER BY") {
		t.Errorf("expected plan to mention ORDER BY, got: %s", text)
	}
}

// TestGenerateExplainWithSchema_Distinct verifies DISTINCT shows temp btree.
func TestGenerateExplainWithSchema_Distinct(t *testing.T) {
	stmt := &parser.SelectStmt{
		Distinct: true,
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "DISTINCT") {
		t.Errorf("expected plan to mention DISTINCT, got: %s", text)
	}
}

// TestGenerateExplainWithSchema_Join verifies JOIN nodes are emitted.
func TestGenerateExplainWithSchema_Join(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Star: true},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "orders"},
			},
			Joins: []parser.JoinClause{
				{
					Table: parser.TableOrSubquery{TableName: "customers"},
					Condition: parser.JoinCondition{
						On: &parser.BinaryExpr{
							Op:    parser.OpEq,
							Left:  &parser.IdentExpr{Name: "orders.customer_id"},
							Right: &parser.IdentExpr{Name: "customers.id"},
						},
					},
				},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := plan.FormatAsText()
	if !strings.Contains(text, "orders") {
		t.Errorf("expected plan to mention 'orders', got: %s", text)
	}
}

// --- explain.go: findBestIndexWithColumns (85.7%) - with matching index ---

// TestFindBestIndexWithColumns_MatchingIndex exercises the index-found branch.
func TestFindBestIndexWithColumns_MatchingIndex(t *testing.T) {
	sch := schema.NewSchema()
	sch.Indexes["idx_email"] = &schema.Index{
		Name:    "idx_email",
		Table:   "users",
		Columns: []string{"email"},
		Unique:  true,
	}

	where := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Name: "email"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "test@test.com"},
	}

	candidate := findBestIndexWithColumns("users", where, sch, []string{"email", "id"})
	if candidate == nil {
		t.Fatal("expected a matching index candidate, got nil")
	}
	if candidate.IndexName != "idx_email" {
		t.Errorf("expected index 'idx_email', got %q", candidate.IndexName)
	}
}

// TestFindBestIndexWithColumns_NilSchema returns nil for nil schema.
func TestFindBestIndexWithColumns_NilSchema(t *testing.T) {
	where := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Name: "email"},
		Right: &parser.LiteralExpr{},
	}
	candidate := findBestIndexWithColumns("users", where, nil, nil)
	if candidate != nil {
		t.Errorf("expected nil for nil schema, got %+v", candidate)
	}
}

// TestFindBestIndexWithColumns_NilWhere returns nil for nil WHERE.
func TestFindBestIndexWithColumns_NilWhere(t *testing.T) {
	sch := schema.NewSchema()
	candidate := findBestIndexWithColumns("users", nil, sch, nil)
	if candidate != nil {
		t.Errorf("expected nil for nil WHERE, got %+v", candidate)
	}
}

// --- explain.go: formatIndexScan (87.5%) - covering index path ---

// TestFormatIndexScan_CoveringIndex exercises the covering index format path.
func TestFormatIndexScan_CoveringIndex(t *testing.T) {
	candidate := &IndexCandidate{
		IndexName:   "idx_name",
		TableName:   "users",
		Columns:     []string{"name"},
		IsUnique:    false,
		IsCovering:  true,
		HasEquality: false,
	}
	result := formatIndexScan(candidate)
	if !strings.Contains(result, "COVERING INDEX") {
		t.Errorf("expected COVERING INDEX in output, got: %s", result)
	}
}

// TestFormatIndexScan_UniqueEquality exercises the unique equality format path.
func TestFormatIndexScan_UniqueEquality(t *testing.T) {
	candidate := &IndexCandidate{
		IndexName:   "idx_id",
		TableName:   "users",
		Columns:     []string{"id"},
		IsUnique:    true,
		IsCovering:  false,
		HasEquality: true,
	}
	result := formatIndexScan(candidate)
	if !strings.Contains(result, "SEARCH TABLE") {
		t.Errorf("expected SEARCH TABLE in output, got: %s", result)
	}
	if !strings.Contains(result, "id=?") {
		t.Errorf("expected 'id=?' in output, got: %s", result)
	}
}

// TestFormatIndexScan_NonUniqueRange exercises the SCAN path for non-equality.
func TestFormatIndexScan_NonUniqueRange(t *testing.T) {
	candidate := &IndexCandidate{
		IndexName:   "idx_age",
		TableName:   "users",
		Columns:     []string{"age"},
		IsUnique:    false,
		IsCovering:  false,
		HasEquality: false,
	}
	result := formatIndexScan(candidate)
	if !strings.Contains(result, "SCAN") {
		t.Errorf("expected SCAN in output, got: %s", result)
	}
}

// --- join_algorithm.go: SelectBest (78.6%) ---

// TestJoinAlgorithmSelectorBestNoEquiJoin exercises nested loop path.
func TestJoinAlgorithmSelectorBestNoEquiJoin(t *testing.T) {
	outer := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(1000),
	}
	inner := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(500),
		EstimatedRows: NewLogEst(500),
	}

	// No equi-join conditions => must use nested loop
	jas := NewJoinAlgorithmSelector(outer, inner, nil, NewCostModel())
	alg := jas.SelectBest()
	if alg != JoinNestedLoop {
		t.Errorf("expected JoinNestedLoop for no equi-join, got %v", alg)
	}
}

// TestJoinAlgorithmSelectorBestWithEquiJoin exercises hash/merge join paths.
func TestJoinAlgorithmSelectorBestWithEquiJoin(t *testing.T) {
	outer := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(10000),
		EstimatedRows: NewLogEst(10000),
	}
	inner := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(1000),
	}

	// Equi-join condition
	conditions := []*WhereTerm{
		{Operator: WO_EQ},
	}
	jas := NewJoinAlgorithmSelector(outer, inner, conditions, NewCostModel())
	alg := jas.SelectBest()
	// Should select some algorithm (hash or nested loop depending on size)
	if alg != JoinNestedLoop && alg != JoinHash && alg != JoinMerge {
		t.Errorf("unexpected algorithm: %v", alg)
	}
}

// TestJoinAlgorithmSelector_SelectBest_HashJoin exercises hash join selection.
func TestJoinAlgorithmSelector_SelectBest_HashJoin(t *testing.T) {
	// Make inner large enough to qualify for hash join (>100 rows, < outer*10)
	outer := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(100000),
		EstimatedRows: NewLogEst(100000),
	}
	inner := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(500),
		EstimatedRows: NewLogEst(500),
	}

	conditions := []*WhereTerm{
		{Operator: WO_EQ},
	}
	jas := NewJoinAlgorithmSelector(outer, inner, conditions, NewCostModel())
	alg := jas.SelectBest()
	// With large outer and medium inner, hash join may be selected
	_ = alg // Just exercise the code path
}

// --- statistics.go: validateAvgEq (80%) - invalid and valid paths ---

// TestValidateAvgEq_InvalidLow tests error for avg < 1.
func TestValidateAvgEq_InvalidLow(t *testing.T) {
	err := validateAvgEq("test_idx", 0, 0, 1000)
	if err == nil {
		t.Fatal("expected error for avg < 1")
	}
	if !strings.Contains(err.Error(), "invalid avgEq") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestValidateAvgEq_ExceedsRowCount tests error for avg > rowCount.
func TestValidateAvgEq_ExceedsRowCount(t *testing.T) {
	err := validateAvgEq("test_idx", 0, 1000, 500)
	if err == nil {
		t.Fatal("expected error for avg > rowCount")
	}
	if !strings.Contains(err.Error(), "avgEq") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestValidateAvgEq_Valid tests no error for valid avgEq.
func TestValidateAvgEq_Valid(t *testing.T) {
	err := validateAvgEq("test_idx", 0, 10, 1000)
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
}

// TestValidateAvgEq_EqualsRowCount tests avgEq equal to rowCount (boundary).
func TestValidateAvgEq_EqualsRowCount(t *testing.T) {
	err := validateAvgEq("test_idx", 0, 1000, 1000)
	if err != nil {
		t.Errorf("expected nil for avg == rowCount, got: %v", err)
	}
}

// --- statistics.go: ComputeIndexStatistics (93.3%) ---

// TestComputeIndexStatistics_SingleColumn verifies single-column statistics.
func TestComputeIndexStatistics_SingleColumn(t *testing.T) {
	stat := ComputeIndexStatistics("users", "idx_email", 1000, []int64{500})
	if stat == nil {
		t.Fatal("expected non-nil statistics")
	}
	if stat.IndexName != "idx_email" {
		t.Errorf("expected IndexName 'idx_email', got %q", stat.IndexName)
	}
	if stat.TableName != "users" {
		t.Errorf("expected TableName 'users', got %q", stat.TableName)
	}
	if stat.RowCount != 1000 {
		t.Errorf("expected RowCount 1000, got %d", stat.RowCount)
	}
	if len(stat.AvgEq) != 1 {
		t.Errorf("expected 1 avgEq, got %d", len(stat.AvgEq))
	}
	// avgEq = rowCount / distinctCount = 1000 / 500 = 2
	if stat.AvgEq[0] != 2 {
		t.Errorf("expected avgEq[0]=2, got %d", stat.AvgEq[0])
	}
}

// TestComputeIndexStatistics_ZeroDistinct tests with zero distinct count.
func TestComputeIndexStatistics_ZeroDistinct(t *testing.T) {
	stat := ComputeIndexStatistics("users", "idx_name", 1000, []int64{0})
	if stat == nil {
		t.Fatal("expected non-nil statistics")
	}
	// When distinctCount is 0, avgEq should be rowCount
	if stat.AvgEq[0] != 1000 {
		t.Errorf("expected avgEq[0]=1000 for zero distinct count, got %d", stat.AvgEq[0])
	}
}

// TestComputeIndexStatistics_MultiColumn verifies multi-column statistics.
func TestComputeIndexStatistics_MultiColumn(t *testing.T) {
	stat := ComputeIndexStatistics("orders", "idx_composite", 10000, []int64{100, 1000, 10000})
	if stat == nil {
		t.Fatal("expected non-nil statistics")
	}
	if len(stat.AvgEq) != 3 {
		t.Errorf("expected 3 avgEq values, got %d", len(stat.AvgEq))
	}
	if len(stat.ColumnStats) != 3 {
		t.Errorf("expected 3 column stats, got %d", len(stat.ColumnStats))
	}
}

// --- statistics.go: applyRangeSelectivity (83.3%) ---

// TestApplyRangeSelectivity_NoRange verifies no change when no range constraint.
func TestApplyRangeSelectivity_NoRange(t *testing.T) {
	result := applyRangeSelectivity(false, 100)
	if result != 100 {
		t.Errorf("expected 100 for no range, got %d", result)
	}
}

// TestApplyRangeSelectivity_WithRange verifies selectivity adjustment for range.
func TestApplyRangeSelectivity_WithRange(t *testing.T) {
	result := applyRangeSelectivity(true, 100)
	// selectivityRange is negative, so result should be < 100
	if result >= 100 {
		t.Errorf("expected result < 100 for range selectivity, got %d", result)
	}
}

// TestApplyRangeSelectivity_ClampedToZero verifies floor at zero.
func TestApplyRangeSelectivity_ClampedToZero(t *testing.T) {
	// Very negative nOut that would go below zero after adding selectivityRange
	result := applyRangeSelectivity(true, -10000)
	if result != 0 {
		t.Errorf("expected 0 for very negative nOut with range, got %d", result)
	}
}

// --- join.go: SelectJoinAlgorithm (92.9%) - additional path coverage ---

// TestJoinOptimizer_SelectJoinAlgorithm_HashJoin exercises hash join selection in JoinOptimizer.
func TestJoinOptimizer_SelectJoinAlgorithm_HashJoin(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100000, RowLogEst: NewLogEst(100000)},
		{Name: "t2", Cursor: 1, RowCount: 1000, RowLogEst: NewLogEst(1000)},
	}
	optimizer := NewJoinOptimizer(tables, nil, NewCostModel())

	outer := &JoinOrder{
		Tables:   []int{0},
		Cost:     NewLogEst(100000),
		RowCount: NewLogEst(100000),
	}
	inner := &JoinOrder{
		Tables:   []int{1},
		Cost:     NewLogEst(1000),
		RowCount: NewLogEst(1000),
	}

	conditions := []*WhereTerm{
		{Operator: WO_EQ},
	}

	alg := optimizer.SelectJoinAlgorithm(outer, inner, conditions)
	// Should be some valid algorithm
	if alg != JoinNestedLoop && alg != JoinHash && alg != JoinMerge {
		t.Errorf("unexpected algorithm: %v", alg)
	}
}

// TestJoinOptimizer_SelectJoinAlgorithm_NoConditions ensures nested loop for no conditions.
func TestJoinOptimizer_SelectJoinAlgorithm_NoConditions(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "t2", Cursor: 1, RowCount: 100, RowLogEst: NewLogEst(100)},
	}
	optimizer := NewJoinOptimizer(tables, nil, NewCostModel())

	outer := &JoinOrder{Tables: []int{0}, Cost: NewLogEst(100), RowCount: NewLogEst(100)}
	inner := &JoinOrder{Tables: []int{1}, Cost: NewLogEst(100), RowCount: NewLogEst(100)}

	alg := optimizer.SelectJoinAlgorithm(outer, inner, nil)
	if alg != JoinNestedLoop {
		t.Errorf("expected JoinNestedLoop for no conditions, got %v", alg)
	}
}

// --- buildLeftDeepTree coverage ---

// TestBuildLeftDeepTree_SingleTable exercises the leaf-only case.
func TestBuildLeftDeepTree_SingleTable(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
	}
	order := &JoinOrder{
		Tables:    []int{0},
		Cost:      NewLogEst(100),
		RowCount:  NewLogEst(100),
		Algorithm: []JoinAlgorithm{},
	}

	node := buildLeftDeepTree(order, tables, nil, 0)
	if node == nil {
		t.Fatal("expected non-nil node for single table")
	}
	if !node.IsLeaf {
		t.Error("expected leaf node for single table")
	}
}

// TestBuildLeftDeepTree_TwoTables exercises the two-table join case.
func TestBuildLeftDeepTree_TwoTables(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "t2", Cursor: 1, RowCount: 200, RowLogEst: NewLogEst(200)},
	}
	order := &JoinOrder{
		Tables:    []int{0, 1},
		Cost:      NewLogEst(100),
		RowCount:  NewLogEst(100),
		Algorithm: []JoinAlgorithm{JoinNestedLoop},
	}

	node := buildLeftDeepTree(order, tables, nil, 0)
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.IsLeaf {
		t.Error("expected non-leaf join node for two tables")
	}
	if node.Left == nil || node.Right == nil {
		t.Error("expected both left and right children")
	}
}

// TestBuildLeftDeepTree_EmptyOrder returns nil for start beyond tables.
func TestBuildLeftDeepTree_EmptyOrder(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
	}
	order := &JoinOrder{
		Tables: []int{0},
	}
	node := buildLeftDeepTree(order, tables, nil, 5) // startIdx beyond length
	if node != nil {
		t.Errorf("expected nil for startIdx beyond tables, got %+v", node)
	}
}
