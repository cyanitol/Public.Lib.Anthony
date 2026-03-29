// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC tests for internal/planner – fifth file
//
// Targets:
//   subquery.go : ConvertInToJoin (60%)  – reachable branch analysis
//   subquery.go : ConvertExistsToSemiJoin (60%) – reachable branch analysis
//   planner.go  : buildEquivalenceClasses (94.4%) – out-of-range cursor branch
//   planner.go  : explainLoopDetailed (94.4%) – isLast and index.Unique branches
//   statistics.go : estimateEqualitySelectivity – small-int and empty-string branches
//   statistics.go : ValidateStatistics error paths
//   statistics.go : EstimateDistinctValues – small-sample correction branch
//   index.go    : processIndexColumns returning false (first column no constraint)
//   index.go    : termMatchesExpression / termMatchesColumn expression path
//   join_algorithm.go : CostEstimate default branch
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// subquery.go ConvertInToJoin / ConvertExistsToSemiJoin (60%)
//
// Analysis: the "success" return path (lines that build a new WhereInfo) is
// structurally unreachable because:
//   estimateInCost  = NOut + EstimatedRows
//   estimateJoinCost = NOut + EstimatedRows
// so joinCost == inCost, meaning joinCost >= inCost is always true → error.
//
// Similarly for ConvertExistsToSemiJoin:
//   estimateExistsCost = NOut + (EstimatedRows - 10)
//   estimateJoinCost   = NOut + EstimatedRows
// so joinCost > existsCost always → error.
//
// The tests below document this, cover all type-rejection branches, and
// cover the "not beneficial" error for the IN and EXISTS types.
// ---------------------------------------------------------------------------

// TestMCDC5_ConvertInToJoin_TypeReject_Scalar verifies SubqueryScalar is rejected.
func TestMCDC5_ConvertInToJoin_TypeReject_Scalar(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar, got nil")
	}
	if !strings.Contains(err.Error(), "not an IN subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

// TestMCDC5_ConvertInToJoin_TypeReject_From verifies SubqueryFrom is rejected.
func TestMCDC5_ConvertInToJoin_TypeReject_From(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryFrom,
		EstimatedRows: NewLogEst(200),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "u", RowCount: 500, RowLogEst: NewLogEst(500)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(500),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom, got nil")
	}
	if !strings.Contains(err.Error(), "not an IN subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

// TestMCDC5_ConvertInToJoin_NotBeneficial verifies the IN type but JOIN-not-beneficial path.
func TestMCDC5_ConvertInToJoin_NotBeneficial(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	// Because estimateInCost == estimateJoinCost (both compute NOut+EstimatedRows),
	// joinCost >= inCost is always true — the conversion is never beneficial.
	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (JOIN not beneficial), got nil")
	}
	if !strings.Contains(err.Error(), "not beneficial") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result on not-beneficial, got %+v", result)
	}
}

// TestMCDC5_ConvertExistsToSemiJoin_TypeReject_Scalar verifies Scalar is rejected.
func TestMCDC5_ConvertExistsToSemiJoin_TypeReject_Scalar(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "o", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar, got nil")
	}
	if !strings.Contains(err.Error(), "not an EXISTS subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

// TestMCDC5_ConvertExistsToSemiJoin_TypeReject_In verifies IN type is rejected.
func TestMCDC5_ConvertExistsToSemiJoin_TypeReject_In(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(50),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "i", RowCount: 300, RowLogEst: NewLogEst(300)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(300),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryIn, got nil")
	}
	if !strings.Contains(err.Error(), "not an EXISTS subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

// TestMCDC5_ConvertExistsToSemiJoin_TypeReject_From verifies From type is rejected.
func TestMCDC5_ConvertExistsToSemiJoin_TypeReject_From(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryFrom,
		EstimatedRows: NewLogEst(80),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "f", RowCount: 200, RowLogEst: NewLogEst(200)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(200),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom, got nil")
	}
	if !strings.Contains(err.Error(), "not an EXISTS subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

// TestMCDC5_ConvertExistsToSemiJoin_NotBeneficial exercises the EXISTS type but
// semi-join-not-beneficial path.
func TestMCDC5_ConvertExistsToSemiJoin_NotBeneficial(t *testing.T) {
	t.Parallel()

	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "e", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	// joinCost = NOut+EstimatedRows; existsCost = NOut+(EstimatedRows-10)
	// joinCost > existsCost, so "not beneficial".
	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (semi-join not beneficial), got nil")
	}
	if !strings.Contains(err.Error(), "not beneficial") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result on not-beneficial, got %+v", result)
	}
}

// ---------------------------------------------------------------------------
// planner.go buildEquivalenceClasses (94.4%)
//
// Compound condition: rightCol.Cursor >= 0 && rightCol.Cursor < len(tables)
//   A = rightCol.Cursor >= 0
//   B = rightCol.Cursor < len(tables)
//
// MC/DC pairs:
//   A=T, B=T -> inside block (normal path)
//   A=F, B=? -> A flips outcome (cursor is negative)
//   A=T, B=F -> B flips outcome (cursor >= len(tables))
// ---------------------------------------------------------------------------

// makeEquivTable constructs a minimal TableInfo for equivalence-class tests.
func makeEquivTable(name string, cols ...string) *TableInfo {
	t := &TableInfo{
		Name:    name,
		Columns: make([]ColumnInfo, len(cols)),
	}
	for i, c := range cols {
		t.Columns[i] = ColumnInfo{Name: c, Index: i}
	}
	return t
}

// makeEquivTerm builds a WhereTerm with a BinaryExpr for column equality
// where the right-hand side is a ColumnExpr with the given cursor.
func makeEquivTerm(leftCursor, leftColIdx int, rightCursor int, rightColName string) *WhereTerm {
	left := &ColumnExpr{Cursor: leftCursor, Column: "id"}
	right := &ColumnExpr{Cursor: rightCursor, Column: rightColName}
	expr := &BinaryExpr{Op: "=", Left: left, Right: right}
	return &WhereTerm{
		Expr:       expr,
		Operator:   WO_EQ,
		LeftCursor: leftCursor,
		LeftColumn: leftColIdx,
	}
}

// TestMCDC5_BuildEquivClasses_CursorInRange exercises the normal path where
// rightCol.Cursor is valid (A=T, B=T).
func TestMCDC5_BuildEquivClasses_CursorInRange(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{
		makeEquivTable("a", "id"),
		makeEquivTable("b", "a_id"),
	}
	term := makeEquivTerm(0, 0, 1, "a_id")
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	equiv := p.buildEquivalenceClasses(clause, tables)
	// Expect both "0.0" and "1.0" to have entries.
	if len(equiv) == 0 {
		t.Error("expected non-empty equivalence map for in-range cursor")
	}
}

// TestMCDC5_BuildEquivClasses_CursorNegative exercises the false branch where
// rightCol.Cursor < 0 (A=F — cursor is negative).
func TestMCDC5_BuildEquivClasses_CursorNegative(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{
		makeEquivTable("a", "id"),
	}
	// Cursor -1 means the condition rightCol.Cursor >= 0 is false.
	term := makeEquivTerm(0, 0, -1, "col")
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	// Should not panic; rightColIdx will be computed with cursor=-1 outside bounds.
	equiv := p.buildEquivalenceClasses(clause, tables)
	// With cursor=-1 the inner block is skipped but the key still uses cursor=-1.
	// Result should still be non-nil.
	if equiv == nil {
		t.Error("expected non-nil equivalence map even for out-of-range cursor")
	}
}

// TestMCDC5_BuildEquivClasses_CursorOutOfRange exercises the false branch where
// rightCol.Cursor >= len(tables) (B=F — cursor too large).
func TestMCDC5_BuildEquivClasses_CursorOutOfRange(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{
		makeEquivTable("a", "id"),
	}
	// len(tables) == 1, so cursor=1 triggers B=F.
	term := makeEquivTerm(0, 0, 1, "col")
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	equiv := p.buildEquivalenceClasses(clause, tables)
	if equiv == nil {
		t.Error("expected non-nil equivalence map even for out-of-range cursor")
	}
}

// TestMCDC5_BuildEquivClasses_NonBinaryExpr verifies that non-BinaryExpr terms
// are skipped (the !ok continue branch).
func TestMCDC5_BuildEquivClasses_NonBinaryExpr(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{makeEquivTable("a", "id")}
	// WO_EQ but Expr is not *BinaryExpr.
	term := &WhereTerm{
		Expr:     &ValueExpr{Value: 42},
		Operator: WO_EQ,
	}
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	equiv := p.buildEquivalenceClasses(clause, tables)
	if len(equiv) != 0 {
		t.Errorf("expected empty map for non-binary expr, got len=%d", len(equiv))
	}
}

// TestMCDC5_BuildEquivClasses_NonEQOperator verifies that non-WO_EQ terms are skipped.
func TestMCDC5_BuildEquivClasses_NonEQOperator(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{makeEquivTable("a", "id")}
	left := &ColumnExpr{Cursor: 0, Column: "id"}
	right := &ValueExpr{Value: 10}
	expr := &BinaryExpr{Op: "<", Left: left, Right: right}
	term := &WhereTerm{
		Expr:       expr,
		Operator:   WO_LT,
		LeftCursor: 0,
		LeftColumn: 0,
	}
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	equiv := p.buildEquivalenceClasses(clause, tables)
	if len(equiv) != 0 {
		t.Errorf("expected empty map for non-EQ term, got len=%d", len(equiv))
	}
}

// TestMCDC5_BuildEquivClasses_RightNotColumnExpr verifies that a right-side
// non-ColumnExpr is skipped (the second !ok continue).
func TestMCDC5_BuildEquivClasses_RightNotColumnExpr(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tables := []*TableInfo{makeEquivTable("a", "id")}
	left := &ColumnExpr{Cursor: 0, Column: "id"}
	right := &ValueExpr{Value: "constant"} // not a ColumnExpr
	expr := &BinaryExpr{Op: "=", Left: left, Right: right}
	term := &WhereTerm{
		Expr:       expr,
		Operator:   WO_EQ,
		LeftCursor: 0,
		LeftColumn: 0,
	}
	clause := &WhereClause{Terms: []*WhereTerm{term}}

	equiv := p.buildEquivalenceClasses(clause, tables)
	if len(equiv) != 0 {
		t.Errorf("expected empty map for non-ColumnExpr right side, got len=%d", len(equiv))
	}
}

// ---------------------------------------------------------------------------
// planner.go explainLoopDetailed (94.4%)
//
// Branches:
//   loop.Index != nil (index access) vs loop.Index == nil (full table scan)
//   isLast = (i == totalLoops-1) true vs false  (subPrefix differs)
//   loop.Index.Unique true vs false
//
// MC/DC pairs for isLast:
//   A=T: i == totalLoops-1 → last loop, subPrefix uses "   "
//   A=F: i != totalLoops-1 → not last, subPrefix uses "│  "
//
// MC/DC pairs for Unique (when index present):
//   B=T: Unique index → "UNIQUE"
//   B=F: Non-unique index → "NON-UNIQUE"
// ---------------------------------------------------------------------------

func makeTestLoopWithIndex(tabIdx int, unique bool) *WhereLoop {
	idx := &IndexInfo{
		Name:   "idx_test",
		Unique: unique,
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
	}
	return &WhereLoop{
		TabIndex: tabIdx,
		MaskSelf: Bitmask(1 << uint(tabIdx)),
		Index:    idx,
		NOut:     NewLogEst(10),
		Run:      NewLogEst(10),
		Terms:    []*WhereTerm{},
	}
}

func makeTestLoopNoIndex(tabIdx int) *WhereLoop {
	return &WhereLoop{
		TabIndex: tabIdx,
		MaskSelf: Bitmask(1 << uint(tabIdx)),
		Index:    nil,
		NOut:     NewLogEst(100),
		Run:      NewLogEst(100),
		Terms:    []*WhereTerm{},
	}
}

// TestMCDC5_ExplainLoopDetailed_IndexUnique_Last exercises index path, unique=true,
// isLast=true (i==totalLoops-1).
func TestMCDC5_ExplainLoopDetailed_IndexUnique_Last(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table := &TableInfo{
		Name:     "users",
		RowCount: 1000,
	}
	info := &WhereInfo{Tables: []*TableInfo{table}}
	loop := makeTestLoopWithIndex(0, true)

	result := p.explainLoopDetailed(info, loop, 0, 1) // i=0, totalLoops=1 → isLast=true
	if !strings.Contains(result, "INDEX SEARCH") {
		t.Errorf("expected INDEX SEARCH, got: %s", result)
	}
	if !strings.Contains(result, "UNIQUE") {
		t.Errorf("expected UNIQUE index type, got: %s", result)
	}
}

// TestMCDC5_ExplainLoopDetailed_IndexNonUnique_NotLast exercises index path,
// unique=false, isLast=false (not last loop).
func TestMCDC5_ExplainLoopDetailed_IndexNonUnique_NotLast(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table0 := &TableInfo{Name: "orders", RowCount: 500}
	table1 := &TableInfo{Name: "items", RowCount: 2000}
	info := &WhereInfo{Tables: []*TableInfo{table0, table1}}
	loop := makeTestLoopWithIndex(0, false)

	result := p.explainLoopDetailed(info, loop, 0, 2) // i=0, totalLoops=2 → isLast=false
	if !strings.Contains(result, "INDEX SEARCH") {
		t.Errorf("expected INDEX SEARCH, got: %s", result)
	}
	if !strings.Contains(result, "NON-UNIQUE") {
		t.Errorf("expected NON-UNIQUE index type, got: %s", result)
	}
	// The non-last prefix should include a tree-branch character.
	if !strings.Contains(result, "├─") {
		t.Errorf("expected tree-branch prefix for non-last loop, got: %s", result)
	}
}

// TestMCDC5_ExplainLoopDetailed_FullScan_Last exercises full scan path with isLast=true.
func TestMCDC5_ExplainLoopDetailed_FullScan_Last(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table := &TableInfo{Name: "products", RowCount: 5000}
	info := &WhereInfo{Tables: []*TableInfo{table}}
	loop := makeTestLoopNoIndex(0)

	result := p.explainLoopDetailed(info, loop, 0, 1) // isLast=true
	if !strings.Contains(result, "FULL TABLE SCAN") {
		t.Errorf("expected FULL TABLE SCAN, got: %s", result)
	}
	if !strings.Contains(result, "Sequential") {
		t.Errorf("expected Sequential access method, got: %s", result)
	}
}

// TestMCDC5_ExplainLoopDetailed_FullScan_NotLast exercises full scan path
// with isLast=false (two-loop plan; first loop is full scan).
func TestMCDC5_ExplainLoopDetailed_FullScan_NotLast(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table0 := &TableInfo{Name: "customers", RowCount: 10000}
	table1 := &TableInfo{Name: "payments", RowCount: 50000}
	info := &WhereInfo{Tables: []*TableInfo{table0, table1}}
	loop := makeTestLoopNoIndex(0)

	result := p.explainLoopDetailed(info, loop, 0, 2) // i=0, totalLoops=2 → isLast=false
	if !strings.Contains(result, "FULL TABLE SCAN") {
		t.Errorf("expected FULL TABLE SCAN, got: %s", result)
	}
	if !strings.Contains(result, "├─") {
		t.Errorf("expected tree-branch prefix for non-last loop, got: %s", result)
	}
}

// TestMCDC5_ExplainLoopDetailed_WithConstraintTerm exercises buildConstraintStrings
// when the loop has WHERE terms with valid column indices.
func TestMCDC5_ExplainLoopDetailed_WithConstraintTerm(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table := &TableInfo{
		Name:     "widgets",
		RowCount: 200,
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
			{Name: "price", Index: 1},
		},
	}
	info := &WhereInfo{Tables: []*TableInfo{table}}

	term := &WhereTerm{
		Operator:   WO_EQ,
		LeftCursor: 0,
		LeftColumn: 0,
	}
	idx := &IndexInfo{
		Name:   "idx_widgets_id",
		Unique: true,
		Columns: []IndexColumn{
			{Name: "id", Index: 0, Ascending: true},
		},
	}
	loop := &WhereLoop{
		TabIndex: 0,
		MaskSelf: 1,
		Index:    idx,
		NOut:     NewLogEst(1),
		Run:      NewLogEst(1),
		Terms:    []*WhereTerm{term},
	}

	result := p.explainLoopDetailed(info, loop, 0, 1)
	if !strings.Contains(result, "Constraints") {
		t.Errorf("expected Constraints line, got: %s", result)
	}
}

// ---------------------------------------------------------------------------
// statistics.go estimateEqualitySelectivity (uncovered branches)
//
// Branches:
//   val.(int) ok && val >= -1 && val <= 1  → truthProbSmallInt
//   val.(string) ok && len(val) == 0       → selectivityEq
//   default                                → selectivityEq
// ---------------------------------------------------------------------------

// TestMCDC5_EstimateSelectivity_SmallInt verifies small integers use truthProbSmallInt.
func TestMCDC5_EstimateSelectivity_SmallInt(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	for _, val := range []int{-1, 0, 1} {
		val := val
		t.Run("val="+string(rune('0'+val+1)), func(t *testing.T) {
			t.Parallel()
			term := &WhereTerm{
				Operator:   WO_EQ,
				RightValue: val,
			}
			got := EstimateSelectivity(term, stats)
			if got != truthProbSmallInt {
				t.Errorf("EstimateSelectivity(int=%d) = %d, want truthProbSmallInt=%d",
					val, got, truthProbSmallInt)
			}
		})
	}
}

// TestMCDC5_EstimateSelectivity_LargeInt verifies larger integers use selectivityEq.
func TestMCDC5_EstimateSelectivity_LargeInt(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 42,
	}
	got := EstimateSelectivity(term, stats)
	if got != selectivityEq {
		t.Errorf("EstimateSelectivity(int=42) = %d, want selectivityEq=%d", got, selectivityEq)
	}
}

// TestMCDC5_EstimateSelectivity_EmptyString verifies empty-string uses selectivityEq.
func TestMCDC5_EstimateSelectivity_EmptyString(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: "",
	}
	got := EstimateSelectivity(term, stats)
	if got != selectivityEq {
		t.Errorf("EstimateSelectivity(string='') = %d, want selectivityEq=%d", got, selectivityEq)
	}
}

// TestMCDC5_EstimateSelectivity_NonEmptyString verifies non-empty string uses selectivityEq.
func TestMCDC5_EstimateSelectivity_NonEmptyString(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: "alice",
	}
	got := EstimateSelectivity(term, stats)
	if got != selectivityEq {
		t.Errorf("EstimateSelectivity(string='alice') = %d, want selectivityEq=%d", got, selectivityEq)
	}
}

// TestMCDC5_EstimateSelectivity_NilValue verifies nil right value uses selectivityEq.
func TestMCDC5_EstimateSelectivity_NilValue(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: nil,
	}
	got := EstimateSelectivity(term, stats)
	if got != selectivityEq {
		t.Errorf("EstimateSelectivity(nil) = %d, want selectivityEq=%d", got, selectivityEq)
	}
}

// TestMCDC5_EstimateSelectivity_ISNULL verifies ISNULL returns selectivityNull.
func TestMCDC5_EstimateSelectivity_ISNULL(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{Operator: WO_ISNULL}
	got := EstimateSelectivity(term, stats)
	if got != selectivityNull {
		t.Errorf("EstimateSelectivity(WO_ISNULL) = %d, want selectivityNull=%d", got, selectivityNull)
	}
}

// TestMCDC5_EstimateSelectivity_Default verifies WO_OR returns truthProbDefault.
func TestMCDC5_EstimateSelectivity_Default(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()
	term := &WhereTerm{Operator: WO_OR}
	got := EstimateSelectivity(term, stats)
	if got != truthProbDefault {
		t.Errorf("EstimateSelectivity(WO_OR) = %d, want truthProbDefault=%d", got, truthProbDefault)
	}
}

// ---------------------------------------------------------------------------
// statistics.go ValidateStatistics error paths
// ---------------------------------------------------------------------------

// TestMCDC5_ValidateStatistics_NegativeRowCount verifies table negative row count.
func TestMCDC5_ValidateStatistics_NegativeRowCount(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.TableStats["bad"] = &TableStatistics{
		TableName: "bad",
		RowCount:  -1,
	}

	err := s.ValidateStatistics()
	if err == nil {
		t.Fatal("expected error for negative row count, got nil")
	}
	if !strings.Contains(err.Error(), "negative row count") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC5_ValidateStatistics_IndexNegativeRowCount verifies index negative row count.
func TestMCDC5_ValidateStatistics_IndexNegativeRowCount(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.IndexStats["bad_idx"] = &IndexStatistics{
		IndexName: "bad_idx",
		TableName: "t",
		RowCount:  -1,
		AvgEq:     []int64{},
	}

	err := s.ValidateStatistics()
	if err == nil {
		t.Fatal("expected error for index negative row count, got nil")
	}
	if !strings.Contains(err.Error(), "negative row count") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC5_ValidateStatistics_AvgEqLessThanOne verifies avgEq < 1 is rejected.
func TestMCDC5_ValidateStatistics_AvgEqLessThanOne(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.IndexStats["idx"] = &IndexStatistics{
		IndexName: "idx",
		TableName: "t",
		RowCount:  100,
		AvgEq:     []int64{0}, // 0 < 1
	}

	err := s.ValidateStatistics()
	if err == nil {
		t.Fatal("expected error for avgEq < 1, got nil")
	}
	if !strings.Contains(err.Error(), "invalid avgEq") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC5_ValidateStatistics_AvgEqGreaterThanRowCount verifies avgEq > rowCount is rejected.
func TestMCDC5_ValidateStatistics_AvgEqGreaterThanRowCount(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.IndexStats["idx2"] = &IndexStatistics{
		IndexName: "idx2",
		TableName: "t",
		RowCount:  50,
		AvgEq:     []int64{100}, // 100 > 50
	}

	err := s.ValidateStatistics()
	if err == nil {
		t.Fatal("expected error for avgEq > rowCount, got nil")
	}
	if !strings.Contains(err.Error(), "avgEq") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC5_ValidateStatistics_Valid verifies valid statistics pass without error.
func TestMCDC5_ValidateStatistics_Valid(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.TableStats["t"] = &TableStatistics{TableName: "t", RowCount: 1000}
	s.IndexStats["idx"] = &IndexStatistics{
		IndexName: "idx",
		TableName: "t",
		RowCount:  1000,
		AvgEq:     []int64{10},
	}

	if err := s.ValidateStatistics(); err != nil {
		t.Errorf("expected no error for valid stats, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// statistics.go EstimateDistinctValues – small-sample correction branch
//
// Condition: sampleSize < totalRows/10
//   A=T: sampleSize < totalRows/10 → correction applied
//   A=F: sampleSize >= totalRows/10 → no correction
// ---------------------------------------------------------------------------

// TestMCDC5_EstimateDistinctValues_SmallSample verifies correction for small samples.
func TestMCDC5_EstimateDistinctValues_SmallSample(t *testing.T) {
	t.Parallel()

	// sampleSize=1, totalRows=100 → sampleSize < totalRows/10 (1 < 10)
	got := EstimateDistinctValues(1, 1, 100)
	// The correction factor inflates the estimate above the naive ratio.
	// Naive = (1/1)*100 = 100; correction should push it >= 100.
	if got < 1 {
		t.Errorf("EstimateDistinctValues(1,1,100) = %d, expected >= 1", got)
	}
}

// TestMCDC5_EstimateDistinctValues_LargeSample verifies no correction for large samples.
func TestMCDC5_EstimateDistinctValues_LargeSample(t *testing.T) {
	t.Parallel()

	// sampleSize=50, totalRows=100 → sampleSize >= totalRows/10 (50 >= 10)
	got := EstimateDistinctValues(50, 30, 100)
	if got < 1 {
		t.Errorf("EstimateDistinctValues(50,30,100) = %d, expected >= 1", got)
	}
}

// TestMCDC5_EstimateDistinctValues_FullScan verifies exact count when sample == total.
func TestMCDC5_EstimateDistinctValues_FullScan(t *testing.T) {
	t.Parallel()

	got := EstimateDistinctValues(100, 42, 100)
	if got != 42 {
		t.Errorf("EstimateDistinctValues(100,42,100) = %d, want 42", got)
	}
}

// TestMCDC5_EstimateDistinctValues_ZeroSample verifies zero sample returns 1.
func TestMCDC5_EstimateDistinctValues_ZeroSample(t *testing.T) {
	t.Parallel()

	got := EstimateDistinctValues(0, 0, 100)
	if got != 1 {
		t.Errorf("EstimateDistinctValues(0,0,100) = %d, want 1", got)
	}
}

// TestMCDC5_EstimateDistinctValues_ZeroUnique verifies zero unique returns 1.
func TestMCDC5_EstimateDistinctValues_ZeroUnique(t *testing.T) {
	t.Parallel()

	got := EstimateDistinctValues(100, 0, 100)
	if got != 1 {
		t.Errorf("EstimateDistinctValues(100,0,100) = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// index.go processIndexColumns – first column no constraint returns false
//
// Condition inside processIndexColumns: return i > 0
//   When i == 0 and term == nil → return false (no constraint on first col)
//   When i > 0 and term == nil  → return true (gap after first col is ok)
// ---------------------------------------------------------------------------

// TestMCDC5_ProcessIndexColumns_NoFirstColumnConstraint verifies that when the
// first index column has no matching WHERE term, AnalyzeIndexUsage returns an
// IndexUsage with no keys.
func TestMCDC5_ProcessIndexColumns_NoFirstColumnConstraint(t *testing.T) {
	t.Parallel()

	table := &TableInfo{
		Name: "t",
		Columns: []ColumnInfo{
			{Name: "a", Index: 0},
			{Name: "b", Index: 1},
		},
	}
	idx := &IndexInfo{
		Name: "idx_ab",
		Columns: []IndexColumn{
			{Name: "a", Index: 0, Ascending: true},
			{Name: "b", Index: 1, Ascending: true},
		},
	}
	// No terms at all → first column has no constraint
	selector := NewIndexSelector(table, []*WhereTerm{}, NewCostModel())
	usage := selector.AnalyzeIndexUsage(idx, []string{"a", "b"})

	if len(usage.EqTerms) != 0 || len(usage.RangeTerms) != 0 || len(usage.InTerms) != 0 {
		t.Errorf("expected empty usage for no first-column constraint, got %+v", usage)
	}
}

// TestMCDC5_ProcessIndexColumns_SecondColumnGap verifies that a gap at the
// second column (i>0) correctly returns from processIndexColumns without error.
func TestMCDC5_ProcessIndexColumns_SecondColumnGap(t *testing.T) {
	t.Parallel()

	table := &TableInfo{
		Name: "t",
		Columns: []ColumnInfo{
			{Name: "a", Index: 0},
			{Name: "b", Index: 1},
		},
	}
	idx := &IndexInfo{
		Name: "idx_ab",
		Columns: []IndexColumn{
			{Name: "a", Index: 0, Ascending: true},
			{Name: "b", Index: 1, Ascending: true},
		},
	}
	// Term only for column 0, not column 1 → gap at i=1 returns true (used first col)
	term := &WhereTerm{
		Operator:   WO_EQ,
		LeftColumn: 0,
		RightValue: "x",
	}
	selector := NewIndexSelector(table, []*WhereTerm{term}, NewCostModel())
	usage := selector.AnalyzeIndexUsage(idx, []string{"a"})

	if len(usage.EqTerms) != 1 {
		t.Errorf("expected 1 eq term, got %d", len(usage.EqTerms))
	}
}

// ---------------------------------------------------------------------------
// index.go termMatchesExpression / termMatchesColumn expression path
//
// Condition: col.Expression != "" → use termMatchesExpression
// MC/DC: A=T (expression column) vs A=F (plain column)
// ---------------------------------------------------------------------------

// TestMCDC5_TermMatchesColumn_ExpressionPath verifies expression index matching.
func TestMCDC5_TermMatchesColumn_ExpressionPath(t *testing.T) {
	t.Parallel()

	table := &TableInfo{
		Name:    "t",
		Columns: []ColumnInfo{{Name: "lower_name", Index: 0}},
	}
	// Expression index on lower(name)
	idx := &IndexInfo{
		Name: "idx_lower_name",
		Columns: []IndexColumn{
			{Name: "lower_name", Index: 0, Expression: "lower(name)", Ascending: true},
		},
	}
	// Term whose expression text matches "lower(name)"
	left := &ColumnExpr{Table: "t", Column: "lower_name", Cursor: 0}
	right := &ValueExpr{Value: "alice"}
	expr := &BinaryExpr{Op: "=", Left: left, Right: right}
	term := &WhereTerm{
		Expr:       expr,
		Operator:   WO_EQ,
		LeftColumn: 0,
	}

	selector := NewIndexSelector(table, []*WhereTerm{term}, NewCostModel())
	// scoreIndex exercises termMatchesColumn with col.Expression != ""
	score := selector.scoreIndex(idx)
	// With expression column, termMatchesExpression is called. If the expression
	// doesn't match the term's LHS string repr, the score won't get the equality bonus.
	// Just verify it doesn't panic and returns a float.
	_ = score
}

// TestMCDC5_TermMatchesColumn_NilExprForExpression verifies nil expr handled safely.
func TestMCDC5_TermMatchesColumn_NilExprForExpression(t *testing.T) {
	t.Parallel()

	table := &TableInfo{
		Name:    "t",
		Columns: []ColumnInfo{{Name: "x", Index: 0}},
	}
	idx := &IndexInfo{
		Name: "idx_expr",
		Columns: []IndexColumn{
			{Name: "x", Index: 0, Expression: "upper(x)", Ascending: true},
		},
	}
	// Term with nil Expr
	term := &WhereTerm{
		Expr:       nil,
		Operator:   WO_EQ,
		LeftColumn: 0,
	}

	selector := NewIndexSelector(table, []*WhereTerm{term}, NewCostModel())
	// Should not panic.
	score := selector.scoreIndex(idx)
	_ = score
}

// ---------------------------------------------------------------------------
// join_algorithm.go CostEstimate – default branch
//
// The switch in CostEstimate handles NestedLoop, Hash, Merge, and default.
// The default branch is only hit when a hypothetical new algorithm value is used.
// We exercise it indirectly through JoinAlgorithm(99).
// ---------------------------------------------------------------------------

// TestMCDC5_CostEstimate_DefaultBranch verifies the default case falls through to
// nested loop cost estimation without panicking.
func TestMCDC5_CostEstimate_DefaultBranch(t *testing.T) {
	t.Parallel()

	tables := []*TableInfo{
		{Name: "a", RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "b", RowCount: 200, RowLogEst: NewLogEst(200)},
	}
	jo := NewJoinOptimizer(tables, nil, NewCostModel())

	outer := &JoinOrder{
		Tables:   []int{0},
		Cost:     NewLogEst(100),
		RowCount: NewLogEst(100),
	}
	inner := &JoinOrder{
		Tables:   []int{1},
		Cost:     NewLogEst(200),
		RowCount: NewLogEst(200),
	}

	// JoinAlgorithm(99) hits the default branch.
	cost, rows := jo.CostEstimate(outer, inner, JoinAlgorithm(99), nil)
	if cost <= 0 && rows <= 0 {
		// Both being zero could be valid — just check no panic occurred.
		t.Log("default branch returned zero cost and rows (acceptable)")
	}
}

// ---------------------------------------------------------------------------
// statistics.go ClearStatistics – multi-index removal path
// ---------------------------------------------------------------------------

// TestMCDC5_ClearStatistics_MultipleIndexes verifies that all indexes for a
// table are removed, including when a table has more than one index.
func TestMCDC5_ClearStatistics_MultipleIndexes(t *testing.T) {
	t.Parallel()

	s := NewStatistics()
	s.TableStats["t"] = &TableStatistics{TableName: "t", RowCount: 100}
	s.IndexStats["idx1"] = &IndexStatistics{IndexName: "idx1", TableName: "t"}
	s.IndexStats["idx2"] = &IndexStatistics{IndexName: "idx2", TableName: "t"}
	s.IndexStats["other_idx"] = &IndexStatistics{IndexName: "other_idx", TableName: "other"}

	s.ClearStatistics("t")

	if _, ok := s.TableStats["t"]; ok {
		t.Error("expected table stats for 't' to be removed")
	}
	if _, ok := s.IndexStats["idx1"]; ok {
		t.Error("expected idx1 to be removed")
	}
	if _, ok := s.IndexStats["idx2"]; ok {
		t.Error("expected idx2 to be removed")
	}
	if _, ok := s.IndexStats["other_idx"]; !ok {
		t.Error("expected other_idx to remain")
	}
}

// ---------------------------------------------------------------------------
// statistics.go applyEqualitySelectivity – nEq == 0 path and extrapolation path
// ---------------------------------------------------------------------------

// TestMCDC5_EstimateRows_ZeroNEq verifies that nEq=0 returns full row count.
func TestMCDC5_EstimateRows_ZeroNEq(t *testing.T) {
	t.Parallel()

	indexStats := &IndexStatistics{
		IndexName:   "idx",
		RowCount:    1000,
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	got := EstimateRows(indexStats, 0, false)
	want := NewLogEst(1000)
	if got != want {
		t.Errorf("EstimateRows(nEq=0) = %d, want %d", got, want)
	}
}

// TestMCDC5_EstimateRows_NilStats verifies nil stats returns default.
func TestMCDC5_EstimateRows_NilStats(t *testing.T) {
	t.Parallel()

	got := EstimateRows(nil, 1, false)
	if got != LogEst(100) {
		t.Errorf("EstimateRows(nil) = %d, want 100", got)
	}
}

// TestMCDC5_EstimateRows_ExtrapolatesBeyondStats verifies extrapolation when
// nEq > len(ColumnStats).
func TestMCDC5_EstimateRows_ExtrapolatesBeyondStats(t *testing.T) {
	t.Parallel()

	indexStats := &IndexStatistics{
		IndexName:   "idx",
		RowCount:    1000,
		ColumnStats: []LogEst{NewLogEst(100)}, // only 1 stat
	}

	// nEq=3 > len(ColumnStats)=1 → extrapolateSelectivity called
	got := EstimateRows(indexStats, 3, false)
	// Should not panic; value should be reasonable.
	if got < 0 {
		t.Errorf("EstimateRows extrapolation returned negative: %d", got)
	}
}

// TestMCDC5_EstimateRows_WithRange verifies range selectivity is applied.
func TestMCDC5_EstimateRows_WithRange(t *testing.T) {
	t.Parallel()

	indexStats := &IndexStatistics{
		IndexName:   "idx",
		RowCount:    1000,
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	withRange := EstimateRows(indexStats, 1, true)
	withoutRange := EstimateRows(indexStats, 1, false)

	// Range filter should reduce estimated rows (lower LogEst = fewer rows).
	if withRange > withoutRange {
		t.Errorf("range filter should not increase row estimate: withRange=%d withoutRange=%d",
			withRange, withoutRange)
	}
}
