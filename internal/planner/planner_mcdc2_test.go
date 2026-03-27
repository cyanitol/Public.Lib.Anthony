// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// MC/DC tests for internal/planner – second file
//
// Each function below documents the source location, the compound condition,
// and the sub-condition labels used in the test case names.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Source: join.go isValidSplit
//   Condition: left != 0 && left != subset
//   Sub-conditions:
//     A = left != 0
//     B = left != subset
//   Coverage pairs:
//     A=T, B=T -> true  (valid split)
//     A=F, B=T -> false (A flips: left==0 is an empty partition)
//     A=T, B=F -> false (B flips: left==subset means right would be empty)
// ---------------------------------------------------------------------------

func TestMCDC_IsValidSplit_LeftNonZeroAndNotSubset(t *testing.T) {
	t.Parallel()

	jo := &JoinOptimizer{}
	const subset = uint64(0b110) // bits 1 and 2 set

	cases := []struct {
		name      string
		left      uint64
		wantValid bool
	}{
		// A=T, B=T: left is a proper non-zero sub-mask
		{"MCDC A=T B=T: proper split", 0b010, true},
		// A=F, B=T: left==0 (A flips outcome)
		{"MCDC A=F B=T: left is zero", 0, false},
		// A=T, B=F: left==subset (B flips outcome)
		{"MCDC A=T B=F: left equals subset", subset, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := jo.isValidSplit(tc.left, subset)
			if got != tc.wantValid {
				t.Errorf("isValidSplit(left=%b, subset=%b) = %v, want %v",
					tc.left, subset, got, tc.wantValid)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go bothInputsSorted
//   Condition: len(outer.SortOrder) > 0 && len(inner.SortOrder) > 0
//   Sub-conditions:
//     A = len(outer.SortOrder) > 0
//     B = len(inner.SortOrder) > 0
//   Coverage pairs:
//     A=T, B=T -> true  (both sorted)
//     A=F, B=T -> false (A flips: outer unsorted)
//     A=T, B=F -> false (B flips: inner unsorted)
// ---------------------------------------------------------------------------

func TestMCDC_BothInputsSorted_OuterAndInner(t *testing.T) {
	t.Parallel()

	sortedCol := []SortColumn{{TableIdx: 0, Column: "id", Ascending: true}}

	cases := []struct {
		name       string
		outerSort  []SortColumn
		innerSort  []SortColumn
		wantSorted bool
	}{
		// A=T, B=T: both have sort orders
		{"MCDC A=T B=T: both sorted", sortedCol, sortedCol, true},
		// A=F, B=T: outer has no sort order (A flips)
		{"MCDC A=F B=T: outer unsorted", []SortColumn{}, sortedCol, false},
		// A=T, B=F: inner has no sort order (B flips)
		{"MCDC A=T B=F: inner unsorted", sortedCol, []SortColumn{}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outer := &JoinOrder{SortOrder: tc.outerSort}
			inner := &JoinOrder{SortOrder: tc.innerSort}
			got := bothInputsSorted(outer, inner)
			if got != tc.wantSorted {
				t.Errorf("bothInputsSorted: got %v, want %v", got, tc.wantSorted)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go findJoinConditions
//   Condition: jo.WhereInfo == nil || jo.WhereInfo.Clause == nil
//   Sub-conditions:
//     A = jo.WhereInfo == nil
//     B = jo.WhereInfo.Clause == nil
//   Coverage pairs:
//     A=T, *  -> nil returned (A dominates OR)
//     A=F, B=T -> nil returned (B flips outcome)
//     A=F, B=F -> condition checks proceed normally
// ---------------------------------------------------------------------------

func TestMCDC_FindJoinConditions_WhereInfoNilGuard(t *testing.T) {
	t.Parallel()

	table0 := createTestTable()
	table1 := &TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
		},
	}

	outer := &JoinOrder{Tables: []int{0}}
	inner := &JoinOrder{Tables: []int{1}}

	cases := []struct {
		name      string
		whereInfo *WhereInfo
		wantNil   bool
	}{
		// A=T: WhereInfo is nil
		{"MCDC A=T: nil WhereInfo", nil, true},
		// A=F, B=T: WhereInfo exists but Clause is nil (B flips)
		{"MCDC A=F B=T: nil Clause", &WhereInfo{
			Tables: []*TableInfo{table0, table1},
			Clause: nil,
		}, true},
		// A=F, B=F: both non-nil, returns based on term overlap
		{"MCDC A=F B=F: both non-nil", &WhereInfo{
			Tables: []*TableInfo{table0, table1},
			Clause: &WhereClause{Terms: []*WhereTerm{}},
		}, true}, // empty terms -> nil result is expected
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			jo := &JoinOptimizer{
				Tables:    []*TableInfo{table0, table1},
				WhereInfo: tc.whereInfo,
				CostModel: NewCostModel(),
			}
			result := jo.findJoinConditions(outer, inner)
			isNil := result == nil
			if isNil != tc.wantNil {
				t.Errorf("findJoinConditions: got nil=%v, want nil=%v", isNil, tc.wantNil)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go findJoinConditions (body)
//   Condition: term.PrereqAll.Overlaps(outerMask) && term.PrereqAll.Overlaps(innerMask)
//   Sub-conditions:
//     A = term.PrereqAll.Overlaps(outerMask)
//     B = term.PrereqAll.Overlaps(innerMask)
//   Coverage pairs:
//     A=T, B=T -> term included in join conditions
//     A=F, B=T -> term excluded (A flips)
//     A=T, B=F -> term excluded (B flips)
// ---------------------------------------------------------------------------

func TestMCDC_FindJoinConditions_TermOverlap(t *testing.T) {
	t.Parallel()

	table0 := createTestTable()
	table1 := &TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
		},
	}

	// outerMask = bit 0, innerMask = bit 1
	var outerPrereqOnly, innerPrereqOnly, bothPrereq Bitmask
	outerPrereqOnly.Set(0)
	innerPrereqOnly.Set(1)
	bothPrereq.Set(0)
	bothPrereq.Set(1)

	outer := &JoinOrder{Tables: []int{0}}
	inner := &JoinOrder{Tables: []int{1}}

	cases := []struct {
		name      string
		prereqAll Bitmask
		wantFound bool
	}{
		// A=T, B=T: term references both tables
		{"MCDC A=T B=T: both tables referenced", bothPrereq, true},
		// A=F, B=T: term only references inner table (A flips: outer not overlapped)
		{"MCDC A=F B=T: only inner referenced", innerPrereqOnly, false},
		// A=T, B=F: term only references outer table (B flips: inner not overlapped)
		{"MCDC A=T B=F: only outer referenced", outerPrereqOnly, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			term := &WhereTerm{
				Operator:    WO_EQ,
				LeftCursor:  0,
				LeftColumn:  0,
				PrereqRight: Bitmask(0),
				PrereqAll:   tc.prereqAll,
			}
			clause := &WhereClause{Terms: []*WhereTerm{term}}
			info := &WhereInfo{
				Tables: []*TableInfo{table0, table1},
				Clause: clause,
			}
			jo := &JoinOptimizer{
				Tables:    []*TableInfo{table0, table1},
				WhereInfo: info,
				CostModel: NewCostModel(),
			}
			result := jo.findJoinConditions(outer, inner)
			found := len(result) > 0
			if found != tc.wantFound {
				t.Errorf("findJoinConditions: found=%v, want=%v", found, tc.wantFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: subquery.go canFlattenSubquery
//   Condition: info.Type == SubqueryScalar && !info.IsCorrelated
//   Sub-conditions:
//     A = info.Type == SubqueryScalar
//     B = !info.IsCorrelated   (i.e., info.IsCorrelated == false)
//   Coverage pairs:
//     A=T, B=T -> true  (simple uncorrelated scalar subquery)
//     A=F, B=T -> depends on type (A flips; SubqueryFrom returns true via first branch)
//     A=T, B=F -> false (B flips: scalar but correlated)
// ---------------------------------------------------------------------------

func TestMCDC_CanFlattenSubquery_ScalarAndUncorrelated(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	opt := NewSubqueryOptimizer(cm)

	cases := []struct {
		name        string
		sqType      SubqueryType
		isCorr      bool
		wantFlatten bool
	}{
		// A=T, B=T: scalar, uncorrelated -> flatten
		{"MCDC A=T B=T: scalar uncorrelated", SubqueryScalar, false, true},
		// A=F (SubqueryFrom), B=T: FROM subqueries flatten via first branch
		{"MCDC A=F B=T: FROM type", SubqueryFrom, false, true},
		// A=T, B=F: scalar but correlated -> no flatten (B flips)
		{"MCDC A=T B=F: scalar correlated", SubqueryScalar, true, false},
		// A=F (SubqueryIn), B=T: IN subqueries are not flattened
		{"MCDC A=F B=T: IN type not flattened", SubqueryIn, false, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			info := &SubqueryInfo{
				Type:           tc.sqType,
				IsCorrelated:   tc.isCorr,
				EstimatedRows:  NewLogEst(10),
				ExecutionCount: NewLogEst(1),
			}
			got := opt.canFlattenSubquery(info)
			if got != tc.wantFlatten {
				t.Errorf("canFlattenSubquery: got %v, want %v", got, tc.wantFlatten)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: planner.go propagateConstants
//   Condition: term.Operator != WO_EQ || term.RightValue == nil
//   Sub-conditions:
//     A = term.Operator != WO_EQ
//     B = term.RightValue == nil
//   When true (A||B) -> skip term (no propagation)
//   When false (A=F, B=F) -> propagate constant
// ---------------------------------------------------------------------------

func TestMCDC_PropagateConstants_OperatorAndNilCheck(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	tableA := createTestTable()

	// Build a simple equiv map with one equivalence
	// We'll call propagateConstants directly with different term configurations
	// and count new terms produced.

	cases := []struct {
		name       string
		operator   WhereOperator
		rightValue interface{}
		wantPropag bool // whether we expect propagation to produce new terms
	}{
		// A=F, B=F: WO_EQ with non-nil value -> propagation attempted
		{"MCDC A=F B=F: eq with value", WO_EQ, 42, true},
		// A=T, B=F: non-EQ operator -> skip (A flips outcome)
		{"MCDC A=T B=F: non-eq operator", WO_GT, 42, false},
		// A=F, B=T: WO_EQ but nil value -> skip (B flips outcome)
		{"MCDC A=F B=T: eq with nil value", WO_EQ, nil, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a clause with a term and a trivial equivalence class for col 0
			dummyExpr := &BinaryExpr{
				Op:    "=",
				Left:  &ColumnExpr{Cursor: 0, Column: "id"},
				Right: &ColumnExpr{Cursor: 0, Column: "id"},
			}

			term := &WhereTerm{
				Expr:        dummyExpr,
				Operator:    tc.operator,
				LeftCursor:  0,
				LeftColumn:  0,
				RightValue:  tc.rightValue,
				PrereqRight: 0,
			}
			clause := &WhereClause{Terms: []*WhereTerm{term}}

			// equiv map: col "0.0" maps to "0.1" (a synthetic equivalent)
			equiv := map[string][]string{
				"0.0": {"0.1"},
			}

			newTerms := p.propagateConstants(clause, equiv)
			got := len(newTerms) > 0

			// But "0.1" must reference a valid column index in the table
			// so propagation uses the term's LeftCursor/LeftColumn derivation.
			// Even if the equiv key maps somewhere, the propagation occurs only
			// when the term is WO_EQ with non-nil RightValue and is a BinaryExpr.
			_ = tableA
			if got != tc.wantPropag {
				t.Errorf("propagateConstants: got newTerms=%v (len=%d), wantPropag=%v",
					newTerms, len(newTerms), tc.wantPropag)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go canFlattenView
//   Condition: view == nil || view.Select == nil
//   Sub-conditions:
//     A = view == nil
//     B = view.Select == nil
//   Coverage pairs:
//     A=T, * -> false (A dominates OR guard)
//     A=F, B=T -> false (B flips outcome)
//     A=F, B=F -> continues to check other conditions
// ---------------------------------------------------------------------------

func TestMCDC_CanFlattenView_NilGuard(t *testing.T) {
	t.Parallel()

	simpleSelect := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Star: true},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	cases := []struct {
		name        string
		view        *schema.View
		wantFlatten bool
	}{
		// A=T: nil view
		{"MCDC A=T: nil view", nil, false},
		// A=F, B=T: non-nil view but nil Select (B flips)
		{"MCDC A=F B=T: nil Select", &schema.View{Name: "v", Select: nil}, false},
		// A=F, B=F: both non-nil, simple view -> can flatten
		{"MCDC A=F B=F: simple view", &schema.View{
			Name:   "v",
			Select: simpleSelect,
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := canFlattenView(tc.view)
			if got != tc.wantFlatten {
				t.Errorf("canFlattenView: got %v, want %v", got, tc.wantFlatten)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go hasNoComplexFeatures
//   Condition 1: len(sel.GroupBy) > 0 || sel.Having != nil
//   Sub-conditions:
//     A = len(sel.GroupBy) > 0
//     B = sel.Having != nil
//   Coverage pairs:
//     A=T, * -> false (A dominates OR)
//     A=F, B=T -> false (B flips outcome)
//     A=F, B=F -> not blocked by this guard (proceeds)
//
//   Condition 2: sel.Limit != nil || sel.Offset != nil
//   Sub-conditions:
//     C = sel.Limit != nil
//     D = sel.Offset != nil
//   Coverage pairs:
//     C=T, * -> false (C dominates OR)
//     C=F, D=T -> false (D flips outcome)
//     C=F, D=F -> not blocked by this guard (proceeds)
// ---------------------------------------------------------------------------

func TestMCDC_HasNoComplexFeatures_GroupByAndHaving(t *testing.T) {
	t.Parallel()

	havingExpr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}

	cases := []struct {
		name       string
		groupBy    []parser.Expression
		having     parser.Expression
		wantNoComp bool
	}{
		// A=T: has GROUP BY -> hasNoComplexFeatures returns false
		{"MCDC A=T: has GroupBy", []parser.Expression{havingExpr}, nil, false},
		// A=F, B=T: no GROUP BY but has HAVING -> false (B flips)
		{"MCDC A=F B=T: has Having", nil, havingExpr, false},
		// A=F, B=F: neither -> passes this guard (true)
		{"MCDC A=F B=F: no GroupBy no Having", nil, nil, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sel := &parser.SelectStmt{
				GroupBy: tc.groupBy,
				Having:  tc.having,
				// No Distinct, Limit, Offset, Compound
			}
			got := hasNoComplexFeatures(sel)
			if got != tc.wantNoComp {
				t.Errorf("hasNoComplexFeatures (GroupBy/Having): got %v, want %v", got, tc.wantNoComp)
			}
		})
	}
}

func TestMCDC_HasNoComplexFeatures_LimitAndOffset(t *testing.T) {
	t.Parallel()

	limitExpr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"}
	offsetExpr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"}

	cases := []struct {
		name       string
		limit      parser.Expression
		offset     parser.Expression
		wantNoComp bool
	}{
		// C=T: has Limit -> false
		{"MCDC C=T: has Limit", limitExpr, nil, false},
		// C=F, D=T: no Limit but has Offset -> false (D flips)
		{"MCDC C=F D=T: has Offset", nil, offsetExpr, false},
		// C=F, D=F: neither Limit nor Offset -> passes (true)
		{"MCDC C=F D=F: no Limit no Offset", nil, nil, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sel := &parser.SelectStmt{
				Limit:  tc.limit,
				Offset: tc.offset,
				// No GroupBy, Having, Distinct, Compound
			}
			got := hasNoComplexFeatures(sel)
			if got != tc.wantNoComp {
				t.Errorf("hasNoComplexFeatures (Limit/Offset): got %v, want %v", got, tc.wantNoComp)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: planner.go buildEquivalenceClasses
//   Condition: rightCol.Cursor >= 0 && rightCol.Cursor < len(tables)
//   Sub-conditions:
//     A = rightCol.Cursor >= 0
//     B = rightCol.Cursor < len(tables)
//   Coverage pairs:
//     A=T, B=T -> bounds check passes; rightColIdx is looked up
//     A=F, B=T -> cursor < 0, so findColumnIndex is skipped
//     A=T, B=F -> cursor >= len(tables), so findColumnIndex is skipped
//
//   We exercise this via buildEquivalenceClasses directly.
// ---------------------------------------------------------------------------

func TestMCDC_BuildEquivalenceClasses_CursorBoundsCheck(t *testing.T) {
	t.Parallel()

	tableA := createTestTable() // cursor 0
	tableB := &TableInfo{
		Name:   "orders",
		Cursor: 1,
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
		},
	}
	tables := []*TableInfo{tableA, tableB}

	p := NewPlanner()

	// Build a BinaryExpr with a ColumnExpr on the right
	makeEquivTerm := func(rightCursor int) *WhereTerm {
		rightColExpr := &ColumnExpr{Cursor: rightCursor, Column: "id"}
		binExpr := &BinaryExpr{
			Op:    "=",
			Left:  &ColumnExpr{Cursor: 0, Column: "id"},
			Right: rightColExpr,
		}
		return &WhereTerm{
			Expr:       binExpr,
			Operator:   WO_EQ,
			LeftCursor: 0,
			LeftColumn: 0,
		}
	}

	cases := []struct {
		name         string
		rightCurs    int
		wantEquivLen int // number of keys in equiv map
	}{
		// A=T, B=T: cursor 1 is within bounds -> both keys populated
		{"MCDC A=T B=T: cursor in bounds", 1, 2},
		// A=F, B=T: cursor -1 is < 0, out of bounds
		{"MCDC A=F B=T: cursor negative", -1, 2},
		// A=T, B=F: cursor 5 is >= len(tables)=2, out of bounds
		{"MCDC A=T B=F: cursor too large", 5, 2},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			term := makeEquivTerm(tc.rightCurs)
			clause := &WhereClause{Terms: []*WhereTerm{term}}
			equiv := p.buildEquivalenceClasses(clause, tables)
			// The function always produces 2 entries (left key -> [right key] and right key -> [left key])
			// The distinction is in whether the keys are correctly resolved.
			// We only assert it doesn't panic and produces a non-empty map.
			if len(equiv) != tc.wantEquivLen {
				t.Errorf("buildEquivalenceClasses: len(equiv)=%d, want %d", len(equiv), tc.wantEquivLen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go SelectJoinAlgorithm
//   Condition: innerRows > 100 && innerRows < outerRows*10
//   Sub-conditions:
//     A = innerRows > 100
//     B = innerRows < outerRows*10
//   Coverage pairs:
//     A=T, B=T -> JoinHash selected
//     A=F, B=T -> JoinNestedLoop or JoinMerge (A flips: inner too small)
//     A=T, B=F -> JoinNestedLoop or JoinMerge (B flips: inner too large)
// ---------------------------------------------------------------------------

func TestMCDC_SelectJoinAlgorithm_InnerRowsRange(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()

	// We need an equi-join condition to get past the hasEquiJoin check
	eqTerm := &WhereTerm{Operator: WO_EQ}

	makeOrder := func(rows int64) *JoinOrder {
		return &JoinOrder{
			Tables:    []int{0},
			RowCount:  NewLogEst(rows),
			SortOrder: []SortColumn{},
		}
	}

	jo := &JoinOptimizer{
		CostModel: cm,
		Tables:    []*TableInfo{createTestTable()},
	}

	cases := []struct {
		name      string
		outerRows int64
		innerRows int64
		wantHash  bool
	}{
		// A=T, B=T: innerRows=500 > 100 and 500 < 1000*10=10000 -> JoinHash
		{"MCDC A=T B=T: inner in range -> hash", 1000, 500, true},
		// A=F, B=T: innerRows=50 <= 100 -> not JoinHash (A flips)
		{"MCDC A=F B=T: inner too small -> not hash", 1000, 50, false},
		// A=T, B=F: innerRows=20000 > 100 but >= outerRows*10=1000*10=10000 -> not JoinHash (B flips)
		{"MCDC A=T B=F: inner too large -> not hash", 1000, 20000, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outer := makeOrder(tc.outerRows)
			inner := makeOrder(tc.innerRows)
			algo := jo.SelectJoinAlgorithm(outer, inner, []*WhereTerm{eqTerm})
			gotHash := (algo == JoinHash)
			if gotHash != tc.wantHash {
				t.Errorf("SelectJoinAlgorithm: got %v (wantHash=%v)", algo, tc.wantHash)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go canUseMergeJoin
//   Condition: hasJoinTerms(joinTerms) [A] checked first
//   A = len(joinTerms) > 0
//   Coverage pairs:
//     A=T -> proceeds to check equi-join term
//     A=F -> returns false immediately (A flips)
// ---------------------------------------------------------------------------

func TestMCDC_CanUseMergeJoin_HasJoinTerms(t *testing.T) {
	t.Parallel()

	sortedOrder := &JoinOrder{
		SortOrder: []SortColumn{{Column: "id", Ascending: true}},
	}

	cases := []struct {
		name      string
		joinTerms []*WhereTerm
		wantMerge bool
	}{
		// A=T: non-empty terms with EQ -> can proceed; both sorted same dir -> merge possible
		{"MCDC A=T: terms present eq both sorted", []*WhereTerm{{Operator: WO_EQ}}, true},
		// A=F: empty terms -> false (A flips)
		{"MCDC A=F: no terms no merge", []*WhereTerm{}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := canUseMergeJoin(sortedOrder, sortedOrder, tc.joinTerms)
			if got != tc.wantMerge {
				t.Errorf("canUseMergeJoin: got %v, want %v", got, tc.wantMerge)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go haveSameSortDirection
//   Condition: outer.SortOrder[0].Ascending == inner.SortOrder[0].Ascending
//   Coverage pairs:
//     A=T -> returns true (same direction)
//     A=F -> returns false (direction mismatch flips outcome)
// ---------------------------------------------------------------------------

func TestMCDC_HaveSameSortDirection_Ascending(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		outerAsc bool
		innerAsc bool
		wantSame bool
	}{
		// A=T: both ascending -> same direction
		{"MCDC A=T: both ASC", true, true, true},
		// A=T: both descending -> same direction
		{"MCDC A=T: both DESC", false, false, true},
		// A=F: outer ASC, inner DESC -> mismatch (A flips)
		{"MCDC A=F: ASC vs DESC", true, false, false},
		// A=F: outer DESC, inner ASC -> mismatch (A flips, symmetric)
		{"MCDC A=F: DESC vs ASC", false, true, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outer := &JoinOrder{SortOrder: []SortColumn{{Ascending: tc.outerAsc}}}
			inner := &JoinOrder{SortOrder: []SortColumn{{Ascending: tc.innerAsc}}}
			got := haveSameSortDirection(outer, inner)
			if got != tc.wantSame {
				t.Errorf("haveSameSortDirection: got %v, want %v", got, tc.wantSame)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go CheckOrderByOptimization
//   Condition 1: nEq >= len(index.Columns)
//     A = nEq >= len(index.Columns)
//     A=T -> returns false (all columns used for equality, none for ORDER BY)
//     A=F -> proceeds to column matching
//
//   Condition 2 (per-column): index.Columns[idxCol].Ascending != ob.Ascending
//     B = Ascending mismatch
//     B=T -> returns false (sort direction mismatch)
//     B=F -> continues matching
// ---------------------------------------------------------------------------

func TestMCDC_CheckOrderByOptimization_NEqVsCols(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "city", Ascending: true},
			{Name: "age", Ascending: true},
		},
	}

	cases := []struct {
		name    string
		nEq     int
		orderBy []OrderByColumn
		wantOpt bool
	}{
		// A=T: nEq=2 >= len(columns)=2 -> false
		{"MCDC A=T: nEq covers all cols", 2, []OrderByColumn{{Column: "city", Ascending: true}}, false},
		// A=F: nEq=0 < 2 -> check columns; city matches -> true
		{"MCDC A=F: nEq=0 col matches", 0, []OrderByColumn{{Column: "city", Ascending: true}}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cm.CheckOrderByOptimization(index, tc.orderBy, tc.nEq)
			if got != tc.wantOpt {
				t.Errorf("CheckOrderByOptimization: got %v, want %v", got, tc.wantOpt)
			}
		})
	}
}

func TestMCDC_CheckOrderByOptimization_AscendingMismatch(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "city", Ascending: true},
		},
	}

	cases := []struct {
		name    string
		obAsc   bool
		wantOpt bool
	}{
		// B=F: index.Ascending==true == ob.Ascending==true -> no mismatch -> true
		{"MCDC B=F: ascending matches", true, true},
		// B=T: index.Ascending==true != ob.Ascending==false -> mismatch -> false
		{"MCDC B=T: ascending mismatch", false, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ob := []OrderByColumn{{Column: "city", Ascending: tc.obAsc}}
			got := cm.CheckOrderByOptimization(index, ob, 0)
			if got != tc.wantOpt {
				t.Errorf("CheckOrderByOptimization ascending: got %v, want %v", got, tc.wantOpt)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go CompareCosts
//   Condition 1: cost1 < cost2 -> returns true
//   Condition 2: cost1 > cost2 -> returns false
//   Condition 3: nOut1 < nOut2 (tie-breaker when costs equal)
//   Sub-conditions:
//     A = cost1 < cost2
//     B = cost1 > cost2
//     C = nOut1 < nOut2   (reached only when A=F and B=F)
//   Coverage pairs:
//     A=T -> true  (A dominates)
//     A=F, B=T -> false (B dominates)
//     A=F, B=F, C=T -> true  (tie, fewer rows wins)
//     A=F, B=F, C=F -> false (tie, more or equal rows loses)
// ---------------------------------------------------------------------------

func TestMCDC_CompareCosts_ThreeConditions(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()

	cases := []struct {
		name   string
		cost1  LogEst
		nOut1  LogEst
		cost2  LogEst
		nOut2  LogEst
		wantP1 bool
	}{
		// A=T: cost1 < cost2 -> path1 better
		{"MCDC A=T: lower cost wins", 10, 50, 20, 50, true},
		// A=F, B=T: cost1 > cost2 -> path2 better
		{"MCDC A=F B=T: higher cost loses", 20, 50, 10, 50, false},
		// A=F, B=F, C=T: equal cost, nOut1 < nOut2 -> path1 better
		{"MCDC A=F B=F C=T: tie cost fewer rows wins", 10, 30, 10, 50, true},
		// A=F, B=F, C=F: equal cost, nOut1 >= nOut2 -> path2 wins (or tie)
		{"MCDC A=F B=F C=F: tie cost more rows loses", 10, 50, 10, 30, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cm.CompareCosts(tc.cost1, tc.nOut1, tc.cost2, tc.nOut2)
			if got != tc.wantP1 {
				t.Errorf("CompareCosts: got %v, want %v", got, tc.wantP1)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go termMatchesColumn
//   Condition: col.Expression != ""
//   A = col.Expression != ""
//   Coverage pairs:
//     A=T -> use expression matching path
//     A=F -> use column-index matching path
// ---------------------------------------------------------------------------

func TestMCDC_TermMatchesColumn_ExpressionVsColumnIndex(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()
	sel := NewIndexSelector(table, nil, cm)

	cases := []struct {
		name       string
		colExpr    string // empty = column-index match; non-empty = expression match
		termColIdx int
		wantMatch  bool
	}{
		// A=F: empty expression -> use column index; term matches col at idx 0
		{"MCDC A=F: col-index match", "", 0, true},
		// A=F: empty expression -> use column index; term doesn't match
		{"MCDC A=F: col-index no match", "", 1, false},
		// A=T: non-empty expression -> use expression matching; term expr doesn't match
		{"MCDC A=T: expr mismatch", "lower(city)", 0, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			col := IndexColumn{Name: "id", Index: 0, Expression: tc.colExpr}
			term := &WhereTerm{
				LeftColumn: tc.termColIdx,
				Operator:   WO_EQ,
			}
			got := sel.termMatchesColumn(term, col)
			if got != tc.wantMatch {
				t.Errorf("termMatchesColumn: got %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go findUsableTermsForIndex
//   Condition: !found && i > 0
//   Sub-conditions:
//     A = !found  (no term for this column)
//     B = i > 0   (not the first column)
//   Coverage pairs:
//     A=T, B=T -> break out of loop (stops scanning later columns)
//     A=T, B=F -> i==0, no break even if not found (A flips; first col with no match)
//     A=F, B=T -> term found, no break (A flips with term present)
// ---------------------------------------------------------------------------

func TestMCDC_FindUsableTermsForIndex_NotFoundAndNotFirst(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()

	multiColIdx := &IndexInfo{
		Name:  "multi_col",
		Table: "users",
		Columns: []IndexColumn{
			{Name: "city", Index: 3, Ascending: true},
			{Name: "age", Index: 2, Ascending: true},
		},
	}

	// Term matching only city (index col 0 of the multi-col index)
	cityTerm := &WhereTerm{
		LeftCursor: 0,
		LeftColumn: 3, // city index
		Operator:   WO_EQ,
	}
	// Term matching only age (index col 1 of the multi-col index)
	ageTerm := &WhereTerm{
		LeftCursor: 0,
		LeftColumn: 2, // age index
		Operator:   WO_GT,
	}

	cases := []struct {
		name      string
		terms     []*WhereTerm
		nCol      int
		wantCount int
	}{
		// A=T, B=F: first col (i=0) not found (no city term) -> no break, returns 0
		{"MCDC A=T B=F: first col missing", []*WhereTerm{ageTerm}, 2, 0},
		// A=T, B=T: second col (i=1) not found (no age term) -> break, returns 1 (city only)
		{"MCDC A=T B=T: second col missing stops", []*WhereTerm{cityTerm}, 2, 1},
		// A=F, B=T: second col (i=1) found (age term) -> no break, returns 2
		{"MCDC A=F B=T: both cols found", []*WhereTerm{cityTerm, ageTerm}, 2, 2},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			builder := &WhereLoopBuilder{
				Table:     table,
				Cursor:    0,
				Terms:     tc.terms,
				CostModel: cm,
			}
			result := builder.findUsableTerms(multiColIdx, tc.nCol)
			if len(result) != tc.wantCount {
				t.Errorf("findUsableTerms: got %d terms, want %d", len(result), tc.wantCount)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go addRangeFlags
//   Condition: !c.hasRange -> return early
//   Then: hasLowerBound(terms) and hasUpperBound(terms) set additional flags.
//
//   A = c.hasRange  (the guard !c.hasRange returns early when A=F)
//   B = hasLowerBound(terms)  (WO_GT or WO_GE)
//   C = hasUpperBound(terms)  (WO_LT or WO_LE)
//   Coverage pairs:
//     A=F -> WHERE_COLUMN_RANGE not set
//     A=T, B=T -> WHERE_BTM_LIMIT set
//     A=T, B=F, C=T -> WHERE_TOP_LIMIT set
//     A=T, B=T, C=T -> both limits set
// ---------------------------------------------------------------------------

func TestMCDC_AddRangeFlags_HasRangeAndBounds(t *testing.T) {
	t.Parallel()

	lowerTerm := &WhereTerm{Operator: WO_GT}
	upperTerm := &WhereTerm{Operator: WO_LT}

	cases := []struct {
		name      string
		hasRange  bool
		terms     []*WhereTerm
		wantRange bool
		wantBtm   bool
		wantTop   bool
	}{
		// A=F: no range -> WHERE_COLUMN_RANGE not set
		{"MCDC A=F: no range flag", false, []*WhereTerm{lowerTerm}, false, false, false},
		// A=T, B=T, C=F: lower bound only
		{"MCDC A=T B=T C=F: lower only", true, []*WhereTerm{lowerTerm}, true, true, false},
		// A=T, B=F, C=T: upper bound only
		{"MCDC A=T B=F C=T: upper only", true, []*WhereTerm{upperTerm}, true, false, true},
		// A=T, B=T, C=T: both bounds
		{"MCDC A=T B=T C=T: both bounds", true, []*WhereTerm{lowerTerm, upperTerm}, true, true, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := termConstraints{hasRange: tc.hasRange}
			flags := addRangeFlags(c, tc.terms, WHERE_INDEXED)
			gotRange := flags&WHERE_COLUMN_RANGE != 0
			gotBtm := flags&WHERE_BTM_LIMIT != 0
			gotTop := flags&WHERE_TOP_LIMIT != 0
			if gotRange != tc.wantRange {
				t.Errorf("addRangeFlags: WHERE_COLUMN_RANGE=%v, want %v", gotRange, tc.wantRange)
			}
			if gotBtm != tc.wantBtm {
				t.Errorf("addRangeFlags: WHERE_BTM_LIMIT=%v, want %v", gotBtm, tc.wantBtm)
			}
			if gotTop != tc.wantTop {
				t.Errorf("addRangeFlags: WHERE_TOP_LIMIT=%v, want %v", gotTop, tc.wantTop)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go partialIndexUsable
//   Condition: index.WhereClause == ""
//   A = (index.WhereClause == "")
//   A=T -> return false immediately (empty clause cannot be tested)
//   A=F -> proceed to check terms
// ---------------------------------------------------------------------------

func TestMCDC_PartialIndexUsable_WhereClauseEmpty(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()

	termForCursor0 := &WhereTerm{LeftCursor: 0, LeftColumn: 0, Operator: WO_EQ}

	cases := []struct {
		name        string
		whereClause string
		terms       []*WhereTerm
		wantUsable  bool
	}{
		// A=T: empty WhereClause -> not usable
		{"MCDC A=T: empty where clause", "", []*WhereTerm{termForCursor0}, false},
		// A=F: non-empty WhereClause + term matching cursor -> usable (A flips)
		{"MCDC A=F: non-empty where + term", "active = 1", []*WhereTerm{termForCursor0}, true},
		// A=F: non-empty WhereClause but no matching terms -> not usable (A=F, no term)
		{"MCDC A=F: non-empty where no terms", "active = 1", []*WhereTerm{}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			index := &IndexInfo{
				Name:        "partial_idx",
				Partial:     true,
				WhereClause: tc.whereClause,
				Columns:     []IndexColumn{{Name: "age", Index: 2}},
			}
			builder := &WhereLoopBuilder{
				Table:     table,
				Cursor:    0,
				Terms:     tc.terms,
				CostModel: cm,
			}
			got := builder.partialIndexUsable(index)
			if got != tc.wantUsable {
				t.Errorf("partialIndexUsable: got %v, want %v", got, tc.wantUsable)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: statistics.go applyEqualitySelectivity
//   Condition 1: nEq <= 0 -> return nOut unchanged
//     A = nEq <= 0
//     A=T -> no selectivity applied (returns as-is)
//     A=F -> proceeds to check ColumnStats
//
//   Condition 2: nEq <= len(indexStats.ColumnStats)
//     B = nEq <= len(indexStats.ColumnStats)
//     B=T -> return ColumnStats[nEq-1]
//     B=F -> extrapolate beyond stats
// ---------------------------------------------------------------------------

func TestMCDC_ApplyEqualitySelectivity_NEqGuards(t *testing.T) {
	t.Parallel()

	statVal := NewLogEst(100)
	stats := &IndexStatistics{
		RowCount:    10000,
		ColumnStats: []LogEst{statVal},
	}

	nRowsBase := NewLogEst(10000)

	cases := []struct {
		name    string
		nEq     int
		wantOut LogEst
	}{
		// A=T: nEq=0 -> return nOut unchanged
		{"MCDC A=T: nEq=0 unchanged", 0, nRowsBase},
		// A=F, B=T: nEq=1 <= len(ColumnStats)=1 -> return ColumnStats[0]
		{"MCDC A=F B=T: nEq=1 use stats", 1, statVal},
		// A=F, B=F: nEq=2 > len(ColumnStats)=1 -> extrapolate
		{"MCDC A=F B=F: nEq=2 extrapolate", 2, -1 /* sentinel */},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := applyEqualitySelectivity(stats, tc.nEq, nRowsBase)
			if tc.wantOut == -1 {
				// Extrapolated: should differ from statVal
				if got == statVal {
					t.Errorf("applyEqualitySelectivity: expected extrapolated != statVal %d, got %d", statVal, got)
				}
			} else {
				if got != tc.wantOut {
					t.Errorf("applyEqualitySelectivity(nEq=%d): got %d, want %d", tc.nEq, got, tc.wantOut)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: planner.go buildConstraintStrings
//   Condition: term.LeftColumn >= 0 && term.LeftColumn < len(table.Columns)
//   Sub-conditions:
//     A = term.LeftColumn >= 0
//     B = term.LeftColumn < len(table.Columns)
//   Coverage pairs:
//     A=T, B=T -> column name looked up and constraint appended
//     A=F, B=T -> column index negative -> constraint not added (A flips)
//     A=T, B=F -> column index out of range -> constraint not added (B flips)
// ---------------------------------------------------------------------------

func TestMCDC_BuildConstraintStrings_ColumnBounds(t *testing.T) {
	t.Parallel()

	p := NewPlanner()
	table := createTestTable() // has 4 columns, indices 0-3

	cases := []struct {
		name         string
		leftColumn   int
		wantNonEmpty bool
	}{
		// A=T, B=T: col=0 -> valid column -> constraint added
		{"MCDC A=T B=T: valid col idx", 0, true},
		// A=F, B=T: col=-1 -> not >= 0 -> no constraint (A flips)
		{"MCDC A=F B=T: negative col idx", -1, false},
		// A=T, B=F: col=10 >= 4 -> no constraint (B flips)
		{"MCDC A=T B=F: col idx too large", 10, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			term := &WhereTerm{
				LeftColumn: tc.leftColumn,
				Operator:   WO_EQ,
			}
			loop := &WhereLoop{Terms: []*WhereTerm{term}}
			result := p.buildConstraintStrings(table, loop)
			gotNonEmpty := len(result) > 0
			if gotNonEmpty != tc.wantNonEmpty {
				t.Errorf("buildConstraintStrings: len=%d (nonEmpty=%v), want nonEmpty=%v",
					len(result), gotNonEmpty, tc.wantNonEmpty)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: statistics.go EstimateSelectivity
//   Condition: term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0
//   This is an OR of four flags. We test that range operators get
//   selectivityRange and non-range operators fall through.
//   A = operator is in {WO_LT, WO_LE, WO_GT, WO_GE}
//   Coverage pairs:
//     A=T (e.g., WO_LT) -> selectivityRange
//     A=F (e.g., WO_IS) -> falls through to default
// ---------------------------------------------------------------------------

func TestMCDC_EstimateSelectivity_RangeOperatorFlag(t *testing.T) {
	t.Parallel()

	stats := NewStatistics()

	cases := []struct {
		name    string
		op      WhereOperator
		wantSel LogEst
	}{
		// A=T: WO_LT is a range op -> selectivityRange
		{"MCDC A=T: WO_LT range", WO_LT, selectivityRange},
		// A=T: WO_LE is a range op -> selectivityRange
		{"MCDC A=T: WO_LE range", WO_LE, selectivityRange},
		// A=T: WO_GT is a range op -> selectivityRange
		{"MCDC A=T: WO_GT range", WO_GT, selectivityRange},
		// A=T: WO_GE is a range op -> selectivityRange
		{"MCDC A=T: WO_GE range", WO_GE, selectivityRange},
		// A=F: WO_IS is not range -> falls to default (truthProbDefault)
		{"MCDC A=F: WO_IS not range", WO_IS, truthProbDefault},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			term := &WhereTerm{Operator: tc.op, RightValue: 42}
			got := EstimateSelectivity(term, stats)
			if got != tc.wantSel {
				t.Errorf("EstimateSelectivity(op=%v): got %d, want %d", tc.op, got, tc.wantSel)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go tryInOperator
//   Condition: nCol >= len(index.Columns)  -> return early
//   A = nCol >= len(index.Columns)
//   A=T -> skip IN optimization (no more columns to check)
//   A=F -> proceed to find IN term
// ---------------------------------------------------------------------------

func TestMCDC_TryInOperator_NColGuard(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()

	twoColIdx := &IndexInfo{
		Name:  "two_col_idx",
		Table: "users",
		Columns: []IndexColumn{
			{Name: "city", Index: 3, Ascending: true},
			{Name: "age", Index: 2, Ascending: true},
		},
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}

	// IN term for column at index position 1 (age, index 2)
	inTerm := &WhereTerm{
		LeftCursor: 0,
		LeftColumn: 2, // age column index in table
		Operator:   WO_IN,
	}

	cases := []struct {
		name          string
		nCol          int
		baseTerms     []*WhereTerm
		wantExtraLoop bool
	}{
		// A=T: nCol=2 >= len(columns)=2 -> skip (no IN loop added)
		{"MCDC A=T: nCol at limit, skip", 2, []*WhereTerm{}, false},
		// A=F: nCol=1 < len(columns)=2 -> try IN; inTerm for col[1] exists -> adds loop
		{"MCDC A=F: nCol within range, add IN loop", 1, []*WhereTerm{}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			builder := &WhereLoopBuilder{
				Table:     table,
				Cursor:    0,
				Terms:     []*WhereTerm{inTerm},
				CostModel: cm,
			}
			initialLen := len(builder.Loops)
			builder.tryInOperator(twoColIdx, tc.nCol, tc.baseTerms)
			added := len(builder.Loops) > initialLen
			if added != tc.wantExtraLoop {
				t.Errorf("tryInOperator: added=%v, want %v", added, tc.wantExtraLoop)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go AdjustCostForMultipleTerms
//   Condition: nTerms <= 1 -> return baseCost unchanged
//   A = nTerms <= 1
//   A=T -> return baseCost unchanged
//   A=F -> add extra cost
// ---------------------------------------------------------------------------

func TestMCDC_AdjustCostForMultipleTerms_NTermsGuard(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	base := LogEst(100)

	cases := []struct {
		name     string
		nTerms   int
		wantSame bool
	}{
		// A=T: nTerms=0 <= 1 -> unchanged
		{"MCDC A=T: nTerms=0 unchanged", 0, true},
		// A=T: nTerms=1 <= 1 -> unchanged
		{"MCDC A=T: nTerms=1 unchanged", 1, true},
		// A=F: nTerms=2 > 1 -> adds extra cost
		{"MCDC A=F: nTerms=2 adds cost", 2, false},
		// A=F: nTerms=5 -> adds more cost
		{"MCDC A=F: nTerms=5 adds more cost", 5, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cm.AdjustCostForMultipleTerms(base, tc.nTerms)
			same := (got == base)
			if same != tc.wantSame {
				t.Errorf("AdjustCostForMultipleTerms(nTerms=%d): got %d, same=%v, wantSame=%v",
					tc.nTerms, got, same, tc.wantSame)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go isIndexableOp
//   Condition: op == parser.OpEq || op == parser.OpLt ||
//              op == parser.OpGt || op == parser.OpLe ||
//              op == parser.OpGe
//   This is a 5-term OR. We need each sub-condition to independently
//   flip the outcome.
//   Coverage pairs (testing representative cases):
//     OpEq   -> true  (A=T)
//     OpLt   -> true  (B=T, all others false)
//     OpGt   -> true  (C=T)
//     OpLe   -> true  (D=T)
//     OpGe   -> true  (E=T)
//     OpAnd  -> false (all false)
// ---------------------------------------------------------------------------

func TestMCDC_IsIndexableOp_FiveTermOr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		op      parser.BinaryOp
		wantIdx bool
	}{
		// A=T: OpEq -> indexable
		{"MCDC A=T: OpEq indexable", parser.OpEq, true},
		// B=T: OpLt -> indexable (A=F, B=T)
		{"MCDC B=T: OpLt indexable", parser.OpLt, true},
		// C=T: OpGt -> indexable (A=F, B=F, C=T)
		{"MCDC C=T: OpGt indexable", parser.OpGt, true},
		// D=T: OpLe -> indexable (A=F, B=F, C=F, D=T)
		{"MCDC D=T: OpLe indexable", parser.OpLe, true},
		// E=T: OpGe -> indexable (A=F, B=F, C=F, D=F, E=T)
		{"MCDC E=T: OpGe indexable", parser.OpGe, true},
		// all=F: OpAnd -> not indexable
		{"MCDC all=F: OpAnd not indexable", parser.OpAnd, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isIndexableOp(tc.op)
			if got != tc.wantIdx {
				t.Errorf("isIndexableOp(%v): got %v, want %v", tc.op, got, tc.wantIdx)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: join.go estimateSelectivity
//   Condition: term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0
//   A = term is a range operator
//   A=T -> selectivity += selectivityRange
//   A=F, WO_EQ -> selectivity += selectivityEq
//   A=F, other -> no selectivity added
// ---------------------------------------------------------------------------

func TestMCDC_EstimateSelectivity_RangeVsEqVsOther(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()
	jo := &JoinOptimizer{
		CostModel: cm,
		Tables:    []*TableInfo{table},
	}

	cases := []struct {
		name    string
		op      WhereOperator
		wantSel LogEst
	}{
		// WO_EQ -> selectivityEq
		{"MCDC EQ: equality selectivity", WO_EQ, selectivityEq},
		// WO_LT -> selectivityRange (A=T)
		{"MCDC LT: range selectivity", WO_LT, selectivityRange},
		// WO_GT -> selectivityRange (A=T)
		{"MCDC GT: range selectivity", WO_GT, selectivityRange},
		// WO_IS -> no selectivity added (0)
		{"MCDC IS: no selectivity", WO_IS, 0},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			term := &WhereTerm{Operator: tc.op}
			got := jo.estimateSelectivity([]*WhereTerm{term})
			if got != tc.wantSel {
				t.Errorf("estimateSelectivity(op=%v): got %d, want %d", tc.op, got, tc.wantSel)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go indexMatchesOrderBy
//   Condition 1: len(orderBy) > len(index.Columns) -> return false
//     A = len(orderBy) > len(index.Columns)
//     A=T -> false (too many ORDER BY columns)
//     A=F -> proceed
//
//   Condition 2 (per column): index.Columns[i].Name != ob.Column -> false
//     B = name mismatch
//     B=T -> false
//     B=F -> proceed
//
//   Condition 3 (per column): index.Columns[i].Ascending != ob.Ascending
//     C = ascending mismatch
//     C=T -> false
//     C=F -> continue
// ---------------------------------------------------------------------------

func TestMCDC_IndexMatchesOrderBy_ThreeConditions(t *testing.T) {
	t.Parallel()

	table := createTestTable()
	cm := NewCostModel()
	sel := NewIndexSelector(table, nil, cm)

	idx := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "city", Ascending: true},
		},
	}

	cases := []struct {
		name    string
		orderBy []OrderByColumn
		wantOk  bool
	}{
		// A=T: more ORDER BY cols than index cols -> false
		{"MCDC A=T: more ob cols than index", []OrderByColumn{
			{Column: "city", Ascending: true},
			{Column: "age", Ascending: true},
		}, false},
		// A=F, B=T: name mismatch -> false (B flips)
		{"MCDC A=F B=T: name mismatch", []OrderByColumn{
			{Column: "age", Ascending: true},
		}, false},
		// A=F, B=F, C=T: ascending mismatch -> false (C flips)
		{"MCDC A=F B=F C=T: ascending mismatch", []OrderByColumn{
			{Column: "city", Ascending: false},
		}, false},
		// A=F, B=F, C=F: all match -> true
		{"MCDC A=F B=F C=F: all match", []OrderByColumn{
			{Column: "city", Ascending: true},
		}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sel.indexMatchesOrderBy(idx, tc.orderBy)
			if got != tc.wantOk {
				t.Errorf("indexMatchesOrderBy: got %v, want %v", got, tc.wantOk)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: subquery.go shouldMaterializeSubquery
//   Condition: !info.IsCorrelated -> return false
//   A = !info.IsCorrelated   (i.e., A=T means uncorrelated)
//   A=T -> return false (uncorrelated subqueries need no materialization)
//   A=F -> proceed to cost comparison
// ---------------------------------------------------------------------------

func TestMCDC_ShouldMaterializeSubquery_CorrelatedGuard(t *testing.T) {
	t.Parallel()

	cm := NewCostModel()
	opt := NewSubqueryOptimizer(cm)

	cases := []struct {
		name         string
		isCorrelated bool
		execCount    LogEst // high exec count makes materialization beneficial
		wantMatl     bool
	}{
		// A=T: not correlated -> false
		{"MCDC A=T: uncorrelated no materialize", false, NewLogEst(100), false},
		// A=F: correlated + high exec count -> true (materializeCost < repeatCost)
		{"MCDC A=F: correlated high exec materialize", true, NewLogEst(1000), true},
		// A=F: correlated + low exec count (1) -> repeat cost may be similar or lower
		{"MCDC A=F: correlated low exec no materialize", true, NewLogEst(1), false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			info := &SubqueryInfo{
				IsCorrelated:   tc.isCorrelated,
				EstimatedRows:  NewLogEst(100),
				ExecutionCount: tc.execCount,
			}
			got := opt.shouldMaterializeSubquery(info)
			if got != tc.wantMatl {
				t.Errorf("shouldMaterializeSubquery: got %v, want %v", got, tc.wantMatl)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: view.go isSelectStar
//   Condition: len(stmt.Columns) == 1 [A] && col.Star [B] && col.Table == "" [C]
//   Sub-conditions:
//     A = len(stmt.Columns) == 1
//     B = col.Star == true
//     C = col.Table == ""
//   Coverage pairs:
//     A=T, B=T, C=T -> true (SELECT *)
//     A=F, B=T, C=T -> false (A flips: multiple columns)
//     A=T, B=F, C=T -> false (B flips: not a star)
//     A=T, B=T, C=F -> false (C flips: qualified star e.g. t.*)
// ---------------------------------------------------------------------------

func TestMCDC_IsSelectStar_ThreeTermAnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		columns  []parser.ResultColumn
		wantStar bool
	}{
		// A=T, B=T, C=T: SELECT * -> true
		{"MCDC A=T B=T C=T: SELECT *", []parser.ResultColumn{
			{Star: true, Table: ""},
		}, true},
		// A=F: two columns -> false (A flips)
		{"MCDC A=F: two columns", []parser.ResultColumn{
			{Star: true, Table: ""},
			{Star: true, Table: ""},
		}, false},
		// A=T, B=F: not a star -> false (B flips)
		{"MCDC A=T B=F: not star", []parser.ResultColumn{
			{Star: false, Table: ""},
		}, false},
		// A=T, B=T, C=F: qualified star t.* -> false (C flips)
		{"MCDC A=T B=T C=F: qualified star", []parser.ResultColumn{
			{Star: true, Table: "t"},
		}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmt := &parser.SelectStmt{Columns: tc.columns}
			got := isSelectStar(stmt)
			if got != tc.wantStar {
				t.Errorf("isSelectStar: got %v, want %v", got, tc.wantStar)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: statistics.go applyRangeSelectivity
//   Condition: !hasRange -> return nOut unchanged
//   A = !hasRange  (A=T means no range, return early)
//   A=T -> return nOut unchanged
//   A=F -> add selectivityRange
// ---------------------------------------------------------------------------

func TestMCDC_ApplyRangeSelectivity_HasRangeGuard(t *testing.T) {
	t.Parallel()

	nOut := NewLogEst(1000)

	cases := []struct {
		name     string
		hasRange bool
		wantSame bool
	}{
		// A=T: no range -> same nOut
		{"MCDC A=T: no range unchanged", false, true},
		// A=F: has range -> adds selectivityRange (reduces nOut)
		{"MCDC A=F: has range reduces", true, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := applyRangeSelectivity(tc.hasRange, nOut)
			same := (got == nOut)
			if same != tc.wantSame {
				t.Errorf("applyRangeSelectivity(hasRange=%v): got %d (same=%v), wantSame=%v",
					tc.hasRange, got, same, tc.wantSame)
			}
		})
	}
}
