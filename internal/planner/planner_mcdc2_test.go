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
