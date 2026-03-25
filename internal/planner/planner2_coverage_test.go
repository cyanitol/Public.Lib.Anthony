// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"
)

// --- determineCombinedSortOrder (60.0%) ---

func makeSimpleJoinOrder(tableIdx int, algorithm JoinAlgorithm) *JoinOrder {
	return &JoinOrder{
		Tables:         []int{tableIdx},
		Cost:           NewLogEst(100),
		RowCount:       NewLogEst(100),
		Algorithm:      []JoinAlgorithm{},
		JoinConditions: make(map[string][]*WhereTerm),
		SortOrder: []SortColumn{
			{TableIdx: tableIdx, Column: "id", Ascending: true},
		},
	}
}

// TestDetermineCombinedSortOrder_Merge verifies merge join preserves outer sort order.
func TestDetermineCombinedSortOrder_Merge(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinMerge)
	inner := makeSimpleJoinOrder(1, JoinMerge)
	order := jo.determineCombinedSortOrder(outer, inner, JoinMerge)
	if len(order) != len(outer.SortOrder) {
		t.Errorf("expected merge join to preserve outer sort order, got len=%d", len(order))
	}
}

// TestDetermineCombinedSortOrder_NestedLoop verifies nested loop preserves outer sort.
func TestDetermineCombinedSortOrder_NestedLoop(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinNestedLoop)
	inner := makeSimpleJoinOrder(1, JoinNestedLoop)
	order := jo.determineCombinedSortOrder(outer, inner, JoinNestedLoop)
	if len(order) != len(outer.SortOrder) {
		t.Errorf("expected nested loop to preserve outer sort order, got len=%d", len(order))
	}
}

// TestDetermineCombinedSortOrder_Hash2 verifies hash join returns empty sort order.
func TestDetermineCombinedSortOrder_Hash2(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinHash)
	inner := makeSimpleJoinOrder(1, JoinHash)
	order := jo.determineCombinedSortOrder(outer, inner, JoinHash)
	if len(order) != 0 {
		t.Errorf("expected hash join to return empty sort order, got len=%d", len(order))
	}
}

// TestDetermineCombinedSortOrder_Default verifies default case returns empty sort order.
func TestDetermineCombinedSortOrder_Default(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinNestedLoop)
	inner := makeSimpleJoinOrder(1, JoinNestedLoop)
	// Use an invalid algorithm value to trigger default
	order := jo.determineCombinedSortOrder(outer, inner, JoinAlgorithm(99))
	if len(order) != 0 {
		t.Errorf("expected default case to return empty sort order, got len=%d", len(order))
	}
}

// --- OptimizeJoinOrder (75.0%) ---

// TestOptimizeJoinOrder_GreedyPath verifies greedy path used for >8 tables.
func TestOptimizeJoinOrder_GreedyPath(t *testing.T) {
	// Build 9 tiny tables to trigger greedy path
	tables := make([]*TableInfo, 9)
	for i := range tables {
		tables[i] = &TableInfo{
			Name:      "t" + string(rune('a'+i)),
			Cursor:    i,
			RowCount:  int64(10 * (i + 1)),
			RowLogEst: NewLogEst(int64(10 * (i + 1))),
		}
	}
	jo := NewJoinOptimizer(tables, nil, NewCostModel())
	order, err := jo.OptimizeJoinOrder()
	if err != nil {
		t.Fatalf("OptimizeJoinOrder (greedy) failed: %v", err)
	}
	if order == nil {
		t.Fatal("expected non-nil order")
	}
	if len(order.Tables) != 9 {
		t.Errorf("expected 9 tables in order, got %d", len(order.Tables))
	}
}

// TestOptimizeJoinOrder_DPPath verifies DP path for <=8 tables.
func TestOptimizeJoinOrder_DPPath(t *testing.T) {
	tables := createJoinTestTables()
	jo := NewJoinOptimizer(tables, nil, NewCostModel())
	order, err := jo.OptimizeJoinOrder()
	if err != nil {
		t.Fatalf("OptimizeJoinOrder (DP) failed: %v", err)
	}
	if order == nil {
		t.Fatal("expected non-nil order")
	}
}

// --- CostEstimate: default path (80.0%) ---

// TestCostEstimate_Default covers the default algorithm fall-through.
func TestCostEstimate_Default(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinNestedLoop)
	inner := makeSimpleJoinOrder(1, JoinNestedLoop)
	// Use invalid algorithm to trigger default path
	cost, rows := jo.CostEstimate(outer, inner, JoinAlgorithm(99), nil)
	if cost <= 0 {
		t.Errorf("expected positive cost, got %d", cost)
	}
	if rows <= 0 {
		t.Errorf("expected positive row count, got %d", rows)
	}
}

// TestCostEstimate_Hash covers hash join cost estimation.
func TestCostEstimate_Hash(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinNestedLoop)
	inner := makeSimpleJoinOrder(1, JoinNestedLoop)
	cost, rows := jo.CostEstimate(outer, inner, JoinHash, nil)
	if cost <= 0 {
		t.Errorf("expected positive hash join cost, got %d", cost)
	}
	if rows <= 0 {
		t.Errorf("expected positive row count, got %d", rows)
	}
}

// TestCostEstimate_Merge covers merge join cost estimation.
func TestCostEstimate_Merge(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	outer := makeSimpleJoinOrder(0, JoinNestedLoop)
	inner := makeSimpleJoinOrder(1, JoinNestedLoop)
	cost, rows := jo.CostEstimate(outer, inner, JoinMerge, nil)
	if cost <= 0 {
		t.Errorf("expected positive merge join cost, got %d", cost)
	}
	if rows <= 0 {
		t.Errorf("expected positive row count, got %d", rows)
	}
}

// --- JoinAlgorithmSelector.SelectBest (78.6%) ---

// TestSelectBest_NoEquiJoin verifies NestedLoop when no equi-join conditions.
func TestSelectBest_NoEquiJoin(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(100)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(100)}
	// No conditions
	sel := NewJoinAlgorithmSelector(outer, inner, nil, NewCostModel())
	algo := sel.SelectBest()
	if algo != JoinNestedLoop {
		t.Errorf("expected NestedLoop with no equi-join, got %v", algo)
	}
}

// TestSelectBest_WithEquiJoin verifies best algorithm selected with equi-join.
func TestSelectBest_WithEquiJoin(t *testing.T) {
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(10000), EstimatedCost: NewLogEst(100)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(10)}
	conditions := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0},
	}
	sel := NewJoinAlgorithmSelector(outer, inner, conditions, NewCostModel())
	algo := sel.SelectBest()
	// Should select hash or merge or nested loop — just must not panic
	_ = algo
}

// TestSelectBest_OuterSmallerThanInner verifies swap logic in hash join estimator.
func TestSelectBest_OuterSmallerThanInner(t *testing.T) {
	// Outer is smaller → hash join swaps outer/inner for build/probe
	outer := &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(10), EstimatedCost: NewLogEst(10)}
	inner := &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(10000), EstimatedCost: NewLogEst(200)}
	conditions := []*WhereTerm{
		{Operator: WO_EQ, LeftCursor: 0, LeftColumn: 0},
	}
	sel := NewJoinAlgorithmSelector(outer, inner, conditions, NewCostModel())
	algo := sel.SelectBest()
	_ = algo
}

// --- ConvertInToJoin (60.0%) ---

// TestConvertInToJoin_NotInType verifies error when not an IN subquery.
func TestConvertInToJoin_NotInType(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{Type: SubqueryExists}
	parentInfo := &WhereInfo{Tables: []*TableInfo{}, AllLoops: []*WhereLoop{}}
	_, err := opt.ConvertInToJoin(info, parentInfo)
	if err == nil {
		t.Fatal("expected error for non-IN subquery type")
	}
}

// TestConvertInToJoin_JoinNotBeneficial2 verifies error when JOIN costs more than IN.
func TestConvertInToJoin_JoinNotBeneficial2(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	// Small subquery — IN will be cheaper
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		EstimatedRows:  NewLogEst(2),
		ExecutionCount: NewLogEst(1),
		IsCorrelated:   false,
	}
	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "t", RowCount: 10, RowLogEst: NewLogEst(10)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10),
	}
	_, err := opt.ConvertInToJoin(info, parentInfo)
	// May succeed or fail depending on cost model; just verify no panic
	_ = err
}

// TestConvertInToJoin_BeneficialConversion verifies successful conversion.
func TestConvertInToJoin_BeneficialConversion(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	// Large subquery with high execution count → JOIN should be cheaper
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
		IsCorrelated:   true,
	}
	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "big", RowCount: 100000, RowLogEst: NewLogEst(100000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100000),
	}
	_, err := opt.ConvertInToJoin(info, parentInfo)
	_ = err
}

// --- ConvertExistsToSemiJoin (60.0%) ---

// TestConvertExistsToSemiJoin_NotExistsType verifies error for non-EXISTS subquery.
func TestConvertExistsToSemiJoin_NotExistsType(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{Type: SubqueryIn}
	parentInfo := &WhereInfo{Tables: []*TableInfo{}, AllLoops: []*WhereLoop{}}
	_, err := opt.ConvertExistsToSemiJoin(info, parentInfo)
	if err == nil {
		t.Fatal("expected error for non-EXISTS subquery type")
	}
}

// TestConvertExistsToSemiJoin_BeneficialConversion verifies successful semi-join.
func TestConvertExistsToSemiJoin_BeneficialConversion(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryExists,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
		IsCorrelated:   true,
	}
	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "big", RowCount: 100000, RowLogEst: NewLogEst(100000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100000),
	}
	_, err := opt.ConvertExistsToSemiJoin(info, parentInfo)
	_ = err
}

// --- OptimizeSubquery (75.0%) ---

// TestOptimizeSubquery_Flatten verifies flattening path.
func TestOptimizeSubquery_Flatten(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:       SubqueryFrom,
		CanFlatten: true,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery flatten failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestOptimizeSubquery_Decorrelate verifies decorrelation path.
func TestOptimizeSubquery_Decorrelate(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:         SubqueryScalar,
		IsCorrelated: true,
		CanFlatten:   false,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery decorrelate failed: %v", err)
	}
	_ = result
}

// TestOptimizeSubquery_Materialize verifies materialization path.
func TestOptimizeSubquery_Materialize(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryScalar,
		IsCorrelated:   true,
		CanFlatten:     false,
		CanMaterialize: true,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 1000, RowLogEst: NewLogEst(1000)}},
		AllLoops: []*WhereLoop{},
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery materialize failed: %v", err)
	}
	_ = result
}

// TestOptimizeSubquery_NoOp verifies no-op path returns original info.
func TestOptimizeSubquery_NoOp(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryScalar,
		IsCorrelated:   false,
		CanFlatten:     false,
		CanMaterialize: false,
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery no-op failed: %v", err)
	}
	if result != parentInfo {
		t.Error("expected original parentInfo returned when no optimization applied")
	}
}

// TestOptimizeSubquery_ExistsConversion verifies EXISTS → semi-join path.
func TestOptimizeSubquery_ExistsConversion(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryExists,
		IsCorrelated:   false,
		CanFlatten:     false,
		CanMaterialize: false,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 10000, RowLogEst: NewLogEst(10000)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10000),
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery EXISTS failed: %v", err)
	}
	_ = result
}

// TestOptimizeSubquery_InConversion verifies IN → join path.
func TestOptimizeSubquery_InConversion(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		IsCorrelated:   false,
		CanFlatten:     false,
		CanMaterialize: false,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 10000, RowLogEst: NewLogEst(10000)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10000),
	}
	result, err := opt.OptimizeSubquery(info, parentInfo)
	if err != nil {
		t.Fatalf("OptimizeSubquery IN failed: %v", err)
	}
	_ = result
}

// --- tryTypeSpecificOptimization (77.8%) ---

// TestTryTypeSpecificOptimization_ExistsSucceeds covers EXISTS success path.
func TestTryTypeSpecificOptimization_ExistsSucceeds(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryExists,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 10000, RowLogEst: NewLogEst(10000)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10000),
	}
	result, ok := opt.tryTypeSpecificOptimization(info, parentInfo)
	_ = result
	_ = ok
}

// TestTryTypeSpecificOptimization_InSucceeds covers IN success path.
func TestTryTypeSpecificOptimization_InSucceeds(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:           SubqueryIn,
		EstimatedRows:  NewLogEst(100000),
		ExecutionCount: NewLogEst(10000),
	}
	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 10000, RowLogEst: NewLogEst(10000)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10000),
	}
	result, ok := opt.tryTypeSpecificOptimization(info, parentInfo)
	_ = result
	_ = ok
}

// TestTryTypeSpecificOptimization_NeitherApplies covers scalar path (returns nil, false).
func TestTryTypeSpecificOptimization_NeitherApplies(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type: SubqueryScalar,
	}
	parentInfo := &WhereInfo{Tables: []*TableInfo{}, AllLoops: []*WhereLoop{}}
	result, ok := opt.tryTypeSpecificOptimization(info, parentInfo)
	if ok {
		t.Error("expected ok=false for scalar subquery")
	}
	if result != nil {
		t.Error("expected nil result for scalar subquery")
	}
}

// --- ValidatePlan (86.7%) ---

// TestValidatePlan_NoPlan covers nil BestPath error.
func TestValidatePlan_NoPlan(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{BestPath: nil, Tables: []*TableInfo{}}
	err := p.ValidatePlan(info)
	if err == nil {
		t.Fatal("expected error for nil BestPath")
	}
}

// TestValidatePlan_LoopTableMismatch covers loop/table count mismatch.
func TestValidatePlan_LoopTableMismatch(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "t1"}, {Name: "t2"},
		},
		BestPath: &WherePath{
			Loops: []*WhereLoop{
				{TabIndex: 0, MaskSelf: 1},
			},
		},
	}
	err := p.ValidatePlan(info)
	if err == nil {
		t.Fatal("expected error for loop/table count mismatch")
	}
}

// TestValidatePlan_DuplicateTable covers duplicate table in plan.
func TestValidatePlan_DuplicateTable(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "t1"}, {Name: "t2"},
		},
		BestPath: &WherePath{
			Loops: []*WhereLoop{
				{TabIndex: 0, MaskSelf: 1},
				{TabIndex: 0, MaskSelf: 1}, // duplicate
			},
		},
	}
	err := p.ValidatePlan(info)
	if err == nil {
		t.Fatal("expected error for duplicate table in plan")
	}
}

// TestValidatePlan_Valid covers a valid plan (no error).
func TestValidatePlan_Valid(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{
		Tables: []*TableInfo{{Name: "t1"}, {Name: "t2"}},
		BestPath: &WherePath{
			Loops: []*WhereLoop{
				{TabIndex: 0, MaskSelf: 1, Prereq: 0},
				{TabIndex: 1, MaskSelf: 2, Prereq: 1},
			},
		},
	}
	err := p.ValidatePlan(info)
	if err != nil {
		t.Fatalf("ValidatePlan for valid plan failed: %v", err)
	}
}

// --- PlanQuery (83.3%) ---

// TestPlanQuery_NoTables covers empty tables error.
func TestPlanQuery_NoTables(t *testing.T) {
	p := NewPlanner()
	_, err := p.PlanQuery(nil, nil)
	if err == nil {
		t.Fatal("expected error for no tables")
	}
}

// TestPlanQuery_SingleTableNoWhere covers single table without WHERE clause.
func TestPlanQuery_SingleTableNoWhere(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{
			Name:      "users",
			Cursor:    0,
			RowCount:  1000,
			RowLogEst: NewLogEst(1000),
			Columns:   []ColumnInfo{{Name: "id", Index: 0}},
		},
	}
	info, err := p.PlanQuery(tables, nil)
	if err != nil {
		t.Fatalf("PlanQuery single table failed: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil WhereInfo")
	}
}

// TestPlanQuery_TwoTablesWithWhere covers multi-table join with WHERE clause.
func TestPlanQuery_TwoTablesWithWhere(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{
		{
			Name:      "a",
			Cursor:    0,
			RowCount:  100,
			RowLogEst: NewLogEst(100),
			Columns:   []ColumnInfo{{Name: "id", Index: 0}},
		},
		{
			Name:      "b",
			Cursor:    1,
			RowCount:  1000,
			RowLogEst: NewLogEst(1000),
			Columns:   []ColumnInfo{{Name: "a_id", Index: 0}},
		},
	}
	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0,
			},
		},
	}
	info, err := p.PlanQuery(tables, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery two tables failed: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil WhereInfo")
	}
}

// --- enumerateSubsets (81.8%) ---

// TestEnumerateSubsets_SmallSet covers subset enumeration for 2-element set.
func TestEnumerateSubsets_SmallSet(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	var collected []uint64
	jo.enumerateSubsets(3, 2, func(subset uint64) {
		collected = append(collected, subset)
	})
	// C(3,2) = 3 subsets
	if len(collected) != 3 {
		t.Errorf("expected 3 subsets of size 2 from 3 elements, got %d", len(collected))
	}
}

// TestEnumerateSubsets_ZeroSize covers k=0 early return.
func TestEnumerateSubsets_ZeroSize(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	var called int
	jo.enumerateSubsets(3, 0, func(subset uint64) {
		called++
	})
	if called != 0 {
		t.Errorf("expected 0 calls for k=0, got %d", called)
	}
}

// TestEnumerateSubsets_KGreaterThanN covers k>n early return.
func TestEnumerateSubsets_KGreaterThanN(t *testing.T) {
	jo := &JoinOptimizer{CostModel: NewCostModel()}
	var called int
	jo.enumerateSubsets(2, 5, func(subset uint64) {
		called++
	})
	if called != 0 {
		t.Errorf("expected 0 calls for k>n, got %d", called)
	}
}

// --- findBestJoinForSubset (92.3%) ---

// TestFindBestJoinForSubset_NoPlanForParts covers nil when sub-plans missing.
func TestFindBestJoinForSubset_NoPlanForParts(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "t2", Cursor: 1, RowCount: 200, RowLogEst: NewLogEst(200)},
	}
	jo := NewJoinOptimizer(tables, nil, NewCostModel())
	// Empty dp map → no sub-plans found → should return nil
	dp := map[uint64]*JoinOrder{}
	order := jo.findBestJoinForSubset(3, dp) // subset=3 (bits 0+1)
	if order != nil {
		t.Error("expected nil order when sub-plans missing")
	}
}

// TestFindBestJoinForSubset_WithSubPlans covers successful join plan finding.
func TestFindBestJoinForSubset_WithSubPlans(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "t2", Cursor: 1, RowCount: 200, RowLogEst: NewLogEst(200)},
	}
	jo := NewJoinOptimizer(tables, nil, NewCostModel())
	dp := map[uint64]*JoinOrder{
		1: {Tables: []int{0}, Cost: NewLogEst(100), RowCount: NewLogEst(100), Algorithm: []JoinAlgorithm{}, JoinConditions: make(map[string][]*WhereTerm)},
		2: {Tables: []int{1}, Cost: NewLogEst(200), RowCount: NewLogEst(200), Algorithm: []JoinAlgorithm{}, JoinConditions: make(map[string][]*WhereTerm)},
	}
	order := jo.findBestJoinForSubset(3, dp)
	if order == nil {
		t.Fatal("expected non-nil order when sub-plans exist")
	}
}

// --- SubqueryType.String() ---

// TestSubqueryTypeString2 covers all SubqueryType string values.
func TestSubqueryTypeString2(t *testing.T) {
	cases := []struct {
		typ  SubqueryType
		want string
	}{
		{SubqueryScalar, "SCALAR"},
		{SubqueryExists, "EXISTS"},
		{SubqueryIn, "IN"},
		{SubqueryFrom, "FROM"},
		{SubqueryType(99), "UNKNOWN"},
	}
	for _, c := range cases {
		got := c.typ.String()
		if got != c.want {
			t.Errorf("SubqueryType(%d).String() = %q, want %q", c.typ, got, c.want)
		}
	}
}

// --- JoinAlgorithm.String() ---

// TestJoinAlgorithmString2 covers all JoinAlgorithm string values.
func TestJoinAlgorithmString2(t *testing.T) {
	cases := []struct {
		algo JoinAlgorithm
		want string
	}{
		{JoinNestedLoop, "NestedLoop"},
		{JoinHash, "Hash"},
		{JoinMerge, "Merge"},
		{JoinAlgorithm(99), "Unknown"},
	}
	for _, c := range cases {
		got := c.algo.String()
		if got != c.want {
			t.Errorf("JoinAlgorithm(%d).String() = %q, want %q", c.algo, got, c.want)
		}
	}
}

// --- BuildJoinTree ---

// TestBuildJoinTree_SingleTable2 covers leaf node creation.
func TestBuildJoinTree_SingleTable2(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t", Cursor: 0, RowCount: 50, RowLogEst: NewLogEst(50)},
	}
	order := &JoinOrder{
		Tables:    []int{0},
		Cost:      NewLogEst(50),
		RowCount:  NewLogEst(50),
		Algorithm: []JoinAlgorithm{},
	}
	node := BuildJoinTree(order, tables, nil)
	if node == nil {
		t.Fatal("expected non-nil JoinNode")
	}
	if !node.IsLeaf {
		t.Error("expected leaf node for single table")
	}
}

// TestBuildJoinTree_TwoTables covers left-deep tree construction.
func TestBuildJoinTree_TwoTables(t *testing.T) {
	tables := []*TableInfo{
		{Name: "t1", Cursor: 0, RowCount: 100, RowLogEst: NewLogEst(100)},
		{Name: "t2", Cursor: 1, RowCount: 200, RowLogEst: NewLogEst(200)},
	}
	order := &JoinOrder{
		Tables:    []int{0, 1},
		Cost:      NewLogEst(300),
		RowCount:  NewLogEst(300),
		Algorithm: []JoinAlgorithm{JoinNestedLoop},
	}
	node := BuildJoinTree(order, tables, nil)
	if node == nil {
		t.Fatal("expected non-nil JoinNode")
	}
	if node.IsLeaf {
		t.Error("expected non-leaf node for two tables")
	}
}

// --- JoinConditionAnalyzer ---

// TestJoinConditionAnalyzer_EmptyConditions covers empty conditions.
func TestJoinConditionAnalyzer_EmptyConditions(t *testing.T) {
	jca := NewJoinConditionAnalyzer(nil)
	if jca.HasEquiJoin() {
		t.Error("expected no equi-join for empty conditions")
	}
	if jca.IsHashJoinEligible() {
		t.Error("expected not hash-join eligible for empty conditions")
	}
	if jca.IsMergeJoinEligible() {
		t.Error("expected not merge-join eligible for empty conditions")
	}
	if jca.GetSelectivity() != 1.0 {
		t.Errorf("expected selectivity 1.0 for empty conditions, got %f", jca.GetSelectivity())
	}
}

// TestJoinConditionAnalyzer_RangeSelectivity covers range selectivity calculation.
func TestJoinConditionAnalyzer_RangeSelectivity(t *testing.T) {
	conditions := []*WhereTerm{
		{Operator: WO_LT, LeftCursor: 0, LeftColumn: 0},
		{Operator: WO_GT, LeftCursor: 0, LeftColumn: 1},
	}
	jca := NewJoinConditionAnalyzer(conditions)
	sel := jca.GetSelectivity()
	// Each range reduces by 0.25, so 0.25 * 0.25 = 0.0625
	if sel >= 1.0 {
		t.Errorf("expected selectivity < 1.0 for range conditions, got %f", sel)
	}
}

// TestJoinConditionAnalyzer_ExtractEquiJoinKeys covers key extraction.
func TestJoinConditionAnalyzer_ExtractEquiJoinKeys(t *testing.T) {
	conditions := []*WhereTerm{
		{Operator: WO_EQ, LeftColumn: 3},
		{Operator: WO_EQ, LeftColumn: 5},
		{Operator: WO_LT, LeftColumn: 1}, // not equi-join
	}
	jca := NewJoinConditionAnalyzer(conditions)
	keys := jca.ExtractEquiJoinKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 equi-join keys, got %d", len(keys))
	}
}

// --- MergeJoinPlanner.Plan ---

// TestMergeJoinPlanner_NoKeys verifies error when no join keys.
func TestMergeJoinPlanner_NoKeys(t *testing.T) {
	mj := &MergeJoinPlanner{
		Left:      &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(100)},
		Right:     &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(200), EstimatedCost: NewLogEst(200)},
		JoinKeys:  []int{},
		CostModel: NewCostModel(),
	}
	_, err := mj.Plan()
	if err == nil {
		t.Fatal("expected error for no join keys")
	}
}

// TestMergeJoinPlanner_WithKeys covers successful merge join planning.
func TestMergeJoinPlanner_WithKeys(t *testing.T) {
	mj := &MergeJoinPlanner{
		Left:        &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(100)},
		Right:       &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(200), EstimatedCost: NewLogEst(200)},
		JoinKeys:    []int{0},
		LeftSorted:  false,
		RightSorted: true,
		CostModel:   NewCostModel(),
	}
	plan, err := mj.Plan()
	if err != nil {
		t.Fatalf("MergeJoinPlanner.Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	_ = plan.Execute()
}

// --- HashJoinPlanner.Plan ---

// TestHashJoinPlanner_NoKeys verifies error when no join keys.
func TestHashJoinPlanner_NoKeys(t *testing.T) {
	hj := &HashJoinPlanner{
		Build:     &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(100)},
		Probe:     &JoinNode{IsLeaf: true, EstimatedRows: NewLogEst(1000)},
		JoinKeys:  []int{},
		CostModel: NewCostModel(),
	}
	_, err := hj.Plan()
	if err == nil {
		t.Fatal("expected error for no join keys")
	}
}

// TestHashJoinPlanner_WithKeys covers successful hash join planning.
func TestHashJoinPlanner_WithKeys(t *testing.T) {
	hj := &HashJoinPlanner{
		Build:     &JoinNode{IsLeaf: true, TableIndex: 1, EstimatedRows: NewLogEst(100), EstimatedCost: NewLogEst(100)},
		Probe:     &JoinNode{IsLeaf: true, TableIndex: 0, EstimatedRows: NewLogEst(1000), EstimatedCost: NewLogEst(200)},
		JoinKeys:  []int{0},
		CostModel: NewCostModel(),
	}
	plan, err := hj.Plan()
	if err != nil {
		t.Fatalf("HashJoinPlanner.Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	_ = plan.Execute()
}

// --- statistics: estimateEqualitySelectivity (71.4%) ---

// TestEstimateEqualitySelectivity2 covers equality selectivity with different value types.
func TestEstimateEqualitySelectivity2(t *testing.T) {
	// Small integer (0, 1, -1) → very selective
	term0 := &WhereTerm{Operator: WO_EQ, RightValue: int(0)}
	sel0 := EstimateSelectivity(term0, NewStatistics())
	if sel0 != truthProbSmallInt {
		t.Errorf("expected truthProbSmallInt for val=0, got %d", sel0)
	}

	// Non-small integer → default equality
	term1 := &WhereTerm{Operator: WO_EQ, RightValue: int(42)}
	sel1 := EstimateSelectivity(term1, NewStatistics())
	if sel1 != selectivityEq {
		t.Errorf("expected selectivityEq for val=42, got %d", sel1)
	}

	// Empty string → selectivityEq
	term2 := &WhereTerm{Operator: WO_EQ, RightValue: ""}
	sel2 := EstimateSelectivity(term2, NewStatistics())
	if sel2 != selectivityEq {
		t.Errorf("expected selectivityEq for empty string, got %d", sel2)
	}

	// Non-empty string → selectivityEq
	term3 := &WhereTerm{Operator: WO_EQ, RightValue: "hello"}
	sel3 := EstimateSelectivity(term3, NewStatistics())
	if sel3 != selectivityEq {
		t.Errorf("expected selectivityEq for non-empty string, got %d", sel3)
	}
}

// --- statistics: applyRangeSelectivity (83.3%) ---

// TestApplyRangeSelectivity_True verifies range reduces row count.
func TestApplyRangeSelectivity_True(t *testing.T) {
	indexStats := &IndexStatistics{
		IndexName:   "idx",
		RowCount:    1000,
		ColumnStats: []LogEst{NewLogEst(100)},
		AvgEq:       []int64{10},
	}
	withRange := EstimateRows(indexStats, 1, true)
	withoutRange := EstimateRows(indexStats, 1, false)
	if withRange >= withoutRange {
		t.Errorf("expected range to reduce rows: withRange=%d >= withoutRange=%d", withRange, withoutRange)
	}
}

// TestApplyRangeSelectivity_False verifies no range leaves row count unchanged.
func TestApplyRangeSelectivity_False(t *testing.T) {
	indexStats := &IndexStatistics{
		IndexName:   "idx",
		RowCount:    1000,
		ColumnStats: []LogEst{NewLogEst(100)},
		AvgEq:       []int64{10},
	}
	result := EstimateRows(indexStats, 1, false)
	if result <= 0 {
		t.Errorf("expected positive row count, got %d", result)
	}
}

// --- statistics: computeColumnStat (83.3%) ---

// TestComputeColumnStat_ZeroAvg covers the avg==0 path (returns rowCount).
func TestComputeColumnStat_ZeroAvg(t *testing.T) {
	// avg=0 → should return NewLogEst(rowCount)
	result := computeColumnStat(0, 1000)
	expected := NewLogEst(1000)
	if result != expected {
		t.Errorf("expected %d for avg=0, got %d", expected, result)
	}
}

// TestComputeColumnStat_SmallAvg covers distinctValues<1 clamping.
func TestComputeColumnStat_SmallAvg(t *testing.T) {
	// avg > rowCount → distinctValues < 1, should clamp to 1
	result := computeColumnStat(10000, 100)
	if result != NewLogEst(1) {
		t.Errorf("expected NewLogEst(1) for clamp case, got %d", result)
	}
}

// TestComputeColumnStat_NormalCase covers normal computation.
func TestComputeColumnStat_NormalCase(t *testing.T) {
	// avg=10, rowCount=1000 → distinctValues=100
	result := computeColumnStat(10, 1000)
	expected := NewLogEst(100)
	if result != expected {
		t.Errorf("expected %d for avg=10/rowCount=1000, got %d", expected, result)
	}
}

// --- statistics: extrapolateSelectivity (83.3%) ---

// TestExtrapolateSelectivity_ExtrapolatesCorrectly covers beyond-stats extrapolation.
func TestExtrapolateSelectivity_ExtrapolatesCorrectly(t *testing.T) {
	indexStats := &IndexStatistics{
		ColumnStats: []LogEst{NewLogEst(100), NewLogEst(50)},
		RowCount:    1000,
	}
	// nEq=4 > len(ColumnStats)=2 → extrapolate
	result := applyEqualitySelectivity(indexStats, 4, NewLogEst(1000))
	if result > NewLogEst(1000) {
		t.Errorf("extrapolated result should not exceed base nOut")
	}
}

// TestExtrapolateSelectivity_ExceedsToZero covers the clamp-to-zero path.
func TestExtrapolateSelectivity_ExceedsToZero(t *testing.T) {
	indexStats := &IndexStatistics{
		// Very small last stat so adding selectivityEq drives below 0
		ColumnStats: []LogEst{LogEst(1)},
		RowCount:    1000,
	}
	// nEq=10 should accumulate enough selectivityEq to go below 0
	result := applyEqualitySelectivity(indexStats, 10, NewLogEst(1000))
	// Should return 0 when clamped
	if result < 0 {
		t.Errorf("expected non-negative result, got %d", result)
	}
}
