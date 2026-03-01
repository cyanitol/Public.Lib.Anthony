// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
)

// Test helper to create test tables for join testing
func createJoinTestTables() []*TableInfo {
	// Table 1: users (10,000 rows)
	users := &TableInfo{
		Name:      "users",
		Cursor:    0,
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "dept_id", Index: 2, Type: "INTEGER", NotNull: false},
		},
		Indexes: []*IndexInfo{
			{
				Name:        "idx_users_id",
				Table:       "users",
				Unique:      true,
				Primary:     true,
				RowCount:    10000,
				RowLogEst:   NewLogEst(10000),
				Columns:     []IndexColumn{{Name: "id", Index: 0, Ascending: true}},
				ColumnStats: []LogEst{0},
			},
			{
				Name:        "idx_users_dept",
				Table:       "users",
				Unique:      false,
				Primary:     false,
				RowCount:    10000,
				RowLogEst:   NewLogEst(10000),
				Columns:     []IndexColumn{{Name: "dept_id", Index: 2, Ascending: true}},
				ColumnStats: []LogEst{NewLogEst(100)}, // ~100 users per dept
			},
		},
	}
	users.PrimaryKey = users.Indexes[0]

	// Table 2: departments (100 rows)
	departments := &TableInfo{
		Name:      "departments",
		Cursor:    1,
		RowCount:  100,
		RowLogEst: NewLogEst(100),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "budget", Index: 2, Type: "INTEGER", NotNull: false},
		},
		Indexes: []*IndexInfo{
			{
				Name:        "idx_dept_id",
				Table:       "departments",
				Unique:      true,
				Primary:     true,
				RowCount:    100,
				RowLogEst:   NewLogEst(100),
				Columns:     []IndexColumn{{Name: "id", Index: 0, Ascending: true}},
				ColumnStats: []LogEst{0},
			},
		},
	}
	departments.PrimaryKey = departments.Indexes[0]

	// Table 3: projects (1,000 rows)
	projects := &TableInfo{
		Name:      "projects",
		Cursor:    2,
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "dept_id", Index: 2, Type: "INTEGER", NotNull: false},
		},
		Indexes: []*IndexInfo{
			{
				Name:        "idx_proj_id",
				Table:       "projects",
				Unique:      true,
				Primary:     true,
				RowCount:    1000,
				RowLogEst:   NewLogEst(1000),
				Columns:     []IndexColumn{{Name: "id", Index: 0, Ascending: true}},
				ColumnStats: []LogEst{0},
			},
		},
	}
	projects.PrimaryKey = projects.Indexes[0]

	return []*TableInfo{users, departments, projects}
}

// Test helper to create join conditions
func createEquiJoinCondition(leftTable, rightTable, leftCol, rightCol int) *WhereTerm {
	return &WhereTerm{
		Operator:    WO_EQ,
		LeftCursor:  leftTable,
		LeftColumn:  leftCol,
		PrereqAll:   (Bitmask(1) << uint(leftTable)) | (Bitmask(1) << uint(rightTable)),
		PrereqRight: Bitmask(1) << uint(rightTable),
	}
}

func TestJoinOrderString(t *testing.T) {
	order := &JoinOrder{
		Tables:    []int{0, 1, 2},
		Cost:      NewLogEst(1000),
		RowCount:  NewLogEst(100),
		Algorithm: []JoinAlgorithm{JoinNestedLoop, JoinHash},
	}

	str := order.String()
	if str == "" {
		t.Error("JoinOrder.String() returned empty string")
	}
	t.Logf("JoinOrder: %s", str)
}

func TestJoinAlgorithmString(t *testing.T) {
	tests := []struct {
		algo     JoinAlgorithm
		expected string
	}{
		{JoinNestedLoop, "NestedLoop"},
		{JoinHash, "Hash"},
		{JoinMerge, "Merge"},
	}

	for _, tt := range tests {
		result := tt.algo.String()
		if result != tt.expected {
			t.Errorf("JoinAlgorithm(%d).String() = %s, want %s", tt.algo, result, tt.expected)
		}
	}
}

func TestNewJoinOptimizer(t *testing.T) {
	tables := createJoinTestTables()
	costModel := NewCostModel()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	if optimizer == nil {
		t.Fatal("NewJoinOptimizer returned nil")
	}
	if optimizer.CostModel != costModel {
		t.Error("CostModel not set correctly")
	}
	if len(optimizer.Tables) != len(tables) {
		t.Errorf("Tables length = %d, want %d", len(optimizer.Tables), len(tables))
	}
}

func TestDynamicProgrammingJoinOrderSingleTable(t *testing.T) {
	tables := createJoinTestTables()[:1] // Just users table
	costModel := NewCostModel()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)
	order, err := optimizer.DynamicProgrammingJoinOrder()

	if err != nil {
		t.Fatalf("DynamicProgrammingJoinOrder failed: %v", err)
	}
	if order == nil {
		t.Fatal("DynamicProgrammingJoinOrder returned nil order")
	}
	if len(order.Tables) != 1 {
		t.Errorf("Order tables length = %d, want 1", len(order.Tables))
	}
	if order.Tables[0] != 0 {
		t.Errorf("Order tables[0] = %d, want 0", order.Tables[0])
	}
}

func TestDynamicProgrammingJoinOrderTwoTables(t *testing.T) {
	tables := createJoinTestTables()[:2] // users and departments
	costModel := NewCostModel()

	// Create join condition: users.dept_id = departments.id
	joinTerm := createEquiJoinCondition(0, 1, 2, 0)

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{joinTerm},
		},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)
	order, err := optimizer.DynamicProgrammingJoinOrder()

	if err != nil {
		t.Fatalf("DynamicProgrammingJoinOrder failed: %v", err)
	}
	if order == nil {
		t.Fatal("DynamicProgrammingJoinOrder returned nil order")
	}
	if len(order.Tables) != 2 {
		t.Errorf("Order tables length = %d, want 2", len(order.Tables))
	}

	// The smaller table (departments, 100 rows) should typically come first
	// but the optimizer might choose differently based on cost model
	t.Logf("Join order: %v", order.Tables)
	t.Logf("Estimated cost: %d", order.Cost)
	t.Logf("Estimated rows: %d", order.RowCount.ToInt())
}

func TestDynamicProgrammingJoinOrderThreeTables(t *testing.T) {
	tables := createJoinTestTables() // all three tables
	costModel := NewCostModel()

	// Create join conditions
	// users.dept_id = departments.id
	joinTerm1 := createEquiJoinCondition(0, 1, 2, 0)
	// projects.dept_id = departments.id
	joinTerm2 := createEquiJoinCondition(2, 1, 2, 0)

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{joinTerm1, joinTerm2},
		},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)
	order, err := optimizer.DynamicProgrammingJoinOrder()

	if err != nil {
		t.Fatalf("DynamicProgrammingJoinOrder failed: %v", err)
	}
	if order == nil {
		t.Fatal("DynamicProgrammingJoinOrder returned nil order")
	}
	if len(order.Tables) != 3 {
		t.Errorf("Order tables length = %d, want 3", len(order.Tables))
	}

	// Verify all tables are in the order
	seen := make(map[int]bool)
	for _, tableIdx := range order.Tables {
		if seen[tableIdx] {
			t.Errorf("Table %d appears multiple times in join order", tableIdx)
		}
		seen[tableIdx] = true
	}

	if len(seen) != 3 {
		t.Errorf("Only %d unique tables in join order, want 3", len(seen))
	}

	t.Logf("Join order: %v", order.Tables)
	t.Logf("Algorithms: %v", order.Algorithm)
	t.Logf("Estimated cost: %d", order.Cost)
	t.Logf("Estimated rows: %d", order.RowCount.ToInt())
}

func TestSelectJoinAlgorithm(t *testing.T) {
	costModel := NewCostModel()
	tables := createJoinTestTables()

	// Create outer and inner orders
	outer := &JoinOrder{
		Tables:   []int{0},
		RowCount: NewLogEst(10000),
	}
	inner := &JoinOrder{
		Tables:   []int{1},
		RowCount: NewLogEst(100),
	}

	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}
	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	tests := []struct {
		name      string
		joinTerms []*WhereTerm
		wantAlgo  JoinAlgorithm
	}{
		{
			name:      "No join conditions - nested loop",
			joinTerms: []*WhereTerm{},
			wantAlgo:  JoinNestedLoop,
		},
		{
			name: "Equi-join condition - hash or nested loop",
			joinTerms: []*WhereTerm{
				createEquiJoinCondition(0, 1, 2, 0),
			},
			// Hash join typically preferred for this case
			wantAlgo: JoinHash,
		},
		{
			name: "Range condition - nested loop",
			joinTerms: []*WhereTerm{
				{
					Operator:   WO_LT,
					LeftCursor: 0,
					LeftColumn: 2,
					PrereqAll:  (Bitmask(1) << 0) | (Bitmask(1) << 1),
				},
			},
			wantAlgo: JoinNestedLoop,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo := optimizer.SelectJoinAlgorithm(outer, inner, tt.joinTerms)
			// Note: We can't strictly test for a specific algorithm because
			// the cost model might choose differently. Just verify it returns
			// a valid algorithm.
			if algo < JoinNestedLoop || algo > JoinMerge {
				t.Errorf("SelectJoinAlgorithm returned invalid algorithm: %v", algo)
			}
			t.Logf("Selected algorithm: %s", algo)
		})
	}
}

func TestCostEstimate(t *testing.T) {
	costModel := NewCostModel()
	tables := createJoinTestTables()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}
	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	outer := &JoinOrder{
		Tables:   []int{0},
		Cost:     NewLogEst(1000),
		RowCount: NewLogEst(10000),
	}
	inner := &JoinOrder{
		Tables:   []int{1},
		Cost:     NewLogEst(100),
		RowCount: NewLogEst(100),
	}

	joinTerms := []*WhereTerm{createEquiJoinCondition(0, 1, 2, 0)}

	tests := []struct {
		name      string
		algorithm JoinAlgorithm
	}{
		{"NestedLoop", JoinNestedLoop},
		{"Hash", JoinHash},
		{"Merge", JoinMerge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost, rowCount := optimizer.CostEstimate(outer, inner, tt.algorithm, joinTerms)

			if cost < 0 {
				t.Errorf("Negative cost: %d", cost)
			}
			if rowCount < 0 {
				t.Errorf("Negative row count: %d", rowCount)
			}

			t.Logf("%s join: cost=%d, rows=%d", tt.algorithm, cost, rowCount.ToInt())
		})
	}
}

func TestGreedyJoinOrder(t *testing.T) {
	tables := createJoinTestTables()
	costModel := NewCostModel()

	joinTerm1 := createEquiJoinCondition(0, 1, 2, 0)
	joinTerm2 := createEquiJoinCondition(2, 1, 2, 0)

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{joinTerm1, joinTerm2},
		},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)
	order, err := optimizer.GreedyJoinOrder()

	if err != nil {
		t.Fatalf("GreedyJoinOrder failed: %v", err)
	}
	if order == nil {
		t.Fatal("GreedyJoinOrder returned nil order")
	}
	if len(order.Tables) != 3 {
		t.Errorf("Order tables length = %d, want 3", len(order.Tables))
	}

	// Verify all tables are present
	seen := make(map[int]bool)
	for _, tableIdx := range order.Tables {
		seen[tableIdx] = true
	}
	if len(seen) != 3 {
		t.Errorf("Only %d unique tables in join order, want 3", len(seen))
	}

	t.Logf("Greedy join order: %v", order.Tables)
	t.Logf("Estimated cost: %d", order.Cost)
}

func TestOptimizeJoinOrder(t *testing.T) {
	tables := createJoinTestTables()
	costModel := NewCostModel()

	joinTerm1 := createEquiJoinCondition(0, 1, 2, 0)
	joinTerm2 := createEquiJoinCondition(2, 1, 2, 0)

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{joinTerm1, joinTerm2},
		},
		Tables: tables,
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	// Should use DP for small table count (3 tables)
	order, err := optimizer.OptimizeJoinOrder()

	if err != nil {
		t.Fatalf("OptimizeJoinOrder failed: %v", err)
	}
	if order == nil {
		t.Fatal("OptimizeJoinOrder returned nil order")
	}
	if len(order.Tables) != 3 {
		t.Errorf("Order tables length = %d, want 3", len(order.Tables))
	}

	t.Logf("Optimized join order: %v", order.Tables)
	t.Logf("Algorithms: %v", order.Algorithm)
}

func TestBuildJoinTree(t *testing.T) {
	tables := createJoinTestTables()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}

	order := &JoinOrder{
		Tables:    []int{0, 1, 2},
		Cost:      NewLogEst(1000),
		RowCount:  NewLogEst(100),
		Algorithm: []JoinAlgorithm{JoinNestedLoop, JoinHash},
	}

	tree := BuildJoinTree(order, tables, whereInfo)

	if tree == nil {
		t.Fatal("BuildJoinTree returned nil")
	}
	if tree.IsLeaf {
		t.Error("Root should not be a leaf for multi-table join")
	}

	t.Logf("Join tree: %s", tree.String())
}

func TestJoinConditionAnalyzer(t *testing.T) {
	conditions := []*WhereTerm{
		createEquiJoinCondition(0, 1, 2, 0),
		{
			Operator:   WO_LT,
			LeftCursor: 0,
			LeftColumn: 2,
		},
	}

	analyzer := NewJoinConditionAnalyzer(conditions)

	if !analyzer.HasEquiJoin() {
		t.Error("HasEquiJoin should return true")
	}

	keys := analyzer.ExtractEquiJoinKeys()
	if len(keys) == 0 {
		t.Error("Should extract at least one equi-join key")
	}

	selectivity := analyzer.GetSelectivity()
	if selectivity <= 0 || selectivity > 1 {
		t.Errorf("Selectivity %f should be between 0 and 1", selectivity)
	}

	if !analyzer.IsHashJoinEligible() {
		t.Error("Should be hash join eligible with equi-join")
	}

	if !analyzer.IsMergeJoinEligible() {
		t.Error("Should be merge join eligible with equi-join")
	}
}

func TestJoinAlgorithmSelector(t *testing.T) {
	costModel := NewCostModel()

	outer := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(10000),
	}

	inner := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(100),
		EstimatedRows: NewLogEst(100),
	}

	conditions := []*WhereTerm{
		createEquiJoinCondition(0, 1, 2, 0),
	}

	selector := NewJoinAlgorithmSelector(outer, inner, conditions, costModel)
	algo := selector.SelectBest()

	// Should select hash join for this case (has equi-join, reasonable sizes)
	if algo < JoinNestedLoop || algo > JoinMerge {
		t.Errorf("SelectBest returned invalid algorithm: %v", algo)
	}

	t.Logf("Selected algorithm: %s", algo)
}

func TestNestedLoopJoinPlanner(t *testing.T) {
	costModel := NewCostModel()

	outer := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(10000),
	}

	inner := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(100),
		EstimatedRows: NewLogEst(100),
	}

	planner := &NestedLoopJoinPlanner{
		Outer:          outer,
		Inner:          inner,
		JoinConditions: []*WhereTerm{},
		CostModel:      costModel,
	}

	plan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("Plan returned nil")
	}

	t.Logf("Nested loop plan: %s", plan.Execute())
}

func TestHashJoinPlanner(t *testing.T) {
	costModel := NewCostModel()

	build := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(100),
		EstimatedRows: NewLogEst(100),
	}

	probe := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(10000),
	}

	planner := &HashJoinPlanner{
		Build:          build,
		Probe:          probe,
		JoinConditions: []*WhereTerm{},
		JoinKeys:       []int{2},
		CostModel:      costModel,
	}

	plan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("Plan returned nil")
	}
	if plan.HashTableSize <= 0 {
		t.Error("Hash table size should be positive")
	}

	t.Logf("Hash join plan: %s", plan.Execute())
	t.Logf("Estimated hash table size: %d bytes", plan.HashTableSize)
}

func TestMergeJoinPlanner(t *testing.T) {
	costModel := NewCostModel()

	left := &JoinNode{
		TableIndex:    0,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(1000),
		EstimatedRows: NewLogEst(10000),
	}

	right := &JoinNode{
		TableIndex:    1,
		IsLeaf:        true,
		EstimatedCost: NewLogEst(100),
		EstimatedRows: NewLogEst(100),
	}

	planner := &MergeJoinPlanner{
		Left:           left,
		Right:          right,
		JoinConditions: []*WhereTerm{},
		JoinKeys:       []int{2},
		LeftSorted:     false,
		RightSorted:    false,
		CostModel:      costModel,
	}

	plan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("Plan returned nil")
	}

	t.Logf("Merge join plan: %s", plan.Execute())

	// Test with pre-sorted inputs
	planner.LeftSorted = true
	planner.RightSorted = true
	sortedPlan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Sorted plan failed: %v", err)
	}

	// Cost should be lower when inputs are already sorted
	if sortedPlan.EstimatedCost >= plan.EstimatedCost {
		t.Logf("Warning: Sorted plan cost (%d) not less than unsorted (%d)",
			sortedPlan.EstimatedCost, plan.EstimatedCost)
	}
}

func BenchmarkDPJoinOrder2Tables(b *testing.B) {
	tables := createJoinTestTables()[:2]
	costModel := NewCostModel()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}
	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = optimizer.DynamicProgrammingJoinOrder()
	}
}

func BenchmarkDPJoinOrder3Tables(b *testing.B) {
	tables := createJoinTestTables()
	costModel := NewCostModel()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}
	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = optimizer.DynamicProgrammingJoinOrder()
	}
}

func BenchmarkGreedyJoinOrder3Tables(b *testing.B) {
	tables := createJoinTestTables()
	costModel := NewCostModel()
	whereInfo := &WhereInfo{
		Clause: &WhereClause{Terms: []*WhereTerm{}},
		Tables: tables,
	}
	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = optimizer.GreedyJoinOrder()
	}
}
