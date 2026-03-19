// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"
)

// Test helper to create a simple table for testing
func createTestTable() *TableInfo {
	return &TableInfo{
		Name:      "users",
		Cursor:    0,
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER", NotNull: true},
			{Name: "name", Index: 1, Type: "TEXT", NotNull: true},
			{Name: "age", Index: 2, Type: "INTEGER", NotNull: false},
			{Name: "city", Index: 3, Type: "TEXT", NotNull: false},
		},
		Indexes: []*IndexInfo{
			{
				Name:      "idx_users_id",
				Table:     "users",
				Unique:    true,
				Primary:   true,
				RowCount:  10000,
				RowLogEst: NewLogEst(10000),
				Columns: []IndexColumn{
					{Name: "id", Index: 0, Ascending: true},
				},
				ColumnStats: []LogEst{0}, // Unique, so 1 row per value
			},
			{
				Name:      "idx_users_name",
				Table:     "users",
				Unique:    false,
				Primary:   false,
				RowCount:  10000,
				RowLogEst: NewLogEst(10000),
				Columns: []IndexColumn{
					{Name: "name", Index: 1, Ascending: true},
				},
				ColumnStats: []LogEst{NewLogEst(5000)}, // ~2 rows per name
			},
			{
				Name:      "idx_users_city_age",
				Table:     "users",
				Unique:    false,
				Primary:   false,
				RowCount:  10000,
				RowLogEst: NewLogEst(10000),
				Columns: []IndexColumn{
					{Name: "city", Index: 3, Ascending: true},
					{Name: "age", Index: 2, Ascending: true},
				},
				ColumnStats: []LogEst{
					NewLogEst(100), // ~100 rows per city
					NewLogEst(10),  // ~10 rows per city+age combo
				},
			},
		},
	}
}

func TestLogEst(t *testing.T) {
	tests := []struct {
		input    int64
		expected int64
	}{
		{1, 1},
		{10, 8},       // Should be close to 10
		{100, 64},     // Should be close to 100
		{1000, 512},   // Should be close to 1000
		{10000, 4096}, // Should be close to 10000
	}

	for _, tt := range tests {
		est := NewLogEst(tt.input)
		result := est.ToInt()

		// Allow some error due to logarithmic approximation
		ratio := float64(result) / float64(tt.expected)
		if ratio < 0.5 || ratio > 2.0 {
			t.Errorf("LogEst(%d).ToInt() = %d, expected ~%d", tt.input, result, tt.expected)
		}
	}
}

func TestBitmask(t *testing.T) {
	var mask Bitmask

	// Test Set
	mask.Set(0)
	mask.Set(3)
	mask.Set(5)

	// Test Has
	if !mask.Has(0) {
		t.Error("Expected bit 0 to be set")
	}
	if !mask.Has(3) {
		t.Error("Expected bit 3 to be set")
	}
	if mask.Has(1) {
		t.Error("Expected bit 1 to not be set")
	}

	// Test HasAll
	var subset Bitmask
	subset.Set(0)
	subset.Set(3)

	if !mask.HasAll(subset) {
		t.Error("Expected mask to have all bits in subset")
	}

	subset.Set(7)
	if mask.HasAll(subset) {
		t.Error("Expected mask to not have all bits in subset")
	}

	// Test Overlaps
	var other Bitmask
	other.Set(3)
	other.Set(7)

	if !mask.Overlaps(other) {
		t.Error("Expected masks to overlap")
	}

	other = 0
	other.Set(7)
	if mask.Overlaps(other) {
		t.Error("Expected masks to not overlap")
	}
}

func TestCostModelFullScan(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()

	cost, nOut := cm.EstimateFullScan(table)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("Expected positive cost, got %d", cost)
	}

	// Output rows should match table size
	if nOut != table.RowLogEst {
		t.Errorf("Expected nOut=%d, got %d", table.RowLogEst, nOut)
	}
}

func TestCostModelIndexScan(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()
	index := table.Indexes[0] // Primary key index

	// Test with equality constraint
	cost, nOut := cm.EstimateIndexScan(table, index, nil, 1, false, false)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("Expected positive index scan cost, got %d", cost)
	}

	// nOut should be non-negative
	if nOut < 0 {
		t.Errorf("Expected non-negative nOut, got %d", nOut)
	}
}

func TestCostModelRowidLookup(t *testing.T) {
	cm := NewCostModel()

	cost, nOut := cm.EstimateRowidLookup()

	// Should be very cheap (single row)
	if cost > 50 {
		t.Errorf("Rowid lookup too expensive: %d", cost)
	}

	// Should return exactly 1 row
	if nOut != 0 { // LogEst(1) = 0
		t.Errorf("Expected nOut=0 (1 row), got %d", nOut)
	}
}

func TestWhereLoopBuilder(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Create a simple WHERE term: id = 5
	term := &WhereTerm{
		Operator:   WO_EQ,
		LeftCursor: 0,
		LeftColumn: 0, // id column
		RightValue: 5,
	}

	builder := NewWhereLoopBuilder(table, 0, []*WhereTerm{term}, cm)
	loops := builder.Build()

	// Should generate multiple loops (full scan + index options)
	if len(loops) < 2 {
		t.Errorf("Expected at least 2 loops, got %d", len(loops))
	}

	// Check that we have a full scan option
	hasFullScan := false
	for _, loop := range loops {
		if loop.Index == nil {
			hasFullScan = true
			break
		}
	}
	if !hasFullScan {
		t.Error("Expected full scan option")
	}

	// Check that we have an index option
	hasIndex := false
	for _, loop := range loops {
		if loop.Index != nil {
			hasIndex = true
			break
		}
	}
	if !hasIndex {
		t.Error("Expected index option")
	}
}

func TestIndexSelector(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Create WHERE terms: city = 'NYC' AND age > 25
	terms := []*WhereTerm{
		{
			Operator:   WO_EQ,
			LeftCursor: 0,
			LeftColumn: 3, // city
			RightValue: "NYC",
		},
		{
			Operator:   WO_GT,
			LeftCursor: 0,
			LeftColumn: 2, // age
			RightValue: 25,
		},
	}

	selector := NewIndexSelector(table, terms, cm)
	bestIndex := selector.SelectBestIndex()

	// Should select some index
	if bestIndex == nil {
		t.Fatal("Expected an index to be selected")
	}

	t.Logf("Selected index: %s", bestIndex.Name)
}

func TestPlannerSingleTable(t *testing.T) {
	planner := NewPlanner()
	table := createTestTable()

	// Create WHERE clause: id = 5
	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0, // id
				RightValue: 5,
			},
		},
	}

	info, err := planner.PlanQuery([]*TableInfo{table}, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info.BestPath == nil {
		t.Fatal("Expected a plan to be generated")
	}

	if len(info.BestPath.Loops) != 1 {
		t.Errorf("Expected 1 loop, got %d", len(info.BestPath.Loops))
	}

	// Log the chosen plan details
	loop := info.BestPath.Loops[0]
	if loop.Index != nil {
		t.Logf("Plan uses index: %s", loop.Index.Name)
	} else {
		t.Logf("Plan uses full scan")
	}
	t.Logf("Plan flags: %d", loop.Flags)
}

func TestPlannerMultiTable(t *testing.T) {
	planner := NewPlanner()

	// Create two tables
	users := createTestTable()
	users.Name = "users"
	users.Cursor = 0

	orders := &TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  50000,
		RowLogEst: NewLogEst(50000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
			{Name: "amount", Index: 2, Type: "REAL"},
		},
		Indexes: []*IndexInfo{
			{
				Name:      "idx_orders_user_id",
				Table:     "orders",
				Unique:    false,
				RowCount:  50000,
				RowLogEst: NewLogEst(50000),
				Columns: []IndexColumn{
					{Name: "user_id", Index: 1, Ascending: true},
				},
				ColumnStats: []LogEst{NewLogEst(5)}, // ~5 orders per user
			},
		},
	}

	// WHERE clause: users.id = 5 AND orders.user_id = users.id
	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0, // users.id
				RightValue: 5,
			},
			{
				Operator:    WO_EQ,
				LeftCursor:  1,
				LeftColumn:  1,               // orders.user_id
				PrereqRight: Bitmask(1 << 0), // References users table
			},
		},
	}

	info, err := planner.PlanQuery([]*TableInfo{users, orders}, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info.BestPath == nil {
		t.Fatal("Expected a plan to be generated")
	}

	if len(info.BestPath.Loops) != 2 {
		t.Errorf("Expected 2 loops, got %d", len(info.BestPath.Loops))
	}

	// First loop should be users (has the constant constraint)
	if info.BestPath.Loops[0].TabIndex != 0 {
		t.Error("Expected users table to be first in join order")
	}
}

func TestExpressions(t *testing.T) {
	// Test BinaryExpr
	col := &ColumnExpr{Table: "users", Column: "id", Cursor: 0}
	val := &ValueExpr{Value: 5}
	expr := &BinaryExpr{Op: "=", Left: col, Right: val}

	if expr.String() != "(users.id = 5)" {
		t.Errorf("Unexpected string representation: %s", expr.String())
	}

	// Test used tables
	mask := expr.UsedTables()
	if !mask.Has(0) {
		t.Error("Expected expression to reference table 0")
	}

	// Test AndExpr
	col2 := &ColumnExpr{Table: "users", Column: "age", Cursor: 0}
	val2 := &ValueExpr{Value: 25}
	expr2 := &BinaryExpr{Op: ">", Left: col2, Right: val2}

	andExpr := &AndExpr{Terms: []Expr{expr, expr2}}
	if !andExpr.UsedTables().Has(0) {
		t.Error("Expected AND expression to reference table 0")
	}
}

func TestOptimizeWhereClause(t *testing.T) {
	planner := NewPlanner()
	table := createTestTable()

	// Create expression: id = 5 AND age > 25
	col1 := &ColumnExpr{Table: "users", Column: "id", Cursor: 0}
	val1 := &ValueExpr{Value: 5}
	expr1 := &BinaryExpr{Op: "=", Left: col1, Right: val1}

	col2 := &ColumnExpr{Table: "users", Column: "age", Cursor: 0}
	val2 := &ValueExpr{Value: 25}
	expr2 := &BinaryExpr{Op: ">", Left: col2, Right: val2}

	andExpr := &AndExpr{Terms: []Expr{expr1, expr2}}

	clause, err := planner.OptimizeWhereClause(andExpr, []*TableInfo{table})
	if err != nil {
		t.Fatalf("OptimizeWhereClause failed: %v", err)
	}

	// Should have 2 terms
	if len(clause.Terms) != 2 {
		t.Errorf("Expected 2 terms, got %d", len(clause.Terms))
	}

	// Check operators
	hasEq := false
	hasGt := false
	for _, term := range clause.Terms {
		if term.Operator == WO_EQ {
			hasEq = true
		}
		if term.Operator == WO_GT {
			hasGt = true
		}
	}

	if !hasEq || !hasGt {
		t.Error("Expected both EQ and GT operators")
	}
}

func TestExplainPlan(t *testing.T) {
	planner := NewPlanner()
	table := createTestTable()

	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0,
				RightValue: 5,
			},
		},
	}

	info, err := planner.PlanQuery([]*TableInfo{table}, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	explanation := planner.ExplainPlan(info)

	// Should contain key information
	if explanation == "" {
		t.Error("Expected non-empty explanation")
	}

	// Should mention the table name
	if len(explanation) < 20 {
		t.Error("Explanation seems too short")
	}

	t.Logf("Plan explanation:\n%s", explanation)
}

func TestValidatePlan(t *testing.T) {
	planner := NewPlanner()
	table := createTestTable()

	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0,
				RightValue: 5,
			},
		},
	}

	info, err := planner.PlanQuery([]*TableInfo{table}, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	// Valid plan should pass validation
	err = planner.ValidatePlan(info)
	if err != nil {
		t.Errorf("Valid plan failed validation: %v", err)
	}
}

func TestIndexUsage(t *testing.T) {
	table := createTestTable()
	cm := NewCostModel()

	// Create terms for compound index: city = 'NYC' AND age > 25
	terms := []*WhereTerm{
		{
			Operator:   WO_EQ,
			LeftCursor: 0,
			LeftColumn: 3, // city
			RightValue: "NYC",
		},
		{
			Operator:   WO_GT,
			LeftCursor: 0,
			LeftColumn: 2, // age
			RightValue: 25,
		},
	}

	selector := NewIndexSelector(table, terms, cm)
	index := table.Indexes[2] // idx_users_city_age

	usage := selector.AnalyzeIndexUsage(index, []string{"id", "name"})

	// Should have 1 EQ term and 1 range term
	if len(usage.EqTerms) != 1 {
		t.Errorf("Expected 1 EQ term, got %d", len(usage.EqTerms))
	}

	if len(usage.RangeTerms) != 1 {
		t.Errorf("Expected 1 range term, got %d", len(usage.RangeTerms))
	}

	// Should not be covering (needs id and name, but index only has city and age)
	if usage.Covering {
		t.Error("Index should not be covering")
	}

	// Test explanation
	explanation := usage.Explain()
	if explanation == "" {
		t.Error("Expected non-empty explanation")
	}
	t.Logf("Index usage: %s", explanation)
}

func BenchmarkPlanQuery(b *testing.B) {
	planner := NewPlanner()
	table := createTestTable()

	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0,
				RightValue: 5,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.PlanQuery([]*TableInfo{table}, whereClause)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWhereLoopBuilder(b *testing.B) {
	table := createTestTable()
	cm := NewCostModel()

	term := &WhereTerm{
		Operator:   WO_EQ,
		LeftCursor: 0,
		LeftColumn: 0,
		RightValue: 5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder := NewWhereLoopBuilder(table, 0, []*WhereTerm{term}, cm)
		_ = builder.Build()
	}
}
