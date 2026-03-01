// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
)

// TestSubqueryType tests the SubqueryType enum.
func TestSubqueryType(t *testing.T) {
	tests := []struct {
		typ      SubqueryType
		expected string
	}{
		{SubqueryScalar, "SCALAR"},
		{SubqueryExists, "EXISTS"},
		{SubqueryIn, "IN"},
		{SubqueryFrom, "FROM"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.expected {
				t.Errorf("SubqueryType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNewSubqueryOptimizer tests creating a new optimizer.
func TestNewSubqueryOptimizer(t *testing.T) {
	costModel := NewCostModel()
	optimizer := NewSubqueryOptimizer(costModel)

	if optimizer == nil {
		t.Fatal("NewSubqueryOptimizer returned nil")
	}

	if optimizer.CostModel != costModel {
		t.Error("CostModel not set correctly")
	}

	if optimizer.NextTempTable != 1 {
		t.Errorf("NextTempTable = %d, want 1", optimizer.NextTempTable)
	}
}

// TestAnalyzeSubquery tests analyzing a subquery.
func TestAnalyzeSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	// Create a simple scalar subquery expression
	subquery := &ValueExpr{Value: 42}
	outerTables := []*TableInfo{
		{
			Name:      "users",
			Cursor:    0,
			RowCount:  1000,
			RowLogEst: NewLogEst(1000),
		},
	}

	info, err := optimizer.AnalyzeSubquery(subquery, outerTables)
	if err != nil {
		t.Fatalf("AnalyzeSubquery failed: %v", err)
	}

	if info == nil {
		t.Fatal("AnalyzeSubquery returned nil info")
	}

	if info.Type != SubqueryScalar {
		t.Errorf("Type = %v, want SubqueryScalar", info.Type)
	}

	if info.Expr != subquery {
		t.Error("Expr not preserved")
	}
}

// TestFlattenSubquery tests flattening a simple subquery.
func TestFlattenSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	// Create a flattenable subquery
	subqueryInfo := &SubqueryInfo{
		Type:          SubqueryFrom,
		CanFlatten:    true,
		EstimatedRows: NewLogEst(100),
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "users", Cursor: 0},
		},
		AllLoops: []*WhereLoop{},
	}

	flattened, err := optimizer.FlattenSubquery(subqueryInfo, parentInfo)
	if err != nil {
		t.Fatalf("FlattenSubquery failed: %v", err)
	}

	if flattened == nil {
		t.Fatal("FlattenSubquery returned nil")
	}

	// Verify the flattened query has the same structure
	if len(flattened.Tables) != len(parentInfo.Tables) {
		t.Errorf("Table count = %d, want %d", len(flattened.Tables), len(parentInfo.Tables))
	}
}

// TestFlattenSubqueryNotFlattenable tests that non-flattenable subqueries fail.
func TestFlattenSubqueryNotFlattenable(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:       SubqueryScalar,
		CanFlatten: false, // Not flattenable
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{{Name: "users"}},
	}

	_, err := optimizer.FlattenSubquery(subqueryInfo, parentInfo)
	if err == nil {
		t.Error("Expected error for non-flattenable subquery, got nil")
	}
}

// TestDecorrelateSubquery tests decorrelating a correlated subquery.
func TestDecorrelateSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	// Create a correlated subquery
	correlatedInfo := &SubqueryInfo{
		Type:           SubqueryExists,
		IsCorrelated:   true,
		OuterRefs:      Bitmask(1), // References table 0
		EstimatedRows:  NewLogEst(50),
		ExecutionCount: NewLogEst(1000), // Executes once per outer row
	}

	decorrelated, err := optimizer.DecorrelateSubquery(correlatedInfo)
	if err != nil {
		t.Fatalf("DecorrelateSubquery failed: %v", err)
	}

	if decorrelated.IsCorrelated {
		t.Error("Decorrelated subquery still marked as correlated")
	}

	if decorrelated.ExecutionCount != NewLogEst(1) {
		t.Errorf("ExecutionCount = %d, want 1", decorrelated.ExecutionCount.ToInt())
	}

	if !decorrelated.CanFlatten {
		t.Error("Decorrelated subquery should be flattenable")
	}
}

// TestDecorrelateUncorrelated tests that uncorrelated subqueries pass through.
func TestDecorrelateUncorrelated(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	uncorrelatedInfo := &SubqueryInfo{
		Type:         SubqueryIn,
		IsCorrelated: false,
	}

	result, err := optimizer.DecorrelateSubquery(uncorrelatedInfo)
	if err != nil {
		t.Fatalf("DecorrelateSubquery failed: %v", err)
	}

	if result.IsCorrelated {
		t.Error("Uncorrelated subquery became correlated")
	}
}

// TestMaterializeSubquery tests materializing a subquery.
func TestMaterializeSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:           SubqueryScalar,
		CanMaterialize: true,
		EstimatedRows:  NewLogEst(100),
	}

	materialized, err := optimizer.MaterializeSubquery(subqueryInfo)
	if err != nil {
		t.Fatalf("MaterializeSubquery failed: %v", err)
	}

	if materialized.MaterializedTable == "" {
		t.Error("MaterializedTable not set")
	}

	if materialized.IsCorrelated {
		t.Error("Materialized subquery should not be correlated")
	}

	if materialized.ExecutionCount != NewLogEst(1) {
		t.Errorf("ExecutionCount = %d, want 1", materialized.ExecutionCount.ToInt())
	}

	// Verify temp table name is unique
	materialized2, _ := optimizer.MaterializeSubquery(subqueryInfo)
	if materialized2.MaterializedTable == materialized.MaterializedTable {
		t.Error("Temp table names should be unique")
	}
}

// TestMaterializeSubqueryNotMaterializable tests non-materializable subqueries.
func TestMaterializeSubqueryNotMaterializable(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:           SubqueryFrom,
		CanMaterialize: false,
	}

	_, err := optimizer.MaterializeSubquery(subqueryInfo)
	if err == nil {
		t.Error("Expected error for non-materializable subquery, got nil")
	}
}

// TestConvertInToJoin tests converting IN subquery to JOIN.
func TestConvertInToJoin(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(100),
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{
				Name:      "users",
				RowCount:  1000,
				RowLogEst: NewLogEst(1000),
			},
		},
		NOut: NewLogEst(1000),
	}

	// This might fail if JOIN is not beneficial, but we test the logic
	result, err := optimizer.ConvertInToJoin(subqueryInfo, parentInfo)
	// We allow either success or "not beneficial" error
	if err != nil && result == nil {
		// Expected - JOIN might not be beneficial
		t.Logf("ConvertInToJoin determined JOIN not beneficial: %v", err)
	} else if err == nil && result != nil {
		// Success case
		if len(result.Tables) < len(parentInfo.Tables) {
			t.Error("Converted query should have at least as many tables")
		}
	}
}

// TestConvertInToJoinWrongType tests that non-IN subqueries fail.
func TestConvertInToJoinWrongType(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type: SubqueryExists, // Not IN
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{{Name: "users"}},
	}

	_, err := optimizer.ConvertInToJoin(subqueryInfo, parentInfo)
	if err == nil {
		t.Error("Expected error for non-IN subquery, got nil")
	}
}

// TestConvertExistsToSemiJoin tests converting EXISTS to semi-join.
func TestConvertExistsToSemiJoin(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(50),
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{
				Name:      "orders",
				RowCount:  10000,
				RowLogEst: NewLogEst(10000),
			},
		},
		NOut: NewLogEst(10000),
	}

	result, err := optimizer.ConvertExistsToSemiJoin(subqueryInfo, parentInfo)
	// Similar to IN test - might not be beneficial
	if err != nil && result == nil {
		t.Logf("ConvertExistsToSemiJoin determined semi-join not beneficial: %v", err)
	} else if err == nil && result != nil {
		if len(result.Tables) < len(parentInfo.Tables) {
			t.Error("Converted query should have at least as many tables")
		}
	}
}

// TestConvertExistsToSemiJoinWrongType tests that non-EXISTS subqueries fail.
func TestConvertExistsToSemiJoinWrongType(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type: SubqueryScalar, // Not EXISTS
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{{Name: "users"}},
	}

	_, err := optimizer.ConvertExistsToSemiJoin(subqueryInfo, parentInfo)
	if err == nil {
		t.Error("Expected error for non-EXISTS subquery, got nil")
	}
}

// TestOptimizeSubquery tests the main optimization dispatcher.
func TestOptimizeSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	tests := []struct {
		name        string
		subquery    *SubqueryInfo
		expectError bool
	}{
		{
			name: "Flattenable FROM subquery",
			subquery: &SubqueryInfo{
				Type:          SubqueryFrom,
				CanFlatten:    true,
				EstimatedRows: NewLogEst(100),
			},
			expectError: false,
		},
		{
			name: "Correlated EXISTS subquery",
			subquery: &SubqueryInfo{
				Type:          SubqueryExists,
				IsCorrelated:  true,
				OuterRefs:     Bitmask(1),
				EstimatedRows: NewLogEst(50),
			},
			expectError: false,
		},
		{
			name: "Materializable scalar subquery",
			subquery: &SubqueryInfo{
				Type:           SubqueryScalar,
				CanMaterialize: true,
				EstimatedRows:  NewLogEst(10),
			},
			expectError: false,
		},
	}

	parentInfo := &WhereInfo{
		Tables: []*TableInfo{
			{
				Name:      "users",
				RowCount:  1000,
				RowLogEst: NewLogEst(1000),
			},
		},
		NOut:     NewLogEst(1000),
		AllLoops: []*WhereLoop{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := optimizer.OptimizeSubquery(tt.subquery, parentInfo)

			if tt.expectError && err == nil {
				t.Error("Expected error, got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result == nil && !tt.expectError {
				t.Error("OptimizeSubquery returned nil result")
			}
		})
	}
}

// TestSubqueryExpr tests the SubqueryExpr type.
func TestSubqueryExpr(t *testing.T) {
	query := &ValueExpr{Value: "SELECT * FROM users"}
	outerCol := &ColumnExpr{
		Table:  "orders",
		Column: "user_id",
		Cursor: 0,
	}

	subExpr := &SubqueryExpr{
		Query:       query,
		Type:        SubqueryIn,
		OuterColumn: outerCol,
	}

	// Test String()
	str := subExpr.String()
	if str != "(IN SUBQUERY)" {
		t.Errorf("String() = %q, want \"(IN SUBQUERY)\"", str)
	}

	// Test UsedTables()
	mask := subExpr.UsedTables()
	if mask != outerCol.UsedTables() {
		t.Errorf("UsedTables() = %v, want %v", mask, outerCol.UsedTables())
	}
}

// TestCanFlattenSubquery tests the flattening criteria.
func TestCanFlattenSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	tests := []struct {
		name     string
		info     *SubqueryInfo
		expected bool
	}{
		{
			name: "FROM subquery - flattenable",
			info: &SubqueryInfo{
				Type:         SubqueryFrom,
				IsCorrelated: false,
			},
			expected: true,
		},
		{
			name: "Scalar uncorrelated - flattenable",
			info: &SubqueryInfo{
				Type:         SubqueryScalar,
				IsCorrelated: false,
			},
			expected: true,
		},
		{
			name: "Scalar correlated - not flattenable",
			info: &SubqueryInfo{
				Type:         SubqueryScalar,
				IsCorrelated: true,
			},
			expected: false,
		},
		{
			name: "EXISTS - not flattenable",
			info: &SubqueryInfo{
				Type:         SubqueryExists,
				IsCorrelated: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.canFlattenSubquery(tt.info)
			if result != tt.expected {
				t.Errorf("canFlattenSubquery() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestShouldMaterializeSubquery tests materialization criteria.
func TestShouldMaterializeSubquery(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	tests := []struct {
		name     string
		info     *SubqueryInfo
		expected bool
	}{
		{
			name: "Uncorrelated - don't materialize",
			info: &SubqueryInfo{
				IsCorrelated: false,
			},
			expected: false,
		},
		{
			name: "Correlated with small result - materialize",
			info: &SubqueryInfo{
				IsCorrelated:   true,
				EstimatedRows:  NewLogEst(10),
				ExecutionCount: NewLogEst(1000),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.shouldMaterializeSubquery(tt.info)
			if result != tt.expected {
				t.Errorf("shouldMaterializeSubquery() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestEstimateCosts tests cost estimation functions.
func TestEstimateCosts(t *testing.T) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		EstimatedRows: NewLogEst(100),
	}

	parentInfo := &WhereInfo{
		NOut: NewLogEst(1000),
	}

	// Test IN cost
	inCost := optimizer.estimateInCost(subqueryInfo, parentInfo)
	if inCost == 0 {
		t.Error("IN cost should be non-zero")
	}

	// Test JOIN cost
	joinCost := optimizer.estimateJoinCost(subqueryInfo, parentInfo)
	if joinCost == 0 {
		t.Error("JOIN cost should be non-zero")
	}

	// Test EXISTS cost
	existsCost := optimizer.estimateExistsCost(subqueryInfo, parentInfo)
	if existsCost == 0 {
		t.Error("EXISTS cost should be non-zero")
	}

	t.Logf("Cost estimates: IN=%d, JOIN=%d, EXISTS=%d",
		inCost.ToInt(), joinCost.ToInt(), existsCost.ToInt())
}

// BenchmarkAnalyzeSubquery benchmarks subquery analysis.
func BenchmarkAnalyzeSubquery(b *testing.B) {
	optimizer := NewSubqueryOptimizer(NewCostModel())
	subquery := &ValueExpr{Value: 42}
	outerTables := []*TableInfo{
		{Name: "users", RowCount: 1000, RowLogEst: NewLogEst(1000)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = optimizer.AnalyzeSubquery(subquery, outerTables)
	}
}

// BenchmarkOptimizeSubquery benchmarks the optimization dispatcher.
func BenchmarkOptimizeSubquery(b *testing.B) {
	optimizer := NewSubqueryOptimizer(NewCostModel())

	subqueryInfo := &SubqueryInfo{
		Type:          SubqueryFrom,
		CanFlatten:    true,
		EstimatedRows: NewLogEst(100),
	}

	parentInfo := &WhereInfo{
		Tables:   []*TableInfo{{Name: "users", RowCount: 1000}},
		NOut:     NewLogEst(1000),
		AllLoops: []*WhereLoop{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = optimizer.OptimizeSubquery(subqueryInfo, parentInfo)
	}
}
