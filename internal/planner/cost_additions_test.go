package planner

import (
	"testing"
)

func TestEstimateIndexLookup(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()
	index := table.Indexes[0] // idx_users_id (unique index)

	cost, nOut := cm.EstimateIndexLookup(table, index, 1, false)

	// Should have very low output (unique lookup)
	if nOut > LogEst(10) {
		t.Errorf("Unique lookup nOut = %d, want low value", nOut)
	}

	// Should have positive cost
	if cost <= 0 {
		t.Errorf("Lookup cost = %d, want positive", cost)
	}

	// Test covering index reduces cost
	costCovering, _ := cm.EstimateIndexLookup(table, index, 1, true)
	if costCovering >= cost {
		t.Error("Covering index should have lower cost")
	}
}

func TestEstimateInOperator(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()
	index := table.Indexes[0]

	// Test with small IN list
	cost, nOut := cm.EstimateInOperator(table, index, 1, 5, false)
	if cost <= 0 {
		t.Error("IN operator cost should be positive")
	}
	if nOut <= 0 {
		t.Error("IN operator nOut should be positive")
	}

	// Test with larger IN list
	largeCost, largeNOut := cm.EstimateInOperator(table, index, 1, 100, false)
	if largeCost <= cost {
		t.Error("Larger IN list should have higher cost")
	}
	if largeNOut <= nOut {
		t.Error("Larger IN list should have more output rows")
	}

	// Test covering index reduces cost
	costCovering, _ := cm.EstimateInOperator(table, index, 1, 5, true)
	if costCovering >= cost {
		t.Error("Covering index should have lower cost for IN operator")
	}
}

func TestEstimateTruthProbability(t *testing.T) {
	cm := NewCostModel()

	// Test with preset TruthProb
	term := &WhereTerm{
		TruthProb: LogEst(50),
	}
	prob := cm.EstimateTruthProbability(term)
	if prob != LogEst(50) {
		t.Errorf("TruthProb = %d, want 50", prob)
	}

	// Test WO_EQ with small int
	term = &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 0,
	}
	prob = cm.EstimateTruthProbability(term)
	if prob != truthProbSmallInt {
		t.Errorf("Small int probability = %d, want %d", prob, truthProbSmallInt)
	}

	// Test WO_EQ with normal value
	term = &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 100,
	}
	prob = cm.EstimateTruthProbability(term)
	if prob != selectivityEq {
		t.Errorf("Normal EQ probability = %d, want %d", prob, selectivityEq)
	}

	// Test range operators
	term = &WhereTerm{Operator: WO_LT}
	prob = cm.EstimateTruthProbability(term)
	if prob != selectivityRange {
		t.Errorf("Range probability = %d, want %d", prob, selectivityRange)
	}

	// Test WO_IN
	term = &WhereTerm{Operator: WO_IN}
	prob = cm.EstimateTruthProbability(term)
	if prob != selectivityIn {
		t.Errorf("IN probability = %d, want %d", prob, selectivityIn)
	}

	// Test WO_ISNULL
	term = &WhereTerm{Operator: WO_ISNULL}
	prob = cm.EstimateTruthProbability(term)
	if prob != selectivityNull {
		t.Errorf("ISNULL probability = %d, want %d", prob, selectivityNull)
	}

	// Test default (WO_AND is not in selectivity map)
	term = &WhereTerm{Operator: WO_AND}
	prob = cm.EstimateTruthProbability(term)
	if prob != truthProbDefault {
		t.Errorf("Default probability = %d, want %d", prob, truthProbDefault)
	}
}

func TestAdjustCostForMultipleTerms(t *testing.T) {
	cm := NewCostModel()

	baseCost := LogEst(100)

	// Single term - no adjustment
	adjusted := cm.AdjustCostForMultipleTerms(baseCost, 1)
	if adjusted != baseCost {
		t.Errorf("Single term cost = %d, want %d", adjusted, baseCost)
	}

	// Multiple terms - should add comparison cost
	adjustedMultiple := cm.AdjustCostForMultipleTerms(baseCost, 5)
	if adjustedMultiple <= baseCost {
		t.Errorf("Multiple terms cost = %d, should be more than %d", adjustedMultiple, baseCost)
	}
}

func TestSelectBestLoopEdgeCases(t *testing.T) {
	cm := NewCostModel()

	// Empty list
	best := cm.SelectBestLoop([]*WhereLoop{})
	if best != nil {
		t.Error("SelectBestLoop([]) should return nil")
	}

	// Single loop
	loop := &WhereLoop{
		TabIndex: 0,
		NOut:     LogEst(100),
	}
	best = cm.SelectBestLoop([]*WhereLoop{loop})
	if best != loop {
		t.Error("SelectBestLoop with single loop should return that loop")
	}
}

func TestCompareCostsRaw(t *testing.T) {
	cm := NewCostModel()

	// Equal costs and NOut - should return false (not better)
	if cm.CompareCosts(100, 50, 100, 50) {
		t.Error("Equal costs should return false (not better)")
	}

	// Lower cost - should return true (better)
	if !cm.CompareCosts(80, 50, 100, 50) {
		t.Error("Lower cost should return true (better)")
	}

	// Higher cost - should return false (not better)
	if cm.CompareCosts(120, 50, 100, 50) {
		t.Error("Higher cost should return false (not better)")
	}

	// Equal cost, fewer output rows - should return true (better)
	if !cm.CompareCosts(100, 30, 100, 50) {
		t.Error("Equal cost with fewer rows should return true (better)")
	}

	// Equal cost, more output rows - should return false (not better)
	if cm.CompareCosts(100, 70, 100, 50) {
		t.Error("Equal cost with more rows should return false (not better)")
	}
}

func TestEstimateEqSelectivity(t *testing.T) {
	cm := NewCostModel()

	// Term with small int value
	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 1,
	}
	sel := cm.estimateEqSelectivity(term)
	if sel != truthProbSmallInt {
		t.Errorf("Small int selectivity = %d, want %d", sel, truthProbSmallInt)
	}

	// Term with large value
	term = &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 1000,
	}
	sel = cm.estimateEqSelectivity(term)
	if sel != selectivityEq {
		t.Errorf("Large value selectivity = %d, want %d", sel, selectivityEq)
	}

	// Term with non-int value
	term = &WhereTerm{
		Operator:   WO_EQ,
		RightValue: "test",
	}
	sel = cm.estimateEqSelectivity(term)
	if sel != selectivityEq {
		t.Errorf("Non-int selectivity = %d, want %d", sel, selectivityEq)
	}
}

func TestEstimateOutputRows(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		RowLogEst:   LogEst(1000),
		ColumnStats: []LogEst{100, 50, 25},
	}

	// Within stats range
	nOut := cm.estimateOutputRows(index, 1)
	if nOut != 100 {
		t.Errorf("Output rows = %d, want 100", nOut)
	}

	nOut = cm.estimateOutputRows(index, 2)
	if nOut != 50 {
		t.Errorf("Output rows = %d, want 50", nOut)
	}

	// Beyond stats range - should use selectivity
	nOut = cm.estimateOutputRows(index, 5)
	if nOut >= index.RowLogEst {
		t.Error("Output rows beyond stats should be reduced")
	}
}

func TestApplySelectivityReductions(t *testing.T) {
	cm := NewCostModel()

	nOut := cm.applySelectivityReductions(LogEst(1000), 0)
	if nOut != 1000 {
		t.Errorf("No reductions: nOut = %d, want 1000", nOut)
	}

	nOut = cm.applySelectivityReductions(LogEst(1000), 3)
	if nOut >= 1000 {
		t.Error("Selectivity reductions should reduce output")
	}
	if nOut < 0 {
		t.Error("Output should not go negative")
	}

	// Test floor at 0
	nOut = cm.applySelectivityReductions(LogEst(10), 100)
	if nOut != 0 {
		t.Errorf("Heavy reductions: nOut = %d, want 0", nOut)
	}
}

func TestCalculateLookupCost(t *testing.T) {
	cm := NewCostModel()

	// Covering index
	cost := cm.calculateLookupCost(LogEst(10), true)
	if cost <= 0 {
		t.Error("Lookup cost should be positive")
	}

	// Non-covering index (should add rowid lookup cost)
	costNonCovering := cm.calculateLookupCost(LogEst(10), false)
	if costNonCovering <= cost {
		t.Error("Non-covering index should have higher cost")
	}
}

func TestEstimateUniqueLookup(t *testing.T) {
	cm := NewCostModel()

	// Covering
	cost, nOut := cm.estimateUniqueLookup(true)
	if nOut != 0 {
		t.Errorf("Unique lookup nOut = %d, want 0", nOut)
	}
	if cost <= 0 {
		t.Error("Unique lookup cost should be positive")
	}

	// Non-covering
	costNonCovering, _ := cm.estimateUniqueLookup(false)
	if costNonCovering <= cost {
		t.Error("Non-covering unique lookup should have higher cost")
	}
}
