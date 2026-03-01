// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
)

// Comprehensive tests for cost.go functions

func TestNewCostModel(t *testing.T) {
	cm := NewCostModel()
	if cm == nil {
		t.Fatal("NewCostModel() returned nil")
	}
	if !cm.UseStatistics {
		t.Error("UseStatistics should be true by default")
	}
}

func TestEstimateFullScanBasic(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
	}

	cost, nOut := cm.EstimateFullScan(table)

	if cost <= 0 {
		t.Error("Cost should be positive")
	}
	if nOut != table.RowLogEst {
		t.Errorf("Expected nOut=%d, got %d", table.RowLogEst, nOut)
	}
}

func TestEstimateIndexScanWithEquality(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:      "idx_test",
		RowLogEst: NewLogEst(10000),
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	cost, nOut := cm.EstimateIndexScan(table, index, nil, 1, false, false)

	if cost <= 0 {
		t.Error("Cost should be positive")
	}
	if nOut >= table.RowLogEst {
		t.Error("Output rows should be less than table rows for selective index scan")
	}
}

func TestEstimateIndexScanWithRange(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:      "idx_test",
		RowLogEst: NewLogEst(10000),
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	cost, nOut := cm.EstimateIndexScan(table, index, nil, 0, true, false)

	if cost <= 0 {
		t.Error("Cost should be positive")
	}
	// Range scan should return fewer rows
	if nOut >= table.RowLogEst {
		t.Error("Output rows should be reduced for range scan")
	}
}

func TestEstimateIndexScanCovering(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:      "idx_test",
		RowLogEst: NewLogEst(10000),
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	costNonCovering, _ := cm.EstimateIndexScan(table, index, nil, 1, false, false)
	costCovering, _ := cm.EstimateIndexScan(table, index, nil, 1, false, true)

	// Covering index should be cheaper (no rowid lookup)
	if costCovering >= costNonCovering {
		t.Error("Covering index should be cheaper than non-covering")
	}
}

func TestEstimateIndexLookupUnique(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:   "idx_unique",
		Unique: true,
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{0}, // One row per value
	}

	cost, nOut := cm.EstimateIndexLookup(table, index, 1, false)

	// Unique lookup should return exactly one row
	if nOut != 0 { // LogEst(1) = 0
		t.Errorf("Expected nOut=0 (1 row), got %d", nOut)
	}
	if cost <= 0 {
		t.Error("Cost should be positive")
	}
}

func TestEstimateIndexLookupNonUnique(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:   "idx_non_unique",
		Unique: false,
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	cost, nOut := cm.EstimateIndexLookup(table, index, 1, false)

	if nOut <= 0 {
		t.Error("Non-unique lookup should return multiple rows")
	}
	if cost <= 0 {
		t.Error("Cost should be positive")
	}
}

func TestEstimateRowidLookup(t *testing.T) {
	cm := NewCostModel()

	cost, nOut := cm.EstimateRowidLookup()

	if nOut != 0 { // LogEst(1) = 0
		t.Errorf("Expected nOut=0 (1 row), got %d", nOut)
	}
	if cost != costIndexSeek {
		t.Errorf("Expected cost=%d, got %d", costIndexSeek, cost)
	}
}

func TestEstimateInOperatorComprehensive(t *testing.T) {
	cm := NewCostModel()
	table := &TableInfo{
		Name:      "test",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}
	index := &IndexInfo{
		Name:      "idx_test",
		RowLogEst: NewLogEst(10000),
		Columns: []IndexColumn{
			{Name: "col1", Index: 0},
		},
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	inListSize := 5
	cost, nOut := cm.EstimateInOperator(table, index, 1, inListSize, false)

	if cost <= 0 {
		t.Error("Cost should be positive")
	}
	// Should be more expensive than single lookup (multiple lookups)
	singleCost, _ := cm.EstimateIndexLookup(table, index, 1, false)
	if cost <= singleCost {
		t.Error("IN operator should be more expensive than single lookup")
	}
	if nOut <= 0 {
		t.Error("Output rows should be positive")
	}
}

func TestEstimateTruthProbabilityEq(t *testing.T) {
	cm := NewCostModel()

	term := &WhereTerm{
		Operator:   WO_EQ,
		RightValue: 5,
	}

	prob := cm.EstimateTruthProbability(term)
	if prob == 0 {
		t.Error("Truth probability should be non-zero")
	}
}

func TestEstimateTruthProbabilitySmallInt(t *testing.T) {
	cm := NewCostModel()

	tests := []struct {
		value    int
		expected LogEst
	}{
		{-1, truthProbSmallInt},
		{0, truthProbSmallInt},
		{1, truthProbSmallInt},
		{2, selectivityEq},
	}

	for _, tt := range tests {
		term := &WhereTerm{
			Operator:   WO_EQ,
			RightValue: tt.value,
		}
		prob := cm.EstimateTruthProbability(term)
		if prob != tt.expected {
			t.Errorf("Expected probability %d for value %d, got %d", tt.expected, tt.value, prob)
		}
	}
}

func TestEstimateTruthProbabilityRange(t *testing.T) {
	cm := NewCostModel()

	operators := []WhereOperator{WO_LT, WO_LE, WO_GT, WO_GE}

	for _, op := range operators {
		term := &WhereTerm{
			Operator: op,
		}
		prob := cm.EstimateTruthProbability(term)
		if prob != selectivityRange {
			t.Errorf("Expected range selectivity for operator %v, got %d", op, prob)
		}
	}
}

func TestEstimateTruthProbabilityIn(t *testing.T) {
	cm := NewCostModel()

	term := &WhereTerm{
		Operator: WO_IN,
	}
	prob := cm.EstimateTruthProbability(term)
	if prob != selectivityIn {
		t.Errorf("Expected IN selectivity %d, got %d", selectivityIn, prob)
	}
}

func TestEstimateTruthProbabilityIsNull(t *testing.T) {
	cm := NewCostModel()

	term := &WhereTerm{
		Operator: WO_ISNULL,
	}
	prob := cm.EstimateTruthProbability(term)
	if prob != selectivityNull {
		t.Errorf("Expected NULL selectivity %d, got %d", selectivityNull, prob)
	}
}

func TestCompareCosts(t *testing.T) {
	cm := NewCostModel()

	tests := []struct {
		name     string
		cost1    LogEst
		nOut1    LogEst
		cost2    LogEst
		nOut2    LogEst
		expected bool
	}{
		{"lower cost wins", 100, 50, 200, 50, true},
		{"higher cost loses", 200, 50, 100, 50, false},
		{"equal cost, fewer rows wins", 100, 50, 100, 100, true},
		{"equal cost, more rows loses", 100, 100, 100, 50, false},
		{"equal cost and rows", 100, 50, 100, 50, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.CompareCosts(tt.cost1, tt.nOut1, tt.cost2, tt.nOut2)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAdjustCostForMultipleTermsComprehensive(t *testing.T) {
	cm := NewCostModel()

	baseCost := LogEst(100)

	tests := []struct {
		nTerms int
	}{
		{1}, // No adjustment
		{2},
		{5},
		{10},
	}

	for _, tt := range tests {
		cost := cm.AdjustCostForMultipleTerms(baseCost, tt.nTerms)
		if tt.nTerms == 1 {
			if cost != baseCost {
				t.Errorf("Cost should not change for 1 term, got %d", cost)
			}
		} else {
			if cost <= baseCost {
				t.Errorf("Cost should increase for multiple terms")
			}
		}
	}
}

func TestEstimateSetupCost(t *testing.T) {
	cm := NewCostModel()
	nRows := NewLogEst(1000)

	tests := []struct {
		name       string
		setupType  SetupType
		expectZero bool
	}{
		{"None", SetupNone, true},
		{"AutoIndex", SetupAutoIndex, false},
		{"Sort", SetupSort, false},
		{"BloomFilter", SetupBloomFilter, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := cm.EstimateSetupCost(tt.setupType, nRows)
			if tt.expectZero && cost != 0 {
				t.Errorf("Expected zero cost for %v, got %d", tt.setupType, cost)
			}
			if !tt.expectZero && cost <= 0 {
				t.Errorf("Expected positive cost for %v, got %d", tt.setupType, cost)
			}
		})
	}
}

func TestCalculateLoopCost(t *testing.T) {
	cm := NewCostModel()

	loop := &WhereLoop{
		Setup: LogEst(100),
		Run:   LogEst(500),
	}

	cost := cm.CalculateLoopCost(loop)
	expected := loop.Setup + loop.Run

	if cost != expected {
		t.Errorf("Expected cost %d, got %d", expected, cost)
	}
}

func TestCombineLoopCostsEmpty(t *testing.T) {
	cm := NewCostModel()

	totalCost, totalRows := cm.CombineLoopCosts([]*WhereLoop{})

	if totalCost != 0 {
		t.Errorf("Expected zero cost for empty loops, got %d", totalCost)
	}
	if totalRows != 0 {
		t.Errorf("Expected zero rows for empty loops, got %d", totalRows)
	}
}

func TestCombineLoopCostsSingle(t *testing.T) {
	cm := NewCostModel()

	loop := &WhereLoop{
		Setup: LogEst(10),
		Run:   LogEst(100),
		NOut:  NewLogEst(50),
	}

	totalCost, totalRows := cm.CombineLoopCosts([]*WhereLoop{loop})

	if totalCost <= 0 {
		t.Error("Total cost should be positive")
	}
	if totalRows != loop.NOut {
		t.Errorf("Total rows should equal loop output")
	}
}

func TestCombineLoopCostsMultiple(t *testing.T) {
	cm := NewCostModel()

	loops := []*WhereLoop{
		{Setup: LogEst(10), Run: LogEst(100), NOut: NewLogEst(10)},
		{Setup: LogEst(5), Run: LogEst(50), NOut: NewLogEst(5)},
		{Setup: LogEst(2), Run: LogEst(20), NOut: NewLogEst(2)},
	}

	totalCost, totalRows := cm.CombineLoopCosts(loops)

	if totalCost <= 0 {
		t.Error("Total cost should be positive")
	}
	if totalRows <= 0 {
		t.Error("Total rows should be positive")
	}

	// Cost should include all setup costs
	minCost := loops[0].Setup + loops[1].Setup + loops[2].Setup
	if totalCost < minCost {
		t.Errorf("Total cost should be at least %d, got %d", minCost, totalCost)
	}
}

func TestEstimateCoveringIndex(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "col1"},
			{Name: "col2"},
			{Name: "col3"},
		},
	}

	tests := []struct {
		name           string
		neededColumns  []string
		expectedResult bool
	}{
		{"all columns covered", []string{"col1", "col2"}, true},
		{"subset covered", []string{"col1"}, true},
		{"not covered", []string{"col1", "col4"}, false},
		{"empty needed", []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.EstimateCoveringIndex(index, tt.neededColumns)
			if result != tt.expectedResult {
				t.Errorf("Expected %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

func TestSelectBestLoop(t *testing.T) {
	cm := NewCostModel()

	loops := []*WhereLoop{
		{Setup: 0, Run: 1000, NOut: 100},
		{Setup: 0, Run: 500, NOut: 50},
		{Setup: 0, Run: 2000, NOut: 200},
	}

	best := cm.SelectBestLoop(loops)
	if best == nil {
		t.Fatal("SelectBestLoop returned nil")
	}

	// Should select the loop with lowest cost
	if best.Run != 500 {
		t.Errorf("Expected best loop with cost 500, got %d", best.Run)
	}
}

func TestSelectBestLoopEmpty(t *testing.T) {
	cm := NewCostModel()

	best := cm.SelectBestLoop([]*WhereLoop{})
	if best != nil {
		t.Error("Expected nil for empty loop list")
	}
}

func TestEstimateOrderByCost(t *testing.T) {
	cm := NewCostModel()

	nRows := NewLogEst(1000)
	cost := cm.EstimateOrderByCost(nRows)

	if cost <= 0 {
		t.Error("Order by cost should be positive")
	}

	// Should be same as sort cost
	sortCost := cm.EstimateSetupCost(SetupSort, nRows)
	if cost != sortCost {
		t.Errorf("Expected cost %d, got %d", sortCost, cost)
	}
}

func TestCheckOrderByOptimization(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		Columns: []IndexColumn{
			{Name: "col1", Index: 0, Ascending: true},
			{Name: "col2", Index: 1, Ascending: true},
			{Name: "col3", Index: 2, Ascending: false},
		},
	}

	tests := []struct {
		name     string
		orderBy  []OrderByColumn
		nEq      int
		expected bool
	}{
		{
			name: "matches after nEq",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: true},
				{Column: "col3", Ascending: false},
			},
			nEq:      1,
			expected: true,
		},
		{
			name: "doesn't match column",
			orderBy: []OrderByColumn{
				{Column: "col4", Ascending: true},
			},
			nEq:      1,
			expected: false,
		},
		{
			name: "doesn't match direction",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: false},
			},
			nEq:      1,
			expected: false,
		},
		{
			name:     "all columns used for equality",
			orderBy:  []OrderByColumn{{Column: "col1", Ascending: true}},
			nEq:      3,
			expected: false,
		},
		{
			name: "not enough index columns",
			orderBy: []OrderByColumn{
				{Column: "col2", Ascending: true},
				{Column: "col3", Ascending: false},
				{Column: "col4", Ascending: true},
			},
			nEq:      1,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.CheckOrderByOptimization(index, tt.orderBy, tt.nEq)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEstimateOutputRowsComprehensive(t *testing.T) {
	cm := NewCostModel()

	index := &IndexInfo{
		RowLogEst: NewLogEst(10000),
		ColumnStats: []LogEst{
			NewLogEst(100),
			NewLogEst(10),
			NewLogEst(1),
		},
	}

	tests := []struct {
		nEq      int
		expected LogEst
	}{
		{0, index.RowLogEst},
		{1, NewLogEst(100)},
		{2, NewLogEst(10)},
		{3, NewLogEst(1)},
	}

	for _, tt := range tests {
		result := cm.estimateOutputRows(index, tt.nEq)
		if result != tt.expected {
			t.Errorf("For nEq=%d, expected %d, got %d", tt.nEq, tt.expected, result)
		}
	}
}

func TestApplySelectivityReductionsComprehensive(t *testing.T) {
	cm := NewCostModel()

	nOut := NewLogEst(10000)

	result := cm.applySelectivityReductions(nOut, 3)

	// Should be significantly reduced
	if result >= nOut {
		t.Error("Output should be reduced")
	}

	// Should not go negative
	if result < 0 {
		t.Error("Output should not be negative")
	}
}

func TestCalculateLookupCostComprehensive(t *testing.T) {
	cm := NewCostModel()

	nOut := NewLogEst(100)

	costNonCovering := cm.calculateLookupCost(nOut, false)
	costCovering := cm.calculateLookupCost(nOut, true)

	if costCovering >= costNonCovering {
		t.Error("Covering index should be cheaper")
	}

	if costNonCovering <= 0 || costCovering <= 0 {
		t.Error("Costs should be positive")
	}
}

func TestEstimateUniqueLookupComprehensive(t *testing.T) {
	cm := NewCostModel()

	costNonCovering, nOutNonCovering := cm.estimateUniqueLookup(false)
	costCovering, nOutCovering := cm.estimateUniqueLookup(true)

	// Both should return 1 row
	if nOutNonCovering != 0 || nOutCovering != 0 {
		t.Error("Unique lookup should return exactly 1 row (LogEst=0)")
	}

	// Covering should be cheaper
	if costCovering >= costNonCovering {
		t.Error("Covering unique lookup should be cheaper")
	}
}
