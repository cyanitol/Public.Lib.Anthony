// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"math"
)

// Cost estimation constants
const (
	// Default cost values (in LogEst units)
	costFullScan     = LogEst(100) // Cost per row for full table scan
	costIndexSeek    = LogEst(10)  // Cost to seek to a position in index
	costIndexNext    = LogEst(5)   // Cost to move to next index entry
	costRowidLookup  = LogEst(19)  // Cost to lookup row by rowid (~19 = log2(500k))
	costComparison   = LogEst(2)   // Cost of a single comparison
	costInMemoryScan = LogEst(1)   // Cost per row when data is in memory

	// Selectivity estimates (how many rows will match)
	selectivityEq      = LogEst(-10) // x = const: ~1/1024 rows (highly selective)
	selectivityRange   = LogEst(-3)  // x > const: ~1/8 rows
	selectivityIn      = LogEst(-7)  // x IN (...): depends on list size
	selectivityNull    = LogEst(-20) // x IS NULL: very few nulls
	selectivityLikePat = LogEst(-4)  // LIKE with pattern: ~1/16 rows

	// Heuristic adjustments
	truthProbDefault    = LogEst(1)   // Default truth probability
	truthProbSmallInt   = LogEst(-50) // For x=0, x=1, x=-1 (very likely)
	truthProbLargeRange = LogEst(-5)  // For range on large index
)

// CostModel handles cost estimation for different access paths.
type CostModel struct {
	// Configuration options
	UseStatistics bool // Use sqlite_stat1 statistics if available
}

// NewCostModel creates a new cost model with default settings.
func NewCostModel() *CostModel {
	return &CostModel{
		UseStatistics: true,
	}
}

// EstimateFullScan estimates the cost of a full table scan.
func (c *CostModel) EstimateFullScan(table *TableInfo) (cost LogEst, nOut LogEst) {
	// Cost is approximately: number of rows * cost per row
	nRows := table.RowLogEst
	cost = nRows + costFullScan
	nOut = nRows
	return
}

// EstimateIndexScan estimates the cost of scanning an index with given constraints.
func (c *CostModel) EstimateIndexScan(
	table *TableInfo,
	index *IndexInfo,
	terms []*WhereTerm,
	nEq int,
	hasRange bool,
	covering bool,
) (cost LogEst, nOut LogEst) {

	nRows := index.RowLogEst
	nOut = estimateOutputRows(index, nRows, nEq, hasRange)
	cost = calculateScanCost(nOut, covering)

	return
}

// estimateOutputRows estimates the number of output rows based on constraints.
func estimateOutputRows(index *IndexInfo, nRows LogEst, nEq int, hasRange bool) LogEst {
	nOut := nRows
	nOut = applyEqConstraints(index, nOut, nEq)
	nOut = applyRangeConstraint(nOut, hasRange)
	return nOut
}

// applyEqConstraints applies equality constraint selectivity.
func applyEqConstraints(index *IndexInfo, nOut LogEst, nEq int) LogEst {
	if nEq <= 0 {
		return nOut
	}

	if nEq < len(index.ColumnStats) {
		return index.ColumnStats[nEq]
	}

	return estimateWithExtrapolation(nOut, nEq)
}

// estimateWithExtrapolation estimates output with extrapolated selectivity.
func estimateWithExtrapolation(nOut LogEst, nEq int) LogEst {
	for i := 0; i < nEq; i++ {
		nOut += selectivityEq
		if nOut < 0 {
			return 0
		}
	}
	return nOut
}

// applyRangeConstraint applies range constraint selectivity.
func applyRangeConstraint(nOut LogEst, hasRange bool) LogEst {
	if !hasRange {
		return nOut
	}
	nOut += selectivityRange
	if nOut < 0 {
		return 0
	}
	return nOut
}

// calculateScanCost calculates total scan cost.
func calculateScanCost(nOut LogEst, covering bool) LogEst {
	seekCost := costIndexSeek
	scanCost := nOut + costIndexNext
	lookupCost := calculateLookupCost(nOut, covering)
	return seekCost + scanCost + lookupCost
}

// calculateLookupCost calculates the cost of row lookups.
func calculateLookupCost(nOut LogEst, covering bool) LogEst {
	if covering {
		return 0
	}
	return nOut + costRowidLookup
}

// EstimateIndexLookup estimates the cost of an exact index lookup (all columns = const).
func (c *CostModel) EstimateIndexLookup(
	table *TableInfo,
	index *IndexInfo,
	nEq int,
	covering bool,
) (cost LogEst, nOut LogEst) {
	if index.Unique && nEq >= len(index.Columns) {
		return c.estimateUniqueLookup(covering)
	}
	nOut = c.estimateOutputRows(index, nEq)
	cost = c.calculateLookupCost(nOut, covering)
	return
}

// estimateUniqueLookup returns cost for unique index with all columns specified.
func (c *CostModel) estimateUniqueLookup(covering bool) (cost LogEst, nOut LogEst) {
	nOut = 0
	cost = costIndexSeek
	if !covering {
		cost += costRowidLookup
	}
	return
}

// estimateOutputRows estimates the number of rows for a given number of equality constraints.
func (c *CostModel) estimateOutputRows(index *IndexInfo, nEq int) LogEst {
	if nEq > 0 && nEq <= len(index.ColumnStats) {
		return index.ColumnStats[nEq-1]
	}
	return c.applySelectivityReductions(index.RowLogEst, nEq)
}

// applySelectivityReductions reduces row estimate by selectivity per equality.
func (c *CostModel) applySelectivityReductions(nOut LogEst, nEq int) LogEst {
	for i := 0; i < nEq; i++ {
		nOut += selectivityEq
		if nOut < 0 {
			return 0
		}
	}
	return nOut
}

// calculateLookupCost computes total cost for lookup.
func (c *CostModel) calculateLookupCost(nOut LogEst, covering bool) LogEst {
	cost := costIndexSeek + nOut + costIndexNext
	if !covering {
		cost += nOut + costRowidLookup
	}
	return cost
}

// EstimateRowidLookup estimates the cost of looking up a row by rowid/primary key.
func (c *CostModel) EstimateRowidLookup() (cost LogEst, nOut LogEst) {
	// Single row lookup by rowid is very fast
	nOut = 0 // 1 row
	cost = costIndexSeek
	return
}

// EstimateInOperator estimates the cost and selectivity of an IN operator.
func (c *CostModel) EstimateInOperator(
	table *TableInfo,
	index *IndexInfo,
	nEq int,
	inListSize int,
	covering bool,
) (cost LogEst, nOut LogEst) {

	// Each value in the IN list requires a separate lookup
	nIn := NewLogEst(int64(inListSize))

	// Estimate rows per lookup
	var rowsPerLookup LogEst
	if nEq > 0 && nEq <= len(index.ColumnStats) {
		rowsPerLookup = index.ColumnStats[nEq-1]
	} else {
		rowsPerLookup = index.RowLogEst + selectivityEq
	}

	// Total output rows = in list size * rows per lookup
	nOut = nIn + rowsPerLookup

	// Cost = (seek + scan) for each IN value
	lookupCost := costIndexSeek + rowsPerLookup + costIndexNext
	if !covering {
		lookupCost += rowsPerLookup + costRowidLookup
	}

	cost = nIn + lookupCost

	return
}

// operatorSelectivity maps operators to their default selectivity.
var operatorSelectivity = map[WhereOperator]LogEst{
	WO_IN:     selectivityIn,
	WO_ISNULL: selectivityNull,
}

// EstimateTruthProbability estimates the probability that a term is true.
// This is used to refine selectivity estimates.
func (c *CostModel) EstimateTruthProbability(term *WhereTerm) LogEst {
	if term.TruthProb != 0 {
		return term.TruthProb
	}
	if term.Operator == WO_EQ {
		return c.estimateEqSelectivity(term)
	}
	if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
		return selectivityRange
	}
	if sel, ok := operatorSelectivity[term.Operator]; ok {
		return sel
	}
	return truthProbDefault
}

// estimateEqSelectivity estimates selectivity for equality operator.
func (c *CostModel) estimateEqSelectivity(term *WhereTerm) LogEst {
	if val, ok := term.RightValue.(int); ok && val >= -1 && val <= 1 {
		return truthProbSmallInt
	}
	return selectivityEq
}

// CompareCosts compares two access paths and returns true if path1 is better (lower cost).
func (c *CostModel) CompareCosts(cost1, nOut1, cost2, nOut2 LogEst) bool {
	// Primary criterion: lower cost
	if cost1 < cost2 {
		return true
	}
	if cost1 > cost2 {
		return false
	}

	// Tie-breaker: fewer output rows
	return nOut1 < nOut2
}

// AdjustCostForMultipleTerms adjusts the cost when multiple WHERE terms
// can be evaluated together.
func (c *CostModel) AdjustCostForMultipleTerms(baseCost LogEst, nTerms int) LogEst {
	// Each additional term adds a small comparison cost
	if nTerms <= 1 {
		return baseCost
	}

	// Add comparison cost for extra terms
	extraCost := NewLogEst(int64(nTerms-1)) + costComparison
	return baseCost + extraCost
}

// EstimateSetupCost estimates one-time setup costs (e.g., creating temp index).
func (c *CostModel) EstimateSetupCost(setupType SetupType, nRows LogEst) LogEst {
	switch setupType {
	case SetupNone:
		return 0

	case SetupAutoIndex:
		// Cost to build automatic index: scan table + create index
		// Roughly: nRows * (scan + insert into index)
		return nRows + NewLogEst(50) // 50 = log2(10^5) for setup overhead

	case SetupSort:
		// Cost to sort: nRows * log(nRows)
		if nRows <= 0 {
			return 0
		}
		// Approximate: nRows + log(nRows) * 3
		logN := LogEst(math.Log2(float64(nRows.ToInt())) * 10)
		return nRows + logN + NewLogEst(3)

	case SetupBloomFilter:
		// Cost to build Bloom filter: scan + populate
		return nRows + NewLogEst(10)

	default:
		return 0
	}
}

// SetupType defines different types of setup operations.
type SetupType int

const (
	SetupNone SetupType = iota
	SetupAutoIndex
	SetupSort
	SetupBloomFilter
)

// CalculateLoopCost calculates the total cost of a WhereLoop.
func (c *CostModel) CalculateLoopCost(loop *WhereLoop) LogEst {
	// Total cost = setup cost + (run cost * number of times executed)
	// For a single loop, it runs once, so:
	return loop.Setup + loop.Run
}

// CombineLoopCosts combines costs for multiple nested loops.
func (c *CostModel) CombineLoopCosts(loops []*WhereLoop) (totalCost LogEst, totalRows LogEst) {
	if len(loops) == 0 {
		return 0, 0
	}

	totalCost = 0
	totalRows = 0

	// Outer loop multiplier (how many times inner loops execute)
	outerRows := LogEst(0) // Start with 1 row (LogEst(1) = 0)

	for _, loop := range loops {
		// Add setup cost (one-time per loop level)
		totalCost += loop.Setup

		// Run cost is multiplied by how many times this loop executes
		// (which is the number of rows from outer loops)
		runCost := loop.Run + outerRows
		totalCost += runCost

		// Update outer rows for next level
		outerRows = outerRows + loop.NOut
	}

	totalRows = outerRows
	return
}

// EstimateCoveringIndex checks if an index covers the query (all needed columns in index).
func (c *CostModel) EstimateCoveringIndex(index *IndexInfo, neededColumns []string) bool {
	// Build set of columns in index
	indexCols := make(map[string]bool)
	for _, col := range index.Columns {
		indexCols[col.Name] = true
	}

	// Check if all needed columns are covered
	for _, col := range neededColumns {
		if !indexCols[col] {
			return false
		}
	}

	return true
}

// SelectBestLoop selects the best WhereLoop from a list of candidates.
func (c *CostModel) SelectBestLoop(loops []*WhereLoop) *WhereLoop {
	if len(loops) == 0 {
		return nil
	}

	best := loops[0]
	bestCost := c.CalculateLoopCost(best)

	for i := 1; i < len(loops); i++ {
		loop := loops[i]
		cost := c.CalculateLoopCost(loop)

		if c.CompareCosts(cost, loop.NOut, bestCost, best.NOut) {
			best = loop
			bestCost = cost
		}
	}

	return best
}

// EstimateOrderByCost estimates the cost if results need to be sorted.
func (c *CostModel) EstimateOrderByCost(nRows LogEst) LogEst {
	// Sorting cost: O(n log n)
	return c.EstimateSetupCost(SetupSort, nRows)
}

// CheckOrderByOptimization checks if an index can satisfy ORDER BY without sorting.
func (c *CostModel) CheckOrderByOptimization(
	index *IndexInfo,
	orderBy []OrderByColumn,
	nEq int, // Number of equality constraints
) bool {
	// After equality constraints, remaining index columns must match ORDER BY
	if nEq >= len(index.Columns) {
		// All index columns used for equality, can't help with ORDER BY
		return false
	}

	// Check if index columns match order by columns (after equality constraints)
	for i, ob := range orderBy {
		idxCol := nEq + i
		if idxCol >= len(index.Columns) {
			// Not enough columns in index
			return false
		}

		// Check column name matches
		if index.Columns[idxCol].Name != ob.Column {
			return false
		}

		// Check sort order matches
		if index.Columns[idxCol].Ascending != ob.Ascending {
			return false
		}
	}

	return true
}

// OrderByColumn represents a column in ORDER BY clause.
type OrderByColumn struct {
	Column    string
	Ascending bool
}
