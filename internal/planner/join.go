package planner

import (
	"fmt"
	"math"
)

// JoinAlgorithm represents the type of join algorithm to use.
type JoinAlgorithm int

const (
	JoinNestedLoop JoinAlgorithm = iota // Nested loop join (default)
	JoinHash                             // Hash join
	JoinMerge                            // Merge join (for sorted inputs)
)

// String returns the string representation of a join algorithm.
func (ja JoinAlgorithm) String() string {
	switch ja {
	case JoinNestedLoop:
		return "NestedLoop"
	case JoinHash:
		return "Hash"
	case JoinMerge:
		return "Merge"
	default:
		return "Unknown"
	}
}

// JoinOrder represents a specific ordering of tables in a join query.
// This is used for dynamic programming join ordering optimization.
type JoinOrder struct {
	// Tables is the ordered list of table indices to join
	Tables []int

	// Cost is the estimated total cost for this join order
	Cost LogEst

	// RowCount is the estimated number of output rows
	RowCount LogEst

	// Algorithm specifies the join algorithm to use for each join step
	// Algorithm[i] is the algorithm for joining Tables[i] with the result
	// of joining Tables[0..i-1]
	Algorithm []JoinAlgorithm

	// JoinConditions maps table pairs to their join conditions
	// Key is "leftIdx-rightIdx", value is list of join terms
	JoinConditions map[string][]*WhereTerm
}

// String returns a string representation of the join order.
func (jo *JoinOrder) String() string {
	s := fmt.Sprintf("JoinOrder[tables=%v cost=%d rows=%d algorithms=%v]",
		jo.Tables, jo.Cost, jo.RowCount, jo.Algorithm)
	return s
}

// JoinOptimizer handles join order optimization and algorithm selection.
type JoinOptimizer struct {
	CostModel *CostModel
	Tables    []*TableInfo
	WhereInfo *WhereInfo
}

// NewJoinOptimizer creates a new join optimizer.
func NewJoinOptimizer(tables []*TableInfo, whereInfo *WhereInfo, costModel *CostModel) *JoinOptimizer {
	return &JoinOptimizer{
		CostModel: costModel,
		Tables:    tables,
		WhereInfo: whereInfo,
	}
}

// DynamicProgrammingJoinOrder finds the optimal join order using dynamic programming.
// This implements a simplified version of the DPccp algorithm used by SQLite.
//
// The algorithm works as follows:
// 1. Start with single-table access paths
// 2. For each subset size k = 2..n:
//    - For each subset S of size k:
//      - For each split of S into S1 and S2:
//        - Compute cost of joining best(S1) with best(S2)
//        - Keep the best plan for S
// 3. Return the best plan for the full set of tables
func (jo *JoinOptimizer) DynamicProgrammingJoinOrder() (*JoinOrder, error) {
	nTables := len(jo.Tables)

	if nTables == 0 {
		return nil, fmt.Errorf("no tables to join")
	}

	// Single table - no join needed
	if nTables == 1 {
		return &JoinOrder{
			Tables:         []int{0},
			Cost:           0,
			RowCount:       jo.Tables[0].RowLogEst,
			Algorithm:      []JoinAlgorithm{},
			JoinConditions: make(map[string][]*WhereTerm),
		}, nil
	}

	// Use bitmask-based DP for efficiency (supports up to 64 tables)
	if nTables > 64 {
		return nil, fmt.Errorf("too many tables for join optimization: %d (max 64)", nTables)
	}

	// bestPlan[bitmask] = best plan for that subset of tables
	bestPlan := make(map[uint64]*JoinOrder)

	// Initialize single-table plans
	for i := 0; i < nTables; i++ {
		mask := uint64(1 << uint(i))
		bestPlan[mask] = &JoinOrder{
			Tables:         []int{i},
			Cost:           jo.estimateSingleTableCost(i),
			RowCount:       jo.Tables[i].RowLogEst,
			Algorithm:      []JoinAlgorithm{},
			JoinConditions: make(map[string][]*WhereTerm),
		}
	}

	// Build up larger subsets
	for size := 2; size <= nTables; size++ {
		// Enumerate all subsets of size 'size'
		jo.enumerateSubsets(nTables, size, func(subset uint64) {
			bestCost := LogEst(math.MaxInt16)
			var bestOrder *JoinOrder

			// Try all ways to split this subset into two non-empty parts
			for left := subset; left > 0; left = (left - 1) & subset {
				if left == 0 || left == subset {
					continue
				}
				right := subset &^ left

				leftPlan, leftOK := bestPlan[left]
				rightPlan, rightOK := bestPlan[right]

				if !leftOK || !rightOK {
					continue
				}

				// Try both join orders: left JOIN right and right JOIN left
				for _, swap := range []bool{false, true} {
					var outer, inner *JoinOrder
					if swap {
						outer, inner = rightPlan, leftPlan
					} else {
						outer, inner = leftPlan, rightPlan
					}

					// Estimate cost of this join
					cost, rowCount, algorithm := jo.estimateJoinCost(outer, inner)

					if cost < bestCost {
						bestCost = cost
						// Create new join order by combining outer and inner
						bestOrder = jo.combineJoinOrders(outer, inner, algorithm, cost, rowCount)
					}
				}
			}

			if bestOrder != nil {
				bestPlan[subset] = bestOrder
			}
		})
	}

	// Return the plan for all tables
	fullMask := (uint64(1) << uint(nTables)) - 1
	if plan, ok := bestPlan[fullMask]; ok {
		return plan, nil
	}

	return nil, fmt.Errorf("failed to find join order for all tables")
}

// enumerateSubsets calls fn for each subset of size k from n elements.
func (jo *JoinOptimizer) enumerateSubsets(n, k int, fn func(uint64)) {
	// Generate all k-bit subsets using Gosper's hack
	if k == 0 || k > n {
		return
	}

	subset := (uint64(1) << uint(k)) - 1
	limit := uint64(1) << uint(n)

	for subset < limit {
		fn(subset)

		// Gosper's hack to get next k-bit subset
		c := subset & -subset
		r := subset + c
		subset = (((r ^ subset) >> 2) / c) | r

		if subset == 0 {
			break
		}
	}
}

// estimateSingleTableCost estimates the cost of accessing a single table.
func (jo *JoinOptimizer) estimateSingleTableCost(tableIdx int) LogEst {
	// Use the best WhereLoop for this table if available
	if jo.WhereInfo != nil {
		for _, loop := range jo.WhereInfo.AllLoops {
			if loop.TabIndex == tableIdx {
				return jo.CostModel.CalculateLoopCost(loop)
			}
		}
	}

	// Fallback to full scan cost
	cost, _ := jo.CostModel.EstimateFullScan(jo.Tables[tableIdx])
	return cost
}

// estimateJoinCost estimates the cost of joining two table sets.
func (jo *JoinOptimizer) estimateJoinCost(outer, inner *JoinOrder) (cost LogEst, rowCount LogEst, algorithm JoinAlgorithm) {
	// Get join conditions between outer and inner tables
	joinTerms := jo.findJoinConditions(outer, inner)

	// Select the best join algorithm
	algorithm = jo.SelectJoinAlgorithm(outer, inner, joinTerms)

	// Estimate cost based on algorithm
	cost, rowCount = jo.CostEstimate(outer, inner, algorithm, joinTerms)

	return
}

// findJoinConditions finds WHERE terms that connect outer and inner table sets.
func (jo *JoinOptimizer) findJoinConditions(outer, inner *JoinOrder) []*WhereTerm {
	if jo.WhereInfo == nil || jo.WhereInfo.Clause == nil {
		return nil
	}

	outerMask := jo.tablesToBitmask(outer.Tables)
	innerMask := jo.tablesToBitmask(inner.Tables)

	var joinTerms []*WhereTerm
	for _, term := range jo.WhereInfo.Clause.Terms {
		// Check if term references both outer and inner tables
		if term.PrereqAll.Overlaps(outerMask) && term.PrereqAll.Overlaps(innerMask) {
			joinTerms = append(joinTerms, term)
		}
	}

	return joinTerms
}

// tablesToBitmask converts a list of table indices to a bitmask.
func (jo *JoinOptimizer) tablesToBitmask(tables []int) Bitmask {
	var mask Bitmask
	for _, t := range tables {
		mask.Set(t)
	}
	return mask
}

// SelectJoinAlgorithm selects the best join algorithm for joining two table sets.
func (jo *JoinOptimizer) SelectJoinAlgorithm(outer, inner *JoinOrder, joinTerms []*WhereTerm) JoinAlgorithm {
	// Check if we have equi-join conditions (required for hash/merge join)
	hasEquiJoin := false
	for _, term := range joinTerms {
		if term.Operator == WO_EQ {
			hasEquiJoin = true
			break
		}
	}

	if !hasEquiJoin {
		// No equi-join: must use nested loop
		return JoinNestedLoop
	}

	outerRows := outer.RowCount.ToInt()
	innerRows := inner.RowCount.ToInt()

	// Hash join is good when:
	// - We have an equi-join condition
	// - Inner table is smaller (for building hash table)
	// - Tables are not too small (overhead not worth it)
	if innerRows > 100 && innerRows < outerRows*10 {
		return JoinHash
	}

	// Merge join is good when:
	// - Both inputs are already sorted on join key
	// - We have an equi-join condition
	// For now, we don't track sort order, so we don't select merge join
	// TODO: Add sort order tracking to enable merge join

	// Default to nested loop join
	return JoinNestedLoop
}

// CostEstimate estimates the cost of a join using a specific algorithm.
func (jo *JoinOptimizer) CostEstimate(outer, inner *JoinOrder, algorithm JoinAlgorithm, joinTerms []*WhereTerm) (cost LogEst, rowCount LogEst) {
	switch algorithm {
	case JoinNestedLoop:
		return jo.estimateNestedLoopCost(outer, inner, joinTerms)
	case JoinHash:
		return jo.estimateHashJoinCost(outer, inner, joinTerms)
	case JoinMerge:
		return jo.estimateMergeJoinCost(outer, inner, joinTerms)
	default:
		return jo.estimateNestedLoopCost(outer, inner, joinTerms)
	}
}

// estimateNestedLoopCost estimates the cost of a nested loop join.
// Cost = outer_cost + outer_rows * inner_cost
func (jo *JoinOptimizer) estimateNestedLoopCost(outer, inner *JoinOrder, joinTerms []*WhereTerm) (cost LogEst, rowCount LogEst) {
	// Output rows = outer_rows * inner_rows * selectivity
	selectivity := jo.estimateSelectivity(joinTerms)
	rowCount = outer.RowCount + inner.RowCount + selectivity

	// Cost = cost to scan outer + (outer_rows * cost to scan inner for each outer row)
	cost = outer.Cost + outer.RowCount + inner.Cost

	return
}

// estimateHashJoinCost estimates the cost of a hash join.
// Cost = inner_cost (build) + outer_cost (probe) + hash_overhead
func (jo *JoinOptimizer) estimateHashJoinCost(outer, inner *JoinOrder, joinTerms []*WhereTerm) (cost LogEst, rowCount LogEst) {
	// Output rows = outer_rows * inner_rows * selectivity
	selectivity := jo.estimateSelectivity(joinTerms)
	rowCount = outer.RowCount + inner.RowCount + selectivity

	// Cost = build hash table from inner + scan outer and probe hash table
	buildCost := inner.Cost + inner.RowCount + NewLogEst(2) // +2 for hash overhead per row
	probeCost := outer.Cost + outer.RowCount + NewLogEst(1) // +1 for probe overhead per row

	cost = buildCost + probeCost

	return
}

// estimateMergeJoinCost estimates the cost of a merge join.
// Cost = outer_cost + inner_cost + merge_cost
// Assumes inputs are already sorted; if not, add sort cost.
func (jo *JoinOptimizer) estimateMergeJoinCost(outer, inner *JoinOrder, joinTerms []*WhereTerm) (cost LogEst, rowCount LogEst) {
	// Output rows = outer_rows * inner_rows * selectivity
	selectivity := jo.estimateSelectivity(joinTerms)
	rowCount = outer.RowCount + inner.RowCount + selectivity

	// Cost = scan outer + scan inner + merge overhead
	// For now, assume inputs need to be sorted
	sortOuterCost := jo.CostModel.EstimateOrderByCost(outer.RowCount)
	sortInnerCost := jo.CostModel.EstimateOrderByCost(inner.RowCount)

	mergeCost := outer.RowCount + inner.RowCount // Linear merge

	cost = outer.Cost + inner.Cost + sortOuterCost + sortInnerCost + mergeCost

	return
}

// estimateSelectivity estimates the selectivity of join conditions.
// Returns a LogEst value representing the reduction factor.
func (jo *JoinOptimizer) estimateSelectivity(joinTerms []*WhereTerm) LogEst {
	if len(joinTerms) == 0 {
		// Cross product: no reduction
		return LogEst(0)
	}

	// Each equi-join typically reduces by ~10x
	// Each range condition reduces by ~4x
	selectivity := LogEst(0)
	for _, term := range joinTerms {
		if term.Operator == WO_EQ {
			selectivity += selectivityEq // ~-10 (divide by 1024)
		} else if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
			selectivity += selectivityRange // ~-3 (divide by 8)
		}
	}

	return selectivity
}

// combineJoinOrders combines two join orders into a single order.
func (jo *JoinOptimizer) combineJoinOrders(outer, inner *JoinOrder, algorithm JoinAlgorithm, cost, rowCount LogEst) *JoinOrder {
	// Combine table lists
	tables := make([]int, len(outer.Tables)+len(inner.Tables))
	copy(tables, outer.Tables)
	copy(tables[len(outer.Tables):], inner.Tables)

	// Combine algorithms
	algorithms := make([]JoinAlgorithm, len(outer.Algorithm)+1)
	copy(algorithms, outer.Algorithm)
	algorithms[len(algorithms)-1] = algorithm

	// Copy join conditions
	joinConditions := make(map[string][]*WhereTerm)
	for k, v := range outer.JoinConditions {
		joinConditions[k] = v
	}
	for k, v := range inner.JoinConditions {
		joinConditions[k] = v
	}

	return &JoinOrder{
		Tables:         tables,
		Cost:           cost,
		RowCount:       rowCount,
		Algorithm:      algorithms,
		JoinConditions: joinConditions,
	}
}

// GreedyJoinOrder implements a greedy join ordering heuristic.
// This is faster than DP but may not find the optimal order.
// Used as a fallback for queries with many tables.
func (jo *JoinOptimizer) GreedyJoinOrder() (*JoinOrder, error) {
	nTables := len(jo.Tables)
	if nTables == 0 {
		return nil, fmt.Errorf("no tables to join")
	}

	if nTables == 1 {
		return &JoinOrder{
			Tables:         []int{0},
			Cost:           0,
			RowCount:       jo.Tables[0].RowLogEst,
			Algorithm:      []JoinAlgorithm{},
			JoinConditions: make(map[string][]*WhereTerm),
		}, nil
	}

	remaining := make(map[int]bool)
	for i := 0; i < nTables; i++ {
		remaining[i] = true
	}

	// Start with the smallest table
	smallestIdx := jo.findSmallestTable(remaining)
	delete(remaining, smallestIdx)

	currentOrder := &JoinOrder{
		Tables:         []int{smallestIdx},
		Cost:           jo.estimateSingleTableCost(smallestIdx),
		RowCount:       jo.Tables[smallestIdx].RowLogEst,
		Algorithm:      []JoinAlgorithm{},
		JoinConditions: make(map[string][]*WhereTerm),
	}

	// Greedily add tables one at a time
	for len(remaining) > 0 {
		bestIdx := -1
		bestCost := LogEst(math.MaxInt16)
		var bestAlgorithm JoinAlgorithm
		var bestRowCount LogEst

		// Try each remaining table
		for tableIdx := range remaining {
			innerOrder := &JoinOrder{
				Tables:         []int{tableIdx},
				Cost:           jo.estimateSingleTableCost(tableIdx),
				RowCount:       jo.Tables[tableIdx].RowLogEst,
				Algorithm:      []JoinAlgorithm{},
				JoinConditions: make(map[string][]*WhereTerm),
			}

			joinTerms := jo.findJoinConditions(currentOrder, innerOrder)
			algorithm := jo.SelectJoinAlgorithm(currentOrder, innerOrder, joinTerms)
			cost, rowCount := jo.CostEstimate(currentOrder, innerOrder, algorithm, joinTerms)

			if cost < bestCost {
				bestCost = cost
				bestIdx = tableIdx
				bestAlgorithm = algorithm
				bestRowCount = rowCount
			}
		}

		if bestIdx == -1 {
			return nil, fmt.Errorf("failed to find next table in greedy join order")
		}

		// Add the best table
		innerOrder := &JoinOrder{
			Tables:         []int{bestIdx},
			Cost:           jo.estimateSingleTableCost(bestIdx),
			RowCount:       jo.Tables[bestIdx].RowLogEst,
			Algorithm:      []JoinAlgorithm{},
			JoinConditions: make(map[string][]*WhereTerm),
		}

		currentOrder = jo.combineJoinOrders(currentOrder, innerOrder, bestAlgorithm, bestCost, bestRowCount)
		delete(remaining, bestIdx)
	}

	return currentOrder, nil
}

// findSmallestTable returns the index of the table with the fewest rows.
func (jo *JoinOptimizer) findSmallestTable(candidates map[int]bool) int {
	smallestIdx := -1
	smallestRows := int64(math.MaxInt64)

	for idx := range candidates {
		if jo.Tables[idx].RowCount < smallestRows {
			smallestRows = jo.Tables[idx].RowCount
			smallestIdx = idx
		}
	}

	return smallestIdx
}

// OptimizeJoinOrder chooses the best join ordering algorithm based on table count.
func (jo *JoinOptimizer) OptimizeJoinOrder() (*JoinOrder, error) {
	nTables := len(jo.Tables)

	// For small numbers of tables, use dynamic programming
	if nTables <= 8 {
		return jo.DynamicProgrammingJoinOrder()
	}

	// For larger numbers of tables, use greedy heuristic
	return jo.GreedyJoinOrder()
}
