// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"fmt"
)

// JoinNode represents a node in the join execution tree.
type JoinNode struct {
	// Algorithm specifies which join algorithm to use
	Algorithm JoinAlgorithm

	// Left is the left input (outer) to the join
	Left *JoinNode

	// Right is the right input (inner) to the join
	Right *JoinNode

	// TableIndex is the table index if this is a leaf node
	TableIndex int

	// IsLeaf indicates if this is a leaf node (single table access)
	IsLeaf bool

	// JoinConditions are the predicates for this join
	JoinConditions []*WhereTerm

	// EstimatedCost is the estimated cost of this join
	EstimatedCost LogEst

	// EstimatedRows is the estimated number of output rows
	EstimatedRows LogEst
}

// String returns a string representation of the join node.
func (jn *JoinNode) String() string {
	if jn.IsLeaf {
		return fmt.Sprintf("Scan(table=%d)", jn.TableIndex)
	}
	return fmt.Sprintf("%s(%s, %s)", jn.Algorithm, jn.Left.String(), jn.Right.String())
}

// BuildJoinTree converts a JoinOrder into a tree of JoinNodes.
func BuildJoinTree(order *JoinOrder, tables []*TableInfo, whereInfo *WhereInfo) *JoinNode {
	if len(order.Tables) == 1 {
		// Single table - leaf node
		return &JoinNode{
			TableIndex:    order.Tables[0],
			IsLeaf:        true,
			EstimatedCost: order.Cost,
			EstimatedRows: order.RowCount,
		}
	}

	// For simplicity, build a left-deep tree
	// More sophisticated implementations could build bushy trees
	return buildLeftDeepTree(order, tables, whereInfo, 0)
}

// buildLeftDeepTree builds a left-deep join tree from a join order.
func buildLeftDeepTree(order *JoinOrder, tables []*TableInfo, whereInfo *WhereInfo, startIdx int) *JoinNode {
	if startIdx >= len(order.Tables) {
		return nil
	}

	if startIdx == len(order.Tables)-1 {
		// Last table - create leaf node
		return &JoinNode{
			TableIndex:    order.Tables[startIdx],
			IsLeaf:        true,
			EstimatedCost: tables[order.Tables[startIdx]].RowLogEst,
			EstimatedRows: tables[order.Tables[startIdx]].RowLogEst,
		}
	}

	// Create leaf for current table
	left := &JoinNode{
		TableIndex:    order.Tables[startIdx],
		IsLeaf:        true,
		EstimatedCost: tables[order.Tables[startIdx]].RowLogEst,
		EstimatedRows: tables[order.Tables[startIdx]].RowLogEst,
	}

	// Recursively build right subtree
	right := buildLeftDeepTree(order, tables, whereInfo, startIdx+1)

	// Determine algorithm
	algorithm := JoinNestedLoop
	if startIdx < len(order.Algorithm) {
		algorithm = order.Algorithm[startIdx]
	}

	// Create join node
	return &JoinNode{
		Algorithm:      algorithm,
		Left:           left,
		Right:          right,
		IsLeaf:         false,
		JoinConditions: nil, // Would be populated from whereInfo
		EstimatedCost:  order.Cost,
		EstimatedRows:  order.RowCount,
	}
}

// NestedLoopJoinPlanner plans a nested loop join.
type NestedLoopJoinPlanner struct {
	Outer          *JoinNode
	Inner          *JoinNode
	JoinConditions []*WhereTerm
	CostModel      *CostModel
}

// Plan generates the execution plan for a nested loop join.
func (nlj *NestedLoopJoinPlanner) Plan() (*NestedLoopJoinPlan, error) {
	return &NestedLoopJoinPlan{
		Outer:          nlj.Outer,
		Inner:          nlj.Inner,
		JoinConditions: nlj.JoinConditions,
		EstimatedCost:  nlj.estimateCost(),
	}, nil
}

// estimateCost estimates the cost of the nested loop join.
func (nlj *NestedLoopJoinPlanner) estimateCost() LogEst {
	// Cost = outer cost + (outer rows * inner cost)
	outerCost := nlj.Outer.EstimatedCost
	innerCost := nlj.Inner.EstimatedCost
	outerRows := nlj.Outer.EstimatedRows

	return outerCost + outerRows + innerCost
}

// NestedLoopJoinPlan represents the execution plan for a nested loop join.
type NestedLoopJoinPlan struct {
	Outer          *JoinNode
	Inner          *JoinNode
	JoinConditions []*WhereTerm
	EstimatedCost  LogEst
}

// Execute describes how to execute the nested loop join.
// This is conceptual - actual execution happens in VDBE.
func (plan *NestedLoopJoinPlan) Execute() string {
	return fmt.Sprintf("FOR each row in %s\n  FOR each row in %s\n    IF join conditions match\n      OUTPUT row",
		plan.Outer.String(), plan.Inner.String())
}

// HashJoinPlanner plans a hash join.
type HashJoinPlanner struct {
	Build          *JoinNode // Table to build hash table from (typically smaller)
	Probe          *JoinNode // Table to probe hash table with
	JoinConditions []*WhereTerm
	JoinKeys       []int // Column indices for join keys
	CostModel      *CostModel
}

// Plan generates the execution plan for a hash join.
func (hj *HashJoinPlanner) Plan() (*HashJoinPlan, error) {
	// Validate that we have equi-join conditions
	if len(hj.JoinKeys) == 0 {
		return nil, fmt.Errorf("hash join requires at least one equi-join condition")
	}

	return &HashJoinPlan{
		Build:          hj.Build,
		Probe:          hj.Probe,
		JoinConditions: hj.JoinConditions,
		JoinKeys:       hj.JoinKeys,
		EstimatedCost:  hj.estimateCost(),
		HashTableSize:  hj.estimateHashTableSize(),
	}, nil
}

// estimateCost estimates the cost of the hash join.
func (hj *HashJoinPlanner) estimateCost() LogEst {
	// Cost = build cost + probe cost + hash overhead
	buildCost := hj.Build.EstimatedCost + hj.Build.EstimatedRows + NewLogEst(2)
	probeCost := hj.Probe.EstimatedCost + hj.Probe.EstimatedRows + NewLogEst(1)
	return buildCost + probeCost
}

// estimateHashTableSize estimates the size of the hash table in memory.
func (hj *HashJoinPlanner) estimateHashTableSize() int64 {
	// Rough estimate: number of rows * average row size
	// Assume ~100 bytes per row on average
	rows := hj.Build.EstimatedRows.ToInt()
	return rows * 100
}

// HashJoinPlan represents the execution plan for a hash join.
type HashJoinPlan struct {
	Build          *JoinNode
	Probe          *JoinNode
	JoinConditions []*WhereTerm
	JoinKeys       []int
	EstimatedCost  LogEst
	HashTableSize  int64
}

// Execute describes how to execute the hash join.
func (plan *HashJoinPlan) Execute() string {
	return fmt.Sprintf("BUILD hash table from %s on keys %v\nPROBE with %s\n  IF hash lookup succeeds AND join conditions match\n    OUTPUT row",
		plan.Build.String(), plan.JoinKeys, plan.Probe.String())
}

// MergeJoinPlanner plans a merge join (sort-merge join).
type MergeJoinPlanner struct {
	Left           *JoinNode
	Right          *JoinNode
	JoinConditions []*WhereTerm
	JoinKeys       []int // Column indices for join keys
	LeftSorted     bool  // True if left input is already sorted
	RightSorted    bool  // True if right input is already sorted
	CostModel      *CostModel
}

// Plan generates the execution plan for a merge join.
func (mj *MergeJoinPlanner) Plan() (*MergeJoinPlan, error) {
	// Validate that we have equi-join conditions
	if len(mj.JoinKeys) == 0 {
		return nil, fmt.Errorf("merge join requires at least one equi-join condition")
	}

	return &MergeJoinPlan{
		Left:           mj.Left,
		Right:          mj.Right,
		JoinConditions: mj.JoinConditions,
		JoinKeys:       mj.JoinKeys,
		LeftSorted:     mj.LeftSorted,
		RightSorted:    mj.RightSorted,
		EstimatedCost:  mj.estimateCost(),
	}, nil
}

// estimateCost estimates the cost of the merge join.
func (mj *MergeJoinPlanner) estimateCost() LogEst {
	leftCost := mj.Left.EstimatedCost
	rightCost := mj.Right.EstimatedCost

	// Add sort costs if needed
	if !mj.LeftSorted {
		leftCost += mj.CostModel.EstimateOrderByCost(mj.Left.EstimatedRows)
	}
	if !mj.RightSorted {
		rightCost += mj.CostModel.EstimateOrderByCost(mj.Right.EstimatedRows)
	}

	// Merge cost is linear in both inputs
	mergeCost := mj.Left.EstimatedRows + mj.Right.EstimatedRows

	return leftCost + rightCost + mergeCost
}

// MergeJoinPlan represents the execution plan for a merge join.
type MergeJoinPlan struct {
	Left           *JoinNode
	Right          *JoinNode
	JoinConditions []*WhereTerm
	JoinKeys       []int
	LeftSorted     bool
	RightSorted    bool
	EstimatedCost  LogEst
}

// Execute describes how to execute the merge join.
func (plan *MergeJoinPlan) Execute() string {
	sortInfo := ""
	if !plan.LeftSorted {
		sortInfo += fmt.Sprintf("SORT %s on keys %v\n", plan.Left.String(), plan.JoinKeys)
	}
	if !plan.RightSorted {
		sortInfo += fmt.Sprintf("SORT %s on keys %v\n", plan.Right.String(), plan.JoinKeys)
	}
	return fmt.Sprintf("%sMERGE %s and %s on keys %v\n  IF join conditions match\n    OUTPUT row",
		sortInfo, plan.Left.String(), plan.Right.String(), plan.JoinKeys)
}

// JoinConditionAnalyzer analyzes join conditions to extract useful information.
type JoinConditionAnalyzer struct {
	Conditions []*WhereTerm
}

// NewJoinConditionAnalyzer creates a new analyzer.
func NewJoinConditionAnalyzer(conditions []*WhereTerm) *JoinConditionAnalyzer {
	return &JoinConditionAnalyzer{
		Conditions: conditions,
	}
}

// HasEquiJoin checks if there are any equi-join conditions.
func (jca *JoinConditionAnalyzer) HasEquiJoin() bool {
	for _, term := range jca.Conditions {
		if term.Operator == WO_EQ {
			return true
		}
	}
	return false
}

// ExtractEquiJoinKeys extracts the column indices for equi-join conditions.
func (jca *JoinConditionAnalyzer) ExtractEquiJoinKeys() []int {
	keys := make([]int, 0)
	for _, term := range jca.Conditions {
		if term.Operator == WO_EQ && term.LeftColumn >= 0 {
			keys = append(keys, term.LeftColumn)
		}
	}
	return keys
}

// GetSelectivity estimates the selectivity of the join conditions.
func (jca *JoinConditionAnalyzer) GetSelectivity() float64 {
	// Simple heuristic: each equi-join reduces by 10x, each range by 4x
	selectivity := 1.0
	for _, term := range jca.Conditions {
		if term.Operator == WO_EQ {
			selectivity *= 0.1 // Divide by 10
		} else if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
			selectivity *= 0.25 // Divide by 4
		}
	}
	return selectivity
}

// IsHashJoinEligible checks if the join conditions are suitable for hash join.
func (jca *JoinConditionAnalyzer) IsHashJoinEligible() bool {
	// Need at least one equi-join
	return jca.HasEquiJoin()
}

// IsMergeJoinEligible checks if the join conditions are suitable for merge join.
func (jca *JoinConditionAnalyzer) IsMergeJoinEligible() bool {
	// Need at least one equi-join
	// Also benefit from having a single join key (multi-key merge is complex)
	keys := jca.ExtractEquiJoinKeys()
	return len(keys) > 0
}

// JoinAlgorithmSelector helps select the best join algorithm.
type JoinAlgorithmSelector struct {
	Outer          *JoinNode
	Inner          *JoinNode
	JoinConditions []*WhereTerm
	CostModel      *CostModel
}

// NewJoinAlgorithmSelector creates a new selector.
func NewJoinAlgorithmSelector(outer, inner *JoinNode, conditions []*WhereTerm, costModel *CostModel) *JoinAlgorithmSelector {
	return &JoinAlgorithmSelector{
		Outer:          outer,
		Inner:          inner,
		JoinConditions: conditions,
		CostModel:      costModel,
	}
}

// SelectBest selects the best join algorithm based on cost estimation.
func (jas *JoinAlgorithmSelector) SelectBest() JoinAlgorithm {
	analyzer := NewJoinConditionAnalyzer(jas.JoinConditions)

	// If no equi-join, only nested loop is possible
	if !analyzer.HasEquiJoin() {
		return JoinNestedLoop
	}

	// Estimate costs for each algorithm
	nestedLoopCost := jas.estimateNestedLoopCost()
	hashJoinCost := jas.estimateHashJoinCost()
	mergeJoinCost := jas.estimateMergeJoinCost()

	// Select the algorithm with minimum cost
	minCost := nestedLoopCost
	bestAlgorithm := JoinNestedLoop

	if analyzer.IsHashJoinEligible() && hashJoinCost < minCost {
		minCost = hashJoinCost
		bestAlgorithm = JoinHash
	}

	if analyzer.IsMergeJoinEligible() && mergeJoinCost < minCost {
		bestAlgorithm = JoinMerge
	}

	return bestAlgorithm
}

// estimateNestedLoopCost estimates the cost of nested loop join.
func (jas *JoinAlgorithmSelector) estimateNestedLoopCost() LogEst {
	planner := &NestedLoopJoinPlanner{
		Outer:          jas.Outer,
		Inner:          jas.Inner,
		JoinConditions: jas.JoinConditions,
		CostModel:      jas.CostModel,
	}
	return planner.estimateCost()
}

// estimateHashJoinCost estimates the cost of hash join.
func (jas *JoinAlgorithmSelector) estimateHashJoinCost() LogEst {
	// Determine which side to build (typically the smaller one)
	build, probe := jas.Inner, jas.Outer
	if jas.Outer.EstimatedRows < jas.Inner.EstimatedRows {
		build, probe = jas.Outer, jas.Inner
	}

	analyzer := NewJoinConditionAnalyzer(jas.JoinConditions)
	planner := &HashJoinPlanner{
		Build:          build,
		Probe:          probe,
		JoinConditions: jas.JoinConditions,
		JoinKeys:       analyzer.ExtractEquiJoinKeys(),
		CostModel:      jas.CostModel,
	}
	return planner.estimateCost()
}

// estimateMergeJoinCost estimates the cost of merge join.
func (jas *JoinAlgorithmSelector) estimateMergeJoinCost() LogEst {
	analyzer := NewJoinConditionAnalyzer(jas.JoinConditions)
	planner := &MergeJoinPlanner{
		Left:           jas.Outer,
		Right:          jas.Inner,
		JoinConditions: jas.JoinConditions,
		JoinKeys:       analyzer.ExtractEquiJoinKeys(),
		LeftSorted:     false, // Conservative: assume not sorted
		RightSorted:    false, // Conservative: assume not sorted
		CostModel:      jas.CostModel,
	}
	return planner.estimateCost()
}
