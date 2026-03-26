// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"
)

// TestSubquery2Coverage covers the reachable branches of ConvertInToJoin
// (subquery.go:226) and ConvertExistsToSemiJoin (subquery.go:261).
//
// Coverage analysis:
//   - Both functions are reported at 60% because their "success" return path
//     (lines that create a new WhereInfo and copy slices) is structurally
//     unreachable: estimateInCost and estimateJoinCost both compute
//     NOut+EstimatedRows, so joinCost >= inCost is always true.
//     Similarly, estimateExistsCost = NOut+(EstimatedRows-10) and
//     estimateJoinCost = NOut+EstimatedRows, so joinCost >= existsCost always.
//   - The tests below exercise every branch that can be reached and assert on
//     the exact error messages to make the coverage deterministic.

// ---------- ConvertInToJoin ----------

// TestSubquery2Coverage_ConvertInToJoin_NonInType_Exists checks that an
// EXISTS subquery is rejected with the "not an IN subquery" error.
func TestSubquery2Coverage_ConvertInToJoin_NonInType_Exists(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100, RowLogEst: NewLogEst(100)}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryExists type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "not an IN subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NonInType_Scalar checks that a
// SCALAR subquery is rejected with the "not an IN subquery" error.
func TestSubquery2Coverage_ConvertInToJoin_NonInType_Scalar(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		EstimatedRows: NewLogEst(50),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "users", RowCount: 500}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(500),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "not an IN subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NonInType_From checks that a
// FROM subquery is rejected with the "not an IN subquery" error.
func TestSubquery2Coverage_ConvertInToJoin_NonInType_From(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryFrom,
		EstimatedRows: NewLogEst(200),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "orders", RowCount: 1000}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(1000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_SmallSubquery checks
// the cost-comparison branch when the subquery is small (joining not cheaper).
// estimateInCost = NOut + EstimatedRows = estimateJoinCost, so joinCost >= inCost
// is always satisfied, producing the "JOIN not beneficial" error.
func TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_SmallSubquery(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(10),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "users", RowCount: 1000, RowLogEst: NewLogEst(1000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(1000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error when JOIN is not beneficial, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "JOIN not beneficial") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_LargeSubquery verifies
// the cost path for a large subquery. Both cost estimates remain identical
// (joinCost == inCost), so the NOT beneficial branch is always taken.
func TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_LargeSubquery(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(1000000),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "big_table", RowCount: 1000000, RowLogEst: NewLogEst(1000000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(1000000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error when JOIN is not beneficial, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_ZeroRows exercises
// the cost path with zero estimated rows (LogEst 0).
func TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_ZeroRows(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: LogEst(0),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "empty", RowCount: 0}},
		AllLoops: []*WhereLoop{},
		NOut:     LogEst(0),
	}

	// Both inCost and joinCost = 0+0 = 0; joinCost >= inCost is 0>=0 = true.
	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (JOIN not beneficial) for zero-row case, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_MultipleLoops verifies
// that parent tables with multiple tables and loops do not affect the
// cost-comparison branch outcome.
func TestSubquery2Coverage_ConvertInToJoin_NotBeneficial_MultipleLoops(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(50),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "orders", RowCount: 5000, RowLogEst: NewLogEst(5000)},
			{Name: "items", RowCount: 2000, RowLogEst: NewLogEst(2000)},
		},
		AllLoops: []*WhereLoop{{}, {}},
		NOut:     NewLogEst(5000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (JOIN not beneficial), got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// ---------- ConvertExistsToSemiJoin ----------

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_In checks that
// an IN subquery is rejected with the "not an EXISTS subquery" error.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_In(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryIn,
		EstimatedRows: NewLogEst(100),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "t", RowCount: 100}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryIn type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "not an EXISTS subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_Scalar checks
// that a SCALAR subquery is rejected.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_Scalar(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryScalar,
		EstimatedRows: NewLogEst(10),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "items", RowCount: 300}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(300),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "not an EXISTS subquery") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_From checks
// that a FROM subquery is rejected.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NonExistsType_From(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryFrom,
		EstimatedRows: NewLogEst(75),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "data", RowCount: 2000}},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(2000),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom type, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_SmallSubquery
// verifies the cost-comparison branch for EXISTS.
// existsCost = NOut + (EstimatedRows - 10), joinCost = NOut + EstimatedRows.
// Since joinCost = existsCost + 10, joinCost >= existsCost is always true.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_SmallSubquery(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(50),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "orders", RowCount: 10000, RowLogEst: NewLogEst(10000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(10000),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error when semi-join is not beneficial, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
	if !strings.Contains(err.Error(), "semi-join not beneficial") {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_LargeSubquery
// verifies the cost path for a large subquery estimation.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_LargeSubquery(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(1000000),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "big_table", RowCount: 1000000, RowLogEst: NewLogEst(1000000)},
		},
		AllLoops: []*WhereLoop{},
		NOut:     NewLogEst(1000000),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error when semi-join is not beneficial, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_ZeroRows
// exercises the cost path with zero-valued LogEst fields.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_ZeroRows(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: LogEst(0),
	}
	parent := &WhereInfo{
		Tables:   []*TableInfo{{Name: "empty", RowCount: 0}},
		AllLoops: []*WhereLoop{},
		NOut:     LogEst(0),
	}

	// existsCost = 0 + (0 - 10) = -10; joinCost = 0 + 0 = 0.
	// 0 >= -10 is true, so semi-join is not beneficial.
	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (semi-join not beneficial) for zero-row case, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_MultipleLoops
// verifies the cost path when parent has multiple tables and loops.
func TestSubquery2Coverage_ConvertExistsToSemiJoin_NotBeneficial_MultipleLoops(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())
	info := &SubqueryInfo{
		Type:          SubqueryExists,
		EstimatedRows: NewLogEst(20),
	}
	parent := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "a", RowCount: 100, RowLogEst: NewLogEst(100)},
			{Name: "b", RowCount: 200, RowLogEst: NewLogEst(200)},
		},
		AllLoops: []*WhereLoop{{}, {}},
		NOut:     NewLogEst(100),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error (semi-join not beneficial), got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %+v", result)
	}
}

// TestSubquery2Coverage_CostInvariant documents the structural constraint
// that makes the success path of ConvertInToJoin unreachable:
// estimateInCost and estimateJoinCost produce identical values.
func TestSubquery2Coverage_CostInvariant(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())

	for _, nOut := range []int64{0, 1, 10, 100, 10000} {
		for _, estimated := range []int64{0, 1, 10, 100, 10000} {
			info := &SubqueryInfo{EstimatedRows: NewLogEst(estimated)}
			parent := &WhereInfo{NOut: NewLogEst(nOut)}

			inCost := opt.estimateInCost(info, parent)
			joinCost := opt.estimateJoinCost(info, parent)

			if joinCost < inCost {
				t.Errorf("unexpected: joinCost(%d) < inCost(%d) for NOut=%d EstimatedRows=%d",
					joinCost, inCost, nOut, estimated)
			}
		}
	}
}

// TestSubquery2Coverage_ExistsCostInvariant documents the structural constraint
// that makes the success path of ConvertExistsToSemiJoin unreachable:
// estimateJoinCost always exceeds estimateExistsCost by 10 (the Subtract value).
func TestSubquery2Coverage_ExistsCostInvariant(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())

	for _, nOut := range []int64{0, 1, 10, 100, 10000} {
		for _, estimated := range []int64{0, 1, 10, 100, 10000} {
			info := &SubqueryInfo{EstimatedRows: NewLogEst(estimated)}
			parent := &WhereInfo{NOut: NewLogEst(nOut)}

			existsCost := opt.estimateExistsCost(info, parent)
			joinCost := opt.estimateJoinCost(info, parent)

			if joinCost < existsCost {
				t.Errorf("unexpected: joinCost(%d) < existsCost(%d) for NOut=%d EstimatedRows=%d",
					joinCost, existsCost, nOut, estimated)
			}
		}
	}
}
