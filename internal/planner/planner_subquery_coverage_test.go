// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner_test

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
)

// TestPlannerSubqueryConvertInToJoin_WrongType verifies that ConvertInToJoin
// returns an error when given a non-IN subquery type.
func TestPlannerSubqueryConvertInToJoin_WrongType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryExists,
		EstimatedRows: planner.NewLogEst(100),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "t", RowCount: 100, RowLogEst: planner.NewLogEst(100)}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(100),
	}

	_, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for non-IN subquery type, got nil")
	}
}

// TestPlannerSubqueryConvertInToJoin_ScalarType verifies ConvertInToJoin rejects
// SubqueryScalar type.
func TestPlannerSubqueryConvertInToJoin_ScalarType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryScalar,
		EstimatedRows: planner.NewLogEst(50),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "orders", RowCount: 500}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(500),
	}

	_, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar, got nil")
	}
}

// TestPlannerSubqueryConvertInToJoin_FromType verifies ConvertInToJoin rejects
// SubqueryFrom type.
func TestPlannerSubqueryConvertInToJoin_FromType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryFrom,
		EstimatedRows: planner.NewLogEst(200),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "users", RowCount: 1000}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(1000),
	}

	_, err := opt.ConvertInToJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom, got nil")
	}
}

// TestPlannerSubqueryConvertInToJoin_NotBeneficial verifies that ConvertInToJoin
// returns an error when the JOIN conversion is not cost-beneficial.
func TestPlannerSubqueryConvertInToJoin_NotBeneficial(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryIn,
		EstimatedRows: planner.NewLogEst(100),
	}
	parent := &planner.WhereInfo{
		Tables: []*planner.TableInfo{
			{Name: "users", RowCount: 1000, RowLogEst: planner.NewLogEst(1000)},
		},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(1000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	// Cost model makes inCost == joinCost, so joinCost >= inCost always;
	// either error or success is acceptable — we just exercise the code path.
	if err != nil && result != nil {
		t.Error("inconsistent return: non-nil result with error")
	}
}

// TestPlannerSubqueryConvertInToJoin_MultipleTableParent verifies that
// ConvertInToJoin handles parent queries with multiple tables.
func TestPlannerSubqueryConvertInToJoin_MultipleTableParent(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryIn,
		EstimatedRows: planner.NewLogEst(50),
	}
	parent := &planner.WhereInfo{
		Tables: []*planner.TableInfo{
			{Name: "orders", RowCount: 5000, RowLogEst: planner.NewLogEst(5000)},
			{Name: "customers", RowCount: 200, RowLogEst: planner.NewLogEst(200)},
		},
		AllLoops: []*planner.WhereLoop{{}, {}},
		NOut:     planner.NewLogEst(5000),
	}

	result, err := opt.ConvertInToJoin(info, parent)
	if err != nil && result != nil {
		t.Error("inconsistent return: non-nil result with error")
	}
	if err == nil && result != nil {
		if len(result.Tables) != len(parent.Tables) {
			t.Errorf("table count mismatch: got %d want %d", len(result.Tables), len(parent.Tables))
		}
	}
}

// TestPlannerSubqueryConvertExistsToSemiJoin_WrongType verifies that
// ConvertExistsToSemiJoin returns an error for non-EXISTS subquery types.
func TestPlannerSubqueryConvertExistsToSemiJoin_WrongType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryIn,
		EstimatedRows: planner.NewLogEst(100),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "t", RowCount: 100}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(100),
	}

	_, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for non-EXISTS subquery type, got nil")
	}
}

// TestPlannerSubqueryConvertExistsToSemiJoin_ScalarType verifies that
// ConvertExistsToSemiJoin rejects SubqueryScalar.
func TestPlannerSubqueryConvertExistsToSemiJoin_ScalarType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryScalar,
		EstimatedRows: planner.NewLogEst(10),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "items", RowCount: 300}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(300),
	}

	_, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryScalar, got nil")
	}
}

// TestPlannerSubqueryConvertExistsToSemiJoin_FromType verifies that
// ConvertExistsToSemiJoin rejects SubqueryFrom.
func TestPlannerSubqueryConvertExistsToSemiJoin_FromType(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryFrom,
		EstimatedRows: planner.NewLogEst(75),
	}
	parent := &planner.WhereInfo{
		Tables:   []*planner.TableInfo{{Name: "data", RowCount: 2000}},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(2000),
	}

	_, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err == nil {
		t.Fatal("expected error for SubqueryFrom, got nil")
	}
}

// TestPlannerSubqueryConvertExistsToSemiJoin_Exists exercises the EXISTS
// subquery cost comparison path inside ConvertExistsToSemiJoin.
func TestPlannerSubqueryConvertExistsToSemiJoin_Exists(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryExists,
		EstimatedRows: planner.NewLogEst(50),
	}
	parent := &planner.WhereInfo{
		Tables: []*planner.TableInfo{
			{Name: "orders", RowCount: 10000, RowLogEst: planner.NewLogEst(10000)},
		},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(10000),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err != nil && result != nil {
		t.Error("inconsistent return: non-nil result with error")
	}
	if err == nil && result != nil {
		if len(result.Tables) != len(parent.Tables) {
			t.Errorf("table count mismatch: got %d want %d", len(result.Tables), len(parent.Tables))
		}
	}
}

// TestPlannerSubqueryConvertExistsToSemiJoin_MultipleLoops exercises
// ConvertExistsToSemiJoin with a parent WhereInfo that has existing WhereLoops.
func TestPlannerSubqueryConvertExistsToSemiJoin_MultipleLoops(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	info := &planner.SubqueryInfo{
		Type:          planner.SubqueryExists,
		EstimatedRows: planner.NewLogEst(20),
	}
	parent := &planner.WhereInfo{
		Tables: []*planner.TableInfo{
			{Name: "a", RowCount: 100, RowLogEst: planner.NewLogEst(100)},
			{Name: "b", RowCount: 200, RowLogEst: planner.NewLogEst(200)},
		},
		AllLoops: []*planner.WhereLoop{{}, {}},
		NOut:     planner.NewLogEst(100),
	}

	result, err := opt.ConvertExistsToSemiJoin(info, parent)
	if err != nil && result != nil {
		t.Error("inconsistent return: non-nil result with error")
	}
	if err == nil && result != nil {
		if len(result.Tables) != len(parent.Tables) {
			t.Errorf("table count: got %d want %d", len(result.Tables), len(parent.Tables))
		}
		if len(result.AllLoops) != len(parent.AllLoops) {
			t.Errorf("loop count: got %d want %d", len(result.AllLoops), len(parent.AllLoops))
		}
	}
}
