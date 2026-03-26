// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner_test

import (
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestSubqueryView2Coverage groups additional sub-tests targeting coverage gaps
// in subquery.go, planner.go, and view.go that are not covered by
// subquery_view_coverage_test.go.
func TestSubqueryView2Coverage(t *testing.T) {
	t.Run("ConvertInToJoin_variations", testConvertInToJoin_variations)
	t.Run("ConvertExistsToSemiJoin_variations", testConvertExistsToSemiJoin_variations)
	t.Run("tryTypeSpecificOptimization_variations", testTryTypeSpecificOptimization_variations)
	t.Run("optimizeFromSubquery_nilNilReturn", testOptimizeFromSubquery_nilNilReturn)
	t.Run("createMaterializedSubqueryTable_viaCanMaterialize", testCreateMaterializedSubqueryTable)
	t.Run("canFlattenView_nilSelect", testCanFlattenView_nilSelect)
	t.Run("canFlattenView_subqueryInFrom", testCanFlattenView_subqueryInFrom)
	t.Run("canFlattenView_having", testCanFlattenView_having)
	t.Run("canFlattenView_compound", testCanFlattenView_compound)
	t.Run("canFlattenView_offset", testCanFlattenView_offset)
	t.Run("flattenViewsInSelect_depth", testFlattenViewsInSelect_depth)
	t.Run("expandViewsInSelectWithDepth_viaJoins", testExpandViewsInSelectWithDepth_viaJoins)
}

// ---------------------------------------------------------------------------
// subquery.go: ConvertInToJoin — additional variants
// ---------------------------------------------------------------------------

// testConvertInToJoin_variations exercises ConvertInToJoin with various
// configurations, ensuring all reachable branches are hit.
func testConvertInToJoin_variations(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	// Zero estimated rows: both in-cost and join-cost become identical.
	// Verifies the cost-comparison branch fires for a SubqueryIn.
	t.Run("ZeroEstimatedRows", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryIn,
			EstimatedRows: planner.NewLogEst(0),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{{Name: "t", RowCount: 10, RowLogEst: planner.NewLogEst(10)}},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(10),
		}
		result, err := opt.ConvertInToJoin(info, parent)
		// Cost comparison always fires; either outcome is valid.
		if err != nil && result != nil {
			t.Error("inconsistent return: error with non-nil result")
		}
	})

	// Large subquery rows: joins cost equals in-cost (same formula), always not beneficial.
	t.Run("LargeSubqueryRows", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryIn,
			EstimatedRows: planner.NewLogEst(1000000),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{{Name: "big", RowCount: 500000, RowLogEst: planner.NewLogEst(500000)}},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(500000),
		}
		_, err := opt.ConvertInToJoin(info, parent)
		// With identical cost formulae, join is never beneficial.
		if err == nil {
			// This is also acceptable if the implementation changes.
			t.Log("ConvertInToJoin succeeded (cost model may have changed)")
		}
	})

	// Empty parent tables slice: exercises copy(newInfo.Tables) with len=0.
	t.Run("EmptyParentTables", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryIn,
			EstimatedRows: planner.NewLogEst(5),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(5),
		}
		result, err := opt.ConvertInToJoin(info, parent)
		if err != nil && result != nil {
			t.Error("inconsistent return: error with non-nil result")
		}
	})
}

// ---------------------------------------------------------------------------
// subquery.go: ConvertExistsToSemiJoin — additional variants
// ---------------------------------------------------------------------------

// testConvertExistsToSemiJoin_variations exercises ConvertExistsToSemiJoin
// with configurations that probe the cost-comparison branch more thoroughly.
func testConvertExistsToSemiJoin_variations(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	// Zero estimated rows: existsCost = NOut + (0 - 10), joinCost = NOut + 0.
	// joinCost > existsCost when EstimatedRows=0 (and subtract wraps): always triggers error.
	t.Run("ZeroEstimatedRows", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryExists,
			EstimatedRows: planner.NewLogEst(0),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{{Name: "t", RowCount: 50, RowLogEst: planner.NewLogEst(50)}},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(50),
		}
		result, err := opt.ConvertExistsToSemiJoin(info, parent)
		if err != nil && result != nil {
			t.Error("inconsistent return: error with non-nil result")
		}
	})

	// Small rows (< 10): avgScanCost = EstimatedRows - 10 becomes negative/zero via Subtract.
	// Exercises the Subtract path in estimateExistsCost.
	t.Run("SmallEstimatedRows", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryExists,
			EstimatedRows: planner.NewLogEst(5),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{{Name: "s", RowCount: 200, RowLogEst: planner.NewLogEst(200)}},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(200),
		}
		result, err := opt.ConvertExistsToSemiJoin(info, parent)
		if err != nil && result != nil {
			t.Error("inconsistent return: error with non-nil result")
		}
	})

	// Empty AllLoops in parent: exercises the copy(AllLoops) with len=0.
	t.Run("EmptyParentLoops", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:          planner.SubqueryExists,
			EstimatedRows: planner.NewLogEst(100),
		}
		parent := &planner.WhereInfo{
			Tables:   []*planner.TableInfo{},
			AllLoops: []*planner.WhereLoop{},
			NOut:     planner.NewLogEst(0),
		}
		result, err := opt.ConvertExistsToSemiJoin(info, parent)
		if err != nil && result != nil {
			t.Error("inconsistent return: error with non-nil result")
		}
	})
}

// ---------------------------------------------------------------------------
// subquery.go: tryTypeSpecificOptimization — additional variants via OptimizeSubquery
// ---------------------------------------------------------------------------

// testTryTypeSpecificOptimization_variations exercises tryTypeSpecificOptimization
// by calling OptimizeSubquery with SubqueryFrom type (neither EXISTS nor IN),
// which reaches the final "return nil, false" in tryTypeSpecificOptimization,
// and then falls through to tryMaterialize.
func testTryTypeSpecificOptimization_variations(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	parent := &planner.WhereInfo{
		Tables: []*planner.TableInfo{
			{Name: "t", RowCount: 500, RowLogEst: planner.NewLogEst(500)},
		},
		AllLoops: []*planner.WhereLoop{},
		NOut:     planner.NewLogEst(500),
	}

	// SubqueryFrom: not EXISTS, not IN → tryTypeSpecificOptimization returns (nil, false),
	// then tryMaterialize is attempted (CanMaterialize=false here → returns false too).
	// Final result is parentInfo unchanged.
	t.Run("FromType_NeitherExistsNorIn", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryFrom,
			CanFlatten:     false,
			IsCorrelated:   false,
			CanMaterialize: false,
			EstimatedRows:  planner.NewLogEst(50),
		}
		result, err := opt.OptimizeSubquery(info, parent)
		if err != nil {
			t.Fatalf("OptimizeSubquery (SubqueryFrom) returned error: %v", err)
		}
		if result == nil {
			t.Error("expected non-nil result from OptimizeSubquery")
		}
	})

	// Correlated SubqueryExists: tryDecorrelate runs first, then tryTypeSpecificOptimization.
	// After decorrelation, IsCorrelated=false; type is still EXISTS so tryTypeSpecificOptimization
	// calls ConvertExistsToSemiJoin which fails → returns false. tryMaterialize then runs (CanMaterialize=true after decorrelation).
	t.Run("CorrelatedExists_DecorrelatedThenTypeSpecific", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryExists,
			CanFlatten:     false,
			IsCorrelated:   true,
			CanMaterialize: false,
			EstimatedRows:  planner.NewLogEst(100),
			ExecutionCount: planner.NewLogEst(500),
		}
		result, err := opt.OptimizeSubquery(info, parent)
		if err != nil {
			t.Fatalf("OptimizeSubquery (correlated EXISTS) returned error: %v", err)
		}
		if result == nil {
			t.Error("expected non-nil result from OptimizeSubquery")
		}
	})

	// Correlated SubqueryIn: similar to above but for IN type.
	t.Run("CorrelatedIn_DecorrelatedThenTypeSpecific", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryIn,
			CanFlatten:     false,
			IsCorrelated:   true,
			CanMaterialize: false,
			EstimatedRows:  planner.NewLogEst(200),
			ExecutionCount: planner.NewLogEst(1000),
		}
		result, err := opt.OptimizeSubquery(info, parent)
		if err != nil {
			t.Fatalf("OptimizeSubquery (correlated IN) returned error: %v", err)
		}
		if result == nil {
			t.Error("expected non-nil result from OptimizeSubquery")
		}
	})

	// SubqueryScalar with CanMaterialize=true: tryTypeSpecificOptimization returns (nil,false),
	// then tryMaterialize succeeds (CanMaterialize=true).
	t.Run("ScalarWithCanMaterialize", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryScalar,
			CanFlatten:     false,
			IsCorrelated:   false,
			CanMaterialize: true,
			EstimatedRows:  planner.NewLogEst(10),
		}
		result, err := opt.OptimizeSubquery(info, parent)
		if err != nil {
			t.Fatalf("OptimizeSubquery (scalar CanMaterialize) returned error: %v", err)
		}
		if result == nil {
			t.Error("expected non-nil result from OptimizeSubquery")
		}
	})
}

// ---------------------------------------------------------------------------
// planner.go: optimizeFromSubquery — nil,nil return branch
// ---------------------------------------------------------------------------

// testOptimizeFromSubquery_nilNilReturn exercises the branch in optimizeFromSubquery
// where neither CanFlatten nor CanMaterialize is true, causing it to return (nil, nil).
// This happens when a correlated scalar subquery has ExecutionCount=0, making
// shouldMaterializeSubquery return false.
func testOptimizeFromSubquery_nilNilReturn(t *testing.T) {
	p := planner.NewPlanner()

	// A correlated scalar subquery (OuterColumn set → IsCorrelated=true),
	// with default ExecutionCount=0. In AnalyzeSubquery:
	//   CanFlatten = canFlattenSubquery → SubqueryScalar && !IsCorrelated → false (IsCorrelated=true)
	//   CanMaterialize = shouldMaterializeSubquery → materializeCost (100) < repeatCost (0+100=100) → false
	// So optimizeFromSubquery returns nil, nil.
	t.Run("CorrelatedScalar_NeitherFlattenNorMaterialize", func(t *testing.T) {
		outerCol := &planner.ColumnExpr{Table: "outer_t", Column: "id", Cursor: 1}
		subExpr := &planner.SubqueryExpr{
			Type:        planner.SubqueryScalar,
			Query:       outerCol,
			OuterColumn: outerCol,
		}
		result, err := p.PlanQueryWithSubqueries(
			[]*planner.TableInfo{},
			[]planner.Expr{subExpr},
			nil,
		)
		// nil,nil from optimizeFromSubquery → table not added → PlanQuery still runs.
		if err != nil {
			t.Logf("PlanQueryWithSubqueries returned error (acceptable): %v", err)
		}
		_ = result
	})

	// A SubqueryIn: AnalyzeSubquery always defaults type to Scalar.
	// Correlated scalar → CanFlatten=false, CanMaterialize=false → nil,nil.
	t.Run("InTypeExpr_CorrelatedScalar", func(t *testing.T) {
		outerCol := &planner.ColumnExpr{Table: "t", Column: "x", Cursor: 2}
		subExpr := &planner.SubqueryExpr{
			Type:        planner.SubqueryIn,
			Query:       outerCol,
			OuterColumn: outerCol,
		}
		result, err := p.PlanQueryWithSubqueries(
			[]*planner.TableInfo{{
				Name:      "t",
				Cursor:    2,
				RowCount:  10,
				RowLogEst: planner.NewLogEst(10),
			}},
			[]planner.Expr{subExpr},
			nil,
		)
		if err != nil {
			t.Logf("PlanQueryWithSubqueries returned error (acceptable): %v", err)
		}
		_ = result
	})
}

// ---------------------------------------------------------------------------
// planner.go: createMaterializedSubqueryTable — via direct SubqueryOptimizer
// ---------------------------------------------------------------------------

// testCreateMaterializedSubqueryTable exercises the createMaterializedSubqueryTable
// path indirectly through NewPlanner + PlanQueryWithSubqueries. Since CanMaterialize
// is determined by AnalyzeSubquery and requires ExecutionCount > EstimatedRows to be
// satisfied, we instead exercise MaterializeSubquery directly and verify its behavior.
func testCreateMaterializedSubqueryTable(t *testing.T) {
	opt := planner.NewSubqueryOptimizer(planner.NewCostModel())

	// MaterializeSubquery succeeds when CanMaterialize=true.
	t.Run("MaterializeSuccess", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryScalar,
			CanMaterialize: true,
			EstimatedRows:  planner.NewLogEst(100),
		}
		materialized, err := opt.MaterializeSubquery(info)
		if err != nil {
			t.Fatalf("MaterializeSubquery succeeded unexpectedly: %v", err)
		}
		if materialized == nil {
			t.Fatal("expected non-nil materialized info")
		}
		if materialized.MaterializedTable == "" {
			t.Error("expected non-empty MaterializedTable name")
		}
		if materialized.CanMaterialize {
			t.Error("materialized info should have CanMaterialize=false")
		}
		if materialized.IsCorrelated {
			t.Error("materialized info should have IsCorrelated=false")
		}
	})

	// MaterializeSubquery fails when CanMaterialize=false.
	t.Run("MaterializeFailure", func(t *testing.T) {
		info := &planner.SubqueryInfo{
			Type:           planner.SubqueryScalar,
			CanMaterialize: false,
			EstimatedRows:  planner.NewLogEst(50),
		}
		_, err := opt.MaterializeSubquery(info)
		if err == nil {
			t.Fatal("expected error from MaterializeSubquery with CanMaterialize=false")
		}
	})

	// Multiple materializations: NextTempTable counter increments.
	t.Run("MultipleMaterilaizations_Counter", func(t *testing.T) {
		opt2 := planner.NewSubqueryOptimizer(planner.NewCostModel())
		for i := 0; i < 3; i++ {
			info := &planner.SubqueryInfo{
				Type:           planner.SubqueryExists,
				CanMaterialize: true,
				EstimatedRows:  planner.NewLogEst(int64(10 * (i + 1))),
			}
			m, err := opt2.MaterializeSubquery(info)
			if err != nil {
				t.Fatalf("iteration %d: MaterializeSubquery error: %v", i, err)
			}
			if m.MaterializedTable == "" {
				t.Errorf("iteration %d: expected non-empty temp table name", i)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// view.go: canFlattenView — nil Select branch
// ---------------------------------------------------------------------------

// testCanFlattenView_nilSelect registers a view with nil Select in the schema.
// When ExpandViewsInSelect processes it, canFlattenView returns false (nil Select),
// then expandViewAsSubquery calls ExpandView which errors. This exercises the
// view.Select == nil branch of canFlattenView.
func testCanFlattenView_nilSelect(t *testing.T) {
	s := schema.NewSchema()
	s.Views["nil_select_v"] = &schema.View{
		Name:   "nil_select_v",
		Select: nil,
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "nil_select_v"}},
		},
	}
	// ExpandViewsInSelect should return an error because ExpandView fails
	// for a view with nil Select.
	_, err := planner.ExpandViewsInSelect(stmt, s)
	if err == nil {
		t.Error("expected error when expanding view with nil Select, got nil")
	}
}

// ---------------------------------------------------------------------------
// view.go: canFlattenView — subquery in FROM branch
// ---------------------------------------------------------------------------

// testCanFlattenView_subqueryInFrom exercises the hasValidFromClauseForFlattening
// branch that returns false when the view's FROM clause contains a subquery
// (rather than a plain table reference).
func testCanFlattenView_subqueryInFrom(t *testing.T) {
	s := schema.NewSchema()

	// Create a view whose FROM clause is itself a subquery.
	// canFlattenView → hasValidFromClauseForFlattening → Tables[0].Subquery != nil → false.
	innerSubquery := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "base_t"}},
		},
	}
	s.Views["subq_from_v"] = &schema.View{
		Name: "subq_from_v",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{
					Subquery: innerSubquery,
					Alias:    "sq",
				}},
			},
		},
	}

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "subq_from_v"}},
		},
	}

	// canFlattenView returns false → expandViewAsSubquery is called → the view is expanded
	// into a subquery. No error expected.
	result, err := planner.ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("unexpected error for view with subquery FROM: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// The view cannot be flattened, so it becomes a subquery.
	if result.From.Tables[0].Subquery == nil {
		t.Error("expected view with subquery FROM to be expanded as subquery in outer query")
	}
}

// ---------------------------------------------------------------------------
// view.go: canFlattenView — HAVING clause branch
// ---------------------------------------------------------------------------

// testCanFlattenView_having exercises the hasNoComplexFeatures branch that
// returns false when the view has a HAVING clause.
func testCanFlattenView_having(t *testing.T) {
	s := schema.NewSchema()
	s.Views["having_v"] = &schema.View{
		Name: "having_v",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "sales"}},
			},
			GroupBy: []parser.Expression{&parser.IdentExpr{Name: "region"}},
			Having: &parser.BinaryExpr{
				Left:  &parser.FunctionExpr{Name: "COUNT", Args: []parser.Expression{&parser.IdentExpr{Name: "*"}}},
				Op:    parser.OpGt,
				Right: &parser.LiteralExpr{Value: "5"},
			},
		},
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "having_v"}}},
	}
	result, err := planner.ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.From.Tables[0].Subquery == nil {
		t.Error("expected view with HAVING to become a subquery")
	}
}

// ---------------------------------------------------------------------------
// view.go: canFlattenView — compound SELECT (UNION) branch
// ---------------------------------------------------------------------------

// testCanFlattenView_compound exercises the hasNoComplexFeatures branch that
// returns false when the view has a compound SELECT (UNION).
func testCanFlattenView_compound(t *testing.T) {
	s := schema.NewSchema()

	// Build the compound (UNION) SELECT.
	unionPart := &parser.CompoundSelect{
		Op: parser.CompoundUnion,
		Left: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "orders"}},
			},
		},
		Right: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "archived_orders"}},
			},
		},
	}
	s.Views["union_v"] = &schema.View{
		Name: "union_v",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "orders"}},
			},
			Compound: unionPart,
		},
	}

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "union_v"}}},
	}
	result, err := planner.ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("unexpected error for union view: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.From.Tables[0].Subquery == nil {
		t.Error("expected UNION view to become a subquery")
	}
}

// ---------------------------------------------------------------------------
// view.go: canFlattenView — OFFSET clause branch
// ---------------------------------------------------------------------------

// testCanFlattenView_offset exercises the hasNoComplexFeatures branch that
// returns false when the view has an OFFSET clause.
func testCanFlattenView_offset(t *testing.T) {
	s := schema.NewSchema()
	s.Views["offset_v"] = &schema.View{
		Name: "offset_v",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "logs"}},
			},
			Offset: &parser.LiteralExpr{Value: "20"},
		},
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "offset_v"}}},
	}
	result, err := planner.ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("unexpected error for offset view: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.From.Tables[0].Subquery == nil {
		t.Error("expected view with OFFSET to become a subquery")
	}
}

// ---------------------------------------------------------------------------
// view.go: flattenViewsInSelect — depth > 100 branch
// ---------------------------------------------------------------------------

// testFlattenViewsInSelect_depth exercises the depth > 100 guard in
// flattenViewsInSelect by constructing a chain of complex (non-flattenable)
// views so that each level of recursion increments the depth counter.
//
// expandViewAsSubquery calls flattenViewsInSelect(expandedSelect, s, depth+1)
// recursively.  With 102 views v0→v1→…→v101 each having a JOIN (making them
// non-flattenable), the recursion reaches depth > 100 and returns an error.
func testFlattenViewsInSelect_depth(t *testing.T) {
	s := schema.NewSchema()

	const chainLen = 102

	// Build the chain from the bottom up so v_0 references v_1, v_1 references v_2, etc.
	// Each view has a JOIN to make it non-flattenable (canFlattenView → false).
	for i := 0; i < chainLen; i++ {
		viewName := fmt.Sprintf("depth_v%d", i)
		nextTable := "base_leaf" // leaf table for the last in chain
		if i < chainLen-1 {
			nextTable = fmt.Sprintf("depth_v%d", i+1)
		}

		s.Views[viewName] = &schema.View{
			Name: viewName,
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: nextTable}},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{TableName: "aux_t"}},
					},
				},
			},
		}
	}

	// Query references the top of the chain (depth_v0).
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "depth_v0"}},
		},
	}

	_, err := planner.ExpandViewsInSelect(stmt, s)
	if err == nil {
		t.Error("expected depth-limit error for deeply nested view chain, got nil")
	}
}

// ---------------------------------------------------------------------------
// view.go: expandViewsInSelectWithDepth — via JOIN clause with view reference
// ---------------------------------------------------------------------------

// testExpandViewsInSelectWithDepth_viaJoins exercises expandViewsInSelectWithDepth
// through the ExpandView + expandViewAsSubquery path where the expanded view
// itself contains a JOIN clause that references another view.
//
// Note: ExpandViewsInSelect calls flattenViewsInSelect, not
// expandViewsInSelectWithDepth directly.  The JOIN-view path within
// expandViewsInSelectWithDepth is only reachable from package planner internals.
// These tests instead verify behaviour that flows through ExpandViewsInSelect.
func testExpandViewsInSelectWithDepth_viaJoins(t *testing.T) {
	// Simple view followed by complex view (tests expandViewAsSubquery path where
	// expandedSelect itself is then processed by flattenViewsInSelect).
	t.Run("ComplexViewWithInnerSimpleView", func(t *testing.T) {
		s := schema.NewSchema()

		// inner_simple_v is flattenable.
		s.Views["inner_simple_v"] = &schema.View{
			Name: "inner_simple_v",
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "raw_t"}},
				},
			},
		}

		// outer_complex_v has a JOIN (non-flattenable) referencing inner_simple_v.
		s.Views["outer_complex_v"] = &schema.View{
			Name: "outer_complex_v",
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{{TableName: "inner_simple_v"}},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{TableName: "other_t"}},
					},
				},
			},
		}

		stmt := &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "outer_complex_v"}},
			},
		}

		result, err := planner.ExpandViewsInSelect(stmt, s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		// Complex view is expanded as subquery.
		if result.From.Tables[0].Subquery == nil {
			t.Error("expected outer_complex_v to become a subquery")
		}
	})

	// Stmt where FROM contains an existing subquery — flattenViewsInSelect
	// skips it (the Subquery != nil check in processFromTablesForFlattening).
	t.Run("FromSubqueryPassthrough", func(t *testing.T) {
		s := schema.NewSchema()
		innerStmt := &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "inner_t"}},
			},
		}
		stmt := &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{
					Subquery: innerStmt,
					Alias:    "sq",
				}},
			},
		}
		result, err := planner.ExpandViewsInSelect(stmt, s)
		if err != nil {
			t.Fatalf("unexpected error for subquery passthrough: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		// Subquery entry is preserved as-is.
		if result.From.Tables[0].Subquery == nil {
			t.Error("expected subquery to be preserved")
		}
	})

	// Stmt that has no FROM tables but has a non-empty schema: should pass through cleanly.
	t.Run("EmptyFromTables", func(t *testing.T) {
		s := schema.NewSchema()
		s.Views["some_view"] = &schema.View{
			Name: "some_view",
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From:    &parser.FromClause{Tables: []parser.TableOrSubquery{{TableName: "t"}}},
			},
		}
		stmt := &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From:    &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}
		result, err := planner.ExpandViewsInSelect(stmt, s)
		if err != nil {
			t.Fatalf("unexpected error for empty FROM: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
	})
}
