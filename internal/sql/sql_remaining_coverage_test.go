// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// --- result.go: resolveChildExprs (71.4%) ---
// Tests that exercise left-only, right-only, and both-children branches.

// TestResolveChildExprs_LeftOnly covers the left-child-only path.
func TestResolveChildExprs_LeftOnly(t *testing.T) {
	rc := &ResultCompiler{parse: &Parse{Vdbe: NewVdbe(nil)}}
	sel := &Select{
		Src: &SrcList{
			Items: []SrcListItem{
				{
					Name:   "t",
					Cursor: 1,
					Table: &Table{
						Name:       "t",
						NumColumns: 1,
						Columns:    []Column{{Name: "id", Affinity: SQLITE_AFF_INTEGER}},
					},
				},
			},
		},
	}

	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:          TK_COLUMN,
			StringValue: "id",
		},
		Right: nil,
	}

	err := rc.resolveChildExprs(sel, expr)
	if err != nil {
		t.Fatalf("resolveChildExprs (left-only) failed: %v", err)
	}
}

// TestResolveChildExprs_RightOnly covers the right-child-only path.
func TestResolveChildExprs_RightOnly(t *testing.T) {
	rc := &ResultCompiler{parse: &Parse{Vdbe: NewVdbe(nil)}}
	sel := &Select{
		Src: &SrcList{
			Items: []SrcListItem{
				{
					Name:   "t",
					Cursor: 1,
					Table: &Table{
						Name:       "t",
						NumColumns: 1,
						Columns:    []Column{{Name: "id", Affinity: SQLITE_AFF_INTEGER}},
					},
				},
			},
		},
	}

	expr := &Expr{
		Op:   TK_PLUS,
		Left: nil,
		Right: &Expr{
			Op:          TK_COLUMN,
			StringValue: "id",
		},
	}

	err := rc.resolveChildExprs(sel, expr)
	if err != nil {
		t.Fatalf("resolveChildExprs (right-only) failed: %v", err)
	}
}

// --- result.go: resolveExprColumns (83.3%) ---

// TestResolveExprColumns_TKDot covers the TK_DOT case via resolveDotExpr
// with a nil left or right (no-op path).
func TestResolveExprColumns_TKDot_NoOp(t *testing.T) {
	rc := &ResultCompiler{parse: &Parse{Vdbe: NewVdbe(nil)}}
	sel := &Select{}

	// TK_DOT with nil Left (short-circuit in resolveDotExpr)
	expr := &Expr{
		Op:    TK_DOT,
		Left:  nil,
		Right: &Expr{Op: TK_ID, StringValue: "col"},
	}

	err := rc.resolveExprColumns(sel, expr)
	if err != nil {
		t.Errorf("resolveExprColumns TK_DOT no-op failed: %v", err)
	}
}

// --- aggregate.go: findAggsInSelect (66.7%) ---

// TestFindAggsInSelect_NilOrderBy verifies a SELECT with no OrderBy doesn't crash.
func TestFindAggsInSelect_NilOrderBy(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: &FuncDef{Name: "count"},
					},
				},
			},
		},
		OrderBy: nil,
	}

	err := ac.findAggsInSelect(sel, aggInfo)
	if err != nil {
		t.Fatalf("findAggsInSelect (nil OrderBy) failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func, got %d", len(aggInfo.AggFuncs))
	}
}

// --- aggregate.go: findAggsInChildren (71.4%) ---

// TestFindAggsInChildren_ListBranch covers the List (function args) path in
// findAggsInChildren by nesting an aggregate inside a non-aggregate function.
func TestFindAggsInChildren_ListBranch(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)
	aggInfo := &AggInfo{}

	expr := &Expr{
		Op:    TK_FUNCTION,
		Left:  nil,
		Right: nil,
		List: &ExprList{
			Items: []ExprListItem{
				{
					Expr: &Expr{
						Op:      TK_AGG_FUNCTION,
						FuncDef: &FuncDef{Name: "sum"},
						List: &ExprList{
							Items: []ExprListItem{
								{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
							},
						},
					},
				},
			},
		},
	}

	err := ac.findAggregateFuncs(expr, aggInfo)
	if err != nil {
		t.Fatalf("findAggregateFuncs (list branch) failed: %v", err)
	}
	if len(aggInfo.AggFuncs) < 1 {
		t.Errorf("expected >=1 agg func from list branch, got %d", len(aggInfo.AggFuncs))
	}
}

// --- select.go: compileSimpleSelect (76.5%) ---

// TestCompileSimpleSelect_NoSrc verifies compileSimpleSelect handles a
// no-FROM (no-source) SELECT (e.g. "SELECT 1+1").
func TestCompileSimpleSelect_NoSrc(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(&Database{Name: "test"})}
	compiler := NewSelectCompiler(parse)
	dest := &SelectDest{}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 42}},
			},
		},
		Src: nil,
	}

	err := compiler.compileSimpleSelect(sel, dest)
	if err != nil {
		t.Fatalf("compileSimpleSelect (no-src) failed: %v", err)
	}
}

// --- select.go: setupDistinct (87.5%) ---

// TestSetupDistinct_Basic covers the setupDistinct path via a Select with SF_Distinct.
func TestSetupDistinct_Basic(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(&Database{Name: "test"})}
	compiler := NewSelectCompiler(parse)

	sel := &Select{
		SelFlags: SF_Distinct,
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}},
			},
		},
	}

	var distinct DistinctCtx
	compiler.setupDistinct(sel, &distinct)

	if distinct.TabTnct == 0 && distinct.AddrTnct == 0 {
		// Both zero could mean it was a no-op; just ensure no panic
		t.Log("setupDistinct ran without panic")
	}
}

// --- orderby.go: resolveChildExpressions (71.4%) ---

// TestResolveChildExpressions_LeftRight covers the left+right child paths.
func TestResolveChildExpressions_LeftRight(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ob := NewOrderByCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_INTEGER, IntValue: 1}, Name: "col1"},
			},
		},
		Src: nil,
	}

	// A binary expression that has left and right children: both are column-type
	// that won't match any alias (so the fallback no-op path is exercised).
	expr := &Expr{
		Op:    TK_PLUS,
		Left:  &Expr{Op: TK_INTEGER, IntValue: 1},
		Right: &Expr{Op: TK_INTEGER, IntValue: 2},
	}

	// Should not error; we just want to hit the child-traversal branches.
	err := ob.resolveChildExpressions(sel, expr)
	if err != nil {
		t.Fatalf("resolveChildExpressions failed: %v", err)
	}
}
