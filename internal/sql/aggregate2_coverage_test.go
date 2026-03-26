// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestFindAggsInSelect_HavingAndOrderBy covers the branch where both Having
// and OrderBy are non-nil, exercising the Having scan followed by the
// OrderBy findAggsInExprList call on the same invocation.
func TestFindAggsInSelect_HavingAndOrderBy(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	havingAgg := &Expr{
		Op:      TK_AGG_FUNCTION,
		FuncDef: &FuncDef{Name: "count"},
	}
	orderByAgg := &Expr{
		Op:      TK_AGG_FUNCTION,
		FuncDef: &FuncDef{Name: "sum"},
	}

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having: havingAgg,
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: orderByAgg},
			},
		},
		SelectID: 1,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	// Expect two aggregate functions: one from Having, one from OrderBy.
	if len(aggInfo.AggFuncs) != 2 {
		t.Errorf("AggFuncs count = %d, want 2", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_EListOnly covers the path where Having is nil and
// OrderBy is nil, so only the EList scan runs.
func TestFindAggsInSelect_EListOnly(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "max"},
				}},
			},
		},
		Having:   nil,
		OrderBy:  nil,
		SelectID: 2,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs count = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_OrderByOnly covers the branch where Having is nil but
// OrderBy is non-nil, reaching the final findAggsInExprList(sel.OrderBy) call.
func TestFindAggsInSelect_OrderByOnly(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having: nil,
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "min"},
				}},
			},
		},
		SelectID: 3,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs count = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_HavingOnly covers the branch where Having is non-nil
// but OrderBy is nil, so the Having scan runs and OrderBy returns nil early.
func TestFindAggsInSelect_HavingOnly(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "avg"},
		},
		OrderBy:  nil,
		SelectID: 4,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	if len(aggInfo.AggFuncs) != 1 {
		t.Errorf("AggFuncs count = %d, want 1", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_MultipleEListAggregates covers the EList scan with
// multiple aggregate functions to exercise the loop inside findAggsInExprList.
func TestFindAggsInSelect_MultipleEListAggregates(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_AGG_FUNCTION, FuncDef: &FuncDef{Name: "count"}}},
				{Expr: &Expr{Op: TK_AGG_FUNCTION, FuncDef: &FuncDef{Name: "sum"}}},
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having:   nil,
		OrderBy:  nil,
		SelectID: 5,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	if len(aggInfo.AggFuncs) != 2 {
		t.Errorf("AggFuncs count = %d, want 2", len(aggInfo.AggFuncs))
	}
}

// TestFindAggsInSelect_HavingAndOrderByBothAggregates covers the path where
// Having contains a nested aggregate expression and OrderBy also has an
// aggregate, verifying both are discovered in a single findAggsInSelect call.
func TestFindAggsInSelect_HavingAndOrderByBothAggregates(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	// HAVING COUNT(*) > 1 AND ORDER BY SUM(x)
	sel := &Select{
		EList: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{Op: TK_COLUMN}},
			},
		},
		Having: &Expr{
			Op: TK_GT,
			Left: &Expr{
				Op:      TK_AGG_FUNCTION,
				FuncDef: &FuncDef{Name: "count"},
			},
			Right: &Expr{Op: TK_INTEGER, IntValue: 1},
		},
		OrderBy: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "sum"},
				}},
			},
		},
		SelectID: 6,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect returned unexpected error: %v", err)
	}

	// count from Having's Left child + sum from OrderBy = 2 aggregates.
	if len(aggInfo.AggFuncs) != 2 {
		t.Errorf("AggFuncs count = %d, want 2", len(aggInfo.AggFuncs))
	}
}
