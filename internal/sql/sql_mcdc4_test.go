// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"
)

// TestMCDC4Sql_AddValueLoad_UnsupportedType covers the default case in
// addValueLoad (line 314 of insert.go) where an unrecognised ValueType causes
// an error.  That error then propagates through compileInsertRow → the
// previously-uncovered "return err" branch of compileInsertRows.
//
// MC/DC for addValueLoad:
//
//	C6: default (unsupported type) → error returned (covered here)
//
// MC/DC for compileInsertRows:
//
//	C2: compileInsertRow returns error → error propagated (covered here)
func TestMCDC4Sql_AddValueLoad_UnsupportedType(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"a"},
		Values: [][]Value{
			{{Type: ValueType(99)}}, // ValueType(99) hits the default case
		},
	}
	_, err := CompileInsert(stmt, 1)
	if err == nil {
		t.Error("CompileInsert with unsupported ValueType should return error")
	}
}

// TestMCDC4Sql_FindAggsInChildren_AllChildrenNonNil covers the branch in
// findAggsInChildren where Left, Right, and List are all non-nil
// simultaneously — exercising all three recursive paths in a single call.
//
// MC/DC for findAggsInChildren:
//
//	C1: Left != nil  → recurse into left child
//	C2: Right != nil → recurse into right child (both C1 and C2 true together)
//	C3: List != nil  → recurse into list (covered by existing tests independently)
func TestMCDC4Sql_FindAggsInChildren_AllChildrenNonNil(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	expr := &Expr{
		Op: TK_PLUS,
		Left: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "sum"},
		},
		Right: &Expr{
			Op:      TK_AGG_FUNCTION,
			FuncDef: &FuncDef{Name: "count"},
		},
		List: &ExprList{
			Items: []ExprListItem{
				{Expr: &Expr{
					Op:      TK_AGG_FUNCTION,
					FuncDef: &FuncDef{Name: "max"},
				}},
			},
		},
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInChildren(expr, aggInfo); err != nil {
		t.Fatalf("findAggsInChildren: unexpected error: %v", err)
	}
	// sum (Left) + count (Right) + max (List) = 3 aggregates.
	if len(aggInfo.AggFuncs) != 3 {
		t.Errorf("AggFuncs count = %d, want 3", len(aggInfo.AggFuncs))
	}
}

// TestMCDC4Sql_FindAggsInSelect_NilEList covers findAggsInExprList with a nil
// list, reached via findAggsInSelect when sel.EList is nil.
//
// MC/DC for findAggsInExprList:
//
//	C1: list == nil → return nil early
func TestMCDC4Sql_FindAggsInSelect_NilEList(t *testing.T) {
	parse := &Parse{Vdbe: NewVdbe(nil)}
	ac := NewAggregateCompiler(parse)

	sel := &Select{
		EList:    nil,
		Having:   nil,
		OrderBy:  nil,
		SelectID: 10,
	}

	aggInfo := &AggInfo{}
	if err := ac.findAggsInSelect(sel, aggInfo); err != nil {
		t.Fatalf("findAggsInSelect with nil EList: unexpected error: %v", err)
	}
	if len(aggInfo.AggFuncs) != 0 {
		t.Errorf("AggFuncs count = %d, want 0", len(aggInfo.AggFuncs))
	}
}

// TestMCDC4Sql_CompileInsertRows_DirectEmptyLoop directly calls compileInsertRows
// with an empty Values slice to exercise the zero-iteration loop path without
// going through validateInsertStmt.
//
// MC/DC for compileInsertRows:
//
//	C1: len(stmt.Values) == 0 → loop body never runs
func TestMCDC4Sql_CompileInsertRows_DirectEmptyLoop(t *testing.T) {
	prog := newProgram()
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"a"},
		Values:  [][]Value{},
	}
	if err := compileInsertRows(prog, stmt, 1, 0); err != nil {
		t.Fatalf("compileInsertRows with empty Values: unexpected error: %v", err)
	}
	for _, inst := range prog.Instructions {
		if inst.OpCode == OpInsert {
			t.Error("no Insert op expected when Values is empty")
		}
	}
}
