// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// TestExtractBaseExprCollate verifies that extractBaseExpr unwraps a CollateExpr.
func TestExtractBaseExprCollate(t *testing.T) {
	inner := &parser.IdentExpr{Name: "col"}
	collated := &parser.CollateExpr{Expr: inner, Collation: "NOCASE"}
	result := extractBaseExpr(collated)
	if result != inner {
		t.Errorf("expected inner IdentExpr, got %T", result)
	}
}

// TestExtractBaseExprNoCollate verifies that extractBaseExpr returns non-CollateExpr unchanged.
func TestExtractBaseExprNoCollate(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	result := extractBaseExpr(lit)
	if result != lit {
		t.Errorf("expected same LiteralExpr, got %T", result)
	}
}

// TestFlattenCompoundBothSides verifies the branch where c.Right.Compound != nil.
// Builds: (SELECT 1 UNION SELECT 2) UNION (SELECT 3 UNION SELECT 4)
// so that both left.Compound and right.Compound are non-nil.
func TestFlattenCompoundBothSides(t *testing.T) {
	makeLitSel := func(val string) *parser.SelectStmt {
		return &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: val}},
			},
		}
	}

	sel1 := makeLitSel("1")
	sel2 := makeLitSel("2")
	sel3 := makeLitSel("3")
	sel4 := makeLitSel("4")

	leftCompound := &parser.CompoundSelect{
		Op:    parser.CompoundUnion,
		Left:  sel1,
		Right: sel2,
	}
	rightCompound := &parser.CompoundSelect{
		Op:    parser.CompoundUnion,
		Left:  sel3,
		Right: sel4,
	}

	leftSel := &parser.SelectStmt{Compound: leftCompound}
	rightSel := &parser.SelectStmt{Compound: rightCompound}

	top := &parser.CompoundSelect{
		Op:    parser.CompoundUnion,
		Left:  leftSel,
		Right: rightSel,
	}

	ops, sels := flattenCompound(top)
	assertFlattenCompoundResult(t, ops, sels, []parser.CompoundOp{
		parser.CompoundUnion,
		parser.CompoundUnion,
		parser.CompoundUnion,
	}, []*parser.SelectStmt{sel1, sel2, sel3, sel4})
}

func assertFlattenCompoundResult(t *testing.T, ops []parser.CompoundOp, sels []*parser.SelectStmt, wantOps []parser.CompoundOp, wantSels []*parser.SelectStmt) {
	t.Helper()
	if len(ops) != len(wantOps) {
		t.Fatalf("expected %d ops, got %d", len(wantOps), len(ops))
	}
	if len(sels) != len(wantSels) {
		t.Fatalf("expected %d selects, got %d", len(wantSels), len(sels))
	}
	for i, op := range ops {
		if op != wantOps[i] {
			t.Errorf("ops[%d]: expected %v, got %v", i, wantOps[i], op)
		}
	}
	for i, sel := range sels {
		if sel != wantSels[i] {
			t.Errorf("sels[%d]: expected %p, got %p", i, wantSels[i], sel)
		}
	}
}

// TestTypeOrderDefault verifies the default branch of typeOrder returns 4
// for types that are not nil, int64, float64, string, or []byte.
func TestTypeOrderDefault(t *testing.T) {
	type customType struct{ x int }
	result := typeOrder(customType{x: 99})
	if result != 4 {
		t.Errorf("expected typeOrder default=4, got %d", result)
	}
}
