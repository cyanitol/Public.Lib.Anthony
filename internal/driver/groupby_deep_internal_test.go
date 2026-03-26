// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// Direct unit tests for evaluateGroupByExprs, which is scaffolding code
// that is not yet called by the active GROUP BY compile path.

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// newEvalGBStmt opens an in-memory Conn and returns a *Stmt suitable for
// calling evaluateGroupByExprs directly.
func newEvalGBStmt(t *testing.T) *Stmt {
	t.Helper()
	conn := openMemConn(t)
	return stmtFor(conn)
}

// ---------------------------------------------------------------------------
// evaluateGroupByExprs — direct unit tests
// ---------------------------------------------------------------------------

// TestEvaluateGroupByExprs_IntegerLiterals calls evaluateGroupByExprs with two
// integer-literal GROUP BY expressions.  It verifies the function succeeds and
// emits OpCopy instructions for each expression.
func TestEvaluateGroupByExprs_IntegerLiterals(t *testing.T) {
	t.Parallel()
	s := newEvalGBStmt(t)

	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	stmt := &parser.SelectStmt{
		GroupBy: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
		},
	}
	groupByRegs := []int{5, 6}

	err := s.evaluateGroupByExprs(vm, gen, stmt, groupByRegs)
	if err != nil {
		t.Fatalf("evaluateGroupByExprs with integer literals: %v", err)
	}

	// Each GROUP BY expression should have produced at least one instruction
	// (GenerateExpr for the literal) plus an OpCopy.
	if len(vm.Program) == 0 {
		t.Error("expected instructions in VDBE program after evaluateGroupByExprs")
	}

	// Verify that OpCopy instructions appear targeting groupByRegs[0] and groupByRegs[1].
	foundCopy := 0
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpCopy {
			if instr.P2 == groupByRegs[0] || instr.P2 == groupByRegs[1] {
				foundCopy++
			}
		}
	}
	if foundCopy < 2 {
		t.Errorf("evaluateGroupByExprs: want >=2 OpCopy to groupByRegs, got %d", foundCopy)
	}
}

// TestEvaluateGroupByExprs_StringLiteral calls evaluateGroupByExprs with a
// string literal GROUP BY expression, covering the single-expression path.
func TestEvaluateGroupByExprs_StringLiteral(t *testing.T) {
	t.Parallel()
	s := newEvalGBStmt(t)

	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	stmt := &parser.SelectStmt{
		GroupBy: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralString, Value: "dept"},
		},
	}
	groupByRegs := []int{3}

	err := s.evaluateGroupByExprs(vm, gen, stmt, groupByRegs)
	if err != nil {
		t.Fatalf("evaluateGroupByExprs with string literal: %v", err)
	}

	// Must emit at least the OpString + OpCopy
	if len(vm.Program) < 2 {
		t.Errorf("evaluateGroupByExprs: want >=2 instructions, got %d", len(vm.Program))
	}
}

// TestEvaluateGroupByExprs_EmptyGroupBy calls evaluateGroupByExprs with an
// empty GroupBy slice (zero expressions).  The function should return nil and
// emit no instructions.
func TestEvaluateGroupByExprs_EmptyGroupBy(t *testing.T) {
	t.Parallel()
	s := newEvalGBStmt(t)

	vm := vdbe.New()
	gen := expr.NewCodeGenerator(vm)

	stmt := &parser.SelectStmt{
		GroupBy: []parser.Expression{},
	}
	groupByRegs := []int{}

	err := s.evaluateGroupByExprs(vm, gen, stmt, groupByRegs)
	if err != nil {
		t.Fatalf("evaluateGroupByExprs with empty GroupBy: %v", err)
	}
	if len(vm.Program) != 0 {
		t.Errorf("empty GroupBy: want 0 instructions, got %d", len(vm.Program))
	}
}

// TestEvaluateGroupByExprs_BinaryExpr calls evaluateGroupByExprs with a
// BinaryExpr (arithmetic expression) as the GROUP BY key, covering the
// binary-expression code-generation branch.
func TestEvaluateGroupByExprs_BinaryExpr(t *testing.T) {
	t.Parallel()
	s := newEvalGBStmt(t)

	vm := vdbe.New()
	vm.AllocMemory(20)
	gen := expr.NewCodeGenerator(vm)

	// Represents: 42 % 3
	stmt := &parser.SelectStmt{
		GroupBy: []parser.Expression{
			&parser.BinaryExpr{
				Op:    parser.OpRem,
				Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
			},
		},
	}
	groupByRegs := []int{7}

	err := s.evaluateGroupByExprs(vm, gen, stmt, groupByRegs)
	if err != nil {
		t.Fatalf("evaluateGroupByExprs with BinaryExpr: %v", err)
	}

	// The binary expression should generate multiple instructions
	if len(vm.Program) < 2 {
		t.Errorf("BinaryExpr GROUP BY: want >=2 instructions, got %d", len(vm.Program))
	}
}
