// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestGenerateInSubquery tests IN (SELECT ...) expression code generation.
func TestGenerateInSubquery(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := NewCodeGenerator(v)

	// Set up a mock subquery compiler
	g.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		// Simulate a simple subquery that returns values
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	// Register a table for testing
	g.RegisterTable(TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
			{Name: "name", Index: 1},
		},
	})
	g.RegisterCursor("users", 0)

	// Create an IN expression with a subquery
	// SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
	inExpr := &parser.InExpr{
		Expr: &parser.IdentExpr{
			Name:  "id",
			Table: "users",
		},
		Select: &parser.SelectStmt{
			// In a real test, this would have proper SELECT structure
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "user_id"}},
			},
		},
		Not: false,
	}

	resultReg, err := g.generateIn(inExpr)
	if err != nil {
		t.Fatalf("generateIn failed: %v", err)
	}

	if resultReg == 0 {
		t.Error("expected non-zero result register")
	}

	// Verify bytecode was generated
	if v.NumOps() == 0 {
		t.Error("expected bytecode to be generated")
	}

	// Check that essential opcodes are present
	assertOpcodePresent(t, v, vdbe.OpOpenEphemeral, "open ephemeral table for subquery results")
	assertOpcodePresent(t, v, vdbe.OpRewind, "iterate through subquery results")
	assertOpcodePresent(t, v, vdbe.OpClose, "close ephemeral table")
}

// TestGenerateNotInSubquery tests NOT IN (SELECT ...) expression.
func TestGenerateNotInSubquery(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := NewCodeGenerator(v)

	// Set up a mock subquery compiler
	g.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	g.RegisterTable(TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
		},
	})
	g.RegisterCursor("users", 0)

	inExpr := &parser.InExpr{
		Expr: &parser.IdentExpr{
			Name:  "id",
			Table: "users",
		},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "user_id"}},
			},
		},
		Not: true, // NOT IN
	}

	resultReg, err := g.generateIn(inExpr)
	if err != nil {
		t.Fatalf("generateIn failed: %v", err)
	}

	if resultReg == 0 {
		t.Error("expected non-zero result register")
	}

	// Check for NOT operation
	foundNot := false
	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		if op.Opcode == vdbe.OpNot {
			foundNot = true
			break
		}
	}

	if !foundNot {
		t.Error("expected OpNot for NOT IN")
	}
}

// TestGenerateScalarSubquery tests scalar subquery expression code generation.
func TestGenerateScalarSubquery(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := newSubqueryCodeGen(v)

	// Create a scalar subquery expression
	subqueryExpr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{
					Expr: &parser.FunctionExpr{
						Name: "MAX",
						Args: []parser.Expression{
							&parser.IdentExpr{Name: "salary"},
						},
					},
				},
			},
		},
	}

	resultReg, err := g.generateSubquery(subqueryExpr)
	if err != nil {
		t.Fatalf("generateSubquery failed: %v", err)
	}

	if resultReg == 0 {
		t.Error("expected non-zero result register")
	}

	// Verify bytecode structure
	assertOpcodePresent(t, v, vdbe.OpNull, "initialize result to NULL")
	assertOpcodePresent(t, v, vdbe.OpCopy, "capture subquery result")
	assertOpcodePresent(t, v, vdbe.OpGoto, "skip to end after capturing result")
	assertOpcodePresent(t, v, vdbe.OpNoop, "replace OpHalt from subquery")
}

// TestGenerateExists tests EXISTS (SELECT ...) expression code generation.
func TestGenerateExists(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := newSubqueryCodeGen(v)

	existsExpr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: false,
	}

	resultReg, err := g.generateExists(existsExpr)
	if err != nil {
		t.Fatalf("generateExists failed: %v", err)
	}

	if resultReg == 0 {
		t.Error("expected non-zero result register")
	}

	// Verify bytecode structure
	assertIntegerOpPresent(t, v, 0, "initialize result to false")
	assertIntegerOpPresent(t, v, 1, "set result to true when row found")
	assertOpcodePresent(t, v, vdbe.OpGoto, "skip to end when row found")
}

// assertIntegerOpPresent checks that an OpInteger with the given P1 value exists.
func assertIntegerOpPresent(t *testing.T, v *vdbe.VDBE, p1 int, desc string) {
	t.Helper()
	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		if op.Opcode == vdbe.OpInteger && op.P1 == p1 {
			return
		}
	}
	t.Errorf("expected OpInteger(%d) to %s", p1, desc)
}

// TestGenerateNotExists tests NOT EXISTS (SELECT ...) expression.
func TestGenerateNotExists(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := NewCodeGenerator(v)

	// Set up a mock subquery compiler
	g.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	existsExpr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: true, // NOT EXISTS
	}

	resultReg, err := g.generateExists(existsExpr)
	if err != nil {
		t.Fatalf("generateExists failed: %v", err)
	}

	if resultReg == 0 {
		t.Error("expected non-zero result register")
	}

	// Check for NOT operation
	foundNot := false
	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		if op.Opcode == vdbe.OpNot {
			foundNot = true
			break
		}
	}

	if !foundNot {
		t.Error("expected OpNot for NOT EXISTS")
	}
}

// TestSubqueryExpressionTypes tests that all subquery expression types are registered.
func TestSubqueryExpressionTypes(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := NewCodeGenerator(v)

	// Test that SubqueryExpr is handled
	subqueryExpr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{},
	}

	_, err := g.GenerateExpr(subqueryExpr)
	if err != nil {
		t.Logf("SubqueryExpr generation: %v (expected - SELECT compilation needed)", err)
	}

	// Test that ExistsExpr is handled
	existsExpr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
	}

	_, err = g.GenerateExpr(existsExpr)
	if err != nil {
		t.Logf("ExistsExpr generation: %v (expected - SELECT compilation needed)", err)
	}

	// Test that InExpr with Select is handled
	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{},
	}

	_, err = g.GenerateExpr(inExpr)
	if err != nil {
		t.Logf("InExpr with subquery generation: %v (expected - SELECT compilation needed)", err)
	}
}

// TestSubqueryNilCheck tests that nil checks work properly.
func TestSubqueryNilCheck(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(100)

	g := NewCodeGenerator(v)

	// Test SubqueryExpr with nil Select
	subqueryExpr := &parser.SubqueryExpr{
		Select: nil,
	}

	_, err := g.generateSubquery(subqueryExpr)
	if err == nil {
		t.Error("expected error for SubqueryExpr with nil Select")
	}

	// Test ExistsExpr with nil Select
	existsExpr := &parser.ExistsExpr{
		Select: nil,
	}

	_, err = g.generateExists(existsExpr)
	if err == nil {
		t.Error("expected error for ExistsExpr with nil Select")
	}
}

// TestSubqueryBytecodeComments tests that generated bytecode has proper comments.
func TestSubqueryBytecodeComments(t *testing.T) {
	t.Parallel()
	t.Run("IN subquery comments", func(t *testing.T) {
		v := vdbe.New()
		v.AllocMemory(100)
		g := newSubqueryCodeGen(v)

		inExpr := &parser.InExpr{
			Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			Select: &parser.SelectStmt{},
		}
		_, _ = g.generateIn(inExpr)

		assertCommentPresent(t, v, []string{
			"IN subquery: init result to false",
			"IN subquery: open ephemeral table",
		}, "IN subquery")
	})

	t.Run("EXISTS comments", func(t *testing.T) {
		v2 := vdbe.New()
		v2.AllocMemory(100)
		g2 := newSubqueryCodeGen(v2)

		existsExpr := &parser.ExistsExpr{Select: &parser.SelectStmt{}}
		_, _ = g2.generateExists(existsExpr)

		assertCommentPresent(t, v2, []string{
			"EXISTS: init result to false",
			"EXISTS subquery: start",
		}, "EXISTS")
	})

	t.Run("scalar subquery comments", func(t *testing.T) {
		v3 := vdbe.New()
		v3.AllocMemory(100)
		g3 := newSubqueryCodeGen(v3)

		scalarExpr := &parser.SubqueryExpr{Select: &parser.SelectStmt{}}
		_, _ = g3.generateSubquery(scalarExpr)

		assertCommentPresent(t, v3, []string{
			"Scalar subquery: init result to NULL",
			"Scalar subquery: start",
		}, "scalar subquery")
	})
}

// assertOpcodePresent checks that an opcode exists in the VDBE program.
func assertOpcodePresent(t *testing.T, v *vdbe.VDBE, opcode vdbe.Opcode, desc string) {
	t.Helper()
	for i := 0; i < v.NumOps(); i++ {
		if v.Program[i].Opcode == opcode {
			return
		}
	}
	t.Errorf("expected %v to %s", opcode, desc)
}

// assertCommentPresent checks that one of the given comments exists in the VDBE program.
func assertCommentPresent(t *testing.T, v *vdbe.VDBE, comments []string, desc string) {
	t.Helper()
	commentSet := make(map[string]bool, len(comments))
	for _, c := range comments {
		commentSet[c] = true
	}
	for i := 0; i < v.NumOps(); i++ {
		if commentSet[v.Program[i].Comment] {
			return
		}
	}
	t.Errorf("expected meaningful comments in %s bytecode", desc)
}

// newSubqueryCodeGen creates a CodeGenerator with a mock subquery compiler.
func newSubqueryCodeGen(v *vdbe.VDBE) *CodeGenerator {
	g := NewCodeGenerator(v)
	g.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})
	return g
}
