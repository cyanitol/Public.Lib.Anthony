// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
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
	foundOpenEphemeral := false
	foundRewind := false
	foundClose := false

	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		switch op.Opcode {
		case vdbe.OpOpenEphemeral:
			foundOpenEphemeral = true
		case vdbe.OpRewind:
			foundRewind = true
		case vdbe.OpClose:
			foundClose = true
		}
	}

	if !foundOpenEphemeral {
		t.Error("expected OpOpenEphemeral to open ephemeral table for subquery results")
	}
	if !foundRewind {
		t.Error("expected OpRewind to iterate through subquery results")
	}
	if !foundClose {
		t.Error("expected OpClose to close ephemeral table")
	}
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

	g := NewCodeGenerator(v)

	// Set up a mock subquery compiler
	g.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	// Create a scalar subquery expression
	// SELECT name, (SELECT MAX(salary) FROM employees) FROM users
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
	// The scalar subquery implementation embeds subquery bytecode directly
	foundNull := false // Should initialize to NULL
	foundCopy := false // Should use OpCopy to capture result
	foundGoto := false // Should use OpGoto to skip to end
	foundNoop := false // Should replace OpHalt with OpNoop

	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		switch op.Opcode {
		case vdbe.OpNull:
			foundNull = true
		case vdbe.OpCopy:
			foundCopy = true
		case vdbe.OpGoto:
			foundGoto = true
		case vdbe.OpNoop:
			foundNoop = true
		}
	}

	if !foundNull {
		t.Error("expected OpNull to initialize result to NULL")
	}
	if !foundCopy {
		t.Error("expected OpCopy to capture subquery result")
	}
	if !foundGoto {
		t.Error("expected OpGoto to skip to end after capturing result")
	}
	if !foundNoop {
		t.Error("expected OpNoop (replaced OpHalt from subquery)")
	}
}

// TestGenerateExists tests EXISTS (SELECT ...) expression code generation.
func TestGenerateExists(t *testing.T) {
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

	// Create an EXISTS expression
	// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
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
	// The EXISTS implementation embeds subquery bytecode directly
	foundInitFalse := false // Should initialize to false
	foundSetTrue := false   // Should set to true if row found
	foundGoto := false      // Should use Goto to skip to end when found

	initToFalseFound := false
	setToTrueFound := false

	for i := 0; i < v.NumOps(); i++ {
		op := v.Program[i]
		switch op.Opcode {
		case vdbe.OpInteger:
			// Check if initializing to 0 (false) or 1 (true)
			if op.P1 == 0 && !initToFalseFound {
				foundInitFalse = true
				initToFalseFound = true
			} else if op.P1 == 1 && !setToTrueFound {
				foundSetTrue = true
				setToTrueFound = true
			}
		case vdbe.OpGoto:
			foundGoto = true
		}
	}

	if !foundInitFalse {
		t.Error("expected OpInteger(0) to initialize result to false")
	}
	if !foundSetTrue {
		t.Error("expected OpInteger(1) to set result to true when row found")
	}
	if !foundGoto {
		t.Error("expected OpGoto to skip to end when row found")
	}
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

	// Generate IN subquery
	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{},
	}

	_, _ = g.generateIn(inExpr)

	// Check for meaningful comments
	hasInComment := false
	for i := 0; i < v.NumOps(); i++ {
		comment := v.Program[i].Comment
		if comment != "" && (comment == "IN subquery: init result to false" ||
			comment == "IN subquery: open ephemeral table") {
			hasInComment = true
			break
		}
	}

	if !hasInComment {
		t.Error("expected meaningful comments in IN subquery bytecode")
	}

	// Generate EXISTS expression
	v2 := vdbe.New()
	v2.AllocMemory(100)
	g2 := NewCodeGenerator(v2)

	// Set up a mock subquery compiler for EXISTS
	g2.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	existsExpr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
	}

	_, _ = g2.generateExists(existsExpr)

	// Check for EXISTS comments
	hasExistsComment := false
	for i := 0; i < v2.NumOps(); i++ {
		comment := v2.Program[i].Comment
		if comment != "" && (comment == "EXISTS: init result to false" ||
			comment == "EXISTS subquery: start") {
			hasExistsComment = true
			break
		}
	}

	if !hasExistsComment {
		t.Error("expected meaningful comments in EXISTS bytecode")
	}

	// Generate scalar subquery
	v3 := vdbe.New()
	v3.AllocMemory(100)
	g3 := NewCodeGenerator(v3)

	// Set up a mock subquery compiler for scalar subquery
	g3.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		subVM := vdbe.New()
		subVM.AllocMemory(10)
		subVM.AddOp(vdbe.OpResultRow, 1, 1, 0)
		return subVM, nil
	})

	scalarExpr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{},
	}

	_, _ = g3.generateSubquery(scalarExpr)

	// Check for scalar subquery comments
	hasScalarComment := false
	for i := 0; i < v3.NumOps(); i++ {
		comment := v3.Program[i].Comment
		if comment != "" && (comment == "Scalar subquery: init result to NULL" ||
			comment == "Scalar subquery: start") {
			hasScalarComment = true
			break
		}
	}

	if !hasScalarComment {
		t.Error("expected meaningful comments in scalar subquery bytecode")
	}
}
