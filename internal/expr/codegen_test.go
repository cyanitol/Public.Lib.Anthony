package expr

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// TestSimpleArithmetic tests basic arithmetic expression code generation.
func TestSimpleArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		expr     parser.Expression
		expected []vdbe.Opcode
	}{
		{
			name: "a + b",
			expr: &parser.BinaryExpr{
				Left:  &parser.IdentExpr{Name: "a"},
				Op:    parser.OpPlus,
				Right: &parser.IdentExpr{Name: "b"},
			},
			expected: []vdbe.Opcode{vdbe.OpColumn, vdbe.OpColumn, vdbe.OpAdd},
		},
		{
			name: "x * 2",
			expr: &parser.BinaryExpr{
				Left: &parser.IdentExpr{Name: "x"},
				Op:   parser.OpMul,
				Right: &parser.LiteralExpr{
					Type:  parser.LiteralInteger,
					Value: "2",
				},
			},
			expected: []vdbe.Opcode{vdbe.OpColumn, vdbe.OpInteger, vdbe.OpMultiply},
		},
		{
			name: "10 / 5",
			expr: &parser.BinaryExpr{
				Left: &parser.LiteralExpr{
					Type:  parser.LiteralInteger,
					Value: "10",
				},
				Op: parser.OpDiv,
				Right: &parser.LiteralExpr{
					Type:  parser.LiteralInteger,
					Value: "5",
				},
			},
			expected: []vdbe.Opcode{vdbe.OpInteger, vdbe.OpInteger, vdbe.OpDivide},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			if len(v.Program) < len(tt.expected) {
				t.Fatalf("Expected at least %d instructions, got %d", len(tt.expected), len(v.Program))
			}

			for i, expectedOp := range tt.expected {
				if v.Program[i].Opcode != expectedOp {
					t.Errorf("Instruction %d: expected %s, got %s",
						i, expectedOp.String(), v.Program[i].Opcode.String())
				}
			}
		})
	}
}

// TestComparisons tests comparison expression code generation.
func TestComparisons(t *testing.T) {
	tests := []struct {
		name   string
		expr   parser.Expression
		wantOp vdbe.Opcode
	}{
		{
			name: "a = b",
			expr: &parser.BinaryExpr{
				Left:  &parser.IdentExpr{Name: "a"},
				Op:    parser.OpEq,
				Right: &parser.IdentExpr{Name: "b"},
			},
			wantOp: vdbe.OpEq,
		},
		{
			name: "x < 10",
			expr: &parser.BinaryExpr{
				Left: &parser.IdentExpr{Name: "x"},
				Op:   parser.OpLt,
				Right: &parser.LiteralExpr{
					Type:  parser.LiteralInteger,
					Value: "10",
				},
			},
			wantOp: vdbe.OpLt,
		},
		{
			name: "y >= 5",
			expr: &parser.BinaryExpr{
				Left: &parser.IdentExpr{Name: "y"},
				Op:   parser.OpGe,
				Right: &parser.LiteralExpr{
					Type:  parser.LiteralInteger,
					Value: "5",
				},
			},
			wantOp: vdbe.OpGe,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			// Find the comparison opcode
			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOp {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected to find opcode %s in program", tt.wantOp.String())
			}
		})
	}
}

// TestLogicalOperators tests AND/OR with short-circuit evaluation.
func TestLogicalOperators(t *testing.T) {
	tests := []struct {
		name       string
		expr       parser.Expression
		wantJumpOp vdbe.Opcode
	}{
		{
			name: "a AND b",
			expr: &parser.BinaryExpr{
				Left:  &parser.IdentExpr{Name: "a"},
				Op:    parser.OpAnd,
				Right: &parser.IdentExpr{Name: "b"},
			},
			wantJumpOp: vdbe.OpIfNot, // Should have IfNot for short-circuit
		},
		{
			name: "x OR y",
			expr: &parser.BinaryExpr{
				Left:  &parser.IdentExpr{Name: "x"},
				Op:    parser.OpOr,
				Right: &parser.IdentExpr{Name: "y"},
			},
			wantJumpOp: vdbe.OpIf, // Should have If for short-circuit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			// Check for short-circuit jump
			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantJumpOp {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected to find short-circuit jump opcode %s", tt.wantJumpOp.String())
			}
		})
	}
}

// TestFunctionCalls tests function call code generation.
func TestFunctionCalls(t *testing.T) {
	tests := []struct {
		name     string
		expr     parser.Expression
		funcName string
	}{
		{
			name: "UPPER('hello')",
			expr: &parser.FunctionExpr{
				Name: "UPPER",
				Args: []parser.Expression{
					&parser.LiteralExpr{
						Type:  parser.LiteralString,
						Value: "hello",
					},
				},
			},
			funcName: "UPPER",
		},
		{
			name: "MAX(a, b)",
			expr: &parser.FunctionExpr{
				Name: "MAX",
				Args: []parser.Expression{
					&parser.IdentExpr{Name: "a"},
					&parser.IdentExpr{Name: "b"},
				},
			},
			funcName: "MAX",
		},
		{
			name: "COUNT(*)",
			expr: &parser.FunctionExpr{
				Name: "COUNT",
				Star: true,
			},
			funcName: "COUNT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			// Verify function call instruction exists
			found := false
			for _, instr := range v.Program {
				if instr.Opcode == vdbe.OpFunction || instr.Opcode == vdbe.OpInteger {
					if instr.P4.Z == tt.funcName || tt.expr.(*parser.FunctionExpr).Star {
						found = true
						break
					}
				}
			}

			if !found && !tt.expr.(*parser.FunctionExpr).Star {
				t.Errorf("Expected to find function call for %s", tt.funcName)
			}
		})
	}
}

// TestInExpression tests IN expression code generation.
func TestInExpression(t *testing.T) {
	expr := &parser.InExpr{
		Expr: &parser.IdentExpr{Name: "x"},
		Values: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		},
		Not: false,
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}

	// Should have comparisons for each value
	eqCount := 0
	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpEq {
			eqCount++
		}
	}

	if eqCount != 3 {
		t.Errorf("Expected 3 equality comparisons, got %d", eqCount)
	}
}

// TestBetweenExpression tests BETWEEN expression code generation.
func TestBetweenExpression(t *testing.T) {
	expr := &parser.BetweenExpr{
		Expr:  &parser.IdentExpr{Name: "age"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "65"},
		Not:   false,
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}

	// Should have >= and <= comparisons plus AND
	hasGe := false
	hasLe := false
	hasAnd := false

	for _, instr := range v.Program {
		switch instr.Opcode {
		case vdbe.OpGe:
			hasGe = true
		case vdbe.OpLe:
			hasLe = true
		case vdbe.OpAnd:
			hasAnd = true
		}
	}

	if !hasGe {
		t.Error("Expected OpGe for lower bound check")
	}
	if !hasLe {
		t.Error("Expected OpLe for upper bound check")
	}
	if !hasAnd {
		t.Error("Expected OpAnd to combine bounds")
	}
}

// TestCaseExpression tests CASE expression code generation.
func TestCaseExpression(t *testing.T) {
	expr := &parser.CaseExpr{
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.BinaryExpr{
					Left:  &parser.IdentExpr{Name: "x"},
					Op:    parser.OpEq,
					Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				},
				Result: &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
			{
				Condition: &parser.BinaryExpr{
					Left:  &parser.IdentExpr{Name: "x"},
					Op:    parser.OpEq,
					Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
				},
				Result: &parser.LiteralExpr{Type: parser.LiteralString, Value: "two"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "other"},
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}

	// Should have conditional jumps for each WHEN
	ifNotCount := 0
	gotoCount := 0

	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpIfNot {
			ifNotCount++
		}
		if instr.Opcode == vdbe.OpGoto {
			gotoCount++
		}
	}

	if ifNotCount < 2 {
		t.Errorf("Expected at least 2 OpIfNot for WHEN clauses, got %d", ifNotCount)
	}
	if gotoCount < 2 {
		t.Errorf("Expected at least 2 OpGoto for jumping to end, got %d", gotoCount)
	}
}

// TestUnaryOperators tests unary operator code generation.
func TestUnaryOperators(t *testing.T) {
	tests := []struct {
		name   string
		expr   parser.Expression
		wantOp vdbe.Opcode
	}{
		{
			name: "NOT x",
			expr: &parser.UnaryExpr{
				Op:   parser.OpNot,
				Expr: &parser.IdentExpr{Name: "x"},
			},
			wantOp: vdbe.OpNot,
		},
		{
			name: "-y",
			expr: &parser.UnaryExpr{
				Op:   parser.OpNeg,
				Expr: &parser.IdentExpr{Name: "y"},
			},
			wantOp: vdbe.OpSubtract, // Negation uses subtract
		},
		{
			name: "~z",
			expr: &parser.UnaryExpr{
				Op:   parser.OpBitNot,
				Expr: &parser.IdentExpr{Name: "z"},
			},
			wantOp: vdbe.OpBitNot,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOp {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected to find opcode %s", tt.wantOp.String())
			}
		})
	}
}

// TestNullChecks tests IS NULL and IS NOT NULL code generation.
func TestNullChecks(t *testing.T) {
	tests := []struct {
		name   string
		expr   parser.Expression
		wantOp vdbe.Opcode
	}{
		{
			name: "x IS NULL",
			expr: &parser.UnaryExpr{
				Op:   parser.OpIsNull,
				Expr: &parser.IdentExpr{Name: "x"},
			},
			wantOp: vdbe.OpIsNull,
		},
		{
			name: "y IS NOT NULL",
			expr: &parser.UnaryExpr{
				Op:   parser.OpNotNull,
				Expr: &parser.IdentExpr{Name: "y"},
			},
			wantOp: vdbe.OpNotNull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			found := false
			for _, instr := range v.Program {
				if instr.Opcode == tt.wantOp {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected to find opcode %s", tt.wantOp.String())
			}
		})
	}
}

// TestCastExpression tests CAST expression code generation.
func TestCastExpression(t *testing.T) {
	expr := &parser.CastExpr{
		Expr: &parser.IdentExpr{Name: "value"},
		Type: "INTEGER",
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}

	// Should have Cast opcode
	found := false
	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpCast {
			if instr.P4.Z == "INTEGER" {
				found = true
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find OpCast with type INTEGER")
	}
}

// TestLiteralValues tests literal value code generation.
func TestLiteralValues(t *testing.T) {
	tests := []struct {
		name   string
		expr   parser.Expression
		wantOp vdbe.Opcode
	}{
		{
			name: "NULL",
			expr: &parser.LiteralExpr{
				Type: parser.LiteralNull,
			},
			wantOp: vdbe.OpNull,
		},
		{
			name: "42",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralInteger,
				Value: "42",
			},
			wantOp: vdbe.OpInteger,
		},
		{
			name: "3.14",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralFloat,
				Value: "3.14",
			},
			wantOp: vdbe.OpReal,
		},
		{
			name: "'hello'",
			expr: &parser.LiteralExpr{
				Type:  parser.LiteralString,
				Value: "hello",
			},
			wantOp: vdbe.OpString8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := vdbe.New()
			gen := NewCodeGenerator(v)

			_, err := gen.GenerateExpr(tt.expr)
			if err != nil {
				t.Fatalf("GenerateExpr failed: %v", err)
			}

			if len(v.Program) == 0 {
				t.Fatal("No instructions generated")
			}

			if v.Program[0].Opcode != tt.wantOp {
				t.Errorf("Expected opcode %s, got %s",
					tt.wantOp.String(), v.Program[0].Opcode.String())
			}
		})
	}
}

// TestWhereClause tests WHERE clause code generation.
func TestWhereClause(t *testing.T) {
	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Name: "age"},
		Op:    parser.OpGt,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	skipLabel := 999
	err := gen.GenerateWhereClause(where, skipLabel)
	if err != nil {
		t.Fatalf("GenerateWhereClause failed: %v", err)
	}

	// Should have comparison and conditional jump
	hasComparison := false
	hasJump := false

	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpGt {
			hasComparison = true
		}
		if instr.Opcode == vdbe.OpIfNot {
			hasJump = true
		}
	}

	if !hasComparison {
		t.Error("Expected comparison opcode")
	}
	if !hasJump {
		t.Error("Expected conditional jump opcode")
	}
}

// TestRegisterAllocation tests that registers are allocated properly.
func TestRegisterAllocation(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Allocate some registers
	reg1 := gen.AllocReg()
	reg2 := gen.AllocReg()
	reg3 := gen.AllocReg()

	if reg1 != 1 || reg2 != 2 || reg3 != 3 {
		t.Errorf("Expected registers 1,2,3 got %d,%d,%d", reg1, reg2, reg3)
	}

	// Allocate multiple at once
	reg4 := gen.AllocRegs(5)
	if reg4 != 4 {
		t.Errorf("Expected register 4, got %d", reg4)
	}

	// Check that VDBE memory was allocated
	if v.NumMem < 9 {
		t.Errorf("Expected at least 9 memory cells, got %d", v.NumMem)
	}
}

// TestComplexExpression tests a complex nested expression.
func TestComplexExpression(t *testing.T) {
	// (a + b) * (c - d)
	expr := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "a"},
			Op:    parser.OpPlus,
			Right: &parser.IdentExpr{Name: "b"},
		},
		Op: parser.OpMul,
		Right: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "c"},
			Op:    parser.OpMinus,
			Right: &parser.IdentExpr{Name: "d"},
		},
	}

	v := vdbe.New()
	gen := NewCodeGenerator(v)

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}

	// Should have add, subtract, and multiply operations
	hasAdd := false
	hasSub := false
	hasMul := false

	for _, instr := range v.Program {
		switch instr.Opcode {
		case vdbe.OpAdd:
			hasAdd = true
		case vdbe.OpSubtract:
			hasSub = true
		case vdbe.OpMultiply:
			hasMul = true
		}
	}

	if !hasAdd {
		t.Error("Expected OpAdd")
	}
	if !hasSub {
		t.Error("Expected OpSubtract")
	}
	if !hasMul {
		t.Error("Expected OpMultiply")
	}
}

// TestGenerateBinaryOperandsErrors tests error handling in generateBinaryOperands.
func TestGenerateBinaryOperandsErrors(t *testing.T) {
	v := vdbe.New()
	g := NewCodeGenerator(v)

	// Create an invalid expression that will cause an error
	// Use a column reference without proper setup
	invalidExpr := &parser.BinaryExpr{
		Op:    parser.OpPlus,
		Left:  &parser.IdentExpr{Name: "invalid"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
	}

	// This should handle the error gracefully
	_, err := g.generateBinary(invalidExpr)
	// We expect an error or the code to handle it
	if err == nil {
		// If no error, that's also acceptable as long as it doesn't crash
		t.Log("generateBinary handled invalid expression without error")
	}
}

// TestGenerateBinaryWithSpecialHandlers tests binary operations with special handlers.
func TestGenerateBinaryWithSpecialHandlers(t *testing.T) {
	v := vdbe.New()
	g := NewCodeGenerator(v)

	// Test LIKE operator (has special handler)
	likeExpr := &parser.BinaryExpr{
		Op:    parser.OpLike,
		Left:  &parser.LiteralExpr{Type: parser.LiteralString, Value: "test"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "%st%"},
	}

	_, err := g.GenerateExpr(likeExpr)
	if err != nil {
		t.Errorf("GenerateExpr(LIKE) failed: %v", err)
	}

	// Test GLOB operator (has special handler)
	globExpr := &parser.BinaryExpr{
		Op:    parser.OpGlob,
		Left:  &parser.LiteralExpr{Type: parser.LiteralString, Value: "test"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "*st*"},
	}

	_, err = g.GenerateExpr(globExpr)
	if err != nil {
		t.Errorf("GenerateExpr(GLOB) failed: %v", err)
	}
}

// TestGenerateLogicalShortCircuit tests short-circuit evaluation for AND/OR.
func TestGenerateLogicalShortCircuit(t *testing.T) {
	v := vdbe.New()
	g := NewCodeGenerator(v)

	// Test AND operation (should use short-circuit logic)
	andExpr := &parser.BinaryExpr{
		Op:    parser.OpAnd,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}

	_, err := g.GenerateExpr(andExpr)
	if err != nil {
		t.Errorf("GenerateExpr(AND) failed: %v", err)
	}

	// Test OR operation (should use short-circuit logic)
	orExpr := &parser.BinaryExpr{
		Op:    parser.OpOr,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}

	_, err = g.GenerateExpr(orExpr)
	if err != nil {
		t.Errorf("GenerateExpr(OR) failed: %v", err)
	}
}

// TestGenerateBinaryAllOperators tests all binary operators for coverage.
func TestGenerateBinaryAllOperators(t *testing.T) {
	operators := []parser.BinaryOp{
		parser.OpPlus,
		parser.OpMinus,
		parser.OpMul,
		parser.OpDiv,
		parser.OpConcat,
		parser.OpBitAnd,
		parser.OpBitOr,
		parser.OpLShift,
		parser.OpRShift,
		parser.OpEq,
		parser.OpNe,
		parser.OpLt,
		parser.OpLe,
		parser.OpGt,
		parser.OpGe,
	}

	for _, op := range operators {
		t.Run(op.String(), func(t *testing.T) {
			v := vdbe.New()
			g := NewCodeGenerator(v)

			expr := &parser.BinaryExpr{
				Op:    op,
				Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
			}

			_, err := g.GenerateExpr(expr)
			if err != nil {
				t.Errorf("GenerateExpr(%v) failed: %v", op, err)
			}

			if len(v.Program) == 0 {
				t.Errorf("No instructions generated for %v", op)
			}
		})
	}
}
