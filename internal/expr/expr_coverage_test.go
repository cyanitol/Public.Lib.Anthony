// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// CodeGenerator accessor methods - 0% coverage
// ============================================================================

func TestSetPrecomputed(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	gen.SetPrecomputed(expr, 5)

	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}
	if reg != 5 {
		t.Errorf("Expected precomputed register 5, got %d", reg)
	}
}

func TestSetPrecomputedNilMap(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// precomputed is nil initially; SetPrecomputed should initialise it
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	gen.SetPrecomputed(expr, 3)
	gen.SetPrecomputed(expr, 7) // overwrite to confirm map is reused
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}
	if reg != 7 {
		t.Errorf("Expected precomputed register 7, got %d", reg)
	}
}

func TestGetVDBE(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	if got := gen.GetVDBE(); got != v {
		t.Errorf("GetVDBE returned wrong VDBE")
	}
}

func TestHasNonZeroCursorFalse(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("t", 0)
	if gen.HasNonZeroCursor() {
		t.Error("Expected false when all cursors are zero")
	}
}

func TestHasNonZeroCursorTrue(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("t", 3)
	if !gen.HasNonZeroCursor() {
		t.Error("Expected true when a cursor is non-zero")
	}
}

func TestHasNonZeroCursorEmpty(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	if gen.HasNonZeroCursor() {
		t.Error("Expected false for empty cursor map")
	}
}

func TestParamIndex(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetParamIndex(5)
	if idx := gen.ParamIndex(); idx != 5 {
		t.Errorf("Expected ParamIndex 5, got %d", idx)
	}
}

func TestCollationForReg(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Reg with no collation
	coll, ok := gen.CollationForReg(1)
	if ok || coll != "" {
		t.Errorf("Expected no collation, got %q, %v", coll, ok)
	}

	// Set a collation then retrieve
	gen.SetCollationForReg(2, "NOCASE")
	coll, ok = gen.CollationForReg(2)
	if !ok || coll != "NOCASE" {
		t.Errorf("Expected NOCASE, got %q, %v", coll, ok)
	}
}

func TestSetCollationForRegEmpty(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Empty collation should be ignored
	gen.SetCollationForReg(1, "")
	_, ok := gen.CollationForReg(1)
	if ok {
		t.Error("Expected no collation for empty string")
	}
}

func TestSetNextCursor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// nextCursor starts at 0
	gen.SetNextCursor(5)
	// Allocate cursor; should start at 5
	c := gen.AllocCursor()
	if c != 5 {
		t.Errorf("Expected cursor 5 after SetNextCursor(5), got %d", c)
	}
	// A lower value should not reduce it
	gen.SetNextCursor(3)
	c2 := gen.AllocCursor()
	if c2 != 6 {
		t.Errorf("Expected cursor 6, got %d", c2)
	}
}

// ============================================================================
// valueToLiteral - 0% coverage
// ============================================================================

func TestValueToLiteralTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   interface{}
		litType parser.LiteralType
	}{
		{"int64", int64(42), parser.LiteralInteger},
		{"float64", float64(3.14), parser.LiteralFloat},
		{"string", "hello", parser.LiteralString},
		{"nil", nil, parser.LiteralNull},
		{"other", []byte("x"), parser.LiteralString},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lit := valueToLiteral(tt.input)
			le, ok := lit.(*parser.LiteralExpr)
			if !ok {
				t.Fatalf("expected *parser.LiteralExpr, got %T", lit)
			}
			if le.Type != tt.litType {
				t.Errorf("expected type %v, got %v", tt.litType, le.Type)
			}
		})
	}
}

// ============================================================================
// emitLiteralValue - 33.3% coverage
// ============================================================================

func TestEmitLiteralValueAllTypes(t *testing.T) {
	t.Parallel()

	cases := []interface{}{
		int64(10),
		float64(2.5),
		"hello",
		nil,
		[]byte("other"),
	}

	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			gen := NewCodeGenerator(v)
			reg := gen.AllocReg()
			gen.emitLiteralValue(reg, c)
			// Just verify no panic and at least one instruction was emitted
			if v.NumOps() == 0 {
				t.Error("Expected at least one instruction")
			}
		})
	}
}

// ============================================================================
// generateSimpleCaseCondition - 0% coverage
// ============================================================================

func TestGenerateSimpleCaseCondition(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Simple CASE: CASE x WHEN 1 THEN 'one' END
	expr := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "forty-two"},
			},
		},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero result register")
	}
}

// ============================================================================
// generateRaise - 0% coverage
// ============================================================================

func TestGenerateRaiseIgnore(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.RaiseExpr{
		Type:    parser.RaiseIgnore,
		Message: "",
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for RAISE(IGNORE) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateRaiseAbort(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.RaiseExpr{
		Type:    parser.RaiseAbort,
		Message: "constraint failed",
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for RAISE(ABORT) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// generateIsExpr / generateIsNotExpr / generateNullSafeCompare - 0% coverage
// ============================================================================

func TestGenerateIsExpr(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
		Op:    parser.OpIs,
		Right: &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for IS failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateIsNotExpr(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
		Op:    parser.OpIsNot,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for IS NOT failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateIsDistinctFrom(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpIsDistinctFrom,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for IS DISTINCT FROM failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateIsNotDistinctFrom(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Op:    parser.OpIsNotDistinctFrom,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for IS NOT DISTINCT FROM failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// adjustInstructionJumps / adjustJumpTarget - 0% coverage
// ============================================================================

func TestAdjustInstructionJumpsWithRule(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{5: 10}

	// OpGoto has adjustP2:true rule
	instr := &vdbe.Instruction{Opcode: vdbe.OpGoto, P2: 5}
	gen.adjustInstructionJumps(instr, addrMap)
	if instr.P2 != 10 {
		t.Errorf("Expected P2=10 after adjustment, got %d", instr.P2)
	}
}

func TestAdjustInstructionJumpsNoRule(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{5: 10}

	// OpAdd has no jump rule
	instr := &vdbe.Instruction{Opcode: vdbe.OpAdd, P2: 5}
	gen.adjustInstructionJumps(instr, addrMap)
	if instr.P2 != 5 {
		t.Errorf("Expected P2 unchanged at 5, got %d", instr.P2)
	}
}

func TestAdjustJumpTargetUnmapped(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{5: 10}
	param := 99
	gen.adjustJumpTarget(&param, addrMap)
	if param != 99 {
		t.Errorf("Expected param unchanged at 99, got %d", param)
	}
}

func TestAdjustJumpTargetZero(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{0: 5}
	param := 0
	gen.adjustJumpTarget(&param, addrMap)
	// P2=0 should not be adjusted (condition is >0)
	if param != 0 {
		t.Errorf("Expected param unchanged at 0, got %d", param)
	}
}

// ============================================================================
// adjustSubqueryCursors - 0% coverage
// ============================================================================

func TestAdjustSubqueryCursorsZeroOffset(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpOpenRead, 0, 0, 0)
	// offset=0 should be a no-op
	gen.adjustSubqueryCursors(sub, 0)
	if sub.Program[0].P1 != 0 {
		t.Errorf("Expected P1 unchanged at 0, got %d", sub.Program[0].P1)
	}
}

func TestAdjustSubqueryCursorsWithOffset(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpOpenRead, 2, 0, 0)
	gen.adjustSubqueryCursors(sub, 3)
	if sub.Program[0].P1 != 5 {
		t.Errorf("Expected P1=5 after cursor adjustment, got %d", sub.Program[0].P1)
	}
}

// ============================================================================
// adjustSubqueryRegisters / adjustInstructionRegisters - 0% coverage
// ============================================================================

func TestAdjustSubqueryRegistersZeroOffset(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpAdd, 1, 2, 3)
	gen.adjustSubqueryRegisters(sub, 0)
	// No change expected
	if sub.Program[0].P1 != 1 || sub.Program[0].P3 != 3 {
		t.Errorf("Expected registers unchanged, got P1=%d P3=%d", sub.Program[0].P1, sub.Program[0].P3)
	}
}

func TestAdjustSubqueryRegistersWithOffset(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	sub := vdbe.New()
	sub.AddOp(vdbe.OpAdd, 1, 2, 3)
	gen.adjustSubqueryRegisters(sub, 10)
	if sub.Program[0].P1 != 11 {
		t.Errorf("Expected P1=11 after register adjustment, got %d", sub.Program[0].P1)
	}
	if sub.Program[0].P3 != 13 {
		t.Errorf("Expected P3=13 after register adjustment, got %d", sub.Program[0].P3)
	}
}

func TestAdjustInstructionRegistersNoRule(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	instr := &vdbe.Instruction{Opcode: vdbe.OpNoop, P1: 1, P3: 3}
	gen.adjustInstructionRegisters(instr, 10)
	if instr.P1 != 1 || instr.P3 != 3 {
		t.Errorf("Expected registers unchanged for OpNoop, got P1=%d P3=%d", instr.P1, instr.P3)
	}
}

// ============================================================================
// emitHaltReplacement / copyAndAdjustInstruction - 0% coverage
// ============================================================================

func TestEmitHaltReplacement(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	before := v.NumOps()
	gen.emitHaltReplacement()
	if v.NumOps() != before+1 {
		t.Errorf("Expected one instruction emitted, got %d", v.NumOps()-before)
	}
	if v.Program[before].Opcode != vdbe.OpNoop {
		t.Errorf("Expected OpNoop, got %v", v.Program[before].Opcode)
	}
}

func TestCopyAndAdjustInstruction(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{3: 7}
	instr := &vdbe.Instruction{Opcode: vdbe.OpGoto, P2: 3}
	before := v.NumOps()
	gen.copyAndAdjustInstruction(instr, addrMap)
	if v.NumOps() != before+1 {
		t.Errorf("Expected one instruction appended")
	}
	if v.Program[before].P2 != 7 {
		t.Errorf("Expected P2=7 after copy+adjust, got %d", v.Program[before].P2)
	}
}

// ============================================================================
// generateSubqueryBytecodeEmbedding / generateExistsBytecodeEmbedding - 0% coverage
// (Both require subqueryCompiler; test the error paths)
// ============================================================================

func TestGenerateSubqueryNoCompilerOrExecutor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{},
	}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Error("Expected error when no subquery compiler or executor set")
	}
}

func TestGenerateExistsNoCompilerOrExecutor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
	}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Error("Expected error when no subquery compiler or executor set")
	}
}

// ============================================================================
// generateExistsMaterialised (with executor) - tests emitCorrelatedExists path
// ============================================================================

func TestGenerateExistsMaterialisedNonCorrelated(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{"row"}}, nil
	})

	expr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
		Not:    false,
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(EXISTS) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateExistsMaterialisedNotExists(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, nil
	})

	expr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{},
		Not:    true,
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(NOT EXISTS) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// codegen_correlated.go - 0% coverage functions
// ============================================================================

func TestExprChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    parser.Expression
		wantLen int
	}{
		{
			"BinaryExpr",
			&parser.BinaryExpr{
				Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Op:    parser.OpPlus,
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			},
			2,
		},
		{
			"UnaryExpr",
			&parser.UnaryExpr{
				Op:   parser.OpNeg,
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			1,
		},
		{
			"ParenExpr",
			&parser.ParenExpr{
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			1,
		},
		{
			"InExpr",
			&parser.InExpr{
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Values: []parser.Expression{
					&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
				},
			},
			2,
		},
		{
			"BetweenExpr",
			&parser.BetweenExpr{
				Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
				Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
			},
			3,
		},
		{
			"LiteralExpr (default)",
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			children := exprChildren(tt.expr)
			if len(children) != tt.wantLen {
				t.Errorf("Expected %d children, got %d", tt.wantLen, len(children))
			}
		})
	}
}

func TestExprChildrenSingle(t *testing.T) {
	t.Parallel()

	cast := &parser.CastExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Type: "INTEGER",
	}
	children := exprChildrenSingle(cast)
	if len(children) != 1 {
		t.Errorf("Expected 1 child for CastExpr, got %d", len(children))
	}

	collate := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "NOCASE",
	}
	children = exprChildrenSingle(collate)
	if len(children) != 1 {
		t.Errorf("Expected 1 child for CollateExpr, got %d", len(children))
	}

	// Default case
	lit := &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	children = exprChildrenSingle(lit)
	if len(children) != 0 {
		t.Errorf("Expected 0 children for LiteralExpr default, got %d", len(children))
	}
}

func TestCaseExprChildren(t *testing.T) {
	t.Parallel()
	ce := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"},
	}
	children := caseExprChildren(ce)
	// Expr + 2*WhenClauses + ElseClause = 4
	if len(children) != 4 {
		t.Errorf("Expected 4 children, got %d", len(children))
	}
}

func TestWalkExprNil(t *testing.T) {
	t.Parallel()
	called := 0
	walkExpr(nil, func(e parser.Expression) { called++ })
	if called != 0 {
		t.Errorf("Expected 0 calls for nil expression, got %d", called)
	}
}

func TestWalkExprVisitsAll(t *testing.T) {
	t.Parallel()
	expr := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpPlus,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	called := 0
	walkExpr(expr, func(e parser.Expression) { called++ })
	// Root + Left + Right = 3
	if called != 3 {
		t.Errorf("Expected 3 calls, got %d", called)
	}
}

func TestBuildRefMap(t *testing.T) {
	t.Parallel()
	refs := []outerRef{
		{Table: "a", Column: "x"},
		{Table: "b", Column: "y"},
	}
	values := []interface{}{int64(1), "hello"}
	m := buildRefMap(refs, values)
	if m["a.x"] != int64(1) {
		t.Errorf("Expected a.x=1, got %v", m["a.x"])
	}
	if m["b.y"] != "hello" {
		t.Errorf("Expected b.y=hello, got %v", m["b.y"])
	}
}

func TestRewriteExprNil(t *testing.T) {
	t.Parallel()
	result := rewriteExpr(nil, map[string]interface{}{})
	if result != nil {
		t.Errorf("Expected nil for nil expression, got %v", result)
	}
}

func TestRewriteIdentNoTable(t *testing.T) {
	t.Parallel()
	ident := &parser.IdentExpr{Name: "col"}
	refMap := map[string]interface{}{"a.col": int64(5)}
	result := rewriteIdent(ident, refMap)
	if result != ident {
		t.Error("Expected unchanged ident for no-table reference")
	}
}

func TestRewriteIdentMatched(t *testing.T) {
	t.Parallel()
	ident := &parser.IdentExpr{Table: "a", Name: "col"}
	refMap := map[string]interface{}{"a.col": int64(42)}
	result := rewriteIdent(ident, refMap)
	lit, ok := result.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("Expected *parser.LiteralExpr, got %T", result)
	}
	if lit.Type != parser.LiteralInteger {
		t.Errorf("Expected LiteralInteger, got %v", lit.Type)
	}
}

func TestRewriteBinaryNoChange(t *testing.T) {
	t.Parallel()
	left := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	right := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}
	bin := &parser.BinaryExpr{Left: left, Op: parser.OpPlus, Right: right}
	refMap := map[string]interface{}{}
	result := rewriteBinary(bin, refMap)
	if result != bin {
		t.Error("Expected same BinaryExpr when nothing changes")
	}
}

func TestRewriteBinaryChanged(t *testing.T) {
	t.Parallel()
	left := &parser.IdentExpr{Table: "a", Name: "x"}
	right := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}
	bin := &parser.BinaryExpr{Left: left, Op: parser.OpPlus, Right: right}
	refMap := map[string]interface{}{"a.x": int64(10)}
	result := rewriteBinary(bin, refMap)
	newBin, ok := result.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("Expected *parser.BinaryExpr, got %T", result)
	}
	if newBin == bin {
		t.Error("Expected new BinaryExpr after rewrite")
	}
}

func TestRewriteUnaryNoChange(t *testing.T) {
	t.Parallel()
	inner := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	unary := &parser.UnaryExpr{Op: parser.OpNeg, Expr: inner}
	refMap := map[string]interface{}{}
	result := rewriteUnary(unary, refMap)
	if result != unary {
		t.Error("Expected same UnaryExpr when nothing changes")
	}
}

func TestRewriteUnaryChanged(t *testing.T) {
	t.Parallel()
	inner := &parser.IdentExpr{Table: "a", Name: "v"}
	unary := &parser.UnaryExpr{Op: parser.OpNeg, Expr: inner}
	refMap := map[string]interface{}{"a.v": int64(7)}
	result := rewriteUnary(unary, refMap)
	newUnary, ok := result.(*parser.UnaryExpr)
	if !ok {
		t.Fatalf("Expected *parser.UnaryExpr, got %T", result)
	}
	if newUnary == unary {
		t.Error("Expected new UnaryExpr after rewrite")
	}
}

func TestRewriteParenNoChange(t *testing.T) {
	t.Parallel()
	inner := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	paren := &parser.ParenExpr{Expr: inner}
	refMap := map[string]interface{}{}
	result := rewriteParen(paren, refMap)
	if result != paren {
		t.Error("Expected same ParenExpr when nothing changes")
	}
}

func TestRewriteParenChanged(t *testing.T) {
	t.Parallel()
	inner := &parser.IdentExpr{Table: "a", Name: "p"}
	paren := &parser.ParenExpr{Expr: inner}
	refMap := map[string]interface{}{"a.p": "str"}
	result := rewriteParen(paren, refMap)
	newParen, ok := result.(*parser.ParenExpr)
	if !ok {
		t.Fatalf("Expected *parser.ParenExpr, got %T", result)
	}
	if newParen == paren {
		t.Error("Expected new ParenExpr after rewrite")
	}
}

func TestRewriteExprDefault(t *testing.T) {
	t.Parallel()
	// FunctionExpr is not handled by rewriteExpr switch, returns as-is
	fn := &parser.FunctionExpr{Name: "abs"}
	refMap := map[string]interface{}{}
	result := rewriteExpr(fn, refMap)
	if result != fn {
		t.Error("Expected unchanged FunctionExpr for default case")
	}
}

func TestValueToLiteralExpr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   interface{}
		litType parser.LiteralType
	}{
		{"nil", nil, parser.LiteralNull},
		{"int64", int64(5), parser.LiteralInteger},
		{"float64", float64(1.5), parser.LiteralFloat},
		{"string", "hello", parser.LiteralString},
		{"default", []byte("x"), parser.LiteralNull},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lit := valueToLiteralExpr(tt.input)
			if lit.Type != tt.litType {
				t.Errorf("Expected type %v, got %v", tt.litType, lit.Type)
			}
		})
	}
}

func TestCollectSubqueryTablesNilFrom(t *testing.T) {
	t.Parallel()
	stmt := &parser.SelectStmt{From: nil}
	tables := collectSubqueryTables(stmt)
	if len(tables) != 0 {
		t.Errorf("Expected empty map for nil From, got %v", tables)
	}
}

func TestCollectSubqueryTablesWithAlias(t *testing.T) {
	t.Parallel()
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users", Alias: "u"},
			},
		},
	}
	tables := collectSubqueryTables(stmt)
	if !tables["u"] {
		t.Error("Expected alias 'u' in tables")
	}
	if !tables["users"] {
		t.Error("Expected table name 'users' in tables")
	}
}

func TestRewriteOuterRefs(t *testing.T) {
	t.Parallel()
	refs := []outerRef{{Table: "a", Column: "id"}}
	values := []interface{}{int64(99)}

	inner := &parser.IdentExpr{Table: "a", Name: "id"}
	where := &parser.BinaryExpr{
		Left:  inner,
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
	}
	stmt := &parser.SelectStmt{Where: where}
	result := rewriteOuterRefs(stmt, refs, values)
	if result == stmt {
		t.Error("Expected new SelectStmt after rewrite")
	}
	if result.Where == where {
		t.Error("Expected Where to be rewritten")
	}
}

func TestFindOuterRefsNoMatch(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("outer_t", 0)

	// Where has no IdentExpr with table qualifier in cursorMap
	stmt := &parser.SelectStmt{
		Where: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	refs := gen.findOuterRefs(stmt)
	if len(refs) != 0 {
		t.Errorf("Expected 0 outer refs, got %d", len(refs))
	}
}

func TestInExprChildren(t *testing.T) {
	t.Parallel()
	expr := &parser.IdentExpr{Name: "x"}
	v1 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	v2 := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}
	in := &parser.InExpr{Expr: expr, Values: []parser.Expression{v1, v2}}
	children := inExprChildren(in)
	if len(children) != 3 {
		t.Errorf("Expected 3 children (expr + 2 values), got %d", len(children))
	}
}

// ============================================================================
// generateIntegerLiteral error paths - 60% coverage
// ============================================================================

func TestGenerateIntegerLiteralLarge(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Value outside int32 range → OpInt64
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "9999999999"}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for large int failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateIntegerLiteralHex(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0xFF"}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr for hex int failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateIntegerLiteralInvalidString(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// Not an integer and not a float -> error
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "not_a_number"}
	_, err := gen.GenerateExpr(expr)
	if err == nil {
		t.Error("Expected error for invalid integer literal")
	}
}

// ============================================================================
// generateInSubqueryMaterialised - 0% coverage
// ============================================================================

func TestGenerateInSubqueryMaterialised(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(1)}, {int64(2)}}, nil
	})

	expr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(IN subquery materialised) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateInSubqueryMaterialisedEmpty(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, nil
	})

	expr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(IN subquery materialised empty) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// generateSubqueryBytecodeEmbedding - 0% coverage
// ============================================================================

func TestGenerateSubqueryBytecodeEmbedding(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		sub := vdbe.New()
		sub.AddOp(vdbe.OpInteger, 42, 1, 0)
		sub.AddOp(vdbe.OpResultRow, 1, 1, 0)
		sub.AddOp(vdbe.OpHalt, 0, 0, 0)
		return sub, nil
	})

	expr := &parser.SubqueryExpr{Select: &parser.SelectStmt{}}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(scalar subquery bytecode embedding) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// generateExistsBytecodeEmbedding / emitExistsHalt - 0% coverage
// ============================================================================

func TestGenerateExistsBytecodeEmbedding(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		sub := vdbe.New()
		sub.AddOp(vdbe.OpInteger, 1, 1, 0)
		sub.AddOp(vdbe.OpResultRow, 1, 1, 0)
		sub.AddOp(vdbe.OpHalt, 0, 0, 0)
		return sub, nil
	})

	expr := &parser.ExistsExpr{Select: &parser.SelectStmt{}}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(EXISTS bytecode embedding) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// emitCorrelatedExists / emitCorrelatedScalar - 0% coverage
// These are called when findOuterRefs returns matches
// ============================================================================

func TestGenerateExistsMaterialisedCorrelated(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("outer_t", 1)
	gen.RegisterTable(TableInfo{
		Name: "outer_t",
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
		},
	})
	v.AllocCursors(2)
	v.AllocMemory(10)

	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{"row"}}, nil
	})
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		return vdbe.New(), nil
	})

	// Build EXISTS with a WHERE referencing outer table
	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "outer_t", Name: "id"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	existsExpr := &parser.ExistsExpr{
		Select: &parser.SelectStmt{
			Where: where,
		},
	}
	reg, err := gen.GenerateExpr(existsExpr)
	if err != nil {
		t.Fatalf("GenerateExpr(correlated EXISTS) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

func TestGenerateSubqueryMaterialisedCorrelated(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("outer_t", 1)
	gen.RegisterTable(TableInfo{
		Name: "outer_t",
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
		},
	})
	v.AllocCursors(2)
	v.AllocMemory(10)

	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(42)}}, nil
	})
	gen.SetSubqueryCompiler(func(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		return vdbe.New(), nil
	})

	where := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "outer_t", Name: "id"},
		Op:    parser.OpEq,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	subqExpr := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{
			Where: where,
		},
	}
	reg, err := gen.GenerateExpr(subqExpr)
	if err != nil {
		t.Fatalf("GenerateExpr(correlated scalar subquery) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// adjustInstructionJumps with dual-adjust rule (adjustP2+adjustP3)
// ============================================================================

func TestAdjustInstructionJumpsDualAdjust(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	addrMap := map[int]int{3: 7, 5: 9}
	// OpInitCoroutine has {adjustP2: true, adjustP3: true}
	instr := &vdbe.Instruction{Opcode: vdbe.OpInitCoroutine, P2: 3, P3: 5}
	gen.adjustInstructionJumps(instr, addrMap)
	if instr.P2 != 7 {
		t.Errorf("Expected P2=7, got %d", instr.P2)
	}
	if instr.P3 != 9 {
		t.Errorf("Expected P3=9, got %d", instr.P3)
	}
}

// ============================================================================
// findTableWithColumn coverage
// ============================================================================

func TestFindTableWithColumnFound(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterTable(TableInfo{
		Name: "t1",
		Columns: []ColumnInfo{
			{Name: "age", Index: 1},
		},
	})
	gen.RegisterTable(TableInfo{
		Name: "t2",
		Columns: []ColumnInfo{
			{Name: "name", Index: 0},
		},
	})
	gen.RegisterCursor("t1", 0)
	gen.RegisterCursor("t2", 1)

	// Generate expression with ambiguous column (no table qualifier)
	// This exercises findTableWithColumn
	expr := &parser.IdentExpr{Name: "age"}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// generateCase with else clause (exercising generateElseClause else branch)
// ============================================================================

func TestGenerateCaseWithElse(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.CaseExpr{
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "no"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "else"},
	}
	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr(CASE with ELSE) failed: %v", err)
	}
	if reg == 0 {
		t.Error("Expected non-zero register")
	}
}

// ============================================================================
// buildExistsCallback / buildScalarCallback invocation (indirect via correlated)
// ============================================================================

func TestBuildExistsCallbackInvoked(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	refs := []outerRef{{Table: "t", Column: "id"}}
	called := false
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		called = true
		return [][]interface{}{{"row"}}, nil
	})

	cb := gen.buildExistsCallback(&parser.SelectStmt{}, refs)
	result, err := cb([]interface{}{int64(1)})
	if err != nil {
		t.Fatalf("callback returned error: %v", err)
	}
	if !called {
		t.Error("Expected subquery executor to be called")
	}
	if !result {
		t.Error("Expected true (rows returned)")
	}
}

func TestBuildScalarCallbackInvoked(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	refs := []outerRef{{Table: "t", Column: "id"}}
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(99)}}, nil
	})

	cb := gen.buildScalarCallback(&parser.SelectStmt{}, refs)
	result, err := cb([]interface{}{int64(1)})
	if err != nil {
		t.Fatalf("callback returned error: %v", err)
	}
	if result != int64(99) {
		t.Errorf("Expected 99, got %v", result)
	}
}

func TestBuildScalarCallbackEmpty(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	refs := []outerRef{{Table: "t", Column: "id"}}
	gen.SetSubqueryExecutor(func(stmt *parser.SelectStmt) ([][]interface{}, error) {
		return nil, nil
	})

	cb := gen.buildScalarCallback(&parser.SelectStmt{}, refs)
	result, err := cb([]interface{}{int64(1)})
	if err != nil {
		t.Fatalf("callback returned error: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil for empty rows, got %v", result)
	}
}
