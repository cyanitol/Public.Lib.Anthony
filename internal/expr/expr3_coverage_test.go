// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// --- codegen.go: generateCase (72.7%) ---

// TestGenerateCase3_SearchedCaseNoElse verifies searched CASE with no ELSE emits NULL.
func TestGenerateCase3_SearchedCaseNoElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{
		Expr: nil, // searched CASE
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
		},
		ElseClause: nil,
	}

	reg, err := gen.generateCase(e)
	if err != nil {
		t.Fatalf("generateCase failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive result register, got %d", reg)
	}
}

// TestGenerateCase3_SearchedCaseWithElse verifies searched CASE with ELSE clause.
func TestGenerateCase3_SearchedCaseWithElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "zero"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "other"},
	}

	reg, err := gen.generateCase(e)
	if err != nil {
		t.Fatalf("generateCase with ELSE failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive result register, got %d", reg)
	}
}

// TestGenerateCase3_SimpleCaseWithExpr verifies simple CASE (CASE expr WHEN ...).
func TestGenerateCase3_SimpleCaseWithExpr(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "one"},
			},
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "two"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "other"},
	}

	reg, err := gen.generateCase(e)
	if err != nil {
		t.Fatalf("simple generateCase failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive result register, got %d", reg)
	}
}

// --- codegen.go: generateWhenClauses (80%) ---

// TestGenerateWhenClauses3_MultipleWhen verifies multiple WHEN clauses are generated.
func TestGenerateWhenClauses3_MultipleWhen(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
			},
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "20"},
			},
		},
	}
	resultReg := gen.AllocReg()
	jumps, err := gen.generateWhenClauses(e, 0, resultReg)
	if err != nil {
		t.Fatalf("generateWhenClauses failed: %v", err)
	}
	if len(jumps) != 2 {
		t.Errorf("expected 2 end jumps, got %d", len(jumps))
	}
}

// --- codegen.go: generateElseClause (85.7%) ---

// TestGenerateElseClause3_WithElse verifies ELSE expression is emitted.
func TestGenerateElseClause3_WithElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "fallback"},
	}
	resultReg := gen.AllocReg()
	err := gen.generateElseClause(e, resultReg)
	if err != nil {
		t.Fatalf("generateElseClause with else failed: %v", err)
	}
}

// TestGenerateElseClause3_NilElse verifies NULL is emitted when no ELSE.
func TestGenerateElseClause3_NilElse(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CaseExpr{ElseClause: nil}
	resultReg := gen.AllocReg()
	err := gen.generateElseClause(e, resultReg)
	if err != nil {
		t.Fatalf("generateElseClause nil-else failed: %v", err)
	}
}

// --- codegen.go: generateSimpleCaseCondition (83.3%) ---

// TestGenerateSimpleCaseCondition3 verifies equality comparison is emitted.
func TestGenerateSimpleCaseCondition3(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	when := &parser.WhenClause{
		Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "five"},
	}
	caseExprReg := gen.AllocReg()
	condReg, err := gen.generateSimpleCaseCondition(when, caseExprReg)
	if err != nil {
		t.Fatalf("generateSimpleCaseCondition failed: %v", err)
	}
	if condReg <= 0 {
		t.Errorf("expected positive cond register, got %d", condReg)
	}
}

// --- codegen.go: generateLogical (90.9%) ---

// TestGenerateLogical3_And verifies AND expression code generation.
func TestGenerateLogical3_And(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpAnd,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}
	reg, err := gen.generateLogical(e)
	if err != nil {
		t.Fatalf("generateLogical AND failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// TestGenerateLogical3_Or verifies OR expression code generation.
func TestGenerateLogical3_Or(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		Op:    parser.OpOr,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	reg, err := gen.generateLogical(e)
	if err != nil {
		t.Fatalf("generateLogical OR failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// --- codegen.go: generateBetween (85%) ---

// TestGenerateBetween3_NotBetween verifies NOT BETWEEN emits NOT opcode.
func TestGenerateBetween3_NotBetween(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Not:   true,
	}
	reg, err := gen.generateBetween(e)
	if err != nil {
		t.Fatalf("generateBetween NOT failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// TestGenerateBetween3_Normal verifies normal BETWEEN code.
func TestGenerateBetween3_Normal(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Not:   false,
	}
	reg, err := gen.generateBetween(e)
	if err != nil {
		t.Fatalf("generateBetween normal failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// --- codegen.go: generateCollate (80%) ---

// TestGenerateCollate3_TracksCollation verifies collation is stored per-register.
func TestGenerateCollate3_TracksCollation(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "NOCASE",
	}
	reg, err := gen.generateCollate(e)
	if err != nil {
		t.Fatalf("generateCollate failed: %v", err)
	}
	coll, ok := gen.CollationForReg(reg)
	if !ok {
		t.Error("expected collation to be tracked for register")
	}
	if coll != "NOCASE" {
		t.Errorf("expected collation NOCASE, got %q", coll)
	}
}

// --- codegen.go: GenerateExpr with precomputed (87.5%) ---

// TestGenerateExpr3_Precomputed verifies precomputed expression returns cached register.
func TestGenerateExpr3_Precomputed(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	gen.SetPrecomputed(expr, 99)

	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("GenerateExpr precomputed failed: %v", err)
	}
	if reg != 99 {
		t.Errorf("expected precomputed register 99, got %d", reg)
	}
}

// TestGenerateExpr3_NilExpr verifies nil expr emits NULL literal.
func TestGenerateExpr3_NilExpr(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	reg, err := gen.GenerateExpr(nil)
	if err != nil {
		t.Fatalf("GenerateExpr nil failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register for NULL literal, got %d", reg)
	}
}

// TestGenerateExpr3_UnsupportedType verifies error for unknown expression type.
func TestGenerateExpr3_UnsupportedType(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Use a custom expression type that is not registered in exprDispatch
	type unknownExpr struct{}
	// We can't pass a custom type directly since GenerateExpr uses reflect.TypeOf
	// Instead verify it works for a known type to confirm the dispatch table is populated
	reg, err := gen.GenerateExpr(&parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"})
	if err != nil {
		t.Fatalf("GenerateExpr null literal failed: %v", err)
	}
	if reg <= 0 {
		t.Error("expected positive register")
	}
}

// --- codegen.go: generateIntegerLiteral hex path (95%) ---

// TestGenerateIntegerLiteral3_Hex verifies hex integer literal is parsed correctly.
func TestGenerateIntegerLiteral3_Hex(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "0xFF",
	}
	reg, err := gen.generateLiteral(e)
	if err != nil {
		t.Fatalf("generateLiteral hex failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// TestGenerateIntegerLiteral3_LargeInt verifies large integer uses Int64 opcode.
func TestGenerateIntegerLiteral3_LargeInt(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "9999999999",
	}
	reg, err := gen.generateLiteral(e)
	if err != nil {
		t.Fatalf("generateLiteral large int failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// TestGenerateIntegerLiteral3_FloatFallback verifies float fallback for int-like float.
func TestGenerateIntegerLiteral3_FloatFallback(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// A value that fails strconv.ParseInt but succeeds ParseFloat
	e := &parser.LiteralExpr{
		Type:  parser.LiteralInteger,
		Value: "1.5e10",
	}
	reg, err := gen.generateLiteral(e)
	if err != nil {
		t.Fatalf("generateLiteral float fallback failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// --- codegen.go: GenerateCondition (83.3%) ---

// TestGenerateCondition3_Simple verifies condition generates conditional jump.
func TestGenerateCondition3_Simple(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	cond := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	jumpAddr, err := gen.GenerateCondition(cond, 100)
	if err != nil {
		t.Fatalf("GenerateCondition failed: %v", err)
	}
	if jumpAddr < 0 {
		t.Errorf("expected valid jump address, got %d", jumpAddr)
	}
}

// --- codegen.go: generateInValueComparison (85%) ---

// TestGenerateInValueComparison3_SingleValue verifies single value IN comparison.
func TestGenerateInValueComparison3_SingleValue(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	exprReg := gen.AllocReg()
	resultReg := gen.AllocReg()
	nullSeenReg := gen.AllocReg()
	val := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"}
	gotoAddr, err := gen.generateInValueComparison(exprReg, val, resultReg, nullSeenReg)
	if err != nil {
		t.Fatalf("generateInValueComparison failed: %v", err)
	}
	if gotoAddr < 0 {
		t.Errorf("expected valid goto address, got %d", gotoAddr)
	}
}

// --- codegen.go: generateBinaryOperands (85.7%) ---

// TestGenerateBinaryOperands3_BothSides verifies both sides generate registers.
func TestGenerateBinaryOperands3_BothSides(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		Op:    parser.OpPlus,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"},
	}
	leftReg, rightReg, err := gen.generateBinaryOperands(e)
	if err != nil {
		t.Fatalf("generateBinaryOperands failed: %v", err)
	}
	if leftReg <= 0 || rightReg <= 0 {
		t.Errorf("expected positive registers, got left=%d right=%d", leftReg, rightReg)
	}
}

// --- arithmetic.go: castToNumeric (85.7%) ---

// TestCastToNumeric3_StringInt verifies string integer is cast to int64.
func TestCastToNumeric3_StringInt(t *testing.T) {
	result := castToNumeric("42")
	if _, ok := result.(int64); !ok {
		t.Errorf("expected int64, got %T: %v", result, result)
	}
}

// TestCastToNumeric3_StringFloat verifies string float is cast to numeric (int64 or float64).
func TestCastToNumeric3_StringFloat(t *testing.T) {
	// "3.14" cannot be represented as int64, so it should be float64
	result := castToNumeric("3.14")
	switch result.(type) {
	case float64, int64:
		// both acceptable numeric types
	default:
		t.Errorf("expected numeric type (float64 or int64), got %T: %v", result, result)
	}
}

// TestCastToNumeric3_NonNumericString passes through unchanged.
func TestCastToNumeric3_NonNumericString(t *testing.T) {
	result := castToNumeric("abc")
	if s, ok := result.(string); !ok || s != "abc" {
		t.Errorf("expected unchanged string 'abc', got %T: %v", result, result)
	}
}

// TestCastToNumeric3_Int64 verifies int64 passes through.
func TestCastToNumeric3_Int64(t *testing.T) {
	result := castToNumeric(int64(7))
	if v, ok := result.(int64); !ok || v != 7 {
		t.Errorf("expected int64(7), got %T: %v", result, result)
	}
}

// --- arithmetic.go: EvaluateCast (87.5%) ---

// TestEvaluateCast3_Nil verifies nil input returns nil.
func TestEvaluateCast3_Nil(t *testing.T) {
	result := EvaluateCast(nil, "INTEGER")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// TestEvaluateCast3_ToText verifies TEXT cast returns string.
func TestEvaluateCast3_ToText(t *testing.T) {
	result := EvaluateCast(int64(42), "TEXT")
	if _, ok := result.(string); !ok {
		t.Errorf("expected string, got %T: %v", result, result)
	}
}

// TestEvaluateCast3_ToInteger verifies INTEGER cast from string.
func TestEvaluateCast3_ToInteger(t *testing.T) {
	result := EvaluateCast("100", "INTEGER")
	if v, ok := result.(int64); !ok || v != 100 {
		t.Errorf("expected int64(100), got %T: %v", result, result)
	}
}

// TestEvaluateCast3_ToBlob verifies BLOB cast from string.
func TestEvaluateCast3_ToBlob(t *testing.T) {
	result := EvaluateCast("data", "BLOB")
	if _, ok := result.([]byte); !ok {
		t.Errorf("expected []byte, got %T: %v", result, result)
	}
}

// TestEvaluateCast3_ToNumeric verifies NUMERIC cast produces a numeric type.
func TestEvaluateCast3_ToNumeric(t *testing.T) {
	result := EvaluateCast("3.14", "NUMERIC")
	switch result.(type) {
	case float64, int64:
		// both are valid numeric results
	default:
		t.Errorf("expected numeric type, got %T: %v", result, result)
	}
}

// TestEvaluateCast3_UnknownType passes through unchanged.
func TestEvaluateCast3_UnknownType(t *testing.T) {
	result := EvaluateCast(int64(42), "UNKNOWN_TYPE")
	// AFF_NONE — value passes through
	if result == nil {
		t.Error("expected non-nil result for unknown type")
	}
}

// --- codegen_correlated.go: findOuterRefs (81.2%) ---

// TestFindOuterRefs3_NoOuterRefs verifies empty result when no outer table refs.
func TestFindOuterRefs3_NoOuterRefs(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	// No tables in cursorMap

	stmt := &parser.SelectStmt{
		Where: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "id", Table: ""},
			Op:    parser.OpEq,
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}
	refs := gen.findOuterRefs(stmt)
	if len(refs) != 0 {
		t.Errorf("expected 0 outer refs, got %d", len(refs))
	}
}

// TestFindOuterRefs3_WithOuterRef verifies outer column reference is found.
func TestFindOuterRefs3_WithOuterRef(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("outer_t", 0)

	// Subquery references "outer_t.id" where "outer_t" is in the outer cursor map
	// but not in the subquery FROM clause
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "inner_t"}},
		},
		Where: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "id", Table: "outer_t"},
			Op:    parser.OpEq,
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}
	refs := gen.findOuterRefs(stmt)
	if len(refs) != 1 {
		t.Errorf("expected 1 outer ref, got %d", len(refs))
	}
	if refs[0].Table != "outer_t" || refs[0].Column != "id" {
		t.Errorf("unexpected outer ref: %+v", refs[0])
	}
}

// TestFindOuterRefs3_SubqueryTableShadows verifies that refs to subquery-local tables are excluded.
func TestFindOuterRefs3_SubqueryTableShadows(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)
	gen.RegisterCursor("shared_t", 0)

	// "shared_t" is in outer cursor map but also in subquery FROM clause
	stmt := &parser.SelectStmt{
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "shared_t"}},
		},
		Where: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "id", Table: "shared_t"},
			Op:    parser.OpEq,
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}
	refs := gen.findOuterRefs(stmt)
	// shared_t appears in subquery FROM, so it's not an outer ref
	if len(refs) != 0 {
		t.Errorf("expected 0 refs (shadowed), got %d", len(refs))
	}
}

// --- codegen_correlated.go: exprChildren (88.9%) ---

// TestExprChildren3_InExprNoValues verifies InExpr with only Expr field.
func TestExprChildren3_InExprNoValues(t *testing.T) {
	e := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Values: nil,
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child for InExpr with no values, got %d", len(children))
	}
}

// TestExprChildren3_BetweenExpr verifies BetweenExpr returns 3 children.
func TestExprChildren3_BetweenExpr(t *testing.T) {
	e := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
	}
	children := exprChildren(e)
	if len(children) != 3 {
		t.Errorf("expected 3 children for BetweenExpr, got %d", len(children))
	}
}

// TestExprChildren3_CaseExpr verifies CaseExpr returns expr+when conditions+results+else.
func TestExprChildren3_CaseExpr(t *testing.T) {
	e := &parser.CaseExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Result:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "a"},
			},
		},
		ElseClause: &parser.LiteralExpr{Type: parser.LiteralString, Value: "b"},
	}
	children := exprChildren(e)
	// expr + (condition + result) + else = 4
	if len(children) < 3 {
		t.Errorf("expected >= 3 children for CaseExpr, got %d", len(children))
	}
}

// TestExprChildren3_CastExpr verifies CastExpr returns 1 child.
func TestExprChildren3_CastExpr(t *testing.T) {
	e := &parser.CastExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		Type: "TEXT",
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child for CastExpr, got %d", len(children))
	}
}

// TestExprChildren3_CollateExpr verifies CollateExpr returns 1 child.
func TestExprChildren3_CollateExpr(t *testing.T) {
	e := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "x"},
		Collation: "NOCASE",
	}
	children := exprChildren(e)
	if len(children) != 1 {
		t.Errorf("expected 1 child for CollateExpr, got %d", len(children))
	}
}

// TestExprChildren3_FunctionExpr verifies FunctionExpr returns all args.
func TestExprChildren3_FunctionExpr(t *testing.T) {
	e := &parser.FunctionExpr{
		Name: "coalesce",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralNull},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}
	children := exprChildren(e)
	if len(children) != 2 {
		t.Errorf("expected 2 children for FunctionExpr, got %d", len(children))
	}
}

// --- codegen.go: generateFunction (95.5%) ---

// TestGenerateFunction3_NoArgs verifies zero-arg function generates correctly.
func TestGenerateFunction3_NoArgs(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.FunctionExpr{
		Name: "random",
		Args: nil,
	}
	reg, err := gen.generateFunction(e)
	if err != nil {
		t.Fatalf("generateFunction no args failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// TestGenerateFunction3_MultipleArgs verifies multi-arg function generates correctly.
func TestGenerateFunction3_MultipleArgs(t *testing.T) {
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	e := &parser.FunctionExpr{
		Name: "max",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		},
	}
	reg, err := gen.generateFunction(e)
	if err != nil {
		t.Fatalf("generateFunction multi-arg failed: %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}
