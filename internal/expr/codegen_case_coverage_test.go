// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr_test

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// intLit returns a valid integer literal expression.
func intLit(v string) *parser.LiteralExpr {
	return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: v}
}

// strLit returns a valid string literal expression.
func strLit(v string) *parser.LiteralExpr {
	return &parser.LiteralExpr{Type: parser.LiteralString, Value: v}
}

// badLit returns a literal with an unsupported type, causing GenerateExpr to fail.
func badLit() *parser.LiteralExpr {
	return &parser.LiteralExpr{Type: parser.LiteralType(999)}
}

// newGen creates a fresh CodeGenerator backed by a new VDBE.
func newGen() *expr.CodeGenerator {
	return expr.NewCodeGenerator(vdbe.New())
}

// ---------------------------------------------------------------------------
// generateCase – searched CASE (Expr == nil) with ELSE
// ---------------------------------------------------------------------------

// TestCodegenCase_SearchedWithElse verifies that a searched CASE expression with
// an explicit ELSE clause generates code without error and returns a register.
func TestCodegenCase_SearchedWithElse(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil, // searched CASE
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: strLit("one")},
		},
		ElseClause: strLit("other"),
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (searched CASE + ELSE): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// generateCase – searched CASE (Expr == nil) without ELSE (emits NULL)
// ---------------------------------------------------------------------------

// TestCodegenCase_SearchedNoElse verifies that a searched CASE expression without
// an ELSE clause generates code without error (the ELSE branch emits NULL).
func TestCodegenCase_SearchedNoElse(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("0"), Result: intLit("42")},
		},
		ElseClause: nil,
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (searched CASE, no ELSE): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// generateCase – simple CASE (Expr != nil) with ELSE
// ---------------------------------------------------------------------------

// TestCodegenCase_SimpleWithElse verifies that a simple CASE expression
// (CASE x WHEN val THEN result ELSE fallback END) generates code without error.
func TestCodegenCase_SimpleWithElse(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: intLit("1"), // simple CASE: CASE 1 WHEN 1 THEN 'one' ELSE 'other' END
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: strLit("one")},
		},
		ElseClause: strLit("other"),
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (simple CASE + ELSE): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// generateCase – simple CASE (Expr != nil) without ELSE (emits NULL)
// ---------------------------------------------------------------------------

// TestCodegenCase_SimpleNoElse verifies that a simple CASE expression without
// an ELSE clause generates a NULL for the else branch.
func TestCodegenCase_SimpleNoElse(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: intLit("2"),
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: strLit("one")},
			{Condition: intLit("2"), Result: strLit("two")},
		},
		ElseClause: nil,
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (simple CASE, no ELSE): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// generateCase – multiple WHEN clauses (searched)
// ---------------------------------------------------------------------------

// TestCodegenCase_MultipleWhens verifies that multiple WHEN clauses are all
// processed without error.
func TestCodegenCase_MultipleWhens(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("0"), Result: strLit("zero")},
			{Condition: intLit("1"), Result: strLit("one")},
			{Condition: intLit("1"), Result: strLit("also-one")},
		},
		ElseClause: strLit("many"),
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (multiple WHENs): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// generateCase – error: bad CASE expression (evaluateCaseExpr fails)
// ---------------------------------------------------------------------------

// TestCodegenCase_BadCaseExpr verifies that generateCase propagates an error
// when the CASE expression itself (the simple-CASE subject) is unsupported.
func TestCodegenCase_BadCaseExpr(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: badLit(), // unsupported literal type → GenerateExpr error
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: strLit("one")},
		},
	}

	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error when CASE subject expression is unsupported")
	}
}

// ---------------------------------------------------------------------------
// generateCase – error: bad WHEN condition (generateWhenCondition fails)
// ---------------------------------------------------------------------------

// TestCodegenCase_BadWhenCondition verifies that generateCase propagates an
// error when a WHEN condition expression is unsupported (searched CASE).
func TestCodegenCase_BadWhenCondition(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{Condition: badLit(), Result: strLit("ok")},
		},
	}

	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error when WHEN condition is unsupported (searched CASE)")
	}
}

// ---------------------------------------------------------------------------
// generateCase – error: bad WHEN condition in simple CASE
// ---------------------------------------------------------------------------

// TestCodegenCase_SimpleCase_BadWhenCondition verifies that generateCase
// propagates an error when a WHEN value is unsupported in simple CASE.
func TestCodegenCase_SimpleCase_BadWhenCondition(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: intLit("1"),
		WhenClauses: []parser.WhenClause{
			{Condition: badLit(), Result: strLit("ok")},
		},
	}

	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error when WHEN value is unsupported (simple CASE)")
	}
}

// ---------------------------------------------------------------------------
// generateCase – error: bad THEN result
// ---------------------------------------------------------------------------

// TestCodegenCase_BadThenResult verifies that generateCase propagates an error
// when a THEN result expression is unsupported.
func TestCodegenCase_BadThenResult(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: badLit()},
		},
	}

	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error when THEN result expression is unsupported")
	}
}

// ---------------------------------------------------------------------------
// generateCase – error: bad ELSE clause (generateElseClause fails)
// ---------------------------------------------------------------------------

// TestCodegenCase_BadElseClause verifies that generateCase propagates an error
// when the ELSE expression is unsupported.
func TestCodegenCase_BadElseClause(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr: nil,
		WhenClauses: []parser.WhenClause{
			{Condition: intLit("1"), Result: strLit("one")},
		},
		ElseClause: badLit(),
	}

	_, err := gen.GenerateExpr(e)
	if err == nil {
		t.Error("expected error when ELSE expression is unsupported")
	}
}

// ---------------------------------------------------------------------------
// generateCase – zero WHEN clauses
// ---------------------------------------------------------------------------

// TestCodegenCase_NoWhenClauses verifies that a CASE with no WHEN clauses
// (unusual but structurally valid) generates code without error.
func TestCodegenCase_NoWhenClauses(t *testing.T) {
	gen := newGen()

	e := &parser.CaseExpr{
		Expr:        nil,
		WhenClauses: nil,
		ElseClause:  strLit("always"),
	}

	reg, err := gen.GenerateExpr(e)
	if err != nil {
		t.Fatalf("GenerateExpr (no WHEN clauses): %v", err)
	}
	if reg <= 0 {
		t.Errorf("expected positive register, got %d", reg)
	}
}
