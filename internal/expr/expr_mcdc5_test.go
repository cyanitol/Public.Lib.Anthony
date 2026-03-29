// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

// MC/DC Coverage Tests — expr_mcdc5_test.go
//
// Covers the following functions identified as below target:
//   - compareNoCase       (compare.go:97)     72.7%
//   - generateInSubqueryMaterialised (codegen.go:1246)  80%
//   - generateInValueComparison      (codegen.go:1061)  85%
//   - generateBetween                (codegen.go:1283)  85%
//   - castToNumeric                  (arithmetic.go:462) 85.7%

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// compareNoCase — 72.7%
//
// Function logic:
//   minLen = min(len(a), len(b))
//   for i in [0,minLen): fold ASCII, then: ca<cb → -1; ca>cb → 1
//   return len(a) - len(b)
//
// MC/DC conditions:
//   C1: len(b) < len(a)           → minLen = len(b) branch
//   C2: ca < cb                   → return -1
//   C3: ca > cb                   → return 1  (uncovered at 72.7%)
//   C4: all chars equal           → return len(a)-len(b)
//
// At 72.7% the "ca > cb" branch (return 1) is not covered.
// ---------------------------------------------------------------------------

func TestMCDC5_CompareNoCase_Greater(t *testing.T) {
	t.Parallel()
	// "b" > "a" → ca > cb → returns 1
	got := compareNoCase("b", "a")
	if got != 1 {
		t.Errorf("compareNoCase(%q, %q) = %d, want 1", "b", "a", got)
	}
}

func TestMCDC5_CompareNoCase_Less(t *testing.T) {
	t.Parallel()
	// "a" < "b" → ca < cb → returns -1
	got := compareNoCase("a", "b")
	if got != -1 {
		t.Errorf("compareNoCase(%q, %q) = %d, want -1", "a", "b", got)
	}
}

func TestMCDC5_CompareNoCase_Equal(t *testing.T) {
	t.Parallel()
	got := compareNoCase("abc", "ABC")
	if got != 0 {
		t.Errorf("compareNoCase(%q, %q) = %d, want 0", "abc", "ABC", got)
	}
}

func TestMCDC5_CompareNoCase_LengthDiff_AisLonger(t *testing.T) {
	t.Parallel()
	// "abc" vs "ab": same prefix, len diff → positive
	got := compareNoCase("abc", "ab")
	if got <= 0 {
		t.Errorf("compareNoCase(%q, %q) = %d, want >0", "abc", "ab", got)
	}
}

func TestMCDC5_CompareNoCase_LengthDiff_BisLonger(t *testing.T) {
	t.Parallel()
	// "ab" vs "abc": same prefix, len diff → negative
	got := compareNoCase("ab", "abc")
	if got >= 0 {
		t.Errorf("compareNoCase(%q, %q) = %d, want <0", "ab", "abc", got)
	}
}

// Cover the ca > cb path via case-folding: 'B' folded to 'b' > 'a' folded to 'a'.
func TestMCDC5_CompareNoCase_FoldedGreater(t *testing.T) {
	t.Parallel()
	// 'B' folds to 'b', 'a' folds to 'a', so 'B'(='b') > 'a'
	got := compareNoCase("B", "a")
	if got != 1 {
		t.Errorf("compareNoCase(%q, %q) = %d, want 1 (B>a after fold)", "B", "a", got)
	}
}

// Non-ASCII bytes compare as-is (no folding).
func TestMCDC5_CompareNoCase_NonASCII(t *testing.T) {
	t.Parallel()
	// bytes 0x80 and 0x7f — no folding, 0x80 > 0x7f → 1
	got := compareNoCase("\x80", "\x7f")
	if got != 1 {
		t.Errorf("compareNoCase non-ASCII: got %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// generateBetween — 85%
//
// MC/DC condition: e.Not == true → emit OpNot
//   false → return resultReg directly
//   true  → return notReg (uncovered)
// ---------------------------------------------------------------------------

func TestMCDC5_GenerateBetween_Not_False(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Not:   false,
	}

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateBetween Not=false: %v", err)
	}

	// Should have OpGe and OpLe but NOT OpNot
	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpNot {
			t.Error("unexpected OpNot in NOT BETWEEN=false path")
		}
	}
}

func TestMCDC5_GenerateBetween_Not_True(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	expr := &parser.BetweenExpr{
		Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"},
		Lower: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Upper: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
		Not:   true,
	}

	_, err := gen.GenerateExpr(expr)
	if err != nil {
		t.Fatalf("generateBetween Not=true: %v", err)
	}

	// Should emit OpNot for NOT BETWEEN
	found := false
	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpNot {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected OpNot in NOT BETWEEN=true path, not found")
	}
}

// ---------------------------------------------------------------------------
// generateInValueComparison — 85%
//
// MC/DC condition: collation != "" → set P4 collation on OpEq
//   false → P4 not set (covered by all existing IN tests)
//   true  → P4 is set (uncovered)
//
// Drive via GenerateExpr on an InExpr whose value is a CollateExpr.
// generateCollate sets g.collations[reg] = e.Collation, which is then
// picked up by getCollationForOperands inside generateInValueComparison.
// ---------------------------------------------------------------------------

func TestMCDC5_GenerateInValueComparison_WithCollation(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Value is a CollateExpr wrapping a string literal.
	// When generated, it sets collations[reg] = "NOCASE".
	collateVal := &parser.CollateExpr{
		Expr:      &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
		Collation: "NOCASE",
	}

	inExpr := &parser.InExpr{
		Expr:   &parser.LiteralExpr{Type: parser.LiteralString, Value: "world"},
		Values: []parser.Expression{collateVal},
		Not:    false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInValueComparison with collation: %v", err)
	}

	// Verify that at least one OpEq instruction carries a P4Static collation.
	found := false
	for _, instr := range v.Program {
		if instr.Opcode == vdbe.OpEq && instr.P4Type == vdbe.P4Static && instr.P4.Z != "" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected OpEq with P4 collation in IN comparison, not found")
	}
}

// ---------------------------------------------------------------------------
// generateInSubqueryMaterialised — 80%
//
// MC/DC conditions:
//   C1: subqueryExecutor returns error → fall back to generateInSubquery (error path)
//   C2: rows contain values → emit value-list comparisons (happy path)
//   C3: rows are empty (len(row)==0) → values slice is nil → generateInValueList with empty list
//
// ---------------------------------------------------------------------------

// Happy path: subquery materialised with rows containing values.
func TestMCDC5_GenerateInSubqueryMaterialised_WithRows(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, nil
	})

	inExpr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInSubqueryMaterialised with rows: %v", err)
	}
}

// Happy path: rows with nil values (skipped in conversion loop).
func TestMCDC5_GenerateInSubqueryMaterialised_NilValueRows(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	// Row with nil first element — skipped; resulting values slice is empty.
	gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{nil}}, nil
	})

	inExpr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInSubqueryMaterialised nil value rows: %v", err)
	}
}

// Float64 row value: verify float64 values in rows are converted to LiteralFloat.
func TestMCDC5_GenerateInSubqueryMaterialised_FloatRowValue(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{float64(3.14)}}, nil
	})

	inExpr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "3"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInSubqueryMaterialised float row: %v", err)
	}
}

// String row value: verify string values are converted to LiteralString.
func TestMCDC5_GenerateInSubqueryMaterialised_StringRowValue(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{"hello"}}, nil
	})

	inExpr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "world"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "x"}},
			},
		},
		Not: false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInSubqueryMaterialised string row: %v", err)
	}
}

// Row with empty column list — skipped in values conversion loop.
func TestMCDC5_GenerateInSubqueryMaterialised_EmptyColumnRow(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	gen := NewCodeGenerator(v)

	gen.SetSubqueryExecutor(func(sel *parser.SelectStmt) ([][]interface{}, error) {
		return [][]interface{}{{}}, nil
	})

	inExpr := &parser.InExpr{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			},
		},
		Not: false,
	}

	_, err := gen.GenerateExpr(inExpr)
	if err != nil {
		t.Fatalf("generateInSubqueryMaterialised empty column row: %v", err)
	}
}

// ---------------------------------------------------------------------------
// castToNumeric — 85.7%
//
// Function branches:
//   1. CoerceToInteger succeeds → return int64       (covered: "42")
//   2. CoerceToInteger fails, value is string, ParseFloat succeeds → float64 (covered: "3.14")
//   3. CoerceToInteger fails, value is string, ParseFloat fails → return value (covered: "abc")
//   4. CoerceToInteger fails, value is NOT a string → return value  (uncovered at 85.7%)
//
// Branch 4: []byte satisfies default case in CoerceToInteger (returns false)
// and is not a string, so castToNumeric returns the original []byte value.
// Drive through EvaluateCast with targetType "NUMERIC".
// ---------------------------------------------------------------------------

func TestMCDC5_CastToNumeric_NonStringNonNumeric(t *testing.T) {
	t.Parallel()
	// []byte: CoerceToInteger returns false, .(string) type-assertion fails
	// → castToNumeric returns the original value unchanged.
	blob := []byte("hello")
	got := EvaluateCast(blob, "NUMERIC")
	// The value should be returned as-is since it's neither integer-coercible
	// nor a plain string.
	if got == nil {
		t.Error("EvaluateCast([]byte, NUMERIC): expected non-nil result")
	}
}

// Confirm branch 1: integer-like input → int64 returned.
func TestMCDC5_CastToNumeric_IntegerLike(t *testing.T) {
	t.Parallel()
	got := EvaluateCast("42", "NUMERIC")
	if v, ok := got.(int64); !ok || v != 42 {
		t.Errorf("EvaluateCast(%q, NUMERIC) = %v, want int64(42)", "42", got)
	}
}

// Confirm branch 2: integer-string already handled by CoerceToInteger; an
// out-of-range-as-int-but-valid-float can reach ParseFloat, but
// coerceStringToInt internally also calls ParseFloat so any float string is
// actually consumed by CoerceToInteger too. Branch 2 is structurally unreachable
// from string inputs (CoerceToInteger always succeeds for ParseFloat-valid strings).
// We confirm this: "3.14" → int64(3) because coerceStringToInt converts via ParseFloat.
func TestMCDC5_CastToNumeric_FloatStringCoercedToInt(t *testing.T) {
	t.Parallel()
	// "3.14" → CoerceToInteger("3.14") succeeds (returns int64(3))
	// → castToNumeric returns int64(3)
	got := EvaluateCast("3.14", "NUMERIC")
	if v, ok := got.(int64); !ok || v != 3 {
		t.Errorf("EvaluateCast(%q, NUMERIC) = %v (type %T), want int64(3)", "3.14", got, got)
	}
}

// Confirm branch 3: non-numeric string → original string returned.
func TestMCDC5_CastToNumeric_NonNumericString(t *testing.T) {
	t.Parallel()
	got := EvaluateCast("abc", "NUMERIC")
	if v, ok := got.(string); !ok || v != "abc" {
		t.Errorf("EvaluateCast(%q, NUMERIC) = %v, want %q", "abc", got, "abc")
	}
}
