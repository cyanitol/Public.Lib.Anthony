// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

// MC/DC (Modified Condition/Decision Coverage) tests for the expr package — file 2.
//
// This file covers compound boolean conditions that were NOT covered in
// codegen_mcdc_test.go.  Each test group is documented with:
//   - source file:line
//   - the exact compound condition
//   - sub-condition labels
//
// Run with: go test -run MCDC ./internal/expr/...

import (
	"testing"
)

// ----------------------------------------------------------------------------
// Condition: (li > 0 && ri > 0 && result < 0) || (li < 0 && ri < 0 && result > 0)
// (arithmetic.go – addOverflows, line ~64)
//
// This is a disjunction of two conjunctions.  For MC/DC we treat it as:
//   A = (li > 0 && ri > 0 && result < 0)   — positive overflow
//   B = (li < 0 && ri < 0 && result > 0)   — negative overflow
// Compound: A || B
//
// MC/DC cases:
//   A=true,  B=false → overflow detected (positive)
//   A=false, B=true  → overflow detected (negative)
//   A=false, B=false → no overflow
// ----------------------------------------------------------------------------

func TestMCDC_AddOverflows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		li   int64
		ri   int64
		want bool
	}{
		{
			// A=true, B=false: large positive + large positive wraps to negative
			name: "MCDC_addOverflows_positive_overflow_A_true_B_false",
			li:   1<<62 + 1,
			ri:   1<<62 + 1,
			want: true,
		},
		{
			// A=false, B=true: large negative + large negative wraps to positive
			name: "MCDC_addOverflows_negative_overflow_A_false_B_true",
			li:   -(1<<62 + 1),
			ri:   -(1<<62 + 1),
			want: true,
		},
		{
			// A=false, B=false: normal addition, no overflow
			name: "MCDC_addOverflows_no_overflow_A_false_B_false",
			li:   1,
			ri:   1,
			want: false,
		},
		{
			// A=false, B=false: positive + negative never overflows via this rule
			name: "MCDC_addOverflows_mixed_sign_no_overflow",
			li:   1<<62 + 1,
			ri:   -(1<<62 + 1),
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.li + tt.ri
			got := addOverflows(tt.li, tt.ri, result)
			if got != tt.want {
				t.Errorf("addOverflows(%d, %d, %d) = %v, want %v",
					tt.li, tt.ri, result, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: (li > 0 && ri < 0 && result < 0) || (li < 0 && ri > 0 && result > 0)
// (arithmetic.go – subtractOverflows, line ~68)
//
// Compound (as above, two-clause disjunction):
//   A = (li > 0 && ri < 0 && result < 0)   — positive minus negative wraps
//   B = (li < 0 && ri > 0 && result > 0)   — negative minus positive wraps
// Compound: A || B
//
// MC/DC cases:
//   A=true,  B=false → overflow detected
//   A=false, B=true  → overflow detected
//   A=false, B=false → no overflow
// ----------------------------------------------------------------------------

func TestMCDC_SubtractOverflows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		li   int64
		ri   int64
		want bool
	}{
		{
			// A=true, B=false: large positive minus large negative → wraps to negative
			// e.g. MaxInt64 - (-1) overflows
			name: "MCDC_subtractOverflows_positive_overflow_A_true_B_false",
			li:   1<<62 + 1,
			ri:   -(1 << 62),
			want: true,
		},
		{
			// A=false, B=true: large negative minus large positive → wraps to positive
			name: "MCDC_subtractOverflows_negative_overflow_A_false_B_true",
			li:   -(1<<62 + 1),
			ri:   1 << 62,
			want: true,
		},
		{
			// A=false, B=false: normal subtraction, no overflow
			name: "MCDC_subtractOverflows_no_overflow_A_false_B_false",
			li:   5,
			ri:   3,
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.li - tt.ri
			got := subtractOverflows(tt.li, tt.ri, result)
			if got != tt.want {
				t.Errorf("subtractOverflows(%d, %d, %d) = %v, want %v",
					tt.li, tt.ri, result, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: (ris && ri == 0) || (!ris && rf == 0.0)
// (arithmetic.go – isModZero, line ~160)
//
// Sub-conditions:
//   A = ris && ri == 0    (right is integer and zero)
//   B = !ris && rf == 0.0 (right is float and zero)
// Compound: A || B
//
// MC/DC cases:
//   A=true,  B=false → integer zero → true
//   A=false, B=true  → float zero   → true
//   A=false, B=false → non-zero operand → false
// ----------------------------------------------------------------------------

func TestMCDC_IsModZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ri   int64
		ris  bool // true if right operand is integer
		rf   float64
		want bool
	}{
		{
			// A=true, B=false: integer zero
			name: "MCDC_isModZero_integer_zero_A_true_B_false",
			ri:   0,
			ris:  true,
			rf:   1.0, // irrelevant
			want: true,
		},
		{
			// A=false, B=true: float zero
			name: "MCDC_isModZero_float_zero_A_false_B_true",
			ri:   1, // irrelevant
			ris:  false,
			rf:   0.0,
			want: true,
		},
		{
			// A=false, B=false: integer non-zero
			name: "MCDC_isModZero_integer_nonzero_A_false_B_false",
			ri:   3,
			ris:  true,
			rf:   0.0, // irrelevant
			want: false,
		},
		{
			// A=false, B=false: float non-zero
			name: "MCDC_isModZero_float_nonzero_A_false_B_false_float",
			ri:   0, // irrelevant
			ris:  false,
			rf:   1.5,
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isModZero(tt.ri, tt.ris, tt.rf)
			if got != tt.want {
				t.Errorf("isModZero(ri=%d, ris=%v, rf=%v) = %v, want %v",
					tt.ri, tt.ris, tt.rf, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: isNumeric(left) && isNumeric(right)
// (compare.go – CompareValues, line ~241)
//
// Sub-conditions:
//   A = isNumeric(left)
//   B = isNumeric(right)
// Compound: A && B
//
// MC/DC cases:
//   A=true,  B=true  → compareNumerics path (int64 vs float64)
//   A=true,  B=false → type-order fallback (int64 vs string)
//   A=false, B=true  → type-order fallback (string vs int64)
//   A=false, B=false → type-order fallback (string vs string — handled by compareSameType)
// ----------------------------------------------------------------------------

func TestMCDC_CompareValues_NumericBothPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  CompareResult
	}{
		{
			// A=true, B=true: int64 vs float64 → compareNumerics
			// 2 vs 1.5 → Greater
			name:  "MCDC_compare_numeric_both_A_true_B_true",
			left:  int64(2),
			right: float64(1.5),
			want:  CmpGreater,
		},
		{
			// A=true, B=false: int64 vs string → type-order (int < string in SQLite order)
			// int64 typeOrder=1, string typeOrder=3 → CmpLess
			name:  "MCDC_compare_numeric_left_only_A_true_B_false",
			left:  int64(42),
			right: "hello",
			want:  CmpLess,
		},
		{
			// A=false, B=true: string vs int64 → type-order → CmpGreater
			name:  "MCDC_compare_numeric_right_only_A_false_B_true",
			left:  "hello",
			right: int64(42),
			want:  CmpGreater,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CompareValues(tt.left, tt.right, AFF_NONE, nil)
			if got != tt.want {
				t.Errorf("CompareValues(%v, %v) = %v, want %v",
					tt.left, tt.right, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition: (cmpLow == CmpGreater || cmpLow == CmpEqual) && (cmpHigh == CmpLess || cmpHigh == CmpEqual)
// (compare.go – EvaluateBetween, line ~486-487)
//
// This is (A || B) && (C || D) where:
//   A = cmpLow == CmpGreater   (value > low)
//   B = cmpLow == CmpEqual     (value == low)
//   C = cmpHigh == CmpLess     (value < high)
//   D = cmpHigh == CmpEqual    (value == high)
//
// We treat the two pairs as compound sub-conditions L = (A||B) and H = (C||D).
// MC/DC for L && H:
//   L=true,  H=true  → true  (value is within range)
//   L=true,  H=false → false (H flips: value > high)
//   L=false, H=true  → false (L flips: value < low)
//
// Additional inner MC/DC for L=(A||B):
//   A=true  (value > low)  independently makes L true  — covered by cases 1,2
//   B=true  (value == low) independently makes L true  — extra case below
//
// Additional inner MC/DC for H=(C||D):
//   C=true  (value < high) independently makes H true  — covered by cases 1,3
//   D=true  (value == high) independently makes H true — extra case below
// ----------------------------------------------------------------------------

func TestMCDC_EvaluateBetween_RangeCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
		low   interface{}
		high  interface{}
		want  interface{} // true, false, or nil
	}{
		{
			// L=true (A: value>low), H=true (C: value<high) → true
			name:  "MCDC_between_in_range_L_true_H_true",
			value: int64(5),
			low:   int64(1),
			high:  int64(10),
			want:  true,
		},
		{
			// L=true (A: value>low), H=false (value > high) → false
			name:  "MCDC_between_above_high_L_true_H_false",
			value: int64(15),
			low:   int64(1),
			high:  int64(10),
			want:  false,
		},
		{
			// L=false (value < low), H=true (C: value < high) → false
			name:  "MCDC_between_below_low_L_false_H_true",
			value: int64(0),
			low:   int64(1),
			high:  int64(10),
			want:  false,
		},
		{
			// B=true: value == low (lower boundary) → in range
			name:  "MCDC_between_at_low_boundary_B_true",
			value: int64(1),
			low:   int64(1),
			high:  int64(10),
			want:  true,
		},
		{
			// D=true: value == high (upper boundary) → in range
			name:  "MCDC_between_at_high_boundary_D_true",
			value: int64(10),
			low:   int64(1),
			high:  int64(10),
			want:  true,
		},
		{
			// NULL propagation: nil value → nil result
			name:  "MCDC_between_null_value",
			value: nil,
			low:   int64(1),
			high:  int64(10),
			want:  nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EvaluateBetween(tt.value, tt.low, tt.high, AFF_NONE, nil)
			if got != tt.want {
				t.Errorf("EvaluateBetween(%v, %v, %v) = %v, want %v",
					tt.value, tt.low, tt.high, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (affinity.go – affinityHandleSelect, line ~50):
//   e.Select != nil && e.Select.Columns != nil && len(e.Select.Columns.Items) > 0
//
// Sub-conditions:
//   A = e.Select != nil
//   B = e.Select.Columns != nil
//   C = len(e.Select.Columns.Items) > 0
// Compound: A && B && C
//
// MC/DC pairs:
//   case 1: A=T B=T C=T → returns affinity of first result column
//   case 2: A=T B=T C=F → returns AFF_NONE (empty column list)
//   case 3: A=T B=F C=x → returns AFF_NONE (Columns is nil)
//   case 4: A=F B=x C=x → returns AFF_NONE (Select is nil)
// ----------------------------------------------------------------------------

func TestMCDC_AffinityHandleSelect_NilGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		e    *Expr
		want Affinity
	}{
		{
			// A=T B=T C=T: Select has columns → affinity of first column (INTEGER)
			name: "MCDC_affinity_select_with_col_A_true_B_true_C_true",
			e: &Expr{
				Op: OpSelect,
				Select: &SelectStmt{
					Columns: &ExprList{
						Items: []*ExprListItem{
							{Expr: &Expr{Op: OpColumn, Affinity: AFF_INTEGER}},
						},
					},
				},
			},
			want: AFF_INTEGER,
		},
		{
			// A=T B=T C=F: Select exists, Columns exists but empty → AFF_NONE
			name: "MCDC_affinity_select_empty_cols_A_true_B_true_C_false",
			e: &Expr{
				Op: OpSelect,
				Select: &SelectStmt{
					Columns: &ExprList{Items: []*ExprListItem{}},
				},
			},
			want: AFF_NONE,
		},
		{
			// A=T B=F C=x: Columns is nil → AFF_NONE
			name: "MCDC_affinity_select_nil_columns_A_true_B_false",
			e: &Expr{
				Op:     OpSelect,
				Select: &SelectStmt{Columns: nil},
			},
			want: AFF_NONE,
		},
		{
			// A=F B=x C=x: Select is nil → AFF_NONE
			name: "MCDC_affinity_select_nil_select_A_false",
			e: &Expr{
				Op:     OpSelect,
				Select: nil,
			},
			want: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := GetExprAffinity(tt.e)
			if got != tt.want {
				t.Errorf("GetExprAffinity(OpSelect) = %v, want %v", got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (affinity.go – affinityHandleFunction, line ~89):
//   e.Affinity == AFF_NONE && e.List != nil && len(e.List.Items) > 0
//
// Sub-conditions:
//   A = e.Affinity == AFF_NONE
//   B = e.List != nil
//   C = len(e.List.Items) > 0
// Compound: A && B && C
//
// MC/DC pairs:
//   case 1: A=T B=T C=T → returns affinity of first argument
//   case 2: A=T B=T C=F → returns e.Affinity (AFF_NONE, empty list)
//   case 3: A=T B=F C=x → returns e.Affinity (nil list)
//   case 4: A=F B=T C=T → returns e.Affinity (non-NONE explicit affinity)
// ----------------------------------------------------------------------------

func TestMCDC_AffinityHandleFunction_ArgAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		e    *Expr
		want Affinity
	}{
		{
			// A=T B=T C=T: no explicit affinity, has args → use first arg's affinity
			name: "MCDC_affinity_fn_no_aff_with_args_A_true_B_true_C_true",
			e: &Expr{
				Op:       OpFunction,
				Affinity: AFF_NONE,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Op: OpColumn, Affinity: AFF_TEXT}},
					},
				},
			},
			want: AFF_TEXT,
		},
		{
			// A=T B=T C=F: no explicit affinity, empty args → AFF_NONE
			name: "MCDC_affinity_fn_no_aff_empty_args_A_true_B_true_C_false",
			e: &Expr{
				Op:       OpFunction,
				Affinity: AFF_NONE,
				List:     &ExprList{Items: []*ExprListItem{}},
			},
			want: AFF_NONE,
		},
		{
			// A=T B=F C=x: no explicit affinity, nil list → AFF_NONE
			name: "MCDC_affinity_fn_no_aff_nil_list_A_true_B_false",
			e: &Expr{
				Op:       OpFunction,
				Affinity: AFF_NONE,
				List:     nil,
			},
			want: AFF_NONE,
		},
		{
			// A=F B=T C=T: explicit affinity set → use it, ignore arg affinity
			name: "MCDC_affinity_fn_explicit_aff_A_false_B_true_C_true",
			e: &Expr{
				Op:       OpFunction,
				Affinity: AFF_INTEGER,
				List: &ExprList{
					Items: []*ExprListItem{
						{Expr: &Expr{Op: OpColumn, Affinity: AFF_TEXT}},
					},
				},
			},
			want: AFF_INTEGER,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := GetExprAffinity(tt.e)
			if got != tt.want {
				t.Errorf("GetExprAffinity(OpFunction) = %v, want %v", got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (compare.go – evaluateIs, line ~298):
//   left == nil && right == nil  →  true
//   left == nil || right == nil  →  false
//
// These are two short-circuit guards that determine IS semantics.
//
// Sub-conditions for the first guard:
//   A = left == nil
//   B = right == nil
// Compound: A && B
//
// MC/DC pairs:
//   A=T B=T → both nil → IS returns true
//   A=T B=F → only left nil → IS returns false
//   A=F B=T → only right nil → IS returns false
//   A=F B=F → neither nil → compare normally
// ----------------------------------------------------------------------------

func TestMCDC_EvaluateIs_NullSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  interface{}
	}{
		{
			// A=T, B=T: NULL IS NULL → true
			name:  "MCDC_is_both_null_A_true_B_true",
			left:  nil,
			right: nil,
			want:  true,
		},
		{
			// A=T, B=F: NULL IS 42 → false
			name:  "MCDC_is_left_null_A_true_B_false",
			left:  nil,
			right: int64(42),
			want:  false,
		},
		{
			// A=F, B=T: 42 IS NULL → false
			name:  "MCDC_is_right_null_A_false_B_true",
			left:  int64(42),
			right: nil,
			want:  false,
		},
		{
			// A=F, B=F: 42 IS 42 → true (equal)
			name:  "MCDC_is_neither_null_equal_A_false_B_false",
			left:  int64(42),
			right: int64(42),
			want:  true,
		},
		{
			// A=F, B=F: 1 IS 2 → false (not equal)
			name:  "MCDC_is_neither_null_not_equal_A_false_B_false",
			left:  int64(1),
			right: int64(2),
			want:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EvaluateComparison(OpIs, tt.left, tt.right, AFF_NONE, nil)
			if got != tt.want {
				t.Errorf("EvaluateComparison(IS, %v, %v) = %v, want %v",
					tt.left, tt.right, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (compare.go – evaluateIsNot, line ~309):
//   left == nil && right == nil  →  false (IS NOT: both nil means equal)
//   left == nil || right == nil  →  true  (IS NOT: one nil means not equal)
//
// Same two-guard structure as evaluateIs, but inverted.
//
// MC/DC pairs:
//   A=T B=T → both nil → IS NOT returns false
//   A=T B=F → only left nil → IS NOT returns true
//   A=F B=T → only right nil → IS NOT returns true
//   A=F B=F → neither nil → compare normally
// ----------------------------------------------------------------------------

func TestMCDC_EvaluateIsNot_NullSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  interface{}
	}{
		{
			// A=T, B=T: NULL IS NOT NULL → false
			name:  "MCDC_isnot_both_null_A_true_B_true",
			left:  nil,
			right: nil,
			want:  false,
		},
		{
			// A=T, B=F: NULL IS NOT 42 → true
			name:  "MCDC_isnot_left_null_A_true_B_false",
			left:  nil,
			right: int64(42),
			want:  true,
		},
		{
			// A=F, B=T: 42 IS NOT NULL → true
			name:  "MCDC_isnot_right_null_A_false_B_true",
			left:  int64(42),
			right: nil,
			want:  true,
		},
		{
			// A=F, B=F: 42 IS NOT 42 → false (equal)
			name:  "MCDC_isnot_neither_null_equal_A_false_B_false",
			left:  int64(42),
			right: int64(42),
			want:  false,
		},
		{
			// A=F, B=F: 1 IS NOT 2 → true (not equal)
			name:  "MCDC_isnot_neither_null_not_equal_A_false_B_false",
			left:  int64(1),
			right: int64(2),
			want:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EvaluateComparison(OpIsNot, tt.left, tt.right, AFF_NONE, nil)
			if got != tt.want {
				t.Errorf("EvaluateComparison(IS NOT, %v, %v) = %v, want %v",
					tt.left, tt.right, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Condition (affinity.go – CompareAffinity, line ~197):
//   IsNumericAffinity(aff1) || IsNumericAffinity(aff2)
//
// Sub-conditions:
//   A = IsNumericAffinity(aff1)   (left expression has numeric affinity)
//   B = IsNumericAffinity(aff2)   (right expression has numeric affinity)
// Compound: A || B
//
// MC/DC cases (3 needed):
//   A=true,  B=false → numeric wins → AFF_NUMERIC
//   A=false, B=true  → numeric wins → AFF_NUMERIC
//   A=false, B=false → no numeric   → AFF_BLOB (both are column affinities)
//
// Drive through CompareAffinity(left, right) with column-affinity Exprs.
// Both affinities are > AFF_NONE so we reach the IsNumericAffinity branch.
// ----------------------------------------------------------------------------

func TestMCDC_CompareAffinity_NumericOrCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		aff1 Affinity
		aff2 Affinity
		want Affinity
	}{
		{
			// A=true, B=false: left is INTEGER (numeric), right is TEXT
			name: "MCDC_compareAff_left_numeric_A_true_B_false",
			aff1: AFF_INTEGER,
			aff2: AFF_TEXT,
			want: AFF_NUMERIC,
		},
		{
			// A=false, B=true: left is TEXT, right is REAL (numeric)
			name: "MCDC_compareAff_right_numeric_A_false_B_true",
			aff1: AFF_TEXT,
			aff2: AFF_REAL,
			want: AFF_NUMERIC,
		},
		{
			// A=false, B=false: both are TEXT — non-numeric → AFF_BLOB
			name: "MCDC_compareAff_both_text_A_false_B_false",
			aff1: AFF_TEXT,
			aff2: AFF_TEXT,
			want: AFF_BLOB,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			left := &Expr{Op: OpColumn, Affinity: tt.aff1}
			right := &Expr{Op: OpColumn, Affinity: tt.aff2}
			got := CompareAffinity(left, right)
			if got != tt.want {
				t.Errorf("CompareAffinity(%v, %v) = %v, want %v",
					tt.aff1, tt.aff2, got, tt.want)
			}
		})
	}
}
