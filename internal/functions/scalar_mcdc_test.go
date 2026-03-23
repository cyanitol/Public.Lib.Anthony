// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// TestMCDC_ReplaceFunc_NullCheck covers:
//
//	Condition: args[0].IsNull() || args[1].IsNull() || args[2].IsNull()
//	A = args[0].IsNull()
//	B = args[1].IsNull()
//	C = args[2].IsNull()
//
// MC/DC cases (N+1 = 4 cases for 3 sub-conditions in an OR chain):
//
//	case "A_true":  A=T → overall T, regardless of B/C
//	case "B_true":  A=F, B=T → overall T, B independently determines outcome
//	case "C_true":  A=F, B=F, C=T → overall T, C independently determines outcome
//	case "all_false": A=F, B=F, C=F → overall F
func TestMCDC_ReplaceFunc_NullCheck(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{
			// A=T (arg0 is null): overall T → returns NULL
			name:     "A_true_arg0_null",
			args:     []Value{NewNullValue(), NewTextValue("x"), NewTextValue("y")},
			wantNull: true,
		},
		{
			// A=F, B=T (arg1 is null): overall T → returns NULL
			name:     "B_true_arg1_null",
			args:     []Value{NewTextValue("hello"), NewNullValue(), NewTextValue("y")},
			wantNull: true,
		},
		{
			// A=F, B=F, C=T (arg2 is null): overall T → returns NULL
			name:     "C_true_arg2_null",
			args:     []Value{NewTextValue("hello"), NewTextValue("x"), NewNullValue()},
			wantNull: true,
		},
		{
			// A=F, B=F, C=F: overall F → returns text result
			name:     "all_false_no_nulls",
			args:     []Value{NewTextValue("hello"), NewTextValue("l"), NewTextValue("r")},
			wantNull: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := replaceFunc(tt.args)
			if err != nil {
				t.Fatalf("replaceFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("replaceFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// TestMCDC_InstrFunc_NullCheck covers:
//
//	Condition: args[0].IsNull() || args[1].IsNull()
//	A = args[0].IsNull()
//	B = args[1].IsNull()
//
// MC/DC cases (N+1 = 3 for 2 sub-conditions in OR):
//
//	case "A_true":   A=T → overall T (arg0 null → return NULL)
//	case "B_true":   A=F, B=T → overall T, B independently determines outcome
//	case "all_false": A=F, B=F → overall F (proceed to search)
func TestMCDC_InstrFunc_NullCheck(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{
			// A=T: arg0 null → NULL
			name:     "A_true_haystack_null",
			args:     []Value{NewNullValue(), NewTextValue("needle")},
			wantNull: true,
		},
		{
			// A=F, B=T: arg1 null → NULL
			name:     "B_true_needle_null",
			args:     []Value{NewTextValue("haystack"), NewNullValue()},
			wantNull: true,
		},
		{
			// A=F, B=F: neither null → returns integer position
			name:     "all_false_search_proceeds",
			args:     []Value{NewTextValue("haystack"), NewTextValue("a")},
			wantNull: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := instrFunc(tt.args)
			if err != nil {
				t.Fatalf("instrFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("instrFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// TestMCDC_InstrFunc_BlobBranch covers:
//
//	Condition: args[0].Type() == TypeBlob && args[1].Type() == TypeBlob
//	A = args[0].Type() == TypeBlob
//	B = args[1].Type() == TypeBlob
//
// MC/DC cases (N+1 = 3 for 2 sub-conditions in AND):
//
//	case "A_false":  A=F → overall F (text search used)
//	case "B_false":  A=T, B=F → overall F, B independently determines outcome
//	case "all_true": A=T, B=T → overall T (blob search used)
func TestMCDC_InstrFunc_BlobBranch(t *testing.T) {
	tests := []struct {
		name       string
		args       []Value
		wantBlobOp bool // true if blob path was taken (result reflects byte indexing)
		wantResult int64
	}{
		{
			// A=F: haystack is text, needle is text → text search
			name:       "A_false_text_text",
			args:       []Value{NewTextValue("hello"), NewTextValue("l")},
			wantBlobOp: false,
			wantResult: 3, // 1-indexed char position
		},
		{
			// A=T, B=F: haystack is blob, needle is text → text path via AsString
			name:       "B_false_blob_text",
			args:       []Value{NewBlobValue([]byte("hello")), NewTextValue("l")},
			wantBlobOp: false,
			wantResult: 3,
		},
		{
			// A=T, B=T: both blob → blob byte search
			name:       "all_true_blob_blob",
			args:       []Value{NewBlobValue([]byte("hello")), NewBlobValue([]byte("l"))},
			wantBlobOp: true,
			wantResult: 3, // 1-indexed byte position
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := instrFunc(tt.args)
			if err != nil {
				t.Fatalf("instrFunc() unexpected error: %v", err)
			}
			if result.AsInt64() != tt.wantResult {
				t.Errorf("instrFunc() = %d, want %d", result.AsInt64(), tt.wantResult)
			}
		})
	}
}

// TestMCDC_NullifFunc_BothNull covers:
//
//	Condition: args[0].IsNull() && args[1].IsNull()
//	A = args[0].IsNull()
//	B = args[1].IsNull()
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F (falls through to next check)
//	case "B_false":  A=T, B=F → overall F, B independently determines outcome
//	case "all_true": A=T, B=T → overall T → returns NULL
func TestMCDC_NullifFunc_BothNull(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{
			// A=F: arg0 not null → condition false, check continues to inequality path
			name:     "A_false_arg0_not_null",
			args:     []Value{NewIntValue(1), NewNullValue()},
			wantNull: false, // arg0 is returned (not-null, not-equal path)
		},
		{
			// A=T, B=F: arg0 null, arg1 not null → AND condition false
			// falls through to the one-null OR which returns args[0] (which is null)
			name:     "B_false_arg1_not_null",
			args:     []Value{NewNullValue(), NewIntValue(1)},
			wantNull: true, // args[0] is null; the one-null OR branch returns it
		},
		{
			// A=T, B=T: both null → condition true → returns NULL
			name:     "all_true_both_null",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := nullifFunc(tt.args)
			if err != nil {
				t.Fatalf("nullifFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("nullifFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// TestMCDC_NullifFunc_OneNull covers:
//
//	Condition: args[0].IsNull() || args[1].IsNull()   (second guard in nullifFunc)
//	A = args[0].IsNull()
//	B = args[1].IsNull()
//
// This guard is reached only when NOT both are null (i.e., A&&B was false).
// MC/DC cases (N+1 = 3):
//
//	case "A_true":   A=T, B=F → overall T → returns args[0] (the null)
//	case "B_true":   A=F, B=T → overall T, B independently determines outcome → returns args[0]
//	case "all_false": A=F, B=F → overall F → falls to value comparison
func TestMCDC_NullifFunc_OneNull(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantInt  int64
	}{
		{
			// A=T, B=F: arg0 null, arg1 not null → OR is true → return arg0 (null)
			name:     "A_true_arg0_null",
			args:     []Value{NewNullValue(), NewIntValue(5)},
			wantNull: true,
		},
		{
			// A=F, B=T: arg0 not null, arg1 null → OR is true → return arg0 (not null)
			name:     "B_true_arg1_null",
			args:     []Value{NewIntValue(7), NewNullValue()},
			wantNull: false,
			wantInt:  7,
		},
		{
			// A=F, B=F: neither null → OR is false → value comparison proceeds
			name:     "all_false_compare_proceeds",
			args:     []Value{NewIntValue(3), NewIntValue(4)},
			wantNull: false,
			wantInt:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := nullifFunc(tt.args)
			if err != nil {
				t.Fatalf("nullifFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("nullifFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsInt64() != tt.wantInt {
				t.Errorf("nullifFunc() value = %d, want %d", result.AsInt64(), tt.wantInt)
			}
		})
	}
}

// TestMCDC_FilterIgnoredChars_EarlyReturn covers:
//
//	Condition: len(args) < 2 || args[1].IsNull()
//	A = len(args) < 2
//	B = args[1].IsNull()
//
// MC/DC cases (N+1 = 3):
//
//	case "A_true":   A=T → overall T → return hexStr unfiltered
//	case "B_true":   A=F, B=T → overall T, B independently determines outcome
//	case "all_false": A=F, B=F → overall F → filtering applied
func TestMCDC_FilterIgnoredChars_EarlyReturn(t *testing.T) {
	tests := []struct {
		name       string
		args       []Value
		wantResult string
	}{
		{
			// A=T: only 1 arg (no ignore arg) → unfiltered
			name:       "A_true_one_arg",
			args:       []Value{NewTextValue("4142")},
			wantResult: "4142",
		},
		{
			// A=F, B=T: 2 args but second is null → unfiltered
			name:       "B_true_second_arg_null",
			args:       []Value{NewTextValue("4142"), NewNullValue()},
			wantResult: "4142",
		},
		{
			// A=F, B=F: 2 args with non-null ignore → filtered
			// hex string "41-42" ignoring "-" → "4142"
			name:       "all_false_filtering_applied",
			args:       []Value{NewTextValue("41-42"), NewTextValue("-")},
			wantResult: "4142",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterIgnoredChars(tt.args)
			if got != tt.wantResult {
				t.Errorf("filterIgnoredChars() = %q, want %q", got, tt.wantResult)
			}
		})
	}
}

// TestMCDC_TrimFunc_CustomCutset covers:
//
//	Condition: len(args) == 2 && !args[1].IsNull()
//	A = len(args) == 2
//	B = !args[1].IsNull()
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → use default space cutset
//	case "B_false":  A=T, B=F (arg1 is null) → overall F → use default space cutset
//	case "all_true": A=T, B=T → overall T → use custom cutset
func TestMCDC_TrimFunc_CustomCutset(t *testing.T) {
	tests := []struct {
		name string
		fn   func([]Value) (Value, error)
		args []Value
		want string
	}{
		{
			// A=F: 1 arg → uses default space cutset
			name: "A_false_one_arg_spaces_trimmed",
			fn:   trimFunc,
			args: []Value{NewTextValue("  hello  ")},
			want: "hello",
		},
		{
			// A=T, B=F: 2 args but arg1 is null → uses default space cutset
			name: "B_false_second_arg_null_spaces_trimmed",
			fn:   trimFunc,
			args: []Value{NewTextValue("  hello  "), NewNullValue()},
			want: "hello",
		},
		{
			// A=T, B=T: 2 args with non-null cutset → uses custom cutset "x"
			name: "all_true_custom_cutset",
			fn:   trimFunc,
			args: []Value{NewTextValue("xxhelloxx"), NewTextValue("x")},
			want: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if err != nil {
				t.Fatalf("trimFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("trimFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_LtrimFunc_CustomCutset covers the same compound condition for ltrimFunc:
//
//	Condition: len(args) == 2 && !args[1].IsNull()
//	A = len(args) == 2
//	B = !args[1].IsNull()
func TestMCDC_LtrimFunc_CustomCutset(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "A_false_one_arg",
			args: []Value{NewTextValue("  hello  ")},
			want: "hello  ",
		},
		{
			name: "B_false_null_cutset",
			args: []Value{NewTextValue("  hello  "), NewNullValue()},
			want: "hello  ",
		},
		{
			name: "all_true_custom_cutset",
			args: []Value{NewTextValue("xxhello"), NewTextValue("x")},
			want: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ltrimFunc(tt.args)
			if err != nil {
				t.Fatalf("ltrimFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("ltrimFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_RtrimFunc_CustomCutset covers the same compound condition for rtrimFunc:
//
//	Condition: len(args) == 2 && !args[1].IsNull()
//	A = len(args) == 2
//	B = !args[1].IsNull()
func TestMCDC_RtrimFunc_CustomCutset(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "A_false_one_arg",
			args: []Value{NewTextValue("  hello  ")},
			want: "  hello",
		},
		{
			name: "B_false_null_cutset",
			args: []Value{NewTextValue("  hello  "), NewNullValue()},
			want: "  hello",
		},
		{
			name: "all_true_custom_cutset",
			args: []Value{NewTextValue("helloxx"), NewTextValue("x")},
			want: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rtrimFunc(tt.args)
			if err != nil {
				t.Fatalf("rtrimFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("rtrimFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_AdjustZeroStart_ExplicitLenAndPositive covers:
//
//	Condition: hasExplicitLength && subLen > 0
//	A = hasExplicitLength
//	B = subLen > 0
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → subLen unchanged
//	case "B_false":  A=T, B=F (subLen==0) → overall F → subLen unchanged
//	case "all_true": A=T, B=T → overall T → subLen decremented by 1
//
// Exercised via substrFunc with start=0 (position 0 in SQLite terms).
func TestMCDC_AdjustZeroStart_ExplicitLenAndPositive(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			// A=F: start=0 with no explicit length (2 args) → subLen not decremented
			// substr("hello", 0) with no explicit len → returns "hello" (from pos 0)
			name: "A_false_no_explicit_length",
			args: []Value{NewTextValue("hello"), NewIntValue(0)},
			want: "hello",
		},
		{
			// A=T, B=F: start=0 with explicit subLen=0 → subLen stays 0 → empty
			name: "B_false_explicit_zero_len",
			args: []Value{NewTextValue("hello"), NewIntValue(0), NewIntValue(0)},
			want: "",
		},
		{
			// A=T, B=T: start=0 with explicit positive subLen → subLen decremented
			// substr("hello", 0, 3) → position 0 wastes one char → "he" (len 2 = 3-1)
			name: "all_true_explicit_positive_len",
			args: []Value{NewTextValue("hello"), NewIntValue(0), NewIntValue(3)},
			want: "he",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := substrFunc(tt.args)
			if err != nil {
				t.Fatalf("substrFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("substrFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_ApplyPadding_ZeroPadAndNotLeftAlign covers:
//
//	Condition: spec.zeroPad && !spec.leftAlign
//	A = spec.zeroPad
//	B = !spec.leftAlign  (i.e., leftAlign is false)
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → space padding applied
//	case "B_false":  A=T, B=F (leftAlign=T) → overall F, B determines outcome → space-right padding
//	case "all_true": A=T, B=T → overall T → zero padding applied
//
// Exercised via printfFunc with %d format.
func TestMCDC_ApplyPadding_ZeroPadAndNotLeftAlign(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			// A=F: no zero-pad flag → space padding on left
			name:   "A_false_space_pad",
			format: "%5d",
			arg:    NewIntValue(42),
			want:   "   42",
		},
		{
			// A=T, B=F: zero-pad with left-align → left-align wins, spaces on right
			name:   "B_false_zero_and_left_align",
			format: "%-05d",
			arg:    NewIntValue(42),
			want:   "42   ",
		},
		{
			// A=T, B=T: zero-pad without left-align → zero padding
			name:   "all_true_zero_pad",
			format: "%05d",
			arg:    NewIntValue(42),
			want:   "00042",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("printfFunc(%q, %v) = %q, want %q", tt.format, tt.arg, result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_LikelihoodFunc_ProbRange covers:
//
//	Condition: prob < 0.0 || prob > 1.0   (inside the !args[1].IsNull() guard)
//	A = prob < 0.0
//	B = prob > 1.0
//
// MC/DC cases (N+1 = 3):
//
//	case "A_true":   A=T → overall T → error returned
//	case "B_true":   A=F, B=T → overall T, B independently determines outcome → error
//	case "all_false": A=F, B=F → overall F → no error, value passed through
func TestMCDC_LikelihoodFunc_ProbRange(t *testing.T) {
	tests := []struct {
		name    string
		args    []Value
		wantErr bool
	}{
		{
			// A=T: prob < 0.0 → error
			name:    "A_true_prob_negative",
			args:    []Value{NewIntValue(1), NewFloatValue(-0.1)},
			wantErr: true,
		},
		{
			// A=F, B=T: prob > 1.0 → error
			name:    "B_true_prob_over_one",
			args:    []Value{NewIntValue(1), NewFloatValue(1.1)},
			wantErr: true,
		},
		{
			// A=F, B=F: 0.0 <= prob <= 1.0 → no error
			name:    "all_false_prob_valid",
			args:    []Value{NewIntValue(1), NewFloatValue(0.5)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := likelihoodFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("likelihoodFunc() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_FormatPrintfHex_AltFormAndNonZero covers:
//
//	Condition: spec.altForm && val != 0
//	A = spec.altForm
//	B = val != 0
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → no "0x" prefix
//	case "B_false":  A=T, B=F (val==0) → overall F, B independently determines outcome → no prefix
//	case "all_true": A=T, B=T → overall T → "0x" prefix added
func TestMCDC_FormatPrintfHex_AltFormAndNonZero(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			// A=F: no alt-form flag → no "0x" prefix
			name:   "A_false_no_alt_form",
			format: "%x",
			arg:    NewIntValue(255),
			want:   "ff",
		},
		{
			// A=T, B=F: alt-form with zero → no "0x" prefix (zero special case)
			name:   "B_false_alt_form_zero",
			format: "%#x",
			arg:    NewIntValue(0),
			want:   "0",
		},
		{
			// A=T, B=T: alt-form with non-zero → "0x" prefix added
			name:   "all_true_alt_form_nonzero",
			format: "%#x",
			arg:    NewIntValue(255),
			want:   "0xff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("printfFunc(%q, %v) = %q, want %q", tt.format, tt.arg, result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_FormatPrintfOctal_AltFormAndNonZero covers:
//
//	Condition: spec.altForm && val != 0   (in formatPrintfOctal)
//	A = spec.altForm
//	B = val != 0
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → no "0" octal prefix
//	case "B_false":  A=T, B=F (val==0) → overall F → no prefix
//	case "all_true": A=T, B=T → overall T → "0" prefix added
func TestMCDC_FormatPrintfOctal_AltFormAndNonZero(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			// A=F: no alt-form → plain octal, no leading zero
			name:   "A_false_no_alt_form",
			format: "%o",
			arg:    NewIntValue(8),
			want:   "10",
		},
		{
			// A=T, B=F: alt-form with zero → "0" not prefixed (zero special case)
			name:   "B_false_alt_form_zero",
			format: "%#o",
			arg:    NewIntValue(0),
			want:   "0",
		},
		{
			// A=T, B=T: alt-form with non-zero → leading "0" added
			name:   "all_true_alt_form_nonzero",
			format: "%#o",
			arg:    NewIntValue(8),
			want:   "010",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("printfFunc(%q, %v) = %q, want %q", tt.format, tt.arg, result.AsString(), tt.want)
			}
		})
	}
}

// TestMCDC_HasSignPrefix covers:
//
//	Condition: len(s) > 0 && (s[0] == '-' || s[0] == '+' || s[0] == ' ')
//	A = len(s) > 0
//	B = s[0] == '-'
//	C = s[0] == '+'
//	D = s[0] == ' '
//
// MC/DC cases (N+1 = 5 for 4 sub-conditions, but the inner OR reduces to:
// effectively A && (B||C||D)).
// We cover the cases that independently flip the outcome:
//
//	case "A_false":        A=F → overall F (empty string)
//	case "B_true":         A=T, B=T → overall T (leading '-')
//	case "C_true":         A=T, B=F, C=T → overall T, C independently determines outcome
//	case "D_true":         A=T, B=F, C=F, D=T → overall T, D independently determines outcome
//	case "BCD_all_false":  A=T, B=F, C=F, D=F → overall F (non-sign first char)
//
// Exercised via applyZeroPadding which calls hasSignPrefix internally,
// observable through printfFunc with sign flags.
func TestMCDC_HasSignPrefix(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{
			// A=F: empty string → false
			name: "A_false_empty_string",
			s:    "",
			want: false,
		},
		{
			// A=T, B=T: starts with '-' → true
			name: "B_true_minus_sign",
			s:    "-42",
			want: true,
		},
		{
			// A=T, B=F, C=T: starts with '+' → true
			name: "C_true_plus_sign",
			s:    "+42",
			want: true,
		},
		{
			// A=T, B=F, C=F, D=T: starts with ' ' → true
			name: "D_true_space_sign",
			s:    " 42",
			want: true,
		},
		{
			// A=T, B=F, C=F, D=F: starts with digit → false
			name: "BCD_false_no_sign",
			s:    "42",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasSignPrefix(tt.s)
			if got != tt.want {
				t.Errorf("hasSignPrefix(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

// TestMCDC_FormatPrintfInteger_ThousandsAndNonZero covers:
//
//	Condition: spec.thousands && val != 0
//	A = spec.thousands
//	B = val != 0
//
// MC/DC cases (N+1 = 3):
//
//	case "A_false":  A=F → overall F → no thousands separator
//	case "B_false":  A=T, B=F (val==0) → overall F → no separator (zero is not formatted)
//	case "all_true": A=T, B=T → overall T → thousands separator added
func TestMCDC_FormatPrintfInteger_ThousandsAndNonZero(t *testing.T) {
	tests := []struct {
		name   string
		format string
		arg    Value
		want   string
	}{
		{
			// A=F: no thousands flag → plain number
			name:   "A_false_no_thousands",
			format: "%d",
			arg:    NewIntValue(1000000),
			want:   "1000000",
		},
		{
			// A=T, B=F: thousands flag with zero → no separator applied
			name:   "B_false_thousands_zero",
			format: "%,d",
			arg:    NewIntValue(0),
			want:   "0",
		},
		{
			// A=T, B=T: thousands flag with non-zero → separators added
			name:   "all_true_thousands_nonzero",
			format: "%,d",
			arg:    NewIntValue(1000000),
			want:   "1,000,000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := printfFunc([]Value{NewTextValue(tt.format), tt.arg})
			if err != nil {
				t.Fatalf("printfFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("printfFunc(%q, %v) = %q, want %q", tt.format, tt.arg, result.AsString(), tt.want)
			}
		})
	}
}
