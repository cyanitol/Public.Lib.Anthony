// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestMCDC_WriteRuneForIdent_LetterOrDigit covers MC/DC for
// the condition: unicode.IsLetter(r) || unicode.IsDigit(r)
// in writeRuneForIdent (emit_helpers.go:40).
//
// Condition table (L=IsLetter, D=IsDigit):
//
//	L=T, D=T → written (letter wins; digit column independent)
//	L=T, D=F → written (L alone is sufficient)
//	L=F, D=T → written (D alone is sufficient)
//	L=F, D=F → not written
func TestMCDC_WriteRuneForIdent_LetterOrDigit(t *testing.T) {
	tests := []struct {
		name    string
		r       rune
		pos     int
		written bool
	}{
		// L=T, D=F: plain letter at position 0
		{"MCDC_L1_D0_letter_pos0", 'a', 0, true},
		// L=T, D=F: plain letter at position > 0
		{"MCDC_L1_D0_letter_posN", 'z', 1, true},
		// L=F, D=T: digit at position 0
		{"MCDC_L0_D1_digit_pos0", '5', 0, true},
		// L=F, D=T: digit at position > 0
		{"MCDC_L0_D1_digit_posN", '9', 3, true},
		// L=F, D=F: punctuation (not letter, not digit, not separator)
		{"MCDC_L0_D0_punct_pos0", '!', 0, false},
		// L=F, D=F: punctuation at non-zero pos (also not a separator)
		{"MCDC_L0_D0_punct_posN", '!', 2, false},
		// L=T, D=T: digit-letter ambiguity — using a Unicode letter that is
		// also representable; standard ASCII letters are letters only, but
		// this row ensures coverage when both predicates could be true.
		// In practice Go's unicode.IsLetter and unicode.IsDigit are mutually
		// exclusive for all Unicode code points, so we use 'A' (L=T, D=F)
		// and '0' (L=F, D=T) to independently flip each operand.
		{"MCDC_L1_D0_upper_pos0", 'A', 0, true},
		{"MCDC_L0_D1_zero_pos0", '0', 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			writeRuneForIdent(&b, tc.r, tc.pos)
			got := b.Len() > 0
			if got != tc.written {
				t.Errorf("writeRuneForIdent(%q, %d): wrote=%v, want=%v",
					tc.r, tc.pos, got, tc.written)
			}
		})
	}
}

// TestMCDC_WriteRuneForIdent_SeparatorAndPos covers MC/DC for
// the condition: (r == '_' || r == '-' || r == ' ') && pos > 0
// in writeRuneForIdent (emit_helpers.go:42).
//
// Condition table (U=r=='_', H=r=='-', S=r==' ', P=pos>0):
// The outer AND means both halves must be independently varied.
//
//	(U||H||S)=T, P=T → underscore written
//	(U||H||S)=T, P=F → nothing written (pos==0 blocks it)
//	(U||H||S)=F, P=T → nothing written (not a separator)
//
// Within the inner OR (U||H||S):
//
//	U=T, H=F, S=F → separator (underscore)
//	U=F, H=T, S=F → separator (hyphen)
//	U=F, H=F, S=T → separator (space)
//	U=F, H=F, S=F → not a separator
func TestMCDC_WriteRuneForIdent_SeparatorAndPos(t *testing.T) {
	tests := []struct {
		name    string
		r       rune
		pos     int
		wantOut string // expected builder output
	}{
		// Outer AND: right operand (pos>0) independently determines outcome
		// (U||H||S)=T, P=T → '_' written
		{"MCDC_sep_underscore_posN", '_', 1, "_"},
		// (U||H||S)=T, P=F → nothing (pos==0)
		{"MCDC_sep_underscore_pos0", '_', 0, ""},
		// (U||H||S)=F, P=T → nothing (not a separator rune)
		{"MCDC_nosep_posN", '!', 2, ""},

		// Inner OR — each separator independently enables the left half
		// U=T, H=F, S=F: underscore at pos>0
		{"MCDC_inner_U1_H0_S0", '_', 3, "_"},
		// U=F, H=T, S=F: hyphen at pos>0
		{"MCDC_inner_U0_H1_S0", '-', 3, "_"},
		// U=F, H=F, S=T: space at pos>0
		{"MCDC_inner_U0_H0_S1", ' ', 3, "_"},
		// U=F, H=F, S=F: non-separator non-letter non-digit at pos>0
		{"MCDC_inner_U0_H0_S0", '@', 3, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			writeRuneForIdent(&b, tc.r, tc.pos)
			if b.String() != tc.wantOut {
				t.Errorf("writeRuneForIdent(%q, %d): got %q, want %q",
					tc.r, tc.pos, b.String(), tc.wantOut)
			}
		})
	}
}

// TestMCDC_WriteImports_FlagCombinations covers MC/DC for the import flags
// in writeImports (emit.go): NeedsFmt, NeedsMath, NeedsString each
// independently influence the import block, and the single-import branch
// fires when exactly none of the three extra flags are set.
//
// Condition: len(imports)==1  ↔  !NeedsFmt && !NeedsMath && !NeedsString
func TestMCDC_WriteImports_FlagCombinations(t *testing.T) {
	tests := []struct {
		name         string
		spec         ModuleSpec
		wantFmt      bool
		wantMath     bool
		wantStrings  bool
		wantSingleIm bool // single-import (no parens) form
	}{
		{
			name:         "MCDC_flags_none",
			spec:         ModuleSpec{},
			wantSingleIm: true,
		},
		{
			name:         "MCDC_flags_fmt_only",
			spec:         ModuleSpec{NeedsFmt: true},
			wantFmt:      true,
			wantSingleIm: false,
		},
		{
			name:         "MCDC_flags_math_only",
			spec:         ModuleSpec{NeedsMath: true},
			wantMath:     true,
			wantSingleIm: false,
		},
		{
			name:         "MCDC_flags_strings_only",
			spec:         ModuleSpec{NeedsString: true},
			wantStrings:  true,
			wantSingleIm: false,
		},
		{
			name:         "MCDC_flags_all",
			spec:         ModuleSpec{NeedsFmt: true, NeedsMath: true, NeedsString: true},
			wantFmt:      true,
			wantMath:     true,
			wantStrings:  true,
			wantSingleIm: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeImports(&buf, tc.spec)
			out := buf.String()

			hasFmt := strings.Contains(out, `"fmt"`)
			hasMath := strings.Contains(out, `"math"`)
			hasStrings := strings.Contains(out, `"strings"`)
			singleIm := !strings.Contains(out, "import (")

			if hasFmt != tc.wantFmt {
				t.Errorf("fmt import: got %v, want %v\noutput: %s", hasFmt, tc.wantFmt, out)
			}
			if hasMath != tc.wantMath {
				t.Errorf("math import: got %v, want %v\noutput: %s", hasMath, tc.wantMath, out)
			}
			if hasStrings != tc.wantStrings {
				t.Errorf("strings import: got %v, want %v\noutput: %s", hasStrings, tc.wantStrings, out)
			}
			if singleIm != tc.wantSingleIm {
				t.Errorf("single-import form: got %v, want %v\noutput: %s", singleIm, tc.wantSingleIm, out)
			}
		})
	}
}

// TestMCDC_WriteBuildFunc_SubFuncsVsInline covers MC/DC for the branch
// len(spec.SubFuncs) > 0 in writeBuildFunc (emit.go:79).
//
// Condition: hasSubFuncs = len(SubFuncs) > 0
//
//	hasSubFuncs=T → appends sub-function calls
//	hasSubFuncs=F → emits inline test cases
func TestMCDC_WriteBuildFunc_SubFuncsVsInline(t *testing.T) {
	tests := []struct {
		name       string
		spec       ModuleSpec
		wantAppend bool // "append(tests" appears in output
		wantInline bool // "sqlTestCase{" or empty slice form appears
	}{
		{
			name: "MCDC_subfuncs_nonempty",
			spec: ModuleSpec{
				BuildFunc: "buildFoo",
				SubFuncs:  []SubFunc{{Name: "genFooA"}, {Name: "genFooB"}},
			},
			wantAppend: true,
			wantInline: false,
		},
		{
			name: "MCDC_subfuncs_empty_no_tests",
			spec: ModuleSpec{
				BuildFunc: "buildBar",
				SubFuncs:  nil,
			},
			wantAppend: false,
			wantInline: true,
		},
		{
			name: "MCDC_subfuncs_empty_with_inline_tests",
			spec: ModuleSpec{
				BuildFunc: "buildBaz",
				SubFuncs:  nil,
				Tests: []TestSpec{
					{Name: "t1", Query: "SELECT 1"},
				},
			},
			wantAppend: false,
			wantInline: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeBuildFunc(&buf, tc.spec)
			out := buf.String()

			hasAppend := strings.Contains(out, "append(tests")
			hasInline := strings.Contains(out, "[]sqlTestCase{")

			if hasAppend != tc.wantAppend {
				t.Errorf("append present: got %v, want %v\noutput: %s", hasAppend, tc.wantAppend, out)
			}
			if hasInline != tc.wantInline {
				t.Errorf("inline slice present: got %v, want %v\noutput: %s", hasInline, tc.wantInline, out)
			}
		})
	}
}

// TestMCDC_EmitFlags covers MC/DC for the three independent boolean/string
// conditions in emitFlags (emit.go:146-156):
//
//	WantErr bool  — tc.WantErr
//	ErrLike string — tc.ErrLike != ""
//	Skip    string — tc.Skip != ""
func TestMCDC_EmitFlags(t *testing.T) {
	tests := []struct {
		name     string
		tc       TestSpec
		wantErr  bool
		wantLike bool
		wantSkip bool
	}{
		{"MCDC_flags_none", TestSpec{}, false, false, false},
		{"MCDC_flags_wantErr_only", TestSpec{WantErr: true}, true, false, false},
		{"MCDC_flags_errLike_only", TestSpec{ErrLike: "syntax"}, false, true, false},
		{"MCDC_flags_skip_only", TestSpec{Skip: "reason"}, false, false, true},
		{"MCDC_flags_all", TestSpec{WantErr: true, ErrLike: "x", Skip: "y"}, true, true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			emitFlags(&buf, tc.tc)
			out := buf.String()

			hasErr := strings.Contains(out, "wantErr: true")
			hasLike := strings.Contains(out, "errLike:")
			hasSkip := strings.Contains(out, "skip:")

			if hasErr != tc.wantErr {
				t.Errorf("wantErr flag: got %v, want %v\noutput: %s", hasErr, tc.wantErr, out)
			}
			if hasLike != tc.wantLike {
				t.Errorf("errLike flag: got %v, want %v\noutput: %s", hasLike, tc.wantLike, out)
			}
			if hasSkip != tc.wantSkip {
				t.Errorf("skip flag: got %v, want %v\noutput: %s", hasSkip, tc.wantSkip, out)
			}
		})
	}
}

// TestMCDC_Run_ModuleVsAll covers MC/DC for the condition
// module != "" in run (main.go:35).
//
//	module != "" → generateModule path (unknown module returns error)
//	module == "" → generateAll path (succeeds with temp dir)
func TestMCDC_Run_ModuleVsAll(t *testing.T) {
	t.Run("MCDC_run_module_nonempty_unknown", func(t *testing.T) {
		err := run("no_such_module_xyz", t.TempDir(), true)
		if err == nil {
			t.Fatal("expected error for unknown module, got nil")
		}
	})

	t.Run("MCDC_run_module_empty_all", func(t *testing.T) {
		err := run("", t.TempDir(), true)
		if err != nil {
			t.Fatalf("expected no error for generateAll dry-run, got: %v", err)
		}
	})
}

// TestMCDC_FinalizeSanitizedName covers MC/DC for the two conditions in
// finalizeSanitizedName (emit_helpers.go:48-55):
//
//	Condition A: len(result) == 0
//	Condition B: unicode.IsDigit(rune(result[0]))  (only evaluated when A=F)
func TestMCDC_FinalizeSanitizedName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// A=T: empty string → "unnamed"
		{"MCDC_empty_input", "", "unnamed"},
		// A=F, B=T: starts with digit → "t_" prefix
		{"MCDC_digit_prefix", "3abc", "t_3abc"},
		// A=F, B=F: starts with letter → unchanged
		{"MCDC_letter_prefix", "abc", "abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := finalizeSanitizedName(tc.input)
			if got != tc.want {
				t.Errorf("finalizeSanitizedName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
