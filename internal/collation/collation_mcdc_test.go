// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package collation

import (
	"testing"
)

// TestMCDC_Unregister_BuiltinProtection covers the compound boolean condition
// in (*CollationRegistry).Unregister at line 115:
//
//	if name == "BINARY" || name == "NOCASE" || name == "RTRIM"
//
// Let:
//
//	A = (name == "BINARY")
//	B = (name == "NOCASE")
//	C = (name == "RTRIM")
//
// Overall predicate: A || B || C
//
// MC/DC requires N+1 = 4 test cases (N=3 sub-conditions), one per sub-condition
// that independently changes the outcome, plus a baseline where all are false.
//
// Truth table:
//
//	Case | A | B | C | Result | Outcome-changing condition
//	-----|---|---|---|--------|---------------------------
//	1    | F | F | F | false  | baseline (all false → no error)
//	2    | T | F | F | true   | A independently flips outcome
//	3    | F | T | F | true   | B independently flips outcome
//	4    | F | F | T | true   | C independently flips outcome
func TestMCDC_Unregister_BuiltinProtection(t *testing.T) {
	t.Parallel()

	// Register a custom collation we can freely unregister for the baseline case.
	customName := "MCDC_CUSTOM"

	tests := []struct {
		name      string // descriptive MC/DC label
		colName   string // collation name to pass to Unregister
		wantErr   bool   // whether Unregister should return an error
		needSetup bool   // whether we need to pre-register colName
	}{
		// Case 1: A=F B=F C=F → predicate false → no error
		{
			name:      "A=F B=F C=F: custom collation not protected, expect no error",
			colName:   customName,
			wantErr:   false,
			needSetup: true,
		},
		// Case 2: A=T B=F C=F → predicate true → error (A flips outcome alone)
		{
			name:      "A=T B=F C=F: name==BINARY, expect error",
			colName:   "BINARY",
			wantErr:   true,
			needSetup: false,
		},
		// Case 3: A=F B=T C=F → predicate true → error (B flips outcome alone)
		{
			name:      "A=F B=T C=F: name==NOCASE, expect error",
			colName:   "NOCASE",
			wantErr:   true,
			needSetup: false,
		},
		// Case 4: A=F B=F C=T → predicate true → error (C flips outcome alone)
		{
			name:      "A=F B=F C=T: name==RTRIM, expect error",
			colName:   "RTRIM",
			wantErr:   true,
			needSetup: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cr := NewCollationRegistry()
			if tt.needSetup {
				if err := cr.Register(tt.colName, func(a, b string) int { return 0 }); err != nil {
					t.Fatalf("setup: Register(%q) error = %v", tt.colName, err)
				}
			}
			err := cr.Unregister(tt.colName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unregister(%q) error = %v, wantErr %v", tt.colName, err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_Register_Guards covers the two independent single-condition guards
// in (*CollationRegistry).Register:
//
//	if name == ""   → return error
//	if fn == nil    → return error
//
// These are not compound (they are sequential independent checks), so MC/DC
// requires one true and one false case per condition (2 cases each).
//
// Condition P: name == ""
//
//	P=T → error returned
//	P=F → no error from this guard (fn must also be valid)
//
// Condition Q: fn == nil
//
//	Q=T → error returned (name must be non-empty to reach this check)
//	Q=F → no error from this guard (name must also be non-empty)
func TestMCDC_Register_Guards(t *testing.T) {
	t.Parallel()

	validFn := func(a, b string) int { return 0 }

	tests := []struct {
		name    string
		colName string
		fn      CollationFunc
		wantErr bool
	}{
		// Condition P (name == ""):
		// P=T: empty name → error
		{
			name:    "P=T: name empty, expect error",
			colName: "",
			fn:      validFn,
			wantErr: true,
		},
		// P=F, Q=F: non-empty name, valid fn → no error
		{
			name:    "P=F Q=F: non-empty name and valid fn, expect no error",
			colName: "MCDC_VALID",
			fn:      validFn,
			wantErr: false,
		},
		// Condition Q (fn == nil):
		// Q=T: nil fn, non-empty name → error
		{
			name:    "Q=T: nil fn with valid name, expect error",
			colName: "MCDC_NIL",
			fn:      nil,
			wantErr: true,
		},
		// Q=F covered by the P=F Q=F case above.
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cr := NewCollationRegistry()
			err := cr.Register(tt.colName, tt.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Register(%q, fn) error = %v, wantErr %v", tt.colName, err, tt.wantErr)
			}
		})
	}
}

// TestMCDC_Compare_EmptyCollationName covers the single-condition guard
// in Compare at line 159:
//
//	if collationName == ""
//
// Condition R: collationName == ""
//
//	R=T: empty string → name is replaced with "BINARY" → comparison uses BINARY semantics
//	R=F: non-empty string → name is used as-is
func TestMCDC_Compare_EmptyCollationName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		a, b          string
		collationName string
		// wantSign: -1 means result<0, 0 means result==0, +1 means result>0
		wantSign int
	}{
		// R=T: empty collation name → falls back to BINARY → case-sensitive
		// "A" < "a" in BINARY (ASCII 65 < 97)
		{
			name:          "R=T: collationName empty, expect BINARY fallback (case-sensitive)",
			a:             "A",
			b:             "a",
			collationName: "",
			wantSign:      -1,
		},
		// R=F: non-empty collation name provided (NOCASE) → case-insensitive
		// "A" == "a" under NOCASE
		{
			name:          "R=F: collationName=NOCASE, expect case-insensitive equality",
			a:             "A",
			b:             "a",
			collationName: "NOCASE",
			wantSign:      0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Compare(tt.a, tt.b, tt.collationName)
			var gotSign int
			switch {
			case result < 0:
				gotSign = -1
			case result > 0:
				gotSign = 1
			default:
				gotSign = 0
			}
			if gotSign != tt.wantSign {
				t.Errorf("Compare(%q, %q, %q) sign = %d, want %d (raw result = %d)",
					tt.a, tt.b, tt.collationName, gotSign, tt.wantSign, result)
			}
		})
	}
}

// TestMCDC_Compare_UnknownCollationFallback covers the single-condition guard
// in Compare at line 164:
//
//	if !ok  (collation not found in registry)
//
// Condition S: !ok (GetCollation returned ok=false)
//
//	S=T: unknown collation name → falls back to BINARY
//	S=F: known collation name → uses that collation
func TestMCDC_Compare_UnknownCollationFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		a, b          string
		collationName string
		wantSign      int
	}{
		// S=T: unknown collation → BINARY fallback → case-sensitive
		// "A" < "a" in BINARY
		{
			name:          "S=T: unknown collation, expect BINARY fallback",
			a:             "A",
			b:             "a",
			collationName: "DOES_NOT_EXIST",
			wantSign:      -1,
		},
		// S=F: known collation NOCASE → case-insensitive, "A"=="a"
		{
			name:          "S=F: known collation NOCASE, expect case-insensitive equality",
			a:             "A",
			b:             "a",
			collationName: "NOCASE",
			wantSign:      0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Compare(tt.a, tt.b, tt.collationName)
			var gotSign int
			switch {
			case result < 0:
				gotSign = -1
			case result > 0:
				gotSign = 1
			default:
				gotSign = 0
			}
			if gotSign != tt.wantSign {
				t.Errorf("Compare(%q, %q, %q) sign = %d, want %d (raw result = %d)",
					tt.a, tt.b, tt.collationName, gotSign, tt.wantSign, result)
			}
		})
	}
}

// TestMCDC_GetCollationFunc_NotFound covers the single-condition guard
// in GetCollationFunc at line 181:
//
//	if !ok
//
// Condition T: !ok (GetCollation returned ok=false)
//
//	T=T: unknown name → return nil
//	T=F: known name → return non-nil function
func TestMCDC_GetCollationFunc_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		colName string
		wantNil bool
	}{
		// T=T: not found → nil returned
		{
			name:    "T=T: unknown collation, expect nil",
			colName: "NONEXISTENT",
			wantNil: true,
		},
		// T=F: found → non-nil function returned
		{
			name:    "T=F: known collation BINARY, expect non-nil",
			colName: "BINARY",
			wantNil: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fn := GetCollationFunc(tt.colName)
			if (fn == nil) != tt.wantNil {
				t.Errorf("GetCollationFunc(%q) nil = %v, wantNil %v", tt.colName, fn == nil, tt.wantNil)
			}
		})
	}
}
