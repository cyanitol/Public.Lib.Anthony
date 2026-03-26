// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vtab

import (
	"testing"
)

// TestMCDC_HasOrderBy exercises the single-condition guard in HasOrderBy.
//
// Condition: len(info.OrderBy) > 0
//
// Although this is a single-condition (not a compound), it is documented here
// for completeness and to confirm the two-outcome paths are both reachable.
//
// Cases:
//
//	A: no OrderBy entries  → false
//	B: one OrderBy entry   → true  (flips outcome)
func TestMCDC_HasOrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		orderBy []OrderBy
		want    bool
	}{
		// A: empty → false
		{
			name:    "MCDC_HasOrderBy_empty_returns_false",
			orderBy: []OrderBy{},
			want:    false,
		},
		// B: one entry → true (flips outcome)
		{
			name:    "MCDC_HasOrderBy_one_entry_returns_true",
			orderBy: []OrderBy{{Column: 0, Desc: false}},
			want:    true,
		},
		// Extra: multiple entries still true
		{
			name: "MCDC_HasOrderBy_two_entries_returns_true",
			orderBy: []OrderBy{
				{Column: 0, Desc: false},
				{Column: 1, Desc: true},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := &IndexInfo{OrderBy: tt.orderBy}
			got := info.HasOrderBy()
			if got != tt.want {
				t.Errorf("HasOrderBy() = %v, want %v (len=%d)", got, tt.want, len(tt.orderBy))
			}
		})
	}
}

// TestMCDC_CountUsableConstraints exercises the single sub-condition in CountUsableConstraints.
//
// Condition: c.Usable
//
// Cases:
//
//	A: Usable=true  → counted
//	B: Usable=false → not counted  (flips contribution)
func TestMCDC_CountUsableConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		constraints []IndexConstraint
		wantCount   int
	}{
		// All usable
		{
			name: "MCDC_Count_all_usable",
			constraints: []IndexConstraint{
				{Column: 0, Op: ConstraintEQ, Usable: true},
				{Column: 1, Op: ConstraintGT, Usable: true},
			},
			wantCount: 2,
		},
		// None usable: A=false flips outcome per slot
		{
			name: "MCDC_Count_none_usable",
			constraints: []IndexConstraint{
				{Column: 0, Op: ConstraintEQ, Usable: false},
				{Column: 1, Op: ConstraintGT, Usable: false},
			},
			wantCount: 0,
		},
		// Mixed: first usable (A=true), second not (A=false) → count 1
		{
			name: "MCDC_Count_mixed_first_usable",
			constraints: []IndexConstraint{
				{Column: 0, Op: ConstraintEQ, Usable: true},
				{Column: 1, Op: ConstraintGT, Usable: false},
			},
			wantCount: 1,
		},
		// Mixed: second usable, first not → count 1
		{
			name: "MCDC_Count_mixed_second_usable",
			constraints: []IndexConstraint{
				{Column: 0, Op: ConstraintEQ, Usable: false},
				{Column: 1, Op: ConstraintGT, Usable: true},
			},
			wantCount: 1,
		},
		// Empty → 0
		{
			name:        "MCDC_Count_empty",
			constraints: []IndexConstraint{},
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := &IndexInfo{
				Constraints:     tt.constraints,
				ConstraintUsage: make([]IndexConstraintUsage, len(tt.constraints)),
			}
			got := info.CountUsableConstraints()
			if got != tt.wantCount {
				t.Errorf("CountUsableConstraints() = %d, want %d", got, tt.wantCount)
			}
		})
	}
}

// TestMCDC_ConstraintOpString exercises the compound map-lookup in ConstraintOp.String().
//
// Condition: if str, ok := constraintOpStrings[op]; ok
//
// Although this is a single type assertion (ok boolean), it has two distinct paths:
//
//	A: ok=true  → returns known string
//	B: ok=false → returns "UNKNOWN"
func TestMCDC_ConstraintOpString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   ConstraintOp
		want string
	}{
		// A=true (op is in map)
		{name: "MCDC_OpString_known_EQ", op: ConstraintEQ, want: "="},
		{name: "MCDC_OpString_known_GT", op: ConstraintGT, want: ">"},
		{name: "MCDC_OpString_known_LE", op: ConstraintLE, want: "<="},
		{name: "MCDC_OpString_known_IS", op: ConstraintIs, want: "IS"},
		{name: "MCDC_OpString_known_LIMIT", op: ConstraintLimit, want: "LIMIT"},
		{name: "MCDC_OpString_known_OFFSET", op: ConstraintOffset, want: "OFFSET"},
		// A=false (op NOT in map → UNKNOWN)
		{name: "MCDC_OpString_unknown_value", op: ConstraintOp(999), want: "UNKNOWN"},
		{name: "MCDC_OpString_function_not_in_map", op: ConstraintFunction, want: "UNKNOWN"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.op.String()
			if got != tt.want {
				t.Errorf("ConstraintOp(%d).String() = %q, want %q", int(tt.op), got, tt.want)
			}
		})
	}
}

// TestMCDC_BaseCursor_EOF exercises the single flag condition in BaseCursor.EOF().
//
// Condition: bc.eof
//
// Cases:
//
//	A: eof=false → returns false
//	B: eof=true  → returns true  (flips outcome)
func TestMCDC_BaseCursor_EOF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		eofFlag bool
		want    bool
	}{
		// A: flag false → not EOF
		{name: "MCDC_BaseCursor_eof_false", eofFlag: false, want: false},
		// B: flag true → EOF  (flips outcome)
		{name: "MCDC_BaseCursor_eof_true", eofFlag: true, want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bc := &BaseCursor{eof: tt.eofFlag}
			got := bc.EOF()
			if got != tt.want {
				t.Errorf("BaseCursor.EOF() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMCDC_testCursorColumn exercises the compound guard in testCursor.Column.
//
// Condition: index < 0 || index >= len(c.rows[c.pos])
//
// The cursor must not be at EOF (pos is valid).
//
// MC/DC cases:
//
//	A=true,  B=false → index=-1  (< 0 short-circuits; flips outcome)
//	A=false, B=true  → index=2, row has 2 cols (>= len; flips outcome)
//	A=false, B=false → index=0 or 1 (valid; returns value)
func TestMCDC_testCursorColumn(t *testing.T) {
	t.Parallel()

	rows := [][]interface{}{
		{int64(1), "Alice"},
	}

	tests := []struct {
		name      string
		pos       int
		colIndex  int
		wantNil   bool // true when Column returns nil (out-of-range path)
		wantValue interface{}
	}{
		// A=true: index<0 short-circuits → nil returned
		{
			name: "MCDC_Column_A1_B0_index_negative",
			pos:  0, colIndex: -1,
			wantNil: true,
		},
		// A=false, B=true: index==len (len=2, index=2) → nil returned
		{
			name: "MCDC_Column_A0_B1_index_equals_len",
			pos:  0, colIndex: 2,
			wantNil: true,
		},
		// A=false, B=false: index=0 → valid
		{
			name: "MCDC_Column_A0_B0_index_zero",
			pos:  0, colIndex: 0,
			wantNil:   false,
			wantValue: int64(1),
		},
		// A=false, B=false: index=1 → valid
		{
			name: "MCDC_Column_A0_B0_index_one",
			pos:  0, colIndex: 1,
			wantNil:   false,
			wantValue: "Alice",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &testCursor{rows: rows, pos: tt.pos}
			got, err := c.Column(tt.colIndex)
			if err != nil {
				t.Fatalf("Column(%d) unexpected error: %v", tt.colIndex, err)
			}
			if tt.wantNil {
				if got != nil {
					t.Errorf("Column(%d) = %v, want nil", tt.colIndex, got)
				}
			} else {
				if got != tt.wantValue {
					t.Errorf("Column(%d) = %v, want %v", tt.colIndex, got, tt.wantValue)
				}
			}
		})
	}
}

// TestMCDC_testCursorEOF_Column_EOFGuard exercises the EOF guard inside Column.
//
// The Column method first checks c.EOF() and returns nil early when at EOF.
// This is distinct from the column-index range check tested above.
//
// Cases:
//
//	A: cursor at EOF   → Column returns nil (early return)
//	B: cursor not EOF  → Column proceeds to index check
func TestMCDC_testCursorEOF_Column_EOFGuard(t *testing.T) {
	t.Parallel()

	rows := [][]interface{}{
		{int64(42), "Bob"},
	}

	tests := []struct {
		name    string
		pos     int
		wantNil bool
	}{
		// A: pos=-1 → EOF → Column returns nil immediately
		{name: "MCDC_Column_EOFGuard_at_eof_returns_nil", pos: -1, wantNil: true},
		// B: pos=0 → not EOF → Column proceeds normally
		{name: "MCDC_Column_EOFGuard_not_eof_proceeds", pos: 0, wantNil: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &testCursor{rows: rows, pos: tt.pos}
			got, _ := c.Column(0)
			if tt.wantNil && got != nil {
				t.Errorf("Column(0) = %v, want nil when at EOF", got)
			}
			if !tt.wantNil && got == nil {
				t.Errorf("Column(0) returned nil when not at EOF")
			}
		})
	}
}

// TestMCDC_RegisterModule_DuplicateGuard exercises the duplicate-registration guard
// in ModuleRegistry.RegisterModule.
//
// Condition: _, exists := r.modules[name]; exists
//
// Cases:
//
//	A: exists=false → registration succeeds (first time)
//	B: exists=true  → returns error (duplicate)  (flips outcome)
func TestMCDC_RegisterModule_DuplicateGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		preFill bool // whether to register the module first
		wantErr bool
	}{
		// A: not pre-filled → no conflict → nil error
		{
			name:    "MCDC_Register_A0_no_conflict",
			preFill: false,
			wantErr: false,
		},
		// B: pre-filled → conflict → error  (flips outcome)
		{
			name:    "MCDC_Register_B1_duplicate_conflict",
			preFill: true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reg := NewModuleRegistry()
			mod := &testModule{name: "dup_test"}
			if tt.preFill {
				if err := reg.RegisterModule("mymod", mod); err != nil {
					t.Fatalf("pre-fill registration failed: %v", err)
				}
			}
			err := reg.RegisterModule("mymod", mod)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for duplicate registration, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC_UnregisterModule_NotFoundGuard exercises the not-found guard
// in ModuleRegistry.UnregisterModule.
//
// Condition: _, exists := r.modules[name]; !exists
//
// Cases:
//
//	A: exists=true  → module found → unregistered successfully
//	B: exists=false → module not found → returns error  (flips outcome)
func TestMCDC_UnregisterModule_NotFoundGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		preRegister bool
		wantErr     bool
	}{
		// A: pre-registered → found → success
		{
			name:        "MCDC_Unregister_A1_found_succeeds",
			preRegister: true,
			wantErr:     false,
		},
		// B: not registered → not found → error  (flips outcome)
		{
			name:        "MCDC_Unregister_B0_not_found_errors",
			preRegister: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reg := NewModuleRegistry()
			mod := &testModule{name: "unreg_test"}
			if tt.preRegister {
				if err := reg.RegisterModule("mymod", mod); err != nil {
					t.Fatalf("registration setup failed: %v", err)
				}
			}
			err := reg.UnregisterModule("mymod")
			if tt.wantErr && err == nil {
				t.Errorf("expected error for missing module, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
