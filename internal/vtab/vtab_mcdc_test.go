// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vtab

import (
	"testing"
)

// TestMCDC_FindConstraint exercises the compound boolean condition in FindConstraint.
//
// Condition: c.Column == column && c.Op == op && c.Usable
//
// MC/DC requires each sub-condition to independently flip the overall outcome.
// The outcome is "return the index i" (true path) vs "continue" (false path).
//
// Cases:
//
//	A: Column matches, Op matches, Usable=true  → found (all true)
//	B: Column wrong,  Op matches, Usable=true  → not found (A flips outcome, B&C fixed true)
//	C: Column matches, Op wrong,  Usable=true  → not found (B flips outcome, A&C fixed true)
//	D: Column matches, Op matches, Usable=false → not found (C flips outcome, A&B fixed true)
func TestMCDC_FindConstraint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		constraints []IndexConstraint
		searchCol   int
		searchOp    ConstraintOp
		wantIdx     int
		// MC/DC condition documentation
		colMatch bool
		opMatch  bool
		usable   bool
	}{
		{
			// All three sub-conditions true → constraint found
			name: "MCDC_all_true_found",
			constraints: []IndexConstraint{
				{Column: 2, Op: ConstraintEQ, Usable: true},
			},
			searchCol: 2, searchOp: ConstraintEQ,
			wantIdx:  0,
			colMatch: true, opMatch: true, usable: true,
		},
		{
			// Column mismatch flips outcome (column=false, op=true, usable=true → not found)
			name: "MCDC_column_mismatch_flips_outcome",
			constraints: []IndexConstraint{
				{Column: 99, Op: ConstraintEQ, Usable: true},
			},
			searchCol: 2, searchOp: ConstraintEQ,
			wantIdx:  -1,
			colMatch: false, opMatch: true, usable: true,
		},
		{
			// Op mismatch flips outcome (column=true, op=false, usable=true → not found)
			name: "MCDC_op_mismatch_flips_outcome",
			constraints: []IndexConstraint{
				{Column: 2, Op: ConstraintGT, Usable: true},
			},
			searchCol: 2, searchOp: ConstraintEQ,
			wantIdx:  -1,
			colMatch: true, opMatch: false, usable: true,
		},
		{
			// Usable=false flips outcome (column=true, op=true, usable=false → not found)
			name: "MCDC_unusable_flips_outcome",
			constraints: []IndexConstraint{
				{Column: 2, Op: ConstraintEQ, Usable: false},
			},
			searchCol: 2, searchOp: ConstraintEQ,
			wantIdx:  -1,
			colMatch: true, opMatch: true, usable: false,
		},
		{
			// No constraints at all → not found
			name:        "MCDC_empty_constraints",
			constraints: []IndexConstraint{},
			searchCol:   0, searchOp: ConstraintEQ,
			wantIdx: -1,
		},
		{
			// Second constraint matches, first does not
			name: "MCDC_second_matches",
			constraints: []IndexConstraint{
				{Column: 0, Op: ConstraintGT, Usable: true},
				{Column: 3, Op: ConstraintLE, Usable: true},
			},
			searchCol: 3, searchOp: ConstraintLE,
			wantIdx: 1,
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
			got := info.FindConstraint(tt.searchCol, tt.searchOp)
			if got != tt.wantIdx {
				t.Errorf("FindConstraint(%d, %v) = %d, want %d",
					tt.searchCol, tt.searchOp, got, tt.wantIdx)
			}
		})
	}
}

// TestMCDC_IsColumnUsed exercises two compound boolean conditions inside IsColumnUsed.
//
// Condition 1 (out-of-range branch): column < 0 || column >= 64
//
//	A: column=-1  → OOB (column<0 true, short-circuits)
//	B: column=64  → OOB (column<0 false, column>=64 true, flips outcome vs C)
//	C: column=0   → in-range (both false)
//
// Condition 2 (large-column inner): column >= 64
//
//	D: column=64  → uses bit 63 path (true)
//	E: column=0   → normal path (false, independent from Condition 1 context)
//
// Condition 3 (bit-mask check): (info.ColUsed & (1 << uint(column))) != 0
//
//	F: bit set     → true
//	G: bit not set → false
func TestMCDC_IsColumnUsed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		colUsed uint64
		column  int
		want    bool
	}{
		// Condition 1 sub-A: column<0 → immediate false (negative index)
		{
			name:    "MCDC_col_negative_returns_false",
			colUsed: 0xFF, column: -1, want: false,
		},
		// Condition 1 sub-B: column>=64, inner condition true → checks bit 63
		{
			name:    "MCDC_col_64_bit63_set_returns_true",
			colUsed: 1 << 63, column: 64, want: true,
		},
		// Condition 1 sub-B: column>=64, bit 63 NOT set → returns false
		{
			name:    "MCDC_col_64_bit63_clear_returns_false",
			colUsed: 0x01, column: 64, want: false,
		},
		// Condition 1 sub-C: column in [0,63], both range-checks false
		// Condition 3 sub-F: bit set
		{
			name:    "MCDC_col_in_range_bit_set",
			colUsed: 0x04, column: 2, want: true,
		},
		// Condition 3 sub-G: bit not set (flips outcome from sub-F)
		{
			name:    "MCDC_col_in_range_bit_clear",
			colUsed: 0x04, column: 3, want: false,
		},
		// Condition 2: column exactly 63 (boundary, bit 63 is the normal path)
		{
			name:    "MCDC_col_63_bit63_set",
			colUsed: 1 << 63, column: 63, want: true,
		},
		// column=63 but bit 63 clear
		{
			name:    "MCDC_col_63_bit63_clear",
			colUsed: 0x00, column: 63, want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := &IndexInfo{ColUsed: tt.colUsed}
			got := info.IsColumnUsed(tt.column)
			if got != tt.want {
				t.Errorf("IsColumnUsed(col=%d, ColUsed=0x%X) = %v, want %v",
					tt.column, tt.colUsed, got, tt.want)
			}
		})
	}
}

// TestMCDC_SetConstraintUsage exercises the compound guard in SetConstraintUsage.
//
// Condition: index >= 0 && index < len(info.ConstraintUsage)
//
// MC/DC cases:
//
//	A: index=-1  → index>=0 false (short-circuits; flips outcome)
//	B: index=5   → index>=0 true, index<len false (len=3; flips outcome vs C)
//	C: index=1   → both true (writes to slot)
func TestMCDC_SetConstraintUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numSlots  int
		index     int
		argvIndex int
		omit      bool
		wantWrite bool // whether we expect the slot to be written
	}{
		{
			// A: index<0 flips outcome
			name:     "MCDC_negative_index_no_write",
			numSlots: 3, index: -1, argvIndex: 7, omit: true,
			wantWrite: false,
		},
		{
			// B: index>=len flips outcome
			name:     "MCDC_index_beyond_len_no_write",
			numSlots: 3, index: 5, argvIndex: 7, omit: true,
			wantWrite: false,
		},
		{
			// C: both conditions true → writes
			name:     "MCDC_valid_index_writes",
			numSlots: 3, index: 1, argvIndex: 7, omit: true,
			wantWrite: true,
		},
		{
			// Edge: index==len (equal to length, not less than)
			name:     "MCDC_index_equals_len_no_write",
			numSlots: 3, index: 3, argvIndex: 7, omit: true,
			wantWrite: false,
		},
		{
			// Edge: index==0 (minimum valid)
			name:     "MCDC_index_zero_writes",
			numSlots: 3, index: 0, argvIndex: 2, omit: false,
			wantWrite: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := NewIndexInfo(tt.numSlots)
			info.SetConstraintUsage(tt.index, tt.argvIndex, tt.omit)

			if tt.wantWrite {
				if tt.index < 0 || tt.index >= tt.numSlots {
					t.Fatalf("test setup error: wantWrite=true but index %d out of range", tt.index)
				}
				usage := info.ConstraintUsage[tt.index]
				if usage.ArgvIndex != tt.argvIndex {
					t.Errorf("ArgvIndex = %d, want %d", usage.ArgvIndex, tt.argvIndex)
				}
				if usage.Omit != tt.omit {
					t.Errorf("Omit = %v, want %v", usage.Omit, tt.omit)
				}
			} else {
				// Verify none of the slots were written unexpectedly
				for i := 0; i < tt.numSlots; i++ {
					if info.ConstraintUsage[i].ArgvIndex != 0 {
						t.Errorf("slot %d was unexpectedly written (ArgvIndex=%d)",
							i, info.ConstraintUsage[i].ArgvIndex)
					}
				}
			}
		})
	}
}

// TestMCDC_TestCursorEOF exercises the compound condition in the testCursor.EOF method.
//
// Condition: c.pos < 0 || c.pos >= len(c.rows)
//
// MC/DC cases:
//
//	A: pos=-1         → pos<0 true (short-circuits; flips outcome)
//	B: pos=3, rows=3  → pos<0 false, pos>=len true (flips outcome vs C)
//	C: pos=1, rows=3  → both false → not EOF
func TestMCDC_TestCursorEOF(t *testing.T) {
	t.Parallel()

	rows := [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
	}

	tests := []struct {
		name    string
		pos     int
		wantEOF bool
	}{
		// A: pos<0 true → EOF (independent flip)
		{name: "MCDC_pos_negative_is_eof", pos: -1, wantEOF: true},
		// B: pos>=len true → EOF (independent flip of second sub-condition)
		{name: "MCDC_pos_equals_len_is_eof", pos: 3, wantEOF: true},
		// B variant: pos clearly beyond length
		{name: "MCDC_pos_beyond_len_is_eof", pos: 10, wantEOF: true},
		// C: both false → not EOF
		{name: "MCDC_pos_zero_not_eof", pos: 0, wantEOF: false},
		{name: "MCDC_pos_middle_not_eof", pos: 1, wantEOF: false},
		{name: "MCDC_pos_last_not_eof", pos: 2, wantEOF: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &testCursor{
				rows: rows,
				pos:  tt.pos,
			}
			got := c.EOF()
			if got != tt.wantEOF {
				t.Errorf("EOF() with pos=%d = %v, want %v", tt.pos, got, tt.wantEOF)
			}
		})
	}
}
