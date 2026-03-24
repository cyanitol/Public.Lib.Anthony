// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Pool tests
// ---------------------------------------------------------------------------

func TestPoolPageBuffer(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"standard 4096", 4096},
		{"small 512", 512},
		{"large 65536", 65536},
		{"large 8192", 8192},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := GetPageBuffer(tc.size)
			if buf == nil {
				t.Fatal("GetPageBuffer returned nil")
			}
			if len(*buf) != tc.size {
				t.Fatalf("expected len %d, got %d", tc.size, len(*buf))
			}
			PutPageBuffer(buf)
		})
	}

	// nil should not panic
	PutPageBuffer(nil)
}

func TestPoolInstructionSlice(t *testing.T) {
	slice := GetInstructionSlice()
	if slice == nil {
		t.Fatal("GetInstructionSlice returned nil")
	}
	if len(*slice) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(*slice))
	}

	instr := GetInstruction()
	*slice = append(*slice, instr)
	if len(*slice) != 1 {
		t.Fatal("expected 1 element after append")
	}

	PutInstructionSlice(slice)
	PutInstructionSlice(nil)
}

func TestPoolMemSlice(t *testing.T) {
	slice := GetMemSlice()
	if slice == nil {
		t.Fatal("GetMemSlice returned nil")
	}
	if len(*slice) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(*slice))
	}

	m := GetMem()
	*slice = append(*slice, m)
	PutMemSlice(slice)
	PutMemSlice(nil)
}

func TestPoolAllocateMems(t *testing.T) {
	counts := []int{0, 1, 5}
	for _, n := range counts {
		mems := AllocateMems(n)
		if len(mems) != n {
			t.Fatalf("AllocateMems(%d): got len %d", n, len(mems))
		}
		FreeMems(mems)
	}
}

func TestPoolAllocateInstructions(t *testing.T) {
	counts := []int{0, 1, 4}
	for _, n := range counts {
		instrs := AllocateInstructions(n)
		if len(instrs) != n {
			t.Fatalf("AllocateInstructions(%d): got len %d", n, len(instrs))
		}
		FreeInstructions(instrs)
	}
}

// ---------------------------------------------------------------------------
// DecodeRecord tests
// ---------------------------------------------------------------------------

func TestDecodeRecord(t *testing.T) {
	tests := []struct {
		name    string
		values  []interface{}
		wantErr bool
	}{
		{"null value", []interface{}{nil}, false},
		{"integer zero", []interface{}{int64(0)}, false},
		{"integer one", []interface{}{int64(1)}, false},
		{"small int", []interface{}{int64(42)}, false},
		{"negative int", []interface{}{int64(-7)}, false},
		{"float64", []interface{}{float64(3.14)}, false},
		{"string", []interface{}{"hello"}, false},
		{"blob", []interface{}{[]byte{0x01, 0x02}}, false},
		{"mixed row", []interface{}{int64(1), "name", float64(9.9), nil}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeSimpleRecord(tc.values)
			got, err := DecodeRecord(encoded)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DecodeRecord error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && len(got) != len(tc.values) {
				t.Fatalf("expected %d values, got %d", len(tc.values), len(got))
			}
		})
	}
}

func TestDecodeRecordErrors(t *testing.T) {
	// empty data
	_, err := DecodeRecord([]byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}

	// truncated header
	_, err = DecodeRecord([]byte{0x05, 0x01})
	if err == nil {
		t.Fatal("expected error for truncated record")
	}
}

// ---------------------------------------------------------------------------
// VdbeError (RaiseError) tests
// ---------------------------------------------------------------------------

func TestVdbeTypesRaiseError(t *testing.T) {
	tests := []struct {
		name       string
		errType    int
		msg        string
		wantIgnore bool
		wantRollback bool
	}{
		{"IGNORE", 0, "ignore me", true, false},
		{"ROLLBACK", 1, "roll back", false, true},
		{"ABORT", 2, "abort!", false, false},
		{"FAIL", 3, "fail", false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			re := &RaiseError{Type: tc.errType, Message: tc.msg}
			if re.Error() != tc.msg {
				t.Fatalf("Error() = %q, want %q", re.Error(), tc.msg)
			}
			if re.IsIgnore() != tc.wantIgnore {
				t.Fatalf("IsIgnore() = %v, want %v", re.IsIgnore(), tc.wantIgnore)
			}
			if re.IsRollback() != tc.wantRollback {
				t.Fatalf("IsRollback() = %v, want %v", re.IsRollback(), tc.wantRollback)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NewSorterWithNulls tests
// ---------------------------------------------------------------------------

func TestNewSorterWithNulls(t *testing.T) {
	tf := true
	ff := false
	s := NewSorterWithNulls(
		[]int{0, 1},
		[]bool{false, true},
		[]*bool{&tf, &ff},
		[]string{"", ""},
		2,
		nil,
	)
	if s == nil {
		t.Fatal("NewSorterWithNulls returned nil")
	}
	if len(s.KeyCols) != 2 {
		t.Fatalf("expected 2 key cols, got %d", len(s.KeyCols))
	}
	if s.NullsFirst[0] == nil || *s.NullsFirst[0] != true {
		t.Fatal("NullsFirst[0] should be true")
	}
	if s.NullsFirst[1] == nil || *s.NullsFirst[1] != false {
		t.Fatal("NullsFirst[1] should be false")
	}
}

// ---------------------------------------------------------------------------
// compareColumn tests
// ---------------------------------------------------------------------------

func TestCompareColumn(t *testing.T) {
	s := NewSorter([]int{0}, []bool{false}, []string{""}, 1)

	a := NewMemInt(10)
	b := NewMemInt(20)

	cmp := s.compareColumn(a, b, 0)
	if cmp >= 0 {
		t.Fatalf("expected a < b, got cmp=%d", cmp)
	}

	cmp = s.compareColumn(b, a, 0)
	if cmp <= 0 {
		t.Fatalf("expected b > a, got cmp=%d", cmp)
	}

	cmp = s.compareColumn(a, a, 0)
	if cmp != 0 {
		t.Fatalf("expected a == a, got cmp=%d", cmp)
	}

	// key index beyond Collations slice falls through to Compare
	s2 := NewSorter([]int{0}, []bool{false}, nil, 1)
	cmp = s2.compareColumn(a, b, 0)
	if cmp >= 0 {
		t.Fatalf("no-collation branch: expected a < b, got cmp=%d", cmp)
	}
}

// ---------------------------------------------------------------------------
// nullsFirstForKey tests
// ---------------------------------------------------------------------------

func TestNullsFirstForKey(t *testing.T) {
	tf := true
	ff := false

	tests := []struct {
		name      string
		desc      []bool
		nullsFirst []*bool
		keyIdx    int
		want      bool
	}{
		{"explicit true", []bool{false}, []*bool{&tf}, 0, true},
		{"explicit false", []bool{false}, []*bool{&ff}, 0, false},
		{"default ASC => nulls first", []bool{false}, nil, 0, true},
		{"default DESC => nulls last", []bool{true}, nil, 0, false},
		{"key beyond nullsFirst => default ASC", []bool{false}, []*bool{}, 0, true},
		{"nil entry in nullsFirst => default ASC", []bool{false}, []*bool{nil}, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Sorter{Desc: tc.desc, NullsFirst: tc.nullsFirst}
			got := s.nullsFirstForKey(tc.keyIdx)
			if got != tc.want {
				t.Fatalf("nullsFirstForKey(%d) = %v, want %v", tc.keyIdx, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// resetSorters tests
// ---------------------------------------------------------------------------

func TestResetSorters(t *testing.T) {
	v := New()

	// nil sorter entry should not panic
	v.Sorters = []SorterInterface{nil}
	v.resetSorters()

	// real sorter should close cleanly
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	row := []*Mem{NewMemInt(1)}
	_ = s.Insert(row)
	v.Sorters = []SorterInterface{s}
	v.resetSorters()

	// empty sorters
	v.Sorters = nil
	v.resetSorters()
}

// ---------------------------------------------------------------------------
// formatP4 tests
// ---------------------------------------------------------------------------

func TestFormatP4(t *testing.T) {
	v := New()

	tests := []struct {
		name    string
		p4type  P4Type
		p4      P4Union
		wantNon bool // true if result should be non-empty
	}{
		{"Int32", P4Int32, P4Union{I: 42}, true},
		{"Int64", P4Int64, P4Union{I64: 9999}, true},
		{"Real", P4Real, P4Union{R: 1.5}, true},
		{"Static", P4Static, P4Union{Z: "hello"}, true},
		{"Dynamic", P4Dynamic, P4Union{Z: "world"}, true},
		{"NotUsed", P4NotUsed, P4Union{}, false},
		{"Unknown", P4KeyInfo, P4Union{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			instr := &Instruction{P4Type: tc.p4type, P4: tc.p4}
			result := v.formatP4(instr)
			if tc.wantNon && result == "" {
				t.Fatalf("formatP4 returned empty string for %s", tc.name)
			}
			if !tc.wantNon && result != "" {
				t.Fatalf("formatP4 returned non-empty %q for %s", result, tc.name)
			}
		})
	}
}
