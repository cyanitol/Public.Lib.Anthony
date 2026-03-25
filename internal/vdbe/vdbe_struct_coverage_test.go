// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"strings"
	"testing"
)

// ============================================================
// compareRowNull — aNull=true, nullsFirst=false (DESC sort)
// The existing tests cover aNull+nf=true (-1) and bNull+nf=true (1),
// but the aNull+nf=false branch (return 1, true) is uncovered.
// ============================================================

func TestVDBEStruct_CompareRowNull_ANullDescSort(t *testing.T) {
	t.Parallel()
	// DESC sort means NULLs sort last (nullsFirstForKey returns false).
	s := NewSorter([]int{0}, []bool{true}, nil, 1)
	aVal := NewMemNull()
	bVal := NewMemInt(5)
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if !isNull {
		t.Error("expected isNull=true when a is NULL")
	}
	// DESC: nullsFirst=false → NULL sorts after non-null → a > b → +1
	if result != 1 {
		t.Errorf("expected +1 (NULL sorts last in DESC), got %d", result)
	}
}

// bNull=true, nullsFirst=false (DESC sort) → return -1, true
func TestVDBEStruct_CompareRowNull_BNullDescSort(t *testing.T) {
	t.Parallel()
	s := NewSorter([]int{0}, []bool{true}, nil, 1)
	aVal := NewMemInt(5)
	bVal := NewMemNull()
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if !isNull {
		t.Error("expected isNull=true when b is NULL")
	}
	// DESC: nullsFirst=false → NULL sorts after non-null → b > a → -1
	if result != -1 {
		t.Errorf("expected -1 (non-null < NULL in DESC), got %d", result)
	}
}

// ============================================================
// compareRows — both-NULL column with multi-key: exercises the
// `continue` path after compareRowNull returns (0, true).
// A second key column breaks the tie.
// ============================================================

func TestVDBEStruct_CompareRows_BothNullFirstKey_TieBreakOnSecond(t *testing.T) {
	t.Parallel()
	// Two key columns: col 0 (NULL==NULL tie), col 1 (int comparison).
	s := NewSorter([]int{0, 1}, []bool{false, false}, nil, 2)

	a := []*Mem{NewMemNull(), NewMemInt(1)}
	b := []*Mem{NewMemNull(), NewMemInt(2)}

	// Both have NULL in col 0 → compareRowNull returns (0, true) → continue.
	// Col 1: 1 < 2 → result should be -1.
	cmp := s.compareRows(a, b)
	if cmp >= 0 {
		t.Errorf("expected negative (a[1]=1 < b[1]=2), got %d", cmp)
	}
}

// compareRows — colIdx out of bounds for both rows: exercises the `continue`
// from isColumnInBounds returning false, then falls through to return 0.
func TestVDBEStruct_CompareRows_ColOutOfBounds(t *testing.T) {
	t.Parallel()
	// Key column index 5 is out of range for rows with 1 element.
	s := NewSorter([]int{5}, []bool{false}, nil, 1)

	a := []*Mem{NewMemInt(10)}
	b := []*Mem{NewMemInt(20)}

	cmp := s.compareRows(a, b)
	if cmp != 0 {
		t.Errorf("expected 0 when key column is out of bounds, got %d", cmp)
	}
}

// ============================================================
// OpenCursor — out-of-range index returns an error.
// The success path is covered by TestVdbeCursorOperations;
// the error branch (index < 0 or >= len) is not.
// ============================================================

func TestVDBEStruct_OpenCursor_NegativeIndex(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocCursors(3)

	err := v.OpenCursor(-1, CursorBTree, 1, true)
	if err == nil {
		t.Error("expected error for negative cursor index")
	}
}

func TestVDBEStruct_OpenCursor_IndexTooLarge(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocCursors(3)

	err := v.OpenCursor(10, CursorBTree, 1, true)
	if err == nil {
		t.Error("expected error when cursor index exceeds allocated count")
	}
}

// ============================================================
// CloseCursor — out-of-range index returns an error.
// ============================================================

func TestVDBEStruct_CloseCursor_NegativeIndex(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocCursors(3)

	err := v.CloseCursor(-1)
	if err == nil {
		t.Error("expected error for negative cursor index in CloseCursor")
	}
}

func TestVDBEStruct_CloseCursor_IndexTooLarge(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocCursors(3)

	err := v.CloseCursor(99)
	if err == nil {
		t.Error("expected error when cursor index exceeds allocated count in CloseCursor")
	}
}

// ============================================================
// ExplainProgram — empty program returns "Empty program".
// The non-empty path is covered by TestVdbeExplain;
// the early-return branch is not.
// ============================================================

func TestVDBEStruct_ExplainProgram_EmptyProgram(t *testing.T) {
	t.Parallel()
	v := New()
	// No instructions added — program is empty.
	result := v.ExplainProgram()
	if result != "Empty program" {
		t.Errorf("expected \"Empty program\", got %q", result)
	}
}

// ExplainProgram with a P4Real instruction, to cover the formatP4 P4Real case
// in case it is not already exercised via ExplainProgram (it's covered as a
// standalone, but ensuring it appears in explain output).
func TestVDBEStruct_ExplainProgram_WithP4Real(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(5)
	v.AddOpWithP4Real(OpReal, 0, 1, 0, 3.14)
	v.AddOp(OpHalt, 0, 0, 0)

	result := v.ExplainProgram()
	if !strings.Contains(result, "3.14") {
		t.Errorf("expected P4Real value 3.14 in explain output, got: %s", result)
	}
}

// ============================================================
// QueryStatistics.String — nil receiver returns "Statistics: disabled".
// ============================================================

func TestVDBEStruct_QueryStatisticsString_NilReceiver(t *testing.T) {
	t.Parallel()
	var s *QueryStatistics
	got := s.String()
	if got != "Statistics: disabled" {
		t.Errorf("expected \"Statistics: disabled\" for nil receiver, got %q", got)
	}
}
