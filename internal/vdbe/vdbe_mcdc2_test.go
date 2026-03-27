// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ============================================================
// MC/DC tests for vdbe.go, window.go, mem.go (batch 2)
//
// For each compound condition A&&B or A||B, N+1 test cases are
// written so each sub-condition independently flips the outcome.
// Test names contain "MCDC" for -run MCDC selection.
// ============================================================

// ------------------------------------------------------------
// C1: vdbe.go:421 – s.Current >= 0 && s.Current < len(s.Rows)
// Outcome: CurrentRow() returns a valid row vs nil
// Cases:
//   A=F, B=* → nil (current < 0)
//   A=T, B=F → nil (current >= len)
//   A=T, B=T → valid row returned
// ------------------------------------------------------------

func TestMCDC_SorterCurrentRow_NegativeCurrent(t *testing.T) {
	t.Parallel()
	// A=false: Current is -1
	s := NewSorter([]int{0}, nil, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(1)})
	s.Current = -1
	if s.CurrentRow() != nil {
		t.Error("Expected nil when Current < 0")
	}
}

func TestMCDC_SorterCurrentRow_CurrentAtEnd(t *testing.T) {
	t.Parallel()
	// A=true, B=false: Current == len(s.Rows) → out of range
	s := NewSorter([]int{0}, nil, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(42)})
	s.Current = 1 // len == 1, so Current >= len
	if s.CurrentRow() != nil {
		t.Error("Expected nil when Current >= len(Rows)")
	}
}

func TestMCDC_SorterCurrentRow_ValidCurrent(t *testing.T) {
	t.Parallel()
	// A=true, B=true: Current is valid
	s := NewSorter([]int{0}, nil, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(99)})
	s.Current = 0
	row := s.CurrentRow()
	if row == nil {
		t.Fatal("Expected valid row when Current is in range")
	}
	if row[0].IntValue() != 99 {
		t.Errorf("Expected 99, got %d", row[0].IntValue())
	}
}

// ------------------------------------------------------------
// C2: vdbe.go:539 – addr >= 0 && addr < len(v.Program)
// Outcome: SetComment sets comment on instruction vs no-op
// Cases:
//   A=F, B=* → no-op (addr < 0)
//   A=T, B=F → no-op (addr >= len)
//   A=T, B=T → comment set
// ------------------------------------------------------------

func TestMCDC_SetComment_NegativeAddr(t *testing.T) {
	t.Parallel()
	// A=false: addr is negative
	v := NewTestVDBE(2)
	v.AddOp(OpNoop, 0, 0, 0)
	v.SetComment(-1, "should not be set")
	// no panic expected, comment of instruction[0] is unchanged
	if v.Program[0].Comment != "" {
		t.Error("Expected no comment set for negative addr")
	}
}

func TestMCDC_SetComment_AddrTooLarge(t *testing.T) {
	t.Parallel()
	// A=true, B=false: addr >= len(Program)
	v := NewTestVDBE(2)
	v.AddOp(OpNoop, 0, 0, 0)
	v.SetComment(5, "out of range")
	// no panic; instruction 0 comment unchanged
	if v.Program[0].Comment != "" {
		t.Error("Expected no comment set for out-of-range addr")
	}
}

func TestMCDC_SetComment_ValidAddr(t *testing.T) {
	t.Parallel()
	// A=true, B=true: valid addr
	v := NewTestVDBE(2)
	v.AddOp(OpNoop, 0, 0, 0)
	v.SetComment(0, "my comment")
	if v.Program[0].Comment != "my comment" {
		t.Errorf("Expected comment 'my comment', got %q", v.Program[0].Comment)
	}
}

// ------------------------------------------------------------
// C3: vdbe.go:568 – index < 0 || index >= len(v.Mem)
// Outcome: GetMem returns error vs valid Mem
// Cases:
//   A=T, B=* → error (index < 0)
//   A=F, B=T → error (index >= len)
//   A=F, B=F → valid Mem returned
// ------------------------------------------------------------

func TestMCDC_GetMem_NegativeIndex(t *testing.T) {
	t.Parallel()
	// A=true: negative index
	v := NewTestVDBE(3)
	_, err := v.GetMem(-1)
	if err == nil {
		t.Error("Expected error for negative index")
	}
}

func TestMCDC_GetMem_IndexTooLarge(t *testing.T) {
	t.Parallel()
	// A=false, B=true: index >= len(Mem)
	v := NewTestVDBE(3)
	_, err := v.GetMem(3)
	if err == nil {
		t.Error("Expected error for index == len(Mem)")
	}
}

func TestMCDC_GetMem_ValidIndex(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid index
	v := NewTestVDBE(3)
	v.Mem[1].SetInt(7)
	m, err := v.GetMem(1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.IntValue() != 7 {
		t.Errorf("Expected 7, got %d", m.IntValue())
	}
}

// ------------------------------------------------------------
// C4: vdbe.go:334 – len(s.Collations) > keyIdx && s.Collations[keyIdx] != ""
// Outcome: collation-aware comparison used in compareColumn
// Cases:
//   A=F, B=* → default Compare (no collations slice or keyIdx out of range)
//   A=T, B=F → default Compare (collation is empty string)
//   A=T, B=T → collation-aware Compare used
// ------------------------------------------------------------

func TestMCDC_SorterCompareColumn_NoCollations(t *testing.T) {
	t.Parallel()
	// A=false: Collations slice is empty
	s := NewSorter([]int{0}, nil, nil, 1)
	a := NewMemStr("abc")
	b := NewMemStr("ABC")
	result := s.compareColumn(a, b, 0)
	// BINARY: "abc" > "ABC" → positive
	if result <= 0 {
		t.Errorf("Expected positive (BINARY: abc > ABC), got %d", result)
	}
}

func TestMCDC_SorterCompareColumn_EmptyCollation(t *testing.T) {
	t.Parallel()
	// A=true (len > keyIdx), B=false (collation is "")
	s := NewSorter([]int{0}, nil, []string{""}, 1)
	a := NewMemStr("abc")
	b := NewMemStr("ABC")
	result := s.compareColumn(a, b, 0)
	// Default Compare used: "abc" > "ABC" in BINARY
	if result <= 0 {
		t.Errorf("Expected positive (BINARY: abc > ABC), got %d", result)
	}
}

func TestMCDC_SorterCompareColumn_WithNocaseCollation(t *testing.T) {
	t.Parallel()
	// A=true, B=true: collation "NOCASE"
	s := NewSorter([]int{0}, nil, []string{"NOCASE"}, 1)
	a := NewMemStr("abc")
	b := NewMemStr("ABC")
	result := s.compareColumn(a, b, 0)
	// NOCASE: "abc" == "ABC" → 0
	if result != 0 {
		t.Errorf("Expected 0 (NOCASE: abc == ABC), got %d", result)
	}
}

// ------------------------------------------------------------
// C5: vdbe.go:342 – len(s.Desc) > keyIdx && s.Desc[keyIdx]
// Outcome: comparison result is inverted (DESC sort)
// Cases:
//   A=F, B=* → no inversion (Desc slice empty or keyIdx out of range)
//   A=T, B=F → no inversion (Desc[keyIdx] == false = ASC)
//   A=T, B=T → inversion applied (DESC)
// ------------------------------------------------------------

func TestMCDC_SorterApplySortDirection_NoDescSlice(t *testing.T) {
	t.Parallel()
	// A=false: Desc slice is nil/empty
	s := NewSorter([]int{0}, nil, nil, 1)
	result := s.applySortDirection(1, 0)
	if result != 1 {
		t.Errorf("Expected 1 (no inversion, no Desc slice), got %d", result)
	}
}

func TestMCDC_SorterApplySortDirection_DescFalse(t *testing.T) {
	t.Parallel()
	// A=true (len > keyIdx), B=false (Desc[keyIdx] = false = ASC)
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	result := s.applySortDirection(1, 0)
	if result != 1 {
		t.Errorf("Expected 1 (ASC: no inversion), got %d", result)
	}
}

func TestMCDC_SorterApplySortDirection_DescTrue(t *testing.T) {
	t.Parallel()
	// A=true, B=true: DESC → invert
	s := NewSorter([]int{0}, []bool{true}, nil, 1)
	result := s.applySortDirection(1, 0)
	if result != -1 {
		t.Errorf("Expected -1 (DESC: inverted), got %d", result)
	}
}

// ------------------------------------------------------------
// C6: vdbe.go:351 – len(s.NullsFirst) > keyIdx && s.NullsFirst[keyIdx] != nil
// Outcome: explicit NullsFirst override vs default
// Cases:
//   A=F, B=* → default (no NullsFirst slice or keyIdx out of range)
//   A=T, B=F → default (NullsFirst[keyIdx] == nil)
//   A=T, B=T → explicit value used
// ------------------------------------------------------------

func boolPtr(b bool) *bool { return &b }

func TestMCDC_NullsFirstForKey_NoNullsFirstSlice(t *testing.T) {
	t.Parallel()
	// A=false: NullsFirst is nil, default: ASC → nulls first (true)
	s := NewSorter([]int{0}, []bool{false}, nil, 1) // ASC
	result := s.nullsFirstForKey(0)
	if !result {
		t.Error("Expected nullsFirst=true for ASC with no override")
	}
}

func TestMCDC_NullsFirstForKey_NullsFirstNilElement(t *testing.T) {
	t.Parallel()
	// A=true (len > keyIdx), B=false (NullsFirst[0] == nil) → default
	nullsFirst := []*bool{nil}
	s := NewSorterWithNulls([]int{0}, []bool{false}, nullsFirst, nil, 1, nil)
	result := s.nullsFirstForKey(0)
	// ASC, no override → nulls first
	if !result {
		t.Error("Expected nullsFirst=true (default ASC with nil element)")
	}
}

func TestMCDC_NullsFirstForKey_ExplicitNullsLast(t *testing.T) {
	t.Parallel()
	// A=true, B=true: explicit NullsFirst=false (nulls last)
	nullsFirst := []*bool{boolPtr(false)}
	s := NewSorterWithNulls([]int{0}, []bool{false}, nullsFirst, nil, 1, nil)
	result := s.nullsFirstForKey(0)
	if result {
		t.Error("Expected nullsFirst=false (explicit NULLS LAST)")
	}
}

// ------------------------------------------------------------
// C7: window.go:229 – ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions)
// Outcome: CurrentRow() returns nil
// Cases:
//   A=T, B=* → nil (CurrentPartIdx is -1, initial state)
//   A=F, B=T → nil (CurrentPartIdx beyond partition count)
//   A=F, B=F → valid row returned
// ------------------------------------------------------------

func TestMCDC_WindowCurrentRow_NegativePartIdx(t *testing.T) {
	t.Parallel()
	// A=true: CurrentPartIdx = -1 (default new state)
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(5)})
	// CurrentPartIdx is -1 (NextRow not yet called)
	row := ws.CurrentRow()
	if row != nil {
		t.Error("Expected nil when CurrentPartIdx < 0")
	}
}

func TestMCDC_WindowCurrentRow_PartIdxPastEnd(t *testing.T) {
	t.Parallel()
	// A=false (>= 0), B=true (>= len)
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.NextRow()
	// Force CurrentPartIdx past end
	ws.CurrentPartIdx = 5
	row := ws.CurrentRow()
	if row != nil {
		t.Error("Expected nil when CurrentPartIdx >= len(Partitions)")
	}
}

func TestMCDC_WindowCurrentRow_ValidPartIdx(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid state
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(42)})
	row := ws.NextRow()
	if row == nil {
		t.Fatal("Expected valid row from NextRow")
	}
	current := ws.CurrentRow()
	if current == nil {
		t.Error("Expected non-nil CurrentRow after NextRow")
	}
}

// ------------------------------------------------------------
// C8: window.go:234 – ws.CurrentPartRow < 0 || ws.CurrentPartRow >= len(partition.Rows)
// Outcome: CurrentRow() returns nil
// Cases:
//   A=T, B=* → nil (CurrentPartRow < 0)
//   A=F, B=T → nil (CurrentPartRow >= len)
//   A=F, B=F → valid row returned
// ------------------------------------------------------------

func TestMCDC_WindowCurrentRow_NegativePartRow(t *testing.T) {
	t.Parallel()
	// A=true: force CurrentPartRow = -1
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.CurrentPartIdx = 0
	ws.CurrentPartRow = -1
	row := ws.CurrentRow()
	if row != nil {
		t.Error("Expected nil when CurrentPartRow < 0")
	}
}

func TestMCDC_WindowCurrentRow_PartRowPastEnd(t *testing.T) {
	t.Parallel()
	// A=false, B=true: CurrentPartRow >= len(Rows)
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.CurrentPartIdx = 0
	ws.CurrentPartRow = 10 // beyond the 1 row
	row := ws.CurrentRow()
	if row != nil {
		t.Error("Expected nil when CurrentPartRow >= len(partition.Rows)")
	}
}

func TestMCDC_WindowCurrentRow_ValidPartRow(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(77)})
	ws.CurrentPartIdx = 0
	ws.CurrentPartRow = 0
	row := ws.CurrentRow()
	if row == nil {
		t.Fatal("Expected non-nil when both indices are valid")
	}
	if row[0].IntValue() != 77 {
		t.Errorf("Expected 77, got %d", row[0].IntValue())
	}
}

// ------------------------------------------------------------
// C9: window.go:361 – ws.LastRankRow != nil && ws.sameOrderByValues(...)
// Outcome: rank increments (same order) vs new rank
// Cases:
//   A=F, B=* → new rank (LastRankRow is nil → first row sets up rank)
//   A=T, B=F → new rank (LastRankRow non-nil, different order values)
//   A=T, B=T → same rank (LastRankRow non-nil, same order values)
// ------------------------------------------------------------

func TestMCDC_WindowUpdateRanking_LastRankRowNil(t *testing.T) {
	t.Parallel()
	// A=false: LastRankRow is nil (initial state)
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.NextRow()
	// UpdateRanking with no prior LastRankRow
	ws.UpdateRanking()
	// After first call, DenseRank should be 1
	if ws.CurrentDenseRank != 1 {
		t.Errorf("Expected DenseRank=1 after first UpdateRanking, got %d", ws.CurrentDenseRank)
	}
}

func TestMCDC_WindowUpdateRanking_DifferentOrderValues(t *testing.T) {
	t.Parallel()
	// A=true (LastRankRow != nil), B=false (different order values)
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.NextRow() // on row 0
	ws.UpdateRanking()
	prevDense := ws.CurrentDenseRank
	ws.NextRow() // on row 1 (value 2, different)
	ws.UpdateRanking()
	if ws.CurrentDenseRank != prevDense+1 {
		t.Errorf("Expected DenseRank to increment for different order values")
	}
}

func TestMCDC_WindowUpdateRanking_SameOrderValues(t *testing.T) {
	t.Parallel()
	// A=true, B=true: same order values → RowsAtCurrentRank increments
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.NextRow() // on row 0
	ws.UpdateRanking()
	prevDense := ws.CurrentDenseRank
	prevRows := ws.RowsAtCurrentRank
	ws.NextRow() // on row 1 (same value 5)
	ws.UpdateRanking()
	if ws.CurrentDenseRank != prevDense {
		t.Errorf("Expected DenseRank unchanged for same order values, got %d vs %d", ws.CurrentDenseRank, prevDense)
	}
	if ws.RowsAtCurrentRank != prevRows+1 {
		t.Errorf("Expected RowsAtCurrentRank to increment, got %d", ws.RowsAtCurrentRank)
	}
}

// ------------------------------------------------------------
// C10: window.go:387 – ws.LastRankingGeneration == currentRowNum && ws.LastRankingGeneration != -999
// Outcome: UpdateRankingFromRow is skipped for same row
// Cases:
//   A=F, B=* → proceeds (different row number or uninitialized)
//   A=T, B=F → proceeds (generation == -999 = uninitialized)
//   A=T, B=T → skipped (already updated for this row)
// ------------------------------------------------------------

func TestMCDC_UpdateRankingFromRow_DifferentRow(t *testing.T) {
	t.Parallel()
	// A=false: LastRankingGeneration != currentRowNum
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.NextRow() // CurrentPartRow = 0
	row1 := ws.CurrentRow()
	ws.UpdateRankingFromRow(row1)
	before := ws.LastRankingGeneration
	ws.NextRow() // CurrentPartRow = 1 (different)
	row2 := ws.CurrentRow()
	ws.UpdateRankingFromRow(row2)
	if ws.LastRankingGeneration == before {
		t.Error("Expected LastRankingGeneration to update for a different row")
	}
}

func TestMCDC_UpdateRankingFromRow_SameRowSkipped(t *testing.T) {
	t.Parallel()
	// A=true, B=true: same row number, not -999 → skipped
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(3)})
	ws.NextRow() // CurrentPartRow = 0
	row := ws.CurrentRow()
	ws.UpdateRankingFromRow(row) // first call, initializes
	denseBefore := ws.CurrentDenseRank
	ws.UpdateRankingFromRow(row) // second call, same row → should be skipped
	if ws.CurrentDenseRank != denseBefore {
		t.Errorf("Expected DenseRank unchanged on repeated UpdateRankingFromRow, got %d vs %d", ws.CurrentDenseRank, denseBefore)
	}
}

func TestMCDC_UpdateRankingFromRow_InitialUninitialized(t *testing.T) {
	t.Parallel()
	// A=true (gen == row 0), B=false (gen == -999) → proceeds (uninitialized marker)
	ws := NewWindowState(nil, []int{0}, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.NextRow()
	// Manually set LastRankingGeneration to -999 (uninitialized) with CurrentPartRow = 0
	ws.LastRankingGeneration = -999
	ws.CurrentPartRow = 0
	row := ws.CurrentRow()
	ws.UpdateRankingFromRow(row) // Should proceed since -999 is uninitialized
	if ws.LastRankingGeneration == -999 {
		t.Error("Expected LastRankingGeneration to be updated from -999")
	}
}

// ------------------------------------------------------------
// C11: window.go:473 – ws.LastRowCounterUpdate == nil || !ws.SameRowValues(...)
// Outcome: IncrementPartRowIfNewRow increments CurrentPartRow
// Cases:
//   A=T, B=* → increment (LastRowCounterUpdate is nil)
//   A=F, B=T → increment (different row values)
//   A=F, B=F → no increment (same row values)
// ------------------------------------------------------------

func TestMCDC_IncrementPartRowIfNewRow_LastUpdateNil(t *testing.T) {
	t.Parallel()
	// A=true: LastRowCounterUpdate == nil → increment
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.LastRowCounterUpdate = nil
	ws.CurrentPartRow = 0
	row := []*Mem{NewMemInt(1)}
	ws.IncrementPartRowIfNewRow(row)
	if ws.CurrentPartRow != 1 {
		t.Errorf("Expected CurrentPartRow=1 after increment from nil, got %d", ws.CurrentPartRow)
	}
}

func TestMCDC_IncrementPartRowIfNewRow_DifferentRow(t *testing.T) {
	t.Parallel()
	// A=false (LastRowCounterUpdate != nil), B=true (different values) → increment
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(1)}
	ws.LastRowCounterUpdate = ws.CopyRow(row1)
	ws.CurrentPartRow = 5
	row2 := []*Mem{NewMemInt(2)} // different
	ws.IncrementPartRowIfNewRow(row2)
	if ws.CurrentPartRow != 6 {
		t.Errorf("Expected CurrentPartRow=6 after increment for different row, got %d", ws.CurrentPartRow)
	}
}

func TestMCDC_IncrementPartRowIfNewRow_SameRow(t *testing.T) {
	t.Parallel()
	// A=false, B=false (same row values) → no increment
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	row := []*Mem{NewMemInt(7)}
	ws.LastRowCounterUpdate = ws.CopyRow(row)
	ws.CurrentPartRow = 3
	ws.IncrementPartRowIfNewRow(row) // same values → should not increment
	if ws.CurrentPartRow != 3 {
		t.Errorf("Expected CurrentPartRow unchanged (3), got %d", ws.CurrentPartRow)
	}
}

// ------------------------------------------------------------
// C12: window.go:492 – ws.WindowFunctionCount > 0 && ws.CallsThisRow >= ws.WindowFunctionCount
// Outcome: IncrementPartRowOnFirstCall resets CallsThisRow
// Cases:
//   A=F, B=* → no reset (WindowFunctionCount == 0)
//   A=T, B=F → no reset (CallsThisRow < WindowFunctionCount)
//   A=T, B=T → reset (CallsThisRow reaches WindowFunctionCount)
// ------------------------------------------------------------

func TestMCDC_IncrementPartRowOnFirstCall_ZeroFuncCount(t *testing.T) {
	t.Parallel()
	// A=false: WindowFunctionCount == 0
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.WindowFunctionCount = 0
	ws.CallsThisRow = 0
	ws.IncrementPartRowOnFirstCall()
	ws.IncrementPartRowOnFirstCall()
	// With count==0, CallsThisRow keeps growing (no reset)
	if ws.CallsThisRow == 0 {
		t.Error("Expected CallsThisRow to accumulate when WindowFunctionCount=0")
	}
}

func TestMCDC_IncrementPartRowOnFirstCall_NotYetFull(t *testing.T) {
	t.Parallel()
	// A=true (count > 0), B=false (calls < count) → no reset yet
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.WindowFunctionCount = 3
	ws.CallsThisRow = 0
	ws.IncrementPartRowOnFirstCall() // calls = 1, not >= 3
	if ws.CallsThisRow == 0 {
		t.Error("Expected CallsThisRow=1 (not yet reset)")
	}
}

func TestMCDC_IncrementPartRowOnFirstCall_ResetOnFull(t *testing.T) {
	t.Parallel()
	// A=true, B=true: call count reaches WindowFunctionCount → reset
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.WindowFunctionCount = 2
	ws.CallsThisRow = 0
	ws.IncrementPartRowOnFirstCall() // calls = 1
	ws.IncrementPartRowOnFirstCall() // calls = 2 → reset to 0
	if ws.CallsThisRow != 0 {
		t.Errorf("Expected CallsThisRow reset to 0, got %d", ws.CallsThisRow)
	}
}

// ------------------------------------------------------------
// C13: window.go:499 – ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions)
// Outcome: GetLagRow returns nil
// Cases:
//   A=T, B=* → nil (CurrentPartIdx < 0)
//   A=F, B=T → nil (CurrentPartIdx >= len)
//   A=F, B=F → check targetIdx (proceed to row check)
// ------------------------------------------------------------

func TestMCDC_GetLagRow_NegativePartIdx(t *testing.T) {
	t.Parallel()
	// A=true: CurrentPartIdx < 0
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	// CurrentPartIdx = -1 (default before NextRow)
	result := ws.GetLagRow(1)
	if result != nil {
		t.Error("Expected nil from GetLagRow when CurrentPartIdx < 0")
	}
}

func TestMCDC_GetLagRow_PartIdxPastEnd(t *testing.T) {
	t.Parallel()
	// A=false, B=true: CurrentPartIdx >= len(Partitions)
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.NextRow()
	ws.CurrentPartIdx = 99
	result := ws.GetLagRow(1)
	if result != nil {
		t.Error("Expected nil when CurrentPartIdx >= len(Partitions)")
	}
}

func TestMCDC_GetLagRow_ValidAccess(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid state; check that valid lag access works
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.NextRow()              // row 0
	ws.NextRow()              // row 1 (CurrentPartRow = 1)
	result := ws.GetLagRow(1) // lag 1 = row 0
	if result == nil {
		t.Fatal("Expected non-nil from GetLagRow with valid offset")
	}
	if result[0].IntValue() != 1 {
		t.Errorf("Expected lagged row value 1, got %d", result[0].IntValue())
	}
}

// ------------------------------------------------------------
// C14: window.go:506 – targetIdx < 0 || targetIdx >= len(partition.Rows)
// Outcome: GetLagRow returns nil for out-of-bounds target
// Cases:
//   A=T, B=* → nil (targetIdx < 0: offset larger than current row)
//   A=F, B=T → nil (targetIdx >= len rows)
//   A=F, B=F → row returned
// ------------------------------------------------------------

func TestMCDC_GetLagRow_OffsetTooLarge(t *testing.T) {
	t.Parallel()
	// A=true: CurrentPartRow - offset < 0
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.NextRow()              // CurrentPartRow = 0
	result := ws.GetLagRow(5) // targetIdx = 0 - 5 = -5
	if result != nil {
		t.Error("Expected nil when lag offset exceeds row position")
	}
}

func TestMCDC_GetLagRow_TargetBeyondEnd(t *testing.T) {
	t.Parallel()
	// A=false, B=true: offset is negative (lead-like behavior for GetLagRow with negative offset)
	// GetLagRow with offset=-1 would produce targetIdx = row + 1 >= len
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.NextRow()               // CurrentPartRow = 0
	result := ws.GetLagRow(-1) // targetIdx = 0 - (-1) = 1 >= len(1)
	if result != nil {
		t.Error("Expected nil when targetIdx >= len(partition.Rows)")
	}
}

// ------------------------------------------------------------
// C15: window.go:437 – len(row1) != len(row2)  (SameRowValues early exit)
// Outcome: SameRowValues returns false immediately for different lengths
// Cases:
//   A=T (len differs) → false
//   A=F (len same) → proceeds to element comparison
// ------------------------------------------------------------

func TestMCDC_SameRowValues_DifferentLengths(t *testing.T) {
	t.Parallel()
	// A=true: different lengths → false immediately
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(1)}
	row2 := []*Mem{NewMemInt(1), NewMemInt(2)}
	if ws.SameRowValues(row1, row2) {
		t.Error("Expected false for rows with different lengths")
	}
}

func TestMCDC_SameRowValues_SameLengthSameValues(t *testing.T) {
	t.Parallel()
	// A=false (same length): all elements equal → true
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(42), NewMemStr("hello")}
	row2 := []*Mem{NewMemInt(42), NewMemStr("hello")}
	if !ws.SameRowValues(row1, row2) {
		t.Error("Expected true for rows with same lengths and values")
	}
}

func TestMCDC_SameRowValues_SameLengthDifferentValues(t *testing.T) {
	t.Parallel()
	// A=false (same length): elements differ → false
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(1)}
	row2 := []*Mem{NewMemInt(2)}
	if ws.SameRowValues(row1, row2) {
		t.Error("Expected false for rows with same length but different values")
	}
}

// ------------------------------------------------------------
// C16: window.go:334 – isPeer && !isCurrentRow  (ExcludeTies in shouldExcludeRow)
// Outcome: row is excluded by ExcludeTies
// Cases:
//   A=F, B=* → not excluded (row is not a peer)
//   A=T, B=F → not excluded (row IS the current row, i.e., isCurrentRow=true → !isCurrentRow=false)
//   A=T, B=T → excluded (peer but not current row)
// ------------------------------------------------------------

func TestMCDC_ExcludeTies_NotPeer(t *testing.T) {
	t.Parallel()
	// A=false: row is not a peer (different ORDER BY value)
	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeTies,
	})
	row := []*Mem{NewMemInt(1)}
	currentRow := []*Mem{NewMemInt(2)} // different ORDER BY value
	result := ws.shouldExcludeRow(row, currentRow, 0)
	if result {
		t.Error("Expected not excluded: not a peer")
	}
}

func TestMCDC_ExcludeTies_PeerIsCurrentRow(t *testing.T) {
	t.Parallel()
	// A=true (peer), B=false (!isCurrentRow = false because absIdx == CurrentPartRow)
	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeTies,
	})
	ws.CurrentPartRow = 0
	row := []*Mem{NewMemInt(5)} // peer (same value)
	currentRow := []*Mem{NewMemInt(5)}
	result := ws.shouldExcludeRow(row, currentRow, 0) // absIdx=0 == CurrentPartRow=0 → isCurrentRow=true
	if result {
		t.Error("Expected not excluded: ExcludeTies keeps current row")
	}
}

func TestMCDC_ExcludeTies_PeerNotCurrentRow(t *testing.T) {
	t.Parallel()
	// A=true (peer), B=true (!isCurrentRow = true since absIdx != CurrentPartRow)
	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeTies,
	})
	ws.CurrentPartRow = 2 // current row is index 2
	row := []*Mem{NewMemInt(5)}
	currentRow := []*Mem{NewMemInt(5)}                // same ORDER BY value → peer
	result := ws.shouldExcludeRow(row, currentRow, 0) // absIdx=0 != CurrentPartRow=2
	if !result {
		t.Error("Expected excluded: ExcludeTies should exclude peers that are not the current row")
	}
}

// ------------------------------------------------------------
// C17: window.go:172 – colIdx >= len(row1) || colIdx >= len(row2)  (samePartition)
// Outcome: column is skipped when either row is too short
// Cases:
//   A=T, B=* → skip this column (row1 too short)
//   A=F, B=T → skip this column (row2 too short)
//   A=F, B=F → compare column values
// ------------------------------------------------------------

func TestMCDC_SamePartition_Row1TooShort(t *testing.T) {
	t.Parallel()
	// A=true: partition column index is out-of-bounds for both rows
	// samePartition skips the column → treats them as same partition
	ws := NewWindowState([]int{5}, nil, nil, DefaultWindowFrame()) // col 5 doesn't exist in 2-col rows
	ws.AddRow([]*Mem{NewMemInt(1), NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(9), NewMemInt(8)}) // different values at col 0, but col 5 is skipped
	// Because colIdx 5 >= len(row), comparison is skipped → same partition
	if len(ws.Partitions) != 1 {
		t.Errorf("Expected 1 partition when partition column is out-of-bounds for both rows, got %d", len(ws.Partitions))
	}
}

func TestMCDC_SamePartition_DifferentPartitionKey(t *testing.T) {
	t.Parallel()
	// A=false, B=false: both rows have the column; values differ → different partition
	ws := NewWindowState([]int{0}, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1), NewMemInt(10)})
	ws.AddRow([]*Mem{NewMemInt(2), NewMemInt(20)}) // col 0 differs
	if len(ws.Partitions) != 2 {
		t.Errorf("Expected 2 partitions for different keys, got %d", len(ws.Partitions))
	}
}

// ------------------------------------------------------------
// C18: mem.go:732 – i < len(s) && (s[i] == '+' || s[i] == '-')  (extractLeadingNumeric)
// Outcome: leading sign character is consumed
// Cases:
//   A=F, B=* → no sign consumed (empty string or no character)
//   A=T, B=F → no sign consumed (character is not +/-)
//   A=T, B=T → sign consumed ('+' or '-' at start)
// ------------------------------------------------------------

func TestMCDC_ExtractLeadingNumeric_EmptyString(t *testing.T) {
	t.Parallel()
	// A=false: empty string → len(s)=0 → no sign check
	result := extractLeadingNumeric("")
	if result != "" {
		t.Errorf("Expected empty string from empty input, got %q", result)
	}
}

func TestMCDC_ExtractLeadingNumeric_LeadingLetter(t *testing.T) {
	t.Parallel()
	// A=true (len > 0), B=false (not +/-): leading letter, no digits
	result := extractLeadingNumeric("abc")
	if result != "" {
		t.Errorf("Expected empty string for non-numeric prefix, got %q", result)
	}
}

func TestMCDC_ExtractLeadingNumeric_PlusSign(t *testing.T) {
	t.Parallel()
	// A=true, B=true: '+' sign consumed, followed by digits
	result := extractLeadingNumeric("+42abc")
	if result != "+42" {
		t.Errorf("Expected '+42', got %q", result)
	}
}

func TestMCDC_ExtractLeadingNumeric_MinusSign(t *testing.T) {
	t.Parallel()
	// A=true, B=true: '-' sign consumed, followed by digits
	result := extractLeadingNumeric("-7.5xyz")
	if result != "-7.5" {
		t.Errorf("Expected '-7.5', got %q", result)
	}
}

// ------------------------------------------------------------
// C19: mem.go:748 – i < len(s) && s[i] >= '0' && s[i] <= '9'  (skipDigits)
// Outcome: digits are consumed
// Cases:
//   A=F, B=* → stop (past end of string)
//   A=T, B=F (s[i] < '0') → stop (non-digit character)
//   A=T, B=T (s[i] in '0'..'9') → continue consuming
// Note: this is a 3-way compound. We treat it as two conditions:
//   (i < len(s)) AND (s[i] >= '0' AND s[i] <= '9')
// ------------------------------------------------------------

func TestMCDC_SkipDigits_AtEnd(t *testing.T) {
	t.Parallel()
	// A=false: i starts at len(s) → returns immediately
	s := "123"
	result := skipDigits(s, 3) // i == len(s)
	if result != 3 {
		t.Errorf("Expected 3 (no advance past end), got %d", result)
	}
}

func TestMCDC_SkipDigits_NonDigit(t *testing.T) {
	t.Parallel()
	// A=true, B=false: character is not a digit
	s := "abc"
	result := skipDigits(s, 0) // 'a' is not a digit
	if result != 0 {
		t.Errorf("Expected 0 (no advance on non-digit), got %d", result)
	}
}

func TestMCDC_SkipDigits_AllDigits(t *testing.T) {
	t.Parallel()
	// A=true, B=true: all digits consumed
	s := "42xyz"
	result := skipDigits(s, 0)
	if result != 2 {
		t.Errorf("Expected 2 (skip '4','2'), got %d", result)
	}
}

// ------------------------------------------------------------
// C20: vdbe.go:817 – addr < 0 || addr >= len(v.Program)
// Outcome: GetInstrAddr bounds check
// (accessed via setJumpDest if exported, or via test of the condition directly)
// We test via GotoPC/SetComment pattern – or test GetMem which has same shape.
// Instead, confirm OpenCursor index bounds: vdbe.go:600/601
// Cases:
//   A=T, B=* → error (index < 0)
//   A=F, B=T → error (index >= len)
//   A=F, B=F → cursor opened
// ------------------------------------------------------------

func TestMCDC_OpenCursor_NegativeIndex(t *testing.T) {
	t.Parallel()
	// A=true: negative index
	v := NewTestVDBE(2)
	_ = v.AllocCursors(3)
	err := v.OpenCursor(-1, CursorBTree, 1, true)
	if err == nil {
		t.Error("Expected error for negative cursor index")
	}
}

func TestMCDC_OpenCursor_IndexTooLarge(t *testing.T) {
	t.Parallel()
	// A=false, B=true: index >= len(Cursors)
	v := NewTestVDBE(2)
	_ = v.AllocCursors(3)
	err := v.OpenCursor(5, CursorBTree, 1, true)
	if err == nil {
		t.Error("Expected error for out-of-range cursor index")
	}
}

func TestMCDC_OpenCursor_ValidIndex(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid index
	v := NewTestVDBE(2)
	_ = v.AllocCursors(3)
	err := v.OpenCursor(1, CursorBTree, 1, true)
	if err != nil {
		t.Errorf("Unexpected error for valid cursor index: %v", err)
	}
	if v.Cursors[1] == nil {
		t.Error("Expected cursor to be opened at index 1")
	}
}

// ------------------------------------------------------------
// C21: window.go:566 – n < 1 || n > len(frameRows)  (GetNthValue bounds)
// Outcome: GetNthValue returns NULL for out-of-bounds n
// Cases:
//   A=T, B=* → NULL (n < 1)
//   A=F, B=T → NULL (n > len(frameRows))
//   A=F, B=F → valid value returned
// ------------------------------------------------------------

func TestMCDC_GetNthValue_NLessThanOne(t *testing.T) {
	t.Parallel()
	// A=true: n = 0
	ws := buildWindowStateWithRows(3)
	ws.Partitions[0].FrameStart = 0
	ws.Partitions[0].FrameEnd = 2
	result := ws.GetNthValue(0, 0)
	if !result.IsNull() {
		t.Error("Expected NULL for n=0 (n < 1)")
	}
}

func TestMCDC_GetNthValue_NTooLarge(t *testing.T) {
	t.Parallel()
	// A=false, B=true: n > len(frameRows)
	ws := buildWindowStateWithRows(3)
	ws.Partitions[0].FrameStart = 0
	ws.Partitions[0].FrameEnd = 2
	result := ws.GetNthValue(0, 10)
	if !result.IsNull() {
		t.Error("Expected NULL for n > frame size")
	}
}

func TestMCDC_GetNthValue_ValidN(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid n
	ws := buildWindowStateWithRows(3)
	ws.Partitions[0].FrameStart = 0
	ws.Partitions[0].FrameEnd = 2
	result := ws.GetNthValue(0, 2) // 2nd row (1-based)
	if result.IsNull() {
		t.Error("Expected non-NULL for valid n=2")
	}
	if result.IntValue() != 2 {
		t.Errorf("Expected value 2 (2nd row), got %d", result.IntValue())
	}
}
