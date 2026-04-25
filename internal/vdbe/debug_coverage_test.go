// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// TestDebugSetStepMode verifies SetStepMode enables single-step execution.
func TestDebugSetStepMode(t *testing.T) {
	t.Parallel()

	// SetStepMode on a VDBE with no debug context should create one.
	v := New()
	v.SetStepMode(true)

	if v.Debug == nil {
		t.Fatal("expected Debug context to be non-nil after SetStepMode")
	}
	if !v.Debug.StepMode {
		t.Error("expected StepMode to be true")
	}

	// Toggling off should work.
	v.SetStepMode(false)
	if v.Debug.StepMode {
		t.Error("expected StepMode to be false after disabling")
	}

	// SetStepMode when Debug already exists should just update StepMode.
	v2 := New()
	v2.SetDebugMode(DebugTrace)
	v2.SetStepMode(true)
	if !v2.Debug.StepMode {
		t.Error("expected StepMode to be true when Debug already set")
	}

	// Verify step mode causes TraceInstruction to return false.
	v3 := New()
	v3.SetStepMode(true)
	v3.AddOp(OpInteger, 1, 0, 0)
	v3.AddOp(OpHalt, 0, 0, 0)
	v3.AllocMemory(1)

	_, err := v3.Step()
	if err != nil {
		t.Fatalf("unexpected error during step with StepMode: %v", err)
	}
}

// TestDebugDumpRegister verifies DumpRegister for valid and out-of-range indices.
func TestDebugDumpRegister(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(3)
	v.Mem[0].SetInt(7)
	v.Mem[1].SetStr("hello")

	// Valid register.
	out := v.DumpRegister(0)
	if !strings.Contains(out, "R0") {
		t.Errorf("expected 'R0' in DumpRegister output, got: %s", out)
	}
	if !strings.Contains(out, "7") {
		t.Errorf("expected '7' in DumpRegister output, got: %s", out)
	}

	// Valid register with string value.
	out1 := v.DumpRegister(1)
	if !strings.Contains(out1, "hello") {
		t.Errorf("expected 'hello' in DumpRegister output, got: %s", out1)
	}

	// Out-of-range negative index.
	outNeg := v.DumpRegister(-1)
	if !strings.Contains(outNeg, "OUT OF RANGE") {
		t.Errorf("expected 'OUT OF RANGE' for negative index, got: %s", outNeg)
	}

	// Out-of-range positive index.
	outOver := v.DumpRegister(100)
	if !strings.Contains(outOver, "OUT OF RANGE") {
		t.Errorf("expected 'OUT OF RANGE' for index beyond Mem, got: %s", outOver)
	}

	// Watched register should show [WATCHED].
	v.WatchRegister(2)
	outWatched := v.DumpRegister(2)
	if !strings.Contains(outWatched, "[WATCHED]") {
		t.Errorf("expected '[WATCHED]' in DumpRegister for watched register, got: %s", outWatched)
	}
}

// TestDebugLogSingleCursor verifies logSingleCursor for closed, nil, and various cursor types.
func TestDebugLogSingleCursor(t *testing.T) {
	t.Parallel()
	v := New()
	v.SetDebugMode(DebugCursors)
	v.AllocCursors(4)

	// Index 0: closed cursor (nil).
	// Index 1: BTree cursor with EOF.
	v.Cursors[1] = &Cursor{
		CurType: CursorBTree,
		EOF:     true,
	}
	// Index 2: Sorter cursor with NullRow.
	v.Cursors[2] = &Cursor{
		CurType: CursorSorter,
		NullRow: true,
	}
	// Index 3: VTab cursor.
	v.Cursors[3] = &Cursor{
		CurType: CursorVTab,
	}

	// Out-of-range index should be a no-op (no panic).
	v.logSingleCursor(-1)
	v.logSingleCursor(100)

	// Closed cursor should not panic.
	v.logSingleCursor(0)

	// Open cursors should not panic.
	v.logSingleCursor(1)
	v.logSingleCursor(2)
	v.logSingleCursor(3)

	// Pseudo cursor for completeness.
	v.AllocCursors(5)
	v.Cursors[4] = &Cursor{
		CurType: CursorPseudo,
	}
	v.logSingleCursor(4)
}

// TestDebugFormatCursorFlags verifies formatCursorFlags for all flag combinations.
func TestDebugFormatCursorFlags(t *testing.T) {
	t.Parallel()

	// Neither EOF nor NullRow.
	c := &Cursor{}
	flags := formatCursorFlags(c)
	if flags != "" {
		t.Errorf("expected empty flags, got: %q", flags)
	}

	// EOF only.
	c.EOF = true
	flags = formatCursorFlags(c)
	if !strings.Contains(flags, "EOF") {
		t.Errorf("expected 'EOF' in flags, got: %q", flags)
	}

	// NullRow only.
	c2 := &Cursor{NullRow: true}
	flags2 := formatCursorFlags(c2)
	if !strings.Contains(flags2, "NULL") {
		t.Errorf("expected 'NULL' in flags, got: %q", flags2)
	}

	// Both EOF and NullRow.
	c3 := &Cursor{EOF: true, NullRow: true}
	flags3 := formatCursorFlags(c3)
	if !strings.Contains(flags3, "EOF") || !strings.Contains(flags3, "NULL") {
		t.Errorf("expected both 'EOF' and 'NULL' in flags, got: %q", flags3)
	}
}

// TestDebugAppendRegisterRange verifies appendRegisterRange builds the correct slice.
func TestDebugAppendRegisterRange(t *testing.T) {
	t.Parallel()
	v := New()

	// Normal range.
	result := v.appendRegisterRange(nil, 2, 5)
	if len(result) != 3 {
		t.Fatalf("expected 3 elements, got %d: %v", len(result), result)
	}
	for i, want := range []int{2, 3, 4} {
		if result[i] != want {
			t.Errorf("result[%d] = %d, want %d", i, result[i], want)
		}
	}

	// Empty range (start == end).
	empty := v.appendRegisterRange(nil, 3, 3)
	if len(empty) != 0 {
		t.Errorf("expected empty slice for empty range, got %v", empty)
	}

	// Appending to an existing slice.
	base := []int{0, 1}
	combined := v.appendRegisterRange(base, 5, 7)
	if len(combined) != 4 {
		t.Fatalf("expected 4 elements, got %d: %v", len(combined), combined)
	}
	if combined[2] != 5 || combined[3] != 6 {
		t.Errorf("unexpected combined values: %v", combined)
	}
}

// buildVDBEWithIndexCursor creates a VDBE with a populated index cursor for seek tests.
func buildVDBEWithIndexCursor(t *testing.T) (*VDBE, *btree.IndexCursor) {
	t.Helper()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	idxCursor := btree.NewIndexCursor(bt, rootPage)
	for _, entry := range []struct {
		key   string
		rowid int64
	}{
		{"apple", 1},
		{"banana", 2},
		{"cherry", 3},
	} {
		if err := idxCursor.InsertIndex([]byte(entry.key), entry.rowid); err != nil {
			t.Fatalf("InsertIndex %q: %v", entry.key, err)
		}
	}

	v := New()
	v.Ctx = &VDBEContext{Btree: bt}
	v.AllocMemory(10)
	v.AllocCursors(2)
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		Writable:    false,
		RootPage:    rootPage,
		BtreeCursor: idxCursor,
		CachedCols:  make([][]byte, 0),
	}
	return v, idxCursor
}

func testSeekGEFoundKey(t *testing.T) {
	t.Parallel()
	v, _ := buildVDBEWithIndexCursor(t)
	v.Mem[3].SetBlob([]byte("banana"))
	if err := v.execSeekGE(&Instruction{Opcode: OpSeekGE, P1: 0, P2: 5, P3: 3}); err != nil {
		t.Fatalf("execSeekGE found: %v", err)
	}
	if v.Cursors[0].EOF {
		t.Error("expected cursor not EOF when key found")
	}
}

func testSeekGENotFoundValid(t *testing.T) {
	t.Parallel()
	v, _ := buildVDBEWithIndexCursor(t)
	v.Mem[3].SetBlob([]byte("zzz"))
	v.PC = 0
	if err := v.execSeekGE(&Instruction{Opcode: OpSeekGE, P1: 0, P2: 5, P3: 3}); err != nil {
		t.Fatalf("execSeekGE not-found: %v", err)
	}
	if v.Cursors[0].EOF {
		t.Error("expected cursor EOF to be false when seek positions past last key but cursor is valid")
	}
}

func testSeekGEKeyAtStart(t *testing.T) {
	t.Parallel()
	v, _ := buildVDBEWithIndexCursor(t)
	v.Mem[3].SetBlob([]byte("apple"))
	if err := v.execSeekGE(&Instruction{Opcode: OpSeekGE, P1: 0, P2: 5, P3: 3}); err != nil {
		t.Fatalf("execSeekGE first key: %v", err)
	}
	if v.Cursors[0].EOF {
		t.Error("expected cursor not EOF when first key found")
	}
}

func testSeekGETableCursor(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(5)
	v.AllocCursors(2)
	v.Cursors[0] = &Cursor{CurType: CursorBTree, IsTable: true, BtreeCursor: nil}
	v.Mem[3].SetInt(42)
	if err := v.execSeekGE(&Instruction{Opcode: OpSeekGE, P1: 0, P2: 5, P3: 3}); err != nil {
		t.Fatalf("execSeekGE table cursor: %v", err)
	}
	if v.Cursors[0].EOF {
		t.Error("expected cursor not EOF for table cursor seek")
	}
}

// TestDebugHandleIndexSeekGE tests handleIndexSeekGE via execSeekGE.
func TestDebugHandleIndexSeekGE(t *testing.T) {
	t.Parallel()
	t.Run("FoundKey", testSeekGEFoundKey)
	t.Run("NotFound_CursorValid", testSeekGENotFoundValid)
	t.Run("KeyAtStart", testSeekGEKeyAtStart)
	t.Run("TableCursor_NoIndex", testSeekGETableCursor)
}
