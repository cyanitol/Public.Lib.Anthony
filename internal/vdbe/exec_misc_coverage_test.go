// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// newVDBEWithBtree creates a VDBE instance backed by an in-memory btree.
func newVDBEWithBtree(t *testing.T) (*VDBE, *btree.Btree) {
	t.Helper()
	bt := btree.NewBtree(4096)
	v := New()
	v.Ctx = &VDBEContext{Btree: bt}
	v.AllocMemory(20)
	return v, bt
}

// TestExecClearEphemeral_NilCursor verifies that clearing a nil cursor is a no-op.
func TestExecClearEphemeral_NilCursor(t *testing.T) {
	t.Parallel()
	v, _ := newVDBEWithBtree(t)
	if err := v.AllocCursors(3); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	// Cursor 1 is nil — should succeed silently.
	instr := &Instruction{Opcode: OpClearEphemeral, P1: 1}
	if err := v.execClearEphemeral(instr); err != nil {
		t.Fatalf("unexpected error with nil cursor: %v", err)
	}
}

// TestExecClearEphemeral_OutOfRange checks that an out-of-range cursor index returns an error.
func TestExecClearEphemeral_OutOfRange(t *testing.T) {
	t.Parallel()
	v, _ := newVDBEWithBtree(t)
	if err := v.AllocCursors(2); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	instr := &Instruction{Opcode: OpClearEphemeral, P1: 99}
	if err := v.execClearEphemeral(instr); err == nil {
		t.Fatal("expected error for out-of-range cursor index")
	}
}

// TestExecClearEphemeral_HappyPath opens an ephemeral cursor then clears it.
func TestExecClearEphemeral_HappyPath(t *testing.T) {
	t.Parallel()
	v, _ := newVDBEWithBtree(t)
	v.AllocMemory(10)

	open := &Instruction{Opcode: OpOpenEphemeral, P1: 0, P2: 1}
	if err := v.execOpenEphemeral(open); err != nil {
		t.Fatalf("execOpenEphemeral: %v", err)
	}

	clear := &Instruction{Opcode: OpClearEphemeral, P1: 0}
	if err := v.execClearEphemeral(clear); err != nil {
		t.Fatalf("execClearEphemeral: %v", err)
	}

	cursor, err := v.GetCursor(0)
	if err != nil {
		t.Fatalf("GetCursor after clear: %v", err)
	}
	if cursor.BtreeCursor == nil {
		t.Error("BtreeCursor should not be nil after clear")
	}
	if len(cursor.CachedCols) != 0 {
		t.Errorf("CachedCols should be empty after clear, got %d entries", len(cursor.CachedCols))
	}
}

// TestGetRowidFromIndexCursor_HappyPath exercises the index cursor rowid retrieval.
// makeIndexPageAndCursor creates an index page, inserts a key, seeks to it, and returns the cursor.
func makeIndexPageAndCursor(t *testing.T, bt *btree.Btree, key []byte, rowid int64) (*btree.IndexCursor, uint32) {
	t.Helper()
	pageNum, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = btree.FileHeaderSize
	}
	pageData[headerOffset+btree.PageHeaderOffsetType] = 0x0a
	for i := 1; i <= 6; i++ {
		pageData[headerOffset+i] = 0
	}
	idxCursor := btree.NewIndexCursor(bt, pageNum)
	if err := idxCursor.InsertIndex(key, rowid); err != nil {
		t.Fatalf("InsertIndex: %v", err)
	}
	found, err := idxCursor.SeekIndex(key)
	if err != nil {
		t.Fatalf("SeekIndex: %v", err)
	}
	if !found {
		t.Fatal("expected to find key")
	}
	return idxCursor, pageNum
}

func TestGetRowidFromIndexCursor_HappyPath(t *testing.T) {
	t.Parallel()
	v, bt := newVDBEWithBtree(t)

	idxCursor, pageNum := makeIndexPageAndCursor(t, bt, []byte("hello"), 42)

	if err := v.AllocCursors(2); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		BtreeCursor: idxCursor,
		RootPage:    pageNum,
		CachedCols:  make([][]byte, 0),
	}
	tableCursor := &Cursor{CurType: CursorBTree, IsTable: true}

	rowid, err := v.getRowidFromIndexCursor(0, tableCursor)
	if err != nil {
		t.Fatalf("getRowidFromIndexCursor: %v", err)
	}
	if rowid != 42 {
		t.Errorf("expected rowid 42, got %d", rowid)
	}
}

// TestGetRowidFromIndexCursor_NonIndexCursor verifies that a plain BtCursor causes
// the table cursor's EOF flag to be set and returns rowid 0 without error.
func TestGetRowidFromIndexCursor_NonIndexCursor(t *testing.T) {
	t.Parallel()
	v, bt := newVDBEWithBtree(t)

	if err := v.AllocCursors(2); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	// Use a regular BtCursor (not an IndexCursor).
	btCur := btree.NewCursor(bt, 1)
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		BtreeCursor: btCur,
		CachedCols:  make([][]byte, 0),
	}
	tableCursor := &Cursor{CurType: CursorBTree, IsTable: true}

	rowid, err := v.getRowidFromIndexCursor(0, tableCursor)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rowid != 0 {
		t.Errorf("expected rowid 0, got %d", rowid)
	}
	if !tableCursor.EOF {
		t.Error("expected tableCursor.EOF to be true for non-index cursor")
	}
}

// TestParseRecordColumn_Null checks empty data yields NULL.
func TestParseRecordColumn_Null(t *testing.T) {
	t.Parallel()
	dst := NewMem()
	if err := ParseRecordColumn(nil, 0, dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dst.IsNull() {
		t.Error("expected NULL for empty record data")
	}
}

// TestParseRecordColumn_IntColumn parses an integer column from an encoded record.
func TestParseRecordColumn_IntColumn(t *testing.T) {
	t.Parallel()
	record := encodeSimpleRecord([]interface{}{int64(99), "hello"})
	dst := NewMem()
	if err := ParseRecordColumn(record, 0, dst); err != nil {
		t.Fatalf("ParseRecordColumn col 0: %v", err)
	}
	if !dst.IsInt() {
		t.Errorf("expected integer, got flags=%v", dst.GetFlags())
	}
	if dst.IntValue() != 99 {
		t.Errorf("expected 99, got %d", dst.IntValue())
	}
}

// TestParseRecordColumn_StrColumn parses a string column from an encoded record.
func TestParseRecordColumn_StrColumn(t *testing.T) {
	t.Parallel()
	record := encodeSimpleRecord([]interface{}{int64(1), "world"})
	dst := NewMem()
	if err := ParseRecordColumn(record, 1, dst); err != nil {
		t.Fatalf("ParseRecordColumn col 1: %v", err)
	}
	if !dst.IsStr() {
		t.Errorf("expected string, got flags=%v", dst.GetFlags())
	}
	if dst.StrValue() != "world" {
		t.Errorf("expected 'world', got '%s'", dst.StrValue())
	}
}

// TestParseRecordColumn_OutOfRange verifies that an out-of-range column index yields NULL.
func TestParseRecordColumn_OutOfRange(t *testing.T) {
	t.Parallel()
	record := encodeSimpleRecord([]interface{}{int64(7)})
	dst := NewMem()
	if err := ParseRecordColumn(record, 5, dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dst.IsNull() {
		t.Error("expected NULL for out-of-range column")
	}
}

// TestMemToInterface_AllTypes exercises every branch of MemToInterface.
func TestMemToInterface_NilAndNull(t *testing.T) {
	t.Parallel()
	if MemToInterface(nil) != nil {
		t.Error("expected nil for nil mem")
	}
	if MemToInterface(NewMemNull()) != nil {
		t.Error("expected nil for NULL mem")
	}
	if MemToInterface(NewMem()) != nil {
		t.Error("expected nil for undefined mem")
	}
}

func testMemToInterfaceInt(t *testing.T) {
	if v, ok := MemToInterface(NewMemInt(123)).(int64); !ok || v != 123 {
		t.Errorf("expected int64(123), got %v", MemToInterface(NewMemInt(123)))
	}
}

func testMemToInterfaceReal(t *testing.T) {
	if v, ok := MemToInterface(NewMemReal(3.14)).(float64); !ok || v != 3.14 {
		t.Errorf("expected float64(3.14), got %v", MemToInterface(NewMemReal(3.14)))
	}
}

func testMemToInterfaceString(t *testing.T) {
	if v, ok := MemToInterface(NewMemStr("hi")).(string); !ok || v != "hi" {
		t.Errorf("expected \"hi\", got %v", MemToInterface(NewMemStr("hi")))
	}
}

func testMemToInterfaceBlob(t *testing.T) {
	if v, ok := MemToInterface(NewMemBlob([]byte{1, 2, 3})).([]byte); !ok || len(v) != 3 {
		t.Errorf("expected []byte{1,2,3}, got %v", MemToInterface(NewMemBlob([]byte{1, 2, 3})))
	}
}

func TestMemToInterface_TypedValues(t *testing.T) {
	t.Parallel()
	t.Run("Int", testMemToInterfaceInt)
	t.Run("Real", testMemToInterfaceReal)
	t.Run("String", testMemToInterfaceString)
	t.Run("Blob", testMemToInterfaceBlob)
}
