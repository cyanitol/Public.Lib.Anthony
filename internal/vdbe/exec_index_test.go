// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// createIndexPage creates a properly initialized index page for testing
// This mirrors the btree package's test helper
func createIndexPage(bt *btree.Btree) (uint32, error) {
	pageNum, err := bt.AllocatePage()
	if err != nil {
		return 0, err
	}

	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return 0, err
	}

	// Page 1 has a 100-byte database file header
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = btree.FileHeaderSize
	}

	// Initialize the page as an empty leaf index page
	// PageTypeLeafIndex = 0x0a
	pageData[headerOffset+btree.PageHeaderOffsetType] = 0x0a
	pageData[headerOffset+btree.PageHeaderOffsetFreeblock] = 0
	pageData[headerOffset+btree.PageHeaderOffsetFreeblock+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetNumCells] = 0
	pageData[headerOffset+btree.PageHeaderOffsetNumCells+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetCellStart] = 0
	pageData[headerOffset+btree.PageHeaderOffsetCellStart+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetFragmented] = 0

	return pageNum, nil
}

// Helper to setup index cursor with test data
func setupIndexCursor(t *testing.T) (*btree.Btree, *btree.IndexCursor, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}
	idxCursor := btree.NewIndexCursor(bt, rootPage)
	return bt, idxCursor, rootPage
}

// Helper to insert test data
func insertTestData(t *testing.T, idxCursor *btree.IndexCursor) {
	t.Helper()
	testData := []struct {
		key   string
		rowid int64
	}{
		{"apple", 100},
		{"banana", 200},
		{"cherry", 300},
	}
	for _, td := range testData {
		if err := idxCursor.InsertIndex([]byte(td.key), td.rowid); err != nil {
			t.Fatalf("failed to insert '%s': %v", td.key, err)
		}
	}
}

// Helper to verify seek result
func verifySeek(t *testing.T, idxCursor *btree.IndexCursor, key string, expectedRowid int64, shouldFind bool) {
	t.Helper()
	found, err := idxCursor.SeekIndex([]byte(key))
	if err != nil {
		t.Fatalf("failed to seek '%s': %v", key, err)
	}
	if found != shouldFind {
		t.Errorf("seek '%s': expected found=%v, got %v", key, shouldFind, found)
	}
	if shouldFind && idxCursor.GetRowid() != expectedRowid {
		t.Errorf("expected rowid %d for '%s', got %d", expectedRowid, key, idxCursor.GetRowid())
	}
}

// TestIndexCursorBasicOperations tests basic index cursor operations
func TestIndexCursorBasicOperations(t *testing.T) {
	t.Parallel()
	t.Run("Insert", func(t *testing.T) {
		t.Parallel()
		_, idxCursor, _ := setupIndexCursor(t)
		insertTestData(t, idxCursor)
	})

	t.Run("Seek", func(t *testing.T) {
		t.Parallel()
		_, idxCursor, _ := setupIndexCursor(t)
		insertTestData(t, idxCursor)

		verifySeek(t, idxCursor, "apple", 100, true)
		verifySeek(t, idxCursor, "banana", 200, true)
		verifySeek(t, idxCursor, "aardvark", 0, false)
	})

	t.Run("Iteration", func(t *testing.T) {
		t.Parallel()
		_, idxCursor, _ := setupIndexCursor(t)
		insertTestData(t, idxCursor)

		err := idxCursor.MoveToFirst()
		if err != nil {
			t.Fatalf("failed to move to first: %v", err)
		}

		keys := []string{}
		for idxCursor.IsValid() {
			keys = append(keys, string(idxCursor.GetKey()))
			if err := idxCursor.NextIndex(); err != nil {
				break
			}
		}

		expected := []string{"apple", "banana", "cherry"}
		if len(keys) != len(expected) {
			t.Errorf("expected %d keys, got %d: %v", len(expected), len(keys), keys)
		}
		for i, k := range keys {
			if i < len(expected) && k != expected[i] {
				t.Errorf("key[%d]: expected %q, got %q", i, expected[i], k)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		_, idxCursor, _ := setupIndexCursor(t)
		insertTestData(t, idxCursor)

		if err := idxCursor.DeleteIndex([]byte("banana"), 200); err != nil {
			t.Fatalf("failed to delete 'banana': %v", err)
		}

		verifySeek(t, idxCursor, "banana", 0, false)
		verifySeek(t, idxCursor, "apple", 100, true)
	})
}

// TestIdxRowidOpcode tests the OpIdxRowid opcode
func TestIdxRowidOpcode(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	idxCursor := btree.NewIndexCursor(bt, rootPage)

	// Insert test data
	if err := idxCursor.InsertIndex([]byte("key1"), 42); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Seek to the key
	found, err := idxCursor.SeekIndex([]byte("key1"))
	if err != nil || !found {
		t.Fatalf("failed to seek: %v", err)
	}

	// Create VDBE
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	v.AllocMemory(10)
	v.AllocCursors(2)

	// Set up cursor - use BtreeCursor field which stores the index cursor
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		Writable:    false,
		RootPage:    rootPage,
		BtreeCursor: idxCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	// Test OpIdxRowid
	v.AddOp(OpIdxRowid, 0, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	if err := v.Run(); err != nil {
		t.Fatalf("VDBE execution failed: %v", err)
	}

	// Check result
	mem, err := v.GetMem(2)
	if err != nil {
		t.Fatalf("failed to get register: %v", err)
	}
	if !mem.IsInt() {
		t.Error("expected integer result")
	}
	if mem.IntValue() != 42 {
		t.Errorf("expected rowid 42, got %d", mem.IntValue())
	}
}

// TestCompareBytes tests the compareBytes helper function.
func TestCompareBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a      []byte
		b      []byte
		expect int
	}{
		{[]byte("abc"), []byte("abc"), 0},
		{[]byte("abc"), []byte("abd"), -1},
		{[]byte("abd"), []byte("abc"), 1},
		{[]byte("ab"), []byte("abc"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte(""), []byte(""), 0},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte(""), 1},
	}

	for _, tt := range tests {
		tt := tt
		result := compareBytes(tt.a, tt.b)
		if (result < 0 && tt.expect >= 0) || (result > 0 && tt.expect <= 0) || (result == 0 && tt.expect != 0) {
			t.Errorf("compareBytes(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expect)
		}
	}
}

// TestIdxInsertOpcode tests the OpIdxInsert opcode
func TestIdxInsertOpcode(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	idxCursor := btree.NewIndexCursor(bt, rootPage)

	// Create VDBE
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	v.AllocMemory(10)
	v.AllocCursors(2)

	// Set up writable index cursor
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		Writable:    true, // Writable
		RootPage:    rootPage,
		BtreeCursor: idxCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	// Set up key and rowid in registers
	v.Mem[1].SetBlob([]byte("testkey"))
	v.Mem[2].SetInt(123)

	// Test OpIdxInsert
	instr := &Instruction{
		Opcode: OpIdxInsert,
		P1:     0, // cursor
		P2:     1, // key register
		P3:     2, // rowid register
	}

	err = v.execIdxInsert(instr)
	if err != nil {
		t.Fatalf("execIdxInsert failed: %v", err)
	}

	// Verify insertion
	found, err := idxCursor.SeekIndex([]byte("testkey"))
	if err != nil {
		t.Fatalf("SeekIndex failed: %v", err)
	}
	if !found {
		t.Error("Key not found after insert")
	}
	if idxCursor.GetRowid() != 123 {
		t.Errorf("Expected rowid 123, got %d", idxCursor.GetRowid())
	}
}

// TestIdxDeleteOpcode tests the OpIdxDelete opcode
func TestIdxDeleteOpcode(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	idxCursor := btree.NewIndexCursor(bt, rootPage)

	// Insert a key first
	if err := idxCursor.InsertIndex([]byte("deletekey"), 456); err != nil {
		t.Fatalf("InsertIndex failed: %v", err)
	}

	// Create VDBE
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	v.AllocMemory(10)
	v.AllocCursors(2)

	// Set up writable index cursor
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		Writable:    true,
		RootPage:    rootPage,
		BtreeCursor: idxCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	// Set up key in register
	v.Mem[1].SetBlob([]byte("deletekey"))

	// Test OpIdxDelete
	instr := &Instruction{
		Opcode: OpIdxDelete,
		P1:     0, // cursor
		P2:     1, // key register
	}

	err = v.execIdxDelete(instr)
	if err != nil {
		t.Fatalf("execIdxDelete failed: %v", err)
	}

	// Verify deletion
	found, err := idxCursor.SeekIndex([]byte("deletekey"))
	if err != nil {
		t.Fatalf("SeekIndex failed: %v", err)
	}
	if found {
		t.Error("Key still found after delete")
	}
}

// idxCompareTest represents a declarative index comparison test
type idxCompareTest struct {
	name       string
	opcode     Opcode
	compareKey string
	shouldJump bool
}

// idxSetupCursor creates and configures a VDBE with an index cursor
func idxSetupCursor(t *testing.T) (*VDBE, *btree.Btree, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	idxCursor := btree.NewIndexCursor(bt, rootPage)
	keys := []string{"apple", "banana", "cherry", "date"}
	for i, key := range keys {
		if err := idxCursor.InsertIndex([]byte(key), int64(i)); err != nil {
			t.Fatalf("InsertIndex failed: %v", err)
		}
	}

	if found, err := idxCursor.SeekIndex([]byte("banana")); err != nil || !found {
		t.Fatalf("SeekIndex failed: %v, found=%v", err, found)
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
		CacheStatus: 0,
	}
	return v, bt, rootPage
}

// idxExecCompare executes an index comparison opcode
func idxExecCompare(v *VDBE, opcode Opcode, instr *Instruction) error {
	switch opcode {
	case OpIdxLT:
		return v.execIdxLT(instr)
	case OpIdxGT:
		return v.execIdxGT(instr)
	case OpIdxLE:
		return v.execIdxLE(instr)
	case OpIdxGE:
		return v.execIdxGE(instr)
	default:
		return nil
	}
}

// idxVerifyJump checks if PC jumped as expected
func idxVerifyJump(t *testing.T, v *VDBE, shouldJump bool, jumpAddr int) {
	t.Helper()
	if shouldJump && v.PC != jumpAddr {
		t.Errorf("Expected jump to %d, PC=%d", jumpAddr, v.PC)
	}
	if !shouldJump && v.PC == jumpAddr {
		t.Error("Should not jump when condition is false")
	}
}

// TestIdxCompareOpcodes tests the OpIdxLT, OpIdxGT, OpIdxLE, OpIdxGE opcodes
func TestIdxCompareOpcodes(t *testing.T) {
	t.Parallel()
	v, _, _ := idxSetupCursor(t)

	tests := []idxCompareTest{
		{name: "LT_True", opcode: OpIdxLT, compareKey: "cherry", shouldJump: true},
		{name: "LT_False", opcode: OpIdxLT, compareKey: "apple", shouldJump: false},
		{name: "GT_True", opcode: OpIdxGT, compareKey: "apple", shouldJump: true},
		{name: "LE_Equal", opcode: OpIdxLE, compareKey: "banana", shouldJump: true},
		{name: "LE_Less", opcode: OpIdxLE, compareKey: "cherry", shouldJump: true},
		{name: "GE_Equal", opcode: OpIdxGE, compareKey: "banana", shouldJump: true},
		{name: "GE_Greater", opcode: OpIdxGE, compareKey: "apple", shouldJump: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			v.Mem[1].SetBlob([]byte(tt.compareKey))
			v.PC = 0

			instr := &Instruction{Opcode: tt.opcode, P1: 0, P2: 10, P3: 1}

			if err := idxExecCompare(v, tt.opcode, instr); err != nil {
				t.Fatalf("exec failed: %v", err)
			}

			idxVerifyJump(t, v, tt.shouldJump, 10)
		})
	}
}

// TestIdxOpcodeErrors tests error conditions for index opcodes
func TestIdxOpcodeErrors(t *testing.T) {
	t.Parallel()

	t.Run("IdxInsert_NoCursor", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)
		v.AllocCursors(2)
		err := v.execIdxInsert(&Instruction{
			Opcode: OpIdxInsert,
			P1:     0,
			P2:     1,
			P3:     2,
		})

		if err == nil {
			t.Error("Expected error for missing cursor")
		}
	})

	t.Run("IdxInsert_NotWritable", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)
		v.AllocCursors(2)
		bt := btree.NewBtree(4096)
		rootPage, _ := createIndexPage(bt)
		idxCursor := btree.NewIndexCursor(bt, rootPage)

		v.Ctx = &VDBEContext{Btree: bt}
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			IsTable:     false,
			Writable:    false, // NOT writable
			RootPage:    rootPage,
			BtreeCursor: idxCursor,
		}

		v.Mem[1].SetBlob([]byte("key"))
		v.Mem[2].SetInt(1)

		err := v.execIdxInsert(&Instruction{
			Opcode: OpIdxInsert,
			P1:     0,
			P2:     1,
			P3:     2,
		})

		if err == nil {
			t.Error("Expected error for non-writable cursor")
		}
	})

	t.Run("IdxDelete_NoCursor", func(t *testing.T) {
		t.Parallel()
		v2 := New()
		v2.AllocMemory(10)
		v2.AllocCursors(2)

		err := v2.execIdxDelete(&Instruction{
			Opcode: OpIdxDelete,
			P1:     0,
			P2:     1,
		})

		if err == nil {
			t.Error("Expected error for missing cursor")
		}
	})

	t.Run("IdxCompare_InvalidCursor", func(t *testing.T) {
		t.Parallel()
		v3 := New()
		v3.AllocMemory(10)
		v3.AllocCursors(2)

		err := v3.execIdxLT(&Instruction{
			Opcode: OpIdxLT,
			P1:     0,
			P2:     10,
			P3:     1,
		})

		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})
}
