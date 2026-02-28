package vdbe

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
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

// TestIndexCursorBasicOperations tests basic index cursor operations
func TestIndexCursorBasicOperations(t *testing.T) {
	t.Parallel()
	// Create in-memory btree
	bt := btree.NewBtree(4096)

	// Create an index page (not a table page)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	// Create index cursor
	idxCursor := btree.NewIndexCursor(bt, rootPage)

	// Test insertion
	t.Run("Insert", func(t *testing.T) {
		t.Parallel()
		if err := idxCursor.InsertIndex([]byte("apple"), 100); err != nil {
			t.Fatalf("failed to insert 'apple': %v", err)
		}

		if err := idxCursor.InsertIndex([]byte("banana"), 200); err != nil {
			t.Fatalf("failed to insert 'banana': %v", err)
		}

		if err := idxCursor.InsertIndex([]byte("cherry"), 300); err != nil {
			t.Fatalf("failed to insert 'cherry': %v", err)
		}
	})

	// Test seeking
	t.Run("Seek", func(t *testing.T) {
		t.Parallel()
		found, err := idxCursor.SeekIndex([]byte("apple"))
		if err != nil {
			t.Fatalf("failed to seek 'apple': %v", err)
		}
		if !found {
			t.Errorf("expected to find 'apple' in index")
		}
		if rowid := idxCursor.GetRowid(); rowid != 100 {
			t.Errorf("expected rowid 100 for 'apple', got %d", rowid)
		}

		found, err = idxCursor.SeekIndex([]byte("banana"))
		if err != nil {
			t.Fatalf("failed to seek 'banana': %v", err)
		}
		if !found {
			t.Errorf("expected to find 'banana' in index")
		}
		if rowid := idxCursor.GetRowid(); rowid != 200 {
			t.Errorf("expected rowid 200 for 'banana', got %d", rowid)
		}

		// Test seeking non-existent key
		found, err = idxCursor.SeekIndex([]byte("aardvark"))
		if err != nil {
			t.Fatalf("failed to seek 'aardvark': %v", err)
		}
		if found {
			t.Errorf("expected not to find 'aardvark' in index")
		}
	})

	// Test iteration from first
	t.Run("Iteration", func(t *testing.T) {
		t.Parallel()
		err := idxCursor.MoveToFirst()
		if err != nil {
			t.Fatalf("failed to move to first: %v", err)
		}

		keys := []string{}
		for idxCursor.IsValid() {
			keys = append(keys, string(idxCursor.GetKey()))
			if err := idxCursor.NextIndex(); err != nil {
				break // Reached end
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

	// Test deletion
	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		// Delete 'banana' (key, rowid pair)
		if err := idxCursor.DeleteIndex([]byte("banana"), 200); err != nil {
			t.Fatalf("failed to delete 'banana': %v", err)
		}

		// Verify deletion
		found, err := idxCursor.SeekIndex([]byte("banana"))
		if err != nil {
			t.Fatalf("failed to seek 'banana' after delete: %v", err)
		}
		if found {
			t.Errorf("expected 'banana' to be deleted")
		}

		// Verify other entries still exist
		found, err = idxCursor.SeekIndex([]byte("apple"))
		if err != nil {
			t.Fatalf("failed to seek 'apple': %v", err)
		}
		if !found {
			t.Errorf("expected 'apple' to still exist")
		}
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

// TestIdxCompareOpcodes tests the OpIdxLT, OpIdxGT, OpIdxLE, OpIdxGE opcodes
func TestIdxCompareOpcodes(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("failed to create index page: %v", err)
	}

	idxCursor := btree.NewIndexCursor(bt, rootPage)

	// Insert keys in sorted order
	keys := []string{"apple", "banana", "cherry", "date"}
	for i, key := range keys {
		if err := idxCursor.InsertIndex([]byte(key), int64(i)); err != nil {
			t.Fatalf("InsertIndex failed: %v", err)
		}
	}

	// Seek to "banana"
	if found, err := idxCursor.SeekIndex([]byte("banana")); err != nil || !found {
		t.Fatalf("SeekIndex failed: %v, found=%v", err, found)
	}

	// Create VDBE
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	v.AllocMemory(10)
	v.AllocCursors(2)

	// Set up index cursor at "banana"
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     false,
		Writable:    false,
		RootPage:    rootPage,
		BtreeCursor: idxCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	t.Run("OpIdxLT", func(t *testing.T) {
		t.Parallel()
		// Compare "banana" < "cherry" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("cherry"))
		v.PC = 0

		instr := &Instruction{
			Opcode: OpIdxLT,
			P1:     0,  // cursor
			P2:     10, // jump address
			P3:     1,  // comparison key register
		}

		err := v.execIdxLT(instr)
		if err != nil {
			t.Fatalf("execIdxLT failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}

		// Compare "banana" < "apple" (should be false, no jump)
		v.Mem[1].SetBlob([]byte("apple"))
		v.PC = 0

		err = v.execIdxLT(instr)
		if err != nil {
			t.Fatalf("execIdxLT failed: %v", err)
		}

		if v.PC == 10 {
			t.Error("Should not jump when condition is false")
		}
	})

	t.Run("OpIdxGT", func(t *testing.T) {
		t.Parallel()
		// Compare "banana" > "apple" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("apple"))
		v.PC = 0

		instr := &Instruction{
			Opcode: OpIdxGT,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		err := v.execIdxGT(instr)
		if err != nil {
			t.Fatalf("execIdxGT failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}
	})

	t.Run("OpIdxLE", func(t *testing.T) {
		t.Parallel()
		// Compare "banana" <= "banana" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("banana"))
		v.PC = 0

		instr := &Instruction{
			Opcode: OpIdxLE,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		err := v.execIdxLE(instr)
		if err != nil {
			t.Fatalf("execIdxLE failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}

		// Compare "banana" <= "cherry" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("cherry"))
		v.PC = 0

		err = v.execIdxLE(instr)
		if err != nil {
			t.Fatalf("execIdxLE failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}
	})

	t.Run("OpIdxGE", func(t *testing.T) {
		t.Parallel()
		// Compare "banana" >= "banana" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("banana"))
		v.PC = 0

		instr := &Instruction{
			Opcode: OpIdxGE,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		err := v.execIdxGE(instr)
		if err != nil {
			t.Fatalf("execIdxGE failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}

		// Compare "banana" >= "apple" (should be true, so jump)
		v.Mem[1].SetBlob([]byte("apple"))
		v.PC = 0

		err = v.execIdxGE(instr)
		if err != nil {
			t.Fatalf("execIdxGE failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected jump to 10, PC=%d", v.PC)
		}
	})
}

// TestIdxOpcodeErrors tests error conditions for index opcodes
func TestIdxOpcodeErrors(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(10)
	v.AllocCursors(2)

	t.Run("IdxInsert_NoCursor", func(t *testing.T) {
		t.Parallel()
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
