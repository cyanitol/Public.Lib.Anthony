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
		result := compareBytes(tt.a, tt.b)
		if (result < 0 && tt.expect >= 0) || (result > 0 && tt.expect <= 0) || (result == 0 && tt.expect != 0) {
			t.Errorf("compareBytes(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expect)
		}
	}
}
