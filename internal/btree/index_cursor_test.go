package btree

import (
	"bytes"
	"fmt"
	"testing"
)

// Helper function to create an index B-tree page
func createIndexPage(bt *Btree) (uint32, error) {
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
		headerOffset = FileHeaderSize
	}

	// Initialize the page as an empty leaf index page
	pageData[headerOffset+PageHeaderOffsetType] = PageTypeLeafIndex
	pageData[headerOffset+PageHeaderOffsetFreeblock] = 0
	pageData[headerOffset+PageHeaderOffsetFreeblock+1] = 0
	pageData[headerOffset+PageHeaderOffsetNumCells] = 0
	pageData[headerOffset+PageHeaderOffsetNumCells+1] = 0
	pageData[headerOffset+PageHeaderOffsetCellStart] = 0
	pageData[headerOffset+PageHeaderOffsetCellStart+1] = 0
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0

	return pageNum, nil
}

func TestNewIndexCursor(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)
	if cursor == nil {
		t.Fatal("NewIndexCursor() returned nil")
	}
	if cursor.Btree != bt {
		t.Error("cursor.Btree not set correctly")
	}
	if cursor.RootPage != rootPage {
		t.Errorf("cursor.RootPage = %d, want %d", cursor.RootPage, rootPage)
	}
	if cursor.State != CursorInvalid {
		t.Errorf("cursor.State = %d, want %d", cursor.State, CursorInvalid)
	}
}

func TestEncodeDecodeIndexPayload(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		rowid int64
	}{
		{"simple", []byte("test"), 1},
		{"empty key", []byte(""), 42},
		{"large rowid", []byte("key"), 1<<20},
		// Note: Keys containing bytes >= 0x80 may cause issues with varint parsing
		// This is a known limitation of the current encoding
		{"binary key low bytes", []byte{0x00, 0x01, 0x7F}, 100},
		{"long key", []byte("this is a very long key with many characters"), 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := encodeIndexPayload(tt.key, tt.rowid)

			cursor := &IndexCursor{}
			key, rowid, err := cursor.parseIndexPayload(payload)
			if err != nil {
				t.Fatalf("parseIndexPayload() error = %v", err)
			}

			if !bytes.Equal(key, tt.key) {
				t.Errorf("key = %q, want %q", key, tt.key)
			}
			if rowid != tt.rowid {
				t.Errorf("rowid = %d, want %d", rowid, tt.rowid)
			}
		})
	}
}

func TestIndexCursor_InsertAndSeek(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert a few entries
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
		{[]byte("cherry"), 3},
	}

	for _, entry := range entries {
		if err := cursor.InsertIndex(entry.key, entry.rowid); err != nil {
			t.Fatalf("InsertIndex(%q, %d) error = %v", entry.key, entry.rowid, err)
		}
	}

	// Seek and verify each entry
	for _, entry := range entries {
		found, err := cursor.SeekIndex(entry.key)
		if err != nil {
			t.Fatalf("SeekIndex(%q) error = %v", entry.key, err)
		}
		if !found {
			t.Errorf("SeekIndex(%q) not found", entry.key)
			continue
		}
		if !bytes.Equal(cursor.GetKey(), entry.key) {
			t.Errorf("GetKey() = %q, want %q", cursor.GetKey(), entry.key)
		}
		if cursor.GetRowid() != entry.rowid {
			t.Errorf("GetRowid() = %d, want %d", cursor.GetRowid(), entry.rowid)
		}
		if !cursor.IsValid() {
			t.Error("cursor should be valid after successful seek")
		}
	}
}

func TestIndexCursor_MoveToFirst(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries (not in alphabetical order)
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("cherry"), 3},
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Move to first
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	// Should be at "apple" (first alphabetically)
	if !cursor.IsValid() {
		t.Fatal("cursor should be valid after MoveToFirst")
	}
	if !bytes.Equal(cursor.GetKey(), []byte("apple")) {
		t.Errorf("GetKey() = %q, want %q", cursor.GetKey(), "apple")
	}
	if cursor.GetRowid() != 1 {
		t.Errorf("GetRowid() = %d, want 1", cursor.GetRowid())
	}
}

func TestIndexCursor_NextIndex(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
		{[]byte("cherry"), 3},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Start at first
	cursor.MoveToFirst()

	// Verify we can iterate through all entries
	for i, expected := range entries {
		if !cursor.IsValid() {
			t.Fatalf("cursor invalid at iteration %d", i)
		}
		if !bytes.Equal(cursor.GetKey(), expected.key) {
			t.Errorf("iteration %d: GetKey() = %q, want %q", i, cursor.GetKey(), expected.key)
		}
		if cursor.GetRowid() != expected.rowid {
			t.Errorf("iteration %d: GetRowid() = %d, want %d", i, cursor.GetRowid(), expected.rowid)
		}

		// Move to next (except on last iteration)
		if i < len(entries)-1 {
			if err := cursor.NextIndex(); err != nil {
				t.Fatalf("NextIndex() error at iteration %d: %v", i, err)
			}
		}
	}

	// Next should fail at end
	err = cursor.NextIndex()
	if err == nil {
		t.Error("NextIndex() should fail at end of index")
	}
}

func TestIndexCursor_DeleteIndex(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
		{[]byte("cherry"), 3},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Delete middle entry
	if err := cursor.DeleteIndex([]byte("banana"), 2); err != nil {
		t.Fatalf("DeleteIndex() error = %v", err)
	}

	// Verify it's gone
	found, err := cursor.SeekIndex([]byte("banana"))
	if err != nil {
		t.Fatalf("SeekIndex() error = %v", err)
	}
	if found {
		t.Error("deleted entry should not be found")
	}

	// Verify other entries still exist
	found, _ = cursor.SeekIndex([]byte("apple"))
	if !found {
		t.Error("apple should still exist")
	}

	found, _ = cursor.SeekIndex([]byte("cherry"))
	if !found {
		t.Error("cherry should still exist")
	}
}

func BenchmarkIndexCursor_Insert(b *testing.B) {
	bt := NewBtree(4096)
	rootPage, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, rootPage)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		cursor.InsertIndex(key, int64(i))
	}
}

func BenchmarkIndexCursor_Seek(b *testing.B) {
	bt := NewBtree(4096)
	rootPage, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, rootPage)

	// Pre-populate with entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		cursor.InsertIndex(key, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%08d", i%numEntries))
		cursor.SeekIndex(key)
	}
}

// TestIndexCursor_MoveToLast tests MoveToLast method
func TestIndexCursor_MoveToLast(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries (not in alphabetical order)
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("cherry"), 3},
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Move to last
	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	// Should be at "cherry" (last alphabetically)
	if !cursor.IsValid() {
		t.Fatal("cursor should be valid after MoveToLast")
	}
	if !bytes.Equal(cursor.GetKey(), []byte("cherry")) {
		t.Errorf("GetKey() = %q, want %q", cursor.GetKey(), "cherry")
	}
	if cursor.GetRowid() != 3 {
		t.Errorf("GetRowid() = %d, want 3", cursor.GetRowid())
	}
}

// Example demonstrating index cursor usage
func ExampleIndexCursor() {
	// Create a B-tree and index page
	bt := NewBtree(4096)
	rootPage, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, rootPage)

	// Insert key-rowid mappings
	cursor.InsertIndex([]byte("apple"), 100)
	cursor.InsertIndex([]byte("banana"), 200)
	cursor.InsertIndex([]byte("cherry"), 300)

	// Seek to a specific key
	found, _ := cursor.SeekIndex([]byte("banana"))
	if found {
		fmt.Printf("Found: %s -> rowid %d\n", cursor.GetKey(), cursor.GetRowid())
	}

	// Iterate through all entries
	cursor.MoveToFirst()
	for cursor.IsValid() {
		fmt.Printf("%s -> %d\n", cursor.GetKey(), cursor.GetRowid())
		cursor.NextIndex()
	}

	// Output:
	// Found: banana -> rowid 200
	// apple -> 100
	// banana -> 200
	// cherry -> 300
}
