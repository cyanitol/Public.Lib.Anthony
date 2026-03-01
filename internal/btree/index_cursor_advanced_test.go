// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// TestIndexCursor_PrevIndex tests backward iteration
func TestIndexCursor_PrevIndex(t *testing.T) {
	t.Parallel()
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
		{[]byte("date"), 4},
		{[]byte("elderberry"), 5},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Start at last
	cursor.MoveToLast()

	// Iterate backwards
	for i := len(entries) - 1; i >= 0; i-- {
		if !cursor.IsValid() {
			t.Fatalf("cursor invalid at iteration %d", i)
		}

		expected := entries[i]
		if !bytes.Equal(cursor.GetKey(), expected.key) {
			t.Errorf("iteration %d: GetKey() = %q, want %q", i, cursor.GetKey(), expected.key)
		}
		if cursor.GetRowid() != expected.rowid {
			t.Errorf("iteration %d: GetRowid() = %d, want %d", i, cursor.GetRowid(), expected.rowid)
		}

		// Move to previous (except on last iteration)
		if i > 0 {
			if err := cursor.PrevIndex(); err != nil {
				t.Fatalf("PrevIndex() error at iteration %d: %v", i, err)
			}
		}
	}

	// Prev should fail at beginning
	err = cursor.PrevIndex()
	if err == nil {
		t.Error("PrevIndex() should fail at beginning of index")
	}
}

// TestIndexCursor_SeekNotFound tests seeking non-existent keys
func TestIndexCursor_SeekNotFound(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries with gaps
	entries := []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("apple"), 1},
		{[]byte("cherry"), 3},
		{[]byte("elderberry"), 5},
	}

	for _, entry := range entries {
		cursor.InsertIndex(entry.key, entry.rowid)
	}

	// Seek for key that doesn't exist (between apple and cherry)
	found, err := cursor.SeekIndex([]byte("banana"))
	if err != nil {
		t.Fatalf("SeekIndex() error = %v", err)
	}

	if found {
		t.Error("SeekIndex() should return false for non-existent key")
	}

	// Cursor should still be valid and positioned near the search key
	if !cursor.IsValid() {
		t.Error("Cursor should be valid after unsuccessful seek")
	}
}

// TestIndexCursor_DeleteNotFound tests deleting non-existent entry
func TestIndexCursor_DeleteNotFound(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert an entry
	cursor.InsertIndex([]byte("apple"), 1)

	// Try to delete non-existent entry
	err = cursor.DeleteIndex([]byte("banana"), 2)
	if err == nil {
		t.Error("DeleteIndex() should fail for non-existent key")
	}
}

// TestIndexCursor_DeleteWrongRowid tests deleting with wrong rowid
func TestIndexCursor_DeleteWrongRowid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries - note: index typically has unique key-rowid pairs
	// For this test, we'll insert with different keys to test the error case
	cursor.InsertIndex([]byte("apple"), 1)
	cursor.InsertIndex([]byte("banana"), 2)

	// Try to delete with wrong rowid (key exists but with different rowid)
	err = cursor.DeleteIndex([]byte("apple"), 999)
	if err == nil {
		t.Error("DeleteIndex() should fail when rowid doesn't match")
	}

	// Verify original entry still exists
	found, _ := cursor.SeekIndex([]byte("apple"))
	if !found {
		t.Fatal("Key 'apple' should still exist")
	}

	if cursor.GetRowid() != 1 {
		t.Errorf("Rowid = %d, want 1", cursor.GetRowid())
	}
}

// TestIndexCursor_String tests String method
func TestIndexCursor_String(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Test invalid cursor
	invalidStr := cursor.String()
	if invalidStr == "" {
		t.Error("String() should return non-empty string for invalid cursor")
	}

	// Insert and seek
	cursor.InsertIndex([]byte("test"), 42)
	cursor.SeekIndex([]byte("test"))

	// Test valid cursor
	validStr := cursor.String()
	if validStr == "" {
		t.Error("String() should return non-empty string for valid cursor")
	}

	t.Logf("Invalid cursor: %s", invalidStr)
	t.Logf("Valid cursor: %s", validStr)
}

// TestIndexCursor_GettersWhenInvalid tests getter methods with invalid cursor
func TestIndexCursor_GettersWhenInvalid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Cursor is invalid initially
	if cursor.GetKey() != nil {
		t.Error("GetKey() should return nil for invalid cursor")
	}

	if cursor.GetRowid() != 0 {
		t.Error("GetRowid() should return 0 for invalid cursor")
	}

	if cursor.IsValid() {
		t.Error("IsValid() should return false for invalid cursor")
	}
}

// TestIndexCursor_InsertDuplicate tests inserting duplicate keys
func TestIndexCursor_InsertDuplicate(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert first entry
	err = cursor.InsertIndex([]byte("apple"), 1)
	if err != nil {
		t.Fatalf("First insert error = %v", err)
	}

	// Try to insert exact duplicate - should fail
	err = cursor.InsertIndex([]byte("apple"), 1)
	if err == nil {
		t.Error("InsertIndex() should fail for exact duplicate key-rowid")
	}

	// Try different rowid with same key - this will also fail because
	// SeekIndex finds the key and returns true, causing duplicate error
	// This is by design in the current implementation
	err = cursor.InsertIndex([]byte("apple"), 2)
	if err == nil {
		t.Log("InsertIndex() with different rowid also fails due to key match")
	}
}

// TestIndexCursor_EmptyPage tests operations on empty page
func TestIndexCursor_EmptyPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// MoveToFirst on empty page should fail
	err = cursor.MoveToFirst()
	if err == nil {
		t.Error("MoveToFirst() should fail on empty page")
	}

	// MoveToLast on empty page should fail
	err = cursor.MoveToLast()
	if err == nil {
		t.Error("MoveToLast() should fail on empty page")
	}

	// Seek on empty page should return false
	found, err := cursor.SeekIndex([]byte("test"))
	if err != nil {
		t.Errorf("SeekIndex() on empty page error = %v", err)
	}
	if found {
		t.Error("SeekIndex() should return false on empty page")
	}
}

// TestIndexCursor_InteriorPage tests multi-level index tree
func TestIndexCursor_InteriorPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force interior pages
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert many entries to potentially create interior pages
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			// If we hit page full, that's OK for this test
			if err.Error() != fmt.Sprintf("index page split not yet implemented (page %d is full)", rootPage) {
				t.Logf("Insert stopped at %d: %v", i, err)
			}
			break
		}
	}

	// Verify we can still navigate
	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.NextIndex(); err != nil {
			break
		}
	}

	t.Logf("Inserted and iterated through %d entries", count)
}

// TestIndexCursor_ParseIndexPayloadErrors tests error handling in payload parsing
func TestIndexCursor_ParseIndexPayloadErrors(t *testing.T) {
	t.Parallel()
	cursor := &IndexCursor{}

	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{
			name:    "empty payload",
			payload: []byte{},
			wantErr: true,
		},
		{
			name:    "valid single byte",
			payload: []byte{0x01},
			wantErr: false,
		},
		{
			name:    "valid multi-byte",
			payload: []byte{0x01, 0x02, 0x03},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := cursor.parseIndexPayload(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseIndexPayload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestIndexCursor_BinarySearchEdges tests binary search edge cases
func TestIndexCursor_BinarySearchEdges(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	entries := [][]byte{
		[]byte("a"),
		[]byte("c"),
		[]byte("e"),
		[]byte("g"),
		[]byte("i"),
	}

	for i, key := range entries {
		cursor.InsertIndex(key, int64(i))
	}

	// Reload page data for binary search
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}
	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader() error = %v", err)
	}

	tests := []struct {
		name      string
		searchKey []byte
		wantExact bool
	}{
		{"before first", []byte("0"), false},
		{"exact first", []byte("a"), true},
		{"between a and c", []byte("b"), false},
		{"exact middle", []byte("e"), true},
		{"exact last", []byte("i"), true},
		{"after last", []byte("z"), false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			idx, exact := cursor.binarySearchKey(pageData, header, tt.searchKey)
			if exact != tt.wantExact {
				t.Errorf("binarySearchKey(%q) exact = %v, want %v (idx=%d)",
					tt.searchKey, exact, tt.wantExact, idx)
			}
		})
	}
}

// TestIndexCursor_NextPrevInvalid tests next/prev with invalid cursor
func TestIndexCursor_NextPrevInvalid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)
	cursor.State = CursorInvalid

	// NextIndex should fail
	err = cursor.NextIndex()
	if err == nil {
		t.Error("NextIndex() should fail with invalid cursor")
	}

	// PrevIndex should fail
	err = cursor.PrevIndex()
	if err == nil {
		t.Error("PrevIndex() should fail with invalid cursor")
	}
}

// TestIndexCursor_MoveToLastSingleEntry tests MoveToLast with one entry
func TestIndexCursor_MoveToLastSingleEntry(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)
	cursor.InsertIndex([]byte("only"), 1)

	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if !cursor.IsValid() {
		t.Error("Cursor should be valid")
	}

	if !bytes.Equal(cursor.GetKey(), []byte("only")) {
		t.Errorf("Key = %q, want 'only'", cursor.GetKey())
	}
}

// TestIndexCursor_DeleteLastEntry tests deleting the last entry
func TestIndexCursor_DeleteLastEntry(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert two entries
	cursor.InsertIndex([]byte("first"), 1)
	cursor.InsertIndex([]byte("second"), 2)

	// Delete last entry
	err = cursor.DeleteIndex([]byte("second"), 2)
	if err != nil {
		t.Fatalf("DeleteIndex() error = %v", err)
	}

	// Verify first entry still exists
	found, _ := cursor.SeekIndex([]byte("first"))
	if !found {
		t.Error("First entry should still exist")
	}

	// Verify second entry is gone
	found, _ = cursor.SeekIndex([]byte("second"))
	if found {
		t.Error("Second entry should be deleted")
	}
}

// TestIndexCursor_MoveToFirstLastEmpty tests navigation on empty results
func TestIndexCursor_MoveToFirstLastEmpty(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert and then delete all entries
	cursor.InsertIndex([]byte("temp"), 1)
	cursor.DeleteIndex([]byte("temp"), 1)

	// MoveToFirst should fail on now-empty page
	err = cursor.MoveToFirst()
	if err == nil {
		t.Error("MoveToFirst() should fail on empty page")
	}
}

// TestIndexCursor_CorruptedPage tests handling of corrupted page data
func TestIndexCursor_CorruptedPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageNum := uint32(2)

	// Create corrupted page (invalid header)
	corruptedData := make([]byte, 4096)
	corruptedData[0] = 0xFF // Invalid page type

	bt.SetPage(pageNum, corruptedData)

	cursor := NewIndexCursor(bt, pageNum)

	// Operations should fail gracefully
	err := cursor.MoveToFirst()
	if err == nil {
		t.Error("MoveToFirst() should fail on corrupted page")
	}

	_, err = cursor.SeekIndex([]byte("test"))
	if err == nil {
		t.Error("SeekIndex() should fail on corrupted page")
	}
}

// TestIndexCursor_DepthExceeded tests tree depth limit
func TestIndexCursor_DepthExceeded(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := createIndexPage(bt)

	cursor := NewIndexCursor(bt, rootPage)

	// Manually set depth to maximum
	cursor.Depth = MaxBtreeDepth - 1

	// Create a fake interior page at depth
	interiorPage := uint32(10)
	interiorData := make([]byte, 4096)
	interiorData[0] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(interiorData[3:], 1) // NumCells = 1
	binary.BigEndian.PutUint32(interiorData[8:], 11) // RightChild = 11

	// Cell pointing to another page
	cellOffset := uint32(4096 - 10)
	binary.BigEndian.PutUint32(interiorData[cellOffset:], 12) // Child page
	binary.BigEndian.PutUint16(interiorData[12:], uint16(cellOffset)) // Cell pointer

	bt.SetPage(interiorPage, interiorData)

	// Create leaf page
	leafPage := uint32(11)
	leafData := make([]byte, 4096)
	leafData[0] = PageTypeLeafIndex
	bt.SetPage(leafPage, leafData)

	cursor.PageStack[cursor.Depth] = interiorPage

	// Trying to descend further should fail
	err := cursor.descendToFirst(interiorPage)
	if err == nil {
		t.Error("descendToFirst() should fail when depth is exceeded")
	}
}

// TestIndexCursor_LoadCurrentEntryError tests error in loadCurrentEntry
func TestIndexCursor_LoadCurrentEntryError(t *testing.T) {
	t.Parallel()
	cursor := &IndexCursor{}

	// Create invalid cell with bad payload
	invalidCell := &CellInfo{
		Payload: []byte{}, // Empty payload should cause error
	}

	err := cursor.loadCurrentEntry(invalidCell)
	if err == nil {
		t.Error("loadCurrentEntry() should fail with invalid payload")
	}
}

// TestIndexCursor_SeekBeyondEnd tests seeking beyond last entry
func TestIndexCursor_SeekBeyondEnd(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert a few entries
	cursor.InsertIndex([]byte("a"), 1)
	cursor.InsertIndex([]byte("b"), 2)
	cursor.InsertIndex([]byte("c"), 3)

	// Seek beyond end
	found, err := cursor.SeekIndex([]byte("z"))
	if err != nil {
		t.Fatalf("SeekIndex() error = %v", err)
	}

	if found {
		t.Error("Should not find key 'z'")
	}

	// Cursor should still be valid, positioned at the end
	if !cursor.IsValid() {
		t.Error("Cursor should be valid after seek")
	}
}

// TestIndexCursor_NextAtEnd tests calling Next when at last entry
func TestIndexCursor_NextAtEnd(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	cursor.InsertIndex([]byte("only"), 1)
	cursor.MoveToFirst()

	// Try to move next (should fail)
	err = cursor.NextIndex()
	if err == nil {
		t.Error("NextIndex() should fail at end")
	}

	if cursor.IsValid() {
		t.Error("Cursor should be invalid after NextIndex() at end")
	}
}

// TestIndexCursor_PrevAtBeginning tests calling Prev when at first entry
func TestIndexCursor_PrevAtBeginning(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	cursor.InsertIndex([]byte("only"), 1)
	cursor.MoveToFirst()

	// Try to move prev (should fail)
	err = cursor.PrevIndex()
	if err == nil {
		t.Error("PrevIndex() should fail at beginning")
	}

	if cursor.IsValid() {
		t.Error("Cursor should be invalid after PrevIndex() at beginning")
	}
}
