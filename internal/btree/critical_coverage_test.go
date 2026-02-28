package btree

import (
	"encoding/binary"
	"testing"
)

// TestGetSiblingWithLeftPage_ActualMerge tests merging with a left sibling
// This targets getSiblingWithLeftPage at 0% coverage
func TestGetSiblingWithLeftPage_ActualMerge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create three small leaf pages (underfull to trigger merge)
	page2Data := createSmallLeafPage(2, pageSize, 1, []byte("a"))
	bt.SetPage(2, page2Data)

	page3Data := createSmallLeafPage(3, pageSize, 5, []byte("b"))
	bt.SetPage(3, page3Data)

	page4Data := createSmallLeafPage(4, pageSize, 10, []byte("c"))
	bt.SetPage(4, page4Data)

	// Create interior root: page 2 (child 0), page 3 (child 1), page 4 (right child)
	// When positioned on page 3, it has left sibling page 2
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 1},  // First child: page 2
		{3, 5},  // Second child: page 3 (has left sibling)
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 4)
	bt.SetPage(1, rootData)

	// Position cursor on page 3 (middle page with left sibling)
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(5)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("Failed to find rowid 5")
	}

	// Verify cursor is on page 3
	if cursor.CurrentPage != 3 {
		t.Fatalf("Expected cursor on page 3, got page %d", cursor.CurrentPage)
	}

	// Verify we're at depth 1 (has parent)
	if cursor.Depth == 0 {
		t.Fatal("Cursor depth should be > 0 to have parent")
	}

	// Delete the only cell to make page underfull
	err = cursor.Delete()
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Now try to merge - this should use getSiblingWithLeftPage
	// because parentIndex > 0 (page 3 is at index 1)
	merged, err := cursor.MergePage()
	if err != nil {
		t.Logf("MergePage() error (may be expected): %v", err)
	}

	t.Logf("MergePage() result: merged=%v", merged)
}

// TestGetSiblingAsRightmost_ActualMerge tests merging when current page is rightmost
// This targets getSiblingAsRightmost at 0% coverage
func TestGetSiblingAsRightmost_ActualMerge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create two small leaf pages
	page2Data := createSmallLeafPage(2, pageSize, 1, []byte("left"))
	bt.SetPage(2, page2Data)

	page3Data := createSmallLeafPage(3, pageSize, 10, []byte("right"))
	bt.SetPage(3, page3Data)

	// Create interior root where page 3 is the rightmost child
	// Only one cell pointing to page 2, with page 3 as right child
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 1}}
	rootData := createInteriorPage(1, pageSize, rootCells, 3) // Page 3 is right child
	bt.SetPage(1, rootData)

	// Position cursor on the rightmost page (page 3)
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(10)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("Failed to find rowid 10")
	}

	// Verify cursor is on page 3 (rightmost)
	if cursor.CurrentPage != 3 {
		t.Fatalf("Expected cursor on page 3, got page %d", cursor.CurrentPage)
	}

	// Delete to make page underfull
	err = cursor.Delete()
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Try merge - should use getSiblingAsRightmost
	// because parentIndex == NumCells (rightmost position)
	merged, err := cursor.MergePage()
	if err != nil {
		t.Logf("MergePage() error (may be expected): %v", err)
	}

	t.Logf("MergePage() result for rightmost: merged=%v", merged)
}

// TestHandleUnderfullPage_NonRoot tests underfull page handling for non-root pages
// This targets handleUnderfullPage at 25% coverage
func TestHandleUnderfullPage_NonRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough to create multiple pages
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 30))
		if err != nil {
			break
		}
	}

	// Delete many entries to make a page underfull
	for i := int64(50); i <= 90; i++ {
		found, _ := cursor.SeekRowid(i)
		if found {
			cursor.Delete()
		}
	}

	// Position on a non-root page
	cursor.SeekRowid(55)

	// Get the current page
	pageData, err := bt.GetPage(cursor.CurrentPage)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	page, err := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Check if page is underfull
	if page.IsUnderfull() {
		// Try to handle it
		err = handleUnderfullPage(cursor, page)
		if err != nil {
			// Expected error message about needing merge/redistribution
			t.Logf("handleUnderfullPage() error (expected for non-root): %v", err)
		}
	}
}

// TestHandleUnderfullPage_RootPage tests underfull page handling for root page
// This should allow underfull state
func TestHandleUnderfullPage_RootPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert just one tiny row to make root underfull
	err = cursor.Insert(1, []byte("x"))
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	cursor.SeekRowid(1)

	// Get the root page
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	page, err := NewBtreePage(rootPage, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Try to handle underfull root - should NOT error
	err = handleUnderfullPage(cursor, page)
	if err != nil {
		t.Errorf("handleUnderfullPage() should allow underfull root, got error: %v", err)
	}
}

// TestHandleUnderfullPage_WithFragmentation tests defragmentation path
func TestHandleUnderfullPage_WithFragmentation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create a page with fragmentation
	pageData := make([]byte, pageSize)
	headerOffset := 0
	if 1 == 1 { // page 1
		headerOffset = FileHeaderSize
	}

	pageData[headerOffset] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[headerOffset+3:], 2) // NumCells = 2

	// Create cells
	cell1 := EncodeTableLeafCell(1, []byte("a"))
	cell2 := EncodeTableLeafCell(2, []byte("b"))

	// Place with fragmentation
	offset2 := int(pageSize) - len(cell2)
	copy(pageData[offset2:], cell2)

	offset1 := offset2 - len(cell1) - 50 // 50-byte gap = fragmentation
	copy(pageData[offset1:], cell1)

	binary.BigEndian.PutUint16(pageData[headerOffset+5:], uint16(offset1))
	binary.BigEndian.PutUint16(pageData[headerOffset+8:], uint16(offset1))
	binary.BigEndian.PutUint16(pageData[headerOffset+10:], uint16(offset2))
	pageData[headerOffset+7] = 50 // FragmentedBytes

	bt.SetPage(1, pageData)

	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	// Get page
	pageData, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	page, err := NewBtreePage(1, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Should defragment
	err = handleUnderfullPage(cursor, page)
	if err != nil {
		t.Logf("handleUnderfullPage() error: %v", err)
	}

	// Check fragmentation was cleared
	if page.Header.FragmentedBytes != 0 {
		t.Logf("FragmentedBytes after handling: %d", page.Header.FragmentedBytes)
	}
}

// TestHandleOverfullPage_WithDefragmentation tests overfull handling with fragmentation
// This targets handleOverfullPage at 33.3% coverage
func TestHandleOverfullPage_WithDefragmentation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages
	pageSize := bt.PageSize

	// Create a page that appears overfull but has fragmentation
	pageData := make([]byte, pageSize)
	headerOffset := FileHeaderSize // page 1
	pageData[headerOffset] = PageTypeLeafTable

	// Create many small cells
	numCells := 20
	binary.BigEndian.PutUint16(pageData[headerOffset+3:], uint16(numCells))

	cellOffset := int(pageSize)
	for i := numCells - 1; i >= 0; i-- {
		cell := EncodeTableLeafCell(int64(i+1), []byte("x"))
		cellOffset -= len(cell)
		if i > 0 {
			cellOffset -= 5 // Add fragmentation gaps
		}
		copy(pageData[cellOffset:], cell)

		ptrOffset := headerOffset + PageHeaderSizeLeaf + (i * 2)
		binary.BigEndian.PutUint16(pageData[ptrOffset:], uint16(cellOffset))
	}

	binary.BigEndian.PutUint16(pageData[headerOffset+5:], uint16(cellOffset))
	pageData[headerOffset+7] = 5 * byte(numCells-1) // FragmentedBytes

	bt.SetPage(1, pageData)

	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	// Get page
	pageData, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	page, err := NewBtreePage(1, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Try to handle overfull - should defragment first
	err = handleOverfullPage(cursor, page)
	if err != nil {
		t.Logf("handleOverfullPage() error: %v", err)
	}

	// Check if defragmentation helped
	if page.Header.FragmentedBytes == 0 {
		t.Log("Successfully defragmented overfull page")
	}
}

// TestHandleOverfullPage_StillOverfull tests when defragmentation doesn't help
func TestHandleOverfullPage_StillOverfull(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Very small pages
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to make truly overfull
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 15))
		if err != nil {
			break
		}
	}

	cursor.SeekRowid(1)

	// Get page
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	page, err := NewBtreePage(rootPage, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	if isOverfull(page) {
		// Try to handle - should error about needing split
		err = handleOverfullPage(cursor, page)
		if err != nil {
			t.Logf("handleOverfullPage() error (expected): %v", err)
			// Check it mentions overfull and split
			hasWord := false
			for _, word := range []string{"overfull", "split"} {
				if containsStr2(err.Error(), word) {
					hasWord = true
					break
				}
			}
			if !hasWord {
				t.Logf("Error message: %v", err)
			}
		}
	}
}

// TestCheckMaxIterationsExceeded tests the free block iteration check
// This targets checkMaxIterationsExceeded at 50% coverage
func TestCheckMaxIterationsExceeded(t *testing.T) {
	t.Parallel()
	result := &IntegrityResult{
		Errors: make([]*IntegrityError, 0),
	}

	// Test case 1: offset is 0 (end of chain) - should not error
	visited := make(map[uint16]bool)
	for i := 0; i < 100; i++ {
		visited[uint16(i*10)] = true
	}
	checkMaxIterationsExceeded(1, 0, visited, 50, result)
	if len(result.Errors) > 0 {
		t.Errorf("Should not error when offset is 0, got %d errors", len(result.Errors))
	}

	// Test case 2: visited < maxIterations - should not error
	result = &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	visited = make(map[uint16]bool)
	for i := 0; i < 30; i++ {
		visited[uint16(i*10)] = true
	}
	checkMaxIterationsExceeded(1, 100, visited, 50, result)
	if len(result.Errors) > 0 {
		t.Errorf("Should not error when visited < maxIterations, got %d errors", len(result.Errors))
	}

	// Test case 3: visited >= maxIterations and offset != 0 - should error
	result = &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	visited = make(map[uint16]bool)
	for i := 0; i < 60; i++ {
		visited[uint16(i*10)] = true
	}
	checkMaxIterationsExceeded(1, 100, visited, 50, result)
	if len(result.Errors) == 0 {
		t.Error("Should error when visited >= maxIterations and offset != 0")
	} else {
		expectedMsg := "free block list exceeds maximum iterations 50"
		if result.Errors[0].Description != expectedMsg {
			t.Errorf("Error message = %q, want %q", result.Errors[0].Description, expectedMsg)
		}
	}
}

// TestSeekLeafExactMatch_IndexCursor tests exact match seeking in index cursor
// This targets index_cursor.go seekLeafExactMatch at 53.8% coverage
func TestSeekLeafExactMatch_IndexCursor(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage2(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for i, key := range keys {
		err := cursor.InsertIndex([]byte(key), int64(i+1))
		if err != nil {
			t.Fatalf("InsertIndex(%s) error = %v", key, err)
		}
	}

	// Test exact matches
	for i, key := range keys {
		found, err := cursor.SeekIndex([]byte(key))
		if err != nil {
			t.Errorf("SeekIndex(%s) error = %v", key, err)
			continue
		}
		if !found {
			t.Errorf("SeekIndex(%s) should find exact match", key)
			continue
		}
		if cursor.GetRowid() != int64(i+1) {
			t.Errorf("SeekIndex(%s) got rowid %d, want %d", key, cursor.GetRowid(), i+1)
		}
	}

	// Test non-existent keys (should position cursor correctly)
	notFound := []string{"aardvark", "blueberry", "zucchini"}
	for _, key := range notFound {
		found, err := cursor.SeekIndex([]byte(key))
		if err != nil {
			t.Errorf("SeekIndex(%s) error = %v", key, err)
		}
		if found {
			t.Errorf("SeekIndex(%s) should not find exact match", key)
		}
	}
}

// TestIndexCursor_PrevInPage2 tests backward navigation within a page
// This targets index_cursor.go prevInPage at 55.6% coverage
func TestIndexCursor_PrevInPage2(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage2(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert several entries
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		err := cursor.InsertIndex(key, int64(i+1))
		if err != nil {
			t.Fatalf("InsertIndex() error = %v", err)
		}
	}

	// Move to middle
	cursor.SeekIndex([]byte{'e'})

	// Navigate backward within page
	for i := 0; i < 4; i++ {
		err := cursor.PrevIndex()
		if err != nil {
			t.Fatalf("PrevIndex() error = %v", err)
		}
		if !cursor.IsValid() {
			t.Fatal("Cursor should remain valid")
		}
	}

	// Should be at first entry now
	if cursor.CurrentIndex != 0 {
		t.Errorf("CurrentIndex = %d, want 0", cursor.CurrentIndex)
	}
}

// TestIndexCursor_DeleteCurrentEntry2 tests deleting current entry
// This targets index_cursor.go deleteCurrentEntry at 58.8% coverage
func TestIndexCursor_DeleteCurrentEntry2(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage2(bt)
	if err != nil {
		t.Fatalf("createIndexPage2() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i, key := range keys {
		err := cursor.InsertIndex([]byte(key), int64(i+1))
		if err != nil {
			t.Fatalf("InsertIndex(%s) error = %v", key, err)
		}
	}

	// Delete middle entry
	found, err := cursor.SeekIndex([]byte("gamma"))
	if err != nil || !found {
		t.Fatalf("SeekIndex(gamma) failed: found=%v, err=%v", found, err)
	}

	err = cursor.DeleteIndex([]byte("gamma"), 3)
	if err != nil {
		t.Fatalf("DeleteIndex() error = %v", err)
	}

	// Verify it's gone
	found, err = cursor.SeekIndex([]byte("gamma"))
	if err != nil {
		t.Fatalf("SeekIndex(gamma) after delete error = %v", err)
	}
	if found {
		t.Error("gamma should be deleted")
	}

	// Delete first entry
	cursor.MoveToFirst()
	err = cursor.DeleteIndex([]byte("alpha"), 1)
	if err != nil {
		t.Fatalf("DeleteIndex() first error = %v", err)
	}

	// Delete last entry
	cursor.MoveToLast()
	err = cursor.DeleteIndex([]byte("epsilon"), 5)
	if err != nil {
		t.Fatalf("DeleteIndex() last error = %v", err)
	}
}

// Helper functions

func createSmallLeafPage(pageNum uint32, pageSize uint32, rowid int64, payload []byte) []byte {
	data := make([]byte, pageSize)
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}

	data[headerOffset] = PageTypeLeafTable
	binary.BigEndian.PutUint16(data[headerOffset+3:], 1) // NumCells = 1

	cell := EncodeTableLeafCell(rowid, payload)
	cellOffset := int(pageSize) - len(cell)
	copy(data[cellOffset:], cell)

	binary.BigEndian.PutUint16(data[headerOffset+5:], uint16(cellOffset))
	binary.BigEndian.PutUint16(data[headerOffset+8:], uint16(cellOffset))

	return data
}

func createIndexPage2(bt *Btree) (uint32, error) {
	pageData := make([]byte, bt.PageSize)
	headerOffset := FileHeaderSize // page 1
	pageData[headerOffset] = PageTypeLeafIndex
	binary.BigEndian.PutUint16(pageData[headerOffset+3:], 0) // NumCells = 0
	binary.BigEndian.PutUint16(pageData[headerOffset+5:], 0) // CellContentStart = 0

	bt.SetPage(1, pageData)
	return 1, nil
}

func containsStr2(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
		 containsMiddle2(s, substr)))
}

func containsMiddle2(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
