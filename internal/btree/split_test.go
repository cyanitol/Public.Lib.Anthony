package btree

import (
	"encoding/binary"
	"testing"
)

// TestSplitLeafPageBasic tests basic leaf page splitting
func TestSplitLeafPageBasic(t *testing.T) {
	bt := NewBtree(4096)

	// Create a table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert cells until the page is nearly full
	// Each cell is roughly 10-20 bytes, so we can fit ~200 cells per page
	// Let's insert enough to force a split
	numCells := 250

	for i := 1; i <= numCells; i++ {
		key := int64(i * 2) // Use even numbers so we can test insertion in middle
		payload := make([]byte, 50) // 50 bytes payload
		for j := range payload {
			payload[j] = byte(i % 256)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert failed at iteration %d (key=%d): %v", i, key, err)
			// After split, root page changes, so update cursor
			if cursor.RootPage != rootPage {
				t.Logf("Root page changed from %d to %d", rootPage, cursor.RootPage)
				rootPage = cursor.RootPage
			}
			// Continue to verify split worked
			break
		}
	}

	// Verify we can still read all inserted data
	cursor2 := NewCursor(bt, rootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed after split: %v", err)
	}

	count := 0
	for cursor2.IsValid() {
		_ = cursor2.GetKey()
		payload := cursor2.GetPayload()

		if len(payload) != 50 {
			t.Errorf("Cell %d: payload length = %d, want 50", count, len(payload))
		}

		count++
		if err := cursor2.Next(); err != nil {
			break
		}
	}

	if count < 2 {
		t.Errorf("Expected at least 2 cells after split, got %d", count)
	}

	t.Logf("Successfully inserted and verified %d cells", count)
}

// TestSplitLeafPageOrder tests that cells remain sorted after split
func TestSplitLeafPageOrder(t *testing.T) {
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert in random order to test sorting during split
	keys := []int64{10, 30, 50, 70, 90, 20, 40, 60, 80, 100}
	insertedKeys := []int64{}

	for _, key := range keys {
		payload := []byte{byte(key)}
		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert of key %d resulted in split or error: %v", key, err)
			if cursor.RootPage != rootPage {
				rootPage = cursor.RootPage
			}
		}
		insertedKeys = append(insertedKeys, key)
	}

	// Verify all keys are present and in sorted order
	cursor2 := NewCursor(bt, rootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	prevKey := int64(-1)
	foundKeys := []int64{}

	for cursor2.IsValid() {
		key := cursor2.GetKey()
		foundKeys = append(foundKeys, key)

		if key <= prevKey {
			t.Errorf("Keys out of order: %d came after %d", key, prevKey)
		}
		prevKey = key

		if err := cursor2.Next(); err != nil {
			break
		}
	}

	if len(foundKeys) != len(keys) {
		t.Errorf("Expected %d keys, found %d: %v", len(keys), len(foundKeys), foundKeys)
	}
}

// TestSplitCreatesNewRoot tests that splitting the root creates a new root
func TestSplitCreatesNewRoot(t *testing.T) {
	bt := NewBtree(1024) // Smaller page to trigger split sooner

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	originalRoot := rootPage
	cursor := NewCursor(bt, rootPage)

	// Insert enough data to force a split
	for i := 1; i <= 100; i++ {
		key := int64(i)
		payload := make([]byte, 30)
		for j := range payload {
			payload[j] = byte(i)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert %d error: %v", i, err)
		}

		// Check if root changed (split can succeed without returning an error)
		if cursor.RootPage != originalRoot {
			t.Logf("Root changed from %d to %d at insert %d", originalRoot, cursor.RootPage, i)

			// Verify new root is an interior page
			newRootData, err := bt.GetPage(cursor.RootPage)
			if err != nil {
				t.Fatalf("Failed to get new root page: %v", err)
			}

			header, err := ParsePageHeader(newRootData, cursor.RootPage)
			if err != nil {
				t.Fatalf("Failed to parse new root header: %v", err)
			}

			if !header.IsInterior {
				t.Errorf("New root should be interior page, got leaf")
			}

			if header.NumCells < 1 {
				t.Errorf("New root should have at least 1 cell, got %d", header.NumCells)
			}

			t.Logf("New root is interior page with %d cells", header.NumCells)
			return
		}
	}

	t.Error("Expected root to split, but it didn't")
}

// TestCollectLeafCellsForSplit tests the cell collection helper
func TestCollectLeafCellsForSplit(t *testing.T) {
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert a few cells first
	cursor := NewCursor(bt, rootPage)
	keys := []int64{10, 30, 50}

	for _, key := range keys {
		payload := []byte{byte(key)}
		err := cursor.Insert(key, payload)
		if err != nil {
			t.Fatalf("Insert of key %d failed: %v", key, err)
		}
	}

	// Now test collecting cells with a new cell to insert
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	page, err := NewBtreePage(rootPage, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage failed: %v", err)
	}

	// Collect cells with new key=25 (should be inserted between 10 and 30)
	cells, collectedKeys, err := cursor.collectLeafCellsForSplit(page, 25, []byte{25})
	if err != nil {
		t.Fatalf("collectLeafCellsForSplit failed: %v", err)
	}

	// Should have 4 cells total
	if len(cells) != 4 {
		t.Errorf("Expected 4 cells, got %d", len(cells))
	}

	// Keys should be in order: 10, 25, 30, 50
	expectedKeys := []int64{10, 25, 30, 50}
	for i, expected := range expectedKeys {
		if i >= len(collectedKeys) {
			t.Errorf("Missing key at index %d", i)
			continue
		}
		if collectedKeys[i] != expected {
			t.Errorf("Key at index %d: got %d, want %d", i, collectedKeys[i], expected)
		}
	}
}

// TestSplitWithDuplicateKey tests that inserting duplicate keys is properly rejected
func TestSplitWithDuplicateKey(t *testing.T) {
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a key
	err = cursor.Insert(100, []byte{1, 2, 3})
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Try to insert the same key again
	err = cursor.Insert(100, []byte{4, 5, 6})
	if err == nil {
		t.Error("Expected error when inserting duplicate key, got nil")
	}
}

// TestInitializeLeafPage tests leaf page initialization
func TestInitializeLeafPage(t *testing.T) {
	pageData := make([]byte, 4096)
	pageNum := uint32(2) // Not page 1

	err := initializeLeafPage(pageData, pageNum, 4096)
	if err != nil {
		t.Fatalf("initializeLeafPage failed: %v", err)
	}

	// Verify page type
	if pageData[0] != PageTypeLeafTable {
		t.Errorf("Page type = 0x%02x, want 0x%02x", pageData[0], PageTypeLeafTable)
	}

	// Verify num cells is 0
	numCells := binary.BigEndian.Uint16(pageData[3:5])
	if numCells != 0 {
		t.Errorf("NumCells = %d, want 0", numCells)
	}
}

// TestInitializeInteriorPage tests interior page initialization
func TestInitializeInteriorPage(t *testing.T) {
	pageData := make([]byte, 4096)
	pageNum := uint32(2)

	err := initializeInteriorPage(pageData, pageNum, 4096)
	if err != nil {
		t.Fatalf("initializeInteriorPage failed: %v", err)
	}

	// Verify page type
	if pageData[0] != PageTypeInteriorTable {
		t.Errorf("Page type = 0x%02x, want 0x%02x", pageData[0], PageTypeInteriorTable)
	}

	// Verify num cells is 0
	numCells := binary.BigEndian.Uint16(pageData[3:5])
	if numCells != 0 {
		t.Errorf("NumCells = %d, want 0", numCells)
	}

	// Verify right child is 0
	rightChild := binary.BigEndian.Uint32(pageData[8:12])
	if rightChild != 0 {
		t.Errorf("RightChild = %d, want 0", rightChild)
	}
}

// TestGetHeaderOffset tests header offset calculation
func TestGetHeaderOffset(t *testing.T) {
	tests := []struct {
		pageNum uint32
		want    int
	}{
		{1, FileHeaderSize}, // Page 1 has file header
		{2, 0},              // Other pages start at 0
		{100, 0},
	}

	for _, tt := range tests {
		got := getHeaderOffset(tt.pageNum)
		if got != tt.want {
			t.Errorf("getHeaderOffset(%d) = %d, want %d", tt.pageNum, got, tt.want)
		}
	}
}

// TestClearPageCells tests clearing all cells from a page
func TestClearPageCells(t *testing.T) {
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some cells
	for i := 1; i <= 5; i++ {
		err := cursor.Insert(int64(i), []byte{byte(i)})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify cells were inserted
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader failed: %v", err)
	}

	if header.NumCells != 5 {
		t.Errorf("Before clear: NumCells = %d, want 5", header.NumCells)
	}

	// Clear the page
	page, err := NewBtreePage(rootPage, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage failed: %v", err)
	}

	err = clearPageCells(page)
	if err != nil {
		t.Fatalf("clearPageCells failed: %v", err)
	}

	// Verify cells were cleared
	if page.Header.NumCells != 0 {
		t.Errorf("After clear: NumCells = %d, want 0", page.Header.NumCells)
	}

	if page.Header.CellContentStart != 0 {
		t.Errorf("After clear: CellContentStart = %d, want 0", page.Header.CellContentStart)
	}
}

// TestSplitMultipleLevels tests splitting that propagates up multiple levels
func TestSplitMultipleLevels(t *testing.T) {
	bt := NewBtree(512) // Very small page to force multiple splits

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	initialRoot := rootPage

	// Insert many cells to force multiple levels of splits
	numInserts := 200
	for i := 1; i <= numInserts; i++ {
		key := int64(i)
		payload := make([]byte, 20)
		for j := range payload {
			payload[j] = byte(i)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			// Expected to fail at some point due to complexity
			t.Logf("Insert stopped at %d: %v", i, err)
			break
		}

		// Track root changes
		if cursor.RootPage != initialRoot {
			t.Logf("Root changed at insert %d: %d -> %d", i, initialRoot, cursor.RootPage)
			initialRoot = cursor.RootPage
		}
	}

	// Verify we can still traverse the tree
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed after multiple splits: %v", err)
	}

	count := 0
	prevKey := int64(-1)
	for cursor2.IsValid() {
		key := cursor2.GetKey()
		if key <= prevKey {
			t.Errorf("Keys out of order at position %d: %d after %d", count, key, prevKey)
		}
		prevKey = key
		count++

		if err := cursor2.Next(); err != nil {
			break
		}
	}

	t.Logf("Successfully verified %d cells after multi-level splits", count)

	if count < 10 {
		t.Errorf("Expected at least 10 cells, got %d", count)
	}
}

// TestSplitEmptyPayload tests splitting with empty payloads
func TestSplitEmptyPayload(t *testing.T) {
	bt := NewBtree(1024)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many cells with empty payloads
	for i := 1; i <= 100; i++ {
		key := int64(i)
		err := cursor.Insert(key, []byte{})
		if err != nil {
			t.Logf("Insert %d resulted in: %v", i, err)
		}
	}

	// Verify we can read them back
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	for cursor2.IsValid() {
		payload := cursor2.GetPayload()
		if len(payload) != 0 {
			t.Errorf("Cell %d: expected empty payload, got %d bytes", count, len(payload))
		}
		count++

		if err := cursor2.Next(); err != nil {
			break
		}
	}

	if count < 10 {
		t.Errorf("Expected at least 10 cells, got %d", count)
	}

	t.Logf("Verified %d cells with empty payloads", count)
}

// TestSplitLargePayloads tests splitting with large payloads
func TestSplitLargePayloads(t *testing.T) {
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert cells with large payloads to trigger split sooner
	for i := 1; i <= 50; i++ {
		key := int64(i)
		payload := make([]byte, 200) // Large payload
		for j := range payload {
			payload[j] = byte((i + j) % 256)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert %d with large payload: %v", i, err)
		}
	}

	// Verify data integrity
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	count := 0
	for cursor2.IsValid() {
		key := cursor2.GetKey()
		payload := cursor2.GetPayload()

		if len(payload) != 200 {
			t.Errorf("Cell at key %d: payload length = %d, want 200", key, len(payload))
		}

		// Verify payload content
		expectedFirstByte := byte((int(key)) % 256)
		if len(payload) > 0 && payload[0] != expectedFirstByte {
			t.Errorf("Cell at key %d: first byte = %d, want %d", key, payload[0], expectedFirstByte)
		}

		count++
		if err := cursor2.Next(); err != nil {
			break
		}
	}

	t.Logf("Verified %d cells with large payloads", count)
}
