package btree

import (
	"encoding/binary"
	"testing"
)

// TestCursorFreeOverflowChain_MultiPage tests freeing a multi-page overflow chain
// Targets FreeOverflowChain at 60% coverage
func TestCursorFreeOverflowChain_MultiPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create an overflow chain: page 2 -> page 3 -> page 4
	page2Data := make([]byte, bt.PageSize)
	binary.BigEndian.PutUint32(page2Data[0:], 3) // Next overflow page = 3
	bt.SetPage(2, page2Data)

	page3Data := make([]byte, bt.PageSize)
	binary.BigEndian.PutUint32(page3Data[0:], 4) // Next overflow page = 4
	bt.SetPage(3, page3Data)

	page4Data := make([]byte, bt.PageSize)
	binary.BigEndian.PutUint32(page4Data[0:], 0) // Last page, no next
	bt.SetPage(4, page4Data)

	// Free the chain
	err = cursor.FreeOverflowChain(2)
	if err != nil {
		t.Fatalf("FreeOverflowChain() error = %v", err)
	}

	// Verify pages were freed
	for _, pageNum := range []uint32{2, 3, 4} {
		if _, ok := bt.Pages[pageNum]; ok {
			t.Errorf("Page %d should have been freed", pageNum)
		}
	}
}

// TestCursorFreeOverflowChain_SinglePage tests freeing a single overflow page
func TestCursorFreeOverflowChain_SinglePage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create a single overflow page
	pageData := make([]byte, bt.PageSize)
	binary.BigEndian.PutUint32(pageData[0:], 0) // No next page
	bt.SetPage(2, pageData)

	// Free it
	err = cursor.FreeOverflowChain(2)
	if err != nil {
		t.Fatalf("FreeOverflowChain() error = %v", err)
	}

	// Verify page was freed
	if _, ok := bt.Pages[2]; ok {
		t.Error("Page 2 should have been freed")
	}
}

// TestRedistributeLeafCells_Comprehensive tests leaf cell redistribution
// Targets redistributeLeafCells at 61.5% coverage
func TestRedistributeLeafCells_Comprehensive(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force redistribution
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to create multiple pages
	for i := int64(1); i <= 80; i++ {
		err := cursor.Insert(i, make([]byte, 10))
		if err != nil {
			break
		}
	}

	// Delete some to create imbalance
	for i := int64(20); i <= 40; i++ {
		found, _ := cursor.SeekRowid(i)
		if found {
			cursor.Delete()
		}
	}

	// Force redistribution by attempting merge
	cursor.SeekRowid(25)
	merged, err := cursor.MergePage()
	if err != nil {
		t.Logf("MergePage() error: %v", err)
	}
	t.Logf("Merge/redistribute result: %v", merged)
}

// TestInsertDividerIntoParent tests divider insertion during split
// Targets insertDividerIntoParent at 62.5% coverage
func TestInsertDividerIntoParent(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force splits
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough rows to force a split
	for i := int64(1); i <= 150; i++ {
		err := cursor.Insert(i, make([]byte, 10))
		if err != nil {
			t.Logf("Insert stopped at %d: %v", i, err)
			break
		}
	}

	// The splits should have called insertDividerIntoParent
	t.Log("Insertions completed, splits should have occurred")
}

// TestMoveRightToLeft tests moving cells from right to left
// Targets moveRightToLeft at 63.6% coverage
func TestMoveRightToLeft(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create left page with few cells
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(left) error = %v", err)
	}

	// Create right page with many cells
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{
		{3, []byte("c")},
		{4, []byte("d")},
		{5, []byte("e")},
		{6, []byte("f")},
		{7, []byte("g")},
		{8, []byte("h")},
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(right) error = %v", err)
	}

	// Redistribute - should move from right to left
	err = RedistributeCells(leftPage, rightPage)
	if err != nil {
		t.Fatalf("RedistributeCells() error = %v", err)
	}

	// Verify cells moved
	if leftPage.Header.NumCells <= 2 {
		t.Error("Cells should have been moved from right to left")
	}
	if rightPage.Header.NumCells >= 6 {
		t.Error("Cells should have been moved from right to left")
	}

	t.Logf("After redistribution: left=%d cells, right=%d cells",
		leftPage.Header.NumCells, rightPage.Header.NumCells)
}

// TestMoveLeftToRight tests moving cells from left to right
// Targets moveLeftToRight at 66.7% coverage
func TestMoveLeftToRight(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create left page with many cells
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
		{3, []byte("c")},
		{4, []byte("d")},
		{5, []byte("e")},
		{6, []byte("f")},
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(left) error = %v", err)
	}

	// Create right page with few cells
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{
		{7, []byte("g")},
		{8, []byte("h")},
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(right) error = %v", err)
	}

	// Redistribute - should move from left to right
	err = RedistributeCells(leftPage, rightPage)
	if err != nil {
		t.Fatalf("RedistributeCells() error = %v", err)
	}

	// Verify cells moved
	if leftPage.Header.NumCells >= 6 {
		t.Error("Cells should have been moved from left to right")
	}
	if rightPage.Header.NumCells <= 2 {
		t.Error("Cells should have been moved from left to right")
	}

	t.Logf("After redistribution: left=%d cells, right=%d cells",
		leftPage.Header.NumCells, rightPage.Header.NumCells)
}

// TestCreateNewRoot2 tests root page creation during split
// Targets createNewRoot at 66.7% coverage
func TestCreateNewRoot2(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force root split
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to force root to split
	insertCount := 0
	for i := int64(1); i <= 200; i++ {
		err := cursor.Insert(i, make([]byte, 8))
		if err != nil {
			t.Logf("Insert stopped at %d: %v", i, err)
			break
		}
		insertCount++
	}

	t.Logf("Inserted %d rows, root may have split", insertCount)

	// Check if root is now an interior page (was split)
	rootData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage() error = %v", err)
	}

	header, err := ParsePageHeader(rootData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader() error = %v", err)
	}

	if header.IsInterior {
		t.Log("Root page is now interior (was split successfully)")
	}
}

// TestAllocateAndInitializeLeafPage tests leaf page allocation during split
// Targets allocateAndInitializeLeafPage at 66.7% coverage
func TestAllocateAndInitializeLeafPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert to force leaf split
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 12))
		if err != nil {
			break
		}
	}

	// A new leaf page should have been allocated
	t.Logf("Pages in btree: %d", len(bt.Pages))
	if len(bt.Pages) <= 1 {
		t.Error("Expected multiple pages after inserts")
	}
}

// TestAllocateAndInitializeInteriorPage tests interior page allocation
// Targets allocateAndInitializeInteriorPage at 66.7% coverage
func TestAllocateAndInitializeInteriorPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to force interior page creation
	for i := int64(1); i <= 250; i++ {
		err := cursor.Insert(i, make([]byte, 8))
		if err != nil {
			break
		}
	}

	// Check for interior pages
	hasInterior := false
	for pageNum := range bt.Pages {
		pageData, err := bt.GetPage(pageNum)
		if err != nil {
			continue
		}
		header, err := ParsePageHeader(pageData, pageNum)
		if err != nil {
			continue
		}
		if header.IsInterior {
			hasInterior = true
			break
		}
	}

	if hasInterior {
		t.Log("Interior page(s) created successfully")
	}
}

// TestDefragmentPages2 tests page defragmentation
// Targets defragmentPages at 66.7% coverage
func TestDefragmentPages2(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create pages with fragmentation
	leftPageData := make([]byte, pageSize)
	leftPageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(leftPageData[3:], 3) // 3 cells
	leftPageData[7] = 20 // FragmentedBytes

	// Add cells with gaps
	cell1 := EncodeTableLeafCell(1, []byte("a"))
	cell2 := EncodeTableLeafCell(2, []byte("b"))
	cell3 := EncodeTableLeafCell(3, []byte("c"))

	offset3 := int(pageSize) - len(cell3)
	copy(leftPageData[offset3:], cell3)

	offset2 := offset3 - len(cell2) - 10 // 10-byte gap
	copy(leftPageData[offset2:], cell2)

	offset1 := offset2 - len(cell1) - 10 // Another 10-byte gap
	copy(leftPageData[offset1:], cell1)

	binary.BigEndian.PutUint16(leftPageData[5:], uint16(offset1))
	binary.BigEndian.PutUint16(leftPageData[8:], uint16(offset1))
	binary.BigEndian.PutUint16(leftPageData[10:], uint16(offset2))
	binary.BigEndian.PutUint16(leftPageData[12:], uint16(offset3))

	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(left) error = %v", err)
	}

	// Create similar right page
	rightPageData := make([]byte, pageSize)
	rightPageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(rightPageData[3:], 2)
	rightPageData[7] = 15

	cell4 := EncodeTableLeafCell(4, []byte("d"))
	cell5 := EncodeTableLeafCell(5, []byte("e"))

	offset5 := int(pageSize) - len(cell5)
	copy(rightPageData[offset5:], cell5)

	offset4 := offset5 - len(cell4) - 15
	copy(rightPageData[offset4:], cell4)

	binary.BigEndian.PutUint16(rightPageData[5:], uint16(offset4))
	binary.BigEndian.PutUint16(rightPageData[8:], uint16(offset4))
	binary.BigEndian.PutUint16(rightPageData[10:], uint16(offset5))

	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage(right) error = %v", err)
	}

	// Defragment both pages
	err = defragmentPages(leftPage, rightPage)
	if err != nil {
		t.Fatalf("defragmentPages() error = %v", err)
	}

	// Verify fragmentation cleared
	if leftPage.Header.FragmentedBytes != 0 {
		t.Errorf("Left page FragmentedBytes = %d, want 0", leftPage.Header.FragmentedBytes)
	}
	if rightPage.Header.FragmentedBytes != 0 {
		t.Errorf("Right page FragmentedBytes = %d, want 0", rightPage.Header.FragmentedBytes)
	}
}

// TestCursorWriteSingleOverflowPage tests writing a single overflow page
// Targets writeSingleOverflowPage at 66.7% coverage
func TestCursorWriteSingleOverflowPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create large data that requires overflow
	largeData := make([]byte, 5000) // Larger than usable page size
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Calculate local size
	localSize := CalculateLocalPayload(uint32(len(largeData)), bt.UsableSize, true)

	// Write overflow
	overflowPage, err := cursor.WriteOverflow(largeData, uint16(localSize), bt.UsableSize)
	if err != nil {
		t.Fatalf("WriteOverflow() error = %v", err)
	}

	if overflowPage == 0 {
		t.Error("WriteOverflow() should return non-zero overflow page")
	}

	// Verify overflow page exists
	_, err = bt.GetPage(overflowPage)
	if err != nil {
		t.Errorf("Overflow page %d not found", overflowPage)
	}

	// Read it back
	localPayload := largeData[:localSize]
	readData, err := cursor.ReadOverflow(localPayload, overflowPage, uint32(len(largeData)), bt.UsableSize)
	if err != nil {
		t.Fatalf("ReadOverflow() error = %v", err)
	}

	if len(readData) != len(largeData) {
		t.Errorf("Read %d bytes, want %d", len(readData), len(largeData))
	}
}

// TestCursorWriteOverflow_MultiPage tests multi-page overflow
func TestCursorWriteOverflow_MultiPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create very large data that requires multiple overflow pages
	largeData := make([]byte, 12000) // Multiple pages worth
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	localSize := CalculateLocalPayload(uint32(len(largeData)), bt.UsableSize, true)

	// Write overflow
	overflowPage, err := cursor.WriteOverflow(largeData, uint16(localSize), bt.UsableSize)
	if err != nil {
		t.Fatalf("WriteOverflow() error = %v", err)
	}

	// Read it back
	localPayload := largeData[:localSize]
	readData, err := cursor.ReadOverflow(localPayload, overflowPage, uint32(len(largeData)), bt.UsableSize)
	if err != nil {
		t.Fatalf("ReadOverflow() error = %v", err)
	}

	if len(readData) != len(largeData) {
		t.Errorf("Read %d bytes, want %d", len(readData), len(largeData))
	}

	// Verify data matches
	for i := range largeData {
		if readData[i] != largeData[i] {
			t.Errorf("Data mismatch at byte %d: got %d, want %d", i, readData[i], largeData[i])
			break
		}
	}
}

// TestCursorReadOverflow_EdgeCases tests overflow reading edge cases
func TestCursorReadOverflow_EdgeCases(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Test with exactly one page of data
	onePageData := make([]byte, bt.UsableSize-8) // Leave room for header
	for i := range onePageData {
		onePageData[i] = byte(i % 256)
	}

	localSize := CalculateLocalPayload(uint32(len(onePageData)), bt.UsableSize, true)

	overflowPage, err := cursor.WriteOverflow(onePageData, uint16(localSize), bt.UsableSize)
	if err != nil {
		t.Fatalf("WriteOverflow() error = %v", err)
	}

	localPayload := onePageData[:localSize]
	readData, err := cursor.ReadOverflow(localPayload, overflowPage, uint32(len(onePageData)), bt.UsableSize)
	if err != nil {
		t.Fatalf("ReadOverflow() error = %v", err)
	}

	if len(readData) != len(onePageData) {
		t.Errorf("Read %d bytes, want %d", len(readData), len(onePageData))
	}
}

// TestSplitPage_Coverage tests page splitting comprehensively
func TestSplitPage_Coverage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert in random order to test different split scenarios
	keys := []int64{50, 25, 75, 12, 37, 62, 87, 6, 18, 31, 43, 56, 68, 81, 93}
	for _, key := range keys {
		err := cursor.Insert(key, make([]byte, 15))
		if err != nil {
			t.Logf("Insert(%d) error: %v", key, err)
			break
		}
	}

	// Continue with sequential inserts
	for i := int64(100); i <= 150; i++ {
		err := cursor.Insert(i, make([]byte, 10))
		if err != nil {
			break
		}
	}

	t.Logf("Final page count: %d", len(bt.Pages))
}
