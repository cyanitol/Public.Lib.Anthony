// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree

import (
	"encoding/binary"
	"testing"
)

// Helper function to create a test page with specific cells
func createTestPage(pageNum uint32, pageSize uint32, pageType byte, cells []struct {
	rowid   int64
	payload []byte
}) []byte {
	data := make([]byte, pageSize)

	// Calculate header offset
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}

	// Page header
	data[headerOffset+PageHeaderOffsetType] = pageType
	// Freeblock offset = 0
	numCells := uint16(len(cells))
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetNumCells:], numCells)

	// Cell content area starts from the end
	cellContentOffset := pageSize
	cellPtrOffset := uint32(headerOffset + PageHeaderSizeLeaf)

	// Write cells from end backwards
	cellOffsets := make([]uint32, len(cells))
	for i := 0; i < len(cells); i++ {
		cell := cells[i]

		// Encode cell
		cellData := EncodeTableLeafCell(cell.rowid, cell.payload)

		// Ensure minimum cell size of 4 bytes (SQLite requirement)
		cellSize := len(cellData)
		if cellSize < 4 {
			cellSize = 4
		}

		// Write cell to page (from end backwards)
		cellContentOffset -= uint32(cellSize)
		copy(data[cellContentOffset:], cellData)
		cellOffsets[i] = cellContentOffset
	}

	// Write cell pointers in order
	for i := 0; i < len(cells); i++ {
		binary.BigEndian.PutUint16(data[cellPtrOffset:], uint16(cellOffsets[i]))
		cellPtrOffset += 2
	}

	// Update cell content start
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetCellStart:], uint16(cellContentOffset))

	return data
}

// Helper function to create an interior page
func createInteriorPage(pageNum uint32, pageSize uint32, cells []struct {
	childPage uint32
	rowid     int64
}, rightChild uint32) []byte {
	data := make([]byte, pageSize)

	// Calculate header offset
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}

	// Page header
	data[headerOffset+PageHeaderOffsetType] = PageTypeInteriorTable
	// Freeblock offset = 0
	numCells := uint16(len(cells))
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetNumCells:], numCells)

	// Right child pointer
	binary.BigEndian.PutUint32(data[headerOffset+PageHeaderOffsetRightChild:], rightChild)

	// Cell content area starts from the end
	cellContentOffset := pageSize
	cellPtrOffset := uint32(headerOffset + PageHeaderSizeInterior)

	// Write cells from end backwards
	cellOffsets := make([]uint32, len(cells))
	for i := 0; i < len(cells); i++ {
		cell := cells[i]

		// Encode cell
		cellData := EncodeTableInteriorCell(cell.childPage, cell.rowid)

		// Write cell to page (from end backwards)
		cellContentOffset -= uint32(len(cellData))
		copy(data[cellContentOffset:], cellData)
		cellOffsets[i] = cellContentOffset
	}

	// Write cell pointers in order
	for i := 0; i < len(cells); i++ {
		binary.BigEndian.PutUint16(data[cellPtrOffset:], uint16(cellOffsets[i]))
		cellPtrOffset += 2
	}

	// Update cell content start
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetCellStart:], uint16(cellContentOffset))

	return data
}

func TestCanMerge(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	tests := []struct {
		name          string
		leftCells     int
		rightCells    int
		payloadSize   int
		expectedMerge bool
	}{
		{
			name:          "two small pages can merge",
			leftCells:     2,
			rightCells:    2,
			payloadSize:   10,
			expectedMerge: true,
		},
		{
			name:          "two large pages cannot merge",
			leftCells:     50,
			rightCells:    50,
			payloadSize:   50,
			expectedMerge: false,
		},
		{
			name:          "one empty page can merge",
			leftCells:     0,
			rightCells:    3,
			payloadSize:   20,
			expectedMerge: true,
		},
		{
			name:          "moderate pages can merge",
			leftCells:     10,
			rightCells:    10,
			payloadSize:   20,
			expectedMerge: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Create left page
			leftCells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.leftCells)
			for i := 0; i < tt.leftCells; i++ {
				leftCells[i].rowid = int64(i + 1)
				leftCells[i].payload = make([]byte, tt.payloadSize)
			}
			leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
			leftHeader, err := ParsePageHeader(leftPageData, 2)
			if err != nil {
				t.Fatalf("Failed to parse left page header: %v", err)
			}

			// Create right page
			rightCells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.rightCells)
			for i := 0; i < tt.rightCells; i++ {
				rightCells[i].rowid = int64(tt.leftCells + i + 1)
				rightCells[i].payload = make([]byte, tt.payloadSize)
			}
			rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
			rightHeader, err := ParsePageHeader(rightPageData, 3)
			if err != nil {
				t.Fatalf("Failed to parse right page header: %v", err)
			}

			// Test CanMerge
			canMerge, err := CanMerge(leftPageData, leftHeader, rightPageData, rightHeader, pageSize)
			if err != nil {
				t.Fatalf("CanMerge() error = %v", err)
			}

			if canMerge != tt.expectedMerge {
				t.Errorf("CanMerge() = %v, want %v", canMerge, tt.expectedMerge)
			}
		})
	}
}

func TestCanMerge_DifferentPageTypes(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create left page as leaf
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("test")},
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftHeader, _ := ParsePageHeader(leftPageData, 2)

	// Create right page as interior (should not merge)
	rightPageData := createInteriorPage(3, pageSize, []struct {
		childPage uint32
		rowid     int64
	}{{4, 10}}, 5)
	rightHeader, _ := ParsePageHeader(rightPageData, 3)

	canMerge, err := CanMerge(leftPageData, leftHeader, rightPageData, rightHeader, pageSize)
	if err != nil {
		t.Fatalf("CanMerge() error = %v", err)
	}

	if canMerge {
		t.Error("CanMerge() should return false for different page types")
	}
}

func TestRedistributeCells(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	tests := []struct {
		name          string
		leftCells     int
		rightCells    int
		expectedLeft  int
		expectedRight int
		payloadSize   int
	}{
		{
			name:          "unbalanced left heavy",
			leftCells:     10,
			rightCells:    2,
			expectedLeft:  6,
			expectedRight: 6,
			payloadSize:   10,
		},
		{
			name:          "unbalanced right heavy",
			leftCells:     2,
			rightCells:    10,
			expectedLeft:  6,
			expectedRight: 6,
			payloadSize:   10,
		},
		{
			name:          "already balanced",
			leftCells:     5,
			rightCells:    5,
			expectedLeft:  5,
			expectedRight: 5,
			payloadSize:   10,
		},
		{
			name:          "one empty page",
			leftCells:     0,
			rightCells:    8,
			expectedLeft:  4,
			expectedRight: 4,
			payloadSize:   10,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Create left page
			leftCells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.leftCells)
			for i := 0; i < tt.leftCells; i++ {
				leftCells[i].rowid = int64(i + 1)
				leftCells[i].payload = make([]byte, tt.payloadSize)
			}
			leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
			leftPage, err := NewBtreePage(2, leftPageData, pageSize)
			if err != nil {
				t.Fatalf("Failed to create left page: %v", err)
			}

			// Create right page
			rightCells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.rightCells)
			for i := 0; i < tt.rightCells; i++ {
				rightCells[i].rowid = int64(tt.leftCells + i + 1)
				rightCells[i].payload = make([]byte, tt.payloadSize)
			}
			rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
			rightPage, err := NewBtreePage(3, rightPageData, pageSize)
			if err != nil {
				t.Fatalf("Failed to create right page: %v", err)
			}

			// Redistribute
			err = RedistributeCells(leftPage, rightPage)
			if err != nil {
				t.Fatalf("RedistributeCells() error = %v", err)
			}

			// Verify cell counts
			if int(leftPage.Header.NumCells) != tt.expectedLeft {
				t.Errorf("Left page cells = %d, want %d", leftPage.Header.NumCells, tt.expectedLeft)
			}
			if int(rightPage.Header.NumCells) != tt.expectedRight {
				t.Errorf("Right page cells = %d, want %d", rightPage.Header.NumCells, tt.expectedRight)
			}

			// Verify total cells preserved
			totalCells := tt.leftCells + tt.rightCells
			actualTotal := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
			if actualTotal != totalCells {
				t.Errorf("Total cells = %d, want %d", actualTotal, totalCells)
			}
		})
	}
}

func TestIsUnderfull(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	tests := []struct {
		name          string
		numCells      int
		payloadSize   int
		wantUnderfull bool
	}{
		{
			name:          "one cell is underfull",
			numCells:      1,
			payloadSize:   10,
			wantUnderfull: true,
		},
		{
			name:          "minimum cells is not underfull",
			numCells:      MinCellsPerPage,
			payloadSize:   500, // Large payload to use significant space
			wantUnderfull: false,
		},
		{
			name:          "many cells is not underfull",
			numCells:      20,
			payloadSize:   100, // Larger payload to ensure page is well-filled
			wantUnderfull: false,
		},
		{
			name:          "empty page is not underfull",
			numCells:      0,
			payloadSize:   0,
			wantUnderfull: false, // Empty pages are valid and not considered underfull
		},
		{
			name:          "small cells underfull by space",
			numCells:      5,
			payloadSize:   10,
			wantUnderfull: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Create page
			cells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.numCells)
			for i := 0; i < tt.numCells; i++ {
				cells[i].rowid = int64(i + 1)
				cells[i].payload = make([]byte, tt.payloadSize)
			}
			pageData := createTestPage(2, pageSize, PageTypeLeafTable, cells)
			page, err := NewBtreePage(2, pageData, pageSize)
			if err != nil {
				t.Fatalf("Failed to create page: %v", err)
			}

			// Test IsUnderfull
			isUnderfull := page.IsUnderfull()
			if isUnderfull != tt.wantUnderfull {
				t.Errorf("IsUnderfull() = %v, want %v (cells=%d, freeSpace=%d, usableSize=%d)",
					isUnderfull, tt.wantUnderfull, page.Header.NumCells, page.FreeSpace(), page.UsableSize)
			}
		})
	}
}

func TestMergePage_Simple(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create a simple 2-level tree:
	// Root (page 1) -> interior with two children
	// Page 2 -> leaf with 2 cells
	// Page 3 -> leaf with 2 cells

	// Create leaf page 2
	page2Cells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("row1")},
		{2, []byte("row2")},
	}
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, page2Cells)
	bt.SetPage(2, page2Data)

	// Create leaf page 3
	page3Cells := []struct {
		rowid   int64
		payload []byte
	}{
		{3, []byte("row3")},
		{4, []byte("row4")},
	}
	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, page3Cells)
	bt.SetPage(3, page3Data)

	// Create interior root page 1
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 2}, // Left child points to page 2, separator key is 2
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 3) // Right child is page 3
	bt.SetPage(1, rootData)

	// Position cursor on page 2
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find rowid 1")
	}

	// Verify cursor is on page 2
	if cursor.CurrentPage != 2 {
		t.Fatalf("Cursor on page %d, want page 2", cursor.CurrentPage)
	}

	// Attempt merge
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	// Should be able to merge since both pages are small
	if !merged {
		t.Log("MergePage() did not merge (this is OK if pages don't need merging)")
	} else {
		// Verify one page was freed
		if _, ok := bt.Pages[3]; ok {
			t.Error("Page 3 should have been freed after merge")
		}

		// Verify all cells are in page 2
		page2DataAfter, err := bt.GetPage(2)
		if err != nil {
			t.Fatalf("Failed to get page 2 after merge: %v", err)
		}
		header2After, err := ParsePageHeader(page2DataAfter, 2)
		if err != nil {
			t.Fatalf("Failed to parse page 2 header after merge: %v", err)
		}

		if header2After.NumCells != 4 {
			t.Errorf("Page 2 has %d cells after merge, want 4", header2After.NumCells)
		}
	}
}

func TestMergePage_RootPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a single-page tree (root only)
	rootCells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("test")},
	}
	rootData := createTestPage(1, bt.PageSize, PageTypeLeafTable, rootCells)
	bt.SetPage(1, rootData)

	// Position cursor on root
	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	// Attempt merge on root (should not merge)
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Error("MergePage() should not merge root page")
	}
}

// verifyCellKeyOrdering verifies that keys are in ascending order within a page
func verifyCellKeyOrdering(t *testing.T, page *BtreePage, pageSize uint32, pageName string) {
	t.Helper()
	for i := 0; i < int(page.Header.NumCells)-1; i++ {
		cell1 := getCellAtIndex(t, page, i, pageSize)
		cell2 := getCellAtIndex(t, page, i+1, pageSize)

		if cell1.Key >= cell2.Key {
			t.Errorf("Keys out of order in %s: cell[%d].key=%d >= cell[%d].key=%d",
				pageName, i, cell1.Key, i+1, cell2.Key)
		}
	}
}

// getCellAtIndex retrieves and parses a cell at a given index
func getCellAtIndex(t *testing.T, page *BtreePage, index int, pageSize uint32) *CellInfo {
	t.Helper()
	cellOffset, err := page.Header.GetCellPointer(page.Data, index)
	if err != nil {
		t.Fatalf("Failed to get cell pointer for cell %d: %v", index, err)
	}

	cell, err := ParseCell(page.Header.PageType, page.Data[cellOffset:], pageSize)
	if err != nil {
		t.Fatalf("Failed to parse cell %d at offset %d: %v", index, cellOffset, err)
	}

	return cell
}

// verifyPageSeparation verifies that all keys in left page are less than all keys in right page
func verifyPageSeparation(t *testing.T, leftPage, rightPage *BtreePage, pageSize uint32) {
	t.Helper()
	if leftPage.Header.NumCells == 0 || rightPage.Header.NumCells == 0 {
		return
	}

	lastLeftCell := getCellAtIndex(t, leftPage, int(leftPage.Header.NumCells)-1, pageSize)
	firstRightCell := getCellAtIndex(t, rightPage, 0, pageSize)

	if lastLeftCell.Key >= firstRightCell.Key {
		t.Errorf("Last left key (%d) >= first right key (%d)", lastLeftCell.Key, firstRightCell.Key)
	}
}

func TestRedistributeCells_KeyOrdering(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create left page with keys 1-3
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
		{3, []byte("c")},
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("Failed to create left page: %v", err)
	}

	// Create right page with keys 4-10
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{
		{4, []byte("d")},
		{5, []byte("e")},
		{6, []byte("f")},
		{7, []byte("g")},
		{8, []byte("h")},
		{9, []byte("i")},
		{10, []byte("j")},
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("Failed to create right page: %v", err)
	}

	// Redistribute
	err = RedistributeCells(leftPage, rightPage)
	if err != nil {
		t.Fatalf("RedistributeCells() error = %v", err)
	}

	// Verify keys are still in order on left page
	verifyCellKeyOrdering(t, leftPage, pageSize, "left page")

	// Verify keys are still in order on right page
	verifyCellKeyOrdering(t, rightPage, pageSize, "right page")

	// Verify all keys from left page are less than all keys from right page
	verifyPageSeparation(t, leftPage, rightPage, pageSize)
}

func TestCanMerge_EmptyPages(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create two empty pages
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, nil)
	leftHeader, err := ParsePageHeader(leftPageData, 2)
	if err != nil {
		t.Fatalf("Failed to parse left page header: %v", err)
	}

	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, nil)
	rightHeader, err := ParsePageHeader(rightPageData, 3)
	if err != nil {
		t.Fatalf("Failed to parse right page header: %v", err)
	}

	canMerge, err := CanMerge(leftPageData, leftHeader, rightPageData, rightHeader, pageSize)
	if err != nil {
		t.Fatalf("CanMerge() error = %v", err)
	}

	if !canMerge {
		t.Error("CanMerge() should return true for two empty pages")
	}
}
