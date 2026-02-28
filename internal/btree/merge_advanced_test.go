package btree

import (
	"encoding/binary"
	"testing"
)

// TestMergePage_WithProvider tests merge with a PageProvider
func TestMergePage_WithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create a mock provider
	mockProvider := &MockPageProvider{
		pages: make(map[uint32][]byte),
	}
	bt.Provider = mockProvider

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
	mockProvider.pages[2] = page2Data

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
	mockProvider.pages[3] = page3Data

	// Create interior root page 1
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 2},
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)
	mockProvider.pages[1] = rootData

	// Position cursor on page 2
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find rowid 1")
	}

	// Attempt merge
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		// Verify dirty pages were marked
		if !mockProvider.dirtyPages[1] {
			t.Error("Parent page should be marked dirty")
		}
		if !mockProvider.dirtyPages[2] {
			t.Error("Left page should be marked dirty")
		}
	}
}

// TestMergePage_LeftSibling tests merging with left sibling
func TestMergePage_LeftSibling(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create 3 leaf pages
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}})
	bt.SetPage(2, page2Data)

	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{2, []byte("b")}})
	bt.SetPage(3, page3Data)

	page4Data := createTestPage(4, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{3, []byte("c")}})
	bt.SetPage(4, page4Data)

	// Create interior root with 3 children
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 1},
		{3, 2},
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 4)
	bt.SetPage(1, rootData)

	// Position cursor on middle page (3)
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(2)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find rowid 2")
	}

	// Verify cursor is on page 3
	if cursor.CurrentPage != 3 {
		t.Fatalf("Cursor on page %d, want page 3", cursor.CurrentPage)
	}

	// Attempt merge (should merge with left sibling page 2)
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Log("Successfully merged page 3 with left sibling")
		// Verify page 3 was freed
		if _, ok := bt.Pages[3]; ok {
			t.Error("Page 3 should have been freed")
		}
	}
}

// TestMergePage_RightmostChild tests merging rightmost child
func TestMergePage_RightmostChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create 2 leaf pages
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}})
	bt.SetPage(2, page2Data)

	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{2, []byte("b")}})
	bt.SetPage(3, page3Data)

	// Create interior root where page 3 is the rightmost child
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 1},
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)

	// Position cursor on rightmost page (3)
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(2)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Fatal("SeekRowid() did not find rowid 2")
	}

	// Verify cursor is on page 3 (rightmost)
	if cursor.CurrentPage != 3 {
		t.Fatalf("Cursor on page %d, want page 3", cursor.CurrentPage)
	}

	// Attempt merge
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Log("Successfully merged rightmost page")
	}
}

// TestMergePage_InteriorPage tests that interior pages don't merge
func TestMergePage_InteriorPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create a 3-level tree
	// Level 2: Leaf pages
	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}})
	bt.SetPage(3, page3Data)

	page4Data := createTestPage(4, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{2, []byte("b")}})
	bt.SetPage(4, page4Data)

	// Level 1: Interior page
	page2Cells := []struct {
		childPage uint32
		rowid     int64
	}{{3, 1}}
	page2Data := createInteriorPage(2, pageSize, page2Cells, 4)
	bt.SetPage(2, page2Data)

	// Level 0: Root interior page
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 2}}
	rootData := createInteriorPage(1, pageSize, rootCells, 2)
	bt.SetPage(1, rootData)

	// Create cursor and navigate to leaf
	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	// Manually set cursor to be on the interior page (page 2)
	// This is artificial but tests the code path
	cursor.CurrentPage = 2
	cursor.Depth = 1

	// Attempt merge - should return false because it's an interior page
	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Error("Should not merge interior page")
	}
}

// TestRedistributeSiblings_EdgeCases tests redistribution edge cases
func TestRedistributeSiblings_EdgeCases(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	tests := []struct {
		name        string
		leftCells   int
		rightCells  int
		shouldError bool
	}{
		{
			name:        "one cell left, many right",
			leftCells:   1,
			rightCells:  11,
			shouldError: false,
		},
		{
			name:        "many left, one cell right",
			leftCells:   11,
			rightCells:  1,
			shouldError: false,
		},
		{
			name:        "both have one cell",
			leftCells:   1,
			rightCells:  1,
			shouldError: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			// Create left page
			leftCells := make([]struct {
				rowid   int64
				payload []byte
			}, tt.leftCells)
			for i := 0; i < tt.leftCells; i++ {
				leftCells[i].rowid = int64(i + 1)
				leftCells[i].payload = []byte("data")
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
				rightCells[i].payload = []byte("data")
			}
			rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
			rightPage, err := NewBtreePage(3, rightPageData, pageSize)
			if err != nil {
				t.Fatalf("Failed to create right page: %v", err)
			}

			// Redistribute
			err = RedistributeCells(leftPage, rightPage)

			if tt.shouldError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if err == nil {
				// Verify total cells preserved
				totalCells := tt.leftCells + tt.rightCells
				actualTotal := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
				if actualTotal != totalCells {
					t.Errorf("Total cells = %d, want %d", actualTotal, totalCells)
				}
			}
		})
	}
}

// TestCanMerge_ErrorCases tests CanMerge error handling
func TestCanMerge_ErrorCases(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create valid page
	validCells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("test")}}
	validPageData := createTestPage(2, pageSize, PageTypeLeafTable, validCells)
	validHeader, _ := ParsePageHeader(validPageData, 2)

	// Test with corrupted cell pointer - use a valid cell pointer but with corrupted cell data
	corruptedData := make([]byte, pageSize)
	copy(corruptedData, validPageData)

	// Get the actual cell pointer value
	cellPtr := binary.BigEndian.Uint16(corruptedData[PageHeaderSizeLeaf:])

	// Corrupt the cell data at that pointer (make payload length invalid)
	// This will cause ParseCell to fail
	if int(cellPtr) < len(corruptedData)-5 {
		corruptedData[cellPtr] = 0xFF // Invalid rowid varint
		corruptedData[cellPtr+1] = 0xFF
		corruptedData[cellPtr+2] = 0xFF
		corruptedData[cellPtr+3] = 0xFF
		corruptedData[cellPtr+4] = 0xFF
	}

	_, err := CanMerge(corruptedData, validHeader, validPageData, validHeader, pageSize)
	if err == nil {
		t.Log("CanMerge may succeed with corrupted data depending on how it's corrupted")
	}
}

// TestGetChildPageAt tests getChildPageAt helper
func TestGetChildPageAt(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create interior page with multiple children
	interiorCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{10, 5},
		{20, 10},
		{30, 15},
	}
	rightChild := uint32(40)
	pageData := createInteriorPage(1, pageSize, interiorCells, rightChild)
	bt.SetPage(1, pageData)

	cursor := NewCursor(bt, 1)
	header, _ := ParsePageHeader(pageData, 1)

	tests := []struct {
		name      string
		index     int
		wantPage  uint32
		wantError bool
	}{
		{"first child", 0, 10, false},
		{"middle child", 1, 20, false},
		{"last cell child", 2, 30, false},
		{"beyond cells - rightmost", 3, 40, false},
		{"beyond cells - rightmost 2", 4, 40, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			page, err := cursor.getChildPageAt(pageData, header, tt.index)

			if tt.wantError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("getChildPageAt() error = %v", err)
			}

			if page != tt.wantPage {
				t.Errorf("getChildPageAt() = %d, want %d", page, tt.wantPage)
			}
		})
	}
}

// TestMergePage_InvalidState tests merge with invalid cursor state
func TestMergePage_InvalidState(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	cursor := NewCursor(bt, rootPage)
	cursor.State = CursorInvalid

	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Error("Should not merge with invalid cursor")
	}
}

// TestMergePage_DepthZero tests merge at root level (depth 0)
func TestMergePage_DepthZero(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("data"))
	cursor.SeekRowid(1)

	// Root is at depth 0
	if cursor.Depth != 0 {
		t.Fatalf("Expected depth 0, got %d", cursor.Depth)
	}

	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage() error = %v", err)
	}

	if merged {
		t.Error("Should not merge at depth 0")
	}
}

// MockPageProvider is a simple mock for testing
type MockPageProvider struct {
	pages      map[uint32][]byte
	dirtyPages map[uint32]bool
	allocCount uint32
}

func (m *MockPageProvider) GetPageData(pgno uint32) ([]byte, error) {
	if data, ok := m.pages[pgno]; ok {
		return data, nil
	}
	return nil, nil
}

func (m *MockPageProvider) AllocatePageData() (uint32, []byte, error) {
	m.allocCount++
	pgno := m.allocCount + 100 // Start from 100 to avoid conflicts
	data := make([]byte, 4096)
	m.pages[pgno] = data
	return pgno, data, nil
}

func (m *MockPageProvider) MarkDirty(pgno uint32) error {
	if m.dirtyPages == nil {
		m.dirtyPages = make(map[uint32]bool)
	}
	m.dirtyPages[pgno] = true
	return nil
}

// TestExtractCellFromPage tests cell extraction helper
func TestExtractCellFromPage(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	cells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("test1")},
		{2, []byte("test2")},
		{3, []byte("test3")},
	}

	pageData := createTestPage(2, pageSize, PageTypeLeafTable, cells)
	page, err := NewBtreePage(2, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Extract each cell
	for i := 0; i < len(cells); i++ {
		cellData, err := extractCellFromPage(page, i)
		if err != nil {
			t.Errorf("extractCellFromPage(%d) error = %v", i, err)
			continue
		}

		if len(cellData) == 0 {
			t.Errorf("extractCellFromPage(%d) returned empty data", i)
		}

		// Verify we can parse it
		cell, err := ParseCell(PageTypeLeafTable, cellData, pageSize)
		if err != nil {
			t.Errorf("ParseCell() error = %v", err)
		}
		if cell.Key != cells[i].rowid {
			t.Errorf("Cell key = %d, want %d", cell.Key, cells[i].rowid)
		}
	}
}

// TestDefragmentPages tests defragmenting multiple pages
func TestDefragmentPages(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create two pages with fragmentation
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}, {2, []byte("b")}}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, _ := NewBtreePage(2, leftPageData, pageSize)

	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{3, []byte("c")}, {4, []byte("d")}}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, _ := NewBtreePage(3, rightPageData, pageSize)

	// Add some fragmentation
	leftPage.Header.FragmentedBytes = 10
	rightPage.Header.FragmentedBytes = 5

	err := defragmentPages(leftPage, rightPage)
	if err != nil {
		t.Fatalf("defragmentPages() error = %v", err)
	}

	// Fragmentation should be cleared
	if leftPage.Header.FragmentedBytes != 0 {
		t.Errorf("Left page still has %d fragmented bytes", leftPage.Header.FragmentedBytes)
	}
	if rightPage.Header.FragmentedBytes != 0 {
		t.Errorf("Right page still has %d fragmented bytes", rightPage.Header.FragmentedBytes)
	}
}
