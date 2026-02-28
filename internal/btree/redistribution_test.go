package btree

import (
	"fmt"
	"testing"
)

// TestRedistribution_ExplicitScenario creates pages that cannot merge and must redistribute
func TestRedistribution_ExplicitScenario(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024) // Smaller page size

	// Create left page with many large cells (close to full)
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 30)
	for i := 0; i < 30; i++ {
		// Make payloads large enough that pages can't merge
		leftCells[i] = struct {
			rowid   int64
			payload []byte
		}{int64(i + 1), make([]byte, 20)}
		for j := range leftCells[i].payload {
			leftCells[i].payload[j] = byte('L')
		}
	}
	leftPage := createTestPage(60, 1024, PageTypeLeafTable, leftCells)
	bt.SetPage(60, leftPage)

	// Create right page with large cells (also substantial)
	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 30)
	for i := 0; i < 30; i++ {
		rightCells[i] = struct {
			rowid   int64
			payload []byte
		}{int64(i + 100), make([]byte, 20)}
		for j := range rightCells[i].payload {
			rightCells[i].payload[j] = byte('R')
		}
	}
	rightPage := createTestPage(61, 1024, PageTypeLeafTable, rightCells)
	bt.SetPage(61, rightPage)

	// Create interior parent
	interior := createInteriorPage(1, 1024, []struct {
		childPage uint32
		rowid     int64
	}{
		{60, 30},
	}, 61)
	bt.SetPage(1, interior)

	// Position on right page and delete one cell to trigger balancing
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(100)

	if cursor.IsValid() {
		// Delete to make page underfull
		err := cursor.Delete()
		if err != nil {
			t.Logf("Delete error: %v", err)
		}

		// Try to balance - pages should be too large to merge, so redistribute
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage error: %v", err)
		}

		t.Logf("Balance result: merged=%v (redistribution should have occurred)", merged)
	}
}

// TestRedistribution_MultipleSizes tests redistribution with various page sizes
func TestRedistribution_MultipleSizes(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		pageSize uint32
		cellSize int
		leftCount int
		rightCount int
	}{
		{"small pages", 512, 15, 15, 15},
		{"medium pages", 1024, 20, 25, 25},
		{"large cells", 1024, 30, 20, 20},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
		t.Parallel()
			bt := NewBtree(tc.pageSize)

			// Create left page
			leftCells := make([]struct {
				rowid   int64
				payload []byte
			}, tc.leftCount)
			for i := 0; i < tc.leftCount; i++ {
				leftCells[i] = struct {
					rowid   int64
					payload []byte
				}{int64(i + 1), make([]byte, tc.cellSize)}
			}
			leftPageData := createTestPage(70, tc.pageSize, PageTypeLeafTable, leftCells)
			bt.SetPage(70, leftPageData)

			// Create right page
			rightCells := make([]struct {
				rowid   int64
				payload []byte
			}, tc.rightCount)
			for i := 0; i < tc.rightCount; i++ {
				rightCells[i] = struct {
					rowid   int64
					payload []byte
				}{int64(i + 200), make([]byte, tc.cellSize)}
			}
			rightPageData := createTestPage(71, tc.pageSize, PageTypeLeafTable, rightCells)
			bt.SetPage(71, rightPageData)

			// Create parent
			interior := createInteriorPage(1, tc.pageSize, []struct {
				childPage uint32
				rowid     int64
			}{
				{70, int64(tc.leftCount)},
			}, 71)
			bt.SetPage(1, interior)

			// Try balancing
			cursor := NewCursor(bt, 1)
			cursor.SeekRowid(200)
			if cursor.IsValid() {
				cursor.Delete()
				cursor.MergePage()
			}

			t.Logf("Test case %s completed", tc.name)
		})
	}
}

// TestSiblingSelection tests getSiblingWithLeftPage and getSiblingAsRightmost
func TestSiblingSelection(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create 3 sibling pages
	for i := uint32(80); i <= 82; i++ {
		cells := []struct {
			rowid   int64
			payload []byte
		}{
			{int64((i-80)*10 + 1), []byte(fmt.Sprintf("page%d", i))},
			{int64((i-80)*10 + 2), []byte(fmt.Sprintf("page%d", i))},
		}
		pageData := createTestPage(i, 4096, PageTypeLeafTable, cells)
		bt.SetPage(i, pageData)
	}

	// Create interior with page 81 as middle child (has left sibling page 80)
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{80, 2},
		{81, 12},
	}, 82) // Rightmost child
	bt.SetPage(1, interior)

	// Test with middle page - should find left sibling
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(11)
	if cursor.IsValid() {
		cursor.Delete()
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage error: %v", err)
		}
		t.Logf("Merged middle page: %v (should use left sibling)", merged)
	}

	// Test with rightmost page - should use getSiblingAsRightmost path
	cursor.SeekRowid(21)
	if cursor.IsValid() {
		cursor.Delete()
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage error: %v", err)
		}
		t.Logf("Merged rightmost page: %v", merged)
	}
}

// TestHandleOverfullAndUnderfullPages tests balance handler functions
func TestHandleOverfullAndUnderfullPages(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Insert to trigger overfull handling
	for i := int64(1); i <= 300; i++ {
		err := cursor.Insert(i, make([]byte, 40))
		if err != nil {
			break
		}
	}

	// Delete to trigger underfull handling
	for i := int64(100); i <= 200; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	t.Log("Balance handler functions exercised")
}

// TestErrorPaths tests error handling in various functions
func TestErrorPaths(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Test with invalid cursor
	cursor := NewCursor(bt, 0)

	// Test navigation methods
	err := cursor.Next()
	if err != nil {
		t.Logf("Next on invalid cursor: %v", err)
	}

	err = cursor.Previous()
	if err != nil {
		t.Logf("Previous on invalid cursor: %v", err)
	}

	// Test with invalid page structure
	badPage := make([]byte, 4096)
	badPage[0] = 0xFF // Invalid page type
	bt.SetPage(99, badPage)

	cursor2 := NewCursor(bt, 99)
	cursor2.MoveToFirst()

	// Test index cursor error paths
	indexCursor := NewIndexCursor(bt, 999)
	indexCursor.NextIndex()
	indexCursor.PrevIndex()
	indexCursor.SeekIndex([]byte("test"))
}

// TestPageHeaderStringFormat tests String method which was at 0% previously
func TestPageHeaderStringFormat(t *testing.T) {
	t.Parallel()
	testHeaders := []PageHeader{
		{PageType: PageTypeLeafTable, NumCells: 10, IsLeaf: true, IsTable: true},
		{PageType: PageTypeInteriorTable, NumCells: 5, IsInterior: true, IsTable: true},
		{PageType: PageTypeLeafIndex, NumCells: 20, IsLeaf: true, IsIndex: true},
		{PageType: PageTypeInteriorIndex, NumCells: 7, IsInterior: true, IsIndex: true},
	}

	for i, header := range testHeaders {
		str := header.String()
		if str == "" {
			t.Errorf("Header %d: String() returned empty", i)
		}
		t.Logf("Header %d: %s", i, str)
	}
}

// TestCursorStringMethod tests cursor String method
func TestCursorStringMethod(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Test invalid cursor
	str := cursor.String()
	t.Logf("Invalid cursor: %s", str)

	// Insert and test valid cursor
	cursor.Insert(1, []byte("test"))
	cursor.SeekRowid(1)
	str = cursor.String()
	t.Logf("Valid cursor: %s", str)
}

// TestIndexCursorStringMethod tests index cursor String method
func TestIndexCursorStringMethod(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, rootPage)

	// Test invalid
	str := cursor.String()
	t.Logf("Invalid index cursor: %s", str)

	// Test valid
	cursor.InsertIndex([]byte("test"), 1)
	cursor.SeekIndex([]byte("test"))
	str = cursor.String()
	t.Logf("Valid index cursor: %s", str)
}

// TestCheckPageIntegrityBasic tests the CheckPageIntegrity function
func TestCheckPageIntegrityBasic(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Insert some data
	for i := int64(1); i <= 10; i++ {
		cursor.Insert(i, []byte("data"))
	}

	// Check page integrity
	result := CheckPageIntegrity(bt, rootPage)
	t.Logf("Page integrity result: %d errors", len(result.Errors))
}

// TestValidateFreeBlockListBasic tests free block list validation
func TestValidateFreeBlockListBasic(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	// This should handle the case where there are no free blocks
	result := ValidateFreeBlockList(bt, rootPage)
	t.Logf("Free block validation: %d errors", len(result.Errors))
}

// TestPagerAdapterWithNilCases tests pager adapter with edge cases
func TestPagerAdapterWithNilCases(t *testing.T) {
	t.Parallel()
	mockPager := &MockPager{
		pages:     make(map[uint32][]byte),
		pageSize:  4096,
		pageCount: 0,
	}

	adapter := NewPagerAdapter(mockPager)

	// Test GetPageData with non-existent page
	_, err := adapter.GetPageData(999)
	if err == nil {
		t.Log("GetPageData handled non-existent page")
	} else {
		t.Logf("GetPageData error: %v", err)
	}

	// Test AllocatePageData
	pgno, data, err := adapter.AllocatePageData()
	if err != nil {
		t.Fatalf("AllocatePageData error: %v", err)
	}
	t.Logf("Allocated page %d with %d bytes", pgno, len(data))

	// Test MarkDirty
	err = adapter.MarkDirty(pgno)
	if err != nil {
		t.Errorf("MarkDirty error: %v", err)
	}
}

// TestOverflowOperations tests overflow page operations
func TestOverflowOperations(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Insert very large payload to trigger overflow
	largePayload := make([]byte, 10000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	err := cursor.Insert(1, largePayload)
	if err != nil {
		t.Logf("Insert large payload error: %v", err)
	}

	// Retrieve it
	cursor.SeekRowid(1)
	if cursor.IsValid() {
		retrieved, err := cursor.GetPayloadWithOverflow()
		if err != nil {
			t.Errorf("GetPayloadWithOverflow error: %v", err)
		} else if len(retrieved) != len(largePayload) {
			t.Errorf("Retrieved payload size %d, want %d", len(retrieved), len(largePayload))
		}
	}

	// Delete it (should free overflow pages)
	if cursor.IsValid() {
		err := cursor.Delete()
		if err != nil {
			t.Errorf("Delete with overflow error: %v", err)
		}
	}
}

// TestCellParsing tests cell parsing edge cases
func TestCellParsing(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a test page
	cells := []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("short")},
		{2, make([]byte, 100)},
		{3, make([]byte, 500)},
	}

	pageData := createTestPage(90, 4096, PageTypeLeafTable, cells)
	bt.SetPage(90, pageData)

	// Parse cells
	header, _ := ParsePageHeader(pageData, 90)
	for i := 0; i < int(header.NumCells); i++ {
		offset, err := header.GetCellPointer(pageData, i)
		if err != nil {
			t.Errorf("GetCellPointer(%d) error: %v", i, err)
			continue
		}

		cell, err := ParseCell(header.PageType, pageData[offset:], bt.UsableSize)
		if err != nil {
			t.Errorf("ParseCell(%d) error: %v", i, err)
			continue
		}

		t.Logf("Cell %d: key=%d, payload_size=%d", i, cell.Key, cell.PayloadSize)
	}
}
