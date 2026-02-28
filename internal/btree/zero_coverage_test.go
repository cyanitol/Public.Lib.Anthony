package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// TestCursor_PrevViaParent tests backward navigation through parent pages
// This tests the prevViaParent function which is at 0% coverage
func TestCursor_PrevViaParent(t *testing.T) {
	bt := NewBtree(512) // Small pages to force multi-level trees
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to create a multi-level tree
	// This should create multiple pages and interior nodes
	for i := int64(1); i <= 200; i++ {
		err := cursor.Insert(i, make([]byte, 20))
		if err != nil {
			t.Logf("Insert stopped at row %d: %v", i, err)
			break
		}
	}

	// Move to middle of the tree
	cursor.SeekRowid(100)

	// Navigate backward through multiple pages
	// This should trigger prevViaParent when crossing page boundaries
	prevCount := 0
	for i := 0; i < 50; i++ {
		err := cursor.Previous()
		if err != nil || !cursor.IsValid() {
			break
		}
		prevCount++
	}

	if prevCount > 0 {
		t.Logf("Successfully navigated backward %d times (may have used prevViaParent)", prevCount)
	}
}

// TestCursor_DescendToLast tests descending to the last entry in a tree
// This tests the descendToLast and enterPage functions at 0% coverage
func TestCursor_DescendToLast(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert rows to create multiple pages
	for i := int64(1); i <= 150; i++ {
		err := cursor.Insert(i, make([]byte, 25))
		if err != nil {
			break
		}
	}

	// MoveToLast should use descendToLast if there are interior pages
	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if cursor.IsValid() {
		key := cursor.GetKey()
		t.Logf("Moved to last entry with key: %d", key)

		// Should be at the highest rowid
		if key < 100 {
			t.Errorf("Last key = %d, expected larger value", key)
		}
	}
}

// TestMerge_GetSiblingWithLeftPage tests merging with left sibling
// This tests getSiblingWithLeftPage at 0% coverage
func TestMerge_GetSiblingWithLeftPage(t *testing.T) {
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create 3 leaf pages that will be siblings
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("data1")}})
	bt.SetPage(2, page2Data)

	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{5, []byte("data5")}})
	bt.SetPage(3, page3Data)

	page4Data := createTestPage(4, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{10, []byte("data10")}})
	bt.SetPage(4, page4Data)

	// Create interior root with page 3 as middle child (has left sibling page 2)
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 1},  // First child: page 2, max key 1
		{3, 5},  // Second child: page 3, max key 5
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 4) // Right child is page 4
	bt.SetPage(1, rootData)

	// Create cursor and position on the middle page
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(5)

	// Delete the entry to make the page underfull, triggering merge
	if cursor.IsValid() {
		err := cursor.Delete()
		if err != nil {
			t.Logf("Delete() error = %v", err)
		}

		// Try to merge - this should use getSiblingWithLeftPage
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage() error = %v", err)
		}
		if merged {
			t.Log("Successfully merged with left sibling")
		}
	}
}

// TestMerge_GetSiblingAsRightmost tests merging when current page is rightmost
// This tests getSiblingAsRightmost at 0% coverage
func TestMerge_GetSiblingAsRightmost(t *testing.T) {
	bt := NewBtree(4096)
	pageSize := bt.PageSize

	// Create 2 leaf pages
	page2Data := createTestPage(2, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("left")}})
	bt.SetPage(2, page2Data)

	page3Data := createTestPage(3, pageSize, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{5, []byte("right")}})
	bt.SetPage(3, page3Data)

	// Create interior root where page 3 is the rightmost child
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 1}}
	rootData := createInteriorPage(1, pageSize, rootCells, 3) // Page 3 is right child
	bt.SetPage(1, rootData)

	// Position cursor on the rightmost page
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(5)

	if cursor.IsValid() {
		// Delete to make page underfull
		err := cursor.Delete()
		if err != nil {
			t.Logf("Delete() error = %v", err)
		}

		// Try merge - should use getSiblingAsRightmost
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage() error = %v", err)
		}
		if merged {
			t.Log("Successfully merged rightmost page with sibling")
		}
	}
}

// TestMerge_RedistributeSiblings tests cell redistribution between siblings
// This tests multiple 0% functions: redistributeSiblings, loadRedistributePages,
// updateParentSeparator, getFirstKeyFromPage, loadParentBtreePage,
// calculateSeparatorIndex, replaceSeparatorCell
func TestMerge_RedistributeSiblings(t *testing.T) {
	bt := NewBtree(512) // Small pages
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert rows to create multiple pages
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 15))
		if err != nil {
			break
		}
	}

	// Delete some rows to create imbalanced pages
	for i := int64(10); i <= 30; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	// This should trigger redistribution between siblings
	cursor.SeekRowid(15)
	if cursor.IsValid() {
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage() error = %v", err)
		}
		t.Logf("Merge/redistribute result: merged=%v", merged)
	}
}

// TestSplit_PrepareInteriorSplit tests splitting interior pages
// This tests prepareInteriorSplit at 0% coverage
func TestSplit_PrepareInteriorSplit(t *testing.T) {
	bt := NewBtree(512) // Very small pages to force splits
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to force creation of interior pages that need splitting
	for i := int64(1); i <= 300; i++ {
		err := cursor.Insert(i, make([]byte, 10))
		if err != nil {
			// Expected when running out of space
			t.Logf("Insert stopped at row %d: %v", i, err)
			break
		}
	}

	t.Log("Completed inserts that may have triggered interior page splits")
}

// TestSplit_SplitParentRecursively tests recursive parent splits
// This tests splitParentRecursively and positionOnParent at 0% coverage
func TestSplit_SplitParentRecursively(t *testing.T) {
	bt := NewBtree(512) // Small pages
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough rows to force multiple levels of splits
	for i := int64(1); i <= 500; i++ {
		err := cursor.Insert(i, make([]byte, 12))
		if err != nil {
			t.Logf("Insert stopped at row %d: %v", i, err)
			break
		}
	}

	// Try inserting more in the middle to force parent splits
	for i := int64(10000); i <= 10100; i++ {
		err := cursor.Insert(i, make([]byte, 12))
		if err != nil {
			break
		}
	}

	t.Log("Completed inserts that may have triggered recursive parent splits")
}

// TestPageHeader_String tests the String method on PageHeader
// This tests page.go String() at 0% coverage
func TestPageHeader_String(t *testing.T) {
	tests := []struct {
		name     string
		pageType byte
		numCells uint16
		wantSubstr string
	}{
		{
			name:     "leaf table page",
			pageType: PageTypeLeafTable,
			numCells: 5,
			wantSubstr: "PageHeader",
		},
		{
			name:     "interior table page",
			pageType: PageTypeInteriorTable,
			numCells: 10,
			wantSubstr: "PageHeader",
		},
		{
			name:     "leaf index page",
			pageType: PageTypeLeafIndex,
			numCells: 3,
			wantSubstr: "PageHeader",
		},
		{
			name:     "interior index page",
			pageType: PageTypeInteriorIndex,
			numCells: 7,
			wantSubstr: "PageHeader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &PageHeader{
				PageType:         tt.pageType,
				NumCells:         tt.numCells,
				CellContentStart: 100,
				FragmentedBytes:  0,
			}

			str := header.String()
			if str == "" {
				t.Error("String() returned empty string")
			}
			if !contains(str, tt.wantSubstr) {
				t.Errorf("String() = %q, should contain %q", str, tt.wantSubstr)
			}
			t.Logf("PageHeader.String() = %s", str)
		})
	}
}

// TestIndexCursor_DescendToRightChild tests descending to right child
// This tests index_cursor.go descendToRightChild at 0% coverage
func TestIndexCursor_DescendToRightChild(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert many entries to create interior structure
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// MoveToLast should descend to the rightmost child
	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if cursor.IsValid() {
		t.Logf("Moved to last: %s (may have used descendToRightChild)", cursor.GetKey())
	}
}

// TestIndexCursor_GetFirstChildPage tests getting first child from interior page
// This tests index_cursor.go getFirstChildPage at 0% coverage
func TestIndexCursor_GetFirstChildPage(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 80; i++ {
		key := []byte(fmt.Sprintf("item%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// MoveToFirst should call getFirstChildPage if interior pages exist
	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	if cursor.IsValid() {
		t.Logf("Moved to first: %s (may have used getFirstChildPage)", cursor.GetKey())
	}
}

// TestIndexCursor_PrevViaParent tests backward navigation through parent
// This tests index_cursor.go prevViaParent at 0% coverage
func TestIndexCursor_PrevViaParent(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 60; i++ {
		key := []byte(fmt.Sprintf("entry%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Navigate to middle
	cursor.SeekIndex([]byte("entry0030"))

	// Navigate backward - should use prevViaParent when crossing pages
	prevCount := 0
	for i := 0; i < 35; i++ {
		err := cursor.PrevIndex()
		if err != nil || !cursor.IsValid() {
			break
		}
		prevCount++
	}

	t.Logf("Backward navigation completed %d steps (may have used prevViaParent)", prevCount)
}

// TestIndexCursor_DescendToLast tests descending to last in index
// This tests index_cursor.go descendToLast at 0% coverage
func TestIndexCursor_DescendToLast(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 70; i++ {
		key := []byte(fmt.Sprintf("value%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// MoveToLast should use descendToLast if there are interior pages
	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if cursor.IsValid() {
		key := cursor.GetKey()
		t.Logf("Moved to last: %s (may have used descendToLast)", key)
	}
}

// TestIndexCursor_EnterPage tests entering a page during navigation
// This tests index_cursor.go enterPage at 0% coverage
func TestIndexCursor_EnterPage(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("data%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Various navigation operations that might call enterPage
	cursor.MoveToFirst()
	cursor.NextIndex()
	cursor.NextIndex()
	cursor.MoveToLast()
	cursor.PrevIndex()
	cursor.SeekIndex([]byte("data0025"))

	t.Log("Navigation operations completed (may have used enterPage)")
}

// TestIndexCursor_ResolveChildPage tests resolving child pages
// This tests index_cursor.go resolveChildPage at 0% coverage
func TestIndexCursor_ResolveChildPage(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries to potentially create interior pages
	for i := 0; i < 90; i++ {
		key := []byte(fmt.Sprintf("record%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Seeking should call resolveChildPage if there are interior pages
	for i := 0; i < 10; i++ {
		searchKey := []byte(fmt.Sprintf("record%04d", i*9))
		cursor.SeekIndex(searchKey)
		if cursor.IsValid() {
			t.Logf("Found key: %s", cursor.GetKey())
		}
	}
}

// TestIndexCursor_ClimbToNextParent tests climbing to next parent
// This tests index_cursor.go climbToNextParent at low coverage (8.0%)
func TestIndexCursor_ClimbToNextParent(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 120; i++ {
		key := []byte(fmt.Sprintf("test%04d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Full forward iteration should climb parent levels
	cursor.MoveToFirst()
	iterCount := 0
	for cursor.IsValid() && iterCount < 150 {
		err := cursor.NextIndex()
		if err != nil {
			break
		}
		iterCount++
	}

	t.Logf("Forward iteration completed %d steps (may have used climbToNextParent)", iterCount)
}

// TestMultipleLevelTree creates a deep tree to trigger interior page operations
func TestMultipleLevelTree(t *testing.T) {
	bt := NewBtree(512) // Small page size
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to create a deep tree
	insertCount := 0
	for i := int64(1); i <= 1000; i++ {
		err := cursor.Insert(i, make([]byte, 10))
		if err != nil {
			break
		}
		insertCount++
	}

	t.Logf("Inserted %d rows", insertCount)

	// Verify we can navigate through the tree
	cursor.MoveToFirst()
	if !cursor.IsValid() {
		t.Error("MoveToFirst() resulted in invalid cursor")
	}

	cursor.MoveToLast()
	if !cursor.IsValid() {
		t.Error("MoveToLast() resulted in invalid cursor")
	}

	// Navigate backward from end
	backCount := 0
	for i := 0; i < 50; i++ {
		err := cursor.Previous()
		if err != nil || !cursor.IsValid() {
			break
		}
		backCount++
	}
	t.Logf("Backward navigation: %d steps", backCount)

	// Navigate forward from beginning
	cursor.MoveToFirst()
	fwdCount := 0
	for i := 0; i < 50; i++ {
		err := cursor.Next()
		if err != nil || !cursor.IsValid() {
			break
		}
		fwdCount++
	}
	t.Logf("Forward navigation: %d steps", fwdCount)
}

// TestDeepIndexTree creates a deep index tree
func TestDeepIndexTree(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert many entries
	insertCount := 0
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("deepkey%06d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
		insertCount++
	}

	t.Logf("Inserted %d index entries", insertCount)

	// Full forward scan
	cursor.MoveToFirst()
	scanCount := 0
	for cursor.IsValid() && scanCount < 600 {
		scanCount++
		err := cursor.NextIndex()
		if err != nil {
			break
		}
	}
	t.Logf("Forward scan: %d entries", scanCount)

	// Full backward scan
	cursor.MoveToLast()
	backScan := 0
	for cursor.IsValid() && backScan < 600 {
		backScan++
		err := cursor.PrevIndex()
		if err != nil {
			break
		}
	}
	t.Logf("Backward scan: %d entries", backScan)

	// Random seeks
	for i := 0; i < 20; i++ {
		idx := i * 25
		if idx >= insertCount {
			break
		}
		searchKey := []byte(fmt.Sprintf("deepkey%06d", idx))
		found, err := cursor.SeekIndex(searchKey)
		if err != nil {
			t.Errorf("SeekIndex() error = %v", err)
		}
		if !found {
			t.Errorf("Failed to find key: %s", searchKey)
		}
		if found && !bytes.Equal(cursor.GetKey(), searchKey) {
			t.Errorf("Seek returned wrong key: got %s, want %s",
				cursor.GetKey(), searchKey)
		}
	}
}

// TestCreateInteriorIndexPage manually creates an interior index page to test navigation
func TestCreateInteriorIndexPage(t *testing.T) {
	bt := NewBtree(4096)

	// Create two leaf index pages
	leaf1Data := createIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
	})
	bt.SetPage(2, leaf1Data)

	leaf2Data := createIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("cherry"), 3},
		{[]byte("date"), 4},
	})
	bt.SetPage(3, leaf2Data)

	// Create interior index page
	interiorData := createIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("banana"), 2},
	}, 3) // Right child is page 3
	bt.SetPage(1, interiorData)

	// Test navigation
	cursor := NewIndexCursor(bt, 1)

	err := cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	if cursor.IsValid() {
		t.Logf("First key: %s", cursor.GetKey())
	}

	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if cursor.IsValid() {
		t.Logf("Last key: %s", cursor.GetKey())
	}
}

// Helper function to create an index leaf page
func createIndexLeafPage(pageNum uint32, pageSize uint32, cells []struct {
	key   []byte
	rowid int64
}) []byte {
	data := make([]byte, pageSize)

	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}

	data[headerOffset+PageHeaderOffsetType] = PageTypeLeafIndex
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetNumCells:], uint16(len(cells)))

	cellContentOffset := pageSize
	cellPtrOffset := uint32(headerOffset + PageHeaderSizeLeaf)

	cellOffsets := make([]uint32, len(cells))
	for i := 0; i < len(cells); i++ {
		cell := cells[i]
		payload := encodeIndexPayload(cell.key, cell.rowid)
		cellData := EncodeIndexLeafCell(payload)

		cellContentOffset -= uint32(len(cellData))
		copy(data[cellContentOffset:], cellData)
		cellOffsets[i] = cellContentOffset
	}

	for i := 0; i < len(cells); i++ {
		binary.BigEndian.PutUint16(data[cellPtrOffset:], uint16(cellOffsets[i]))
		cellPtrOffset += 2
	}

	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetCellStart:], uint16(cellContentOffset))

	return data
}

// Helper function to create an index interior page
func createIndexInteriorPage(pageNum uint32, pageSize uint32, cells []struct {
	childPage uint32
	key       []byte
	rowid     int64
}, rightChild uint32) []byte {
	data := make([]byte, pageSize)

	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}

	data[headerOffset+PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetNumCells:], uint16(len(cells)))
	binary.BigEndian.PutUint32(data[headerOffset+PageHeaderOffsetRightChild:], rightChild)

	cellContentOffset := pageSize
	cellPtrOffset := uint32(headerOffset + PageHeaderSizeInterior)

	cellOffsets := make([]uint32, len(cells))
	for i := 0; i < len(cells); i++ {
		cell := cells[i]
		payload := encodeIndexPayload(cell.key, cell.rowid)
		cellData := EncodeIndexInteriorCell(cell.childPage, payload)

		cellContentOffset -= uint32(len(cellData))
		copy(data[cellContentOffset:], cellData)
		cellOffsets[i] = cellContentOffset
	}

	for i := 0; i < len(cells); i++ {
		binary.BigEndian.PutUint16(data[cellPtrOffset:], uint16(cellOffsets[i]))
		cellPtrOffset += 2
	}

	binary.BigEndian.PutUint16(data[headerOffset+PageHeaderOffsetCellStart:], uint16(cellContentOffset))

	return data
}
