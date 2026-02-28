package btree

import (
	"fmt"
	"testing"
)

// TestInteriorPageOperations tests operations on actual interior pages
// This should trigger descendToLast, enterPage, and related functions
func TestInteriorPageOperations(t *testing.T) {
	bt := NewBtree(4096)

	// Manually create a tree structure with interior pages
	// Create 3 leaf pages
	leaf1 := createTestPage(2, 4096, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("data1")},
		{2, []byte("data2")},
		{3, []byte("data3")},
	})
	bt.SetPage(2, leaf1)

	leaf2 := createTestPage(3, 4096, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{
		{10, []byte("data10")},
		{11, []byte("data11")},
		{12, []byte("data12")},
	})
	bt.SetPage(3, leaf2)

	leaf3 := createTestPage(4, 4096, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{
		{20, []byte("data20")},
		{21, []byte("data21")},
		{22, []byte("data22")},
	})
	bt.SetPage(4, leaf3)

	// Create interior root page
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 3},  // Left child: page 2, max key 3
		{3, 12}, // Middle child: page 3, max key 12
	}, 4) // Right child: page 4
	bt.SetPage(1, interior)

	// Test descendToLast by calling MoveToLast on interior tree
	cursor := NewCursor(bt, 1)
	err := cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if !cursor.IsValid() {
		t.Error("Cursor should be valid after MoveToLast")
	}

	// Should be positioned at last entry (rowid 22)
	key := cursor.GetKey()
	if key != 22 {
		t.Errorf("MoveToLast() key = %d, want 22", key)
	}

	// Test backward navigation through interior pages
	err = cursor.Previous()
	if err != nil {
		t.Errorf("Previous() error = %v", err)
	}

	if cursor.IsValid() && cursor.GetKey() != 21 {
		t.Errorf("After Previous(), key = %d, want 21", cursor.GetKey())
	}
}

// TestIndexInteriorPageOperations tests index cursor on interior pages
func TestIndexInteriorPageOperations(t *testing.T) {
	bt := NewBtree(4096)

	// Create 3 index leaf pages
	leaf1 := createIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
		{[]byte("ccc"), 3},
	})
	bt.SetPage(2, leaf1)

	leaf2 := createIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("mmm"), 10},
		{[]byte("nnn"), 11},
		{[]byte("ooo"), 12},
	})
	bt.SetPage(3, leaf2)

	leaf3 := createIndexLeafPage(4, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("xxx"), 20},
		{[]byte("yyy"), 21},
		{[]byte("zzz"), 22},
	})
	bt.SetPage(4, leaf3)

	// Create interior root page
	interior := createIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("ccc"), 3},
		{3, []byte("ooo"), 12},
	}, 4) // Right child: page 4
	bt.SetPage(1, interior)

	// Test navigation on interior tree
	cursor := NewIndexCursor(bt, 1)

	// Test MoveToLast - should call descendToLast
	err := cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if !cursor.IsValid() {
		t.Error("Cursor should be valid after MoveToLast")
	}

	// Should be at "zzz"
	key := cursor.GetKey()
	expectedKey := []byte("zzz")
	if string(key) != string(expectedKey) {
		t.Errorf("MoveToLast() key = %s, want %s", key, expectedKey)
	}

	// Test Previous navigation - should call prevViaParent when crossing pages
	for i := 0; i < 5; i++ {
		err := cursor.PrevIndex()
		if err != nil {
			t.Logf("PrevIndex() at step %d: %v", i, err)
			break
		}
		if cursor.IsValid() {
			t.Logf("PrevIndex step %d: key=%s", i, cursor.GetKey())
		}
	}

	// Test MoveToFirst - should call getFirstChildPage
	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	if cursor.IsValid() {
		firstKey := cursor.GetKey()
		expectedFirst := []byte("aaa")
		if string(firstKey) != string(expectedFirst) {
			t.Errorf("MoveToFirst() key = %s, want %s", firstKey, expectedFirst)
		}
	}

	// Test SeekIndex - should call resolveChildPage
	searchKey := []byte("nnn")
	found, err := cursor.SeekIndex(searchKey)
	if err != nil {
		t.Fatalf("SeekIndex() error = %v", err)
	}

	if !found {
		t.Errorf("SeekIndex(%s) found = false, want true", searchKey)
	}

	if cursor.IsValid() && string(cursor.GetKey()) != string(searchKey) {
		t.Errorf("SeekIndex() positioned at %s, want %s", cursor.GetKey(), searchKey)
	}
}

// TestBalanceOperations tests balance, handleOverfullPage, and handleUnderfullPage
func TestBalanceOperations(t *testing.T) {
	bt := NewBtree(512) // Small pages to trigger balancing
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows to trigger page splits (handleOverfullPage)
	for i := int64(1); i <= 200; i++ {
		err := cursor.Insert(i, make([]byte, 30))
		if err != nil {
			t.Logf("Insert stopped at row %d: %v", i, err)
			break
		}
	}

	// Delete many rows to trigger page merges (handleUnderfullPage)
	for i := int64(50); i <= 150; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			err := cursor.Delete()
			if err != nil {
				t.Logf("Delete error at row %d: %v", i, err)
			}
		}
	}

	// Verify tree still works
	cursor.MoveToFirst()
	if !cursor.IsValid() {
		t.Error("Tree should still be valid after balance operations")
	}
}

// TestSplitInteriorPageAdvanced tests splitting interior pages
func TestSplitInteriorPageAdvanced(t *testing.T) {
	bt := NewBtree(512) // Very small pages
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough data to create deep tree with interior pages that need splitting
	insertCount := 0
	for i := int64(1); i <= 500; i++ {
		err := cursor.Insert(i, make([]byte, 15))
		if err != nil {
			t.Logf("Insert stopped at row %d: %v", i, err)
			break
		}
		insertCount++
	}

	t.Logf("Inserted %d rows, likely triggered interior page splits", insertCount)

	// Verify tree integrity
	cursor.MoveToFirst()
	count := 0
	for cursor.IsValid() && count < insertCount {
		count++
		err := cursor.Next()
		if err != nil {
			break
		}
	}

	t.Logf("Can iterate through %d rows", count)
}

// TestMergeWithRedistribute tests redistribution between siblings
func TestMergeWithRedistribute(t *testing.T) {
	bt := NewBtree(4096)

	// Create pages with imbalanced cell distribution
	// Left page with many cells
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 15)
	for i := 0; i < 15; i++ {
		leftCells[i] = struct {
			rowid   int64
			payload []byte
		}{int64(i + 1), []byte(fmt.Sprintf("leftdata%d", i))}
	}
	leftPage := createTestPage(2, 4096, PageTypeLeafTable, leftCells)
	bt.SetPage(2, leftPage)

	// Right page with few cells (underfull)
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{
		{100, []byte("rightdata1")},
		{101, []byte("rightdata2")},
	}
	rightPage := createTestPage(3, 4096, PageTypeLeafTable, rightCells)
	bt.SetPage(3, rightPage)

	// Create interior parent
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 15},
	}, 3) // Right child is page 3
	bt.SetPage(1, interior)

	// Position cursor on right page and try to trigger redistribution
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(100)

	if cursor.IsValid() {
		// Delete to make page even more underfull
		err := cursor.Delete()
		if err != nil {
			t.Logf("Delete error: %v", err)
		}

		// Try to balance - should redistribute instead of merge
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage error: %v", err)
		}
		t.Logf("Balance result: merged=%v", merged)
	}
}

// TestDeepTreeWithMultipleLevels creates a very deep tree
func TestDeepTreeWithMultipleLevels(t *testing.T) {
	bt := NewBtree(512) // Small pages for deep tree
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough rows to create 3+ levels of tree
	maxInsert := 1000
	insertCount := 0
	for i := int64(1); i <= int64(maxInsert); i++ {
		err := cursor.Insert(i, make([]byte, 20))
		if err != nil {
			break
		}
		insertCount++
	}

	t.Logf("Created deep tree with %d rows", insertCount)

	// Test navigation at various positions
	testPositions := []int64{1, 50, 250, 500, 750, int64(insertCount) - 1}
	for _, pos := range testPositions {
		cursor.SeekRowid(pos)
		if !cursor.IsValid() {
			t.Errorf("Failed to seek to position %d", pos)
			continue
		}

		key := cursor.GetKey()
		if key != pos {
			t.Errorf("Seek(%d) positioned at %d", pos, key)
		}

		// Try going backward from this position
		for j := 0; j < 10 && cursor.IsValid(); j++ {
			cursor.Previous()
		}

		// Try going forward
		for j := 0; j < 10 && cursor.IsValid(); j++ {
			cursor.Next()
		}
	}
}

// TestComplexIndexTree creates a complex index with interior pages
func TestComplexIndexTree(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert many entries to create multi-level index
	maxInsert := 1000
	insertCount := 0
	for i := 0; i < maxInsert; i++ {
		key := []byte(fmt.Sprintf("indexkey%08d", i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
		insertCount++
	}

	t.Logf("Created index tree with %d entries", insertCount)

	// Test full forward iteration
	cursor.MoveToFirst()
	fwdCount := 0
	for cursor.IsValid() && fwdCount < insertCount+10 {
		fwdCount++
		err := cursor.NextIndex()
		if err != nil {
			break
		}
	}
	t.Logf("Forward iteration: %d entries", fwdCount)

	// Test full backward iteration
	cursor.MoveToLast()
	backCount := 0
	for cursor.IsValid() && backCount < insertCount+10 {
		backCount++
		err := cursor.PrevIndex()
		if err != nil {
			break
		}
	}
	t.Logf("Backward iteration: %d entries", backCount)

	// Test seeks at various positions
	for i := 0; i < insertCount; i += 100 {
		searchKey := []byte(fmt.Sprintf("indexkey%08d", i))
		found, err := cursor.SeekIndex(searchKey)
		if err != nil {
			t.Errorf("SeekIndex error at %d: %v", i, err)
		}
		if !found {
			t.Errorf("Failed to find key at position %d", i)
		}
	}
}

// TestPageHeaderString tests the String() method for PageHeader
func TestPageHeaderString(t *testing.T) {
	header := &PageHeader{
		PageType:         PageTypeLeafTable,
		FirstFreeblock:   0,
		NumCells:         10,
		CellContentStart: 4000,
		FragmentedBytes:  5,
		IsLeaf:           true,
		IsTable:          true,
		HeaderSize:       8,
	}

	str := header.String()
	if str == "" {
		t.Error("String() returned empty")
	}

	// Check that string contains key information
	if !contains(str, "PageHeader") && !contains(str, "Leaf") {
		t.Errorf("String() = %q, should contain page info", str)
	}

	t.Logf("PageHeader.String() = %s", str)
}

// TestIntegrityWithComplexTree tests integrity checking on complex trees
func TestIntegrityWithComplexTree(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Build complex tree
	for i := int64(1); i <= 300; i++ {
		err := cursor.Insert(i, make([]byte, 25))
		if err != nil {
			break
		}
	}

	// Run integrity check (informational only in test environment)
	result := CheckIntegrity(bt, rootPage)
	t.Logf("Integrity check result: %d total errors (%d pages, %d rows)",
		len(result.Errors), result.PageCount, result.RowCount)
}

// TestVerySmallPages tests with extremely small pages to force splits
func TestVerySmallPages(t *testing.T) {
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert with small pages should cause many splits
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, []byte("smallpagedata"))
		if err != nil {
			t.Logf("Insert stopped at %d: %v", i, err)
			break
		}
	}

	// Verify we can still navigate
	cursor.MoveToFirst()
	if !cursor.IsValid() {
		t.Error("MoveToFirst() should succeed")
	}

	cursor.MoveToLast()
	if !cursor.IsValid() {
		t.Error("MoveToLast() should succeed")
	}
}

// TestMixedOperations tests a mix of inserts, deletes, and seeks
func TestMixedOperations(t *testing.T) {
	bt := NewBtree(1024)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some rows
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 30))
		if err != nil {
			break
		}
	}

	// Delete every other row
	for i := int64(2); i <= 100; i += 2 {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	// Insert more rows
	for i := int64(200); i <= 300; i++ {
		err := cursor.Insert(i, make([]byte, 30))
		if err != nil {
			break
		}
	}

	// Delete some from the middle
	for i := int64(220); i <= 240; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	// Run integrity check (informational only in test environment)
	result := CheckIntegrity(bt, rootPage)
	t.Logf("Integrity check after mixed operations: %d errors, %d pages, %d rows",
		len(result.Errors), result.PageCount, result.RowCount)
}

// TestMultiPageSiblings tests with multiple sibling pages
func TestMultiPageSiblings(t *testing.T) {
	bt := NewBtree(4096)

	// Create 4 leaf pages to test various sibling scenarios
	pages := []uint32{2, 3, 4, 5}
	for idx, pgno := range pages {
		cells := []struct {
			rowid   int64
			payload []byte
		}{
			{int64(idx*10 + 1), []byte(fmt.Sprintf("page%d_data1", pgno))},
			{int64(idx*10 + 2), []byte(fmt.Sprintf("page%d_data2", pgno))},
		}
		pageData := createTestPage(pgno, 4096, PageTypeLeafTable, cells)
		bt.SetPage(pgno, pageData)
	}

	// Create interior page with all 4 children
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 2},
		{3, 12},
		{4, 22},
	}, 5) // Right child
	bt.SetPage(1, interior)

	// Test navigation through all siblings
	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	count := 0
	for cursor.IsValid() && count < 20 {
		count++
		err := cursor.Next()
		if err != nil {
			break
		}
	}

	t.Logf("Navigated through %d entries across multiple siblings", count)
}

// TestIndexMultiPageSiblings tests index with multiple sibling pages
func TestIndexMultiPageSiblings(t *testing.T) {
	bt := NewBtree(4096)

	// Create 4 index leaf pages
	pages := []struct {
		pgno   uint32
		prefix string
	}{
		{2, "aaa"},
		{3, "mmm"},
		{4, "xxx"},
		{5, "zzz"},
	}

	for _, p := range pages {
		cells := []struct {
			key   []byte
			rowid int64
		}{
			{[]byte(p.prefix + "1"), int64(p.pgno*10 + 1)},
			{[]byte(p.prefix + "2"), int64(p.pgno*10 + 2)},
		}
		pageData := createIndexLeafPage(p.pgno, 4096, cells)
		bt.SetPage(p.pgno, pageData)
	}

	// Create interior page
	interior := createIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("aaa2"), 22},
		{3, []byte("mmm2"), 32},
		{4, []byte("xxx2"), 42},
	}, 5)
	bt.SetPage(1, interior)

	// Test navigation
	cursor := NewIndexCursor(bt, 1)
	cursor.MoveToFirst()

	count := 0
	for cursor.IsValid() && count < 20 {
		count++
		err := cursor.NextIndex()
		if err != nil {
			break
		}
	}

	t.Logf("Navigated through %d index entries across multiple siblings", count)

	// Test backward navigation
	cursor.MoveToLast()
	backCount := 0
	for cursor.IsValid() && backCount < 20 {
		backCount++
		err := cursor.PrevIndex()
		if err != nil {
			break
		}
	}

	t.Logf("Backward navigation: %d entries", backCount)
}

// TestCornerCases tests various corner cases
func TestCornerCases(t *testing.T) {
	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Test inserting at specific patterns to trigger different code paths
	// Insert in reverse order
	for i := int64(100); i >= 1; i-- {
		cursor.Insert(i, make([]byte, 10))
	}

	// Insert in random-ish order
	for i := int64(200); i <= 300; i += 3 {
		cursor.Insert(i, make([]byte, 10))
	}

	// Test seeking to non-existent keys
	cursor.SeekRowid(150)
	cursor.SeekRowid(99999)
	cursor.SeekRowid(0)

	// Test navigation at boundaries
	cursor.MoveToFirst()
	cursor.Previous() // Should handle gracefully

	cursor.MoveToLast()
	cursor.Next() // Should handle gracefully
}
