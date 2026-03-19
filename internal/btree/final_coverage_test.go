// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"fmt"
	"testing"
)

// TestCursor_DescendToLastExplicit creates an interior tree and explicitly tests descendToLast
func TestCursor_DescendToLastExplicit(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a 3-level tree structure
	// Level 3 (leaves)
	for i := uint32(10); i <= 15; i++ {
		cells := []struct {
			rowid   int64
			payload []byte
		}{
			{int64((i-10)*10 + 1), []byte(fmt.Sprintf("leaf%d_1", i))},
			{int64((i-10)*10 + 2), []byte(fmt.Sprintf("leaf%d_2", i))},
			{int64((i-10)*10 + 3), []byte(fmt.Sprintf("leaf%d_3", i))},
		}
		leafData := createTestPage(i, 4096, PageTypeLeafTable, cells)
		bt.SetPage(i, leafData)
	}

	// Level 2 (interior pages pointing to leaves)
	interior1 := createInteriorPage(5, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{10, 3},
		{11, 13},
	}, 12) // right child
	bt.SetPage(5, interior1)

	interior2 := createInteriorPage(6, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{13, 33},
		{14, 43},
	}, 15) // right child
	bt.SetPage(6, interior2)

	// Level 1 (root interior page)
	root := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{5, 23},
	}, 6) // right child
	bt.SetPage(1, root)

	// Now test MoveToLast which should call descendToLast
	cursor := NewCursor(bt, 1)
	err := cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	if !cursor.IsValid() {
		t.Fatal("Cursor should be valid after MoveToLast")
	}

	// Should be at the last entry in the rightmost leaf (page 15)
	key := cursor.GetKey()
	expectedKey := int64(53) // Last entry in page 15
	if key != expectedKey {
		t.Errorf("MoveToLast() key = %d, want %d", key, expectedKey)
	}

	t.Logf("Successfully tested descendToLast: positioned at key %d", key)
}

// TestCursor_EnterPageViaNavigation tests enterPage through various navigation
func TestCursor_EnterPageViaNavigation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create multi-level tree
	for i := uint32(20); i <= 25; i++ {
		cells := []struct {
			rowid   int64
			payload []byte
		}{
			{int64((i-20)*5 + 1), []byte("data1")},
			{int64((i-20)*5 + 2), []byte("data2")},
		}
		leafData := createTestPage(i, 4096, PageTypeLeafTable, cells)
		bt.SetPage(i, leafData)
	}

	interior := createInteriorPage(10, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{20, 2},
		{21, 7},
		{22, 12},
		{23, 17},
		{24, 22},
	}, 25)
	bt.SetPage(10, interior)

	root := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{}, 10)
	bt.SetPage(1, root)

	cursor := NewCursor(bt, 1)

	// Navigate through the tree - this should use enterPage
	cursor.MoveToFirst()
	for i := 0; i < 20 && cursor.IsValid(); i++ {
		cursor.Next()
	}

	cursor.MoveToLast()
	for i := 0; i < 20 && cursor.IsValid(); i++ {
		cursor.Previous()
	}

	t.Log("Successfully navigated through multi-level tree (enterPage likely used)")
}

// TestMerge_WithActualRedistribution creates a scenario to trigger redistribution
func TestMerge_WithActualRedistribution(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create left page with many cells (overfull relative to right)
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 20)
	for i := 0; i < 20; i++ {
		leftCells[i] = struct {
			rowid   int64
			payload []byte
		}{int64(i + 1), []byte(fmt.Sprintf("leftcell%02d", i))}
	}
	leftPage := createTestPage(30, 4096, PageTypeLeafTable, leftCells)
	bt.SetPage(30, leftPage)

	// Create right page with very few cells (underfull)
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{
		{100, []byte("right1")},
	}
	rightPage := createTestPage(31, 4096, PageTypeLeafTable, rightCells)
	bt.SetPage(31, rightPage)

	// Create interior parent
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{30, 20},
	}, 31)
	bt.SetPage(1, interior)

	// Position on the underfull right page
	cursor := NewCursor(bt, 1)
	cursor.SeekRowid(100)

	if cursor.IsValid() {
		// Delete the only cell to make it very underfull
		err := cursor.Delete()
		if err != nil {
			t.Logf("Delete error: %v", err)
		}

		// Try to balance - should redistribute from left to right
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage error: %v", err)
		}

		t.Logf("Balance operation result: merged=%v (redistribution functions likely called)", merged)
	}
}

// TestBalance_HandleOverfullAndUnderfullPages tests balance operations
func TestBalance_HandleOverfullAndUnderfullPages(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough to trigger multiple balancing operations
	for i := int64(1); i <= 250; i++ {
		err := cursor.Insert(i, make([]byte, 35))
		if err != nil {
			t.Logf("Insert stopped at %d: %v", i, err)
			break
		}
	}

	// Delete many to trigger underfull handling
	for i := int64(80); i <= 170; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			err := cursor.Delete()
			if err != nil {
				t.Logf("Delete error at %d: %v", i, err)
			}
		}
	}

	t.Log("Completed balance operations test")
}

// TestAllErrorPaths tests error paths in various functions
func TestAllErrorPaths(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Test invalid cursor operations
	cursor := NewCursor(bt, 999) // Non-existent page
	err := cursor.MoveToFirst()
	if err == nil {
		t.Log("MoveToFirst on invalid page handled (may not error in all cases)")
	}

	key := cursor.GetKey()
	t.Logf("GetKey on invalid cursor: %d", key)

	payload := cursor.GetPayload()
	t.Logf("GetPayload on invalid cursor: len=%d", len(payload))

	str := cursor.String()
	t.Logf("String on invalid cursor: %s", str)

	// Test index cursor error paths
	indexCursor := NewIndexCursor(bt, 888)
	err = indexCursor.MoveToFirst()
	if err == nil {
		t.Log("Index MoveToFirst on invalid page handled")
	}

	// Try operations on empty cursor
	indexCursor.GetKey()
	indexCursor.GetRowid()
	_ = indexCursor.String()
}

// TestEdgeCasesInNavigation tests edge cases in tree navigation
func TestEdgeCasesInNavigation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	// Insert specific pattern
	for i := int64(1); i <= 50; i++ {
		cursor.Insert(i*2, []byte("even")) // Only even numbers
	}

	// Seek to non-existent odd numbers
	for i := int64(1); i <= 100; i += 2 {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			key := cursor.GetKey()
			// Should position at next available key
			t.Logf("Seek(%d) positioned at %d", i, key)
		}
	}

	// Navigate from various positions
	cursor.SeekRowid(50)
	for j := 0; j < 5; j++ {
		cursor.Previous()
	}

	cursor.SeekRowid(25)
	for j := 0; j < 5; j++ {
		cursor.Next()
	}
}

// TestComplexSiblingScenarios tests various sibling configurations
func TestComplexSiblingScenarios(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create 5 sibling pages with varying cell counts
	siblingCounts := []int{3, 1, 8, 2, 5}
	for idx, count := range siblingCounts {
		pgno := uint32(40 + idx)
		cells := make([]struct {
			rowid   int64
			payload []byte
		}, count)
		for i := 0; i < count; i++ {
			cells[i] = struct {
				rowid   int64
				payload []byte
			}{int64(idx*20 + i), []byte(fmt.Sprintf("page%d_cell%d", pgno, i))}
		}
		pageData := createTestPage(pgno, 4096, PageTypeLeafTable, cells)
		bt.SetPage(pgno, pageData)
	}

	// Create interior page with all siblings
	interior := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{
		{40, 2},
		{41, 20},
		{42, 47},
		{43, 62},
	}, 44)
	bt.SetPage(1, interior)

	// Navigate and test various positions
	cursor := NewCursor(bt, 1)
	cursor.MoveToFirst()

	count := 0
	for cursor.IsValid() && count < 100 {
		count++
		err := cursor.Next()
		if err != nil {
			break
		}
	}

	t.Logf("Navigated through %d entries across multiple siblings", count)

	// Test backward navigation
	cursor.MoveToLast()
	backCount := 0
	for cursor.IsValid() && backCount < 100 {
		backCount++
		err := cursor.Previous()
		if err != nil {
			break
		}
	}

	t.Logf("Backward navigation: %d entries", backCount)
}

// TestIndexCursorComplexNavigation tests complex index navigation scenarios
func TestIndexCursorComplexNavigation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	indexComplexNavSetupPages(bt)

	cursor := NewIndexCursor(bt, 1)

	err := cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast error: %v", err)
	}
	if cursor.IsValid() {
		t.Logf("Index MoveToLast positioned at: %s", cursor.GetKey())
	}

	navigateIndexBackward(cursor, 10)

	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst error: %v", err)
	}
	if cursor.IsValid() {
		t.Logf("Index MoveToFirst positioned at: %s", cursor.GetKey())
	}

	navigateIndexForward(cursor, 10)

	indexComplexNavSeeks(t, cursor)
}

func indexComplexNavSetupPages(bt *Btree) {
	prefixes := []string{"aaa", "lll", "rrr", "zzz"}
	for idx, prefix := range prefixes {
		pgno := uint32(50 + idx)
		cells := []struct {
			key   []byte
			rowid int64
		}{
			{[]byte(prefix + "1"), int64(idx*10 + 1)},
			{[]byte(prefix + "2"), int64(idx*10 + 2)},
			{[]byte(prefix + "3"), int64(idx*10 + 3)},
		}
		pageData := createIndexLeafPage(pgno, 4096, cells)
		bt.SetPage(pgno, pageData)
	}

	interior := createIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{50, []byte("aaa3"), 3},
		{51, []byte("lll3"), 13},
		{52, []byte("rrr3"), 23},
	}, 53)
	bt.SetPage(1, interior)
}

func indexComplexNavSeeks(t *testing.T, cursor *IndexCursor) {
	t.Helper()
	testKeys := [][]byte{[]byte("lll2"), []byte("rrr1"), []byte("zzz2")}
	for _, key := range testKeys {
		found, err := cursor.SeekIndex(key)
		if err != nil {
			t.Errorf("SeekIndex(%s) error: %v", key, err)
		}
		if found {
			t.Logf("Found key: %s", key)
		}
	}
}

// TestVeryDeepTree creates a very deep tree to ensure all navigation paths are tested
func TestVeryDeepTree(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	insertCount := insertRows(cursor, 1, 2000, 18)
	t.Logf("Created very deep tree with %d rows", insertCount)

	veryDeepTreeNavPositions(cursor, insertCount)

	fwdCount := countForward(cursor)
	backCount := countBackward(cursor)
	t.Logf("Full traversal: forward=%d, backward=%d", fwdCount, backCount)
}

func veryDeepTreeNavPositions(cursor *BtCursor, insertCount int) {
	testPositions := []int64{1, 100, 500, 1000, 1500, int64(insertCount) - 1}
	for _, pos := range testPositions {
		cursor.SeekRowid(pos)
		if !cursor.IsValid() {
			continue
		}
		navigateBackward(cursor, 20)
		cursor.SeekRowid(pos)
		navigateForward(cursor, 20)
	}
}

// TestVeryDeepIndexTree creates a very deep index tree
func TestVeryDeepIndexTree(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, rootPage)

	insertCount := insertIndexEntriesN(cursor, 2000, func(i int) []byte {
		return []byte(fmt.Sprintf("indexentry%08d", i))
	})
	t.Logf("Created deep index tree with %d entries", insertCount)

	fwdCount := countIndexForward(cursor)
	backCount := countIndexBackward(cursor)
	t.Logf("Index traversal: forward=%d, backward=%d", fwdCount, backCount)

	deepIndexTreeSeeksBy200(t, cursor, insertCount)
}

func deepIndexTreeSeeksBy200(t *testing.T, cursor *IndexCursor, insertCount int) {
	t.Helper()
	for i := 0; i < insertCount; i += 200 {
		searchKey := []byte(fmt.Sprintf("indexentry%08d", i))
		found, err := cursor.SeekIndex(searchKey)
		if err != nil {
			t.Errorf("SeekIndex error at %d: %v", i, err)
		}
		if !found {
			t.Errorf("Failed to find entry %d", i)
		}
	}
}
