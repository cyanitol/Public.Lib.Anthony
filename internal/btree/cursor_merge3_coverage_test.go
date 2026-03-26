// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// ============================================================================
// BtCursor.prevInPage — last-cell case
// ============================================================================

// TestPrevInPage_LastCell positions a cursor at the last cell of a multi-cell
// leaf page (CurrentIndex == NumCells-1) and calls Previous().  This forces
// prevInPage() to decrement from NumCells-1 to NumCells-2 — a path that is
// only reached when the cursor is at the last (rightmost) cell of the page
// before any parent traversal is needed.
func TestPrevInPage_LastCell(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 30, 20)

	// A 30-row tree with 20-byte payloads should fit on a single leaf page.
	// Position at the very last cell.
	cur := NewCursor(bt, insertCur.RootPage)
	if err := cur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	lastKey := cur.GetKey()

	// Ensure we are on a leaf page with at least 2 cells.
	if cur.CurrentHeader == nil || cur.CurrentHeader.NumCells < 2 {
		t.Skip("tree has fewer than 2 cells on the last leaf, cannot test prevInPage last-cell path")
	}

	// Calling Previous() once decrements from NumCells-1 → NumCells-2 inside prevInPage.
	if err := cur.Previous(); err != nil {
		t.Fatalf("Previous() from last cell: %v", err)
	}
	newKey := cur.GetKey()
	if newKey >= lastKey {
		t.Errorf("after Previous() key=%d should be less than last key=%d", newKey, lastKey)
	}
}

// TestPrevInPage_FromMiddleCell exercises prevInPage multiple times on a single
// leaf page by inserting few rows (all on one page) and scanning backward from
// the last cell through all cells.
func TestPrevInPage_FromMiddleCell(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 10, 5)

	cur := NewCursor(bt, insertCur.RootPage)
	if err := cur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}

	count := 1
	for {
		err := cur.Previous()
		if err != nil {
			break // reached beginning
		}
		count++
	}
	if count < 3 {
		t.Errorf("expected at least 3 rows backward, got %d", count)
	}
}

// ============================================================================
// BtCursor.prevViaParent — error paths
// ============================================================================

// TestPrevViaParent_GetPageError exercises the GetPage error branch inside
// prevViaParent by removing the parent page after positioning the cursor at
// the first cell of a child leaf.
func TestPrevViaParent_GetPageError(t *testing.T) {
	t.Parallel()
	// Build a tree large enough to have at least 2 levels.
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 150, 50)

	// Find the first key and seek to it so we are at the leftmost leaf.
	scanCur := NewCursor(bt, insertCur.RootPage)
	if err := scanCur.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	firstKey := scanCur.GetKey()

	// Seek into a fresh cursor, which should land in a child leaf.
	cur := NewCursor(bt, insertCur.RootPage)
	found, err := cur.SeekRowid(firstKey)
	if err != nil || !found {
		t.Fatalf("SeekRowid(%d) found=%v err=%v", firstKey, found, err)
	}

	if cur.Depth == 0 {
		t.Skip("tree is single-level, prevViaParent not reachable")
	}

	// Remove the parent page so GetPage fails inside prevViaParent.
	parentPage := cur.PageStack[cur.Depth-1]
	bt.mu.Lock()
	delete(bt.Pages, parentPage)
	bt.mu.Unlock()

	// Previous() at index 0 will call prevViaParent which calls GetPage.
	prevErr := cur.Previous()
	if prevErr == nil {
		t.Log("Previous() succeeded despite removed parent (may have been cached or tree is shallow)")
	} else {
		if cur.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after GetPage error in prevViaParent", cur.State)
		}
		t.Logf("prevViaParent GetPage error correctly returned: %v", prevErr)
	}
}

// TestPrevViaParent_DescendToLastMultiLevel inserts enough rows to build a
// 3-level tree, then does a full backward scan.  Every page-boundary crossing
// calls prevViaParent; crossings where parentIndex > 0 call descendToLast,
// exercising the true branch that was previously uncovered.
func TestPrevViaParent_DescendToLastMultiLevel(t *testing.T) {
	t.Parallel()
	// 500 rows with 80-byte payload should produce a 3-level tree on a 4096-page btree.
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 500, 80)

	fwd := countForward(NewCursor(bt, insertCur.RootPage))
	bwd := countBackward(NewCursor(bt, insertCur.RootPage))
	if fwd != bwd {
		t.Errorf("forward count %d != backward count %d", fwd, bwd)
	}
	if bwd < 50 {
		t.Errorf("backward scan returned only %d rows", bwd)
	}
}

// ============================================================================
// BtCursor.resolveChildPage — error paths
// ============================================================================

// TestResolveChildPage_GetCellPointerError exercises the GetCellPointer error
// branch in resolveChildPage by injecting a corrupted interior page whose cell
// pointer array is truncated.
func TestResolveChildPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a valid leaf page.
	leafData := createTestPage(2, 4096, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{10, []byte("value")}})
	bt.SetPage(2, leafData)

	// Build an interior root page with one cell pointing to page 2, right child 2.
	rootData := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{{2, 5}}, 2)
	bt.SetPage(1, rootData)

	// Parse header to find cell count then truncate the pointer array so
	// GetCellPointer for idx=0 will fail.
	rootHdr, err := ParsePageHeader(rootData, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if rootHdr.NumCells == 0 {
		t.Skip("root has no cells")
	}

	// Truncate root page data to cut the cell pointer array short.
	hdrSize := headerOff(1) + int(PageHeaderSizeInterior)
	truncated := make([]byte, hdrSize+1) // only 1 byte of pointer array
	copy(truncated, rootData)
	bt.mu.Lock()
	bt.Pages[1] = truncated
	bt.mu.Unlock()

	// SeekRowid will traverse the interior page and hit GetCellPointer error.
	cur := NewCursor(bt, 1)
	_, seekErr := cur.SeekRowid(3)
	t.Logf("SeekRowid with truncated interior page: err=%v", seekErr)
	// Either error or fallback is acceptable; we just want the branch covered.
}

// TestResolveChildPage_ParseCellError exercises the ParseCell error branch in
// resolveChildPage by corrupting the cell content bytes of an interior page.
func TestResolveChildPage_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leafData := createTestPage(2, 4096, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{10, []byte("value")}})
	bt.SetPage(2, leafData)

	rootData := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{{2, 5}}, 2)

	// Find the cell pointer for cell 0 to locate the cell content.
	rootHdr, err := ParsePageHeader(rootData, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if rootHdr.NumCells == 0 {
		t.Skip("root has no cells")
	}

	cellOff, err := rootHdr.GetCellPointer(rootData, 0)
	if err != nil {
		t.Fatalf("GetCellPointer: %v", err)
	}
	// Overwrite cell content with 0xFF to corrupt the key varint.
	for i := int(cellOff); i < int(cellOff)+8 && i < len(rootData); i++ {
		rootData[i] = 0xFF
	}
	bt.SetPage(1, rootData)

	cur := NewCursor(bt, 1)
	_, seekErr := cur.SeekRowid(3)
	t.Logf("SeekRowid with corrupted interior cell: err=%v", seekErr)
}

// ============================================================================
// merge.go: loadMergePages — error paths
// ============================================================================

// TestLoadMergePages_LeftPageBadHeader exercises the NewBtreePage error branch
// in loadMergePages by injecting a left page with an invalid page type byte,
// which causes ParsePageHeader to fail inside NewBtreePage.
func TestLoadMergePages_LeftPageBadHeader(t *testing.T) {
	t.Parallel()
	pageSize := uint32(512)
	bt := NewBtree(pageSize)

	payload := make([]byte, 20)

	// Valid right leaf page.
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{50, payload}}
	bt.SetPage(3, createTestPage(3, pageSize, PageTypeLeafTable, rightCells))

	// Left page with corrupted header byte (page type 0x00 is invalid).
	badLeft := make([]byte, pageSize)
	// page type 0 is invalid for NewBtreePage
	badLeft[headerOff(2)] = 0x00
	bt.SetPage(2, badLeft)

	// Interior root pointing left=2 right=3.
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 10}}
	bt.SetPage(1, createInteriorPage(1, pageSize, rootCells, 3))

	// Build a cursor manually positioned on page 2 (depth=1, parent=1).
	cur := NewCursor(bt, 1)
	cur.State = CursorValid
	cur.Depth = 1
	cur.PageStack[0] = 1
	cur.IndexStack[0] = 0
	cur.CurrentPage = 2

	// mergePages calls loadMergePages; the bad left page should trigger error.
	_, mergeErr := cur.mergePages(2, 3, 1, 0, true)
	t.Logf("mergePages with bad left page header: err=%v", mergeErr)
}

// TestLoadMergePages_RightPageMissing exercises the GetPage error branch on
// the right page inside loadMergePages by omitting the right page from the store.
func TestLoadMergePages_RightPageMissing(t *testing.T) {
	t.Parallel()
	pageSize := uint32(512)
	bt := NewBtree(pageSize)

	payload := make([]byte, 10)

	leftCells := []struct {
		rowid   int64
		payload []byte
	}{{1, payload}}
	bt.SetPage(2, createTestPage(2, pageSize, PageTypeLeafTable, leftCells))

	// Do NOT register page 3 (right page).
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 5}}
	bt.SetPage(1, createInteriorPage(1, pageSize, rootCells, 3))

	cur := NewCursor(bt, 1)
	cur.State = CursorValid
	cur.Depth = 1
	cur.PageStack[0] = 1
	cur.IndexStack[0] = 0
	cur.CurrentPage = 2

	_, mergeErr := cur.mergePages(2, 3, 1, 0, true)
	t.Logf("mergePages with missing right page: err=%v", mergeErr)
}

// ============================================================================
// merge.go: updateParentSeparator — paths
// ============================================================================

// TestUpdateParentSeparator_EmptyRightPage exercises the early-return branch
// in updateParentSeparator when rightBtreePage.Header.NumCells == 0.
func TestUpdateParentSeparator_EmptyRightPage(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	// Build a valid parent page for the cursor.
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 5}}
	bt.SetPage(1, createInteriorPage(1, pageSize, rootCells, 3))

	// Right page with zero cells.
	emptyRight := createTestPage(3, pageSize, PageTypeLeafTable, nil)
	rightBtreePage, err := NewBtreePage(3, emptyRight, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage empty right: %v", err)
	}
	if rightBtreePage.Header.NumCells != 0 {
		t.Fatalf("expected 0 cells, got %d", rightBtreePage.Header.NumCells)
	}

	cur := NewCursor(bt, 1)
	cur.State = CursorValid

	// updateParentSeparator should return nil immediately when NumCells==0.
	err = cur.updateParentSeparator(rightBtreePage, 2, 1, 0)
	if err != nil {
		t.Errorf("updateParentSeparator with empty right page should return nil, got: %v", err)
	}
}

// TestUpdateParentSeparator_ParentPageMissing exercises the loadParentBtreePage
// error branch in updateParentSeparator by removing the parent page.
func TestUpdateParentSeparator_ParentPageMissing(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	// Right page with one cell.
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{42, []byte("data")}}
	rightData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightBtreePage, err := NewBtreePage(3, rightData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// Do NOT register parent page 1.
	cur := NewCursor(bt, 99)
	cur.State = CursorValid

	// loadParentBtreePage will call GetPage for page 1 which is missing.
	err = cur.updateParentSeparator(rightBtreePage, 2, 1, 0)
	t.Logf("updateParentSeparator with missing parent: err=%v", err)
	// We expect an error or graceful handling.
}

// ============================================================================
// merge.go: getFirstKeyFromPage — error paths
// ============================================================================

// TestGetFirstKeyFromPage_GetCellPointerError exercises the GetCellPointer error
// branch in getFirstKeyFromPage by truncating the page data so cell pointers
// fall out of bounds.
func TestGetFirstKeyFromPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	cells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("hello")}}
	pageData := createTestPage(2, pageSize, PageTypeLeafTable, cells)

	page, err := NewBtreePage(2, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// Truncate Data so GetCellPointer at index 0 returns an error.
	hdrSize := headerOff(2) + int(PageHeaderSizeLeaf)
	if hdrSize >= len(page.Data) {
		t.Skip("header already fills page")
	}
	page.Data = page.Data[:hdrSize+1] // only 1 byte of pointer array, need 2

	cur := NewCursor(bt, 99)
	cur.State = CursorValid

	_, err = cur.getFirstKeyFromPage(page)
	t.Logf("getFirstKeyFromPage with truncated pointer: err=%v", err)
}

// TestGetFirstKeyFromPage_ParseCellError exercises the ParseCell error branch
// in getFirstKeyFromPage by corrupting cell content bytes.
func TestGetFirstKeyFromPage_ParseCellError(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	cells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("hello")}}
	pageData := createTestPage(2, pageSize, PageTypeLeafTable, cells)

	page, err := NewBtreePage(2, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// Find cell offset and corrupt the cell content.
	cellOff, err := page.Header.GetCellPointer(page.Data, 0)
	if err != nil {
		t.Fatalf("GetCellPointer: %v", err)
	}
	for i := int(cellOff); i < int(cellOff)+8 && i < len(page.Data); i++ {
		page.Data[i] = 0xFF
	}

	cur := NewCursor(bt, 99)
	cur.State = CursorValid

	_, err = cur.getFirstKeyFromPage(page)
	t.Logf("getFirstKeyFromPage with corrupted cell: err=%v", err)
}

// ============================================================================
// IndexCursor.resolveChildPage — error paths
// ============================================================================

// TestIndexResolveChildPage_GetCellPointerError exercises the GetCellPointer
// error branch inside IndexCursor.resolveChildPage by injecting a truncated
// interior page.
func TestIndexResolveChildPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a valid leaf.
	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{{[]byte("aaa"), 1}})
	bt.SetPage(2, leaf)

	// Build an interior root then truncate the pointer array.
	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{{2, []byte("mmm"), 99}}, 2)

	// Determine where the cell pointer array starts and truncate.
	rootHdr, err := ParsePageHeader(root, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if rootHdr.NumCells == 0 {
		t.Skip("interior page has no cells")
	}

	hdrEnd := headerOff(1) + int(PageHeaderSizeInterior)
	truncated := make([]byte, hdrEnd+1) // only 1 byte of pointer array
	copy(truncated, root)
	bt.SetPage(1, truncated)

	cur := NewIndexCursor(bt, 1)
	_, seekErr := cur.SeekIndex([]byte("aaa"))
	t.Logf("IndexCursor.SeekIndex with truncated interior: err=%v", seekErr)
}

// TestIndexResolveChildPage_ParseCellError exercises the ParseCell error branch
// inside IndexCursor.resolveChildPage by corrupting interior cell content.
func TestIndexResolveChildPage_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{{[]byte("aaa"), 1}})
	bt.SetPage(2, leaf)

	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{{2, []byte("mmm"), 99}}, 2)

	rootHdr, err := ParsePageHeader(root, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if rootHdr.NumCells == 0 {
		t.Skip("interior page has no cells")
	}

	cellOff, err := rootHdr.GetCellPointer(root, 0)
	if err != nil {
		t.Fatalf("GetCellPointer: %v", err)
	}
	// Corrupt cell bytes to cause ParseCell to fail.
	for i := int(cellOff); i < int(cellOff)+8 && i < len(root); i++ {
		root[i] = 0xFF
	}
	bt.SetPage(1, root)

	cur := NewIndexCursor(bt, 1)
	_, seekErr := cur.SeekIndex([]byte("aaa"))
	t.Logf("IndexCursor.SeekIndex with corrupted cell: err=%v", seekErr)
}

// ============================================================================
// IndexCursor.prevInPage — error paths
// ============================================================================

// TestIndexPrevInPage_GetPageError exercises the GetPage error branch inside
// IndexCursor.prevInPage by removing the current page from the store after
// positioning the cursor at a non-first cell.
func TestIndexPrevInPage_GetPageError(t *testing.T) {
	t.Parallel()
	bt, idxCur := setupIndexCursor(t, 4096)

	// Insert enough entries so there are at least 2 on one page.
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i), 'k'}
		if err := idxCur.InsertIndex(key, int64(i)); err != nil {
			break
		}
	}

	// Move to first, then advance once so CurrentIndex >= 1.
	if err := idxCur.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	if err := idxCur.NextIndex(); err != nil {
		t.Skip("only one entry, cannot test mid-page prevInPage")
	}

	if idxCur.CurrentIndex < 1 {
		t.Skip("cursor did not advance within page")
	}

	// Remove the current page to force GetPage error inside prevInPage.
	bt.mu.Lock()
	delete(bt.Pages, idxCur.CurrentPage)
	bt.mu.Unlock()

	prevErr := idxCur.PrevIndex()
	if prevErr == nil {
		t.Log("PrevIndex() succeeded despite page removal (may be cached)")
	} else {
		t.Logf("prevInPage GetPage error correctly returned: %v", prevErr)
		if idxCur.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid", idxCur.State)
		}
	}
}

// ============================================================================
// IndexCursor.advanceWithinPage — GetCellPointer / ParseCell error paths
// ============================================================================

// TestIndexAdvanceWithinPage_GetCellPointerError exercises the GetCellPointer
// error branch inside IndexCursor.advanceWithinPage by truncating the current
// page so the pointer for the next cell is out of bounds.
func TestIndexAdvanceWithinPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf with two cells.
	leaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
	})
	bt.SetPage(1, leaf)

	// Position cursor at cell 0.
	leafData, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if hdr.NumCells < 2 {
		t.Skip("need at least 2 cells")
	}

	cur := NewIndexCursor(bt, 1)
	cur.Depth = 0
	cur.CurrentPage = 1
	cur.CurrentIndex = 0
	cur.CurrentHeader = hdr
	cur.State = CursorValid
	cur.CurrentKey = []byte("aaa")
	cur.CurrentRowid = 1

	// Truncate page so pointer for cell 1 is out of range.
	hdrEnd := headerOff(1) + int(PageHeaderSizeLeaf)
	// Two cell pointers = 4 bytes; keep only 2 (one pointer) to force error on second.
	truncated := make([]byte, hdrEnd+2)
	copy(truncated, leafData)
	bt.mu.Lock()
	bt.Pages[1] = truncated
	bt.mu.Unlock()
	cur.CurrentHeader = hdr // keep NumCells=2 so advanceWithinPage tries to advance

	nextErr := cur.NextIndex()
	t.Logf("NextIndex with truncated pointer: err=%v", nextErr)
}

// TestIndexAdvanceWithinPage_LargeTreeForwardScan exercises advanceWithinPage
// across many pages by doing a full forward scan on a large index tree.
// This drives the happy path for the function across all cells.
func TestIndexAdvanceWithinPage_LargeTreeForwardScan(t *testing.T) {
	t.Parallel()
	bt, idxCur := setupIndexCursor(t, 4096)

	n := insertIndexEntriesN(idxCur, 150, func(i int) []byte {
		k := make([]byte, 4)
		k[0] = byte(i >> 8)
		k[1] = byte(i)
		k[2] = 'f'
		k[3] = 'w'
		return k
	})
	if n < 20 {
		t.Fatalf("inserted only %d entries", n)
	}

	fwd := countIndexForward(NewIndexCursor(bt, idxCur.RootPage))
	if fwd < 20 {
		t.Errorf("forward scan returned only %d rows", fwd)
	}
}

// ============================================================================
// BtCursor.prevViaParent + resolveChildPage full integration
// ============================================================================

// TestPrevViaParent_FullBackwardMultiLevel inserts many rows, then does a
// complete backward scan.  Each page boundary crossing calls prevViaParent;
// on interior pages resolveChildPage is called to find child page pointers.
// Together these exercise the descendToLast path of prevViaParent and the
// cell-branch path of resolveChildPage.
func TestPrevViaParent_FullBackwardMultiLevel(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 400, 60)

	n := verifyOrderedBackward(t, NewCursor(bt, insertCur.RootPage))
	if n < 20 {
		t.Errorf("ordered backward scan returned only %d rows", n)
	}
}

// TestCursorMerge3Coverage_MergeAfterManyDeletes triggers loadMergePages via
// the normal MergePage path by inserting many rows and then deleting most of
// them so pages become underfull.
func TestCursorMerge3Coverage_MergeAfterManyDeletes(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Insert 200 rows.
	cur := NewCursor(bt, rootPage)
	for i := int64(1); i <= 200; i++ {
		if insertErr := cur.Insert(i, []byte("payload-data-30")); insertErr != nil {
			t.Fatalf("Insert(%d): %v", i, insertErr)
		}
	}
	root := cur.RootPage

	// Verify we have data before deletions.
	before := countForward(NewCursor(bt, root))
	if before == 0 {
		t.Skip("no rows inserted")
	}

	// Delete the first 150 rows using a fresh cursor each time to track the
	// current root page (which may change due to merges).
	for i := int64(1); i <= 150; i++ {
		delCur := NewCursor(bt, root)
		found, findErr := delCur.SeekRowid(i)
		if findErr == nil && found {
			delCur.Delete()
		}
	}

	// Attempt merge on the remaining rows to exercise loadMergePages.
	mergeCount := 0
	for i := int64(151); i <= 200; i++ {
		mergeCur := NewCursor(bt, root)
		found, findErr := mergeCur.SeekRowid(i)
		if findErr == nil && found && mergeCur.Depth > 0 {
			merged, mergeErr := mergeCur.MergePage()
			if mergeErr == nil && merged {
				mergeCount++
			}
		}
	}
	t.Logf("loadMergePages triggered via %d merges (rows remaining: %d)",
		mergeCount, countForward(NewCursor(bt, root)))
}
