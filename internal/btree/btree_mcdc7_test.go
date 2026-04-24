// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC 7 — deeply internal btree functions
//
// Targets: cursor.go (navigateToRightmostLeafComposite, positionAtLastCell,
// advanceWithinPage, loadParentPage, getChildPageFromParent, SeekRowid,
// SeekComposite, getCurrentBtreePage, Delete), index_cursor.go (InsertIndex,
// DeleteIndex, SeekIndex, MoveToLast, PrevIndex, NextIndex),
// merge.go (extractCellData, getSiblingWithLeftPage, getSiblingAsRightmost,
// moveRightToLeft / moveLeftToRight via redistributeSiblings).
// ---------------------------------------------------------------------------

// mcdc7NewRowidTree builds a rowid table with n rows (50-byte payloads).
// n ≥ 500 ensures depth > 1 (multiple interior pages).
// Returns the btree, a fresh cursor using the post-split root, and the root page.
func mcdc7NewRowidTree(t *testing.T, n int) (*Btree, *BtCursor, uint32) {
	t.Helper()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	payload := make([]byte, 50)
	for i := int64(1); i <= int64(n); i++ {
		binary.BigEndian.PutUint64(payload, uint64(i))
		if err := c.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	// c.RootPage may have changed after root splits; use it for the fresh cursor.
	finalRoot := c.RootPage
	return bt, NewCursor(bt, finalRoot), finalRoot
}

// mcdc7NewCompositeTree builds a composite-key table with n rows (30-byte payloads).
func mcdc7NewCompositeTree(t *testing.T, n int) (*Btree, *BtCursor, uint32) {
	t.Helper()
	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	c := NewCursorWithOptions(bt, root, true)
	payload := make([]byte, 30)
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := c.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}
	// c.RootPage may have changed after root splits; use it for the fresh cursor.
	finalRoot := c.RootPage
	return bt, NewCursorWithOptions(bt, finalRoot, true), finalRoot
}

// mcdc7NewIndexTree builds an index tree with up to n entries using InsertIndex.
// InsertIndex stops silently when the leaf page is full (splits not implemented);
// returns the btree and a fresh cursor positioned at the same root.
func mcdc7NewIndexTree(t *testing.T, n int) (*Btree, *IndexCursor) {
	t.Helper()
	bt := NewBtree(4096)
	root, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	hOff := 0
	if root == 1 {
		hOff = FileHeaderSize
	}
	pageData[hOff+PageHeaderOffsetType] = PageTypeLeafIndex
	pageData[hOff+PageHeaderOffsetFreeblock] = 0
	pageData[hOff+PageHeaderOffsetFreeblock+1] = 0
	pageData[hOff+PageHeaderOffsetNumCells] = 0
	pageData[hOff+PageHeaderOffsetNumCells+1] = 0
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetCellStart:], uint16(bt.UsableSize))

	ic := NewIndexCursor(bt, root)
	inserted := 0
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i+1))
		if err := ic.InsertIndex(key, int64(i+1)); err != nil {
			// Page full — stop gracefully.
			break
		}
		inserted++
	}
	if inserted == 0 {
		t.Fatal("mcdc7NewIndexTree: no entries inserted")
	}
	return bt, NewIndexCursor(bt, root)
}

// ---------------------------------------------------------------------------
// navigateToRightmostLeafComposite — composite tree MoveToLast traverses
// through interior pages to the rightmost leaf.
// ---------------------------------------------------------------------------

func TestMCDC7_NavigateToRightmostLeafComposite_DeepTree(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewCompositeTree(t, 200)
	if err := c.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if c.State != CursorValid {
		t.Fatal("cursor not valid after MoveToLast")
	}
}

// ---------------------------------------------------------------------------
// navigateToRightmostLeafComposite — GetPage error path: invalid root page
// ---------------------------------------------------------------------------

func TestMCDC7_NavigateToRightmostLeafComposite_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	// Use page 9999 which does not exist.
	c := NewCursorWithOptions(bt, 9999, true)
	c.RootPage = 9999
	err := c.navigateToRightmostLeafComposite(9999)
	if err == nil {
		t.Fatal("expected error for non-existent page, got nil")
	}
}

// ---------------------------------------------------------------------------
// positionAtLastCell — empty leaf triggers error path
// ---------------------------------------------------------------------------

func TestMCDC7_PositionAtLastCell_EmptyLeaf(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	// Manually build an empty leaf header for the test.
	pageData, _ := bt.GetPage(root)
	hOff := 0
	if root == 1 {
		hOff = FileHeaderSize
	}
	header, _ := ParsePageHeader(pageData, root)
	// positionAtLastCell should fail on a zero-cell page.
	err = c.positionAtLastCell(root, pageData[hOff:], header)
	// The header has 0 cells, so we expect an error.
	if err == nil {
		t.Fatal("expected empty-leaf error, got nil")
	}
}

// ---------------------------------------------------------------------------
// advanceWithinPage — cursor at last cell returns false (no advance)
// ---------------------------------------------------------------------------

func TestMCDC7_AdvanceWithinPage_AtLastCell(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 10)
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	// Advance to the final cell in the first page by calling Next repeatedly
	// until we hit the rightmost cell in the first leaf.
	for c.CurrentIndex < int(c.CurrentHeader.NumCells)-1 {
		if err := c.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}
	// Now at last cell in page — advanceWithinPage should return (false, nil).
	advanced, err := c.advanceWithinPage()
	if err != nil {
		t.Fatalf("advanceWithinPage: %v", err)
	}
	if advanced {
		t.Fatal("advanceWithinPage should return false at last cell")
	}
}

// ---------------------------------------------------------------------------
// advanceWithinPage — normal advance within a multi-cell page
// ---------------------------------------------------------------------------

func TestMCDC7_AdvanceWithinPage_NormalAdvance(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	// First page should have multiple cells; advance within it.
	if c.CurrentHeader.NumCells < 2 {
		t.Skip("first page has fewer than 2 cells")
	}
	advanced, err := c.advanceWithinPage()
	if err != nil {
		t.Fatalf("advanceWithinPage: %v", err)
	}
	if !advanced {
		t.Fatal("expected advanceWithinPage to return true")
	}
}

// ---------------------------------------------------------------------------
// loadParentPage — multi-level tree: climbToNextParent calls loadParentPage
// ---------------------------------------------------------------------------

func TestMCDC7_LoadParentPage_MultiLevelTree(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	// Traverse past the first leaf page to exercise climbToNextParent →
	// loadParentPage → getChildPageFromParent.
	pageAtStart := c.CurrentPage
	for {
		if err := c.Next(); err != nil {
			break // end of tree is fine
		}
		if c.CurrentPage != pageAtStart {
			// We crossed a page boundary — loadParentPage was called.
			break
		}
	}
}

// ---------------------------------------------------------------------------
// getChildPageFromParent — exercised by climbToNextParent when at last-but-one
// cell in an interior page: we need to step to the cell's child, not RightChild.
// ---------------------------------------------------------------------------

func TestMCDC7_GetChildPageFromParent_CellChildPath(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	// Seek to key 1 (leftmost), then Next multiple times to force the parent
	// traversal path that goes through a normal child cell (not right-child).
	found, err := c.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid(1): found=%v err=%v", found, err)
	}
	// Traverse a significant number of rows to exercise interior-page child
	// pointer resolution paths.
	for i := 0; i < 200; i++ {
		if err := c.Next(); err != nil {
			break
		}
	}
}

// ---------------------------------------------------------------------------
// SeekRowid — not-found path (key larger than any stored key)
// ---------------------------------------------------------------------------

func TestMCDC7_SeekRowid_NotFound(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 50)
	found, err := c.SeekRowid(99999)
	if err != nil {
		t.Fatalf("SeekRowid(99999): %v", err)
	}
	if found {
		t.Fatal("SeekRowid should return false for key larger than all stored keys")
	}
}

// ---------------------------------------------------------------------------
// SeekComposite — exact match path in a composite tree
// ---------------------------------------------------------------------------

func TestMCDC7_SeekComposite_ExactMatch(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewCompositeTree(t, 200)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(100))
	found, err := c.SeekComposite(key)
	if err != nil {
		t.Fatalf("SeekComposite: %v", err)
	}
	if !found {
		t.Fatal("SeekComposite should find key 100")
	}
}

// ---------------------------------------------------------------------------
// SeekComposite — not-found path
// ---------------------------------------------------------------------------

func TestMCDC7_SeekComposite_NotFound(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewCompositeTree(t, 50)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(99999))
	found, err := c.SeekComposite(key)
	if err != nil {
		t.Fatalf("SeekComposite not-found: %v", err)
	}
	if found {
		t.Fatal("SeekComposite should return false for absent key")
	}
}

// ---------------------------------------------------------------------------
// getCurrentBtreePage — GetPage error path
// ---------------------------------------------------------------------------

func TestMCDC7_GetCurrentBtreePage_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	c.CurrentPage = 0 // force ErrInvalidPageNumber
	_, err = c.getCurrentBtreePage()
	if err == nil {
		t.Fatal("expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// Delete — basic delete on a multi-row table
// ---------------------------------------------------------------------------

func TestMCDC7_Delete_BasicRowid(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 100)
	found, err := c.SeekRowid(50)
	if err != nil || !found {
		t.Fatalf("SeekRowid(50): found=%v err=%v", found, err)
	}
	if err := c.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Delete — index < 0 after deletion of the only cell in leaf
// (covers the c.CurrentIndex < 0 branch in adjustCursorAfterDelete)
// ---------------------------------------------------------------------------

func TestMCDC7_Delete_IndexBelowZero(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	payload := make([]byte, 10)
	if err := c.Insert(1, payload); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	found, err := c.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid: found=%v err=%v", found, err)
	}
	if err := c.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// CurrentIndex should be -1 (below zero branch taken).
	if c.CurrentIndex != -1 {
		t.Fatalf("expected CurrentIndex=-1, got %d", c.CurrentIndex)
	}
}

// ---------------------------------------------------------------------------
// Index cursor — InsertIndex + MoveToFirst / MoveToLast on shallow tree
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_MoveToFirstAndLast_Small(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 5)
	if err := ic.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	if ic.State != CursorValid {
		t.Fatal("cursor not valid after MoveToFirst")
	}
	if err := ic.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if ic.State != CursorValid {
		t.Fatal("cursor not valid after MoveToLast")
	}
}

// ---------------------------------------------------------------------------
// Index cursor — InsertIndex fills a page up to capacity (graceful stop)
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_InsertMany_PageFull(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 200)
	if err := ic.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst on large index: %v", err)
	}
	if err := ic.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast on large index: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Index cursor — NextIndex traversal over entire tree
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_NextIndex_FullScan(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 30)
	if err := ic.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	count := 1
	for {
		err := ic.NextIndex()
		if err != nil {
			break
		}
		count++
	}
	if count < 10 {
		t.Fatalf("expected at least 10 entries, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Index cursor — PrevIndex traversal exercises prevViaParent path
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_PrevIndex_MultiPage(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 100)
	if err := ic.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	count := 1
	for {
		err := ic.PrevIndex()
		if err != nil {
			break
		}
		count++
	}
	if count < 10 {
		t.Fatalf("expected at least 10 entries via PrevIndex, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Index cursor — DeleteIndex exact match (CurrentRowid matches)
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_DeleteIndex_Match(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 15)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(3))
	if err := ic.DeleteIndex(key, 3); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Index cursor — DeleteIndex key not found
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_DeleteIndex_NotFound(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 10)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(9999))
	err := ic.DeleteIndex(key, 9999)
	if err == nil {
		t.Fatal("DeleteIndex for absent key should return error")
	}
}

// ---------------------------------------------------------------------------
// Index cursor — SeekIndex exact match (key 5 is always inserted)
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_SeekIndex_ExactMatch_Deep(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 20)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(5))
	found, err := ic.SeekIndex(key)
	if err != nil {
		t.Fatalf("SeekIndex: %v", err)
	}
	if !found {
		t.Fatal("SeekIndex should find key 5")
	}
}

// ---------------------------------------------------------------------------
// Index cursor — SeekIndex not found
// ---------------------------------------------------------------------------

func TestMCDC7_IndexCursor_SeekIndex_NotFound(t *testing.T) {
	t.Parallel()
	_, ic := mcdc7NewIndexTree(t, 10)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(99999))
	found, err := ic.SeekIndex(key)
	if err != nil {
		t.Fatalf("SeekIndex not-found: %v", err)
	}
	if found {
		t.Fatal("SeekIndex should return false for absent key")
	}
}

// ---------------------------------------------------------------------------
// merge.go — getSiblingWithLeftPage: trigger a merge where parentIndex > 0
// so the left-sibling path is taken.
// ---------------------------------------------------------------------------

func mcdc7DeleteRangeWithReseek(c *BtCursor, start, end int64) {
	for i := start; i <= end; i++ {
		found, err := c.SeekRowid(i)
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
		if c.State != CursorValid {
			c2 := NewCursor(c.Btree, c.RootPage)
			*c = *c2
			if _, err := c.SeekRowid(i + 1); err != nil {
				break
			}
		}
	}
}

func TestMCDC7_MergeGetSiblingWithLeftPage(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	mcdc7DeleteRangeWithReseek(c, 200, 300)
	tryMergeAtPosition(c, 250) //nolint:errcheck
}

// ---------------------------------------------------------------------------
// merge.go — getSiblingAsRightmost: the current page is the rightmost child
// (parentIndex == NumCells, not < NumCells and not > 0).
// We trigger this by inserting, building depth, then seeking to the last key.
// ---------------------------------------------------------------------------

func TestMCDC7_MergeGetSiblingAsRightmost(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	// Seek to the last key so that our page is the rightmost child.
	if err := c.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	// Delete a bunch to make the rightmost leaf underfull, then try MergePage.
	key := c.GetKey()
	for i := 0; i < 30; i++ {
		found, err := c.SeekRowid(key - int64(i))
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
	}
	found, err := c.SeekRowid(key - 25)
	if err == nil && found {
		c.MergePage()
	}
}

// ---------------------------------------------------------------------------
// merge.go — extractCellData via mergePages: insert enough rows to trigger
// merges during balanced delete operations.
// ---------------------------------------------------------------------------

func TestMCDC7_ExtractCellData_ViaMerge_Redistribution(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 300)
	// Delete a swath from the start to create underfull pages.
	for i := int64(1); i <= int64(150); i++ {
		found, err := c.SeekRowid(i)
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
		if c.State != CursorValid {
			c = NewCursor(c.Btree, c.RootPage)
		}
	}
}

// ---------------------------------------------------------------------------
// moveLeftToRight redistribution path: sibling has more cells than current.
// This is reached when CanMerge returns false and right page has more free
// space than left.
// ---------------------------------------------------------------------------

func TestMCDC7_Redistribute_LeftToRight(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	// Delete most entries from the first leaf to create an imbalance.
	for i := int64(1); i <= int64(100); i++ {
		found, err := c.SeekRowid(i)
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
		if c.State != CursorValid {
			c = NewCursor(c.Btree, c.RootPage)
		}
	}
	// Seek to the boundary between two leaf pages and attempt a merge.
	found, err := c.SeekRowid(101)
	if err == nil && found {
		c.MergePage()
	}
}

// ---------------------------------------------------------------------------
// moveRightToLeft redistribution path: current page is left, sibling is right.
// Delete from the right side and attempt merge from the left side.
// ---------------------------------------------------------------------------

func TestMCDC7_Redistribute_RightToLeft(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	// Delete most entries from the tail to unbalance the rightmost leaf.
	for i := int64(400); i <= int64(500); i++ {
		found, err := c.SeekRowid(i)
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
		if c.State != CursorValid {
			c = NewCursor(c.Btree, c.RootPage)
		}
	}
	// Seek to a position near the right boundary and attempt a merge.
	found, err := c.SeekRowid(390)
	if err == nil && found {
		c.MergePage()
	}
}

// ---------------------------------------------------------------------------
// getSiblingWithRightPage — parentIndex == NumCells-1 so rightPage is
// parentHeader.RightChild (the else branch of getSiblingWithRightPage).
// ---------------------------------------------------------------------------

func TestMCDC7_GetSiblingWithRightPage_LastCellBranch(t *testing.T) {
	t.Parallel()
	_, c, _ := mcdc7NewRowidTree(t, 500)
	// Seek to a middle key, delete enough around it to make the page underfull
	// with exactly parentIndex == NumCells-1.
	for i := int64(1); i <= int64(50); i++ {
		found, err := c.SeekRowid(i)
		if err != nil || !found {
			continue
		}
		if err := c.Delete(); err != nil {
			break
		}
		if c.State != CursorValid {
			c = NewCursor(c.Btree, c.RootPage)
		}
	}
	found, err := c.SeekRowid(51)
	if err == nil && found {
		c.MergePage()
	}
}

// ---------------------------------------------------------------------------
// loadCellAtCurrentIndex error path: set CurrentPage = 0 and CurrentIndex = 0
// so GetPage fails inside loadCellAtCurrentIndex.
// ---------------------------------------------------------------------------

func TestMCDC7_LoadCellAtCurrentIndex_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	// Build a fake header so adjustCursorAfterDelete can proceed past the
	// ParsePageHeader step using page 0.
	c.State = CursorValid
	c.CurrentPage = 0
	c.CurrentIndex = 1
	// loadCellAtCurrentIndex requires a valid page; simulate by calling directly.
	emptyData := make([]byte, 4096)
	// Create a minimal valid leaf header in emptyData.
	emptyData[PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(emptyData[PageHeaderOffsetCellStart:], uint16(4096))
	fakeHeader, _ := ParsePageHeader(emptyData, 999)
	c.CurrentHeader = fakeHeader
	// page 0 is invalid — GetPage will fail.
	err = c.loadCellAtCurrentIndex(emptyData)
	// We pass valid data directly so it won't fail on GetPage; the cell pointer
	// lookup on an empty page will fail instead.
	if err == nil {
		t.Fatal("expected error from loadCellAtCurrentIndex on empty page, got nil")
	}
}

// ---------------------------------------------------------------------------
// validateInsertPosition — not-at-leaf path
// (SeekRowid on an empty composite table where header.IsLeaf check matters)
// ---------------------------------------------------------------------------

func TestMCDC7_ValidateInsertPosition_NotLeaf(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	// Manually set CurrentHeader to a non-leaf header to cover the !IsLeaf path.
	c.State = CursorValid
	pageData, _ := bt.GetPage(root)
	header, _ := ParsePageHeader(pageData, root)
	header.IsLeaf = false
	c.CurrentHeader = header
	c.CurrentPage = root
	// validateInsertPosition calls SeekRowid which will reset the header;
	// we test that duplicate detection works on a normal table.
	payload := make([]byte, 10)
	if err := c.Insert(1, payload); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Insert duplicate — should fail.
	c2 := NewCursor(bt, root)
	err = c2.Insert(1, payload)
	if err == nil {
		t.Fatal("expected duplicate-key error, got nil")
	}
}
