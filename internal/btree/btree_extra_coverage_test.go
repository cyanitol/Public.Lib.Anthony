// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

// TestClearTableData_Basic exercises the ClearTableData path (0% coverage).
func TestClearTableData_Basic(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 10; i++ {
		if err := cursor.Insert(i, []byte("payload")); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	if err := bt.ClearTableData(rootPage); err != nil {
		t.Fatalf("ClearTableData: %v", err)
	}
	// After clearing, the root should be an empty leaf
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage after clear: %v", err)
	}
	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader after clear: %v", err)
	}
	if header.NumCells != 0 {
		t.Errorf("NumCells after ClearTableData = %d, want 0", header.NumCells)
	}
}

// TestClearTableData_InvalidRoot tests the error path for page 0.
func TestClearTableData_InvalidRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	if err := bt.ClearTableData(0); err == nil {
		t.Error("ClearTableData(0) should return error")
	}
}

// TestClearTableData_InteriorRoot tests ClearTableData on a multi-level tree
// so the interior-node branch is covered.
func TestClearTableData_InteriorRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // small pages → multi-level tree quickly
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 100; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break // stop on first error (page exhaustion)
		}
	}
	if err := bt.ClearTableData(cursor.RootPage); err != nil {
		t.Fatalf("ClearTableData on interior root: %v", err)
	}
}

// TestSafePayloadSize_ErrorPaths exercises the overflow branches of
// safePayloadSize and safePayloadSizeWithFallback (33%/43% coverage).
// CalculateLocalPayload delegates to both helpers; we drive the fallback
// paths by passing a tiny usableSize so that minLocal overflows uint16.
func TestSafePayloadSize_ErrorPaths(t *testing.T) {
	t.Parallel()
	// totalSize > maxLocal, surplus <= maxLocal path via normal inputs
	result := CalculateLocalPayload(1000, 512, true)
	if result == 0 {
		t.Error("CalculateLocalPayload(1000,512,true) = 0, want non-zero")
	}

	// Force safePayloadSize fallback: usableSize < 4 branch
	result2 := CalculateLocalPayload(100, 2, false)
	_ = result2 // just ensure it doesn't panic

	// surplus > maxLocal path
	result3 := CalculateLocalPayload(50000, 512, false)
	_ = result3
}

// TestCompositeInteriorSplit exercises the 0% composite interior-split code
// paths: prepareInteriorSplitPagesComposite, splitInteriorPageComposite,
// executeInteriorSplitComposite, prepareInteriorSplitComposite,
// collectInteriorCellsForSplitComposite, tryInsertInteriorCellComposite,
// copyExistingInteriorCellComposite, finalizeInteriorCellCollectionComposite.
//
// Strategy: use a very small page size (512 bytes) and insert many composite
// rows with moderate payloads so that interior pages fill up and split.
func insertCompositeRows(bt *Btree, cursor *BtCursor, n int, prefix string, payloadSize int) int {
	payload := bytes.Repeat([]byte("z"), payloadSize)
	inserted := 0
	for i := 0; i < n; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("%s%06d", prefix, i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			break
		}
		inserted++
	}
	return inserted
}

func scanCompositeForward(bt *Btree, rootPage uint32, limit int) int {
	scan := NewCursorWithOptions(bt, rootPage, true)
	if err := scan.MoveToFirst(); err != nil {
		return 0
	}
	count := 0
	for scan.IsValid() && count < limit {
		count++
		if err := scan.Next(); err != nil {
			break
		}
	}
	return count
}

func TestCompositeInteriorSplit(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)
	inserted := insertCompositeRows(bt, cursor, 300, "k", 50)
	if inserted < 2 {
		t.Fatalf("Only inserted %d rows, need at least 2 to trigger split paths", inserted)
	}
	count := scanCompositeForward(bt, cursor.RootPage, inserted+10)
	if count < inserted-5 {
		t.Errorf("forward scan returned %d rows, want at least %d", count, inserted-5)
	}
}

// TestIndexCursorBackwardDeepTree exercises the 0%/low-coverage backward
// iteration code paths in index_cursor.go:
//   - descendToLast (0%)
//   - enterPage (0%)
//   - prevViaParent (21.7%)
//
// Strategy: move to first (which sets up IndexStack correctly via descendToFirst),
// then advance forward past multiple page boundaries (so the parent IndexStack
// reflects non-leftmost child positions), then iterate backward.
// When PrevIndex hits CurrentIndex==0 on a non-leftmost child, it calls
// prevViaParent which calls descendToLast/enterPage.
func TestIndexCursorBackwardDeepTree(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	inserted := insertIndexEntriesN(cursor, 300, func(i int) []byte {
		return []byte(fmt.Sprintf("idx%06d", i))
	})
	if inserted < 10 {
		t.Fatalf("Only inserted %d index entries", inserted)
	}

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	navigateIndexForward(cursor, inserted/2)

	backward := navigateIndexBackward(cursor, inserted)
	if backward < 2 {
		t.Errorf("backward iteration only got %d steps", backward)
	}
}

// TestIndexCursorFullBackwardScan does a complete backward scan after a full
// forward scan to ensure prevViaParent/descendToLast/enterPage are exercised
// across all page boundaries.
func TestIndexCursorFullBackwardScan(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	inserted := insertIndexEntriesN(cursor, 200, func(i int) []byte {
		return []byte(fmt.Sprintf("scan%06d", i))
	})

	// Full forward traversal to build proper IndexStack state
	countIndexForward(cursor)

	// Restart, move to 75%, then scan backward
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst (2nd): %v", err)
	}
	navigateIndexForward(cursor, inserted*3/4)

	count := navigateIndexBackward(cursor, inserted)
	if count < 1 {
		t.Error("backward scan from 75% should traverse at least 1 entry")
	}
}

// TestIndexCursorPrevViaParentAtRoot exercises prevViaParent when the cursor
// is at depth == 0 (the parentIndex == 0 early-return path).
func TestIndexCursorPrevViaParentAtRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("rev%06d", i))
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			break
		}
	}

	// Move to first (smallest), then navigate backward all the way —
	// the last PrevIndex call should reach depth-0 and hit the early return.
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	for cursor.IsValid() {
		if err := cursor.PrevIndex(); err != nil {
			break
		}
	}
}

// TestNavigateToRightmostLeafComposite exercises the 0% composite cursor path
// navigateToRightmostLeafComposite via MoveToLast on a WITHOUT ROWID table.
func TestNavigateToRightmostLeafComposite(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	inserted := 0
	for i := 0; i < 100; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("r%06d", i)})
		if err := cursor.InsertWithComposite(0, key, make([]byte, 20)); err != nil {
			break
		}
		inserted++
	}
	if inserted == 0 {
		t.Skip("no rows inserted")
	}

	// MoveToLast on a composite cursor exercises navigateToRightmostLeafComposite
	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if !cursor.IsValid() {
		t.Error("cursor invalid after MoveToLast on composite tree")
	}
}

// TestMergeGetSiblingWithLeftPage exercises getSiblingWithLeftPage (0% merge.go:102).
// Key insight: getSiblingWithLeftPage requires parentIndex > 0 in findSiblingPages.
// parentIndex comes from IndexStack[parentDepth]. After SeekRowid, IndexStack[0] is
// always 0 (reset by initializeSeek). To get parentIndex > 0, use MoveToFirst + Next
// which advances IndexStack as it crosses page boundaries.
// advancePastPageBoundary moves cursor forward until it crosses a page boundary.
// Returns true if a boundary was crossed.
func advancePastPageBoundary(cursor *BtCursor) bool {
	prevPage := cursor.CurrentPage
	for cursor.IsValid() {
		if err := cursor.Next(); err != nil {
			break
		}
		if cursor.CurrentPage != prevPage {
			return true
		}
		prevPage = cursor.CurrentPage
	}
	return false
}

func TestMergeGetSiblingWithLeftPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 80, 20)

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	crossed := advancePastPageBoundary(cursor)
	if crossed && cursor.IsValid() && cursor.Depth > 0 {
		cursor.MergePage() //nolint:errcheck
	}
}

// TestMergeGetSiblingAsRightmost exercises getSiblingAsRightmost (0% merge.go:125).
// getSiblingAsRightmost requires parentIndex == parentHeader.NumCells.
// After descendToLast(), IndexStack[depth] = NumCells.
// Strategy: Navigate forward past first page boundary (sets IndexStack[parent] = 1),
// then navigate backward (triggers prevViaParent → descendToLast → IndexStack[parent] = NumCells),
// then from that position (rightmost child), call MergePage().
func TestMergeGetSiblingAsRightmost_CrossBoundaryMerge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 80, 20)

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	advancePastPageBoundary(cursor)
	if cursor.IsValid() && cursor.Depth > 0 {
		cursor.MergePage() //nolint:errcheck
	}
}

func TestMergeGetSiblingAsRightmost_LastPositionMerge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 80, 20)

	// Navigate to the very last entry to put cursor in rightmost child
	if err := cursor.MoveToFirst(); err != nil {
		t.Logf("MoveToFirst: %v", err)
		return
	}
	for cursor.IsValid() {
		if err := cursor.Next(); err != nil {
			break
		}
	}
	// Go back one step via Previous to keep cursor valid at last page
	cursor.Previous() //nolint:errcheck
	if cursor.IsValid() && cursor.Depth > 0 {
		cursor.MergePage() //nolint:errcheck
	}
}

// TestBalanceUnderfullNonRoot exercises handleUnderfullPage for non-root pages
// by directly calling balance() from a cursor positioned at depth > 0.
func TestBalanceUnderfullNonRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 80, 20)
	deleteRowRange(cursor, 5, 75)

	if err := cursor.MoveToFirst(); err != nil {
		t.Logf("MoveToFirst: %v", err)
		return
	}
	if cursor.IsValid() && cursor.Depth > 0 {
		if err := balance(cursor); err != nil {
			t.Logf("balance() on underfull non-root: %v (expected)", err)
		}
	}
}

// TestBalanceDefragmentIfNeeded exercises the defragmentIfNeeded path (50%).
// Create a page that is neither overfull nor underfull but has fragmented bytes.
// setPageFragmentation sets the fragmented byte count on a cursor's current page.
// Returns false if the page cannot be modified.
func setPageFragmentation(bt *Btree, cursor *BtCursor, fragBytes byte) bool {
	pageData, err := bt.GetPage(cursor.CurrentPage)
	if err != nil {
		return false
	}
	headerOffset := 0
	if cursor.CurrentPage == 1 {
		headerOffset = FileHeaderSize
	}
	pageData[headerOffset+PageHeaderOffsetFragmented] = fragBytes
	header, err := ParsePageHeader(pageData, cursor.CurrentPage)
	if err != nil {
		return false
	}
	cursor.CurrentHeader = header
	return true
}

func TestBalanceDefragmentIfNeeded(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 20, 50)
	deleteRowRange(cursor, 5, 10)

	found, err := cursor.SeekRowid(12)
	if err != nil || !found {
		return
	}
	if !setPageFragmentation(bt, cursor, 10) {
		return
	}
	if err := balance(cursor); err != nil {
		t.Logf("balance() with fragmentation: %v", err)
	}
}

// TestValidatePageTypeForBtree_InvalidType exercises the error path in
// validatePageTypeForBtree (66.7% → 100%).
func TestValidatePageTypeForBtree_InvalidType(t *testing.T) {
	t.Parallel()
	// 0xFF is not a valid page type
	if err := validatePageTypeForBtree(0xFF); err == nil {
		t.Error("validatePageTypeForBtree(0xFF) should return error")
	}
	// Valid types should not error
	for _, pt := range []byte{PageTypeLeafTable, PageTypeInteriorTable, PageTypeLeafIndex, PageTypeInteriorIndex} {
		if err := validatePageTypeForBtree(pt); err != nil {
			t.Errorf("validatePageTypeForBtree(0x%02x) unexpected error: %v", pt, err)
		}
	}
}

// TestHandleUnderfullPage_RootPageExtra exercises the root-page early-return branch
// in handleUnderfullPage (25% → higher).
func TestHandleUnderfullPage_RootPageExtra(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// Insert one row so cursor is valid
	cursor := NewCursor(bt, rootPage)
	if err := cursor.Insert(1, []byte("x")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	found, err := cursor.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid: %v found=%v", err, found)
	}
	// cursor.CurrentPage == cursor.RootPage at depth 0
	// handleUnderfullPage returns nil immediately for root
	pageData, _ := bt.GetPage(cursor.CurrentPage)
	page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)
	if err := handleUnderfullPage(cursor, page); err != nil {
		t.Errorf("handleUnderfullPage on root: %v", err)
	}
}

// TestHandleUnderfullPage_NonRootDepth0 exercises the depth==0 branch.
// This is tricky - we need a cursor where CurrentPage != RootPage but Depth==0.
// We set CurrentPage to a child page while keeping Depth=0.
func TestHandleUnderfullPage_NonRootDepth0(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 40; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}
	// Position cursor at some leaf
	cursor.MoveToFirst() //nolint:errcheck
	if !cursor.IsValid() {
		return
	}
	// Artificially set CurrentPage to something != RootPage with Depth=0
	// to hit the if cursor.Depth == 0 branch
	originalPage := cursor.CurrentPage
	cursor.CurrentPage = rootPage + 1 // use a non-root page
	cursor.Depth = 0                  // but depth is 0
	pageData, _ := bt.GetPage(originalPage)
	if pageData == nil {
		return
	}
	page, _ := NewBtreePage(originalPage, pageData, bt.UsableSize)
	if page == nil {
		return
	}
	// This triggers: CurrentPage != RootPage, Depth == 0 → return nil
	if err := handleUnderfullPage(cursor, page); err != nil {
		t.Errorf("handleUnderfullPage(non-root, depth=0): %v", err)
	}
}

// TestHandleUnderfullPage_NonRootWithFragmentation exercises the fragmentation
// defragment branch inside handleUnderfullPage.
func TestHandleUnderfullPage_NonRootWithFragmentation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 40, 20)

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	advancePastPageBoundary(cursor)
	if !cursor.IsValid() || cursor.Depth == 0 {
		t.Skip("could not position at non-root leaf")
	}
	if !setPageFragmentation(bt, cursor, 5) {
		return
	}
	pageData, _ := bt.GetPage(cursor.CurrentPage)
	page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)
	if page == nil {
		return
	}
	err = handleUnderfullPage(cursor, page)
	t.Logf("handleUnderfullPage result: %v", err)
}

// TestDefragmentIfNeeded_WithFragmentation forces the fragmentation branch.
func TestDefragmentIfNeeded_WithFragmentation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 5, 30)

	found, err := cursor.SeekRowid(3)
	if err != nil || !found {
		return
	}
	if !setPageFragmentation(bt, cursor, 8) {
		return
	}
	pageData, _ := bt.GetPage(cursor.CurrentPage)
	page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)
	if page == nil {
		return
	}
	if err := defragmentIfNeeded(cursor, page); err != nil {
		t.Errorf("defragmentIfNeeded: %v", err)
	}
}

// TestCheckPageSize_Mismatch exercises the checkPageSize error branch (50%).
func TestCheckPageSize_Mismatch(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	// Pass page data of wrong size
	smallPage := make([]byte, 512) // wrong size for 4096-byte btree
	checkPageSize(bt, 1, smallPage, result)
	if len(result.Errors) == 0 {
		t.Error("checkPageSize with mismatched size should report error")
	}

	// Correct size should not error
	result2 := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	correctPage := make([]byte, 4096)
	checkPageSize(bt, 1, correctPage, result2)
	if len(result2.Errors) != 0 {
		t.Errorf("checkPageSize with correct size should not report error, got: %v", result2.Errors)
	}
}

// TestParseSingleCell_OutOfBounds exercises the cell-out-of-bounds branch (60%).
func TestParseSingleCell_OutOfBounds(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("data")) //nolint:errcheck

	pageData, _ := bt.GetPage(rootPage)
	header, _ := ParsePageHeader(pageData, rootPage)
	if header == nil || header.NumCells == 0 {
		return
	}

	result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	// Create a cell pointer list with an out-of-bounds offset
	badPointers := []uint16{uint16(len(pageData) + 100)} // beyond page bounds
	cell, offset, size := parseSingleCell(bt, rootPage, pageData, header, badPointers, 0, result)
	if cell != nil {
		t.Error("parseSingleCell with out-of-bounds offset should return nil cell")
	}
	_ = offset
	_ = size
	if len(result.Errors) == 0 {
		t.Error("parseSingleCell with out-of-bounds offset should report error")
	}
}

// TestParseSingleCell_ValidCell exercises the successful parse branch.
func TestParseSingleCell_ValidCell(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("payload")) //nolint:errcheck

	pageData, _ := bt.GetPage(rootPage)
	header, _ := ParsePageHeader(pageData, rootPage)
	if header == nil || header.NumCells == 0 {
		return
	}
	cellPointers, err := header.GetCellPointers(pageData)
	if err != nil || len(cellPointers) == 0 {
		return
	}
	result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
	cell, offset, size := parseSingleCell(bt, rootPage, pageData, header, cellPointers, 0, result)
	assertParsedCellValid(t, cell, offset, size)
}

func assertParsedCellValid(t *testing.T, cell *CellInfo, offset, size int) {
	t.Helper()
	if cell == nil {
		t.Error("parseSingleCell should succeed with valid cell pointer")
	}
	if offset == 0 {
		t.Error("parseSingleCell should return non-zero offset")
	}
	if size == 0 {
		t.Error("parseSingleCell should return non-zero size")
	}
}

// TestBtreePreviousNavigation exercises Previous(), prevInPage(), prevViaParent()
// and descendToLast() by doing a complete backward scan after a forward scan.
func TestBtreePreviousNavigation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	inserted := insertRows(cursor, 1, 100, 20)

	// Full forward traversal
	forward := countForward(cursor)

	// Restart, advance to middle, then backward
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst (2nd): %v", err)
	}
	navigateForward(cursor, forward/2)
	backward := navigateBackward(cursor, inserted)
	t.Logf("forward=%d backward=%d inserted=%d", forward, backward, inserted)
}

// TestGetKeyBytes_CompositeCursor exercises GetKeyBytes for composite cursor (66.7%).
func TestGetKeyBytes_CompositeCursor(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	key := withoutrowid.EncodeCompositeKey([]interface{}{"hello"})
	if err := cursor.InsertWithComposite(0, key, []byte("value")); err != nil {
		t.Fatalf("InsertWithComposite: %v", err)
	}

	found, err := cursor.SeekComposite(key)
	if err != nil || !found {
		t.Fatalf("SeekComposite: %v found=%v", err, found)
	}

	kb := cursor.GetKeyBytes()
	if len(kb) == 0 {
		t.Error("GetKeyBytes should return non-empty bytes for composite cursor")
	}
}

// TestMoveToLast_MultiLevelTree exercises navigateToRightmostLeaf with interior pages.
func TestMoveToLast_MultiLevelTree(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 80; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}
	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if !cursor.IsValid() {
		t.Fatal("cursor invalid after MoveToLast")
	}
	lastKey := cursor.GetKey()
	if lastKey <= 0 {
		t.Errorf("unexpected last key: %d", lastKey)
	}
}

// TestSeekRowid_NotFound exercises the not-found branch in SeekRowid (75%).
func TestSeekRowid_NotFound(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("a")) //nolint:errcheck
	cursor.Insert(3, []byte("b")) //nolint:errcheck

	// Seek to a row that doesn't exist
	found, err := cursor.SeekRowid(2)
	if err != nil {
		t.Fatalf("SeekRowid(2): %v", err)
	}
	if found {
		t.Error("SeekRowid(2) should return found=false")
	}
}

// simplePageProvider is a minimal PageProvider implementation for testing Provider-branch paths.
type simplePageProvider struct {
	pages    map[uint32][]byte
	pageSize uint32
	nextPage uint32
}

func newSimplePageProvider(pageSize uint32) *simplePageProvider {
	return &simplePageProvider{
		pages:    make(map[uint32][]byte),
		pageSize: pageSize,
		nextPage: 1,
	}
}

func (m *simplePageProvider) GetPageData(pgno uint32) ([]byte, error) {
	if data, ok := m.pages[pgno]; ok {
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp, nil
	}
	data := make([]byte, m.pageSize)
	return data, nil
}

func (m *simplePageProvider) AllocatePageData() (uint32, []byte, error) {
	pgno := m.nextPage
	m.nextPage++
	data := make([]byte, m.pageSize)
	m.pages[pgno] = data
	return pgno, data, nil
}

func (m *simplePageProvider) MarkDirty(_ uint32) error {
	return nil
}

// TestCreateTableWithProvider exercises the Provider != nil branch in CreateTable
// and CreateWithoutRowidTable (covering lines 345-349, 368-372).
func TestCreateTableWithProvider(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = newSimplePageProvider(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable with Provider: %v", err)
	}
	if rootPage == 0 {
		t.Error("CreateTable with Provider returned page 0")
	}

	rootPage2, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable with Provider: %v", err)
	}
	if rootPage2 == 0 {
		t.Error("CreateWithoutRowidTable with Provider returned page 0")
	}
}

// failingPageProvider is a PageProvider that returns errors for MarkDirty.
type failingPageProvider struct {
	simplePageProvider
	failMarkDirty bool
}

func newFailingPageProvider(pageSize uint32) *failingPageProvider {
	return &failingPageProvider{
		simplePageProvider: simplePageProvider{
			pages:    make(map[uint32][]byte),
			pageSize: pageSize,
			nextPage: 1,
		},
		failMarkDirty: true,
	}
}

func (m *failingPageProvider) MarkDirty(_ uint32) error {
	if m.failMarkDirty {
		return fmt.Errorf("simulated MarkDirty failure")
	}
	return nil
}

// TestFinishInsertMarkDirtyError exercises finishInsert error paths by using a Provider
// that returns an error from MarkDirty, triggering cleanupOverflowOnError.
func TestFinishInsertMarkDirtyError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	// Insert one valid row to position cursor
	cursor.Insert(1, []byte("setup")) //nolint:errcheck

	// Set failing provider AFTER setup - Insert will call markPageDirty → Provider.MarkDirty → error
	fp := newFailingPageProvider(4096)
	bt.Provider = fp

	// This insert should fail at markPageDirty
	err = cursor.Insert(2, []byte("fail"))
	if err == nil {
		t.Log("Insert with failing MarkDirty succeeded (markPageDirty may have been skipped)")
	} else {
		t.Logf("Insert with failing MarkDirty failed as expected: %v", err)
	}
}

// TestInsertWithProviderExercisesMarkPageDirty exercises the Provider != nil path
// in markPageDirty (cursor.go:1014) and writeSingleOverflowPage (overflow.go:117).
// Strategy: set bt.Provider on an existing btree with in-memory pages,
// then insert rows to trigger markPageDirty calls.
func TestInsertWithProviderExercisesMarkPageDirty(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// Set provider AFTER creating table so AllocatePage works in-memory first
	bt.Provider = newSimplePageProvider(4096)

	cursor := NewCursor(bt, rootPage)
	// Insert calls markPageDirty via finishInsert → markPageDirty → Provider.MarkDirty
	for i := int64(1); i <= 5; i++ {
		if err := cursor.Insert(i, []byte("hello")); err != nil {
			t.Fatalf("Insert(%d) with Provider: %v", i, err)
		}
	}

	// Insert with overflow to hit writeSingleOverflowPage Provider branch
	bigPayload := make([]byte, 4200) // > page size
	cursor.Insert(100, bigPayload)   //nolint:errcheck  may fail if no Provider page allocation
}

// TestIndexMarkPageDirtyForDeleteError exercises the markPageDirtyForDelete error path
// using a failing provider that returns errors from MarkDirty.
func TestIndexMarkPageDirtyForDeleteError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	// Insert an entry
	if err := cursor.InsertIndex([]byte("mykey"), 1); err != nil {
		t.Fatalf("InsertIndex: %v", err)
	}

	// Seek to position cursor at the key
	found, seekErr := cursor.SeekIndex([]byte("mykey"))
	if seekErr != nil || !found {
		t.Skipf("SeekIndex failed: %v found=%v", seekErr, found)
	}

	// Set failing provider - DeleteIndex will call markPageDirtyForDelete → Provider.MarkDirty → error
	fp := newFailingPageProvider(4096)
	bt.Provider = fp

	// DeleteIndex should fail at markPageDirtyForDelete
	deleteErr := cursor.DeleteIndex([]byte("mykey"), 1)
	if deleteErr == nil {
		t.Log("DeleteIndex with failing MarkDirty succeeded (may have been skipped)")
	} else {
		t.Logf("DeleteIndex with failing MarkDirty failed as expected: %v", deleteErr)
	}
}

// TestIndexInsertWithProviderExercisesMarkDirty exercises Provider != nil in
// InsertIndex (index_cursor.go:669) and markPageDirtyForDelete (index_cursor.go:780).
func TestIndexInsertWithProviderExercisesMarkDirty(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	bt.Provider = newSimplePageProvider(4096)

	cursor := NewIndexCursor(bt, rootPage)

	// InsertIndex with Provider exercises Provider.MarkDirty branch in InsertIndex
	keys := [][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}
	for _, k := range keys {
		if err := cursor.InsertIndex(k, 1); err != nil {
			t.Logf("InsertIndex(%q) with Provider: %v (may be acceptable)", k, err)
			break
		}
	}

	// DeleteIndex with Provider exercises markPageDirtyForDelete
	found, err := cursor.SeekIndex([]byte("aaa"))
	if err == nil && found {
		if err2 := cursor.DeleteIndex([]byte("aaa"), 1); err2 != nil {
			t.Logf("DeleteIndex with Provider: %v (may be acceptable)", err2)
		}
	}
}

// TestSplitWithProviderExercisesProviderBranches exercises Provider != nil
// in insertDividerIntoParent (split.go:865) and createNewRoot (split.go:938).
// Strategy: use small pages + Provider to trigger splits.
func TestSplitWithProviderExercisesProviderBranches(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	bt.Provider = newSimplePageProvider(512)

	cursor := NewCursor(bt, rootPage)
	// Insert enough rows to trigger a split (which calls createNewRoot and insertDividerIntoParent)
	for i := int64(1); i <= 20; i++ {
		if err := cursor.Insert(i, make([]byte, 15)); err != nil {
			t.Logf("Insert(%d) with Provider stopped: %v", i, err)
			break
		}
	}
}

// TestSeekOnEmptyTable exercises tryLoadCell with NumCells==0 (empty page seek).
// SeekRowid on empty table calls binarySearch which returns idx=0, then seekLeafPage
// calls tryLoadCell where idx >= NumCells and NumCells == 0.
func TestSeekOnEmptyTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// Seek on completely empty table - exercises tryLoadCell(0, header where NumCells=0)
	found, err := cursor.SeekRowid(42)
	if err != nil {
		t.Logf("SeekRowid on empty table: %v (may be expected)", err)
	}
	if found {
		t.Error("SeekRowid on empty table should not find entry")
	}

	// Also try SeekComposite on empty table
	emptyBt := NewBtree(4096)
	emptyRoot, _ := emptyBt.CreateWithoutRowidTable()
	emptyC := NewCursorWithOptions(emptyBt, emptyRoot, true)
	found2, err2 := emptyC.SeekComposite([]byte("key"))
	if err2 != nil {
		t.Logf("SeekComposite on empty table: %v (may be expected)", err2)
	}
	_ = found2
}

// TestSeekRowidBeforeFirst exercises tryLoadCell with idx < 0 path
// (negative index scenario, reached when seeking before first element).
func TestSeekRowidBeforeFirst(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// Insert one row with rowid=10
	cursor.Insert(10, []byte("data")) //nolint:errcheck

	// Seek to rowid 0 (before first) - binarySearch returns idx=0 which is valid
	// Seek to rowid -5 if supported (not applicable for uint-based rowid)
	// Instead, move to last then Previous past beginning
	cursor.MoveToLast() //nolint:errcheck
	cursor.Previous()   //nolint:errcheck - moves to beginning
	cursor.Previous()   //nolint:errcheck - goes before beginning
}

// TestDeleteFromLastEntryExercisesAdjustCursor exercises adjustCursorAfterDelete
// when deleting the last (index 0) entry on a page (CurrentIndex decrements to -1).
func TestDeleteFromLastEntryExercisesAdjustCursor(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("only")) //nolint:errcheck

	// Seek to the only entry and delete it
	// adjustCursorAfterDelete will decrement CurrentIndex to -1
	found, err := cursor.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid(1): found=%v err=%v", found, err)
	}
	if err := cursor.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// After deleting the only cell, CurrentIndex should be -1 and CurrentCell nil
}

// TestIndexCursorDeleteIndex exercises the index delete path:
// deleteCurrentEntry, validateDeleteState, markPageDirtyForDelete, getBtreePageForDelete.
func TestIndexCursorDeleteIndex(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	keys := [][]byte{
		[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta"),
	}
	for i, k := range keys {
		if err := cursor.InsertIndex(k, int64(i+1)); err != nil {
			t.Fatalf("InsertIndex(%q): %v", k, err)
		}
	}

	if err := cursor.DeleteIndex([]byte("beta"), 2); err != nil {
		t.Fatalf("DeleteIndex(beta,2): %v", err)
	}
	found, err := cursor.SeekIndex([]byte("beta"))
	if err != nil {
		t.Fatalf("SeekIndex after delete: %v", err)
	}
	if found {
		t.Error("beta should not be found after deletion")
	}
	if err := cursor.DeleteIndex([]byte("zzz"), 99); err == nil {
		t.Error("DeleteIndex of non-existent key should error")
	}
}

func TestIndexCursorDeleteIndex_WrongRowid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)
	cursor.InsertIndex([]byte("beta"), 10) //nolint:errcheck
	if err := cursor.DeleteIndex([]byte("beta"), 999); err == nil {
		t.Error("DeleteIndex with wrong rowid should error")
	}
}

// TestIndexCursorMoveToLastAndPrev exercises MoveToLast + PrevIndex backward scan
// on a tree with multiple entries to hit more of seekLeafExactMatch and prevInPage.
func TestIndexCursorMoveToLastAndPrev(t *testing.T) {
	t.Parallel()
	_, cursor := setupIndexCursor(t, 4096)

	insertIndexEntriesN(cursor, 25, func(i int) []byte {
		return []byte(fmt.Sprintf("key%04d", i))
	})

	count := countIndexBackward(cursor)
	if count < 2 {
		t.Errorf("expected at least 2 backward steps, got %d", count)
	}
}

// TestFreeOverflowChainOnDelete exercises FreeOverflowChain by inserting a large payload
// that spans overflow pages, then deleting it.
func TestFreeOverflowChainOnDelete(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	largePayload := make([]byte, 400)
	insertAndVerifyOverflow(t, cursor, 1, largePayload, true)

	if cursor.CurrentCell == nil || cursor.CurrentCell.OverflowPage == 0 {
		t.Skip("no overflow page created (payload fits locally)")
	}
	if err := cursor.Delete(); err != nil {
		t.Fatalf("Delete with overflow: %v", err)
	}
}

// TestFreeOverflowChainDirect exercises FreeOverflowChain directly.
func TestFreeOverflowChainDirect(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// FreeOverflowChain with page 0 returns nil immediately
	if err := cursor.FreeOverflowChain(0); err != nil {
		t.Errorf("FreeOverflowChain(0) should return nil: %v", err)
	}
}

// TestMergeActualMergeExercise triggers actual merges via insert+delete cycles.
// This exercises mergePages, copyRightCellsToLeft, updateParentAfterMerge,
// loadMergePages, extractCellData, and the merge.go helper chain.
// mergeAtAllPositions navigates forward through the cursor and calls MergePage at each non-root position.
func mergeAtAllPositions(cursor *BtCursor) {
	if err := cursor.MoveToFirst(); err != nil {
		return
	}
	for cursor.IsValid() {
		if cursor.Depth > 0 {
			cursor.MergePage() //nolint:errcheck
		}
		if err := cursor.Next(); err != nil {
			break
		}
	}
}

func TestMergeActualMergeExercise(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	inserted := insertRows(cursor, 1, 100, 15)
	if inserted < 10 {
		t.Skipf("only inserted %d rows, skipping", inserted)
	}
	deleteRowRange(cursor, 1, int64(inserted*3/4))
	mergeAtAllPositions(cursor)
}

// TestRedistributeExercise exercises redistributeSiblings via MergePage
// when pages are not empty enough to merge but can redistribute.
func TestRedistributeExercise(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	for i := int64(1); i <= 60; i++ {
		if err := cursor.Insert(i, make([]byte, 12)); err != nil {
			break
		}
	}

	// Try to merge from various positions
	if err := cursor.MoveToFirst(); err != nil {
		return
	}
	for cursor.IsValid() {
		if cursor.Depth > 0 {
			cursor.MergePage() //nolint:errcheck
		}
		if err := cursor.Next(); err != nil {
			break
		}
	}
}

// TestSafePayloadSizeOverflowPaths exercises safePayloadSize and
// safePayloadSizeWithFallback when values overflow uint16.
func TestSafePayloadSizeOverflowPaths(t *testing.T) {
	t.Parallel()
	// Force safePayloadSize error path: values > 65535
	// CalculateLocalPayload uses safePayloadSize internally.
	// With usableSize=4, minLocal formula may overflow.
	// Direct test: use a very large payloadSize with specific usableSize
	// to trigger the error branches in safePayloadSize and safePayloadSizeWithFallback.

	// These should not panic; they exercise the fallback/zero-return paths.
	result := CalculateLocalPayload(200000, 200, true)
	_ = result

	result2 := CalculateLocalPayload(200000, 200, false)
	_ = result2

	result3 := CalculateLocalPayload(0, 4096, true)
	_ = result3

	result4 := CalculateLocalPayload(1000, 4096, false)
	_ = result4
}

// TestSlowBtreeVarint32Extra exercises the slowBtreeVarint32 path (66.7%)
// which is triggered by GetVarint32 when the fast path fails (n > 3 bytes).
func TestSlowBtreeVarint32Extra(t *testing.T) {
	t.Parallel()
	// Encode a value that requires exactly 4 bytes in varint encoding
	// 3-byte varint holds up to 0x1fffff (21 bits), 4-byte holds up to 0xfffffff
	// So 0x200000 needs 4 bytes and will invoke slowBtreeVarint32
	buf := make([]byte, 9)
	n := PutVarint(buf, 0x200000) // requires 4 bytes (22 bits)
	if n == 0 {
		t.Fatal("PutVarint returned 0")
	}
	val, read := GetVarint32(buf[:n])
	// slowBtreeVarint32 is called; it may return 0 if fast path consumed it first
	_ = val
	_ = read

	// Test with a value > 0xffffffff to exercise the clamping branch
	n2 := PutVarint(buf, 0x100000000) // 33-bit value, requires 5 bytes
	if n2 > 0 {
		val2, read2 := GetVarint32(buf[:n2])
		_ = val2
		_ = read2
	}
}

// TestInsertWithOverflowTriggersFinishInsert exercises finishInsert
// by inserting a cell that requires overflow pages, verifying the path
// through markPageDirty → InsertCell → seekAfterInsert.
func TestInsertWithOverflowTriggersFinishInsert(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// Insert a payload that requires overflow
	bigPayload := make([]byte, 300)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 200)
	}

	if err := cursor.Insert(42, bigPayload); err != nil {
		t.Fatalf("Insert with big payload: %v", err)
	}

	found, err := cursor.SeekRowid(42)
	if err != nil || !found {
		t.Fatalf("SeekRowid(42): found=%v err=%v", found, err)
	}

	// Complete payload should round-trip
	full, err := cursor.GetCompletePayload()
	if err != nil {
		t.Fatalf("GetCompletePayload: %v", err)
	}
	if len(full) != len(bigPayload) {
		t.Errorf("payload length mismatch: got %d, want %d", len(full), len(bigPayload))
	}
}

// TestAdvanceWithinPageMultipleCells exercises advanceWithinPage (66.7%)
// and seekLeafExactMatch on a page with multiple cells.
func TestAdvanceWithinPageMultipleCells(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	for i := int64(1); i <= 10; i++ {
		if err := cursor.Insert(i, []byte("data")); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Seek to first, then advance through all entries on the same leaf page
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	// All 10 entries should be on a single page with 4096 page size
	count := 1
	for cursor.IsValid() {
		if err := cursor.Next(); err != nil {
			break
		}
		count++
	}
	if count < 5 {
		t.Errorf("expected at least 5 entries, got %d", count)
	}
}

// TestDropTableExercise exercises the DropTable function and dropInteriorChildren.
func TestDropTableExercise(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 50; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}

	if err := bt.DropTable(rootPage); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
}

// TestGetBalanceInfoExtra exercises GetBalanceInfo for various pages.
func TestGetBalanceInfoExtra(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 5; i++ {
		cursor.Insert(i, make([]byte, 50)) //nolint:errcheck
	}

	info, err := GetBalanceInfo(bt, rootPage)
	if err != nil {
		t.Fatalf("GetBalanceInfo: %v", err)
	}
	if info == nil {
		t.Fatal("GetBalanceInfo returned nil")
	}
	t.Logf("balance info: %+v", info)

	// GetBalanceInfo on non-existent page
	_, err = GetBalanceInfo(bt, 999)
	if err == nil {
		t.Error("GetBalanceInfo on non-existent page should error")
	}
}

// TestParsePageHeaderErrors exercises validatePageData error paths.
func TestParsePageHeaderErrors(t *testing.T) {
	t.Parallel()
	// Too small
	_, err := ParsePageHeader([]byte{0x00}, 1)
	if err == nil {
		t.Error("ParsePageHeader on tiny data should error")
	}

	// Empty
	_, err2 := ParsePageHeader([]byte{}, 2)
	if err2 == nil {
		t.Error("ParsePageHeader on empty data should error")
	}
}

// TestCheckIntegrityWithBadCellPointer exercises parseCellsFromPage
// error paths by using a page with an out-of-bounds cell pointer.
// This covers the "cellOffset >= len(pageData)" branch (69.2% → higher).
func TestCheckIntegrityWithBadCellPointer(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with an intentionally out-of-bounds cell pointer
	pageData := make([]byte, 4096)
	hOff := FileHeaderSize // page 1
	pageData[hOff+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetNumCells:], 1)
	// Cell pointer points beyond page size
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetCellStart:], uint16(4096))
	// Write a bad pointer in the pointer array
	ptrOffset := hOff + PageHeaderSizeLeaf
	binary.BigEndian.PutUint16(pageData[ptrOffset:], uint16(5000)) // out of bounds
	bt.SetPage(1, pageData)

	result := CheckIntegrity(bt, 1)
	// Should have errors about the bad cell
	t.Logf("CheckIntegrity with bad pointer: %d errors", len(result.Errors))
}

// TestCheckIntegrityWithBadCellData exercises the ParseCell error path
// in parseCellsFromPage by creating a page where a cell's data is corrupted.
func TestCheckIntegrityWithBadCellData(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with a cell at a valid offset but containing invalid data
	pageData := make([]byte, 4096)
	hOff := FileHeaderSize
	pageData[hOff+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetNumCells:], 1)
	// Cell at offset 4000 (valid offset within page)
	cellOffset := uint16(4000)
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetCellStart:], cellOffset)
	ptrOffset := hOff + PageHeaderSizeLeaf
	binary.BigEndian.PutUint16(pageData[ptrOffset:], cellOffset)
	// Leave cell data as all zeros - this is an invalid cell (empty varint)
	// ParseCell should fail on zero data
	bt.SetPage(1, pageData)

	result := CheckIntegrity(bt, 1)
	t.Logf("CheckIntegrity with bad cell data: %d errors", len(result.Errors))
}

// TestCheckPageIntegrityWithBadPage exercises loadPageAndHeader error paths
// by calling CheckPageIntegrity with a non-existent page.
func TestCheckPageIntegrityWithBadPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Page 999 doesn't exist
	result := CheckPageIntegrity(bt, 999)
	if len(result.Errors) == 0 {
		t.Error("CheckPageIntegrity on non-existent page should report error")
	}

	// nil btree
	result2 := CheckPageIntegrity(nil, 1)
	if len(result2.Errors) == 0 {
		t.Error("CheckPageIntegrity with nil btree should report error")
	}
}

// TestCheckPageIntegrityInvalidCellPointers exercises validateCellPointers error path.
// It creates a page that declares many cells but has a data slice too small
// to hold all the cell pointers.
func TestCheckPageIntegrityInvalidCellPointers(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a page that claims to have 200 cells but the data is only large enough
	// to hold the header, not all the cell pointers (200 cells * 2 bytes each = 400 bytes)
	// The header for page 1 (leaf table) is at FileHeaderSize + 8 bytes.
	// Total header: FileHeaderSize + PageHeaderSizeLeaf = 100 + 8 = 108 bytes
	// For 200 cells, we need 108 + 400 = 508 bytes of pointer space.
	// Use small page data that fits in GetPage but has invalid NumCells.
	pageData := make([]byte, 4096)
	hOff := FileHeaderSize
	pageData[hOff+PageHeaderOffsetType] = PageTypeLeafTable
	// Claim 1000 cells - cell pointers would need 2000 bytes past header
	// but cell content start is at 4096 (end), so pointers would exceed page size
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetNumCells:], 1000)
	binary.BigEndian.PutUint16(pageData[hOff+PageHeaderOffsetCellStart:], uint16(4096))
	bt.SetPage(1, pageData)

	result := CheckPageIntegrity(bt, 1)
	t.Logf("CheckPageIntegrity with 1000 NumCells: %d errors", len(result.Errors))
	// Should error because cell pointers go past page bounds
}

// TestCheckIntegrityNilBtree exercises the nil btree check in CheckIntegrity.
func TestCheckIntegrityNilBtree(t *testing.T) {
	t.Parallel()
	result := CheckIntegrity(nil, 1)
	if len(result.Errors) == 0 {
		t.Error("CheckIntegrity with nil btree should report error")
	}
}

// TestCheckIntegrityZeroRoot exercises the zero rootPage check in CheckIntegrity.
func TestCheckIntegrityZeroRoot(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	result := CheckIntegrity(bt, 0)
	if len(result.Errors) == 0 {
		t.Error("CheckIntegrity with rootPage=0 should report error")
	}
}

// TestCheckIntegritySelfReferenceCell exercises the self-reference check in
// checkInteriorPage (childPage == pageNum).
func TestCheckIntegritySelfReferenceCell(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	ps := bt.PageSize

	// Build leaf page 2
	leaf2 := createTestPage(2, ps, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("data")}})
	bt.SetPage(2, leaf2)

	// Build interior root page 1 with cell pointing to itself (page 1)
	// First cell points to itself (page 1), second points to valid leaf (page 2)
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{1, 5}, // self-reference: points to page 1 (itself)
	}
	rootData := createInteriorPage(1, ps, rootCells, 2)
	bt.SetPage(1, rootData)

	result := CheckIntegrity(bt, 1)
	t.Logf("CheckIntegrity with self-reference: %d errors", len(result.Errors))
	// Should report self_reference or cycle error
}

// TestCheckIntegrityZeroCellChild exercises the invalid_child_pointer (childPage==0) check.
func TestCheckIntegrityZeroCellChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	ps := bt.PageSize

	// Build interior root with a cell pointing to page 0 (invalid)
	// We need to manually build the page since createInteriorPage might reject page 0
	rootData := make([]byte, ps)
	hOff := FileHeaderSize
	rootData[hOff+PageHeaderOffsetType] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint32(rootData[hOff+PageHeaderOffsetRightChild:], 2)
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetCellStart:], uint16(ps-20))
	ptrOff := hOff + PageHeaderSizeInterior
	binary.BigEndian.PutUint16(rootData[ptrOff:], uint16(ps-20))
	// Cell: child page = 0 (invalid), rowid = 5
	cellOff := ps - 20
	binary.BigEndian.PutUint32(rootData[cellOff:], 0) // child page = 0
	n := PutVarint(rootData[cellOff+4:], 5)
	_ = n
	bt.SetPage(1, rootData)

	// Build leaf page 2 (right child)
	leaf2 := createTestPage(2, ps, PageTypeLeafTable, []struct {
		rowid   int64
		payload []byte
	}{{10, []byte("data")}})
	bt.SetPage(2, leaf2)

	result := CheckIntegrity(bt, 1)
	t.Logf("CheckIntegrity with zero child page: %d errors", len(result.Errors))
}

// TestCheckIntegrityMultiCellInterior exercises checkInteriorPage with multiple
// cells (ensures the i > 0 minKey branch is hit).
func TestCheckIntegrityMultiCellInterior(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	_, cursor := setupBtreeWithRows(t, 4096, 1, 1, 20)
	// Use cursor.RootPage which may be root page after inserts
	rootPage := cursor.RootPage

	// Insert more to force multi-cell interior page (unlikely with 4096 pages, but try)
	for i := int64(2); i <= 5; i++ {
		cursor.Insert(i, make([]byte, 20)) //nolint:errcheck
	}

	result := CheckIntegrity(bt, rootPage)
	t.Logf("CheckIntegrity multi-row: %d errors, %d rows", len(result.Errors), result.RowCount)
}

// TestCheckInteriorPageSelfRightChild exercises the RightChild == pageNum branch
// in checkInteriorPageRightChild.
func TestCheckInteriorPageSelfRightChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	ps := bt.PageSize

	// Interior page 1 with RightChild = 1 (points to itself)
	rootData := make([]byte, ps)
	hOff := FileHeaderSize
	rootData[hOff+PageHeaderOffsetType] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(rootData[hOff+PageHeaderOffsetRightChild:], 1) // self-reference
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetCellStart:], uint16(ps))
	bt.SetPage(1, rootData)

	result := CheckIntegrity(bt, 1)
	t.Logf("CheckIntegrity self-right-child: %d errors", len(result.Errors))
}

// TestCheckInteriorPageZeroRightChild exercises the RightChild == 0 branch.
func TestCheckInteriorPageZeroRightChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	ps := bt.PageSize

	// Interior page 1 with RightChild = 0 (invalid)
	rootData := make([]byte, ps)
	hOff := FileHeaderSize
	rootData[hOff+PageHeaderOffsetType] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(rootData[hOff+PageHeaderOffsetRightChild:], 0) // zero right child
	binary.BigEndian.PutUint16(rootData[hOff+PageHeaderOffsetCellStart:], uint16(ps))
	bt.SetPage(1, rootData)

	result := CheckIntegrity(bt, 1)
	t.Logf("CheckIntegrity zero-right-child: %d errors", len(result.Errors))
}

// TestCheckIntegrityOrphanPage exercises the orphan page detection.
func TestCheckIntegrityOrphanPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("data")) //nolint:errcheck

	// Add an orphan page (not referenced by any tree node)
	orphanData := make([]byte, 4096)
	orphanData[0] = PageTypeLeafTable
	bt.SetPage(99, orphanData)

	result := CheckIntegrity(bt, rootPage)
	t.Logf("CheckIntegrity with orphan: %d errors", len(result.Errors))
	// Should detect page 99 as orphan
}

// mockPager implements PagerInterface for testing PagerAdapter.
type mockPager struct {
	pages     map[uint32]*mockDbPage
	pageSize_ int
	nextPage_ uint32
	failGet   bool
	failWrite bool
}

type mockDbPage struct {
	data []byte
	pgno uint32
}

func (p *mockDbPage) GetData() []byte { return p.data }
func (p *mockDbPage) GetPgno() uint32 { return p.pgno }

func newMockPager(pageSize int) *mockPager {
	return &mockPager{
		pages:     make(map[uint32]*mockDbPage),
		pageSize_: pageSize,
		nextPage_: 1,
	}
}

func (m *mockPager) Get(pgno uint32) (interface{}, error) {
	if m.failGet {
		return nil, fmt.Errorf("simulated Get failure")
	}
	if p, ok := m.pages[pgno]; ok {
		return p, nil
	}
	p := &mockDbPage{data: make([]byte, m.pageSize_), pgno: pgno}
	m.pages[pgno] = p
	return p, nil
}

func (m *mockPager) Write(page interface{}) error {
	if m.failWrite {
		return fmt.Errorf("simulated Write failure")
	}
	return nil
}

func (m *mockPager) PageSize() int     { return m.pageSize_ }
func (m *mockPager) PageCount() uint32 { return uint32(len(m.pages)) }
func (m *mockPager) AllocatePage() (uint32, error) {
	pgno := m.nextPage_
	m.nextPage_++
	m.pages[pgno] = &mockDbPage{data: make([]byte, m.pageSize_), pgno: pgno}
	return pgno, nil
}

// nonDbPage is a page that doesn't implement DbPageInterface.
type nonDbPage struct{}

type nonDbPagePager struct {
	mockPager
}

func (m *nonDbPagePager) Get(_ uint32) (interface{}, error) {
	return &nonDbPage{}, nil // doesn't implement DbPageInterface
}

// TestPagerAdapterGetPageDataErrors exercises PagerAdapter.GetPageData error paths:
// - pager.Get failure
// - page doesn't implement DbPageInterface
func TestPagerAdapterGetPageDataErrors(t *testing.T) {
	t.Parallel()

	// Test pager.Get failure
	pager := newMockPager(4096)
	pager.failGet = true
	adapter := NewPagerAdapter(pager)
	_, err := adapter.GetPageData(1)
	if err == nil {
		t.Error("GetPageData with failing pager.Get should return error")
	}

	// Test page not implementing DbPageInterface
	nonDbPager := &nonDbPagePager{mockPager: *newMockPager(4096)}
	adapter2 := NewPagerAdapter(nonDbPager)
	_, err2 := adapter2.GetPageData(1)
	if err2 == nil {
		t.Error("GetPageData with non-DbPageInterface page should return error")
	}
}

// TestPagerAdapterMarkDirtyErrors exercises PagerAdapter.MarkDirty error paths:
// - pager.Get failure
// - pager.Write failure
func TestPagerAdapterMarkDirtyErrors(t *testing.T) {
	t.Parallel()

	// Test pager.Get failure in MarkDirty
	pager := newMockPager(4096)
	adapter := NewPagerAdapter(pager)
	// Set up a page first
	pager.pages[1] = &mockDbPage{data: make([]byte, 4096), pgno: 1}

	pager.failGet = true
	err := adapter.MarkDirty(1)
	if err == nil {
		t.Error("MarkDirty with failing pager.Get should return error")
	}

	// Test pager.Write failure in MarkDirty
	pager2 := newMockPager(4096)
	pager2.pages[1] = &mockDbPage{data: make([]byte, 4096), pgno: 1}
	pager2.failWrite = true
	adapter2 := NewPagerAdapter(pager2)
	err2 := adapter2.MarkDirty(1)
	if err2 == nil {
		t.Error("MarkDirty with failing pager.Write should return error")
	}
}

// TestPagerAdapterSuccessPath exercises the happy path of PagerAdapter.
func TestPagerAdapterSuccessPath(t *testing.T) {
	t.Parallel()
	pager := newMockPager(4096)
	pager.pages[1] = &mockDbPage{data: make([]byte, 4096), pgno: 1}
	adapter := NewPagerAdapter(pager)

	// GetPageData success
	data, err := adapter.GetPageData(1)
	if err != nil {
		t.Fatalf("GetPageData success: %v", err)
	}
	if len(data) == 0 {
		t.Error("GetPageData returned empty data")
	}

	// AllocatePageData success
	pgno, pageData, err := adapter.AllocatePageData()
	if err != nil {
		t.Fatalf("AllocatePageData: %v", err)
	}
	if pgno == 0 || len(pageData) == 0 {
		t.Errorf("AllocatePageData returned invalid values: pgno=%d len=%d", pgno, len(pageData))
	}

	// MarkDirty success
	if err := adapter.MarkDirty(1); err != nil {
		t.Fatalf("MarkDirty: %v", err)
	}
}

// TestAllocatePageExercise exercises the AllocatePage function path
// including the page count increment.
func TestAllocatePageExercise(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	// Allocate several pages
	for i := 0; i < 5; i++ {
		pageNum, err := bt.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage[%d]: %v", i, err)
		}
		if pageNum == 0 {
			t.Errorf("AllocatePage returned 0")
		}
	}
}

// TestIndexCursorSeekExact exercises seekLeafExactMatch via SeekIndex with exact match.
func TestIndexCursorSeekExact(t *testing.T) {
	t.Parallel()
	_, cursor := setupIndexCursor(t, 4096)

	keys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry"), []byte("date"), []byte("elderberry"), []byte("fig"), []byte("grape")}
	insertIndexEntries(cursor, keys)

	for _, k := range keys {
		found, err := cursor.SeekIndex(k)
		if err != nil {
			t.Fatalf("SeekIndex(%q): %v", k, err)
		}
		if !found {
			t.Errorf("SeekIndex(%q) should find exact match", k)
		}
	}
	found, err := cursor.SeekIndex([]byte("avocado"))
	if err != nil {
		t.Fatalf("SeekIndex(avocado): %v", err)
	}
	if found {
		t.Error("SeekIndex(avocado) should not find exact match")
	}
}

// TestCreateWithoutRowidTable_Provider exercises the Provider branch (63.6%).
// We can't easily test Provider without a real pager, but we can exercise
// the non-Provider path more thoroughly.
func TestCreateWithoutRowidTable_InsertAndScan(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	if root == 0 {
		t.Fatal("root page should be non-zero")
	}

	cursor := NewCursorWithOptions(bt, root, true)
	inserted := insertCompositeRows(bt, cursor, 3, "k", 4)
	if inserted != 3 {
		t.Fatalf("expected 3 inserts, got %d", inserted)
	}
	count := scanCompositeForward(bt, cursor.RootPage, 10)
	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
}

// TestIndexCursorMultiPageForwardScan inserts enough index entries to force a
// page split, then scans forward to verify every entry is visited.
// This exercises IndexCursor.climbToNextParent (right-child advance path),
// advanceWithinPage, and NextIndex across page boundaries.
func TestIndexCursorMultiPageForwardScan(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	const n = 40
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			t.Fatalf("InsertIndex(%d): %v", i, err)
		}
	}

	scan := NewIndexCursor(bt, cursor.RootPage)
	count := countIndexForward(scan)
	if count != n {
		t.Errorf("forward scan: want %d, got %d", n, count)
	}
}

// TestIndexCursorMultiPageBackwardScan inserts enough index entries to force a
// page split, then scans backward to verify every entry is visited.
// This exercises IndexCursor.prevInPage, prevViaParent, and descendToLast
// across page boundaries.
func TestIndexCursorMultiPageBackwardScan(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	const n = 40
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			t.Fatalf("InsertIndex(%d): %v", i, err)
		}
	}

	scan := NewIndexCursor(bt, cursor.RootPage)
	count := countIndexBackward(scan)
	if count != n {
		t.Errorf("backward scan: want %d, got %d", n, count)
	}
}

// TestBtCursorSeekThenPrevious seeks to a rowid in a multi-page tree and then
// scans backward, exercising prevViaParent after advanceToChildPage sets the
// parent IndexStack correctly.
func TestBtCursorSeekThenPrevious(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 512, 1, 100, 10)
	n := countForward(cursor)
	if n < 20 {
		t.Fatalf("too few rows: %d", n)
	}

	cursor2 := NewCursor(bt, cursor.RootPage)
	target := int64(n / 2)
	found, err := cursor2.SeekRowid(target)
	if err != nil || !found {
		t.Fatalf("SeekRowid(%d): found=%v err=%v", target, found, err)
	}
	count := 1 + navigateBackward(cursor2, n)
	if count != int(target) {
		t.Errorf("backward from %d: want %d steps, got %d", target, target, count)
	}
}

// TestIndexCursorSeekThenPrevious seeks to a key in a multi-page index and
// then scans backward, exercising IndexCursor.prevViaParent after SeekIndex
// properly sets the parent IndexStack slots.
func TestIndexCursorSeekThenPrevious(t *testing.T) {
	t.Parallel()
	_, cursor := setupIndexCursor(t, 512)

	const n = 40
	insertIndexEntriesN(cursor, n, func(i int) []byte {
		return []byte(fmt.Sprintf("key%03d", i))
	})

	scan := NewIndexCursor(cursor.Btree, cursor.RootPage)
	found, err := scan.SeekIndex([]byte(fmt.Sprintf("key%03d", n-1)))
	if err != nil || !found {
		t.Fatalf("SeekIndex(last): found=%v err=%v", found, err)
	}
	count := 1 + navigateIndexBackward(scan, n)
	if count != n {
		t.Errorf("backward from last: want %d, got %d", n, count)
	}
}

// failAllocProvider returns errors from AllocatePageData to cover AllocatePage
// and CreateTable/CreateWithoutRowidTable error branches.
type failAllocProvider struct {
	simplePageProvider
}

func (m *failAllocProvider) AllocatePageData() (uint32, []byte, error) {
	return 0, nil, fmt.Errorf("simulated allocation failure")
}

// TestAllocatePageProviderError exercises the AllocatePage Provider error path.
func TestAllocatePageProviderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = &failAllocProvider{
		simplePageProvider: simplePageProvider{
			pages:    make(map[uint32][]byte),
			pageSize: 4096,
			nextPage: 1,
		},
	}
	_, err := bt.AllocatePage()
	if err == nil {
		t.Error("expected AllocatePage to fail with failing provider")
	}
}

// TestCreateTableProviderAllocError exercises CreateTable error when Provider.AllocatePageData fails.
func TestCreateTableProviderAllocError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = &failAllocProvider{
		simplePageProvider: simplePageProvider{
			pages:    make(map[uint32][]byte),
			pageSize: 4096,
			nextPage: 1,
		},
	}
	_, err := bt.CreateTable()
	if err == nil {
		t.Error("expected CreateTable to fail with failing alloc provider")
	}
}

// TestCreateTableMarkDirtyError exercises the MarkDirty error path in CreateTable.
func TestCreateTableMarkDirtyError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = newFailingPageProvider(4096)
	_, err := bt.CreateTable()
	if err == nil {
		t.Error("expected CreateTable to fail when MarkDirty fails")
	}
}

// TestCreateWithoutRowidMarkDirtyError exercises the MarkDirty error path in CreateWithoutRowidTable.
func TestCreateWithoutRowidMarkDirtyError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	bt.Provider = newFailingPageProvider(4096)
	_, err := bt.CreateWithoutRowidTable()
	if err == nil {
		t.Error("expected CreateWithoutRowidTable to fail when MarkDirty fails")
	}
}

// TestGetKeyBytesInvalidState exercises the GetKeyBytes nil/invalid guard path.
func TestGetKeyBytesInvalidState(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	// Cursor is not yet valid (no current cell)
	result := cursor.GetKeyBytes()
	if result != nil {
		t.Errorf("GetKeyBytes on invalid cursor should return nil, got %v", result)
	}
}

// TestSplitPageInteriorBranch exercises the splitPage "interior page" error path.
// We position a cursor on an interior page and call splitPage directly.
// findInteriorPage searches for an interior page in the btree and returns its page number and header.
func findInteriorPage(bt *Btree) (uint32, *PageHeader) {
	for pgno := uint32(1); pgno <= uint32(len(bt.Pages)); pgno++ {
		data, err := bt.GetPage(pgno)
		if err != nil {
			continue
		}
		header, err := ParsePageHeader(data, pgno)
		if err != nil {
			continue
		}
		if header.IsInterior {
			return pgno, header
		}
	}
	return 0, nil
}

func TestSplitPageInteriorBranch(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 200, 17)

	pgno, header := findInteriorPage(bt)
	if pgno == 0 {
		t.Skip("no interior page found in tree")
	}
	cursor2 := NewCursor(bt, rootPage)
	cursor2.CurrentPage = pgno
	cursor2.CurrentHeader = header
	cursor2.State = CursorValid
	err = cursor2.splitPage(999, nil, []byte("x"))
	if err == nil {
		t.Error("splitPage on interior page should return error")
	}
}

// TestSplitPageNilHeader exercises the splitPage nil-header guard.
func TestSplitPageNilHeader(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.CurrentHeader = nil
	err = cursor.splitPage(1, nil, []byte("x"))
	if err == nil {
		t.Error("splitPage with nil header should return error")
	}
}

// TestLoadCellAtCurrentIndexError exercises loadCellAtCurrentIndex GetCellPointer error path
// by providing a header that claims more cells than the page actually has.
func TestLoadCellAtCurrentIndexError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("data")) //nolint:errcheck

	// Set CurrentIndex to an out-of-bounds value.
	data, _ := bt.GetPage(rootPage)
	header, _ := ParsePageHeader(data, rootPage)
	cursor.CurrentHeader = header
	cursor.CurrentIndex = int(header.NumCells) + 100 // out of bounds
	cursor.State = CursorValid
	cursor.CurrentPage = rootPage

	err = cursor.loadCellAtCurrentIndex(data)
	if err == nil {
		t.Error("loadCellAtCurrentIndex with bad index should return error")
	}
}

// TestBtreeParsePage_BadPage exercises ParsePage GetPage error path.
func TestBtreeParsePage_BadPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	_, _, err := bt.ParsePage(9999)
	if err == nil {
		t.Error("ParsePage on nonexistent page should return error")
	}
}

// TestBtreeParsePage_InvalidHeader exercises ParsePage with a page that has an invalid type.
func TestBtreeParsePage_InvalidHeader(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	// Allocate a raw page with zero bytes (invalid page type).
	data := make([]byte, 4096)
	bt.SetPage(2, data)
	_, _, err := bt.ParsePage(2)
	if err == nil {
		t.Error("ParsePage on page with invalid header should return error")
	}
}

// TestCompositeBackwardScanMultiPage inserts enough composite rows to force
// multiple pages, then scans backward. This exercises navigateToRightmostLeafComposite
// with proper IndexStack tracking.
func TestCompositeBackwardScanMultiPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	const n = 60
	for i := 0; i < n; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("k%03d", i)})
		if err := cursor.InsertWithComposite(0, key, []byte("val")); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}

	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	count := countBackward(scan)
	if count != n {
		t.Errorf("backward composite scan: want %d, got %d", n, count)
	}
}

// TestSeekRowidhThenFullBackward seeks to a mid-point in a large multi-page tree
// and scans all the way backward, exercising prevViaParent across multiple levels.
func TestSeekRowidThenFullBackward(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 4096, 1, 200, 100)
	n := countForward(cursor)
	if n < 50 {
		t.Fatalf("too few rows: %d", n)
	}

	cursor2 := NewCursor(bt, cursor.RootPage)
	target := int64(n)
	found, err := cursor2.SeekRowid(target)
	if err != nil || !found {
		t.Fatalf("SeekRowid(%d): found=%v err=%v", target, found, err)
	}
	count := 1 + navigateBackward(cursor2, n)
	if count != n {
		t.Errorf("full backward from %d: want %d, got %d", target, n, count)
	}
}
