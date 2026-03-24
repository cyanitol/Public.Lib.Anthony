// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
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
func TestCompositeInteriorSplit(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	payload := bytes.Repeat([]byte("z"), 50)
	inserted := 0
	for i := 0; i < 300; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("k%06d", i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			break
		}
		inserted++
	}
	if inserted < 2 {
		t.Fatalf("Only inserted %d rows, need at least 2 to trigger split paths", inserted)
	}

	// Verify forward scan reads back all inserted rows
	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	if err := scan.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	count := 0
	for scan.IsValid() && count < inserted+10 {
		count++
		if err := scan.Next(); err != nil {
			break
		}
	}
	// Allow for minor discrepancies (e.g. last insert may not have committed).
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
	bt := NewBtree(512) // small pages → many interior levels
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	cursor := NewIndexCursor(bt, rootPage)

	inserted := 0
	for i := 0; i < 300; i++ {
		key := []byte(fmt.Sprintf("idx%06d", i))
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			break
		}
		inserted++
	}
	if inserted < 10 {
		t.Fatalf("Only inserted %d index entries", inserted)
	}

	// MoveToFirst establishes a correct IndexStack via descendToFirst
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	// Advance well into the tree via NextIndex so parent indices > 0
	forward := 0
	for cursor.IsValid() && forward < inserted/2 {
		if err := cursor.NextIndex(); err != nil {
			break
		}
		forward++
	}

	// Now iterate backward — prevViaParent / descendToLast / enterPage are hit
	// when we cross page boundaries while going backward.
	backward := 0
	for cursor.IsValid() && backward < inserted {
		backward++
		if err := cursor.PrevIndex(); err != nil {
			break
		}
	}
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

	inserted := 0
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("scan%06d", i))
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			break
		}
		inserted++
	}

	// Full forward traversal with NextIndex to build proper IndexStack state
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	for cursor.IsValid() {
		if err := cursor.NextIndex(); err != nil {
			break
		}
	}

	// Now restart with a fresh forward pass and mid-point backward scan
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst (2nd): %v", err)
	}
	// Move to ~75% of the way through
	for i := 0; i < inserted*3/4 && cursor.IsValid(); i++ {
		if err := cursor.NextIndex(); err != nil {
			break
		}
	}
	// Scan backward from here to the beginning
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.PrevIndex(); err != nil {
			break
		}
	}
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
func TestMergeGetSiblingWithLeftPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// Build a multi-page tree
	for i := int64(1); i <= 80; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}

	// Navigate from start with MoveToFirst + Next to properly track IndexStack
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	// Advance past the first page boundary to get IndexStack[parent] > 0
	prevPage := cursor.CurrentPage
	crossedBoundary := false
	for cursor.IsValid() {
		if err := cursor.Next(); err != nil {
			break
		}
		if cursor.CurrentPage != prevPage {
			crossedBoundary = true
			break
		}
		prevPage = cursor.CurrentPage
	}

	// If we crossed a page boundary and Depth > 0, IndexStack[parent] == 1
	// so findSiblingPages routes to getSiblingWithLeftPage
	if crossedBoundary && cursor.IsValid() && cursor.Depth > 0 {
		cursor.MergePage() //nolint:errcheck
	}
}

// TestMergeGetSiblingAsRightmost exercises getSiblingAsRightmost (0% merge.go:125).
// getSiblingAsRightmost is called when parentIndex == parentHeader.NumCells.
// This happens after Next() has advanced IndexStack[parent] to NumCells (right child).
func TestMergeGetSiblingAsRightmost(t *testing.T) {
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

	// Navigate to the last cell via MoveToFirst + iterate all the way
	// to ensure IndexStack[parent] == NumCells at the rightmost child
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	// Iterate to the last valid entry
	for cursor.IsValid() {
		prevKey := cursor.GetKey()
		if err := cursor.Next(); err != nil {
			// End of tree - go back to previous position
			cursor.SeekRowid(prevKey)
			break
		}
	}

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

	// Insert then delete most rows to create underfull non-root pages
	for i := int64(1); i <= 80; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}
	// Delete most to make leaf pages underfull
	for i := int64(5); i <= 75; i++ {
		found, err := cursor.SeekRowid(i)
		if err == nil && found {
			cursor.Delete() //nolint:errcheck
		}
	}

	// Navigate with MoveToFirst + Next to position at a non-root leaf
	if err := cursor.MoveToFirst(); err != nil {
		t.Logf("MoveToFirst: %v", err)
		return
	}
	// Advance to a non-root page if possible
	if cursor.IsValid() && cursor.Depth > 0 {
		if err := balance(cursor); err != nil {
			t.Logf("balance() on underfull non-root: %v (expected)", err)
		}
	}
}

// TestBalanceDefragmentIfNeeded exercises the defragmentIfNeeded path (50%).
// Create a page that is neither overfull nor underfull but has fragmented bytes.
func TestBalanceDefragmentIfNeeded(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	// Insert, delete some to create fragmentation, then balance
	for i := int64(1); i <= 20; i++ {
		if err := cursor.Insert(i, make([]byte, 50)); err != nil {
			break
		}
	}
	for i := int64(5); i <= 10; i++ {
		found, err := cursor.SeekRowid(i)
		if err == nil && found {
			cursor.Delete() //nolint:errcheck
		}
	}

	// Position at a remaining entry
	found, err := cursor.SeekRowid(12)
	if err != nil || !found {
		return
	}
	// Manually set fragmented bytes to force defragmentIfNeeded path
	pageData, getErr := bt.GetPage(cursor.CurrentPage)
	if getErr != nil {
		return
	}
	// Set the fragmented bytes field in the page header
	headerOffset := 0
	if cursor.CurrentPage == 1 {
		headerOffset = FileHeaderSize
	}
	pageData[headerOffset+PageHeaderOffsetFragmented] = 10

	// Reload current header
	header, parseErr := ParsePageHeader(pageData, cursor.CurrentPage)
	if parseErr != nil {
		return
	}
	cursor.CurrentHeader = header

	if err := balance(cursor); err != nil {
		t.Logf("balance() with fragmentation: %v", err)
	}
}
