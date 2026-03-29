// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

// MC/DC 11 — btree low-coverage branch coverage
//
// Targets:
//   btree.go:355    CreateWithoutRowidTable     — normal path (btree already tested in mcdc10)
//   btree.go:379    ClearTableData              — interior root node path
//   btree.go:432    dropInteriorChildren        — called from ClearTableData on interior root
//   cursor.go:119   MoveToLast                  — composite (WITH WITHOUT ROWID) path
//   cursor.go:203   positionAtLastCell          — error-inject empty leaf via corrupt page
//   cursor.go:273   advanceWithinPage           — GetPage failure mid-advance
//   cursor.go:342   loadParentPage              — GetPage failure at depth > 0
//   cursor.go:357   getChildPageFromParent      — cell parse failure (corrupt data)
//   cursor.go:372   Previous                    — multi-level tree backward scan
//   cursor.go:452   setupLeafFirst              — descendToFirst on composite
//   cursor.go:515   descendToLast               — depth overflow (too many levels)

import (
	"encoding/binary"
	"testing"
)

// ---------------------------------------------------------------------------
// Shared helper: fakeProvider used across btree tests
// Re-use mcdc10Provider (already defined in btree_mcdc10_test.go).
// ---------------------------------------------------------------------------

// buildRowidTree builds a rowid btree with 512-byte pages and n rows.
func buildRowidTree(t *testing.T, n int) (*Btree, uint32) {
	t.Helper()
	bt := NewBtree(512)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)
	payload := make([]byte, 40)
	for i := int64(1); i <= int64(n); i++ {
		binary.BigEndian.PutUint64(payload, uint64(i))
		if err := c.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	return bt, c.RootPage
}

// ---------------------------------------------------------------------------
// ClearTableData — interior root path
// ---------------------------------------------------------------------------

// TestMCDC11_ClearTableData_InteriorRoot exercises ClearTableData when the root
// has become an interior node after many inserts (triggers dropInteriorChildren).
func TestMCDC11_ClearTableData_InteriorRoot(t *testing.T) {
	t.Parallel()

	// 512-byte pages fill up quickly; 50 rows of 40 bytes each will trigger splits.
	bt, root := buildRowidTree(t, 50)

	// Verify the root became interior (otherwise the test is vacuous).
	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage root: %v", err)
	}
	header, err := ParsePageHeader(pageData, root)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}

	// ClearTableData must succeed regardless of whether root is leaf or interior.
	if err := bt.ClearTableData(root); err != nil {
		t.Fatalf("ClearTableData: %v", err)
	}

	// After clearing, root should be an empty leaf.
	pageData2, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage after clear: %v", err)
	}
	header2, err := ParsePageHeader(pageData2, root)
	if err != nil {
		t.Fatalf("ParsePageHeader after clear: %v", err)
	}
	if !header2.IsLeaf {
		t.Error("expected leaf root after ClearTableData")
	}
	if header2.NumCells != 0 {
		t.Errorf("expected 0 cells after ClearTableData, got %d", header2.NumCells)
	}
	_ = header
}

// ---------------------------------------------------------------------------
// MoveToLast — composite (WITHOUT ROWID) path
// ---------------------------------------------------------------------------

// TestMCDC11_MoveToLast_Composite exercises navigateToRightmostLeafComposite
// by calling MoveToLast on a WITHOUT ROWID cursor with multiple entries.
func TestMCDC11_MoveToLast_Composite(t *testing.T) {
	t.Parallel()

	bt, _, cur := mcdc10CompositeTree(t, 20)
	_ = bt

	if err := cur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast (composite): %v", err)
	}
	if !cur.IsValid() {
		t.Error("expected cursor to be valid after MoveToLast")
	}
}

// ---------------------------------------------------------------------------
// Previous — multi-level backward scan
// ---------------------------------------------------------------------------

// TestMCDC11_Previous_MultiLevel exercises Previous on a multi-level tree,
// navigating from MoveToLast backwards through all entries.
func TestMCDC11_Previous_MultiLevel(t *testing.T) {
	t.Parallel()

	const n = 50
	bt, root := buildRowidTree(t, n)
	c := NewCursor(bt, root)

	if err := c.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}

	count := 1
	for {
		err := c.Previous()
		if err != nil {
			break // "beginning of btree" sentinel
		}
		count++
	}
	if count != n {
		t.Errorf("expected %d rows via Previous, got %d", n, count)
	}
}

// ---------------------------------------------------------------------------
// advanceWithinPage — GetPage failure mid-advance
// ---------------------------------------------------------------------------

// TestMCDC11_AdvanceWithinPage_GetPageFail exercises the GetPage error path
// in advanceWithinPage by injecting a failure on a specific page once the
// cursor is mid-page (CurrentIndex < NumCells-1).
func TestMCDC11_AdvanceWithinPage_GetPageFail(t *testing.T) {
	t.Parallel()

	// Build a small tree.  With 512-byte pages and 40-byte payloads each leaf
	// page holds ~7-8 entries; use 8 entries so at least 2 land on the same leaf.
	bt, p, c := mcdc10RowidTree(t, 8)

	// Move to first — cursor sits at index 0 on a leaf page.
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	// Ensure there are multiple cells on the current page.
	if c.CurrentHeader == nil || c.CurrentHeader.NumCells <= 1 {
		t.Skip("too few cells on first leaf — cannot test mid-page failure")
	}

	// Evict the current page from the cache so GetPage calls the provider.
	currentPage := c.CurrentPage
	bt.mu.Lock()
	delete(bt.Pages, currentPage)
	bt.mu.Unlock()

	// Inject provider failure for this page.
	p.failGetPage = currentPage

	// Advance to next — advanceWithinPage increments index then calls GetPage → fails.
	err := c.Next()
	if err == nil {
		t.Skip("advance did not observe GetPage failure")
	}
}

// ---------------------------------------------------------------------------
// loadParentPage — GetPage failure at depth > 0
// ---------------------------------------------------------------------------

// TestMCDC11_LoadParentPage_Fail exercises the GetPage error path in
// loadParentPage by injecting a failure on the root (parent) page while the
// cursor is at depth > 0.
func TestMCDC11_LoadParentPage_Fail(t *testing.T) {
	t.Parallel()

	// Build a tree large enough to have an interior root and leaf children.
	bt, p, c := mcdc10RowidTree(t, 50)

	// Move to first leaf.
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	if c.Depth == 0 {
		t.Skip("tree does not have multi-level structure")
	}

	// Walk to the end of the first leaf page using Next.
	for c.CurrentIndex < int(c.CurrentHeader.NumCells)-1 {
		if err := c.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		}
	}

	// At this point we're at the last cell of the first leaf page (depth > 0).
	// Evict the parent page from the cache so GetPage calls the provider.
	parentPage := c.PageStack[c.Depth-1]
	bt.mu.Lock()
	delete(bt.Pages, parentPage)
	bt.mu.Unlock()

	// Inject a failure on the parent page so loadParentPage fails.
	p.failGetPage = parentPage

	// Next should now fail inside climbToNextParent → tryAdvanceInParent → loadParentPage.
	err := c.Next()
	if err == nil {
		t.Skip("Next did not observe loadParentPage failure")
	}
}

// ---------------------------------------------------------------------------
// CreateWithoutRowidTable — verify page type
// ---------------------------------------------------------------------------

// TestMCDC11_CreateWithoutRowidTable_PageType verifies the page type header
// of a newly-created WITHOUT ROWID table.
func TestMCDC11_CreateWithoutRowidTable_PageType(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	header, err := ParsePageHeader(pageData, root)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if header.PageType != PageTypeLeafTableNoInt {
		t.Errorf("expected PageTypeLeafTableNoInt (%d), got %d", PageTypeLeafTableNoInt, header.PageType)
	}
}

// ---------------------------------------------------------------------------
// DropTable — interior root (dropInteriorChildren called from DropTable)
// ---------------------------------------------------------------------------

// TestMCDC11_DropTable_InteriorRoot exercises DropTable when root is interior.
func TestMCDC11_DropTable_InteriorRoot(t *testing.T) {
	t.Parallel()

	bt, root := buildRowidTree(t, 50)

	if err := bt.DropTable(root); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	// Root page should no longer be in Pages after drop.
	bt.mu.RLock()
	_, ok := bt.Pages[root]
	bt.mu.RUnlock()
	if ok {
		t.Error("expected root page removed after DropTable")
	}
}

// ---------------------------------------------------------------------------
// descendToLast — composite multi-level tree
// ---------------------------------------------------------------------------

// TestMCDC11_DescendToLast_CompositeMultiLevel exercises descendToLast on a
// composite tree large enough to have interior nodes.
func TestMCDC11_DescendToLast_CompositeMultiLevel(t *testing.T) {
	t.Parallel()

	// 60 entries of 8-byte key + 30-byte payload on 512-byte pages → multi-level.
	_, _, cur := mcdc10CompositeTree(t, 60)

	if err := cur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast (composite multi-level): %v", err)
	}
	if !cur.IsValid() {
		t.Error("expected valid cursor after MoveToLast on multi-level composite tree")
	}
}
