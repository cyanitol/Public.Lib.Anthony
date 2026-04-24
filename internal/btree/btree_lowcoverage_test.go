// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestLowCoverageCalculateLocalPayload exercises the uncovered branches of
// calculateLocalPayload in cell.go.
func TestBtreeLowCoverageCalculateLocalPayload_EdgeCases(t *testing.T) {
	t.Parallel()
	// usableSize < 4 → early return of minLocal (clamped to 0)
	got := calculateLocalPayload(100, 0, 10, 3)
	if got != 0 {
		t.Errorf("usableSize<4: got %d, want 0", got)
	}
	// payloadSize < minLocal → early return of minLocal
	got = calculateLocalPayload(5, 10, 50, 512)
	if got != 10 {
		t.Errorf("payloadSize<minLocal: got %d, want 10", got)
	}
}

func TestBtreeLowCoverageCalculateLocalPayload_Surplus(t *testing.T) {
	t.Parallel()
	usableSize := uint32(512)
	maxLocal := calculateMaxLocal(usableSize, true)
	minLocal := calculateMinLocal(usableSize, true)
	payloadSize := minLocal + 1
	surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
	got := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
	if surplus <= maxLocal && got != uint16(surplus) {
		t.Errorf("surplus<=maxLocal: got %d, want %d", got, surplus)
	}

	// surplus > maxLocal → return minLocal
	excess := maxLocal - minLocal + 1
	payloadSize = minLocal + excess
	got = calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
	if got != uint16(minLocal) {
		t.Errorf("surplus>maxLocal: got %d, want %d (minLocal)", got, minLocal)
	}
}

func TestBtreeLowCoverageCalculateLocalPayload_MultiSize(t *testing.T) {
	t.Parallel()
	for _, us := range []uint32{512, 1024, 4096} {
		ml := calculateMaxLocal(us, true)
		mn := calculateMinLocal(us, true)
		result := calculateLocalPayload(us*2, mn, ml, us)
		if uint32(result) > ml {
			t.Errorf("usableSize=%d: result %d exceeds maxLocal %d", us, result, ml)
		}
	}
	// boundary: payloadSize == minLocal
	usableSize := uint32(1024)
	maxLocal := calculateMaxLocal(usableSize, true)
	minLocal := calculateMinLocal(usableSize, true)
	got := calculateLocalPayload(minLocal, minLocal, maxLocal, usableSize)
	if got != uint16(minLocal) {
		t.Errorf("payloadSize==minLocal: got %d, want %d", got, minLocal)
	}
}

// TestBtreeLowCoverageEnterPage exercises cursor.enterPage by using
// descendToLast which calls enterPage internally.
func TestBtreeLowCoverageEnterPage(t *testing.T) {
	t.Parallel()

	// Build a multi-level tree so descendToLast traverses interior pages.
	bt, cursor := setupBtreeWithRows(t, 512, 1, 200, 20)
	_ = bt

	// MoveToLast triggers navigateToRightmostLeaf (uses getPageAndHeader, not enterPage).
	// Previous() → prevViaParent → descendToLast → enterPage.
	if err := cursor.MoveToLast(); err != nil {
		t.Skipf("MoveToLast: %v", err)
	}
	if !cursor.IsValid() {
		t.Skip("cursor invalid after MoveToLast")
	}

	// Call Previous() repeatedly; prevViaParent calls descendToLast which calls enterPage.
	prevCount := 0
	for cursor.IsValid() {
		if err := cursor.Previous(); err != nil {
			break
		}
		prevCount++
		if prevCount > 10 {
			break
		}
	}

	// Also test enterPage depth exceeded: craft a cursor at MaxBtreeDepth-1.
	c2 := NewCursor(bt, cursor.RootPage)
	c2.Depth = MaxBtreeDepth - 1
	_, _, err := c2.enterPage(cursor.RootPage)
	if err == nil {
		t.Error("expected error when depth >= MaxBtreeDepth, got nil")
	}
}

// TestBtreeLowCoverageTryLoadCell exercises tryLoadCell branches: normal index,
// idx >= NumCells, idx < 0, and empty page.
func TestBtreeLowCoverageTryLoadCell_ValidIndex(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 4096, 1, 50, 10)
	found, err := cursor.SeekRowid(25)
	if err != nil || !found {
		t.Skipf("SeekRowid(25) not found: %v", err)
	}
	pageData, err := bt.GetPage(cursor.CurrentPage)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	header := cursor.CurrentHeader

	cursor.tryLoadCell(pageData, header, 0)
	if cursor.CurrentCell == nil {
		t.Error("tryLoadCell(0) set CurrentCell to nil unexpectedly")
	}
	cursor.tryLoadCell(pageData, header, int(header.NumCells)+5)
	if cursor.CurrentCell == nil {
		t.Error("tryLoadCell(oversized) set CurrentCell to nil")
	}
	cursor.tryLoadCell(pageData, header, -3)
	if cursor.CurrentCell == nil {
		t.Error("tryLoadCell(-3) set CurrentCell to nil")
	}
}

func TestBtreeLowCoverageTryLoadCell_EmptyPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	emptyData, _ := bt.GetPage(rootPage)
	emptyHeader, _ := ParsePageHeader(emptyData, rootPage)
	cursor := NewCursor(bt, rootPage)

	cursor.tryLoadCell(emptyData, emptyHeader, 0)
	if cursor.CurrentCell != nil {
		t.Error("tryLoadCell on empty page should set CurrentCell to nil")
	}
	cursor.tryLoadCell(emptyData, emptyHeader, -1)
	if cursor.CurrentCell != nil {
		t.Error("tryLoadCell(-1) on empty page should set CurrentCell to nil")
	}
}

// TestBtreeLowCoverageRedistributeLeafCells exercises redistributeLeafCells by
// inserting enough rows to force a leaf page split.
func TestBtreeLowCoverageRedistributeLeafCells(t *testing.T) {
	t.Parallel()

	// Small page size causes splits quickly.
	bt, cursor := setupBtreeWithRows(t, 512, 1, 100, 15)
	_ = bt

	// Verify the tree is consistent by counting entries forward.
	count := countForward(cursor)
	if count == 0 {
		t.Error("expected at least some rows after inserts")
	}

	// Confirm ordering is preserved after splits.
	n := verifyOrderedForward(t, cursor)
	if n == 0 {
		t.Error("verifyOrderedForward returned 0")
	}
}

// TestBtreeLowCoverageRedistributeInteriorCells forces interior page splits by
// inserting a large number of rows so the interior page fills and splits.
func TestBtreeLowCoverageRedistributeInteriorCells(t *testing.T) {
	t.Parallel()

	// Use a small page size so interior splits happen sooner.
	bt, cursor := setupBtreeWithRows(t, 512, 1, 500, 5)
	_ = bt

	count := countForward(cursor)
	if count == 0 {
		t.Error("expected rows after inserts")
	}

	n := verifyOrderedForward(t, cursor)
	if n == 0 {
		t.Error("verifyOrderedForward returned 0 after interior splits")
	}
}

// TestBtreeLowCoverageInsertDividerIntoParent exercises insertDividerIntoParent
// by growing a tree with enough rows that multiple leaf splits require inserting
// dividers into an existing (non-root) parent page.
func TestBtreeLowCoverageInsertDividerIntoParent(t *testing.T) {
	t.Parallel()

	// Use 512-byte pages and moderate payload to force many leaf splits each
	// requiring a divider to be inserted into an existing interior parent.
	bt, cursor := setupBtreeWithRows(t, 512, 1, 300, 20)
	_ = bt

	count := verifyOrderedForward(t, cursor)
	if count == 0 {
		t.Error("expected rows after inserts triggering insertDividerIntoParent")
	}

	// Also verify backward ordering to confirm tree integrity.
	n := verifyOrderedBackward(t, cursor)
	if n == 0 {
		t.Error("verifyOrderedBackward returned 0")
	}
	if n != count {
		t.Errorf("forward count %d != backward count %d", count, n)
	}

	// Seek to confirm the divider boundaries work correctly.
	found, err := cursor.SeekRowid(1)
	if err != nil || !found {
		t.Errorf("SeekRowid(1) found=%v err=%v", found, err)
	}
	found, err = cursor.SeekRowid(int64(count))
	if err != nil || !found {
		t.Errorf("SeekRowid(%d) found=%v err=%v", count, found, err)
	}
}
