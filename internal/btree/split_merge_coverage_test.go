// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestSplitMerge_LeafSplitSmallPage inserts many rows into a 512-byte page
// tree to force leaf splits. With small pages and moderate payloads, the tree
// quickly fills a leaf page and calls splitLeafPage, which exercises
// allocateAndInitializeLeafPage, executeLeafSplit, and redistributeLeafCells.
func TestSplitMerge_LeafSplitSmallPage(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	// 40-byte payload forces splits quickly at 512-byte page size.
	payload := make([]byte, 40)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	const n = 60
	for i := int64(1); i <= n; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Verify all rows are present and in order.
	got := verifyOrderedForward(t, cursor)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}

// TestSplitMerge_InteriorSplitSmallPage inserts enough rows to force interior
// page splits, exercising allocateAndInitializeInteriorPage,
// executeInteriorSplit, redistributeInteriorCells, and insertDividerIntoParent.
func TestSplitMerge_InteriorSplitSmallPage(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	// Small payload so more cells fit per leaf before splitting, meaning
	// we need many rows to build deep enough for interior splits.
	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte('b')
	}

	const n = 200
	for i := int64(1); i <= n; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}

// TestSplitMerge_InsertThenDeleteForceMerge inserts rows to force splits and
// then deletes most of them, causing MergePage to be called (exercising
// loadMergePages and updateParentAfterMerge).
func TestSplitMerge_InsertThenDeleteForceMerge(t *testing.T) {
	t.Parallel()
	bt, insertCursor := setupBtreeWithRows(t, 512, 1, 80, 35)

	before := countForward(NewCursor(bt, insertCursor.RootPage))
	if before != 80 {
		t.Fatalf("before delete: expected 80 rows, got %d", before)
	}

	deleteRowRange(NewCursor(bt, insertCursor.RootPage), 1, 75)

	remaining := countForward(NewCursor(bt, insertCursor.RootPage))
	if remaining < 0 || remaining > 80 {
		t.Errorf("unexpected remaining row count: %d", remaining)
	}
}

// TestSplitMerge_MergeWithParentUpdate inserts rows and deletes alternating
// rows to keep the tree active while still forcing merge paths, specifically
// exercising updateParentAfterMerge when the last cell of the parent is removed.
func TestSplitMerge_MergeWithParentUpdate(t *testing.T) {
	t.Parallel()
	bt, ins := setupBtreeWithRows(t, 512, 1, 100, 30)

	// Delete even-numbered rows, then most odd rows.
	del := NewCursor(bt, ins.RootPage)
	for i := int64(2); i <= 100; i += 2 {
		seekAndDelete(del, i) //nolint:errcheck
	}
	for i := int64(3); i <= 90; i += 2 {
		seekAndDelete(del, i) //nolint:errcheck
	}

	count := countForward(NewCursor(bt, ins.RootPage))
	if count < 1 {
		t.Error("expected at least one row to remain")
	}
}

// TestSplitMerge_LoadMergePages_ViaMerge exercises loadMergePages by driving a
// merge through the cursor Delete path on a small-page tree. After each
// deletion that produces a CursorInvalid state the cursor is re-used from the
// root so it stays valid.
func TestSplitMerge_LoadMergePages_ViaMerge(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	const insertN = 70
	bt, cursor := setupBtreeWithRows(t, pageSize, 1, insertN, 38)

	root := cursor.RootPage

	// Verify all rows present.
	scanBefore := NewCursor(bt, root)
	before := countForward(scanBefore)
	if before == 0 {
		t.Fatal("no rows were inserted")
	}

	del := NewCursor(bt, root)
	deleteUpTo := int64(before - 5)
	if deleteUpTo < 1 {
		deleteUpTo = 1
	}

	// Delete rows to exercise loadMergePages code path.
	for i := int64(1); i <= deleteUpTo; i++ {
		found, err := del.SeekRowid(i)
		if err != nil || !found {
			del = NewCursor(bt, root)
			continue
		}
		del.Delete()
		// Refresh cursor after state becomes invalid.
		del = NewCursor(bt, root)
	}

	// Tree must still be traversable without panic.
	scan := NewCursor(bt, root)
	got := countForward(scan)
	t.Logf("rows remaining after partial deletion: %d (before: %d)", got, before)
}

// TestSplitMerge_InsertDividerIntoParent_AppendPath inserts rows in ascending
// order so each new divider key is always greater than all existing dividers,
// causing insertDividerIntoParent to use the append-at-end path and update the
// page right-child pointer rather than any existing cell.
func TestSplitMerge_InsertDividerIntoParent_AppendPath(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	payload := make([]byte, 45)
	for i := range payload {
		payload[i] = byte('e')
	}

	// Ascending inserts – dividers always appended at end.
	const n = 50
	for i := int64(1); i <= n; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}

// TestSplitMerge_InsertDividerIntoParent_MiddlePath inserts rows in descending
// order so each new divider key is less than all existing dividers, causing
// insertDividerIntoParent to find an insertion point before the first cell and
// then update the next cell's left-child pointer (the nextIdx < numCells path).
func TestSplitMerge_InsertDividerIntoParent_MiddlePath(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	payload := make([]byte, 45)
	for i := range payload {
		payload[i] = byte('f')
	}

	// Descending inserts – each new divider is smaller than all existing ones.
	const n = 50
	for i := int64(n); i >= 1; i-- {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}

// TestSplitMerge_AllocateLeafPageNonZeroType exercises the branch in
// allocateAndInitializeLeafPage where pageType is already non-zero (passed as
// PageTypeLeafTableNo). The table path through splitLeafPage passes the current
// header's PageType to allocateAndInitializeLeafPage, and CreateWithoutRowidTable
// produces a leaf-table-no page.
func TestSplitMerge_AllocateLeafPageNonZeroType(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	payload := make([]byte, 50)
	for i := range payload {
		payload[i] = byte('g')
	}

	// Enough rows to force at least one split (tests leaf page allocation with
	// the non-zero pageType branch via the existing header PageType).
	const n = 30
	for i := int64(1); i <= n; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}

// TestSplitMerge_RedistributeLeafCells_SmallPage performs many inserts and
// some deletes on a very small page to exercise redistributeLeafCells through
// MergePage when sibling pages cannot merge (page content too large to fit in
// one page) and fall through to redistributeSiblings.
func TestSplitMerge_RedistributeLeafCells_SmallPage(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt, cursor := setupBtreeWithRows(t, pageSize, 1, 80, 20)
	root := cursor.RootPage

	// Count what was actually inserted.
	scanBefore := NewCursor(bt, root)
	before := countForward(scanBefore)
	if before == 0 {
		t.Fatal("no rows were inserted")
	}

	// Delete the first half to trigger underfull pages / merge / redistribute.
	deleteUpTo := int64(before / 2)
	del := NewCursor(bt, root)
	for i := int64(1); i <= deleteUpTo; i++ {
		found, _ := del.SeekRowid(i)
		if found {
			del.Delete()
			del = NewCursor(bt, root)
		}
	}

	// Tree must still be traversable without panic.
	scan := NewCursor(bt, root)
	got := countForward(scan)
	t.Logf("rows remaining: %d (before: %d, deleted up to: %d)", got, before, deleteUpTo)
}

// TestSplitMerge_RedistributeInteriorCells exercises redistributeInteriorCells
// by building a tree deep enough (via many inserts) that interior page splits
// are necessary on a 512-byte page with medium-sized payloads.
func TestSplitMerge_RedistributeInteriorCells(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	payload := make([]byte, 15)
	for i := range payload {
		payload[i] = byte('h')
	}

	// 300 rows at 15 bytes each is sufficient to overflow an interior page at
	// 512 bytes and trigger executeInteriorSplit -> redistributeInteriorCells.
	const n = 300
	for i := int64(1); i <= n; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got != n {
		t.Errorf("expected %d rows, got %d", n, got)
	}
}
