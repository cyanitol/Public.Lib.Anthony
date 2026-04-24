// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestDefrag_LeafSplitAfterFragmentation inserts rows to fill a page, deletes
// several to create fragmented free space, then inserts more rows to trigger a
// leaf split. The split calls redistributeLeafCells -> defragmentBothPages ->
// Defragment on both the left and right pages produced by the split.
// deleteEveryNth deletes every n-th row in [start, end] using seekAndDelete.
func deleteEveryNth(bt *Btree, root uint32, start, end, step int64) uint32 {
	cur := NewCursor(bt, root)
	for i := start; i <= end; i += step {
		seekAndDelete(cur, i) //nolint:errcheck
		cur = NewCursor(bt, root)
	}
	return root
}

// insertRowsWithReset inserts rows [start, end] with given payload, re-creating cursor after each.
func insertRowsWithReset(t *testing.T, bt *Btree, root uint32, start, end int64, payload []byte) uint32 {
	t.Helper()
	cursor := NewCursor(bt, root)
	for i := start; i <= end; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		cursor = NewCursor(bt, cursor.RootPage)
	}
	return cursor.RootPage
}

func TestDefrag_LeafSplitAfterFragmentation(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 512, 1, 40, 48)
	root := cursor.RootPage

	deleteEveryNth(bt, root, 2, 20, 2)
	root = insertRowsWithReset(t, bt, root, 41, 70, make([]byte, 48))

	got := verifyOrderedForward(t, NewCursor(bt, root))
	if got < 1 {
		t.Error("expected at least one row after insert/delete/insert cycle")
	}
}

// TestDefrag_InteriorSplitAfterFragmentation builds a tree deep enough to
// trigger interior page splits after deleting rows to create fragmentation,
// exercising redistributeInteriorCells -> defragmentBothPages on interior pages.
func TestDefrag_InteriorSplitAfterFragmentation(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 512, 1, 220, 18)
	root := cursor.RootPage

	deleteEveryNth(bt, root, 10, 100, 5)
	root = insertRowsWithReset(t, bt, root, 221, 300, make([]byte, 18))

	got := verifyOrderedForward(t, NewCursor(bt, root))
	if got < 1 {
		t.Error("expected at least one row after interior fragmentation+split")
	}
}

// TestDefrag_VaryingPayloadSizes triggers multiple leaf splits with payloads
// of three different sizes in sequence. Different sizes produce different cell
// counts per page and different medianIdx values, exercising varied code paths
// through redistributeLeafCells and defragmentBothPages.
func TestDefrag_VaryingPayloadSizes(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	sizes := []int{30, 50, 42}
	rowid := int64(1)
	for _, sz := range sizes {
		p := make([]byte, sz)
		for i := range p {
			p[i] = byte('c' + sz%26)
		}
		for n := 0; n < 25; n++ {
			if err := cursor.Insert(rowid, p); err != nil {
				t.Fatalf("Insert(rowid=%d, size=%d): %v", rowid, sz, err)
			}
			cursor = NewCursor(bt, cursor.RootPage)
			rowid++
		}
	}

	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	want := int(rowid - 1)
	if got != want {
		t.Errorf("expected %d rows, got %d", want, got)
	}
}

// TestDefrag_BackwardInsertWithFragmentation inserts rows in descending order
// (which places new dividers before existing ones, exercising the middle-insert
// path of insertDividerIntoParent), deletes some rows, then continues inserting
// to trigger splits on fragmented pages.
func TestDefrag_BackwardInsertWithFragmentation(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	// Insert descending to exercise front-of-parent divider insertion.
	cursor := NewCursor(bt, root)
	payload := make([]byte, 44)
	for i := int64(50); i >= 1; i-- {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	root = cursor.RootPage

	deleteEveryNth(bt, root, 3, 50, 3)
	root = insertRowsWithReset(t, bt, root, 51, 80, payload)

	bwd := countBackward(NewCursor(bt, root))
	if bwd < 1 {
		t.Error("expected rows to be accessible via backward traversal")
	}
}
