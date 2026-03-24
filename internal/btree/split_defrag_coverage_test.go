// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestDefrag_LeafSplitAfterFragmentation inserts rows to fill a page, deletes
// several to create fragmented free space, then inserts more rows to trigger a
// leaf split. The split calls redistributeLeafCells -> defragmentBothPages ->
// Defragment on both the left and right pages produced by the split.
func TestDefrag_LeafSplitAfterFragmentation(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	// 48-byte payload: enough to fill a 512-byte page quickly.
	payload := make([]byte, 48)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	// Phase 1: insert rows to build a multi-page tree.
	const phase1 = 40
	for i := int64(1); i <= phase1; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Phase 2: delete every other row in the lower half to fragment pages.
	for i := int64(2); i <= phase1/2; i += 2 {
		found, seekErr := cursor.SeekRowid(i)
		if seekErr == nil && found {
			cursor.Delete()
		}
		cursor = NewCursor(bt, cursor.RootPage)
	}

	// Phase 3: insert more rows so a leaf page fills up and splits again.
	// The leaf being split will have fragmented space from earlier deletions,
	// so defragmentBothPages must compact it during redistribution.
	const phase3Start = phase1 + 1
	const phase3End = phase1 + 30
	root = cursor.RootPage
	cursor = NewCursor(bt, root)
	for i := int64(phase3Start); i <= phase3End; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		cursor = NewCursor(bt, cursor.RootPage)
	}

	// Verify tree integrity: all remaining rows are accessible in order.
	scan := NewCursor(bt, cursor.RootPage)
	got := verifyOrderedForward(t, scan)
	if got < 1 {
		t.Error("expected at least one row after insert/delete/insert cycle")
	}
	t.Logf("rows present after fragmentation+split cycle: %d", got)
}

// TestDefrag_InteriorSplitAfterFragmentation builds a tree deep enough to
// trigger interior page splits after deleting rows to create fragmentation,
// exercising redistributeInteriorCells -> defragmentBothPages on interior pages.
func TestDefrag_InteriorSplitAfterFragmentation(t *testing.T) {
	t.Parallel()
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	// 18-byte payload: many rows fit per leaf, requiring many leaves before
	// interior pages overflow.
	payload := make([]byte, 18)
	for i := range payload {
		payload[i] = byte('b')
	}

	// Phase 1: insert enough rows for deep tree with multiple interior pages.
	const phase1 = 220
	for i := int64(1); i <= phase1; i++ {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	root = cursor.RootPage

	// Phase 2: delete a spread of rows to fragment interior and leaf pages.
	del := NewCursor(bt, root)
	for i := int64(10); i <= 100; i += 5 {
		found, seekErr := del.SeekRowid(i)
		if seekErr == nil && found {
			del.Delete()
		}
		del = NewCursor(bt, root)
	}

	// Phase 3: insert more rows to force additional interior splits on the
	// now-fragmented pages, triggering redistributeInteriorCells.
	ins := NewCursor(bt, root)
	const phase3Start = phase1 + 1
	const phase3End = phase1 + 80
	for i := int64(phase3Start); i <= phase3End; i++ {
		if err := ins.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		ins = NewCursor(bt, ins.RootPage)
	}

	// Verify the tree is intact and ordered.
	scan := NewCursor(bt, ins.RootPage)
	got := verifyOrderedForward(t, scan)
	if got < 1 {
		t.Error("expected at least one row after interior fragmentation+split")
	}
	t.Logf("rows present after interior fragmentation+split: %d", got)
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
	const pageSize = 512
	bt := NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)

	payload := make([]byte, 44)
	for i := range payload {
		payload[i] = byte('d')
	}

	// Insert descending so dividers always go at front of parent.
	const n = 50
	for i := int64(n); i >= 1; i-- {
		if err := cursor.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	root = cursor.RootPage

	// Delete every third row to create fragmentation.
	del := NewCursor(bt, root)
	for i := int64(3); i <= n; i += 3 {
		found, seekErr := del.SeekRowid(i)
		if seekErr == nil && found {
			del.Delete()
		}
		del = NewCursor(bt, root)
	}

	// Insert new rows (ascending) to trigger splits on fragmented pages.
	ins := NewCursor(bt, root)
	for i := int64(n + 1); i <= n+30; i++ {
		if err := ins.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		ins = NewCursor(bt, ins.RootPage)
	}

	// Verify backward traversal also works correctly after the mixed operations.
	scan := NewCursor(bt, ins.RootPage)
	bwd := countBackward(scan)
	if bwd < 1 {
		t.Error("expected rows to be accessible via backward traversal")
	}
	t.Logf("rows accessible backward: %d", bwd)
}
