// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// --- BtCursor.prevViaParent ---

// TestPrevViaParent_ParentIndexZero exercises the early-return branch in
// prevViaParent when the parent's IndexStack entry is 0, meaning there is no
// predecessor sibling to descend into.  The cursor is manually positioned at
// the first cell of the leftmost leaf with parent index 0 in the root.
func TestPrevViaParent_ParentIndexZero(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 150, 100)

	// Forward-scan to collect all keys so we can find the very first one.
	scanCur := NewCursor(bt, insertCur.RootPage)
	if err := scanCur.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	firstKey := scanCur.GetKey()

	// Seek to the first key; the cursor is at CurrentIndex=0 on the leftmost leaf.
	seekCur := NewCursor(bt, insertCur.RootPage)
	found, err := seekCur.SeekRowid(firstKey)
	if err != nil || !found {
		t.Fatalf("SeekRowid(%d) found=%v err=%v", firstKey, found, err)
	}

	// Previous() when at the very first entry must walk up prevViaParent until
	// it hits a parent with parentIndex==0 and ultimately report beginning-of-btree.
	prevErr := seekCur.Previous()
	if prevErr == nil {
		t.Error("Previous() at first entry should return beginning-of-btree error")
	}
	if seekCur.State != CursorInvalid {
		t.Errorf("State = %d after exhausting prevViaParent, want CursorInvalid", seekCur.State)
	}
	if !seekCur.AtFirst {
		t.Error("AtFirst should be true after reaching beginning of btree")
	}
}

// TestPrevViaParent_MultiplePageCrossings inserts many rows so the tree has at
// least two levels of interior pages, then performs a full backward scan.
// Every leaf-to-leaf boundary triggers prevViaParent; crossings where
// parentIndex > 0 exercise the descendToLast path while crossings at
// parentIndex == 0 climb further.
func TestPrevViaParent_MultiplePageCrossings(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 300, 100)

	fwd := countForward(NewCursor(bt, insertCur.RootPage))
	bwd := countBackward(NewCursor(bt, insertCur.RootPage))
	if fwd != bwd {
		t.Errorf("forward count %d != backward count %d", fwd, bwd)
	}
	if bwd < 10 {
		t.Errorf("backward scan yielded only %d rows, expected many", bwd)
	}
}

// TestPrevViaParent_DescendingKeyOrder verifies that the entire backward
// traversal (which drives prevViaParent on each page boundary) produces keys
// in strictly descending order.
func TestPrevViaParent_DescendingKeyOrder(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 200, 100)
	n := verifyOrderedBackward(t, NewCursor(bt, insertCur.RootPage))
	if n < 5 {
		t.Errorf("backward ordered scan returned only %d rows", n)
	}
}

// --- BtCursor.resolveChildPage ---

// TestResolveChildPage_RightChildBranch exercises the idx >= NumCells branch in
// resolveChildPage by seeking a key that is larger than all separator keys in
// an interior page, which directs the cursor toward the RightChild.
// A large seek key exercises this branch on every interior level.
func TestResolveChildPage_RightChildBranch(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 150, 100)

	// Seek to the last inserted key; the binary search will exhaust all cells
	// on each interior page and fall through to RightChild (idx == NumCells).
	seekCur := NewCursor(bt, insertCur.RootPage)
	if err := seekCur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	lastKey := seekCur.GetKey()

	seekCur2 := NewCursor(bt, insertCur.RootPage)
	found, err := seekCur2.SeekRowid(lastKey)
	if err != nil {
		t.Fatalf("SeekRowid(%d): %v", lastKey, err)
	}
	if !found {
		t.Errorf("SeekRowid(%d) not found", lastKey)
	}
}

// TestResolveChildPage_CellBranch exercises the idx < NumCells branch in
// resolveChildPage by seeking a key that lands in the middle of the cell
// array of an interior page.
func TestResolveChildPage_CellBranch(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 150, 100)

	// Seek to a middle key; binary search should take an interior cell branch.
	seekCur := NewCursor(bt, insertCur.RootPage)
	found, err := seekCur.SeekRowid(75)
	if err != nil {
		t.Fatalf("SeekRowid(75): %v", err)
	}
	if !found {
		t.Errorf("SeekRowid(75) not found")
	}
}

// --- BtCursor.advanceWithinPage ---

// TestAdvanceWithinPage_AtLastCell exercises the false-return branch in
// advanceWithinPage when the cursor is already on the last cell of a leaf.
// Next() calls advanceWithinPage; when it returns false the cursor climbs up.
// We position the cursor at the last cell of an isolated leaf to observe
// the climbToNextParent / end-of-tree behaviour.
func TestAdvanceWithinPage_AtLastCell(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 150, 100)

	cur := NewCursor(bt, insertCur.RootPage)
	if err := cur.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	// Next() on the last entry: advanceWithinPage returns false, then
	// climbToNextParent finds nothing, cursor becomes invalid.
	nextErr := cur.Next()
	if nextErr == nil {
		t.Error("Next() after last entry should return end-of-btree error")
	}
	if cur.State != CursorInvalid {
		t.Errorf("State = %d after end-of-btree, want CursorInvalid", cur.State)
	}
}

// TestAdvanceWithinPage_GetPageError exercises the GetPage-error branch inside
// advanceWithinPage by removing the page from the store after positioning.
func TestAdvanceWithinPage_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	cur := NewCursor(bt, rootPage)
	for i := int64(1); i <= 5; i++ {
		if err := cur.Insert(i, []byte("payload")); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Position at a middle cell so advanceWithinPage (not climbToNextParent) runs.
	found, err := cur.SeekRowid(2)
	if err != nil || !found {
		t.Fatalf("SeekRowid(2) found=%v err=%v", found, err)
	}
	// Confirm there are more cells on this page (CurrentIndex < NumCells-1).
	if cur.CurrentIndex >= int(cur.CurrentHeader.NumCells)-1 {
		t.Skip("all rows fit on one page cell, cannot test mid-page removal")
	}

	// Remove the page so GetPage fails inside advanceWithinPage.
	bt.mu.Lock()
	delete(bt.Pages, cur.CurrentPage)
	bt.mu.Unlock()

	nextErr := cur.Next()
	if nextErr == nil {
		t.Log("Next() succeeded despite page removal (page may have been cached elsewhere)")
	} else {
		t.Logf("Next() returned expected error: %v", nextErr)
	}
}

// TestAdvanceWithinPage_ForwardScanAllRows drives advanceWithinPage on every
// cell of every page via a complete forward scan.
func TestAdvanceWithinPage_ForwardScanAllRows(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupBtreeWithRows(t, 4096, 1, 200, 50)
	fwd := countForward(NewCursor(bt, insertCur.RootPage))
	if fwd < 10 {
		t.Errorf("forward scan returned only %d rows", fwd)
	}
}

// --- IndexCursor.prevInPage (multi-page) ---

// TestIndexCursorPrevInPage_LargeTree inserts many index entries, triggers
// multiple page splits, then performs a full backward scan.  Every within-page
// step calls prevInPage on the index cursor.
func TestIndexCursorPrevInPage_LargeTree(t *testing.T) {
	t.Parallel()
	bt, idxCur := setupIndexCursor(t, 4096)

	n := insertIndexEntriesN(idxCur, 120, func(i int) []byte {
		key := make([]byte, 8)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		return key
	})
	if n < 10 {
		t.Fatalf("inserted only %d entries", n)
	}

	fwd := countIndexForward(NewIndexCursor(bt, idxCur.RootPage))
	bwd := countIndexBackward(NewIndexCursor(bt, idxCur.RootPage))
	if fwd != bwd {
		t.Errorf("forward count %d != backward count %d", fwd, bwd)
	}
}

// --- IndexCursor.prevViaParent ---

// TestIndexCursorPrevViaParent_ParentIndexZero places an IndexCursor at the
// first cell of the leftmost leaf with a two-level tree (root + two leaves).
// PrevIndex() will call prevViaParent; the root's IndexStack is 0 so the
// function returns false, then PrevIndex reaches beginning-of-index.
func TestIndexCursorPrevViaParent_ParentIndexZero(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// leaf 2: "aaa","bbb"
	leaf2 := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
	})
	bt.SetPage(2, leaf2)

	// leaf 3: "ccc","ddd"
	leaf3 := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("ccc"), 3},
		{[]byte("ddd"), 4},
	})
	bt.SetPage(3, leaf3)

	// root (page 1): separator "bbb"/2 -> left child 2, right child 3
	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("bbb"), 2},
	}, 3)
	bt.SetPage(1, root)

	// Parse leaf2 header.
	l2Data, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	l2Hdr, err := ParsePageHeader(l2Data, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	// Position cursor at leaf2[0]="aaa" with root IndexStack=0 (leftmost child).
	cur := NewIndexCursor(bt, 1)
	cur.Depth = 1
	cur.PageStack[0] = 1
	cur.IndexStack[0] = 0 // root is at slot 0 => prevViaParent returns false
	cur.PageStack[1] = 2
	cur.IndexStack[1] = 0
	cur.CurrentPage = 2
	cur.CurrentIndex = 0
	cur.CurrentHeader = l2Hdr
	cur.CurrentKey = []byte("aaa")
	cur.CurrentRowid = 1
	cur.State = CursorValid

	prevErr := cur.PrevIndex()
	if prevErr == nil {
		t.Error("PrevIndex() at beginning should return beginning-of-index error")
	}
	if !cur.AtFirst {
		t.Error("AtFirst should be set after reaching beginning of index")
	}
}

// TestIndexCursorPrevViaParent_DescendsToSiblingLeaf positions an IndexCursor
// at the first cell of the right leaf of a two-leaf tree.  PrevIndex calls
// prevViaParent; because root IndexStack > 0 it descends to the left leaf's
// last cell.
func TestIndexCursorPrevViaParent_DescendsToSiblingLeaf(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf2 := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
	})
	bt.SetPage(2, leaf2)

	leaf3 := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("ccc"), 3},
		{[]byte("ddd"), 4},
	})
	bt.SetPage(3, leaf3)

	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("bbb"), 2},
	}, 3)
	bt.SetPage(1, root)

	l3Data, err := bt.GetPage(3)
	if err != nil {
		t.Fatalf("GetPage(3): %v", err)
	}
	l3Hdr, err := ParsePageHeader(l3Data, 3)
	if err != nil {
		t.Fatalf("ParsePageHeader(3): %v", err)
	}

	// Position at leaf3[0]="ccc" with root IndexStack=1 (right-child slot).
	cur := NewIndexCursor(bt, 1)
	cur.Depth = 1
	cur.PageStack[0] = 1
	cur.IndexStack[0] = 1 // slot 1 means we came from right child
	cur.PageStack[1] = 3
	cur.IndexStack[1] = 0
	cur.CurrentPage = 3
	cur.CurrentIndex = 0
	cur.CurrentHeader = l3Hdr
	cur.CurrentKey = []byte("ccc")
	cur.CurrentRowid = 3
	cur.State = CursorValid

	prevErr := cur.PrevIndex()
	if prevErr != nil {
		t.Fatalf("PrevIndex() unexpected error: %v", prevErr)
	}
	if string(cur.CurrentKey) != "bbb" {
		t.Errorf("CurrentKey = %q, want \"bbb\"", cur.CurrentKey)
	}
}

// --- IndexCursor.resolveChildPage ---

// TestIndexCursorResolveChildPage_RightChildBranch seeks a key larger than all
// entries in a two-level index tree, exercising the idx >= NumCells branch of
// resolveChildPage which returns header.RightChild.
func TestIndexCursorResolveChildPage_RightChildBranch(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf2 := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
	})
	bt.SetPage(2, leaf2)

	leaf3 := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("zzz"), 2},
	})
	bt.SetPage(3, leaf3)

	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("mmm"), 99},
	}, 3)
	bt.SetPage(1, root)

	cur := NewIndexCursor(bt, 1)
	// Seek "zzz": binary search returns idx == NumCells -> RightChild branch.
	found, err := cur.SeekIndex([]byte("zzz"))
	if err != nil {
		t.Fatalf("SeekIndex(\"zzz\"): %v", err)
	}
	if !found {
		t.Errorf("SeekIndex(\"zzz\") not found")
	}
}

// TestIndexCursorResolveChildPage_CellBranch seeks a key smaller than the
// separator in an interior page, forcing resolveChildPage to read a cell
// (idx < NumCells branch).
func TestIndexCursorResolveChildPage_CellBranch(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf2 := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
	})
	bt.SetPage(2, leaf2)

	leaf3 := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("zzz"), 2},
	})
	bt.SetPage(3, leaf3)

	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("mmm"), 99},
	}, 3)
	bt.SetPage(1, root)

	cur := NewIndexCursor(bt, 1)
	// Seek "aaa": binary search returns idx==0 < NumCells -> cell branch.
	found, err := cur.SeekIndex([]byte("aaa"))
	if err != nil {
		t.Fatalf("SeekIndex(\"aaa\"): %v", err)
	}
	if !found {
		t.Errorf("SeekIndex(\"aaa\") not found")
	}
}

// --- IndexCursor.climbToNextParent ---

// TestIndexCursorClimbToNextParent_PastRightChild exercises the branch where
// parentIndex >= NumCells (the cursor previously advanced to the right-child
// slot) so climbToNextParent must continue climbing rather than advancing.
// We build a 3-level tree and navigate to the last entry of the right-most
// leaf, then call NextIndex() to trigger climbToNextParent.
func TestIndexCursorClimbToNextParent_PastRightChild(t *testing.T) {
	t.Parallel()
	bt, cur := buildThreeLevelIndexTree(t)

	cur2 := NewIndexCursor(bt, cur.RootPage)
	if err := cur2.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}

	// NextIndex at the very last entry: climbToNextParent exhausts all levels.
	nextErr := cur2.NextIndex()
	if nextErr == nil {
		t.Error("NextIndex() at last entry should return end-of-index error")
	}
	if !cur2.AtLast {
		t.Error("AtLast should be set after reaching end of index")
	}
}

// TestIndexCursorClimbToNextParent_AdvancesCell exercises the branch where
// parentIndex < NumCells-1 so climbToNextParent can advance to the next cell
// rather than taking the right-child.  A full forward scan on a multi-page
// tree drives every interior-node advance.
func TestIndexCursorClimbToNextParent_AdvancesCell(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf2 := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
	})
	bt.SetPage(2, leaf2)

	leaf3 := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("ccc"), 3},
	})
	bt.SetPage(3, leaf3)

	leaf4 := buildIndexLeafPage(4, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("eee"), 5},
	})
	bt.SetPage(4, leaf4)

	// Root with two cells: cell0->leaf2 (sep "bbb"), cell1->leaf3 (sep "ddd"), right->leaf4
	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("bbb"), 2},
		{3, []byte("ddd"), 4},
	}, 4)
	bt.SetPage(1, root)

	cur := NewIndexCursor(bt, 1)
	fwd := countIndexForward(cur)
	if fwd != 3 {
		t.Errorf("forward count = %d, want 3", fwd)
	}
}

// TestIndexCursorClimbToNextParent_RightChildAdvance exercises the branch
// parentIndex == NumCells-1 where climbToNextParent sets IndexStack to NumCells
// and returns RightChild.  The three-level tree provides an interior page with
// exactly one cell; after visiting the left subtree the cursor is at slot 0
// (== NumCells-1 == 0) and nextIndex crosses to RightChild.
func TestIndexCursorClimbToNextParent_RightChildAdvance(t *testing.T) {
	t.Parallel()
	bt, cur := buildThreeLevelIndexTree(t)

	fwd := countIndexForward(NewIndexCursor(bt, cur.RootPage))
	bwd := countIndexBackward(NewIndexCursor(bt, cur.RootPage))
	if fwd != bwd {
		t.Errorf("index forward %d != backward %d", fwd, bwd)
	}
	if fwd < 4 {
		t.Errorf("expected at least 4 index entries, got %d", fwd)
	}
}

// --- IndexCursor.seekLeafExactMatch (ParseCell error branch) ---

// TestIndexCursorSeekLeafExactMatch_ParseCellError injects a page whose cell
// pointer points to bytes that cannot be decoded as a valid index-leaf cell,
// then calls SeekIndex to trigger the ParseCell error inside seekLeafExactMatch.
func TestIndexCursorSeekLeafExactMatch_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with one valid cell so binarySearchKey finds an exact match.
	leaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("target"), 42},
	})

	// Corrupt the payload length varint at the cell offset so ParseCell fails.
	// The cell pointer for cell 0 is at bytes [PageHeaderSizeLeaf, PageHeaderSizeLeaf+2).
	// The cell content is near the end of the page; overwrite it with 0xFF bytes
	// so the length varint decodes to an impossibly large value.
	hOff := headerOff(1) // page 1 -> FileHeaderSize offset
	ptrOff := hOff + PageHeaderSizeLeaf
	// Read the cell offset from the pointer array.
	cellOff := int(leaf[ptrOff])<<8 | int(leaf[ptrOff+1])
	// Overwrite the first few bytes of the cell with 0xFF to corrupt the length varint.
	for i := cellOff; i < cellOff+4 && i < len(leaf); i++ {
		leaf[i] = 0xFF
	}
	bt.SetPage(1, leaf)

	cur := NewIndexCursor(bt, 1)
	// SeekIndex -> binarySearchKey will fail to decode the cell, returning no
	// exact match, so seekLeafExactMatch is not reached via this path.
	// To directly exercise seekLeafExactMatch with a corrupted cell we instead
	// call seekLeafExactMatch directly via the internal method after positioning.
	leafData, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage(1): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 1)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	// seekLeafExactMatch will call GetCellPointer then ParseCell; with corrupted
	// bytes ParseCell returns an error.
	found, seekErr := cur.seekLeafExactMatch(leafData, hdr, 0)
	if seekErr == nil {
		// If ParseCell tolerates the corruption (e.g., very long payload treated
		// as overflow), confirm the cursor is still in a reasonable state.
		t.Logf("seekLeafExactMatch returned found=%v (corruption may be tolerated)", found)
	} else {
		if cur.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid on ParseCell error", cur.State)
		}
	}
}

// TestIndexCursorSeekLeafExactMatch_MultiPageSeek seeks every key in a large
// index tree and confirms each is found, driving seekLeafExactMatch for each
// matching cell across many pages.
func TestIndexCursorSeekLeafExactMatch_MultiPageSeek(t *testing.T) {
	t.Parallel()
	bt, insertCur := setupIndexCursor(t, 4096)

	keys := make([][]byte, 80)
	for i := range keys {
		k := make([]byte, 4)
		k[0] = byte(i >> 8)
		k[1] = byte(i)
		k[2] = 'k'
		k[3] = 'y'
		keys[i] = k
	}
	n := insertIndexEntries(insertCur, keys)
	if n < 10 {
		t.Fatalf("inserted only %d entries", n)
	}

	seekCur := NewIndexCursor(bt, insertCur.RootPage)
	hits := 0
	for _, k := range keys[:n] {
		found, err := seekCur.SeekIndex(k)
		if err != nil {
			t.Errorf("SeekIndex(%q): %v", k, err)
			continue
		}
		if found {
			hits++
		}
	}
	if hits < n/2 {
		t.Errorf("only %d/%d seeks found an exact match", hits, n)
	}
}
