// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// buildForwardTree builds a two-level IndexCursor tree used by several tests:
//
//	root (interior, page 1)
//	  cell[0]: childPage=2, separator key "bbb"/rowid=2
//	  rightChild: page 3
//	leaf page 2: "aaa"/1, "bbb"/2
//	leaf page 3: "ccc"/3, "ddd"/4
//
// The cursor is returned positioned at the tree root.
func buildForwardTree(t *testing.T) (*Btree, *IndexCursor) {
	t.Helper()
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

	return bt, NewIndexCursor(bt, 1)
}

// --- advanceWithinPage ---

// TestIndexFwdCoverage_AdvanceWithinPage_MultiCell verifies that NextIndex
// correctly advances within a multi-cell leaf (exercises the advanceWithinPage
// success path for cells 0→1→end).
func TestIndexFwdCoverage_AdvanceWithinPage_MultiCell(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 10},
		{[]byte("bbb"), 20},
		{[]byte("ccc"), 30},
	})
	bt.SetPage(1, leaf)

	c := NewIndexCursor(bt, 1)
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	want := []struct {
		key   string
		rowid int64
	}{{"aaa", 10}, {"bbb", 20}, {"ccc", 30}}
	for i, w := range want {
		if !c.IsValid() {
			t.Fatalf("step %d: cursor not valid", i)
		}
		if string(c.GetKey()) != w.key {
			t.Errorf("step %d: key=%q want %q", i, c.GetKey(), w.key)
		}
		if c.GetRowid() != w.rowid {
			t.Errorf("step %d: rowid=%d want %d", i, c.GetRowid(), w.rowid)
		}
		if i < len(want)-1 {
			if err := c.NextIndex(); err != nil {
				t.Fatalf("step %d: NextIndex: %v", i, err)
			}
		}
	}
}

// TestIndexFwdCoverage_AdvanceWithinPage_GetPageError verifies that
// advanceWithinPage propagates a GetPage error when the current page has
// been removed from the store.
func TestIndexFwdCoverage_AdvanceWithinPage_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("xxx"), 1},
		{[]byte("yyy"), 2},
	})
	bt.SetPage(2, leaf)

	leafData, _ := bt.GetPage(2)
	hdr, _ := ParsePageHeader(leafData, 2)

	c := manualIndexCursorOnLeaf(bt, 2, 0, hdr)

	// Remove the page so GetPage fails inside advanceWithinPage.
	delete(bt.Pages, 2)

	err := c.NextIndex()
	if err == nil {
		t.Fatal("NextIndex() expected error when page is missing, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// --- climbToNextParent ---

// TestIndexFwdCoverage_ClimbToNextParent_CrossBoundary verifies that NextIndex
// correctly climbs from the last cell of a leaf back up to the parent and then
// descends into the right-child leaf (exercises climbToNextParent success path
// and the parentIndex < NumCells-1 branch).
func TestIndexFwdCoverage_ClimbToNextParent_CrossBoundary(t *testing.T) {
	t.Parallel()
	bt, cursor := buildForwardTree(t)
	_ = bt

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	want := []string{"aaa", "bbb", "ccc", "ddd"}
	for i, w := range want {
		if !cursor.IsValid() {
			t.Fatalf("step %d: cursor not valid", i)
		}
		if string(cursor.GetKey()) != w {
			t.Errorf("step %d: key=%q want %q", i, cursor.GetKey(), w)
		}
		if i < len(want)-1 {
			if err := cursor.NextIndex(); err != nil {
				t.Fatalf("step %d: NextIndex: %v", i, err)
			}
		}
	}
	// One more NextIndex should exhaust the tree.
	if err := cursor.NextIndex(); err == nil {
		t.Error("NextIndex() past end should return error")
	}
}

// TestIndexFwdCoverage_ClimbToNextParent_RightChildSlot verifies the path
// where parentIndex == NumCells-1, which advances to the right child
// (rightChild != 0 branch in climbToNextParent).
//
// Tree layout: root has two separator cells (3 children), so after exhausting
// the middle leaf the climb step at index==NumCells-1 takes the right-child branch.
func TestIndexFwdCoverage_ClimbToNextParent_RightChildSlot(t *testing.T) {
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

	// Root has two cells: [child=2 | "aaa"] and [child=3 | "ccc"], rightChild=4.
	root := buildIndexInteriorPage(1, 4096, []struct {
		childPage uint32
		key       []byte
		rowid     int64
	}{
		{2, []byte("aaa"), 1},
		{3, []byte("ccc"), 3},
	}, 4)
	bt.SetPage(1, root)

	c := NewIndexCursor(bt, 1)
	if err := c.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	want := []string{"aaa", "ccc", "eee"}
	for i, w := range want {
		if !c.IsValid() {
			t.Fatalf("step %d: cursor not valid", i)
		}
		if string(c.GetKey()) != w {
			t.Errorf("step %d: key=%q want %q", i, c.GetKey(), w)
		}
		if i < len(want)-1 {
			if err := c.NextIndex(); err != nil {
				t.Fatalf("step %d: NextIndex: %v", i, err)
			}
		}
	}
}

// TestIndexFwdCoverage_ClimbToNextParent_ParentAlreadyPastCells exercises the
// "parentIndex >= NumCells" continue branch in climbToNextParent.  We manually
// position the cursor at the last cell of the right-child leaf of the root so
// that the root's IndexStack entry is already == NumCells (past all separators).
// Climbing should detect that and continue upward, eventually exhausting all
// ancestors and returning not-found.
func TestIndexFwdCoverage_ClimbToNextParent_ParentAlreadyPastCells(t *testing.T) {
	t.Parallel()
	bt, _ := buildForwardTree(t)

	// Get leaf3 header.
	l3Data, err := bt.GetPage(3)
	if err != nil {
		t.Fatalf("GetPage(3): %v", err)
	}
	l3Hdr, err := ParsePageHeader(l3Data, 3)
	if err != nil {
		t.Fatalf("ParsePageHeader(3): %v", err)
	}

	// Manually position cursor on last cell of leaf3 ("ddd", index 1).
	// Set root IndexStack[0] = NumCells of root (1) so it looks like we've
	// already consumed the right-child slot — climbToNextParent will see
	// parentIndex (1) >= NumCells (1) and continue.
	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1 // == root.NumCells == 1, triggers "past cells" branch
	c.PageStack[1] = 3
	c.IndexStack[1] = 1
	c.CurrentPage = 3
	c.CurrentIndex = 1
	c.CurrentHeader = l3Hdr
	c.CurrentKey = []byte("ddd")
	c.CurrentRowid = 4
	c.State = CursorValid

	// advanceWithinPage will fail (index 1 is last cell), so climbToNextParent
	// is called; root IndexStack == NumCells triggers the continue, depth
	// falls to 0 and loop exits with not-found → NextIndex returns error.
	err = c.NextIndex()
	if err == nil {
		t.Error("NextIndex() at end of right-most leaf should return error")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid after end-of-index", c.State)
	}
}

// TestIndexFwdCoverage_ClimbToNextParent_GetPageError verifies that
// climbToNextParent propagates a GetPage error on the parent page.
func TestIndexFwdCoverage_ClimbToNextParent_GetPageError(t *testing.T) {
	t.Parallel()
	bt, _ := buildForwardTree(t)

	l2Data, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	l2Hdr, err := ParsePageHeader(l2Data, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 0 // parent index 0; climb will try to read page 1
	c.PageStack[1] = 2
	c.IndexStack[1] = 1 // last cell of leaf2
	c.CurrentPage = 2
	c.CurrentIndex = 1
	c.CurrentHeader = l2Hdr
	c.CurrentKey = []byte("bbb")
	c.CurrentRowid = 2
	c.State = CursorValid

	// Remove the parent page so GetPage fails inside climbToNextParent.
	delete(bt.Pages, 1)

	err = c.NextIndex()
	if err == nil {
		t.Fatal("NextIndex() expected error when parent page missing, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// --- resolveChildPage ---

// TestIndexFwdCoverage_ResolveChildPage_RightChild verifies that SeekIndex
// takes the right-child branch in resolveChildPage when the search key is
// greater than all separator cells on an interior page.
func TestIndexFwdCoverage_ResolveChildPage_RightChild(t *testing.T) {
	t.Parallel()
	bt, cursor := buildForwardTree(t)
	_ = bt

	// "zzz" > "bbb" (the only separator), so idx returned by binarySearchKey
	// equals NumCells (1) ≥ NumCells, which triggers the right-child branch.
	found, err := cursor.SeekIndex([]byte("zzz"))
	if err != nil {
		t.Fatalf("SeekIndex(zzz): %v", err)
	}
	if found {
		t.Error("SeekIndex(zzz): expected found=false for non-existent key")
	}
	if !cursor.IsValid() {
		t.Error("cursor should be valid after seeking past all keys")
	}
}

// TestIndexFwdCoverage_ResolveChildPage_ExactSeparator verifies that SeekIndex
// correctly resolves the child page pointed to by a separator cell (idx < NumCells).
func TestIndexFwdCoverage_ResolveChildPage_ExactSeparator(t *testing.T) {
	t.Parallel()
	bt, cursor := buildForwardTree(t)
	_ = bt

	// "aaa" < separator "bbb", so idx=0 < NumCells(1): resolveChildPage reads
	// the cell and returns its child page.
	found, err := cursor.SeekIndex([]byte("aaa"))
	if err != nil {
		t.Fatalf("SeekIndex(aaa): %v", err)
	}
	if !found {
		t.Error("SeekIndex(aaa): expected found=true")
	}
	if string(cursor.GetKey()) != "aaa" {
		t.Errorf("GetKey()=%q want 'aaa'", cursor.GetKey())
	}
}

// --- descendToRightChild ---

// TestIndexFwdCoverage_DescendToRightChild_NoRightChild verifies that
// descendToRightChild returns an error when the interior page has
// RightChild == 0 (corrupted or empty right-child pointer).
func TestIndexFwdCoverage_DescendToRightChild_NoRightChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build an interior page with RightChild = 0.
	data := make([]byte, 4096)
	data[PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(data[PageHeaderOffsetRightChild:], 0) // no right child
	binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], 4096)
	bt.SetPage(2, data)

	hdr, err := ParsePageHeader(data, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}

	c := NewIndexCursor(bt, 2)
	c.Depth = 0
	c.State = CursorValid

	_, err = c.descendToRightChild(2, hdr)
	if err == nil {
		t.Fatal("descendToRightChild() expected error for RightChild==0, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexFwdCoverage_DescendToRightChild_DepthExceeded verifies that
// descendToRightChild returns an error when depth would exceed MaxBtreeDepth.
func TestIndexFwdCoverage_DescendToRightChild_DepthExceeded(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("k"), 1},
	})
	bt.SetPage(2, leaf)

	data := make([]byte, 4096)
	data[PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(data[PageHeaderOffsetRightChild:], 2)
	binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], 4096)
	bt.SetPage(3, data)

	hdr, err := ParsePageHeader(data, 3)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}

	c := NewIndexCursor(bt, 3)
	// Set depth to MaxBtreeDepth-1 so the next increment hits the limit.
	c.Depth = MaxBtreeDepth - 1
	c.State = CursorValid

	_, err = c.descendToRightChild(3, hdr)
	if err == nil {
		t.Fatal("descendToRightChild() expected depth-exceeded error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexFwdCoverage_DescendToRightChild_Success verifies the happy path:
// MoveToLast on a two-level tree calls descendToRightChild to reach the
// right-child leaf and positions the cursor at the last key there.
func TestIndexFwdCoverage_DescendToRightChild_Success(t *testing.T) {
	t.Parallel()
	_, cursor := buildForwardTree(t)

	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if !cursor.IsValid() {
		t.Fatal("cursor not valid after MoveToLast")
	}
	if string(cursor.GetKey()) != "ddd" {
		t.Errorf("GetKey()=%q want 'ddd'", cursor.GetKey())
	}
	if cursor.GetRowid() != 4 {
		t.Errorf("GetRowid()=%d want 4", cursor.GetRowid())
	}
}

// TestIndexFwdCoverage_BackwardFromLast verifies that MoveToLast followed by
// repeated PrevIndex visits all keys in reverse across page boundaries.
func TestIndexFwdCoverage_BackwardFromLast(t *testing.T) {
	t.Parallel()
	_, cursor := buildForwardTree(t)

	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}

	want := []string{"ddd", "ccc", "bbb", "aaa"}
	for i, w := range want {
		if !cursor.IsValid() {
			t.Fatalf("step %d: cursor not valid", i)
		}
		if string(cursor.GetKey()) != w {
			t.Errorf("step %d: key=%q want %q", i, cursor.GetKey(), w)
		}
		if i < len(want)-1 {
			if err := cursor.PrevIndex(); err != nil {
				t.Fatalf("step %d: PrevIndex: %v", i, err)
			}
		}
	}
}

// TestIndexFwdCoverage_SeekAndIterate exercises resolveChildPage on interior
// pages during SeekIndex, then verifies the cursor can iterate forward from
// the found position.
func TestIndexFwdCoverage_SeekAndIterate(t *testing.T) {
	t.Parallel()
	_, cursor := buildForwardTree(t)

	found, err := cursor.SeekIndex([]byte("bbb"))
	if err != nil {
		t.Fatalf("SeekIndex(bbb): %v", err)
	}
	if !found {
		t.Fatal("SeekIndex(bbb): expected found=true")
	}
	if string(cursor.GetKey()) != "bbb" {
		t.Errorf("GetKey()=%q want 'bbb'", cursor.GetKey())
	}

	// Advance past "bbb" — crosses the page boundary via climbToNextParent.
	if err := cursor.NextIndex(); err != nil {
		t.Fatalf("NextIndex after seek: %v", err)
	}
	if !cursor.IsValid() {
		t.Fatal("cursor not valid after crossing page boundary")
	}
	if string(cursor.GetKey()) != "ccc" {
		t.Errorf("GetKey()=%q want 'ccc'", cursor.GetKey())
	}
}
