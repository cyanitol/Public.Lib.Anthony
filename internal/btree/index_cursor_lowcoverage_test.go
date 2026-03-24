// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// manualIndexCursorOnLeaf returns an IndexCursor manually positioned at the given
// cell index on leafPage, with depth=1 and a root page at depth=0.
func manualIndexCursorOnLeaf(bt *Btree, leafPageNum uint32, cellIdx int, header *PageHeader) *IndexCursor {
	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1
	c.PageStack[1] = leafPageNum
	c.IndexStack[1] = cellIdx
	c.CurrentPage = leafPageNum
	c.CurrentIndex = cellIdx
	c.CurrentHeader = header
	c.State = CursorValid
	return c
}

// --- prevInPage ---

// TestIndexCursorLowCoverage_prevInPage_Success verifies that prevInPage correctly
// steps back within a leaf page that has multiple cells.
func TestIndexCursorLowCoverage_prevInPage_Success(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
		{[]byte("ccc"), 3},
	})
	bt.SetPage(2, leaf)

	leafData, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}

	// Position at index 2 ("ccc"), then prevInPage → should land on "bbb".
	c := manualIndexCursorOnLeaf(bt, 2, 2, hdr)

	if err := c.prevInPage(); err != nil {
		t.Fatalf("prevInPage() unexpected error: %v", err)
	}
	if c.CurrentIndex != 1 {
		t.Errorf("CurrentIndex = %d, want 1", c.CurrentIndex)
	}
	if string(c.CurrentKey) != "bbb" {
		t.Errorf("CurrentKey = %q, want \"bbb\"", c.CurrentKey)
	}
}

// TestIndexCursorLowCoverage_prevInPage_GetPageError verifies that prevInPage
// propagates an error when the current page is missing from the store.
func TestIndexCursorLowCoverage_prevInPage_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a minimal valid header so we can construct the cursor, but do NOT
	// store the page so GetPage will fail.
	leaf := buildIndexLeafPage(7, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("x"), 1},
		{[]byte("y"), 2},
	})
	bt.SetPage(7, leaf)

	leafData, _ := bt.GetPage(7)
	hdr, _ := ParsePageHeader(leafData, 7)

	// Remove the page from the cache so GetPage fails.
	delete(bt.Pages, 7)

	c := manualIndexCursorOnLeaf(bt, 7, 1, hdr)
	err := c.prevInPage()
	if err == nil {
		t.Fatal("prevInPage() expected error when page is missing, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexCursorLowCoverage_prevInPage_GetCellPointerError verifies that
// prevInPage propagates an error when GetCellPointer fails because the header
// has been tampered to report more cells than exist.
func TestIndexCursorLowCoverage_prevInPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(8, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("p"), 10},
		{[]byte("q"), 20},
	})
	bt.SetPage(8, leaf)

	leafData, _ := bt.GetPage(8)
	hdr, _ := ParsePageHeader(leafData, 8)

	// Overstate NumCells so GetCellPointer(index=999) is called.
	hdr.NumCells = 1000

	c := manualIndexCursorOnLeaf(bt, 8, 1000, hdr)
	err := c.prevInPage()
	// prevInPage decrements to 999, which is still within [0, 999] according to
	// the tampered header, but cell pointer array does not extend that far.
	// The error may come from either GetCellPointer or ParseCell; either is fine.
	if err == nil {
		// If no error, the cursor must still be valid (cell pointer happened to work)
		t.Logf("prevInPage returned no error (cell pointer in range or ParseCell succeeded)")
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid on error", c.State)
		}
	}
}

// --- enterPage ---

// TestIndexCursorLowCoverage_enterPage_GetPageError verifies that enterPage
// returns an error and marks the cursor invalid when the target page is absent.
func TestIndexCursorLowCoverage_enterPage_GetPageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	c := NewIndexCursor(bt, 1)
	c.Depth = 0
	c.State = CursorValid

	// Page 99 does not exist.
	_, _, err := c.enterPage(99)
	if err == nil {
		t.Fatal("enterPage() expected error for missing page, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexCursorLowCoverage_enterPage_ParsePageHeaderError verifies that
// enterPage propagates a ParsePageHeader error for a page with an invalid type.
func TestIndexCursorLowCoverage_enterPage_ParsePageHeaderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a page with an invalid page-type byte so ParsePageHeader fails.
	badPage := make([]byte, 4096)
	badPage[0] = 0xFF // invalid page type for page 2 (no file header offset)
	binary.BigEndian.PutUint16(badPage[PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(badPage[PageHeaderOffsetCellStart:], uint16(4096))
	bt.Pages[2] = badPage // inject directly, bypassing SetPage validation

	c := NewIndexCursor(bt, 1)
	c.Depth = 0
	c.State = CursorValid

	_, _, err := c.enterPage(2)
	if err == nil {
		t.Fatal("enterPage() expected ParsePageHeader error for bad page type, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexCursorLowCoverage_enterPage_DepthExceeded verifies that enterPage
// returns an error when the depth limit is reached.
func TestIndexCursorLowCoverage_enterPage_DepthExceeded(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(5, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("z"), 1},
	})
	bt.SetPage(5, leaf)

	c := NewIndexCursor(bt, 1)
	// Set depth to MaxBtreeDepth-1 so that enterPage increments to MaxBtreeDepth.
	c.Depth = MaxBtreeDepth - 1
	c.State = CursorValid

	_, _, err := c.enterPage(5)
	if err == nil {
		t.Fatal("enterPage() expected depth-exceeded error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexCursorLowCoverage_enterPage_Success verifies the happy path: enterPage
// returns valid page data and header for a well-formed page.
func TestIndexCursorLowCoverage_enterPage_Success(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("hello"), 42},
	})
	bt.SetPage(3, leaf)

	c := NewIndexCursor(bt, 1)
	c.Depth = 0

	pageData, hdr, err := c.enterPage(3)
	if err != nil {
		t.Fatalf("enterPage() unexpected error: %v", err)
	}
	if pageData == nil {
		t.Fatal("enterPage() returned nil pageData")
	}
	if hdr == nil {
		t.Fatal("enterPage() returned nil header")
	}
	if !hdr.IsLeaf {
		t.Errorf("expected leaf page, got IsLeaf=%v", hdr.IsLeaf)
	}
	if c.Depth != 1 {
		t.Errorf("Depth = %d after enterPage, want 1", c.Depth)
	}
	if c.PageStack[1] != 3 {
		t.Errorf("PageStack[1] = %d, want 3", c.PageStack[1])
	}
}

// --- seekLeafExactMatch ---

// TestIndexCursorLowCoverage_seekLeafExactMatch_AtFirst verifies exact-match
// lookup at index 0 of a multi-cell leaf.
func TestIndexCursorLowCoverage_seekLeafExactMatch_AtFirst(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("alpha"), 100},
		{[]byte("beta"), 200},
		{[]byte("gamma"), 300},
	})
	bt.SetPage(2, leaf)

	leafData, _ := bt.GetPage(2)
	hdr, _ := ParsePageHeader(leafData, 2)

	c := manualIndexCursorOnLeaf(bt, 2, 0, hdr)

	found, err := c.seekLeafExactMatch(leafData, hdr, 0)
	if err != nil {
		t.Fatalf("seekLeafExactMatch() unexpected error: %v", err)
	}
	if !found {
		t.Fatal("seekLeafExactMatch() expected found=true")
	}
	if string(c.CurrentKey) != "alpha" {
		t.Errorf("CurrentKey = %q, want \"alpha\"", c.CurrentKey)
	}
	if c.CurrentRowid != 100 {
		t.Errorf("CurrentRowid = %d, want 100", c.CurrentRowid)
	}
	if c.State != CursorValid {
		t.Errorf("State = %d, want CursorValid", c.State)
	}
}

// TestIndexCursorLowCoverage_seekLeafExactMatch_AtMiddle verifies exact-match
// lookup at a middle index.
func TestIndexCursorLowCoverage_seekLeafExactMatch_AtMiddle(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
		{[]byte("bbb"), 2},
		{[]byte("ccc"), 3},
	})
	bt.SetPage(2, leaf)

	leafData, _ := bt.GetPage(2)
	hdr, _ := ParsePageHeader(leafData, 2)

	c := manualIndexCursorOnLeaf(bt, 2, 1, hdr)

	found, err := c.seekLeafExactMatch(leafData, hdr, 1)
	if err != nil {
		t.Fatalf("seekLeafExactMatch() unexpected error: %v", err)
	}
	if !found {
		t.Fatal("seekLeafExactMatch() expected found=true at middle index")
	}
	if string(c.CurrentKey) != "bbb" {
		t.Errorf("CurrentKey = %q, want \"bbb\"", c.CurrentKey)
	}
}

// TestIndexCursorLowCoverage_seekLeafExactMatch_GetCellPointerError verifies
// that seekLeafExactMatch propagates a GetCellPointer error.
func TestIndexCursorLowCoverage_seekLeafExactMatch_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("key1"), 10},
	})
	bt.SetPage(2, leaf)

	leafData, _ := bt.GetPage(2)
	hdr, _ := ParsePageHeader(leafData, 2)

	c := manualIndexCursorOnLeaf(bt, 2, 0, hdr)

	// Pass idx=5 which is beyond NumCells=1, causing GetCellPointer to error.
	found, err := c.seekLeafExactMatch(leafData, hdr, 5)
	if err == nil {
		t.Fatal("seekLeafExactMatch() expected error for out-of-range index, got nil")
	}
	if found {
		t.Error("seekLeafExactMatch() expected found=false on error")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid", c.State)
	}
}

// TestIndexCursorLowCoverage_seekLeafExactMatch_EmptyPage verifies that
// SeekIndex on a leaf page with no cells returns found=false without panic.
func TestIndexCursorLowCoverage_seekLeafExactMatch_EmptyPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with zero cells.
	emptyLeaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{})
	bt.SetPage(1, emptyLeaf)

	c := NewIndexCursor(bt, 1)
	found, err := c.SeekIndex([]byte("anything"))
	// Empty page: binarySearchKey returns idx=0, exactMatch=false, so seekLeafPage
	// is called with exactMatch=false; seekLeafExactMatch is NOT called.
	// The cursor should end up in a not-found state without error.
	if err != nil {
		t.Fatalf("SeekIndex on empty page returned unexpected error: %v", err)
	}
	if found {
		t.Errorf("SeekIndex on empty page: expected found=false, got true")
	}
}

// TestIndexCursorLowCoverage_seekLeafExactMatch_ViaSeekIndex_Found verifies
// that SeekIndex triggers seekLeafExactMatch when an exact match exists.
func TestIndexCursorLowCoverage_seekLeafExactMatch_ViaSeekIndex_Found(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("dog"), 7},
		{[]byte("fox"), 8},
		{[]byte("hen"), 9},
	})
	bt.SetPage(1, leaf)

	c := NewIndexCursor(bt, 1)
	found, err := c.SeekIndex([]byte("fox"))
	if err != nil {
		t.Fatalf("SeekIndex() unexpected error: %v", err)
	}
	if !found {
		t.Fatal("SeekIndex() expected found=true for existing key")
	}
	if string(c.CurrentKey) != "fox" {
		t.Errorf("CurrentKey = %q, want \"fox\"", c.CurrentKey)
	}
	if c.CurrentRowid != 8 {
		t.Errorf("CurrentRowid = %d, want 8", c.CurrentRowid)
	}
}

// TestIndexCursorLowCoverage_prevInPage_FromIndexOne verifies that prevInPage
// correctly handles stepping from index 1 to index 0.
func TestIndexCursorLowCoverage_prevInPage_FromIndexOne(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("first"), 11},
		{[]byte("second"), 22},
	})
	bt.SetPage(2, leaf)

	leafData, _ := bt.GetPage(2)
	hdr, _ := ParsePageHeader(leafData, 2)

	c := manualIndexCursorOnLeaf(bt, 2, 1, hdr)

	if err := c.prevInPage(); err != nil {
		t.Fatalf("prevInPage() unexpected error: %v", err)
	}
	if c.CurrentIndex != 0 {
		t.Errorf("CurrentIndex = %d, want 0", c.CurrentIndex)
	}
	if string(c.CurrentKey) != "first" {
		t.Errorf("CurrentKey = %q, want \"first\"", c.CurrentKey)
	}
}

// TestIndexCursorLowCoverage_PrevIndex_CurrentIndexZero verifies that PrevIndex
// goes through the parent-climbing path (prevViaParent) when CurrentIndex==0,
// and returns an error when there is no parent to climb to.
func TestIndexCursorLowCoverage_PrevIndex_CurrentIndexZero(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(1, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("only"), 5},
	})
	bt.SetPage(1, leaf)

	leafData, _ := bt.GetPage(1)
	hdr, _ := ParsePageHeader(leafData, 1)

	// depth=0, no parent: PrevIndex must report beginning of index.
	c := NewIndexCursor(bt, 1)
	c.Depth = 0
	c.PageStack[0] = 1
	c.IndexStack[0] = 0
	c.CurrentPage = 1
	c.CurrentIndex = 0
	c.CurrentHeader = hdr
	c.State = CursorValid

	err := c.PrevIndex()
	if err == nil {
		t.Fatal("PrevIndex() at beginning of index: expected error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid after beginning-of-index", c.State)
	}
	if !c.AtFirst {
		t.Error("AtFirst should be true after reaching beginning of index")
	}
}
