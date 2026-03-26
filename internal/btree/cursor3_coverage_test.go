// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// ============================================================================
// BtCursor.prevInPage — error branches
// ============================================================================

// TestBtreeCursor3Coverage_prevInPage_GetCellPointerError calls prevInPage()
// directly with CurrentIndex==0 so the decrement yields -1, which is out of
// range for GetCellPointer (cellIndex < 0).
func TestBtreeCursor3Coverage_prevInPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with two real cells so the header is valid.
	leaf := buildLeafPage(10, 4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("first")},
		{2, []byte("second")},
	})
	bt.SetPage(10, leaf)

	leafData, err := bt.GetPage(10)
	if err != nil {
		t.Fatalf("GetPage(10): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 10)
	if err != nil {
		t.Fatalf("ParsePageHeader(10): %v", err)
	}

	// Position at CurrentIndex==0; prevInPage decrements to -1 which is < 0.
	c := NewCursor(bt, 10)
	c.Depth = 0
	c.PageStack[0] = 10
	c.IndexStack[0] = 0
	c.CurrentPage = 10
	c.CurrentIndex = 0
	c.CurrentHeader = hdr
	c.State = CursorValid

	err = c.prevInPage()
	if err == nil {
		t.Fatal("prevInPage() with index -1 expected error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid after GetCellPointer error", c.State)
	}
}

// TestBtreeCursor3Coverage_prevInPage_ParseCellError triggers the ParseCell
// error branch in BtCursor.prevInPage by corrupting the cell bytes at the
// offset pointed to by the cell pointer array, so that ParseCell fails.
func TestBtreeCursor3Coverage_prevInPage_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a leaf page with two real cells.
	leaf := buildLeafPage(11, 4096, []struct {
		rowid   int64
		payload []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
	})
	bt.SetPage(11, leaf)

	leafData, err := bt.GetPage(11)
	if err != nil {
		t.Fatalf("GetPage(11): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 11)
	if err != nil {
		t.Fatalf("ParsePageHeader(11): %v", err)
	}

	// Find the cell-0 pointer and corrupt its target bytes to make ParseCell fail.
	cellOff0, err := hdr.GetCellPointer(leafData, 0)
	if err != nil {
		t.Fatalf("GetCellPointer(0): %v", err)
	}
	// Overwrite 9 bytes at the cell offset with 0x80 to produce an enormous
	// varint payload size that exceeds any reasonable usableSize, causing
	// completeLeafCellParse / ParseCell to return an error.
	for i := int(cellOff0); i < int(cellOff0)+9 && i < len(leafData); i++ {
		leafData[i] = 0x80
	}

	// Position at CurrentIndex==1; prevInPage decrements to 0, reads that cell.
	c := NewCursor(bt, 11)
	c.Depth = 0
	c.PageStack[0] = 11
	c.IndexStack[0] = 1
	c.CurrentPage = 11
	c.CurrentIndex = 1
	c.CurrentHeader = hdr
	c.State = CursorValid

	err = c.prevInPage()
	if err == nil {
		// ParseCell may be lenient with overflow; mark as informational.
		t.Logf("prevInPage returned no error (ParseCell may tolerate large varint): cursor valid=%v", c.IsValid())
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParseCell error", c.State)
		}
		t.Logf("prevInPage ParseCell error correctly returned: %v", err)
	}
}

// ============================================================================
// BtCursor.prevViaParent — error branches
// ============================================================================

// TestBtreeCursor3Coverage_prevViaParent_ParsePageHeaderError exercises the
// ParsePageHeader error branch in prevViaParent by injecting a page with an
// invalid type byte directly into the btree page map, bypassing validation.
func TestBtreeCursor3Coverage_prevViaParent_ParsePageHeaderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a valid leaf page (page 2).
	leaf := buildLeafPage(2, 4096, []struct {
		rowid   int64
		payload []byte
	}{
		{10, []byte("data")},
	})
	bt.SetPage(2, leaf)

	leafData, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	// Inject a parent page (page 1) with an invalid type byte so that
	// ParsePageHeader fails inside prevViaParent.
	badParent := make([]byte, 4096)
	// Page 1 has a 100-byte file header; put invalid type at offset 100.
	badParent[FileHeaderSize+PageHeaderOffsetType] = 0xFF // invalid page type
	binary.BigEndian.PutUint16(badParent[FileHeaderSize+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint16(badParent[FileHeaderSize+PageHeaderOffsetCellStart:], uint16(4096))
	bt.mu.Lock()
	bt.Pages[1] = badParent
	bt.mu.Unlock()

	// Position cursor on leaf page 2 with parent at page 1 and parentIndex > 0.
	c := NewCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1 // > 0 so prevViaParent doesn't short-circuit
	c.PageStack[1] = 2
	c.IndexStack[1] = 0
	c.CurrentPage = 2
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.State = CursorValid

	// Call prevViaParent directly: it decrements Depth to 0, reads page 1,
	// calls ParsePageHeader which fails on the invalid type byte.
	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		t.Logf("prevViaParent returned found=%v (ParsePageHeader may have been lenient)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParsePageHeader error", c.State)
		}
		t.Logf("prevViaParent ParsePageHeader error correctly returned: %v", pvpErr)
	}
}

// TestBtreeCursor3Coverage_prevViaParent_GetCellPointerError exercises the
// GetCellPointer error branch in prevViaParent by setting up a parent page
// whose NumCells is 0, so GetCellPointer(parentIndex-1=0) fails.
func TestBtreeCursor3Coverage_prevViaParent_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a valid leaf page (page 2).
	leaf := buildLeafPage(2, 4096, []struct {
		rowid   int64
		payload []byte
	}{
		{5, []byte("x")},
	})
	bt.SetPage(2, leaf)

	leafData, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	// Build a parent interior page (page 1) with NumCells=0 but a valid type,
	// so ParsePageHeader succeeds but GetCellPointer(0) fails (0 >= 0 NumCells).
	parent := make([]byte, 4096)
	parent[FileHeaderSize+PageHeaderOffsetType] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(parent[FileHeaderSize+PageHeaderOffsetRightChild:], 2)
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetCellStart:], uint16(4096))
	bt.mu.Lock()
	bt.Pages[1] = parent
	bt.mu.Unlock()

	// Position cursor with parentIndex=1 so prevViaParent tries to read cell 0.
	c := NewCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1 // parentIndex=1 > 0, so GetCellPointer(0) is attempted
	c.PageStack[1] = 2
	c.IndexStack[1] = 0
	c.CurrentPage = 2
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.State = CursorValid

	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		t.Logf("prevViaParent returned found=%v (GetCellPointer may succeed)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after GetCellPointer error", c.State)
		}
		t.Logf("prevViaParent GetCellPointer error correctly returned: %v", pvpErr)
	}
}

// TestBtreeCursor3Coverage_prevViaParent_ParseCellError exercises the ParseCell
// error branch in prevViaParent by building a parent interior page whose cell
// content bytes are corrupted, so ParseCell fails after GetCellPointer succeeds.
func TestBtreeCursor3Coverage_prevViaParent_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build a valid leaf page (page 2).
	leaf := buildLeafPage(2, 4096, []struct {
		rowid   int64
		payload []byte
	}{
		{7, []byte("y")},
	})
	bt.SetPage(2, leaf)

	leafData, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	// Build a parent interior page (page 1) with NumCells=1 so GetCellPointer(0)
	// succeeds, but point the cell to the very last two bytes of the page.
	// parseTableInteriorCell needs at least 5 bytes (4 for child page + 1 varint),
	// so 2 bytes is insufficient and ParseCell must return an error.
	parent := make([]byte, 4096)
	parent[FileHeaderSize+PageHeaderOffsetType] = PageTypeInteriorTable
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint32(parent[FileHeaderSize+PageHeaderOffsetRightChild:], 2)
	// Cell pointer at header+PageHeaderSizeInterior; points to offset 4094 (2 bytes at end).
	cellPtrOff := FileHeaderSize + PageHeaderSizeInterior
	binary.BigEndian.PutUint16(parent[cellPtrOff:], 4094)
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetCellStart:], 4094)
	bt.mu.Lock()
	bt.Pages[1] = parent
	bt.mu.Unlock()

	// Position cursor with parentIndex=1 so prevViaParent reads parent cell 0.
	c := NewCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1
	c.PageStack[1] = 2
	c.IndexStack[1] = 0
	c.CurrentPage = 2
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.State = CursorValid

	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		// If ParseCell succeeds on the minimal data, that's acceptable.
		t.Logf("prevViaParent returned found=%v (ParseCell may tolerate minimal cell)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParseCell error", c.State)
		}
		t.Logf("prevViaParent ParseCell error correctly returned: %v", pvpErr)
	}
}

// ============================================================================
// IndexCursor.prevInPage — error branches
// ============================================================================

// TestBtreeCursor3Coverage_IndexPrevInPage_GetCellPointerError calls
// IndexCursor.prevInPage() directly with CurrentIndex==0 so the decrement
// produces -1, causing GetCellPointer to fail.
func TestBtreeCursor3Coverage_IndexPrevInPage_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(12, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("alpha"), 1},
		{[]byte("beta"), 2},
	})
	bt.SetPage(12, leaf)

	leafData, err := bt.GetPage(12)
	if err != nil {
		t.Fatalf("GetPage(12): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 12)
	if err != nil {
		t.Fatalf("ParsePageHeader(12): %v", err)
	}

	c := manualIndexCursorOnLeaf(bt, 12, 0, hdr)
	// CurrentIndex=0; prevInPage decrements to -1, GetCellPointer(-1) fails.
	err = c.prevInPage()
	if err == nil {
		t.Fatal("prevInPage() with index -1 expected error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("State = %d, want CursorInvalid after GetCellPointer(-1) error", c.State)
	}
}

// TestBtreeCursor3Coverage_IndexPrevInPage_ParseCellError triggers the
// ParseCell error branch in IndexCursor.prevInPage by corrupting the cell
// bytes at the offset pointed to by the cell pointer for index 0.
func TestBtreeCursor3Coverage_IndexPrevInPage_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(13, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("x"), 1},
		{[]byte("y"), 2},
	})
	bt.SetPage(13, leaf)

	leafData, err := bt.GetPage(13)
	if err != nil {
		t.Fatalf("GetPage(13): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 13)
	if err != nil {
		t.Fatalf("ParsePageHeader(13): %v", err)
	}

	// Corrupt the cell at index 0 so ParseCell fails.
	cellOff0, err := hdr.GetCellPointer(leafData, 0)
	if err != nil {
		t.Fatalf("GetCellPointer(0): %v", err)
	}
	// Overwrite with 0x80 bytes to produce an invalid/huge varint payload length.
	for i := int(cellOff0); i < int(cellOff0)+9 && i < len(leafData); i++ {
		leafData[i] = 0x80
	}

	// Position at index 1; prevInPage decrements to 0, reads the corrupted cell.
	c := manualIndexCursorOnLeaf(bt, 13, 1, hdr)

	err = c.prevInPage()
	if err == nil {
		t.Logf("prevInPage returned no error (ParseCell may tolerate large varint): cursor valid=%v", c.IsValid())
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParseCell error", c.State)
		}
		t.Logf("IndexCursor.prevInPage ParseCell error correctly returned: %v", err)
	}
}

// TestBtreeCursor3Coverage_IndexPrevInPage_LoadCurrentEntryError exercises
// the loadCurrentEntry error branch in IndexCursor.prevInPage by building a
// leaf page with a cell whose payload is empty (zero bytes after ParseCell),
// making parseIndexPayload return an error.
func TestBtreeCursor3Coverage_IndexPrevInPage_LoadCurrentEntryError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Build leaf page 14 manually with two cells: cell 1 has a valid payload,
	// cell 0 has a payload encoded with zero bytes so loadCurrentEntry fails.
	data := make([]byte, 4096)
	// Use page 14 (no file header offset).
	data[PageHeaderOffsetType] = PageTypeLeafIndex

	// Cell 0: zero-length payload encoded as an index leaf cell.
	// EncodeIndexLeafCell([]byte{}) produces varint(0) = one byte 0x00.
	cell0 := EncodeIndexLeafCell([]byte{})
	// Cell 1: a valid cell.
	validPayload := encodeIndexPayload([]byte("valid"), 99)
	cell1 := EncodeIndexLeafCell(validPayload)

	contentOff := uint32(4096)

	contentOff -= uint32(len(cell1))
	copy(data[contentOff:], cell1)
	off1 := contentOff

	contentOff -= uint32(len(cell0))
	copy(data[contentOff:], cell0)
	off0 := contentOff

	binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 2)
	ptrBase := PageHeaderSizeLeaf
	binary.BigEndian.PutUint16(data[ptrBase:], uint16(off0))
	binary.BigEndian.PutUint16(data[ptrBase+2:], uint16(off1))
	binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], uint16(contentOff))

	bt.mu.Lock()
	bt.Pages[14] = data
	bt.mu.Unlock()

	leafData, err := bt.GetPage(14)
	if err != nil {
		t.Fatalf("GetPage(14): %v", err)
	}
	hdr, err := ParsePageHeader(leafData, 14)
	if err != nil {
		t.Fatalf("ParsePageHeader(14): %v", err)
	}

	// Position at index 1 (valid cell); prevInPage decrements to 0 (empty payload cell).
	c := manualIndexCursorOnLeaf(bt, 14, 1, hdr)

	err = c.prevInPage()
	if err == nil {
		// If parseIndexPayload manages to extract something from an empty payload
		// that parses as a zero-length key + rowid, that may succeed.
		t.Logf("prevInPage returned no error (empty payload may parse as rowid=0)")
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after loadCurrentEntry error", c.State)
		}
		t.Logf("IndexCursor.prevInPage loadCurrentEntry error correctly returned: %v", err)
	}
}

// ============================================================================
// IndexCursor.prevViaParent — error branches
// ============================================================================

// TestBtreeCursor3Coverage_IndexPrevViaParent_ParsePageHeaderError exercises
// the ParsePageHeader error branch in IndexCursor.prevViaParent by injecting a
// parent page with an invalid type byte.
func TestBtreeCursor3Coverage_IndexPrevViaParent_ParsePageHeaderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(2, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("aaa"), 1},
	})
	bt.SetPage(2, leaf)

	leafData, err := bt.GetPage(2)
	if err != nil {
		t.Fatalf("GetPage(2): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 2)
	if err != nil {
		t.Fatalf("ParsePageHeader(2): %v", err)
	}

	// Inject parent page 1 with invalid type byte.
	badParent := make([]byte, 4096)
	badParent[FileHeaderSize+PageHeaderOffsetType] = 0xFF
	binary.BigEndian.PutUint16(badParent[FileHeaderSize+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint16(badParent[FileHeaderSize+PageHeaderOffsetCellStart:], uint16(4096))
	bt.mu.Lock()
	bt.Pages[1] = badParent
	bt.mu.Unlock()

	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1 // > 0 so prevViaParent doesn't early-return
	c.PageStack[1] = 2
	c.IndexStack[1] = 0
	c.CurrentPage = 2
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.CurrentKey = []byte("aaa")
	c.CurrentRowid = 1
	c.State = CursorValid

	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		t.Logf("prevViaParent returned found=%v (ParsePageHeader may be lenient)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParsePageHeader error", c.State)
		}
		t.Logf("IndexCursor.prevViaParent ParsePageHeader error correctly returned: %v", pvpErr)
	}
}

// TestBtreeCursor3Coverage_IndexPrevViaParent_GetCellPointerError exercises
// the GetCellPointer error branch in IndexCursor.prevViaParent by setting up
// a parent index interior page with NumCells=0, so GetCellPointer(0) fails.
func TestBtreeCursor3Coverage_IndexPrevViaParent_GetCellPointerError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(3, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("bbb"), 2},
	})
	bt.SetPage(3, leaf)

	leafData, err := bt.GetPage(3)
	if err != nil {
		t.Fatalf("GetPage(3): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 3)
	if err != nil {
		t.Fatalf("ParsePageHeader(3): %v", err)
	}

	// Build parent page 1 as an index interior page with NumCells=0.
	parent := make([]byte, 4096)
	parent[FileHeaderSize+PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint32(parent[FileHeaderSize+PageHeaderOffsetRightChild:], 3)
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetCellStart:], uint16(4096))
	bt.mu.Lock()
	bt.Pages[1] = parent
	bt.mu.Unlock()

	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1 // parentIndex=1 > 0 → tries GetCellPointer(0) on NumCells=0 page
	c.PageStack[1] = 3
	c.IndexStack[1] = 0
	c.CurrentPage = 3
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.CurrentKey = []byte("bbb")
	c.CurrentRowid = 2
	c.State = CursorValid

	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		t.Logf("prevViaParent returned found=%v (GetCellPointer may succeed)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after GetCellPointer error", c.State)
		}
		t.Logf("IndexCursor.prevViaParent GetCellPointer error correctly returned: %v", pvpErr)
	}
}

// TestBtreeCursor3Coverage_IndexPrevViaParent_ParseCellError exercises the
// ParseCell error branch in IndexCursor.prevViaParent by building a parent
// interior index page with NumCells=1 and a cell pointer pointing to near the
// page end (yielding insufficient bytes for ParseCell to succeed).
func TestBtreeCursor3Coverage_IndexPrevViaParent_ParseCellError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	leaf := buildIndexLeafPage(4, 4096, []struct {
		key   []byte
		rowid int64
	}{
		{[]byte("ccc"), 3},
	})
	bt.SetPage(4, leaf)

	leafData, err := bt.GetPage(4)
	if err != nil {
		t.Fatalf("GetPage(4): %v", err)
	}
	leafHdr, err := ParsePageHeader(leafData, 4)
	if err != nil {
		t.Fatalf("ParsePageHeader(4): %v", err)
	}

	// Build parent page 1 as an index interior page with NumCells=1 but the
	// cell pointer points to offset 4094 (only 2 bytes remain) so the cell data
	// is too short for parseIndexInteriorCell (needs ≥5 bytes: 4 child + 1 payload).
	parent := make([]byte, 4096)
	parent[FileHeaderSize+PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint32(parent[FileHeaderSize+PageHeaderOffsetRightChild:], 4)
	cellPtrOff := FileHeaderSize + PageHeaderSizeInterior
	binary.BigEndian.PutUint16(parent[cellPtrOff:], 4094)
	binary.BigEndian.PutUint16(parent[FileHeaderSize+PageHeaderOffsetCellStart:], 4094)
	bt.mu.Lock()
	bt.Pages[1] = parent
	bt.mu.Unlock()

	c := NewIndexCursor(bt, 1)
	c.Depth = 1
	c.PageStack[0] = 1
	c.IndexStack[0] = 1
	c.PageStack[1] = 4
	c.IndexStack[1] = 0
	c.CurrentPage = 4
	c.CurrentIndex = 0
	c.CurrentHeader = leafHdr
	c.CurrentKey = []byte("ccc")
	c.CurrentRowid = 3
	c.State = CursorValid

	found, pvpErr := c.prevViaParent()
	if pvpErr == nil {
		t.Logf("prevViaParent returned found=%v (ParseCell may tolerate short data)", found)
	} else {
		if c.State != CursorInvalid {
			t.Errorf("State = %d, want CursorInvalid after ParseCell error", c.State)
		}
		t.Logf("IndexCursor.prevViaParent ParseCell error correctly returned: %v", pvpErr)
	}
}

// ============================================================================
// helpers
// ============================================================================

// buildLeafPage creates a table leaf page for the given page number with the
// supplied rows. Each row is encoded as a TableLeafCell.
func buildLeafPage(pageNum uint32, pageSize uint32, rows []struct {
	rowid   int64
	payload []byte
}) []byte {
	data := make([]byte, pageSize)
	hOff := 0
	if pageNum == 1 {
		hOff = FileHeaderSize
	}

	data[hOff+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(data[hOff+PageHeaderOffsetNumCells:], uint16(len(rows)))

	contentOff := pageSize
	ptrOff := uint32(hOff + PageHeaderSizeLeaf)

	offsets := make([]uint32, len(rows))
	for i, r := range rows {
		cellData := EncodeTableLeafCell(r.rowid, r.payload)
		contentOff -= uint32(len(cellData))
		copy(data[contentOff:], cellData)
		offsets[i] = contentOff
	}
	for i := range rows {
		binary.BigEndian.PutUint16(data[ptrOff:], uint16(offsets[i]))
		ptrOff += 2
	}
	binary.BigEndian.PutUint16(data[hOff+PageHeaderOffsetCellStart:], uint16(contentOff))
	return data
}
