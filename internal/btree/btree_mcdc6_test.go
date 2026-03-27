// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC 6 — error-path coverage for split.go, cursor.go, and merge.go
//
// All tests set c.CurrentPage = 0 to force GetPage → ErrInvalidPageNumber,
// or construct minimal page/header structures to trigger ParseCell / GetCellPointer
// errors, covering the uncovered 70% branches in the target functions.
// ---------------------------------------------------------------------------

// mcdc6NewCursor builds a minimal BtCursor backed by an in-memory Btree.
func mcdc6NewCursor(t *testing.T) *BtCursor {
	t.Helper()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return NewCursor(bt, root)
}

// ---------------------------------------------------------------------------
// prepareLeafSplit — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareLeafSplit_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CurrentPage = 0 // forces ErrInvalidPageNumber

	_, _, _, err := c.prepareLeafSplit(1, []byte("payload"))
	if err == nil {
		t.Fatal("prepareLeafSplit: expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareLeafSplitComposite — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareLeafSplitComposite_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CompositePK = true
	c.CurrentPage = 0

	_, _, _, err := c.prepareLeafSplitComposite([]byte("key"), []byte("payload"))
	if err == nil {
		t.Fatal("prepareLeafSplitComposite: expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareInteriorSplit — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareInteriorSplit_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CurrentPage = 0

	_, _, _, _, err := c.prepareInteriorSplit(1, 2)
	if err == nil {
		t.Fatal("prepareInteriorSplit: expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareInteriorSplitComposite — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareInteriorSplitComposite_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CompositePK = true
	c.CurrentPage = 0

	_, _, _, _, err := c.prepareInteriorSplitComposite([]byte("key"), 2)
	if err == nil {
		t.Fatal("prepareInteriorSplitComposite: expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareLeafSplit — NewBtreePage error: corrupt page type after GetPage
//
// Allocate a real page then corrupt its page-type byte so ParsePageHeader
// (called inside NewBtreePage) returns an error.
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareLeafSplit_NewBtreePageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Allocate a fresh page and set an invalid page type.
	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFF // invalid type

	c := NewCursor(bt, root)
	c.CurrentPage = pgno

	_, _, _, err = c.prepareLeafSplit(1, []byte("x"))
	if err == nil {
		t.Fatal("prepareLeafSplit: expected NewBtreePage error for corrupt type, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareLeafSplitComposite — NewBtreePage error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareLeafSplitComposite_NewBtreePageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFE // invalid type

	c := NewCursorWithOptions(bt, root, true)
	c.CurrentPage = pgno

	_, _, _, err = c.prepareLeafSplitComposite([]byte("key"), []byte("val"))
	if err == nil {
		t.Fatal("prepareLeafSplitComposite: expected NewBtreePage error, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareInteriorSplit — NewBtreePage error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareInteriorSplit_NewBtreePageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFD // invalid type

	c := NewCursor(bt, root)
	c.CurrentPage = pgno

	_, _, _, _, err = c.prepareInteriorSplit(1, 2)
	if err == nil {
		t.Fatal("prepareInteriorSplit: expected NewBtreePage error, got nil")
	}
}

// ---------------------------------------------------------------------------
// prepareInteriorSplitComposite — NewBtreePage error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_PrepareInteriorSplitComposite_NewBtreePageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFC // invalid type

	c := NewCursorWithOptions(bt, root, true)
	c.CurrentPage = pgno

	_, _, _, _, err = c.prepareInteriorSplitComposite([]byte("key"), 2)
	if err == nil {
		t.Fatal("prepareInteriorSplitComposite: expected NewBtreePage error, got nil")
	}
}

// ---------------------------------------------------------------------------
// performCellDeletion — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_PerformCellDeletion_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CurrentPage = 0

	err := c.performCellDeletion()
	if err == nil {
		t.Fatal("performCellDeletion: expected error for page 0, got nil")
	}
}

// ---------------------------------------------------------------------------
// performCellDeletion — NewBtreePage error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_PerformCellDeletion_NewBtreePageError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFB // invalid type

	c := NewCursor(bt, root)
	c.CurrentPage = pgno

	err = c.performCellDeletion()
	if err == nil {
		t.Fatal("performCellDeletion: expected NewBtreePage error, got nil")
	}
}

// ---------------------------------------------------------------------------
// adjustCursorAfterDelete — GetPage error when CurrentPage == 0
// ---------------------------------------------------------------------------

func TestMCDC6_AdjustCursorAfterDelete_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.CurrentPage = 0

	err := c.adjustCursorAfterDelete()
	if err == nil {
		t.Fatal("adjustCursorAfterDelete: expected error for page 0, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("adjustCursorAfterDelete: State = %v, want CursorInvalid", c.State)
	}
}

// ---------------------------------------------------------------------------
// adjustCursorAfterDelete — ParsePageHeader error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_AdjustCursorAfterDelete_ParsePageHeaderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xFA // invalid type

	c := NewCursor(bt, root)
	c.CurrentPage = pgno
	c.CurrentIndex = 1

	err = c.adjustCursorAfterDelete()
	if err == nil {
		t.Fatal("adjustCursorAfterDelete: expected ParsePageHeader error, got nil")
	}
}

// ---------------------------------------------------------------------------
// enterPage — depth exceeded (A=T branch)
// ---------------------------------------------------------------------------

func TestMCDC6_EnterPage_DepthExceeded(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.Depth = MaxBtreeDepth - 1 // enterPage increments to MaxBtreeDepth → error

	_, _, err := c.enterPage(c.RootPage)
	if err == nil {
		t.Fatal("enterPage: expected depth-exceeded error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("enterPage depth exceeded: State = %v, want CursorInvalid", c.State)
	}
}

// ---------------------------------------------------------------------------
// enterPage — GetPage error (page 0)
// ---------------------------------------------------------------------------

func TestMCDC6_EnterPage_GetPageError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)
	c.Depth = 0 // depth guard not triggered

	_, _, err := c.enterPage(0) // pageNum 0 → ErrInvalidPageNumber
	if err == nil {
		t.Fatal("enterPage: expected GetPage error for page 0, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("enterPage GetPage error: State = %v, want CursorInvalid", c.State)
	}
}

// ---------------------------------------------------------------------------
// enterPage — ParsePageHeader error: corrupt page type
// ---------------------------------------------------------------------------

func TestMCDC6_EnterPage_ParsePageHeaderError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	pgno, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	data, err := bt.GetPage(pgno)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	data[PageHeaderOffsetType] = 0xF9 // invalid type

	c := NewCursor(bt, root)
	c.Depth = 0

	_, _, err = c.enterPage(pgno)
	if err == nil {
		t.Fatal("enterPage: expected ParsePageHeader error, got nil")
	}
	if c.State != CursorInvalid {
		t.Errorf("enterPage ParsePageHeader error: State = %v, want CursorInvalid", c.State)
	}
}

// ---------------------------------------------------------------------------
// finishInsert — markPageDirty error via nil Btree provider path
//
// markPageDirty only errors when Provider is set and MarkDirty fails.
// We attach a failing provider and call finishInsert with a valid page.
// ---------------------------------------------------------------------------

func TestMCDC6_FinishInsert_MarkPageDirtyError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Attach a provider that always fails MarkDirty.
	prov := newSepErrProvider(bt)
	prov.markDirtyFails = true
	bt.Provider = prov

	c := NewCursor(bt, root)
	c.CurrentPage = root

	// Build a valid BtreePage for the root so InsertCell would succeed
	// (but markPageDirty fails first).
	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage root: %v", err)
	}
	btPage, err := NewBtreePage(root, pageData, bt.UsableSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	cellData := make([]byte, 10)
	err = c.finishInsert(1, nil, cellData, 0, btPage)
	if err == nil {
		t.Fatal("finishInsert: expected markPageDirty error, got nil")
	}
}

// ---------------------------------------------------------------------------
// extractCellData — GetCellPointer error: index out of bounds on empty page
// ---------------------------------------------------------------------------

func TestMCDC6_ExtractCellData_GetCellPointerError(t *testing.T) {
	t.Parallel()
	c := mcdc6NewCursor(t)

	// Build a minimal leaf page with NumCells=0.
	pageData := make([]byte, 4096)
	pageData[PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], 0)

	header := &PageHeader{
		PageType:      PageTypeLeafTable,
		NumCells:      0,
		IsLeaf:        true,
		IsTable:       true,
		HeaderSize:    PageHeaderSizeLeaf,
		CellPtrOffset: PageHeaderSizeLeaf,
	}

	// index=0 on a page with NumCells=0 should fail GetCellPointer.
	_, err := c.extractCellData(pageData, header, 0)
	if err == nil {
		t.Fatal("extractCellData: expected GetCellPointer error for index 0 on empty page, got nil")
	}
}

// ---------------------------------------------------------------------------
// extractCellData — success path: one valid cell
// ---------------------------------------------------------------------------

func TestMCDC6_ExtractCellData_Success(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)

	// Insert one row so we have a real cell on the root page.
	if err := c.Insert(42, []byte("hello")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	pageData, err := bt.GetPage(c.RootPage)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	header, err := ParsePageHeader(pageData, c.RootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}

	// extractCellData on the first cell should succeed.
	cellBytes, err := c.extractCellData(pageData, header, 0)
	if err != nil {
		t.Fatalf("extractCellData: unexpected error: %v", err)
	}
	if len(cellBytes) == 0 {
		t.Error("extractCellData: returned empty cell bytes")
	}
}

// ---------------------------------------------------------------------------
// createNewRoot — success via split: rowid tree that forces root split
// ---------------------------------------------------------------------------

func TestMCDC6_CreateNewRoot_ViaRootSplit(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // tiny page to force splits quickly
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)

	// Insert enough rows to force a root split (createNewRoot called).
	payload := make([]byte, 40)
	for i := int64(1); i <= 30; i++ {
		if err := c.Insert(i, payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Verify the root changed (split occurred) and data is intact.
	found, err := c.SeekRowid(15)
	if err != nil {
		t.Fatalf("SeekRowid(15): %v", err)
	}
	if !found {
		t.Error("SeekRowid(15): not found after root split")
	}
}

// ---------------------------------------------------------------------------
// createNewRoot — composite PK branch (pageType = PageTypeInteriorTableNo)
// ---------------------------------------------------------------------------

func TestMCDC6_CreateNewRoot_CompositePK(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	c := NewCursorWithOptions(bt, root, true)

	payload := make([]byte, 30)
	for i := 1; i <= 30; i++ {
		key := []byte{byte(i / 10), byte(i % 10), byte(i)}
		if err := c.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}

	found, err := c.SeekComposite([]byte{0x01, 0x05, 15})
	_ = found
	_ = err
	// Primary goal: no panic from createNewRoot composite path.
}

// ---------------------------------------------------------------------------
// adjustCursorAfterDelete — CurrentIndex < 0 branch (only cell deleted)
// ---------------------------------------------------------------------------

func TestMCDC6_AdjustCursorAfterDelete_NegativeIndex(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, root)

	if err := c.Insert(1, []byte("only")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Position cursor at the single cell (index 0).
	found, err := c.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid(1): err=%v found=%v", err, found)
	}

	// Delete the cell, then call adjustCursorAfterDelete directly.
	if err := c.performCellDeletion(); err != nil {
		t.Fatalf("performCellDeletion: %v", err)
	}

	// After deletion of the only cell, CurrentIndex starts at 0; adjusting
	// should decrement to -1 and set CurrentCell = nil.
	c.CurrentIndex = 0
	err = c.adjustCursorAfterDelete()
	if err != nil {
		t.Fatalf("adjustCursorAfterDelete: unexpected error: %v", err)
	}
	if c.CurrentIndex != -1 {
		t.Errorf("CurrentIndex = %d, want -1", c.CurrentIndex)
	}
	if c.CurrentCell != nil {
		t.Error("CurrentCell should be nil after index underflows to -1")
	}
}
