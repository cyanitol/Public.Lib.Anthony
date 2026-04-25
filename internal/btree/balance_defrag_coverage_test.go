// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// buildDefragPage constructs a valid leaf-table page with real cells and a
// gap between them, then writes FragmentedBytes into the header so that
// defragmentIfNeeded / handleOverfullPage will enter their defrag branches.
//
// The returned page has:
//   - Two real cells (parseable by ParseCell)
//   - A deliberate gap between them (fragmented space)
//   - Header.FragmentedBytes set to gapSize
//   - FreeSpace large enough that the page is NOT overfull after defrag
func buildDefragPage(pageSize uint32, gapSize int) (*BtreePage, *Btree, error) {
	pageNum := uint32(2)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	cell1 := EncodeTableLeafCell(1, []byte("alpha"))
	cell2 := EncodeTableLeafCell(2, []byte("beta"))

	// Place cell2 at the very end of the page.
	off2 := int(pageSize) - len(cell2)
	copy(pageData[off2:], cell2)

	// Place cell1 leaving a gap of gapSize bytes before cell2.
	off1 := off2 - gapSize - len(cell1)
	if off1 < PageHeaderSizeLeaf+4 {
		// If the gap is too large for the page, shrink gapSize to fit
		off1 = PageHeaderSizeLeaf + 4
	}
	copy(pageData[off1:], cell1)

	// Write NumCells = 2
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 2)

	// Write CellContentStart = off1 (lowest content offset)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], uint16(off1))

	// Write cell pointer array (leaf header = 8 bytes, pointers start at byte 8)
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(off1))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf+2:], uint16(off2))

	// Write FragmentedBytes
	if gapSize > 255 {
		gapSize = 255
	}
	pageData[PageHeaderOffsetFragmented] = byte(gapSize)

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		return nil, nil, err
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	return page, bt, nil
}

// makeCursorForPage builds a minimal cursor pointing at pageNum with the
// given bt and RootPage.
func makeCursorForPage(bt *Btree, pageNum, rootPage uint32) *BtCursor {
	return &BtCursor{
		Btree:       bt,
		CurrentPage: pageNum,
		RootPage:    rootPage,
		State:       CursorValid,
		Depth:       0,
	}
}

// buildBrokenDefragPage creates a page with FragmentedBytes > 0 but an
// invalid cell pointer so that page.Defragment() returns an error.
// This is used to hit the "defragmentPage error" branches in
// defragmentIfNeeded and handleOverfullPage.
func buildBrokenDefragPage(pageSize uint32) (*BtreePage, *Btree, error) {
	pageNum := uint32(9)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// NumCells = 1 with a cell pointer pointing past the end of the page.
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], uint16(pageSize-1))
	// Cell pointer at PageHeaderSizeLeaf points to offset pageSize+100 (past end).
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(pageSize-1))

	// FragmentedBytes set so defrag branch is entered.
	pageData[PageHeaderOffsetFragmented] = 5

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		return nil, nil, err
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	return page, bt, nil
}

// TestBalanceDefragIfNeededFragmentedBranch exercises the
// page.Header.FragmentedBytes > 0 branch inside defragmentIfNeeded.
func TestBalanceDefragIfNeededFragmentedBranch(t *testing.T) {
	t.Parallel()

	page, bt, err := buildDefragPage(4096, 20)
	if err != nil {
		t.Fatalf("buildDefragPage: %v", err)
	}

	// Verify precondition: FragmentedBytes is set.
	if page.Header.FragmentedBytes == 0 {
		t.Fatal("precondition: FragmentedBytes should be > 0")
	}

	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	if err := defragmentIfNeeded(cursor, page); err != nil {
		t.Errorf("defragmentIfNeeded returned unexpected error: %v", err)
	}

	// After defragmentation the fragmented bytes should be cleared.
	if page.Header.FragmentedBytes != 0 {
		t.Errorf("FragmentedBytes after defragmentIfNeeded = %d, want 0", page.Header.FragmentedBytes)
	}
}

// TestBalanceDefragOverfullDefragSolvesIt exercises the branch in
// handleOverfullPage that returns nil after defragmentation resolves overflow.
//
// Strategy: build a page that FreeSpace() reports < 6 (overfull threshold)
// because the cell-content-start pointer is set artificially low, AND has
// FragmentedBytes > 0.  After Defragment() runs, the cells are compacted to
// the end of the page and FreeSpace becomes large, so isOverfull becomes false
// and handleOverfullPage returns nil.
func TestBalanceDefragOverfullDefragSolvesIt(t *testing.T) {
	t.Parallel()

	pageSize := uint32(4096)
	pageNum := uint32(3)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// Two small cells.
	cell1 := EncodeTableLeafCell(1, []byte("x"))
	cell2 := EncodeTableLeafCell(2, []byte("y"))

	// Pack cells at the very end.
	realOff2 := int(pageSize) - len(cell2)
	realOff1 := realOff2 - len(cell1)
	copy(pageData[realOff2:], cell2)
	copy(pageData[realOff1:], cell1)

	// Write NumCells = 2
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 2)

	// Deliberately set CellContentStart very close to the cell-pointer array
	// end so that FreeSpace() < 6 (triggering isOverfull).
	// Cell pointer array ends at: PageHeaderSizeLeaf + 2*2 = 12
	// Set CellContentStart = 12 -> freeSpace = 12 - 12 - 2 = -2 -> 0 (reported as 0)
	fakeCellStart := uint16(PageHeaderSizeLeaf + 4) // 12
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], fakeCellStart)

	// Write actual cell pointers (real locations at end of page).
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(realOff1))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf+2:], uint16(realOff2))

	// Set FragmentedBytes so the defrag branch runs.
	pageData[PageHeaderOffsetFragmented] = 10

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// Verify that isOverfull is true before defrag (FreeSpace < 6).
	if !isOverfull(page) {
		t.Logf("FreeSpace=%d, fakeCellStart=%d", page.FreeSpace(), fakeCellStart)
		t.Skip("page not overfull with this construction; skipping")
	}
	if page.Header.FragmentedBytes == 0 {
		t.Skip("FragmentedBytes not set; defrag branch would be skipped")
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	cursor := makeCursorForPage(bt, pageNum, pageNum)

	// handleOverfullPage should defrag (clearing fragmented bytes), then
	// detect !isOverfull and return nil.
	err = handleOverfullPage(cursor, page)
	if err != nil {
		t.Errorf("handleOverfullPage should return nil after defrag resolves overflow, got: %v", err)
	}
}

// TestBalanceDefragLoadPageForBalanceParseError exercises the NewBtreePage
// error path in loadPageForBalance.  We inject a page with an invalid page
// type so that ParsePageHeader returns an error.
func TestBalanceDefragLoadPageForBalanceParseError(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	badPageNum := uint32(5)
	// Build a page with an invalid page-type byte (0x99 is not a valid type).
	badPageData := make([]byte, 4096)
	badPageData[0] = 0x99
	bt.Pages[badPageNum] = badPageData

	cursor := &BtCursor{
		Btree:       bt,
		CurrentPage: badPageNum,
		RootPage:    badPageNum,
		State:       CursorValid,
		Depth:       0,
	}

	_, err := loadPageForBalance(cursor)
	if err == nil {
		t.Error("expected error from loadPageForBalance with invalid page type, got nil")
	}
}

// TestBalanceDefragExecuteBalanceDefragPath exercises the third branch of
// executeBalance (defragmentIfNeeded) by presenting a page that is neither
// overfull nor underfull but has FragmentedBytes > 0.
//
// We use createBalanceTestPage with cell sizes that put the page in balanced
// territory (>33% fill, not overfull), then set FragmentedBytes manually.
// buildBalancedPage creates a balanced page suitable for defrag testing,
// or calls t.Skip if no balanced page can be constructed.
func buildBalancedPage(t *testing.T) *BtreePage {
	t.Helper()
	// Try 25 cells of 60 bytes first; fallback to 40 cells of 40 bytes.
	configs := [][2]int{{25, 60}, {40, 40}}
	for _, cfg := range configs {
		numCells, cellSize := cfg[0], cfg[1]
		cellSizes := make([]int, numCells)
		for i := range cellSizes {
			cellSizes[i] = cellSize
		}
		page := createBalanceTestPage(4096, PageTypeLeafTable, numCells, cellSizes)
		if !isOverfull(page) && !isUnderfull(page) {
			return page
		}
	}
	t.Skip("cannot construct a balanced page with these parameters; skipping")
	return nil
}

func TestBalanceDefragExecuteBalanceDefragPath(t *testing.T) {
	t.Parallel()

	page := buildBalancedPage(t)

	// Inject fragmented bytes so defragmentIfNeeded's body runs.
	page.Header.FragmentedBytes = 12

	bt := &Btree{
		UsableSize: 4096,
		PageSize:   4096,
		Pages:      map[uint32][]byte{page.PageNum: page.Data},
	}
	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err := executeBalance(cursor, page)
	if err != nil {
		t.Errorf("executeBalance on balanced page with fragments: unexpected error: %v", err)
	}

	if page.Header.FragmentedBytes != 0 {
		t.Errorf("FragmentedBytes after executeBalance = %d, want 0", page.Header.FragmentedBytes)
	}
}

// buildCorruptDefragPage creates a page that passes NewBtreePage but then has
// a deliberately corrupted cell pointer so that defragmentPage will fail when
// it calls extractAllCellsForDefrag.
//
// The page has NumCells=1 with a valid-looking cell written at the end, and
// FragmentedBytes>0.  After NewBtreePage succeeds we overwrite the cell pointer
// in the raw data slice to point beyond the page boundary so that
// extractAllCellsForDefrag returns an "invalid cell offset" error.
func buildCorruptDefragPage(pageSize uint32) (*BtreePage, *Btree, error) {
	pageNum := uint32(10)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// Write a real cell at the end of the page so NewBtreePage is happy.
	cell := EncodeTableLeafCell(1, []byte("data"))
	off := int(pageSize) - len(cell)
	copy(pageData[off:], cell)

	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], uint16(off))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(off)) // cell pointer[0]
	pageData[PageHeaderOffsetFragmented] = 5

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		return nil, nil, err
	}

	// NOW corrupt the cell pointer so it points out of bounds.
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(pageSize+1))

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	return page, bt, nil
}

// TestBalanceDefragErrorPaths_DefragmentIfNeededFails exercises the error
// return inside defragmentIfNeeded (line 134) by providing a page whose
// cell pointer is corrupt so that defragmentPage returns an error.
func TestBalanceDefragErrorPaths_DefragmentIfNeededFails(t *testing.T) {
	t.Parallel()

	page, bt, err := buildCorruptDefragPage(4096)
	if err != nil {
		t.Fatalf("buildCorruptDefragPage: %v", err)
	}

	if page.Header.FragmentedBytes == 0 {
		t.Fatal("precondition: FragmentedBytes should be > 0")
	}

	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err = defragmentIfNeeded(cursor, page)
	if err == nil {
		t.Error("defragmentIfNeeded with corrupt cell pointer should return error, got nil")
	}
}

// TestBalanceDefragErrorPaths_HandleOverfullPageDefragFails exercises the
// error return inside handleOverfullPage (line 146) when defragmentPage fails.
//
// The page must have FragmentedBytes > 0 so the defrag branch is entered, and
// a corrupt cell pointer so defragmentPage errors.
func TestBalanceDefragErrorPaths_HandleOverfullPageDefragFails(t *testing.T) {
	t.Parallel()

	page, bt, err := buildCorruptDefragPage(4096)
	if err != nil {
		t.Fatalf("buildCorruptDefragPage: %v", err)
	}

	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err = handleOverfullPage(cursor, page)
	if err == nil {
		t.Error("handleOverfullPage with corrupt cell pointer should return error, got nil")
	}
}

// TestBalanceDefragErrorPaths_HandleUnderfullPageDefragFails exercises the
// error return inside handleUnderfullPage (line 170) when defragmentPage
// fails on an underfull non-root page.
func TestBalanceDefragErrorPaths_HandleUnderfullPageDefragFails(t *testing.T) {
	t.Parallel()

	page, bt, err := buildCorruptDefragPage(4096)
	if err != nil {
		t.Fatalf("buildCorruptDefragPage: %v", err)
	}

	// Use a different root page so the "is root" check does not short-circuit.
	cursor := &BtCursor{
		Btree:       bt,
		CurrentPage: page.PageNum,
		RootPage:    uint32(1),
		State:       CursorValid,
		Depth:       0, // depth==0 is checked after defrag; defrag error returns first
	}

	err = handleUnderfullPage(cursor, page)
	if err == nil {
		t.Error("handleUnderfullPage with corrupt cell pointer should return error, got nil")
	}
}

// TestBalanceDefragErrorPaths_ExecuteBalanceOverfullBranch exercises the
// isOverfull branch of executeBalance (lines 119-121) by calling executeBalance
// directly with a page that is genuinely overfull and has no fragmentation so
// handleOverfullPage immediately returns the split error.
func TestBalanceDefragErrorPaths_ExecuteBalanceOverfullBranch(t *testing.T) {
	t.Parallel()

	pageSize := uint32(512)
	// createBalanceTestPage with 12 cells of 40 bytes produces an overfull page
	// on a 512-byte page (see TestIsOverfull "overfull page" case).
	cellSizes := make([]int, 12)
	for i := range cellSizes {
		cellSizes[i] = 40
	}
	page := createBalanceTestPage(pageSize, PageTypeLeafTable, 12, cellSizes)

	if !isOverfull(page) {
		t.Skip("page is not overfull with this construction; skipping")
	}

	// Ensure no fragmentation so handleOverfullPage takes the direct split-error path.
	page.Header.FragmentedBytes = 0

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{page.PageNum: page.Data},
	}
	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err := executeBalance(cursor, page)
	if err == nil {
		t.Error("executeBalance on overfull page should return error, got nil")
	}
}

// TestBalanceDefragErrorPaths_GetBalanceInfoParseError exercises the
// NewBtreePage error path in GetBalanceInfo (lines 206-208) by injecting a
// page with an invalid page-type byte so ParsePageHeader returns an error.
func TestBalanceDefragErrorPaths_GetBalanceInfoParseError(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	badPageNum := uint32(20)
	// 0x77 is not a valid page type, which causes ParsePageHeader to fail.
	badPageData := make([]byte, 4096)
	badPageData[0] = 0x77
	bt.Pages[badPageNum] = badPageData

	_, err := GetBalanceInfo(bt, badPageNum)
	if err == nil {
		t.Error("GetBalanceInfo with invalid page type should return error, got nil")
	}
}

// TestBalanceDefragUnderfullWithFragmentation exercises the fragmented-bytes
// defrag branch inside handleUnderfullPage (lines 168-172).
//
// We need a page that:
//   - isUnderfull returns true
//   - cursor.CurrentPage != cursor.RootPage (not root)
//   - page.Header.FragmentedBytes > 0
func TestBalanceDefragUnderfullWithFragmentation(t *testing.T) {
	t.Parallel()

	pageSize := uint32(4096)
	pageNum := uint32(7)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// Insert only two tiny cells to make the page clearly underfull.
	cell1 := EncodeTableLeafCell(1, []byte("a"))
	cell2 := EncodeTableLeafCell(2, []byte("b"))

	off2 := int(pageSize) - len(cell2)
	off1 := off2 - len(cell1)
	copy(pageData[off2:], cell2)
	copy(pageData[off1:], cell1)

	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 2)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], uint16(off1))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(off1))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf+2:], uint16(off2))

	// Set FragmentedBytes so the defrag branch inside handleUnderfullPage runs.
	pageData[PageHeaderOffsetFragmented] = 8

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	if !isUnderfull(page) {
		t.Skip("page not underfull with two tiny cells; skipping")
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}

	// cursor.CurrentPage != cursor.RootPage and Depth > 0 to reach the defrag code.
	cursor := &BtCursor{
		Btree:       bt,
		CurrentPage: pageNum,
		RootPage:    uint32(1), // different from pageNum
		State:       CursorValid,
		Depth:       1, // non-zero so we get past the depth==0 short-circuit
	}

	// handleUnderfullPage should defrag (FragmentedBytes > 0) and then
	// return the "underfull and may need merge" error (since Depth > 0).
	err = handleUnderfullPage(cursor, page)
	// An error about merge/redistribution is expected (Depth > 0, non-root).
	// The important thing is that the defrag branch ran (no panic, no defrag error).
	if err == nil {
		// Depth > 0 and non-root always returns an error in this implementation.
		t.Error("expected merge error from handleUnderfullPage, got nil")
	}

	// FragmentedBytes should have been cleared by the defrag.
	if page.Header.FragmentedBytes != 0 {
		t.Errorf("FragmentedBytes after handleUnderfullPage = %d, want 0", page.Header.FragmentedBytes)
	}
}

// TestBalanceDefragExecuteBalanceOverfullPath exercises the overfull branch of
// executeBalance by constructing a page that is genuinely overfull (FreeSpace < 6)
// and passing it directly to executeBalance.
func TestBalanceDefragExecuteBalanceOverfullPath(t *testing.T) {
	t.Parallel()

	// Build an overfull page using createBalanceTestPage with a small page size
	// and many large cells. On a 512-byte page with 12 cells of 40 bytes each,
	// 12*40 = 480 bytes, leaving little free space.
	numCells := 12
	cellSizes := make([]int, numCells)
	for i := range cellSizes {
		cellSizes[i] = 40
	}
	page := createBalanceTestPage(512, PageTypeLeafTable, numCells, cellSizes)

	if !isOverfull(page) {
		t.Skip("page not overfull with this construction; skipping")
	}

	bt := &Btree{
		UsableSize: 512,
		PageSize:   512,
		Pages:      map[uint32][]byte{page.PageNum: page.Data},
	}
	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err := executeBalance(cursor, page)
	// Should return an "overfull and requires split" error.
	if err == nil {
		t.Error("executeBalance on overfull page: expected error, got nil")
	}
}

// TestBalanceDefragIfNeededDefragError exercises the error-return branch inside
// defragmentIfNeeded when defragmentPage itself fails.
//
// We use a page with FragmentedBytes > 0 but a cell pointer that points to
// offset pageSize-1 (only 1 byte available), causing ParseCell to fail during
// defragmentation.
func TestBalanceDefragIfNeededDefragError(t *testing.T) {
	t.Parallel()

	page, bt, err := buildBrokenDefragPage(4096)
	if err != nil {
		t.Fatalf("buildBrokenDefragPage: %v", err)
	}

	if page.Header.FragmentedBytes == 0 {
		t.Fatal("precondition: FragmentedBytes must be > 0")
	}

	cursor := makeCursorForPage(bt, page.PageNum, page.PageNum)

	err = defragmentIfNeeded(cursor, page)
	if err == nil {
		t.Error("expected error from defragmentIfNeeded when defragmentPage fails, got nil")
	}
}

// TestBalanceDefragHandleOverfullPageDefragError exercises the error-return
// branch inside handleOverfullPage when defragmentPage fails.
func TestBalanceDefragHandleOverfullPageDefragError(t *testing.T) {
	t.Parallel()

	pageSize := uint32(4096)
	pageNum := uint32(11)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// Use two cells so the page has NumCells > 0.
	// Set CellContentStart so close to cell pointer array end that isOverfull is true.
	// NumCells = 1 with bogus pointer -> defrag will fail.
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 1)
	// CellContentStart = PageHeaderSizeLeaf + 2 = 10 -> FreeSpace will be ≤ 0.
	fakeCellStart := uint16(PageHeaderSizeLeaf + 2)
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], fakeCellStart)
	// Cell pointer at offset 8 points to pageSize-1 (only 1 byte, ParseCell will fail).
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(pageSize-1))
	// FragmentedBytes set so the defrag branch is entered.
	pageData[PageHeaderOffsetFragmented] = 7

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	if !isOverfull(page) {
		t.Skip("page not overfull; skipping")
	}
	if page.Header.FragmentedBytes == 0 {
		t.Skip("FragmentedBytes not set; defrag branch would not run")
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	cursor := makeCursorForPage(bt, pageNum, pageNum)

	err = handleOverfullPage(cursor, page)
	if err == nil {
		t.Error("expected error from handleOverfullPage when defragmentPage fails, got nil")
	}
}

// TestBalanceDefragHandleUnderfullPageDefragError exercises the error-return
// branch inside handleUnderfullPage when defragmentPage fails.
func TestBalanceDefragHandleUnderfullPageDefragError(t *testing.T) {
	t.Parallel()

	pageSize := uint32(4096)
	pageNum := uint32(13)
	pageData := make([]byte, pageSize)
	pageData[0] = PageTypeLeafTable

	// One cell with a bogus pointer so defrag fails, but the page has
	// NumCells = 1 with tiny payload -> isUnderfull = true.
	// Place the cell pointer to point at pageSize-1 so ParseCell fails during defrag.
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetNumCells:], 1)
	// CellContentStart near the end so it looks like the cell is there,
	// but we put an invalid pointer so defrag will error.
	binary.BigEndian.PutUint16(pageData[PageHeaderOffsetCellStart:], uint16(pageSize-1))
	binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(pageSize-1))
	// FragmentedBytes set so the defrag branch inside handleUnderfullPage runs.
	pageData[PageHeaderOffsetFragmented] = 3

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	if !isUnderfull(page) {
		t.Skip("page not underfull; skipping")
	}
	if page.Header.FragmentedBytes == 0 {
		t.Skip("FragmentedBytes not set")
	}

	bt := &Btree{
		UsableSize: pageSize,
		PageSize:   pageSize,
		Pages:      map[uint32][]byte{pageNum: pageData},
	}
	// Non-root, depth > 0 so we reach the fragmented-bytes check.
	cursor := &BtCursor{
		Btree:       bt,
		CurrentPage: pageNum,
		RootPage:    uint32(1),
		State:       CursorValid,
		Depth:       1,
	}

	err = handleUnderfullPage(cursor, page)
	if err == nil {
		t.Error("expected error from handleUnderfullPage when defragmentPage fails, got nil")
	}
}
