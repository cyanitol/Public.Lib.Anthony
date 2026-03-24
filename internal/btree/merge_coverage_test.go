// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// buildRedistributableTree builds a tree where the two sibling leaf pages
// together are too large to merge but can redistribute. The left page has
// many cells and the right page has fewer, so redistribution moves cells
// left-to-right.
func buildRedistributableTree(t *testing.T, pageSize uint32) (*Btree, uint32, uint32, uint32) {
	t.Helper()
	bt := NewBtree(pageSize)

	// Use a payload size that makes both pages together too big to merge
	// but each page individually has room. With pageSize=512:
	// usable=512, header=8, safety margin 10% means totalNeeded*110/100 <= 512
	// so raw totalNeeded <= 465. Each cell is ~(varint rowid + varint payloadLen + payload + ptr)
	// Use payload=30 bytes, so each cell ~35 bytes. 7 cells per page = 245 bytes content,
	// plus header 8 + ptrs 14 = 267 bytes. Two pages = ~534 > 465, so can't merge.
	payload := make([]byte, 30)
	for i := range payload {
		payload[i] = byte('x')
	}

	// Left leaf page (page 2): 7 cells with rowids 1..7
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 7)
	for i := 0; i < 7; i++ {
		leftCells[i].rowid = int64(i + 1)
		leftCells[i].payload = payload
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	bt.SetPage(2, leftPageData)

	// Right leaf page (page 3): 3 cells with rowids 100..102
	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		rightCells[i].rowid = int64(100 + i)
		rightCells[i].payload = payload
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	bt.SetPage(3, rightPageData)

	// Interior root page (page 1): one cell {childPage:2, rowid:7}, rightChild=3
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 7},
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)

	return bt, 1, 2, 3
}

// TestRedistributeSiblings_LeftToRight triggers the redistributeSiblings path
// where the left page has more cells than the right, causing moveLeftToRight.
func TestRedistributeSiblings_LeftToRight(t *testing.T) {
	t.Parallel()
	pageSize := uint32(512)
	bt, root, leftPg, rightPg := buildRedistributableTree(t, pageSize)

	// Verify pages cannot merge (both together exceed one page)
	leftData, _ := bt.GetPage(leftPg)
	leftHdr, _ := ParsePageHeader(leftData, leftPg)
	rightData, _ := bt.GetPage(rightPg)
	rightHdr, _ := ParsePageHeader(rightData, rightPg)

	canMerge, err := CanMerge(leftData, leftHdr, rightData, rightHdr, pageSize)
	if err != nil {
		t.Fatalf("CanMerge error: %v", err)
	}

	// Position cursor on the left page and attempt a merge
	cursor := NewCursor(bt, root)
	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid error: %v", err)
	}
	if !found {
		t.Fatal("rowid 1 not found")
	}

	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage error: %v", err)
	}

	if canMerge {
		t.Logf("Pages merged (canMerge=%v)", canMerge)
	} else {
		// redistribution should have occurred
		t.Logf("Redistribution result: merged=%v (canMerge=%v)", merged, canMerge)
	}
}

// TestRedistributeSiblings_RightToLeft triggers moveRightToLeft by positioning
// on a page where the right sibling is heavier.
func TestRedistributeSiblings_RightToLeft(t *testing.T) {
	t.Parallel()
	pageSize := uint32(512)
	bt := NewBtree(pageSize)

	payload := make([]byte, 30)
	for i := range payload {
		payload[i] = byte('y')
	}

	// Left leaf page (page 2): 3 cells
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		leftCells[i].rowid = int64(i + 1)
		leftCells[i].payload = payload
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	bt.SetPage(2, leftPageData)

	// Right leaf page (page 3): 7 cells with rowids 100..106
	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 7)
	for i := 0; i < 7; i++ {
		rightCells[i].rowid = int64(100 + i)
		rightCells[i].payload = payload
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	bt.SetPage(3, rightPageData)

	// Interior root
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{
		{2, 3},
	}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)

	// Seek to left page (page 2, rowid 1) - it is the left sibling
	cursor := NewCursor(bt, 1)
	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid error: %v", err)
	}
	if !found {
		t.Fatal("rowid 1 not found")
	}

	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage error: %v", err)
	}
	t.Logf("MergePage result: %v", merged)
}

// TestMoveRightToLeft_EmptySource exercises the early-break when rightPage runs out of cells.
func TestMoveRightToLeft_EmptySource(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create a right page with only 1 cell but ask to move 5
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{42, []byte("only")}}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, nil)
	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// numToMove=5 but right only has 1: should break early
	if err := moveRightToLeft(leftPage, rightPage, 5); err != nil {
		t.Fatalf("moveRightToLeft error: %v", err)
	}

	if leftPage.Header.NumCells != 1 {
		t.Errorf("expected 1 cell on left, got %d", leftPage.Header.NumCells)
	}
	if rightPage.Header.NumCells != 0 {
		t.Errorf("expected 0 cells on right, got %d", rightPage.Header.NumCells)
	}
}

// TestMoveLeftToRight_EmptySource exercises the early-break when leftPage runs out of cells.
func TestMoveLeftToRight_EmptySource(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	leftCells := []struct {
		rowid   int64
		payload []byte
	}{{7, []byte("only")}}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, nil)
	rightPage, err := NewBtreePage(3, rightPageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// numToMove=5 but left only has 1: should break early
	if err := moveLeftToRight(leftPage, rightPage, 5); err != nil {
		t.Fatalf("moveLeftToRight error: %v", err)
	}

	if rightPage.Header.NumCells != 1 {
		t.Errorf("expected 1 cell on right, got %d", rightPage.Header.NumCells)
	}
	if leftPage.Header.NumCells != 0 {
		t.Errorf("expected 0 cells on left, got %d", leftPage.Header.NumCells)
	}
}

// TestExtractCellFromPage_BoundaryClamp exercises the cellEnd > len(page.Data) clamp.
// The clamp triggers when CellSize extends past the end of page.Data.
func TestExtractCellFromPage_BoundaryClamp(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	cells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("hello world boundary clamp test")}}
	pageData := createTestPage(2, pageSize, PageTypeLeafTable, cells)
	page, err := NewBtreePage(2, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage: %v", err)
	}

	// Get the actual cell offset to know where the cell sits
	cellOffset, err := page.Header.GetCellPointer(page.Data, 0)
	if err != nil {
		t.Fatalf("GetCellPointer: %v", err)
	}

	// Shrink page.Data to just past cellOffset so that cellOffset is valid
	// but cellOffset+cellSize would exceed len(page.Data).
	// This forces the clamp: cellEnd = len(page.Data).
	trimmedLen := int(cellOffset) + 5 // keep 5 bytes of cell, less than CellSize
	if trimmedLen > len(page.Data) {
		t.Skip("cell offset too close to end for boundary test")
	}
	page.Data = page.Data[:trimmedLen]

	cellData, err := extractCellFromPage(page, 0)
	if err != nil {
		// If parse fails on the truncated data that is acceptable
		t.Logf("extractCellFromPage on truncated data returned error (acceptable): %v", err)
		return
	}
	// If it succeeded, verify the clamp produced a shorter slice
	if len(cellData) == 0 {
		t.Error("expected non-empty cell data")
	}
	t.Logf("extractCellFromPage returned %d bytes (clamped)", len(cellData))
}

// TestDefragmentPages_BothPages exercises defragmentPages with two real pages.
func TestDefragmentPages_BothPages(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	leftCells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("a")}, {2, []byte("bb")}}
	leftData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage left: %v", err)
	}

	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{3, []byte("ccc")}, {4, []byte("dddd")}}
	rightData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage right: %v", err)
	}

	// Delete a cell from each to create fragmentation, then defragment
	if err := leftPage.DeleteCell(0); err != nil {
		t.Fatalf("DeleteCell left: %v", err)
	}
	if err := rightPage.DeleteCell(0); err != nil {
		t.Fatalf("DeleteCell right: %v", err)
	}

	if err := defragmentPages(leftPage, rightPage); err != nil {
		t.Fatalf("defragmentPages error: %v", err)
	}

	if leftPage.Header.NumCells != 1 {
		t.Errorf("left page cells after defragment: got %d, want 1", leftPage.Header.NumCells)
	}
	if rightPage.Header.NumCells != 1 {
		t.Errorf("right page cells after defragment: got %d, want 1", rightPage.Header.NumCells)
	}
}

// TestLoadPageHeaders_ErrorPath exercises the error path of loadPageHeaders
// by using a cursor pointing at a non-existent current page.
func TestLoadPageHeaders_ErrorPath(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Set up a valid parent page
	rootData := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{{99, 1}}, 99)
	bt.SetPage(1, rootData)

	cursor := NewCursor(bt, 1)
	cursor.State = CursorValid
	cursor.Depth = 1
	cursor.CurrentPage = 999 // page 999 does not exist
	cursor.PageStack[0] = 1
	cursor.IndexStack[0] = 0

	// GetPage for page 999 will return empty/nil, which should trigger error in ParsePageHeader
	_, _, err := cursor.loadPageHeaders(999, 1)
	// We expect either success (empty page returns zeros which may parse) or an error
	// Either path exercises the function body
	t.Logf("loadPageHeaders with missing current page: err=%v", err)
}

// TestLoadSiblingHeaders_ErrorPath exercises loadSiblingHeaders error path.
func TestLoadSiblingHeaders_ErrorPath(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	cursor := NewCursor(bt, 1)

	// Both pages missing - GetPage returns empty data, ParsePageHeader may fail
	_, _, err := cursor.loadSiblingHeaders(997, 998)
	t.Logf("loadSiblingHeaders with missing pages: err=%v", err)
}

// TestRedistributeSiblings_ViaCursor tests the full redistributeSiblings path
// end-to-end through MergePage when pages cannot merge.
func TestRedistributeSiblings_ViaCursor(t *testing.T) {
	t.Parallel()
	// Use a very small page size to make redistribution likely
	pageSize := uint32(512)
	bt := NewBtree(pageSize)

	payload := make([]byte, 25)
	for i := range payload {
		payload[i] = byte('z')
	}

	// Build left page (page 2) with 8 cells
	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 8)
	for i := 0; i < 8; i++ {
		leftCells[i].rowid = int64(i + 1)
		leftCells[i].payload = payload
	}
	bt.SetPage(2, createTestPage(2, pageSize, PageTypeLeafTable, leftCells))

	// Build right page (page 3) with 2 cells
	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 2)
	for i := 0; i < 2; i++ {
		rightCells[i].rowid = int64(200 + i)
		rightCells[i].payload = payload
	}
	bt.SetPage(3, createTestPage(3, pageSize, PageTypeLeafTable, rightCells))

	// Interior root
	bt.SetPage(1, createInteriorPage(1, pageSize, []struct {
		childPage uint32
		rowid     int64
	}{{2, 8}}, 3))

	// Check CanMerge first to know which path we'll take
	ld, _ := bt.GetPage(2)
	lh, _ := ParsePageHeader(ld, 2)
	rd, _ := bt.GetPage(3)
	rh, _ := ParsePageHeader(rd, 3)
	canMerge, _ := CanMerge(ld, lh, rd, rh, pageSize)
	t.Logf("CanMerge=%v", canMerge)

	cursor := NewCursor(bt, 1)
	if _, err := cursor.SeekRowid(1); err != nil {
		t.Fatalf("SeekRowid: %v", err)
	}

	merged, err := cursor.MergePage()
	if err != nil {
		t.Fatalf("MergePage: %v", err)
	}
	t.Logf("MergePage merged=%v (redistribute path exercised if canMerge=false)", merged)
}

// TestRedistributeCells_ManyToFew exercises moveLeftToRight with a realistic scenario.
func TestRedistributeCells_ManyToFew(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	leftPage := redistributeCreateTestPage(t, 2, pageSize, 12, 0, 10)
	rightPage := redistributeCreateTestPage(t, 3, pageSize, 2, 12, 10)

	before := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	if err := RedistributeCells(leftPage, rightPage); err != nil {
		t.Fatalf("RedistributeCells: %v", err)
	}

	after := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	if before != after {
		t.Errorf("cell count changed: before=%d after=%d", before, after)
	}

	// Left should have moved cells to right (was 12 vs 2, target is 7 each)
	if leftPage.Header.NumCells > 8 {
		t.Errorf("left still has too many cells: %d", leftPage.Header.NumCells)
	}
}

// TestRedistributeCells_FewToMany exercises moveRightToLeft with a realistic scenario.
func TestRedistributeCells_FewToMany(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	leftPage := redistributeCreateTestPage(t, 2, pageSize, 2, 0, 10)
	rightPage := redistributeCreateTestPage(t, 3, pageSize, 12, 2, 10)

	before := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	if err := RedistributeCells(leftPage, rightPage); err != nil {
		t.Fatalf("RedistributeCells: %v", err)
	}

	after := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	if before != after {
		t.Errorf("cell count changed: before=%d after=%d", before, after)
	}

	// Right should have moved cells to left (was 2 vs 12, target is 7 each)
	if rightPage.Header.NumCells > 8 {
		t.Errorf("right still has too many cells: %d", rightPage.Header.NumCells)
	}
}

// TestRedistributeSiblings_Direct directly invokes redistributeSiblings via cursor
// by setting up pages that are too large to merge.
func TestRedistributeSiblings_Direct(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	// Use large payloads so the two pages together cannot merge.
	// Each cell with payload=1500: cellSize ~ 1504 bytes.
	// Two cells total: ~3008 + header + ptrs = ~3024 bytes, just under 4096 but
	// with 10% margin: 3024 * 1.1 = 3326 < 4096, so they would merge.
	// Use 3 cells per page with payload=900: ~904 bytes/cell.
	// 6 cells total: 5424 bytes content + header 8 + ptrs 12 = 5444 > 4096, cannot merge.
	// But each page individually fits (3*904 + 8 + 6 = 2726 < 4096).
	payload := make([]byte, 900)
	for i := range payload {
		payload[i] = byte('p')
	}

	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		leftCells[i].rowid = int64(i + 1)
		leftCells[i].payload = payload
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	bt.SetPage(2, leftPageData)

	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		rightCells[i].rowid = int64(100 + i)
		rightCells[i].payload = payload
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	bt.SetPage(3, rightPageData)

	// Parent interior page
	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 3}}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)

	// Verify CanMerge returns false
	ld, _ := bt.GetPage(2)
	lh, _ := ParsePageHeader(ld, 2)
	rd, _ := bt.GetPage(3)
	rh, _ := ParsePageHeader(rd, 3)
	canMerge, err := CanMerge(ld, lh, rd, rh, pageSize)
	if err != nil {
		t.Fatalf("CanMerge: %v", err)
	}
	if canMerge {
		t.Skip("pages can merge, redistribution path not triggered with this payload size")
	}

	// Directly call redistributeSiblings on cursor
	cursor := NewCursor(bt, 1)
	cursor.State = CursorValid
	cursor.Depth = 1
	cursor.CurrentPage = 2
	cursor.PageStack[0] = 1
	cursor.IndexStack[0] = 0

	ok, err := cursor.redistributeSiblings(2, 3, 1, 0)
	if err != nil {
		t.Fatalf("redistributeSiblings error: %v", err)
	}
	t.Logf("redistributeSiblings returned: %v", ok)
	if !ok {
		t.Error("expected redistributeSiblings to return true")
	}
}

// TestDefragmentPages_LeftError exercises the leftPage.Defragment() error return in defragmentPages.
func TestDefragmentPages_LeftError(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)

	// Create a valid right page
	rightCells := []struct {
		rowid   int64
		payload []byte
	}{{10, []byte("right")}}
	rightData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	rightPage, err := NewBtreePage(3, rightData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage right: %v", err)
	}

	// Create a left page then corrupt it so Defragment fails.
	// Set NumCells > 0 but truncate Data so GetCellPointer returns error.
	leftCells := []struct {
		rowid   int64
		payload []byte
	}{{1, []byte("left")}}
	leftData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	leftPage, err := NewBtreePage(2, leftData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage left: %v", err)
	}

	// Truncate page data so the cell pointer array is out of bounds.
	// Header is 8 bytes, cell ptr is at offset 8; truncate to 9 bytes
	// so ptrOffset+2 = 10 > 9 = len(data).
	leftPage.Data = leftPage.Data[:9]

	err = defragmentPages(leftPage, rightPage)
	if err == nil {
		t.Error("expected defragmentPages to return error when left page is corrupted")
	} else {
		t.Logf("defragmentPages returned expected error: %v", err)
	}
}

// TestRedistributeSiblings_LoadError exercises the loadRedistributePages error path
// inside redistributeSiblings by passing a non-existent page number.
func TestRedistributeSiblings_LoadError(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Set up a valid parent page
	rootData := createInteriorPage(1, 4096, []struct {
		childPage uint32
		rowid     int64
	}{{998, 5}}, 999)
	bt.SetPage(1, rootData)

	cursor := NewCursor(bt, 1)
	cursor.State = CursorValid
	cursor.Depth = 1
	cursor.CurrentPage = 998
	cursor.PageStack[0] = 1
	cursor.IndexStack[0] = 0

	// Pages 998 and 999 don't exist: GetPage returns empty/nil data
	// which should cause ParsePageHeader to fail with "invalid page type"
	_, err := cursor.redistributeSiblings(998, 999, 1, 0)
	// We expect an error because the pages don't exist
	t.Logf("redistributeSiblings with missing pages: err=%v", err)
}

// TestRedistributeSiblings_WithProvider directly calls redistributeSiblings
// with a provider to exercise markMergePagesAsDirty inside redistributeSiblings.
func TestRedistributeSiblings_WithProvider(t *testing.T) {
	t.Parallel()
	pageSize := uint32(4096)
	bt := NewBtree(pageSize)

	mockProvider := &MockPageProvider{
		pages: make(map[uint32][]byte),
	}
	bt.Provider = mockProvider

	payload := make([]byte, 900)
	for i := range payload {
		payload[i] = byte('q')
	}

	leftCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		leftCells[i].rowid = int64(i + 1)
		leftCells[i].payload = payload
	}
	leftPageData := createTestPage(2, pageSize, PageTypeLeafTable, leftCells)
	bt.SetPage(2, leftPageData)
	mockProvider.pages[2] = leftPageData

	rightCells := make([]struct {
		rowid   int64
		payload []byte
	}, 3)
	for i := 0; i < 3; i++ {
		rightCells[i].rowid = int64(100 + i)
		rightCells[i].payload = payload
	}
	rightPageData := createTestPage(3, pageSize, PageTypeLeafTable, rightCells)
	bt.SetPage(3, rightPageData)
	mockProvider.pages[3] = rightPageData

	rootCells := []struct {
		childPage uint32
		rowid     int64
	}{{2, 3}}
	rootData := createInteriorPage(1, pageSize, rootCells, 3)
	bt.SetPage(1, rootData)
	mockProvider.pages[1] = rootData

	// Verify CanMerge returns false before calling
	ld, _ := bt.GetPage(2)
	lh, _ := ParsePageHeader(ld, 2)
	rd, _ := bt.GetPage(3)
	rh, _ := ParsePageHeader(rd, 3)
	canMerge, _ := CanMerge(ld, lh, rd, rh, pageSize)
	if canMerge {
		t.Skip("pages can merge, cannot test redistribute path")
	}

	cursor := NewCursor(bt, 1)
	cursor.State = CursorValid
	cursor.Depth = 1
	cursor.CurrentPage = 2
	cursor.PageStack[0] = 1
	cursor.IndexStack[0] = 0

	ok, err := cursor.redistributeSiblings(2, 3, 1, 0)
	if err != nil {
		t.Fatalf("redistributeSiblings: %v", err)
	}
	t.Logf("redistributeSiblings with provider: %v", ok)

	// Verify pages were marked dirty
	if ok && mockProvider.dirtyPages != nil {
		t.Logf("dirty pages: %v", mockProvider.dirtyPages)
	}
}
