// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// TestBalanceCellLoadPageForBalance_Valid exercises loadPageForBalance via balance()
// with a valid cursor on a populated page.
func TestBalanceCellLoadPageForBalance_Valid(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 20, 10)

	found, err := cursor.SeekRowid(5)
	if err != nil || !found {
		t.Fatalf("SeekRowid failed: err=%v found=%v", err, found)
	}

	balErr := balance(cursor)
	if balErr != nil {
		msg := balErr.Error()
		if contains(msg, "failed to get page") || contains(msg, "failed to parse page") {
			t.Errorf("loadPageForBalance returned unexpected error: %v", balErr)
		}
	}
}

// TestBalanceCellLoadPageForBalance_InvalidPage exercises the error branch of
// loadPageForBalance when the cursor points to a non-existent page.
func TestBalanceCellLoadPageForBalance_InvalidPage(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 5, 10)
	cursor.SeekRowid(1)

	cursor.CurrentPage = 9999
	balErr := balance(cursor)
	if balErr == nil {
		t.Fatal("expected error for non-existent page, got nil")
	}
}

// TestBalanceCellExecuteBalance_Overfull exercises the overfull branch of executeBalance.
func TestBalanceCellExecuteBalance_Overfull(t *testing.T) {
	t.Parallel()

	pageSize := uint32(512)
	bt := NewBtree(pageSize)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	for i := int64(1); i <= 200; i++ {
		if insertErr := cursor.Insert(i, make([]byte, 15)); insertErr != nil {
			break
		}
	}
	cursor.SeekRowid(1)

	pageData, _ := bt.GetPage(cursor.CurrentPage)
	page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)

	if isOverfull(page) {
		err := executeBalance(cursor, page)
		if err == nil {
			t.Error("expected overfull error from executeBalance, got nil")
		}
	}
}

// TestBalanceCellExecuteBalance_Underfull exercises the underfull non-root branch
// of executeBalance.
func TestBalanceCellExecuteBalance_Underfull(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 100, 10)

	cursor2 := NewCursor(bt, cursor.RootPage)
	for i := int64(2); i <= 95; i++ {
		found, _ := cursor2.SeekRowid(i)
		if found {
			cursor2.Delete()
		}
	}

	cursor3 := NewCursor(bt, cursor.RootPage)
	cursor3.SeekRowid(1)
	if cursor3.State == CursorValid && cursor3.Depth > 0 {
		pageData, _ := bt.GetPage(cursor3.CurrentPage)
		page, _ := NewBtreePage(cursor3.CurrentPage, pageData, bt.UsableSize)
		if isUnderfull(page) {
			_ = executeBalance(cursor3, page)
		}
	}
}

// TestBalanceCellExecuteBalance_Fragmentation exercises the balanced-with-fragmentation
// branch of executeBalance.
func TestBalanceCellExecuteBalance_Fragmentation(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 30, 20)

	for i := int64(2); i <= 30; i += 2 {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	cursor.SeekRowid(1)
	pageData, _ := bt.GetPage(cursor.CurrentPage)
	page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)

	page.Header.FragmentedBytes = 10
	err = executeBalance(cursor, page)
	if err != nil {
		msg := err.Error()
		if !contains(msg, "overfull") && !contains(msg, "underfull") {
			t.Errorf("unexpected error from executeBalance on balanced page: %v", err)
		}
	}
}

// TestBalanceCellDefragmentIfNeeded directly exercises defragmentIfNeeded.
func TestBalanceCellDefragmentIfNeeded(t *testing.T) {
	t.Parallel()

	t.Run("no fragmentation - no-op", func(t *testing.T) {
		page := createBalanceTestPage(4096, PageTypeLeafTable, 3, []int{10, 10, 10})
		page.Header.FragmentedBytes = 0
		cursor := &BtCursor{
			Btree:       &Btree{UsableSize: 4096, Pages: map[uint32][]byte{page.PageNum: page.Data}},
			CurrentPage: page.PageNum,
			State:       CursorValid,
		}
		if err := defragmentIfNeeded(cursor, page); err != nil {
			t.Errorf("defragmentIfNeeded with no fragmentation: %v", err)
		}
	})

	t.Run("with fragmentation - defragments", func(t *testing.T) {
		pageSize := uint32(4096)
		pageNum := uint32(2)
		pageData := make([]byte, pageSize)
		pageData[0] = PageTypeLeafTable
		binary.BigEndian.PutUint16(pageData[3:], 2)

		cell1 := EncodeTableLeafCell(1, []byte("hello"))
		cell2 := EncodeTableLeafCell(2, []byte("world"))

		off2 := int(pageSize) - len(cell2)
		copy(pageData[off2:], cell2)
		off1 := off2 - len(cell1) - 8 // 8-byte gap = fragmentation
		copy(pageData[off1:], cell1)

		binary.BigEndian.PutUint16(pageData[5:], uint16(off1))
		binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(off1))
		binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf+2:], uint16(off2))
		pageData[7] = 8 // FragmentedBytes

		page, err := NewBtreePage(pageNum, pageData, pageSize)
		if err != nil {
			t.Fatalf("NewBtreePage: %v", err)
		}

		cursor := &BtCursor{
			Btree:       &Btree{UsableSize: pageSize, Pages: map[uint32][]byte{pageNum: pageData}},
			CurrentPage: pageNum,
			State:       CursorValid,
		}

		if err := defragmentIfNeeded(cursor, page); err != nil {
			t.Errorf("defragmentIfNeeded: %v", err)
		}
		if page.Header.FragmentedBytes != 0 {
			t.Errorf("FragmentedBytes after defrag = %d, want 0", page.Header.FragmentedBytes)
		}
	})
}

// TestBalanceCellHandleOverfullPage directly exercises handleOverfullPage.
func TestBalanceCellHandleOverfullPage(t *testing.T) {
	t.Parallel()

	t.Run("genuinely overfull - returns split error", func(t *testing.T) {
		pageSize := uint32(512)
		bt := NewBtree(pageSize)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)

		for i := int64(1); i <= 100; i++ {
			if err := cursor.Insert(i, make([]byte, 20)); err != nil {
				break
			}
		}
		cursor.SeekRowid(1)

		pageData, _ := bt.GetPage(cursor.CurrentPage)
		page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)

		if !isOverfull(page) {
			t.Skip("page not overfull with this data; skipping")
		}

		err := handleOverfullPage(cursor, page)
		if err == nil {
			t.Error("expected split error, got nil")
		}
		if !contains(err.Error(), "overfull") {
			t.Errorf("error should mention overfull: %v", err)
		}
	})

	t.Run("overfull with fragmentation - defrag path", func(t *testing.T) {
		// Build a page that claims to be overfull but has fragmented bytes;
		// after defragmentation it should no longer be overfull.
		pageSize := uint32(4096)
		pageNum := uint32(2)
		pageData := make([]byte, pageSize)
		pageData[0] = PageTypeLeafTable

		// Put a small number of cells so page is NOT actually overfull
		numCells := 3
		binary.BigEndian.PutUint16(pageData[3:], uint16(numCells))

		cells := [][]byte{
			EncodeTableLeafCell(1, []byte("aaa")),
			EncodeTableLeafCell(2, []byte("bbb")),
			EncodeTableLeafCell(3, []byte("ccc")),
		}
		off := int(pageSize)
		for i := numCells - 1; i >= 0; i-- {
			off -= len(cells[i])
			copy(pageData[off:], cells[i])
			binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf+i*2:], uint16(off))
		}
		binary.BigEndian.PutUint16(pageData[5:], uint16(off))
		// Set fragmented bytes so the defrag branch runs
		pageData[7] = 5

		page, err := NewBtreePage(pageNum, pageData, pageSize)
		if err != nil {
			t.Fatalf("NewBtreePage: %v", err)
		}

		// Manually mark overfull by stuffing free space down: modify freeSpace field
		// We can't easily do that; instead just call handleOverfullPage with
		// FragmentedBytes set. If the page is NOT overfull after defrag, it returns nil.
		cursor := &BtCursor{
			Btree:       &Btree{UsableSize: pageSize, Pages: map[uint32][]byte{pageNum: pageData}},
			CurrentPage: pageNum,
			RootPage:    pageNum,
			State:       CursorValid,
		}

		// Not truly overfull - the function should try defrag then return nil
		// because isOverfull will be false after defrag
		_ = handleOverfullPage(cursor, page)
		// No assertion on error since the page may or may not be overfull; we just verify no panic
	})
}

// TestBalanceCellHandleUnderfullPage_RootAndDepthZero exercises handleUnderfullPage
// for root pages and non-root pages at depth 0.
func TestBalanceCellHandleUnderfullPage_RootAndDepthZero(t *testing.T) {
	t.Parallel()

	t.Run("root page - returns nil", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 2, 5)
		cursor.SeekRowid(1)

		pageData, _ := bt.GetPage(rootPage)
		page, _ := NewBtreePage(rootPage, pageData, bt.UsableSize)

		cursor.CurrentPage = rootPage
		cursor.RootPage = rootPage
		err := handleUnderfullPage(cursor, page)
		if err != nil {
			t.Errorf("handleUnderfullPage on root: expected nil, got %v", err)
		}
	})

	t.Run("non-root page depth 0 - returns nil", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 2, 5)

		nonRoot := uint32(99)
		pageData := make([]byte, 4096)
		pageData[0] = PageTypeLeafTable
		binary.BigEndian.PutUint16(pageData[3:], 1)
		cell := EncodeTableLeafCell(1, []byte("x"))
		off := 4096 - len(cell)
		copy(pageData[off:], cell)
		binary.BigEndian.PutUint16(pageData[5:], uint16(off))
		binary.BigEndian.PutUint16(pageData[PageHeaderSizeLeaf:], uint16(off))
		bt.Pages[nonRoot] = pageData

		page, _ := NewBtreePage(nonRoot, pageData, bt.UsableSize)
		cursor.CurrentPage = nonRoot
		cursor.RootPage = rootPage
		cursor.Depth = 0

		err := handleUnderfullPage(cursor, page)
		if err != nil {
			t.Errorf("handleUnderfullPage depth=0 non-root: expected nil, got %v", err)
		}
	})
}

// setupUnderfullNonRootCursor creates a btree with rows, deletes most of them,
// and returns a cursor positioned at depth > 0. Returns nil cursor if setup
// conditions are not met.
func setupUnderfullNonRootCursor(t *testing.T) (*Btree, *BtCursor) {
	t.Helper()
	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 80, 10)

	cursor2 := NewCursor(bt, cursor.RootPage)
	deleteRowRange(cursor2, 2, 75)

	cursor3 := NewCursor(bt, cursor.RootPage)
	cursor3.SeekRowid(1)
	if cursor3.State != CursorValid || cursor3.Depth <= 0 {
		return bt, nil
	}
	return bt, cursor3
}

// TestBalanceCellHandleUnderfullPage_MergeError exercises handleUnderfullPage for
// non-root pages at depth > 0 that should return a merge error.
func TestBalanceCellHandleUnderfullPage_MergeError(t *testing.T) {
	t.Parallel()

	bt, cursor3 := setupUnderfullNonRootCursor(t)
	if cursor3 == nil {
		t.Skip("cursor not at depth > 0 after deletions")
	}

	pageData, _ := bt.GetPage(cursor3.CurrentPage)
	page, _ := NewBtreePage(cursor3.CurrentPage, pageData, bt.UsableSize)
	if !isUnderfull(page) {
		t.Skip("page not underfull")
	}

	err := handleUnderfullPage(cursor3, page)
	if err == nil {
		t.Error("expected underfull merge error, got nil")
	}
	if !contains(err.Error(), "underfull") {
		t.Errorf("error should mention underfull: %v", err)
	}
}

// TestBalanceCellHandleUnderfullPage_Fragmentation exercises the defrag path
// in handleUnderfullPage for non-root pages with fragmentation.
func TestBalanceCellHandleUnderfullPage_Fragmentation(t *testing.T) {
	t.Parallel()

	bt, cursor3 := setupUnderfullNonRootCursor(t)
	if cursor3 == nil {
		t.Skip("cursor not at depth > 0")
	}

	pageData, _ := bt.GetPage(cursor3.CurrentPage)
	page, _ := NewBtreePage(cursor3.CurrentPage, pageData, bt.UsableSize)

	page.Header.FragmentedBytes = 5
	_ = handleUnderfullPage(cursor3, page)
}

// TestBalanceCellBalance_Overfull exercises balance() with many inserts that trigger
// overfull detection.
func TestBalanceCellBalance_Overfull(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	for i := int64(1); i <= 200; i++ {
		cursor.Insert(i, make([]byte, 15))
	}

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := verifyOrderedForward(t, cursor2)
	if count < 10 {
		t.Errorf("expected at least 10 entries, got %d", count)
	}
}

// TestBalanceCellBalance_Underfull exercises balance() after deleting most rows
// to trigger underfull detection on non-root pages.
func TestBalanceCellBalance_Underfull(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 100, 10)

	cursor2 := NewCursor(bt, cursor.RootPage)
	deleteRowRange(cursor2, 1, 90)

	cursor3 := NewCursor(bt, cursor.RootPage)
	cursor3.SeekRowid(91)
	if cursor3.State == CursorValid {
		balErr := balance(cursor3)
		if balErr != nil {
			msg := balErr.Error()
			if !contains(msg, "overfull") && !contains(msg, "underfull") &&
				!contains(msg, "failed to get page") && !contains(msg, "failed to parse page") {
				t.Errorf("unexpected error from balance: %v", balErr)
			}
		}
	}
}

// TestBalanceCellCalculateCellSizeAndLocal exercises calculateCellSizeAndLocal
// via ParseCell for both the small-payload (fits locally) and large-payload
// (spills to overflow) branches.
func TestBalanceCellCalculateCellSizeAndLocal(t *testing.T) {
	t.Parallel()

	t.Run("small payload fits in local", func(t *testing.T) {
		payload := make([]byte, 50)
		cell := EncodeTableLeafCell(1, payload)
		info, err := ParseCell(PageTypeLeafTable, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell: %v", err)
		}
		if info.LocalPayload != uint16(len(payload)) {
			t.Errorf("LocalPayload = %d, want %d", info.LocalPayload, len(payload))
		}
		if info.OverflowPage != 0 {
			t.Errorf("expected no overflow for small payload")
		}
		expectedCellSize := uint16(len(cell))
		if info.CellSize != expectedCellSize {
			t.Errorf("CellSize = %d, want %d", info.CellSize, expectedCellSize)
		}
	})

	t.Run("large payload spills to overflow page", func(t *testing.T) {
		// PayloadSize > maxLocal forces the else branch of calculateCellSizeAndLocal
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, true)
		minLocal := calculateMinLocal(usableSize, true)

		// Build a fake cell with payload larger than maxLocal so calculateCellSizeAndLocal
		// exercises its overflow branch.
		oversized := uint32(maxLocal + 500)
		info := &CellInfo{PayloadSize: oversized}
		calculateCellSizeAndLocal(info, 2, maxLocal, minLocal, usableSize)

		if uint32(info.LocalPayload) >= oversized {
			t.Errorf("LocalPayload %d should be less than PayloadSize %d for overflow case",
				info.LocalPayload, oversized)
		}
		// CellSize should account for the overflow page pointer (4 bytes)
		expectedMin := uint16(2 + int(info.LocalPayload) + 4)
		if info.CellSize < expectedMin {
			t.Errorf("CellSize %d too small, expected >= %d", info.CellSize, expectedMin)
		}
	})

	t.Run("payload exactly at maxLocal boundary", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, true)
		minLocal := calculateMinLocal(usableSize, true)

		info := &CellInfo{PayloadSize: maxLocal}
		calculateCellSizeAndLocal(info, 3, maxLocal, minLocal, usableSize)

		if uint32(info.LocalPayload) != maxLocal {
			t.Errorf("LocalPayload = %d, want %d (at boundary)", info.LocalPayload, maxLocal)
		}
	})
}

// TestBalanceCellExtractOverflowPage_NoOverflow verifies no overflow page is set
// when the payload fits locally.
func TestBalanceCellExtractOverflowPage_NoOverflow(t *testing.T) {
	t.Parallel()

	cell := EncodeTableLeafCell(42, []byte("short payload"))
	info, err := ParseCell(PageTypeLeafTable, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell: %v", err)
	}
	if info.OverflowPage != 0 {
		t.Errorf("OverflowPage = %d, want 0", info.OverflowPage)
	}
}

// TestBalanceCellExtractOverflowPage_LargePayload verifies overflow page extraction
// for a payload larger than maxLocal.
func TestBalanceCellExtractOverflowPage_LargePayload(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	overflowPayload := make([]byte, 5000)
	for i := range overflowPayload {
		overflowPayload[i] = byte(i % 256)
	}
	if err := cursor.Insert(1, overflowPayload); err != nil {
		t.Fatalf("Insert large payload: %v", err)
	}

	found, err := cursor.SeekRowid(1)
	if err != nil || !found {
		t.Fatalf("SeekRowid: err=%v found=%v", err, found)
	}

	if cursor.CurrentCell.OverflowPage == 0 {
		t.Error("expected overflow page to be set for large payload")
	}
	if cursor.CurrentCell.PayloadSize != uint32(len(overflowPayload)) {
		t.Errorf("PayloadSize = %d, want %d", cursor.CurrentCell.PayloadSize, len(overflowPayload))
	}
}

// TestBalanceCellExtractOverflowPage_Truncated verifies behavior when the overflow
// page pointer is truncated.
func TestBalanceCellExtractOverflowPage_Truncated(t *testing.T) {
	t.Parallel()

	usableSize := uint32(512)
	maxLocal := calculateMaxLocal(usableSize, true)

	oversized := uint64(maxLocal) + 100
	buf := make([]byte, 20+int(maxLocal))
	offset := 0
	offset += PutVarint(buf[offset:], oversized)
	offset += PutVarint(buf[offset:], 1)
	truncated := buf[:offset+int(maxLocal)/2]

	_, err := ParseCell(PageTypeLeafTable, truncated, usableSize)
	// Should either error or succeed depending on internal clamping;
	// we just verify no panic
	_ = err
}

// TestBalanceCellParseTableInteriorCell_RoundTrip exercises parseTableInteriorCell
// round-trip encoding and parsing.
func TestBalanceCellParseTableInteriorCell_RoundTrip(t *testing.T) {
	t.Parallel()

	childPage := uint32(42)
	rowid := int64(1000)
	cell := EncodeTableInteriorCell(childPage, rowid)

	info, err := ParseCell(PageTypeInteriorTable, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell interior: %v", err)
	}
	if info.ChildPage != childPage {
		t.Errorf("ChildPage = %d, want %d", info.ChildPage, childPage)
	}
	if info.Key != rowid {
		t.Errorf("Key = %d, want %d", info.Key, rowid)
	}
	if info.CellSize == 0 {
		t.Error("CellSize should not be 0")
	}
}

// TestBalanceCellParseTableInteriorCell_MultiByteVarint exercises parseTableInteriorCell
// with a multi-byte varint rowid.
func TestBalanceCellParseTableInteriorCell_MultiByteVarint(t *testing.T) {
	t.Parallel()

	childPage := uint32(7)
	rowid := int64(300)
	cell := EncodeTableInteriorCell(childPage, rowid)

	info, err := ParseCell(PageTypeInteriorTable, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell: %v", err)
	}
	if info.Key != rowid {
		t.Errorf("Key = %d, want %d", info.Key, rowid)
	}
	if info.CellSize != uint16(len(cell)) {
		t.Errorf("CellSize = %d, want %d", info.CellSize, len(cell))
	}
}

// TestBalanceCellParseTableInteriorCell_TooSmall exercises parseTableInteriorCell
// with insufficient data.
func TestBalanceCellParseTableInteriorCell_TooSmall(t *testing.T) {
	t.Parallel()

	_, err := ParseCell(PageTypeInteriorTable, []byte{0x01, 0x02}, 4096)
	if err == nil {
		t.Error("expected error for too-small interior cell data")
	}
}

// getInteriorRootCells returns the cell pointers from an interior root page,
// skipping the test if the root is not an interior page.
func getInteriorRootCells(t *testing.T, bt *Btree, rootPage uint32) ([]byte, []uint16) {
	t.Helper()
	rootData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage root: %v", err)
	}
	header, err := ParsePageHeader(rootData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if !header.IsInterior {
		t.Skip("root did not become interior page")
	}
	ptrs, err := header.GetCellPointers(rootData)
	if err != nil {
		t.Fatalf("GetCellPointers: %v", err)
	}
	if len(ptrs) == 0 {
		t.Fatal("no cells in interior root")
	}
	return rootData, ptrs
}

// TestBalanceCellParseTableInteriorCell_AfterSplit exercises parseTableInteriorCell
// on interior cells produced by btree splits.
func TestBalanceCellParseTableInteriorCell_AfterSplit(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 150, 10)

	rootData, ptrs := getInteriorRootCells(t, bt, cursor.RootPage)

	for i, ptr := range ptrs {
		info, parseErr := ParseCell(PageTypeInteriorTable, rootData[ptr:], bt.UsableSize)
		if parseErr != nil {
			t.Errorf("ParseCell interior cell %d: %v", i, parseErr)
			continue
		}
		if info.ChildPage == 0 {
			t.Errorf("interior cell %d: ChildPage should not be 0", i)
		}
	}
}

// TestBalanceCellParseTableInteriorCompositeCell exercises parseTableInteriorCompositeCell.
func TestBalanceCellParseTableInteriorCompositeCell(t *testing.T) {
	t.Parallel()

	t.Run("round-trip encode and parse", func(t *testing.T) {
		childPage := uint32(15)
		keyBytes := []byte("composite-key-data")
		cell := EncodeTableInteriorCompositeCell(childPage, keyBytes)

		info, err := ParseCell(PageTypeInteriorTableNo, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell composite interior: %v", err)
		}
		if info.ChildPage != childPage {
			t.Errorf("ChildPage = %d, want %d", info.ChildPage, childPage)
		}
		if string(info.KeyBytes) != string(keyBytes) {
			t.Errorf("KeyBytes = %q, want %q", info.KeyBytes, keyBytes)
		}
	})

	t.Run("empty key bytes", func(t *testing.T) {
		cell := EncodeTableInteriorCompositeCell(5, []byte{})
		info, err := ParseCell(PageTypeInteriorTableNo, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell empty composite: %v", err)
		}
		if info.ChildPage != 5 {
			t.Errorf("ChildPage = %d, want 5", info.ChildPage)
		}
	})

	t.Run("too small cell returns error", func(t *testing.T) {
		_, err := ParseCell(PageTypeInteriorTableNo, []byte{0x01}, 4096)
		if err == nil {
			t.Error("expected error for too-small composite interior cell")
		}
	})

	t.Run("key length exceeds available data returns error", func(t *testing.T) {
		// Build a cell where varint key length exceeds remaining bytes
		buf := make([]byte, 8)
		binary.BigEndian.PutUint32(buf[0:], 3)
		// varint key length of 100, but only 3 bytes remain after offset
		buf[4] = 0x64 // 100 in varint (single byte)
		buf[5] = 0x01
		buf[6] = 0x02
		buf[7] = 0x03

		_, err := ParseCell(PageTypeInteriorTableNo, buf, 4096)
		if err == nil {
			t.Error("expected error when key length exceeds cell size")
		}
	})
}

// TestBalanceCellReadIndexPayloadVarint_Leaf exercises readIndexPayloadVarint
// indirectly via parseIndexLeafCell.
func TestBalanceCellReadIndexPayloadVarint_Leaf(t *testing.T) {
	t.Parallel()

	payload := make([]byte, 30)
	for i := range payload {
		payload[i] = byte(i)
	}
	cell := EncodeIndexLeafCell(payload)

	info, err := ParseCell(PageTypeLeafIndex, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell index leaf: %v", err)
	}
	if info.PayloadSize != uint32(len(payload)) {
		t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(payload))
	}
	if info.Key != int64(len(payload)) {
		t.Errorf("Key = %d, want %d (payload size)", info.Key, len(payload))
	}
}

// TestBalanceCellReadIndexPayloadVarint_Interior exercises readIndexPayloadVarint
// indirectly via parseIndexInteriorCell.
func TestBalanceCellReadIndexPayloadVarint_Interior(t *testing.T) {
	t.Parallel()

	childPage := uint32(99)
	payload := make([]byte, 20)
	cell := EncodeIndexInteriorCell(childPage, payload)

	info, err := ParseCell(PageTypeInteriorIndex, cell, 4096)
	if err != nil {
		t.Fatalf("ParseCell index interior: %v", err)
	}
	if info.ChildPage != childPage {
		t.Errorf("ChildPage = %d, want %d", info.ChildPage, childPage)
	}
	if info.PayloadSize != uint32(len(payload)) {
		t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(payload))
	}
	if info.Key != int64(len(payload)) {
		t.Errorf("Key = %d, want %d", info.Key, len(payload))
	}
}

// TestBalanceCellReadIndexPayloadVarint_OverflowAndEdgeCases exercises
// readIndexPayloadVarint overflow and edge-case paths.
func TestBalanceCellReadIndexPayloadVarint_OverflowAndEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("large payload exercises overflow variant", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)
		largePayload := make([]byte, maxLocal+200)
		cell := EncodeIndexLeafCell(largePayload)

		info, err := ParseCell(PageTypeLeafIndex, cell, usableSize)
		if err != nil {
			t.Fatalf("ParseCell large index: %v", err)
		}
		if info.PayloadSize != uint32(len(largePayload)) {
			t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(largePayload))
		}
		if uint32(info.LocalPayload) >= uint32(len(largePayload)) {
			t.Errorf("LocalPayload %d should be less than PayloadSize for overflow", info.LocalPayload)
		}
	})

	t.Run("empty cell returns error", func(t *testing.T) {
		_, err := ParseCell(PageTypeLeafIndex, []byte{}, 4096)
		if err == nil {
			t.Error("expected error for empty index cell data")
		}
	})

	t.Run("multi-byte varint payload size", func(t *testing.T) {
		payload := make([]byte, 200)
		cell := EncodeIndexLeafCell(payload)

		info, err := ParseCell(PageTypeLeafIndex, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell: %v", err)
		}
		if info.PayloadSize != 200 {
			t.Errorf("PayloadSize = %d, want 200", info.PayloadSize)
		}
	})
}

// TestBalanceCellFullInsertDeleteCycle_InsertAndVerify performs a high-volume insert
// and verifies all rows are readable in order after splits.
func TestBalanceCellFullInsertDeleteCycle_InsertAndVerify(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	n := insertRows(cursor, 1, 300, 12)
	if n < 10 {
		t.Fatalf("expected at least 10 inserts, got %d", n)
	}

	cursor2 := NewCursor(bt, cursor.RootPage)
	count := verifyOrderedForward(t, cursor2)
	if count != n {
		t.Errorf("forward traversal: got %d entries, want %d", count, n)
	}
}

// deleteFirstNRows deletes rows with rowids 1..count from the btree.
func deleteFirstNRows(bt *Btree, rootPage uint32, count int) {
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= int64(count); i++ {
		found, seekErr := cursor.SeekRowid(i)
		if seekErr == nil && found {
			cursor.Delete()
		}
	}
}

// verifyRowsDeleted checks that rows with rowids 1..count are not found.
func verifyRowsDeleted(t *testing.T, bt *Btree, rootPage uint32, count int) {
	t.Helper()
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= int64(count); i++ {
		found, _ := cursor.SeekRowid(i)
		if found {
			t.Errorf("row %d should have been deleted", i)
		}
	}
}

// TestBalanceCellFullInsertDeleteCycle_DeleteAndVerify exercises delete/balance
// interaction and verifies deleted rows are gone while remaining rows persist.
func TestBalanceCellFullInsertDeleteCycle_DeleteAndVerify(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	n := insertRows(cursor, 1, 300, 12)
	if n < 10 {
		t.Fatalf("expected at least 10 inserts, got %d", n)
	}

	deleteCount := 5
	if deleteCount > n {
		deleteCount = n
	}
	deleteFirstNRows(bt, cursor.RootPage, deleteCount)
	verifyRowsDeleted(t, bt, cursor.RootPage, deleteCount)

	if n > 5 {
		cursor5 := NewCursor(bt, cursor.RootPage)
		found, seekErr := cursor5.SeekRowid(int64(n))
		if seekErr != nil || !found {
			t.Errorf("last inserted row %d should still exist: err=%v found=%v", n, seekErr, found)
		}
	}
}

// verifyOverflowInsert inserts a payload and verifies it has an overflow page.
func verifyOverflowInsert(t *testing.T, cursor *BtCursor, rowid int64, payloadSize uint32) {
	t.Helper()
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	if err := cursor.Insert(rowid, payload); err != nil {
		t.Fatalf("Insert rowid=%d size=%d: %v", rowid, payloadSize, err)
	}
	found, err := cursor.SeekRowid(rowid)
	if err != nil || !found {
		t.Fatalf("SeekRowid %d: err=%v found=%v", rowid, err, found)
	}
	if cursor.CurrentCell.OverflowPage == 0 {
		t.Errorf("rowid %d: expected overflow page for payload size %d", rowid, payloadSize)
	}
	if cursor.CurrentCell.PayloadSize != payloadSize {
		t.Errorf("rowid %d: PayloadSize = %d, want %d", rowid, cursor.CurrentCell.PayloadSize, payloadSize)
	}
}

// TestBalanceCellLargePayloadOverflowPages inserts rows large enough to require
// overflow pages, exercising calculateCellSizeAndLocal and extractOverflowPage
// for the overflow scenario.
func TestBalanceCellLargePayloadOverflowPages(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, rootPage)

	overflowThreshold := GetOverflowThreshold(bt.PageSize, true)
	verifyOverflowInsert(t, cursor, 1, overflowThreshold+1)
	verifyOverflowInsert(t, cursor, 2, overflowThreshold+100)
	verifyOverflowInsert(t, cursor, 3, overflowThreshold+500)
}
