// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// TestBalanceCellLoadPageForBalance exercises loadPageForBalance via balance().
// A valid cursor on a populated page triggers loadPageForBalance and then executeBalance.
func TestBalanceCellLoadPageForBalance(t *testing.T) {
	t.Parallel()

	t.Run("valid cursor loads page", func(t *testing.T) {
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

		// balance calls loadPageForBalance internally; we just need it not to panic
		// on a bad-page error. Any error here must be overfull/underfull, not a page load error.
		balErr := balance(cursor)
		if balErr != nil {
			msg := balErr.Error()
			if contains(msg, "failed to get page") || contains(msg, "failed to parse page") {
				t.Errorf("loadPageForBalance returned unexpected error: %v", balErr)
			}
		}
	})

	t.Run("cursor on invalid page triggers load error path", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 5, 10)
		cursor.SeekRowid(1)

		// Point cursor at a page that does not exist to exercise the error branch
		cursor.CurrentPage = 9999
		balErr := balance(cursor)
		if balErr == nil {
			t.Fatal("expected error for non-existent page, got nil")
		}
	})
}

// TestBalanceCellExecuteBalance exercises all three branches of executeBalance:
// overfull, underfull, and balanced-with-fragmentation.
func TestBalanceCellExecuteBalance(t *testing.T) {
	t.Parallel()

	t.Run("overfull branch", func(t *testing.T) {
		// Build a page that is genuinely overfull by constructing it directly.
		pageSize := uint32(512)
		bt := NewBtree(pageSize)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)

		// Fill until insert fails (page full)
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
	})

	t.Run("underfull non-root branch", func(t *testing.T) {
		// Use a very small page so splits occur, then delete most cells so
		// a non-root leaf becomes underfull.
		bt := NewBtree(512)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 100, 10)

		// Delete most rows; leaves that are not root should become underfull.
		cursor2 := NewCursor(bt, cursor.RootPage)
		for i := int64(2); i <= 95; i++ {
			found, _ := cursor2.SeekRowid(i)
			if found {
				cursor2.Delete()
			}
		}

		// Walk through pages and try to trigger executeBalance on underfull non-root pages.
		cursor3 := NewCursor(bt, cursor.RootPage)
		cursor3.SeekRowid(1)
		if cursor3.State == CursorValid && cursor3.Depth > 0 {
			pageData, _ := bt.GetPage(cursor3.CurrentPage)
			page, _ := NewBtreePage(cursor3.CurrentPage, pageData, bt.UsableSize)
			if isUnderfull(page) {
				// This exercises handleUnderfullPage with Depth > 0
				_ = executeBalance(cursor3, page)
			}
		}
	})

	t.Run("balanced page with fragmentation", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 30, 20)

		// Delete alternating rows to create fragmentation
		for i := int64(2); i <= 30; i += 2 {
			cursor.SeekRowid(i)
			if cursor.IsValid() {
				cursor.Delete()
			}
		}

		cursor.SeekRowid(1)
		pageData, _ := bt.GetPage(cursor.CurrentPage)
		page, _ := NewBtreePage(cursor.CurrentPage, pageData, bt.UsableSize)

		// Force fragmented bytes in the header so defragmentIfNeeded runs
		page.Header.FragmentedBytes = 10
		err = executeBalance(cursor, page)
		// Should succeed (balanced page gets defragmented)
		if err != nil {
			msg := err.Error()
			if !contains(msg, "overfull") && !contains(msg, "underfull") {
				t.Errorf("unexpected error from executeBalance on balanced page: %v", err)
			}
		}
	})
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

// TestBalanceCellHandleUnderfullPage directly exercises handleUnderfullPage.
func TestBalanceCellHandleUnderfullPage(t *testing.T) {
	t.Parallel()

	t.Run("root page - returns nil", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 2, 5)
		cursor.SeekRowid(1)

		pageData, _ := bt.GetPage(rootPage)
		page, _ := NewBtreePage(rootPage, pageData, bt.UsableSize)

		// Ensure it reads as underfull for the test to be meaningful
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

		// Simulate cursor on a non-root page but depth 0
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
		cursor.RootPage = rootPage // different, so not root
		cursor.Depth = 0

		err := handleUnderfullPage(cursor, page)
		// depth == 0, so returns nil
		if err != nil {
			t.Errorf("handleUnderfullPage depth=0 non-root: expected nil, got %v", err)
		}
	})

	t.Run("non-root page depth > 0 - returns merge error", func(t *testing.T) {
		bt := NewBtree(512)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 80, 10)

		// Delete most rows to create underfull non-root leaves
		cursor2 := NewCursor(bt, cursor.RootPage)
		for i := int64(2); i <= 75; i++ {
			found, _ := cursor2.SeekRowid(i)
			if found {
				cursor2.Delete()
			}
		}

		cursor3 := NewCursor(bt, cursor.RootPage)
		cursor3.SeekRowid(1)
		if cursor3.State != CursorValid || cursor3.Depth <= 0 {
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
	})

	t.Run("non-root with fragmentation runs defrag", func(t *testing.T) {
		bt := NewBtree(512)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 80, 10)

		cursor2 := NewCursor(bt, cursor.RootPage)
		for i := int64(2); i <= 75; i++ {
			found, _ := cursor2.SeekRowid(i)
			if found {
				cursor2.Delete()
			}
		}

		cursor3 := NewCursor(bt, cursor.RootPage)
		cursor3.SeekRowid(1)
		if cursor3.State != CursorValid || cursor3.Depth <= 0 {
			t.Skip("cursor not at depth > 0")
		}

		pageData, _ := bt.GetPage(cursor3.CurrentPage)
		page, _ := NewBtreePage(cursor3.CurrentPage, pageData, bt.UsableSize)

		// Force fragmented bytes to exercise defrag path
		page.Header.FragmentedBytes = 5
		_ = handleUnderfullPage(cursor3, page)
		// Just verify no panic; error is expected (merge needed)
	})
}

// TestBalanceCellBalance exercises balance() itself across several conditions.
func TestBalanceCellBalance(t *testing.T) {
	t.Parallel()

	t.Run("many inserts trigger overfull detection", func(t *testing.T) {
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
	})

	t.Run("delete triggers underfull on non-root", func(t *testing.T) {
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
				// Only unexpected errors should fail the test
				if !contains(msg, "overfull") && !contains(msg, "underfull") &&
					!contains(msg, "failed to get page") && !contains(msg, "failed to parse page") {
					t.Errorf("unexpected error from balance: %v", balErr)
				}
			}
		}
	})
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

// TestBalanceCellExtractOverflowPage exercises extractOverflowPage via ParseCell.
func TestBalanceCellExtractOverflowPage(t *testing.T) {
	t.Parallel()

	t.Run("no overflow when payload fits locally", func(t *testing.T) {
		cell := EncodeTableLeafCell(42, []byte("short payload"))
		info, err := ParseCell(PageTypeLeafTable, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell: %v", err)
		}
		if info.OverflowPage != 0 {
			t.Errorf("OverflowPage = %d, want 0", info.OverflowPage)
		}
	})

	t.Run("overflow page extracted for large payload", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)

		// Insert payload larger than maxLocal to trigger overflow
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
	})

	t.Run("overflow page number truncated error", func(t *testing.T) {
		// Craft a cell where the overflow pointer is cut off
		usableSize := uint32(512)
		maxLocal := calculateMaxLocal(usableSize, true)

		// Build raw cell: varint(payloadSize > maxLocal), varint(rowid), localPayload bytes
		// but without the 4-byte overflow page pointer at the end
		oversized := uint64(maxLocal) + 100
		buf := make([]byte, 20+int(maxLocal))
		offset := 0
		offset += PutVarint(buf[offset:], oversized)
		offset += PutVarint(buf[offset:], 1) // rowid
		// Do NOT append the overflow pointer - truncated cell
		truncated := buf[:offset+int(maxLocal)/2]

		_, err := ParseCell(PageTypeLeafTable, truncated, usableSize)
		// Should either error or succeed depending on internal clamping;
		// we just verify no panic
		_ = err
	})
}

// TestBalanceCellParseTableInteriorCell exercises parseTableInteriorCell.
func TestBalanceCellParseTableInteriorCell(t *testing.T) {
	t.Parallel()

	t.Run("round-trip encode and parse", func(t *testing.T) {
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
	})

	t.Run("multi-byte varint rowid", func(t *testing.T) {
		// rowid requiring 2+ varint bytes
		childPage := uint32(7)
		rowid := int64(300) // encodes as 2-byte varint
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
	})

	t.Run("too small - returns error", func(t *testing.T) {
		_, err := ParseCell(PageTypeInteriorTable, []byte{0x01, 0x02}, 4096)
		if err == nil {
			t.Error("expected error for too-small interior cell data")
		}
	})

	t.Run("interior cells appear after btree split", func(t *testing.T) {
		// Force tree to split so interior pages exist, then read interior cells
		bt := NewBtree(512)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)
		insertRows(cursor, 1, 150, 10)

		// root should now be an interior page
		rootData, err := bt.GetPage(cursor.RootPage)
		if err != nil {
			t.Fatalf("GetPage root: %v", err)
		}
		header, err := ParsePageHeader(rootData, cursor.RootPage)
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

		// Parse each interior cell to exercise parseTableInteriorCell
		for i, ptr := range ptrs {
			info, err := ParseCell(PageTypeInteriorTable, rootData[ptr:], bt.UsableSize)
			if err != nil {
				t.Errorf("ParseCell interior cell %d: %v", i, err)
				continue
			}
			if info.ChildPage == 0 {
				t.Errorf("interior cell %d: ChildPage should not be 0", i)
			}
		}
	})
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

// TestBalanceCellReadIndexPayloadVarint exercises readIndexPayloadVarint
// indirectly via parseIndexLeafCell and parseIndexInteriorCell.
func TestBalanceCellReadIndexPayloadVarint(t *testing.T) {
	t.Parallel()

	t.Run("index leaf cell - payload size and key set correctly", func(t *testing.T) {
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
		// For index pages, Key is set equal to payload size
		if info.Key != int64(len(payload)) {
			t.Errorf("Key = %d, want %d (payload size)", info.Key, len(payload))
		}
	})

	t.Run("index interior cell - reads payload size after child page", func(t *testing.T) {
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
	})

	t.Run("large payload exercises overflow variant of readIndexPayloadVarint path", func(t *testing.T) {
		// PayloadSize > maxLocal for index page
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
		// Payload size of 200 requires 2-byte varint encoding
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

// TestBalanceCellFullInsertDeleteCycle performs a high-volume insert/delete
// cycle to exercise balance code paths end-to-end.
func TestBalanceCellFullInsertDeleteCycle(t *testing.T) {
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

	// Verify all inserted rows are readable in order after splits
	cursor2 := NewCursor(bt, cursor.RootPage)
	count := verifyOrderedForward(t, cursor2)
	if count != n {
		t.Errorf("forward traversal: got %d entries, want %d", count, n)
	}

	// Delete a few rows to exercise delete/balance interaction
	cursor3 := NewCursor(bt, cursor.RootPage)
	for i := int64(1); i <= 5 && i <= int64(n); i++ {
		found, err := cursor3.SeekRowid(i)
		if err == nil && found {
			cursor3.Delete()
		}
	}

	// Verify the specific deleted rows are gone
	cursor4 := NewCursor(bt, cursor.RootPage)
	for i := int64(1); i <= 5 && i <= int64(n); i++ {
		found, _ := cursor4.SeekRowid(i)
		if found {
			t.Errorf("row %d should have been deleted", i)
		}
	}

	// Verify at least one remaining row is still accessible
	if n > 5 {
		cursor5 := NewCursor(bt, cursor.RootPage)
		found, err := cursor5.SeekRowid(int64(n))
		if err != nil || !found {
			t.Errorf("last inserted row %d should still exist: err=%v found=%v", n, err, found)
		}
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

	testCases := []struct {
		rowid       int64
		payloadSize uint32
	}{
		{1, overflowThreshold + 1},   // just over threshold
		{2, overflowThreshold + 100}, // moderately over threshold
		{3, overflowThreshold + 500}, // further over threshold
	}

	for _, tc := range testCases {
		payload := make([]byte, tc.payloadSize)
		for i := range payload {
			payload[i] = byte(i % 256)
		}
		if err := cursor.Insert(tc.rowid, payload); err != nil {
			t.Fatalf("Insert rowid=%d size=%d: %v", tc.rowid, tc.payloadSize, err)
		}

		found, err := cursor.SeekRowid(tc.rowid)
		if err != nil || !found {
			t.Fatalf("SeekRowid %d: err=%v found=%v", tc.rowid, err, found)
		}
		if cursor.CurrentCell.OverflowPage == 0 {
			t.Errorf("rowid %d: expected overflow page for payload size %d", tc.rowid, tc.payloadSize)
		}
		if cursor.CurrentCell.PayloadSize != tc.payloadSize {
			t.Errorf("rowid %d: PayloadSize = %d, want %d",
				tc.rowid, cursor.CurrentCell.PayloadSize, tc.payloadSize)
		}
	}
}
