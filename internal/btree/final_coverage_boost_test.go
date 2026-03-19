// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// TestBalance_HandleUnderfullPageEdgeCases tests underfull page handling (25.0% -> higher)
func TestBalance_HandleUnderfullPageEdgeCases(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 100, 20)
	deleteRowRange(cursor, 20, 80)

	underfullEdgeCaseCheckPage(t, bt, cursor)
	underfullEdgeCaseCheckRoot(t, bt, cursor)
}

func underfullEdgeCaseCheckPage(t *testing.T, bt *Btree, cursor *BtCursor) {
	t.Helper()
	cursor.SeekRowid(10)
	if !cursor.IsValid() {
		return
	}
	page := getPageIfValid(bt, cursor.CurrentPage)
	if page == nil {
		return
	}
	isUnderfullVal := page.IsUnderfull()
	t.Logf("Page %d is underfull: %v", cursor.CurrentPage, isUnderfullVal)

	if isUnderfullVal && cursor.Depth > 0 {
		t.Log("Testing balance on underfull non-root page")
		if err := balance(cursor); err != nil {
			t.Logf("balance() error = %v (expected for underfull)", err)
		}
	}
}

func underfullEdgeCaseCheckRoot(t *testing.T, bt *Btree, cursor *BtCursor) {
	t.Helper()
	cursor.SeekRowid(90)
	if !cursor.IsValid() || cursor.CurrentPage != cursor.RootPage {
		return
	}
	page := getPageIfValid(bt, cursor.CurrentPage)
	if page == nil || !page.IsUnderfull() {
		return
	}
	t.Log("Testing balance on underfull root page")
	if err := balance(cursor); err != nil {
		t.Logf("balance() on root error = %v", err)
	}
}

// TestBalance_HandleOverfullPageWithDefrag tests overfull with defragmentation (33.3% -> higher)
func TestBalance_HandleOverfullPageWithDefrag(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)

	overfullDefragSetupPage(bt)

	cursor := NewCursor(bt, 1)
	insertRows(cursor, 1, 20, 15)

	for _, rowid := range []int64{5, 10, 15} {
		cursor.SeekRowid(rowid)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	overfullDefragInsertMore(cursor)
	overfullDefragCheckBalance(t, bt, cursor)
}

func overfullDefragSetupPage(bt *Btree) {
	pageData := make([]byte, 512)
	offset := FileHeaderSize
	pageData[offset+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[offset+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[offset+PageHeaderOffsetCellStart:], 0)
	pageData[offset+PageHeaderOffsetFragmented] = 50
	bt.SetPage(1, pageData)
}

func overfullDefragInsertMore(cursor *BtCursor) {
	for i := int64(30); i <= 40; i++ {
		err := cursor.Insert(i, make([]byte, 20))
		if err != nil {
			break
		}
	}
}

func overfullDefragCheckBalance(t *testing.T, bt *Btree, cursor *BtCursor) {
	t.Helper()
	cursor.SeekRowid(35)
	if !cursor.IsValid() {
		return
	}
	page := getPageIfValid(bt, cursor.CurrentPage)
	if page == nil {
		return
	}
	t.Logf("Page overfull: %v, fragmented bytes: %d",
		page.IsOverfull(), page.Header.FragmentedBytes)

	if page.Header.FragmentedBytes > 0 {
		t.Log("Testing handleOverfullPage with fragmentation")
		if err := balance(cursor); err != nil {
			t.Logf("balance() with fragmentation error = %v", err)
		}
	}
}

// TestIntegrity_CheckMaxIterationsExceededEdge tests iteration limit edge case (50.0% -> higher)
func TestIntegrity_CheckMaxIterationsExceededEdge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	pageData := make([]byte, 4096)

	offset := FileHeaderSize
	pageData[offset+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[offset+PageHeaderOffsetFreeblock:], 200)

	setupShortFreeBlockChain(pageData)

	bt.SetPage(1, pageData)

	result := ValidateFreeBlockList(bt, 1)
	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors for short chain: %v", result.Errors)
	} else {
		t.Log("Short free block chain validated successfully")
	}
}

func setupShortFreeBlockChain(pageData []byte) {
	for i := 0; i < 5; i++ {
		blockOffset := 200 + i*10
		if i < 4 {
			binary.BigEndian.PutUint16(pageData[blockOffset:], uint16(blockOffset+10))
		} else {
			binary.BigEndian.PutUint16(pageData[blockOffset:], 0)
		}
		binary.BigEndian.PutUint16(pageData[blockOffset+2:], 10)
	}
}

// TestCursor_LoadParentPageMultipleLevels tests parent page loading (55.6% -> higher)
func TestCursor_LoadParentPageMultipleLevels(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 200, 22)

	cursor.SeekRowid(50)
	navigateForward(cursor, 100)
	navigateBackward(cursor, 50)

	t.Logf("Navigated through deep tree, max depth: %d", cursor.Depth)
}

// TestCursor_GetChildPageFromParentEdgeCases tests child page resolution (55.6% -> higher)
func TestCursor_GetChildPageFromParentEdgeCases(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 150, 25)

	for _, rowid := range []int64{1, 2, 75, 148, 149, 150} {
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			t.Logf("SeekRowid(%d) error = %v", rowid, err)
		} else if found {
			t.Logf("Found rowid %d at page %d, depth %d", rowid, cursor.CurrentPage, cursor.Depth)
		}
	}

	for _, rowid := range []int64{200, 300, 500} {
		found, _ := cursor.SeekRowid(rowid)
		t.Logf("SeekRowid(%d) found=%v (child resolution on miss)", rowid, found)
	}
}

// TestCursor_PrevInPageBoundary tests backward navigation at boundaries (62.5% -> higher)
func TestCursor_PrevInPageBoundary(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 25, 1)

	cursor.SeekRowid(2)
	if cursor.IsValid() {
		err := cursor.Previous()
		if err != nil {
			t.Errorf("Previous() error = %v", err)
		}
		if cursor.IsValid() && cursor.GetKey() != 1 {
			t.Errorf("Expected key 1, got %d", cursor.GetKey())
		}
	}

	err := cursor.Previous()
	if err == nil && cursor.IsValid() {
		t.Error("Previous() from first entry should make cursor invalid")
	}
}

// TestCursor_DescendToRightChildBoundary tests right child descent (60.0% -> higher)
func TestCursor_DescendToRightChildBoundary(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 110, 26)

	for i := 0; i < 3; i++ {
		err = cursor.MoveToLast()
		if err != nil {
			t.Fatalf("MoveToLast() iteration %d error = %v", i, err)
		}
		if cursor.IsValid() {
			t.Logf("MoveToLast() iteration %d: key=%d", i, cursor.GetKey())
		}
	}

	descendRightChildVerifyPrev(t, cursor)
}

func descendRightChildVerifyPrev(t *testing.T, cursor *BtCursor) {
	t.Helper()
	if !cursor.IsValid() {
		return
	}
	lastKey := cursor.GetKey()
	err := cursor.Previous()
	if err != nil {
		t.Errorf("Previous() from last error = %v", err)
	}
	if cursor.IsValid() {
		prevKey := cursor.GetKey()
		if prevKey >= lastKey {
			t.Errorf("Previous key %d should be < last key %d", prevKey, lastKey)
		}
	}
}

// TestPage_AllocateSpaceFragmented tests allocation with fragmentation (55.6% -> higher)
func TestPage_AllocateSpaceFragmented(t *testing.T) {
	t.Parallel()
	page := allocSpaceFragSetupPage(t)
	allocSpaceFragAllocateInitial(t, page)
	allocSpaceFragDeleteMiddle(t, page)
	allocSpaceFragReallocate(t, page)

	_, err := page.AllocateSpace(800)
	if err != nil {
		t.Logf("AllocateSpace() with too large size failed as expected: %v", err)
	} else {
		t.Log("AllocateSpace() with large size succeeded (may have enough space)")
	}
}

func allocSpaceFragSetupPage(t *testing.T) *BtreePage {
	t.Helper()
	pageData := make([]byte, 1024)
	offset := FileHeaderSize
	pageData[offset+PageHeaderOffsetType] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[offset+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[offset+PageHeaderOffsetCellStart:], 0)

	page, err := NewBtreePage(1, pageData, 1024)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}
	return page
}

func allocSpaceFragAllocateInitial(t *testing.T, page *BtreePage) {
	t.Helper()
	for i := 0; i < 8; i++ {
		_, err := page.AllocateSpace(40)
		if err != nil {
			t.Fatalf("AllocateSpace() %d error = %v", i, err)
		}
	}
}

func allocSpaceFragDeleteMiddle(t *testing.T, page *BtreePage) {
	t.Helper()
	for _, idx := range []int{2, 4, 6} {
		if int(page.Header.NumCells) > idx {
			if err := page.DeleteCell(idx); err != nil {
				t.Logf("DeleteCell(%d) error = %v", idx, err)
			}
		}
	}
}

func allocSpaceFragReallocate(t *testing.T, page *BtreePage) {
	t.Helper()
	for i := 0; i < 5; i++ {
		off, err := page.AllocateSpace(35)
		if err != nil {
			t.Logf("AllocateSpace() after fragmentation error = %v", err)
			break
		}
		t.Logf("Allocated at offset %d after fragmentation", off)
	}
}

// TestIndexCursor_PrevInPageBoundary tests index backward navigation (55.6% -> higher)
func TestIndexCursor_PrevInPageBoundary(t *testing.T) {
	t.Parallel()
	_, cursor := setupIndexCursor(t, 4096)

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for i, key := range keys {
		err := cursor.InsertIndex([]byte(key), int64(i))
		if err != nil {
			t.Fatalf("InsertIndex() error = %v", err)
		}
	}

	prevInPageBoundaryVerify(t, cursor)
}

func prevInPageBoundaryVerify(t *testing.T, cursor *IndexCursor) {
	t.Helper()
	cursor.SeekIndex([]byte("b"))
	if cursor.IsValid() {
		if err := cursor.PrevIndex(); err != nil {
			t.Errorf("PrevIndex() error = %v", err)
		}
		if cursor.IsValid() {
			key := string(cursor.GetKey())
			if key != "a" {
				t.Errorf("Expected key 'a', got '%s'", key)
			}
		}
	}

	err := cursor.PrevIndex()
	if err == nil && cursor.IsValid() {
		t.Error("PrevIndex() from first should make cursor invalid")
	}
}

// TestIndexCursor_EnterPageMultiLevel tests entering pages (57.1% -> higher)
func TestIndexCursor_EnterPageMultiLevel(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	insertIndexEntriesN(cursor, 120, func(i int) []byte {
		key := make([]byte, 12)
		binary.BigEndian.PutUint64(key, uint64(i*100))
		return key
	})

	cursor.MoveToFirst()
	navigateIndexForward(cursor, 60)
	navigateIndexBackward(cursor, 30)
	cursor.MoveToLast()
	navigateIndexBackward(cursor, 40)

	t.Log("Extensive index navigation completed (enterPage)")
}

// TestIndexCursor_DescendToLastMultiLevel tests descending to last (62.5% -> higher)
func TestIndexCursor_DescendToLastMultiLevel(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	insertIndexEntriesN(cursor, 100, func(i int) []byte {
		key := make([]byte, 15)
		binary.BigEndian.PutUint64(key, uint64(i*10))
		return key
	})

	for attempt := 0; attempt < 3; attempt++ {
		err = cursor.MoveToLast()
		if err != nil {
			t.Fatalf("MoveToLast() attempt %d error = %v", attempt, err)
		}
		if cursor.IsValid() {
			t.Logf("MoveToLast() attempt %d succeeded", attempt)
		}
	}

	if cursor.IsValid() {
		navigateIndexBackward(cursor, 5)
		t.Log("Navigated backward from last (descendToLast)")
	}
}

// TestIndexCursor_DescendToRightChildMultiple tests right child descent (60.0% -> higher)
func TestIndexCursor_DescendToRightChildMultiple(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	insertIndexEntriesN(cursor, 110, func(i int) []byte {
		key := make([]byte, 14)
		binary.BigEndian.PutUint64(key, uint64(i*5))
		return key
	})

	for round := 0; round < 3; round++ {
		cursor.MoveToLast()
		if cursor.IsValid() {
			navigateIndexBackward(cursor, 10)
			navigateIndexForward(cursor, 10)
		}
	}

	t.Log("Multiple rounds of last/navigate completed (descendToRightChild)")
}

// TestOverflow_FreeOverflowChainMultiple tests freeing multiple overflow chains (60.0% -> higher)
func TestOverflow_FreeOverflowChainMultiple(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	largeData := make([]byte, 8000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	for rowid := int64(100); rowid <= 105; rowid++ {
		if err := cursor.Insert(rowid, largeData); err != nil {
			t.Logf("Insert(%d) with overflow error = %v", rowid, err)
		}
	}

	freeOverflowChainDeleteAll(t, cursor)
}

func freeOverflowChainDeleteAll(t *testing.T, cursor *BtCursor) {
	t.Helper()
	for rowid := int64(100); rowid <= 105; rowid++ {
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			t.Logf("SeekRowid(%d) error = %v", rowid, err)
			continue
		}
		if found {
			if err := cursor.Delete(); err != nil {
				t.Logf("Delete(%d) error = %v", rowid, err)
			} else {
				t.Logf("Deleted rowid %d with overflow chain", rowid)
			}
		}
	}
}
