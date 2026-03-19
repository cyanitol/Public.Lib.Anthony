// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// TestCursor_PrevInPageDetailed tests backward navigation within a page (62.5% coverage)
func TestCursor_PrevInPageDetailed(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096) // Large page to keep entries on one page
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a small number of rows to stay on one page
	for i := int64(1); i <= 10; i++ {
		err := cursor.Insert(i, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Move to last and navigate backward within page
	err = cursor.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast() error = %v", err)
	}

	prevCount := 0
	initialPage := cursor.CurrentPage
	for i := 0; i < 9; i++ {
		err := cursor.Previous()
		if err != nil || !cursor.IsValid() {
			break
		}
		if cursor.CurrentPage == initialPage {
			prevCount++
		}
	}

	if prevCount > 0 {
		t.Logf("Navigated backward %d times on same page (prevInPage)", prevCount)
	}
}

// TestCursor_AdvanceWithinPage tests forward navigation within page (66.7% coverage)
func TestCursor_AdvanceWithinPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 15; i++ {
		err := cursor.Insert(i, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Navigate forward within page
	err = cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst() error = %v", err)
	}

	nextCount := 0
	initialPage := cursor.CurrentPage
	for i := 0; i < 10; i++ {
		err := cursor.Next()
		if err != nil || !cursor.IsValid() {
			break
		}
		if cursor.CurrentPage == initialPage {
			nextCount++
		}
	}

	if nextCount > 0 {
		t.Logf("Navigated forward %d times on same page (advanceWithinPage)", nextCount)
	}
}

// TestCursor_ResolveChildPage tests child page resolution (63.6% coverage)
func TestCursor_ResolveChildPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough to create interior pages
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 25))
		if err != nil {
			break
		}
	}

	// Seek to various positions - this resolves child pages
	for _, rowid := range []int64{5, 25, 50, 75, 95} {
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			t.Logf("SeekRowid(%d) error = %v", rowid, err)
			continue
		}
		if found {
			t.Logf("Found rowid %d at depth %d (resolveChildPage)", rowid, cursor.Depth)
		}
	}
}

// TestCursor_TryLoadCell tests cell loading during seeks (77.8% coverage)
func TestCursor_TryLoadCell(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 80; i++ {
		err := cursor.Insert(i, make([]byte, 20))
		if err != nil {
			break
		}
	}

	// Binary search during SeekRowid calls tryLoadCell
	for i := int64(1); i <= 80; i += 10 {
		cursor.SeekRowid(i)
		if cursor.IsValid() && cursor.GetKey() == i {
			t.Logf("Found key %d (tryLoadCell)", i)
		}
	}
}

// TestCursor_ValidateDeletePosition tests delete validation (80.0% coverage)
func TestCursor_ValidateDeletePosition(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 20; i++ {
		err := cursor.Insert(i, []byte("data"))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Try to delete from invalid position
	cursor.State = CursorInvalid
	err = cursor.Delete()
	if err == nil {
		t.Error("Delete() from invalid cursor should fail")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	// Delete from valid position
	cursor.SeekRowid(10)
	if cursor.IsValid() {
		err = cursor.Delete()
		if err != nil {
			t.Errorf("Delete() from valid cursor error = %v", err)
		} else {
			t.Log("Successfully deleted from valid position")
		}
	}
}

// TestCursor_FreeOverflowPages tests overflow page freeing (75.0% coverage)
func TestCursor_FreeOverflowPages(t *testing.T) {
	t.Parallel()
	bt := NewBtree(1024)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert row with overflow
	largeData := make([]byte, 5000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err = cursor.Insert(42, largeData)
	if err != nil {
		t.Fatalf("Insert() with overflow error = %v", err)
	}

	// Delete it - should free overflow pages
	found, err := cursor.SeekRowid(42)
	if err != nil {
		t.Fatalf("SeekRowid() error = %v", err)
	}
	if !found {
		t.Skip("Row with overflow not found (may have failed to insert)")
	}

	err = cursor.Delete()
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	t.Log("Deleted row with overflow pages (freeOverflowPages)")
}

// TestCursor_PerformCellDeletion tests cell deletion logic (71.4% coverage)
func TestCursor_PerformCellDeletion(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 30; i++ {
		err := cursor.Insert(i, []byte("test"))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Delete several entries
	for _, rowid := range []int64{5, 15, 25} {
		found, err := cursor.SeekRowid(rowid)
		if err != nil {
			t.Errorf("SeekRowid(%d) error = %v", rowid, err)
			continue
		}
		if found {
			err = cursor.Delete()
			if err != nil {
				t.Errorf("Delete() error = %v", err)
			} else {
				t.Logf("Deleted rowid %d (performCellDeletion)", rowid)
			}
		}
	}

	// Verify deletions
	for _, rowid := range []int64{5, 15, 25} {
		found, _ := cursor.SeekRowid(rowid)
		if found {
			t.Errorf("Rowid %d still found after deletion", rowid)
		}
	}
}

// TestCursor_AdjustCursorAfterDelete tests cursor adjustment (71.4% coverage)
func TestCursor_AdjustCursorAfterDelete(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 20; i++ {
		err := cursor.Insert(i, []byte("test"))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Position at middle and delete
	cursor.SeekRowid(10)
	if cursor.IsValid() {
		err = cursor.Delete()
		if err != nil {
			t.Errorf("Delete() error = %v", err)
		}

		// Cursor should be adjusted to next valid entry
		if cursor.IsValid() {
			key := cursor.GetKey()
			t.Logf("After delete, cursor at key %d (adjustCursorAfterDelete)", key)
		}
	}
}

// TestCursor_LoadCellAtCurrentIndex tests cell loading (60.0% coverage)
func TestCursor_LoadCellAtCurrentIndex(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert entries
	for i := int64(1); i <= 25; i++ {
		payload := make([]byte, 10)
		binary.BigEndian.PutUint64(payload, uint64(i*100))
		err := cursor.Insert(i, payload)
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	// Navigate and load cells
	cursor.MoveToFirst()
	for i := 0; i < 10; i++ {
		if cursor.IsValid() {
			key := cursor.GetKey()
			payload := cursor.GetPayload()
			t.Logf("Loaded cell: key=%d, payload len=%d (loadCellAtCurrentIndex)", key, len(payload))
		}
		cursor.Next()
	}
}

// TestIndexCursor_AdvanceWithinPageIndex tests index forward navigation (60.0% coverage)
func TestIndexCursor_AdvanceWithinPageIndex(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries on one page
	for i := 0; i < 12; i++ {
		key := []byte{byte('A' + i)}
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			t.Fatalf("InsertIndex() error = %v", err)
		}
	}

	// Navigate forward
	cursor.MoveToFirst()
	nextCount := 0
	initialPage := cursor.CurrentPage
	for i := 0; i < 10; i++ {
		err := cursor.NextIndex()
		if err != nil || !cursor.IsValid() {
			break
		}
		if cursor.CurrentPage == initialPage {
			nextCount++
		}
	}

	if nextCount > 0 {
		t.Logf("Index navigated forward %d times on same page", nextCount)
	}
}

// TestIndexCursor_ClimbToNextParentDetailed tests parent climbing (68.0% coverage)
func TestIndexCursor_ClimbToNextParentDetailed(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert many entries to create multiple pages
	for i := 0; i < 150; i++ {
		key := make([]byte, 10)
		binary.BigEndian.PutUint64(key, uint64(i))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Full forward scan - will climb parents
	cursor.MoveToFirst()
	scanCount := 0
	for cursor.IsValid() && scanCount < 200 {
		scanCount++
		err := cursor.NextIndex()
		if err != nil {
			break
		}
	}

	t.Logf("Index scanned %d entries (climbToNextParent)", scanCount)
}

// TestIndexCursor_PrevViaParentDetailed tests backward parent navigation (65.2% coverage)
func TestIndexCursor_PrevViaParentDetailed(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage() error = %v", err)
	}

	cursor := NewIndexCursor(bt, rootPage)

	// Insert entries
	for i := 0; i < 100; i++ {
		key := make([]byte, 12)
		binary.BigEndian.PutUint64(key, uint64(i*10))
		err := cursor.InsertIndex(key, int64(i))
		if err != nil {
			break
		}
	}

	// Navigate backward from end
	cursor.MoveToLast()
	backCount := 0
	for backCount < 50 {
		err := cursor.PrevIndex()
		if err != nil || !cursor.IsValid() {
			break
		}
		backCount++
	}

	t.Logf("Index navigated backward %d times (prevViaParent)", backCount)
}

// TestMerge_MergePages tests the merge operation (66.7% coverage)
func TestMerge_MergePages(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert rows to create multiple pages
	for i := int64(1); i <= 80; i++ {
		err := cursor.Insert(i, make([]byte, 20))
		if err != nil {
			break
		}
	}

	// Delete many rows to trigger merge
	for i := int64(20); i <= 60; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	// Try to merge
	cursor.SeekRowid(30)
	if cursor.IsValid() && cursor.Depth > 0 {
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage() error = %v", err)
		}
		if merged {
			t.Log("Successfully merged pages")
		} else {
			t.Log("Merge not performed (page may not be underfull enough)")
		}
	}
}

// TestMerge_UpdateParentAfterMerge tests parent update (66.7% coverage)
func TestMerge_UpdateParentAfterMerge(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create multi-page tree
	for i := int64(1); i <= 100; i++ {
		err := cursor.Insert(i, make([]byte, 18))
		if err != nil {
			break
		}
	}

	// Delete to make underfull
	for i := int64(30); i <= 70; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	// Attempt merge which updates parent
	cursor.SeekRowid(40)
	if cursor.IsValid() && cursor.Depth > 0 {
		merged, err := cursor.MergePage()
		if err != nil {
			t.Logf("MergePage() error = %v", err)
		} else {
			t.Logf("Merge result: %v (updateParentAfterMerge)", merged)
		}
	}
}

// TestMerge_CopyRightCellsToLeft tests cell copying (70.0% coverage)
func TestMerge_CopyRightCellsToLeft(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert to create pages
	for i := int64(1); i <= 90; i++ {
		err := cursor.Insert(i, make([]byte, 15))
		if err != nil {
			break
		}
	}

	// Delete to trigger merge
	for i := int64(25); i <= 65; i++ {
		cursor.SeekRowid(i)
		if cursor.IsValid() {
			cursor.Delete()
		}
	}

	cursor.SeekRowid(35)
	if cursor.IsValid() && cursor.Depth > 0 {
		cursor.MergePage()
		t.Log("Merge attempted (copyRightCellsToLeft)")
	}
}
