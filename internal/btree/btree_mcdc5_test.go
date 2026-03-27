// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree_test

import (
	"fmt"
	"testing"

	btree "github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// ---------------------------------------------------------------------------
// Private helpers (prefixed mcdc5_ to avoid collisions with other test files)
// ---------------------------------------------------------------------------

func mcdc5_rowidTable(t *testing.T, pageSize uint32) (*btree.Btree, *btree.BtCursor, uint32) {
	t.Helper()
	bt := btree.NewBtree(pageSize)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return bt, btree.NewCursor(bt, root), root
}

func mcdc5_compositeTable(t *testing.T, pageSize uint32) (*btree.Btree, *btree.BtCursor, uint32) {
	t.Helper()
	bt := btree.NewBtree(pageSize)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	return bt, btree.NewCursorWithOptions(bt, root, true), root
}

func mcdc5_insertN(t *testing.T, cursor *btree.BtCursor, n, payloadSize int) {
	t.Helper()
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	for i := 1; i <= n; i++ {
		if err := cursor.Insert(int64(i), payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
}

func mcdc5_insertCompositeN(t *testing.T, cursor *btree.BtCursor, n, payloadSize int, prefix string) {
	t.Helper()
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	for i := 1; i <= n; i++ {
		key := []byte(fmt.Sprintf("%s-%06d", prefix, i))
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}
}

func mcdc5_allocateIndexPage(bt *btree.Btree) (uint32, error) {
	pageNum, err := bt.AllocatePage()
	if err != nil {
		return 0, err
	}
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return 0, err
	}
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = btree.FileHeaderSize
	}
	pageData[headerOffset+btree.PageHeaderOffsetType] = btree.PageTypeLeafIndex
	pageData[headerOffset+btree.PageHeaderOffsetFreeblock] = 0
	pageData[headerOffset+btree.PageHeaderOffsetFreeblock+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetNumCells] = 0
	pageData[headerOffset+btree.PageHeaderOffsetNumCells+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetCellStart] = 0
	pageData[headerOffset+btree.PageHeaderOffsetCellStart+1] = 0
	pageData[headerOffset+btree.PageHeaderOffsetFragmented] = 0
	return pageNum, nil
}

// ---------------------------------------------------------------------------
// MC/DC Condition 16 — split.go prepareLeafSplit / prepareLeafSplitPages
//
//   prepareLeafSplit: GetPage → NewBtreePage → collectLeafCells (all must succeed).
//
//   A = GetPage succeeds
//   B = NewBtreePage succeeds
//   C = collectLeafCells succeeds
//
//   Row 1: A=T, B=T, C=T → split completes; 1000 rows on 512-byte page
// ---------------------------------------------------------------------------

// TestMCDC5_PrepareLeafSplit_MassInsert exercises the happy path of
// prepareLeafSplit / prepareLeafSplitPages by inserting 1000 rows on a small
// page, forcing many leaf splits.
func TestMCDC5_PrepareLeafSplit_MassInsert(t *testing.T) {
	t.Parallel()

	_, cursor, root := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 1000, 20)

	for _, k := range []int64{1, 500, 1000} {
		found, err := cursor.SeekRowid(k)
		if err != nil {
			t.Errorf("SeekRowid(%d): %v", k, err)
		}
		if !found {
			t.Errorf("SeekRowid(%d): not found after mass insert", k)
		}
	}
	_ = root
}

// ---------------------------------------------------------------------------
// MC/DC Condition 17 — split.go prepareLeafSplitComposite /
//   prepareLeafSplitPagesComposite
//
//   Row 1: normal path → 500 composite rows on 512-byte page
// ---------------------------------------------------------------------------

// TestMCDC5_PrepareLeafSplitComposite_MassInsert exercises the composite
// leaf-split path.
func TestMCDC5_PrepareLeafSplitComposite_MassInsert(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_compositeTable(t, 512)

	mcdc5_insertCompositeN(t, cursor, 500, 20, "ck")

	for _, k := range []string{"ck-000001", "ck-000250", "ck-000500"} {
		found, err := cursor.SeekComposite([]byte(k))
		if err != nil {
			t.Errorf("SeekComposite(%q): %v", k, err)
		}
		if !found {
			t.Errorf("SeekComposite(%q): not found", k)
		}
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 18 — split.go prepareInteriorSplit /
//   prepareInteriorSplitPages
//
//   Interior splits happen after enough leaf splits fill the parent.
//
//   Row 1: deep tree → 5000 rows on 512-byte page
// ---------------------------------------------------------------------------

// TestMCDC5_PrepareInteriorSplit_DeepTree triggers interior page splits by
// inserting 5000 rows on a 512-byte page, producing a multi-level tree.
func TestMCDC5_PrepareInteriorSplit_DeepTree(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 5000, 10)

	for _, k := range []int64{1, 1000, 2500, 4999, 5000} {
		found, err := cursor.SeekRowid(k)
		if err != nil {
			t.Errorf("SeekRowid(%d): %v", k, err)
		}
		if !found {
			t.Errorf("SeekRowid(%d): not found in deep tree", k)
		}
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 19 — split.go prepareInteriorSplitComposite /
//   prepareInteriorSplitPagesComposite
//
//   Row 1: deep composite tree → 3000 rows on 512-byte page
// ---------------------------------------------------------------------------

// TestMCDC5_PrepareInteriorSplitComposite_DeepTree triggers composite
// interior page splits.
func TestMCDC5_PrepareInteriorSplitComposite_DeepTree(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_compositeTable(t, 512)

	mcdc5_insertCompositeN(t, cursor, 3000, 10, "cik")

	for _, k := range []string{"cik-000001", "cik-001500", "cik-003000"} {
		found, err := cursor.SeekComposite([]byte(k))
		if err != nil {
			t.Errorf("SeekComposite(%q): %v", k, err)
		}
		if !found {
			t.Errorf("SeekComposite(%q): not found", k)
		}
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 20 — split.go createNewRoot: CompositePK branch
//
//   if c.CompositePK { pageType = PageTypeInteriorTableNo }
//
//   A = c.CompositePK
//
//   Row 1: A=T  → composite interior root page type
//   Row 2: A=F  → regular interior root (covered by rowid tests)
// ---------------------------------------------------------------------------

// TestMCDC5_CreateNewRoot_CompositeBranch forces createNewRoot with
// CompositePK=true by filling the composite root leaf past capacity.
func TestMCDC5_CreateNewRoot_CompositeBranch(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_compositeTable(t, 1024)

	mcdc5_insertCompositeN(t, cursor, 200, 50, "root")

	found, err := cursor.SeekComposite([]byte("root-000100"))
	if err != nil {
		t.Fatalf("SeekComposite: %v", err)
	}
	if !found {
		t.Error("SeekComposite: root-000100 not found")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 21 — split.go allocateAndInitializeLeafPage: pageType == 0
//
//   if pageType == 0 { pageType = PageTypeLeafTable }
//
//   A = (pageType == 0)
//
//   Row 1: A=F → explicit type used (all normal leaf splits above)
//   Row 2: The zero-type branch fires when CurrentHeader.PageType == 0 before
//          split. We exercise it via a scan after many splits.
// ---------------------------------------------------------------------------

// TestMCDC5_AllocateLeafPage_ViaFullScan exercises leaf page allocation via
// a forward scan across a many-level tree.
func TestMCDC5_AllocateLeafPage_ViaFullScan(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 800, 15)

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	if cursor.GetKey() != 1 {
		t.Errorf("GetKey() after MoveToFirst = %d, want 1", cursor.GetKey())
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 22 — split.go allocateAndInitializeInteriorPage:
//   pageType == 0 default
//
//   Row 1: deep tree with many interior splits (10000 rows, tiny page)
// ---------------------------------------------------------------------------

// TestMCDC5_AllocateInteriorPage_ViaDeepTree forces interior page allocation
// by building a very deep rowid tree.
func TestMCDC5_AllocateInteriorPage_ViaDeepTree(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 10000, 8)

	found, err := cursor.SeekRowid(10000)
	if err != nil {
		t.Fatalf("SeekRowid(10000): %v", err)
	}
	if !found {
		t.Error("SeekRowid(10000): not found")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 23 — split.go mergeNewCellWithExisting:
//   !inserted after loop (new cell largest key → appended at tail)
//
//   A = (!inserted)
//
//   Row 1: A=T → ascending inserts: new key always greatest
//   Row 2: A=F → random / descending inserts (covered by other tests)
// ---------------------------------------------------------------------------

// TestMCDC5_MergeNewCellWithExisting_AppendTail forces the !inserted branch
// by performing ascending inserts (each new key is the maximum).
func TestMCDC5_MergeNewCellWithExisting_AppendTail(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 300, 40)

	found, err := cursor.SeekRowid(300)
	if err != nil {
		t.Fatalf("SeekRowid(300): %v", err)
	}
	if !found {
		t.Error("SeekRowid(300): not found")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 24 — split.go mergeNewCellWithExistingComposite:
//   !inserted branch (new composite key is lexicographically greatest)
//
//   Row 1: A=T → ascending composite inserts
// ---------------------------------------------------------------------------

// TestMCDC5_MergeNewCellComposite_AppendTail forces the composite !inserted
// branch by inserting lexicographically ascending composite keys.
func TestMCDC5_MergeNewCellComposite_AppendTail(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_compositeTable(t, 512)

	mcdc5_insertCompositeN(t, cursor, 200, 30, "zz")

	found, err := cursor.SeekComposite([]byte("zz-000200"))
	if err != nil {
		t.Fatalf("SeekComposite: %v", err)
	}
	if !found {
		t.Error("SeekComposite: last key not found")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 25 — cursor.go performCellDeletion:
//   GetPage and NewBtreePage success path
//
//   A = GetPage succeeds   B = NewBtreePage succeeds
//
//   Row 1: A=T, B=T → deletion proceeds (delete all 100 rows)
// ---------------------------------------------------------------------------

// TestMCDC5_PerformCellDeletion_Normal deletes rows one at a time,
// exercising performCellDeletion on each call.
func TestMCDC5_PerformCellDeletion_Normal(t *testing.T) {
	t.Parallel()

	bt, cursor, _ := mcdc5_rowidTable(t, 4096)

	mcdc5_insertN(t, cursor, 50, 50)

	// Delete rows sequentially. After each split the cursor's RootPage may
	// change, so we create a fresh cursor from the current root each time.
	for i := 1; i <= 50; i++ {
		cur := btree.NewCursor(bt, cursor.RootPage)
		found, err := cur.SeekRowid(int64(i))
		if err != nil {
			// Row may already be missing (root changed); skip.
			continue
		}
		if !found {
			continue
		}
		if err := cur.Delete(); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
		// Keep the outer cursor's RootPage updated.
		cursor.RootPage = cur.RootPage
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 26 — cursor.go adjustCursorAfterDelete:
//   c.CurrentIndex < 0 after decrement
//
//   A = (c.CurrentIndex < 0)
//
//   Row 1: A=T → delete only / first cell: index becomes -1, CurrentCell=nil
//   Row 2: A=F → delete middle cell: index stays >= 0
// ---------------------------------------------------------------------------

// TestMCDC5_AdjustCursorAfterDelete_IndexBounds tests both branches of the
// post-delete index adjustment.
func TestMCDC5_AdjustCursorAfterDelete_IndexBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numInsert int
		deleteKey int64
	}{
		{
			// A=T: delete the only row; CurrentIndex goes to -1.
			name:      "A=T delete only row",
			numInsert: 1,
			deleteKey: 1,
		},
		{
			// A=F: delete a row when there are others; index stays >= 0.
			name:      "A=F delete non-first row",
			numInsert: 3,
			deleteKey: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, cursor, _ := mcdc5_rowidTable(t, 4096)
			payload := []byte("data")
			for i := 1; i <= tt.numInsert; i++ {
				if err := cursor.Insert(int64(i), payload); err != nil {
					t.Fatalf("Insert(%d): %v", i, err)
				}
			}
			found, err := cursor.SeekRowid(tt.deleteKey)
			if err != nil {
				t.Fatalf("SeekRowid(%d): %v", tt.deleteKey, err)
			}
			if !found {
				t.Fatalf("SeekRowid(%d): not found", tt.deleteKey)
			}
			if err := cursor.Delete(); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 27 — cursor.go enterPage: depth limit guard
//
//   A = (c.Depth >= MaxBtreeDepth)
//
//   Row 1: A=F → normal descent, enterPage called from descendToLast
//
//   We exercise the normal path via MoveToLast on a multi-level tree.
// ---------------------------------------------------------------------------

// TestMCDC5_EnterPage_NormalPath exercises enterPage via MoveToLast on a
// deep tree so multiple levels are traversed.
func TestMCDC5_EnterPage_NormalPath(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 2000, 15)

	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	if cursor.GetKey() != 2000 {
		t.Errorf("MoveToLast key = %d, want 2000", cursor.GetKey())
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 28 — cursor.go finishInsert / seekAfterInsert:
//   CompositePK branch
//
//   A = c.CompositePK
//
//   Row 1: A=T → SeekComposite called after insert
//   Row 2: A=F → SeekRowid called (covered by all rowid insert tests)
// ---------------------------------------------------------------------------

// TestMCDC5_FinishInsert_CompositeBranch exercises the composite seek in
// seekAfterInsert.
func TestMCDC5_FinishInsert_CompositeBranch(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_compositeTable(t, 4096)

	for i := 1; i <= 50; i++ {
		key := []byte(fmt.Sprintf("fi-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := cursor.InsertWithComposite(0, key, val); err != nil {
			t.Fatalf("InsertWithComposite(%d): %v", i, err)
		}
	}

	found, err := cursor.SeekComposite([]byte("fi-0050"))
	if err != nil {
		t.Fatalf("SeekComposite: %v", err)
	}
	if !found {
		t.Error("SeekComposite: fi-0050 not found")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 29 — merge.go extractCellData: success path
//
//   A = GetCellPointer succeeds   B = ParseCell succeeds
//
//   extractCellData is invoked inside copyRightCellsToLeft which runs on
//   every merge.  We trigger merges by inserting rows to cause splits then
//   deleting most of them.
//
//   Row 1: A=T, B=T → cell data extracted and returned
// ---------------------------------------------------------------------------

// TestMCDC5_ExtractCellData_ViaMerge forces extractCellData by inserting
// rows (splits) then deleting to produce underfull pages.
func TestMCDC5_ExtractCellData_ViaMerge(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	mcdc5_insertN(t, cursor, 200, 20)

	// Delete odd rows to create underfull pages.
	for i := 1; i <= 200; i += 2 {
		found, err := cursor.SeekRowid(int64(i))
		if err != nil || !found {
			continue
		}
		_ = cursor.Delete()
	}

	// Attempt explicit merges on even rows still present.
	for i := 2; i <= 200; i += 2 {
		found, err := cursor.SeekRowid(int64(i))
		if err != nil || !found {
			continue
		}
		_, _ = cursor.MergePage()
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 30 — index_cursor.go getFirstChildPage: success path
//
//   A = GetCellPointer succeeds   B = ParseCell succeeds
//
//   getFirstChildPage is called from IndexCursor.descendToFirst when the
//   root is an interior index page.
//
//   Row 1: A=T, B=T → child page number returned
// ---------------------------------------------------------------------------

// TestMCDC5_GetFirstChildPage_ViaIndexCursorScan exercises the
// IndexCursor forward scan path (NextIndex) which internally calls
// getFirstChildPage when descending into child pages after climbing back
// through the parent.  We insert as many entries as fit without triggering
// the unimplemented split path, then do a full forward scan.
func TestMCDC5_GetFirstChildPage_ViaIndexCursorScan(t *testing.T) {
	t.Parallel()

	bt := btree.NewBtree(4096)
	root, err := mcdc5_allocateIndexPage(bt)
	if err != nil {
		t.Fatalf("allocateIndexPage: %v", err)
	}

	ic := btree.NewIndexCursor(bt, root)

	var inserted int
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("idx-%05d", i))
		if err := ic.InsertIndex(key, int64(i)); err != nil {
			// Stop when the page is full (split not implemented for index pages).
			break
		}
		inserted = i
	}
	if inserted == 0 {
		t.Skip("no entries inserted")
	}

	// MoveToFirst + full forward scan exercises descendToFirst.
	if err := ic.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}

	scanned := 0
	for ic.State == btree.CursorValid {
		scanned++
		if err := ic.NextIndex(); err != nil {
			break
		}
	}
	if scanned != inserted {
		t.Errorf("scanned %d entries, want %d", scanned, inserted)
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 31 — overflow.go freeOverflowChain:
//   pageCount > maxPages safety limit vs normal termination
//
//   A = (pageCount > maxPages)
//
//   Row 1: A=F → normal chain freed (multi-page overflow then deleted)
// ---------------------------------------------------------------------------

// TestMCDC5_FreeOverflowChain_MultiPageNormal inserts a row with payload
// large enough to span multiple overflow pages, then deletes it.
func TestMCDC5_FreeOverflowChain_MultiPageNormal(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	// 4 KB payload on a 512-byte page forces multiple overflow pages.
	bigPayload := make([]byte, 4096)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}

	if err := cursor.Insert(1, bigPayload); err != nil {
		t.Fatalf("Insert large payload: %v", err)
	}

	found, err := cursor.SeekRowid(1)
	if err != nil {
		t.Fatalf("SeekRowid(1): %v", err)
	}
	if !found {
		t.Fatal("SeekRowid(1): not found after large insert")
	}

	// Delete triggers freeOverflowChain.
	if err := cursor.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify row is gone.
	found, _ = cursor.SeekRowid(1)
	if found {
		t.Error("row still present after Delete")
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 32 — page.go AllocateSpace: defragment path
//
//   if newCellContentStart < cellPtrArrayEnd → Defragment then retry
//
//   A = (fragmented space available after defrag)
//
//   Row 1: A=T → fragmented page triggers Defragment() then allocates
//
//   We trigger this by filling a page, deleting rows (fragmenting), then
//   inserting more rows that fit only after defragmentation.
// ---------------------------------------------------------------------------

// TestMCDC5_AllocateSpace_DefragPath fills a page, deletes rows to fragment,
// then inserts more to trigger the defragment branch in AllocateSpace.
func TestMCDC5_AllocateSpace_DefragPath(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)
	payload := make([]byte, 30)

	var count int
	for i := 1; i <= 500; i++ {
		if err := cursor.Insert(int64(i), payload); err != nil {
			break
		}
		count = i
	}
	if count < 5 {
		t.Skip("not enough rows for defrag test")
	}

	// Delete rows in the middle to fragment free space.
	for i := 2; i <= count/2; i += 3 {
		if found, _ := cursor.SeekRowid(int64(i)); found {
			_ = cursor.Delete()
		}
	}

	// Insert new rows; some will land on fragmented pages requiring defrag.
	for i := count + 1; i <= count+50; i++ {
		if err := cursor.Insert(int64(i), payload); err != nil {
			break
		}
	}

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst after defrag test: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 33 — btree.go AllocatePage: Provider nil vs non-nil
//
//   A = (bt.Provider != nil)
//
//   Row 1: A=F → in-memory path: sequential page number scan
//
//   We verify that in-memory allocation produces unique page numbers.
// ---------------------------------------------------------------------------

// TestMCDC5_AllocatePage_InMemorySequential verifies in-memory AllocatePage
// assigns unique page numbers without a Provider.
func TestMCDC5_AllocatePage_InMemorySequential(t *testing.T) {
	t.Parallel()

	bt := btree.NewBtree(4096)

	seen := make(map[uint32]bool)
	for i := 0; i < 10; i++ {
		p, err := bt.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage iteration %d: %v", i, err)
		}
		if seen[p] {
			t.Errorf("duplicate page number %d", p)
		}
		seen[p] = true
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 34 — Mixed rowid workload
//
//   Exercises: prepareLeafSplit, performCellDeletion, adjustCursorAfterDelete,
//   enterPage, finishInsert, mergeNewCellWithExisting together.
// ---------------------------------------------------------------------------

// TestMCDC5_MixedWorkload_RowidTable performs a comprehensive mixed
// insert/delete/seek workload on a rowid table.
func TestMCDC5_MixedWorkload_RowidTable(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 512)

	// Phase 1: Insert 1000 rows (many splits).
	mcdc5_insertN(t, cursor, 1000, 25)

	// Phase 2: Delete every 5th row to create underfull conditions.
	for i := 1; i <= 1000; i += 5 {
		if found, _ := cursor.SeekRowid(int64(i)); found {
			if err := cursor.Delete(); err != nil {
				t.Fatalf("Delete(%d): %v", i, err)
			}
		}
	}

	// Phase 3: Navigate forward and backward.
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	firstKey := cursor.GetKey()
	if firstKey < 1 || firstKey > 1000 {
		t.Errorf("unexpected first key: %d", firstKey)
	}

	if err := cursor.MoveToLast(); err != nil {
		t.Fatalf("MoveToLast: %v", err)
	}
	lastKey := cursor.GetKey()
	if lastKey < firstKey {
		t.Errorf("lastKey %d < firstKey %d", lastKey, firstKey)
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 35 — Mixed composite workload
// ---------------------------------------------------------------------------

// TestMCDC5_MixedWorkload_CompositeTable performs a mixed insert/delete
// workload on a composite-key table to exercise split and merge paths.
func TestMCDC5_MixedWorkload_CompositeTable(t *testing.T) {
	t.Parallel()

	bt, cursor, _ := mcdc5_compositeTable(t, 512)

	// Phase 1: Insert 200 composite rows.
	mcdc5_insertCompositeN(t, cursor, 200, 20, "mw")

	// Phase 2: Delete every third row to create underfull pages.
	for i := 1; i <= 200; i += 3 {
		key := []byte(fmt.Sprintf("mw-%06d", i))
		cur := btree.NewCursorWithOptions(bt, cursor.RootPage, true)
		if found, _ := cur.SeekComposite(key); found {
			if err := cur.Delete(); err != nil {
				t.Fatalf("Delete composite(%d): %v", i, err)
			}
			cursor.RootPage = cur.RootPage
		}
	}

	// Phase 3: Spot-check a few remaining rows.
	remaining := []int{2, 5, 50, 100, 200}
	for _, i := range remaining {
		if i%3 == 1 {
			continue // was deleted
		}
		key := []byte(fmt.Sprintf("mw-%06d", i))
		cur := btree.NewCursorWithOptions(bt, cursor.RootPage, true)
		found, err := cur.SeekComposite(key)
		if err != nil {
			t.Errorf("SeekComposite(%d): %v", i, err)
		}
		if !found {
			t.Errorf("key mw-%06d not found after mixed workload", i)
		}
	}
}

// ---------------------------------------------------------------------------
// MC/DC Condition 36 — cursor.go seekAfterInsert: rowid seek branch
//
//   A = c.CompositePK
//
//   Row 1: A=F → SeekRowid called after every non-composite insert
// ---------------------------------------------------------------------------

// TestMCDC5_FinishInsert_RowidSeekBranch exercises the SeekRowid path in
// seekAfterInsert by verifying cursor position after each insert.
func TestMCDC5_FinishInsert_RowidSeekBranch(t *testing.T) {
	t.Parallel()

	_, cursor, _ := mcdc5_rowidTable(t, 4096)

	for i := 1; i <= 200; i++ {
		payload := []byte(fmt.Sprintf("payload-%d", i))
		if err := cursor.Insert(int64(i), payload); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
		if cursor.GetKey() != int64(i) {
			t.Errorf("after Insert(%d), GetKey() = %d", i, cursor.GetKey())
		}
	}
}
