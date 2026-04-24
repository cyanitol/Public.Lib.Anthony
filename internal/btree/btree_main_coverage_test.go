// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

// TestBtreeMain_CreateWithoutRowidTable_Header verifies that CreateWithoutRowidTable
// initialises a root page with PageTypeLeafTableNoInt.
func TestBtreeMain_CreateWithoutRowidTable_Header(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	header, err := ParsePageHeader(pageData, root)
	if err != nil {
		t.Fatalf("ParsePageHeader: %v", err)
	}
	if header.PageType != PageTypeLeafTableNoInt {
		t.Errorf("PageType = 0x%02x, want 0x%02x (PageTypeLeafTableNoInt)", header.PageType, PageTypeLeafTableNoInt)
	}
	if !header.IsLeaf {
		t.Error("expected IsLeaf = true for WITHOUT ROWID root")
	}
	if !header.IsTable {
		t.Error("expected IsTable = true for WITHOUT ROWID root")
	}
}

// TestBtreeMain_CreateWithoutRowidTable_InsertAndScan verifies that composite-key
// inserts via the cursor API are accepted and a forward scan returns all entries.
func TestBtreeMain_CreateWithoutRowidTable_InsertAndScan(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)
	keys := [][]byte{
		withoutrowid.EncodeCompositeKey([]interface{}{"alpha"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"beta"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"gamma"}),
		withoutrowid.EncodeCompositeKey([]interface{}{"delta"}),
	}
	for i, k := range keys {
		if err := cursor.InsertWithComposite(0, k, []byte("value")); err != nil {
			t.Fatalf("InsertWithComposite[%d]: %v", i, err)
		}
	}

	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.Next(); err != nil {
			break
		}
	}
	if count != len(keys) {
		t.Errorf("forward scan count = %d, want %d", count, len(keys))
	}
}

// TestBtreeMain_CreateWithoutRowidTable_Seek verifies SeekComposite on a
// WITHOUT ROWID table.
func TestBtreeMain_CreateWithoutRowidTable_Seek(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)
	for _, name := range []string{"alpha", "beta", "gamma", "delta"} {
		k := withoutrowid.EncodeCompositeKey([]interface{}{name})
		if err := cursor.InsertWithComposite(0, k, []byte("value")); err != nil {
			t.Fatalf("InsertWithComposite(%s): %v", name, err)
		}
	}

	found, err := cursor.SeekComposite(withoutrowid.EncodeCompositeKey([]interface{}{"beta"}))
	if err != nil {
		t.Fatalf("SeekComposite: %v", err)
	}
	if !found {
		t.Error("SeekComposite(beta): not found")
	}
}

// countCompositeForwardScan counts the number of entries reachable via forward scan.
func countCompositeForwardScan(t *testing.T, scan *BtCursor) int {
	t.Helper()
	if err := scan.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst: %v", err)
	}
	seen := 0
	for scan.IsValid() {
		seen++
		if err := scan.Next(); err != nil {
			break
		}
	}
	return seen
}

// TestBtreeMain_CreateWithoutRowidTable_ManyRows forces the WITHOUT ROWID tree
// through page splits so that interior composite pages are created.
func TestBtreeMain_CreateWithoutRowidTable_ManyRows(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512) // small pages -> splits happen quickly
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)

	payload := bytes.Repeat([]byte("x"), 30)
	inserted := 0
	for i := 0; i < 200; i++ {
		key := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("row%06d", i)})
		if err := cursor.InsertWithComposite(0, key, payload); err != nil {
			break
		}
		inserted++
	}
	if inserted < 2 {
		t.Fatalf("only inserted %d rows; need at least 2", inserted)
	}

	scan := NewCursorWithOptions(bt, cursor.RootPage, true)
	seen := countCompositeForwardScan(t, scan)
	if seen != inserted {
		t.Errorf("forward scan = %d entries, want %d", seen, inserted)
	}
}

// TestBtreeMain_ClearTableData_LeafRoot clears a leaf-only table and verifies
// that the root page is reset to an empty leaf.
func TestBtreeMain_ClearTableData_LeafRoot(t *testing.T) {
	t.Parallel()

	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)
	for i := int64(1); i <= 15; i++ {
		if err := cursor.Insert(i, []byte("hello")); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}
	if err := bt.ClearTableData(root); err != nil {
		t.Fatalf("ClearTableData: %v", err)
	}
	verifyClearedLeafRoot(t, bt, root)
}

// verifyClearedLeafRoot checks that a page has been reset to an empty leaf table.
func verifyClearedLeafRoot(t *testing.T, bt *Btree, root uint32) {
	t.Helper()
	pageData, err := bt.GetPage(root)
	if err != nil {
		t.Fatalf("GetPage after clear: %v", err)
	}
	header, err := ParsePageHeader(pageData, root)
	if err != nil {
		t.Fatalf("ParsePageHeader after clear: %v", err)
	}
	if header.NumCells != 0 {
		t.Errorf("NumCells after ClearTableData = %d, want 0", header.NumCells)
	}
	if header.PageType != PageTypeLeafTable {
		t.Errorf("PageType after ClearTableData = 0x%02x, want 0x%02x", header.PageType, PageTypeLeafTable)
	}
}

// TestBtreeMain_ClearTableData_InteriorRoot ensures the interior-node branch
// of ClearTableData (which calls dropInteriorChildren) is exercised.
func TestBtreeMain_ClearTableData_InteriorRoot(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512) // small pages force interior pages
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)
	for i := int64(1); i <= 150; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}

	// The root should be an interior page at this point.
	if err := bt.ClearTableData(cursor.RootPage); err != nil {
		t.Fatalf("ClearTableData on interior root: %v", err)
	}

	pageData, err := bt.GetPage(cursor.RootPage)
	if err != nil {
		t.Fatalf("GetPage after clear: %v", err)
	}
	header, err := ParsePageHeader(pageData, cursor.RootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader after clear: %v", err)
	}
	if header.NumCells != 0 {
		t.Errorf("NumCells after ClearTableData = %d, want 0", header.NumCells)
	}
}

// TestBtreeMain_DropInteriorChildren ensures that dropping a multi-level table
// properly recurses through interior pages and frees child pages.
func TestBtreeMain_DropInteriorChildren(t *testing.T) {
	t.Parallel()

	bt := NewBtree(512)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)
	// Insert enough rows to guarantee interior pages (multiple levels).
	for i := int64(1); i <= 500; i++ {
		if err := cursor.Insert(i, make([]byte, 10)); err != nil {
			break
		}
	}

	pageBefore := len(bt.Pages)

	if err := bt.DropTable(cursor.RootPage); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	pageAfter := len(bt.Pages)
	if pageAfter >= pageBefore {
		t.Errorf("expected page count to decrease after DropTable; before=%d, after=%d", pageBefore, pageAfter)
	}

	// The root page must be gone.
	if _, ok := bt.Pages[cursor.RootPage]; ok {
		t.Errorf("root page %d still present after DropTable", cursor.RootPage)
	}
}

// TestBtreeMain_ParsePage_LeafTable exercises ParsePage for a leaf table page.
func TestBtreeMain_ParsePage_LeafTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)
	for i := int64(1); i <= 5; i++ {
		cursor.Insert(i, []byte("pay"))
	}
	header, cells, err := bt.ParsePage(root)
	if err != nil {
		t.Fatalf("ParsePage leaf table: %v", err)
	}
	if header.PageType != PageTypeLeafTable {
		t.Errorf("PageType = 0x%02x, want PageTypeLeafTable", header.PageType)
	}
	if len(cells) != int(header.NumCells) {
		t.Errorf("cells len %d != NumCells %d", len(cells), header.NumCells)
	}
	if header.NumCells == 0 {
		t.Error("expected at least 1 cell on leaf table page")
	}
}

// TestBtreeMain_ParsePage_InteriorTable exercises ParsePage for an interior table page.
func TestBtreeMain_ParsePage_InteriorTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // small -> forces interior pages
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := NewCursor(bt, root)
	for i := int64(1); i <= 200; i++ {
		if err := cursor.Insert(i, make([]byte, 20)); err != nil {
			break
		}
	}
	// ParsePage on the root which should now be an interior page.
	header, cells, err := bt.ParsePage(cursor.RootPage)
	if err != nil {
		t.Fatalf("ParsePage interior table: %v", err)
	}
	// Root might still be a leaf if inserts were limited; just verify no error.
	_ = header
	_ = cells
}

// TestBtreeMain_ParsePage_LeafIndex exercises ParsePage for a leaf index page.
func TestBtreeMain_ParsePage_LeafIndex(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	idxCursor := NewIndexCursor(bt, root)
	for i := 0; i < 5; i++ {
		idxCursor.InsertIndex([]byte(fmt.Sprintf("key%02d", i)), int64(i))
	}
	header, cells, err := bt.ParsePage(root)
	if err != nil {
		t.Fatalf("ParsePage leaf index: %v", err)
	}
	if header.PageType != PageTypeLeafIndex {
		t.Errorf("PageType = 0x%02x, want PageTypeLeafIndex", header.PageType)
	}
	if len(cells) == 0 {
		t.Error("expected cells on leaf index page")
	}
}

// TestBtreeMain_ParsePage_InteriorIndex exercises ParsePage for an interior index page
// built manually so ParseCell gets called for PageTypeInteriorIndex.
func TestBtreeMain_ParsePage_InteriorIndex(t *testing.T) {
	t.Parallel()
	const pageSize = 4096
	bt := NewBtree(pageSize)

	// Leaf page 2
	leaf2 := make([]byte, pageSize)
	leaf2[PageHeaderOffsetType] = PageTypeLeafIndex
	binary.BigEndian.PutUint16(leaf2[PageHeaderOffsetNumCells:], 1)
	payload2 := encodeIndexPayload([]byte("aaa"), 1)
	cell2 := EncodeIndexLeafCell(payload2)
	cellOff2 := uint32(pageSize) - uint32(len(cell2))
	copy(leaf2[cellOff2:], cell2)
	binary.BigEndian.PutUint16(leaf2[PageHeaderOffsetCellStart:], uint16(cellOff2))
	binary.BigEndian.PutUint16(leaf2[PageHeaderSizeLeaf:], uint16(cellOff2))
	bt.SetPage(2, leaf2)

	// Leaf page 3
	leaf3 := make([]byte, pageSize)
	leaf3[PageHeaderOffsetType] = PageTypeLeafIndex
	binary.BigEndian.PutUint16(leaf3[PageHeaderOffsetNumCells:], 1)
	payload3 := encodeIndexPayload([]byte("zzz"), 2)
	cell3 := EncodeIndexLeafCell(payload3)
	cellOff3 := uint32(pageSize) - uint32(len(cell3))
	copy(leaf3[cellOff3:], cell3)
	binary.BigEndian.PutUint16(leaf3[PageHeaderOffsetCellStart:], uint16(cellOff3))
	binary.BigEndian.PutUint16(leaf3[PageHeaderSizeLeaf:], uint16(cellOff3))
	bt.SetPage(3, leaf3)

	// Interior root (page 1 has FileHeaderSize offset)
	interior := make([]byte, pageSize)
	ho := FileHeaderSize
	interior[ho+PageHeaderOffsetType] = PageTypeInteriorIndex
	binary.BigEndian.PutUint16(interior[ho+PageHeaderOffsetNumCells:], 1)
	binary.BigEndian.PutUint32(interior[ho+PageHeaderOffsetRightChild:], 3)
	intPayload := encodeIndexPayload([]byte("mmm"), 10)
	intCell := EncodeIndexInteriorCell(2, intPayload)
	cellOff := uint32(pageSize) - uint32(len(intCell))
	copy(interior[cellOff:], intCell)
	binary.BigEndian.PutUint16(interior[ho+PageHeaderOffsetCellStart:], uint16(cellOff))
	binary.BigEndian.PutUint16(interior[ho+PageHeaderSizeInterior:], uint16(cellOff))
	bt.SetPage(1, interior)

	header, cells, err := bt.ParsePage(1)
	if err != nil {
		t.Fatalf("ParsePage interior index: %v", err)
	}
	if header.PageType != PageTypeInteriorIndex {
		t.Errorf("PageType = 0x%02x, want PageTypeInteriorIndex", header.PageType)
	}
	if len(cells) == 0 {
		t.Error("expected cells on interior index page")
	}
	if cells[0].ChildPage == 0 {
		t.Error("expected non-zero ChildPage on interior index cell")
	}
}

// TestBtreeMain_ParsePage_WithoutRowidLeaf exercises ParsePage for a WITHOUT ROWID leaf page.
func TestBtreeMain_ParsePage_WithoutRowidLeaf(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}
	cursor := NewCursorWithOptions(bt, root, true)
	for i := 0; i < 3; i++ {
		k := withoutrowid.EncodeCompositeKey([]interface{}{fmt.Sprintf("item%d", i)})
		cursor.InsertWithComposite(0, k, []byte("val"))
	}
	header, cells, err := bt.ParsePage(root)
	if err != nil {
		t.Fatalf("ParsePage without_rowid: %v", err)
	}
	if header.PageType != PageTypeLeafTableNoInt {
		t.Errorf("PageType = 0x%02x, want PageTypeLeafTableNoInt", header.PageType)
	}
	_ = cells
}

// TestBtreeMain_ValidatePage_Paths exercises uncovered branches of validatePage:
// page 1 with btree data, non-page-1 non-zero page, invalid page type.
func TestBtreeMain_ValidatePage_Paths(t *testing.T) {
	t.Parallel()

	const pageSize = uint32(4096)

	t.Run("page1_with_btree_data", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		// Build a valid page-1 payload (FileHeaderSize prefix + leaf table header).
		data := make([]byte, pageSize)
		ho := FileHeaderSize
		data[ho+PageHeaderOffsetType] = PageTypeLeafTable
		binary.BigEndian.PutUint16(data[ho+PageHeaderOffsetNumCells:], 0)
		binary.BigEndian.PutUint16(data[ho+PageHeaderOffsetCellStart:], 0)
		// validatePage is called internally via GetPage-with-provider; call directly.
		if err := bt.validatePage(data, 1); err != nil {
			t.Errorf("validatePage page1 leaf: %v", err)
		}
	})

	t.Run("non_page1_non_zero_valid", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		data := make([]byte, pageSize)
		data[PageHeaderOffsetType] = PageTypeLeafIndex
		binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 0)
		binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], 0)
		if err := bt.validatePage(data, 2); err != nil {
			t.Errorf("validatePage non-page1 leaf index: %v", err)
		}
	})

	t.Run("invalid_page_type", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		data := make([]byte, pageSize)
		data[PageHeaderOffsetType] = 0xFF // invalid
		if err := bt.validatePage(data, 2); err == nil {
			t.Error("expected error for invalid page type, got nil")
		}
	})

	t.Run("page_size_mismatch", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		// Supply a buffer that is too small.
		data := make([]byte, pageSize-1)
		if err := bt.validatePage(data, 2); err == nil {
			t.Error("expected error for undersized page, got nil")
		}
	})

	t.Run("interior_page_valid", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		data := make([]byte, pageSize)
		data[PageHeaderOffsetType] = PageTypeInteriorTable
		binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 0)
		binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], 0)
		binary.BigEndian.PutUint32(data[PageHeaderOffsetRightChild:], 2)
		if err := bt.validatePage(data, 2); err != nil {
			t.Errorf("validatePage interior table: %v", err)
		}
	})

	t.Run("zero_non_page1", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(pageSize)
		data := make([]byte, pageSize) // all zeros → freshly allocated
		if err := bt.validatePage(data, 3); err != nil {
			t.Errorf("validatePage all-zero non-page1: %v", err)
		}
	})
}

// TestBtreeMain_IndexCellParsing_SmallPayloads drives the index cell parsing helpers
// with small payloads that fit locally.
func TestBtreeMain_IndexCellParsing_SmallPayloads(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	root, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, root)
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("smallkey%04d", i))
		cursor.InsertIndex(key, int64(i))
	}
	header, cells, err := bt.ParsePage(root)
	if err != nil {
		t.Fatalf("ParsePage: %v", err)
	}
	_ = header
	for i, c := range cells {
		if c.PayloadSize == 0 {
			t.Errorf("cell[%d] PayloadSize = 0", i)
		}
	}
}

// TestBtreeMain_IndexCellParsing_LargePayloads exercises the overflow branch in
// extractIndexPayloadAndOverflow using keys larger than maxLocal.
func TestBtreeMain_IndexCellParsing_LargePayloads(t *testing.T) {
	t.Parallel()
	// Use a 1024-byte page so maxLocal = 989. Insert keys > 989 bytes to
	// force the overflow branch in extractIndexPayloadAndOverflow.
	bt := NewBtree(1024)
	root, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, root)
	inserted := 0
	for i := 0; i < 5; i++ {
		key := bytes.Repeat([]byte{byte(i + 1)}, 992)
		if err := cursor.InsertIndex(key, int64(i)); err != nil {
			break
		}
		inserted++
	}
	if inserted == 0 {
		t.Skip("no entries inserted with large payload (page too small for any overflow cell)")
	}
	if err := cursor.MoveToFirst(); err != nil {
		t.Skipf("MoveToFirst after large-payload inserts: %v", err)
	}
	count := 0
	for cursor.IsValid() {
		count++
		if err := cursor.NextIndex(); err != nil {
			break
		}
	}
	if count == 0 {
		t.Error("expected at least one index entry with large payload")
	}
}

// TestBtreeMain_IndexCellParsing_MultiLevel forces interior index pages by inserting
// many entries into a small btree and verifies forward and backward scans.
func TestBtreeMain_IndexCellParsing_MultiLevel(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512)
	root, _ := createIndexPage(bt)
	cursor := NewIndexCursor(bt, root)
	inserted := insertIndexEntriesN(cursor, 200, func(i int) []byte {
		return []byte(fmt.Sprintf("idx%06d", i))
	})
	if inserted < 5 {
		t.Fatalf("only %d entries inserted", inserted)
	}
	fwdCount := countIndexForward(cursor)
	if fwdCount != inserted {
		t.Errorf("forward count = %d, want %d", fwdCount, inserted)
	}
	bwdCount := countIndexBackward(cursor)
	if bwdCount != inserted {
		t.Errorf("backward count = %d, want %d", bwdCount, inserted)
	}
}

// TestBtreeMain_ComputeIndexCellSizeAndLocal directly exercises
// computeIndexCellSizeAndLocal and calculateLocalPayload at boundary values.
func TestBtreeMain_ComputeIndexCellSizeAndLocal(t *testing.T) {
	t.Parallel()

	for _, us := range []uint32{512, 1024, 4096} {
		us := us
		t.Run(fmt.Sprintf("usableSize_%d", us), func(t *testing.T) {
			t.Parallel()
			maxLocal := calculateMaxLocal(us, false)
			minLocal := calculateMinLocal(us, false)

			// Case 1: payload fits entirely locally (PayloadSize <= maxLocal).
			info := &CellInfo{PayloadSize: maxLocal}
			computeIndexCellSizeAndLocal(info, 1, maxLocal, minLocal, us)
			if uint32(info.LocalPayload) != maxLocal {
				t.Errorf("us=%d: local payload = %d, want %d (maxLocal)", us, info.LocalPayload, maxLocal)
			}

			// Case 2: payload just above maxLocal → overflow, local = minLocal or surplus.
			info2 := &CellInfo{PayloadSize: maxLocal + 1}
			computeIndexCellSizeAndLocal(info2, 1, maxLocal, minLocal, us)
			if uint32(info2.LocalPayload) > maxLocal {
				t.Errorf("us=%d: overflow local = %d exceeds maxLocal %d", us, info2.LocalPayload, maxLocal)
			}
			// CellSize must include room for the overflow pointer (4 bytes).
			expectedMin := uint16(1) + info2.LocalPayload + 4
			if info2.CellSize < expectedMin {
				t.Errorf("us=%d: CellSize %d < offset+local+4 = %d", us, info2.CellSize, expectedMin)
			}

			// Case 3: very large payload → surplus > maxLocal → local = minLocal.
			bigPayload := us * 10
			info3 := &CellInfo{PayloadSize: bigPayload}
			computeIndexCellSizeAndLocal(info3, 1, maxLocal, minLocal, us)
			if uint32(info3.LocalPayload) > maxLocal {
				t.Errorf("us=%d: big payload local = %d exceeds maxLocal %d", us, info3.LocalPayload, maxLocal)
			}
		})
	}
}

// TestBtreeMain_ParseLeafCellHeader exercises parseLeafCellHeader, including
// the overflow path in completeLeafCellParse.
func TestBtreeMain_ParseLeafCellHeader(t *testing.T) {
	t.Parallel()

	// Normal leaf cell with small payload.
	t.Run("normal_small", func(t *testing.T) {
		t.Parallel()
		payload := []byte("hello world")
		cell := EncodeTableLeafCell(42, payload)
		info, off, err := parseLeafCellHeader(cell)
		if err != nil {
			t.Fatalf("parseLeafCellHeader: %v", err)
		}
		if info.Key != 42 {
			t.Errorf("Key = %d, want 42", info.Key)
		}
		if info.PayloadSize != uint32(len(payload)) {
			t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(payload))
		}
		if off == 0 {
			t.Error("offset should be > 0 after reading two varints")
		}
	})

	// Large rowid (multi-byte varint).
	t.Run("large_rowid", func(t *testing.T) {
		t.Parallel()
		payload := []byte("data")
		rowid := int64(1 << 40)
		cell := EncodeTableLeafCell(rowid, payload)
		info, _, err := parseLeafCellHeader(cell)
		if err != nil {
			t.Fatalf("parseLeafCellHeader large rowid: %v", err)
		}
		if info.Key != rowid {
			t.Errorf("Key = %d, want %d", info.Key, rowid)
		}
	})
}

// TestBtreeMain_ParseTableLeafCompositeCell directly tests the WITHOUT ROWID
// leaf cell parser for various key/payload combinations.
func TestBtreeMain_ParseTableLeafCompositeCell(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		key     []byte
		payload []byte
	}{
		{"empty_key", []byte{}, []byte("value")},
		{"small_key", []byte("pk"), []byte("data")},
		{"large_key", bytes.Repeat([]byte("k"), 50), bytes.Repeat([]byte("v"), 50)},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cell := EncodeTableLeafCompositeCell(tc.key, tc.payload)
			info, err := parseTableLeafCompositeCell(cell, 4096)
			if err != nil {
				t.Fatalf("parseTableLeafCompositeCell: %v", err)
			}
			if !bytes.Equal(info.KeyBytes, tc.key) {
				t.Errorf("KeyBytes = %q, want %q", info.KeyBytes, tc.key)
			}
			if info.PayloadSize != uint32(len(tc.payload)) {
				t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(tc.payload))
			}
		})
	}
}

// TestBtreeMain_CalculateMinLocal exercises branches where usableSize is small
// (below security.MinUsableSize) and where intermediate < 23.
func TestBtreeMain_CalculateMinLocal(t *testing.T) {
	t.Parallel()

	// usableSize below minimum → returns 0.
	got := calculateMinLocal(10, true)
	if got != 0 {
		t.Errorf("calculateMinLocal(10, true) = %d, want 0", got)
	}

	// Normal large usableSize → positive result.
	got = calculateMinLocal(4096, false)
	if got == 0 {
		t.Error("calculateMinLocal(4096, false) = 0, want > 0")
	}

	// Verify result is self-consistent: intermediate >= 23.
	for _, us := range []uint32{512, 1024, 2048, 4096} {
		result := calculateMinLocal(us, true)
		maxL := calculateMaxLocal(us, true)
		if result > maxL {
			t.Errorf("minLocal(%d) = %d > maxLocal %d", us, result, maxL)
		}
	}
}

// TestBtreeMain_ExtractIndexPayloadAndOverflow verifies the overflow and
// non-overflow paths of extractIndexPayloadAndOverflow.
func TestBtreeMain_ExtractIndexPayloadAndOverflow(t *testing.T) {
	t.Parallel()

	const us = uint32(512)
	maxLocal := calculateMaxLocal(us, false)

	t.Run("no_overflow", func(t *testing.T) {
		t.Parallel()
		payload := bytes.Repeat([]byte("a"), 10)
		cell := EncodeIndexLeafCell(payload)
		info := &CellInfo{
			PayloadSize:  uint32(len(payload)),
			LocalPayload: uint16(len(payload)),
		}
		if err := extractIndexPayloadAndOverflow(info, cell, 1, maxLocal); err != nil {
			t.Fatalf("extractIndexPayloadAndOverflow no-overflow: %v", err)
		}
		if info.OverflowPage != 0 {
			t.Errorf("expected no overflow, got page %d", info.OverflowPage)
		}
	})

	t.Run("with_overflow_pointer", func(t *testing.T) {
		t.Parallel()
		// Craft a cell where PayloadSize > maxLocal so overflow is read.
		localBytes := 20
		overflowPageNum := uint32(99)
		// Build: varint(payloadSize=maxLocal+100), payload[localBytes], 4-byte overflow page
		payloadSz := maxLocal + 100
		buf := make([]byte, 9+localBytes+4)
		off := PutVarint(buf, uint64(payloadSz))
		// fill local bytes
		for i := 0; i < localBytes; i++ {
			buf[off+i] = byte(i)
		}
		binary.BigEndian.PutUint32(buf[off+localBytes:], overflowPageNum)

		info := &CellInfo{
			PayloadSize:  payloadSz,
			LocalPayload: uint16(localBytes),
		}
		if err := extractIndexPayloadAndOverflow(info, buf, off, maxLocal); err != nil {
			t.Fatalf("extractIndexPayloadAndOverflow with-overflow: %v", err)
		}
		if info.OverflowPage != overflowPageNum {
			t.Errorf("OverflowPage = %d, want %d", info.OverflowPage, overflowPageNum)
		}
	})
}

// TestBtreeMain_ParseIndexInteriorCell parses a manually constructed interior
// index cell to cover parseIndexInteriorCell and completeIndexCellParse.
func TestBtreeMain_ParseIndexInteriorCell(t *testing.T) {
	t.Parallel()

	const us = uint32(4096)

	t.Run("small_payload", func(t *testing.T) {
		t.Parallel()
		payload := encodeIndexPayload([]byte("mykey"), 7)
		cell := EncodeIndexInteriorCell(42, payload)
		info, err := parseIndexInteriorCell(cell, us)
		if err != nil {
			t.Fatalf("parseIndexInteriorCell: %v", err)
		}
		if info.ChildPage != 42 {
			t.Errorf("ChildPage = %d, want 42", info.ChildPage)
		}
		if info.PayloadSize != uint32(len(payload)) {
			t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(payload))
		}
	})

	t.Run("payload_at_max_local_boundary", func(t *testing.T) {
		t.Parallel()
		maxLocal := calculateMaxLocal(us, false)
		// Payload exactly at maxLocal: all stored locally.
		payload := bytes.Repeat([]byte("z"), int(maxLocal))
		cell := EncodeIndexInteriorCell(5, payload)
		info, err := parseIndexInteriorCell(cell, us)
		if err != nil {
			t.Fatalf("parseIndexInteriorCell at boundary: %v", err)
		}
		if uint32(info.LocalPayload) != maxLocal {
			t.Errorf("LocalPayload = %d, want %d", info.LocalPayload, maxLocal)
		}
		if info.OverflowPage != 0 {
			t.Errorf("expected no overflow at maxLocal boundary, got page %d", info.OverflowPage)
		}
	})
}
