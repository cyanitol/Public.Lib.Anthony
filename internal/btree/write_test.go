// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if rootPage == 0 {
		t.Error("CreateTable() returned page number 0")
	}

	// Verify the page was created
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage(%d) error = %v", rootPage, err)
	}

	// Verify it's a leaf table page
	// For page 1, the page header starts at offset 100 (FileHeaderSize)
	// For other pages, it starts at offset 0
	headerOffset := 0
	if rootPage == 1 {
		headerOffset = FileHeaderSize
	}
	if pageData[headerOffset] != PageTypeLeafTable {
		t.Errorf("Page type = 0x%02x, want 0x%02x", pageData[headerOffset], PageTypeLeafTable)
	}

	// Verify it has 0 cells
	numCells := binary.BigEndian.Uint16(pageData[headerOffset+3 : headerOffset+5])
	if numCells != 0 {
		t.Errorf("NumCells = %d, want 0", numCells)
	}
}

func TestDropTable(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Drop the table
	err = bt.DropTable(rootPage)
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	// Verify the page was deleted
	_, err = bt.GetPage(rootPage)
	if err == nil {
		t.Error("GetPage() expected error after DropTable, got nil")
	}
}

func TestNewRowid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create an empty table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Get first rowid
	rowid, err := bt.NewRowid(rootPage)
	if err != nil {
		t.Fatalf("NewRowid() error = %v", err)
	}

	if rowid != 1 {
		t.Errorf("NewRowid() = %d, want 1", rowid)
	}
}

func TestAllocatePage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Allocate first page
	page1, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	// Allocate second page
	page2, err := bt.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}

	if page1 == page2 {
		t.Errorf("AllocatePage() returned duplicate page numbers: %d", page1)
	}

	// Verify pages were created
	_, err = bt.GetPage(page1)
	if err != nil {
		t.Errorf("GetPage(%d) error = %v", page1, err)
	}

	_, err = bt.GetPage(page2)
	if err != nil {
		t.Errorf("GetPage(%d) error = %v", page2, err)
	}
}

func TestEncodeTableLeafCell(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rowid   int64
		payload []byte
	}{
		{
			name:    "small payload",
			rowid:   1,
			payload: []byte("hello"),
		},
		{
			name:    "larger payload",
			rowid:   42,
			payload: []byte("The quick brown fox jumps over the lazy dog"),
		},
		{
			name:    "empty payload",
			rowid:   100,
			payload: []byte{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cell := EncodeTableLeafCell(tt.rowid, tt.payload)

			// Verify we can parse it back
			parsed, err := parseTableLeafCell(cell, 4096)
			if err != nil {
				t.Fatalf("parseTableLeafCell() error = %v", err)
			}

			if parsed.Key != tt.rowid {
				t.Errorf("Parsed rowid = %d, want %d", parsed.Key, tt.rowid)
			}

			if string(parsed.Payload) != string(tt.payload) {
				t.Errorf("Parsed payload = %q, want %q", parsed.Payload, tt.payload)
			}
		})
	}
}

func TestEncodeTableInteriorCell(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		childPage uint32
		rowid     int64
	}{
		{
			name:      "page 2 rowid 10",
			childPage: 2,
			rowid:     10,
		},
		{
			name:      "page 100 rowid 1000",
			childPage: 100,
			rowid:     1000,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cell := EncodeTableInteriorCell(tt.childPage, tt.rowid)

			// Verify we can parse it back
			parsed, err := parseTableInteriorCell(cell)
			if err != nil {
				t.Fatalf("parseTableInteriorCell() error = %v", err)
			}

			if parsed.ChildPage != tt.childPage {
				t.Errorf("Parsed childPage = %d, want %d", parsed.ChildPage, tt.childPage)
			}

			if parsed.Key != tt.rowid {
				t.Errorf("Parsed rowid = %d, want %d", parsed.Key, tt.rowid)
			}
		})
	}
}

func TestBtreePageInsertCell(t *testing.T) {
	t.Parallel()
	// Create a page (use page 2 to avoid file header at offset 100)
	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable             // Leaf table page
	binary.BigEndian.PutUint16(pageData[3:], 0) // NumCells = 0
	binary.BigEndian.PutUint16(pageData[5:], 0) // CellContentStart = 0 (end of page)

	btreePage, err := NewBtreePage(2, pageData, 4096)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Insert a cell
	cell := EncodeTableLeafCell(1, []byte("test data"))
	err = btreePage.InsertCell(0, cell)
	if err != nil {
		t.Fatalf("InsertCell() error = %v", err)
	}

	// Verify the cell was inserted
	if btreePage.Header.NumCells != 1 {
		t.Errorf("NumCells = %d, want 1", btreePage.Header.NumCells)
	}

	// Verify we can parse it back
	cellPtr, err := btreePage.Header.GetCellPointer(pageData, 0)
	if err != nil {
		t.Fatalf("GetCellPointer() error = %v", err)
	}

	parsed, err := ParseCell(PageTypeLeafTable, pageData[cellPtr:], 4096)
	if err != nil {
		t.Fatalf("ParseCell() error = %v", err)
	}

	if parsed.Key != 1 {
		t.Errorf("Parsed key = %d, want 1", parsed.Key)
	}

	if string(parsed.Payload) != "test data" {
		t.Errorf("Parsed payload = %q, want %q", parsed.Payload, "test data")
	}
}

func TestBtreePageDeleteCell(t *testing.T) {
	t.Parallel()
	// Create a page with one cell (use page 2 to avoid file header at offset 100)
	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 1) // NumCells = 1

	// Insert a cell manually
	cell := EncodeTableLeafCell(1, []byte("test"))
	cellOffset := 4096 - len(cell)
	copy(pageData[cellOffset:], cell)
	binary.BigEndian.PutUint16(pageData[5:], uint16(cellOffset)) // CellContentStart
	binary.BigEndian.PutUint16(pageData[8:], uint16(cellOffset)) // Cell pointer

	btreePage, err := NewBtreePage(2, pageData, 4096)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Delete the cell
	err = btreePage.DeleteCell(0)
	if err != nil {
		t.Fatalf("DeleteCell() error = %v", err)
	}

	// Verify the cell was deleted
	if btreePage.Header.NumCells != 0 {
		t.Errorf("NumCells = %d, want 0", btreePage.Header.NumCells)
	}
}

func TestBtreePageDefragment(t *testing.T) {
	t.Parallel()
	// Create a page with fragmented space (use page 2 to avoid file header at offset 100)
	pageData := make([]byte, 4096)
	pageData[0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 2) // NumCells = 2

	// Insert two cells with a gap
	cell1 := EncodeTableLeafCell(1, []byte("cell1"))
	cell2 := EncodeTableLeafCell(2, []byte("cell2"))

	// Cell 2 at end
	offset2 := 4096 - len(cell2)
	copy(pageData[offset2:], cell2)

	// Cell 1 with a gap
	offset1 := offset2 - len(cell1) - 10 // Leave a 10-byte gap
	copy(pageData[offset1:], cell1)

	binary.BigEndian.PutUint16(pageData[5:], uint16(offset1))  // CellContentStart
	binary.BigEndian.PutUint16(pageData[8:], uint16(offset1))  // Cell 1 pointer
	binary.BigEndian.PutUint16(pageData[10:], uint16(offset2)) // Cell 2 pointer

	btreePage, err := NewBtreePage(2, pageData, 4096)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Defragment
	err = btreePage.Defragment()
	if err != nil {
		t.Fatalf("Defragment() error = %v", err)
	}

	// Verify cells are now adjacent
	ptr1, _ := btreePage.Header.GetCellPointer(pageData, 0)
	ptr2, _ := btreePage.Header.GetCellPointer(pageData, 1)

	// ptr2 should be at end, ptr1 should be just before it
	expectedPtr2 := uint16(4096 - len(cell2))
	expectedPtr1 := uint16(int(expectedPtr2) - len(cell1))

	if ptr2 != expectedPtr2 {
		t.Errorf("Cell 2 pointer = %d, want %d", ptr2, expectedPtr2)
	}

	if ptr1 != expectedPtr1 {
		t.Errorf("Cell 1 pointer = %d, want %d", ptr1, expectedPtr1)
	}
}

func TestCursorInsert(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	for _, row := range []struct {
		key     int64
		payload string
	}{{1, "first"}, {2, "second"}, {3, "third"}} {
		if err := cursor.Insert(row.key, []byte(row.payload)); err != nil {
			t.Fatalf("Insert(%d) error = %v", row.key, err)
		}
	}

	cursorInsertVerifySeeks(t, cursor)
}

func cursorInsertVerifySeeks(t *testing.T, cursor *BtCursor) {
	t.Helper()
	for _, tc := range []struct {
		key     int64
		payload string
	}{{1, "first"}, {2, "second"}, {3, "third"}} {
		found, err := cursor.SeekRowid(tc.key)
		if err != nil {
			t.Fatalf("SeekRowid(%d) error = %v", tc.key, err)
		}
		if !found {
			t.Errorf("SeekRowid(%d) not found", tc.key)
		}
		if string(cursor.GetPayload()) != tc.payload {
			t.Errorf("Payload = %q, want %q", cursor.GetPayload(), tc.payload)
		}
	}
}

func TestCursorDelete(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	cursor.Insert(1, []byte("first"))
	cursor.Insert(2, []byte("second"))
	cursor.Insert(3, []byte("third"))

	seekAndDelete(cursor, 2)

	cursorDeleteVerify(t, cursor)
}

func cursorDeleteVerify(t *testing.T, cursor *BtCursor) {
	t.Helper()
	found, err := cursor.SeekRowid(2)
	if err != nil {
		t.Fatalf("SeekRowid(2) after delete error = %v", err)
	}
	if found {
		t.Error("SeekRowid(2) found after delete")
	}
	for _, key := range []int64{1, 3} {
		found, err = cursor.SeekRowid(key)
		if err != nil {
			t.Fatalf("SeekRowid(%d) error = %v", key, err)
		}
		if !found {
			t.Errorf("SeekRowid(%d) not found", key)
		}
	}
}

func TestCursorSeekRowid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	// Create a table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some rows (not in order)
	cursor.Insert(5, []byte("five"))
	cursor.Insert(2, []byte("two"))
	cursor.Insert(8, []byte("eight"))
	cursor.Insert(1, []byte("one"))
	cursor.Insert(9, []byte("nine"))

	tests := []struct {
		rowid       int64
		shouldFind  bool
		expectedKey int64
	}{
		{1, true, 1},
		{2, true, 2},
		{3, false, 5}, // Should position at next higher key
		{5, true, 5},
		{8, true, 8},
		{9, true, 9},
		{10, false, 9}, // Should position at last key
	}

	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("seek_%d", tt.rowid), func(t *testing.T) {
			found, err := cursor.SeekRowid(tt.rowid)
			if err != nil {
				t.Fatalf("SeekRowid(%d) error = %v", tt.rowid, err)
			}

			if found != tt.shouldFind {
				t.Errorf("SeekRowid(%d) found = %v, want %v", tt.rowid, found, tt.shouldFind)
			}

			if cursor.State == CursorValid {
				key := cursor.GetKey()
				if !tt.shouldFind && key != tt.expectedKey {
					t.Errorf("Cursor positioned at key %d, want %d", key, tt.expectedKey)
				}
			}
		})
	}
}

func TestCursorInsertDuplicateKey(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a row
	err = cursor.Insert(1, []byte("first"))
	if err != nil {
		t.Fatalf("Insert(1) error = %v", err)
	}

	// Try to insert duplicate key
	err = cursor.Insert(1, []byte("duplicate"))
	if err == nil {
		t.Error("Insert(1) duplicate expected error, got nil")
	}
}

func TestNewRowidWithData(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some rows
	cursor.Insert(1, []byte("first"))
	cursor.Insert(5, []byte("fifth"))
	cursor.Insert(3, []byte("third"))

	// Get new rowid - should be 6 (max + 1)
	rowid, err := bt.NewRowid(rootPage)
	if err != nil {
		t.Fatalf("NewRowid() error = %v", err)
	}

	if rowid != 6 {
		t.Errorf("NewRowid() = %d, want 6", rowid)
	}
}
