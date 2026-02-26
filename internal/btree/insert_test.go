package btree

import (
	"testing"
)

func TestBtreeInsertAndRead(t *testing.T) {
	bt := NewBtree(4096)

	// Create a table (allocates page 1 and initializes it)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	t.Logf("Created table with root page %d", rootPage)

	// Verify page exists and is initialized
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	// Check page header
	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader failed: %v", err)
	}
	t.Logf("Initial header: type=0x%02x, numCells=%d", header.PageType, header.NumCells)

	if header.PageType != PageTypeLeafTable {
		t.Errorf("Expected leaf table page type (0x0d), got 0x%02x", header.PageType)
	}
	if header.NumCells != 0 {
		t.Errorf("Expected 0 cells initially, got %d", header.NumCells)
	}

	// Create a cursor and insert some data
	cursor := NewCursor(bt, rootPage)

	// Insert row with rowid=1, payload="hello"
	payload := []byte{2, 1, 13 + 2*5, 1, 'h', 'e', 'l', 'l', 'o'} // Simple record format
	err = cursor.Insert(1, payload)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Re-read the page header to verify NumCells was updated
	pageData, err = bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage after insert failed: %v", err)
	}
	header, err = ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader after insert failed: %v", err)
	}
	t.Logf("After insert: numCells=%d", header.NumCells)

	if header.NumCells != 1 {
		t.Errorf("Expected 1 cell after insert, got %d", header.NumCells)
	}

	// Now try to read the data back
	cursor2 := NewCursor(bt, rootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	if !cursor2.IsValid() {
		t.Fatal("Cursor not valid after MoveToFirst")
	}

	key := cursor2.GetKey()
	if key != 1 {
		t.Errorf("Expected key=1, got %d", key)
	}

	readPayload := cursor2.GetPayload()
	t.Logf("Read payload: %v", readPayload)
}
