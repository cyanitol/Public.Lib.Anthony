// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

func TestBtreeInsertAndRead(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	t.Logf("Created table with root page %d", rootPage)

	btreeInsertAndReadVerifyInit(t, bt, rootPage)

	cursor := NewCursor(bt, rootPage)
	payload := []byte{2, 1, 13 + 2*5, 1, 'h', 'e', 'l', 'l', 'o'}
	err = cursor.Insert(1, payload)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	btreeInsertAndReadVerifyAfter(t, bt, rootPage)
	btreeInsertAndReadVerifyRead(t, bt, rootPage)
}

func btreeInsertAndReadVerifyInit(t *testing.T, bt *Btree, rootPage uint32) {
	t.Helper()
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader failed: %v", err)
	}
	if header.PageType != PageTypeLeafTable {
		t.Errorf("Expected leaf table page type (0x0d), got 0x%02x", header.PageType)
	}
	if header.NumCells != 0 {
		t.Errorf("Expected 0 cells initially, got %d", header.NumCells)
	}
}

func btreeInsertAndReadVerifyAfter(t *testing.T, bt *Btree, rootPage uint32) {
	t.Helper()
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage after insert failed: %v", err)
	}
	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader after insert failed: %v", err)
	}
	if header.NumCells != 1 {
		t.Errorf("Expected 1 cell after insert, got %d", header.NumCells)
	}
}

func btreeInsertAndReadVerifyRead(t *testing.T, bt *Btree, rootPage uint32) {
	t.Helper()
	cursor2 := NewCursor(bt, rootPage)
	if err := cursor2.MoveToFirst(); err != nil {
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
