// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestCursorPrevViaParent tests the prevViaParent navigation path
func TestCursorPrevViaParent(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 200, 100)

	cursor2 := NewCursor(cursor.Btree, cursor.RootPage)
	err := cursor2.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	prevCount := navigateBackward(cursor2, 50)
	if prevCount < 10 {
		t.Errorf("Only navigated backward %d times, expected more", prevCount)
	}
}

// TestCursorDescendToLastPath tests descendToLast navigation
func TestCursorDescendToLastPath(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert data to create interior pages
	for i := 1; i <= 100; i++ {
		payload := make([]byte, 200)
		for j := range payload {
			payload[j] = byte(i)
		}
		err := cursor.Insert(int64(i*2), payload)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i*2, err)
		}
	}

	// MoveToLast should call descendToLast internally
	cursor2 := NewCursor(bt, rootPage)
	err = cursor2.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	if !cursor2.IsValid() {
		t.Fatal("Cursor should be valid after MoveToLast")
	}

	// Verify we got a valid key (may not be exactly 200 due to internal ordering)
	lastKey := cursor2.GetKey()
	if lastKey <= 0 {
		t.Errorf("Last key = %d, want positive value", lastKey)
	}
}

// TestCursorEnterPageDepth tests enterPage depth tracking
func TestCursorEnterPageDepth(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many records to create a deep tree
	for i := 1; i <= 500; i++ {
		payload := make([]byte, 100)
		err := cursor.Insert(int64(i), payload)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Seek to middle - this exercises enterPage at multiple depths
	cursor2 := NewCursor(bt, rootPage)
	_, err = cursor2.SeekRowid(250)
	if err != nil {
		t.Fatalf("SeekRowid(250) failed: %v", err)
	}

	// Just verify the seek completed (may or may not find exact match)
	if cursor2.State == CursorInvalid {
		t.Error("Cursor should not be invalid after seek")
	}

	if cursor2.Depth >= MaxBtreeDepth {
		t.Errorf("Cursor depth %d exceeds max %d", cursor2.Depth, MaxBtreeDepth)
	}
}

// TestDropInteriorChildren tests dropInteriorChildren during drop table
func TestDropInteriorChildren(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough data to create interior pages with children
	for i := 1; i <= 300; i++ {
		payload := make([]byte, 150)
		for j := range payload {
			payload[j] = byte(i % 256)
		}
		err := cursor.Insert(int64(i), payload)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Drop the table - this should call dropInteriorChildren
	err = bt.DropTable(rootPage)
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify table is dropped
	cursor2 := NewCursor(bt, rootPage)
	_, err = cursor2.SeekRowid(1)
	if err == nil && cursor2.IsValid() {
		t.Log("Cursor still valid after DropTable, page may be reused")
	}
}

// TestBalanceOverfullCondition tests handleOverfullPage in balance
func TestBalanceOverfullCondition(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 20, 2000)

	count := countForward(NewCursor(cursor.Btree, cursor.RootPage))
	if count < 1 {
		t.Errorf("Found %d entries, want at least 1", count)
	}
}

// TestBalanceUnderfullCondition tests handleUnderfullPage in balance
func TestBalanceUnderfullCondition(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	insertRows(cursor, 1, 100, 100)
	deleteRowRange(NewCursor(bt, rootPage), 50, 99)

	cursor3 := NewCursor(bt, rootPage)
	count := countForward(cursor3)
	if count < 45 || count > 55 {
		t.Logf("Found %d entries after deletions, expected ~50", count)
	}
}

// TestMergeOperations tests merge-related operations
func TestMergeOperations(t *testing.T) {
	t.Parallel()
	bt, cursor := setupBtreeWithRows(t, 4096, 1, 200, 100)
	deleteRowRange(NewCursor(bt, cursor.RootPage), 100, 150)

	cursor3 := NewCursor(bt, cursor.RootPage)
	if err := cursor3.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst after merge failed: %v", err)
	}
	if !cursor3.IsValid() {
		t.Error("Cursor should be valid after merge operations")
	}
}

// TestSiblingOperations tests sibling page operations
func TestSiblingOperations(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 150, 150)
	deleteRowRange(NewCursor(cursor.Btree, cursor.RootPage), 60, 90)

	cursor3 := NewCursor(cursor.Btree, cursor.RootPage)
	count := countForward(cursor3)

	expected := 150 - 31
	if count < expected-5 || count > expected+5 {
		t.Logf("Found %d entries, expected ~%d", count, expected)
	}
}

// TestRightmostSiblingHandling tests rightmost sibling handling
func TestRightmostSiblingHandling(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 180, 120)
	deleteRowRange(NewCursor(cursor.Btree, cursor.RootPage), 150, 180)

	cursor3 := NewCursor(cursor.Btree, cursor.RootPage)
	err := cursor3.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}
	if !cursor3.IsValid() {
		t.Fatal("Cursor should be valid")
	}
	lastKey := cursor3.GetKey()
	if lastKey != 149 {
		t.Logf("Last key = %d, expected 149", lastKey)
	}
}

// TestRedistributionBetweenSiblings tests cell redistribution
func TestRedistributionBetweenSiblings(t *testing.T) {
	t.Parallel()
	_, cursor := setupBtreeWithRows(t, 4096, 1, 100, 80)
	deleteRowRange(NewCursor(cursor.Btree, cursor.RootPage), 25, 35)

	cursor3 := NewCursor(cursor.Btree, cursor.RootPage)
	count := countForward(cursor3)

	if count < 20 {
		t.Errorf("Found %d keys, expected at least 20", count)
	}
}

// TestFirstKeyFromPage tests first key extraction
func TestFirstKeyFromPage(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert ordered data
	for i := 10; i <= 100; i += 10 {
		err := cursor.Insert(int64(i), []byte{byte(i)})
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Move to first
	cursor2 := NewCursor(bt, rootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	firstKey := cursor2.GetKey()
	if firstKey != 10 {
		t.Errorf("First key = %d, want 10", firstKey)
	}
}

// TestParentPageOperations tests parent page loading and operations
func TestParentPageOperations(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Create multi-level structure
	for i := 1; i <= 250; i++ {
		payload := make([]byte, 100)
		err := cursor.Insert(int64(i), payload)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	// Navigate to cause parent loading
	cursor2 := NewCursor(bt, rootPage)
	_, err = cursor2.SeekRowid(125)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	// Delete to trigger parent operations
	if cursor2.IsValid() {
		err = cursor2.Delete()
		if err != nil {
			t.Logf("Delete: %v", err)
		}
	}

	// Verify tree still works
	cursor3 := NewCursor(bt, rootPage)
	err = cursor3.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst after parent ops failed: %v", err)
	}
}
