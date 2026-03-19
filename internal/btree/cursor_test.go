// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestCursorPrevious tests the Previous method
func TestCursorPrevious(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert some test data
	keys := []int64{10, 20, 30, 40, 50}
	for _, key := range keys {
		err := cursor.Insert(key, []byte{byte(key)})
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", key, err)
		}
	}

	// Move to last
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	// Navigate backwards
	expectedKeys := []int64{50, 40, 30, 20, 10}
	for i, expected := range expectedKeys {
		if !cursor2.IsValid() {
			t.Fatalf("Cursor invalid at iteration %d", i)
		}

		key := cursor2.GetKey()
		if key != expected {
			t.Errorf("Iteration %d: got key %d, want %d", i, key, expected)
		}

		if i < len(expectedKeys)-1 {
			err = cursor2.Previous()
			if err != nil {
				t.Logf("Previous() at iteration %d: %v", i, err)
			}
		}
	}

	// One more Previous should fail
	err = cursor2.Previous()
	if err == nil {
		t.Error("Expected error when calling Previous() before first entry, got nil")
	}
}

// TestCursorGetPayloadWithOverflow tests GetPayloadWithOverflow
func TestCursorGetPayloadWithOverflow(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a large payload that will trigger overflow
	largePayload := make([]byte, 5000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	err = cursor.Insert(100, largePayload)
	if err != nil {
		t.Fatalf("Insert large payload failed: %v", err)
	}

	// Seek to the key
	_, err = cursor.SeekRowid(100)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	// Get payload with overflow
	payload, err := cursor.GetPayloadWithOverflow()
	if err != nil {
		t.Fatalf("GetPayloadWithOverflow failed: %v", err)
	}

	if len(payload) != len(largePayload) {
		t.Errorf("Payload length = %d, want %d", len(payload), len(largePayload))
	}

	// Verify content
	for i := 0; i < len(payload) && i < len(largePayload); i++ {
		if payload[i] != largePayload[i] {
			t.Errorf("Payload byte %d = %d, want %d", i, payload[i], largePayload[i])
			break
		}
	}
}

// TestCursorString tests the String method
func TestCursorString(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	str := cursor.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
}

// TestCursorMoveToFirstEmpty tests MoveToFirst on empty table
func TestCursorMoveToFirstEmpty(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)
	err = cursor.MoveToFirst()
	if err == nil {
		t.Error("MoveToFirst on empty table should fail, got nil")
	}
}

// TestCursorDescendToRightChild tests descendToRightChild
func TestCursorDescendToRightChild(t *testing.T) {
	t.Parallel()
	bt := NewBtree(512) // Small pages to force tree growth

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert enough data to create interior pages
	for i := 1; i <= 200; i++ {
		key := int64(i)
		payload := make([]byte, 30)
		for j := range payload {
			payload[j] = byte(i % 256)
		}

		err := cursor.Insert(key, payload)
		if err != nil {
			t.Logf("Insert %d: %v", i, err)
		}
	}

	// Try to navigate to last (which exercises descendToRightChild)
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	// Verify we're at the last key
	if cursor2.IsValid() {
		key := cursor2.GetKey()
		if key < 100 {
			t.Errorf("Expected high key value, got %d", key)
		}
	}
}

// TestCursorInvalidOperations tests error conditions
func TestCursorInvalidOperations(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)
	cursor := NewCursor(bt, 999) // Non-existent root

	// MoveToFirst should fail
	err := cursor.MoveToFirst()
	if err == nil {
		t.Error("MoveToFirst on non-existent page should fail, got nil")
	}

	// MoveToLast should fail
	cursor2 := NewCursor(bt, 999)
	err = cursor2.MoveToLast()
	if err == nil {
		t.Error("MoveToLast on non-existent page should fail, got nil")
	}

	// SeekRowid should fail
	cursor3 := NewCursor(bt, 999)
	_, err = cursor3.SeekRowid(100)
	if err == nil {
		t.Error("SeekRowid on non-existent page should fail, got nil")
	}
}

// TestCursorNavigationEdgeCases tests edge cases in navigation
func TestCursorNavigationEdgeCases(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert single key
	err = cursor.Insert(42, []byte("answer"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test navigation with single entry
	cursor2 := NewCursor(bt, cursor.RootPage)
	err = cursor2.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	// Next should fail
	err = cursor2.Next()
	if err == nil {
		t.Error("Next() on single entry should fail at end, got nil")
	}

	// Start over
	cursor3 := NewCursor(bt, cursor.RootPage)
	err = cursor3.MoveToLast()
	if err != nil {
		t.Fatalf("MoveToLast failed: %v", err)
	}

	// Previous should fail
	err = cursor3.Previous()
	if err == nil {
		t.Error("Previous() on single entry should fail at beginning, got nil")
	}
}

// TestCursorDeleteInvalid tests deletion error conditions
func TestCursorDeleteInvalid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Try to delete without valid position
	err = cursor.Delete()
	if err == nil {
		t.Error("Delete() without valid position should fail, got nil")
	}
}

// TestCursorGetPayload tests GetPayload method
func TestCursorGetPayload(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a key
	testPayload := []byte("test payload")
	err = cursor.Insert(100, testPayload)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Seek and get payload
	_, err = cursor.SeekRowid(100)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	payload := cursor.GetPayload()
	if string(payload) != string(testPayload) {
		t.Errorf("GetPayload() = %q, want %q", payload, testPayload)
	}
}

// TestCursorGetKey tests GetKey method
func TestCursorGetKey(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert a key
	err = cursor.Insert(12345, []byte("data"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Seek and get key
	_, err = cursor.SeekRowid(12345)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	key := cursor.GetKey()
	if key != 12345 {
		t.Errorf("GetKey() = %d, want 12345", key)
	}
}

// TestCursorIsValid tests IsValid method
func TestCursorIsValid(t *testing.T) {
	t.Parallel()
	bt := NewBtree(4096)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Cursor starts invalid
	if cursor.IsValid() {
		t.Error("New cursor should be invalid")
	}

	// Insert and seek
	err = cursor.Insert(100, []byte("data"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	_, err = cursor.SeekRowid(100)
	if err != nil {
		t.Fatalf("SeekRowid failed: %v", err)
	}

	// Now should be valid
	if !cursor.IsValid() {
		t.Error("Cursor should be valid after successful seek")
	}
}
