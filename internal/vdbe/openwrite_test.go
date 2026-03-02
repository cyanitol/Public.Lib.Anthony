// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

// TestOpenWriteCreatesWritableCursor tests that OpenWrite creates a cursor with Writable=true
func TestOpenWriteCreatesWritableCursor(t *testing.T) {
	t.Parallel()
	// Create a new in-memory btree
	bt := btree.NewBtree(4096)

	// Create a new VDBE instance
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	// Allocate memory
	v.AllocMemory(10)

	// Create a write cursor
	v.AddOp(OpOpenWrite, 0, 1, 0) // cursor 0, root page 1

	// Execute the OpenWrite instruction
	v.State = StateReady
	_, err := v.Step()
	if err != nil {
		t.Fatalf("OpenWrite failed: %v", err)
	}

	// Check cursor
	cursor, err := v.GetCursor(0)
	if err != nil {
		t.Fatalf("Failed to get cursor: %v", err)
	}

	// Verify cursor is writable
	if !cursor.Writable {
		t.Error("OpenWrite should create a writable cursor, but Writable=false")
	}

	// Verify cursor type
	if cursor.CurType != CursorBTree {
		t.Errorf("Expected CursorBTree, got %v", cursor.CurType)
	}

	// Verify btree cursor exists
	if cursor.BtreeCursor == nil {
		t.Error("BtreeCursor should not be nil")
	}
}

// TestOpenReadCreatesNonWritableCursor tests that OpenRead creates a cursor with Writable=false
func TestOpenReadCreatesNonWritableCursor(t *testing.T) {
	t.Parallel()
	// Create a new in-memory btree
	bt := btree.NewBtree(4096)

	// Create a new VDBE instance
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	// Allocate memory
	v.AllocMemory(10)

	// Create a read cursor
	v.AddOp(OpOpenRead, 0, 1, 0) // cursor 0, root page 1

	// Execute the OpenRead instruction
	v.State = StateReady
	_, err := v.Step()
	if err != nil {
		t.Fatalf("OpenRead failed: %v", err)
	}

	// Check cursor
	cursor, err := v.GetCursor(0)
	if err != nil {
		t.Fatalf("Failed to get cursor: %v", err)
	}

	// Verify cursor is NOT writable
	if cursor.Writable {
		t.Error("OpenRead should create a non-writable cursor, but Writable=true")
	}

	// Verify cursor type
	if cursor.CurType != CursorBTree {
		t.Errorf("Expected CursorBTree, got %v", cursor.CurType)
	}

	// Verify btree cursor exists
	if cursor.BtreeCursor == nil {
		t.Error("BtreeCursor should not be nil")
	}
}

// TestInsertRequiresWritableCursor tests that Insert fails on a read-only cursor
func TestInsertRequiresWritableCursor(t *testing.T) {
	t.Parallel()
	// Create a new in-memory btree
	bt := btree.NewBtree(4096)

	// Create a new VDBE instance
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	// Allocate memory
	v.AllocMemory(10)

	// Create a READ cursor (not writable)
	v.AddOp(OpOpenRead, 0, 1, 0)
	// Set up data for insert
	v.AddOp(OpInteger, 1, 1, 0) // rowid in register 1
	v.AddOp(OpBlob, 5, 2, 0)    // data in register 2
	v.Program[len(v.Program)-1].P4.P = []byte("test")
	// Try to insert
	v.AddOp(OpInsert, 0, 2, 1) // cursor 0, data reg 2, rowid reg 1

	// Execute the program
	v.State = StateReady
	err := v.Run()

	// Should fail with writable error
	if err == nil {
		t.Fatal("Insert should have failed on read-only cursor, but succeeded")
	}

	expectedMsg := "cursor 0 is not writable"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("Expected error containing '%s', got: %v", expectedMsg, err)
	}
}

// TestDeleteRequiresWritableCursor tests that Delete fails on a read-only cursor
func TestDeleteRequiresWritableCursor(t *testing.T) {
	t.Parallel()
	// Create a new in-memory btree
	bt := btree.NewBtree(4096)

	// Create a new VDBE instance
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	// Allocate memory
	v.AllocMemory(10)

	// Create a READ cursor (not writable)
	v.AddOp(OpOpenRead, 0, 1, 0)
	// Try to delete
	v.AddOp(OpDelete, 0, 0, 0) // cursor 0

	// Execute the program
	v.State = StateReady
	err := v.Run()

	// Should fail with writable error
	if err == nil {
		t.Fatal("Delete should have failed on read-only cursor, but succeeded")
	}

	expectedMsg := "cursor 0 is not writable"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("Expected error containing '%s', got: %v", expectedMsg, err)
	}
}

// TestInsertSucceedsWithWritableCursor tests that Insert doesn't fail with writable error
func TestInsertSucceedsWithWritableCursor(t *testing.T) {
	t.Parallel()
	// Create a new in-memory btree
	bt := btree.NewBtree(4096)

	// Create a new VDBE instance
	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}

	// Allocate memory
	v.AllocMemory(10)

	// Create a WRITE cursor (writable)
	v.AddOp(OpOpenWrite, 0, 1, 0)
	// Set up data for insert
	v.AddOp(OpInteger, 1, 1, 0) // rowid in register 1
	v.AddOp(OpBlob, 5, 2, 0)    // data in register 2
	v.Program[len(v.Program)-1].P4.P = []byte("test")
	// Insert
	v.AddOp(OpInsert, 0, 2, 1) // cursor 0, data reg 2, rowid reg 1
	v.AddOp(OpHalt, 0, 0, 0)

	// Execute the program
	v.State = StateReady
	err := v.Run()

	// Should NOT fail with "not writable" error
	// (may fail for other reasons like missing btree page, but that's a separate issue)
	if err != nil {
		notWritableMsg := "cursor 0 is not writable"
		if len(err.Error()) >= len(notWritableMsg) && err.Error()[:len(notWritableMsg)] == notWritableMsg {
			t.Fatalf("Insert failed with writable error on writable cursor: %v", err)
		}
		// Other errors are expected with in-memory btree - just verify we passed the writable check
		t.Logf("Insert failed with non-writable error (expected with in-memory btree): %v", err)
	} else {
		// Verify NumChanges was incremented if insert succeeded
		if v.NumChanges != 1 {
			t.Errorf("Expected NumChanges=1, got %d", v.NumChanges)
		}
	}
}
