package vdbe

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

func TestBtreeCursorBasic(t *testing.T) {
	t.Parallel()
	// Create a simple test to verify the btree cursor works
	bt := createTestBtree()

	// Create cursor
	cursor := btree.NewCursor(bt, 1)

	// Move to first
	err := cursor.MoveToFirst()
	if err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}

	// Get key (rowid)
	key := cursor.GetKey()
	if key != 1 {
		t.Errorf("Expected first key=1, got %d", key)
	}

	// Get payload
	payload := cursor.GetPayload()
	if payload == nil {
		t.Fatal("Payload is nil")
	}

	t.Logf("Payload length: %d", len(payload))
	t.Logf("Payload bytes: %v", payload)

	// Parse column 0 (should be 42)
	mem := NewMem()
	err = parseRecordColumn(payload, 0, mem)
	if err != nil {
		t.Fatalf("Failed to parse column: %v", err)
	}

	if !mem.IsInt() {
		t.Errorf("Expected int column, got %v", mem)
	}

	if mem.IntValue() != 42 {
		t.Errorf("Expected 42, got %d", mem.IntValue())
	}

	// Parse column 1 (should be "Alice")
	mem2 := NewMem()
	err = parseRecordColumn(payload, 1, mem2)
	if err != nil {
		t.Fatalf("Failed to parse column 1: %v", err)
	}

	if !mem2.IsStr() {
		t.Errorf("Expected string column, got %v", mem2)
	}

	if mem2.StrValue() != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", mem2.StrValue())
	}
}
