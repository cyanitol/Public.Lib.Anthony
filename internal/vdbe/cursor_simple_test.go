// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

func verifyParsedColumn(t *testing.T, payload []byte, colIdx int, expectInt bool, expectIntVal int64, expectStr string) {
	t.Helper()
	mem := NewMem()
	if err := parseRecordColumn(payload, colIdx, mem); err != nil {
		t.Fatalf("Failed to parse column %d: %v", colIdx, err)
	}
	if expectInt {
		if !mem.IsInt() {
			t.Errorf("Column %d: expected int, got %v", colIdx, mem)
		}
		if mem.IntValue() != expectIntVal {
			t.Errorf("Column %d: expected %d, got %d", colIdx, expectIntVal, mem.IntValue())
		}
	} else {
		if !mem.IsStr() {
			t.Errorf("Column %d: expected string, got %v", colIdx, mem)
		}
		if mem.StrValue() != expectStr {
			t.Errorf("Column %d: expected %q, got %q", colIdx, expectStr, mem.StrValue())
		}
	}
}

func TestBtreeCursorBasic(t *testing.T) {
	t.Parallel()
	bt := createTestBtree()
	cursor := btree.NewCursor(bt, 1)
	if err := cursor.MoveToFirst(); err != nil {
		t.Fatalf("MoveToFirst failed: %v", err)
	}
	if key := cursor.GetKey(); key != 1 {
		t.Errorf("Expected first key=1, got %d", key)
	}
	payload := cursor.GetPayload()
	if payload == nil {
		t.Fatal("Payload is nil")
	}
	t.Logf("Payload length: %d, bytes: %v", len(payload), payload)
	verifyParsedColumn(t, payload, 0, true, 42, "")
	verifyParsedColumn(t, payload, 1, false, 0, "Alice")
}
