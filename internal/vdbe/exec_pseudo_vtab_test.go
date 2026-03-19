// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestOpenPseudoOpcode tests the OpOpenPseudo opcode
func TestOpenPseudoOpcode(t *testing.T) {
	t.Parallel()
	t.Run("BasicOpenPseudo", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.AllocCursors(3)

		// Set up pseudo-table data in register 2
		v.Mem[2].SetBlob([]byte{1, 2, 3, 4})

		instr := &Instruction{
			Opcode: OpOpenPseudo,
			P1:     1, // cursor number
			P2:     2, // register containing data
			P3:     3, // number of columns
		}

		err := v.execOpenPseudo(instr)
		if err != nil {
			t.Fatalf("execOpenPseudo failed: %v", err)
		}

		// Check cursor was created
		cursor := v.Cursors[1]
		if cursor == nil {
			t.Fatal("Cursor not created")
		}

		if cursor.CurType != CursorPseudo {
			t.Errorf("Expected CursorPseudo, got %v", cursor.CurType)
		}

		if !cursor.IsTable {
			t.Error("Pseudo cursor should be marked as table")
		}

		if cursor.PseudoReg != 2 {
			t.Errorf("Expected PseudoReg=2, got %d", cursor.PseudoReg)
		}

		if cursor.NullRow {
			t.Error("Cursor should not be null row initially")
		}

		if cursor.EOF {
			t.Error("Cursor should not be EOF initially")
		}
	})

	t.Run("OpenPseudo_AllocatesCursors", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		// Don't pre-allocate cursors

		instr := &Instruction{
			Opcode: OpOpenPseudo,
			P1:     5, // cursor number beyond current size
			P2:     1,
			P3:     1,
		}

		err := v.execOpenPseudo(instr)
		if err != nil {
			t.Fatalf("execOpenPseudo failed: %v", err)
		}

		if len(v.Cursors) <= 5 {
			t.Errorf("Expected cursors to be allocated, got length %d", len(v.Cursors))
		}

		if v.Cursors[5] == nil {
			t.Error("Cursor 5 should be created")
		}
	})
}

// Helper to create VTab cursor for testing
func setupVTabCursor(t *testing.T, initialized bool) (*VDBE, *Cursor) {
	t.Helper()
	v := NewTestVDBE(10)
	v.Cursors = make([]*Cursor, 2)
	cursor := &Cursor{
		CurType: CursorVTab,
		VTable:  "mock_vtable",
	}
	if initialized {
		cursor.VTabCursor = "mock_vtab_cursor"
	}
	v.Cursors[0] = cursor
	return v, cursor
}

// Helper to verify cursor properties
func verifyCursor(t *testing.T, cursor *Cursor, expectedType CursorType, shouldHaveVTable bool) {
	t.Helper()
	if cursor == nil {
		t.Fatal("Cursor not created")
	}
	if cursor.CurType != expectedType {
		t.Errorf("Expected %v, got %v", expectedType, cursor.CurType)
	}
	if shouldHaveVTable && cursor.VTable == nil {
		t.Error("VTable should be set")
	}
}

// Helper to test error case
func expectError(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error for %s", context)
	}
}

// TestVirtualTableOpcodes tests virtual table related opcodes
func TestVirtualTableOpcodes(t *testing.T) {
	t.Parallel()
	t.Run("VOpen_Basic", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		mockVTable := "mock_vtable"

		instr := &Instruction{
			Opcode: OpVOpen,
			P1:     0,
			P2:     0,
			P3:     0,
			P4:     P4Union{P: mockVTable},
			P4Type: P4VTab,
		}

		err := v.execVOpen(instr)
		if err != nil {
			t.Fatalf("execVOpen failed: %v", err)
		}

		verifyCursor(t, v.Cursors[0], CursorVTab, true)
	})

	t.Run("VOpen_NilVTable", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		instr := &Instruction{
			Opcode: OpVOpen,
			P1:     0,
			P4:     P4Union{P: nil},
			P4Type: P4VTab,
		}
		expectError(t, v.execVOpen(instr), "nil virtual table")
	})

	t.Run("VOpen_WrongP4Type", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		instr := &Instruction{
			Opcode: OpVOpen,
			P1:     0,
			P4:     P4Union{Z: "not a vtable"},
			P4Type: P4Static,
		}
		expectError(t, v.execVOpen(instr), "wrong P4 type")
	})

	t.Run("VFilter_Basic", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		// Create a virtual table cursor
		v.Cursors = make([]*Cursor, 2)
		v.Cursors[0] = &Cursor{
			CurType:    CursorVTab,
			VTable:     "mock_vtable",
			VTabCursor: nil,
			EOF:        true,
		}

		// Set up constraint values
		v.Mem[1].SetInt(100)
		v.Mem[2].SetStr("test")

		instr := &Instruction{
			Opcode: OpVFilter,
			P1:     0, // cursor
			P2:     2, // argc
			P3:     1, // idxNum
			P4:     P4Union{Z: "idxStr"},
			P4Type: P4Static,
			P5:     1, // start register for args
		}

		err := v.execVFilter(instr)
		if err != nil {
			t.Fatalf("execVFilter failed: %v", err)
		}

		// Check EOF was reset
		if v.Cursors[0].EOF {
			t.Error("EOF should be false after VFilter")
		}
	})

	t.Run("VFilter_WrongCursorType", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Cursors = make([]*Cursor, 2)
		v.Cursors[0] = &Cursor{CurType: CursorBTree}
		expectError(t, v.execVFilter(&Instruction{Opcode: OpVFilter, P1: 0, P2: 0, P3: 0}), "wrong cursor type")
	})

	t.Run("VColumn_Basic", func(t *testing.T) {
		t.Parallel()
		v, _ := setupVTabCursor(t, true)
		instr := &Instruction{Opcode: OpVColumn, P1: 0, P2: 1, P3: 5}
		err := v.execVColumn(instr)
		if err != nil {
			t.Fatalf("execVColumn failed: %v", err)
		}
		if !v.Mem[5].IsNull() {
			t.Logf("Note: VColumn currently returns NULL (stub implementation)")
		}
	})

	t.Run("VColumn_NoCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		expectError(t, v.execVColumn(&Instruction{Opcode: OpVColumn, P1: 0, P2: 1, P3: 5}), "missing cursor")
	})

	t.Run("VColumn_UninitializedCursor", func(t *testing.T) {
		t.Parallel()
		v, _ := setupVTabCursor(t, false)
		expectError(t, v.execVColumn(&Instruction{Opcode: OpVColumn, P1: 0, P2: 1, P3: 5}), "uninitialized cursor")
	})

	t.Run("VNext_Basic", func(t *testing.T) {
		t.Parallel()
		v, cursor := setupVTabCursor(t, true)
		cursor.EOF = false
		v.PC = 0
		err := v.execVNext(&Instruction{Opcode: OpVNext, P1: 0, P2: 10})
		if err != nil {
			t.Fatalf("execVNext failed: %v", err)
		}
		if v.PC == 10 {
			t.Error("Should not jump when EOF is true")
		}
	})

	t.Run("VNext_WrongCursorType", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Cursors = make([]*Cursor, 2)
		v.Cursors[0] = &Cursor{CurType: CursorBTree}
		expectError(t, v.execVNext(&Instruction{Opcode: OpVNext, P1: 0, P2: 10}), "wrong cursor type")
	})

	t.Run("VNext_UninitializedCursor", func(t *testing.T) {
		t.Parallel()
		v, _ := setupVTabCursor(t, false)
		expectError(t, v.execVNext(&Instruction{Opcode: OpVNext, P1: 0, P2: 10}), "uninitialized cursor")
	})

	t.Run("VRowid_Direct", func(t *testing.T) {
		t.Parallel()
		v, _ := setupVTabCursor(t, true)
		err := v.execVRowid(&Instruction{P1: 0, P2: 3})
		if err != nil {
			t.Fatalf("execVRowid failed: %v", err)
		}
		if !v.Mem[3].IsInt() {
			t.Error("VRowid should return an integer")
		}
	})

	t.Run("VRowid_WrongCursorType", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Cursors = make([]*Cursor, 2)
		v.Cursors[0] = &Cursor{CurType: CursorBTree}
		expectError(t, v.execVRowid(&Instruction{P1: 0, P2: 3}), "wrong cursor type")
	})

	t.Run("VRowid_UninitializedCursor", func(t *testing.T) {
		t.Parallel()
		v, _ := setupVTabCursor(t, false)
		expectError(t, v.execVRowid(&Instruction{P1: 0, P2: 3}), "uninitialized cursor")
	})
}
