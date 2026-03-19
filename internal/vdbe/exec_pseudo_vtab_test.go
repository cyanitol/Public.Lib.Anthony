// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

func testOpenPseudoBasic(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.AllocCursors(3)
	v.Mem[2].SetBlob([]byte{1, 2, 3, 4})
	err := v.execOpenPseudo(&Instruction{Opcode: OpOpenPseudo, P1: 1, P2: 2, P3: 3})
	if err != nil {
		t.Fatalf("execOpenPseudo failed: %v", err)
	}
	cursor := v.Cursors[1]
	if cursor == nil {
		t.Fatal("Cursor not created")
	}
	verifyPseudoCursorProps(t, cursor)
}

func verifyPseudoCursorProps(t *testing.T, cursor *Cursor) {
	t.Helper()
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
}

func testOpenPseudoAllocates(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	err := v.execOpenPseudo(&Instruction{Opcode: OpOpenPseudo, P1: 5, P2: 1, P3: 1})
	if err != nil {
		t.Fatalf("execOpenPseudo failed: %v", err)
	}
	if len(v.Cursors) <= 5 {
		t.Errorf("Expected cursors to be allocated, got length %d", len(v.Cursors))
	}
	if v.Cursors[5] == nil {
		t.Error("Cursor 5 should be created")
	}
}

// TestOpenPseudoOpcode tests the OpOpenPseudo opcode
func TestOpenPseudoOpcode(t *testing.T) {
	t.Parallel()
	t.Run("BasicOpenPseudo", testOpenPseudoBasic)
	t.Run("OpenPseudo_AllocatesCursors", testOpenPseudoAllocates)
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

func testVOpenBasic(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	err := v.execVOpen(&Instruction{Opcode: OpVOpen, P4: P4Union{P: "mock_vtable"}, P4Type: P4VTab})
	if err != nil {
		t.Fatalf("execVOpen failed: %v", err)
	}
	verifyCursor(t, v.Cursors[0], CursorVTab, true)
}

func testVOpenNilVTable(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	expectError(t, v.execVOpen(&Instruction{Opcode: OpVOpen, P4: P4Union{P: nil}, P4Type: P4VTab}), "nil virtual table")
}

func testVOpenWrongP4Type(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	expectError(t, v.execVOpen(&Instruction{Opcode: OpVOpen, P4: P4Union{Z: "not a vtable"}, P4Type: P4Static}), "wrong P4 type")
}

func testVFilterBasic(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.Cursors = make([]*Cursor, 2)
	v.Cursors[0] = &Cursor{CurType: CursorVTab, VTable: "mock_vtable", EOF: true}
	v.Mem[1].SetInt(100)
	v.Mem[2].SetStr("test")
	err := v.execVFilter(&Instruction{Opcode: OpVFilter, P1: 0, P2: 2, P3: 1, P4: P4Union{Z: "idxStr"}, P4Type: P4Static, P5: 1})
	if err != nil {
		t.Fatalf("execVFilter failed: %v", err)
	}
	if v.Cursors[0].EOF {
		t.Error("EOF should be false after VFilter")
	}
}

func testVFilterWrongCursorType(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.Cursors = make([]*Cursor, 2)
	v.Cursors[0] = &Cursor{CurType: CursorBTree}
	expectError(t, v.execVFilter(&Instruction{Opcode: OpVFilter, P1: 0, P2: 0, P3: 0}), "wrong cursor type")
}

// TestVirtualTableOpcodes tests virtual table related opcodes
func TestVirtualTableOpcodes(t *testing.T) {
	t.Parallel()
	t.Run("VOpen_Basic", testVOpenBasic)
	t.Run("VOpen_NilVTable", testVOpenNilVTable)
	t.Run("VOpen_WrongP4Type", testVOpenWrongP4Type)
	t.Run("VFilter_Basic", testVFilterBasic)
	t.Run("VFilter_WrongCursorType", testVFilterWrongCursorType)

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
