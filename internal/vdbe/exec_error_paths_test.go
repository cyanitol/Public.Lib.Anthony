// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestExecCopyErrorPaths tests error paths in execCopy
func TestExecCopyErrorPaths(t *testing.T) {
	t.Parallel()
	t.Run("InvalidSrc", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(3)
		instr := &Instruction{
			Opcode: OpCopy,
			P1:     100, // Out of bounds
			P2:     0,
		}
		err := v.execCopy(instr)
		if err == nil {
			t.Error("Expected error for invalid source register")
		}
	})

	t.Run("InvalidDst", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(3)
		instr := &Instruction{
			Opcode: OpCopy,
			P1:     0,
			P2:     100, // Out of bounds
		}
		err := v.execCopy(instr)
		if err == nil {
			t.Error("Expected error for invalid destination register")
		}
	})
}

// TestExecIntegerErrorPath tests error path in execInteger
func TestExecIntegerErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpInteger,
		P1:     0,
		P2:     100, // Out of bounds
	}
	err := v.execInteger(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecNullErrorPath tests error path in execNull
func TestExecNullErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpNull,
		P2:     100, // Out of bounds
		P3:     1,
	}
	err := v.execNull(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecIfErrorPath tests error path in execIf
func TestExecIfErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpIf,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execIf(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecIfNotErrorPath tests error path in execIfNot
func TestExecIfNotErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpIfNot,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execIfNot(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecIfPosErrorPath tests error path in execIfPos
func TestExecIfPosErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpIfPos,
		P1:     100, // Out of bounds
		P2:     10,
		P3:     -1,
	}
	err := v.execIfPos(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecGosubErrorPath tests error path in execGosub
func TestExecGosubErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpGosub,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execGosub(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecReturnErrorPath tests error path in execReturn
func TestExecReturnErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpReturn,
		P1:     100, // Out of bounds
	}
	err := v.execReturn(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecMoveErrorPath tests error path in execMove
func TestExecMoveErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)

	t.Run("InvalidSrc", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpMove,
			P1:     100, // Out of bounds
			P2:     0,
			P3:     1,
		}
		err := v.execMove(instr)
		if err == nil {
			t.Error("Expected error for invalid source register")
		}
	})

	t.Run("InvalidDst", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpMove,
			P1:     0,
			P2:     100, // Out of bounds
			P3:     1,
		}
		err := v.execMove(instr)
		if err == nil {
			t.Error("Expected error for invalid destination register")
		}
	})
}

// TestExecSCopyErrorPath tests error path in execSCopy
func TestExecSCopyErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)

	t.Run("InvalidSrc", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpSCopy,
			P1:     100, // Out of bounds
			P2:     0,
		}
		err := v.execSCopy(instr)
		if err == nil {
			t.Error("Expected error for invalid source register")
		}
	})

	t.Run("InvalidDst", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpSCopy,
			P1:     0,
			P2:     100, // Out of bounds
		}
		err := v.execSCopy(instr)
		if err == nil {
			t.Error("Expected error for invalid destination register")
		}
	})
}

// TestExecAddImmErrorPath tests error path in execAddImm
func TestExecAddImmErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpAddImm,
		P1:     100, // Out of bounds
		P2:     5,
	}
	err := v.execAddImm(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecIsNullErrorPath tests error path in execIsNull
func TestExecIsNullErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpIsNull,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execIsNull(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecNotNullErrorPath tests error path in execNotNull
func TestExecNotNullErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	instr := &Instruction{
		Opcode: OpNotNull,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execNotNull(instr)
	if err == nil {
		t.Error("Expected error for invalid register")
	}
}

// TestExecCloseErrorPath tests error path in execClose
func TestExecCloseErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	instr := &Instruction{
		Opcode: OpClose,
		P1:     100, // Out of bounds
	}
	err := v.execClose(instr)
	if err == nil {
		t.Error("Expected error for invalid cursor")
	}
}

// TestExecRewindErrorPath tests error path in execRewind
func TestExecRewindErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	instr := &Instruction{
		Opcode: OpRewind,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execRewind(instr)
	if err == nil {
		t.Error("Expected error for invalid cursor")
	}
}

// TestExecNextErrorPath tests error path in execNext
func TestExecNextErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	instr := &Instruction{
		Opcode: OpNext,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execNext(instr)
	if err == nil {
		t.Error("Expected error for invalid cursor")
	}
}

// TestExecPrevErrorPath tests error path in execPrev
func TestExecPrevErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	instr := &Instruction{
		Opcode: OpPrev,
		P1:     100, // Out of bounds
		P2:     10,
	}
	err := v.execPrev(instr)
	if err == nil {
		t.Error("Expected error for invalid cursor")
	}
}

// TestExecSeekGEErrorPath tests error path in execSeekGE
func TestExecSeekGEErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	t.Run("InvalidCursor", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpSeekGE,
			P1:     100, // Out of bounds
			P2:     10,
			P3:     0,
		}
		err := v.execSeekGE(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

	t.Run("InvalidKey", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpSeekGE,
			P1:     0,
			P2:     10,
			P3:     100, // Out of bounds
		}
		err := v.execSeekGE(instr)
		if err == nil {
			t.Error("Expected error for invalid key register")
		}
	})
}

// TestExecColumnErrorPath tests error path in execColumn
func TestExecColumnErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	t.Run("InvalidCursor", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpColumn,
			P1:     100, // Out of bounds
			P2:     0,
			P3:     0,
		}
		err := v.execColumn(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

	t.Run("InvalidDst", func(t *testing.T) {
		t.Parallel()
		v.Cursors[0] = &Cursor{}
		instr := &Instruction{
			Opcode: OpColumn,
			P1:     0,
			P2:     0,
			P3:     100, // Out of bounds
		}
		err := v.execColumn(instr)
		if err == nil {
			t.Error("Expected error for invalid destination register")
		}
	})
}

// TestExecRowidErrorPath tests error path in execRowid
func TestExecRowidErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	t.Run("InvalidCursor", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpRowid,
			P1:     100, // Out of bounds
			P2:     0,
		}
		err := v.execRowid(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

	t.Run("InvalidDst", func(t *testing.T) {
		t.Parallel()
		v.Cursors[0] = &Cursor{}
		instr := &Instruction{
			Opcode: OpRowid,
			P1:     0,
			P2:     100, // Out of bounds
		}
		err := v.execRowid(instr)
		if err == nil {
			t.Error("Expected error for invalid destination register")
		}
	})
}

// TestExecNotExistsErrorPath tests error path in execNotExists
func TestExecNotExistsErrorPath(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(3)
	v.AllocCursors(2)

	t.Run("InvalidCursor", func(t *testing.T) {
		t.Parallel()
		instr := &Instruction{
			Opcode: OpNotExists,
			P1:     100, // Out of bounds
			P2:     10,
			P3:     0,
		}
		err := v.execNotExists(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

	t.Run("InvalidRowidReg", func(t *testing.T) {
		t.Parallel()
		v.Cursors[0] = &Cursor{}
		instr := &Instruction{
			Opcode: OpNotExists,
			P1:     0,
			P2:     10,
			P3:     100, // Out of bounds
		}
		err := v.execNotExists(instr)
		if err == nil {
			t.Error("Expected error for invalid rowid register")
		}
	})
}

// TestCompareOperationsErrorPaths tests error paths in comparison operations
func TestCompareOperationsErrorPaths(t *testing.T) {
	t.Parallel()
	opcodes := []struct {
		name   string
		opcode Opcode
	}{
		{"Eq", OpEq},
		{"Ne", OpNe},
		{"Lt", OpLt},
		{"Le", OpLe},
		{"Gt", OpGt},
		{"Ge", OpGe},
	}

	for _, op := range opcodes {
		t.Run(op.name+"_InvalidLeft", func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(3)
			instr := &Instruction{
				Opcode: op.opcode,
				P1:     100, // Out of bounds
				P2:     0,
				P3:     0,
			}
			err := v.execInstruction(instr)
			if err == nil {
				t.Errorf("Expected error for invalid left register in %s", op.name)
			}
		})

		t.Run(op.name+"_InvalidRight", func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(3)
			v.Mem[0].SetInt(10)
			instr := &Instruction{
				Opcode: op.opcode,
				P1:     0,
				P2:     100, // Out of bounds
				P3:     0,
			}
			err := v.execInstruction(instr)
			if err == nil {
				t.Errorf("Expected error for invalid right register in %s", op.name)
			}
		})

		t.Run(op.name+"_InvalidResult", func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(3)
			v.Mem[0].SetInt(10)
			v.Mem[1].SetInt(20)
			instr := &Instruction{
				Opcode: op.opcode,
				P1:     0,
				P2:     1,
				P3:     100, // Out of bounds
			}
			err := v.execInstruction(instr)
			if err == nil {
				t.Errorf("Expected error for invalid result register in %s", op.name)
			}
		})
	}
}

// TestExecAddImmNonInt tests execAddImm with non-integer value
func TestExecAddImmNonInt(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetStr("42")

	instr := &Instruction{
		Opcode: OpAddImm,
		P1:     0,
		P2:     10,
	}

	err := v.execAddImm(instr)
	if err != nil {
		t.Fatalf("execAddImm failed: %v", err)
	}

	// Should convert to int and add
	if v.Mem[0].IntValue() != 52 {
		t.Errorf("Expected 52, got %d", v.Mem[0].IntValue())
	}
}
