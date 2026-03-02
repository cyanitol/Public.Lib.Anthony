// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"
)

// TestCoroutineOpcodes tests coroutine-related opcodes
func TestCoroutineOpcodes(t *testing.T) {
	t.Parallel()
	t.Run("InitCoroutine", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		instr := &Instruction{
			Opcode: OpInitCoroutine,
			P1:     1,  // coroutine ID
			P2:     10, // jump address (skip coroutine body)
			P3:     5,  // entry point
		}

		v.PC = 0
		err := v.execInitCoroutine(instr)
		if err != nil {
			t.Fatalf("execInitCoroutine failed: %v", err)
		}

		// Check coroutine was created
		coInfo, ok := v.Coroutines[1]
		if !ok {
			t.Fatal("Coroutine not created")
		}

		if coInfo.EntryPoint != 5 {
			t.Errorf("Expected entry point 5, got %d", coInfo.EntryPoint)
		}

		if coInfo.Active {
			t.Error("Coroutine should not be active after init")
		}

		// Check PC was jumped
		if v.PC != 10 {
			t.Errorf("Expected PC=10 after init, got %d", v.PC)
		}
	})

	t.Run("Yield", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		// Initialize coroutine first
		v.Coroutines[1] = &CoroutineInfo{
			EntryPoint: 5,
			YieldAddr:  0,
			Active:     false,
		}

		// Yield from current position (PC=3) to entry point (5)
		v.PC = 3
		instr := &Instruction{
			Opcode: OpYield,
			P1:     1, // coroutine ID
			P2:     0, // use current PC as return address
		}

		err := v.execYield(instr)
		if err != nil {
			t.Fatalf("execYield failed: %v", err)
		}

		// Check coroutine state
		coInfo := v.Coroutines[1]
		if !coInfo.Active {
			t.Error("Coroutine should be active after yield")
		}

		if coInfo.YieldAddr != 3 {
			t.Errorf("Expected yield addr 3, got %d", coInfo.YieldAddr)
		}

		if v.PC != 5 {
			t.Errorf("Expected PC=5 (entry point), got %d", v.PC)
		}
	})

	t.Run("YieldWithRegister", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		v.Coroutines[1] = &CoroutineInfo{
			EntryPoint: 5,
			YieldAddr:  0,
			Active:     false,
		}

		// Store return address in register
		v.Mem[2].SetInt(7)

		v.PC = 3
		instr := &Instruction{
			Opcode: OpYield,
			P1:     1, // coroutine ID
			P2:     2, // get return address from register 2
		}

		err := v.execYield(instr)
		if err != nil {
			t.Fatalf("execYield failed: %v", err)
		}

		if v.Coroutines[1].YieldAddr != 7 {
			t.Errorf("Expected yield addr 7 (from register), got %d", v.Coroutines[1].YieldAddr)
		}
	})

	t.Run("EndCoroutine", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		// Set up active coroutine
		v.Coroutines[1] = &CoroutineInfo{
			EntryPoint: 5,
			YieldAddr:  8,
			Active:     true,
		}

		v.PC = 6
		instr := &Instruction{
			Opcode: OpEndCoroutine,
			P1:     1, // coroutine ID
		}

		err := v.execEndCoroutine(instr)
		if err != nil {
			t.Fatalf("execEndCoroutine failed: %v", err)
		}

		// Check coroutine state
		coInfo := v.Coroutines[1]
		if coInfo.Active {
			t.Error("Coroutine should not be active after end")
		}

		// Check PC returned to yield address
		if v.PC != 8 {
			t.Errorf("Expected PC=8 (yield addr), got %d", v.PC)
		}
	})

	t.Run("EndCoroutine_NotActive", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		// Set up inactive coroutine
		v.Coroutines[1] = &CoroutineInfo{
			EntryPoint: 5,
			YieldAddr:  8,
			Active:     false,
		}

		instr := &Instruction{
			Opcode: OpEndCoroutine,
			P1:     1,
		}

		err := v.execEndCoroutine(instr)
		if err == nil {
			t.Error("Expected error for ending inactive coroutine")
		}
	})

	t.Run("EndCoroutine_NotInitialized", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		instr := &Instruction{
			Opcode: OpEndCoroutine,
			P1:     99, // Non-existent coroutine
		}

		err := v.execEndCoroutine(instr)
		if err == nil {
			t.Error("Expected error for non-existent coroutine")
		}
	})

	t.Run("Yield_NotInitialized", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Coroutines = make(map[int]*CoroutineInfo)

		instr := &Instruction{
			Opcode: OpYield,
			P1:     99, // Non-existent coroutine
			P2:     0,
		}

		err := v.execYield(instr)
		if err == nil {
			t.Error("Expected error for non-existent coroutine")
		}
	})
}

// TestProgramOpcode tests the OpProgram opcode for sub-programs
func TestProgramOpcode(t *testing.T) {
	t.Parallel()
	t.Run("BasicProgram", func(t *testing.T) {
		t.Parallel()
		// Create main VDBE
		v := NewTestVDBE(10)
		v.SubPrograms = make(map[int]*VDBE)

		// Create sub-program
		subProg := NewTestVDBE(5)
		subProg.Mem[0].SetInt(42)
		subProg.AddOp(OpHalt, 0, 0, 0)

		// Execute sub-program via OpProgram
		instr := &Instruction{
			Opcode: OpProgram,
			P1:     1, // sub-program ID
			P4:     P4Union{P: subProg},
			P4Type: P4SubProgram,
		}

		err := v.execProgram(instr)
		if err != nil {
			t.Fatalf("execProgram failed: %v", err)
		}

		// Check sub-program was stored
		storedSub, ok := v.SubPrograms[1]
		if !ok {
			t.Fatal("Sub-program not stored")
		}

		if storedSub.Parent != v {
			t.Error("Sub-program parent not set correctly")
		}

		if storedSub.Ctx != v.Ctx {
			t.Error("Sub-program context not shared")
		}
	})

	t.Run("Program_NilP4", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.SubPrograms = make(map[int]*VDBE)

		instr := &Instruction{
			Opcode: OpProgram,
			P1:     1,
			P4:     P4Union{P: nil},
			P4Type: P4SubProgram,
		}

		err := v.execProgram(instr)
		if err == nil {
			t.Error("Expected error for nil sub-program")
		}
	})

	t.Run("Program_WrongP4Type", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.SubPrograms = make(map[int]*VDBE)

		instr := &Instruction{
			Opcode: OpProgram,
			P1:     1,
			P4:     P4Union{Z: "not a vdbe"},
			P4Type: P4Static, // Wrong type
		}

		err := v.execProgram(instr)
		if err == nil {
			t.Error("Expected error for wrong P4 type")
		}
	})

	t.Run("Program_InvalidP4Content", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.SubPrograms = make(map[int]*VDBE)

		instr := &Instruction{
			Opcode: OpProgram,
			P1:     1,
			P4:     P4Union{P: "not a VDBE pointer"},
			P4Type: P4SubProgram,
		}

		err := v.execProgram(instr)
		if err == nil {
			t.Error("Expected error for invalid P4 content")
		}
	})
}

// TestParamOpcode tests the OpParam opcode for accessing parent registers
func TestParamOpcode(t *testing.T) {
	t.Parallel()
	t.Run("BasicParam", func(t *testing.T) {
		t.Parallel()
		// Create parent VDBE
		parent := NewTestVDBE(10)
		parent.Mem[3].SetInt(99)

		// Create child VDBE
		child := NewTestVDBE(5)
		child.Parent = parent

		// Copy parameter from parent register 3 to child register 1
		instr := &Instruction{
			Opcode: OpParam,
			P1:     3, // parent register
			P2:     1, // child register
		}

		err := child.execParam(instr)
		if err != nil {
			t.Fatalf("execParam failed: %v", err)
		}

		// Check value was copied
		if !child.Mem[1].IsInt() || child.Mem[1].IntValue() != 99 {
			t.Errorf("Expected child r1=99, got %v", child.Mem[1].IntValue())
		}
	})

	t.Run("Param_NoParent", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		// No parent set

		instr := &Instruction{
			Opcode: OpParam,
			P1:     3,
			P2:     1,
		}

		err := v.execParam(instr)
		if err == nil {
			t.Error("Expected error when no parent VDBE")
		}
	})

	t.Run("Param_InvalidParentRegister", func(t *testing.T) {
		t.Parallel()
		parent := NewTestVDBE(5)
		child := NewTestVDBE(5)
		child.Parent = parent

		instr := &Instruction{
			Opcode: OpParam,
			P1:     99, // Invalid register
			P2:     1,
		}

		err := child.execParam(instr)
		if err == nil {
			t.Error("Expected error for invalid parent register")
		}
	})

	t.Run("Param_StringValue", func(t *testing.T) {
		t.Parallel()
		parent := NewTestVDBE(10)
		parent.Mem[2].SetStr("hello")

		child := NewTestVDBE(5)
		child.Parent = parent

		instr := &Instruction{
			Opcode: OpParam,
			P1:     2,
			P2:     1,
		}

		err := child.execParam(instr)
		if err != nil {
			t.Fatalf("execParam failed: %v", err)
		}

		if child.Mem[1].StringValue() != "hello" {
			t.Errorf("Expected 'hello', got '%s'", child.Mem[1].StringValue())
		}
	})
}
