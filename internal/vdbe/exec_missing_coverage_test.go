package vdbe

import (
	"testing"
)

// TestMissingOpcodesCoverage adds coverage for opcodes that have 0% coverage
func TestMissingOpcodesCoverage(t *testing.T) {
	t.Run("OpNoop", func(t *testing.T) {
		v := NewTestVDBE(5)
		instr := &Instruction{Opcode: OpNoop}
		err := v.execNoop(instr)
		if err != nil {
			t.Errorf("execNoop failed: %v", err)
		}
	})

	t.Run("OpGosub_Return", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.PC = 1

		// Test Gosub - stores current PC (1) and jumps to P2 (3)
		gosubInstr := &Instruction{Opcode: OpGosub, P1: 0, P2: 3}
		err := v.execGosub(gosubInstr)
		if err != nil {
			t.Errorf("execGosub failed: %v", err)
		}

		// PC should be set to 3
		if v.PC != 3 {
			t.Errorf("Expected PC=3 after Gosub, got %d", v.PC)
		}

		// Return address should be in r0 (PC was 1)
		if v.Mem[0].IntValue() != 1 {
			t.Errorf("Expected return addr=1, got %d", v.Mem[0].IntValue())
		}

		// Test Return
		returnInstr := &Instruction{Opcode: OpReturn, P1: 0}
		err = v.execReturn(returnInstr)
		if err != nil {
			t.Errorf("execReturn failed: %v", err)
		}

		// PC should be restored to 1
		if v.PC != 1 {
			t.Errorf("Expected PC=1 after Return, got %d", v.PC)
		}
	})

	t.Run("OpIfNot", func(t *testing.T) {
		v := NewTestVDBE(5)

		// Test with false value (should jump)
		v.Mem[0].SetInt(0)
		instr := &Instruction{Opcode: OpIfNot, P1: 0, P2: 10}
		err := v.execIfNot(instr)
		if err != nil {
			t.Errorf("execIfNot failed: %v", err)
		}
		if v.PC != 10 {
			t.Errorf("Expected PC=10 after IfNot with false, got %d", v.PC)
		}

		// Test with true value (should not jump)
		v.PC = 0
		v.Mem[0].SetInt(1)
		err = v.execIfNot(instr)
		if err != nil {
			t.Errorf("execIfNot failed: %v", err)
		}
		if v.PC != 0 {
			t.Errorf("Expected PC=0 after IfNot with true, got %d", v.PC)
		}
	})

	t.Run("OpIfPos", func(t *testing.T) {
		v := NewTestVDBE(5)

		// Test with positive value (should add P3 and jump)
		// P3 is typically -1, so val + (-1) decrements
		v.Mem[0].SetInt(3)
		instr := &Instruction{Opcode: OpIfPos, P1: 0, P2: 10, P3: -1}
		err := v.execIfPos(instr)
		if err != nil {
			t.Errorf("execIfPos failed: %v", err)
		}
		if v.PC != 10 {
			t.Errorf("Expected PC=10 after IfPos with positive, got %d", v.PC)
		}
		if v.Mem[0].IntValue() != 2 {
			t.Errorf("Expected r0=2 (decremented), got %d", v.Mem[0].IntValue())
		}

		// Test with non-positive value (should not jump)
		v.PC = 0
		v.Mem[0].SetInt(0)
		err = v.execIfPos(instr)
		if err != nil {
			t.Errorf("execIfPos failed: %v", err)
		}
		if v.PC != 0 {
			t.Errorf("Expected PC=0 after IfPos with zero, got %d", v.PC)
		}
	})

	t.Run("OpInt64", func(t *testing.T) {
		v := NewTestVDBE(5)
		instr := &Instruction{
			Opcode: OpInt64,
			P2:     1,
			P4:     P4Union{I64: 9223372036854775807},
			P4Type: P4Int64,
		}
		err := v.execInt64(instr)
		if err != nil {
			t.Errorf("execInt64 failed: %v", err)
		}
		if v.Mem[1].IntValue() != 9223372036854775807 {
			t.Errorf("Expected max int64, got %d", v.Mem[1].IntValue())
		}
	})

	t.Run("OpNull", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(42)
		v.Mem[1].SetInt(43)
		v.Mem[2].SetInt(44)

		// Set r0-r2 to NULL  (P2 through P2+P3)
		// P2=0, P3=2 means r0 through r2
		instr := &Instruction{Opcode: OpNull, P2: 0, P3: 2}
		err := v.execNull(instr)
		if err != nil {
			t.Errorf("execNull failed: %v", err)
		}

		for i := 0; i < 3; i++ {
			if !v.Mem[i].IsNull() {
				t.Errorf("Expected r%d to be NULL", i)
			}
		}
	})

	t.Run("OpCopy", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(42)

		instr := &Instruction{Opcode: OpCopy, P1: 0, P2: 1}
		err := v.execCopy(instr)
		if err != nil {
			t.Errorf("execCopy failed: %v", err)
		}

		if v.Mem[1].IntValue() != 42 {
			t.Errorf("Expected r1=42, got %d", v.Mem[1].IntValue())
		}
	})

	t.Run("OpMove", func(t *testing.T) {
		v := NewTestVDBE(10)
		v.Mem[0].SetInt(42)
		v.Mem[1].SetInt(43)

		// Move r0,r1 to r5,r6
		instr := &Instruction{Opcode: OpMove, P1: 0, P2: 5, P3: 2}
		err := v.execMove(instr)
		if err != nil {
			t.Errorf("execMove failed: %v", err)
		}

		if v.Mem[5].IntValue() != 42 {
			t.Errorf("Expected r5=42, got %d", v.Mem[5].IntValue())
		}
		if v.Mem[6].IntValue() != 43 {
			t.Errorf("Expected r6=43, got %d", v.Mem[6].IntValue())
		}
	})

	t.Run("OpSCopy", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(42)

		instr := &Instruction{Opcode: OpSCopy, P1: 0, P2: 1}
		err := v.execSCopy(instr)
		if err != nil {
			t.Errorf("execSCopy failed: %v", err)
		}

		if v.Mem[1].IntValue() != 42 {
			t.Errorf("Expected r1=42, got %d", v.Mem[1].IntValue())
		}
	})

	t.Run("OpSubtract", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(50)
		v.Mem[1].SetInt(8)

		// P3 = P1 - P2, so r2 = r0 - r1 = 50 - 8 = 42
		instr := &Instruction{Opcode: OpSubtract, P1: 0, P2: 1, P3: 2}
		err := v.execSubtract(instr)
		if err != nil {
			t.Errorf("execSubtract failed: %v", err)
		}

		if v.Mem[2].IntValue() != 42 {
			t.Errorf("Expected r2=42, got %d", v.Mem[2].IntValue())
		}
	})

	t.Run("OpMultiply", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(6)
		v.Mem[1].SetInt(7)

		instr := &Instruction{Opcode: OpMultiply, P1: 1, P2: 0, P3: 2}
		err := v.execMultiply(instr)
		if err != nil {
			t.Errorf("execMultiply failed: %v", err)
		}

		if v.Mem[2].IntValue() != 42 {
			t.Errorf("Expected r2=42, got %d", v.Mem[2].IntValue())
		}
	})

	t.Run("OpDivide", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(84)
		v.Mem[1].SetInt(2)

		// P3 = P1 / P2, so r2 = r0 / r1 = 84 / 2 = 42
		instr := &Instruction{Opcode: OpDivide, P1: 0, P2: 1, P3: 2}
		err := v.execDivide(instr)
		if err != nil {
			t.Errorf("execDivide failed: %v", err)
		}

		if v.Mem[2].IntValue() != 42 {
			t.Errorf("Expected r2=42, got %d", v.Mem[2].IntValue())
		}

		// Test division by zero
		v.Mem[1].SetInt(0)
		err = v.execDivide(instr)
		if err != nil {
			t.Errorf("execDivide failed: %v", err)
		}
		if !v.Mem[2].IsNull() {
			t.Errorf("Expected NULL for division by zero")
		}
	})

	t.Run("OpRemainder", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(47)
		v.Mem[1].SetInt(5)

		// P3 = P1 % P2, so r2 = r0 % r1 = 47 % 5 = 2
		instr := &Instruction{Opcode: OpRemainder, P1: 0, P2: 1, P3: 2}
		err := v.execRemainder(instr)
		if err != nil {
			t.Errorf("execRemainder failed: %v", err)
		}

		if v.Mem[2].IntValue() != 2 {
			t.Errorf("Expected r2=2, got %d", v.Mem[2].IntValue())
		}

		// Test remainder by zero
		v.Mem[1].SetInt(0)
		err = v.execRemainder(instr)
		if err != nil {
			t.Errorf("execRemainder failed: %v", err)
		}
		if !v.Mem[2].IsNull() {
			t.Errorf("Expected NULL for remainder by zero")
		}
	})

	t.Run("OpAddImm", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(40)

		// Add P2 to register P1
		instr := &Instruction{Opcode: OpAddImm, P1: 0, P2: 2}
		err := v.execAddImm(instr)
		if err != nil {
			t.Errorf("execAddImm failed: %v", err)
		}

		if v.Mem[0].IntValue() != 42 {
			t.Errorf("Expected r0=42, got %d", v.Mem[0].IntValue())
		}
	})

	t.Run("OpIsNull", func(t *testing.T) {
		v := NewTestVDBE(5)

		// Test with NULL value (should jump)
		v.Mem[0].SetNull()
		instr := &Instruction{Opcode: OpIsNull, P1: 0, P2: 10}
		err := v.execIsNull(instr)
		if err != nil {
			t.Errorf("execIsNull failed: %v", err)
		}
		if v.PC != 10 {
			t.Errorf("Expected PC=10 after IsNull with NULL, got %d", v.PC)
		}

		// Test with non-NULL value (should not jump)
		v.PC = 0
		v.Mem[0].SetInt(42)
		err = v.execIsNull(instr)
		if err != nil {
			t.Errorf("execIsNull failed: %v", err)
		}
		if v.PC != 0 {
			t.Errorf("Expected PC=0 after IsNull with non-NULL, got %d", v.PC)
		}
	})

	t.Run("OpNotNull", func(t *testing.T) {
		v := NewTestVDBE(5)

		// Test with non-NULL value (should jump)
		v.Mem[0].SetInt(42)
		instr := &Instruction{Opcode: OpNotNull, P1: 0, P2: 10}
		err := v.execNotNull(instr)
		if err != nil {
			t.Errorf("execNotNull failed: %v", err)
		}
		if v.PC != 10 {
			t.Errorf("Expected PC=10 after NotNull with non-NULL, got %d", v.PC)
		}

		// Test with NULL value (should not jump)
		v.PC = 0
		v.Mem[0].SetNull()
		err = v.execNotNull(instr)
		if err != nil {
			t.Errorf("execNotNull failed: %v", err)
		}
		if v.PC != 0 {
			t.Errorf("Expected PC=0 after NotNull with NULL, got %d", v.PC)
		}
	})

	t.Run("OpSeekGE", func(t *testing.T) {
		// This requires a cursor, which is more complex
		// Just test the basic error path for now
		v := NewTestVDBE(5)
		v.AllocCursors(5)

		v.Mem[0].SetInt(42)
		instr := &Instruction{Opcode: OpSeekGE, P1: 0, P2: 10, P3: 0}
		err := v.execSeekGE(instr)
		// Expect error since cursor not set up
		if err == nil {
			t.Errorf("Expected error for SeekGE without cursor")
		}
	})

	t.Run("OpSeekLE", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(5)

		v.Mem[0].SetInt(42)
		instr := &Instruction{Opcode: OpSeekLE, P1: 0, P2: 10, P3: 0}
		err := v.execSeekLE(instr)
		// Expect error since cursor not set up
		if err == nil {
			t.Errorf("Expected error for SeekLE without cursor")
		}
	})
}

// TestVDBEUtilityMethods tests utility methods with 0% coverage
func TestVDBEUtilityMethods(t *testing.T) {
	t.Run("Finalize", func(t *testing.T) {
		v := New()
		v.AllocMemory(5)
		v.AddOp(OpHalt, 0, 0, 0)

		err := v.Finalize()
		if err != nil {
			t.Fatalf("Finalize failed: %v", err)
		}

		if v.State != StateHalt {
			t.Errorf("Expected state StateHalt, got %v", v.State)
		}
	})

	t.Run("GetError", func(t *testing.T) {
		v := New()
		v.SetError("test error")

		err := v.GetError()
		if err != "test error" {
			t.Errorf("Expected 'test error', got '%s'", err)
		}
	})

	t.Run("IsReadOnly_SetReadOnly", func(t *testing.T) {
		v := New()

		if v.IsReadOnly() {
			t.Error("Expected read-write by default")
		}

		v.SetReadOnly(true)
		if !v.IsReadOnly() {
			t.Error("Expected read-only after SetReadOnly(true)")
		}

		v.SetReadOnly(false)
		if v.IsReadOnly() {
			t.Error("Expected read-write after SetReadOnly(false)")
		}
	})

	t.Run("NumOps", func(t *testing.T) {
		v := New()
		v.AddOp(OpHalt, 0, 0, 0)
		v.AddOp(OpInteger, 1, 0, 0)
		v.AddOp(OpInteger, 2, 1, 0)

		if v.NumOps() != 3 {
			t.Errorf("Expected 3 operations, got %d", v.NumOps())
		}
	})

	t.Run("GetInstruction", func(t *testing.T) {
		v := New()
		v.AddOp(OpInteger, 42, 5, 0)

		instr, err := v.GetInstruction(0)
		if err != nil {
			t.Fatalf("GetInstruction failed: %v", err)
		}
		if instr.Opcode != OpInteger {
			t.Errorf("Expected OpInteger, got %v", instr.Opcode)
		}
		if instr.P1 != 42 {
			t.Errorf("Expected P1=42, got %d", instr.P1)
		}
		if instr.P2 != 5 {
			t.Errorf("Expected P2=5, got %d", instr.P2)
		}

		// Test out of bounds
		instr, err = v.GetInstruction(10)
		if err == nil {
			t.Error("Expected error for out of bounds instruction")
		}
	})

	t.Run("AddOpWithP4Int", func(t *testing.T) {
		v := New()
		v.AddOpWithP4Int(OpInteger, 0, 0, 0, 42)

		if v.Program[0].P4.I != 42 {
			t.Errorf("Expected P4.I=42, got %d", v.Program[0].P4.I)
		}
		if v.Program[0].P4Type != P4Int32 {
			t.Errorf("Expected P4Type=P4Int32, got %v", v.Program[0].P4Type)
		}
	})

	t.Run("AddOpWithP4Real", func(t *testing.T) {
		v := New()
		v.AddOpWithP4Real(OpReal, 0, 0, 0, 3.14)

		if v.Program[0].P4.R != 3.14 {
			t.Errorf("Expected P4.R=3.14, got %f", v.Program[0].P4.R)
		}
		if v.Program[0].P4Type != P4Real {
			t.Errorf("Expected P4Type=P4Real, got %v", v.Program[0].P4Type)
		}
	})

	t.Run("AddOpWithP4Blob", func(t *testing.T) {
		v := New()
		blob := []byte{1, 2, 3, 4}
		v.AddOpWithP4Blob(OpBlob, 0, 0, 0, blob)

		if p4Blob, ok := v.Program[0].P4.P.([]byte); !ok || len(p4Blob) != 4 {
			t.Errorf("Expected P4.P to be []byte{1,2,3,4}, got %v", v.Program[0].P4.P)
		}
		if v.Program[0].P4Type != P4Dynamic {
			t.Errorf("Expected P4Type=P4Dynamic, got %v", v.Program[0].P4Type)
		}
	})
}

// TestNewFunctionContextWithRegistry tests the function context creation
func TestNewFunctionContextWithRegistry(t *testing.T) {
	ctx := NewFunctionContextWithRegistry(nil)
	if ctx == nil {
		t.Error("Expected non-nil context")
	}
}

// TestResetAggregateState tests resetting aggregate state
func TestResetAggregateState(t *testing.T) {
	ctx := NewFunctionContext()
	ctx.GetOrCreateAggregateState(0)
	ctx.ResetAggregateState(0)
	// Just ensure no panic
}
