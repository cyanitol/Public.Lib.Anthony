// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"
)

// Helper to assert no error occurred
func assertNoError(t *testing.T, err error, operation string) {
	t.Helper()
	if err != nil {
		t.Errorf("%s failed: %v", operation, err)
	}
}

// Helper to assert PC value
func assertPC(t *testing.T, v *VDBE, expected int, context string) {
	t.Helper()
	if v.PC != expected {
		t.Errorf("Expected PC=%d %s, got %d", expected, context, v.PC)
	}
}

// Helper to assert register int value
func assertRegInt(t *testing.T, v *VDBE, reg int, expected int64, context string) {
	t.Helper()
	if v.Mem[reg].IntValue() != expected {
		t.Errorf("Expected r%d=%d %s, got %d", reg, expected, context, v.Mem[reg].IntValue())
	}
}

// Helper to assert register is NULL
func assertRegNull(t *testing.T, v *VDBE, reg int) {
	t.Helper()
	if !v.Mem[reg].IsNull() {
		t.Errorf("Expected r%d to be NULL", reg)
	}
}

// TestMissingOpcodesCoverage adds coverage for opcodes that have 0% coverage
func TestMissingOpcodesCoverage(t *testing.T) {
	t.Parallel()
	t.Run("OpNoop", testOpNoop)
	t.Run("OpGosub_Return", testOpGosubReturn)
	t.Run("OpIfNot", testOpIfNot)
	t.Run("OpIfPos", testOpIfPos)
	t.Run("OpInt64", testOpInt64)
	t.Run("OpNull", testOpNull)
	t.Run("OpCopy", testOpCopy)
	t.Run("OpMove", testOpMove)
	t.Run("OpSCopy", testOpSCopy)
	t.Run("OpSubtract", testOpSubtract)
	t.Run("OpMultiply", testOpMultiply)
	t.Run("OpDivide", testOpDivide)
	t.Run("OpRemainder", testOpRemainder)
	t.Run("OpAddImm", testOpAddImm)
	t.Run("OpIsNull", testOpIsNull)
	t.Run("OpNotNull", testOpNotNull)
	t.Run("OpSeekGE", testOpSeekGE)
	t.Run("OpSeekLE", testOpSeekLE)
}

func testOpNoop(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{Opcode: OpNoop}
	assertNoError(t, v.execNoop(instr), "execNoop")
}

func testOpGosubReturn(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.PC = 1

	gosubInstr := &Instruction{Opcode: OpGosub, P1: 0, P2: 3}
	assertNoError(t, v.execGosub(gosubInstr), "execGosub")
	assertPC(t, v, 3, "after Gosub")
	assertRegInt(t, v, 0, 1, "return addr")

	returnInstr := &Instruction{Opcode: OpReturn, P1: 0}
	assertNoError(t, v.execReturn(returnInstr), "execReturn")
	assertPC(t, v, 1, "after Return")
}

func testOpIfNot(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{Opcode: OpIfNot, P1: 0, P2: 10}

	v.Mem[0].SetInt(0)
	assertNoError(t, v.execIfNot(instr), "execIfNot with false")
	assertPC(t, v, 10, "after IfNot with false")

	v.PC = 0
	v.Mem[0].SetInt(1)
	assertNoError(t, v.execIfNot(instr), "execIfNot with true")
	assertPC(t, v, 0, "after IfNot with true")
}

func testOpIfPos(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{Opcode: OpIfPos, P1: 0, P2: 10, P3: -1}

	v.Mem[0].SetInt(3)
	assertNoError(t, v.execIfPos(instr), "execIfPos with positive")
	assertPC(t, v, 10, "after IfPos with positive")
	assertRegInt(t, v, 0, 2, "(decremented)")

	v.PC = 0
	v.Mem[0].SetInt(0)
	assertNoError(t, v.execIfPos(instr), "execIfPos with zero")
	assertPC(t, v, 0, "after IfPos with zero")
}

func testOpInt64(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{
		Opcode: OpInt64,
		P2:     1,
		P4:     P4Union{I64: 9223372036854775807},
		P4Type: P4Int64,
	}
	assertNoError(t, v.execInt64(instr), "execInt64")
	assertRegInt(t, v, 1, 9223372036854775807, "")
}

func testOpNull(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(42)
	v.Mem[1].SetInt(43)
	v.Mem[2].SetInt(44)

	instr := &Instruction{Opcode: OpNull, P2: 0, P3: 2}
	assertNoError(t, v.execNull(instr), "execNull")

	for i := 0; i < 3; i++ {
		assertRegNull(t, v, i)
	}
}

func testOpCopy(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(42)

	instr := &Instruction{Opcode: OpCopy, P1: 0, P2: 1}
	assertNoError(t, v.execCopy(instr), "execCopy")
	assertRegInt(t, v, 1, 42, "")
}

func testOpMove(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(42)
	v.Mem[1].SetInt(43)

	instr := &Instruction{Opcode: OpMove, P1: 0, P2: 5, P3: 2}
	assertNoError(t, v.execMove(instr), "execMove")
	assertRegInt(t, v, 5, 42, "")
	assertRegInt(t, v, 6, 43, "")
}

func testOpSCopy(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(42)

	instr := &Instruction{Opcode: OpSCopy, P1: 0, P2: 1}
	assertNoError(t, v.execSCopy(instr), "execSCopy")
	assertRegInt(t, v, 1, 42, "")
}

func testOpSubtract(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(50)
	v.Mem[1].SetInt(8)

	instr := &Instruction{Opcode: OpSubtract, P1: 0, P2: 1, P3: 2}
	assertNoError(t, v.execSubtract(instr), "execSubtract")
	assertRegInt(t, v, 2, 42, "")
}

func testOpMultiply(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(6)
	v.Mem[1].SetInt(7)

	instr := &Instruction{Opcode: OpMultiply, P1: 1, P2: 0, P3: 2}
	assertNoError(t, v.execMultiply(instr), "execMultiply")
	assertRegInt(t, v, 2, 42, "")
}

func testOpDivide(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(84)
	v.Mem[1].SetInt(2)

	instr := &Instruction{Opcode: OpDivide, P1: 0, P2: 1, P3: 2}
	assertNoError(t, v.execDivide(instr), "execDivide")
	assertRegInt(t, v, 2, 42, "")

	v.Mem[1].SetInt(0)
	assertNoError(t, v.execDivide(instr), "execDivide by zero")
	assertRegNull(t, v, 2)
}

func testOpRemainder(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(47)
	v.Mem[1].SetInt(5)

	instr := &Instruction{Opcode: OpRemainder, P1: 0, P2: 1, P3: 2}
	assertNoError(t, v.execRemainder(instr), "execRemainder")
	assertRegInt(t, v, 2, 2, "")

	v.Mem[1].SetInt(0)
	assertNoError(t, v.execRemainder(instr), "execRemainder by zero")
	assertRegNull(t, v, 2)
}

func testOpAddImm(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(40)

	instr := &Instruction{Opcode: OpAddImm, P1: 0, P2: 2}
	assertNoError(t, v.execAddImm(instr), "execAddImm")
	assertRegInt(t, v, 0, 42, "")
}

func testOpIsNull(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{Opcode: OpIsNull, P1: 0, P2: 10}

	v.Mem[0].SetNull()
	assertNoError(t, v.execIsNull(instr), "execIsNull with NULL")
	assertPC(t, v, 10, "after IsNull with NULL")

	v.PC = 0
	v.Mem[0].SetInt(42)
	assertNoError(t, v.execIsNull(instr), "execIsNull with non-NULL")
	assertPC(t, v, 0, "after IsNull with non-NULL")
}

func testOpNotNull(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	instr := &Instruction{Opcode: OpNotNull, P1: 0, P2: 10}

	v.Mem[0].SetInt(42)
	assertNoError(t, v.execNotNull(instr), "execNotNull with non-NULL")
	assertPC(t, v, 10, "after NotNull with non-NULL")

	v.PC = 0
	v.Mem[0].SetNull()
	assertNoError(t, v.execNotNull(instr), "execNotNull with NULL")
	assertPC(t, v, 0, "after NotNull with NULL")
}

func testOpSeekGE(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.AllocCursors(5)
	v.Mem[0].SetInt(42)
	instr := &Instruction{Opcode: OpSeekGE, P1: 0, P2: 10, P3: 0}
	err := v.execSeekGE(instr)
	if err == nil {
		t.Errorf("Expected error for SeekGE without cursor")
	}
}

func testOpSeekLE(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.AllocCursors(5)
	v.Mem[0].SetInt(42)
	instr := &Instruction{Opcode: OpSeekLE, P1: 0, P2: 10, P3: 0}
	err := v.execSeekLE(instr)
	if err == nil {
		t.Errorf("Expected error for SeekLE without cursor")
	}
}

// utilSetupFinalize sets up VDBE for Finalize test
func utilSetupFinalize(v *VDBE) {
	v.AllocMemory(5)
	v.AddOp(OpHalt, 0, 0, 0)
}

// utilVerifyFinalize verifies Finalize behavior
func utilVerifyFinalize(t *testing.T, v *VDBE) {
	if err := v.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	if v.State != StateHalt {
		t.Errorf("Expected StateHalt, got %v", v.State)
	}
}

// utilSetupGetError sets up VDBE for GetError test
func utilSetupGetError(v *VDBE) {
	v.SetError("test error")
}

// utilVerifyGetError verifies GetError behavior
func utilVerifyGetError(t *testing.T, v *VDBE) {
	if err := v.GetError(); err != "test error" {
		t.Errorf("Expected 'test error', got '%s'", err)
	}
}

// utilVerifyReadOnly verifies IsReadOnly/SetReadOnly behavior
func utilVerifyReadOnly(t *testing.T, v *VDBE) {
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
}

// utilSetupNumOps sets up VDBE for NumOps test
func utilSetupNumOps(v *VDBE) {
	v.AddOp(OpHalt, 0, 0, 0)
	v.AddOp(OpInteger, 1, 0, 0)
	v.AddOp(OpInteger, 2, 1, 0)
}

// utilVerifyNumOps verifies NumOps behavior
func utilVerifyNumOps(t *testing.T, v *VDBE) {
	if v.NumOps() != 3 {
		t.Errorf("Expected 3 operations, got %d", v.NumOps())
	}
}

// utilSetupGetInstruction sets up VDBE for GetInstruction test
func utilSetupGetInstruction(v *VDBE) {
	v.AddOp(OpInteger, 42, 5, 0)
}

// utilVerifyGetInstruction verifies GetInstruction behavior
func utilVerifyGetInstruction(t *testing.T, v *VDBE) {
	instr, err := v.GetInstruction(0)
	if err != nil {
		t.Fatalf("GetInstruction failed: %v", err)
	}
	if instr.Opcode != OpInteger || instr.P1 != 42 || instr.P2 != 5 {
		t.Errorf("Unexpected instruction values")
	}
	if _, err := v.GetInstruction(10); err == nil {
		t.Error("Expected error for out of bounds")
	}
}

// utilSetupP4Int sets up VDBE for P4Int test
func utilSetupP4Int(v *VDBE) {
	v.AddOpWithP4Int(OpInteger, 0, 0, 0, 42)
}

// utilVerifyP4Int verifies P4Int behavior
func utilVerifyP4Int(t *testing.T, v *VDBE) {
	if v.Program[0].P4.I != 42 || v.Program[0].P4Type != P4Int32 {
		t.Errorf("P4Int not set correctly")
	}
}

// utilSetupP4Real sets up VDBE for P4Real test
func utilSetupP4Real(v *VDBE) {
	v.AddOpWithP4Real(OpReal, 0, 0, 0, 3.14)
}

// utilVerifyP4Real verifies P4Real behavior
func utilVerifyP4Real(t *testing.T, v *VDBE) {
	if v.Program[0].P4.R != 3.14 || v.Program[0].P4Type != P4Real {
		t.Errorf("P4Real not set correctly")
	}
}

// utilSetupP4Blob sets up VDBE for P4Blob test
func utilSetupP4Blob(v *VDBE) {
	v.AddOpWithP4Blob(OpBlob, 0, 0, 0, []byte{1, 2, 3, 4})
}

// utilVerifyP4Blob verifies P4Blob behavior
func utilVerifyP4Blob(t *testing.T, v *VDBE) {
	p4Blob, ok := v.Program[0].P4.P.([]byte)
	if !ok || len(p4Blob) != 4 || v.Program[0].P4Type != P4Dynamic {
		t.Errorf("P4Blob not set correctly")
	}
}

// utilTestCase represents a declarative utility method test
type utilTestCase struct {
	name   string
	setup  func(*VDBE)
	verify func(*testing.T, *VDBE)
}

// TestVDBEUtilityMethods tests utility methods with 0% coverage
func TestVDBEUtilityMethods(t *testing.T) {
	t.Parallel()
	tests := []utilTestCase{
		{name: "Finalize", setup: utilSetupFinalize, verify: utilVerifyFinalize},
		{name: "GetError", setup: utilSetupGetError, verify: utilVerifyGetError},
		{name: "IsReadOnly_SetReadOnly", setup: nil, verify: utilVerifyReadOnly},
		{name: "NumOps", setup: utilSetupNumOps, verify: utilVerifyNumOps},
		{name: "GetInstruction", setup: utilSetupGetInstruction, verify: utilVerifyGetInstruction},
		{name: "AddOpWithP4Int", setup: utilSetupP4Int, verify: utilVerifyP4Int},
		{name: "AddOpWithP4Real", setup: utilSetupP4Real, verify: utilVerifyP4Real},
		{name: "AddOpWithP4Blob", setup: utilSetupP4Blob, verify: utilVerifyP4Blob},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			if tt.setup != nil {
				tt.setup(v)
			}
			tt.verify(t, v)
		})
	}
}

// TestNewFunctionContextWithRegistry tests the function context creation
func TestNewFunctionContextWithRegistry(t *testing.T) {
	t.Parallel()
	ctx := NewFunctionContextWithRegistry(nil)
	if ctx == nil {
		t.Error("Expected non-nil context")
	}
}

// TestResetAggregateState tests resetting aggregate state
func TestResetAggregateState(t *testing.T) {
	t.Parallel()
	ctx := NewFunctionContext()
	ctx.GetOrCreateAggregateState(0)
	ctx.ResetAggregateState(0)
	// Just ensure no panic
}

// TestGetPseudoCursorPayload tests the getPseudoCursorPayload function
func TestGetPseudoCursorPayload(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	v.AllocCursors(2)

	// Set up pseudo-table cursor with data
	recordData := []byte{1, 2, 3, 4, 5}
	v.Mem[5].SetBlob(recordData)

	cursor := &Cursor{
		CurType:   CursorPseudo,
		IsTable:   true,
		PseudoReg: 5,
	}
	v.Cursors[0] = cursor

	// Get payload from pseudo cursor
	dst := NewMem()
	payload := v.getPseudoCursorPayload(cursor, dst)

	if len(payload) != len(recordData) {
		t.Errorf("Expected payload length %d, got %d", len(recordData), len(payload))
	}

	for i, b := range recordData {
		if payload[i] != b {
			t.Errorf("Byte %d: expected %d, got %d", i, b, payload[i])
		}
	}
}

// TestExecRollback tests the execRollback function
func TestExecRollback(t *testing.T) {
	t.Parallel()
	t.Run("Rollback_WriteTransaction", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		mockPager := &MockTransactionPager{
			inWriteTxn: true,
			inTxn:      true,
		}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}

		instr := &Instruction{
			Opcode: OpRollback,
		}

		err := v.execRollback(instr)
		if err != nil {
			t.Fatalf("execRollback failed: %v", err)
		}

		if !mockPager.rolledBack {
			t.Error("Expected Rollback to be called")
		}
	})

	t.Run("Rollback_ReadTransaction", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		mockPager := &MockTransactionPager{
			inWriteTxn: false,
			inTxn:      true,
		}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}

		instr := &Instruction{
			Opcode: OpRollback,
		}

		err := v.execRollback(instr)
		if err != nil {
			t.Fatalf("execRollback failed: %v", err)
		}

		if !mockPager.endedRead {
			t.Error("Expected EndRead to be called")
		}
	})

	t.Run("Rollback_NoPager", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		// No context

		instr := &Instruction{
			Opcode: OpRollback,
		}

		err := v.execRollback(instr)
		if err == nil {
			t.Error("Expected error when no pager")
		}
	})
}

// MockTransactionPager for testing
type MockTransactionPager struct {
	inTxn      bool
	inWriteTxn bool
	committed  bool
	rolledBack bool
	endedRead  bool
}

func (m *MockTransactionPager) BeginRead() error {
	m.inTxn = true
	return nil
}

func (m *MockTransactionPager) EndRead() error {
	m.inTxn = false
	m.endedRead = true
	return nil
}

func (m *MockTransactionPager) BeginWrite() error {
	m.inTxn = true
	m.inWriteTxn = true
	return nil
}

func (m *MockTransactionPager) Commit() error {
	m.inTxn = false
	m.inWriteTxn = false
	m.committed = true
	return nil
}

func (m *MockTransactionPager) Rollback() error {
	m.inTxn = false
	m.inWriteTxn = false
	m.rolledBack = true
	return nil
}

func (m *MockTransactionPager) InTransaction() bool {
	return m.inTxn
}

func (m *MockTransactionPager) InWriteTransaction() bool {
	return m.inWriteTxn
}
