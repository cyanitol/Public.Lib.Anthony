// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

// MC/DC 11 — internal error-injection tests for vdbe
//
// These tests directly invoke unexported functions with injected failures
// to cover error-return paths that cannot be reached from SQL level.
//
// Targets:
//   fk_adapter.go:253  validateContext    — nil vdbe / nil Ctx error paths
//   fk_adapter.go:333  getTable           — table-not-found error path
//   fk_adapter.go:1310 UpdateRow          — WITH ROWID error check path
//   exec.go:4131       execAdd            — GetMem error path (out-of-bounds register)
//   exec.go:4181       execSubtract       — GetMem error path
//   exec.go:4202       execMultiply       — GetMem error path
//   exec.go:4223       execDivide         — GetMem error path
//   exec.go:4244       execRemainder      — GetMem error path
//   exec.go:566        execBlob           — GetMem error path
//   exec.go:955        execSeekRowid      — cursor not open error path
//   spill_file.go:68   writeRunToFile     — os.Create error path (bad dir)

import (
	"os"
	"testing"
)

// ---------------------------------------------------------------------------
// VDBERowReader — validateContext error paths
// ---------------------------------------------------------------------------

// TestMCDC11_ValidateContext_NilVDBE exercises the nil-vdbe branch in validateContext.
func TestMCDC11_ValidateContext_NilVDBE(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{vdbe: nil}
	_, err := r.RowExists("any_table", []string{"id"}, []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil vdbe, got nil")
	}
}

// TestMCDC11_ValidateContext_NilCtx exercises the nil-Ctx branch in validateContext.
func TestMCDC11_ValidateContext_NilCtx(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.Ctx = nil // explicitly set to nil
	r := &VDBERowReader{vdbe: vm}
	_, err := r.RowExists("any_table", []string{"id"}, []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil Ctx, got nil")
	}
}

// TestMCDC11_ValidateContext_NilCtx_FindReferencingRows exercises validateContext
// via FindReferencingRows.
func TestMCDC11_ValidateContext_NilCtx_FindReferencingRows(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{vdbe: nil}
	_, err := r.FindReferencingRows("any_table", []string{"pid"}, []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil vdbe in FindReferencingRows")
	}
}

// TestMCDC11_ValidateContext_NilCtx_FindReferencingRowsWithData exercises same
// via FindReferencingRowsWithData.
func TestMCDC11_ValidateContext_NilCtx_FindReferencingRowsWithData(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{vdbe: nil}
	_, err := r.FindReferencingRowsWithData("any_table", []string{"pid"}, []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil vdbe in FindReferencingRowsWithData")
	}
}

// TestMCDC11_ValidateContext_NilCtx_RowExistsWithCollation covers RowExistsWithCollation.
func TestMCDC11_ValidateContext_NilCtx_RowExistsWithCollation(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{vdbe: nil}
	_, err := r.RowExistsWithCollation("t", []string{"v"}, []interface{}{"x"}, []string{"BINARY"})
	if err == nil {
		t.Fatal("expected error for nil vdbe in RowExistsWithCollation")
	}
}

// ---------------------------------------------------------------------------
// VDBERowModifier — UpdateRow WITHOUT ROWID error path
// ---------------------------------------------------------------------------

// mcdc11MockSchema implements the schemaWithGetTableByName interface used by
// fk_adapter.go:getTable, always returning (nil, false) to simulate a missing table.
type mcdc11MockSchema struct{}

func (m *mcdc11MockSchema) GetTableByName(name string) (interface{}, bool) { return nil, false }

// newMCDC11MockSchemaVM creates a VDBE with a schema that always reports "table not found".
func newMCDC11MockSchemaVM() *VDBE {
	vm := New()
	vm.Ctx = &VDBEContext{Schema: &mcdc11MockSchema{}}
	vm.AllocMemory(10)
	vm.AllocCursors(10)
	return vm
}

// TestMCDC11_UpdateRow_NoSchema exercises UpdateRow when Schema is nil.
// The getTable call fails with "invalid schema type".
func TestMCDC11_UpdateRow_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	m := &VDBERowModifier{reader: r}
	err := m.UpdateRow("any_table", 1, map[string]interface{}{"v": 42})
	if err == nil {
		t.Fatal("expected error for nil Schema in UpdateRow")
	}
}

// TestMCDC11_DeleteRow_NoSchema exercises DeleteRow when Schema is nil.
func TestMCDC11_DeleteRow_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	m := &VDBERowModifier{reader: r}
	err := m.DeleteRow("any_table", 1)
	if err == nil {
		t.Fatal("expected error for nil Schema in DeleteRow")
	}
}

// TestMCDC11_DeleteRowByKey_NoSchema exercises DeleteRowByKey when Schema is nil.
func TestMCDC11_DeleteRowByKey_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	m := &VDBERowModifier{reader: r}
	err := m.DeleteRowByKey("any_table", []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil Schema in DeleteRowByKey")
	}
}

// TestMCDC11_UpdateRowByKey_NoSchema exercises UpdateRowByKey when Schema is nil.
func TestMCDC11_UpdateRowByKey_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	m := &VDBERowModifier{reader: r}
	err := m.UpdateRowByKey("any_table", []interface{}{1}, map[string]interface{}{"v": 99})
	if err == nil {
		t.Fatal("expected error for nil Schema in UpdateRowByKey")
	}
}

// TestMCDC11_ReadRowByRowid_NoSchema exercises ReadRowByRowid when Schema is nil.
func TestMCDC11_ReadRowByRowid_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	_, err := r.ReadRowByRowid("any_table", 1)
	if err == nil {
		t.Fatal("expected error for nil Schema in ReadRowByRowid")
	}
}

// TestMCDC11_ReadRowByKey_NoSchema exercises ReadRowByKey when Schema is nil.
func TestMCDC11_ReadRowByKey_NoSchema(t *testing.T) {
	t.Parallel()

	vm := newMCDC11MockSchemaVM()
	defer vm.Finalize()
	r := &VDBERowReader{vdbe: vm}
	_, err := r.ReadRowByKey("any_table", []interface{}{1})
	if err == nil {
		t.Fatal("expected error for nil Schema in ReadRowByKey")
	}
}

// ---------------------------------------------------------------------------
// execAdd/execSubtract/execMultiply/execDivide/execRemainder — GetMem errors
// These functions have 3 GetMem calls; injecting out-of-bounds registers covers
// the error return paths.
// ---------------------------------------------------------------------------

// newMCDC11VM creates a minimal VDBE with n registers and 0 cursors.
func newMCDC11VM(n int) *VDBE {
	vm := New()
	vm.AllocMemory(n)
	return vm
}

// TestMCDC11_ExecAdd_GetMemP1Error exercises execAdd GetMem(P1) error.
func TestMCDC11_ExecAdd_GetMemP1Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execAdd(instr); err == nil {
		t.Fatal("expected GetMem P1 out-of-bounds error")
	}
}

// TestMCDC11_ExecAdd_GetMemP2Error exercises execAdd GetMem(P2) error.
func TestMCDC11_ExecAdd_GetMemP2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 9999, P3: 0}
	if err := vm.execAdd(instr); err == nil {
		t.Fatal("expected GetMem P2 out-of-bounds error")
	}
}

// TestMCDC11_ExecAdd_GetMemP3Error exercises execAdd GetMem(P3) error.
func TestMCDC11_ExecAdd_GetMemP3Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 1, P3: 9999}
	if err := vm.execAdd(instr); err == nil {
		t.Fatal("expected GetMem P3 out-of-bounds error")
	}
}

// TestMCDC11_ExecSubtract_GetMemError exercises execSubtract GetMem(P1) error.
func TestMCDC11_ExecSubtract_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execSubtract(instr); err == nil {
		t.Fatal("expected GetMem error in execSubtract")
	}
}

// TestMCDC11_ExecMultiply_GetMemError exercises execMultiply GetMem(P1) error.
func TestMCDC11_ExecMultiply_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execMultiply(instr); err == nil {
		t.Fatal("expected GetMem error in execMultiply")
	}
}

// TestMCDC11_ExecDivide_GetMemError exercises execDivide GetMem(P1) error.
func TestMCDC11_ExecDivide_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execDivide(instr); err == nil {
		t.Fatal("expected GetMem error in execDivide")
	}
}

// TestMCDC11_ExecRemainder_GetMemError exercises execRemainder GetMem(P1) error.
func TestMCDC11_ExecRemainder_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execRemainder(instr); err == nil {
		t.Fatal("expected GetMem error in execRemainder")
	}
}

// TestMCDC11_ExecBlob_GetMemError exercises execBlob GetMem error.
func TestMCDC11_ExecBlob_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(2)
	defer vm.Finalize()
	instr := &Instruction{P2: 9999}
	if err := vm.execBlob(instr); err == nil {
		t.Fatal("expected GetMem P2 out-of-bounds error in execBlob")
	}
}

// TestMCDC11_ExecBlob_P4NotBytes exercises execBlob when P4.P is not []byte.
func TestMCDC11_ExecBlob_P4NotBytes(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P2: 0, P4: P4Union{P: "not a byte slice"}}
	if err := vm.execBlob(instr); err == nil {
		t.Fatal("expected error when P4 is not []byte")
	}
}

// ---------------------------------------------------------------------------
// execSeekRowid — cursor errors
// ---------------------------------------------------------------------------

// TestMCDC11_ExecSeekRowid_GetCursorError exercises execSeekRowid GetCursor error.
func TestMCDC11_ExecSeekRowid_GetCursorError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(5)
	defer vm.Finalize()
	vm.AllocCursors(3)
	// P1 = out-of-bounds cursor → GetCursor fails
	instr := &Instruction{P1: 9999, P2: 0, P3: 1}
	if err := vm.execSeekRowid(instr); err == nil {
		t.Fatal("expected GetCursor error in execSeekRowid")
	}
}

// TestMCDC11_ExecConcat_GetMemError exercises execConcat GetMem error.
func TestMCDC11_ExecConcat_GetMemError(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 9999, P2: 0, P3: 0}
	if err := vm.execConcat(instr); err == nil {
		t.Fatal("expected GetMem error in execConcat")
	}
}

// ---------------------------------------------------------------------------
// spill_file.go — writeRunToFile: os.Create error path
// ---------------------------------------------------------------------------

// TestMCDC11_WriteRunToFile_ClosedFile exercises writeRunToFile when the
// file is already closed, causing binary.Write to fail immediately.
func TestMCDC11_WriteRunToFile_ClosedFile(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, DefaultSorterConfig())
	defer s.Close()

	// Create a temp file, close it immediately, then pass the closed *os.File.
	f, err := os.CreateTemp("", "mcdc11_spill_*.tmp")
	if err != nil {
		t.Skipf("cannot create temp file: %v", err)
	}
	f.Close() // close it; subsequent writes will fail with "write on closed file"
	defer os.Remove(f.Name())

	rows := [][]*Mem{{NewMemInt(99), NewMemStr("injection")}}
	if err := s.writeRunToFile(f, rows); err == nil {
		t.Fatal("expected write error on closed file, got nil")
	}
}

// TestMCDC11_WriteAndRecordSpill_BadDir exercises writeAndRecordSpill when
// os.Create fails because TempDir does not exist.
func TestMCDC11_WriteAndRecordSpill_BadDir(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, DefaultSorterConfig())
	defer s.Close()

	const badPath = "/nonexistent_mcdc11_baddir/spill.tmp"
	if _, statErr := os.Stat("/nonexistent_mcdc11_baddir"); !os.IsNotExist(statErr) {
		t.Skip("test directory unexpectedly exists")
	}

	s.Sorter.Insert([]*Mem{NewMemInt(1), NewMemStr("a")})
	if err := s.writeAndRecordSpill(badPath, 1); err == nil {
		t.Fatal("expected error for nonexistent directory in writeAndRecordSpill")
	}
}

// ---------------------------------------------------------------------------
// getTableFromSchema — nil/missing schema paths
// ---------------------------------------------------------------------------

// TestMCDC11_GetTableFromSchema_NilCtx exercises the nil-Ctx branch directly.
func TestMCDC11_GetTableFromSchema_NilCtx(t *testing.T) {
	t.Parallel()
	vm := New()
	vm.Ctx = nil
	result := vm.getTableFromSchema("any_table")
	if result != nil {
		t.Errorf("expected nil for nil Ctx, got %v", result)
	}
}

// TestMCDC11_GetTableFromSchema_NilSchema exercises the nil-Schema branch.
func TestMCDC11_GetTableFromSchema_NilSchema(t *testing.T) {
	t.Parallel()
	vm := New()
	vm.Ctx = &VDBEContext{Schema: nil}
	result := vm.getTableFromSchema("any_table")
	if result != nil {
		t.Errorf("expected nil for nil Schema, got %v", result)
	}
}

// TestMCDC11_GetTableFromSchema_NotFound exercises the table-not-found path.
func TestMCDC11_GetTableFromSchema_NotFound(t *testing.T) {
	t.Parallel()
	vm := New()
	vm.Ctx = &VDBEContext{Schema: &mcdc11MockSchema{}}
	result := vm.getTableFromSchema("nonexistent_table")
	if result != nil {
		t.Errorf("expected nil for missing table, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// getShiftOperands — GetMem error paths for P2 and P3
// ---------------------------------------------------------------------------

// TestMCDC11_GetShiftOperands_P2Error exercises GetMem(P2) error in getShiftOperands.
func TestMCDC11_GetShiftOperands_P2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	// P1 valid, P2 out-of-bounds
	instr := &Instruction{P1: 0, P2: 9999, P3: 1}
	_, _, _, err := vm.getShiftOperands(instr)
	if err == nil {
		t.Fatal("expected GetMem(P2) error")
	}
}

// TestMCDC11_GetShiftOperands_P3Error exercises GetMem(P3) error in getShiftOperands.
func TestMCDC11_GetShiftOperands_P3Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	// P1 and P2 valid, P3 out-of-bounds
	instr := &Instruction{P1: 0, P2: 1, P3: 9999}
	_, _, _, err := vm.getShiftOperands(instr)
	if err == nil {
		t.Fatal("expected GetMem(P3) error")
	}
}

// ---------------------------------------------------------------------------
// execNewRowid — invalid btree type
// ---------------------------------------------------------------------------

// TestMCDC11_ExecNewRowid_InvalidBtree exercises the "invalid btree context type"
// error path in execNewRowid when Ctx.Btree is not a *btree.Btree.
func TestMCDC11_ExecNewRowid_InvalidBtree(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(5)
	defer vm.Finalize()
	vm.AllocCursors(3)
	// Set Ctx.Btree to nil (type assertion will fail)
	vm.Ctx = &VDBEContext{Btree: nil}
	vm.Cursors[0] = &Cursor{RootPage: 1}

	instr := &Instruction{P1: 0, P3: 1}
	if err := vm.execNewRowid(instr); err == nil {
		t.Fatal("expected 'invalid btree context type' error")
	}
}

// ---------------------------------------------------------------------------
// execSubtract/execMultiply/execDivide/execRemainder — P2/P3 GetMem errors
// ---------------------------------------------------------------------------

// TestMCDC11_ExecSubtract_P2Error exercises execSubtract GetMem(P2) error.
func TestMCDC11_ExecSubtract_P2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 9999, P3: 1}
	if err := vm.execSubtract(instr); err == nil {
		t.Fatal("expected GetMem P2 error in execSubtract")
	}
}

// TestMCDC11_ExecSubtract_P3Error exercises execSubtract GetMem(P3) error.
func TestMCDC11_ExecSubtract_P3Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 1, P3: 9999}
	if err := vm.execSubtract(instr); err == nil {
		t.Fatal("expected GetMem P3 error in execSubtract")
	}
}

// TestMCDC11_ExecMultiply_P2Error exercises execMultiply GetMem(P2) error.
func TestMCDC11_ExecMultiply_P2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 9999, P3: 1}
	if err := vm.execMultiply(instr); err == nil {
		t.Fatal("expected GetMem P2 error in execMultiply")
	}
}

// TestMCDC11_ExecDivide_P2Error exercises execDivide GetMem(P2) error.
func TestMCDC11_ExecDivide_P2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 9999, P3: 1}
	if err := vm.execDivide(instr); err == nil {
		t.Fatal("expected GetMem P2 error in execDivide")
	}
}

// TestMCDC11_ExecRemainder_P2Error exercises execRemainder GetMem(P2) error.
func TestMCDC11_ExecRemainder_P2Error(t *testing.T) {
	t.Parallel()
	vm := newMCDC11VM(3)
	defer vm.Finalize()
	instr := &Instruction{P1: 0, P2: 9999, P3: 1}
	if err := vm.execRemainder(instr); err == nil {
		t.Fatal("expected GetMem P2 error in execRemainder")
	}
}
