// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// Helper to create INSERT VM
func createInsertVM(ctx *VDBEContext, rootPage uint32) *VDBE {
	vm := New()
	vm.Ctx = ctx
	vm.AllocMemory(10)
	vm.AllocCursors(1)

	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpOpenWrite, 0, int(rootPage), 2)
	vm.AddOp(OpNewRowid, 0, 0, 0)
	vm.AddOp(OpInteger, 42, 1, 0)
	vm.AddOpWithP4Str(OpString8, 0, 2, 0, "test")
	vm.AddOp(OpMakeRecord, 1, 2, 3)
	vm.AddOp(OpInsert, 0, 3, 0)
	vm.AddOp(OpClose, 0, 0, 0)
	vm.AddOp(OpHalt, 0, 0, 0)

	return vm
}

// Helper to create SELECT VM
func createSelectVM(ctx *VDBEContext, rootPage uint32) *VDBE {
	vm := New()
	vm.Ctx = ctx
	vm.AllocMemory(10)
	vm.AllocCursors(1)

	vm.AddOp(OpInit, 0, 0, 0)
	vm.AddOp(OpOpenRead, 0, int(rootPage), 2)
	rewindAddr := vm.AddOp(OpRewind, 0, 0, 0)
	vm.AddOp(OpColumn, 0, 0, 0)
	vm.AddOp(OpColumn, 0, 1, 1)
	vm.AddOp(OpResultRow, 0, 2, 0)
	vm.AddOp(OpNext, 0, rewindAddr+1, 0)
	vm.AddOp(OpClose, 0, 0, 0)
	haltAddr := vm.AddOp(OpHalt, 0, 0, 0)

	vm.Program[rewindAddr].P2 = haltAddr
	return vm
}

// Helper to step until row ready or halt
func stepUntilRowOrHalt(t *testing.T, vm *VDBE, maxSteps int) {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		hasMore, err := vm.Step()
		if err != nil {
			t.Fatalf("Step failed at PC=%d: %v", vm.PC, err)
		}
		if vm.State == StateRowReady || vm.State == StateHalt || !hasMore {
			return
		}
	}
}

// Helper to verify result columns
func verifyResultColumns(t *testing.T, vm *VDBE, expectedInt int64, expectedStr string) {
	t.Helper()
	if len(vm.ResultRow) < 2 {
		t.Fatalf("Expected at least 2 columns, got %d", len(vm.ResultRow))
	}
	col0 := vm.ResultRow[0]
	if !col0.IsInt() || col0.IntValue() != expectedInt {
		t.Errorf("Expected column 0 = %d, got %v", expectedInt, col0)
	}
	col1 := vm.ResultRow[1]
	if !col1.IsStr() || col1.StrValue() != expectedStr {
		t.Errorf("Expected column 1 = '%s', got %v", expectedStr, col1)
	}
}

func TestVDBEInsert(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	t.Logf("Created table with root page %d", rootPage)

	ctx := &VDBEContext{Btree: bt}
	vm := createInsertVM(ctx, rootPage)
	t.Logf("INSERT bytecode:\n%s", vm.ExplainProgram())

	err = vm.Run()
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	t.Logf("INSERT successful, NumChanges=%d", vm.NumChanges)

	if vm.NumChanges != 1 {
		t.Errorf("Expected 1 change, got %d", vm.NumChanges)
	}

	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	header, err := btree.ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader failed: %v", err)
	}
	t.Logf("After INSERT: numCells=%d", header.NumCells)

	if header.NumCells != 1 {
		t.Fatalf("Expected 1 cell after INSERT, got %d", header.NumCells)
	}
}

func setupSelectTestVM(t *testing.T) *VDBE {
	t.Helper()
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	ctx := &VDBEContext{Btree: bt}
	if err := createInsertVM(ctx, rootPage).Run(); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	vm := createSelectVM(ctx, rootPage)
	t.Logf("SELECT bytecode:\n%s", vm.ExplainProgram())
	stepUntilRowOrHalt(t, vm, 100)
	return vm
}

func logResultRow(t *testing.T, vm *VDBE) {
	t.Helper()
	t.Logf("Got result row with %d columns", len(vm.ResultRow))
	for i, mem := range vm.ResultRow {
		logColumnValue(t, i, mem)
	}
}

func logColumnValue(t *testing.T, i int, mem *Mem) {
	t.Helper()
	switch {
	case mem.IsInt():
		t.Logf("  Column %d: %d (int)", i, mem.IntValue())
	case mem.IsStr():
		t.Logf("  Column %d: %q (string)", i, mem.StrValue())
	case mem.IsNull():
		t.Logf("  Column %d: NULL", i)
	default:
		t.Logf("  Column %d: unknown type", i)
	}
}

func TestVDBESelect(t *testing.T) {
	t.Parallel()
	vm := setupSelectTestVM(t)
	if vm.State == StateHalt {
		t.Fatalf("SELECT halted without returning rows, PC=%d", vm.PC)
	}
	if vm.State != StateRowReady {
		t.Fatalf("Expected StateRowReady, got %d", vm.State)
	}
	if vm.ResultRow == nil {
		t.Fatal("ResultRow is nil")
	}
	logResultRow(t, vm)
	verifyResultColumns(t, vm, 42, "test")
}
