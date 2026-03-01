package vdbe

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

func TestVDBEInsertAndSelect(t *testing.T) {
	t.Parallel()
	// Create a btree and initialize a table
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	t.Logf("Created table with root page %d", rootPage)

	// Create VDBE context
	ctx := &VDBEContext{
		Btree: bt,
	}

	// Test INSERT (not parallel - parent checks page data after this completes)
	t.Run("INSERT", func(t *testing.T) {
		vm := New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate INSERT bytecode
		// Init
		vm.AddOp(OpInit, 0, 0, 0)
		// OpenWrite cursor 0 on root page
		vm.AddOp(OpOpenWrite, 0, int(rootPage), 2)
		// NewRowid - generate rowid in reg 0
		vm.AddOp(OpNewRowid, 0, 0, 0)
		// Integer 42 in reg 1
		vm.AddOp(OpInteger, 42, 1, 0)
		// String "test" in reg 2 (P2 is destination register)
		vm.AddOpWithP4Str(OpString8, 0, 2, 0, "test")
		// MakeRecord from regs 1-2 into reg 3
		vm.AddOp(OpMakeRecord, 1, 2, 3)
		// Insert record from reg 3
		vm.AddOp(OpInsert, 0, 3, 0)
		// Close
		vm.AddOp(OpClose, 0, 0, 0)
		// Halt
		vm.AddOp(OpHalt, 0, 0, 0)

		t.Logf("INSERT bytecode:\n%s", vm.ExplainProgram())

		err := vm.Run()
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		t.Logf("INSERT successful, NumChanges=%d", vm.NumChanges)

		if vm.NumChanges != 1 {
			t.Errorf("Expected 1 change, got %d", vm.NumChanges)
		}
	})

	// Verify the data was inserted by checking the page directly
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

	// Test SELECT (not parallel - depends on INSERT having completed)
	t.Run("SELECT", func(t *testing.T) {
		vm := New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate SELECT bytecode
		// Init
		vm.AddOp(OpInit, 0, 0, 0)
		// OpenRead cursor 0 on root page
		vm.AddOp(OpOpenRead, 0, int(rootPage), 2)
		// Rewind to first row, jump to Halt if empty
		rewindAddr := vm.AddOp(OpRewind, 0, 0, 0)

		// Column 0 from cursor 0 into reg 0
		vm.AddOp(OpColumn, 0, 0, 0)
		// Column 1 from cursor 0 into reg 1
		vm.AddOp(OpColumn, 0, 1, 1)
		// ResultRow from regs 0-1
		vm.AddOp(OpResultRow, 0, 2, 0)
		// Next - jump back to Column if more rows
		vm.AddOp(OpNext, 0, rewindAddr+1, 0)

		// Close
		vm.AddOp(OpClose, 0, 0, 0)
		// Halt
		haltAddr := vm.AddOp(OpHalt, 0, 0, 0)

		// Fix Rewind jump target
		vm.Program[rewindAddr].P2 = haltAddr

		t.Logf("SELECT bytecode:\n%s", vm.ExplainProgram())

		// Step through to get the first row - Step() only executes ONE instruction
		// so we need to loop until we hit StateRowReady or StateHalt
		var hasMore bool
		var err error
		maxSteps := 100 // Prevent infinite loops
		for i := 0; i < maxSteps; i++ {
			hasMore, err = vm.Step()
			if err != nil {
				t.Fatalf("Step failed at PC=%d: %v", vm.PC, err)
			}
			t.Logf("Step %d: PC=%d, State=%d, hasMore=%v", i, vm.PC, vm.State, hasMore)
			if vm.State == StateRowReady || vm.State == StateHalt || !hasMore {
				break
			}
		}

		if vm.State == StateHalt {
			t.Fatalf("SELECT halted without returning rows, PC=%d", vm.PC)
		}

		if vm.State != StateRowReady {
			t.Fatalf("Expected StateRowReady, got %d", vm.State)
		}

		if vm.ResultRow == nil {
			t.Fatal("ResultRow is nil")
		}

		t.Logf("Got result row with %d columns", len(vm.ResultRow))
		for i, mem := range vm.ResultRow {
			if mem.IsInt() {
				t.Logf("  Column %d: %d (int)", i, mem.IntValue())
			} else if mem.IsStr() {
				t.Logf("  Column %d: %q (string)", i, mem.StrValue())
			} else if mem.IsNull() {
				t.Logf("  Column %d: NULL", i)
			} else {
				t.Logf("  Column %d: unknown type", i)
			}
		}

		// Verify values
		if len(vm.ResultRow) < 2 {
			t.Fatalf("Expected at least 2 columns, got %d", len(vm.ResultRow))
		}

		col0 := vm.ResultRow[0]
		if !col0.IsInt() || col0.IntValue() != 42 {
			t.Errorf("Expected column 0 = 42, got %v", col0)
		}

		col1 := vm.ResultRow[1]
		if !col1.IsStr() || col1.StrValue() != "test" {
			t.Errorf("Expected column 1 = 'test', got %v", col1)
		}
	})
}
