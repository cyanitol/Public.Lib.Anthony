// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

func TestDriverInsertSelect(t *testing.T) {
	// Create an in-memory btree (no pager)
	bt := btree.NewBtree(4096)

	// Create schema
	sch := schema.NewSchema()

	// Create a simple table definition using parser structs
	createStmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER", Constraints: []parser.ColumnConstraint{{Type: parser.ConstraintPrimaryKey}}},
			{Name: "value", Type: "TEXT"},
		},
	}

	// Create the table in schema
	table, err := sch.CreateTable(createStmt)
	if err != nil {
		t.Fatalf("failed to create table in schema: %v", err)
	}

	// Allocate root page for the table
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("failed to create btree table: %v", err)
	}
	table.RootPage = rootPage
	t.Logf("Created table with root page %d", rootPage)

	// Create VDBE context
	ctx := &vdbe.VDBEContext{
		Btree:  bt,
		Schema: sch,
	}

	// Test INSERT
	t.Run("INSERT", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate INSERT bytecode (similar to what compileInsert would do)
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpOpenWrite, 0, int(rootPage), 2)
		vm.AddOp(vdbe.OpNewRowid, 0, 0, 0)
		vm.AddOp(vdbe.OpInteger, 1, 1, 0)                   // id = 1
		vm.AddOpWithP4Str(vdbe.OpString8, 0, 2, 0, "hello") // value = "hello"
		vm.AddOp(vdbe.OpMakeRecord, 1, 2, 3)
		vm.AddOp(vdbe.OpInsert, 0, 3, 0)
		vm.AddOp(vdbe.OpClose, 0, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)

		t.Logf("INSERT bytecode:\n%s", vm.ExplainProgram())

		err := vm.Run()
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		t.Logf("INSERT successful, NumChanges=%d", vm.NumChanges)
	})

	// Verify page has data
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	header, err := btree.ParsePageHeader(pageData, rootPage)
	if err != nil {
		t.Fatalf("ParsePageHeader failed: %v", err)
	}
	t.Logf("After INSERT: numCells=%d", header.NumCells)

	// Test SELECT
	t.Run("SELECT", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate SELECT bytecode (similar to what compileSelect would do)
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpOpenRead, 0, int(rootPage), 2)
		rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
		vm.AddOp(vdbe.OpColumn, 0, 0, 0) // column 0 (id) into reg 0
		vm.AddOp(vdbe.OpColumn, 0, 1, 1) // column 1 (value) into reg 1
		vm.AddOp(vdbe.OpResultRow, 0, 2, 0)
		vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
		vm.AddOp(vdbe.OpClose, 0, 0, 0)
		haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		vm.Program[rewindAddr].P2 = haltAddr

		t.Logf("SELECT bytecode:\n%s", vm.ExplainProgram())

		// Step until we get a row
		for i := 0; i < 20; i++ {
			hasMore, err := vm.Step()
			if err != nil {
				t.Fatalf("Step failed: %v", err)
			}
			if vm.State == vdbe.StateRowReady {
				t.Logf("Got row with %d columns", len(vm.ResultRow))
				for j, mem := range vm.ResultRow {
					if mem.IsInt() {
						t.Logf("  Column %d: %d (int)", j, mem.IntValue())
					} else if mem.IsStr() {
						t.Logf("  Column %d: %q (string)", j, mem.StrValue())
					} else {
						t.Logf("  Column %d: type=%v", j, mem)
					}
				}
				// Verify values
				if len(vm.ResultRow) >= 2 {
					if vm.ResultRow[0].IntValue() != 1 {
						t.Errorf("Expected id=1, got %d", vm.ResultRow[0].IntValue())
					}
					if vm.ResultRow[1].StrValue() != "hello" {
						t.Errorf("Expected value='hello', got %q", vm.ResultRow[1].StrValue())
					}
				}
				return
			}
			if !hasMore {
				t.Fatalf("No more rows but never got StateRowReady")
			}
		}
		t.Fatalf("Too many steps without getting a row")
	})
}
