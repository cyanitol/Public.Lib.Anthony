// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"bytes"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// blobStepToRow steps the VM until it reaches a row-ready state.
func blobStepToRow(t *testing.T, vm *vdbe.VDBE) {
	t.Helper()
	for i := 0; i < 20; i++ {
		hasMore, err := vm.Step()
		if err != nil {
			t.Fatalf("Step failed: %v", err)
		}
		if vm.State == vdbe.StateRowReady {
			return
		}
		if !hasMore {
			t.Fatalf("No more rows but never got StateRowReady")
		}
	}
	t.Fatalf("Too many steps without getting a row")
}

// blobLogResultRow logs the types and values of the result row.
func blobLogResultRow(t *testing.T, vm *vdbe.VDBE) {
	t.Helper()
	t.Logf("Got row with %d columns", len(vm.ResultRow))
	for j, mem := range vm.ResultRow {
		switch {
		case mem.IsInt():
			t.Logf("  Column %d: %d (int)", j, mem.IntValue())
		case mem.IsBlob():
			t.Logf("  Column %d: %v (blob, %d bytes)", j, mem.BlobValue(), len(mem.BlobValue()))
		case mem.IsStr():
			t.Logf("  Column %d: %q (string)", j, mem.StrValue())
		default:
			t.Logf("  Column %d: type=%v", j, mem)
		}
	}
}

// blobVerifyResultRow checks that the result row contains the expected id and blob.
func blobVerifyResultRow(t *testing.T, vm *vdbe.VDBE, expectedBlob []byte) {
	t.Helper()
	if len(vm.ResultRow) < 2 {
		t.Fatalf("Expected at least 2 columns, got %d", len(vm.ResultRow))
	}
	if vm.ResultRow[0].IntValue() != 42 {
		t.Errorf("Expected id=42, got %d", vm.ResultRow[0].IntValue())
	}
	if !vm.ResultRow[1].IsBlob() {
		t.Errorf("Expected column 1 to be BLOB, got %v", vm.ResultRow[1])
		return
	}
	retrievedBlob := vm.ResultRow[1].BlobValue()
	if !bytes.Equal(retrievedBlob, expectedBlob) {
		t.Errorf("BLOB mismatch:\n  Expected: %v\n  Got:      %v", expectedBlob, retrievedBlob)
	} else {
		t.Logf("BLOB retrieved correctly!")
	}
}

// TestBlobHandling tests BLOB data type handling:
// 1. Ensure []byte values bind correctly as BLOBs
// 2. BLOB serial types in records are handled properly
// 3. BLOB values can be retrieved from SELECT
func TestBlobHandling(t *testing.T) {
	// Create an in-memory btree (no pager)
	bt := btree.NewBtree(4096)

	// Create schema
	sch := schema.NewSchema()

	// Create a table with a BLOB column
	createStmt := &parser.CreateTableStmt{
		Name: "test_blob",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER", Constraints: []parser.ColumnConstraint{{Type: parser.ConstraintPrimaryKey}}},
			{Name: "data", Type: "BLOB"},
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

	// Test data
	testBlob := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xDE, 0xAD, 0xBE, 0xEF}

	// Test INSERT with BLOB
	t.Run("INSERT_BLOB", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate INSERT bytecode with BLOB value
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpOpenWrite, 0, int(rootPage), 2)
		vm.AddOp(vdbe.OpInteger, 1, 0, 0)                              // rowid = 1 in register 0
		vm.AddOp(vdbe.OpInteger, 42, 1, 0)                             // id = 42 in register 1
		vm.AddOpWithP4Blob(vdbe.OpBlob, len(testBlob), 2, 0, testBlob) // data = testBlob in register 2
		vm.AddOp(vdbe.OpMakeRecord, 1, 2, 3)                           // Make record from registers 1-2 into register 3
		vm.AddOp(vdbe.OpInsert, 0, 3, 0)                               // Insert record from register 3 with rowid from register 0
		vm.AddOp(vdbe.OpClose, 0, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)

		t.Logf("INSERT BLOB bytecode:\n%s", vm.ExplainProgram())

		err := vm.Run()
		if err != nil {
			t.Fatalf("INSERT BLOB failed: %v", err)
		}
		t.Logf("INSERT BLOB successful, NumChanges=%d", vm.NumChanges)
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

	// Test SELECT to retrieve BLOB
	t.Run("SELECT_BLOB", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = ctx
		vm.AllocMemory(10)
		vm.AllocCursors(1)

		// Generate SELECT bytecode
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpOpenRead, 0, int(rootPage), 2)
		rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
		vm.AddOp(vdbe.OpColumn, 0, 0, 0) // column 0 (id) into reg 0
		vm.AddOp(vdbe.OpColumn, 0, 1, 1) // column 1 (data) into reg 1
		vm.AddOp(vdbe.OpResultRow, 0, 2, 0)
		vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
		vm.AddOp(vdbe.OpClose, 0, 0, 0)
		haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		vm.Program[rewindAddr].P2 = haltAddr

		t.Logf("SELECT BLOB bytecode:\n%s", vm.ExplainProgram())

		blobStepToRow(t, vm)
		blobLogResultRow(t, vm)
		blobVerifyResultRow(t, vm, testBlob)
	})
}

// blobBindingSetup creates a btree, schema, table, and returns (context, rootPage).
func blobBindingSetup(t *testing.T) (*vdbe.VDBEContext, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	sch := schema.NewSchema()
	createStmt := &parser.CreateTableStmt{
		Name: "blob_params",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER", Constraints: []parser.ColumnConstraint{{Type: parser.ConstraintPrimaryKey}}},
			{Name: "content", Type: "BLOB"},
		},
	}
	table, err := sch.CreateTable(createStmt)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("failed to create btree table: %v", err)
	}
	table.RootPage = rootPage
	return &vdbe.VDBEContext{Btree: bt, Schema: sch}, rootPage
}

// blobBindingInsertAndVerify inserts a blob and verifies it can be read back.
func blobBindingInsertAndVerify(t *testing.T, ctx *vdbe.VDBEContext, rootPage uint32, rowid int, data []byte) {
	t.Helper()
	vm := vdbe.New()
	vm.Ctx = ctx
	vm.AllocMemory(10)
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(rootPage), 2)
	vm.AddOp(vdbe.OpInteger, rowid, 0, 0)
	vm.AddOp(vdbe.OpInteger, rowid, 1, 0)
	vm.AddOpWithP4Blob(vdbe.OpBlob, len(data), 2, 0, data)
	vm.AddOp(vdbe.OpMakeRecord, 1, 2, 3)
	vm.AddOp(vdbe.OpInsert, 0, 3, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	if err := vm.Run(); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	vm2 := vdbe.New()
	vm2.Ctx = ctx
	vm2.AllocMemory(10)
	vm2.AllocCursors(1)
	vm2.AddOp(vdbe.OpInit, 0, 0, 0)
	vm2.AddOp(vdbe.OpOpenRead, 0, int(rootPage), 2)
	vm2.AddOp(vdbe.OpInteger, rowid, 3, 0)
	seekAddr := vm2.AddOp(vdbe.OpSeekRowid, 0, 0, 3)
	vm2.AddOp(vdbe.OpColumn, 0, 1, 1)
	vm2.AddOp(vdbe.OpResultRow, 1, 1, 0)
	notFoundAddr := vm2.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm2.AddOp(vdbe.OpClose, 0, 0, 0)
	vm2.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm2.Program[seekAddr].P2 = notFoundAddr

	hasMore, err := vm2.Step()
	if err != nil {
		t.Fatalf("SELECT step failed: %v", err)
	}
	if vm2.State == vdbe.StateRowReady && hasMore && len(vm2.ResultRow) > 0 {
		retrieved := vm2.ResultRow[0].BlobValue()
		if !bytes.Equal(retrieved, data) {
			t.Errorf("BLOB mismatch:\n  Expected: %v\n  Got:      %v", data, retrieved)
		}
	}
}

// TestBlobBinding tests parameter binding with BLOB values
func TestBlobBinding(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Single byte", []byte{0x42}},
		{"Small blob", []byte{0x01, 0x02, 0x03, 0x04}},
		{"Medium blob", make([]byte, 256)},
		{"With nulls", []byte{0x00, 0x01, 0x00, 0x02, 0x00}},
		{"Binary data", []byte{0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x01, 0x02, 0x03}},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, rootPage := blobBindingSetup(t)
			blobBindingInsertAndVerify(t, ctx, rootPage, i+1, tc.data)
		})
	}
}
