package driver

import (
	"bytes"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

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
					} else if mem.IsBlob() {
						blob := mem.BlobValue()
						t.Logf("  Column %d: %v (blob, %d bytes)", j, blob, len(blob))
					} else if mem.IsStr() {
						t.Logf("  Column %d: %q (string)", j, mem.StrValue())
					} else {
						t.Logf("  Column %d: type=%v", j, mem)
					}
				}

				// Verify values
				if len(vm.ResultRow) >= 2 {
					if vm.ResultRow[0].IntValue() != 42 {
						t.Errorf("Expected id=42, got %d", vm.ResultRow[0].IntValue())
					}
					if !vm.ResultRow[1].IsBlob() {
						t.Errorf("Expected column 1 to be BLOB, got %v", vm.ResultRow[1])
					} else {
						retrievedBlob := vm.ResultRow[1].BlobValue()
						if !bytes.Equal(retrievedBlob, testBlob) {
							t.Errorf("BLOB mismatch:\n  Expected: %v\n  Got:      %v",
								testBlob, retrievedBlob)
						} else {
							t.Logf("BLOB retrieved correctly!")
						}
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

// TestBlobBinding tests parameter binding with BLOB values
func TestBlobBinding(t *testing.T) {
	// Test with different BLOB sizes
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
			// Create a fresh btree for each test case
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

			ctx := &vdbe.VDBEContext{
				Btree:  bt,
				Schema: sch,
			}

			// INSERT
			vm := vdbe.New()
			vm.Ctx = ctx
			vm.AllocMemory(10)
			vm.AllocCursors(1)

			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpOpenWrite, 0, int(rootPage), 2)
			vm.AddOp(vdbe.OpInteger, i+1, 0, 0)                          // rowid
			vm.AddOp(vdbe.OpInteger, i+1, 1, 0)                          // id
			vm.AddOpWithP4Blob(vdbe.OpBlob, len(tc.data), 2, 0, tc.data) // content
			vm.AddOp(vdbe.OpMakeRecord, 1, 2, 3)
			vm.AddOp(vdbe.OpInsert, 0, 3, 0)
			vm.AddOp(vdbe.OpClose, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)

			if err := vm.Run(); err != nil {
				t.Fatalf("INSERT failed: %v", err)
			}

			// SELECT and verify
			vm2 := vdbe.New()
			vm2.Ctx = ctx
			vm2.AllocMemory(10)
			vm2.AllocCursors(1)

			vm2.AddOp(vdbe.OpInit, 0, 0, 0)
			vm2.AddOp(vdbe.OpOpenRead, 0, int(rootPage), 2)
			vm2.AddOp(vdbe.OpInteger, i+1, 3, 0)             // rowid to seek
			seekAddr := vm2.AddOp(vdbe.OpSeekRowid, 0, 0, 3) // seek to rowid in register 3
			vm2.AddOp(vdbe.OpColumn, 0, 1, 1)                // content into reg 1
			vm2.AddOp(vdbe.OpResultRow, 1, 1, 0)
			notFoundAddr := vm2.AddOp(vdbe.OpHalt, 0, 0, 0)
			vm2.AddOp(vdbe.OpClose, 0, 0, 0)
			vm2.AddOp(vdbe.OpHalt, 0, 0, 0)
			vm2.Program[seekAddr].P2 = notFoundAddr

			hasMore, err := vm2.Step()
			if err != nil {
				t.Fatalf("SELECT step failed: %v", err)
			}

			if vm2.State == vdbe.StateRowReady && hasMore {
				if len(vm2.ResultRow) > 0 {
					retrieved := vm2.ResultRow[0].BlobValue()
					if !bytes.Equal(retrieved, tc.data) {
						t.Errorf("BLOB mismatch for %s:\n  Expected: %v\n  Got:      %v",
							tc.name, tc.data, retrieved)
					}
				}
			}
		})
	}
}
