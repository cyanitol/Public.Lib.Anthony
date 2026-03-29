// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"errors"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// Local mock types for cascadeDelete tests.
// The existing mocks in foreign_key_test.go and constraint_coverage_test.go
// are declared in the same package, so we build distinct types here to avoid
// name collisions.
// ---------------------------------------------------------------------------

// testRowReader is a minimal RowReader that can be made to return an error
// from ReadRowByRowid (to exercise the "row already deleted" continue path).
type testRowReader struct {
	// rowsByID maps rowid -> values; if not present ReadRowByRowid returns an error.
	rowsByID map[int64]map[string]interface{}
	// returnErrorForTable, when non-empty, makes every ReadRowByRowid for that
	// table return an error (simulates a row that was already cascade-deleted).
	returnErrorForTable string
}

func newTestRowReader() *testRowReader {
	return &testRowReader{rowsByID: make(map[int64]map[string]interface{})}
}

func (r *testRowReader) RowExists(table string, columns []string, values []interface{}) (bool, error) {
	return false, nil
}

func (r *testRowReader) RowExistsWithCollation(table string, columns []string, values []interface{}, collations []string) (bool, error) {
	return false, nil
}

func (r *testRowReader) FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error) {
	return []int64{}, nil
}

func (r *testRowReader) ReadRowByRowid(table string, rowid int64) (map[string]interface{}, error) {
	if r.returnErrorForTable != "" && table == r.returnErrorForTable {
		return nil, errors.New("row not found: already deleted")
	}
	if vals, ok := r.rowsByID[rowid]; ok {
		return vals, nil
	}
	return make(map[string]interface{}), nil
}

// testRowDeleter records which rowids were deleted via DeleteRow.
type testRowDeleter struct {
	deletedRowIDs []int64
}

func newTestRowDeleter() *testRowDeleter {
	return &testRowDeleter{}
}

func (d *testRowDeleter) DeleteRow(table string, rowid int64) error {
	d.deletedRowIDs = append(d.deletedRowIDs, rowid)
	return nil
}

// testRowDeleterExtended also implements RowDeleterExtended (adds DeleteRowByKey).
type testRowDeleterExtended struct {
	testRowDeleter
	deletedKeys [][]interface{}
}

func newTestRowDeleterExtended() *testRowDeleterExtended {
	return &testRowDeleterExtended{}
}

func (d *testRowDeleterExtended) DeleteRowByKey(table string, keyValues []interface{}) error {
	d.deletedKeys = append(d.deletedKeys, keyValues)
	return nil
}

// testRowUpdater is a no-op RowUpdater.
type testRowUpdater struct{}

func (u *testRowUpdater) UpdateRow(table string, rowid int64, values map[string]interface{}) error {
	return nil
}

// ---------------------------------------------------------------------------
// TestMCDC6_CascadeDelete_TableNotFound
//
// Branch: `!ok` — the target table is absent from the schema.
// cascadeDelete must return a "table not found" error.
// ---------------------------------------------------------------------------

func TestMCDC6_CascadeDelete_TableNotFound(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	sch := schema.NewSchema()
	// "ghost_table" is deliberately not added to the schema.

	err := m.cascadeDelete(
		"ghost_table",
		[]int64{1},
		sch,
		newTestRowDeleter(),
		&testRowUpdater{},
		newTestRowReader(),
	)
	if err == nil {
		t.Fatal("cascadeDelete: expected error for missing table, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestMCDC6_CascadeDelete_RowAlreadyDeleted
//
// Branch: rowReader.ReadRowByRowid returns an error (continue path).
// The function must skip the row and return nil rather than propagating the
// read error — this simulates a row that was already removed by a prior cascade.
// ---------------------------------------------------------------------------

func TestMCDC6_CascadeDelete_RowAlreadyDeleted(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Tables["parent"] = &schema.Table{
		Name:       "parent",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}

	reader := newTestRowReader()
	reader.returnErrorForTable = "parent" // all ReadRowByRowid calls on "parent" error out

	deleter := newTestRowDeleter()

	err := m.cascadeDelete(
		"parent",
		[]int64{42, 43},
		sch,
		deleter,
		&testRowUpdater{},
		reader,
	)
	if err != nil {
		t.Fatalf("cascadeDelete with already-deleted rows: expected nil, got %v", err)
	}
	// No rows should have been deleted because ReadRowByRowid always erred.
	if len(deleter.deletedRowIDs) != 0 {
		t.Errorf("expected no DeleteRow calls, got %d", len(deleter.deletedRowIDs))
	}
}

// ---------------------------------------------------------------------------
// TestMCDC6_CascadeDelete_WithoutRowidPath
//
// Branch: `tableObj.WithoutRowID && hasDeleterExt` — true for both conditions.
// DeleteRowByKey must be called instead of DeleteRow.
// ---------------------------------------------------------------------------

func TestMCDC6_CascadeDelete_WithoutRowidPath(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Tables["item"] = &schema.Table{
		Name:         "item",
		PrimaryKey:   []string{"code"},
		WithoutRowID: true,
		Columns: []*schema.Column{
			{Name: "code", Type: "TEXT", PrimaryKey: true},
		},
	}

	reader := newTestRowReader()
	// Provide actual row data so ReadRowByRowid succeeds and the delete is reached.
	reader.rowsByID[1] = map[string]interface{}{"code": "ABC"}

	deleterExt := newTestRowDeleterExtended()

	err := m.cascadeDelete(
		"item",
		[]int64{1},
		sch,
		deleterExt,
		&testRowUpdater{},
		reader,
	)
	if err != nil {
		t.Fatalf("cascadeDelete WITHOUT ROWID path: unexpected error: %v", err)
	}
	if len(deleterExt.deletedKeys) != 1 {
		t.Errorf("expected 1 DeleteRowByKey call, got %d", len(deleterExt.deletedKeys))
	}
	if len(deleterExt.deletedRowIDs) != 0 {
		t.Errorf("expected 0 DeleteRow calls on WITHOUT ROWID path, got %d", len(deleterExt.deletedRowIDs))
	}
}

// ---------------------------------------------------------------------------
// TestMCDC6_CascadeDelete_RegularTable
//
// Branch: regular table (WithoutRowID==false OR !hasDeleterExt).
// DeleteRow must be called.
// ---------------------------------------------------------------------------

func TestMCDC6_CascadeDelete_RegularTable(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Tables["order"] = &schema.Table{
		Name:         "order",
		PrimaryKey:   []string{"id"},
		WithoutRowID: false, // regular rowid table
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}

	reader := newTestRowReader()
	reader.rowsByID[10] = map[string]interface{}{"id": int64(10)}
	reader.rowsByID[11] = map[string]interface{}{"id": int64(11)}

	deleter := newTestRowDeleter() // does NOT implement RowDeleterExtended

	err := m.cascadeDelete(
		"order",
		[]int64{10, 11},
		sch,
		deleter,
		&testRowUpdater{},
		reader,
	)
	if err != nil {
		t.Fatalf("cascadeDelete regular table: unexpected error: %v", err)
	}
	if len(deleter.deletedRowIDs) != 2 {
		t.Errorf("expected 2 DeleteRow calls, got %d", len(deleter.deletedRowIDs))
	}
}
