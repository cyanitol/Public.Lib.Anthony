// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// ---------------------------------------------------------------------------
// valuesEqual — method on *VDBERowReader
// Lines covered: 938-942 in fk_adapter.go
// ---------------------------------------------------------------------------

func TestFKAdapter5ValuesEqual_NullCases(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqual(NewMemNull(), nil) {
		t.Error("expected valuesEqual(NULL, nil) = true")
	}
	if r.valuesEqual(NewMemNull(), int64(1)) {
		t.Error("expected valuesEqual(NULL, 1) = false")
	}
	if r.valuesEqual(NewMemNull(), "hello") {
		t.Error("expected valuesEqual(NULL, string) = false")
	}
}

func TestFKAdapter5ValuesEqual_IntCases(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqual(NewMemInt(42), int64(42)) {
		t.Error("expected valuesEqual(int64(42), int64(42)) = true")
	}
	if r.valuesEqual(NewMemInt(42), int64(99)) {
		t.Error("expected valuesEqual(int64(42), int64(99)) = false")
	}
	if !r.valuesEqual(NewMemInt(7), int(7)) {
		t.Error("expected valuesEqual(int64(7), int(7)) = true")
	}
}

func TestFKAdapter5ValuesEqual_StringAndMismatch(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqual(NewMemStr("world"), "world") {
		t.Error("expected valuesEqual(str(world), world) = true")
	}
	if r.valuesEqual(NewMemStr("world"), "other") {
		t.Error("expected valuesEqual(str(world), other) = false")
	}
	if r.valuesEqual(NewMemInt(5), "5") {
		t.Error("expected valuesEqual(int, string) = false (type mismatch)")
	}
}

// ---------------------------------------------------------------------------
// ReadRowByRowid — improve coverage from 64.7%
// Lines covered: 163-196 in fk_adapter.go
// ---------------------------------------------------------------------------

// fk5MakeSchema builds a fkaCovSchema with one regular table and one WITHOUT ROWID table.
func fk5MakeSchema(regularPage, withoutRowIDPage uint32) *fkaCovSchema {
	regularTable := &fkaCovMockTable{
		RootPage:     regularPage,
		WithoutRowID: false,
		columns: []interface{}{
			&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
			&fkaCovMockColumn{name: "val", colType: "TEXT"},
		},
	}
	withoutRowIDTable := &fkaCovMockTable{
		RootPage:     withoutRowIDPage,
		WithoutRowID: true,
		columns: []interface{}{
			&fkaCovMockColumn{name: "pk", colType: "TEXT", isPK: true},
		},
	}
	return &fkaCovSchema{
		tables: map[string]interface{}{
			"regular":      regularTable,
			"withoutrowid": withoutRowIDTable,
		},
	}
}

func TestFKAdapter5ReadRowByRowid_Errors(t *testing.T) {
	t.Parallel()

	// NilContext
	r := &VDBERowReader{vdbe: nil}
	if _, err := r.ReadRowByRowid("any", 1); err == nil {
		t.Error("expected error for nil vdbe context")
	}

	// TableNotFound
	schema := &fkaCovSchema{tables: map[string]interface{}{}}
	r = newReaderWithSchema(schema)
	if _, err := r.ReadRowByRowid("missing_table", 1); err == nil {
		t.Error("expected error for missing table")
	}

	// WithoutRowID
	bt := btree.NewBtree(4096)
	withoutRowIDPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	schema2 := fk5MakeSchema(1, withoutRowIDPage)
	v := New()
	v.Ctx = &VDBEContext{Btree: bt, Schema: schema2}
	r = &VDBERowReader{vdbe: v}
	if _, err := r.ReadRowByRowid("withoutrowid", 1); err == nil {
		t.Error("expected error for WITHOUT ROWID table")
	}
}

func TestFKAdapter5ReadRowByRowid_RowNotFound(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	schema := &fkaCovSchema{
		tables: map[string]interface{}{
			"items": &fkaCovMockTable{
				RootPage: root, WithoutRowID: false,
				columns: []interface{}{
					&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
					&fkaCovMockColumn{name: "name", colType: "TEXT"},
				},
			},
		},
	}
	v := New()
	v.Ctx = &VDBEContext{Btree: bt, Schema: schema}
	r := &VDBERowReader{vdbe: v}
	if _, err := r.ReadRowByRowid("items", 99); err == nil {
		t.Error("expected error for rowid not found in empty table")
	}
}

func TestFKAdapter5ReadRowByRowid_HappyPath(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	nameBytes := []byte("alice")
	serialType := uint64(len(nameBytes)*2 + 13)
	payload := append([]byte{byte(2), byte(serialType)}, nameBytes...)
	cur := btree.NewCursor(bt, root)
	if err := cur.Insert(5, payload); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	schema := &fkaCovSchema{
		tables: map[string]interface{}{
			"people": &fkaCovMockTable{
				RootPage: root, WithoutRowID: false,
				columns: []interface{}{
					&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
					&fkaCovMockColumn{name: "name", colType: "TEXT"},
				},
			},
		},
	}
	v := New()
	v.Ctx = &VDBEContext{Btree: bt, Schema: schema}
	r := &VDBERowReader{vdbe: v}
	row, err := r.ReadRowByRowid("people", 5)
	if err != nil {
		t.Fatalf("ReadRowByRowid(5): %v", err)
	}
	if row["id"] != int64(5) {
		t.Errorf("expected id=5, got %v", row["id"])
	}
	if row["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", row["name"])
	}
}
