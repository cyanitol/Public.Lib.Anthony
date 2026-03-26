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

func TestFKAdapter5ValuesEqual(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{}

	t.Run("NullMem_NilValue_True", func(t *testing.T) {
		t.Parallel()
		mem := NewMemNull()
		if !r.valuesEqual(mem, nil) {
			t.Error("expected valuesEqual(NULL, nil) = true")
		}
	})

	t.Run("NullMem_NonNilValue_False", func(t *testing.T) {
		t.Parallel()
		mem := NewMemNull()
		if r.valuesEqual(mem, int64(1)) {
			t.Error("expected valuesEqual(NULL, 1) = false")
		}
	})

	t.Run("NullMem_StringValue_False", func(t *testing.T) {
		t.Parallel()
		mem := NewMemNull()
		if r.valuesEqual(mem, "hello") {
			t.Error("expected valuesEqual(NULL, \"hello\") = false")
		}
	})

	t.Run("IntMem_MatchingInt64_True", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(42)
		if !r.valuesEqual(mem, int64(42)) {
			t.Error("expected valuesEqual(int64(42), int64(42)) = true")
		}
	})

	t.Run("IntMem_NonMatchingInt64_False", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(42)
		if r.valuesEqual(mem, int64(99)) {
			t.Error("expected valuesEqual(int64(42), int64(99)) = false")
		}
	})

	t.Run("IntMem_MatchingInt_True", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(7)
		if !r.valuesEqual(mem, int(7)) {
			t.Error("expected valuesEqual(int64(7), int(7)) = true")
		}
	})

	t.Run("StrMem_MatchingString_True", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("world")
		if !r.valuesEqual(mem, "world") {
			t.Error("expected valuesEqual(str(world), \"world\") = true")
		}
	})

	t.Run("StrMem_NonMatchingString_False", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("world")
		if r.valuesEqual(mem, "other") {
			t.Error("expected valuesEqual(str(world), \"other\") = false")
		}
	})

	t.Run("IntMem_StringValue_False", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(5)
		if r.valuesEqual(mem, "5") {
			t.Error("expected valuesEqual(int, string) = false (type mismatch)")
		}
	})
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
			"regular":       regularTable,
			"withoutrowid":  withoutRowIDTable,
		},
	}
}

func TestFKAdapter5ReadRowByRowid(t *testing.T) {
	t.Parallel()

	t.Run("NilContext_ReturnsError", func(t *testing.T) {
		t.Parallel()
		r := &VDBERowReader{vdbe: nil}
		_, err := r.ReadRowByRowid("any", 1)
		if err == nil {
			t.Error("expected error for nil vdbe context")
		}
	})

	t.Run("TableNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		schema := &fkaCovSchema{tables: map[string]interface{}{}}
		r := newReaderWithSchema(schema)
		_, err := r.ReadRowByRowid("missing_table", 1)
		if err == nil {
			t.Error("expected error for missing table")
		}
	})

	t.Run("WithoutRowID_ReturnsError", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		withoutRowIDPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		schema := fk5MakeSchema(1, withoutRowIDPage)
		v := New()
		v.Ctx = &VDBEContext{
			Btree:  bt,
			Schema: schema,
		}
		r := &VDBERowReader{vdbe: v}
		_, err = r.ReadRowByRowid("withoutrowid", 1)
		if err == nil {
			t.Error("expected error for WITHOUT ROWID table")
		}
	})

	t.Run("RowidNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		root, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		schema := &fkaCovSchema{
			tables: map[string]interface{}{
				"items": &fkaCovMockTable{
					RootPage:     root,
					WithoutRowID: false,
					columns: []interface{}{
						&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
						&fkaCovMockColumn{name: "name", colType: "TEXT"},
					},
				},
			},
		}
		v := New()
		v.Ctx = &VDBEContext{
			Btree:  bt,
			Schema: schema,
		}
		r := &VDBERowReader{vdbe: v}
		// Table is empty; rowid 99 does not exist.
		_, err = r.ReadRowByRowid("items", 99)
		if err == nil {
			t.Error("expected error for rowid not found in empty table")
		}
	})

	t.Run("HappyPath_ReturnsRowValues", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		root, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}

		// Build a text payload for the "name" column (id is the rowid alias).
		// textRecord encodes one TEXT column. Use the same encoding as exec_rowaccess tests.
		nameVal := "alice"
		nameBytes := []byte(nameVal)
		serialType := uint64(len(nameBytes)*2 + 13)
		// Header: [headerSize, serialType]. Both fit in 1 byte here.
		headerSize := byte(2) // 1 byte for size varint + 1 byte for serial type
		payload := append([]byte{headerSize, byte(serialType)}, nameBytes...)

		cur := btree.NewCursor(bt, root)
		if err := cur.Insert(5, payload); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		schema := &fkaCovSchema{
			tables: map[string]interface{}{
				"people": &fkaCovMockTable{
					RootPage:     root,
					WithoutRowID: false,
					columns: []interface{}{
						&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
						&fkaCovMockColumn{name: "name", colType: "TEXT"},
					},
				},
			},
		}
		v := New()
		v.Ctx = &VDBEContext{
			Btree:  bt,
			Schema: schema,
		}
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
	})
}
