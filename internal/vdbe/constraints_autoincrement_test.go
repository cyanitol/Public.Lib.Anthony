// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// mockSchemaForAutoincrement implements schemaWithGetTableByName.
type mockSchemaForAutoincrement struct {
	table interface{}
}

func (m *mockSchemaForAutoincrement) GetTableByName(name string) (interface{}, bool) {
	if m.table != nil {
		return m.table, true
	}
	return nil, false
}

// mockTableWithAutoincrement implements autoincrementColumnChecker.
type mockTableWithAutoincrement struct {
	idx int
}

func (m *mockTableWithAutoincrement) GetAutoincrementColumnIndex() int {
	return m.idx
}

// mockTableNoAutoincrement does not implement autoincrementColumnChecker.
type mockTableNoAutoincrement struct{}

// TestIsAutoincrementTable covers all branches of isAutoincrementTable.
func autoincExpectFalse(t *testing.T, ctx *VDBEContext, table, msg string) {
	t.Helper()
	v := New()
	v.Ctx = ctx
	if v.isAutoincrementTable(table) {
		t.Error(msg)
	}
}

func TestIsAutoincrementTable(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		autoincExpectFalse(t, nil, "t", "expected false for nil ctx")
	})

	t.Run("NilSchema", func(t *testing.T) {
		autoincExpectFalse(t, &VDBEContext{Schema: nil}, "t", "expected false for nil schema")
	})

	t.Run("SchemaNotTableGetter", func(t *testing.T) {
		autoincExpectFalse(t, &VDBEContext{Schema: struct{}{}}, "t", "expected false when schema does not implement GetTableByName")
	})

	t.Run("TableNotFound", func(t *testing.T) {
		autoincExpectFalse(t, &VDBEContext{Schema: &mockSchemaForAutoincrement{table: nil}}, "missing", "expected false when table not found")
	})

	t.Run("TableNotAutoincrementChecker", func(t *testing.T) {
		autoincExpectFalse(t, &VDBEContext{Schema: &mockSchemaForAutoincrement{
			table: &mockTableNoAutoincrement{},
		}}, "t", "expected false when table does not implement GetAutoincrementColumnIndex")
	})

	t.Run("AutoincrementIndexNegative", func(t *testing.T) {
		autoincExpectFalse(t, &VDBEContext{Schema: &mockSchemaForAutoincrement{
			table: &mockTableWithAutoincrement{idx: -1},
		}}, "t", "expected false when autoincrement index is -1")
	})

	t.Run("AutoincrementIndexZero", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &mockSchemaForAutoincrement{
			table: &mockTableWithAutoincrement{idx: 0},
		}}
		if !v.isAutoincrementTable("t") {
			t.Error("expected true when autoincrement index is 0")
		}
	})

	t.Run("AutoincrementIndexPositive", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &mockSchemaForAutoincrement{
			table: &mockTableWithAutoincrement{idx: 2},
		}}
		if !v.isAutoincrementTable("t") {
			t.Error("expected true when autoincrement index is 2")
		}
	})
}
