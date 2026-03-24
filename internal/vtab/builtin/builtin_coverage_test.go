// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package builtin

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// buildTestSchema constructs a *schema.Schema populated with one table,
// one index, one view, and one trigger for use in sqlite_master tests.
func buildTestSchema() *schema.Schema {
	sch := schema.NewSchema()

	sch.AddTableUnsafe(&schema.Table{
		Name:     "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
	})

	sch.AddIndexUnsafe(&schema.Index{
		Name:     "idx_users_name",
		Table:    "users",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_users_name ON users(name)",
	})

	sch.AddViewUnsafe(&schema.View{
		Name: "v_users",
		SQL:  "CREATE VIEW v_users AS SELECT * FROM users",
	})

	sch.AddTriggerUnsafe(&schema.Trigger{
		Name:  "trg_users_insert",
		Table: "users",
		SQL:   "CREATE TRIGGER trg_users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
	})

	return sch
}

// TestSQLiteMasterLoadSchemaRows_WithRealSchema covers the *schema.Schema
// branch inside loadSchemaRows (tables, indexes, views, triggers paths).
func TestSQLiteMasterLoadSchemaRows_WithRealSchema(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, err := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)

	// No filter: should return all four schema objects.
	if err := masterCursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	rowCount := 0
	for !masterCursor.EOF() {
		rowid, err := masterCursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}
		if rowid != int64(rowCount) {
			t.Errorf("unexpected rowid: want %d got %d", rowCount, rowid)
		}

		for col := 0; col <= 4; col++ {
			if _, err := masterCursor.Column(col); err != nil {
				t.Errorf("Column(%d) failed: %v", col, err)
			}
		}
		rowCount++
		if err := masterCursor.Next(); err != nil {
			t.Errorf("Next failed: %v", err)
		}
	}

	if rowCount != 4 {
		t.Errorf("expected 4 rows (table+index+view+trigger), got %d", rowCount)
	}
}

// TestSQLiteMasterLoadSchemaRows_TableFilter covers the type-filter path
// when db is a *schema.Schema. MC/DC case: typeFilter set, nameFilter empty.
func TestSQLiteMasterLoadSchemaRows_TableFilter(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, err := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)

	if err := masterCursor.Filter(0, "", []interface{}{"table"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if masterCursor.EOF() {
		t.Fatal("expected at least one table row")
	}

	typeVal, err := masterCursor.Column(0)
	if err != nil {
		t.Fatalf("Column(0) failed: %v", err)
	}
	if typeVal != "table" {
		t.Errorf("expected type='table', got %v", typeVal)
	}
}

// TestSQLiteMasterLoadSchemaRows_IndexFilter covers filtering to only indexes.
// MC/DC: typeFilter="index", nameFilter empty.
func TestSQLiteMasterLoadSchemaRows_IndexFilter(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", []interface{}{"index"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if masterCursor.EOF() {
		t.Fatal("expected at least one index row")
	}
	typeVal, _ := masterCursor.Column(0)
	if typeVal != "index" {
		t.Errorf("expected type='index', got %v", typeVal)
	}
}

// TestSQLiteMasterLoadSchemaRows_ViewFilter covers filtering to only views.
// MC/DC: typeFilter="view", nameFilter empty.
func TestSQLiteMasterLoadSchemaRows_ViewFilter(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", []interface{}{"view"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if masterCursor.EOF() {
		t.Fatal("expected at least one view row")
	}
	typeVal, _ := masterCursor.Column(0)
	if typeVal != "view" {
		t.Errorf("expected type='view', got %v", typeVal)
	}
}

// TestSQLiteMasterLoadSchemaRows_TriggerFilter covers filtering to only triggers.
// MC/DC: typeFilter="trigger", nameFilter empty.
func TestSQLiteMasterLoadSchemaRows_TriggerFilter(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", []interface{}{"trigger"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if masterCursor.EOF() {
		t.Fatal("expected at least one trigger row")
	}
	typeVal, _ := masterCursor.Column(0)
	if typeVal != "trigger" {
		t.Errorf("expected type='trigger', got %v", typeVal)
	}
}

// TestSQLiteMasterLoadSchemaRows_TypeAndNameFilter covers the nameFilter branch
// in assignFilterValue (second string arg). MC/DC: both typeFilter and nameFilter set.
func TestSQLiteMasterLoadSchemaRows_TypeAndNameFilter(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", []interface{}{"table", "users"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if masterCursor.EOF() {
		t.Fatal("expected to find 'users' table")
	}

	nameVal, err := masterCursor.Column(1)
	if err != nil {
		t.Fatalf("Column(1) failed: %v", err)
	}
	if nameVal != "users" {
		t.Errorf("expected name='users', got %v", nameVal)
	}

	// Only one row should match.
	masterCursor.Next()
	if !masterCursor.EOF() {
		t.Error("expected exactly one matching row")
	}
}

// TestSQLiteMasterLoadSchemaRows_TypeAndNameFilter_NoMatch covers the no-match path
// when both typeFilter and nameFilter are set but nothing matches.
// MC/DC: typeFilter set, nameFilter set, no row matches both.
func TestSQLiteMasterLoadSchemaRows_TypeAndNameFilter_NoMatch(t *testing.T) {
	t.Parallel()
	sch := buildTestSchema()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(sch, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", []interface{}{"table", "nonexistent_table"}); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if !masterCursor.EOF() {
		t.Error("expected no rows for non-existent table name")
	}
}

// TestSQLiteMasterLoadSchemaRows_NilSchema covers the nil db (non-schema) fallback path.
func TestSQLiteMasterLoadSchemaRows_NilSchema(t *testing.T) {
	t.Parallel()

	module := NewSQLiteMasterModule()
	vtable, _, _ := module.Connect(nil, "sqlite_master", "main", "sqlite_master", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	if err := masterCursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have the fallback sqlite_master row.
	if masterCursor.EOF() {
		t.Fatal("expected fallback row when db is nil")
	}
}

// TestSQLiteMasterBestIndex_NonEQOp covers BestIndex with usable constraints
// that have a non-EQ operator (not picked up), ensuring the else branch for
// estimatedCost=100 is reachable separately from the with-constraint branch.
func TestSQLiteMasterBestIndex_NonEQOp(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}

	info := &vtab.IndexInfo{
		Constraints:     make([]vtab.IndexConstraint, 1),
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 1),
	}

	// Usable but with a GT operator (not EQ) — should not be selected.
	info.Constraints[0] = vtab.IndexConstraint{
		Column: 0,
		Op:     vtab.ConstraintGT,
		Usable: true,
	}

	if err := table.BestIndex(info); err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// No constraint selected => full scan cost.
	if info.EstimatedCost != 100.0 {
		t.Errorf("expected cost 100.0 (no usable EQ), got %f", info.EstimatedCost)
	}
	if info.ConstraintUsage[0].ArgvIndex != 0 {
		t.Errorf("expected ArgvIndex 0 for non-EQ constraint, got %d", info.ConstraintUsage[0].ArgvIndex)
	}
}

// TestPragmaFunctionModule_NoPrefix covers PragmaFunctionModule.Connect when the
// module name does NOT start with "pragma_", so pragmaType stays "table_info".
func TestPragmaFunctionModule_NoPrefix(t *testing.T) {
	t.Parallel()
	module := NewPragmaFunctionModule()

	vtable, schemaStr, err := module.Connect(nil, "mymodule", "main", "test", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if vtable == nil {
		t.Fatal("expected non-nil vtable")
	}
	if schemaStr == "" {
		t.Error("expected non-empty schema string")
	}

	// pragmaType should default to "table_info" (no "pragma_" prefix stripped).
	pragmaTable, ok := vtable.(*PragmaTable)
	if !ok {
		t.Fatal("expected *PragmaTable")
	}
	if pragmaTable.pragmaType != "table_info" {
		t.Errorf("expected pragmaType='table_info' for no-prefix module name, got %q", pragmaTable.pragmaType)
	}
}

// TestPragmaFunctionModuleCreate_NoPrefix covers Create (which delegates to Connect)
// with a module name lacking the "pragma_" prefix.
func TestPragmaFunctionModuleCreate_NoPrefix(t *testing.T) {
	t.Parallel()
	module := NewPragmaFunctionModule()

	vtable, schemaStr, err := module.Create(nil, "mymodule", "main", "test", nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if vtable == nil {
		t.Fatal("expected non-nil vtable")
	}
	if schemaStr == "" {
		t.Error("expected non-empty schema string")
	}
}

// TestAssignFilterValue_SecondArgSetsNameFilter directly exercises the nameFilter
// assignment branch: first string arg goes to typeFilter, second to nameFilter.
// MC/DC: typeFilter != "" AND nameFilter == "" => assign nameFilter.
func TestAssignFilterValue_SecondArgSetsNameFilter(t *testing.T) {
	t.Parallel()
	c := &SQLiteMasterCursor{}

	// First call: typeFilter is empty, so it gets assigned.
	c.assignFilterValue("table")
	if c.typeFilter != "table" {
		t.Errorf("expected typeFilter='table', got %q", c.typeFilter)
	}
	if c.nameFilter != "" {
		t.Errorf("expected nameFilter still empty, got %q", c.nameFilter)
	}

	// Second call: typeFilter already set, nameFilter empty => nameFilter gets assigned.
	c.assignFilterValue("users")
	if c.nameFilter != "users" {
		t.Errorf("expected nameFilter='users', got %q", c.nameFilter)
	}
}

// TestAssignFilterValue_NonString covers the early return when arg is not a string.
// MC/DC: !ok => return without setting any filter.
func TestAssignFilterValue_NonString(t *testing.T) {
	t.Parallel()
	c := &SQLiteMasterCursor{}
	c.assignFilterValue(42) // int, not string
	if c.typeFilter != "" {
		t.Errorf("expected typeFilter still empty after non-string arg, got %q", c.typeFilter)
	}
	if c.nameFilter != "" {
		t.Errorf("expected nameFilter still empty after non-string arg, got %q", c.nameFilter)
	}
}

// TestAssignFilterValue_BothAlreadySet covers the case where both typeFilter and
// nameFilter are already set; a third call should be a no-op.
// MC/DC: typeFilter != "" AND nameFilter != "" => neither branch taken.
func TestAssignFilterValue_BothAlreadySet(t *testing.T) {
	t.Parallel()
	c := &SQLiteMasterCursor{
		typeFilter: "table",
		nameFilter: "users",
	}
	c.assignFilterValue("extra")
	if c.typeFilter != "table" {
		t.Errorf("typeFilter should not change, got %q", c.typeFilter)
	}
	if c.nameFilter != "users" {
		t.Errorf("nameFilter should not change, got %q", c.nameFilter)
	}
}

// TestSQLiteMasterCursorColumnEOF_AfterClose verifies that Column returns an
// error after the cursor has been closed (rows set to nil, EOF=true).
func TestSQLiteMasterCursorColumnEOF_AfterClose(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, _ := table.Open()
	masterCursor := cursor.(*SQLiteMasterCursor)
	masterCursor.Filter(0, "", nil)

	cursor.Close() // sets rows to nil

	_, err := masterCursor.Column(0)
	if err == nil {
		t.Error("expected error calling Column after Close")
	}

	_, err = masterCursor.Rowid()
	if err == nil {
		t.Error("expected error calling Rowid after Close")
	}
}

// TestPragmaCursorColumnAllFields exercises every column index for
// both table_info and index_list cursors to ensure full column coverage.
func TestPragmaCursorColumnAllFields(t *testing.T) {
	t.Parallel()

	t.Run("table_info_all_columns", func(t *testing.T) {
		t.Parallel()
		module := NewPragmaTableInfoModule()
		vtable, _, _ := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
		cursor, _ := vtable.Open()
		defer cursor.Close()

		pragmaCursor := cursor.(*PragmaCursor)
		pragmaCursor.Filter(0, "", []interface{}{"some_table"})

		if pragmaCursor.EOF() {
			t.Skip("no rows to test")
		}

		// table_info has 6 columns: cid, name, type, notnull, dflt_value, pk
		for col := 0; col <= 5; col++ {
			if _, err := pragmaCursor.Column(col); err != nil {
				t.Errorf("Column(%d) failed: %v", col, err)
			}
		}
	})

	t.Run("index_list_all_columns", func(t *testing.T) {
		t.Parallel()
		module := NewPragmaIndexListModule()
		vtable, _, _ := module.Connect(nil, "pragma_index_list", "main", "pragma_index_list", nil)
		cursor, _ := vtable.Open()
		defer cursor.Close()

		pragmaCursor := cursor.(*PragmaCursor)
		pragmaCursor.Filter(0, "", []interface{}{"some_table"})

		if pragmaCursor.EOF() {
			t.Skip("no rows to test")
		}

		// index_list has 5 columns: seq, name, unique, origin, partial
		for col := 0; col <= 4; col++ {
			if _, err := pragmaCursor.Column(col); err != nil {
				t.Errorf("Column(%d) failed: %v", col, err)
			}
		}
	})
}
