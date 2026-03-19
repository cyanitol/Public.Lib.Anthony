// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package builtin

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// TestSQLiteMasterModule tests the sqlite_master virtual table module.
func TestSQLiteMasterModule(t *testing.T) {
	t.Parallel()
	module := NewSQLiteMasterModule()

	// Test Connect
	vtable, schema, err := module.Connect(nil, "sqlite_master", "main", "sqlite_master", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	if vtable == nil {
		t.Fatal("Expected non-nil virtual table")
	}

	// Test Create (should be same as Connect for sqlite_master)
	vtable2, schema2, err := module.Create(nil, "sqlite_master", "main", "sqlite_master", nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if schema2 != schema {
		t.Error("Create and Connect should return same schema")
	}

	if vtable2 == nil {
		t.Fatal("Expected non-nil virtual table from Create")
	}
}

// TestSQLiteMasterTable tests the sqlite_master virtual table.
func TestSQLiteMasterTable(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}

	// Test BestIndex
	info := &vtab.IndexInfo{
		Constraints:     make([]vtab.IndexConstraint, 2),
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 2),
	}

	// Add usable constraints
	info.Constraints[0] = vtab.IndexConstraint{
		Column: 0,
		Op:     vtab.ConstraintEQ,
		Usable: true,
	}
	info.Constraints[1] = vtab.IndexConstraint{
		Column: 1,
		Op:     vtab.ConstraintEQ,
		Usable: true,
	}

	err := table.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// Check that constraints were used
	if info.ConstraintUsage[0].ArgvIndex != 1 {
		t.Errorf("Expected constraint 0 to be used with ArgvIndex 1, got %d", info.ConstraintUsage[0].ArgvIndex)
	}
	if info.ConstraintUsage[1].ArgvIndex != 2 {
		t.Errorf("Expected constraint 1 to be used with ArgvIndex 2, got %d", info.ConstraintUsage[1].ArgvIndex)
	}

	// Test Open
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	if cursor == nil {
		t.Fatal("Expected non-nil cursor")
	}
}

// Prefix: smCur_
func smCur_openCursor(t *testing.T) *SQLiteMasterCursor {
	t.Helper()
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	masterCursor, ok := cursor.(*SQLiteMasterCursor)
	if !ok {
		t.Fatal("Expected cursor to be *SQLiteMasterCursor")
	}
	return masterCursor
}

func smCur_testFilter(t *testing.T, cursor *SQLiteMasterCursor) {
	t.Helper()
	err := cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	if cursor.EOF() {
		t.Error("Expected at least one row (sqlite_master)")
	}
}

func smCur_checkColumn(t *testing.T, cursor *SQLiteMasterCursor, colNum int, wantVal interface{}) {
	t.Helper()
	val, err := cursor.Column(colNum)
	if err != nil {
		t.Errorf("Column(%d) failed: %v", colNum, err)
	}
	if val != wantVal {
		t.Errorf("Column(%d): expected %v, got %v", colNum, wantVal, val)
	}
}

func smCur_testColumns(t *testing.T, cursor *SQLiteMasterCursor) {
	t.Helper()
	if cursor.EOF() {
		return
	}
	smCur_checkColumn(t, cursor, 0, "table")
	smCur_checkColumn(t, cursor, 1, "sqlite_master")
	smCur_checkColumn(t, cursor, 2, "sqlite_master")
	smCur_checkColumn(t, cursor, 3, int64(1))

	sqlVal, err := cursor.Column(4)
	if err != nil {
		t.Errorf("Column(4) failed: %v", err)
	}
	if sqlVal == "" {
		t.Error("Expected non-empty SQL")
	}
}

func smCur_testRowid(t *testing.T, cursor *SQLiteMasterCursor) {
	t.Helper()
	if cursor.EOF() {
		return
	}
	rowid, err := cursor.Rowid()
	if err != nil {
		t.Errorf("Rowid failed: %v", err)
	}
	if rowid != 0 {
		t.Errorf("Expected rowid=0, got %d", rowid)
	}
}

func smCur_testNext(t *testing.T, cursor *SQLiteMasterCursor) {
	t.Helper()
	err := cursor.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if !cursor.EOF() {
		t.Error("Expected EOF after one row")
	}
}

// TestSQLiteMasterCursor tests the sqlite_master cursor.
func TestSQLiteMasterCursor(t *testing.T) {
	t.Parallel()
	cursor := smCur_openCursor(t)
	defer cursor.Close()

	smCur_testFilter(t, cursor)
	smCur_testColumns(t, cursor)
	smCur_testRowid(t, cursor)
	smCur_testNext(t, cursor)
}

// TestSQLiteMasterCursorWithFilters tests filtering in sqlite_master.
func TestSQLiteMasterCursorWithFilters(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)

	// Test filtering by type
	err = masterCursor.Filter(0, "", []interface{}{"table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have the sqlite_master table
	if masterCursor.EOF() {
		t.Error("Expected at least one table row")
	}

	// Test filtering by both type and name
	masterCursor2, _ := table.Open()
	defer masterCursor2.Close()
	masterCursor2.(*SQLiteMasterCursor).Filter(0, "", []interface{}{"table", "sqlite_master"})

	if masterCursor2.(*SQLiteMasterCursor).EOF() {
		t.Error("Expected to find sqlite_master table")
	}

	// Test filtering with non-matching name
	masterCursor3, _ := table.Open()
	defer masterCursor3.Close()
	masterCursor3.(*SQLiteMasterCursor).Filter(0, "", []interface{}{"table", "nonexistent"})

	if !masterCursor3.(*SQLiteMasterCursor).EOF() {
		t.Error("Expected no results for non-existent table")
	}
}

// TestPragmaTableInfoModule tests the pragma_table_info module.
func TestPragmaTableInfoModule(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()

	// Test Connect
	vtable, schema, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	if vtable == nil {
		t.Fatal("Expected non-nil virtual table")
	}

	// Test that it's a PragmaTable
	pragmaTable, ok := vtable.(*PragmaTable)
	if !ok {
		t.Fatal("Expected vtable to be *PragmaTable")
	}

	if pragmaTable.pragmaType != "table_info" {
		t.Errorf("Expected pragmaType='table_info', got %s", pragmaTable.pragmaType)
	}
}

// TestPragmaIndexListModule tests the pragma_index_list module.
func TestPragmaIndexListModule(t *testing.T) {
	t.Parallel()
	module := NewPragmaIndexListModule()

	// Test Connect
	vtable, schema, err := module.Connect(nil, "pragma_index_list", "main", "pragma_index_list", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	if vtable == nil {
		t.Fatal("Expected non-nil virtual table")
	}

	// Test that it's a PragmaTable
	pragmaTable, ok := vtable.(*PragmaTable)
	if !ok {
		t.Fatal("Expected vtable to be *PragmaTable")
	}

	if pragmaTable.pragmaType != "index_list" {
		t.Errorf("Expected pragmaType='index_list', got %s", pragmaTable.pragmaType)
	}
}

// TestPragmaTableInfoCursor tests the pragma_table_info cursor.
func TestPragmaTableInfoCursor(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor, ok := cursor.(*PragmaCursor)
	if !ok {
		t.Fatal("Expected cursor to be *PragmaCursor")
	}

	// Test Filter
	err = pragmaCursor.Filter(0, "", []interface{}{"example_table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have some rows for the example table
	if pragmaCursor.EOF() {
		t.Error("Expected some rows for table_info")
	}

	// Test iterating through rows
	count := 0
	for !pragmaCursor.EOF() {
		// Test Column access
		cid, err := pragmaCursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		t.Logf("Column %d: cid=%v", count, cid)

		name, err := pragmaCursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}
		t.Logf("Column %d: name=%v", count, name)

		count++
		err = pragmaCursor.Next()
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
	}

	if count == 0 {
		t.Error("Expected at least one column")
	}
}

// TestPragmaIndexListCursor tests the pragma_index_list cursor.
func TestPragmaIndexListCursor(t *testing.T) {
	t.Parallel()
	module := NewPragmaIndexListModule()
	vtable, _, err := module.Connect(nil, "pragma_index_list", "main", "pragma_index_list", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)

	// Test Filter
	err = pragmaCursor.Filter(0, "", []interface{}{"example_table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Test that we get at least one index
	if pragmaCursor.EOF() {
		t.Error("Expected at least one index for example_table")
	}

	// Test Column access
	if !pragmaCursor.EOF() {
		seq, err := pragmaCursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		if seq == nil {
			t.Error("Expected non-nil seq")
		}

		name, err := pragmaCursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}
		if name == nil {
			t.Error("Expected non-nil name")
		}
	}
}

// TestPragmaTableCreate tests the Create method for pragma tables.
func TestPragmaTableCreate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		module     *PragmaModule
		wantSchema string
	}{
		{
			name:       "table_info",
			module:     NewPragmaTableInfoModule(),
			wantSchema: "pragma_table_info",
		},
		{
			name:       "index_list",
			module:     NewPragmaIndexListModule(),
			wantSchema: "pragma_index_list",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vtable, schema, err := tt.module.Create(nil, "pragma_"+tt.name, "main", tt.wantSchema, nil)
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}
			if vtable == nil {
				t.Error("Expected non-nil vtable")
			}
			if schema == "" {
				t.Error("Expected non-empty schema")
			}
		})
	}
}

// TestPragmaTableBestIndex tests BestIndex for pragma tables.
func TestPragmaTableBestIndex(t *testing.T) {
	t.Parallel()
	table := &PragmaTable{
		db:         nil,
		pragmaType: "table_info",
	}

	info := &vtab.IndexInfo{
		Constraints:     make([]vtab.IndexConstraint, 0),
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 0),
	}

	err := table.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	if info.EstimatedCost != 10.0 {
		t.Errorf("Expected cost 10.0, got %f", info.EstimatedCost)
	}
	if info.EstimatedRows != 10 {
		t.Errorf("Expected rows 10, got %d", info.EstimatedRows)
	}
}

// TestPragmaCursorRowid tests the Rowid method.
func TestPragmaCursorRowid(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)
	err = pragmaCursor.Filter(0, "", []interface{}{"example_table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if !pragmaCursor.EOF() {
		rowid, err := pragmaCursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}
		if rowid != 0 {
			t.Errorf("Expected rowid 0, got %d", rowid)
		}
	}

	// Test Rowid at EOF
	for !pragmaCursor.EOF() {
		pragmaCursor.Next()
	}
	_, err = pragmaCursor.Rowid()
	if err == nil {
		t.Error("Expected error when calling Rowid at EOF")
	}
}

// TestPragmaCursorColumnErrors tests error cases for Column method.
func TestPragmaCursorColumnErrors(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)
	err = pragmaCursor.Filter(0, "", []interface{}{"example_table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Test column out of range
	if !pragmaCursor.EOF() {
		_, err = pragmaCursor.Column(99)
		if err == nil {
			t.Error("Expected error for out of range column")
		}

		_, err = pragmaCursor.Column(-1)
		if err == nil {
			t.Error("Expected error for negative column index")
		}
	}

	// Test column at EOF
	for !pragmaCursor.EOF() {
		pragmaCursor.Next()
	}
	_, err = pragmaCursor.Column(0)
	if err == nil {
		t.Error("Expected error when calling Column at EOF")
	}
}

// TestPragmaFunctionModule tests the pragma function module.
func TestPragmaFunctionModule(t *testing.T) {
	t.Parallel()
	module := NewPragmaFunctionModule()

	tests := []struct {
		name       string
		moduleName string
		wantType   string
	}{
		{
			name:       "table_info",
			moduleName: "pragma_table_info",
			wantType:   "table_info",
		},
		{
			name:       "index_list",
			moduleName: "pragma_index_list",
			wantType:   "index_list",
		},
		{
			name:       "foreign_key_list",
			moduleName: "pragma_foreign_key_list",
			wantType:   "foreign_key_list",
		},
		{
			name:       "unknown pragma",
			moduleName: "pragma_unknown",
			wantType:   "unknown",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vtable, schema, err := module.Connect(nil, tt.moduleName, "main", "test", nil)
			if err != nil {
				t.Fatalf("Connect failed: %v", err)
			}

			if vtable == nil {
				t.Error("Expected non-nil vtable")
			}

			if schema == "" {
				t.Error("Expected non-empty schema")
			}

			pragmaTable, ok := vtable.(*PragmaTable)
			if !ok {
				t.Fatal("Expected *PragmaTable")
			}

			if pragmaTable.pragmaType != tt.wantType {
				t.Errorf("Expected pragma type %s, got %s", tt.wantType, pragmaTable.pragmaType)
			}
		})
	}
}

// TestPragmaFunctionModuleCreate tests the Create method.
func TestPragmaFunctionModuleCreate(t *testing.T) {
	t.Parallel()
	module := NewPragmaFunctionModule()

	vtable, schema, err := module.Create(nil, "pragma_table_info", "main", "test", nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if vtable == nil {
		t.Error("Expected non-nil vtable")
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}
}

// TestPragmaCursorEmptyTable tests pragma cursor with empty table name.
func TestPragmaCursorEmptyTable(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)

	// Filter with empty table name (no args)
	err = pragmaCursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have default example_table
	if pragmaCursor.EOF() {
		t.Error("Expected rows for example_table")
	}
}

// TestSQLiteMasterBestIndexEdgeCases tests edge cases in BestIndex.
func TestSQLiteMasterBestIndexEdgeCases(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}

	// Test with non-usable constraints
	info := &vtab.IndexInfo{
		Constraints:     make([]vtab.IndexConstraint, 2),
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 2),
	}

	info.Constraints[0] = vtab.IndexConstraint{
		Column: 0,
		Op:     vtab.ConstraintEQ,
		Usable: false, // Not usable
	}
	info.Constraints[1] = vtab.IndexConstraint{
		Column: 1,
		Op:     vtab.ConstraintEQ,
		Usable: false, // Not usable
	}

	err := table.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// No constraints should be used
	if info.ConstraintUsage[0].ArgvIndex != 0 {
		t.Errorf("Expected non-usable constraint to have ArgvIndex 0, got %d", info.ConstraintUsage[0].ArgvIndex)
	}
}

// TestSQLiteMasterColumnEdgeCases tests edge cases in Column method.
func TestSQLiteMasterColumnEdgeCases(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	err = masterCursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if !masterCursor.EOF() {
		// Test invalid column index
		_, err = masterCursor.Column(99)
		if err == nil {
			t.Error("Expected error for invalid column index")
		}

		// Test negative column index
		_, err = masterCursor.Column(-1)
		if err == nil {
			t.Error("Expected error for negative column index")
		}
	}
}

// TestSQLiteMasterRowidError tests Rowid error cases.
func TestSQLiteMasterRowidError(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	err = masterCursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Move to EOF
	for !masterCursor.EOF() {
		masterCursor.Next()
	}

	// Test Rowid at EOF
	_, err = masterCursor.Rowid()
	if err == nil {
		t.Error("Expected error when calling Rowid at EOF")
	}
}

// TestSQLiteMasterFilterTypeMatch tests matchesFilters edge cases.
func TestSQLiteMasterFilterTypeMatch(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)

	// Test with integer type filter (edge case)
	err = masterCursor.Filter(0, "", []interface{}{123})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should handle non-string type gracefully
	if !masterCursor.EOF() {
		t.Log("Filter with non-string type handled")
	}
}

// TestPragmaModuleUnknownType tests error handling for unknown pragma types.
func TestPragmaModuleUnknownType(t *testing.T) {
	t.Parallel()
	module := &PragmaModule{pragmaType: "unknown_type"}

	_, _, err := module.Connect(nil, "pragma_unknown", "main", "test", nil)
	if err == nil {
		t.Error("Expected error for unknown pragma type")
	}
}

// TestPragmaCursorWithNilArgs tests Filter with nil arguments.
func TestPragmaCursorWithNilArgs(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, err := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)

	// Test with nil argument
	err = pragmaCursor.Filter(0, "", []interface{}{nil})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should use default table
	if pragmaCursor.EOF() {
		t.Error("Expected rows for default table")
	}
}

// TestPragmaCursorEmptyTableName tests empty table name handling.
func TestPragmaCursorEmptyTableName(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, _ := module.Connect(nil, "pragma_table_info", "main", "pragma_table_info", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)
	pragmaCursor.tableName = ""

	// Generate with empty table name
	rows := pragmaCursor.generateTableInfo()
	if len(rows) != 0 {
		t.Error("Expected no rows for empty table name")
	}

	rows = pragmaCursor.generateIndexList()
	if len(rows) != 0 {
		t.Error("Expected no rows for empty table name")
	}
}

// TestPragmaDefaultCase tests the default case in pragma switch.
func TestPragmaDefaultCase(t *testing.T) {
	t.Parallel()
	module := NewPragmaTableInfoModule()
	vtable, _, _ := module.Connect(nil, "pragma_table_info", "main", "test", nil)
	cursor, _ := vtable.Open()
	defer cursor.Close()

	pragmaCursor := cursor.(*PragmaCursor)
	pragmaCursor.pragmaType = "unknown"

	err := pragmaCursor.Filter(0, "", []interface{}{"test_table"})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have empty rows for unknown type
	if !pragmaCursor.EOF() {
		t.Error("Expected EOF for unknown pragma type")
	}
}

// TestSQLiteMasterNonUsableConstraints tests BestIndex with more constraints.
func TestSQLiteMasterNonUsableConstraints(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}

	info := &vtab.IndexInfo{
		Constraints:     make([]vtab.IndexConstraint, 3),
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 3),
	}

	// Mix of usable and non-usable
	info.Constraints[0] = vtab.IndexConstraint{Column: 0, Op: vtab.ConstraintEQ, Usable: true}
	info.Constraints[1] = vtab.IndexConstraint{Column: 1, Op: vtab.ConstraintEQ, Usable: false}
	info.Constraints[2] = vtab.IndexConstraint{Column: 2, Op: vtab.ConstraintEQ, Usable: true}

	err := table.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// Only usable constraints should be used
	if info.ConstraintUsage[0].ArgvIndex == 0 {
		t.Error("Expected usable constraint 0 to be used")
	}
	if info.ConstraintUsage[1].ArgvIndex != 0 {
		t.Error("Expected non-usable constraint 1 not to be used")
	}

	// Note: Constraint 2 might not be used if constraint 0 already provides enough filtering
	t.Logf("ConstraintUsage: %v, %v, %v",
		info.ConstraintUsage[0].ArgvIndex,
		info.ConstraintUsage[1].ArgvIndex,
		info.ConstraintUsage[2].ArgvIndex)
}

// TestSQLiteMasterColumnAllTypes tests all column types.
func TestSQLiteMasterColumnAllTypes(t *testing.T) {
	t.Parallel()
	table := &SQLiteMasterTable{db: nil}
	cursor, _ := table.Open()
	defer cursor.Close()

	masterCursor := cursor.(*SQLiteMasterCursor)
	masterCursor.Filter(0, "", nil)

	if !masterCursor.EOF() {
		// Test all columns
		for col := 0; col <= 4; col++ {
			_, err := masterCursor.Column(col)
			if err != nil {
				t.Errorf("Column(%d) failed: %v", col, err)
			}
		}
	}
}
