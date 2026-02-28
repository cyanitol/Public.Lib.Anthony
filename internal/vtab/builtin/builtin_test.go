package builtin

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
)

// TestSQLiteMasterModule tests the sqlite_master virtual table module.
func TestSQLiteMasterModule(t *testing.T) {
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

// TestSQLiteMasterCursor tests the sqlite_master cursor.
func TestSQLiteMasterCursor(t *testing.T) {
	table := &SQLiteMasterTable{db: nil}
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	masterCursor, ok := cursor.(*SQLiteMasterCursor)
	if !ok {
		t.Fatal("Expected cursor to be *SQLiteMasterCursor")
	}

	// Test Filter with no constraints
	err = masterCursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have at least the sqlite_master table itself
	if masterCursor.EOF() {
		t.Error("Expected at least one row (sqlite_master)")
	}

	// Test Column access
	if !masterCursor.EOF() {
		typeVal, err := masterCursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		if typeVal != "table" {
			t.Errorf("Expected type='table', got %v", typeVal)
		}

		nameVal, err := masterCursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}
		if nameVal != "sqlite_master" {
			t.Errorf("Expected name='sqlite_master', got %v", nameVal)
		}

		tblNameVal, err := masterCursor.Column(2)
		if err != nil {
			t.Errorf("Column(2) failed: %v", err)
		}
		if tblNameVal != "sqlite_master" {
			t.Errorf("Expected tbl_name='sqlite_master', got %v", tblNameVal)
		}

		rootPageVal, err := masterCursor.Column(3)
		if err != nil {
			t.Errorf("Column(3) failed: %v", err)
		}
		if rootPageVal != int64(1) {
			t.Errorf("Expected rootpage=1, got %v", rootPageVal)
		}

		sqlVal, err := masterCursor.Column(4)
		if err != nil {
			t.Errorf("Column(4) failed: %v", err)
		}
		if sqlVal == "" {
			t.Error("Expected non-empty SQL")
		}
	}

	// Test Rowid
	if !masterCursor.EOF() {
		rowid, err := masterCursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}
		if rowid != 0 {
			t.Errorf("Expected rowid=0, got %d", rowid)
		}
	}

	// Test Next
	err = masterCursor.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}

	// Should be EOF after one row
	if !masterCursor.EOF() {
		t.Error("Expected EOF after one row")
	}
}

// TestSQLiteMasterCursorWithFilters tests filtering in sqlite_master.
func TestSQLiteMasterCursorWithFilters(t *testing.T) {
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

