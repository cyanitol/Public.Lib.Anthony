package schema

import (
	"testing"
)

func TestNewDatabaseRegistry(t *testing.T) {
	dr := NewDatabaseRegistry()
	if dr == nil {
		t.Fatal("NewDatabaseRegistry() returned nil")
	}
	if dr.databases == nil {
		t.Error("databases map is nil")
	}
}

func TestAttachDatabase(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach main database
	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Verify database exists
	db, ok := dr.GetDatabase("main")
	if !ok {
		t.Error("Database not found after attach")
	}
	if db.Name != "main" {
		t.Errorf("db.Name = %q, want %q", db.Name, "main")
	}
	if db.Path != "/path/to/main.db" {
		t.Errorf("db.Path = %q, want %q", db.Path, "/path/to/main.db")
	}
	if db.Schema == nil {
		t.Error("db.Schema should not be nil")
	}
}

func TestAttachDatabaseDuplicate(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach database
	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Attach again with same name should fail
	err = dr.AttachDatabase("main", "/path/to/other.db", nil, nil)
	if err == nil {
		t.Error("Expected error attaching duplicate database")
	}
}

func TestAttachDatabaseCaseInsensitive(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach with lowercase
	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Try to attach with uppercase - should fail
	err = dr.AttachDatabase("MAIN", "/path/to/other.db", nil, nil)
	if err == nil {
		t.Error("Expected error attaching duplicate database (case-insensitive)")
	}
}

func TestDetachDatabase(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach a user database
	err := dr.AttachDatabase("mydb", "/path/to/mydb.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Detach it
	err = dr.DetachDatabase("mydb")
	if err != nil {
		t.Fatalf("DetachDatabase() error = %v", err)
	}

	// Verify it's gone
	_, ok := dr.GetDatabase("mydb")
	if ok {
		t.Error("Database still exists after detach")
	}
}

func TestDetachMainDatabase(t *testing.T) {
	dr := NewDatabaseRegistry()

	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Cannot detach main
	err = dr.DetachDatabase("main")
	if err == nil {
		t.Error("Expected error detaching main database")
	}
}

func TestDetachTempDatabase(t *testing.T) {
	dr := NewDatabaseRegistry()

	err := dr.AttachDatabase("temp", "/path/to/temp.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	// Cannot detach temp
	err = dr.DetachDatabase("temp")
	if err == nil {
		t.Error("Expected error detaching temp database")
	}
}

func TestDetachNonexistent(t *testing.T) {
	dr := NewDatabaseRegistry()

	err := dr.DetachDatabase("nonexistent")
	if err == nil {
		t.Error("Expected error detaching nonexistent database")
	}
}

func TestGetDatabaseCaseInsensitive(t *testing.T) {
	dr := NewDatabaseRegistry()

	err := dr.AttachDatabase("MyDb", "/path/to/mydb.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	tests := []string{"MyDb", "mydb", "MYDB", "mYdB"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			db, ok := dr.GetDatabase(name)
			if !ok {
				t.Errorf("GetDatabase(%q) not found", name)
			}
			if db.Name != "MyDb" {
				t.Errorf("db.Name = %q, want %q", db.Name, "MyDb")
			}
		})
	}
}

func TestGetTableQualified(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach main database with a table
	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["users"] = &Table{Name: "users"}

	// Qualified lookup
	table, schema, ok := dr.GetTable("main", "users")
	if !ok {
		t.Error("GetTable() not found")
	}
	if table.Name != "users" {
		t.Errorf("table.Name = %q, want %q", table.Name, "users")
	}
	if schema != "main" {
		t.Errorf("schema = %q, want %q", schema, "main")
	}
}

func TestGetTableUnqualified(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach main database
	err := dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	if err != nil {
		t.Fatalf("AttachDatabase() error = %v", err)
	}

	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["users"] = &Table{Name: "users"}

	// Unqualified lookup should find in main
	table, schema, ok := dr.GetTable("", "users")
	if !ok {
		t.Error("GetTable() not found")
	}
	if table.Name != "users" {
		t.Errorf("table.Name = %q, want %q", table.Name, "users")
	}
	if schema != "main" {
		t.Errorf("schema = %q, want %q", schema, "main")
	}
}

func TestGetTableUnqualifiedSearchOrder(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach multiple databases
	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	dr.AttachDatabase("temp", "/path/to/temp.db", nil, nil)
	dr.AttachDatabase("other", "/path/to/other.db", nil, nil)

	// Add same table to multiple databases
	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["users"] = &Table{Name: "users"}

	tempDB, _ := dr.GetDatabase("temp")
	tempDB.Schema.Tables["users"] = &Table{Name: "users"}

	otherDB, _ := dr.GetDatabase("other")
	otherDB.Schema.Tables["users"] = &Table{Name: "users"}

	// Unqualified lookup should find main first
	_, schema, ok := dr.GetTable("", "users")
	if !ok {
		t.Error("GetTable() not found")
	}
	if schema != "main" {
		t.Errorf("schema = %q, want %q (should search main first)", schema, "main")
	}
}

func TestGetTableUnqualifiedTemp(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Attach main and temp
	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	dr.AttachDatabase("temp", "/path/to/temp.db", nil, nil)

	// Add table only to temp
	tempDB, _ := dr.GetDatabase("temp")
	tempDB.Schema.Tables["temp_table"] = &Table{Name: "temp_table"}

	// Should find in temp
	_, schema, ok := dr.GetTable("", "temp_table")
	if !ok {
		t.Error("GetTable() not found")
	}
	if schema != "temp" {
		t.Errorf("schema = %q, want %q", schema, "temp")
	}
}

func TestGetTableUnqualifiedNotFound(t *testing.T) {
	dr := NewDatabaseRegistry()

	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)

	_, _, ok := dr.GetTable("", "nonexistent")
	if ok {
		t.Error("GetTable() should return false for nonexistent table")
	}
}

func TestGetTableQualifiedNotFound(t *testing.T) {
	dr := NewDatabaseRegistry()

	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)

	// Table doesn't exist
	_, _, ok := dr.GetTable("main", "nonexistent")
	if ok {
		t.Error("GetTable() should return false for nonexistent table")
	}

	// Schema doesn't exist
	_, _, ok = dr.GetTable("nonexistent", "users")
	if ok {
		t.Error("GetTable() should return false for nonexistent schema")
	}
}

func TestListDatabases(t *testing.T) {
	dr := NewDatabaseRegistry()

	// Empty registry
	dbs := dr.ListDatabases()
	if len(dbs) != 0 {
		t.Errorf("ListDatabases() returned %d databases, want 0", len(dbs))
	}

	// Add databases
	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)
	dr.AttachDatabase("temp", "/path/to/temp.db", nil, nil)
	dr.AttachDatabase("mydb", "/path/to/mydb.db", nil, nil)

	dbs = dr.ListDatabases()
	if len(dbs) != 3 {
		t.Fatalf("ListDatabases() returned %d databases, want 3", len(dbs))
	}

	// Verify all databases are listed
	dbMap := make(map[string]bool)
	for _, name := range dbs {
		dbMap[name] = true
	}

	if !dbMap["main"] || !dbMap["temp"] || !dbMap["mydb"] {
		t.Error("Not all databases listed")
	}
}

func TestGetMainDatabase(t *testing.T) {
	dr := NewDatabaseRegistry()

	// No main database
	_, ok := dr.GetMainDatabase()
	if ok {
		t.Error("GetMainDatabase() should return false when main doesn't exist")
	}

	// Attach main
	dr.AttachDatabase("main", "/path/to/main.db", nil, nil)

	db, ok := dr.GetMainDatabase()
	if !ok {
		t.Error("GetMainDatabase() not found")
	}
	if db.Name != "main" {
		t.Errorf("db.Name = %q, want %q", db.Name, "main")
	}
}
