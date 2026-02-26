package schema

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

func TestNewSchema(t *testing.T) {
	s := NewSchema()
	if s == nil {
		t.Fatal("NewSchema() returned nil")
	}
	if s.Tables == nil {
		t.Error("Tables map is nil")
	}
	if s.Indexes == nil {
		t.Error("Indexes map is nil")
	}
	if len(s.Tables) != 0 {
		t.Errorf("New schema has %d tables, want 0", len(s.Tables))
	}
	if len(s.Indexes) != 0 {
		t.Errorf("New schema has %d indexes, want 0", len(s.Indexes))
	}
}

func TestCreateTable(t *testing.T) {
	s := NewSchema()

	// Create a simple table
	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "name",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintNotNull},
				},
			},
			{
				Name: "age",
				Type: "INTEGER",
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if table.Name != "users" {
		t.Errorf("table.Name = %q, want %q", table.Name, "users")
	}

	if len(table.Columns) != 3 {
		t.Fatalf("table has %d columns, want 3", len(table.Columns))
	}

	// Check id column
	if table.Columns[0].Name != "id" {
		t.Errorf("column 0 name = %q, want %q", table.Columns[0].Name, "id")
	}
	if table.Columns[0].Type != "INTEGER" {
		t.Errorf("column 0 type = %q, want %q", table.Columns[0].Type, "INTEGER")
	}
	if table.Columns[0].Affinity != AffinityInteger {
		t.Errorf("column 0 affinity = %v, want INTEGER", table.Columns[0].Affinity)
	}
	if !table.Columns[0].PrimaryKey {
		t.Error("column 0 should be primary key")
	}

	// Check name column
	if table.Columns[1].Name != "name" {
		t.Errorf("column 1 name = %q, want %q", table.Columns[1].Name, "name")
	}
	if !table.Columns[1].NotNull {
		t.Error("column 1 should be NOT NULL")
	}
	if table.Columns[1].Affinity != AffinityText {
		t.Errorf("column 1 affinity = %v, want TEXT", table.Columns[1].Affinity)
	}

	// Check primary key
	if len(table.PrimaryKey) != 1 {
		t.Fatalf("table has %d primary key columns, want 1", len(table.PrimaryKey))
	}
	if table.PrimaryKey[0] != "id" {
		t.Errorf("primary key = %q, want %q", table.PrimaryKey[0], "id")
	}

	// Verify table is in schema
	if _, ok := s.GetTable("users"); !ok {
		t.Error("table not found in schema")
	}
}

func TestCreateTableWithTableConstraints(t *testing.T) {
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "orders",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "user_id", Type: "INTEGER"},
			{Name: "total", Type: "REAL"},
		},
		Constraints: []parser.TableConstraint{
			{
				Type: parser.ConstraintPrimaryKey,
				PrimaryKey: &parser.PrimaryKeyTableConstraint{
					Columns: []parser.IndexedColumn{
						{Column: "id"},
					},
				},
			},
			{
				Type: parser.ConstraintUnique,
				Name: "unique_user_order",
				Unique: &parser.UniqueTableConstraint{
					Columns: []parser.IndexedColumn{
						{Column: "user_id"},
						{Column: "id"},
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if len(table.Constraints) != 2 {
		t.Fatalf("table has %d constraints, want 2", len(table.Constraints))
	}

	// Check primary key constraint
	if table.Constraints[0].Type != ConstraintPrimaryKey {
		t.Error("first constraint should be PRIMARY KEY")
	}

	// Check unique constraint
	if table.Constraints[1].Type != ConstraintUnique {
		t.Error("second constraint should be UNIQUE")
	}
	if len(table.Constraints[1].Columns) != 2 {
		t.Errorf("unique constraint has %d columns, want 2", len(table.Constraints[1].Columns))
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	}

	// First creation should succeed
	_, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("First CreateTable() error = %v", err)
	}

	// Second creation without IF NOT EXISTS should fail
	_, err = s.CreateTable(stmt)
	if err == nil {
		t.Error("Expected error creating duplicate table")
	}

	// Second creation with IF NOT EXISTS should succeed
	stmt.IfNotExists = true
	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable with IF NOT EXISTS error = %v", err)
	}
	if table == nil {
		t.Error("Expected existing table, got nil")
	}
}

func TestCreateIndex(t *testing.T) {
	s := NewSchema()

	// First create a table
	tableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Create an index
	indexStmt := &parser.CreateIndexStmt{
		Name:  "idx_email",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
		Unique: true,
	}

	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	if index.Name != "idx_email" {
		t.Errorf("index.Name = %q, want %q", index.Name, "idx_email")
	}
	if index.Table != "users" {
		t.Errorf("index.Table = %q, want %q", index.Table, "users")
	}
	if !index.Unique {
		t.Error("index should be unique")
	}
	if len(index.Columns) != 1 {
		t.Fatalf("index has %d columns, want 1", len(index.Columns))
	}
	if index.Columns[0] != "email" {
		t.Errorf("index column = %q, want %q", index.Columns[0], "email")
	}

	// Verify index is in schema
	if _, ok := s.GetIndex("idx_email"); !ok {
		t.Error("index not found in schema")
	}
}

func TestCreateIndexOnNonexistentTable(t *testing.T) {
	s := NewSchema()

	indexStmt := &parser.CreateIndexStmt{
		Name:  "idx_test",
		Table: "nonexistent",
		Columns: []parser.IndexedColumn{
			{Column: "col"},
		},
	}

	_, err := s.CreateIndex(indexStmt)
	if err == nil {
		t.Error("Expected error creating index on nonexistent table")
	}
}

func TestGetTable(t *testing.T) {
	s := NewSchema()

	// Add a table directly
	s.Tables["TestTable"] = &Table{Name: "TestTable"}

	// Test case-insensitive lookup
	tests := []string{"TestTable", "testtable", "TESTTABLE", "tEsTtAbLe"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			table, ok := s.GetTable(name)
			if !ok {
				t.Errorf("GetTable(%q) not found", name)
			}
			if table.Name != "TestTable" {
				t.Errorf("GetTable(%q) returned wrong table: %q", name, table.Name)
			}
		})
	}

	// Test non-existent table
	_, ok := s.GetTable("nonexistent")
	if ok {
		t.Error("GetTable should return false for nonexistent table")
	}
}

func TestListTables(t *testing.T) {
	s := NewSchema()

	// Empty schema
	tables := s.ListTables()
	if len(tables) != 0 {
		t.Errorf("ListTables() returned %d tables, want 0", len(tables))
	}

	// Add tables
	s.Tables["users"] = &Table{Name: "users"}
	s.Tables["orders"] = &Table{Name: "orders"}
	s.Tables["products"] = &Table{Name: "products"}

	tables = s.ListTables()
	if len(tables) != 3 {
		t.Fatalf("ListTables() returned %d tables, want 3", len(tables))
	}

	// Should be sorted
	expected := []string{"orders", "products", "users"}
	for i, name := range expected {
		if tables[i] != name {
			t.Errorf("tables[%d] = %q, want %q", i, tables[i], name)
		}
	}
}

func TestDropTable(t *testing.T) {
	s := NewSchema()

	// Create table and indexes
	s.Tables["users"] = &Table{Name: "users"}
	s.Indexes["idx_email"] = &Index{Name: "idx_email", Table: "users"}
	s.Indexes["idx_name"] = &Index{Name: "idx_name", Table: "users"}
	s.Indexes["idx_other"] = &Index{Name: "idx_other", Table: "other"}

	// Drop the table
	err := s.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	// Table should be gone
	if _, ok := s.GetTable("users"); ok {
		t.Error("Table still exists after drop")
	}

	// Indexes for the table should be gone
	if _, ok := s.GetIndex("idx_email"); ok {
		t.Error("Index idx_email still exists")
	}
	if _, ok := s.GetIndex("idx_name"); ok {
		t.Error("Index idx_name still exists")
	}

	// Index for other table should remain
	if _, ok := s.GetIndex("idx_other"); !ok {
		t.Error("Index idx_other was incorrectly removed")
	}

	// Dropping nonexistent table should error
	err = s.DropTable("nonexistent")
	if err == nil {
		t.Error("Expected error dropping nonexistent table")
	}
}

func TestDropIndex(t *testing.T) {
	s := NewSchema()

	s.Indexes["idx_test"] = &Index{Name: "idx_test"}

	err := s.DropIndex("idx_test")
	if err != nil {
		t.Fatalf("DropIndex() error = %v", err)
	}

	if _, ok := s.GetIndex("idx_test"); ok {
		t.Error("Index still exists after drop")
	}

	// Dropping nonexistent index should error
	err = s.DropIndex("nonexistent")
	if err == nil {
		t.Error("Expected error dropping nonexistent index")
	}
}

func TestTableGetColumn(t *testing.T) {
	table := &Table{
		Columns: []*Column{
			{Name: "ID"},
			{Name: "Name"},
			{Name: "Email"},
		},
	}

	// Test case-insensitive lookup
	tests := []string{"ID", "id", "Id", "iD"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			col, ok := table.GetColumn(name)
			if !ok {
				t.Errorf("GetColumn(%q) not found", name)
			}
			if col.Name != "ID" {
				t.Errorf("GetColumn(%q) returned wrong column: %q", name, col.Name)
			}
		})
	}

	// Test non-existent column
	_, ok := table.GetColumn("nonexistent")
	if ok {
		t.Error("GetColumn should return false for nonexistent column")
	}
}

func TestTableGetColumnIndex(t *testing.T) {
	table := &Table{
		Columns: []*Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
		},
	}

	tests := []struct {
		name string
		want int
	}{
		{"id", 0},
		{"ID", 0},
		{"name", 1},
		{"NAME", 1},
		{"email", 2},
		{"nonexistent", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := table.GetColumnIndex(tt.name)
			if got != tt.want {
				t.Errorf("GetColumnIndex(%q) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestTableHasRowID(t *testing.T) {
	tests := []struct {
		name         string
		withoutRowID bool
		want         bool
	}{
		{"normal table", false, true},
		{"WITHOUT ROWID table", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &Table{WithoutRowID: tt.withoutRowID}
			got := table.HasRowID()
			if got != tt.want {
				t.Errorf("HasRowID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTableIndexes(t *testing.T) {
	s := NewSchema()

	// Add indexes
	s.Indexes["idx1"] = &Index{Name: "idx1", Table: "users"}
	s.Indexes["idx2"] = &Index{Name: "idx2", Table: "users"}
	s.Indexes["idx3"] = &Index{Name: "idx3", Table: "orders"}

	indexes := s.GetTableIndexes("users")
	if len(indexes) != 2 {
		t.Fatalf("GetTableIndexes() returned %d indexes, want 2", len(indexes))
	}

	// Should be sorted by name
	if indexes[0].Name != "idx1" || indexes[1].Name != "idx2" {
		t.Error("Indexes not sorted correctly")
	}

	// Case-insensitive
	indexes = s.GetTableIndexes("USERS")
	if len(indexes) != 2 {
		t.Errorf("Case-insensitive lookup returned %d indexes, want 2", len(indexes))
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewSchema()

	// Test concurrent reads and writes
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			stmt := &parser.CreateTableStmt{
				Name: "table" + string(rune(i)),
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			}
			s.CreateTable(stmt)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = s.ListTables()
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}
