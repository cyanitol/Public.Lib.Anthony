// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package schema

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func TestNewSchema(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	s := NewSchema()

	// Add a table directly
	s.Tables["TestTable"] = &Table{Name: "TestTable"}

	// Test case-insensitive lookup
	tests := []string{"TestTable", "testtable", "TESTTABLE", "tEsTtAbLe"}
	for _, name := range tests {
		name := name
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
	t.Parallel()
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
		name := name
		if tables[i] != name {
			t.Errorf("tables[%d] = %q, want %q", i, tables[i], name)
		}
	}
}

func TestDropTable(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
		name := name
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
	t.Parallel()
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := table.GetColumnIndex(tt.name)
			if got != tt.want {
				t.Errorf("GetColumnIndex(%q) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestTableHasRowID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		withoutRowID bool
		want         bool
	}{
		{"normal table", false, true},
		{"WITHOUT ROWID table", true, false},
	}

	for _, tt := range tests {
		tt := tt
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
	t.Parallel()
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
	t.Parallel()
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

func TestRenameTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a table with an index
	s.Tables["users"] = &Table{Name: "users"}
	s.Indexes["idx_users"] = &Index{Name: "idx_users", Table: "users"}

	// Rename the table
	err := s.RenameTable("users", "customers")
	if err != nil {
		t.Fatalf("RenameTable() error = %v", err)
	}

	// Old table should be gone
	if _, ok := s.GetTable("users"); ok {
		t.Error("Old table still exists")
	}

	// New table should exist
	table, ok := s.GetTable("customers")
	if !ok {
		t.Error("New table not found")
	}
	if table.Name != "customers" {
		t.Errorf("table.Name = %q, want %q", table.Name, "customers")
	}

	// Index should reference new table name
	if s.Indexes["idx_users"].Table != "customers" {
		t.Errorf("Index table = %q, want %q", s.Indexes["idx_users"].Table, "customers")
	}

	// Renaming to existing name should fail
	s.Tables["products"] = &Table{Name: "products"}
	err = s.RenameTable("customers", "products")
	if err == nil {
		t.Error("Expected error renaming to existing table")
	}

	// Renaming non-existent table should fail
	err = s.RenameTable("nonexistent", "new_name")
	if err == nil {
		t.Error("Expected error renaming non-existent table")
	}
}

func TestRenameTableCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["MyTable"] = &Table{Name: "MyTable"}

	// Rename with different case
	err := s.RenameTable("mytable", "NewTable")
	if err != nil {
		t.Fatalf("RenameTable() error = %v", err)
	}

	// Old table should be gone
	if _, ok := s.GetTable("MyTable"); ok {
		t.Error("Old table still exists")
	}

	// New table should exist
	if _, ok := s.GetTable("NewTable"); !ok {
		t.Error("New table not found")
	}
}

func TestGetColumnCollation(t *testing.T) {
	t.Parallel()
	table := &Table{
		Columns: []*Column{
			{Name: "id", Collation: ""},
			{Name: "name", Collation: "NOCASE"},
			{Name: "email", Collation: "RTRIM"},
		},
	}

	tests := []struct {
		index int
		want  string
	}{
		{0, ""},
		{1, "NOCASE"},
		{2, "RTRIM"},
		{-1, ""}, // Invalid index
		{10, ""}, // Out of bounds
	}

	for _, tt := range tests {
		tt := tt
		got := table.GetColumnCollation(tt.index)
		if got != tt.want {
			t.Errorf("GetColumnCollation(%d) = %q, want %q", tt.index, got, tt.want)
		}
	}
}

func TestGetColumnCollationByName(t *testing.T) {
	t.Parallel()
	table := &Table{
		Columns: []*Column{
			{Name: "id", Collation: ""},
			{Name: "name", Collation: "NOCASE"},
		},
	}

	tests := []struct {
		name string
		want string
	}{
		{"id", ""},
		{"name", "NOCASE"},
		{"NAME", "NOCASE"}, // Case-insensitive
		{"nonexistent", ""},
	}

	for _, tt := range tests {
		tt := tt
		got := table.GetColumnCollationByName(tt.name)
		if got != tt.want {
			t.Errorf("GetColumnCollationByName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestColumnGetEffectiveCollation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		collation string
		want      string
	}{
		{"explicit NOCASE", "NOCASE", "NOCASE"},
		{"explicit RTRIM", "RTRIM", "RTRIM"},
		{"default (empty)", "", "BINARY"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			col := &Column{Collation: tt.collation}
			got := col.GetEffectiveCollation()
			if got != tt.want {
				t.Errorf("GetEffectiveCollation() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreateTableWithNilStatement(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	_, err := s.CreateTable(nil)
	if err == nil {
		t.Error("Expected error for nil statement")
	}
}

func TestCreateIndexWithNilStatement(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	_, err := s.CreateIndex(nil)
	if err == nil {
		t.Error("Expected error for nil statement")
	}
}

func TestCreateIndexIfNotExists(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a table first
	s.Tables["users"] = &Table{Name: "users"}

	indexStmt := &parser.CreateIndexStmt{
		Name:  "idx_test",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
	}

	// First creation should succeed
	_, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("First CreateIndex() error = %v", err)
	}

	// Second creation without IF NOT EXISTS should fail
	_, err = s.CreateIndex(indexStmt)
	if err == nil {
		t.Error("Expected error creating duplicate index")
	}

	// Second creation with IF NOT EXISTS should succeed
	indexStmt.IfNotExists = true
	idx, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("CreateIndex with IF NOT EXISTS error = %v", err)
	}
	if idx == nil {
		t.Error("Expected existing index, got nil")
	}
}

func TestCreateIndexWithPartialWhere(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a table
	s.Tables["users"] = &Table{Name: "users"}

	// Create partial index with WHERE clause
	indexStmt := &parser.CreateIndexStmt{
		Name:  "idx_active",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "active"},
			Right: &parser.LiteralExpr{Value: "1"},
		},
	}

	idx, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	if !idx.Partial {
		t.Error("Index should be partial")
	}

	if idx.Where == "" {
		t.Error("Index Where clause should not be empty")
	}
}

func TestListIndexes(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Empty schema
	indexes := s.ListIndexes()
	if len(indexes) != 0 {
		t.Errorf("ListIndexes() returned %d indexes, want 0", len(indexes))
	}

	// Add indexes
	s.Indexes["idx_users"] = &Index{Name: "idx_users"}
	s.Indexes["idx_orders"] = &Index{Name: "idx_orders"}
	s.Indexes["idx_products"] = &Index{Name: "idx_products"}

	indexes = s.ListIndexes()
	if len(indexes) != 3 {
		t.Fatalf("ListIndexes() returned %d indexes, want 3", len(indexes))
	}

	// Should be sorted
	expected := []string{"idx_orders", "idx_products", "idx_users"}
	for i, name := range expected {
		name := name
		if indexes[i] != name {
			t.Errorf("indexes[%d] = %q, want %q", i, indexes[i], name)
		}
	}
}

func TestGetIndexCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["MyIndex"] = &Index{Name: "MyIndex"}

	tests := []string{"MyIndex", "myindex", "MYINDEX", "mYiNdEx"}
	for _, name := range tests {
		name := name
		t.Run(name, func(t *testing.T) {
			idx, ok := s.GetIndex(name)
			if !ok {
				t.Errorf("GetIndex(%q) not found", name)
			}
			if idx.Name != "MyIndex" {
				t.Errorf("GetIndex(%q) returned wrong index: %q", name, idx.Name)
			}
		})
	}
}

func TestDropIndexCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["MyIndex"] = &Index{Name: "MyIndex"}

	err := s.DropIndex("myindex")
	if err != nil {
		t.Fatalf("DropIndex() error = %v", err)
	}

	if _, ok := s.GetIndex("MyIndex"); ok {
		t.Error("Index still exists after drop")
	}
}

func TestDropTableCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["MyTable"] = &Table{Name: "MyTable"}
	s.Indexes["idx_mytable"] = &Index{Name: "idx_mytable", Table: "MyTable"}

	err := s.DropTable("mytable")
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	if _, ok := s.GetTable("MyTable"); ok {
		t.Error("Table still exists after drop")
	}

	// Index should also be dropped
	if _, ok := s.GetIndex("idx_mytable"); ok {
		t.Error("Index still exists after table drop")
	}
}

func TestCreateTableWithAutoincrement(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type: parser.ConstraintPrimaryKey,
						PrimaryKey: &parser.PrimaryKeyConstraint{
							Autoincrement: true,
						},
					},
				},
			},
			{
				Name: "name",
				Type: "TEXT",
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if !table.Columns[0].Autoincrement {
		t.Error("Column should have AUTOINCREMENT")
	}

	// Check sequence was initialized
	if !s.Sequences.HasSequence("users") {
		t.Error("Sequence not initialized for AUTOINCREMENT table")
	}
}

func TestCreateTableWithInvalidAutoincrement(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// AUTOINCREMENT on non-INTEGER column should fail
	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{
						Type: parser.ConstraintPrimaryKey,
						PrimaryKey: &parser.PrimaryKeyConstraint{
							Autoincrement: true,
						},
					},
				},
			},
		},
	}

	_, err := s.CreateTable(stmt)
	if err == nil {
		t.Error("Expected error for AUTOINCREMENT on TEXT column")
	}
}

func TestCreateTableWithGeneratedColumn(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "a",
				Type: "INTEGER",
			},
			{
				Name: "b",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type: parser.ConstraintGenerated,
						Generated: &parser.GeneratedConstraint{
							Expr:   &parser.BinaryExpr{},
							Stored: true,
						},
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if !table.Columns[1].Generated {
		t.Error("Column should be marked as generated")
	}

	if !table.Columns[1].GeneratedStored {
		t.Error("Column should be marked as STORED")
	}
}

func TestCreateTableWithCheck(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	checkExpr := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.IdentExpr{Name: "age"},
		Right: &parser.LiteralExpr{Value: "0"},
	}

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "age",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: checkExpr,
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if table.Columns[0].Check == "" {
		t.Error("Column should have CHECK constraint")
	}
}

func TestCreateTableWithDefault(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	defaultExpr := &parser.LiteralExpr{Value: "0"}

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "status",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:    parser.ConstraintDefault,
						Default: defaultExpr,
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if table.Columns[0].Default == nil {
		t.Error("Column should have DEFAULT value")
	}
}

func TestCreateTableWithForeignKey(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "orders",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "user_id", Type: "INTEGER"},
		},
		Constraints: []parser.TableConstraint{
			{
				Type: parser.ConstraintForeignKey,
				ForeignKey: &parser.ForeignKeyTableConstraint{
					Columns: []string{"user_id"},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if len(table.Constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(table.Constraints))
	}

	if table.Constraints[0].Type != ConstraintForeignKey {
		t.Error("Constraint should be FOREIGN KEY")
	}
}

func TestCreateTableWithTableCheck(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	checkExpr := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.IdentExpr{Name: "end_date"},
		Right: &parser.IdentExpr{Name: "start_date"},
	}

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "start_date", Type: "TEXT"},
			{Name: "end_date", Type: "TEXT"},
		},
		Constraints: []parser.TableConstraint{
			{
				Type:  parser.ConstraintCheck,
				Check: checkExpr,
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if len(table.Constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(table.Constraints))
	}

	if table.Constraints[0].Type != ConstraintCheck {
		t.Error("Constraint should be CHECK")
	}

	if table.Constraints[0].Expression == "" {
		t.Error("Check constraint should have expression")
	}
}

func TestCreateTableWithCollation(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "name",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{
						Type:    parser.ConstraintCollate,
						Collate: "NOCASE",
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if table.Columns[0].Collation != "NOCASE" {
		t.Errorf("Column collation = %q, want %q", table.Columns[0].Collation, "NOCASE")
	}
}

func TestCreateTableWithStrict(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name:   "test",
		Strict: true,
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if !table.Strict {
		t.Error("Table should be STRICT")
	}
}

func TestCreateTableWithoutRowID(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name:         "test",
		WithoutRowID: true,
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if !table.WithoutRowID {
		t.Error("Table should be WITHOUT ROWID")
	}

	if table.HasRowID() {
		t.Error("HasRowID() should return false for WITHOUT ROWID table")
	}
}

func TestDropTableWithSequence(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create table with AUTOINCREMENT
	s.Tables["users"] = &Table{Name: "users"}
	s.Sequences.InitSequence("users")

	// Verify sequence exists
	if !s.Sequences.HasSequence("users") {
		t.Error("Sequence should exist before drop")
	}

	// Drop table
	err := s.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	// Sequence should be dropped
	if s.Sequences.HasSequence("users") {
		t.Error("Sequence should be dropped with table")
	}
}

func TestApplyGeneratedConstraintVirtual(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "a",
				Type: "INTEGER",
			},
			{
				Name: "b",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type: parser.ConstraintGenerated,
						Generated: &parser.GeneratedConstraint{
							Expr:    &parser.BinaryExpr{},
							Virtual: true,
						},
					},
				},
			},
		},
	}

	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	if !table.Columns[1].Generated {
		t.Error("Column should be marked as generated")
	}

	if table.Columns[1].GeneratedStored {
		t.Error("Column should not be marked as STORED (should be VIRTUAL)")
	}
}

func TestApplyGeneratedConstraintNil(t *testing.T) {
	t.Parallel()
	// Test the nil check in applyGeneratedConstraint
	col := &Column{Name: "test"}
	pkCols := []string{}

	constraint := parser.ColumnConstraint{
		Type:      parser.ConstraintGenerated,
		Generated: nil,
	}

	applyGeneratedConstraint(col, constraint, &pkCols)

	if col.Generated {
		t.Error("Column should not be marked as generated when constraint is nil")
	}
}

func TestApplyTablePrimaryKeyNil(t *testing.T) {
	t.Parallel()
	tc := &TableConstraint{}
	pkCols := []string{}

	constraint := parser.TableConstraint{
		Type:       parser.ConstraintPrimaryKey,
		PrimaryKey: nil,
	}

	applyTablePrimaryKey(tc, constraint, &pkCols)

	if len(tc.Columns) != 0 {
		t.Error("Columns should be empty when PrimaryKey is nil")
	}
}

func TestApplyTableUniqueNil(t *testing.T) {
	t.Parallel()
	tc := &TableConstraint{}
	pkCols := []string{}

	constraint := parser.TableConstraint{
		Type:   parser.ConstraintUnique,
		Unique: nil,
	}

	applyTableUnique(tc, constraint, &pkCols)

	if len(tc.Columns) != 0 {
		t.Error("Columns should be empty when Unique is nil")
	}
}

func TestCreateViewNilSelect(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateViewStmt{
		Name:   "empty_view",
		Select: nil, // Nil select statement
	}

	view, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	if view.Select != nil {
		t.Error("View should have nil Select")
	}
}

func TestParseViewSQLWrongType(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name: "not_a_view",
		SQL:  "CREATE TABLE users(id INTEGER)", // Not a VIEW
	}

	_, err := s.parseViewSQL(row)
	if err == nil {
		t.Error("Expected error for non-CREATE VIEW statement")
	}
}

func TestParseViewSQLMultipleStatements(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name: "bad_view",
		SQL:  "CREATE VIEW v1 AS SELECT 1; CREATE VIEW v2 AS SELECT 2",
	}

	_, err := s.parseViewSQL(row)
	if err == nil {
		t.Error("Expected error for multiple statements")
	}
}
