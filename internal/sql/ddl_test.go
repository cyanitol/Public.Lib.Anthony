// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

func TestNewSchema(t *testing.T) {
	schema := NewSchema()
	if schema == nil {
		t.Fatal("NewSchema returned nil")
	}
	if schema.Tables == nil {
		t.Error("Tables map is nil")
	}
	if schema.Indexes == nil {
		t.Error("Indexes map is nil")
	}
	if len(schema.Tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(schema.Tables))
	}
	if len(schema.Indexes) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(schema.Indexes))
	}
}

func TestSchemaAddTable(t *testing.T) {
	schema := NewSchema()

	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER", Affinity: SQLITE_AFF_INTEGER, PrimaryKey: true},
			{Name: "name", DeclType: "TEXT", Affinity: SQLITE_AFF_TEXT},
		},
		RootPage: 2,
	}

	err := schema.AddTable(table)
	if err != nil {
		t.Fatalf("AddTable failed: %v", err)
	}

	retrieved := schema.GetTable("users")
	if retrieved == nil {
		t.Fatal("GetTable returned nil")
	}
	if retrieved.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", retrieved.Name)
	}

	// Try to add duplicate
	err = schema.AddTable(table)
	if err == nil {
		t.Error("Expected error when adding duplicate table")
	}
}

func TestSchemaRemoveTable(t *testing.T) {
	schema := NewSchema()

	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   2,
	}

	schema.AddTable(table)

	err := schema.RemoveTable("users")
	if err != nil {
		t.Fatalf("RemoveTable failed: %v", err)
	}

	if schema.GetTable("users") != nil {
		t.Error("Table still exists after removal")
	}

	// Try to remove non-existent table
	err = schema.RemoveTable("nonexistent")
	if err == nil {
		t.Error("Expected error when removing non-existent table")
	}
}

// assertVDBEHasOpcodes checks that the VDBE program contains all required opcodes.
func assertVDBEHasOpcodes(t *testing.T, v *vdbe.VDBE, opcodes []vdbe.Opcode) {
	t.Helper()
	found := make(map[vdbe.Opcode]bool)
	for i := 0; i < v.NumOps(); i++ {
		instr, _ := v.GetInstruction(i)
		if instr != nil {
			found[instr.Opcode] = true
		}
	}
	for _, op := range opcodes {
		if !found[op] {
			t.Errorf("VDBE program missing %v", op)
		}
	}
}

func TestCompileCreateTableBasic(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	v, err := CompileCreateTable(stmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateTable failed: %v", err)
	}
	if v == nil {
		t.Fatal("VDBE is nil")
	}

	table := schema.GetTable("users")
	if table == nil {
		t.Fatal("Table not added to schema")
	}
	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", table.Name)
	}
	if table.NumColumns != 2 {
		t.Errorf("Expected 2 columns, got %d", table.NumColumns)
	}
	if v.NumOps() == 0 {
		t.Error("VDBE program is empty")
	}

	assertVDBEHasOpcodes(t, v, []vdbe.Opcode{vdbe.OpInit, vdbe.OpOpenWrite, vdbe.OpInsert, vdbe.OpHalt})
}

func TestCompileCreateTableWithConstraints(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey, PrimaryKey: &parser.PrimaryKeyConstraint{Autoincrement: true}},
				},
			},
			{
				Name: "email",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintNotNull},
					{Type: parser.ConstraintUnique},
				},
			},
			{
				Name: "age",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintDefault, Default: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
				},
			},
		},
	}

	v, err := CompileCreateTable(stmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateTable failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}

	table := schema.GetTable("users")
	if table == nil {
		t.Fatal("Table not added to schema")
	}

	// Check PRIMARY KEY
	if table.PrimaryKey != 0 {
		t.Errorf("Expected primary key at column 0, got %d", table.PrimaryKey)
	}
	if !table.Columns[0].PrimaryKey {
		t.Error("Column 0 should be marked as primary key")
	}

	// Check NOT NULL
	if !table.Columns[1].NotNull {
		t.Error("Column 1 (email) should have NOT NULL constraint")
	}

	// Check DEFAULT
	if table.Columns[2].DefaultValue == nil {
		t.Error("Column 2 (age) should have DEFAULT value")
	}
}

func TestCompileCreateTableIfNotExists(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table first time
	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	}

	_, err := CompileCreateTable(stmt, schema, bt)
	if err != nil {
		t.Fatalf("First create failed: %v", err)
	}

	// Try to create again without IF NOT EXISTS - should fail
	_, err = CompileCreateTable(stmt, schema, bt)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}

	// Try with IF NOT EXISTS - should succeed
	stmt.IfNotExists = true
	v, err := CompileCreateTable(stmt, schema, bt)
	if err != nil {
		t.Fatalf("Create with IF NOT EXISTS failed: %v", err)
	}
	if v == nil {
		t.Error("VDBE is nil")
	}
}

func TestCompileCreateTableInvalidName(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	tests := []struct {
		name      string
		tableName string
	}{
		{"empty name", ""},
		{"sqlite_master", "sqlite_master"},
		{"sqlite_schema", "sqlite_schema"},
		{"SQLITE_MASTER", "SQLITE_MASTER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &parser.CreateTableStmt{
				Name: tt.tableName,
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			}

			_, err := CompileCreateTable(stmt, schema, bt)
			if err == nil {
				t.Errorf("Expected error for table name '%s'", tt.tableName)
			}
		})
	}
}

func TestCompileDropTableBasic(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// First create a table
	createStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Now drop it
	dropStmt := &parser.DropTableStmt{
		Name: "users",
	}

	v, err := CompileDropTable(dropStmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileDropTable failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}

	// Check that table was removed from schema
	if schema.GetTable("users") != nil {
		t.Error("Table still exists in schema after drop")
	}

	// Check VDBE program
	if v.NumOps() == 0 {
		t.Error("VDBE program is empty")
	}
}

func TestCompileDropTableIfExists(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Try to drop non-existent table without IF EXISTS - should fail
	dropStmt := &parser.DropTableStmt{
		Name: "nonexistent",
	}

	_, err := CompileDropTable(dropStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when dropping non-existent table")
	}

	// Try with IF EXISTS - should succeed
	dropStmt.IfExists = true
	v, err := CompileDropTable(dropStmt, schema, bt)
	if err != nil {
		t.Fatalf("Drop with IF EXISTS failed: %v", err)
	}
	if v == nil {
		t.Error("VDBE is nil")
	}
}

func TestCompileDropTableSystemTable(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Add a fake system table to schema
	schema.AddTable(&Table{
		Name:       "sqlite_master",
		NumColumns: 5,
		Columns:    []Column{{Name: "type"}},
		RootPage:   1,
	})

	dropStmt := &parser.DropTableStmt{
		Name: "sqlite_master",
	}

	_, err := CompileDropTable(dropStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when dropping system table")
	}

	// Check with different case
	dropStmt.Name = "SQLITE_SCHEMA"
	schema.AddTable(&Table{
		Name:     "SQLITE_SCHEMA",
		Columns:  []Column{{Name: "type"}},
		RootPage: 1,
	})

	_, err = CompileDropTable(dropStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when dropping system table (uppercase)")
	}
}

func TestCompileCreateIndexBasic(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// First create a table
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
			{Name: "name", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Create index
	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_users_email",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email", Order: parser.SortAsc},
		},
	}

	v, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateIndex failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}

	// Check that index was added to schema
	index := schema.Indexes["idx_users_email"]
	if index == nil {
		t.Fatal("Index not added to schema")
	}
	if index.Name != "idx_users_email" {
		t.Errorf("Expected index name 'idx_users_email', got '%s'", index.Name)
	}
	if index.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", index.Table)
	}
	if len(index.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(index.Columns))
	}
	if index.Columns[0] != "email" {
		t.Errorf("Expected column 'email', got '%s'", index.Columns[0])
	}
}

func TestCompileCreateIndexUnique(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Create unique index
	createIndexStmt := &parser.CreateIndexStmt{
		Name:   "idx_users_email_unique",
		Table:  "users",
		Unique: true,
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
	}

	v, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateIndex failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}

	index := schema.Indexes["idx_users_email_unique"]
	if index == nil {
		t.Fatal("Index not added to schema")
	}
	if !index.Unique {
		t.Error("Index should be marked as unique")
	}
}

func TestCompileCreateIndexIfNotExists(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Create index
	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_email",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
	}

	_, err = CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create index failed: %v", err)
	}

	// Try to create again without IF NOT EXISTS - should fail
	_, err = CompileCreateIndex(createIndexStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when creating duplicate index")
	}

	// Try with IF NOT EXISTS - should succeed
	createIndexStmt.IfNotExists = true
	v, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create index with IF NOT EXISTS failed: %v", err)
	}
	if v == nil {
		t.Error("VDBE is nil")
	}
}

func TestCompileCreateIndexNonExistentTable(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_email",
		Table: "nonexistent",
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
	}

	_, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when creating index on non-existent table")
	}
}

func TestCompileCreateIndexNonExistentColumn(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Try to create index on non-existent column
	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_email",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "email"}, // This column doesn't exist
		},
	}

	_, err = CompileCreateIndex(createIndexStmt, schema, bt)
	if err == nil {
		t.Error("Expected error when creating index on non-existent column")
	}
}

func TestCompileCreateIndexMultipleColumns(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "last_name", Type: "TEXT"},
			{Name: "first_name", Type: "TEXT"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Create multi-column index
	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_name",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "last_name", Order: parser.SortAsc},
			{Column: "first_name", Order: parser.SortAsc},
		},
	}

	v, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateIndex failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}

	index := schema.Indexes["idx_name"]
	if index == nil {
		t.Fatal("Index not added to schema")
	}
	if len(index.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(index.Columns))
	}
	if index.Columns[0] != "last_name" {
		t.Errorf("Expected first column 'last_name', got '%s'", index.Columns[0])
	}
	if index.Columns[1] != "first_name" {
		t.Errorf("Expected second column 'first_name', got '%s'", index.Columns[1])
	}
}

func TestTypeNameToAffinity(t *testing.T) {
	tests := []struct {
		typeName string
		expected Affinity
	}{
		{"INTEGER", SQLITE_AFF_INTEGER},
		{"INT", SQLITE_AFF_INTEGER},
		{"TINYINT", SQLITE_AFF_INTEGER},
		{"BIGINT", SQLITE_AFF_INTEGER},
		{"TEXT", SQLITE_AFF_TEXT},
		{"VARCHAR(100)", SQLITE_AFF_TEXT},
		{"CHAR(10)", SQLITE_AFF_TEXT},
		{"CLOB", SQLITE_AFF_TEXT},
		{"BLOB", SQLITE_AFF_BLOB},
		{"REAL", SQLITE_AFF_REAL},
		{"FLOAT", SQLITE_AFF_REAL},
		{"DOUBLE", SQLITE_AFF_REAL},
		{"NUMERIC", SQLITE_AFF_NUMERIC},
		{"DECIMAL", SQLITE_AFF_NUMERIC},
		{"", SQLITE_AFF_BLOB},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := typeNameToAffinity(tt.typeName)
			if result != tt.expected {
				t.Errorf("typeNameToAffinity(%q) = %v, expected %v", tt.typeName, result, tt.expected)
			}
		})
	}
}

func TestGenerateCreateTableSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateTableStmt
		contains []string
	}{
		{
			name: "basic table",
			stmt: &parser.CreateTableStmt{
				Name: "users",
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "TEXT"},
				},
			},
			contains: []string{"CREATE TABLE", "users", "id", "INTEGER", "name", "TEXT"},
		},
		{
			name: "table with IF NOT EXISTS",
			stmt: &parser.CreateTableStmt{
				Name:        "users",
				IfNotExists: true,
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			},
			contains: []string{"CREATE TABLE", "IF NOT EXISTS", "users"},
		},
		{
			name: "table with PRIMARY KEY",
			stmt: &parser.CreateTableStmt{
				Name: "users",
				Columns: []parser.ColumnDef{
					{
						Name: "id",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{Type: parser.ConstraintPrimaryKey, PrimaryKey: &parser.PrimaryKeyConstraint{}},
						},
					},
				},
			},
			contains: []string{"CREATE TABLE", "users", "id", "INTEGER", "PRIMARY KEY"},
		},
		{
			name: "table with AUTOINCREMENT",
			stmt: &parser.CreateTableStmt{
				Name: "users",
				Columns: []parser.ColumnDef{
					{
						Name: "id",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{Type: parser.ConstraintPrimaryKey, PrimaryKey: &parser.PrimaryKeyConstraint{Autoincrement: true}},
						},
					},
				},
			},
			contains: []string{"CREATE TABLE", "users", "id", "INTEGER", "PRIMARY KEY", "AUTOINCREMENT"},
		},
		{
			name: "table with NOT NULL",
			stmt: &parser.CreateTableStmt{
				Name: "users",
				Columns: []parser.ColumnDef{
					{
						Name: "email",
						Type: "TEXT",
						Constraints: []parser.ColumnConstraint{
							{Type: parser.ConstraintNotNull},
						},
					},
				},
			},
			contains: []string{"CREATE TABLE", "users", "email", "TEXT", "NOT NULL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := generateCreateTableSQL(tt.stmt)
			for _, substr := range tt.contains {
				if !strings.Contains(sql, substr) {
					t.Errorf("Generated SQL does not contain %q:\n%s", substr, sql)
				}
			}
		})
	}
}

func TestGenerateCreateIndexSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateIndexStmt
		contains []string
	}{
		{
			name: "basic index",
			stmt: &parser.CreateIndexStmt{
				Name:  "idx_email",
				Table: "users",
				Columns: []parser.IndexedColumn{
					{Column: "email"},
				},
			},
			contains: []string{"CREATE INDEX", "idx_email", "ON", "users", "email"},
		},
		{
			name: "unique index",
			stmt: &parser.CreateIndexStmt{
				Name:   "idx_email",
				Table:  "users",
				Unique: true,
				Columns: []parser.IndexedColumn{
					{Column: "email"},
				},
			},
			contains: []string{"CREATE UNIQUE INDEX", "idx_email", "ON", "users", "email"},
		},
		{
			name: "index with IF NOT EXISTS",
			stmt: &parser.CreateIndexStmt{
				Name:        "idx_email",
				Table:       "users",
				IfNotExists: true,
				Columns: []parser.IndexedColumn{
					{Column: "email"},
				},
			},
			contains: []string{"CREATE INDEX", "IF NOT EXISTS", "idx_email", "ON", "users", "email"},
		},
		{
			name: "multi-column index",
			stmt: &parser.CreateIndexStmt{
				Name:  "idx_name",
				Table: "users",
				Columns: []parser.IndexedColumn{
					{Column: "last_name", Order: parser.SortAsc},
					{Column: "first_name", Order: parser.SortDesc},
				},
			},
			contains: []string{"CREATE INDEX", "idx_name", "ON", "users", "last_name", "ASC", "first_name", "DESC"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := generateCreateIndexSQL(tt.stmt)
			for _, substr := range tt.contains {
				if !strings.Contains(sql, substr) {
					t.Errorf("Generated SQL does not contain %q:\n%s", substr, sql)
				}
			}
		})
	}
}

func TestAllocateRootPage(t *testing.T) {
	bt := btree.NewBtree(4096)

	// First allocation should be page 2 (page 1 is sqlite_master)
	page1 := allocateRootPage(bt)
	if page1 != 2 {
		t.Errorf("Expected first allocated page to be 2, got %d", page1)
	}

	// Add a page to btree
	bt.SetPage(2, make([]byte, 4096))

	// Next allocation should be page 3
	page2 := allocateRootPage(bt)
	if page2 != 3 {
		t.Errorf("Expected second allocated page to be 3, got %d", page2)
	}
}

func TestConvertExpr(t *testing.T) {
	tests := []struct {
		name     string
		input    parser.Expression
		expected int
	}{
		{
			name:     "integer literal",
			input:    &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
			expected: TK_INTEGER,
		},
		{
			name:     "float literal",
			input:    &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"},
			expected: TK_FLOAT,
		},
		{
			name:     "string literal",
			input:    &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
			expected: TK_STRING,
		},
		{
			name:     "null literal",
			input:    &parser.LiteralExpr{Type: parser.LiteralNull},
			expected: TK_NULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertExpr(tt.input)
			if result == nil {
				t.Fatal("convertExpr returned nil")
			}
			if result.Op != tt.expected {
				t.Errorf("Expected Op %d, got %d", tt.expected, result.Op)
			}
		})
	}
}

func TestGenerateCreateTriggerSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateTriggerStmt
		contains []string
	}{
		{
			name: "basic BEFORE INSERT trigger",
			stmt: &parser.CreateTriggerStmt{
				Name:       "trigger_insert",
				Table:      "users",
				Timing:     parser.TriggerBefore,
				Event:      parser.TriggerInsert,
				ForEachRow: true,
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TRIGGER", "trigger_insert", "BEFORE INSERT", "ON users", "FOR EACH ROW", "BEGIN", "END"},
		},
		{
			name: "AFTER DELETE trigger with TEMP",
			stmt: &parser.CreateTriggerStmt{
				Name:       "trigger_delete",
				Temp:       true,
				Table:      "logs",
				Timing:     parser.TriggerAfter,
				Event:      parser.TriggerDelete,
				ForEachRow: false,
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TEMP TRIGGER", "trigger_delete", "AFTER DELETE", "ON logs", "BEGIN", "END"},
		},
		{
			name: "UPDATE OF trigger",
			stmt: &parser.CreateTriggerStmt{
				Name:       "trigger_update",
				Table:      "accounts",
				Timing:     parser.TriggerBefore,
				Event:      parser.TriggerUpdate,
				UpdateOf:   []string{"balance", "status"},
				ForEachRow: true,
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TRIGGER", "trigger_update", "BEFORE UPDATE", "OF balance, status", "ON accounts", "FOR EACH ROW", "BEGIN", "END"},
		},
		{
			name: "INSTEAD OF trigger",
			stmt: &parser.CreateTriggerStmt{
				Name:       "trigger_view",
				Table:      "my_view",
				Timing:     parser.TriggerInsteadOf,
				Event:      parser.TriggerInsert,
				ForEachRow: true,
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TRIGGER", "trigger_view", "INSTEAD OF INSERT", "ON my_view", "FOR EACH ROW", "BEGIN", "END"},
		},
		{
			name: "trigger with IF NOT EXISTS",
			stmt: &parser.CreateTriggerStmt{
				Name:        "trigger_safe",
				IfNotExists: true,
				Table:       "data",
				Timing:      parser.TriggerAfter,
				Event:       parser.TriggerInsert,
				ForEachRow:  true,
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TRIGGER", "IF NOT EXISTS", "trigger_safe", "AFTER INSERT", "ON data", "BEGIN", "END"},
		},
		{
			name: "trigger with WHEN clause",
			stmt: &parser.CreateTriggerStmt{
				Name:       "trigger_conditional",
				Table:      "orders",
				Timing:     parser.TriggerBefore,
				Event:      parser.TriggerUpdate,
				ForEachRow: true,
				When:       &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Body: []parser.Statement{
					&parser.SelectStmt{},
				},
			},
			contains: []string{"CREATE TRIGGER", "trigger_conditional", "BEFORE UPDATE", "ON orders", "FOR EACH ROW", "WHEN", "BEGIN", "END"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := generateCreateTriggerSQL(tt.stmt)
			for _, substr := range tt.contains {
				if !strings.Contains(sql, substr) {
					t.Errorf("Generated SQL does not contain %q:\n%s", substr, sql)
				}
			}
		})
	}
}

func TestSchemaRemoveIndex(t *testing.T) {
	schema := NewSchema()

	index := &Index{
		Name:    "idx_users_id",
		Table:   "users",
		Columns: []string{"id"},
	}

	schema.AddIndex(index)

	err := schema.RemoveIndex("idx_users_id")
	if err != nil {
		t.Fatalf("RemoveIndex failed: %v", err)
	}

	if _, exists := schema.Indexes["idx_users_id"]; exists {
		t.Error("Index still exists after removal")
	}

	// Try to remove non-existent index
	err = schema.RemoveIndex("nonexistent")
	if err == nil {
		t.Error("Expected error when removing non-existent index")
	}
}

func TestCompileCreateView(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create a simple SELECT statement for the view
	selectStmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	tests := []struct {
		name        string
		stmt        *parser.CreateViewStmt
		wantErr     bool
		errContains string
	}{
		{
			name: "basic view",
			stmt: &parser.CreateViewStmt{
				Name:   "user_view",
				Select: selectStmt,
			},
			wantErr: false,
		},
		{
			name: "view with IF NOT EXISTS",
			stmt: &parser.CreateViewStmt{
				Name:        "user_view2",
				Select:      selectStmt,
				IfNotExists: true,
			},
			wantErr: false,
		},
		{
			name: "temporary view",
			stmt: &parser.CreateViewStmt{
				Name:      "temp_view",
				Select:    selectStmt,
				Temporary: true,
			},
			wantErr: false,
		},
		{
			name: "view with columns",
			stmt: &parser.CreateViewStmt{
				Name:    "view_with_cols",
				Columns: []string{"user_id", "user_name"},
				Select:  selectStmt,
			},
			wantErr: false,
		},
		{
			name: "empty view name",
			stmt: &parser.CreateViewStmt{
				Name:   "",
				Select: selectStmt,
			},
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name: "reserved view name",
			stmt: &parser.CreateViewStmt{
				Name:   "sqlite_master",
				Select: selectStmt,
			},
			wantErr:     true,
			errContains: "reserved",
		},
		{
			name: "no SELECT statement",
			stmt: &parser.CreateViewStmt{
				Name:   "bad_view",
				Select: nil,
			},
			wantErr:     true,
			errContains: "SELECT statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := CompileCreateView(tt.stmt, schema, bt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if v == nil {
					t.Error("Expected VDBE, got nil")
				} else {
					// Add view to schema for subsequent tests
					schema.Views[tt.stmt.Name] = &View{
						Name:   tt.stmt.Name,
						Select: tt.stmt.Select,
					}
				}
			}
		})
	}
}

func TestCompileCreateViewDuplicate(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	selectStmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
	}

	// Add a view first
	schema.Views = map[string]*View{
		"existing_view": {
			Name:   "existing_view",
			Select: selectStmt,
		},
	}

	// Try to create duplicate without IF NOT EXISTS
	stmt := &parser.CreateViewStmt{
		Name:   "existing_view",
		Select: selectStmt,
	}
	_, err := CompileCreateView(stmt, schema, bt)
	if err == nil {
		t.Error("Expected error for duplicate view")
	}

	// Try with IF NOT EXISTS
	stmt.IfNotExists = true
	v, err := CompileCreateView(stmt, schema, bt)
	if err != nil {
		t.Errorf("IF NOT EXISTS should not error: %v", err)
	}
	if v == nil {
		t.Error("Expected VDBE")
	}
}

func TestCompileCreateViewConflictWithTable(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Add a table first
	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   2,
	}
	schema.AddTable(table)

	selectStmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
	}

	// Try to create view with same name as table
	stmt := &parser.CreateViewStmt{
		Name:   "users",
		Select: selectStmt,
	}
	_, err := CompileCreateView(stmt, schema, bt)
	if err == nil {
		t.Error("Expected error when view name conflicts with table")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Expected 'already exists' error, got: %v", err)
	}
}

func TestCompileDropView(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Add a view
	schema.Views = map[string]*View{
		"test_view": {
			Name: "test_view",
		},
		"sqlite_master": {
			Name: "sqlite_master",
		},
	}

	tests := []struct {
		name        string
		stmt        *parser.DropViewStmt
		wantErr     bool
		errContains string
	}{
		{
			name: "basic drop",
			stmt: &parser.DropViewStmt{
				Name: "test_view",
			},
			wantErr: false,
		},
		{
			name: "drop with IF EXISTS",
			stmt: &parser.DropViewStmt{
				Name:     "nonexistent",
				IfExists: true,
			},
			wantErr: false,
		},
		{
			name: "drop nonexistent without IF EXISTS",
			stmt: &parser.DropViewStmt{
				Name: "nonexistent2",
			},
			wantErr:     true,
			errContains: "does not exist",
		},
		{
			name: "drop reserved view",
			stmt: &parser.DropViewStmt{
				Name: "sqlite_master",
			},
			wantErr:     true,
			errContains: "system view",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := CompileDropView(tt.stmt, schema, bt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if v == nil {
					t.Error("Expected VDBE, got nil")
				}
			}
		})
	}
}

func TestGenerateCreateViewSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateViewStmt
		contains []string
	}{
		{
			name: "basic view",
			stmt: &parser.CreateViewStmt{
				Name: "my_view",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.IdentExpr{Name: "id"}},
					},
				},
			},
			contains: []string{"CREATE", "VIEW", "my_view"},
		},
		{
			name: "temporary view",
			stmt: &parser.CreateViewStmt{
				Name:      "temp_view",
				Temporary: true,
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.IdentExpr{Name: "id"}},
					},
				},
			},
			contains: []string{"CREATE", "TEMP", "VIEW", "temp_view"},
		},
		{
			name: "view with IF NOT EXISTS",
			stmt: &parser.CreateViewStmt{
				Name:        "safe_view",
				IfNotExists: true,
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.IdentExpr{Name: "id"}},
					},
				},
			},
			contains: []string{"CREATE", "VIEW", "IF NOT EXISTS", "safe_view"},
		},
		{
			name: "view with columns",
			stmt: &parser.CreateViewStmt{
				Name:    "cols_view",
				Columns: []string{"col1", "col2"},
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{
						{Expr: &parser.IdentExpr{Name: "id"}},
					},
				},
			},
			contains: []string{"CREATE", "VIEW", "cols_view", "(", "col1", "col2", ")"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := generateCreateViewSQL(tt.stmt)
			for _, substr := range tt.contains {
				if !strings.Contains(sql, substr) {
					t.Errorf("Generated SQL does not contain %q:\n%s", substr, sql)
				}
			}
		})
	}
}

func TestCompileCreateTrigger(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Add a table for the trigger
	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
		RootPage: 2,
	}
	schema.AddTable(table)

	tests := []struct {
		name        string
		stmt        *parser.CreateTriggerStmt
		wantErr     bool
		errContains string
	}{
		{
			name: "basic trigger",
			stmt: &parser.CreateTriggerStmt{
				Name:   "my_trigger",
				Table:  "users",
				Timing: parser.TriggerAfter,
				Event:  parser.TriggerInsert,
			},
			wantErr: false,
		},
		{
			name: "empty trigger name",
			stmt: &parser.CreateTriggerStmt{
				Name:  "",
				Table: "users",
			},
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name: "table not found",
			stmt: &parser.CreateTriggerStmt{
				Name:  "bad_trigger",
				Table: "nonexistent",
			},
			wantErr:     true,
			errContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := CompileCreateTrigger(tt.stmt, schema, bt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if v == nil {
					t.Error("Expected VDBE, got nil")
				}
			}
		})
	}
}

func TestCompileDropTrigger(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	tests := []struct {
		name        string
		stmt        *parser.DropTriggerStmt
		wantErr     bool
		errContains string
	}{
		{
			name: "basic drop",
			stmt: &parser.DropTriggerStmt{
				Name: "my_trigger",
			},
			wantErr: false,
		},
		{
			name: "drop with IF EXISTS",
			stmt: &parser.DropTriggerStmt{
				Name:     "any_trigger",
				IfExists: true,
			},
			wantErr: false,
		},
		{
			name: "drop reserved trigger",
			stmt: &parser.DropTriggerStmt{
				Name: "sqlite_master",
			},
			wantErr:     true,
			errContains: "system trigger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := CompileDropTrigger(tt.stmt, schema, bt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if v == nil {
					t.Error("Expected VDBE, got nil")
				}
			}
		})
	}
}

// Test Schema.AddIndex with duplicate
func TestSchemaAddIndexDuplicate(t *testing.T) {
	schema := NewSchema()

	index := &Index{
		Name:    "idx_test",
		Table:   "users",
		Columns: []string{"id"},
	}

	err := schema.AddIndex(index)
	if err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	// Try to add duplicate
	err = schema.AddIndex(index)
	if err == nil {
		t.Error("Expected error when adding duplicate index")
	}
}

// Test findColumnIndex with non-existent column
func TestFindColumnIndexNotFound(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
	}

	idx := findColumnIndex(table, "nonexistent")
	if idx != -1 {
		t.Errorf("findColumnIndex for nonexistent column = %d, want -1", idx)
	}
}

// Test findColumnIndex with existing column
func TestFindColumnIndexFound(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
			{Name: "email", DeclType: "TEXT"},
		},
	}

	idx := findColumnIndex(table, "email")
	if idx != 2 {
		t.Errorf("findColumnIndex for 'email' = %d, want 2", idx)
	}
}

// Test convertExpr with column reference
func TestConvertExprColumnRef(t *testing.T) {
	expr := &parser.IdentExpr{Name: "column1"}
	result := convertExpr(expr)

	if result == nil {
		t.Fatal("convertExpr returned nil")
	}

	// Should convert to appropriate type
	if result.Op == 0 {
		t.Error("Result Op should be set")
	}
}

// Test allocateRootPage multiple times
func TestAllocateRootPageSequence(t *testing.T) {
	bt := btree.NewBtree(4096)

	page1 := allocateRootPage(bt)
	if page1 < 2 {
		t.Errorf("First page = %d, should be >= 2", page1)
	}

	// Add the page
	bt.SetPage(page1, make([]byte, 4096))

	page2 := allocateRootPage(bt)
	if page2 <= page1 {
		t.Errorf("Second page %d should be > first page %d", page2, page1)
	}
}

// Test typeNameToAffinity with mixed case
func TestTypeNameToAffinityMixedCase(t *testing.T) {
	tests := []struct {
		typeName string
		expected Affinity
	}{
		{"InTeGeR", SQLITE_AFF_INTEGER},
		{"TeXt", SQLITE_AFF_TEXT},
		{"REAL", SQLITE_AFF_REAL},
		{"blob", SQLITE_AFF_BLOB},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := typeNameToAffinity(tt.typeName)
			if result != tt.expected {
				t.Errorf("typeNameToAffinity(%q) = %v, want %v", tt.typeName, result, tt.expected)
			}
		})
	}
}

// Test typeNameToAffinity with type containing numbers
func TestTypeNameToAffinityWithNumbers(t *testing.T) {
	tests := []struct {
		typeName string
		expected Affinity
	}{
		{"INT8", SQLITE_AFF_INTEGER},
		{"VARCHAR2", SQLITE_AFF_TEXT},
		{"FLOAT64", SQLITE_AFF_REAL},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := typeNameToAffinity(tt.typeName)
			if result != tt.expected {
				t.Errorf("typeNameToAffinity(%q) = %v, want %v", tt.typeName, result, tt.expected)
			}
		})
	}
}

// Test CompileCreateIndex with DESC order
func TestCompileCreateIndexDescOrder(t *testing.T) {
	schema := NewSchema()
	bt := btree.NewBtree(4096)

	// Create table first
	createTableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "created_at", Type: "INTEGER"},
		},
	}

	_, err := CompileCreateTable(createTableStmt, schema, bt)
	if err != nil {
		t.Fatalf("Create table failed: %v", err)
	}

	// Create index with DESC
	createIndexStmt := &parser.CreateIndexStmt{
		Name:  "idx_created_desc",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "created_at", Order: parser.SortDesc},
		},
	}

	v, err := CompileCreateIndex(createIndexStmt, schema, bt)
	if err != nil {
		t.Fatalf("CompileCreateIndex with DESC failed: %v", err)
	}

	if v == nil {
		t.Fatal("VDBE is nil")
	}
}

// Test generateCreateTableSQL with multiple constraints
func TestGenerateCreateTableSQLMultipleConstraints(t *testing.T) {
	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "email",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintNotNull},
					{Type: parser.ConstraintUnique},
				},
			},
		},
	}

	sql := generateCreateTableSQL(stmt)
	if !strings.Contains(sql, "NOT NULL") {
		t.Error("SQL should contain NOT NULL")
	}
	if !strings.Contains(sql, "UNIQUE") {
		t.Error("SQL should contain UNIQUE")
	}
}
