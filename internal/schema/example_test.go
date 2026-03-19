// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema_test

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// Example demonstrates basic schema management operations.
func Example() {
	// Create a new schema
	s := schema.NewSchema()

	// Parse and create a table
	p := parser.NewParser("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
	stmts, _ := p.Parse()

	table, _ := s.CreateTable(stmts[0].(*parser.CreateTableStmt))

	fmt.Printf("Created table: %s\n", table.Name)
	fmt.Printf("Number of columns: %d\n", len(table.Columns))
	fmt.Printf("Has rowid: %v\n", table.HasRowID())

	// List all tables
	tables := s.ListTables()
	fmt.Printf("Tables: %v\n", tables)

	// Output:
	// Created table: users
	// Number of columns: 3
	// Has rowid: true
	// Tables: [users]
}

// ExampleDetermineAffinity demonstrates SQLite type affinity rules.
func ExampleDetermineAffinity() {
	// INTEGER affinity
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("INTEGER")))
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("BIGINT")))

	// TEXT affinity
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("VARCHAR(100)")))
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("TEXT")))

	// NUMERIC affinity
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("DECIMAL(10,2)")))

	// REAL affinity
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("DOUBLE")))

	// BLOB affinity (empty type)
	fmt.Println(schema.AffinityName(schema.DetermineAffinity("")))

	// Output:
	// INTEGER
	// INTEGER
	// TEXT
	// TEXT
	// NUMERIC
	// REAL
	// BLOB
}

// ExampleSchema_CreateTable demonstrates creating a table with various column types.
func ExampleSchema_CreateTable() {
	s := schema.NewSchema()

	sql := `CREATE TABLE products (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		price REAL,
		quantity INTEGER DEFAULT 0,
		created_at TEXT
	)`

	p := parser.NewParser(sql)
	stmts, _ := p.Parse()
	table, _ := s.CreateTable(stmts[0].(*parser.CreateTableStmt))

	for _, col := range table.Columns {
		fmt.Printf("%s: %s (affinity: %s)\n",
			col.Name, col.Type, schema.AffinityName(col.Affinity))
	}

	// Output:
	// id: INTEGER (affinity: INTEGER)
	// name: TEXT (affinity: TEXT)
	// price: REAL (affinity: REAL)
	// quantity: INTEGER (affinity: INTEGER)
	// created_at: TEXT (affinity: TEXT)
}

// ExampleSchema_CreateIndex demonstrates creating indexes programmatically.
func ExampleSchema_CreateIndex() {
	s := schema.NewSchema()

	// Create a table first
	tableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}
	s.CreateTable(tableStmt)

	// Create a unique index on email
	indexStmt := &parser.CreateIndexStmt{
		Name:   "idx_users_email",
		Table:  "users",
		Unique: true,
		Columns: []parser.IndexedColumn{
			{Column: "email"},
		},
	}
	index, _ := s.CreateIndex(indexStmt)

	fmt.Printf("Index: %s\n", index.Name)
	fmt.Printf("Table: %s\n", index.Table)
	fmt.Printf("Columns: %v\n", index.Columns)
	fmt.Printf("Unique: %v\n", index.Unique)

	// Output:
	// Index: idx_users_email
	// Table: users
	// Columns: [email]
	// Unique: true
}

// ExampleSchema_GetTable demonstrates case-insensitive table lookup.
func ExampleSchema_GetTable() {
	s := schema.NewSchema()

	// Create a table
	p := parser.NewParser("CREATE TABLE MyTable (id INTEGER)")
	stmts, _ := p.Parse()
	s.CreateTable(stmts[0].(*parser.CreateTableStmt))

	// Look up the table with different cases
	table1, ok1 := s.GetTable("MyTable")
	table2, ok2 := s.GetTable("mytable")
	table3, ok3 := s.GetTable("MYTABLE")

	fmt.Printf("MyTable found: %v, name: %s\n", ok1, table1.Name)
	fmt.Printf("mytable found: %v, name: %s\n", ok2, table2.Name)
	fmt.Printf("MYTABLE found: %v, name: %s\n", ok3, table3.Name)

	// Output:
	// MyTable found: true, name: MyTable
	// mytable found: true, name: MyTable
	// MYTABLE found: true, name: MyTable
}

// ExampleTable_GetColumn demonstrates column lookup.
func ExampleTable_GetColumn() {
	s := schema.NewSchema()

	p := parser.NewParser("CREATE TABLE users (UserID INTEGER, UserName TEXT)")
	stmts, _ := p.Parse()
	table, _ := s.CreateTable(stmts[0].(*parser.CreateTableStmt))

	// Case-insensitive column lookup
	col, ok := table.GetColumn("username")
	if ok {
		fmt.Printf("Column: %s, Type: %s, Affinity: %s\n",
			col.Name, col.Type, schema.AffinityName(col.Affinity))
	}

	// Output:
	// Column: UserName, Type: TEXT, Affinity: TEXT
}

// ExampleSchema_GetTableIndexes demonstrates retrieving all indexes for a table.
func ExampleSchema_GetTableIndexes() {
	s := schema.NewSchema()

	// Create table
	tableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
		},
	}
	s.CreateTable(tableStmt)

	// Create multiple indexes
	indexDefs := []struct {
		name    string
		columns []string
		unique  bool
	}{
		{"idx_name", []string{"name"}, false},
		{"idx_email", []string{"email"}, true},
		{"idx_id_name", []string{"id", "name"}, false},
	}

	for _, def := range indexDefs {
		cols := make([]parser.IndexedColumn, len(def.columns))
		for i, col := range def.columns {
			cols[i] = parser.IndexedColumn{Column: col}
		}
		indexStmt := &parser.CreateIndexStmt{
			Name:    def.name,
			Table:   "users",
			Columns: cols,
			Unique:  def.unique,
		}
		s.CreateIndex(indexStmt)
	}

	// Get all indexes for the table
	tableIndexes := s.GetTableIndexes("users")
	fmt.Printf("Number of indexes: %d\n", len(tableIndexes))
	for _, idx := range tableIndexes {
		fmt.Printf("  %s: %v (unique: %v)\n", idx.Name, idx.Columns, idx.Unique)
	}

	// Output:
	// Number of indexes: 3
	//   idx_email: [email] (unique: true)
	//   idx_id_name: [id name] (unique: false)
	//   idx_name: [name] (unique: false)
}

// ExampleIsNumericAffinity demonstrates checking for numeric affinities.
func ExampleIsNumericAffinity() {
	types := []string{"INTEGER", "REAL", "NUMERIC", "TEXT", "BLOB"}

	for _, typeName := range types {
		aff := schema.DetermineAffinity(typeName)
		isNumeric := schema.IsNumericAffinity(aff)
		fmt.Printf("%s: %v\n", typeName, isNumeric)
	}

	// Output:
	// INTEGER: true
	// REAL: true
	// NUMERIC: true
	// TEXT: false
	// BLOB: false
}
