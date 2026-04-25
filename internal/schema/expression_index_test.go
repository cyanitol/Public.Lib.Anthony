// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// createSchemaWithTable creates a schema and a table with the given columns.
func createSchemaWithTable(t *testing.T, tableName string, columns []parser.ColumnDef) *Schema {
	t.Helper()
	s := NewSchema()
	_, err := s.CreateTable(&parser.CreateTableStmt{Name: tableName, Columns: columns})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	return s
}

// createIndexFromSQL parses and creates an index on the given schema.
func createIndexFromSQL(t *testing.T, s *Schema, sql string) *Index {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	index, err := s.CreateIndex(stmts[0].(*parser.CreateIndexStmt))
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	return index
}

// assertExprIndexBasic checks basic index fields (name, table, single column).
func assertExprIndexBasic(t *testing.T, index *Index, wantName, wantTable, wantCol string) {
	t.Helper()
	if index.Name != wantName {
		t.Errorf("Expected index name %q, got %q", wantName, index.Name)
	}
	if index.Table != wantTable {
		t.Errorf("Expected table %q, got %q", wantTable, index.Table)
	}
	if len(index.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(index.Columns))
	}
	if index.Columns[0] != wantCol {
		t.Errorf("Expected column %q, got %q", wantCol, index.Columns[0])
	}
}

// assertExprIndexFunc checks that the first expression is a FunctionExpr with the given name.
func assertExprIndexFunc(t *testing.T, index *Index, wantFunc string) {
	t.Helper()
	if len(index.Expressions) != 1 || index.Expressions[0] == nil {
		t.Fatal("Expected 1 non-nil expression")
	}
	funcExpr, ok := index.Expressions[0].(*parser.FunctionExpr)
	if !ok {
		t.Fatalf("Expected FunctionExpr, got %T", index.Expressions[0])
	}
	if funcExpr.Name != wantFunc {
		t.Errorf("Expected %s function, got %s", wantFunc, funcExpr.Name)
	}
}

// TestExpressionIndexCreation tests that expression indexes are stored correctly in the schema
func TestExpressionIndexCreation(t *testing.T) {
	t.Parallel()
	s := createSchemaWithTable(t, "users", []parser.ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "TEXT"},
		{Name: "email", Type: "TEXT"},
	})

	index := createIndexFromSQL(t, s, "CREATE INDEX idx_lower_name ON users(LOWER(name))")
	assertExprIndexBasic(t, index, "idx_lower_name", "users", "LOWER(name)")
	assertExprIndexFunc(t, index, "LOWER")
}

// TestMixedExpressionIndex tests an index with both expressions and regular columns
func TestMixedExpressionIndex(t *testing.T) {
	t.Parallel()
	s := createSchemaWithTable(t, "people", []parser.ColumnDef{
		{Name: "first_name", Type: "TEXT"},
		{Name: "last_name", Type: "TEXT"},
		{Name: "age", Type: "INTEGER"},
	})

	index := createIndexFromSQL(t, s, "CREATE INDEX idx_name_age ON people(LOWER(last_name), age)")

	if len(index.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(index.Columns))
	}
	if index.Columns[0] != "LOWER(last_name)" {
		t.Errorf("Expected 'LOWER(last_name)', got '%s'", index.Columns[0])
	}
	if index.Columns[1] != "age" {
		t.Errorf("Expected 'age', got '%s'", index.Columns[1])
	}
	if len(index.Expressions) != 2 {
		t.Fatalf("Expected 2 expressions, got %d", len(index.Expressions))
	}
	if index.Expressions[0] == nil {
		t.Error("First expression should not be nil")
	}
	if index.Expressions[1] == nil {
		t.Error("Second expression should not be nil")
	}
}

// TestMultipleExpressionIndex tests an index with multiple expressions
func TestMultipleExpressionIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tableStmt := &parser.CreateTableStmt{
		Name: "records",
		Columns: []parser.ColumnDef{
			{Name: "first", Type: "TEXT"},
			{Name: "last", Type: "TEXT"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Index with two expressions
	p := parser.NewParser("CREATE INDEX idx_names ON records(LOWER(last), LOWER(first))")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if len(index.Expressions) != 2 {
		t.Fatalf("Expected 2 expressions, got %d", len(index.Expressions))
	}

	// Both should be expressions
	if index.Expressions[0] == nil || index.Expressions[1] == nil {
		t.Error("Both expressions should be non-nil")
	}
}

// TestArithmeticExpressionIndex tests an index with arithmetic expressions
func TestArithmeticExpressionIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tableStmt := &parser.CreateTableStmt{
		Name: "sales",
		Columns: []parser.ColumnDef{
			{Name: "price", Type: "REAL"},
			{Name: "tax", Type: "REAL"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Expression index on price + tax
	p := parser.NewParser("CREATE INDEX idx_total ON sales(price + tax)")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if len(index.Expressions) != 1 {
		t.Fatalf("Expected 1 expression, got %d", len(index.Expressions))
	}

	// Should be a binary expression
	if binExpr, ok := index.Expressions[0].(*parser.BinaryExpr); ok {
		if binExpr.Op != parser.OpPlus {
			t.Errorf("Expected OpPlus, got %v", binExpr.Op)
		}
	} else {
		t.Errorf("Expected BinaryExpr, got %T", index.Expressions[0])
	}
}

// TestUniqueExpressionIndex tests unique expression indexes
func TestUniqueExpressionIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tableStmt := &parser.CreateTableStmt{
		Name: "emails",
		Columns: []parser.ColumnDef{
			{Name: "email", Type: "TEXT"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	p := parser.NewParser("CREATE UNIQUE INDEX idx_email_lower ON emails(LOWER(email))")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if !index.Unique {
		t.Error("Expected index to be unique")
	}

	if len(index.Expressions) != 1 || index.Expressions[0] == nil {
		t.Error("Expected expression to be set")
	}
}

// TestPartialExpressionIndex tests partial expression indexes (with WHERE)
func TestPartialExpressionIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tableStmt := &parser.CreateTableStmt{
		Name: "items",
		Columns: []parser.ColumnDef{
			{Name: "name", Type: "TEXT"},
			{Name: "active", Type: "INTEGER"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	p := parser.NewParser("CREATE INDEX idx_active_lower_name ON items(LOWER(name)) WHERE active = 1")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if !index.Partial {
		t.Error("Expected index to be partial")
	}

	if index.Where == "" {
		t.Error("Expected WHERE clause to be set")
	}

	if len(index.Expressions) != 1 || index.Expressions[0] == nil {
		t.Error("Expected expression to be set")
	}
}
