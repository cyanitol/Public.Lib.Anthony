package schema

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// TestExpressionIndexCreation tests that expression indexes are stored correctly in the schema
func TestExpressionIndexCreation(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a table
	tableStmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Parse an expression index
	p := parser.NewParser("CREATE INDEX idx_lower_name ON users(LOWER(name))")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)

	// Create the index
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index properties
	if index.Name != "idx_lower_name" {
		t.Errorf("Expected index name 'idx_lower_name', got '%s'", index.Name)
	}

	if index.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", index.Table)
	}

	if len(index.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(index.Columns))
	}

	// Check that column name was extracted
	if index.Columns[0] != "LOWER(name)" {
		t.Errorf("Expected column 'LOWER(name)', got '%s'", index.Columns[0])
	}

	// Check that expression was stored
	if len(index.Expressions) != 1 {
		t.Fatalf("Expected 1 expression, got %d", len(index.Expressions))
	}

	if index.Expressions[0] == nil {
		t.Error("Expression should not be nil")
	}

	// Verify it's a function expression
	if funcExpr, ok := index.Expressions[0].(*parser.FunctionExpr); ok {
		if funcExpr.Name != "LOWER" {
			t.Errorf("Expected LOWER function, got %s", funcExpr.Name)
		}
	} else {
		t.Errorf("Expected FunctionExpr, got %T", index.Expressions[0])
	}
}

// TestMixedExpressionIndex tests an index with both expressions and regular columns
func TestMixedExpressionIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tableStmt := &parser.CreateTableStmt{
		Name: "people",
		Columns: []parser.ColumnDef{
			{Name: "first_name", Type: "TEXT"},
			{Name: "last_name", Type: "TEXT"},
			{Name: "age", Type: "INTEGER"},
		},
	}
	_, err := s.CreateTable(tableStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Mixed index: expression + regular column
	p := parser.NewParser("CREATE INDEX idx_name_age ON people(LOWER(last_name), age)")
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	indexStmt := stmts[0].(*parser.CreateIndexStmt)
	index, err := s.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Should have 2 columns
	if len(index.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(index.Columns))
	}

	// First should be expression
	if index.Columns[0] != "LOWER(last_name)" {
		t.Errorf("Expected 'LOWER(last_name)', got '%s'", index.Columns[0])
	}

	// Second should be regular column
	if index.Columns[1] != "age" {
		t.Errorf("Expected 'age', got '%s'", index.Columns[1])
	}

	// Should have expressions array
	if len(index.Expressions) != 2 {
		t.Fatalf("Expected 2 expressions, got %d", len(index.Expressions))
	}

	// First should be a function expression
	if index.Expressions[0] == nil {
		t.Error("First expression should not be nil")
	}

	// Second should also be an expression (IdentExpr for regular columns)
	if index.Expressions[1] == nil {
		t.Error("Second expression should not be nil (all columns have expressions)")
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
