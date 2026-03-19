// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestNewCompiler tests compiler initialization.
func TestNewCompiler(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	if compiler == nil {
		t.Fatal("Compiler is nil")
	}
	if compiler.engine != db {
		t.Error("Compiler engine not set correctly")
	}
	if len(compiler.handlers) == 0 {
		t.Error("Compiler handlers not initialized")
	}
}

// TestCompileUnsupportedStatement tests compilation of unsupported statement types.
func TestCompileUnsupportedStatement(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)

	// Try to compile an unsupported statement type (use nil as a placeholder)
	_, err = compiler.Compile(nil)
	if err == nil {
		t.Error("Expected error for unsupported statement type")
	}
}

// TestCompileUpdate tests UPDATE statement compilation.
func TestCompileUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Execute("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	compiler := NewCompiler(db)
	stmt := &parser.UpdateStmt{
		Table: "users",
		Sets: []parser.Assignment{
			{Column: "name", Value: &parser.LiteralExpr{Type: parser.LiteralString, Value: "Alice"}},
		},
	}

	vm, err := compiler.CompileUpdate(stmt)
	if err != nil {
		t.Fatalf("Failed to compile UPDATE: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if vm.IsReadOnly() {
		t.Error("UPDATE should not be read-only")
	}
}

// TestCompileUpdateTableNotFound tests UPDATE with non-existent table.
func TestCompileUpdateTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.UpdateStmt{
		Table: "nonexistent",
		Sets: []parser.Assignment{
			{Column: "col", Value: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}

	_, err = compiler.CompileUpdate(stmt)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCompileDelete tests DELETE statement compilation.
func TestCompileDelete(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Execute("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	compiler := NewCompiler(db)
	stmt := &parser.DeleteStmt{
		Table: "users",
	}

	vm, err := compiler.CompileDelete(stmt)
	if err != nil {
		t.Fatalf("Failed to compile DELETE: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if vm.IsReadOnly() {
		t.Error("DELETE should not be read-only")
	}
}

// TestCompileDeleteTableNotFound tests DELETE with non-existent table.
func TestCompileDeleteTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.DeleteStmt{
		Table: "nonexistent",
	}

	_, err = compiler.CompileDelete(stmt)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCompileCreateIndex tests CREATE INDEX compilation.
func TestCompileCreateIndex(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Execute("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	compiler := NewCompiler(db)
	stmt := &parser.CreateIndexStmt{
		Name:  "idx_name",
		Table: "users",
		Columns: []parser.IndexedColumn{
			{Column: "name"},
		},
	}

	vm, err := compiler.CompileCreateIndex(stmt)
	if err != nil {
		t.Fatalf("Failed to compile CREATE INDEX: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if vm.IsReadOnly() {
		t.Error("CREATE INDEX should not be read-only")
	}

	// Verify index was created in schema
	index, ok := db.schema.GetIndex("idx_name")
	if !ok {
		t.Error("Index not found in schema")
	}
	if index.Name != "idx_name" {
		t.Errorf("Expected index name 'idx_name', got '%s'", index.Name)
	}
}

// TestCompileDropTable tests DROP TABLE compilation.
func TestCompileDropTable(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Execute("CREATE TABLE testtbl (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	compiler := NewCompiler(db)
	stmt := &parser.DropTableStmt{
		Name: "testtbl",
	}

	vm, err := compiler.CompileDropTable(stmt)
	if err != nil {
		t.Fatalf("Failed to compile DROP TABLE: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}

	// Verify table was dropped from schema
	_, ok := db.schema.GetTable("testtbl")
	if ok {
		t.Error("Table should have been dropped from schema")
	}
}

// TestCompileDropTableIfExists tests DROP TABLE IF EXISTS.
func TestCompileDropTableIfExists(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.DropTableStmt{
		Name:     "nonexistent",
		IfExists: true,
	}

	vm, err := compiler.CompileDropTable(stmt)
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS should not error: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
}

// TestCompileDropTableNotFound tests DROP TABLE for non-existent table.
func TestCompileDropTableNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.DropTableStmt{
		Name:     "nonexistent",
		IfExists: false,
	}

	_, err = compiler.CompileDropTable(stmt)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestCompileDropIndex tests DROP INDEX compilation.
func TestCompileDropIndex(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table and index first
	_, err = db.Execute("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Execute("CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	compiler := NewCompiler(db)
	stmt := &parser.DropIndexStmt{
		Name: "idx_name",
	}

	vm, err := compiler.CompileDropIndex(stmt)
	if err != nil {
		t.Fatalf("Failed to compile DROP INDEX: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}

	// Verify index was dropped from schema
	_, ok := db.schema.GetIndex("idx_name")
	if ok {
		t.Error("Index should have been dropped from schema")
	}
}

// TestCompileDropIndexIfExists tests DROP INDEX IF EXISTS.
func TestCompileDropIndexIfExists(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.DropIndexStmt{
		Name:     "nonexistent",
		IfExists: true,
	}

	vm, err := compiler.CompileDropIndex(stmt)
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS should not error: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
}

// TestCompileDropIndexNotFound tests DROP INDEX for non-existent index.
func TestCompileDropIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.DropIndexStmt{
		Name:     "nonexistent",
		IfExists: false,
	}

	_, err = compiler.CompileDropIndex(stmt)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestCompileBegin tests BEGIN statement compilation.
func TestCompileBegin(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.BeginStmt{}

	vm, err := compiler.CompileBegin(stmt)
	if err != nil {
		t.Fatalf("Failed to compile BEGIN: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if !vm.InTxn {
		t.Error("VDBE should be in transaction")
	}
	if vm.IsReadOnly() {
		t.Error("BEGIN should not be read-only")
	}
}

// TestCompileCommit tests COMMIT statement compilation.
func TestCompileCommit(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.CommitStmt{}

	vm, err := compiler.CompileCommit(stmt)
	if err != nil {
		t.Fatalf("Failed to compile COMMIT: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if vm.IsReadOnly() {
		t.Error("COMMIT should not be read-only")
	}
}

// TestCompileRollback tests ROLLBACK statement compilation.
func TestCompileRollback(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compiler := NewCompiler(db)
	stmt := &parser.RollbackStmt{}

	vm, err := compiler.CompileRollback(stmt)
	if err != nil {
		t.Fatalf("Failed to compile ROLLBACK: %v", err)
	}
	if vm == nil {
		t.Fatal("VDBE is nil")
	}
	if vm.IsReadOnly() {
		t.Error("ROLLBACK should not be read-only")
	}
}

// TestResolveColumnIndex tests resolving column indexes.
func TestResolveColumnIndex(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "age"},
		},
	}

	tests := []struct {
		name      string
		col       parser.ResultColumn
		wantIdx   int
		wantError bool
	}{
		{
			name: "valid column",
			col: parser.ResultColumn{
				Expr: &parser.IdentExpr{Name: "name"},
			},
			wantIdx:   1,
			wantError: false,
		},
		{
			name: "non-existent column",
			col: parser.ResultColumn{
				Expr: &parser.IdentExpr{Name: "nonexistent"},
			},
			wantIdx:   0,
			wantError: true,
		},
		{
			name: "non-ident expression",
			col: parser.ResultColumn{
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			wantIdx:   0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := resolveColumnIndex(tt.col, table)
			if tt.wantError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if idx != tt.wantIdx {
				t.Errorf("Expected index %d, got %d", tt.wantIdx, idx)
			}
		})
	}
}

// TestResolveOneTableColName tests resolving column names.
func TestResolveOneTableColName(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	tests := []struct {
		name     string
		index    int
		col      parser.ResultColumn
		wantName string
	}{
		{
			name:  "column with alias",
			index: 0,
			col: parser.ResultColumn{
				Alias: "my_alias",
				Expr:  &parser.IdentExpr{Name: "id"},
			},
			wantName: "my_alias",
		},
		{
			name:  "star column",
			index: 0,
			col: parser.ResultColumn{
				Star: true,
			},
			wantName: "id",
		},
		{
			name:  "ident expression",
			index: 0,
			col: parser.ResultColumn{
				Expr: &parser.IdentExpr{Name: "name"},
			},
			wantName: "name",
		},
		{
			name:  "default name",
			index: 2,
			col: parser.ResultColumn{
				Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			wantName: "column_2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := resolveOneTableColName(tt.index, tt.col, table)
			if name != tt.wantName {
				t.Errorf("Expected name '%s', got '%s'", tt.wantName, name)
			}
		})
	}
}

// TestResolveTableColNames tests resolving multiple column names.
func TestResolveTableColNames(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	cols := []parser.ResultColumn{
		{Expr: &parser.IdentExpr{Name: "id"}},
		{Alias: "fullname", Expr: &parser.IdentExpr{Name: "name"}},
	}

	names := resolveTableColNames(cols, table)
	if len(names) != 2 {
		t.Fatalf("Expected 2 names, got %d", len(names))
	}
	if names[0] != "id" {
		t.Errorf("Expected first name 'id', got '%s'", names[0])
	}
	if names[1] != "fullname" {
		t.Errorf("Expected second name 'fullname', got '%s'", names[1])
	}
}

// TestEmitColumnOps tests emitting column operations.
func TestEmitColumnOps(t *testing.T) {
	vm := vdbe.New()
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "age"},
		},
	}

	tests := []struct {
		name      string
		cols      []parser.ResultColumn
		wantError bool
	}{
		{
			name: "star column",
			cols: []parser.ResultColumn{
				{Star: true},
			},
			wantError: false,
		},
		{
			name: "specific column",
			cols: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "name"}},
			},
			wantError: false,
		},
		{
			name: "non-existent column",
			cols: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "nonexistent"}},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := emitColumnOps(vm, 0, tt.cols, table)
			if tt.wantError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestCollectQueryTables tests collecting tables from a query.
func TestCollectQueryTables(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Execute("CREATE TABLE users (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}
	_, err = db.Execute("CREATE TABLE posts (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	compiler := NewCompiler(db)
	usersTable, _ := db.schema.GetTable("users")

	tests := []struct {
		name      string
		stmt      *parser.SelectStmt
		wantCount int
		wantError bool
	}{
		{
			name: "single table",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
				},
			},
			wantCount: 1,
			wantError: false,
		},
		{
			name: "table with join",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{TableName: "posts"}},
					},
				},
			},
			wantCount: 2,
			wantError: false,
		},
		{
			name: "join with non-existent table",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{TableName: "nonexistent"}},
					},
				},
			},
			wantCount: 0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, err := compiler.collectQueryTables(tt.stmt, "users", usersTable)
			checkErrorExpectation(t, err, tt.wantError)
			if !tt.wantError && err == nil && len(tables) != tt.wantCount {
				t.Errorf("Expected %d tables, got %d", tt.wantCount, len(tables))
			}
		})
	}
}

func checkErrorExpectation(t *testing.T, err error, wantError bool) {
	t.Helper()
	if wantError && err == nil {
		t.Error("Expected error but got none")
	}
	if !wantError && err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}
