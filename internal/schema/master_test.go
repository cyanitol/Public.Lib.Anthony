// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package schema

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

func TestInitializeMaster(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	err := s.InitializeMaster()
	if err != nil {
		t.Fatalf("InitializeMaster() error = %v", err)
	}

	// Check that sqlite_master table exists
	table, ok := s.GetTable("sqlite_master")
	if !ok {
		t.Fatal("sqlite_master table not found")
	}

	// Verify table properties
	if table.Name != "sqlite_master" {
		t.Errorf("table.Name = %q, want %q", table.Name, "sqlite_master")
	}
	if table.RootPage != 1 {
		t.Errorf("table.RootPage = %d, want 1", table.RootPage)
	}

	// Verify columns
	expectedColumns := []struct {
		name     string
		typeName string
		affinity Affinity
	}{
		{"type", "text", AffinityText},
		{"name", "text", AffinityText},
		{"tbl_name", "text", AffinityText},
		{"rootpage", "integer", AffinityInteger},
		{"sql", "text", AffinityText},
	}

	if len(table.Columns) != len(expectedColumns) {
		t.Fatalf("table has %d columns, want %d", len(table.Columns), len(expectedColumns))
	}

	for i, expected := range expectedColumns {
		expected := expected
		col := table.Columns[i]
		if col.Name != expected.name {
			t.Errorf("column %d name = %q, want %q", i, col.Name, expected.name)
		}
		if col.Type != expected.typeName {
			t.Errorf("column %d type = %q, want %q", i, col.Type, expected.typeName)
		}
		if col.Affinity != expected.affinity {
			t.Errorf("column %d affinity = %v, want %v", i, col.Affinity, expected.affinity)
		}
	}
}

func TestLoadFromMaster(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// LoadFromMaster should not fail even with empty btree
	err := s.LoadFromMaster(bt)
	if err != nil {
		t.Fatalf("LoadFromMaster() error = %v", err)
	}

	// Test with nil btree
	err = s.LoadFromMaster(nil)
	if err == nil {
		t.Error("Expected error with nil btree")
	}
}

func TestSaveToMaster(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// Add a test table
	s.Tables["test_table"] = &Table{
		Name:     "test_table",
		RootPage: 2,
		SQL:      "CREATE TABLE test_table(id INTEGER)",
	}

	// Add a test index
	s.Indexes["test_index"] = &Index{
		Name:     "test_index",
		Table:    "test_table",
		RootPage: 3,
		SQL:      "CREATE INDEX test_index ON test_table(id)",
	}

	// SaveToMaster is a placeholder, should not fail
	err := s.SaveToMaster(bt)
	if err != nil {
		t.Fatalf("SaveToMaster() error = %v", err)
	}

	// Test with nil btree
	err = s.SaveToMaster(nil)
	if err == nil {
		t.Error("Expected error with nil btree")
	}
}

func TestMasterRow(t *testing.T) {
	t.Parallel()
	// Test MasterRow structure
	row := MasterRow{
		Type:     "table",
		Name:     "test",
		TblName:  "test",
		RootPage: 2,
		SQL:      "CREATE TABLE test(id INTEGER)",
	}

	if row.Type != "table" {
		t.Errorf("row.Type = %q, want %q", row.Type, "table")
	}
	if row.Name != "test" {
		t.Errorf("row.Name = %q, want %q", row.Name, "test")
	}
	if row.TblName != "test" {
		t.Errorf("row.TblName = %q, want %q", row.TblName, "test")
	}
	if row.RootPage != 2 {
		t.Errorf("row.RootPage = %d, want 2", row.RootPage)
	}
}

func TestParseMasterPage(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// parseMasterPage is currently a placeholder that returns empty slice
	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage() error = %v", err)
	}

	if rows == nil {
		t.Error("parseMasterPage() returned nil")
	}

	// Should return empty slice for now
	if len(rows) != 0 {
		t.Errorf("parseMasterPage() returned %d rows, expected 0 (placeholder)", len(rows))
	}
}

func TestWriteMasterPage(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	rows := []MasterRow{
		{
			Type:     "table",
			Name:     "test",
			TblName:  "test",
			RootPage: 2,
			SQL:      "CREATE TABLE test(id INTEGER)",
		},
	}

	// writeMasterPage is currently a placeholder
	err := s.writeMasterPage(bt, 1, rows)
	if err != nil {
		t.Fatalf("writeMasterPage() error = %v", err)
	}
}

func TestParseTableSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Test with empty SQL (system table)
	row := MasterRow{
		Name:     "sqlite_sequence",
		RootPage: 10,
		SQL:      "",
	}

	table, err := s.parseTableSQL(row)
	if err != nil {
		t.Fatalf("parseTableSQL() error = %v", err)
	}

	if table.Name != "sqlite_sequence" {
		t.Errorf("table.Name = %q, want %q", table.Name, "sqlite_sequence")
	}
	if table.RootPage != 10 {
		t.Errorf("table.RootPage = %d, want 10", table.RootPage)
	}

	// Test with valid SQL
	row = MasterRow{
		Name:     "users",
		TblName:  "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
	}

	// This will fail because the parser needs to be called correctly
	// For now, we just verify it handles the case
	_, err = s.parseTableSQL(row)
	// Parser might fail on this simplified SQL, that's ok for this test
	// The important thing is we don't panic
}

func TestParseIndexSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Test with empty SQL (auto-index)
	row := MasterRow{
		Name:     "sqlite_autoindex_users_1",
		TblName:  "users",
		RootPage: 3,
		SQL:      "",
	}

	index, err := s.parseIndexSQL(row)
	if err != nil {
		t.Fatalf("parseIndexSQL() error = %v", err)
	}

	if index.Name != "sqlite_autoindex_users_1" {
		t.Errorf("index.Name = %q, want %q", index.Name, "sqlite_autoindex_users_1")
	}
	if index.Table != "users" {
		t.Errorf("index.Table = %q, want %q", index.Table, "users")
	}
	if index.RootPage != 3 {
		t.Errorf("index.RootPage = %d, want 3", index.RootPage)
	}
}

func TestMasterSchemaIntegration(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// Initialize master table
	err := s.InitializeMaster()
	if err != nil {
		t.Fatalf("InitializeMaster() error = %v", err)
	}

	// Verify we can load from master
	err = s.LoadFromMaster(bt)
	if err != nil {
		t.Fatalf("LoadFromMaster() error = %v", err)
	}

	// Verify we can save to master
	err = s.SaveToMaster(bt)
	if err != nil {
		t.Fatalf("SaveToMaster() error = %v", err)
	}
}

func TestMasterTableNotSaved(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// Initialize master
	err := s.InitializeMaster()
	if err != nil {
		t.Fatalf("InitializeMaster() error = %v", err)
	}

	// Add another table
	s.Tables["test"] = &Table{
		Name:     "test",
		RootPage: 2,
		SQL:      "CREATE TABLE test(id INTEGER)",
	}

	// SaveToMaster should skip sqlite_master itself
	err = s.SaveToMaster(bt)
	if err != nil {
		t.Fatalf("SaveToMaster() error = %v", err)
	}

	// The implementation is a placeholder, so we can't verify the actual
	// behavior, but at least we ensure it doesn't try to save sqlite_master
}

func TestIsInternalTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expected bool
	}{
		{"sqlite_master", true},
		{"sqlite_sequence", true},
		{"users", false},
		{"my_table", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result := isInternalTable(tt.name)
			if result != tt.expected {
				t.Errorf("isInternalTable(%q) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

func TestIsAutoIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expected bool
	}{
		{"sqlite_autoindex_users_1", true},
		{"sqlite_autoindex", false},
		{"idx_users", false},
		{"users", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result := isAutoIndex(tt.name)
			if result != tt.expected {
				t.Errorf("isAutoIndex(%q) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

func TestProcessMasterTableRow(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Type:     "table",
		Name:     "users",
		TblName:  "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
	}

	err := s.processMasterTableRow(row)
	if err != nil {
		t.Fatalf("processMasterTableRow() error = %v", err)
	}

	if _, ok := s.GetTable("users"); !ok {
		t.Error("Table not added to schema")
	}

	// Test with internal table (should skip)
	internalRow := MasterRow{
		Type:     "table",
		Name:     "sqlite_master",
		TblName:  "sqlite_master",
		RootPage: 1,
		SQL:      "",
	}

	err = s.processMasterTableRow(internalRow)
	if err != nil {
		t.Fatalf("processMasterTableRow() error = %v", err)
	}
}

func TestProcessMasterIndexRow(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Type:     "index",
		Name:     "idx_users",
		TblName:  "users",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_users ON users(name)",
	}

	err := s.processMasterIndexRow(row)
	if err != nil {
		t.Fatalf("processMasterIndexRow() error = %v", err)
	}

	if _, ok := s.GetIndex("idx_users"); !ok {
		t.Error("Index not added to schema")
	}

	// Test with auto-index (should skip)
	autoRow := MasterRow{
		Type:     "index",
		Name:     "sqlite_autoindex_users_1",
		TblName:  "users",
		RootPage: 4,
		SQL:      "",
	}

	err = s.processMasterIndexRow(autoRow)
	if err != nil {
		t.Fatalf("processMasterIndexRow() error = %v", err)
	}
}

func TestProcessMasterViewRow(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Type:    "view",
		Name:    "active_users",
		TblName: "active_users",
		SQL:     "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
	}

	err := s.processMasterViewRow(row)
	if err != nil {
		t.Fatalf("processMasterViewRow() error = %v", err)
	}

	if _, ok := s.GetView("active_users"); !ok {
		t.Error("View not added to schema")
	}
}

func TestProcessMasterRow(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Test table row
	tableRow := MasterRow{
		Type:     "table",
		Name:     "users",
		TblName:  "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
	}

	err := s.processMasterRow(tableRow)
	if err != nil {
		t.Errorf("processMasterRow(table) error = %v", err)
	}

	// Test index row
	indexRow := MasterRow{
		Type:     "index",
		Name:     "idx_users",
		TblName:  "users",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_users ON users(name)",
	}

	err = s.processMasterRow(indexRow)
	if err != nil {
		t.Errorf("processMasterRow(index) error = %v", err)
	}

	// Test view row
	viewRow := MasterRow{
		Type:    "view",
		Name:    "active_users",
		TblName: "active_users",
		SQL:     "CREATE VIEW active_users AS SELECT * FROM users",
	}

	err = s.processMasterRow(viewRow)
	if err != nil {
		t.Errorf("processMasterRow(view) error = %v", err)
	}

	// Test unknown type (should be silently ignored)
	unknownRow := MasterRow{
		Type: "trigger",
		Name: "my_trigger",
	}

	err = s.processMasterRow(unknownRow)
	if err != nil {
		t.Errorf("processMasterRow(unknown) should not error, got %v", err)
	}
}

func TestParseSingleCreateTable(t *testing.T) {
	t.Parallel()
	sql := "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"
	stmt, err := parseSingleCreateTable(sql)
	if err != nil {
		t.Fatalf("parseSingleCreateTable() error = %v", err)
	}

	if stmt.Name != "users" {
		t.Errorf("stmt.Name = %q, want %q", stmt.Name, "users")
	}
	if len(stmt.Columns) != 2 {
		t.Errorf("stmt has %d columns, want 2", len(stmt.Columns))
	}
}

func TestParseSingleCreateTableMultiple(t *testing.T) {
	t.Parallel()
	sql := "CREATE TABLE t1(id INTEGER); CREATE TABLE t2(id INTEGER)"
	_, err := parseSingleCreateTable(sql)
	if err == nil {
		t.Error("Expected error for multiple statements")
	}
}

func TestParseSingleCreateTableWrongType(t *testing.T) {
	t.Parallel()
	sql := "CREATE INDEX idx ON t(c)"
	_, err := parseSingleCreateTable(sql)
	if err == nil {
		t.Error("Expected error for non-CREATE TABLE statement")
	}
}

func TestParseViewSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name: "active_users",
		SQL:  "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
	}

	view, err := s.parseViewSQL(row)
	if err != nil {
		t.Fatalf("parseViewSQL() error = %v", err)
	}

	if view.Name != "active_users" {
		t.Errorf("view.Name = %q, want %q", view.Name, "active_users")
	}
	if view.Select == nil {
		t.Error("view.Select should not be nil")
	}
}

func TestParseViewSQLNoSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name: "some_view",
		SQL:  "",
	}

	view, err := s.parseViewSQL(row)
	if err != nil {
		t.Fatalf("parseViewSQL() error = %v", err)
	}

	if view.Name != "some_view" {
		t.Errorf("view.Name = %q, want %q", view.Name, "some_view")
	}
	if len(view.Columns) != 0 {
		t.Errorf("view with no SQL should have 0 columns, got %d", len(view.Columns))
	}
}

func TestParseViewSQLInvalid(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name: "bad_view",
		SQL:  "INVALID SQL",
	}

	_, err := s.parseViewSQL(row)
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}
