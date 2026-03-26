// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// --- ALTER TABLE additional coverage (parseAlterTableAdd at 80%) ---

// TestAlterTableAdd_WithColumnKeyword tests ADD COLUMN with explicit COLUMN keyword.
func TestAlterTableAdd_WithColumnKeyword(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users ADD COLUMN email TEXT"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	alter, ok := stmts[0].(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmts[0])
	}
	add, ok := alter.Action.(*AddColumnAction)
	if !ok {
		t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
	}
	if add.Column.Name != "email" {
		t.Errorf("expected column name 'email', got %q", add.Column.Name)
	}
	if add.Column.Type != "TEXT" {
		t.Errorf("expected column type 'TEXT', got %q", add.Column.Type)
	}
}

// TestAlterTableAdd_WithoutColumnKeyword tests ADD without COLUMN keyword (optional).
func TestAlterTableAdd_WithoutColumnKeyword(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users ADD phone TEXT"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	alter, ok := stmts[0].(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmts[0])
	}
	add, ok := alter.Action.(*AddColumnAction)
	if !ok {
		t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
	}
	if add.Column.Name != "phone" {
		t.Errorf("expected column name 'phone', got %q", add.Column.Name)
	}
}

// TestAlterTableAdd_WithConstraints tests ADD COLUMN with NOT NULL constraint.
func TestAlterTableAdd_WithConstraints(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE products ADD COLUMN price REAL NOT NULL DEFAULT 0.0"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	alter, ok := stmts[0].(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmts[0])
	}
	add, ok := alter.Action.(*AddColumnAction)
	if !ok {
		t.Fatalf("expected *AddColumnAction, got %T", alter.Action)
	}
	if add.Column.Name != "price" {
		t.Errorf("expected column name 'price', got %q", add.Column.Name)
	}
}

// TestAlterTableDrop_Column tests DROP COLUMN statement.
func TestAlterTableDrop_Column(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users DROP COLUMN phone"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	alter, ok := stmts[0].(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmts[0])
	}
	drop, ok := alter.Action.(*DropColumnAction)
	if !ok {
		t.Fatalf("expected *DropColumnAction, got %T", alter.Action)
	}
	if drop.ColumnName != "phone" {
		t.Errorf("expected column name 'phone', got %q", drop.ColumnName)
	}
}

// TestAlterTableDrop_ErrorMissingColumn tests error when COLUMN keyword is missing.
func TestAlterTableDrop_ErrorMissingColumn(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users DROP phone"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for missing COLUMN keyword after DROP")
	}
}

// TestAlterTableDrop_ErrorMissingColumnName tests error when column name is missing.
func TestAlterTableDrop_ErrorMissingColumnName(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users DROP COLUMN"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for missing column name")
	}
}

// TestAlterTableAdd_ErrorInColumnDef tests error when column definition is invalid.
func TestAlterTableAdd_ErrorInColumnDef(t *testing.T) {
	t.Parallel()
	// Missing column name after ADD
	sql := "ALTER TABLE users ADD"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for missing column definition after ADD")
	}
}

// TestAlterTableError_InvalidAction tests error for unknown action after table name.
func TestAlterTableError_InvalidAction(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users MODIFY COLUMN name TEXT"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for invalid ALTER action")
	}
}

// TestAlterTableError_NoTable tests error when TABLE keyword is missing.
func TestAlterTableError_NoTable(t *testing.T) {
	t.Parallel()
	sql := "ALTER VIEW users RENAME TO admins"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for missing TABLE keyword after ALTER")
	}
}

// TestAlterTable_RenameNoTO tests error for RENAME without TO or COLUMN.
func TestAlterTable_RenameNoTO(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users RENAME users2"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for RENAME without TO")
	}
}

// TestAlterTable_RenameToMissingName tests error for RENAME TO without a new name.
func TestAlterTable_RenameToMissingName(t *testing.T) {
	t.Parallel()
	sql := "ALTER TABLE users RENAME TO"
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatal("expected error for RENAME TO without new table name")
	}
}

// --- ANALYZE additional coverage (parseAnalyze at 90%) ---

// TestAnalyze_NoArgs tests plain ANALYZE with no arguments.
func TestAnalyze_NoArgs(t *testing.T) {
	t.Parallel()
	sql := "ANALYZE"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	a, ok := stmts[0].(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
	}
	if a.Name != "" {
		t.Errorf("expected empty name, got %q", a.Name)
	}
	if a.Schema != "" {
		t.Errorf("expected empty schema, got %q", a.Schema)
	}
}

// TestAnalyze_TableName tests ANALYZE with a table name.
func TestAnalyze_TableName(t *testing.T) {
	t.Parallel()
	sql := "ANALYZE mytable"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	a, ok := stmts[0].(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
	}
	if a.Name != "mytable" {
		t.Errorf("expected name 'mytable', got %q", a.Name)
	}
	if a.Schema != "" {
		t.Errorf("expected empty schema, got %q", a.Schema)
	}
}

// TestAnalyze_SchemaQualified tests ANALYZE with schema.table syntax.
func TestAnalyze_SchemaQualified(t *testing.T) {
	t.Parallel()
	sql := "ANALYZE main.mytable"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	a, ok := stmts[0].(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
	}
	if a.Schema != "main" {
		t.Errorf("expected schema 'main', got %q", a.Schema)
	}
	if a.Name != "mytable" {
		t.Errorf("expected name 'mytable', got %q", a.Name)
	}
}

// TestAnalyze_IndexName tests ANALYZE on a specific index.
func TestAnalyze_IndexName(t *testing.T) {
	t.Parallel()
	sql := "ANALYZE idx_users_email"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	a, ok := stmts[0].(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
	}
	if a.Name != "idx_users_email" {
		t.Errorf("expected name 'idx_users_email', got %q", a.Name)
	}
}

// TestAnalyze_SchemaQualifiedIndex tests ANALYZE on schema-qualified index.
func TestAnalyze_SchemaQualifiedIndex(t *testing.T) {
	t.Parallel()
	sql := "ANALYZE main.idx_users_email"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	a, ok := stmts[0].(*AnalyzeStmt)
	if !ok {
		t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
	}
	if a.Schema != "main" {
		t.Errorf("expected schema 'main', got %q", a.Schema)
	}
	if a.Name != "idx_users_email" {
		t.Errorf("expected name 'idx_users_email', got %q", a.Name)
	}
}

// --- REINDEX additional coverage (parseReindex at 90%) ---

// TestReindex_NoArgs tests plain REINDEX with no arguments.
func TestReindex_NoArgs(t *testing.T) {
	t.Parallel()
	sql := "REINDEX"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Name != "" {
		t.Errorf("expected empty name, got %q", r.Name)
	}
}

// TestReindex_TableName tests REINDEX with a table name.
func TestReindex_TableName(t *testing.T) {
	t.Parallel()
	sql := "REINDEX users"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Name != "users" {
		t.Errorf("expected name 'users', got %q", r.Name)
	}
}

// TestReindex_IndexName tests REINDEX with an index name.
func TestReindex_IndexName(t *testing.T) {
	t.Parallel()
	sql := "REINDEX idx_email"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Name != "idx_email" {
		t.Errorf("expected 'idx_email', got %q", r.Name)
	}
}

// TestReindex_SchemaQualified tests REINDEX with schema-qualified name.
func TestReindex_SchemaQualified(t *testing.T) {
	t.Parallel()
	sql := "REINDEX main.users"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Schema != "main" {
		t.Errorf("expected schema 'main', got %q", r.Schema)
	}
	if r.Name != "users" {
		t.Errorf("expected name 'users', got %q", r.Name)
	}
}

// TestReindex_SchemaQualifiedIndex tests REINDEX with schema-qualified index name.
func TestReindex_SchemaQualifiedIndex(t *testing.T) {
	t.Parallel()
	sql := "REINDEX main.idx_email"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Schema != "main" {
		t.Errorf("expected schema 'main', got %q", r.Schema)
	}
	if r.Name != "idx_email" {
		t.Errorf("expected name 'idx_email', got %q", r.Name)
	}
}

// TestReindex_CollationName tests REINDEX with a collation name.
func TestReindex_CollationName(t *testing.T) {
	t.Parallel()
	sql := "REINDEX NOCASE"
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	r, ok := stmts[0].(*ReindexStmt)
	if !ok {
		t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
	}
	if r.Name != "NOCASE" {
		t.Errorf("expected name 'NOCASE', got %q", r.Name)
	}
}

// TestParseAnalyzeViaParseString tests ANALYZE via ParseString convenience function.
func TestParseAnalyzeViaParseString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql        string
		wantName   string
		wantSchema string
	}{
		{"ANALYZE", "", ""},
		{"ANALYZE t1", "t1", ""},
		{"ANALYZE main.t1", "t1", "main"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.sql, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			a, ok := stmts[0].(*AnalyzeStmt)
			if !ok {
				t.Fatalf("expected *AnalyzeStmt, got %T", stmts[0])
			}
			if a.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", a.Name, tt.wantName)
			}
			if a.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", a.Schema, tt.wantSchema)
			}
		})
	}
}

// TestParseReindexViaParseString tests REINDEX via ParseString convenience function.
func TestParseReindexViaParseString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql        string
		wantName   string
		wantSchema string
	}{
		{"REINDEX", "", ""},
		{"REINDEX idx1", "idx1", ""},
		{"REINDEX main.idx1", "idx1", "main"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.sql, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString error: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}
			r, ok := stmts[0].(*ReindexStmt)
			if !ok {
				t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
			}
			if r.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", r.Name, tt.wantName)
			}
			if r.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", r.Schema, tt.wantSchema)
			}
		})
	}
}
