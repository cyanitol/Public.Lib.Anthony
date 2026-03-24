// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"strings"
	"testing"
)

// makeAlterTable builds a simple table with the given columns for alter tests.
func makeAlterTable(name string, cols ...*Column) *Table {
	return &Table{Name: name, Columns: cols}
}

// makeCol creates a Column with the given name and type.
func makeCol(name, typ string) *Column {
	return &Column{Name: name, Type: typ}
}

func TestRenameColumn_Success(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["users"] = makeAlterTable("users",
		makeCol("id", "INTEGER"),
		makeCol("name", "TEXT"),
	)
	s.Indexes["idx_name"] = &Index{Name: "idx_name", Table: "users", Columns: []string{"name"}}

	if err := s.RenameColumn("users", "name", "username"); err != nil {
		t.Fatalf("RenameColumn() error = %v", err)
	}

	tbl := s.Tables["users"]
	if tbl.Columns[1].Name != "username" {
		t.Errorf("column not renamed, got %q", tbl.Columns[1].Name)
	}
	if s.Indexes["idx_name"].Columns[0] != "username" {
		t.Errorf("index column not updated, got %q", s.Indexes["idx_name"].Columns[0])
	}
	if !strings.Contains(tbl.SQL, "username") {
		t.Errorf("SQL not rebuilt, got %q", tbl.SQL)
	}
}

func TestRenameColumn_TableNotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	if err := s.RenameColumn("missing", "a", "b"); err == nil {
		t.Error("expected error for missing table")
	}
}

func TestRenameColumn_OldColumnNotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("id", "INTEGER"))
	if err := s.RenameColumn("t", "ghost", "real"); err == nil {
		t.Error("expected error for missing old column")
	}
}

func TestRenameColumn_NewColumnConflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("a", "TEXT"), makeCol("b", "TEXT"))
	if err := s.RenameColumn("t", "a", "b"); err == nil {
		t.Error("expected error when new column already exists")
	}
}

func TestRenameColumn_UpdatesPrimaryKey(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = &Table{
		Name:       "t",
		Columns:    []*Column{makeCol("id", "INTEGER"), makeCol("val", "TEXT")},
		PrimaryKey: []string{"id"},
	}
	if err := s.RenameColumn("t", "id", "pk"); err != nil {
		t.Fatalf("RenameColumn() error = %v", err)
	}
	tbl := s.Tables["t"]
	if tbl.PrimaryKey[0] != "pk" {
		t.Errorf("PrimaryKey not updated, got %q", tbl.PrimaryKey[0])
	}
}

func TestDropColumn_Success(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("id", "INTEGER"), makeCol("extra", "TEXT"))
	if err := s.DropColumn("t", "extra"); err != nil {
		t.Fatalf("DropColumn() error = %v", err)
	}
	tbl := s.Tables["t"]
	if len(tbl.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(tbl.Columns))
	}
	if tbl.Columns[0].Name != "id" {
		t.Errorf("wrong column kept: %q", tbl.Columns[0].Name)
	}
}

func TestDropColumn_TableNotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	if err := s.DropColumn("ghost", "col"); err == nil {
		t.Error("expected error for missing table")
	}
}

func TestDropColumn_ColumnNotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("id", "INTEGER"), makeCol("name", "TEXT"))
	if err := s.DropColumn("t", "nope"); err == nil {
		t.Error("expected error for missing column")
	}
}

func TestDropColumn_LastColumn(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("only", "INTEGER"))
	if err := s.DropColumn("t", "only"); err == nil {
		t.Error("expected error when dropping last column")
	}
}

func TestDropColumn_PKColumn(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = &Table{
		Name:       "t",
		Columns:    []*Column{makeCol("id", "INTEGER"), makeCol("val", "TEXT")},
		PrimaryKey: []string{"id"},
	}
	if err := s.DropColumn("t", "id"); err == nil {
		t.Error("expected error when dropping PK column")
	}
}

func TestDropColumn_IndexedColumn(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("a", "TEXT"), makeCol("b", "TEXT"))
	s.Indexes["idx_a"] = &Index{Name: "idx_a", Table: "t", Columns: []string{"a"}}
	if err := s.DropColumn("t", "a"); err == nil {
		t.Error("expected error when dropping indexed column")
	}
}

func TestUpdateTableSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("x", "INTEGER"))
	s.Tables["t"].SQL = ""
	s.UpdateTableSQL("t")
	if s.Tables["t"].SQL == "" {
		t.Error("SQL should be rebuilt after UpdateTableSQL")
	}
}

func TestUpdateTableSQL_NotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	// Should not panic when table doesn't exist
	s.UpdateTableSQL("ghost")
}

func TestUpdateRenameTableSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["newname"] = &Table{
		Name:    "newname",
		Columns: []*Column{makeCol("id", "INTEGER")},
		SQL:     "",
	}
	s.Indexes["idx_newname"] = &Index{
		Name:    "idx_newname",
		Table:   "newname",
		Columns: []string{"id"},
		SQL:     "",
	}
	s.UpdateRenameTableSQL("newname")
	if s.Tables["newname"].SQL == "" {
		t.Error("table SQL should be rebuilt")
	}
	if s.Indexes["idx_newname"].SQL == "" {
		t.Error("index SQL should be rebuilt")
	}
}

func TestUpdateRenameTableSQL_NotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	// Should not panic when table doesn't exist
	s.UpdateRenameTableSQL("ghost")
}

func TestRebuildCreateTableSQL_Minimal(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "foo",
		Columns: []*Column{makeCol("id", "INTEGER")},
	}
	sql := RebuildCreateTableSQL(tbl)
	if !strings.HasPrefix(sql, "CREATE TABLE foo (") {
		t.Errorf("unexpected SQL: %q", sql)
	}
}

func TestRebuildCreateTableSQL_WithoutRowID(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:         "bar",
		Columns:      []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		WithoutRowID: true,
	}
	sql := RebuildCreateTableSQL(tbl)
	if !strings.Contains(sql, "WITHOUT ROWID") {
		t.Errorf("expected WITHOUT ROWID, got %q", sql)
	}
}

func TestRebuildCreateTableSQL_Strict(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "baz",
		Columns: []*Column{makeCol("id", "INTEGER")},
		Strict:  true,
	}
	sql := RebuildCreateTableSQL(tbl)
	if !strings.Contains(sql, "STRICT") {
		t.Errorf("expected STRICT, got %q", sql)
	}
}

func TestRebuildCreateTableSQL_AllColumnConstraints(t *testing.T) {
	t.Parallel()
	def := "hello"
	tbl := &Table{
		Name: "all_constraints",
		Columns: []*Column{
			{
				Name:          "id",
				Type:          "INTEGER",
				PrimaryKey:    true,
				Autoincrement: true,
				NotNull:       true,
				Unique:        true,
				Default:       &def,
				Collation:     "NOCASE",
			},
		},
	}
	sql := RebuildCreateTableSQL(tbl)
	for _, want := range []string{"PRIMARY KEY", "AUTOINCREMENT", "NOT NULL", "UNIQUE", "DEFAULT", "COLLATE NOCASE"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %q in SQL, got: %q", want, sql)
		}
	}
}

func TestRebuildCreateTableSQL_TableConstraints(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "constrained",
		Columns: []*Column{makeCol("a", "INTEGER"), makeCol("b", "INTEGER")},
		Constraints: []TableConstraint{
			{Type: ConstraintPrimaryKey, Columns: []string{"a"}},
			{Type: ConstraintUnique, Name: "uq_ab", Columns: []string{"a", "b"}},
			{Type: ConstraintCheck, Expression: "a > 0"},
			{Type: ConstraintForeignKey, Columns: []string{"b"}},
		},
	}
	sql := RebuildCreateTableSQL(tbl)
	for _, want := range []string{"PRIMARY KEY", "UNIQUE", "CHECK", "FOREIGN KEY", "CONSTRAINT uq_ab"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %q in SQL, got: %q", want, sql)
		}
	}
}

func TestRebuildCreateTableSQL_NoType(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "typeless",
		Columns: []*Column{{Name: "x"}},
	}
	sql := RebuildCreateTableSQL(tbl)
	if !strings.Contains(sql, "x") {
		t.Errorf("expected column x in SQL: %q", sql)
	}
}

func TestUpdateIndexColumnRefs_MultipleColumns(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("a", "TEXT"), makeCol("b", "TEXT"), makeCol("c", "TEXT"))
	s.Indexes["idx_multi"] = &Index{
		Name:    "idx_multi",
		Table:   "t",
		Columns: []string{"a", "b"},
	}
	s.updateIndexColumnRefs("t", "a", "alpha")
	if s.Indexes["idx_multi"].Columns[0] != "alpha" {
		t.Errorf("column ref not updated, got %q", s.Indexes["idx_multi"].Columns[0])
	}
	if s.Indexes["idx_multi"].Columns[1] != "b" {
		t.Errorf("unrelated column modified, got %q", s.Indexes["idx_multi"].Columns[1])
	}
}

func TestUpdateIndexColumnRefs_OtherTableIgnored(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["a"] = makeAlterTable("a", makeCol("col", "TEXT"))
	s.Tables["b"] = makeAlterTable("b", makeCol("col", "TEXT"))
	s.Indexes["idx_b"] = &Index{Name: "idx_b", Table: "b", Columns: []string{"col"}}
	s.updateIndexColumnRefs("a", "col", "renamed")
	if s.Indexes["idx_b"].Columns[0] != "col" {
		t.Errorf("index from different table was modified")
	}
}

func TestRenameColumn_CaseInsensitiveTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["Users"] = makeAlterTable("Users", makeCol("email", "TEXT"), makeCol("age", "INTEGER"))
	if err := s.RenameColumn("USERS", "email", "mail"); err != nil {
		t.Fatalf("RenameColumn() with mixed-case table name: %v", err)
	}
	if s.Tables["Users"].Columns[0].Name != "mail" {
		t.Errorf("column rename not applied, got %q", s.Tables["Users"].Columns[0].Name)
	}
}

func TestDropColumn_CaseInsensitiveTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["Foo"] = makeAlterTable("Foo", makeCol("a", "TEXT"), makeCol("b", "TEXT"))
	if err := s.DropColumn("FOO", "b"); err != nil {
		t.Fatalf("DropColumn() with mixed-case table name: %v", err)
	}
	if len(s.Tables["Foo"].Columns) != 1 {
		t.Errorf("expected 1 column after drop, got %d", len(s.Tables["Foo"].Columns))
	}
}
