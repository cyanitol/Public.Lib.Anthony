// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"testing"
)

func TestAddTableUnsafe(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	table := &Table{
		Name:     "test_table",
		RootPage: 3,
		SQL:      "CREATE TABLE test_table(id INTEGER)",
		Columns:  []*Column{{Name: "id", Type: "INTEGER", Affinity: AffinityInteger}},
	}
	s.AddTableUnsafe(table)
	got, ok := s.Tables["test_table"]
	if !ok {
		t.Fatal("AddTableUnsafe: table not found after insertion")
	}
	if got.RootPage != 3 {
		t.Errorf("AddTableUnsafe: RootPage = %d, want 3", got.RootPage)
	}
}

func TestAddTableUnsafe_Overwrite(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	t1 := &Table{Name: "t", RootPage: 1, SQL: "CREATE TABLE t(x INTEGER)"}
	t2 := &Table{Name: "t", RootPage: 2, SQL: "CREATE TABLE t(y TEXT)"}
	s.AddTableUnsafe(t1)
	s.AddTableUnsafe(t2)
	if s.Tables["t"].RootPage != 2 {
		t.Errorf("AddTableUnsafe overwrite: RootPage = %d, want 2", s.Tables["t"].RootPage)
	}
}

func TestAddViewUnsafe(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	view := &View{
		Name: "test_view",
		SQL:  "CREATE VIEW test_view AS SELECT 1",
	}
	s.AddViewUnsafe(view)
	got, ok := s.Views["test_view"]
	if !ok {
		t.Fatal("AddViewUnsafe: view not found after insertion")
	}
	if got.Name != "test_view" {
		t.Errorf("AddViewUnsafe: Name = %q, want %q", got.Name, "test_view")
	}
}

func TestAddViewUnsafe_Overwrite(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	v1 := &View{Name: "v", SQL: "CREATE VIEW v AS SELECT 1"}
	v2 := &View{Name: "v", SQL: "CREATE VIEW v AS SELECT 2"}
	s.AddViewUnsafe(v1)
	s.AddViewUnsafe(v2)
	if s.Views["v"].SQL != "CREATE VIEW v AS SELECT 2" {
		t.Errorf("AddViewUnsafe overwrite: SQL = %q, want SELECT 2", s.Views["v"].SQL)
	}
}

func TestAddIndexUnsafe(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	idx := &Index{
		Name:    "idx_test",
		Table:   "test_table",
		Columns: []string{"id"},
		SQL:     "CREATE INDEX idx_test ON test_table(id)",
	}
	s.AddIndexUnsafe(idx)
	got, ok := s.Indexes["idx_test"]
	if !ok {
		t.Fatal("AddIndexUnsafe: index not found after insertion")
	}
	if got.Table != "test_table" {
		t.Errorf("AddIndexUnsafe: Table = %q, want %q", got.Table, "test_table")
	}
}

func TestAddIndexUnsafe_Overwrite(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	i1 := &Index{Name: "i", Table: "t1", Columns: []string{"a"}}
	i2 := &Index{Name: "i", Table: "t2", Columns: []string{"b"}}
	s.AddIndexUnsafe(i1)
	s.AddIndexUnsafe(i2)
	if s.Indexes["i"].Table != "t2" {
		t.Errorf("AddIndexUnsafe overwrite: Table = %q, want t2", s.Indexes["i"].Table)
	}
}

func TestAddTriggerUnsafe(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	trigger := &Trigger{
		Name:  "trg_test",
		Table: "test_table",
		SQL:   "CREATE TRIGGER trg_test AFTER INSERT ON test_table BEGIN SELECT 1; END",
	}
	s.AddTriggerUnsafe(trigger)
	got, ok := s.Triggers["trg_test"]
	if !ok {
		t.Fatal("AddTriggerUnsafe: trigger not found after insertion")
	}
	if got.Table != "test_table" {
		t.Errorf("AddTriggerUnsafe: Table = %q, want %q", got.Table, "test_table")
	}
}

func TestAddTriggerUnsafe_NilTriggersMap(t *testing.T) {
	t.Parallel()
	s := &Schema{
		Tables:    make(map[string]*Table),
		Indexes:   make(map[string]*Index),
		Views:     make(map[string]*View),
		Triggers:  nil,
		Sequences: NewSequenceManager(),
	}
	trigger := &Trigger{Name: "trg_nil", Table: "t"}
	s.AddTriggerUnsafe(trigger)
	if s.Triggers["trg_nil"] == nil {
		t.Fatal("AddTriggerUnsafe with nil map: trigger not inserted")
	}
}

func TestAddTriggerUnsafe_Overwrite(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	tr1 := &Trigger{Name: "tr", Table: "t1"}
	tr2 := &Trigger{Name: "tr", Table: "t2"}
	s.AddTriggerUnsafe(tr1)
	s.AddTriggerUnsafe(tr2)
	if s.Triggers["tr"].Table != "t2" {
		t.Errorf("AddTriggerUnsafe overwrite: Table = %q, want t2", s.Triggers["tr"].Table)
	}
}

func TestCreateVirtualTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	err := s.CreateVirtualTable("vt_fts", "fts5", []string{"content", "title"}, nil, "")
	if err != nil {
		t.Fatalf("CreateVirtualTable: unexpected error: %v", err)
	}
	got, ok := s.Tables["vt_fts"]
	if !ok {
		t.Fatal("CreateVirtualTable: table not found after creation")
	}
	if !got.IsVirtual {
		t.Error("CreateVirtualTable: IsVirtual should be true")
	}
	if got.Module != "fts5" {
		t.Errorf("CreateVirtualTable: Module = %q, want fts5", got.Module)
	}
	if len(got.Columns) != 2 {
		t.Errorf("CreateVirtualTable: len(Columns) = %d, want 2", len(got.Columns))
	}
}

func TestCreateVirtualTable_ReservedName(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	err := s.CreateVirtualTable("sqlite_sequence", "fts5", nil, nil, "")
	if err == nil {
		t.Fatal("CreateVirtualTable: expected error for reserved name, got nil")
	}
}

func TestCreateVirtualTable_AlreadyExists(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	_ = s.CreateVirtualTable("vt", "fts5", nil, nil, "")
	err := s.CreateVirtualTable("vt", "fts5", nil, nil, "")
	if err == nil {
		t.Fatal("CreateVirtualTable: expected error for duplicate name, got nil")
	}
}

func TestCreateVirtualTable_EmptyArgs(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	err := s.CreateVirtualTable("vt_empty", "rtree", []string{}, nil, "")
	if err != nil {
		t.Fatalf("CreateVirtualTable with empty args: unexpected error: %v", err)
	}
	got, ok := s.Tables["vt_empty"]
	if !ok {
		t.Fatal("CreateVirtualTable with empty args: table not found")
	}
	if len(got.Columns) != 0 {
		t.Errorf("CreateVirtualTable with empty args: Columns = %d, want 0", len(got.Columns))
	}
}

func TestGetRootPage(t *testing.T) {
	t.Parallel()
	table := &Table{Name: "t", RootPage: 42}
	if got := table.GetRootPage(); got != 42 {
		t.Errorf("GetRootPage() = %d, want 42", got)
	}
}

func TestSetRootPage(t *testing.T) {
	t.Parallel()
	table := &Table{Name: "t", RootPage: 1}
	table.SetRootPage(99)
	if table.RootPage != 99 {
		t.Errorf("SetRootPage: RootPage = %d, want 99", table.RootPage)
	}
}

func TestSetAndGetRootPage_RoundTrip(t *testing.T) {
	t.Parallel()
	table := &Table{Name: "t"}
	table.SetRootPage(7)
	if got := table.GetRootPage(); got != 7 {
		t.Errorf("SetRootPage/GetRootPage round-trip: got %d, want 7", got)
	}
}

func TestGetSequences(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	result := s.GetSequences()
	if result == nil {
		t.Fatal("GetSequences: returned nil")
	}
	sm, ok := result.(*SequenceManager)
	if !ok {
		t.Fatalf("GetSequences: returned type %T, want *SequenceManager", result)
	}
	if sm != s.Sequences {
		t.Error("GetSequences: returned different SequenceManager than s.Sequences")
	}
}

func TestEnsureSqliteSequenceTable_Creates(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.EnsureSqliteSequenceTable(5)
	got, ok := s.Tables["sqlite_sequence"]
	if !ok {
		t.Fatal("EnsureSqliteSequenceTable: table not created")
	}
	if got.RootPage != 5 {
		t.Errorf("EnsureSqliteSequenceTable: RootPage = %d, want 5", got.RootPage)
	}
	if len(got.Columns) != 2 {
		t.Errorf("EnsureSqliteSequenceTable: len(Columns) = %d, want 2", len(got.Columns))
	}
}

func TestEnsureSqliteSequenceTable_Idempotent(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.EnsureSqliteSequenceTable(5)
	s.EnsureSqliteSequenceTable(99)
	got := s.Tables["sqlite_sequence"]
	if got.RootPage != 5 {
		t.Errorf("EnsureSqliteSequenceTable idempotent: RootPage = %d, want 5 (first call wins)", got.RootPage)
	}
}
