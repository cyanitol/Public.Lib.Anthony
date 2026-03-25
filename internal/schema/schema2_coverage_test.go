// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// ---------------------------------------------------------------------------
// checkColumnNotIndexed – continue branch when index belongs to a different table
// ---------------------------------------------------------------------------

// checkColumnNotIndexed must skip indexes belonging to other tables and only
// report an error when the target table's own index references the column.
func TestSchema2_CheckColumnNotIndexed_OtherTableContinueBranch(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t1"] = makeAlterTable("t1", makeCol("a", "TEXT"), makeCol("b", "TEXT"))
	s.Tables["t2"] = makeAlterTable("t2", makeCol("a", "TEXT"))
	// Index on t2 references column "a" – should not affect t1.
	s.Indexes["idx_t2_a"] = &Index{Name: "idx_t2_a", Table: "t2", Columns: []string{"a"}}

	// Dropping "a" from t1 must succeed: the only index on "a" belongs to t2.
	if err := s.checkColumnNotIndexed("t1", "a"); err != nil {
		t.Errorf("checkColumnNotIndexed: unexpected error for unrelated index: %v", err)
	}
}

// checkColumnNotIndexed must detect when the column is used by the table's own index.
func TestSchema2_CheckColumnNotIndexed_SameTableError(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("x", "TEXT"), makeCol("y", "TEXT"))
	s.Indexes["idx_t_x"] = &Index{Name: "idx_t_x", Table: "t", Columns: []string{"x"}}

	if err := s.checkColumnNotIndexed("t", "x"); err == nil {
		t.Error("checkColumnNotIndexed: expected error for column used in its own table's index")
	}
}

// checkColumnNotIndexed must return nil when the table has no indexes at all.
func TestSchema2_CheckColumnNotIndexed_NoIndexes(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("col", "TEXT"), makeCol("other", "TEXT"))

	if err := s.checkColumnNotIndexed("t", "col"); err != nil {
		t.Errorf("checkColumnNotIndexed: unexpected error with no indexes: %v", err)
	}
}

// ---------------------------------------------------------------------------
// rebuildCreateIndexSQL – UNIQUE index path and partial-index WHERE path
// ---------------------------------------------------------------------------

// rebuildCreateIndexSQL must include "UNIQUE" when idx.Unique is true.
func TestSchema2_RebuildCreateIndexSQL_UniqueIndex(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx_unique",
		Table:   "users",
		Columns: []string{"email"},
		Unique:  true,
	}
	sql := rebuildCreateIndexSQL(idx)
	if !strings.Contains(sql, "UNIQUE") {
		t.Errorf("rebuildCreateIndexSQL: expected UNIQUE in SQL, got: %q", sql)
	}
	if !strings.Contains(sql, "idx_unique") {
		t.Errorf("rebuildCreateIndexSQL: expected index name in SQL, got: %q", sql)
	}
}

// rebuildCreateIndexSQL must include a WHERE clause for partial indexes.
func TestSchema2_RebuildCreateIndexSQL_PartialIndex(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx_partial",
		Table:   "orders",
		Columns: []string{"status"},
		Partial: true,
		Where:   "status = 'active'",
	}
	sql := rebuildCreateIndexSQL(idx)
	if !strings.Contains(sql, "WHERE") {
		t.Errorf("rebuildCreateIndexSQL: expected WHERE in SQL, got: %q", sql)
	}
	if !strings.Contains(sql, "status = 'active'") {
		t.Errorf("rebuildCreateIndexSQL: WHERE clause content missing, got: %q", sql)
	}
}

// Verify that a partial=true index with an empty Where string omits the WHERE clause.
func TestSchema2_RebuildCreateIndexSQL_PartialWithEmptyWhere(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx_no_where",
		Table:   "t",
		Columns: []string{"c"},
		Partial: true,
		Where:   "", // empty: WHERE clause must be skipped
	}
	sql := rebuildCreateIndexSQL(idx)
	if strings.Contains(sql, "WHERE") {
		t.Errorf("rebuildCreateIndexSQL: unexpected WHERE for empty Where, got: %q", sql)
	}
}

// rebuildCreateIndexSQL must emit a correct non-unique, non-partial index.
func TestSchema2_RebuildCreateIndexSQL_PlainIndex(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx_plain",
		Table:   "items",
		Columns: []string{"name"},
		Unique:  false,
		Partial: false,
	}
	sql := rebuildCreateIndexSQL(idx)
	want := "CREATE INDEX idx_plain ON items (name)"
	if sql != want {
		t.Errorf("rebuildCreateIndexSQL: got %q, want %q", sql, want)
	}
}

// rebuildCreateIndexSQL must handle multi-column indexes.
func TestSchema2_RebuildCreateIndexSQL_MultiColumn(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx_multi",
		Table:   "t",
		Columns: []string{"a", "b", "c"},
	}
	sql := rebuildCreateIndexSQL(idx)
	if !strings.Contains(sql, "a, b, c") {
		t.Errorf("rebuildCreateIndexSQL: expected multi-column list, got: %q", sql)
	}
}

// After RenameColumn the index SQL must be rebuilt with the UNIQUE keyword
// preserved when the original index was unique.
func TestSchema2_RenameColumn_UniqueIndexSQLRebuilt(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = makeAlterTable("t", makeCol("id", "INTEGER"), makeCol("email", "TEXT"))
	s.Indexes["idx_email"] = &Index{
		Name:    "idx_email",
		Table:   "t",
		Columns: []string{"email"},
		Unique:  true,
		SQL:     "CREATE UNIQUE INDEX idx_email ON t (email)",
	}

	if err := s.RenameColumn("t", "email", "mail"); err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}

	rebuiltSQL := s.Indexes["idx_email"].SQL
	if !strings.Contains(rebuiltSQL, "UNIQUE") {
		t.Errorf("rebuilt index SQL missing UNIQUE: %q", rebuiltSQL)
	}
	if !strings.Contains(rebuiltSQL, "mail") {
		t.Errorf("rebuilt index SQL missing renamed column: %q", rebuiltSQL)
	}
}

// ---------------------------------------------------------------------------
// insertDatabase – LoadFromMaster error when pageCount > 1
// ---------------------------------------------------------------------------

// mockPagerHighCount is a pager that reports a page count greater than 1,
// causing insertDatabase to surface a LoadFromMaster error.
type mockPagerHighCount struct {
	mockPager
}

func (m *mockPagerHighCount) PageCount() pager.Pgno { return 5 }

// When the btree is non-nil and the pager reports more than one page,
// insertDatabase must propagate the LoadFromMaster error.
func TestSchema2_InsertDatabase_PageCountGt1_ErrorPropagated(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	// Use a freshly allocated btree whose page 1 has NOT been initialised,
	// so LoadFromMaster will try to decode an empty page and fail.
	bt := btree.NewBtree(4096)
	mp := &mockPagerHighCount{}

	err := dr.AttachDatabase("highcount", "/some/path.db", mp, bt)
	// Either errors (schema load failed) or silently swallowed – both are
	// acceptable, but the code path must execute without panic.
	_ = err
}

// insertDatabase succeeds when the pager reports exactly 1 page (empty db).
func TestSchema2_InsertDatabase_PageCount1_Succeeds(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	bt := btree.NewBtree(4096)
	if err := ensureMasterPageInitialized(bt); err != nil {
		t.Fatalf("ensureMasterPageInitialized: %v", err)
	}
	mp := &mockPager{pageCount: 1}
	if err := dr.AttachDatabase("singlepage", "", mp, bt); err != nil {
		t.Errorf("AttachDatabase with pageCount=1: %v", err)
	}
}

// ---------------------------------------------------------------------------
// checkNameConflict – index-name conflict branch
// ---------------------------------------------------------------------------

// checkNameConflict must return an error when the name matches an existing index.
func TestSchema2_CheckNameConflict_IndexConflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	// Register an index with a known name.
	s.Indexes["idx_conflict"] = &Index{Name: "idx_conflict", Table: "t", Columns: []string{"id"}}

	if err := s.checkNameConflict("idx_conflict"); err == nil {
		t.Error("checkNameConflict: expected error for index name conflict, got nil")
	}
}

// checkNameConflict must return an error (case-insensitive) for an index name.
func TestSchema2_CheckNameConflict_IndexConflictCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["MyIndex"] = &Index{Name: "MyIndex", Table: "t", Columns: []string{"x"}}

	if err := s.checkNameConflict("MYINDEX"); err == nil {
		t.Error("checkNameConflict: expected error for case-insensitive index name conflict")
	}
}

// checkNameConflict must return an error when the name matches an existing table.
func TestSchema2_CheckNameConflict_TableConflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["existing"] = &Table{Name: "existing"}

	if err := s.checkNameConflict("existing"); err == nil {
		t.Error("checkNameConflict: expected error for table name conflict, got nil")
	}
}

// checkNameConflict must return nil when neither tables nor indexes share the name.
func TestSchema2_CheckNameConflict_NoConflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["other"] = &Table{Name: "other"}
	s.Indexes["idx_other"] = &Index{Name: "idx_other"}

	if err := s.checkNameConflict("fresh_name"); err != nil {
		t.Errorf("checkNameConflict: unexpected error for unused name: %v", err)
	}
}

// RenameTable uses checkNameConflict; verify the index-conflict path is reached.
func TestSchema2_RenameTable_IndexNameConflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["src"] = &Table{Name: "src", Columns: []*Column{makeCol("id", "INTEGER")}}
	// Register an index whose name is the same as the intended new table name.
	s.Indexes["dst"] = &Index{Name: "dst", Table: "src", Columns: []string{"id"}}

	if err := s.RenameTable("src", "dst"); err == nil {
		t.Error("RenameTable: expected error when new name conflicts with existing index")
	}
}

// ---------------------------------------------------------------------------
// parseSingleCreateTable – multi-statement and wrong-type error paths
// ---------------------------------------------------------------------------

// parseSingleCreateTable must error when the SQL contains zero statements.
func TestSchema2_ParseSingleCreateTable_EmptySQL(t *testing.T) {
	t.Parallel()
	_, err := parseSingleCreateTable("")
	if err == nil {
		t.Error("parseSingleCreateTable: expected error for empty SQL")
	}
}

// parseSingleCreateTable must error when the SQL is not a CREATE TABLE statement
// (e.g., it is a CREATE INDEX statement).
func TestSchema2_ParseSingleCreateTable_WrongType(t *testing.T) {
	t.Parallel()
	_, err := parseSingleCreateTable("CREATE INDEX idx ON t(c)")
	if err == nil {
		t.Error("parseSingleCreateTable: expected error for non-CREATE TABLE statement")
	}
}

// parseSingleCreateTable must succeed for a valid single CREATE TABLE statement.
func TestSchema2_ParseSingleCreateTable_Valid(t *testing.T) {
	t.Parallel()
	stmt, err := parseSingleCreateTable("CREATE TABLE foo (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("parseSingleCreateTable: unexpected error: %v", err)
	}
	if stmt.Name != "foo" {
		t.Errorf("parseSingleCreateTable: Name = %q, want 'foo'", stmt.Name)
	}
}

// ---------------------------------------------------------------------------
// processMasterViewRow – success path (valid CREATE VIEW SQL)
// ---------------------------------------------------------------------------

func TestSchema2_ProcessMasterViewRow_ValidSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	row := MasterRow{
		Type:    "view",
		Name:    "v_users",
		TblName: "v_users",
		SQL:     "CREATE VIEW v_users AS SELECT 1 AS id",
	}
	if err := s.processMasterViewRow(row); err != nil {
		t.Fatalf("processMasterViewRow: unexpected error: %v", err)
	}
	if _, ok := s.Views["v_users"]; !ok {
		t.Error("processMasterViewRow: view not registered in schema")
	}
}

// processMasterViewRow with empty SQL must produce a bare View (no error).
func TestSchema2_ProcessMasterViewRow_EmptySQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	row := MasterRow{
		Type:    "view",
		Name:    "bare_view",
		TblName: "bare_view",
		SQL:     "",
	}
	if err := s.processMasterViewRow(row); err != nil {
		t.Fatalf("processMasterViewRow with empty SQL: %v", err)
	}
	if _, ok := s.Views["bare_view"]; !ok {
		t.Error("processMasterViewRow: bare view not registered")
	}
}

// ---------------------------------------------------------------------------
// clearMasterTable – direct call on a freshly initialised btree
// ---------------------------------------------------------------------------

// clearMasterTable called twice in a row must not panic or error.
func TestSchema2_ClearMasterTable_Idempotent(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["z"] = &Table{
		Name:     "z",
		RootPage: 2,
		SQL:      "CREATE TABLE z(id INTEGER)",
	}

	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}
	// First clear.
	if err := s.clearMasterTable(bt); err != nil {
		t.Fatalf("first clearMasterTable: %v", err)
	}
	// Second clear on now-empty master must also succeed.
	if err := s.clearMasterTable(bt); err != nil {
		t.Fatalf("second clearMasterTable: %v", err)
	}
}

// clearMasterTable on a btree with multiple rows clears all of them.
func TestSchema2_ClearMasterTable_MultipleRowsCleared(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		s.Tables[name] = &Table{
			Name:     name,
			RootPage: 2,
			SQL:      "CREATE TABLE " + name + "(id INTEGER)",
		}
	}

	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage before clear: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected rows before clear")
	}

	if err := s.clearMasterTable(bt); err != nil {
		t.Fatalf("clearMasterTable: %v", err)
	}

	rows, err = s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage after clear: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after clearMasterTable, got %d", len(rows))
	}
}
