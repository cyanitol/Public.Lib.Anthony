// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

// MC/DC (Modified Condition/Decision Coverage) tests for the schema package — file 5.
//
// Targets:
//   - database.go:96  insertDatabase (71.4%)
//   - master.go:128   SaveToMaster (76.9%)
//   - master.go:162   ensureMasterPageInitialized (83.3%)
//   - master.go:284   clearMasterTable (83.3%)
//   - sqlite_sequence.go:230  GenerateAutoincrementRowid (85.7%) — MaxRowid branch
//   - trigger.go:185  evaluateWhenClause (85.7%) — nil expr + IdentExpr branches
//   - view.go:57      CreateView (90.9%) — reserved name + if-not-exists branches
//
// Run with: go test -run TestMCDC5 ./internal/schema/...

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ---------------------------------------------------------------------------
// insertDatabase (database.go:96) — 71.4%
// Uncovered branches:
//   A. bt == nil → skip schema load (fast path)
//   B. bt != nil, error from LoadFromMaster, PageCount <= 1 → no error returned
//   C. bt != nil, error from LoadFromMaster, PageCount >  1 → error returned
// ---------------------------------------------------------------------------

func TestMCDC5_InsertDatabase_NilBtree(t *testing.T) {
	t.Parallel()
	// A: bt == nil → schema load is skipped; no error expected.
	dr := NewDatabaseRegistry()
	err := dr.insertDatabase("main", "main", ":memory:", nil, nil)
	if err != nil {
		t.Errorf("insertDatabase nil btree: unexpected error: %v", err)
	}
	if _, ok := dr.databases["main"]; !ok {
		t.Error("insertDatabase nil btree: database not stored")
	}
}

func TestMCDC5_InsertDatabase_EmptyBtree_PageCount0(t *testing.T) {
	t.Parallel()
	// B: bt != nil, empty btree (page count 0 or 1) → LoadFromMaster may return
	// an error for an uninitialized page, but since PageCount <= 1 the error is
	// swallowed and insertDatabase returns nil.
	bt := btree.NewBtree(4096)
	mp := &mockPager{pageCount: 1}
	dr := NewDatabaseRegistry()
	err := dr.insertDatabase("db2", "db2", ":memory:", mp, bt)
	if err != nil {
		t.Errorf("insertDatabase empty btree page=1: unexpected error: %v", err)
	}
}

func TestMCDC5_InsertDatabase_EmptyBtree_PageCount0_ZeroPager(t *testing.T) {
	t.Parallel()
	// B variant: pageCount == 0
	bt := btree.NewBtree(4096)
	mp := &mockPager{pageCount: 0}
	dr := NewDatabaseRegistry()
	err := dr.insertDatabase("db3", "db3", ":memory:", mp, bt)
	if err != nil {
		t.Errorf("insertDatabase empty btree page=0: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// SaveToMaster (master.go:128) — 76.9%
// Uncovered branches:
//   A. bt == nil → error returned immediately
//   B. bt != nil, schema has rows → write rows path
// ---------------------------------------------------------------------------

func TestMCDC5_SaveToMaster_NilBtree(t *testing.T) {
	t.Parallel()
	// A: bt == nil → should return a "nil btree" error.
	s := NewSchema()
	err := s.SaveToMaster(nil)
	if err == nil {
		t.Error("SaveToMaster nil btree: expected error, got nil")
	}
}

func TestMCDC5_SaveToMaster_EmptySchema(t *testing.T) {
	t.Parallel()
	// B: bt != nil, schema is empty → ensureMasterPageInitialized + clearMasterTable +
	// zero rows written.  Should succeed.
	bt := btree.NewBtree(4096)
	s := NewSchema()
	err := s.SaveToMaster(bt)
	if err != nil {
		t.Errorf("SaveToMaster empty schema: unexpected error: %v", err)
	}
}

func TestMCDC5_SaveToMaster_WithTable(t *testing.T) {
	t.Parallel()
	// B variant: schema has a table → at least one row written.
	bt := btree.NewBtree(4096)
	s := NewSchema()
	s.Tables["t"] = &Table{
		Name:     "t",
		RootPage: 2,
		SQL:      "CREATE TABLE t (id INTEGER PRIMARY KEY)",
		Columns:  []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	err := s.SaveToMaster(bt)
	if err != nil {
		t.Errorf("SaveToMaster with table: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ensureMasterPageInitialized (master.go:162) — 83.3%
// Uncovered branch: GetPage returns an error (page does not yet exist).
// The function then creates a fresh page via SetPage.
// ---------------------------------------------------------------------------

func TestMCDC5_EnsureMasterPageInitialized_PageNotExist(t *testing.T) {
	t.Parallel()
	// A new in-memory btree has no pages; GetPage(1) will return an error,
	// triggering the SetPage branch.
	bt := btree.NewBtree(4096)
	err := ensureMasterPageInitialized(bt)
	if err != nil {
		t.Errorf("ensureMasterPageInitialized fresh btree: unexpected error: %v", err)
	}
}

func TestMCDC5_EnsureMasterPageInitialized_PageExists(t *testing.T) {
	t.Parallel()
	// Page 1 already exists (after a first call) → GetPage succeeds → no SetPage branch.
	bt := btree.NewBtree(4096)
	// First call initializes the page.
	if err := ensureMasterPageInitialized(bt); err != nil {
		t.Fatalf("first ensureMasterPageInitialized: unexpected error: %v", err)
	}
	// Second call exercises the "page already exists" path.
	if err := ensureMasterPageInitialized(bt); err != nil {
		t.Errorf("second ensureMasterPageInitialized: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// clearMasterTable (master.go:284) — 83.3%
// Uncovered branch: MoveToFirst returns an error (empty btree, nothing to clear).
// ---------------------------------------------------------------------------

func TestMCDC5_ClearMasterTable_Empty(t *testing.T) {
	t.Parallel()
	// Empty btree → MoveToFirst fails → return nil (nothing to clear).
	bt := btree.NewBtree(4096)
	s := NewSchema()
	err := s.clearMasterTable(bt)
	if err != nil {
		t.Errorf("clearMasterTable empty btree: unexpected error: %v", err)
	}
}

func TestMCDC5_ClearMasterTable_AfterSave(t *testing.T) {
	t.Parallel()
	// btree that has had rows written → clearMasterTable finds rows and deletes them.
	bt := btree.NewBtree(4096)
	s := NewSchema()
	s.Tables["u"] = &Table{
		Name:     "u",
		RootPage: 2,
		SQL:      "CREATE TABLE u (id INTEGER PRIMARY KEY)",
		Columns:  []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	// Write some rows first.
	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}
	// Now clear them.
	err := s.clearMasterTable(bt)
	if err != nil {
		t.Errorf("clearMasterTable after save: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GenerateAutoincrementRowid (sqlite_sequence.go:230) — 85.7%
// Uncovered branch: explicit rowid > MaxRowid → error.
// ---------------------------------------------------------------------------

func TestMCDC5_GenerateAutoincrementRowid_NextSequenceOverflow(t *testing.T) {
	t.Parallel()
	// Trigger the NextSequence overflow error by setting currentMaxRowid = MaxRowid.
	// GenerateAutoincrementRowid calls NextSequence when hasExplicitRowid=false,
	// and NextSequence returns an error when next >= MaxRowid.
	sm := NewSequenceManager()
	sm.InitSequence("tbl")
	// Set current sequence to MaxRowid so the next increment would overflow.
	sm.SetSequence("tbl", MaxRowid)
	_, err := GenerateAutoincrementRowid(sm, "tbl", 0, false, 0)
	if err == nil {
		t.Error("GenerateAutoincrementRowid overflow: expected error, got nil")
	}
}

func TestMCDC5_GenerateAutoincrementRowid_ExplicitAtMaxRowid(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()
	sm.InitSequence("tbl")
	// Explicit rowid == MaxRowid is valid (explicitRowid > MaxRowid is dead code).
	got, err := GenerateAutoincrementRowid(sm, "tbl", MaxRowid, true, 0)
	if err != nil {
		t.Errorf("GenerateAutoincrementRowid at MaxRowid: unexpected error: %v", err)
	}
	if got != MaxRowid {
		t.Errorf("GenerateAutoincrementRowid at MaxRowid: got %d, want %d", got, MaxRowid)
	}
}

// ---------------------------------------------------------------------------
// evaluateWhenClause (trigger.go:185) — 85.7%
// Uncovered branches:
//   A. expr == nil → returns true
//   B. *parser.IdentExpr → evaluateIdentExpr path
//   C. unsupported expression type → default → returns true
// ---------------------------------------------------------------------------

func TestMCDC5_EvaluateWhenClause_NilExpr(t *testing.T) {
	t.Parallel()
	// A: nil expression → evaluateWhenClause returns (true, nil).
	got, err := evaluateWhenClause(nil, nil, nil)
	if err != nil {
		t.Errorf("evaluateWhenClause nil: unexpected error: %v", err)
	}
	if !got {
		t.Error("evaluateWhenClause nil: expected true")
	}
}

func TestMCDC5_EvaluateWhenClause_IdentExpr_NewRow(t *testing.T) {
	t.Parallel()
	// B: IdentExpr referencing NEW.x where x is truthy.
	// Use Table field for the "NEW" qualifier and Name for the column.
	expr := &parser.IdentExpr{Table: "NEW", Name: "x"}
	newRow := map[string]interface{}{"x": int64(1)}
	got, err := evaluateWhenClause(expr, nil, newRow)
	if err != nil {
		t.Errorf("evaluateWhenClause IdentExpr: unexpected error: %v", err)
	}
	if !got {
		t.Error("evaluateWhenClause IdentExpr NEW.x=1: expected true")
	}
}

func TestMCDC5_EvaluateWhenClause_IdentExpr_OldRow(t *testing.T) {
	t.Parallel()
	// B: IdentExpr referencing OLD.x where x is falsy.
	expr := &parser.IdentExpr{Table: "OLD", Name: "x"}
	oldRow := map[string]interface{}{"x": int64(0)}
	got, err := evaluateWhenClause(expr, oldRow, nil)
	if err != nil {
		t.Errorf("evaluateWhenClause IdentExpr OLD.x=0: unexpected error: %v", err)
	}
	if got {
		t.Error("evaluateWhenClause IdentExpr OLD.x=0: expected false")
	}
}

func TestMCDC5_EvaluateWhenClause_LiteralExpr_True(t *testing.T) {
	t.Parallel()
	// LiteralExpr integer 1 → true.
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	got, err := evaluateWhenClause(expr, nil, nil)
	if err != nil {
		t.Errorf("evaluateWhenClause LiteralExpr 1: unexpected error: %v", err)
	}
	if !got {
		t.Error("evaluateWhenClause LiteralExpr 1: expected true")
	}
}

func TestMCDC5_EvaluateWhenClause_LiteralExpr_Zero(t *testing.T) {
	t.Parallel()
	// LiteralExpr integer 0 → false.
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}
	got, err := evaluateWhenClause(expr, nil, nil)
	if err != nil {
		t.Errorf("evaluateWhenClause LiteralExpr 0: unexpected error: %v", err)
	}
	if got {
		t.Error("evaluateWhenClause LiteralExpr 0: expected false")
	}
}

// ---------------------------------------------------------------------------
// CreateView (view.go:57) — 90.9%
// Uncovered branches:
//   A. stmt == nil → error
//   B. reserved name → error
//   C. view already exists + IfNotExists=true → return existing, nil
//   D. view already exists + IfNotExists=false → error
// ---------------------------------------------------------------------------

func TestMCDC5_CreateView_NilStmt(t *testing.T) {
	t.Parallel()
	// A: nil statement → error.
	s := NewSchema()
	_, err := s.CreateView(nil)
	if err == nil {
		t.Error("CreateView nil stmt: expected error, got nil")
	}
}

func TestMCDC5_CreateView_ReservedName(t *testing.T) {
	t.Parallel()
	// B: reserved name → error.
	s := NewSchema()
	stmt := &parser.CreateViewStmt{
		Name: "sqlite_master",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	_, err := s.CreateView(stmt)
	if err == nil {
		t.Error("CreateView reserved name: expected error, got nil")
	}
}

func TestMCDC5_CreateView_AlreadyExists_IfNotExists(t *testing.T) {
	t.Parallel()
	// C: view already exists + IfNotExists=true → return existing view, no error.
	s := NewSchema()
	existing := &View{Name: "v_users", SQL: "CREATE VIEW v_users AS SELECT 1"}
	s.Views["v_users"] = existing

	stmt := &parser.CreateViewStmt{
		Name:        "v_users",
		IfNotExists: true,
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	got, err := s.CreateView(stmt)
	if err != nil {
		t.Errorf("CreateView IfNotExists existing: unexpected error: %v", err)
	}
	if got != existing {
		t.Error("CreateView IfNotExists existing: expected existing view returned")
	}
}

func TestMCDC5_CreateView_AlreadyExists_Error(t *testing.T) {
	t.Parallel()
	// D: view already exists + IfNotExists=false → error.
	s := NewSchema()
	s.Views["v_users"] = &View{Name: "v_users"}

	stmt := &parser.CreateViewStmt{
		Name:        "v_users",
		IfNotExists: false,
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	_, err := s.CreateView(stmt)
	if err == nil {
		t.Error("CreateView duplicate no IfNotExists: expected error, got nil")
	}
}

func TestMCDC5_CreateView_Success(t *testing.T) {
	t.Parallel()
	// Happy path: new view is created.
	s := NewSchema()
	stmt := &parser.CreateViewStmt{
		Name: "v_active",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	view, err := s.CreateView(stmt)
	if err != nil {
		t.Errorf("CreateView success: unexpected error: %v", err)
	}
	if view == nil || view.Name != "v_active" {
		t.Errorf("CreateView success: got %v, want view named v_active", view)
	}
}

func TestMCDC5_CreateView_Temporary(t *testing.T) {
	t.Parallel()
	// Temporary view variant — exercises Temporary=true branch in generateCreateViewSQL.
	s := NewSchema()
	stmt := &parser.CreateViewStmt{
		Name:      "v_temp",
		Temporary: true,
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	view, err := s.CreateView(stmt)
	if err != nil {
		t.Errorf("CreateView temporary: unexpected error: %v", err)
	}
	if !view.Temporary {
		t.Error("CreateView temporary: expected Temporary=true")
	}
}
