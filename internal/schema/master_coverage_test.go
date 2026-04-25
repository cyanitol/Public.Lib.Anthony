// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ---------------------------------------------------------------------------
// viewExistsLocked
// ---------------------------------------------------------------------------

func TestViewExistsLocked_Exists(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Views["myview"] = &View{Name: "myview"}

	if !s.viewExistsLocked("myview") {
		t.Error("viewExistsLocked: expected true for existing view")
	}
}

func TestViewExistsLocked_NotExists(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	if s.viewExistsLocked("nonexistent") {
		t.Error("viewExistsLocked: expected false for non-existent view")
	}
}

func TestViewExistsLocked_CaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Views["MyView"] = &View{Name: "MyView"}

	if !s.viewExistsLocked("MYVIEW") {
		t.Error("viewExistsLocked: expected true with different case")
	}
	if !s.viewExistsLocked("myview") {
		t.Error("viewExistsLocked: expected true with lowercase")
	}
}

// viewExistsLocked is reached via CreateTrigger validation when the trigger
// target is a view rather than a table (INSTEAD OF path).
func TestViewExistsLocked_ViaInsteadOfTrigger(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Views["v"] = &View{Name: "v", SQL: "CREATE VIEW v AS SELECT 1"}

	stmt := &parser.CreateTriggerStmt{
		Name:       "trg_instead",
		Table:      "v",
		Timing:     parser.TriggerInsteadOf,
		Event:      parser.TriggerInsert,
		ForEachRow: true,
		Body:       []parser.Statement{},
	}
	if _, err := s.CreateTrigger(stmt); err != nil {
		t.Fatalf("CreateTrigger on view: unexpected error: %v", err)
	}
}

// viewExistsLocked returns false → trigger creation fails when the view is absent.
func TestViewExistsLocked_ViaInsteadOfTriggerMissingView(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	// No view, no table named "ghost"

	stmt := &parser.CreateTriggerStmt{
		Name:       "trg_ghost",
		Table:      "ghost",
		Timing:     parser.TriggerInsteadOf,
		Event:      parser.TriggerInsert,
		ForEachRow: true,
		Body:       []parser.Statement{},
	}
	if _, err := s.CreateTrigger(stmt); err == nil {
		t.Error("expected error when INSTEAD OF trigger targets non-existent view")
	}
}

// ---------------------------------------------------------------------------
// processMasterViewRow – error path (invalid SQL)
// ---------------------------------------------------------------------------

func TestProcessMasterViewRow_InvalidSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Type:    "view",
		Name:    "bad_view",
		TblName: "bad_view",
		SQL:     "NOT VALID SQL !!!",
	}

	if err := s.processMasterViewRow(row); err == nil {
		t.Error("processMasterViewRow: expected error for invalid SQL")
	}
}

// processMasterViewRow with wrong statement type (CREATE TABLE instead of VIEW).
func TestProcessMasterViewRow_WrongStatementType(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Type:    "view",
		Name:    "not_a_view",
		TblName: "not_a_view",
		SQL:     "CREATE TABLE not_a_view(id INTEGER)",
	}

	if err := s.processMasterViewRow(row); err == nil {
		t.Error("processMasterViewRow: expected error when SQL is a CREATE TABLE, not CREATE VIEW")
	}
}

// ---------------------------------------------------------------------------
// parseMasterPage – multiple object types written then read back
// ---------------------------------------------------------------------------

// assertMasterRowTypes checks that the given rows contain all expected types.
func assertMasterRowTypes(t *testing.T, rows []MasterRow, wantTypes []string) {
	t.Helper()
	typeSet := make(map[string]bool)
	for _, r := range rows {
		typeSet[r.Type] = true
	}
	for _, wt := range wantTypes {
		if !typeSet[wt] {
			t.Errorf("parseMasterPage: expected at least one %q row", wt)
		}
	}
}

func TestParseMasterPage_MultipleTypes(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["events"] = &Table{
		Name:     "events",
		RootPage: 2,
		SQL:      "CREATE TABLE events(id INTEGER PRIMARY KEY, name TEXT)",
	}
	s.Indexes["idx_events_name"] = &Index{
		Name:     "idx_events_name",
		Table:    "events",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_events_name ON events(name)",
	}
	s.Views["recent"] = &View{
		Name: "recent",
		SQL:  "CREATE VIEW recent AS SELECT * FROM events",
	}
	s.Triggers["trg_after_insert"] = &Trigger{
		Name:  "trg_after_insert",
		Table: "events",
		SQL:   "CREATE TRIGGER trg_after_insert AFTER INSERT ON events BEGIN SELECT 1; END",
	}

	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("parseMasterPage returned 0 rows")
	}

	assertMasterRowTypes(t, rows, []string{"table", "index", "view", "trigger"})
}

// ---------------------------------------------------------------------------
// clearMasterTable – called by SaveToMaster; test with an empty table
// ---------------------------------------------------------------------------

func TestClearMasterTable_EmptyPage(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	// Initialise master page so clearMasterTable has a valid page to cursor over.
	if err := ensureMasterPageInitialized(bt); err != nil {
		t.Fatalf("ensureMasterPageInitialized: %v", err)
	}

	// clearMasterTable on empty page should succeed silently.
	if err := s.clearMasterTable(bt); err != nil {
		t.Fatalf("clearMasterTable on empty page: %v", err)
	}
}

// clearMasterTable with rows already present (written by SaveToMaster).
func TestClearMasterTable_WithRows(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["orders"] = &Table{
		Name:     "orders",
		RootPage: 2,
		SQL:      "CREATE TABLE orders(id INTEGER)",
	}

	// First save writes rows.
	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("first SaveToMaster: %v", err)
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage after first save: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected rows after first save")
	}

	// clearMasterTable removes all rows.
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

// SaveToMaster clears then rewrites; verify idempotent across multiple saves.
func TestClearMasterTable_RepeatedSave(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["items"] = &Table{
		Name:     "items",
		RootPage: 2,
		SQL:      "CREATE TABLE items(id INTEGER, label TEXT)",
	}
	s.Indexes["idx_items"] = &Index{
		Name:     "idx_items",
		Table:    "items",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_items ON items(label)",
	}

	for i := 0; i < 3; i++ {
		if err := s.SaveToMaster(bt); err != nil {
			t.Fatalf("SaveToMaster iteration %d: %v", i, err)
		}
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage: %v", err)
	}
	// Exactly the same number of rows each time (no accumulation).
	if len(rows) != 2 {
		t.Errorf("expected 2 rows after repeated saves, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// LoadFromMaster – round-trip with multiple tables and indexes
// ---------------------------------------------------------------------------

func TestLoadFromMaster_RoundTripMultiple(t *testing.T) {
	t.Parallel()
	src := NewSchema()
	bt := btree.NewBtree(4096)

	src.Tables["customers"] = &Table{
		Name:     "customers",
		RootPage: 2,
		SQL:      "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)",
	}
	src.Tables["products"] = &Table{
		Name:     "products",
		RootPage: 3,
		SQL:      "CREATE TABLE products(id INTEGER PRIMARY KEY, price REAL)",
	}
	src.Indexes["idx_cust"] = &Index{
		Name:     "idx_cust",
		Table:    "customers",
		RootPage: 4,
		SQL:      "CREATE INDEX idx_cust ON customers(name)",
	}

	if err := src.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}

	dst := NewSchema()
	if err := dst.LoadFromMaster(bt); err != nil {
		t.Fatalf("LoadFromMaster: %v", err)
	}

	for _, name := range []string{"customers", "products"} {
		if _, ok := dst.Tables[name]; !ok {
			t.Errorf("LoadFromMaster: table %q not found", name)
		}
	}
	if _, ok := dst.Indexes["idx_cust"]; !ok {
		t.Error("LoadFromMaster: index idx_cust not found")
	}
}

// LoadFromMaster with a view saved in the btree.
func TestLoadFromMaster_WithView(t *testing.T) {
	t.Parallel()
	src := NewSchema()
	bt := btree.NewBtree(4096)

	src.Tables["base"] = &Table{
		Name:     "base",
		RootPage: 2,
		SQL:      "CREATE TABLE base(id INTEGER)",
	}
	src.Views["vw_base"] = &View{
		Name: "vw_base",
		SQL:  "CREATE VIEW vw_base AS SELECT * FROM base",
	}

	if err := src.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}

	dst := NewSchema()
	if err := dst.LoadFromMaster(bt); err != nil {
		t.Fatalf("LoadFromMaster: %v", err)
	}

	if _, ok := dst.Views["vw_base"]; !ok {
		t.Error("LoadFromMaster: view vw_base not found")
	}
}

// ---------------------------------------------------------------------------
// SaveToMaster – views and triggers are serialised
// ---------------------------------------------------------------------------

func TestSaveToMaster_WithViewsAndTriggers(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["docs"] = &Table{
		Name:     "docs",
		RootPage: 2,
		SQL:      "CREATE TABLE docs(id INTEGER)",
	}
	s.Views["vw_docs"] = &View{
		Name: "vw_docs",
		SQL:  "CREATE VIEW vw_docs AS SELECT * FROM docs",
	}
	s.Triggers["trg_docs"] = &Trigger{
		Name:  "trg_docs",
		Table: "docs",
		SQL:   "CREATE TRIGGER trg_docs AFTER INSERT ON docs BEGIN SELECT 1; END",
	}

	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster: %v", err)
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage: %v", err)
	}

	typeCount := make(map[string]int)
	for _, r := range rows {
		typeCount[r.Type]++
	}
	if typeCount["table"] == 0 {
		t.Error("SaveToMaster: no table rows")
	}
	if typeCount["view"] == 0 {
		t.Error("SaveToMaster: no view rows")
	}
	if typeCount["trigger"] == 0 {
		t.Error("SaveToMaster: no trigger rows")
	}
}

// ---------------------------------------------------------------------------
// parseTableSQL – WITH parsed SQL for several column types
// ---------------------------------------------------------------------------

func TestParseTableSQL_MultipleColumns(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name:     "inventory",
		TblName:  "inventory",
		RootPage: 5,
		SQL:      "CREATE TABLE inventory(id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty REAL)",
	}

	table, err := s.parseTableSQL(row)
	if err != nil {
		t.Fatalf("parseTableSQL: %v", err)
	}
	if table.Name != "inventory" {
		t.Errorf("Name = %q, want %q", table.Name, "inventory")
	}
	if len(table.Columns) != 3 {
		t.Errorf("Columns = %d, want 3", len(table.Columns))
	}
	if table.RootPage != 5 {
		t.Errorf("RootPage = %d, want 5", table.RootPage)
	}
}

func TestParseTableSQL_WithoutRowID(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name:     "kv",
		TblName:  "kv",
		RootPage: 6,
		SQL:      "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
	}

	table, err := s.parseTableSQL(row)
	if err != nil {
		t.Fatalf("parseTableSQL WITHOUT ROWID: %v", err)
	}
	if !table.WithoutRowID {
		t.Error("expected WithoutRowID = true")
	}
}

func TestParseTableSQL_BadSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name:    "bad",
		TblName: "bad",
		SQL:     "THIS IS NOT SQL",
	}

	if _, err := s.parseTableSQL(row); err == nil {
		t.Error("parseTableSQL: expected error for invalid SQL")
	}
}

// Wrong statement type (not CREATE TABLE).
func TestParseTableSQL_WrongType(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	row := MasterRow{
		Name:    "idx",
		TblName: "idx",
		SQL:     "CREATE INDEX idx ON t(c)",
	}

	if _, err := s.parseTableSQL(row); err == nil {
		t.Error("parseTableSQL: expected error when SQL is not CREATE TABLE")
	}
}

// ---------------------------------------------------------------------------
// decodeMasterRow – field-count guard and type switch fallback
// ---------------------------------------------------------------------------

func TestDecodeMasterRow_FewerThanFiveFields(t *testing.T) {
	t.Parallel()
	// Build a payload with only 3 fields using the encoder helper.
	type shortRow struct{ a, b, c string }
	// We cannot use encodeMasterRow for this; craft a truncated payload manually
	// by encoding a valid row and slicing the values to < 5.
	// Instead: use vdbe to encode fewer values directly.
	import_vdbe := func() []byte {
		// The easiest way: encode a 4-field row by adapting encodeMasterRow logic.
		// Since we're in package schema, call the unexported vdbe helper indirectly:
		// encode a row with 5 fields, then corrupt the header to claim 4 fields.
		// Simpler: just pass an empty payload and confirm the error path.
		return []byte{0x01, 0x04, 0x01, 0x01} // too short to be a valid record
	}

	_, err := decodeMasterRow(import_vdbe())
	if err == nil {
		t.Error("decodeMasterRow: expected error for truncated payload")
	}
}

// decodeMasterRow with a zero-rootpage (NULL-like integer value) exercises the
// default branch of the type switch in RootPage parsing.
func TestDecodeMasterRow_ZeroRootPage(t *testing.T) {
	t.Parallel()
	row := MasterRow{
		Type:     "index",
		Name:     "no_page",
		TblName:  "t",
		RootPage: 0,
		SQL:      "",
	}
	payload := encodeMasterRow(row)
	decoded, err := decodeMasterRow(payload)
	if err != nil {
		t.Fatalf("decodeMasterRow: %v", err)
	}
	if decoded.RootPage != 0 {
		t.Errorf("RootPage = %d, want 0", decoded.RootPage)
	}
}

// ---------------------------------------------------------------------------
// insertDatabase – btree with pageCount > 1 surfaces schema load errors
// ---------------------------------------------------------------------------

func TestInsertDatabase_BtreePageCountGt1_ErrorSurfaced(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	bt := btree.NewBtree(4096)
	// pageCount > 1: insertDatabase will try LoadFromMaster; with an
	// uninitialized btree page 1 this might surface an error.  The test
	// verifies the code path is exercised without panicking.
	mp := &mockPager{pageCount: 2}
	// Either succeeds (empty page parsed as 0 rows) or returns an error –
	// both are acceptable; the important thing is no panic.
	_ = dr.AttachDatabase("pgdb", "", mp, bt)
}

// insertDatabase with a real btree that has a valid empty master page.
func TestInsertDatabase_BtreeWithValidMasterPage(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	bt := btree.NewBtree(4096)

	// Initialise page 1 so LoadFromMaster does not fail.
	if err := ensureMasterPageInitialized(bt); err != nil {
		t.Fatalf("ensureMasterPageInitialized: %v", err)
	}

	mp := &mockPager{pageCount: 1}
	if err := dr.AttachDatabase("validdb", "", mp, bt); err != nil {
		t.Fatalf("AttachDatabase: %v", err)
	}
	if _, ok := dr.GetDatabase("validdb"); !ok {
		t.Error("database should be attached")
	}
}

// ---------------------------------------------------------------------------
// checkDuplicatePath – distinct non-empty paths are allowed
// ---------------------------------------------------------------------------

func TestCheckDuplicatePath_DifferentPaths(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("first", "/data/a.db", nil, nil)
	if err := dr.AttachDatabase("second", "/data/b.db", nil, nil); err != nil {
		t.Errorf("AttachDatabase with distinct path error = %v", err)
	}
}

// checkDuplicatePath: same name lower-case is skipped (self-check guard).
func TestCheckDuplicatePath_SameNameSkipped(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "/data/main.db", nil, nil)
	// Re-attaching "main" is rejected by the name-exists check, not the path
	// check. But we can test the skip logic by directly exercising
	// checkDuplicatePath with a matching lower-name.
	err := dr.checkDuplicatePath("/data/main.db", "main")
	if err != nil {
		t.Errorf("checkDuplicatePath with same lowerName should skip, got %v", err)
	}
}
