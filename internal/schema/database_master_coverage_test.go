// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"errors"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// mockPager implements pager.PagerInterface with a configurable Close error.
type mockPager struct {
	closeErr  error
	pageCount pager.Pgno
}

func (m *mockPager) Get(_ pager.Pgno) (*pager.DbPage, error) { return nil, nil }
func (m *mockPager) Put(_ *pager.DbPage)                     {}
func (m *mockPager) PageSize() int                           { return 4096 }
func (m *mockPager) PageCount() pager.Pgno                   { return m.pageCount }
func (m *mockPager) IsReadOnly() bool                        { return false }
func (m *mockPager) GetHeader() *pager.DatabaseHeader        { return nil }
func (m *mockPager) GetFreePageCount() uint32                { return 0 }
func (m *mockPager) Write(_ *pager.DbPage) error             { return nil }
func (m *mockPager) AllocatePage() (pager.Pgno, error)       { return 0, nil }
func (m *mockPager) FreePage(_ pager.Pgno) error             { return nil }
func (m *mockPager) Vacuum(_ *pager.VacuumOptions) error     { return nil }
func (m *mockPager) SetUserVersion(_ uint32) error           { return nil }
func (m *mockPager) SetSchemaCookie(_ uint32) error          { return nil }
func (m *mockPager) VerifyFreeList() error                   { return nil }
func (m *mockPager) BeginRead() error                        { return nil }
func (m *mockPager) EndRead() error                          { return nil }
func (m *mockPager) BeginWrite() error                       { return nil }
func (m *mockPager) Commit() error                           { return nil }
func (m *mockPager) Rollback() error                         { return nil }
func (m *mockPager) InWriteTransaction() bool                { return false }
func (m *mockPager) Savepoint(_ string) error                { return nil }
func (m *mockPager) Release(_ string) error                  { return nil }
func (m *mockPager) RollbackTo(_ string) error               { return nil }
func (m *mockPager) Close() error                            { return m.closeErr }

// --- DatabaseCount ---

func TestDatabaseCount(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	if got := dr.DatabaseCount(); got != 0 {
		t.Errorf("DatabaseCount() = %d, want 0", got)
	}
	dr.AttachDatabase("main", "", nil, nil)
	if got := dr.DatabaseCount(); got != 1 {
		t.Errorf("DatabaseCount() after attach = %d, want 1", got)
	}
	dr.AttachDatabase("temp", "", nil, nil)
	if got := dr.DatabaseCount(); got != 2 {
		t.Errorf("DatabaseCount() after 2 attaches = %d, want 2", got)
	}
}

// --- ListDatabasesOrdered ---

func TestListDatabasesOrdered_Empty(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	result := dr.ListDatabasesOrdered()
	if len(result) != 0 {
		t.Errorf("ListDatabasesOrdered() = %d entries, want 0", len(result))
	}
}

func TestListDatabasesOrdered_MainOnly(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	result := dr.ListDatabasesOrdered()
	if len(result) != 1 {
		t.Fatalf("ListDatabasesOrdered() = %d entries, want 1", len(result))
	}
	if result[0].Name != "main" {
		t.Errorf("first entry = %q, want \"main\"", result[0].Name)
	}
}

func TestListDatabasesOrdered_Order(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	dr.AttachDatabase("temp", "", nil, nil)
	dr.AttachDatabase("extra", "", nil, nil)

	result := dr.ListDatabasesOrdered()
	if len(result) != 3 {
		t.Fatalf("ListDatabasesOrdered() = %d entries, want 3", len(result))
	}
	if result[0].Name != "main" {
		t.Errorf("result[0].Name = %q, want \"main\"", result[0].Name)
	}
	if result[1].Name != "temp" {
		t.Errorf("result[1].Name = %q, want \"temp\"", result[1].Name)
	}
}

func TestListDatabasesOrdered_NoMainNoTemp(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("extra1", "", nil, nil)
	dr.AttachDatabase("extra2", "", nil, nil)

	result := dr.ListDatabasesOrdered()
	if len(result) != 2 {
		t.Fatalf("ListDatabasesOrdered() = %d entries, want 2", len(result))
	}
}

// --- CloseAttached ---

func TestCloseAttached_Empty(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	if err := dr.CloseAttached(); err != nil {
		t.Errorf("CloseAttached() on empty registry error = %v", err)
	}
}

func TestCloseAttached_SkipsMainAndTemp(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	dr.AttachDatabase("temp", "", nil, nil)
	if err := dr.CloseAttached(); err != nil {
		t.Errorf("CloseAttached() error = %v", err)
	}
	if _, ok := dr.GetDatabase("main"); !ok {
		t.Error("main should still be attached after CloseAttached()")
	}
	if _, ok := dr.GetDatabase("temp"); !ok {
		t.Error("temp should still be attached after CloseAttached()")
	}
}

func TestCloseAttached_ClosesUserDB(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	mp := &mockPager{}
	dr.AttachDatabase("extra", "", mp, nil)
	if err := dr.CloseAttached(); err != nil {
		t.Errorf("CloseAttached() error = %v", err)
	}
	if _, ok := dr.GetDatabase("extra"); ok {
		t.Error("extra should be removed after CloseAttached()")
	}
}

func TestCloseAttached_PagerCloseError(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	mp := &mockPager{closeErr: errors.New("close failed")}
	dr.AttachDatabase("bad", "", mp, nil)
	if err := dr.CloseAttached(); err == nil {
		t.Error("CloseAttached() expected error when pager.Close() fails")
	}
}

// --- DetachDatabase with pager close error ---

func TestDetachDatabase_PagerCloseError(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	mp := &mockPager{closeErr: errors.New("close error")}
	dr.AttachDatabase("mydb", "", mp, nil)
	if err := dr.DetachDatabase("mydb"); err == nil {
		t.Error("DetachDatabase() expected error when pager.Close() fails")
	}
}

// --- checkAttachLimit ---

func TestCheckAttachLimit_ExceedsMax(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	// Fill up to max with user databases
	for i := 0; i < MaxAttachedDatabases; i++ {
		name := string(rune('a'+i)) + "db"
		if err := dr.AttachDatabase(name, "", nil, nil); err != nil {
			t.Fatalf("AttachDatabase(%q) error = %v", name, err)
		}
	}
	// One more should fail
	if err := dr.AttachDatabase("overflow", "", nil, nil); err == nil {
		t.Error("AttachDatabase() expected error when exceeding max attached databases")
	}
}

func TestCheckAttachLimit_MainAndTempNotCounted(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	// main and temp don't count against the limit
	dr.AttachDatabase("main", "", nil, nil)
	dr.AttachDatabase("temp", "", nil, nil)
	for i := 0; i < MaxAttachedDatabases; i++ {
		name := string(rune('a'+i)) + "db"
		if err := dr.AttachDatabase(name, "", nil, nil); err != nil {
			t.Fatalf("AttachDatabase(%q) error = %v (main/temp should not count)", name, err)
		}
	}
}

// --- checkDuplicatePath ---

func TestCheckDuplicatePath_SameFileTwice(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("first", "/data/myfile.db", nil, nil)
	if err := dr.AttachDatabase("second", "/data/myfile.db", nil, nil); err == nil {
		t.Error("AttachDatabase() expected error for duplicate file path")
	}
}

func TestCheckDuplicatePath_EmptyPathAllowed(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("a", "", nil, nil)
	if err := dr.AttachDatabase("b", "", nil, nil); err != nil {
		t.Errorf("AttachDatabase() with empty path should be allowed, got %v", err)
	}
}

func TestCheckDuplicatePath_MemoryAllowed(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("a", ":memory:", nil, nil)
	if err := dr.AttachDatabase("b", ":memory:", nil, nil); err != nil {
		t.Errorf("AttachDatabase() with :memory: path should be allowed, got %v", err)
	}
}

// --- ResolveTable ---

func TestResolveTable_Qualified(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["users"] = &Table{Name: "users"}

	table, db, schema, ok := dr.ResolveTable("main", "users")
	if !ok {
		t.Fatal("ResolveTable() not found")
	}
	if table.Name != "users" {
		t.Errorf("table.Name = %q, want \"users\"", table.Name)
	}
	if db == nil {
		t.Error("ResolveTable() db should not be nil")
	}
	if schema != "main" {
		t.Errorf("schema = %q, want \"main\"", schema)
	}
}

func TestResolveTable_QualifiedSchemaNotFound(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	_, _, _, ok := dr.ResolveTable("nonexistent", "users")
	if ok {
		t.Error("ResolveTable() should return false for missing schema")
	}
}

func TestResolveTable_QualifiedTableNotFound(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	_, _, _, ok := dr.ResolveTable("main", "missing")
	if ok {
		t.Error("ResolveTable() should return false for missing table")
	}
}

func TestResolveTable_Unqualified(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["orders"] = &Table{Name: "orders"}

	table, db, schema, ok := dr.ResolveTable("", "orders")
	if !ok {
		t.Fatal("ResolveTable() not found")
	}
	if table.Name != "orders" {
		t.Errorf("table.Name = %q, want \"orders\"", table.Name)
	}
	if db == nil {
		t.Error("ResolveTable() db should not be nil")
	}
	if schema != "main" {
		t.Errorf("schema = %q, want \"main\"", schema)
	}
}

func TestResolveTable_UnqualifiedNotFound(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	_, _, _, ok := dr.ResolveTable("", "ghost")
	if ok {
		t.Error("ResolveTable() should return false for missing table")
	}
}

func TestResolveTable_UnqualifiedPrefersMain(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	dr.AttachDatabase("main", "", nil, nil)
	dr.AttachDatabase("temp", "", nil, nil)
	mainDB, _ := dr.GetDatabase("main")
	mainDB.Schema.Tables["shared"] = &Table{Name: "shared"}
	tempDB, _ := dr.GetDatabase("temp")
	tempDB.Schema.Tables["shared"] = &Table{Name: "shared"}

	_, _, resolvedSchema, ok := dr.ResolveTable("", "shared")
	if !ok {
		t.Fatal("ResolveTable() not found")
	}
	if resolvedSchema != "main" {
		t.Errorf("schema = %q, want \"main\" (main has priority)", resolvedSchema)
	}
}

// --- insertDatabase with btree (non-nil) ---

func TestInsertDatabase_NilBtree(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	// nil btree: should succeed without loading schema
	if err := dr.AttachDatabase("nobt", "", nil, nil); err != nil {
		t.Errorf("AttachDatabase with nil btree error = %v", err)
	}
}

func TestInsertDatabase_BtreeEmptyDB(t *testing.T) {
	t.Parallel()
	dr := NewDatabaseRegistry()
	bt := btree.NewBtree(4096)
	mp := &mockPager{pageCount: 0}
	// pageCount <= 1: schema load error should be swallowed
	if err := dr.AttachDatabase("emptybt", "", mp, bt); err != nil {
		t.Errorf("AttachDatabase with empty btree (pageCount=0) error = %v", err)
	}
}

// --- decodeMasterRow / encodeMasterRow round-trip ---

func TestEncodeDecode_MasterRow(t *testing.T) {
	t.Parallel()
	original := MasterRow{
		Type:     "table",
		Name:     "users",
		TblName:  "users",
		RootPage: 42,
		SQL:      "CREATE TABLE users(id INTEGER)",
	}

	payload := encodeMasterRow(original)
	if len(payload) == 0 {
		t.Fatal("encodeMasterRow returned empty payload")
	}

	decoded, err := decodeMasterRow(payload)
	if err != nil {
		t.Fatalf("decodeMasterRow() error = %v", err)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.TblName != original.TblName {
		t.Errorf("TblName = %q, want %q", decoded.TblName, original.TblName)
	}
	if decoded.RootPage != original.RootPage {
		t.Errorf("RootPage = %d, want %d", decoded.RootPage, original.RootPage)
	}
	if decoded.SQL != original.SQL {
		t.Errorf("SQL = %q, want %q", decoded.SQL, original.SQL)
	}
}

func TestEncodeDecode_IndexRow(t *testing.T) {
	t.Parallel()
	original := MasterRow{
		Type:     "index",
		Name:     "idx_foo",
		TblName:  "foo",
		RootPage: 7,
		SQL:      "CREATE INDEX idx_foo ON foo(bar)",
	}
	payload := encodeMasterRow(original)
	decoded, err := decodeMasterRow(payload)
	if err != nil {
		t.Fatalf("decodeMasterRow() error = %v", err)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.RootPage != original.RootPage {
		t.Errorf("RootPage = %d, want %d", decoded.RootPage, original.RootPage)
	}
}

func TestDecodeMasterRow_TruncatedPayload(t *testing.T) {
	t.Parallel()
	// decodeMasterRow should error on too-short or invalid payload
	_, err := decodeMasterRow([]byte{})
	if err == nil {
		t.Error("decodeMasterRow(empty) expected error")
	}
}

// --- intToVarintBytes / varintBytesToUint ---

func TestIntToVarintBytes_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []uint64{0, 1, 127, 128, 255, 16383, 2097151}
	for _, v := range cases {
		b := intToVarintBytes(v)
		if len(b) == 0 {
			t.Errorf("intToVarintBytes(%d) returned empty slice", v)
			continue
		}
		got := varintBytesToUint(b)
		if got != v {
			t.Errorf("round-trip(%d): got %d", v, got)
		}
	}
}

// --- clearMasterTable via SaveToMaster round-trip ---

func TestClearMasterTable_ViaRoundTrip(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["tbl"] = &Table{
		Name:     "tbl",
		RootPage: 2,
		SQL:      "CREATE TABLE tbl(id INTEGER)",
	}

	// First save writes rows into master
	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("first SaveToMaster() error = %v", err)
	}

	// Second save should clear first and rewrite
	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("second SaveToMaster() error = %v", err)
	}
}

// --- parseMasterPage with populated btree ---

func TestParseMasterPage_AfterSave(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	bt := btree.NewBtree(4096)

	s.Tables["product"] = &Table{
		Name:     "product",
		RootPage: 2,
		SQL:      "CREATE TABLE product(id INTEGER)",
	}

	if err := s.SaveToMaster(bt); err != nil {
		t.Fatalf("SaveToMaster() error = %v", err)
	}

	rows, err := s.parseMasterPage(bt, 1)
	if err != nil {
		t.Fatalf("parseMasterPage() error = %v", err)
	}
	if len(rows) == 0 {
		t.Error("parseMasterPage() returned 0 rows after SaveToMaster")
	}
}

// --- buildMasterRows covers views and triggers ---

func TestBuildMasterRows_WithViewsAndTriggers(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["t"] = &Table{Name: "t", RootPage: 2, SQL: "CREATE TABLE t(id INTEGER)"}
	s.Views["v"] = &View{Name: "v", SQL: "CREATE VIEW v AS SELECT 1"}
	s.Triggers["tr"] = &Trigger{Name: "tr", Table: "t", SQL: "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"}
	s.Indexes["idx"] = &Index{Name: "idx", Table: "t", SQL: "CREATE INDEX idx ON t(id)"}

	rows := s.buildMasterRows()
	typeCount := make(map[string]int)
	for _, r := range rows {
		typeCount[r.Type]++
	}

	if typeCount["table"] == 0 {
		t.Error("buildMasterRows: expected at least one table row")
	}
	if typeCount["view"] == 0 {
		t.Error("buildMasterRows: expected at least one view row")
	}
	if typeCount["trigger"] == 0 {
		t.Error("buildMasterRows: expected at least one trigger row")
	}
	if typeCount["index"] == 0 {
		t.Error("buildMasterRows: expected at least one index row")
	}
}

func TestBuildMasterRows_SkipsInternalTables(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["sqlite_master"] = &Table{Name: "sqlite_master", RootPage: 1}
	s.Tables["real"] = &Table{Name: "real", RootPage: 2, SQL: "CREATE TABLE real(id INTEGER)"}

	rows := s.buildMasterRows()
	for _, r := range rows {
		if r.Name == "sqlite_master" {
			t.Error("buildMasterRows should skip sqlite_master")
		}
	}
}

func TestBuildMasterRows_AutoIndexWithSQL(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["sqlite_autoindex_t_1"] = &Index{
		Name:  "sqlite_autoindex_t_1",
		Table: "t",
		SQL:   "CREATE UNIQUE INDEX sqlite_autoindex_t_1 ON t(id)",
	}

	rows := s.buildMasterRows()
	found := false
	for _, r := range rows {
		if r.Name == "sqlite_autoindex_t_1" {
			found = true
		}
	}
	if !found {
		t.Error("buildMasterRows: auto-index with SQL should be included")
	}
}
