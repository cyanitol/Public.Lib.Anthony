// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"testing"
)

// --- Table/Index/View/Trigger count accessors ---

func TestCountAccessors(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	if s.TableCount() != 0 {
		t.Errorf("TableCount() = %d, want 0", s.TableCount())
	}
	if s.IndexCount() != 0 {
		t.Errorf("IndexCount() = %d, want 0", s.IndexCount())
	}
	if s.ViewCount() != 0 {
		t.Errorf("ViewCount() = %d, want 0", s.ViewCount())
	}
	if s.TriggerCount() != 0 {
		t.Errorf("TriggerCount() = %d, want 0", s.TriggerCount())
	}

	s.Tables["t"] = &Table{Name: "t"}
	s.Indexes["i"] = &Index{Name: "i"}
	s.Views["v"] = &View{Name: "v"}
	s.Triggers["tr"] = &Trigger{Name: "tr"}

	if s.TableCount() != 1 {
		t.Errorf("TableCount() = %d, want 1", s.TableCount())
	}
	if s.IndexCount() != 1 {
		t.Errorf("IndexCount() = %d, want 1", s.IndexCount())
	}
	if s.ViewCount() != 1 {
		t.Errorf("ViewCount() = %d, want 1", s.ViewCount())
	}
	if s.TriggerCount() != 1 {
		t.Errorf("TriggerCount() = %d, want 1", s.TriggerCount())
	}
}

// --- IsView ---

func TestIsView(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Views["myview"] = &View{Name: "myview"}

	if !s.IsView("myview") {
		t.Error("expected IsView to return true for existing view")
	}
	if s.IsView("notaview") {
		t.Error("expected IsView to return false for non-existent view")
	}
}

// --- GetTableByName ---

func TestGetTableByName(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["users"] = &Table{Name: "users"}

	tbl, ok := s.GetTableByName("users")
	if !ok {
		t.Fatal("expected to find users table")
	}
	if tbl == nil {
		t.Error("expected non-nil table")
	}

	_, ok = s.GetTableByName("nonexistent")
	if ok {
		t.Error("expected false for non-existent table")
	}
}

// --- GetTableByRootPage ---

func TestGetTableByRootPage(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["users"] = &Table{Name: "users", RootPage: 42}

	tbl, ok := s.GetTableByRootPage(42)
	if !ok {
		t.Fatal("expected to find table with rootpage 42")
	}
	if tbl == nil {
		t.Error("expected non-nil result")
	}

	_, ok = s.GetTableByRootPage(999)
	if ok {
		t.Error("expected false for unknown rootpage")
	}
}

// --- ListIndexesForTable ---

func TestListIndexesForTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = &Table{Name: "t"}
	s.Indexes["idx1"] = &Index{Name: "idx1", Table: "t"}
	s.Indexes["idx2"] = &Index{Name: "idx2", Table: "t"}
	s.Indexes["other"] = &Index{Name: "other", Table: "other_table"}

	result := s.ListIndexesForTable("t")
	if len(result) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(result))
	}

	result = s.ListIndexesForTable("nonexistent")
	if len(result) != 0 {
		t.Errorf("expected 0 indexes for nonexistent table, got %d", len(result))
	}
}

// --- isRowidAlias ---

func TestIsRowidAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want bool
	}{
		{"rowid", true},
		{"ROWID", true},
		{"_rowid_", true},
		{"oid", true},
		{"OID", true},
		{"id", false},
		{"my_row", false},
	}
	for _, tt := range tests {
		got := isRowidAlias(tt.name)
		if got != tt.want {
			t.Errorf("isRowidAlias(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// --- Table.findIntegerPrimaryKeyIndex ---

func TestFindIntegerPrimaryKeyIndex(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name: "t",
		Columns: []*Column{
			{Name: "name", Type: "TEXT"},
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}
	idx := tbl.findIntegerPrimaryKeyIndex()
	if idx != 1 {
		t.Errorf("findIntegerPrimaryKeyIndex() = %d, want 1", idx)
	}

	noIntPK := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "id", Type: "TEXT", PrimaryKey: true}},
	}
	idx = noIntPK.findIntegerPrimaryKeyIndex()
	if idx != -1 {
		t.Errorf("findIntegerPrimaryKeyIndex() = %d, want -1 for non-integer PK", idx)
	}
}

// --- Table.GetIntegerPKColumn ---

func TestGetIntegerPKColumn(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	if got := tbl.GetIntegerPKColumn(); got != "id" {
		t.Errorf("GetIntegerPKColumn() = %q, want 'id'", got)
	}

	noIntPK := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "pk", Type: "TEXT", PrimaryKey: true}},
	}
	if got := noIntPK.GetIntegerPKColumn(); got != "" {
		t.Errorf("GetIntegerPKColumn() = %q, want '' for text PK", got)
	}
}

// --- Table.GetColumnIndexWithRowidAliases ---

func TestGetColumnIndexWithRowidAliases(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "name", Type: "TEXT"}, {Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}

	// Direct column match
	if idx := tbl.GetColumnIndexWithRowidAliases("name"); idx != 0 {
		t.Errorf("direct match 'name' = %d, want 0", idx)
	}

	// Rowid alias matches integer PK
	if idx := tbl.GetColumnIndexWithRowidAliases("rowid"); idx != 1 {
		t.Errorf("rowid alias = %d, want 1 (integer PK column)", idx)
	}

	// Non-existent, non-alias column
	if idx := tbl.GetColumnIndexWithRowidAliases("ghost"); idx != -1 {
		t.Errorf("missing column = %d, want -1", idx)
	}

	// Rowid alias on table without integer PK -> -2
	noIntPK := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "pk", Type: "TEXT", PrimaryKey: true}},
	}
	if idx := noIntPK.GetColumnIndexWithRowidAliases("rowid"); idx != -2 {
		t.Errorf("rowid alias without INT PK = %d, want -2", idx)
	}
}

// --- Column.GetEffectiveCollation and GetCollation ---

func TestColumnCollation(t *testing.T) {
	t.Parallel()
	col := &Column{Collation: "RTRIM"}

	if got := col.GetCollation(); got != "RTRIM" {
		t.Errorf("GetCollation() = %q, want 'RTRIM'", got)
	}
	if got := col.GetEffectiveCollation(); got != "RTRIM" {
		t.Errorf("GetEffectiveCollation() = %q, want 'RTRIM'", got)
	}

	empty := &Column{}
	if got := empty.GetCollation(); got != "" {
		t.Errorf("GetCollation() empty = %q", got)
	}
	if got := empty.GetEffectiveCollation(); got != "BINARY" {
		t.Errorf("GetEffectiveCollation() default = %q, want 'BINARY'", got)
	}
}

// --- Table.HasRowID ---

func TestHasRowID(t *testing.T) {
	t.Parallel()
	normal := &Table{Name: "t", WithoutRowID: false}
	if !normal.HasRowID() {
		t.Error("normal table should have rowid")
	}

	withoutRowID := &Table{Name: "t", WithoutRowID: true}
	if withoutRowID.HasRowID() {
		t.Error("WITHOUT ROWID table should not have rowid")
	}
}

// --- Table.GetRecordColumnNames ---

func TestGetRecordColumnNames(t *testing.T) {
	t.Parallel()
	// Normal table: INTEGER PK excluded from record
	tbl := &Table{
		Name: "t",
		Columns: []*Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		WithoutRowID: false,
	}
	names := tbl.GetRecordColumnNames()
	if len(names) != 1 {
		t.Fatalf("expected 1 record column (id excluded), got %d", len(names))
	}
	if names[0] != "name" {
		t.Errorf("expected 'name', got %q", names[0])
	}

	// WITHOUT ROWID: all columns included
	withoutRowID := &Table{
		Name: "t",
		Columns: []*Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		WithoutRowID: true,
	}
	names = withoutRowID.GetRecordColumnNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 record columns for WITHOUT ROWID, got %d", len(names))
	}
}

// --- Table.GetRowidColumnName ---

func TestGetRowidColumnName(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	if got := tbl.GetRowidColumnName(); got != "id" {
		t.Errorf("GetRowidColumnName() = %q, want 'id'", got)
	}

	noIntPK := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "pk", Type: "TEXT", PrimaryKey: true}},
	}
	if got := noIntPK.GetRowidColumnName(); got != "" {
		t.Errorf("GetRowidColumnName() = %q, want '' for non-integer PK", got)
	}

	withoutRowID := &Table{
		Name:         "t",
		Columns:      []*Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		WithoutRowID: true,
	}
	if got := withoutRowID.GetRowidColumnName(); got != "" {
		t.Errorf("GetRowidColumnName() = %q, want '' for WITHOUT ROWID table", got)
	}
}

// --- Table.GetPrimaryKey ---

func TestGetPrimaryKey(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:       "t",
		PrimaryKey: []string{"id", "name"},
	}
	pk := tbl.GetPrimaryKey()
	if len(pk) != 2 || pk[0] != "id" || pk[1] != "name" {
		t.Errorf("GetPrimaryKey() = %v, want [id name]", pk)
	}
}

// --- Table.GetColumns, GetColumnNames ---

func TestTableGetColumnsAndNames(t *testing.T) {
	t.Parallel()
	tbl := &Table{
		Name:    "t",
		Columns: []*Column{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "TEXT"}},
	}

	cols := tbl.GetColumns()
	if len(cols) != 2 {
		t.Errorf("GetColumns() len = %d, want 2", len(cols))
	}

	names := tbl.GetColumnNames()
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Errorf("GetColumnNames() = %v, want [a b]", names)
	}
}

// --- Column interface accessors ---

func TestColumnAccessors(t *testing.T) {
	t.Parallel()
	defaultVal := "42"
	col := &Column{
		Name:       "score",
		Type:       "INTEGER",
		NotNull:    true,
		PrimaryKey: true,
		Unique:     true,
		Default:    &defaultVal,
		Check:      "score > 0",
	}

	if col.GetName() != "score" {
		t.Errorf("GetName() = %q, want 'score'", col.GetName())
	}
	if col.GetType() != "INTEGER" {
		t.Errorf("GetType() = %q, want 'INTEGER'", col.GetType())
	}
	if !col.IsNotNull() {
		t.Error("IsNotNull() should be true")
	}
	if !col.GetNotNull() {
		t.Error("GetNotNull() should be true")
	}
	if !col.IsPrimaryKeyColumn() {
		t.Error("IsPrimaryKeyColumn() should be true")
	}
	if !col.IsUniqueColumn() {
		t.Error("IsUniqueColumn() should be true")
	}
	if col.GetCheck() != "score > 0" {
		t.Errorf("GetCheck() = %q, want 'score > 0'", col.GetCheck())
	}
	if col.GetDefault() == nil {
		t.Error("GetDefault() should not be nil")
	}
	if !col.IsIntegerPrimaryKey() {
		t.Error("IsIntegerPrimaryKey() should be true for INTEGER PK")
	}
}

// --- Table.GetTableStats / SetTableStats ---

func TestTableStats(t *testing.T) {
	t.Parallel()
	tbl := &Table{Name: "t"}

	if tbl.GetTableStats() != nil {
		t.Error("expected nil stats initially")
	}

	stats := &TableStats{RowCount: 100, AverageRowSize: 64}
	tbl.SetTableStats(stats)

	got := tbl.GetTableStats()
	if got == nil {
		t.Fatal("expected stats after SetTableStats")
	}
	if got.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", got.RowCount)
	}
}

// --- Table.SetRootPage / GetRootPage ---

func TestTableRootPage(t *testing.T) {
	t.Parallel()
	tbl := &Table{Name: "t"}
	tbl.SetRootPage(5)
	if tbl.GetRootPage() != 5 {
		t.Errorf("GetRootPage() = %d, want 5", tbl.GetRootPage())
	}
}

// --- Index.IsUnique / GetColumns ---

func TestIndexAccessors(t *testing.T) {
	t.Parallel()
	idx := &Index{
		Name:    "idx",
		Table:   "t",
		Unique:  true,
		Columns: []string{"a", "b"},
	}

	if !idx.IsUnique() {
		t.Error("IsUnique() should be true")
	}
	if cols := idx.GetColumns(); len(cols) != 2 {
		t.Errorf("GetColumns() len = %d, want 2", len(cols))
	}

	nonUnique := &Index{Unique: false}
	if nonUnique.IsUnique() {
		t.Error("IsUnique() should be false for non-unique index")
	}
}

// --- AddTriggerDirect, AddTableDirect, AddIndexDirect, AddViewDirect ---

func TestDirectAddMethods(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	tbl := &Table{Name: "direct_table"}
	s.AddTableDirect(tbl)
	if _, ok := s.Tables["direct_table"]; !ok {
		t.Error("AddTableDirect: table not found")
	}

	idx := &Index{Name: "direct_idx", Table: "direct_table"}
	s.AddIndexDirect(idx)
	if _, ok := s.Indexes["direct_idx"]; !ok {
		t.Error("AddIndexDirect: index not found")
	}

	view := &View{Name: "direct_view"}
	s.AddViewDirect(view)
	if _, ok := s.Views["direct_view"]; !ok {
		t.Error("AddViewDirect: view not found")
	}

	trigger := &Trigger{Name: "direct_trigger"}
	s.AddTriggerDirect(trigger)
	if _, ok := s.Triggers["direct_trigger"]; !ok {
		t.Error("AddTriggerDirect: trigger not found")
	}
}

// --- AddTriggerDirect with nil Triggers map ---

func TestAddTriggerDirect_NilMap(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Triggers = nil

	trigger := &Trigger{Name: "x"}
	s.AddTriggerDirect(trigger)

	if s.Triggers == nil {
		t.Error("Triggers map should be initialized after AddTriggerDirect")
	}
}

// --- buildMasterRows covers views and triggers paths ---

func TestBuildMasterRows_AllTypes(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = &Table{Name: "t", RootPage: 2, SQL: "CREATE TABLE t(id INTEGER)"}
	s.Indexes["i"] = &Index{Name: "i", Table: "t", RootPage: 3, SQL: "CREATE INDEX i ON t(id)"}
	s.Views["v"] = &View{Name: "v", SQL: "CREATE VIEW v AS SELECT 1"}
	s.Triggers["tr"] = &Trigger{Name: "tr", Table: "t", SQL: "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"}

	rows := s.buildMasterRows()

	// Should have 4 rows (table, index, view, trigger)
	if len(rows) != 4 {
		t.Errorf("buildMasterRows() returned %d rows, want 4", len(rows))
	}

	typeCount := map[string]int{}
	for _, row := range rows {
		typeCount[row.Type]++
	}
	if typeCount["table"] != 1 {
		t.Errorf("expected 1 table row, got %d", typeCount["table"])
	}
	if typeCount["index"] != 1 {
		t.Errorf("expected 1 index row, got %d", typeCount["index"])
	}
	if typeCount["view"] != 1 {
		t.Errorf("expected 1 view row, got %d", typeCount["view"])
	}
	if typeCount["trigger"] != 1 {
		t.Errorf("expected 1 trigger row, got %d", typeCount["trigger"])
	}
}

func TestBuildMasterRows_SkipsInternalTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["sqlite_master"] = &Table{Name: "sqlite_master", RootPage: 1}
	s.Tables["users"] = &Table{Name: "users", RootPage: 2, SQL: "CREATE TABLE users(id INTEGER)"}

	rows := s.buildMasterRows()
	for _, row := range rows {
		if row.Name == "sqlite_master" {
			t.Error("sqlite_master should not appear in buildMasterRows")
		}
	}
}

// --- encodeMasterRow / decodeMasterRow ---

func TestEncodeMasterRow(t *testing.T) {
	t.Parallel()
	row := MasterRow{
		Type:     "table",
		Name:     "users",
		TblName:  "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users(id INTEGER)",
	}

	encoded := encodeMasterRow(row)
	if len(encoded) == 0 {
		t.Error("encodeMasterRow returned empty payload")
	}

	decoded, err := decodeMasterRow(encoded)
	if err != nil {
		t.Fatalf("decodeMasterRow() error = %v", err)
	}

	if decoded.Type != row.Type {
		t.Errorf("decoded.Type = %q, want %q", decoded.Type, row.Type)
	}
	if decoded.Name != row.Name {
		t.Errorf("decoded.Name = %q, want %q", decoded.Name, row.Name)
	}
	if decoded.TblName != row.TblName {
		t.Errorf("decoded.TblName = %q, want %q", decoded.TblName, row.TblName)
	}
	if decoded.SQL != row.SQL {
		t.Errorf("decoded.SQL = %q, want %q", decoded.SQL, row.SQL)
	}
}

// --- RenameTable ---

func TestRenameTable_Success(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["old"] = &Table{Name: "old", Columns: []*Column{{Name: "id", Type: "INTEGER"}}}
	s.Indexes["idx"] = &Index{Name: "idx", Table: "old", Columns: []string{"id"}}

	if err := s.RenameTable("old", "new"); err != nil {
		t.Fatalf("RenameTable() error = %v", err)
	}

	if _, ok := s.Tables["new"]; !ok {
		t.Error("new table not found after rename")
	}
	if _, ok := s.Tables["old"]; ok {
		t.Error("old table still exists after rename")
	}
	if s.Indexes["idx"].Table != "new" {
		t.Errorf("index table ref = %q, want 'new'", s.Indexes["idx"].Table)
	}
}

func TestRenameTable_NotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	if err := s.RenameTable("ghost", "new"); err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestRenameTable_Conflict(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["a"] = &Table{Name: "a"}
	s.Tables["b"] = &Table{Name: "b"}
	if err := s.RenameTable("a", "b"); err == nil {
		t.Error("expected error when target name conflicts")
	}
}

// --- DropIndex ---

func TestDropIndex_Success(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["idx"] = &Index{Name: "idx"}

	if err := s.DropIndex("idx"); err != nil {
		t.Fatalf("DropIndex() error = %v", err)
	}
	if _, ok := s.Indexes["idx"]; ok {
		t.Error("index still exists after drop")
	}
}

func TestDropIndex_NotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	if err := s.DropIndex("ghost"); err == nil {
		t.Error("expected error for non-existent index")
	}
}

func TestDropIndex_CaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Indexes["MyIndex"] = &Index{Name: "MyIndex"}

	if err := s.DropIndex("myindex"); err != nil {
		t.Fatalf("DropIndex() case-insensitive error = %v", err)
	}
}

// --- DropTable removes associated indexes ---

func TestDropTable_RemovesIndexes(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Tables["t"] = &Table{Name: "t", Columns: []*Column{{Name: "id", Type: "INTEGER"}}}
	s.Indexes["idx_t"] = &Index{Name: "idx_t", Table: "t"}
	s.Indexes["idx_other"] = &Index{Name: "idx_other", Table: "other"}

	if err := s.DropTable("t"); err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}

	if _, ok := s.Indexes["idx_t"]; ok {
		t.Error("index for dropped table should be removed")
	}
	if _, ok := s.Indexes["idx_other"]; !ok {
		t.Error("index for other table should remain")
	}
}

func TestDropTable_NotFound(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	if err := s.DropTable("ghost"); err == nil {
		t.Error("expected error for non-existent table")
	}
}

// --- parseIndexSQL with partial index (WHERE clause) ---

func TestParseIndexSQL_PartialIndex(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	row := MasterRow{
		Name:     "idx_active",
		TblName:  "users",
		RootPage: 3,
		SQL:      "CREATE INDEX idx_active ON users(id) WHERE active = 1",
	}

	idx, err := s.parseIndexSQL(row)
	if err != nil {
		t.Fatalf("parseIndexSQL() error = %v", err)
	}

	if !idx.Partial {
		t.Error("expected Partial = true for index with WHERE clause")
	}
	if idx.Where == "" {
		t.Error("expected non-empty Where clause")
	}
}

// --- parseViewSQL with multiple statements (should error) ---

func TestParseViewSQL_WrongType(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	row := MasterRow{
		Name: "v",
		SQL:  "CREATE TABLE v(id INTEGER)",
	}

	_, err := s.parseViewSQL(row)
	if err == nil {
		t.Error("expected error when SQL is not a CREATE VIEW statement")
	}
}
