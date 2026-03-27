// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// TestMCDC3_GetVarintGeneral_9BytePath exercises the 9-byte varint path
// (i==8 branch in getVarintGeneral).
//
// MC/DC for getVarintGeneral:
//
//	C1: i < 8 → accumulate with 7-bit shift (normal path)
//	C2: i == 8 → accumulate with 8-bit shift, return 9 (covered here)
func TestMCDC3_GetVarintGeneral_9BytePath(t *testing.T) {
	// Build a 9-byte varint: all continuation bits set for first 8 bytes.
	buf := []byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x09}
	val, n := GetVarint(buf, 0)
	if n != 9 {
		t.Errorf("expected 9 bytes read, got %d", n)
	}
	if val == 0 {
		t.Error("varint value should be non-zero for 9-byte encoding")
	}
}

// TestMCDC3_GetVarint_2BytePath exercises the 2-byte fast path.
//
// MC/DC for GetVarint:
//
//	C2: buf[offset]>=0x80 AND buf[offset+1]<0x80 → 2-byte varint
func TestMCDC3_GetVarint_2BytePath(t *testing.T) {
	buf := []byte{0x81, 0x05}
	val, n := GetVarint(buf, 0)
	if n != 2 {
		t.Errorf("expected 2 bytes read, got %d", n)
	}
	// value = (0x01 << 7) | 0x05 = 128+5 = 133
	if val != 133 {
		t.Errorf("got val=%d, want 133", val)
	}
}

// TestMCDC3_ParseRecord_EmptyData covers the empty-data error path.
//
// MC/DC for ParseRecord:
//
//	C1: len(data)==0 → error returned
func TestMCDC3_ParseRecord_EmptyData(t *testing.T) {
	_, err := ParseRecord([]byte{})
	if err == nil {
		t.Error("ParseRecord([]) should return error")
	}
}

// TestMCDC3_CompileInsertWithAutoInc_Branches covers both hasAutoInc branches.
//
// MC/DC for CompileInsertWithAutoInc:
//
//	C1: hasAutoInc=true  → auto-inc block entered (covered here)
//	C2: hasAutoInc=false → block skipped (covered here)
func TestMCDC3_CompileInsertWithAutoInc_Branches(t *testing.T) {
	tests := []struct {
		name       string
		hasAutoInc bool
	}{
		{name: "without auto-inc", hasAutoInc: false},
		{name: "with auto-inc", hasAutoInc: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &InsertStmt{
				Table:   "t",
				Columns: []string{"id"},
				Values: [][]Value{
					{{Type: TypeInteger, Int: 1}},
				},
			}
			prog, err := CompileInsertWithAutoInc(stmt, 2, tt.hasAutoInc)
			if err != nil {
				t.Fatalf("CompileInsertWithAutoInc(hasAutoInc=%v): %v", tt.hasAutoInc, err)
			}
			if prog == nil {
				t.Error("expected non-nil program")
			}
		})
	}
}

// TestMCDC3_CompileInsert_MultipleRows covers the compileInsertRows loop for
// multiple rows.
//
// MC/DC for compileInsertRows:
//
//	C1: single row (loop body once)
//	C2: multiple rows (loop body multiple times) (covered here)
func TestMCDC3_CompileInsert_MultipleRows(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"id", "v"},
		Values: [][]Value{
			{{Type: TypeInteger, Int: 1}, {Type: TypeText, Text: "a"}},
			{{Type: TypeInteger, Int: 2}, {Type: TypeText, Text: "b"}},
			{{Type: TypeNull}, {Type: TypeNull}},
		},
	}
	prog, err := CompileInsert(stmt, 2)
	if err != nil {
		t.Fatalf("CompileInsert: %v", err)
	}
	if len(prog.Instructions) == 0 {
		t.Error("expected instructions in program")
	}
}

// TestMCDC3_AddValueLoad_AllTypes covers every branch in addValueLoad.
//
// MC/DC for addValueLoad:
//
//	C1: TypeNull    → OpNull
//	C2: TypeInteger → OpInteger
//	C3: TypeFloat   → OpReal
//	C4: TypeText    → OpString
//	C5: TypeBlob    → OpBlob
func TestMCDC3_AddValueLoad_AllTypes(t *testing.T) {
	prog := newProgram()

	tests := []struct {
		name string
		val  Value
	}{
		{name: "null", val: Value{Type: TypeNull}},
		{name: "integer", val: Value{Type: TypeInteger, Int: 42}},
		{name: "float", val: Value{Type: TypeFloat, Float: 3.14}},
		{name: "text", val: Value{Type: TypeText, Text: "hello"}},
		{name: "blob", val: Value{Type: TypeBlob, Blob: []byte{0x01, 0x02}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := prog.addValueLoad(tt.val, 0)
			if err != nil {
				t.Errorf("addValueLoad(%v): %v", tt.val.Type, err)
			}
		})
	}
}

// TestMCDC3_ComputeAvgEq_AllBranches covers all branches of computeAvgEq.
//
// MC/DC for computeAvgEq:
//
//	C1: distinctCount>0 AND avgEq>=1 → return avgEq
//	C2: distinctCount>0 AND avgEq<1  → return 1
//	C3: distinctCount==0             → return rowCount
func TestMCDC3_ComputeAvgEq_AllBranches(t *testing.T) {
	tests := []struct {
		name          string
		rowCount      int64
		distinctCount int64
		want          int64
	}{
		{name: "normal", rowCount: 100, distinctCount: 10, want: 10},
		{name: "avgEq<1", rowCount: 1, distinctCount: 10, want: 1},
		{name: "zero distinct", rowCount: 50, distinctCount: 0, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeAvgEq(tt.rowCount, tt.distinctCount)
			if got != tt.want {
				t.Errorf("computeAvgEq(%d,%d)=%d, want %d",
					tt.rowCount, tt.distinctCount, got, tt.want)
			}
		})
	}
}

// TestMCDC3_AnalyzeDatabase_SkipsSystemTables verifies AnalyzeDatabase skips
// sqlite_master and sqlite_stat1 (MC/DC: system table name filter branch).
func TestMCDC3_AnalyzeDatabase_SkipsSystemTables(t *testing.T) {
	s := NewSchema()
	s.Tables["sqlite_master"] = &Table{Name: "sqlite_master", NumColumns: 1}
	s.Tables["sqlite_stat1"] = &Table{Name: "sqlite_stat1", NumColumns: 1}
	s.Tables["user_table"] = &Table{Name: "user_table", NumColumns: 1, Columns: []Column{{Name: "id"}}}

	results, err := AnalyzeDatabase(s)
	if err != nil {
		t.Fatalf("AnalyzeDatabase: %v", err)
	}
	for _, r := range results {
		if r.TableName == "sqlite_master" || r.TableName == "sqlite_stat1" {
			t.Errorf("AnalyzeDatabase should skip system table %q", r.TableName)
		}
	}
}

// TestMCDC3_ExecuteAnalyze_SpecificTable covers the specific-table branch.
//
// MC/DC for ExecuteAnalyze:
//
//	C1: opts.TableName!="" → analyze specific table (covered here)
func TestMCDC3_ExecuteAnalyze_SpecificTable(t *testing.T) {
	s := NewSchema()
	s.Tables["my_table"] = &Table{
		Name:       "my_table",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}

	report, err := ExecuteAnalyze(s, AnalyzeOptions{TableName: "my_table"})
	if err != nil {
		t.Fatalf("ExecuteAnalyze(specific): %v", err)
	}
	if report.TablesAnalyzed != 1 {
		t.Errorf("TablesAnalyzed=%d, want 1", report.TablesAnalyzed)
	}
}

// TestMCDC3_CompileCreateTable_IfNotExists covers the IF NOT EXISTS branch.
//
// MC/DC for checkTableExists:
//
//	C1: table exists AND IfNotExists=true  → return halt VDBE, no error
//	C2: table exists AND IfNotExists=false → return error
func TestMCDC3_CompileCreateTable_IfNotExists(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	s.Tables["existing_t"] = &Table{Name: "existing_t", NumColumns: 1, Columns: []Column{{Name: "id"}}}

	tests := []struct {
		name        string
		ifNotExists bool
		wantErr     bool
	}{
		{name: "IF NOT EXISTS", ifNotExists: true, wantErr: false},
		{name: "no IF NOT EXISTS", ifNotExists: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &parser.CreateTableStmt{
				Name:        "existing_t",
				IfNotExists: tt.ifNotExists,
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
			}
			_, err := CompileCreateTable(stmt, s, bt)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC3_CompileCreateTable_EmptyColumns covers the empty-columns error path.
func TestMCDC3_CompileCreateTable_EmptyColumns(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	stmt := &parser.CreateTableStmt{
		Name:    "no_cols_tbl",
		Columns: []parser.ColumnDef{},
	}
	_, err := CompileCreateTable(stmt, s, bt)
	if err == nil {
		t.Error("CompileCreateTable with no columns should return error")
	}
}

// TestMCDC3_CompileCreateIndex_IfNotExists covers the IF NOT EXISTS path for indexes.
//
// MC/DC for checkIndexExists:
//
//	C1: index exists AND IfNotExists=true  → halt VDBE, no error
//	C2: index exists AND IfNotExists=false → error
func TestMCDC3_CompileCreateIndex_IfNotExists(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	s.Tables["t2"] = &Table{
		Name:       "t2",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
		RootPage:   1,
	}
	s.Indexes["existing_idx2"] = &Index{Name: "existing_idx2", Table: "t2", Columns: []string{"id"}}

	tests := []struct {
		name        string
		ifNotExists bool
		wantErr     bool
	}{
		{name: "IF NOT EXISTS", ifNotExists: true, wantErr: false},
		{name: "no IF NOT EXISTS", ifNotExists: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &parser.CreateIndexStmt{
				Name:        "existing_idx2",
				Table:       "t2",
				IfNotExists: tt.ifNotExists,
				Columns:     []parser.IndexedColumn{{Column: "id"}},
			}
			_, err := CompileCreateIndex(stmt, s, bt)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC3_CompileCreateIndex_MissingTable covers the missing-table error path.
func TestMCDC3_CompileCreateIndex_MissingTable(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	stmt := &parser.CreateIndexStmt{
		Name:    "idx_missing_t",
		Table:   "no_such_table",
		Columns: []parser.IndexedColumn{{Column: "col"}},
	}
	_, err := CompileCreateIndex(stmt, s, bt)
	if err == nil {
		t.Error("CompileCreateIndex on missing table should return error")
	}
}

// TestMCDC3_CompileDropTable_AllBranches covers DropTable error branches.
//
// MC/DC for CompileDropTable:
//
//	C1: table nil AND IfExists=true  → no error
//	C2: table nil AND IfExists=false → error
//	C3: table is system table        → error
//	C4: table found, normal          → success
func TestMCDC3_CompileDropTable_AllBranches(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	s.Tables["real_t2"] = &Table{Name: "real_t2", NumColumns: 1, RootPage: 2}

	tests := []struct {
		name     string
		tblName  string
		ifExists bool
		wantErr  bool
	}{
		{name: "missing IF EXISTS", tblName: "gone_tbl", ifExists: true, wantErr: false},
		{name: "missing no IF EXISTS", tblName: "gone_tbl", ifExists: false, wantErr: true},
		{name: "system table", tblName: "sqlite_master", ifExists: false, wantErr: true},
		{name: "existing table", tblName: "real_t2", ifExists: false, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &parser.DropTableStmt{
				Name:     tt.tblName,
				IfExists: tt.ifExists,
			}
			_, err := CompileDropTable(stmt, s, bt)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for %q", tt.name)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tt.name, err)
			}
		})
	}
}

// TestMCDC3_ConvertExpr_AllLiteralTypes covers all literal type branches in convertExpr.
//
// MC/DC for convertExpr:
//
//	C1: expr==nil          → return nil
//	C2: LiteralInteger     → TK_INTEGER
//	C3: LiteralFloat       → TK_FLOAT
//	C4: LiteralString      → TK_STRING
//	C5: LiteralNull        → TK_NULL
func TestMCDC3_ConvertExpr_AllLiteralTypes(t *testing.T) {
	tests := []struct {
		name    string
		expr    parser.Expression
		wantNil bool
		wantOp  int
	}{
		{name: "nil", expr: nil, wantNil: true},
		{name: "integer", expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}, wantOp: TK_INTEGER},
		{name: "float", expr: &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"}, wantOp: TK_FLOAT},
		{name: "string", expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}, wantOp: TK_STRING},
		{name: "null", expr: &parser.LiteralExpr{Type: parser.LiteralNull}, wantOp: TK_NULL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertExpr(tt.expr)
			if tt.wantNil {
				if result != nil {
					t.Error("expected nil result")
				}
				return
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.Op != tt.wantOp {
				t.Errorf("Op=%d, want %d", result.Op, tt.wantOp)
			}
		})
	}
}

// TestMCDC3_CompileInsertRows_MismatchedValues covers the row-length mismatch error.
//
// MC/DC for compileInsertRows:
//
//	C1: len(row) != numCols → error returned (covered here)
func TestMCDC3_CompileInsertRows_MismatchedValues(t *testing.T) {
	stmt := &InsertStmt{
		Table:   "t",
		Columns: []string{"a", "b"},
		Values: [][]Value{
			{{Type: TypeInteger, Int: 1}}, // only 1 value for 2 columns
		},
	}
	_, err := CompileInsert(stmt, 2)
	if err == nil {
		t.Error("CompileInsert with mismatched column count should error")
	}
}

// TestMCDC3_Schema_AddRemoveIndex covers Add/Remove index error paths.
//
// MC/DC for AddIndex/RemoveIndex:
//
//	C1: AddIndex with duplicate  → error
//	C2: RemoveIndex non-existent → error
func TestMCDC3_Schema_AddRemoveIndex(t *testing.T) {
	s := NewSchema()
	idx := &Index{Name: "mcdc3_idx", Table: "t", Columns: []string{"id"}}

	if err := s.AddIndex(idx); err != nil {
		t.Fatalf("first AddIndex: %v", err)
	}
	if err := s.AddIndex(idx); err == nil {
		t.Error("duplicate AddIndex should error")
	}
	if err := s.RemoveIndex("no_such_mcdc3"); err == nil {
		t.Error("RemoveIndex of non-existent should error")
	}
	if err := s.RemoveIndex("mcdc3_idx"); err != nil {
		t.Errorf("RemoveIndex: %v", err)
	}
}

// TestMCDC3_Schema_AddRemoveTable covers Add/Remove table error paths.
func TestMCDC3_Schema_AddRemoveTable(t *testing.T) {
	s := NewSchema()
	tbl := &Table{Name: "mcdc3_tbl", NumColumns: 1}

	if err := s.AddTable(tbl); err != nil {
		t.Fatalf("first AddTable: %v", err)
	}
	if err := s.AddTable(tbl); err == nil {
		t.Error("duplicate AddTable should error")
	}
	if err := s.RemoveTable("no_such_mcdc3"); err == nil {
		t.Error("RemoveTable of non-existent should error")
	}
}

// TestMCDC3_TypeNameToAffinity_AllBranches covers all affinity determination paths.
//
// MC/DC for typeNameToAffinity:
//
//	C1: empty name           → SQLITE_AFF_BLOB
//	C2: contains "INT"       → SQLITE_AFF_INTEGER
//	C3: contains CHAR/CLOB/TEXT → SQLITE_AFF_TEXT
//	C4: contains "BLOB"      → SQLITE_AFF_BLOB
//	C5: contains REAL/FLOA/DOUB → SQLITE_AFF_REAL
//	C6: otherwise            → SQLITE_AFF_NUMERIC
func TestMCDC3_TypeNameToAffinity_AllBranches(t *testing.T) {
	tests := []struct {
		typeName string
		want     Affinity
	}{
		{"", SQLITE_AFF_BLOB},
		{"INTEGER", SQLITE_AFF_INTEGER},
		{"INT", SQLITE_AFF_INTEGER},
		{"VARCHAR(255)", SQLITE_AFF_TEXT},
		{"TEXT", SQLITE_AFF_TEXT},
		{"CLOB", SQLITE_AFF_TEXT},
		{"BLOB", SQLITE_AFF_BLOB},
		{"REAL", SQLITE_AFF_REAL},
		{"FLOAT", SQLITE_AFF_REAL},
		{"DOUBLE", SQLITE_AFF_REAL},
		{"NUMERIC", SQLITE_AFF_NUMERIC},
		{"DECIMAL(10,2)", SQLITE_AFF_NUMERIC},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := typeNameToAffinity(tt.typeName)
			if got != tt.want {
				t.Errorf("typeNameToAffinity(%q)=%v, want %v", tt.typeName, got, tt.want)
			}
		})
	}
}

// TestMCDC3_CompileInsert_ValidateErrors covers nil and empty-values paths.
//
// MC/DC for validateInsertStmt:
//
//	C1: stmt==nil      → error
//	C2: len(Values)==0 → error
func TestMCDC3_CompileInsert_ValidateErrors(t *testing.T) {
	if _, err := CompileInsert(nil, 1); err == nil {
		t.Error("CompileInsert(nil) should error")
	}
	if _, err := CompileInsert(&InsertStmt{Table: "t"}, 1); err == nil {
		t.Error("CompileInsert with empty Values should error")
	}
}

// TestMCDC3_Program_Disassemble_P4Types covers all P4 type branches in Disassemble.
//
// MC/DC for Disassemble P4 formatting:
//
//	C1: P4 is string > 20 chars → truncated
//	C2: P4 is string <= 20 chars → quoted as-is
//	C3: P4 is float64            → formatted with %.6f
//	C4: P4 is []byte             → formatted as blob
//	C5: P4 is other type         → %v
func TestMCDC3_Program_Disassemble_P4Types(t *testing.T) {
	prog := newProgram()
	prog.add(OpString, 0, 1, 0, "short", 0, "short string P4")
	prog.add(OpString, 0, 2, 0, "this string is definitely longer than twenty chars!", 0, "long string P4")
	prog.add(OpReal, 0, 3, 0, float64(3.14), 0, "float P4")
	prog.add(OpBlob, 2, 4, 0, []byte{0xDE, 0xAD}, 0, "blob P4")
	prog.add(OpInteger, 42, 5, 0, int(42), 0, "other type P4")
	prog.add(OpNull, 0, 6, 0, nil, 0, "nil P4")

	out := prog.Disassemble()
	if len(out) == 0 {
		t.Error("Disassemble returned empty string")
	}
}

// TestMCDC3_AnalyzeDatabase_WithIndex covers the analyzeTableIndexes path when
// an index exists on the table.
//
// MC/DC for analyzeTableIndexes:
//
//	C1: index.Table == table.Name → include in analysis (covered here)
func TestMCDC3_AnalyzeDatabase_WithIndex(t *testing.T) {
	s := NewSchema()
	s.Tables["indexed_t"] = &Table{
		Name:       "indexed_t",
		NumColumns: 1,
		Columns:    []Column{{Name: "id"}},
	}
	s.Indexes["idx_on_t"] = &Index{
		Name:    "idx_on_t",
		Table:   "indexed_t",
		Columns: []string{"id"},
		Unique:  true,
	}
	s.Indexes["idx_other"] = &Index{
		Name:    "idx_other",
		Table:   "other_table",
		Columns: []string{"col"},
	}

	result, err := AnalyzeTable("indexed_t", s)
	if err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	if result.IndexesSeen != 1 {
		t.Errorf("IndexesSeen=%d, want 1", result.IndexesSeen)
	}
}

// TestMCDC3_CompileCreateTable_WithConstraints covers the PRIMARY KEY autoincrement path.
//
// MC/DC for applyPrimaryKey:
//
//	C1: constraint.PrimaryKey != nil AND Autoincrement → RowidColumn set
func TestMCDC3_CompileCreateTable_WithConstraints(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()
	stmt := &parser.CreateTableStmt{
		Name: "constrained_t",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type: parser.ConstraintPrimaryKey,
						PrimaryKey: &parser.PrimaryKeyConstraint{
							Autoincrement: true,
						},
					},
				},
			},
			{Name: "v", Type: "TEXT"},
		},
	}
	vm, err := CompileCreateTable(stmt, s, bt)
	if err != nil {
		t.Fatalf("CompileCreateTable: %v", err)
	}
	if vm == nil {
		t.Error("expected non-nil VDBE")
	}

	tbl := s.GetTable("constrained_t")
	if tbl == nil {
		t.Fatal("table should be in schema")
	}
	if tbl.RowidColumn != 0 {
		t.Errorf("RowidColumn=%d, want 0 for AUTOINCREMENT pk", tbl.RowidColumn)
	}
}

// TestMCDC3_CompileCreateTable_ReservedName covers the reserved-name error path.
//
// MC/DC for validateTableName:
//
//	C1: name == "sqlite_master" → error
//	C2: name == "sqlite_schema" → error
func TestMCDC3_CompileCreateTable_ReservedName(t *testing.T) {
	bt := btree.NewBtree(4096)
	s := NewSchema()

	for _, name := range []string{"sqlite_master", "sqlite_schema"} {
		stmt := &parser.CreateTableStmt{
			Name:    name,
			Columns: []parser.ColumnDef{{Name: "id", Type: "INTEGER"}},
		}
		_, err := CompileCreateTable(stmt, s, bt)
		if err == nil {
			t.Errorf("CompileCreateTable(%q) should return error for reserved name", name)
		}
	}
}
