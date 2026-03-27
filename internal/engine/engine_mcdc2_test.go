// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"io"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestMCDC2_OpenWithOptionsReadOnly exercises the readOnly=true branch in
// OpenWithOptions (MC/DC: readOnly=false covered elsewhere, readOnly=true here).
func TestMCDC2_OpenWithOptionsReadOnly(t *testing.T) {
	// MC/DC conditions for OpenWithOptions:
	//   C1: readOnly flag true → pager opened read-only
	//   C2: pg.PageCount() > 1 → schema loaded (false here for new file)
	// C1: readOnly=false → writable mode
	t.Run("writable", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := OpenWithOptions(tmpDir+"/test.db", false)
		if err != nil {
			t.Fatalf("OpenWithOptions(readOnly=false): %v", err)
		}
		if db.IsReadOnly() {
			t.Error("IsReadOnly() should be false for writable mode")
		}
		db.Close()
	})

	// C2: readOnly=true → read-only mode (file must exist first)
	t.Run("read-only", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := tmpDir + "/test.db"
		// Create the file first by opening writable.
		wdb, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Open (create): %v", err)
		}
		wdb.Close()
		// Now open read-only.
		db, err := OpenWithOptions(dbPath, true)
		if err != nil {
			t.Fatalf("OpenWithOptions(readOnly=true): %v", err)
		}
		if !db.IsReadOnly() {
			t.Error("IsReadOnly() should be true for read-only mode")
		}
		db.Close()
	})
}

// TestMCDC2_CloseWithRollbackError covers the Close path that rolls back an
// active transaction (MC/DC: inTransaction=true branch in Close).
func TestMCDC2_CloseWithRollbackError(t *testing.T) {
	// MC/DC for Close:
	//   C1: e.inTransaction == true  → execute rollback path
	//   C1: e.inTransaction == false → skip rollback path (covered by other tests)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Force a transaction so Close must roll it back.
	_, err = db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Close should succeed despite active transaction (rolls back internally).
	if err := db.Close(); err != nil {
		t.Errorf("Close with active transaction: %v", err)
	}
}

// TestMCDC2_ExecuteEmptySQL covers the Execute path that returns an empty
// result when no statements are parsed (MC/DC: len(statements)==0 branch).
func TestMCDC2_ExecuteEmptySQL(t *testing.T) {
	// MC/DC for Execute:
	//   C1: len(statements)==0 → return empty Result (covered here)
	//   C1: len(statements)>0  → normal execution (covered elsewhere)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// An empty string parses to zero statements.
	result, err := db.Execute("")
	if err != nil {
		t.Fatalf("Execute empty string: %v", err)
	}
	if result == nil {
		t.Fatal("Execute empty string returned nil result")
	}
}

// TestMCDC2_CompileInsertMissingTable covers the error path in CompileInsert
// when the table does not exist in the schema.
func TestMCDC2_CompileInsertMissingTable(t *testing.T) {
	// MC/DC for CompileInsert:
	//   C1: table not found → return error (covered here)
	//   C1: table found     → compile succeeds (covered by insert tests)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	c := NewCompiler(db)
	stmt := &parser.InsertStmt{
		Table: "nonexistent_table",
		Values: [][]parser.Expression{
			{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}
	_, err = c.CompileInsert(stmt)
	if err == nil {
		t.Error("CompileInsert with nonexistent table should return error")
	}
}

// TestMCDC2_CompileCreateTableDuplicate covers the error branch in
// CompileCreateTable when the table already exists.
func TestMCDC2_CompileCreateTableDuplicate(t *testing.T) {
	// MC/DC for CompileCreateTable:
	//   C1: schema.CreateTable succeeds → compile proceeds
	//   C2: schema.CreateTable errors   → error returned (covered here)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("CREATE TABLE dup_t (id INTEGER)")
	if err != nil {
		t.Fatalf("first CREATE TABLE: %v", err)
	}

	_, err = db.Execute("CREATE TABLE dup_t (id INTEGER)")
	if err == nil {
		t.Error("second CREATE TABLE should return error for duplicate table")
	}
}

// TestMCDC2_CompileCreateIndexMissingTable covers the error path in
// CompileCreateIndex when the referenced table does not exist.
func TestMCDC2_CompileCreateIndexMissingTable(t *testing.T) {
	// MC/DC for CompileCreateIndex:
	//   C1: schema.CreateIndex errors → error returned (covered here)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("CREATE INDEX idx_x ON no_such_table (col)")
	if err == nil {
		t.Error("CREATE INDEX on missing table should return error")
	}
}

// TestMCDC2_CompileDropTableIfExists covers the IfExists branch in
// CompileDropTable when the table does not exist.
func TestMCDC2_CompileDropTableIfExists(t *testing.T) {
	// MC/DC for CompileDropTable:
	//   C1: table not found AND IfExists=true  → no error (covered here)
	//   C2: table not found AND IfExists=false → error (covered next)
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "DROP IF EXISTS absent table",
			sql:     "DROP TABLE IF EXISTS no_such_table",
			wantErr: false,
		},
		{
			name:    "DROP absent table no IF EXISTS",
			sql:     "DROP TABLE no_such_table_x",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			db, err := Open(tmpDir + "/test.db")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			_, err = db.Execute(tt.sql)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC2_CompileDropIndexIfExists covers the IfExists branch in
// CompileDropIndex when the index does not exist.
func TestMCDC2_CompileDropIndexIfExists(t *testing.T) {
	// MC/DC for CompileDropIndex:
	//   C1: index not found AND IfExists=true  → no error (covered here)
	//   C2: index not found AND IfExists=false → error
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "DROP INDEX IF EXISTS absent",
			sql:     "DROP INDEX IF EXISTS no_such_idx",
			wantErr: false,
		},
		{
			name:    "DROP INDEX absent no IF EXISTS",
			sql:     "DROP INDEX no_such_idx_x",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			db, err := Open(tmpDir + "/test.db")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			_, err = db.Execute(tt.sql)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC2_CommitNoTransactionPath covers the pager.ErrNoTransaction branch
// in Tx.Commit (pager has no write-side transaction when Begin is called but
// no writes have occurred).
func TestMCDC2_CommitNoTransactionPath(t *testing.T) {
	// MC/DC for Tx.Commit:
	//   C1: pager.Commit returns ErrNoTransaction → tolerated, commit succeeds
	//   C1: pager.Commit returns nil              → commit succeeds normally
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// No writes → pager has no active transaction → ErrNoTransaction tolerated.
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit with no prior write: %v", err)
	}
}

// TestMCDC2_RollbackNoTransactionPath covers the pager.ErrNoTransaction path
// in Tx.Rollback (no-write case).
func TestMCDC2_RollbackNoTransactionPath(t *testing.T) {
	// MC/DC for Tx.Rollback:
	//   C1: pager.Rollback returns ErrNoTransaction → tolerated
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback with no prior write: %v", err)
	}
}

// TestMCDC2_GetColumnNamesFromColumns exercises the branch in
// getColumnNamesFromColumns where a column has an alias vs identifier vs neither.
func TestMCDC2_GetColumnNamesFromColumns(t *testing.T) {
	// MC/DC for column-name extraction:
	//   C1: col has Alias   → use alias
	//   C2: col has IdentExpr → use ident name
	//   C3: otherwise       → use generated name
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// MC/DC for resolveNoFromColNames (no-FROM SELECT):
	//   C1: col.Alias != "" → use alias name
	//   C2: col.Alias == "" → use generated "column_N" name
	c := NewCompiler(db)
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			// C1: alias present → use alias
			{Alias: "my_alias", Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
			// C2: no alias → generated name "column_1"
			{Expr: &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}},
		},
	}
	vm, err := c.CompileSelect(stmt)
	if err != nil {
		t.Fatalf("CompileSelect: %v", err)
	}
	if len(vm.ResultCols) != 2 {
		t.Errorf("expected 2 result columns, got %d", len(vm.ResultCols))
	}
	if vm.ResultCols[0] != "my_alias" {
		t.Errorf("col[0]=%q, want %q", vm.ResultCols[0], "my_alias")
	}
	if vm.ResultCols[1] != "column_1" {
		t.Errorf("col[1]=%q, want %q", vm.ResultCols[1], "column_1")
	}
}

// TestMCDC2_QueryRowNoRows covers the io.EOF path in QueryRow.Scan when the
// query returns no rows.
func TestMCDC2_QueryRowNoRows(t *testing.T) {
	// MC/DC for QueryRow.Scan:
	//   C1: rows.Next() returns false AND rows.Err()==nil → io.EOF returned
	//
	// Build QueryRow with an exhausted Rows (done=true) to exercise the io.EOF path.
	emptyRows := &Rows{done: true, currentRow: nil}
	qr := &QueryRow{rows: emptyRows}
	var v int64
	err := qr.Scan(&v)
	if err != io.EOF {
		t.Errorf("QueryRow.Scan with empty rows: got %v, want io.EOF", err)
	}
}

// TestMCDC2_PreparedStmtReuse covers reset-and-reuse of a PreparedStmt
// (MC/DC: ps.closed=false branch in Execute/Query).
func TestMCDC2_PreparedStmtReuse(t *testing.T) {
	// MC/DC for PreparedStmt.Execute:
	//   C1: ps.closed=false → proceed
	//   C1: ps.closed=true  → error (covered by other tests)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE reuse_t (v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	ps, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer ps.Close()

	// Execute twice to exercise the reset path.
	for i := 0; i < 2; i++ {
		result, err := ps.Execute()
		if err != nil {
			t.Errorf("Execute iteration %d: %v", i, err)
		}
		if result == nil {
			t.Errorf("Execute iteration %d returned nil result", i)
		}
	}
}

// TestMCDC2_SchemaTableNotFound covers the getTable error path in
// schema access when the table does not exist (covers schema.GetTable false branch).
func TestMCDC2_SchemaTableNotFound(t *testing.T) {
	// MC/DC for schema lookup:
	//   C1: table found    → proceed (covered elsewhere)
	//   C1: table notfound → error (covered here via execute)
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("INSERT INTO no_such_table VALUES (1)")
	if err == nil {
		t.Error("INSERT into nonexistent table should error")
	}

	_, ok := db.schema.GetTable("no_such_table")
	if ok {
		t.Error("nonexistent table should not be in schema")
	}
}

// TestMCDC2_ScanIntoNullMem covers the nil mem branch in scanInto.
func TestMCDC2_ScanIntoNullMem(t *testing.T) {
	// MC/DC for scanInto:
	//   C1: mem == nil → error returned
	var dest interface{}
	err := scanInto(nil, &dest)
	if err == nil {
		t.Error("scanInto(nil, ...) should return error")
	}
}

// TestMCDC2_ScanIntoInterfaceDest covers the *interface{} destination branch
// in scanInto (fast-path assignment).
func TestMCDC2_ScanIntoInterfaceDest(t *testing.T) {
	// MC/DC for scanInto:
	//   C2: dest is *interface{} → assign via memToInterface
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 42")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var dest interface{}
	if err := rows.Scan(&dest); err != nil {
		t.Errorf("Scan into *interface{}: %v", err)
	}
}

// TestMCDC2_DropTableThenDropIndex covers the compound path: create table,
// create index, drop index, drop table. Exercises the found-and-dropped
// paths in both CompileDropTable and CompileDropIndex.
func TestMCDC2_DropTableThenDropIndex(t *testing.T) {
	// MC/DC for CompileDropTable C1=found, C1=DropBtree succeeds:
	// MC/DC for CompileDropIndex C1=found, C1=DropBtree succeeds:
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE di_t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err = db.Execute("CREATE INDEX di_idx ON di_t (name)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	if _, err = db.Execute("DROP INDEX di_idx"); err != nil {
		t.Errorf("DROP INDEX: %v", err)
	}

	if _, err = db.Execute("DROP TABLE di_t"); err != nil {
		t.Errorf("DROP TABLE: %v", err)
	}

	// Verify table is gone.
	_, ok := db.schema.GetTable("di_t")
	if ok {
		t.Error("table should be absent after DROP TABLE")
	}
}

// TestMCDC2_TxQueryFinished covers the error path in Tx.Query when the
// transaction is already done.
func TestMCDC2_TxQueryFinished(t *testing.T) {
	// MC/DC for Tx.Query:
	//   C1: tx.done=true → error returned
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	tx.Commit()

	_, err = tx.Query("SELECT 1")
	if err == nil {
		t.Error("Query on committed transaction should error")
	}
}

// TestMCDC2_TxExecFinished covers the error path in Tx.Exec when done.
func TestMCDC2_TxExecFinished(t *testing.T) {
	// MC/DC for Tx.Exec:
	//   C1: tx.done=true → error returned
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	tx.Commit()

	_, err = tx.Exec("SELECT 1")
	if err == nil {
		t.Error("Exec on committed transaction should error")
	}
}

// TestMCDC2_PreparedStmtQueryClosed covers the ps.closed=true path in
// PreparedStmt.Query.
func TestMCDC2_PreparedStmtQueryClosed(t *testing.T) {
	// MC/DC for PreparedStmt.Query:
	//   C1: ps.closed=true → error returned
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ps, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	ps.Close()

	_, err = ps.Query()
	if err == nil {
		t.Error("Query on closed PreparedStmt should error")
	}
}

// TestMCDC2_SchemaGetIndexFound ensures CreateIndex and GetIndex are covered.
func TestMCDC2_SchemaGetIndexFound(t *testing.T) {
	// MC/DC for CompileCreateIndex: C1=schema.CreateIndex succeeds
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE idx_t (id INTEGER, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err = db.Execute("CREATE INDEX idx_v ON idx_t (v)"); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	idx, ok := db.schema.GetIndex("idx_v")
	if !ok {
		t.Fatal("index should exist after CREATE INDEX")
	}
	if idx.Name != "idx_v" {
		t.Errorf("index name=%q, want %q", idx.Name, "idx_v")
	}
}

// TestMCDC2_LoadSchemaExistingDB exercises the loadSchema branch where
// PageCount > 1 (existing DB). Since loadSchema is a no-op placeholder,
// we just verify OpenWithOptions doesn't error when the DB already exists.
func TestMCDC2_LoadSchemaExistingDB(t *testing.T) {
	// MC/DC for OpenWithOptions:
	//   C2: pg.PageCount()>1 → loadSchema called
	tmpDir := t.TempDir()

	// Create a DB and write at least one page worth of data.
	db, err := Open(tmpDir + "/persistent.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Execute("CREATE TABLE x (id INTEGER)")
	db.Close()

	// Re-open: if PageCount>1 the loadSchema path is triggered.
	db2, err := OpenWithOptions(tmpDir+"/persistent.db", false)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	db2.Close()
}

// TestMCDC2_ScanBoolDest covers the *bool destination branch in scanTyped.
func TestMCDC2_ScanBoolDest(t *testing.T) {
	// MC/DC for scanTyped switch:
	//   *bool destination → mem.IntValue() != 0
	//
	// Use a table-backed query so the Mem type is MemInt.
	mem := vdbe.NewMem()
	mem.SetInt(1)
	rows := &Rows{currentRow: []*vdbe.Mem{mem}}
	var b bool
	if err := rows.Scan(&b); err != nil {
		t.Errorf("Scan into *bool: %v", err)
	}
	if !b {
		t.Error("*bool should be true for value 1")
	}
}

// TestMCDC2_ScanIntDest covers the *int destination branch in scanTyped.
func TestMCDC2_ScanIntDest(t *testing.T) {
	// MC/DC for scanTyped switch: *int destination
	//
	// Build a Mem with MemInt flag directly to exercise the *int case.
	mem := vdbe.NewMem()
	mem.SetInt(99)
	rows := &Rows{currentRow: []*vdbe.Mem{mem}}
	var v int
	if err := rows.Scan(&v); err != nil {
		t.Errorf("Scan into *int: %v", err)
	}
	if v != 99 {
		t.Errorf("got %d, want 99", v)
	}
}

// TestMCDC2_CompileSelectWithTableScan exercises the table-scan path by
// selecting from a real table (FROM clause present).
func TestMCDC2_CompileSelectWithTableScan(t *testing.T) {
	// MC/DC for CompileSelect:
	//   C1: stmt.From != nil → open cursor and scan
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE scan_t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	result, err := db.Execute("SELECT id, name FROM scan_t")
	if err != nil {
		t.Fatalf("SELECT FROM table: %v", err)
	}
	if result == nil {
		t.Error("SELECT result should not be nil")
	}
}

// TestMCDC2_ExistingSchemaTable validates that schema is accessible after
// engine operations (GetSchema path).
func TestMCDC2_ExistingSchemaTable(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	sch := db.GetSchema()
	if sch == nil {
		t.Fatal("GetSchema returned nil")
	}

	if _, err = db.Execute("CREATE TABLE schema_t (x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, ok := sch.GetTable("schema_t")
	if !ok {
		t.Error("schema_t should be in schema after CREATE TABLE")
	}
}

// TestMCDC2_TxExecuteForwardedToEngine verifies Tx.Execute delegates to
// engine and succeeds when the transaction is not done.
func TestMCDC2_TxExecuteForwardedToEngine(t *testing.T) {
	// MC/DC for Tx.Execute:
	//   C1: tx.done=false → delegate to engine.Execute
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE txfwd_t (v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	result, err := tx.Execute("SELECT 42")
	if err != nil {
		t.Fatalf("Tx.Execute: %v", err)
	}
	if result == nil {
		t.Error("Tx.Execute should return non-nil result")
	}
	tx.Rollback()
}
