// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// newTestDB opens a fresh database in t.TempDir() and registers a cleanup close.
func newTestDB(t *testing.T) *Engine {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "crc_test.db"))
	if err != nil {
		t.Fatalf("newTestDB: Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// setupSingleRowTable creates a one-row table. The engine supports one insert
// per table reliably; subsequent inserts fail with a duplicate-rowid error.
func setupSingleRowTable(t *testing.T, db *Engine) {
	t.Helper()
	mustExec(t, db, `CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)`)
	mustExec(t, db, `INSERT INTO users (name, age) VALUES ('Alice', 30)`)
}

// ═══════════════════════════════════════════════════════
// compileUpdateWhere
// ═══════════════════════════════════════════════════════

// TestCompilerResultUpdateWhereNoMatch exercises UPDATE WHERE with no matching rows.
// The WHERE clause compilation path is exercised even when no row matches.
func TestCompilerResultUpdateWhereNoMatch(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`UPDATE users SET age = 0 WHERE name = 'NoSuchPerson'`)
	if err != nil {
		t.Fatalf("UPDATE WHERE no match: %v", err)
	}

	result, err := db.Execute(`SELECT name FROM users`)
	if err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row unchanged, got %d", len(result.Rows))
	}
}

// TestCompilerResultUpdateWhereAnd exercises UPDATE with a compound AND condition.
func TestCompilerResultUpdateWhereAnd(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`UPDATE users SET age = 99 WHERE name = 'Alice' AND age = 30`)
	if err != nil {
		t.Fatalf("UPDATE WHERE AND: %v", err)
	}
}

// TestCompilerResultUpdateWhereOr exercises UPDATE with an OR condition.
func TestCompilerResultUpdateWhereOr(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`UPDATE users SET age = 0 WHERE name = 'Alice' OR name = 'Bob'`)
	if err != nil {
		t.Fatalf("UPDATE WHERE OR: %v", err)
	}
}

// TestCompilerResultUpdateWhereDirect builds the AST manually so CompileUpdate
// definitely enters compileUpdateWhere (non-nil Where branch).
func TestCompilerResultUpdateWhereDirect(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE items (id INTEGER, val TEXT)`)

	c := NewCompiler(db)
	stmt := &parser.UpdateStmt{
		Table: "items",
		Sets: []parser.Assignment{
			{Column: "val", Value: &parser.LiteralExpr{Type: parser.LiteralString, Value: "new"}},
		},
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	vm, err := c.CompileUpdate(stmt)
	if err != nil {
		t.Fatalf("CompileUpdate with WHERE: %v", err)
	}
	if vm == nil {
		t.Fatal("returned VDBE is nil")
	}
}

// ═══════════════════════════════════════════════════════
// applySetAssignments
// ═══════════════════════════════════════════════════════

// TestCompilerResultApplySetMultiple exercises the SET assignment loop with two
// columns being updated in a single UPDATE statement.
func TestCompilerResultApplySetMultiple(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`UPDATE users SET name = 'Updated', age = 55 WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("UPDATE multiple SET: %v", err)
	}
}

// TestCompilerResultApplySetBadColumn verifies that referencing an unknown column
// in a SET clause returns a compile error.
func TestCompilerResultApplySetBadColumn(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE t (id INTEGER)`)

	c := NewCompiler(db)
	stmt := &parser.UpdateStmt{
		Table: "t",
		Sets: []parser.Assignment{
			{Column: "nonexistent", Value: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}

	_, err := c.CompileUpdate(stmt)
	if err == nil {
		t.Fatal("expected error for unknown column in SET")
	}
}

// TestCompilerResultApplySetExpression exercises the expression code-gen path
// inside applySetAssignments when the SET value is a column reference.
func TestCompilerResultApplySetExpression(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`UPDATE users SET age = age WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("UPDATE SET column expression: %v", err)
	}
}

// ═══════════════════════════════════════════════════════
// patchWhereSkipLabel
// ═══════════════════════════════════════════════════════

// TestCompilerResultPatchWhereSkipLabelUpdate verifies that the WHERE skip
// label emitted in compileUpdateWhere is correctly patched by closeUpdateLoop.
// A non-matching WHERE causes the OpIfNot to jump to the patched label.
func TestCompilerResultPatchWhereSkipLabelUpdate(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE patch_update (v INTEGER)`)
	mustExec(t, db, `INSERT INTO patch_update (v) VALUES (10)`)

	// WHERE clause that matches nothing — OpIfNot skip path exercises the
	// label-patching logic without triggering the row-update path.
	_, err := db.Execute(`UPDATE patch_update SET v = 99 WHERE v = 999`)
	if err != nil {
		t.Fatalf("patchWhereSkipLabel via UPDATE (no match): %v", err)
	}
}

// TestCompilerResultPatchWhereSkipLabelDelete verifies the same label-patching
// mechanism via the DELETE compiler path.
func TestCompilerResultPatchWhereSkipLabelDelete(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE patch_delete (v INTEGER)`)
	mustExec(t, db, `INSERT INTO patch_delete (v) VALUES (10)`)

	// Non-matching WHERE — skip label is taken.
	_, err := db.Execute(`DELETE FROM patch_delete WHERE v = 999`)
	if err != nil {
		t.Fatalf("patchWhereSkipLabel via DELETE (no match): %v", err)
	}
}

// ═══════════════════════════════════════════════════════
// compileDeleteWhere
// ═══════════════════════════════════════════════════════

// TestCompilerResultDeleteWhereNoMatch exercises DELETE WHERE with a condition
// that matches no rows (the nil-WHERE branch is the else; this covers the
// non-nil branch that emits OpIfNot).
func TestCompilerResultDeleteWhereNoMatch(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`DELETE FROM users WHERE name = 'NoSuchPerson'`)
	if err != nil {
		t.Fatalf("DELETE WHERE no match: %v", err)
	}

	result, err := db.Execute(`SELECT name FROM users`)
	if err != nil {
		t.Fatalf("SELECT after DELETE: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row unchanged, got %d", len(result.Rows))
	}
}

// TestCompilerResultDeleteWhereMatch exercises DELETE WHERE that matches a row.
func TestCompilerResultDeleteWhereMatch(t *testing.T) {
	db := newTestDB(t)
	setupSingleRowTable(t, db)

	_, err := db.Execute(`DELETE FROM users WHERE age = 30`)
	if err != nil {
		t.Fatalf("DELETE WHERE match: %v", err)
	}
}

// TestCompilerResultDeleteWhereDirect builds the AST manually to directly
// exercise compileDeleteWhere via CompileDelete.
func TestCompilerResultDeleteWhereDirect(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE del_direct (id INTEGER, val TEXT)`)

	c := NewCompiler(db)
	stmt := &parser.DeleteStmt{
		Table: "del_direct",
		Where: &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		},
	}

	vm, err := c.CompileDelete(stmt)
	if err != nil {
		t.Fatalf("CompileDelete with WHERE: %v", err)
	}
	if vm == nil {
		t.Fatal("returned VDBE is nil")
	}
}

// ═══════════════════════════════════════════════════════
// OpenWithOptions
// ═══════════════════════════════════════════════════════

// TestEngineOpenWithOptionsReadWrite exercises OpenWithOptions(path, false).
func TestEngineOpenWithOptionsReadWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rw.db")

	db, err := OpenWithOptions(dbPath, false)
	if err != nil {
		t.Fatalf("OpenWithOptions rw: %v", err)
	}
	defer db.Close()

	if db.IsReadOnly() {
		t.Error("expected read-write database")
	}

	_, err = db.Execute(`CREATE TABLE rw (id INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE after OpenWithOptions rw: %v", err)
	}
}

// TestEngineOpenWithOptionsReadOnly exercises OpenWithOptions(path, true) on a
// pre-existing database file.
func TestEngineOpenWithOptionsReadOnly(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ro.db")

	// Seed the database file.
	seed, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open for ro setup: %v", err)
	}
	mustExec(t, seed, `CREATE TABLE ro_tbl (id INTEGER)`)
	seed.Close()

	// Re-open as read-only.
	rodb, err := OpenWithOptions(dbPath, true)
	if err != nil {
		t.Fatalf("OpenWithOptions ro: %v", err)
	}
	defer rodb.Close()

	if !rodb.IsReadOnly() {
		t.Error("expected read-only database")
	}
}

// ═══════════════════════════════════════════════════════
// Close
// ═══════════════════════════════════════════════════════

// TestEngineCloseAfterDML exercises Close after running DML statements.
func TestEngineCloseAfterDML(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "dml.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	mustExec(t, db, `CREATE TABLE dml_tbl (id INTEGER, val TEXT)`)
	mustExec(t, db, `INSERT INTO dml_tbl (val) VALUES ('hello')`)
	mustExec(t, db, `UPDATE dml_tbl SET val = 'world' WHERE val = 'hello'`)

	if err := db.Close(); err != nil {
		t.Fatalf("Close after DML: %v", err)
	}
}

// TestEngineCloseWithOpenTransaction exercises the rollback path inside Close
// when a transaction was started but never committed or rolled back.
func TestEngineCloseWithOpenTransaction(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "txclose.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_, err = db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Close should implicitly roll back and return no error.
	if err := db.Close(); err != nil {
		t.Fatalf("Close with open transaction: %v", err)
	}
}

// ═══════════════════════════════════════════════════════
// PreparedStmt.Execute / PreparedStmt.Query
// ═══════════════════════════════════════════════════════

// TestPreparedStmtExecuteClosed verifies Execute on a closed statement errors.
func TestPreparedStmtExecuteClosed(t *testing.T) {
	db := newTestDB(t)

	ps, err := db.Prepare(`SELECT 1`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if err := ps.Close(); err != nil {
		t.Fatalf("Close prepared: %v", err)
	}

	_, err = ps.Execute()
	if err == nil {
		t.Fatal("expected error executing closed statement")
	}
}

// TestPreparedStmtQueryClosed verifies Query on a closed statement errors.
func TestPreparedStmtQueryClosed(t *testing.T) {
	db := newTestDB(t)

	ps, err := db.Prepare(`SELECT 1`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if err := ps.Close(); err != nil {
		t.Fatalf("Close prepared: %v", err)
	}

	_, err = ps.Query()
	if err == nil {
		t.Fatal("expected error querying closed statement")
	}
}

// TestPreparedStmtDoubleClose verifies that closing twice does not error.
func TestPreparedStmtDoubleClose(t *testing.T) {
	db := newTestDB(t)

	ps, err := db.Prepare(`SELECT 1`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if err := ps.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("second Close (should be no-op): %v", err)
	}
}

// ═══════════════════════════════════════════════════════
// Engine.Execute / Engine.Query — error paths
// ═══════════════════════════════════════════════════════

// TestEngineExecuteInvalidSQL verifies that unparseable SQL returns an error.
func TestEngineExecuteInvalidSQL(t *testing.T) {
	db := newTestDB(t)

	_, err := db.Execute(`THIS IS NOT SQL !!!`)
	if err == nil {
		t.Fatal("expected parse error for invalid SQL")
	}
}

// TestEngineQueryNonSelect verifies Query returns an error for non-SELECT SQL.
func TestEngineQueryNonSelect(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE q (id INTEGER)`)

	_, err := db.Query(`INSERT INTO q VALUES (1)`)
	if err == nil {
		t.Fatal("expected error when Query receives INSERT")
	}
}

// TestEngineQueryInvalidSQL verifies Query returns a parse error for bad SQL.
func TestEngineQueryInvalidSQL(t *testing.T) {
	db := newTestDB(t)

	_, err := db.Query(`NOT VALID SQL`)
	if err == nil {
		t.Fatal("expected parse error from Query")
	}
}

// TestEngineQueryScanNullColumn verifies that Scan handles a NULL column value.
func TestEngineQueryScanNullColumn(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE nulltbl (id INTEGER, val TEXT)`)
	mustExec(t, db, `INSERT INTO nulltbl VALUES (1, NULL)`)

	rows, err := db.Query(`SELECT id, val FROM nulltbl`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row")
	}

	var id int
	var val interface{}
	if err := rows.Scan(&id, &val); err != nil {
		t.Fatalf("Scan with NULL column: %v", err)
	}
}
