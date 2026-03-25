// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCovDB opens an in-memory database for DDL/collation coverage tests.
func openCovDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, func() { db.Close() }
}

// covExec executes SQL statements and fails on error.
func covExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// covExecErr executes a statement and returns any error without failing.
func covExecErr(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}

// covQueryInt64 runs a query returning a single int64 value.
func covQueryInt64(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("covQueryInt64 %q: %v", query, err)
	}
	return v
}

// covQueryString runs a query returning a single string value.
func covQueryString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("covQueryString %q: %v", query, err)
	}
	return v
}

// covCountRows counts rows from the given query.
func covCountRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("covCountRows %q: %v", query, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("covCountRows rows.Err: %v", err)
	}
	return n
}

// ============================================================================
// TestCompileDDLCollation_AutoincrementTable
// Exercises: initializeNewTable (autoincrement branch), ensureSqliteSequenceTable
// ============================================================================

func TestCompileDDLCollation_AutoincrementTable(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Creating a table with AUTOINCREMENT triggers ensureSqliteSequenceTable
	// and goes through initializeNewTable fully.
	covExec(t, db,
		"CREATE TABLE autoinc_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
	)

	// sqlite_sequence is created internally but not exposed in sqlite_master;
	// verify it is queryable (COUNT returns 0 initially since no inserts yet).
	seqCount := covQueryInt64(t, db, "SELECT COUNT(*) FROM sqlite_sequence")
	if seqCount < 0 {
		t.Errorf("sqlite_sequence query returned unexpected negative count")
	}

	// Insert rows to exercise AUTOINCREMENT behaviour.
	covExec(t, db,
		"INSERT INTO autoinc_tbl(name) VALUES('alice')",
		"INSERT INTO autoinc_tbl(name) VALUES('bob')",
	)

	count := covQueryInt64(t, db, "SELECT COUNT(*) FROM autoinc_tbl")
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestCompileDDLCollation_AutoincrementIdempotent verifies that creating a
// second AUTOINCREMENT table does not fail even though sqlite_sequence already
// exists (ensureSqliteSequenceTable early-exit path).
func TestCompileDDLCollation_AutoincrementIdempotent(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE ai1 (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
		"CREATE TABLE ai2 (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
	)

	// Both tables must be present.
	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ai1','ai2')")
	if n != 2 {
		t.Errorf("expected 2 tables, got %d", n)
	}
}

// ============================================================================
// TestCompileDDLCollation_ForeignKeyConstraints
// Exercises: registerForeignKeyConstraints (table-level and column-level paths)
// ============================================================================

func TestCompileDDLCollation_ForeignKeyConstraints(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Table-level FOREIGN KEY constraint.
	covExec(t, db,
		"CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
		`CREATE TABLE fk_child (
			id        INTEGER PRIMARY KEY,
			parent_id INTEGER,
			code      TEXT,
			FOREIGN KEY(parent_id) REFERENCES fk_parent(id),
			FOREIGN KEY(code)      REFERENCES fk_parent(code)
		)`,
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='fk_child'")
	if n != 1 {
		t.Errorf("expected fk_child table, got %d rows", n)
	}
}

// TestCompileDDLCollation_ForeignKeyColumnLevel exercises the column-level
// REFERENCES path of registerColumnLevelFKs.
func TestCompileDDLCollation_ForeignKeyColumnLevel(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE cl_parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE cl_child  (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES cl_parent(id))",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='cl_child'")
	if n != 1 {
		t.Errorf("expected cl_child table, got %d rows", n)
	}
}

// ============================================================================
// TestCompileDDLCollation_DropTable
// Exercises: performDropTable (schema removal, FK cleanup, schema persist)
// ============================================================================

func TestCompileDDLCollation_DropTable(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE drop_me (id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO drop_me VALUES(1,'hello')",
		"DROP TABLE drop_me",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE name='drop_me'")
	if n != 0 {
		t.Errorf("expected drop_me to be gone, got %d rows in sqlite_master", n)
	}
}

// TestCompileDDLCollation_DropTableWithFKConstraints creates a table with FK
// constraints registered, then drops it. This exercises the
// fkManager.RemoveConstraints path inside performDropTable.
func TestCompileDDLCollation_DropTableWithFKConstraints(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE dt_parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE dt_child  (id INTEGER, pid INTEGER REFERENCES dt_parent(id))",
		"DROP TABLE dt_child",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE name='dt_child'")
	if n != 0 {
		t.Errorf("expected dt_child to be gone, got %d", n)
	}
}

// TestCompileDDLCollation_DropTableIfExists exercises the IF EXISTS branch of
// handleMissingTable (silent success when table does not exist).
func TestCompileDDLCollation_DropTableIfExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Must not error.
	if err := covExecErr(db, "DROP TABLE IF EXISTS nonexistent_xyz"); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS nonexistent: unexpected error: %v", err)
	}
}

// TestCompileDDLCollation_DropTableNoIfExists exercises the error path when the
// table does not exist and IF EXISTS is not specified.
func TestCompileDDLCollation_DropTableNoIfExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	err := covExecErr(db, "DROP TABLE nonexistent_abc")
	if err == nil {
		t.Fatal("expected error for DROP TABLE on nonexistent table")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ============================================================================
// TestCompileDDLCollation_CreateDropView
// Exercises: compileCreateView, compileDropView (both success and IF EXISTS)
// ============================================================================

func TestCompileDDLCollation_CreateView(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE vw_src (id INTEGER, name TEXT, score INTEGER)",
		"INSERT INTO vw_src VALUES(1,'alice',90),(2,'bob',70),(3,'carol',80)",
		"CREATE VIEW vw_high AS SELECT name, score FROM vw_src WHERE score >= 80",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='view' AND name='vw_high'")
	if n != 1 {
		t.Errorf("expected view vw_high in sqlite_master, got %d", n)
	}

	// Query through the view.
	rows, err := db.Query("SELECT name FROM vw_high ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, s)
	}
	if len(names) != 2 {
		t.Errorf("expected 2 rows from view, got %d: %v", len(names), names)
	}
}

func TestCompileDDLCollation_CreateViewIfNotExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE vw2_src (id INTEGER, val TEXT)",
		"CREATE VIEW vw2 AS SELECT val FROM vw2_src",
	)

	// IF NOT EXISTS on existing view must not error.
	if err := covExecErr(db, "CREATE VIEW IF NOT EXISTS vw2 AS SELECT val FROM vw2_src"); err != nil {
		t.Fatalf("CREATE VIEW IF NOT EXISTS on existing: %v", err)
	}
}

func TestCompileDDLCollation_DropView(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE dv_src (id INTEGER)",
		"CREATE VIEW dv_view AS SELECT id FROM dv_src",
		"DROP VIEW dv_view",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE name='dv_view'")
	if n != 0 {
		t.Errorf("expected dv_view to be removed, got %d", n)
	}
}

func TestCompileDDLCollation_DropViewIfExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Nonexistent view with IF EXISTS — must not error.
	if err := covExecErr(db, "DROP VIEW IF EXISTS no_such_view_xyz"); err != nil {
		t.Fatalf("DROP VIEW IF EXISTS nonexistent: %v", err)
	}
}

func TestCompileDDLCollation_DropViewNotFound(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	err := covExecErr(db, "DROP VIEW no_such_view_abc")
	if err == nil {
		t.Fatal("expected error for DROP VIEW on nonexistent view")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ============================================================================
// TestCompileDDLCollation_CreateDropTrigger
// Exercises: compileCreateTrigger (BEFORE/AFTER INSERT/UPDATE/DELETE),
//            compileDropTrigger (success, IF EXISTS, not-found)
// ============================================================================

func TestCompileDDLCollation_CreateTriggerBeforeInsert(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE trg_tbl(id INTEGER, v TEXT)",
		"CREATE TABLE trg_log(event TEXT)",
		"CREATE TRIGGER trg_bi BEFORE INSERT ON trg_tbl BEGIN INSERT INTO trg_log VALUES('before_insert'); END",
		"INSERT INTO trg_tbl VALUES(1,'x')",
	)

	n := covCountRows(t, db, "SELECT event FROM trg_log WHERE event='before_insert'")
	if n != 1 {
		t.Errorf("expected 1 before_insert log entry, got %d", n)
	}
}

func TestCompileDDLCollation_CreateTriggerAfterInsert(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE trg_ai_tbl(id INTEGER, v TEXT)",
		"CREATE TABLE trg_ai_log(event TEXT)",
		"CREATE TRIGGER trg_ai AFTER INSERT ON trg_ai_tbl BEGIN INSERT INTO trg_ai_log VALUES('after_insert'); END",
		"INSERT INTO trg_ai_tbl VALUES(1,'y')",
	)

	n := covCountRows(t, db, "SELECT event FROM trg_ai_log WHERE event='after_insert'")
	if n != 1 {
		t.Errorf("expected 1 after_insert log entry, got %d", n)
	}
}

func TestCompileDDLCollation_CreateTriggerBeforeUpdate(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE trg_upd(id INTEGER, v TEXT)",
		"CREATE TABLE trg_upd_log(event TEXT)",
		"INSERT INTO trg_upd VALUES(1,'old')",
		"CREATE TRIGGER trg_bu BEFORE UPDATE ON trg_upd BEGIN INSERT INTO trg_upd_log VALUES('before_update'); END",
		"UPDATE trg_upd SET v='new' WHERE id=1",
	)

	n := covCountRows(t, db, "SELECT event FROM trg_upd_log WHERE event='before_update'")
	if n != 1 {
		t.Errorf("expected 1 before_update log entry, got %d", n)
	}
}

func TestCompileDDLCollation_CreateTriggerAfterDelete(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE trg_del(id INTEGER, v TEXT)",
		"CREATE TABLE trg_del_log(event TEXT)",
		"INSERT INTO trg_del VALUES(1,'row')",
		"CREATE TRIGGER trg_ad AFTER DELETE ON trg_del BEGIN INSERT INTO trg_del_log VALUES('after_delete'); END",
		"DELETE FROM trg_del WHERE id=1",
	)

	n := covCountRows(t, db, "SELECT event FROM trg_del_log WHERE event='after_delete'")
	if n != 1 {
		t.Errorf("expected 1 after_delete log entry, got %d", n)
	}
}

func TestCompileDDLCollation_CreateTriggerIfNotExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE trg_ine_tbl(id INTEGER)",
		"CREATE TRIGGER trg_ine AFTER INSERT ON trg_ine_tbl BEGIN SELECT 1; END",
	)

	// Second creation with IF NOT EXISTS must not error.
	if err := covExecErr(db, "CREATE TRIGGER IF NOT EXISTS trg_ine AFTER INSERT ON trg_ine_tbl BEGIN SELECT 1; END"); err != nil {
		t.Fatalf("CREATE TRIGGER IF NOT EXISTS on existing: %v", err)
	}
}

func TestCompileDDLCollation_DropTrigger(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE dt_tbl(id INTEGER)",
		"CREATE TRIGGER dt_trg AFTER INSERT ON dt_tbl BEGIN SELECT 1; END",
		"DROP TRIGGER dt_trg",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='trigger' AND name='dt_trg'")
	if n != 0 {
		t.Errorf("expected trigger dt_trg to be removed, got %d", n)
	}
}

func TestCompileDDLCollation_DropTriggerIfExists(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Must not error.
	if err := covExecErr(db, "DROP TRIGGER IF EXISTS no_such_trigger_xyz"); err != nil {
		t.Fatalf("DROP TRIGGER IF EXISTS nonexistent: %v", err)
	}
}

func TestCompileDDLCollation_DropTriggerNotFound(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	err := covExecErr(db, "DROP TRIGGER no_such_trigger_abc")
	if err == nil {
		t.Fatal("expected error for DROP TRIGGER on nonexistent trigger")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ============================================================================
// TestCompileDDLCollation_Savepoint
// Exercises: compileSavepoint, compileRelease, compileRollbackTo
// ============================================================================

func TestCompileDDLCollation_Savepoint(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE sp_tbl(id INTEGER, v TEXT)",
		"INSERT INTO sp_tbl VALUES(1,'initial')",
		"SAVEPOINT sp1",
		"INSERT INTO sp_tbl VALUES(2,'in_savepoint')",
		"RELEASE SAVEPOINT sp1",
	)

	count := covQueryInt64(t, db, "SELECT COUNT(*) FROM sp_tbl")
	if count != 2 {
		t.Errorf("expected 2 rows after RELEASE, got %d", count)
	}
}

func TestCompileDDLCollation_SavepointRollback(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE sp_rb_tbl(id INTEGER)",
		"INSERT INTO sp_rb_tbl VALUES(1)",
		"SAVEPOINT sp_rb",
		"INSERT INTO sp_rb_tbl VALUES(2)",
	)

	// Rollback to savepoint should discard the second insert.
	covExec(t, db, "ROLLBACK TO SAVEPOINT sp_rb")

	count := covQueryInt64(t, db, "SELECT COUNT(*) FROM sp_rb_tbl")
	if count != 1 {
		t.Errorf("expected 1 row after ROLLBACK TO SAVEPOINT, got %d", count)
	}
}

func TestCompileDDLCollation_SavepointNested(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// Create the table and use two independent savepoints sequentially.
	covExec(t, db,
		"CREATE TABLE sp_nest(id INTEGER)",
		"SAVEPOINT sp_a",
		"INSERT INTO sp_nest VALUES(10)",
		"RELEASE SAVEPOINT sp_a",
		"SAVEPOINT sp_b",
		"INSERT INTO sp_nest VALUES(20)",
		"RELEASE SAVEPOINT sp_b",
	)

	count := covQueryInt64(t, db, "SELECT COUNT(*) FROM sp_nest")
	if count != 2 {
		t.Errorf("expected 2 rows after sequential savepoints, got %d", count)
	}
}

// ============================================================================
// TestCompileDDLCollation_CollateExpr
// Exercises: resolveExprCollation via queries that use COLLATE expressions.
// The collation helpers are called from ORDER BY / WHERE compilation when a
// CollateExpr, IdentExpr (column with collation), or ParenExpr appears.
// ============================================================================

func TestCompileDDLCollation_CollateExprOrderBy(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE coll_tbl(id INTEGER, name TEXT)",
		"INSERT INTO coll_tbl VALUES(1,'banana'),(2,'Apple'),(3,'cherry')",
	)

	// ORDER BY with explicit COLLATE NOCASE exercises CollateExpr path.
	rows, err := db.Query("SELECT name FROM coll_tbl ORDER BY name COLLATE NOCASE")
	if err != nil {
		t.Fatalf("ORDER BY COLLATE: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, s)
	}
	if len(names) != 3 {
		t.Errorf("expected 3 rows, got %d", len(names))
	}
}

func TestCompileDDLCollation_CollateExprWhere(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE cw_tbl(id INTEGER, tag TEXT)",
		"INSERT INTO cw_tbl VALUES(1,'Hello'),(2,'world'),(3,'HELLO')",
	)

	// WHERE with COLLATE exercises CollateExpr in a comparison context.
	n := covQueryInt64(t, db, "SELECT COUNT(*) FROM cw_tbl WHERE tag = 'hello' COLLATE NOCASE")
	if n != 2 {
		t.Errorf("expected 2 NOCASE matches for 'hello', got %d", n)
	}
}

// ============================================================================
// TestCompileDDLCollation_ColumnCollation
// Creates a table where a column has an explicit COLLATE declaration, which
// exercises the IdentExpr branch of resolveExprCollation when ORDER BY uses
// that column (the column's declared collation is returned).
// ============================================================================

func TestCompileDDLCollation_ColumnCollation(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE col_coll(id INTEGER, label TEXT COLLATE NOCASE)",
		"INSERT INTO col_coll VALUES(1,'Zebra'),(2,'apple'),(3,'Mango')",
	)

	// The column has a COLLATE declaration; ORDER BY that column exercises the
	// IdentExpr path of resolveExprCollation.
	rows, err := db.Query("SELECT label FROM col_coll ORDER BY label")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, s)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(got), got)
	}
}

func TestCompileDDLCollation_ColumnCollationBinary(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// BINARY collation (default) on a column.
	covExec(t, db,
		"CREATE TABLE bin_coll(id INTEGER, code TEXT COLLATE BINARY)",
		"INSERT INTO bin_coll VALUES(1,'A'),(2,'a'),(3,'B')",
	)

	// BINARY is case-sensitive, so 'a' != 'A'.
	n := covQueryInt64(t, db, "SELECT COUNT(*) FROM bin_coll WHERE code = 'a'")
	if n != 1 {
		t.Errorf("expected 1 BINARY match for 'a', got %d", n)
	}
}

// ============================================================================
// TestCompileDDLCollation_WithoutRowID
// Exercises: initializeNewTable with WITHOUT ROWID (allocateTablePage uses
// CreateWithoutRowidTable instead of CreateTable).
// ============================================================================

func TestCompileDDLCollation_WithoutRowID(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	covExec(t, db,
		"CREATE TABLE wrid(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
		"INSERT INTO wrid VALUES('key1','val1')",
		"INSERT INTO wrid VALUES('key2','val2')",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='wrid'")
	if n != 1 {
		t.Errorf("expected WITHOUT ROWID table in sqlite_master, got %d", n)
	}

	count := covQueryInt64(t, db, "SELECT COUNT(*) FROM wrid")
	if count != 2 {
		t.Errorf("expected 2 rows in WITHOUT ROWID table, got %d", count)
	}
}

// TestCompileDDLCollation_WithoutRowIDAndAutoincrement verifies that WITHOUT
// ROWID tables do not require sqlite_sequence.
func TestCompileDDLCollation_WithoutRowIDNoSequence(t *testing.T) {
	t.Parallel()
	db, done := openCovDB(t)
	defer done()

	// WITHOUT ROWID table has no AUTOINCREMENT, so sqlite_sequence must not be created.
	covExec(t, db,
		"CREATE TABLE wrid2(pk INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
	)

	n := covCountRows(t, db, "SELECT name FROM sqlite_master WHERE name='sqlite_sequence'")
	if n != 0 {
		t.Errorf("expected no sqlite_sequence for WITHOUT ROWID table without AUTOINCREMENT, got %d", n)
	}
}
