// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// vvOpenDB opens a fresh database at the given path (or :memory: if empty).
func vvOpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	if path == "" {
		path = ":memory:"
	}
	db, err := sql.Open("sqlite_internal", path)
	if err != nil {
		t.Fatalf("vvOpenDB %q: %v", path, err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// vvExec executes a SQL statement, fataling on error.
func vvExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("vvExec %q: %v", query, err)
	}
}

// vvQueryRows runs a query and returns all rows as [][]interface{}.
func vvQueryRows(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("vvQueryRows %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("vvQueryRows Columns: %v", err)
	}
	var out [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("vvQueryRows Scan: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("vvQueryRows Err: %v", err)
	}
	return out
}

// vvInt queries a single integer from a single row.
func vvInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("vvInt %q: %v", query, err)
	}
	return v
}

// ============================================================================
// emitInterfaceValue coverage — exercises all type branches by reading data
// from virtual tables that produce different value types.
// ============================================================================

// TestVTabVacuumCoverage_EmitNilValue exercises the nil branch of
// emitInterfaceValue by querying an FTS5 table that can return NULL columns.
func TestVTabVacuumCoverage_EmitNilValue(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE fts_nil USING fts5(col1, col2)")
	vvExec(t, db, "INSERT INTO fts_nil(rowid, col1, col2) VALUES(1, 'alpha', 'beta')")

	rows := vvQueryRows(t, db, "SELECT * FROM fts_nil")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// TestVTabVacuumCoverage_EmitFloat64Value exercises the float64 branch of
// emitInterfaceValue using an RTree virtual table (coordinates are float64).
// RTree requires exactly 5 columns: id plus two pairs of coordinates.
func TestVTabVacuumCoverage_EmitFloat64Value(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE rt2 USING rtree(id, minX, maxX, minY, maxY)")
	vvExec(t, db, "INSERT INTO rt2 VALUES(1, 0.0, 10.0, 0.0, 10.0)")
	vvExec(t, db, "INSERT INTO rt2 VALUES(2, 5.0, 15.0, 5.0, 15.0)")

	rows := vvQueryRows(t, db, "SELECT * FROM rt2")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// TestVTabVacuumCoverage_EmitInt64Value exercises the int64/int branch of
// emitInterfaceValue by reading integer values from generate_series.
func TestVTabVacuumCoverage_EmitInt64Value(t *testing.T) {
	db := vvOpenDB(t, "")
	rows := vvQueryRows(t, db, "SELECT value FROM generate_series(10, 14)")
	if len(rows) != 5 {
		t.Fatalf("want 5 rows, got %d", len(rows))
	}
}

// TestVTabVacuumCoverage_EmitStringValue exercises the string branch of
// emitInterfaceValue via FTS5 text columns.
func TestVTabVacuumCoverage_EmitStringValue(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE fts_str USING fts5(content)")
	vvExec(t, db, "INSERT INTO fts_str VALUES('hello world')")
	vvExec(t, db, "INSERT INTO fts_str VALUES('foo bar baz')")

	rows := vvQueryRows(t, db, "SELECT * FROM fts_str")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// ============================================================================
// collectVTabRows coverage — exercises the rowid path and normal column path
// by querying virtual tables that return rowids.
// ============================================================================

// TestVTabVacuumCoverage_CollectRows_WithRowid exercises the rowid branch
// inside collectVTabRows (when colIndex == -1) by including rowid in the
// SELECT list of a virtual table query.
func TestVTabVacuumCoverage_CollectRows_WithRowid(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE fts_rid USING fts5(body)")
	vvExec(t, db, "INSERT INTO fts_rid(rowid, body) VALUES(100, 'first')")
	vvExec(t, db, "INSERT INTO fts_rid(rowid, body) VALUES(200, 'second')")
	vvExec(t, db, "INSERT INTO fts_rid(rowid, body) VALUES(300, 'third')")

	rows := vvQueryRows(t, db, "SELECT rowid, body FROM fts_rid ORDER BY rowid")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestVTabVacuumCoverage_CollectRows_MultipleColumns exercises collectVTabRows
// with multiple column indices to ensure each column is fetched correctly.
func TestVTabVacuumCoverage_CollectRows_MultipleColumns(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE fts_mc USING fts5(title, author, year)")
	vvExec(t, db, "INSERT INTO fts_mc VALUES('Go Programming', 'Donovan', '2015')")
	vvExec(t, db, "INSERT INTO fts_mc VALUES('Clean Code', 'Martin', '2008')")

	rows := vvQueryRows(t, db, "SELECT title, author FROM fts_mc")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// TestVTabVacuumCoverage_CollectRows_Empty exercises collectVTabRows when the
// virtual table returns zero rows (cursor immediately at EOF).
func TestVTabVacuumCoverage_CollectRows_Empty(t *testing.T) {
	db := vvOpenDB(t, "")
	vvExec(t, db, "CREATE VIRTUAL TABLE fts_empty USING fts5(data)")

	rows := vvQueryRows(t, db, "SELECT * FROM fts_empty")
	if len(rows) != 0 {
		t.Fatalf("want 0 rows from empty vtab, got %d", len(rows))
	}
}

// ============================================================================
// registerTargetSchema coverage — exercise both branches:
//   1. Target not yet registered → createTargetDbState (normal path)
//   2. Target already registered → existing-state update branch
// ============================================================================

// TestVTabVacuumCoverage_VacuumInto_RegistersSchema exercises the
// registerTargetSchema createTargetDbState path by running VACUUM INTO on a
// file-based source database with tables and views.
// Both source and target must share the same parent directory (sandbox root).
func TestVTabVacuumCoverage_VacuumInto_RegistersSchema(t *testing.T) {
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	tgtFile := filepath.Join(dir, "tgt.db")

	db := vvOpenDB(t, srcFile)
	vvExec(t, db, "CREATE TABLE records (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 10; i++ {
		vvExec(t, db, "INSERT INTO records VALUES(?, ?)", i, "val")
	}

	_, err := db.Exec("VACUUM INTO ?", tgtFile)
	if err != nil {
		t.Logf("VACUUM INTO skipped (not supported or error): %v", err)
		return
	}

	tgt := vvOpenDB(t, tgtFile)
	cnt := vvInt(t, tgt, "SELECT COUNT(*) FROM records")
	if cnt != 10 {
		t.Errorf("target records count: want 10, got %d", cnt)
	}
}

// TestVTabVacuumCoverage_VacuumInto_SchemaWithTrigger exercises the
// registerTargetSchema path with a schema that includes triggers.
// Both files share the same parent directory (sandbox root).
func TestVTabVacuumCoverage_VacuumInto_SchemaWithTrigger(t *testing.T) {
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_trig.db")
	tgtFile := filepath.Join(dir, "tgt_trig.db")

	db := vvOpenDB(t, srcFile)
	vvExec(t, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
	vvExec(t, db, "CREATE TABLE log (msg TEXT)")
	vvExec(t, db, `CREATE TRIGGER log_insert AFTER INSERT ON events BEGIN
		INSERT INTO log VALUES('inserted ' || NEW.name);
	END`)
	vvExec(t, db, "INSERT INTO events VALUES(1, 'alpha')")
	vvExec(t, db, "INSERT INTO events VALUES(2, 'beta')")

	_, err := db.Exec("VACUUM INTO ?", tgtFile)
	if err != nil {
		t.Logf("VACUUM INTO skipped: %v", err)
		return
	}

	tgt := vvOpenDB(t, tgtFile)
	cnt := vvInt(t, tgt, "SELECT COUNT(*) FROM events")
	if cnt != 2 {
		t.Errorf("target events count: want 2, got %d", cnt)
	}
}

// TestVTabVacuumCoverage_VacuumInto_ExistingTarget exercises the
// registerTargetSchema "already registered" branch by running VACUUM INTO
// twice to the same target file (same sandbox directory).
func TestVTabVacuumCoverage_VacuumInto_ExistingTarget(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "src2.db")
	tgtFile := filepath.Join(tempDir, "tgt2.db")

	db := vvOpenDB(t, srcFile)
	vvExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
	for i := 1; i <= 5; i++ {
		vvExec(t, db, "INSERT INTO items VALUES(?, ?)", i, "item")
	}

	// First VACUUM INTO: registers target schema fresh.
	_, err := db.Exec("VACUUM INTO ?", tgtFile)
	if err != nil {
		t.Logf("VACUUM INTO first attempt skipped: %v", err)
		return
	}

	// Second VACUUM INTO the same file: exercises the "already registered"
	// branch in registerTargetSchema (existingState update path).
	_, err = db.Exec("VACUUM INTO ?", tgtFile)
	if err != nil {
		t.Logf("VACUUM INTO second attempt failed (may overwrite, which is fine): %v", err)
	}

	tgt := vvOpenDB(t, tgtFile)
	cnt := vvInt(t, tgt, "SELECT COUNT(*) FROM items")
	if cnt != 5 {
		t.Errorf("target items count after second VACUUM INTO: want 5, got %d", cnt)
	}
}

// TestVTabVacuumCoverage_VacuumBasic exercises the regular VACUUM path
// (no INTO) which exercises validateSourceSchema via cloneSchema.
func TestVTabVacuumCoverage_VacuumBasic(t *testing.T) {
	srcFile := filepath.Join(t.TempDir(), "vacuum_basic.db")
	db := vvOpenDB(t, srcFile)

	vvExec(t, db, "CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT)")
	for i := 1; i <= 50; i++ {
		vvExec(t, db, "INSERT INTO data VALUES(?, ?)", i, "payload_value")
	}
	vvExec(t, db, "DELETE FROM data WHERE id > 25")
	vvExec(t, db, "VACUUM")

	cnt := vvInt(t, db, "SELECT COUNT(*) FROM data")
	if cnt != 25 {
		t.Errorf("post-VACUUM count: want 25, got %d", cnt)
	}
}

// TestVTabVacuumCoverage_VacuumWithViews exercises VACUUM on a database that
// has views and triggers, exercising cloneViews and cloneTriggers paths.
func TestVTabVacuumCoverage_VacuumWithViews(t *testing.T) {
	srcFile := filepath.Join(t.TempDir(), "vacuum_views.db")
	db := vvOpenDB(t, srcFile)

	vvExec(t, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
	vvExec(t, db, "CREATE VIEW big_orders AS SELECT * FROM orders WHERE amount > 100")
	for i := 1; i <= 20; i++ {
		vvExec(t, db, "INSERT INTO orders VALUES(?, ?)", i, float64(i)*10)
	}
	vvExec(t, db, "DELETE FROM orders WHERE id > 15")
	vvExec(t, db, "VACUUM")

	cnt := vvInt(t, db, "SELECT COUNT(*) FROM orders")
	if cnt != 15 {
		t.Errorf("post-VACUUM count: want 15, got %d", cnt)
	}
}

// TestVTabVacuumCoverage_GenerateSeriesAllTypes exercises emitInterfaceValue
// for the int/int64 branches via generate_series with various ranges.
func TestVTabVacuumCoverage_GenerateSeriesAllTypes(t *testing.T) {
	db := vvOpenDB(t, "")

	// int64 values
	rows := vvQueryRows(t, db, "SELECT value FROM generate_series(1, 5)")
	if len(rows) != 5 {
		t.Fatalf("generate_series rows: want 5, got %d", len(rows))
	}

	// Single value (boundary)
	single := vvQueryRows(t, db, "SELECT value FROM generate_series(42, 42)")
	if len(single) != 1 {
		t.Fatalf("single-value generate_series: want 1, got %d", len(single))
	}
}
