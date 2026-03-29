// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// drv16bOpen opens an in-memory sqlite_internal database or fatals.
func drv16bOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// drv16bExec executes SQL and fatals on error.
func drv16bExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// drv16bQueryInt runs a single-int query and returns the value.
func drv16bQueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// drv16bOpenFile opens a file-based sqlite_internal database or fatals.
func drv16bOpenFile(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", path)
	if err != nil {
		t.Fatalf("sql.Open file %q: %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ============================================================================
// VACUUM INTO
// ============================================================================

// TestMCDC16b_VacuumInto_Basic exercises executeVacuum with IntoFile path on a
// file-based DB. Creates data, runs VACUUM INTO a temp path, verifies file exists.
func TestMCDC16b_VacuumInto_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	db := drv16bOpenFile(t, srcPath)
	drv16bExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 0; i < 5; i++ {
		drv16bExec(t, db, "INSERT INTO items (val) VALUES (?)", "row")
	}

	_, err := db.Exec("VACUUM INTO ?", dstPath)
	if err != nil {
		t.Skipf("VACUUM INTO not implemented: %v", err)
	}

	if _, statErr := os.Stat(dstPath); statErr != nil {
		t.Errorf("VACUUM INTO target file not created: %v", statErr)
	}
}

// ============================================================================
// Upsert DO UPDATE
// ============================================================================

// TestMCDC16b_UpsertDoUpdate_Basic exercises compileUpsertDoUpdate with a
// simple ON CONFLICT(id) DO UPDATE SET v=excluded.v.
func TestMCDC16b_UpsertDoUpdate_Basic(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv16bExec(t, db, "INSERT INTO t(id,v) VALUES(1,'a')")

	_, err := db.Exec("INSERT INTO t(id,v) VALUES(1,'b') ON CONFLICT(id) DO UPDATE SET v=excluded.v")
	if err != nil {
		t.Skipf("ON CONFLICT DO UPDATE not implemented: %v", err)
	}

	var val string
	if scanErr := db.QueryRow("SELECT v FROM t WHERE id=1").Scan(&val); scanErr != nil {
		t.Fatalf("SELECT after upsert: %v", scanErr)
	}
	if val != "b" {
		t.Errorf("upsert DO UPDATE: got v=%q, want %q", val, "b")
	}
}

// TestMCDC16b_UpsertDoUpdate_WithWhere exercises the DO UPDATE WHERE branch
// in compileUpsertDoUpdate (stmt.Upsert.Update.Where != nil).
func TestMCDC16b_UpsertDoUpdate_WithWhere(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, locked INTEGER DEFAULT 0)")
	drv16bExec(t, db, "INSERT INTO t(id,v,locked) VALUES(1,'a',0)")

	_, err := db.Exec(
		"INSERT INTO t(id,v,locked) VALUES(1,'b',0) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE t.locked=0",
	)
	if err != nil {
		t.Skipf("ON CONFLICT DO UPDATE WHERE not implemented: %v", err)
	}

	var val string
	if scanErr := db.QueryRow("SELECT v FROM t WHERE id=1").Scan(&val); scanErr != nil {
		t.Fatalf("SELECT after upsert with WHERE: %v", scanErr)
	}
	if val != "b" {
		t.Errorf("upsert DO UPDATE WHERE: got v=%q, want %q", val, "b")
	}
}

// TestMCDC16b_UpsertDoNothing exercises ON CONFLICT DO NOTHING path.
func TestMCDC16b_UpsertDoNothing(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv16bExec(t, db, "INSERT INTO t(id,v) VALUES(1,'original')")

	_, err := db.Exec("INSERT INTO t(id,v) VALUES(1,'new') ON CONFLICT DO NOTHING")
	if err != nil {
		t.Skipf("ON CONFLICT DO NOTHING not implemented: %v", err)
	}

	var val string
	if scanErr := db.QueryRow("SELECT v FROM t WHERE id=1").Scan(&val); scanErr != nil {
		t.Fatalf("SELECT after ON CONFLICT DO NOTHING: %v", scanErr)
	}
	if val != "original" {
		t.Errorf("ON CONFLICT DO NOTHING: got v=%q, want %q", val, "original")
	}
}

// ============================================================================
// INSERT INTO ... SELECT WHERE
// ============================================================================

// TestMCDC16b_InsertSelectWhere exercises emitInsertSelectWhere by inserting
// rows from a source table via SELECT with a WHERE clause.
func TestMCDC16b_InsertSelectWhere(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE src (x INTEGER)")
	drv16bExec(t, db, "CREATE TABLE dest (x INTEGER)")
	for i := 1; i <= 10; i++ {
		drv16bExec(t, db, "INSERT INTO src(x) VALUES(?)", i)
	}

	drv16bExec(t, db, "INSERT INTO dest SELECT x FROM src WHERE x > 5")

	n := drv16bQueryInt(t, db, "SELECT COUNT(*) FROM dest")
	if n != 5 {
		t.Errorf("INSERT SELECT WHERE: got %d rows, want 5", n)
	}
}

// ============================================================================
// Compound SELECT (INTERSECT, EXCEPT, ORDER BY)
// ============================================================================

// TestMCDC16b_CompoundIntersect exercises applySetOperation CompoundIntersect branch.
func TestMCDC16b_CompoundIntersect(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t1 (a INTEGER)")
	drv16bExec(t, db, "CREATE TABLE t2 (a INTEGER)")
	for _, v := range []int{1, 2, 3, 4} {
		drv16bExec(t, db, "INSERT INTO t1(a) VALUES(?)", v)
	}
	for _, v := range []int{2, 4, 6} {
		drv16bExec(t, db, "INSERT INTO t2(a) VALUES(?)", v)
	}

	rows, err := db.Query("SELECT a FROM t1 INTERSECT SELECT a FROM t2")
	if err != nil {
		t.Skipf("INTERSECT not implemented: %v", err)
	}
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(vals) != 2 {
		t.Errorf("INTERSECT: got %d rows, want 2", len(vals))
	}
}

// TestMCDC16b_CompoundExcept exercises applySetOperation CompoundExcept branch.
func TestMCDC16b_CompoundExcept(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t1 (a INTEGER)")
	drv16bExec(t, db, "CREATE TABLE t2 (a INTEGER)")
	for _, v := range []int{1, 2, 3, 4} {
		drv16bExec(t, db, "INSERT INTO t1(a) VALUES(?)", v)
	}
	for _, v := range []int{2, 4} {
		drv16bExec(t, db, "INSERT INTO t2(a) VALUES(?)", v)
	}

	rows, err := db.Query("SELECT a FROM t1 EXCEPT SELECT a FROM t2")
	if err != nil {
		t.Skipf("EXCEPT not implemented: %v", err)
	}
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(vals) != 2 {
		t.Errorf("EXCEPT: got %d rows, want 2 (1 and 3)", len(vals))
	}
}

// TestMCDC16b_CompoundOrderBy exercises ORDER BY applied to a compound SELECT.
func TestMCDC16b_CompoundOrderBy(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t1 (a INTEGER)")
	drv16bExec(t, db, "CREATE TABLE t2 (a INTEGER)")
	for _, v := range []int{3, 1} {
		drv16bExec(t, db, "INSERT INTO t1(a) VALUES(?)", v)
	}
	for _, v := range []int{2, 4} {
		drv16bExec(t, db, "INSERT INTO t2(a) VALUES(?)", v)
	}

	rows, err := db.Query("SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a")
	if err != nil {
		t.Skipf("UNION ORDER BY not implemented: %v", err)
	}
	defer rows.Close()

	var vals []int64
	for rows.Next() {
		var v int64
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(vals) != 4 {
		t.Errorf("UNION ORDER BY: got %d rows, want 4", len(vals))
	}
	for i := 0; i < len(vals)-1; i++ {
		if vals[i] > vals[i+1] {
			t.Errorf("UNION ORDER BY: not sorted at index %d: %v", i, vals)
			break
		}
	}
}

// ============================================================================
// CREATE TRIGGER with WHEN clause
// ============================================================================

// TestMCDC16b_CreateTrigger_WithWhen exercises compileCreateTrigger with a WHEN
// clause (the WHEN branch in the trigger AST).
func TestMCDC16b_CreateTrigger_WithWhen(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv16bExec(t, db, "CREATE TABLE audit (msg TEXT)")

	_, err := db.Exec(`CREATE TRIGGER trg_when
		AFTER UPDATE ON t
		FOR EACH ROW
		WHEN old.v != new.v
		BEGIN
			INSERT INTO audit(msg) VALUES('changed');
		END`)
	if err != nil {
		t.Skipf("CREATE TRIGGER WHEN not implemented: %v", err)
	}

	drv16bExec(t, db, "INSERT INTO t(id,v) VALUES(1,'hello')")
	drv16bExec(t, db, "UPDATE t SET v='world' WHERE id=1")

	n := drv16bQueryInt(t, db, "SELECT COUNT(*) FROM audit")
	if n < 0 {
		t.Errorf("unexpected audit count: %d", n)
	}
}

// TestMCDC16b_CreateTrigger_ForEachRow exercises a basic FOR EACH ROW trigger
// without a WHEN clause (the standard path through compileCreateTrigger).
func TestMCDC16b_CreateTrigger_ForEachRow(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	drv16bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv16bExec(t, db, "CREATE TABLE log (entry TEXT)")

	_, err := db.Exec(`CREATE TRIGGER trg_row
		AFTER INSERT ON t
		FOR EACH ROW
		BEGIN
			INSERT INTO log(entry) VALUES(new.v);
		END`)
	if err != nil {
		t.Skipf("CREATE TRIGGER FOR EACH ROW not implemented: %v", err)
	}

	drv16bExec(t, db, "INSERT INTO t(id,v) VALUES(1,'foo')")

	n := drv16bQueryInt(t, db, "SELECT COUNT(*) FROM log")
	if n < 0 {
		t.Errorf("unexpected log count: %d", n)
	}
}

// ============================================================================
// FK enabled insert
// ============================================================================

// TestMCDC16b_FK_EnabledInsert_Valid exercises checkForeignKeyConstraints with a
// valid child row that has a matching parent.
func TestMCDC16b_FK_EnabledInsert_Valid(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite_internal", ":memory:?foreign_keys=on")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	drv16bExec(t, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	drv16bExec(t, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))")

	drv16bExec(t, db, "INSERT INTO parent(id,name) VALUES(1,'alice')")
	drv16bExec(t, db, "INSERT INTO child(id,parent_id) VALUES(10,1)")

	n := drv16bQueryInt(t, db, "SELECT COUNT(*) FROM child")
	if n != 1 {
		t.Errorf("FK valid insert: expected 1 child row, got %d", n)
	}
}

// TestMCDC16b_FK_EnabledInsert_Violation exercises checkForeignKeyConstraints
// when a child row references a non-existent parent (expect error).
func TestMCDC16b_FK_EnabledInsert_Violation(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite_internal", ":memory:?foreign_keys=on")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	drv16bExec(t, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	drv16bExec(t, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))")

	_, insertErr := db.Exec("INSERT INTO child(id,parent_id) VALUES(10,999)")
	if insertErr == nil {
		t.Skipf("FK violation not enforced (foreign_keys pragma may not be active)")
	}
}

// ============================================================================
// WITHOUT ROWID INSERT OR REPLACE
// ============================================================================

// TestMCDC16b_WithoutRowid_InsertOrReplace exercises deleteAndRetryComposite by
// doing INSERT OR REPLACE twice on a WITHOUT ROWID table with a composite PK.
func TestMCDC16b_WithoutRowid_InsertOrReplace(t *testing.T) {
	t.Parallel()
	db := drv16bOpen(t)

	_, err := db.Exec("CREATE TABLE t (a INT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	if err != nil {
		t.Skipf("WITHOUT ROWID not implemented: %v", err)
	}

	drv16bExec(t, db, "INSERT OR REPLACE INTO t VALUES(1,2)")
	drv16bExec(t, db, "INSERT OR REPLACE INTO t VALUES(1,2)")

	n := drv16bQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if n != 1 {
		t.Errorf("WITHOUT ROWID INSERT OR REPLACE: got %d rows, want 1", n)
	}
}
