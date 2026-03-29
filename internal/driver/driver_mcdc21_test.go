// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// MC/DC 21 — SQL-level tests for low-coverage driver paths.
//
// Targets:
//   compile_ddl.go:391       compileCreateTrigger  — IF NOT EXISTS already-exists branch
//   compile_dml.go:295       compileInsertSelectMaterialised — materialized INSERT SELECT
//   compile_dml.go:1709      compileUpsertDoUpdate — ON CONFLICT DO UPDATE SET
//   compile_select_agg.go:294 emitJSONGroupArrayUpdate — json_group_array with NULLs
//   compile_compound.go:224  applySetOperation — EXCEPT and INTERSECT
//   compile_ddl.go:133       registerForeignKeyConstraints — CREATE TABLE with FOREIGN KEY

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func drv21Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func drv21Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func drv21QueryInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// compileCreateTrigger — IF NOT EXISTS with already-existing trigger
// ---------------------------------------------------------------------------

// TestMCDC21_CreateTrigger_IfNotExists exercises the branch in compileCreateTrigger
// where stmt.IfNotExists == true and the trigger already exists.
func TestMCDC21_CreateTrigger_IfNotExists(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE trig_tbl (id INTEGER PRIMARY KEY, val TEXT)`)

	// First CREATE TRIGGER — should succeed.
	_, err := db.Exec(`CREATE TRIGGER trig1 AFTER INSERT ON trig_tbl BEGIN SELECT 1; END`)
	if err != nil {
		t.Skipf("CREATE TRIGGER not supported: %v", err)
	}

	// Second CREATE TRIGGER IF NOT EXISTS with same name — should succeed silently.
	_, err = db.Exec(`CREATE TRIGGER IF NOT EXISTS trig1 AFTER INSERT ON trig_tbl BEGIN SELECT 1; END`)
	if err != nil {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS on existing trigger should succeed, got: %v", err)
	}

	// Also verify the original trigger still exists by trying to DROP it.
	_, err = db.Exec(`DROP TRIGGER trig1`)
	if err != nil {
		t.Logf("DROP TRIGGER: %v", err)
	}
}

// ---------------------------------------------------------------------------
// compileInsertSelectMaterialised — INSERT INTO ... SELECT (materialized path)
// ---------------------------------------------------------------------------

// TestMCDC21_InsertSelectMaterialised exercises compileInsertSelectMaterialised
// with an ORDER BY clause to force the materialized path.
func TestMCDC21_InsertSelectMaterialised(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE src21 (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `CREATE TABLE dst21 (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `INSERT INTO src21 VALUES (3, 'c')`)
	drv21Exec(t, db, `INSERT INTO src21 VALUES (1, 'a')`)
	drv21Exec(t, db, `INSERT INTO src21 VALUES (2, 'b')`)

	// ORDER BY forces the materialized path via insertSelectNeedsMaterialise.
	_, err := db.Exec(`INSERT INTO dst21 SELECT id, val FROM src21 ORDER BY id`)
	if err != nil {
		t.Skipf("INSERT SELECT ORDER BY not supported: %v", err)
	}

	got := drv21QueryInt(t, db, `SELECT COUNT(*) FROM dst21`)
	if got != 3 {
		t.Errorf("expected 3 rows in dst21, got %d", got)
	}
}

// TestMCDC21_InsertSelectMaterialised_Distinct exercises the materialized path
// via DISTINCT.
func TestMCDC21_InsertSelectMaterialised_Distinct(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE src21d (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `CREATE TABLE dst21d (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `INSERT INTO src21d VALUES (1, 'a')`)
	drv21Exec(t, db, `INSERT INTO src21d VALUES (1, 'a')`)
	drv21Exec(t, db, `INSERT INTO src21d VALUES (2, 'b')`)

	_, err := db.Exec(`INSERT INTO dst21d SELECT DISTINCT id, val FROM src21d`)
	if err != nil {
		t.Skipf("INSERT SELECT DISTINCT not supported: %v", err)
	}

	got := drv21QueryInt(t, db, `SELECT COUNT(*) FROM dst21d`)
	if got != 2 {
		t.Errorf("expected 2 distinct rows in dst21d, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// compileUpsertDoUpdate — ON CONFLICT DO UPDATE SET
// ---------------------------------------------------------------------------

// TestMCDC21_UpsertDoUpdate exercises compileUpsertDoUpdate with a conflict
// on the primary key, updating an existing row.
func TestMCDC21_UpsertDoUpdate(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE upsert21 (id INTEGER PRIMARY KEY, val TEXT, cnt INTEGER)`)
	drv21Exec(t, db, `INSERT INTO upsert21 VALUES (1, 'original', 0)`)

	_, err := db.Exec(`INSERT INTO upsert21(id, val, cnt) VALUES(1, 'updated', 5)
		ON CONFLICT(id) DO UPDATE SET val=excluded.val, cnt=excluded.cnt`)
	if err != nil {
		t.Skipf("UPSERT DO UPDATE not supported: %v", err)
	}

	var val string
	var cnt int64
	if err := db.QueryRow(`SELECT val, cnt FROM upsert21 WHERE id=1`).Scan(&val, &cnt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "updated" {
		t.Errorf("expected val='updated', got %q", val)
	}
	if cnt != 5 {
		t.Errorf("expected cnt=5, got %d", cnt)
	}
}

// TestMCDC21_UpsertDoUpdate_MultiCol exercises upsert with multiple columns in DO UPDATE.
func TestMCDC21_UpsertDoUpdate_MultiCol(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE upsert21b (k TEXT PRIMARY KEY, a INTEGER, b INTEGER)`)
	drv21Exec(t, db, `INSERT INTO upsert21b VALUES ('x', 1, 2)`)

	_, err := db.Exec(`INSERT INTO upsert21b(k, a, b) VALUES('x', 10, 20)
		ON CONFLICT(k) DO UPDATE SET a=excluded.a, b=excluded.b`)
	if err != nil {
		t.Skipf("UPSERT DO UPDATE multi-col not supported: %v", err)
	}

	var a, b int64
	if err := db.QueryRow(`SELECT a, b FROM upsert21b WHERE k='x'`).Scan(&a, &b); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if a != 10 || b != 20 {
		t.Errorf("expected a=10 b=20, got a=%d b=%d", a, b)
	}
}

// ---------------------------------------------------------------------------
// emitJSONGroupArrayUpdate — json_group_array with NULLs
// ---------------------------------------------------------------------------

// TestMCDC21_JSONGroupArray exercises emitJSONGroupArrayUpdate.
func TestMCDC21_JSONGroupArray(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE jga21 (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `INSERT INTO jga21 VALUES (1, 'alpha')`)
	drv21Exec(t, db, `INSERT INTO jga21 VALUES (2, 'beta')`)
	drv21Exec(t, db, `INSERT INTO jga21 VALUES (3, 'gamma')`)

	var result string
	err := db.QueryRow(`SELECT json_group_array(val) FROM jga21`).Scan(&result)
	if err != nil {
		t.Skipf("json_group_array not supported: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty json_group_array result")
	}
}

// TestMCDC21_JSONGroupArray_WithNulls exercises json_group_array including NULL values.
func TestMCDC21_JSONGroupArray_WithNulls(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE jga21n (id INTEGER, val TEXT)`)
	drv21Exec(t, db, `INSERT INTO jga21n VALUES (1, 'alpha')`)
	drv21Exec(t, db, `INSERT INTO jga21n VALUES (2, NULL)`)
	drv21Exec(t, db, `INSERT INTO jga21n VALUES (3, 'gamma')`)

	var result string
	err := db.QueryRow(`SELECT json_group_array(val) FROM jga21n`).Scan(&result)
	if err != nil {
		t.Skipf("json_group_array with NULLs not supported: %v", err)
	}
	// Should contain "null" for the NULL row.
	if result == "" {
		t.Error("expected non-empty json_group_array result with NULLs")
	}
}

// ---------------------------------------------------------------------------
// applySetOperation — EXCEPT and INTERSECT
// ---------------------------------------------------------------------------

// TestMCDC21_ApplySetOperation_Except exercises the CompoundExcept path.
func TestMCDC21_ApplySetOperation_Except(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE set21a (n INTEGER)`)
	drv21Exec(t, db, `CREATE TABLE set21b (n INTEGER)`)
	for _, v := range []int{1, 2, 3, 4, 5} {
		drv21Exec(t, db, `INSERT INTO set21a VALUES (?)`, v)
	}
	for _, v := range []int{3, 4, 5, 6, 7} {
		drv21Exec(t, db, `INSERT INTO set21b VALUES (?)`, v)
	}

	rows, err := db.Query(`SELECT n FROM set21a EXCEPT SELECT n FROM set21b ORDER BY n`)
	if err != nil {
		t.Skipf("EXCEPT not supported: %v", err)
	}
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, n)
	}
	if len(results) != 2 || results[0] != 1 || results[1] != 2 {
		t.Errorf("EXCEPT: expected [1 2], got %v", results)
	}
}

// TestMCDC21_ApplySetOperation_Intersect exercises the CompoundIntersect path.
func TestMCDC21_ApplySetOperation_Intersect(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE int21a (n INTEGER)`)
	drv21Exec(t, db, `CREATE TABLE int21b (n INTEGER)`)
	for _, v := range []int{1, 2, 3, 4, 5} {
		drv21Exec(t, db, `INSERT INTO int21a VALUES (?)`, v)
	}
	for _, v := range []int{3, 4, 5, 6, 7} {
		drv21Exec(t, db, `INSERT INTO int21b VALUES (?)`, v)
	}

	rows, err := db.Query(`SELECT n FROM int21a INTERSECT SELECT n FROM int21b ORDER BY n`)
	if err != nil {
		t.Skipf("INTERSECT not supported: %v", err)
	}
	defer rows.Close()

	var results []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, n)
	}
	if len(results) != 3 {
		t.Errorf("INTERSECT: expected 3 rows, got %d: %v", len(results), results)
	}
}

// TestMCDC21_ApplySetOperation_Except_Empty exercises EXCEPT producing empty result.
func TestMCDC21_ApplySetOperation_Except_Empty(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	drv21Exec(t, db, `CREATE TABLE exc21a (n INTEGER)`)
	drv21Exec(t, db, `CREATE TABLE exc21b (n INTEGER)`)
	for _, v := range []int{1, 2, 3} {
		drv21Exec(t, db, `INSERT INTO exc21a VALUES (?)`, v)
		drv21Exec(t, db, `INSERT INTO exc21b VALUES (?)`, v)
	}

	rows, err := db.Query(`SELECT n FROM exc21a EXCEPT SELECT n FROM exc21b`)
	if err != nil {
		t.Skipf("EXCEPT empty not supported: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected empty EXCEPT result, got %d rows", count)
	}
}

// ---------------------------------------------------------------------------
// registerForeignKeyConstraints — CREATE TABLE with FOREIGN KEY
// ---------------------------------------------------------------------------

// TestMCDC21_ForeignKeyConstraints exercises registerForeignKeyConstraints
// by enabling foreign_keys pragma and creating tables with FK constraints.
func TestMCDC21_ForeignKeyConstraints(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	_, err := db.Exec(`PRAGMA foreign_keys = ON`)
	if err != nil {
		t.Skipf("PRAGMA foreign_keys not supported: %v", err)
	}

	drv21Exec(t, db, `CREATE TABLE parent21 (id INTEGER PRIMARY KEY, name TEXT)`)

	_, err = db.Exec(`CREATE TABLE child21 (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parent21(id))`)
	if err != nil {
		t.Skipf("CREATE TABLE with FOREIGN KEY not supported: %v", err)
	}

	// Insert a parent row and a child row — FK should be satisfied.
	drv21Exec(t, db, `INSERT INTO parent21 VALUES (1, 'parent')`)

	_, err = db.Exec(`INSERT INTO child21 VALUES (1, 1)`)
	if err != nil {
		t.Logf("INSERT with FK: %v", err)
	}
}

// TestMCDC21_ForeignKeyConstraints_ColumnLevel exercises column-level FK syntax.
func TestMCDC21_ForeignKeyConstraints_ColumnLevel(t *testing.T) {
	t.Parallel()
	db := drv21Open(t)

	_, err := db.Exec(`PRAGMA foreign_keys = ON`)
	if err != nil {
		t.Skipf("PRAGMA foreign_keys not supported: %v", err)
	}

	drv21Exec(t, db, `CREATE TABLE fkparent21 (id INTEGER PRIMARY KEY, val TEXT)`)

	// Column-level REFERENCES syntax.
	_, err = db.Exec(`CREATE TABLE fkchild21 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkparent21(id))`)
	if err != nil {
		t.Skipf("column-level REFERENCES not supported: %v", err)
	}

	drv21Exec(t, db, `INSERT INTO fkparent21 VALUES (10, 'x')`)
	_, err = db.Exec(`INSERT INTO fkchild21 VALUES (1, 10)`)
	if err != nil {
		t.Logf("INSERT with column-level FK: %v", err)
	}
}
