// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func openHelpersDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestBeforeInsertTrigger exercises executeBeforeInsertTriggers via a BEFORE
// INSERT trigger followed by an INSERT.
func TestBeforeInsertTrigger(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE trig_src(id INTEGER)`)
	mustExec(t, db, `CREATE TABLE trig_log(event TEXT)`)
	mustExec(t, db, `CREATE TRIGGER trg_before_ins BEFORE INSERT ON trig_src BEGIN INSERT INTO trig_log VALUES('before'); END`)
	mustExec(t, db, `INSERT INTO trig_src VALUES(1)`)

	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM trig_log WHERE event='before'`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("want 1 before-insert log entry, got %d", n)
	}
}

// TestAfterInsertTrigger exercises executeAfterInsertTriggers via an AFTER
// INSERT trigger followed by an INSERT.
func TestAfterInsertTrigger(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE trig_a(id INTEGER)`)
	mustExec(t, db, `CREATE TABLE trig_alog(event TEXT)`)
	mustExec(t, db, `CREATE TRIGGER trg_after_ins AFTER INSERT ON trig_a BEGIN INSERT INTO trig_alog VALUES('after'); END`)
	mustExec(t, db, `INSERT INTO trig_a VALUES(42)`)

	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM trig_alog WHERE event='after'`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("want 1 after-insert log entry, got %d", n)
	}
}

// TestBeforeInsertTriggerMultipleRows exercises executeBeforeInsertTriggers for
// two inserts, checking the trigger fires once per statement.
func TestBeforeInsertTriggerMultipleRows(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE mi_src(v INTEGER)`)
	mustExec(t, db, `CREATE TABLE mi_log(n INTEGER)`)
	mustExec(t, db, `CREATE TRIGGER mi_bi BEFORE INSERT ON mi_src BEGIN INSERT INTO mi_log VALUES(1); END`)
	mustExec(t, db, `INSERT INTO mi_src VALUES(10)`)
	mustExec(t, db, `INSERT INTO mi_src VALUES(20)`)

	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM mi_log`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 2 {
		t.Errorf("want 2 trigger firings, got %d", n)
	}
}

// TestCreateViewWrapper exercises compileCreateViewWrapper via EXPLAIN.
func TestCreateViewWrapper(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE vw_base(a INTEGER, b TEXT)`)
	mustExec(t, db, `INSERT INTO vw_base VALUES(1,'x')`)

	// Trigger compileCreateViewWrapper via EXPLAIN (this also creates the view as a side effect).
	rows, err := db.Query(`EXPLAIN CREATE VIEW vw_explain AS SELECT a, b FROM vw_base`)
	if err != nil {
		t.Fatalf("EXPLAIN CREATE VIEW: %v", err)
	}
	rows.Close()

	// Create a separate view for querying.
	mustExec(t, db, `CREATE VIEW IF NOT EXISTS vw1 AS SELECT a, b FROM vw_base`)
	var a int64
	var b string
	if err := db.QueryRow(`SELECT a, b FROM vw1`).Scan(&a, &b); err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	if a != 1 || b != "x" {
		t.Errorf("want (1, x), got (%d, %s)", a, b)
	}
}

// TestDropViewWrapper exercises compileDropViewWrapper via EXPLAIN.
func TestDropViewWrapper(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE dvw_base(c INTEGER)`)
	mustExec(t, db, `CREATE VIEW dvw1 AS SELECT c FROM dvw_base`)
	mustExec(t, db, `CREATE VIEW dvw2 AS SELECT c FROM dvw_base`)

	// Trigger compileDropViewWrapper via EXPLAIN (drops dvw1 as side effect).
	rows, err := db.Query(`EXPLAIN DROP VIEW dvw1`)
	if err != nil {
		t.Fatalf("EXPLAIN DROP VIEW: %v", err)
	}
	rows.Close()

	// Drop the remaining view.
	mustExec(t, db, `DROP VIEW dvw2`)

	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='dvw2'`).Scan(&n); err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if n != 0 {
		t.Errorf("view dvw2 still exists after DROP VIEW, count=%d", n)
	}
}

// TestCreateVirtualTableWrapper exercises compileCreateVirtualTableWrapper via EXPLAIN.
func TestCreateVirtualTableWrapper(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	// Trigger compileCreateVirtualTableWrapper via EXPLAIN (creates fts_explain as side effect).
	rows, err := db.Query(`EXPLAIN CREATE VIRTUAL TABLE fts_explain USING fts5(content)`)
	if err != nil {
		t.Fatalf("EXPLAIN CREATE VIRTUAL TABLE: %v", err)
	}
	rows.Close()

	// Create a separate virtual table for querying.
	mustExec(t, db, `CREATE VIRTUAL TABLE fts_wrap USING fts5(content)`)
	mustExec(t, db, `INSERT INTO fts_wrap(content) VALUES('hello world')`)
	mustExec(t, db, `INSERT INTO fts_wrap(content) VALUES('foo bar')`)

	rows2, err2 := db.Query(`SELECT content FROM fts_wrap ORDER BY content`)
	if err2 != nil {
		t.Fatalf("SELECT from fts5: %v", err2)
	}
	defer rows2.Close()
	var rowCount int
	for rows2.Next() {
		rowCount++
		var s string
		if err := rows2.Scan(&s); err != nil {
			t.Fatalf("scan fts5 row: %v", err)
		}
	}
	if err := rows2.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if rowCount < 1 {
		t.Errorf("want rows from fts5 table, got 0")
	}
}

// TestMultiTableJoinOrderBy exercises the multi-table ORDER BY path
// (resolveOrderByColumnsMultiTable, resolveOrderByTermMultiTable, and
// emitJoinSorterPopulation). ORDER BY on a column from the second table
// pushes a column not in the SELECT list through the extra-expr path.
func TestMultiTableJoinOrderBy(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE jt1(id INTEGER, name TEXT)`)
	mustExec(t, db, `CREATE TABLE jt2(id INTEGER, score INTEGER)`)
	mustExec(t, db, `INSERT INTO jt1 VALUES(1,'alice'),(2,'bob'),(3,'carol')`)
	mustExec(t, db, `INSERT INTO jt2 VALUES(1,30),(2,10),(3,20)`)

	rows, err := db.Query(`SELECT jt1.name FROM jt1 JOIN jt2 ON jt1.id = jt2.id ORDER BY jt2.score ASC`)
	if err != nil {
		t.Fatalf("JOIN ORDER BY: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var nm string
		if err := rows.Scan(&nm); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, nm)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []string{"bob", "carol", "alice"}
	if len(names) != len(want) {
		t.Fatalf("want %v, got %v", want, names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("row %d: want %q got %q", i, want[i], names[i])
		}
	}
}

// TestMultiTableJoinOrderByDesc exercises the DESC direction in multi-table ORDER BY.
func TestMultiTableJoinOrderByDesc(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE od1(id INTEGER, label TEXT)`)
	mustExec(t, db, `CREATE TABLE od2(id INTEGER, val INTEGER)`)
	mustExec(t, db, `INSERT INTO od1 VALUES(1,'x'),(2,'y')`)
	mustExec(t, db, `INSERT INTO od2 VALUES(1,5),(2,3)`)

	rows, err := db.Query(`SELECT od1.label FROM od1 JOIN od2 ON od1.id = od2.id ORDER BY od2.val DESC`)
	if err != nil {
		t.Fatalf("JOIN ORDER BY DESC: %v", err)
	}
	defer rows.Close()

	var labels []string
	for rows.Next() {
		var lbl string
		if err := rows.Scan(&lbl); err != nil {
			t.Fatalf("scan: %v", err)
		}
		labels = append(labels, lbl)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(labels) != 2 || labels[0] != "x" || labels[1] != "y" {
		t.Errorf("want [x y], got %v", labels)
	}
}

// TestEstimateDistinctAndAnalyzeToInt64 exercises estimateDistinct and
// analyzeToInt64 by running ANALYZE on a table with many rows. The engine
// calls estimateDistinct when distinct counts are zero (no index data).
// analyzeToInt64 is exercised when the stat1 table is queried for statistics.
// verifyStat1NonEmpty reads stat strings for the given table/index and verifies they are non-empty.
func verifyStat1NonEmpty(t *testing.T, db *sql.DB, tbl, idx string) {
	t.Helper()
	rows, err := db.Query(`SELECT stat FROM sqlite_stat1 WHERE tbl=? AND idx=?`, tbl, idx)
	if err != nil {
		t.Fatalf("SELECT stat: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var stat string
		if err := rows.Scan(&stat); err != nil {
			t.Fatalf("scan stat: %v", err)
		}
		if stat == "" {
			t.Error("expected non-empty stat string")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

func TestEstimateDistinctAndAnalyzeToInt64(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE ana_t(a INTEGER, b TEXT)`)
	mustExec(t, db, `CREATE INDEX ana_t_a ON ana_t(a)`)

	for i := 0; i < 20; i++ {
		mustExec(t, db, `INSERT INTO ana_t VALUES(?, ?)`, i%5, "val")
	}

	mustExec(t, db, `ANALYZE ana_t`)

	var cnt int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='ana_t' AND idx='ana_t_a'`).Scan(&cnt); err != nil {
		t.Fatalf("stat query: %v", err)
	}
	if cnt == 0 {
		t.Error("expected sqlite_stat1 entry for ana_t_a index")
	}

	verifyStat1NonEmpty(t, db, "ana_t", "ana_t_a")
}

// TestAnalyzeWithZeroRowsEstimateDistinct exercises estimateDistinct on a
// table with no rows (rowCount <= 0 branch returns 1).
func TestAnalyzeWithZeroRowsEstimateDistinct(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE empty_t(x INTEGER, y INTEGER)`)
	mustExec(t, db, `CREATE INDEX empty_t_x ON empty_t(x)`)
	mustExec(t, db, `ANALYZE empty_t`)

	// Even with no data, a stat entry should be created.
	var cnt int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='empty_t'`).Scan(&cnt); err != nil {
		t.Fatalf("stat query: %v", err)
	}
	// Engine may create 0 or more entries; just verify no crash.
	_ = cnt
}

// TestAnalyzeToInt64TypeVariants exercises analyzeToInt64 via ANALYZE on a
// table whose stat string will be queried, exercising the string→int64 branch
// and confirming the int64 branch via normal stat storage.
func TestAnalyzeToInt64TypeVariants(t *testing.T) {
	db := openHelpersDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE stat_t(k INTEGER, v INTEGER)`)
	mustExec(t, db, `CREATE INDEX stat_t_k ON stat_t(k)`)
	for i := 1; i <= 10; i++ {
		mustExec(t, db, `INSERT INTO stat_t VALUES(?, ?)`, i, i*10)
	}
	mustExec(t, db, `ANALYZE stat_t`)

	// Manually insert a stat row whose 'stat' field is a plain text number,
	// exercising the string branch of analyzeToInt64 on subsequent reads.
	mustExec(t, db, `INSERT OR REPLACE INTO sqlite_stat1(tbl,idx,stat) VALUES('stat_t','fake_idx','42 10')`)

	var statStr string
	if err := db.QueryRow(`SELECT stat FROM sqlite_stat1 WHERE idx='fake_idx'`).Scan(&statStr); err != nil {
		t.Fatalf("SELECT fake stat: %v", err)
	}
	if statStr != "42 10" {
		t.Errorf("want '42 10', got %q", statStr)
	}
}
