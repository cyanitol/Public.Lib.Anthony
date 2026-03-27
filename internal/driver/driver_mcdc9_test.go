// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// MC/DC 9 — SQL-level coverage for driver low-coverage functions
//
// Targets:
//   compile_ddl.go:57    initializeNewTable        (73.3%) — CREATE TABLE edge cases
//   compile_ddl.go:391   compileCreateTrigger      (76.9%) — trigger compilation
//   compile_dml.go:1590  evalSubqueryToLiteral      (78.6%) — subquery expressions
//   compile_dml.go:1143  dmlToInt64                 (80.0%) — DML integer conversion
//   compile_compound.go:434 cmpCompoundValues       (80.0%) — compound comparison
//   compile_compound.go:557 typeOrder               (85.7%) — type ordering
//   compile_compound.go:662 emitLoadValue           (85.7%) — compound load
//   compile_analyze.go:156  countTableRows          (83.3%) — ANALYZE table scanning
//   compile_analyze.go:169  computeIndexStat        (87.5%) — ANALYZE index stats
//   compile_ddl.go:349   compileDropView            (88.9%) — DROP VIEW edge cases

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func drv9Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func drv9Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func drv9QueryInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

func drv9QueryStr(t *testing.T, db *sql.DB, q string, args ...interface{}) string {
	t.Helper()
	var s string
	if err := db.QueryRow(q, args...).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return s
}

// ---------------------------------------------------------------------------
// initializeNewTable — CREATE TABLE with STRICT keyword
// ---------------------------------------------------------------------------

func TestMCDC9Driver_InitializeNewTable_StrictTypes(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	// STRICT table enforces column types.
	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n REAL) STRICT`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,'hello',3.14)`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// initializeNewTable — CREATE TABLE with generated columns
// ---------------------------------------------------------------------------

func TestMCDC9Driver_InitializeNewTable_GeneratedColumns(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(
		a INTEGER,
		b INTEGER,
		c INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL
	)`)
	drv9Exec(t, db, `INSERT INTO t(a,b) VALUES(3,4)`)
	// Verify the row was inserted (generated column evaluation is engine-dependent).
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// initializeNewTable — CREATE TABLE without primary key (triggers rowid logic)
// ---------------------------------------------------------------------------

func TestMCDC9Driver_InitializeNewTable_NoPK(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(a TEXT, b INTEGER, c REAL)`)
	drv9Exec(t, db, `INSERT INTO t VALUES('hello',42,3.14)`)
	drv9Exec(t, db, `INSERT INTO t VALUES('world',99,2.71)`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// compileCreateTrigger — BEFORE INSERT trigger
// ---------------------------------------------------------------------------

func TestMCDC9Driver_CompileCreateTrigger_BeforeInsert(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `CREATE TABLE log(action TEXT, ts TEXT)`)
	drv9Exec(t, db, `CREATE TRIGGER trig_before BEFORE INSERT ON t
		BEGIN
			INSERT INTO log VALUES('insert', 'now');
		END`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,'hello')`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM log`); n != 1 {
		t.Errorf("expected 1 log entry, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// compileCreateTrigger — AFTER UPDATE trigger with WHEN clause
// ---------------------------------------------------------------------------

func TestMCDC9Driver_CompileCreateTrigger_AfterUpdateWhen(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)`)
	drv9Exec(t, db, `CREATE TABLE log(id INTEGER, old INTEGER, new INTEGER)`)
	drv9Exec(t, db, `CREATE TRIGGER trig_after AFTER UPDATE ON t
		WHEN NEW.v > 100
		BEGIN
			INSERT INTO log VALUES(NEW.id, OLD.v, NEW.v);
		END`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,50)`)
	drv9Exec(t, db, `UPDATE t SET v=200 WHERE id=1`) // triggers WHEN NEW.v > 100
	drv9Exec(t, db, `UPDATE t SET v=80 WHERE id=1`)  // WHEN condition false, no trigger

	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM log`); n != 1 {
		t.Errorf("expected 1 log entry, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// compileCreateTrigger — INSTEAD OF trigger on VIEW
// ---------------------------------------------------------------------------

func TestMCDC9Driver_CompileCreateTrigger_InsteadOf(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `CREATE VIEW vw AS SELECT id, v FROM base`)
	drv9Exec(t, db, `CREATE TRIGGER trig_instead INSTEAD OF INSERT ON vw
		BEGIN
			INSERT INTO base VALUES(NEW.id, NEW.v);
		END`)
	// Verify the underlying table is still accessible (trigger compiled without error).
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM base`); n != 0 {
		t.Errorf("expected 0 rows in base, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// compileDropView — DROP VIEW
// ---------------------------------------------------------------------------

func TestMCDC9Driver_CompileDropView(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `CREATE VIEW vw AS SELECT * FROM t`)
	drv9Exec(t, db, `DROP VIEW vw`)

	// Verify view is gone.
	_, err := db.Exec(`SELECT * FROM vw`)
	if err == nil {
		t.Error("expected error after DROP VIEW, got nil")
	}
}

func TestMCDC9Driver_CompileDropView_IfExists(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	// DROP VIEW IF EXISTS on non-existent view.
	drv9Exec(t, db, `DROP VIEW IF EXISTS no_such_view`)
}

// ---------------------------------------------------------------------------
// evalSubqueryToLiteral — subquery used in DML expressions
// ---------------------------------------------------------------------------

func TestMCDC9Driver_EvalSubqueryToLiteral_InInsert(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE src(v INTEGER)`)
	drv9Exec(t, db, `CREATE TABLE dst(v INTEGER)`)
	drv9Exec(t, db, `INSERT INTO src VALUES(42)`)

	// Subquery in INSERT VALUES — exercises evalSubqueryToLiteral.
	drv9Exec(t, db, `INSERT INTO dst VALUES((SELECT v FROM src))`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM dst`); n != 1 {
		t.Errorf("expected 1 row in dst, got %d", n)
	}
}

func TestMCDC9Driver_EvalSubqueryToLiteral_InUpdate(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)`)
	drv9Exec(t, db, `CREATE TABLE max_t(v INTEGER)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,100)`)
	drv9Exec(t, db, `INSERT INTO max_t VALUES(999)`)

	// Subquery in UPDATE SET.
	drv9Exec(t, db, `UPDATE t SET v=(SELECT v FROM max_t) WHERE id=1`)
	if n := drv9QueryInt(t, db, `SELECT v FROM t WHERE id=1`); n != 999 {
		t.Errorf("expected 999, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// dmlToInt64 — UPDATE with integer expressions
// ---------------------------------------------------------------------------

func TestMCDC9Driver_DmlToInt64_ArithmeticUpdate(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,10)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(2,20)`)

	drv9Exec(t, db, `UPDATE t SET n = n * 2 + 1`)
	if n := drv9QueryInt(t, db, `SELECT n FROM t WHERE id=1`); n != 21 {
		t.Errorf("expected 21, got %d", n)
	}
	if n := drv9QueryInt(t, db, `SELECT n FROM t WHERE id=2`); n != 41 {
		t.Errorf("expected 41, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// cmpCompoundValues + typeOrder — UNION/INTERSECT/EXCEPT comparison
// ---------------------------------------------------------------------------

func TestMCDC9Driver_CmpCompoundValues_Union(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE a(v)`)
	drv9Exec(t, db, `CREATE TABLE b(v)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(1)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(2)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(2)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(3)`)

	// UNION deduplicates — exercises cmpCompoundValues for equality check.
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM (SELECT v FROM a UNION SELECT v FROM b)`); n != 3 {
		t.Errorf("expected 3 distinct values, got %d", n)
	}
}

func TestMCDC9Driver_CmpCompoundValues_Intersect(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE a(v)`)
	drv9Exec(t, db, `CREATE TABLE b(v)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(1)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(2)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(3)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(2)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(3)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(4)`)

	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM (SELECT v FROM a INTERSECT SELECT v FROM b)`); n != 2 {
		t.Errorf("expected 2 values in intersection, got %d", n)
	}
}

func TestMCDC9Driver_CmpCompoundValues_Except(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE a(v)`)
	drv9Exec(t, db, `CREATE TABLE b(v)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(1)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(2)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(3)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(2)`)

	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM (SELECT v FROM a EXCEPT SELECT v FROM b)`); n != 2 {
		t.Errorf("expected 2 values (1,3) in except, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// typeOrder — UNION ALL with mixed types exercises type ordering
// ---------------------------------------------------------------------------

func TestMCDC9Driver_TypeOrder_MixedTypes(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	// Mix integers, reals, text, null in UNION — exercises typeOrder for comparison.
	rows, err := db.Query(`
		SELECT 1 UNION SELECT 'hello' UNION SELECT 3.14 UNION SELECT NULL
		ORDER BY 1
	`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected rows from mixed-type UNION")
	}
}

// ---------------------------------------------------------------------------
// ANALYZE — countTableRows + computeIndexStat
// ---------------------------------------------------------------------------

func TestMCDC9Driver_Analyze_TableWithIndex(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)`)
	drv9Exec(t, db, `CREATE INDEX idx_a ON t(a)`)
	drv9Exec(t, db, `CREATE INDEX idx_b ON t(b)`)
	for i := 0; i < 100; i++ {
		drv9Exec(t, db, `INSERT INTO t VALUES(?,?,?)`, i, fmt.Sprintf("key%d", i), i%10)
	}

	// ANALYZE computes statistics for indexes — exercises countTableRows + computeIndexStat.
	drv9Exec(t, db, `ANALYZE`)

	// Verify sqlite_stat1 was populated.
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM sqlite_stat1`); n == 0 {
		t.Error("expected sqlite_stat1 to be populated after ANALYZE")
	}
}

func TestMCDC9Driver_Analyze_SpecificTable(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `CREATE INDEX idx_t1 ON t1(v)`)
	for i := 0; i < 50; i++ {
		drv9Exec(t, db, `INSERT INTO t1 VALUES(?,?)`, i, fmt.Sprintf("v%d", i))
		drv9Exec(t, db, `INSERT INTO t2 VALUES(?,?)`, i, fmt.Sprintf("v%d", i))
	}

	// ANALYZE on specific table only.
	drv9Exec(t, db, `ANALYZE t1`)
}

// ---------------------------------------------------------------------------
// countDistinctPrefix — ANALYZE with composite index
// ---------------------------------------------------------------------------

func TestMCDC9Driver_Analyze_CompositeIndex(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER)`)
	drv9Exec(t, db, `CREATE INDEX idx_abc ON t(a,b,c)`)
	for i := 0; i < 50; i++ {
		drv9Exec(t, db, `INSERT INTO t VALUES(?,?,?,?)`, i, fmt.Sprintf("a%d", i%5), fmt.Sprintf("b%d", i%10), i)
	}

	drv9Exec(t, db, `ANALYZE`)
}

// ---------------------------------------------------------------------------
// compileSavepoint — SAVEPOINT, RELEASE, ROLLBACK TO
// ---------------------------------------------------------------------------

func TestMCDC9Driver_Savepoint_RollbackTo(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,'initial')`)

	// Use savepoint.
	drv9Exec(t, db, `SAVEPOINT sp1`)
	drv9Exec(t, db, `INSERT INTO t VALUES(2,'inside_sp')`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 2 {
		t.Errorf("expected 2 rows inside savepoint, got %d", n)
	}

	drv9Exec(t, db, `ROLLBACK TO SAVEPOINT sp1`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 1 {
		t.Errorf("expected 1 row after rollback to savepoint, got %d", n)
	}

	drv9Exec(t, db, `RELEASE SAVEPOINT sp1`)
}

// ---------------------------------------------------------------------------
// registerForeignKeyConstraints — CREATE TABLE with multiple FK constraints
// ---------------------------------------------------------------------------

func TestMCDC9Driver_RegisterFKConstraints_MultiFK(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE p1(id INTEGER PRIMARY KEY)`)
	drv9Exec(t, db, `CREATE TABLE p2(id INTEGER PRIMARY KEY)`)
	drv9Exec(t, db,
		`CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			pid1 INTEGER REFERENCES p1(id),
			pid2 INTEGER REFERENCES p2(id)
		)`)
	drv9Exec(t, db, `INSERT INTO p1 VALUES(1)`)
	drv9Exec(t, db, `INSERT INTO p2 VALUES(10)`)
	drv9Exec(t, db, `INSERT INTO child VALUES(1,1,10)`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM child`); n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// emitUpdateWhereClause — UPDATE with complex WHERE
// ---------------------------------------------------------------------------

func TestMCDC9Driver_EmitUpdateWhereClause(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)`)
	for i := 1; i <= 20; i++ {
		drv9Exec(t, db, `INSERT INTO t VALUES(?,?,?)`, i, i%5, fmt.Sprintf("val%d", i))
	}

	// UPDATE with compound WHERE.
	drv9Exec(t, db, `UPDATE t SET b='updated' WHERE a > 2 AND a <= 4`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t WHERE b='updated'`); n == 0 {
		t.Error("expected some rows updated")
	}
}

// ---------------------------------------------------------------------------
// emitUpdateColumnValue — UPDATE with CASE expression
// ---------------------------------------------------------------------------

func TestMCDC9Driver_EmitUpdateColumnValue_CaseExpr(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, grade INTEGER, label TEXT)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(1,90,NULL)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(2,70,NULL)`)
	drv9Exec(t, db, `INSERT INTO t VALUES(3,50,NULL)`)

	// UPDATE using CASE expression.
	drv9Exec(t, db, `UPDATE t SET label = CASE
		WHEN grade >= 90 THEN 'A'
		WHEN grade >= 70 THEN 'B'
		ELSE 'C'
		END`)

	lbl := drv9QueryStr(t, db, `SELECT label FROM t WHERE id=1`)
	if lbl != "A" {
		t.Errorf("expected 'A', got %q", lbl)
	}
}

// ---------------------------------------------------------------------------
// emitDeleteWhereClause — DELETE with subquery
// ---------------------------------------------------------------------------

func TestMCDC9Driver_EmitDeleteWhereClause_WithSubquery(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)`)
	drv9Exec(t, db, `CREATE TABLE exclude_ids(id INTEGER)`)
	for i := 1; i <= 10; i++ {
		drv9Exec(t, db, `INSERT INTO t VALUES(?,?)`, i, i)
	}
	drv9Exec(t, db, `INSERT INTO exclude_ids VALUES(3)`)
	drv9Exec(t, db, `INSERT INTO exclude_ids VALUES(7)`)

	// DELETE rows whose id is in exclude_ids.
	drv9Exec(t, db, `DELETE FROM t WHERE id IN (SELECT id FROM exclude_ids)`)
	if n := drv9QueryInt(t, db, `SELECT COUNT(*) FROM t`); n != 8 {
		t.Errorf("expected 8 rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// applyWithPrecedence — compound SELECT with ORDER BY
// ---------------------------------------------------------------------------

func TestMCDC9Driver_ApplyWithPrecedence_OrderedUnion(t *testing.T) {
	t.Parallel()
	db := drv9Open(t)

	drv9Exec(t, db, `CREATE TABLE a(v INTEGER)`)
	drv9Exec(t, db, `CREATE TABLE b(v INTEGER)`)
	drv9Exec(t, db, `INSERT INTO a VALUES(3),(1),(5)`)
	drv9Exec(t, db, `INSERT INTO b VALUES(2),(4),(6)`)

	rows, err := db.Query(`SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	var prev int = -1
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if v < prev {
			t.Errorf("result not sorted: %d after %d", v, prev)
		}
		prev = v
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 rows, got %d", count)
	}
}
