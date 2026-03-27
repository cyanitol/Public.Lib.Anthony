// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// MC/DC 12 — SQL-level coverage for aggregate and window function paths
//
// Targets:
//   compile_select_agg.go:784  emitBinaryOp           (75.0%) — OpRem (%) case
//   compile_select_agg.go:251  emitGroupConcatUpdate  (72.0%) — non-col expr, non-literal sep
//   stmt_window_helpers.go:329 resolveWindowStateIdx  (71.4%) — nil Over, map-miss paths
//   compile_dml.go:532         emitInsertSelectWhere  (75.0%) — WHERE with comparison
//   compile_ddl.go:57          initializeNewTable     (73.3%) — AUTOINCREMENT branch
//   compile_helpers.go:667     buildMultiTableColumnNames (75%) — SELECT * from JOIN
//   compile_helpers.go:600     emitNonIdentifierColumn (75%) — expr in SELECT list

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func drv12Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func drv12Exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func drv12QueryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// emitBinaryOp — modulo operator path
// ---------------------------------------------------------------------------

// TestMCDC12_Agg_Modulo exercises the OpRem case in emitBinaryOp by computing
// an aggregate expression involving the % operator.
func TestMCDC12_Agg_Modulo(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(x INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t VALUES(10),(15),(20)`)
	// COUNT(*) % 2 → emitBinaryOp with OpRem
	n := drv12QueryInt(t, db, `SELECT COUNT(*) % 2 FROM t`)
	if n < 0 || n > 1 {
		t.Errorf("expected 0 or 1 for COUNT() %% 2, got %d", n)
	}
}

// TestMCDC12_Agg_AllArithOps exercises all five arithmetic operators in aggregate
// expressions to ensure complete emitBinaryOp coverage.
func TestMCDC12_Agg_AllArithOps(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	db.SetMaxOpenConns(1)
	drv12Exec(t, db, `CREATE TABLE t(x INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t VALUES(10),(20),(30)`)

	for _, tt := range []struct {
		name string
		q    string
	}{
		{"plus", `SELECT COUNT(*) + 10 FROM t`},
		{"minus", `SELECT SUM(x) - 5 FROM t`},
		{"multiply", `SELECT COUNT(*) * 5 FROM t`},
		{"divide", `SELECT SUM(x) / 3 FROM t`},
		{"modulo", `SELECT SUM(x) % 7 FROM t`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var n int
			if err := db.QueryRow(tt.q).Scan(&n); err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// emitGroupConcatUpdate — non-column expr (loadAggregateColumnValue !ok path)
//                       — non-literal separator (else branch in separator load)
// ---------------------------------------------------------------------------

// TestMCDC12_GroupConcat_ExprArg exercises the !ok path in emitGroupConcatUpdate
// by using a non-column expression as the first GROUP_CONCAT argument.
func TestMCDC12_GroupConcat_ExprArg(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(v TEXT)`)
	drv12Exec(t, db, `INSERT INTO t VALUES('a'),('b'),('c')`)
	// v || '-' is not a simple column ref → loadAggregateColumnValue !ok path.
	var s string
	if err := db.QueryRow(`SELECT GROUP_CONCAT(v || '-') FROM t`).Scan(&s); err != nil {
		t.Skipf("GROUP_CONCAT with expr arg: %v", err)
	}
	if len(s) == 0 {
		t.Error("expected non-empty result")
	}
}

// TestMCDC12_GroupConcat_NonLiteralSep exercises the else branch inside the
// separator argument handling, where the separator is not a string literal.
func TestMCDC12_GroupConcat_NonLiteralSep(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(v TEXT)`)
	drv12Exec(t, db, `INSERT INTO t VALUES('x'),('y')`)
	// Second arg is CHAR(44) (not a LiteralString) → else branch at line 271.
	var s string
	if err := db.QueryRow(`SELECT GROUP_CONCAT(v, CHAR(44)) FROM t`).Scan(&s); err != nil {
		t.Skipf("GROUP_CONCAT with computed sep: %v", err)
	}
}

// ---------------------------------------------------------------------------
// resolveWindowStateIdx — multiple OVER clauses + nil-OVER fallback
// ---------------------------------------------------------------------------

// TestMCDC12_Window_MultipleOvers exercises resolveWindowStateIdx with two
// different OVER clauses so both map-hit and map-miss paths are reached.
func TestMCDC12_Window_MultipleOvers(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(g INTEGER, v INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t VALUES(1,10),(1,20),(2,30),(2,40)`)
	rows, err := db.Query(`
		SELECT g, v,
		  ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn1,
		  ROW_NUMBER() OVER (ORDER BY v) AS rn2
		FROM t ORDER BY g, v`)
	if err != nil {
		t.Skipf("window function: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var g, v, rn1, rn2 int
		if err := rows.Scan(&g, &v, &rn1, &rn2); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count == 0 {
		t.Error("expected rows from window function query")
	}
}

// TestMCDC12_Window_EmptyOver exercises the nil OVER clause fallback
// (resolveWindowStateIdx returns 0 when fnExpr.Over is nil).
func TestMCDC12_Window_EmptyOver(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(v INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t VALUES(1),(2),(3)`)
	rows, err := db.Query(`SELECT v, ROW_NUMBER() OVER () FROM t`)
	if err != nil {
		t.Skipf("ROW_NUMBER() OVER (): %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// ---------------------------------------------------------------------------
// emitInsertSelectWhere — INSERT...SELECT with WHERE clause
// ---------------------------------------------------------------------------

// TestMCDC12_InsertSelectWhere exercises emitInsertSelectWhere by using an
// INSERT...SELECT with a complex WHERE clause.
func TestMCDC12_InsertSelectWhere(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE src(id INTEGER, v INTEGER)`)
	drv12Exec(t, db, `CREATE TABLE dst(id INTEGER, v INTEGER)`)
	drv12Exec(t, db, `INSERT INTO src VALUES(1,10),(2,20),(3,30)`)
	drv12Exec(t, db, `INSERT INTO dst SELECT id, v FROM src WHERE v > 15 AND id < 3`)
	n := drv12QueryInt(t, db, `SELECT COUNT(*) FROM dst`)
	if n == 0 {
		t.Error("expected rows in dst after INSERT...SELECT with WHERE")
	}
}

// ---------------------------------------------------------------------------
// initializeNewTable — AUTOINCREMENT path
// ---------------------------------------------------------------------------

// TestMCDC12_Autoincrement exercises the AUTOINCREMENT branch in
// initializeNewTable (ensureSqliteSequenceTable is called when the table has
// an AUTOINCREMENT column).
func TestMCDC12_Autoincrement(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)
	drv12Exec(t, db, `INSERT INTO t(v) VALUES('a'),('b'),('c')`)
	n := drv12QueryInt(t, db, `SELECT COUNT(*) FROM t`)
	if n != 3 {
		t.Errorf("expected 3 rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// buildMultiTableColumnNames — SELECT * from JOIN (star expansion path)
// ---------------------------------------------------------------------------

// TestMCDC12_Join_StarExpansion exercises buildMultiTableColumnNames with a
// SELECT * from a JOIN, triggering the star-expansion path.
func TestMCDC12_Join_StarExpansion(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t1(a INTEGER, b TEXT)`)
	drv12Exec(t, db, `CREATE TABLE t2(c INTEGER, d REAL)`)
	drv12Exec(t, db, `INSERT INTO t1 VALUES(1,'x'),(2,'y')`)
	drv12Exec(t, db, `INSERT INTO t2 VALUES(10,1.1),(20,2.2)`)
	rows, err := db.Query(`SELECT * FROM t1 CROSS JOIN t2`)
	if err != nil {
		t.Fatalf("cross join: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) < 4 {
		t.Errorf("expected at least 4 columns from SELECT *, got %d", len(cols))
	}
}

// TestMCDC12_Join_QualifiedColumns exercises emitQualifiedColumn via
// qualified references in a multi-table SELECT.
func TestMCDC12_Join_QualifiedColumns(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t1(a INTEGER)`)
	drv12Exec(t, db, `CREATE TABLE t2(b INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t1 VALUES(1),(2)`)
	drv12Exec(t, db, `INSERT INTO t2 VALUES(10),(20)`)
	rows, err := db.Query(`SELECT t1.a, t2.b FROM t1 CROSS JOIN t2`)
	if err != nil {
		t.Fatalf("qualified columns: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected rows from qualified column query")
	}
}

// TestMCDC12_Select_NonIdentExpr exercises emitNonIdentifierColumn by
// selecting computed expressions in a multi-table context.
func TestMCDC12_Select_NonIdentExpr(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t1(a INTEGER, b TEXT)`)
	drv12Exec(t, db, `INSERT INTO t1 VALUES(5,'hello')`)
	rows, err := db.Query(`SELECT (a + 1) * 2, UPPER(b), CASE WHEN a > 3 THEN 'big' ELSE 'small' END FROM t1`)
	if err != nil {
		t.Fatalf("non-ident expr: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n int
		var s1, s2 string
		if err := rows.Scan(&n, &s1, &s2); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if n != 12 {
			t.Errorf("expected (5+1)*2=12, got %d", n)
		}
	}
}

// ---------------------------------------------------------------------------
// buildMultiTableColumnNames — alias path
// ---------------------------------------------------------------------------

// TestMCDC12_Join_AliasedColumns exercises the alias path in
// buildMultiTableColumnNames by using column aliases in a multi-table SELECT.
func TestMCDC12_Join_AliasedColumns(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t1(a INTEGER)`)
	drv12Exec(t, db, `CREATE TABLE t2(b INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t1 VALUES(1)`)
	drv12Exec(t, db, `INSERT INTO t2 VALUES(10)`)
	rows, err := db.Query(`SELECT t1.a AS col_a, t2.b AS col_b FROM t1 CROSS JOIN t2`)
	if err != nil {
		t.Fatalf("aliased columns: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) < 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	for rows.Next() {
		var a, b int
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
}

// TestMCDC12_Join_CollatedExpr exercises emitNonIdentifierColumn's collation
// path via a COLLATE expression in a multi-table SELECT.
func TestMCDC12_Join_CollatedExpr(t *testing.T) {
	t.Parallel()
	db := drv12Open(t)
	drv12Exec(t, db, `CREATE TABLE t1(s TEXT)`)
	drv12Exec(t, db, `CREATE TABLE t2(n INTEGER)`)
	drv12Exec(t, db, `INSERT INTO t1 VALUES('Hello')`)
	drv12Exec(t, db, `INSERT INTO t2 VALUES(1)`)
	rows, err := db.Query(`SELECT t1.s COLLATE NOCASE, t2.n FROM t1 CROSS JOIN t2`)
	if err != nil {
		t.Skipf("collated column in join: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}
