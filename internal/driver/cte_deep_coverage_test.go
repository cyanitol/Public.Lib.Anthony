// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"testing"
)

// TestCTEDeep_MultiUse exercises a CTE referenced more than once in the main
// query (using it twice in subqueries), pushing the inlining paths.
func TestCTEDeep_MultiUse(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	// Reference the CTE twice: once as a scalar subquery in SELECT and once in WHERE.
	got := csInt(t, db,
		`WITH nums AS (SELECT 3 AS n)
		SELECT (SELECT n FROM nums) + (SELECT n FROM nums)`)
	if got != 6 {
		t.Errorf("want 6, got %d", got)
	}
}

// TestCTEDeep_CollateInBody exercises rewriteCollateExpr when a COLLATE clause
// appears inside the CTE body's WHERE predicate.
func TestCTEDeep_CollateInBody(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
	cscExec(t, db, "INSERT INTO items VALUES(1,'apple'),(2,'Banana'),(3,'cherry')")
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT name FROM items WHERE name > 'a' COLLATE NOCASE)
		SELECT name FROM cte ORDER BY name`)
	if len(rows) == 0 {
		t.Fatal("expected rows from CTE with COLLATE in WHERE")
	}
}

// TestCTEDeep_CollateInColumn exercises rewriteCollateExpr when a COLLATE clause
// appears on a column expression in the CTE SELECT list.
func TestCTEDeep_CollateInColumn(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE names(name TEXT)")
	cscExec(t, db, "INSERT INTO names VALUES('Zoo'),('apple'),('Mango')")
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT name COLLATE NOCASE AS n FROM names)
		SELECT n FROM cte ORDER BY n`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestCTEDeep_RecursiveAccumulator exercises the recursive CTE path including
// buildSimpleAddrMap via the inlineMainQueryBytecode call chain.
func TestCTEDeep_RecursiveAccumulator(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		`WITH RECURSIVE counter(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM counter WHERE n < 20
		)
		SELECT SUM(n) FROM counter`)
	// SUM(1..20) = 210
	if got != 210 {
		t.Errorf("want 210, got %d", got)
	}
}

// TestCTEDeep_AggregateHaving exercises compileCTEPopulation path with
// GROUP BY + HAVING inside the CTE body.
func TestCTEDeep_AggregateHaving(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE employees(dept TEXT, salary INTEGER)")
	cscExec(t, db, "INSERT INTO employees VALUES('eng',80000),('eng',90000),('hr',70000),('hr',72000),('ceo',200000)")
	rows := queryCSRows(t, db,
		`WITH dept_stats AS (
			SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
			FROM employees
			GROUP BY dept
			HAVING cnt > 1
		)
		SELECT dept FROM dept_stats WHERE avg_sal > 75000 ORDER BY dept`)
	if len(rows) == 0 {
		t.Fatal("expected rows from CTE with GROUP BY + HAVING + WHERE on avg")
	}
	// 'eng' average is 85000 > 75000; 'hr' average is 71000 < 75000
	if rows[0][0] != "eng" {
		t.Errorf("want eng, got %v", rows[0][0])
	}
}

// TestCTEDeep_FilterWithCTE exercises a CTE used as a WHERE filter source,
// covering the CTE materialization and main query lookup paths.
func TestCTEDeep_FilterWithCTE(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, active INTEGER)")
	cscExec(t, db, "INSERT INTO items VALUES(1,'alpha',1),(2,'beta',0),(3,'gamma',1)")
	// Use CTE as simple materialized filter rather than IN subquery.
	rows := queryCSRows(t, db,
		`WITH active AS (SELECT id, name FROM items WHERE active = 1)
		SELECT name FROM active ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "alpha" || rows[1][0] != "gamma" {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// TestCTEDeep_ComplexOrderByWhere exercises a CTE with ORDER BY and WHERE in
// the outer query (ROW_NUMBER window function may not be available, so we use
// a ranked-like CTE with explicit row numbering via recursive CTE).
func TestCTEDeep_ComplexOrderByWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE scores(id INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO scores VALUES(1,30),(2,10),(3,20),(4,40),(5,50)")
	rows := queryCSRows(t, db,
		`WITH ranked AS (SELECT id, val FROM scores ORDER BY val)
		SELECT id, val FROM ranked WHERE val <= 30 ORDER BY val`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows (val 10,20,30), got %d", len(rows))
	}
}

// TestCTEDeep_RecursiveFibonacciLong exercises the recursive CTE execution
// path with multiple columns to stress the recursive member inlining.
func TestCTEDeep_RecursiveFibonacciLong(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		`WITH RECURSIVE fib(a, b) AS (
			SELECT 0, 1
			UNION ALL
			SELECT b, a + b FROM fib WHERE b < 1000
		)
		SELECT COUNT(*) FROM fib`)
	if got == 0 {
		t.Error("expected nonzero count from recursive fibonacci CTE")
	}
}

// TestCTEDeep_NestedChainedCollate exercises rewriteCollateExpr in a chained
// CTE where an intermediate CTE has a COLLATE expression.
func TestCTEDeep_NestedChainedCollate(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE words(w TEXT)")
	cscExec(t, db, "INSERT INTO words VALUES('Hello'),('world'),('Go')")
	rows := queryCSRows(t, db,
		`WITH
			lower_words AS (SELECT w COLLATE NOCASE AS lw FROM words),
			sorted AS (SELECT lw FROM lower_words)
		SELECT lw FROM sorted ORDER BY lw`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestCTEDeep_RecursiveCounter10 exercises compileRecursiveCTE with a plain
// counter to 10, verifying the recursive loop and accumulator paths.
func TestCTEDeep_RecursiveCounter10(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH RECURSIVE cnt(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM cnt WHERE n < 10
		)
		SELECT n FROM cnt ORDER BY n`)
	if len(rows) != 10 {
		t.Fatalf("want 10 rows, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		if rows[i][0] != want {
			t.Errorf("row %d: want %d, got %v", i, want, rows[i][0])
		}
	}
}

// TestCTEDeep_MultiUseSameExpr exercises a CTE referenced in both the SELECT
// and WHERE of the outer query.
func TestCTEDeep_MultiUseSameExpr(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH base AS (SELECT 5 AS v)
		SELECT v FROM base WHERE v = (SELECT v FROM base)`)
	if len(rows) != 1 || rows[0][0] != int64(5) {
		t.Errorf("want single row with v=5, got %v", rows)
	}
}

// TestCTEDeep_CollateNocase exercises the COLLATE NOCASE rewrite path where
// the collation appears in the CTE body expression (not just ORDER BY).
func TestCTEDeep_CollateNocase(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE strings(s TEXT)")
	cscExec(t, db, "INSERT INTO strings VALUES('ABC'),('abc'),('xyz')")
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT s FROM strings WHERE s = 'abc' COLLATE NOCASE)
		SELECT s FROM cte ORDER BY s`)
	// Both 'ABC' and 'abc' match with NOCASE
	if len(rows) < 1 {
		t.Fatal("expected at least one match with COLLATE NOCASE")
	}
}
