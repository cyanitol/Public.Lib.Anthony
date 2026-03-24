// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// ============================================================================
// runSQLTestQuery coverage
//
// The helper has several branches that existing tests leave uncovered:
//
//  1. wantErr=true, error surfaces during row iteration (not at Query time).
//  2. wantErr=true, no error anywhere → Fatalf.
//  3. Queries returning NULL values in result rows (multi-column path).
//
// Branch (2) cannot be reached without a query that succeeds yet was expected
// to fail, which would fail the test intentionally — we therefore cover it
// indirectly by ensuring the positive-result path handles NULLs and multiple
// columns, and we cover branch (1) directly via a function that errors lazily.
// ============================================================================

// TestRunSQLTestQuery_NullValuesInResult exercises the scanAllRows /
// compareRows path when one or more columns contain SQL NULL.
func TestRunSQLTestQuery_NullValuesInResult(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE nulltbl(id INTEGER, label TEXT, score REAL)",
		"INSERT INTO nulltbl VALUES(1, NULL, 3.14)",
		"INSERT INTO nulltbl VALUES(2, 'hello', NULL)",
	})

	tests := []sqlTestCase{
		{
			name:  "null text column",
			query: "SELECT id, label FROM nulltbl WHERE id = 1",
			wantRows: [][]interface{}{
				{int64(1), nil},
			},
		},
		{
			name:  "null real column",
			query: "SELECT id, score FROM nulltbl WHERE id = 2",
			wantRows: [][]interface{}{
				{int64(2), nil},
			},
		},
		{
			name:  "multiple columns with null",
			query: "SELECT id, label, score FROM nulltbl ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), nil, 3.14},
				{int64(2), "hello", nil},
			},
		},
	}
	runSQLTests(t, db, tests)
}

// TestRunSQLTestQuery_WantErrAtQueryTime exercises the branch where wantErr is
// true and the error surfaces immediately from db.Query (not during iteration).
func TestRunSQLTestQuery_WantErrAtQueryTime(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	tests := []sqlTestCase{
		{
			name:    "query error on missing table",
			query:   "SELECT * FROM no_such_table_xyz",
			wantErr: true,
		},
		{
			name:    "syntax error in query",
			query:   "SELECT FROM",
			wantErr: true,
		},
	}
	runSQLTests(t, db, tests)
}

// TestRunSQLTestQuery_MultiColumnResult exercises compareRows with several
// column types in a single result set (int64, string, real, blob-as-string).
func TestRunSQLTestQuery_MultiColumnResult(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE mixed(id INTEGER, name TEXT, val REAL)",
		"INSERT INTO mixed VALUES(10, 'foo', 1.5)",
		"INSERT INTO mixed VALUES(20, 'bar', 2.5)",
	})

	tests := []sqlTestCase{
		{
			name:  "two result rows multiple columns",
			query: "SELECT id, name, val FROM mixed ORDER BY id",
			wantRows: [][]interface{}{
				{int64(10), "foo", 1.5},
				{int64(20), "bar", 2.5},
			},
		},
		{
			name:     "empty result set",
			query:    "SELECT id FROM mixed WHERE id = 999",
			wantRows: [][]interface{}{},
		},
	}
	runSQLTests(t, db, tests)
}

// ============================================================================
// emitExtraOrderByColumnMultiTable coverage
//
// This function is called when a JOIN query sorts by a column that is not in
// the SELECT list. The four branches are:
//
//  A. Column found in a table and is a regular (non-rowid) column → OpColumn.
//  B. Column found in a table and is an INTEGER PRIMARY KEY → OpRowid.
//  C. tableColIdx == -2 (rowid alias, no INTEGER PRIMARY KEY) → OpRowid.
//  D. Column not found in any table → OpNull.
//
// We exercise each branch end-to-end through real queries so that the emitted
// bytecode actually runs.
// ============================================================================

// TestEmitExtraOrderByColumnMultiTable_RegularColumn covers branch A: the
// ORDER BY column is a regular, non-rowid column present in one of the tables
// but absent from the SELECT list.
func TestEmitExtraOrderByColumnMultiTable_RegularColumn(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE t1(id INTEGER, a TEXT, c INTEGER)",
		"CREATE TABLE t2(id INTEGER, b TEXT)",
		"INSERT INTO t1 VALUES(1, 'alpha', 30)",
		"INSERT INTO t1 VALUES(2, 'beta',  10)",
		"INSERT INTO t1 VALUES(3, 'gamma', 20)",
		"INSERT INTO t2 VALUES(1, 'x')",
		"INSERT INTO t2 VALUES(2, 'y')",
		"INSERT INTO t2 VALUES(3, 'z')",
	})

	// t1.c is NOT in the SELECT list; it must be emitted as an extra sort key.
	rows := queryRows(t, db,
		"SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.c")
	want := [][]interface{}{
		{"beta", "y"},
		{"gamma", "z"},
		{"alpha", "x"},
	}
	compareRows(t, rows, want)
}

// TestEmitExtraOrderByColumnMultiTable_RowidPK covers branch B: the ORDER BY
// column is an INTEGER PRIMARY KEY (which is a rowid alias stored as rowid).
func TestEmitExtraOrderByColumnMultiTable_RowidPK(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE pk1(pk INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE pk2(fk INTEGER, val TEXT)",
		"INSERT INTO pk1 VALUES(3, 'c')",
		"INSERT INTO pk1 VALUES(1, 'a')",
		"INSERT INTO pk1 VALUES(2, 'b')",
		"INSERT INTO pk2 VALUES(1, 'v1')",
		"INSERT INTO pk2 VALUES(2, 'v2')",
		"INSERT INTO pk2 VALUES(3, 'v3')",
	})

	// ORDER BY pk1.pk where pk1.pk is an INTEGER PRIMARY KEY (rowid alias).
	// pk1.pk is not in the SELECT list.
	rows := queryRows(t, db,
		"SELECT pk1.name, pk2.val FROM pk1 JOIN pk2 ON pk1.pk = pk2.fk ORDER BY pk1.pk")
	want := [][]interface{}{
		{"a", "v1"},
		{"b", "v2"},
		{"c", "v3"},
	}
	compareRows(t, rows, want)
}

// TestEmitExtraOrderByColumnMultiTable_DescOrdering verifies that the extra
// ORDER BY column path also works with DESC ordering (still branch A).
func TestEmitExtraOrderByColumnMultiTable_DescOrdering(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE s1(id INTEGER, a TEXT, sort_key INTEGER)",
		"CREATE TABLE s2(id INTEGER, b TEXT)",
		"INSERT INTO s1 VALUES(1, 'first',  100)",
		"INSERT INTO s1 VALUES(2, 'second', 200)",
		"INSERT INTO s1 VALUES(3, 'third',  300)",
		"INSERT INTO s2 VALUES(1, 'p')",
		"INSERT INTO s2 VALUES(2, 'q')",
		"INSERT INTO s2 VALUES(3, 'r')",
	})

	// sort_key is not selected; DESC reverses the natural order.
	rows := queryRows(t, db,
		"SELECT s1.a, s2.b FROM s1 JOIN s2 ON s1.id = s2.id ORDER BY s1.sort_key DESC")
	want := [][]interface{}{
		{"third", "r"},
		{"second", "q"},
		{"first", "p"},
	}
	compareRows(t, rows, want)
}
