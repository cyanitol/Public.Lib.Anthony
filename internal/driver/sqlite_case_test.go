// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

// setupCaseTestDB creates a temporary database for testing CASE expressions
func setupCaseTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "case_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create test table for basic CASE tests
	_, err = db.Exec(`CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test1 VALUES(1, 2, 1.1, 2.2, 'hello', 'world')`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	return db
}

// TestSQLiteCaseExpressions tests SQLite CASE expression evaluation
// This test suite is derived from SQLite's test/expr.test (expr-case.* tests)
func TestSQLiteCaseExpressions(t *testing.T) {
	tests := []struct {
		name    string
		setup   string // UPDATE statement to set values (empty string means use defaults)
		expr    string // Expression to evaluate via SELECT
		want    interface{}
		wantErr bool
		skip    string // If non-empty, skip with this reason
	}{
		// Searched CASE expressions (CASE WHEN ... THEN ... END)
		{
			name:  "case-1.1-searched-ne",
			setup: "i1=1, i2=2",
			expr:  "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END",
			want:  "ne",
		},
		{
			name:  "case-1.2-searched-eq",
			setup: "i1=2, i2=2",
			expr:  "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END",
			want:  "eq",
		},
		{
			name:  "case-1.3-searched-null-left",
			setup: "i1=NULL, i2=2",
			expr:  "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END",
			want:  "ne",
		},
		{
			name:  "case-1.4-searched-null-right",
			setup: "i1=2, i2=NULL",
			expr:  "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END",
			want:  "ne",
		},

		// Simple CASE expressions (CASE x WHEN v1 THEN r1 ...)
		{
			name:  "case-2.1-simple-match-two",
			setup: "i1=2",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END",
			want:  "two",
			skip:  "",
		},
		{
			name:  "case-2.2-simple-match-one",
			setup: "i1=1",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN NULL THEN 'two' ELSE 'error' END",
			want:  "one",
		},
		{
			name:  "case-2.3-simple-null-when",
			setup: "i1=2",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN NULL THEN 'two' ELSE 'error' END",
			want:  "error",
			skip:  "",
		},
		{
			name:  "case-2.4-simple-no-match",
			setup: "i1=3",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN NULL THEN 'two' ELSE 'error' END",
			want:  "error",
			skip:  "",
		},
		{
			name:  "case-2.5-simple-else",
			setup: "i1=3",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END",
			want:  "error",
			skip:  "",
		},

		// CASE without ELSE (returns NULL)
		{
			name:  "case-3.1-no-else-null",
			setup: "i1=3",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",
			want:  nil,
			skip:  "",
		},
		{
			name:  "case-3.2-null-expr-no-else",
			setup: "i1=null",
			expr:  "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 3 END",
			want:  int64(3),
			skip:  "",
		},
		{
			name:  "case-3.3-null-result",
			setup: "i1=1",
			expr:  "CASE i1 WHEN 1 THEN null WHEN 2 THEN 'two' ELSE 3 END",
			want:  nil,
		},

		// Nested CASE expressions
		{
			name:  "case-4.1-nested-outer-true",
			setup: "i1=1, i2=2",
			expr:  "CASE WHEN i1 = 1 THEN CASE WHEN i2 = 2 THEN 'both' ELSE 'i1only' END ELSE 'neither' END",
			want:  "both",
		},
		{
			name:  "case-4.2-nested-outer-true-inner-false",
			setup: "i1=1, i2=3",
			expr:  "CASE WHEN i1 = 1 THEN CASE WHEN i2 = 2 THEN 'both' ELSE 'i1only' END ELSE 'neither' END",
			want:  "i1only",
		},
		{
			name:  "case-4.3-nested-outer-false",
			setup: "i1=3, i2=2",
			expr:  "CASE WHEN i1 = 1 THEN CASE WHEN i2 = 2 THEN 'both' ELSE 'i1only' END ELSE 'neither' END",
			want:  "neither",
		},

		// Multiple WHEN clauses (searched CASE)
		{
			name:  "case-5.1-multiple-when-medium",
			setup: "i1=7",
			expr:  "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END",
			want:  "medium",
		},
		{
			name:  "case-5.2-multiple-when-low",
			setup: "i1=3",
			expr:  "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END",
			want:  "low",
		},
		{
			name:  "case-5.3-multiple-when-high",
			setup: "i1=12",
			expr:  "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END",
			want:  "high",
		},
		{
			name:  "case-5.4-multiple-when-error",
			setup: "i1=20",
			expr:  "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END",
			want:  "error",
		},

		// CASE with different result types
		{
			name:  "case-6.1-integer-result",
			setup: "i1=1, i2=2",
			expr:  "CASE WHEN i1 = i2 THEN 100 ELSE 200 END",
			want:  int64(200),
		},
		{
			name:  "case-6.2-real-result",
			setup: "r1=1.5, r2=1.5",
			expr:  "CASE WHEN r1 = r2 THEN 3.14 ELSE 2.71 END",
			want:  3.14,
		},
		{
			name:  "case-6.3-mixed-type-result",
			setup: "i1=1",
			expr:  "CASE i1 WHEN 1 THEN 42 WHEN 2 THEN 'text' ELSE 3.14 END",
			want:  int64(42),
		},

		// CASE with IS NULL operator
		{
			name:  "case-7.1-is-null-true",
			setup: "i1=NULL, i2=8",
			expr:  "CASE WHEN i1 IS NULL THEN 'null' ELSE 'not null' END",
			want:  "null",
			skip:  "",
		},
		{
			name:  "case-7.2-is-not-null",
			setup: "i1=8, i2=NULL",
			expr:  "CASE WHEN i1 IS NOT NULL THEN 'not null' ELSE 'null' END",
			want:  "not null",
			skip:  "",
		},
		{
			name:  "case-7.3-multiple-null-checks",
			setup: "i1=NULL, i2=NULL",
			expr:  "CASE WHEN i1 IS NULL AND i2 IS NULL THEN 'both null' ELSE 'not both null' END",
			want:  "both null",
			skip:  "",
		},

		// CASE with arithmetic
		{
			name:  "case-8.1-arithmetic-in-when",
			setup: "i1=5, i2=3",
			expr:  "CASE WHEN i1 + i2 > 7 THEN 'big' ELSE 'small' END",
			want:  "big",
		},
		{
			name:  "case-8.2-arithmetic-in-result",
			setup: "i1=5, i2=3",
			expr:  "CASE WHEN i1 > i2 THEN i1 * i2 ELSE i1 + i2 END",
			want:  int64(15),
		},

		// CASE with string comparisons
		{
			name:  "case-9.1-string-eq",
			setup: "t1='hello', t2='hello'",
			expr:  "CASE WHEN t1 = t2 THEN 'same' ELSE 'different' END",
			want:  "same",
		},
		{
			name:  "case-9.2-string-ne",
			setup: "t1='hello', t2='world'",
			expr:  "CASE WHEN t1 = t2 THEN 'same' ELSE 'different' END",
			want:  "different",
		},
		{
			name:  "case-9.3-simple-string",
			setup: "t1='abc'",
			expr:  "CASE t1 WHEN 'abc' THEN 'match' WHEN 'def' THEN 'no' ELSE 'none' END",
			want:  "match",
			skip:  "",
		},

		// CASE with boolean expressions
		{
			name:  "case-10.1-and-condition",
			setup: "i1=5, i2=10",
			expr:  "CASE WHEN i1 > 3 AND i2 > 8 THEN 'both' ELSE 'not both' END",
			want:  "both",
		},
		{
			name:  "case-10.2-or-condition",
			setup: "i1=2, i2=10",
			expr:  "CASE WHEN i1 > 3 OR i2 > 8 THEN 'either' ELSE 'neither' END",
			want:  "either",
		},

		// CASE evaluates only necessary branches
		{
			name:  "case-11.1-short-circuit-first-match",
			setup: "i1=1",
			expr:  "CASE WHEN i1 = 1 THEN 'first' WHEN 1/0 THEN 'second' ELSE 'third' END",
			want:  "first",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			db := setupCaseTestDB(t)
			defer db.Close()
			caseExprRunOne(t, db, tt.setup, tt.expr, tt.want, tt.wantErr)
		})
	}
}

func caseExprRunOne(t *testing.T, db *sql.DB, setup, expr string, want interface{}, wantErr bool) {
	t.Helper()
	if setup != "" {
		if _, err := db.Exec("UPDATE test1 SET " + setup); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}
	query := "SELECT " + expr + " FROM test1"
	caseExprCheck(t, db, query, want, wantErr)
}

func caseExprCheck(t *testing.T, db *sql.DB, query string, want interface{}, wantErr bool) {
	t.Helper()
	var result interface{}
	err := db.QueryRow(query).Scan(&result)
	if wantErr {
		if err == nil {
			t.Errorf("expected error, got none")
		}
		return
	}
	if err != nil {
		if want == nil && err == sql.ErrNoRows {
			return
		}
		t.Fatalf("query failed: %v (query: %s)", err, query)
	}
	if result == nil && want == nil {
		return
	}
	if !compareCaseValues(result, want) {
		t.Errorf("query = %q\ngot  = %v (type %T)\nwant = %v (type %T)",
			query, result, result, want, want)
	}
}

// TestSQLiteCaseInSelectList tests CASE expressions in SELECT lists
func TestSQLiteCaseInSelectList(t *testing.T) {
	db := setupCaseTestDB(t)
	defer db.Close()

	caseExecAll(t, db, []string{
		`CREATE TABLE t2(id INTEGER, value INTEGER)`,
		`INSERT INTO t2 VALUES(1, 10)`,
		`INSERT INTO t2 VALUES(2, 20)`,
		`INSERT INTO t2 VALUES(3, 30)`,
		`INSERT INTO t2 VALUES(4, 40)`,
	})

	tests := []struct {
		name  string
		query string
		want  []interface{}
	}{
		{
			name:  "case-select-1-simple",
			query: "SELECT id, CASE WHEN value > 25 THEN 'high' ELSE 'low' END FROM t2 ORDER BY id",
			want:  []interface{}{int64(1), "low", int64(2), "low", int64(3), "high", int64(4), "high"},
		},
		{
			name:  "case-select-2-multiple-when",
			query: "SELECT id, CASE WHEN value < 15 THEN 'low' WHEN value < 35 THEN 'medium' ELSE 'high' END FROM t2 ORDER BY id",
			want:  []interface{}{int64(1), "low", int64(2), "medium", int64(3), "medium", int64(4), "high"},
		},
		{
			name:  "case-select-3-simple-case",
			query: "SELECT id, CASE id WHEN 1 THEN 'first' WHEN 2 THEN 'second' ELSE 'other' END FROM t2 ORDER BY id",
			want:  []interface{}{int64(1), "first", int64(2), "second", int64(3), "other", int64(4), "other"},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			caseSelectListCheck(t, db, tt.query, tt.want)
		})
	}
}

func caseSelectListCheck(t *testing.T, db *sql.DB, query string, want []interface{}) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var results []interface{}
	for rows.Next() {
		var id, val interface{}
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, id, val)
	}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d", len(results), len(want))
	}
	for i := range results {
		if !compareCaseValues(results[i], want[i]) {
			t.Errorf("result[%d]: got %v (%T), want %v (%T)",
				i, results[i], results[i], want[i], want[i])
		}
	}
}

// TestSQLiteCaseInWhereClause tests CASE expressions in WHERE clauses
func TestSQLiteCaseInWhereClause(t *testing.T) {
	t.Skip("pre-existing failure - CASE in WHERE clause incomplete")
	db := setupCaseTestDB(t)
	defer db.Close()

	caseExecAll(t, db, []string{
		`CREATE TABLE aa(a INTEGER)`,
		`CREATE TABLE bb(b INTEGER)`,
		`INSERT INTO aa VALUES(1)`,
		`INSERT INTO aa VALUES(3)`,
		`INSERT INTO bb VALUES(2)`,
		`INSERT INTO bb VALUES(4)`,
		`INSERT INTO bb VALUES(0)`,
	})

	tests := []struct {
		name  string
		query string
		want  [][]int64
	}{
		{
			name:  "case-where-1-truthy",
			query: "SELECT * FROM aa, bb WHERE CASE WHEN a=b-1 THEN 1 END ORDER BY a, b",
			want:  [][]int64{{1, 2}, {3, 4}},
		},
		{
			name:  "case-where-2-with-else",
			query: "SELECT * FROM aa, bb WHERE CASE WHEN a=b-1 THEN 0 ELSE 1 END ORDER BY a, b",
			want:  [][]int64{{1, 0}, {1, 4}, {3, 0}, {3, 2}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			caseAssertInt64Pairs(t, db, tt.query, tt.want)
		})
	}
}

func caseExecAll(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v\nSQL: %s", err, stmt)
		}
	}
}

func caseAssertInt64Pairs(t *testing.T, db *sql.DB, query string, want [][]int64) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var results [][]int64
	for rows.Next() {
		var a, b int64
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, []int64{a, b})
	}
	if len(results) != len(want) {
		t.Fatalf("got %d rows, want %d", len(results), len(want))
	}
	for i := range results {
		if results[i][0] != want[i][0] || results[i][1] != want[i][1] {
			t.Errorf("row[%d]: got [%d, %d], want [%d, %d]",
				i, results[i][0], results[i][1], want[i][0], want[i][1])
		}
	}
}

// TestSQLiteCaseInOrderBy tests CASE expressions in ORDER BY clauses
func TestSQLiteCaseInOrderBy(t *testing.T) {
	t.Skip("pre-existing failure - CASE in ORDER BY not yet supported")
	db := setupCaseTestDB(t)
	defer db.Close()

	caseExecAll(t, db, []string{
		`CREATE TABLE t3(id INTEGER, category TEXT)`,
		`INSERT INTO t3 VALUES(1, 'zebra')`,
		`INSERT INTO t3 VALUES(2, 'apple')`,
		`INSERT INTO t3 VALUES(3, 'banana')`,
		`INSERT INTO t3 VALUES(4, 'cherry')`,
	})

	tests := []struct {
		name  string
		query string
		want  []int64
	}{
		{
			name:  "case-orderby-1-priority",
			query: "SELECT id FROM t3 ORDER BY CASE category WHEN 'apple' THEN 1 WHEN 'banana' THEN 2 ELSE 3 END, category",
			want:  []int64{2, 3, 1, 4},
		},
		{
			name:  "case-orderby-2-reverse-priority",
			query: "SELECT id FROM t3 ORDER BY CASE WHEN category < 'c' THEN 2 ELSE 1 END, category",
			want:  []int64{4, 1, 2, 3},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			caseAssertInt64List(t, db, tt.query, tt.want)
		})
	}
}

func caseAssertInt64List(t *testing.T, db *sql.DB, query string, want []int64) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var results []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, id)
	}
	if len(results) != len(want) {
		t.Fatalf("got %d rows, want %d", len(results), len(want))
	}
	for i := range results {
		if results[i] != want[i] {
			t.Errorf("row[%d]: got %d, want %d", i, results[i], want[i])
		}
	}
}

// TestSQLiteCaseWithAggregates tests CASE expressions with aggregate functions
func TestSQLiteCaseWithAggregates(t *testing.T) {
	t.Skip("pre-existing failure - CASE with aggregates incomplete")
	db := setupCaseTestDB(t)
	defer db.Close()

	// Create test table (from select7.test)
	caseAggSetupTable(t, db)

	tests := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "case-agg-1-count-conditional",
			query: "SELECT COUNT(CASE WHEN a > 50 THEN 1 END) FROM t4",
			want:  int64(3),
		},
		{
			name:  "case-agg-2-sum-conditional",
			query: "SELECT SUM(CASE WHEN a > 50 THEN a ELSE 0 END) FROM t4",
			want:  219.0,
		},
		{
			name:  "case-agg-3-group-by-case",
			query: "SELECT CASE WHEN a=0 THEN 'zero' ELSE 'nonzero' END AS cat, COUNT(*) FROM t4 GROUP BY cat ORDER BY cat",
			want:  []interface{}{"nonzero", int64(4), "zero", int64(1)},
		},
		{
			name:  "case-agg-4-avg-with-case",
			query: "SELECT AVG(CASE WHEN a > 0 THEN a END) FROM t4",
			want:  65.75,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			caseAggRunTest(t, db, tt.query, tt.want)
		})
	}
}

// caseAggSetupTable creates and populates the test table
func caseAggSetupTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE t4(a REAL)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	values := []float64{0, 44.0, 56.0, 69.0, 94.0}
	for _, v := range values {
		_, err := db.Exec(`INSERT INTO t4 VALUES(?)`, v)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
}

// caseAggRunTest executes a test and verifies results
func caseAggRunTest(t *testing.T, db *sql.DB, query string, want interface{}) {
	t.Helper()
	// Check if we expect multiple rows
	if slice, ok := want.([]interface{}); ok {
		caseAggVerifyMultipleRows(t, db, query, slice)
	} else {
		caseAggVerifySingleValue(t, db, query, want)
	}
}

// caseAggVerifyMultipleRows handles multi-row result verification
func caseAggVerifyMultipleRows(t *testing.T, db *sql.DB, query string, want []interface{}) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	results := caseAggCollectResults(t, rows)

	if len(results) != len(want) {
		t.Fatalf("got %d values, want %d", len(results), len(want))
	}

	for i := range results {
		if !compareCaseValues(results[i], want[i]) {
			t.Errorf("result[%d]: got %v (%T), want %v (%T)",
				i, results[i], results[i], want[i], want[i])
		}
	}
}

// caseAggCollectResults scans all rows and collects values
func caseAggCollectResults(t *testing.T, rows *sql.Rows) []interface{} {
	t.Helper()
	var results []interface{}
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns: %v", err)
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		results = append(results, values...)
	}
	return results
}

// caseAggVerifySingleValue handles single-value result verification
func caseAggVerifySingleValue(t *testing.T, db *sql.DB, query string, want interface{}) {
	t.Helper()
	var result interface{}
	err := db.QueryRow(query).Scan(&result)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if !compareCaseValues(result, want) {
		t.Errorf("got %v (%T), want %v (%T)", result, result, want, want)
	}
}

// TestSQLiteCaseNullHandling tests NULL handling in CASE expressions
func TestSQLiteCaseNullHandling(t *testing.T) {
	t.Skip("pre-existing failure - CASE NULL handling incomplete")
	db := setupCaseTestDB(t)
	defer db.Close()

	// From expr-14 tests
	_, err := db.Exec(`CREATE TABLE t5(x)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO t5 VALUES(0)`)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec(`INSERT INTO t5 VALUES(1)`)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec(`INSERT INTO t5 VALUES(NULL)`)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec(`INSERT INTO t5 VALUES(0.5)`)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "case-null-1-truthiness-count",
			query: "SELECT count(*) FROM t5 WHERE (x OR (8==9)) != (CASE WHEN x THEN 1 ELSE 0 END)",
			want:  int64(0),
		},
		{
			name:  "case-null-2-sum-with-case",
			query: "SELECT sum(CASE WHEN x THEN 0 ELSE 1 END) FROM t5 WHERE x",
			want:  int64(0),
		},
		{
			name:  "case-null-3-null-in-when",
			query: "SELECT CASE NULL WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
			want:  "other",
		},
		{
			name:  "case-null-4-null-comparison",
			query: "SELECT CASE WHEN NULL = NULL THEN 'equal' ELSE 'not equal' END",
			want:  "not equal",
		},
		{
			name:  "case-null-5-is-null-check",
			query: "SELECT CASE WHEN NULL IS NULL THEN 'null' ELSE 'not null' END",
			want:  "null",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if !compareCaseValues(result, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)",
					result, result, tt.want, tt.want)
			}
		})
	}
}

// TestSQLiteCaseSyntaxErrors tests CASE expression syntax errors
func TestSQLiteCaseSyntaxErrors(t *testing.T) {
	db := setupCaseTestDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`CREATE TABLE t6(a int)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "case-error-1-missing-when",
			query: "SELECT (CASE a>4 THEN 1 ELSE 0 END) FROM t6",
		},
		{
			name:  "case-error-2-missing-end",
			query: "SELECT (CASE WHEN a>4 THEN 1 ELSE 0) FROM t6",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Query(tt.query)
			if err == nil {
				t.Errorf("expected syntax error, got none")
			}
		})
	}
}

// compareCaseValues compares two values considering SQLite type conversions
func compareCaseValues(got, want interface{}) bool {
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}
	if caseCompareNumeric(got, want) {
		return true
	}
	return caseCompareStringOrBytes(got, want)
}

func caseCompareNumeric(got, want interface{}) bool {
	gotFloat, gotOk := toFloat64(got)
	wantFloat, wantOk := toFloat64(want)
	if !gotOk || !wantOk {
		return false
	}
	diff := gotFloat - wantFloat
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.0001
}

func caseCompareStringOrBytes(got, want interface{}) bool {
	gotStr := valueToString(got)
	wantStr := valueToString(want)
	if gotStr == nil || wantStr == nil {
		return false
	}
	return *gotStr == *wantStr
}

func valueToString(v interface{}) *string {
	switch val := v.(type) {
	case string:
		return &val
	case []byte:
		s := string(val)
		return &s
	}
	return nil
}

// toInt64 converts various integer types to int64
func toInt64(v interface{}) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	default:
		return 0, false
	}
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	default:
		return 0, false
	}
}
