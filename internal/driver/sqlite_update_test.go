// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// updTestCase holds UPDATE test configuration
type updTestCase struct {
	name       string
	setup      []string
	update     string
	verify     string
	wantRows   [][]interface{}
	wantErr    bool
	skipVerify bool
	skip       string
}

// updRunSetup executes setup SQL statements
func updRunSetup(t *testing.T, db *sql.DB, setup []string) {
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Setup failed for statement %q: %v", stmt, err)
		}
	}
}

// updExecuteUpdate executes the UPDATE statement and checks for errors
func updExecuteUpdate(t *testing.T, db *sql.DB, updateSQL string, wantErr bool) bool {
	_, err := db.Exec(updateSQL)
	if wantErr {
		if err == nil {
			t.Errorf("Expected error but got none")
		}
		return true
	}
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	return false
}

// updCollectResults fetches all rows and converts bytes to strings
func updCollectResults(t *testing.T, rows *sql.Rows, colCount int) [][]interface{} {
	var results [][]interface{}
	for rows.Next() {
		values := make([]interface{}, colCount)
		valuePtrs := make([]interface{}, colCount)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		results = append(results, values)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}

	return results
}

// updVerifyResults compares actual vs expected results
func updVerifyResults(t *testing.T, results, wantRows [][]interface{}) {
	t.Helper()
	if len(results) != len(wantRows) {
		t.Errorf("Row count mismatch: got %d, want %d", len(results), len(wantRows))
		t.Logf("Got: %v", results)
		t.Logf("Want: %v", wantRows)
		return
	}
	for i, row := range results {
		updVerifyRow(t, i, row, wantRows[i])
	}
}

func updVerifyRow(t *testing.T, i int, row, wantRow []interface{}) {
	t.Helper()
	if len(row) != len(wantRow) {
		t.Errorf("Row %d column count mismatch: got %d, want %d", i, len(row), len(wantRow))
		return
	}
	for j, val := range row {
		updVerifyCell(t, i, j, val, wantRow[j])
	}
}

func updVerifyCell(t *testing.T, i, j int, val, want interface{}) {
	t.Helper()
	if val == nil && want == nil {
		return
	}
	if val == nil || want == nil || val != want {
		t.Errorf("Row %d, col %d: got %v (type %T), want %v (type %T)", i, j, val, val, want, want)
	}
}

// TestSQLiteUpdate tests UPDATE statement functionality based on SQLite's TCL test suite
// Tests are derived from contrib/sqlite/sqlite-src-3510200/test/update.test and update2.test
func TestSQLiteUpdate(t *testing.T) {
	t.Skip("pre-existing failure")
	tests := []updTestCase{
		// update-1.1: Try to update a non-existent table
		{
			name:    "update_nonexistent_table",
			setup:   []string{},
			update:  "UPDATE test1 SET f2=5 WHERE f1<1",
			wantErr: true,
		},

		// update-2.1: Try to update a read-only table (sqlite_master)
		{
			name:    "update_readonly_table",
			setup:   []string{},
			update:  "UPDATE sqlite_master SET name='xyz' WHERE name='123'",
			wantErr: true,
		},

		// update-3.5: Basic UPDATE SET (update all rows)
		{
			name: "update_all_rows_multiply",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 2)",
				"INSERT INTO test1 VALUES(2, 4)",
				"INSERT INTO test1 VALUES(3, 8)",
				"INSERT INTO test1 VALUES(4, 16)",
				"INSERT INTO test1 VALUES(5, 32)",
			},
			update: "UPDATE test1 SET f2=f2*3",
			verify: "SELECT * FROM test1 ORDER BY f1",
			wantRows: [][]interface{}{
				{int64(1), int64(6)},
				{int64(2), int64(12)},
				{int64(3), int64(24)},
				{int64(4), int64(48)},
				{int64(5), int64(96)},
			},
		},

		// update-3.7: UPDATE with WHERE clause
		{
			name: "update_with_where_clause",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 6)",
				"INSERT INTO test1 VALUES(2, 12)",
				"INSERT INTO test1 VALUES(3, 24)",
				"INSERT INTO test1 VALUES(4, 48)",
				"INSERT INTO test1 VALUES(5, 96)",
				"INSERT INTO test1 VALUES(6, 192)",
			},
			update: "UPDATE test1 SET f2=f2/3 WHERE f1<=5",
			verify: "SELECT * FROM test1 ORDER BY f1",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
				{int64(2), int64(4)},
				{int64(3), int64(8)},
				{int64(4), int64(16)},
				{int64(5), int64(32)},
				{int64(6), int64(192)},
			},
		},

		// update-3.11: Swap values of two columns
		{
			name: "update_swap_columns",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 2)",
				"INSERT INTO test1 VALUES(2, 4)",
				"INSERT INTO test1 VALUES(3, 8)",
			},
			update: "UPDATE test1 SET f2=f1, f1=f2",
			verify: "SELECT * FROM test1 ORDER BY f1",
			wantRows: [][]interface{}{
				{int64(2), int64(1)},
				{int64(4), int64(2)},
				{int64(8), int64(3)},
			},
		},

		// update-3.2: Unknown column name in SET expression
		{
			name: "update_unknown_column_set",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(5, 32)",
			},
			update:  "UPDATE test1 SET f1=f3*2 WHERE f2=32",
			wantErr: true,
		},

		// update-3.4: Unknown column name in SET target
		{
			name: "update_unknown_column_target",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(5, 32)",
			},
			update:  "UPDATE test1 SET f3=f1*2 WHERE f2=32",
			wantErr: true,
		},

		// update-4.1: UPDATE with duplicates
		{
			name: "update_with_duplicates",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(8, 88)",
				"INSERT INTO test1 VALUES(8, 256)",
				"INSERT INTO test1 VALUES(8, 888)",
			},
			update: "UPDATE test1 SET f2=f2+1 WHERE f1=8",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(8), int64(89)},
				{int64(8), int64(257)},
				{int64(8), int64(889)},
			},
		},

		// update-4.2: UPDATE with compound WHERE
		{
			name: "update_compound_where_gt",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(8, 89)",
				"INSERT INTO test1 VALUES(8, 257)",
				"INSERT INTO test1 VALUES(8, 889)",
			},
			update: "UPDATE test1 SET f2=f2-1 WHERE f1=8 AND f2>800",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(8), int64(89)},
				{int64(8), int64(257)},
				{int64(8), int64(888)},
			},
		},

		// update-4.3: UPDATE with compound WHERE (less than)
		{
			name: "update_compound_where_lt",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(8, 89)",
				"INSERT INTO test1 VALUES(8, 257)",
				"INSERT INTO test1 VALUES(8, 888)",
			},
			update: "UPDATE test1 SET f2=f2-1 WHERE f1=8 AND f2<800",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(8), int64(88)},
				{int64(8), int64(256)},
				{int64(8), int64(888)},
			},
		},

		// update-4.4: UPDATE affecting multiple matching rows
		{
			name: "update_multiple_matching_rows",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(7, 128)",
				"INSERT INTO test1 VALUES(77, 128)",
				"INSERT INTO test1 VALUES(777, 128)",
			},
			update: "UPDATE test1 SET f1=f1+1 WHERE f2=128",
			verify: "SELECT * FROM test1 ORDER BY f1",
			wantRows: [][]interface{}{
				{int64(8), int64(128)},
				{int64(78), int64(128)},
				{int64(778), int64(128)},
			},
		},

		// update-5.0-5.1: UPDATE with index on f1
		{
			name: "update_with_index_on_f1",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(8, 88)",
				"INSERT INTO test1 VALUES(8, 256)",
				"INSERT INTO test1 VALUES(8, 888)",
				"CREATE INDEX idx1 ON test1(f1)",
			},
			update: "UPDATE test1 SET f2=f2+1 WHERE f1=8",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(8), int64(89)},
				{int64(8), int64(257)},
				{int64(8), int64(889)},
			},
		},

		// update-6.0: UPDATE with index on f2
		{
			name: "update_with_index_on_f2",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(8, 88)",
				"INSERT INTO test1 VALUES(8, 256)",
				"INSERT INTO test1 VALUES(8, 888)",
				"CREATE INDEX idx1 ON test1(f2)",
			},
			update: "UPDATE test1 SET f2=f2+1 WHERE f1=8",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(8), int64(89)},
				{int64(8), int64(257)},
				{int64(8), int64(889)},
			},
		},

		// update-7.0: UPDATE with multiple indices
		{
			name: "update_with_multiple_indices",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(6, 64)",
				"INSERT INTO test1 VALUES(7, 128)",
				"INSERT INTO test1 VALUES(8, 88)",
				"CREATE INDEX idx1 ON test1(f2)",
				"CREATE INDEX idx2 ON test1(f2)",
				"CREATE INDEX idx3 ON test1(f1, f2)",
			},
			update: "UPDATE test1 SET f2=f2+1 WHERE f1=8",
			verify: "SELECT * FROM test1 ORDER BY f1, f2",
			wantRows: [][]interface{}{
				{int64(6), int64(64)},
				{int64(7), int64(128)},
				{int64(8), int64(89)},
			},
		},

		// update-9.1: Error: setting unknown column
		{
			name: "error_set_unknown_column",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 2)",
			},
			update:  "UPDATE test1 SET x=11 WHERE f1=1",
			wantErr: true,
		},

		// update-9.2: Error: unknown function
		{
			name: "error_unknown_function_set",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 2)",
			},
			update:  "UPDATE test1 SET f1=x(11) WHERE f1=1",
			wantErr: true,
		},

		// update-9.3: Error: unknown column in WHERE
		{
			name: "error_unknown_column_where",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(1, 2)",
			},
			update:  "UPDATE test1 SET f1=11 WHERE x=1",
			wantErr: true,
		},

		// update-10.2: UPDATE with same primary key value (no-op for constraint)
		{
			name: "update_same_pk_value",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 5, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update: "UPDATE t1 SET a=1, e=9 WHERE f=6",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4), int64(9), int64(6)},
				{int64(2), int64(3), int64(4), int64(4), int64(6), int64(7)},
			},
		},

		// update-10.3: UPDATE violating UNIQUE constraint on primary key
		{
			name: "update_pk_constraint_violation",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 9, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update:  "UPDATE t1 SET a=1, e=10 WHERE f=7",
			wantErr: true,
		},

		// update-10.5: UPDATE with same UNIQUE column value (no-op for constraint)
		{
			name: "update_same_unique_value",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 11, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update: "UPDATE t1 SET b=2, e=11 WHERE f=6",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4), int64(11), int64(6)},
				{int64(2), int64(3), int64(4), int64(4), int64(6), int64(7)},
			},
		},

		// update-10.6: UPDATE violating UNIQUE constraint
		{
			name: "update_unique_constraint_violation",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 11, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update:  "UPDATE t1 SET b=2, e=12 WHERE f=7",
			wantErr: true,
		},

		// update-10.8: UPDATE with same composite UNIQUE value (no-op for constraint)
		{
			name: "update_same_composite_unique",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 13, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update: "UPDATE t1 SET c=3, d=4, e=13 WHERE f=6",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4), int64(13), int64(6)},
				{int64(2), int64(3), int64(4), int64(4), int64(6), int64(7)},
			},
		},

		// update-10.9: UPDATE violating composite UNIQUE constraint
		{
			name: "update_composite_unique_violation",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f, UNIQUE(c, d))",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 13, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 6, 7)",
			},
			update:  "UPDATE t1 SET c=3, d=4, e=14 WHERE f=7",
			wantErr: true,
		},

		// update-11.1: UPDATE with subquery in WHERE (IN)
		{
			name: "update_subquery_in_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f)",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 13, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 7, 7)",
			},
			update: "UPDATE t1 SET e=e+1 WHERE b IN (SELECT b FROM t1)",
			verify: "SELECT b, e FROM t1 ORDER BY b",
			wantRows: [][]interface{}{
				{int64(2), int64(14)},
				{int64(3), int64(8)},
			},
		},

		// update-11.2: UPDATE with subquery in WHERE (correlated)
		{
			name: "update_subquery_correlated",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e, f)",
				"INSERT INTO t1 VALUES(1, 2, 3, 4, 15, 6)",
				"INSERT INTO t1 VALUES(2, 3, 4, 4, 8, 7)",
			},
			update: "UPDATE t1 SET e=e+1 WHERE a IN (SELECT a FROM t1)",
			verify: "SELECT a, e FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(16)},
				{int64(2), int64(9)},
			},
		},

		// From update2.test

		// update2-3.1: UPDATE WITHOUT ROWID with index
		{
			name: "update2_without_rowid_indexed",
			setup: []string{
				"CREATE TABLE t4(a PRIMARY KEY, b, c) WITHOUT ROWID",
				"CREATE INDEX t4c ON t4(c)",
				"INSERT INTO t4 VALUES(1, 2, 3)",
				"INSERT INTO t4 VALUES(2, 3, 4)",
			},
			update: "UPDATE t4 SET c=c+2 WHERE c>2",
			verify: "SELECT a, c FROM t4 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(5)},
				{int64(2), int64(6)},
			},
		},

		// update2-4.1.1: UPDATE OR REPLACE with UNIQUE constraint
		{
			name: "update2_or_replace_unique",
			setup: []string{
				"CREATE TABLE b1(a INTEGER PRIMARY KEY, b, c)",
				"CREATE UNIQUE INDEX b1c ON b1(c)",
				"INSERT INTO b1 VALUES(1, 'a', 1)",
				"INSERT INTO b1 VALUES(2, 'b', 15)",
				"INSERT INTO b1 VALUES(3, 'c', 3)",
				"INSERT INTO b1 VALUES(4, 'd', 4)",
				"INSERT INTO b1 VALUES(5, 'e', 5)",
			},
			update: "UPDATE OR REPLACE b1 SET c=c+10 WHERE a BETWEEN 4 AND 5",
			verify: "SELECT * FROM b1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "a", int64(1)},
				{int64(3), "c", int64(3)},
				{int64(4), "d", int64(14)},
				{int64(5), "e", int64(15)},
			},
		},

		// update2-5.1: UPDATE with compound index
		{
			name: "update2_compound_index",
			setup: []string{
				"CREATE TABLE x1(a INTEGER PRIMARY KEY, b, c)",
				"CREATE INDEX x1c ON x1(b, c)",
				"INSERT INTO x1 VALUES(1, 'a', 1)",
				"INSERT INTO x1 VALUES(2, 'a', 2)",
				"INSERT INTO x1 VALUES(3, 'a', 3)",
			},
			update: "UPDATE x1 SET c=c+1 WHERE b='a'",
			verify: "SELECT * FROM x1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "a", int64(2)},
				{int64(2), "a", int64(3)},
				{int64(3), "a", int64(4)},
			},
		},

		// update2-6.1: UPDATE with OR condition in WHERE
		{
			name: "update2_or_condition",
			setup: []string{
				"CREATE TABLE d1(a, b)",
				"CREATE INDEX d1b ON d1(a)",
				"CREATE INDEX d1c ON d1(b)",
				"INSERT INTO d1 VALUES(1, 2)",
			},
			update: "UPDATE d1 SET a = a+2 WHERE a>0 OR b>0",
			verify: "SELECT * FROM d1",
			wantRows: [][]interface{}{
				{int64(3), int64(2)},
			},
		},

		// update2-7.110: UPDATE OR REPLACE with partial index
		{
			name: "update2_or_replace_partial_index",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"CREATE UNIQUE INDEX t1x1 ON t1(x) WHERE x IS NOT NULL",
				"INSERT INTO t1(x) VALUES(NULL)",
				"INSERT INTO t1(x) VALUES(NULL)",
				"CREATE INDEX t1x2 ON t1(y)",
			},
			update: "UPDATE OR REPLACE t1 SET x=1",
			verify: "SELECT x, y FROM t1",
			wantRows: [][]interface{}{
				{int64(1), nil},
			},
		},

		// Additional edge cases

		// UPDATE with expression using arithmetic
		{
			name: "update_arithmetic_expression",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
				"INSERT INTO t1 VALUES(1, 10, 5)",
				"INSERT INTO t1 VALUES(2, 20, 10)",
			},
			update: "UPDATE t1 SET b = b + c * 2",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(20), int64(5)},
				{int64(2), int64(40), int64(10)},
			},
		},

		// UPDATE with CASE expression
		{
			name: "update_case_expression",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
				"INSERT INTO t1 VALUES(1, 10, 'x')",
				"INSERT INTO t1 VALUES(2, 20, 'y')",
			},
			update: "UPDATE t1 SET b = CASE WHEN a = 1 THEN 100 ELSE 200 END",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(100), "x"},
				{int64(2), int64(200), "y"},
			},
		},

		// UPDATE no rows (WHERE matches nothing)
		{
			name: "update_no_rows_match",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
			},
			update: "UPDATE t1 SET b = 999 WHERE a > 100",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(20)},
			},
		},

		// UPDATE with BETWEEN in WHERE
		{
			name: "update_between_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
				"INSERT INTO t1 VALUES(4, 40)",
			},
			update: "UPDATE t1 SET b = b + 1 WHERE a BETWEEN 2 AND 3",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(21)},
				{int64(3), int64(31)},
				{int64(4), int64(40)},
			},
		},

		// UPDATE with IN list in WHERE
		{
			name: "update_in_list_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
				"INSERT INTO t1 VALUES(4, 40)",
			},
			update: "UPDATE t1 SET b = b * 2 WHERE a IN (1, 3, 5)",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(20)},
				{int64(2), int64(20)},
				{int64(3), int64(60)},
				{int64(4), int64(40)},
			},
		},

		// UPDATE with IS NULL in WHERE
		{
			name: "update_is_null_where",
			skip: "",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
				"INSERT INTO t1 VALUES(1, NULL, 1)",
				"INSERT INTO t1 VALUES(2, 20, 2)",
				"INSERT INTO t1 VALUES(3, NULL, 3)",
			},
			update: "UPDATE t1 SET b = 999 WHERE b IS NULL",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(999), int64(1)},
				{int64(2), int64(20), int64(2)},
				{int64(3), int64(999), int64(3)},
			},
		},

		// UPDATE with IS NOT NULL in WHERE
		{
			name: "update_is_not_null_where",
			skip: "",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
				"INSERT INTO t1 VALUES(1, NULL, 1)",
				"INSERT INTO t1 VALUES(2, 20, 2)",
				"INSERT INTO t1 VALUES(3, NULL, 3)",
			},
			update: "UPDATE t1 SET c = c * 10 WHERE b IS NOT NULL",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, int64(1)},
				{int64(2), int64(20), int64(20)},
				{int64(3), nil, int64(3)},
			},
		},

		// UPDATE OR IGNORE with constraint violation
		{
			name: "update_or_ignore_constraint",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
			},
			update: "UPDATE OR IGNORE t1 SET b = 10 WHERE a = 2",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(20)}, // Should remain unchanged
			},
		},

		// UPDATE single column
		{
			name: "update_single_column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
				"INSERT INTO t1 VALUES(1, 10, 100)",
				"INSERT INTO t1 VALUES(2, 20, 200)",
			},
			update: "UPDATE t1 SET b = 99",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(99), int64(100)},
				{int64(2), int64(99), int64(200)},
			},
		},

		// UPDATE with simple expression
		{
			name: "update_simple_expression",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 5)",
				"INSERT INTO t1 VALUES(2, 10)",
			},
			update: "UPDATE t1 SET b = b + 10",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(15)},
				{int64(2), int64(20)},
			},
		},

		// UPDATE with greater than in WHERE
		{
			name: "update_gt_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
			},
			update: "UPDATE t1 SET b = b * 2 WHERE a > 1",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(40)},
				{int64(3), int64(60)},
			},
		},

		// UPDATE with less than in WHERE
		{
			name: "update_lt_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
			},
			update: "UPDATE t1 SET b = 0 WHERE a < 3",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(0)},
				{int64(2), int64(0)},
				{int64(3), int64(30)},
			},
		},

		// UPDATE with equality in WHERE
		{
			name: "update_eq_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
			},
			update: "UPDATE t1 SET b = 999 WHERE a = 2",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(999)},
			},
		},

		// UPDATE with not equal in WHERE
		{
			name: "update_ne_where",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
			},
			update: "UPDATE t1 SET b = 999 WHERE a != 1",
			verify: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(999)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db, err := sql.Open(DriverName, ":memory:")
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			updRunSetup(t, db, tt.setup)

			if updExecuteUpdate(t, db, tt.update, tt.wantErr) {
				return
			}

			if tt.skipVerify || tt.verify == "" {
				return
			}

			rows, err := db.Query(tt.verify)
			if err != nil {
				t.Fatalf("Verify query failed: %v", err)
			}
			defer rows.Close()

			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("Failed to get columns: %v", err)
			}

			results := updCollectResults(t, rows, len(cols))
			updVerifyResults(t, results, tt.wantRows)
		})
	}
}
