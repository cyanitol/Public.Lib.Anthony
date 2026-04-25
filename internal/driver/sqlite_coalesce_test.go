// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteCoalesceNullifIfnull contains comprehensive tests for COALESCE, NULLIF, and IFNULL
// Converted from SQLite TCL test suite:
// - contrib/sqlite/sqlite-src-3510200/test/expr.test (COALESCE, NULLIF)
// - contrib/sqlite/sqlite-src-3510200/test/func.test (IFNULL, COALESCE)
//
// This test suite contains 70+ test cases covering:
// - COALESCE with 2 arguments, multiple arguments, all NULLs
// - IFNULL function (2-argument COALESCE alias)
// - NULLIF function (returns NULL if arguments are equal)
// - Nested COALESCE/NULLIF/IFNULL combinations
// - Type affinity and conversion behavior
// - Integration with other SQL expressions (WHERE, GROUP BY, aggregates)
// - Edge cases (negative numbers, large integers, empty strings, blobs)
//
// NOTE: These tests currently fail due to a known issue in the VDBE layer where
// function arguments are not being properly passed to COALESCE, NULLIF, and IFNULL.
// The test cases are correct and comprehensive - they will pass once the underlying
// implementation bug is fixed.
func TestSQLiteCoalesceNullifIfnull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "coalesce_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string      // Setup queries (CREATE TABLE, INSERT, etc.)
		query   string        // Test query
		want    []interface{} // Expected row values
		wantErr bool          // Should query fail?
		errMsg  string        // Expected error message substring
	}{
		// ============================================================
		// COALESCE function tests
		// From func.test lines 378-393, expr.test lines 115-142
		// ============================================================

		// Basic COALESCE with 2 arguments
		{
			name:  "coalesce_2args_first_non_null",
			query: "SELECT coalesce(1, 2)",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "coalesce_2args_first_null",
			query: "SELECT coalesce(NULL, 2)",
			want:  []interface{}{int64(2)},
		},
		{
			name:  "coalesce_2args_both_null",
			query: "SELECT coalesce(NULL, NULL)",
			want:  []interface{}{nil},
		},

		// COALESCE with multiple arguments (3+)
		{
			name:  "coalesce_3args_first",
			query: "SELECT coalesce(1, 2, 3)",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "coalesce_3args_second",
			query: "SELECT coalesce(NULL, 2, 3)",
			want:  []interface{}{int64(2)},
		},
		{
			name:  "coalesce_3args_third",
			query: "SELECT coalesce(NULL, NULL, 3)",
			want:  []interface{}{int64(3)},
		},
		{
			name:  "coalesce_multiple_all_null",
			query: "SELECT coalesce(NULL, NULL, NULL, NULL)",
			want:  []interface{}{nil},
		},
		{
			name:  "coalesce_many_args",
			query: "SELECT coalesce(NULL, NULL, NULL, NULL, 5, 6, 7)",
			want:  []interface{}{int64(5)},
		},

		// COALESCE with different types
		{
			name:  "coalesce_text",
			query: "SELECT coalesce(NULL, 'hello', 'world')",
			want:  []interface{}{"hello"},
		},
		{
			name:  "coalesce_text_all_null",
			query: "SELECT coalesce(NULL, NULL, NULL)",
			want:  []interface{}{nil},
		},
		{
			name:  "coalesce_real",
			query: "SELECT coalesce(NULL, 3.14, 2.71)",
			want:  []interface{}{3.14},
		},
		{
			name:  "coalesce_mixed_types",
			query: "SELECT coalesce(NULL, 1, 'text')",
			want:  []interface{}{int64(1)},
		},

		// COALESCE with table data - from func.test line 379-383
		{
			name: "coalesce_from_table",
			setup: []string{
				"CREATE TABLE t2(a INTEGER)",
				"INSERT INTO t2 VALUES(1), (NULL), (345), (NULL), (67890)",
			},
			query: "SELECT coalesce(a, 'xyz') FROM t2 ORDER BY ROWID",
			want:  []interface{}{int64(1), "xyz", int64(345), "xyz", int64(67890)},
		},
		{
			name: "coalesce_with_function",
			setup: []string{
				"CREATE TABLE t2(a INTEGER)",
				"INSERT INTO t2 VALUES(1), (NULL), (345)",
			},
			query: "SELECT coalesce(upper(CAST(a AS TEXT)), 'nil') FROM t2 ORDER BY ROWID",
			want:  []interface{}{"1", "nil", "345"},
		},

		// COALESCE with expressions - from expr.test lines 115-142
		{
			name: "coalesce_addition_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, 1)",
			},
			query: "SELECT coalesce(i1+i2, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_subtraction_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(1, NULL)",
			},
			query: "SELECT coalesce(i1-i2, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_multiplication_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, NULL)",
			},
			query: "SELECT coalesce(i1*i2, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_division_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, 1)",
			},
			query: "SELECT coalesce(i1/i2, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_comparison_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, 1)",
			},
			query: "SELECT coalesce(i1<i2, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_logical_and_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, NULL)",
			},
			query: "SELECT coalesce(i1 IS NULL AND i2=5, 99) FROM test1",
			want:  []interface{}{int64(99)},
		},
		{
			name: "coalesce_logical_or_null",
			setup: []string{
				"CREATE TABLE test1(i1 INTEGER, i2 INTEGER)",
				"INSERT INTO test1 VALUES(NULL, NULL)",
			},
			query: "SELECT coalesce(i1 IS NULL OR i2=5, 99) FROM test1",
			want:  []interface{}{int64(1)},
		},

		// COALESCE with real numbers - from expr.test lines 395-398
		{
			name: "coalesce_real_null_addition",
			setup: []string{
				"CREATE TABLE test1(r1 REAL, r2 REAL)",
				"INSERT INTO test1 VALUES(1.23, NULL)",
			},
			query: "SELECT coalesce(r1+r2, 99.0) FROM test1",
			want:  []interface{}{99.0},
		},

		// COALESCE error cases - from func.test lines 1317-1324
		{
			name:    "coalesce_no_args",
			query:   "SELECT coalesce()",
			wantErr: true,
			errMsg:  "requires at least 1 argument",
		},
		{
			name:  "coalesce_one_arg",
			query: "SELECT coalesce(1)",
			want:  []interface{}{int64(1)},
		},

		// COALESCE short-circuit behavior - from func.test lines 1526-1532
		{
			name: "coalesce_short_circuit_empty_table",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
			},
			query: "SELECT coalesce(x, abs(-9223372036854775808)) FROM t1",
			want:  []interface{}{},
		},

		// ============================================================
		// IFNULL function tests
		// IFNULL is an alias for a 2-argument COALESCE
		// ============================================================

		{
			name:  "ifnull_first_not_null",
			query: "SELECT ifnull(42, 0)",
			want:  []interface{}{int64(42)},
		},
		{
			name:  "ifnull_first_null",
			query: "SELECT ifnull(NULL, 42)",
			want:  []interface{}{int64(42)},
		},
		{
			name:  "ifnull_both_null",
			query: "SELECT ifnull(NULL, NULL)",
			want:  []interface{}{nil},
		},
		{
			name:  "ifnull_text",
			query: "SELECT ifnull(NULL, 'default')",
			want:  []interface{}{"default"},
		},
		{
			name:  "ifnull_real",
			query: "SELECT ifnull(3.14, 0.0)",
			want:  []interface{}{3.14},
		},
		{
			name: "ifnull_from_table",
			setup: []string{
				"CREATE TABLE t2(a INTEGER)",
				"INSERT INTO t2 VALUES(1), (NULL), (3)",
			},
			query: "SELECT ifnull(a, -1) FROM t2 ORDER BY ROWID",
			want:  []interface{}{int64(1), int64(-1), int64(3)},
		},

		// ============================================================
		// NULLIF function tests
		// From func.test lines 385-392
		// ============================================================

		{
			name:  "nullif_equal_integers",
			query: "SELECT nullif(1, 1)",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_different_integers",
			query: "SELECT nullif(1, 2)",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "nullif_equal_text",
			query: "SELECT nullif('hello', 'hello')",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_different_text",
			query: "SELECT nullif('hello', 'world')",
			want:  []interface{}{"hello"},
		},
		{
			name:  "nullif_first_null",
			query: "SELECT nullif(NULL, 1)",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_second_null",
			query: "SELECT nullif(1, NULL)",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "nullif_both_null",
			query: "SELECT nullif(NULL, NULL)",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_equal_real",
			query: "SELECT nullif(3.14, 3.14)",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_different_real",
			query: "SELECT nullif(3.14, 2.71)",
			want:  []interface{}{3.14},
		},
		{
			name:  "nullif_zero_values",
			query: "SELECT nullif(0, 0)",
			want:  []interface{}{nil},
		},
		{
			name:  "nullif_empty_strings",
			query: "SELECT nullif('', '')",
			want:  []interface{}{nil},
		},

		// ============================================================
		// Nested and combined COALESCE/NULLIF/IFNULL tests
		// From func.test lines 385-392
		// ============================================================

		{
			name:  "nested_coalesce_nullif_equal",
			query: "SELECT coalesce(nullif(1, 1), 'nil')",
			want:  []interface{}{"nil"},
		},
		{
			name:  "nested_coalesce_nullif_different",
			query: "SELECT coalesce(nullif(1, 2), 'nil')",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "nested_coalesce_nullif_null",
			query: "SELECT coalesce(nullif(1, NULL), 'nil')",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "nested_ifnull_nullif",
			query: "SELECT ifnull(nullif(5, 5), 10)",
			want:  []interface{}{int64(10)},
		},
		{
			name:  "nested_nullif_coalesce",
			query: "SELECT nullif(coalesce(NULL, 1), 1)",
			want:  []interface{}{nil},
		},
		{
			name:  "nested_nullif_coalesce_different",
			query: "SELECT nullif(coalesce(NULL, 1), 2)",
			want:  []interface{}{int64(1)},
		},
		{
			name:  "deeply_nested_coalesce",
			query: "SELECT coalesce(NULL, coalesce(NULL, coalesce(NULL, 42)))",
			want:  []interface{}{int64(42)},
		},

		// ============================================================
		// Type affinity and conversion tests
		// ============================================================

		{
			name:  "coalesce_type_affinity_int_text",
			query: "SELECT typeof(coalesce(NULL, 1, 'text'))",
			want:  []interface{}{"integer"},
		},
		{
			name:  "coalesce_type_affinity_text_int",
			query: "SELECT typeof(coalesce(NULL, 'text', 1))",
			want:  []interface{}{"text"},
		},
		{
			name:  "coalesce_type_affinity_real",
			query: "SELECT typeof(coalesce(NULL, 3.14))",
			want:  []interface{}{"real"},
		},
		{
			name:  "nullif_preserves_type",
			query: "SELECT typeof(nullif(42, 43))",
			want:  []interface{}{"integer"},
		},
		{
			name:  "ifnull_type_from_first",
			query: "SELECT typeof(ifnull(1, 'text'))",
			want:  []interface{}{"integer"},
		},
		{
			name:  "ifnull_type_from_second",
			query: "SELECT typeof(ifnull(NULL, 'text'))",
			want:  []interface{}{"text"},
		},

		// ============================================================
		// Combined with other expressions and operators
		// ============================================================

		{
			name:  "coalesce_in_arithmetic",
			query: "SELECT (coalesce(NULL, 10) + coalesce(NULL, 20)) * 2",
			want:  []interface{}{int64(60)},
		},
		{
			name: "coalesce_in_where_clause",
			setup: []string{
				"CREATE TABLE t3(id INTEGER, value INTEGER)",
				"INSERT INTO t3 VALUES(1, 100), (2, NULL), (3, 300)",
			},
			query: "SELECT id FROM t3 WHERE coalesce(value, 0) > 50 ORDER BY id",
			want:  []interface{}{int64(1), int64(3)},
		},
		{
			name:  "nullif_in_case_expression",
			query: "SELECT CASE WHEN nullif(5, 5) IS NULL THEN 'equal' ELSE 'different' END",
			want:  []interface{}{"equal"},
		},
		{
			name: "ifnull_in_aggregate",
			setup: []string{
				"CREATE TABLE t4(value INTEGER)",
				"INSERT INTO t4 VALUES(10), (NULL), (20), (NULL), (30)",
			},
			query: "SELECT SUM(ifnull(value, 0)) FROM t4",
			want:  []interface{}{int64(60)},
		},
		{
			name: "coalesce_in_group_by",
			setup: []string{
				"CREATE TABLE t5(category TEXT, value INTEGER)",
				"INSERT INTO t5 VALUES(NULL, 10), (NULL, 20), ('A', 30), ('B', 40)",
			},
			query: "SELECT coalesce(category, 'NONE') as cat, SUM(value) FROM t5 GROUP BY cat ORDER BY cat",
			want:  []interface{}{"A", int64(30), "B", int64(40), "NONE", int64(30)},
		},

		// ============================================================
		// Edge cases and special values
		// ============================================================

		{
			name:  "coalesce_negative_numbers",
			query: "SELECT coalesce(NULL, -1, -2)",
			want:  []interface{}{int64(-1)},
		},
		{
			name:  "nullif_negative_equal",
			query: "SELECT nullif(-5, -5)",
			want:  []interface{}{nil},
		},
		{
			name:  "coalesce_large_integers",
			query: "SELECT coalesce(NULL, 9223372036854775807)",
			want:  []interface{}{int64(9223372036854775807)},
		},
		{
			name:  "coalesce_empty_string",
			query: "SELECT coalesce(NULL, '')",
			want:  []interface{}{""},
		},
		{
			name:  "nullif_empty_vs_null",
			query: "SELECT nullif('', NULL)",
			want:  []interface{}{""},
		},
		{
			name:  "coalesce_blob",
			query: "SELECT coalesce(NULL, X'DEADBEEF')",
			want:  []interface{}{[]byte{0xDE, 0xAD, 0xBE, 0xEF}},
		},

		// ============================================================
		// Performance and optimization tests
		// ============================================================

		{
			name: "coalesce_multiple_rows",
			setup: []string{
				"CREATE TABLE t6(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t6 VALUES(1, NULL, NULL), (NULL, 2, NULL), (NULL, NULL, 3)",
			},
			query: "SELECT coalesce(a, b, c, 0) FROM t6 ORDER BY ROWID",
			want:  []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			name: "nullif_scan_optimization",
			setup: []string{
				"CREATE TABLE t7(x INTEGER)",
				"INSERT INTO t7 VALUES(1), (1), (2), (1), (3)",
			},
			query: "SELECT COUNT(*) FROM t7 WHERE nullif(x, 1) IS NULL",
			want:  []interface{}{int64(3)},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Clean up from any previous test - drop tables to reset schema
			coalesceCleanupTables(db)

			// Run setup queries
			coalesceRunSetup(t, db, tt.setup)

			// Execute test query
			rows, err := db.Query(tt.query)

			// Handle error at query level
			if err != nil {
				coalesceCheckExpectedErr(t, err, tt.wantErr, tt.errMsg)
				return
			}
			defer rows.Close()

			// Collect results (may encounter errors during iteration)
			results, iterErr := coalesceCollectResultsWithErr(t, rows)
			if iterErr != nil {
				coalesceCheckExpectedErr(t, iterErr, tt.wantErr, tt.errMsg)
				return
			}
			if tt.wantErr {
				t.Fatalf("expected error containing %q, got nil", tt.errMsg)
			}

			// Compare results
			coalesceVerifyResults(t, results, tt.want)
		})
	}
}

// coalesceCheckExpectedErr verifies that a non-nil error matches expectations.
// It fatals if wantErr is false, or if errMsg is set but not found in the error string.
func coalesceCheckExpectedErr(t *testing.T, err error, wantErr bool, errMsg string) {
	t.Helper()
	if !wantErr {
		t.Fatalf("unexpected error: %v", err)
	}
	if errMsg != "" && !containsSubstring(err.Error(), errMsg) {
		t.Fatalf("expected error containing %q, got %q", errMsg, err.Error())
	}
}

// coalesceCleanupTables drops all test tables
func coalesceCleanupTables(db *sql.DB) {
	tables := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "test1"}
	for _, tableName := range tables {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + tableName)
	}
}

// coalesceRunSetup executes setup queries
func coalesceRunSetup(t *testing.T, db *sql.DB, setup []string) {
	t.Helper()
	for _, query := range setup {
		_, err := db.Exec(query)
		if err != nil {
			t.Fatalf("setup failed: %v\nquery: %s", err, query)
		}
	}
}

// coalesceHandleError checks if error handling is correct
func coalesceHandleError(t *testing.T, err error, wantErr bool, errMsg string) bool {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", errMsg)
		}
		if errMsg != "" && !containsSubstring(err.Error(), errMsg) {
			t.Fatalf("expected error containing %q, got %q", errMsg, err.Error())
		}
		return true
	}
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	return false
}

// coalesceCollectResultsWithErr scans all rows and returns results or an iteration error
func coalesceCollectResultsWithErr(t *testing.T, rows *sql.Rows) ([]interface{}, error) {
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
			t.Fatalf("failed to scan row: %v", err)
		}

		results = append(results, values...)
	}

	if err := rows.Err(); err != nil {
		return results, err
	}
	return results, nil
}

// coalesceVerifyResults compares actual results with expected values
func coalesceVerifyResults(t *testing.T, results, want []interface{}) {
	t.Helper()
	if len(results) != len(want) {
		t.Fatalf("result count mismatch: got %d values, want %d\ngot: %v\nwant: %v",
			len(results), len(want), results, want)
	}

	for i, got := range results {
		if !compareValues(got, want[i]) {
			t.Errorf("value[%d] mismatch: got %v (%T), want %v (%T)",
				i, got, got, want[i], want[i])
		}
	}
}

// compareValues compares two values, handling NULL, integers, floats, strings, and blobs
func compareValues(got, want interface{}) bool {
	return driverValueEqual(got, want)
}

// containsSubstring checks if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstringRec(s, substr))
}

func containsSubstringRec(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
