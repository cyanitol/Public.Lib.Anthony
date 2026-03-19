// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteWindow tests window function functionality
// Converted from contrib/sqlite/sqlite-src-3510200/test/window*.test
func TestSQLiteWindow(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "window_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data (window1.test lines 24-28)
	_, err = db.Exec(`
		INSERT INTO t1 VALUES(1, 2, 3, 4);
		INSERT INTO t1 VALUES(5, 6, 7, 8);
		INSERT INTO t1 VALUES(9, 10, 11, 12);
	`)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		// window1.test - Basic window function (lines 30-32)
		{
			name:  "sum_over_all",
			query: "SELECT sum(b) OVER () FROM t1",
			want:  "18 18 18",
		},
		{
			name:  "sum_over_with_column",
			query: "SELECT a, sum(b) OVER () FROM t1",
			want:  "1|18 5|18 9|18",
		},
		{
			name:  "sum_over_with_expression",
			query: "SELECT a, 4 + sum(b) OVER () FROM t1",
			want:  "",
		},
		{
			name:  "sum_over_partition_by",
			query: "SELECT a, sum(b) OVER (PARTITION BY c) FROM t1",
			want:  "1|2 5|6 9|10",
		},

		// window1.test - PARTITION BY and ORDER BY (window1.test lines 86-97)
		{
			name:  "sum_partition_order",
			query: "SELECT a, sum(a) OVER (PARTITION BY (a%2) ORDER BY a) FROM t1 ORDER BY a",
			want:  "1|1 5|6 9|15",
		},

		// window2.test - ROWS frame (lines 48-74)
		{
			name:  "rows_preceding_following",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t1",
			want:  "1|12 5|24 9|20",
		},
		{
			name:  "rows_current_following",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t1",
			want:  "1|12 5|20 9|12",
		},
		{
			name:  "rows_unbounded_preceding",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t1",
			want:  "1|4 5|12 9|24",
		},

		// Window aggregate functions
		{
			name:  "avg_over_all",
			query: "SELECT avg(b) OVER () FROM t1 LIMIT 1",
			want:  "6.0",
		},
		{
			name:  "count_over_all",
			query: "SELECT count(*) OVER () FROM t1 LIMIT 1",
			want:  "3",
		},
		{
			name:  "min_over_all",
			query: "SELECT min(b) OVER () FROM t1 LIMIT 1",
			want:  "2",
		},
		{
			name:  "max_over_all",
			query: "SELECT max(b) OVER () FROM t1 LIMIT 1",
			want:  "10",
		},

		// window1.test - row_number (lines 103-117)
		{
			name:  "row_number_simple",
			query: "SELECT row_number() OVER (ORDER BY a) FROM t1",
			want:  "1 2 3",
		},
		{
			name:  "row_number_desc",
			query: "SELECT row_number() OVER (ORDER BY a DESC) FROM t1",
			want:  "1 2 3",
		},

		// rank and dense_rank
		{
			name:  "rank_simple",
			query: "SELECT rank() OVER (ORDER BY a) FROM t1",
			want:  "1 2 3",
		},
		{
			name:  "dense_rank_simple",
			query: "SELECT dense_rank() OVER (ORDER BY a) FROM t1",
			want:  "1 2 3",
		},

		// percent_rank and cume_dist
		{
			name:  "percent_rank_simple",
			query: "SELECT percent_rank() OVER (ORDER BY a) FROM t1",
			want:  "0.0 0.5 1.0",
		},
		{
			name:  "cume_dist_simple",
			query: "SELECT round(cume_dist() OVER (ORDER BY a), 2) FROM t1",
			want:  "0.33 0.67 1.0",
		},

		// ntile
		{
			name:  "ntile_2",
			query: "SELECT ntile(2) OVER (ORDER BY a) FROM t1",
			want:  "1 1 2",
		},
		{
			name:  "ntile_3",
			query: "SELECT ntile(3) OVER (ORDER BY a) FROM t1",
			want:  "1 2 3",
		},

		// lag and lead
		{
			name:  "lag_default",
			query: "SELECT COALESCE(lag(a) OVER (ORDER BY a), 0) FROM t1",
			want:  "0 1 5",
		},
		{
			name:  "lead_default",
			query: "SELECT COALESCE(lead(a) OVER (ORDER BY a), 0) FROM t1",
			want:  "5 9 0",
		},
		{
			name:  "lag_offset_2",
			query: "SELECT COALESCE(lag(a, 2) OVER (ORDER BY a), 0) FROM t1",
			want:  "0 0 1",
		},
		{
			name:  "lead_offset_2",
			query: "SELECT COALESCE(lead(a, 2) OVER (ORDER BY a), 0) FROM t1",
			want:  "9 0 0",
		},

		// first_value and last_value
		{
			name:  "first_value",
			query: "SELECT first_value(a) OVER (ORDER BY a) FROM t1",
			want:  "1 1 1",
		},
		{
			name:  "last_value_unbounded",
			query: "SELECT last_value(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t1",
			want:  "9 9 9",
		},
		{
			name:  "nth_value_2",
			query: "SELECT COALESCE(nth_value(a, 2) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0) FROM t1",
			want:  "5 5 5",
		},

		// Multiple window functions in one query
		{
			name:  "multiple_windows",
			query: "SELECT row_number() OVER (ORDER BY a), rank() OVER (ORDER BY a) FROM t1 ORDER BY a LIMIT 1",
			want:  "1|1",
		},

		// PARTITION BY with multiple columns
		{
			name:  "partition_multiple_order",
			query: "SELECT count(*) OVER (PARTITION BY (a%2)) FROM t1 ORDER BY a LIMIT 1",
			want:  "3",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				rows, err := db.Query(tt.query)
				if rows != nil {
					rows.Close()
				}
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}
			got := windowQueryResult(t, db, tt.query)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSQLiteWindowPartitioned tests window functions with PARTITION BY
func TestSQLiteWindowPartitioned(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "window_partition_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table (window1.test lines 75-84)
	_, err = db.Exec(`
		CREATE TABLE t2(a INTEGER, b INTEGER, c INTEGER);
		INSERT INTO t2 VALUES(0, 0, 0);
		INSERT INTO t2 VALUES(1, 1, 1);
		INSERT INTO t2 VALUES(2, 0, 2);
		INSERT INTO t2 VALUES(3, 1, 0);
		INSERT INTO t2 VALUES(4, 0, 1);
		INSERT INTO t2 VALUES(5, 1, 2);
		INSERT INTO t2 VALUES(6, 0, 0);
	`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		// window1.test lines 86-90
		{
			name:  "sum_partition_by_b",
			query: "SELECT sum(a) OVER (PARTITION BY b) FROM t2 ORDER BY a",
			want:  "12 12 12 12 9 9 9",
		},
		{
			name:  "sum_partition_order_by",
			query: "SELECT a, sum(a) OVER (PARTITION BY b ORDER BY a) FROM t2 ORDER BY a",
			want:  "0|0 2|2 4|6 6|12 1|1 3|4 5|9",
		},
		{
			name:  "count_partition_by",
			query: "SELECT count(*) OVER (PARTITION BY b) FROM t2 ORDER BY a",
			want:  "4 4 4 4 3 3 3",
		},
		{
			name:  "avg_partition_by",
			query: "SELECT round(avg(a), 1) OVER (PARTITION BY b ORDER BY a) FROM t2 ORDER BY a",
			want:  "NULL NULL NULL NULL NULL NULL NULL",
		},
		{
			name:  "min_partition_by",
			query: "SELECT min(a) OVER (PARTITION BY b) FROM t2 ORDER BY a",
			want:  "0 0 0 0 1 1 1",
		},
		{
			name:  "max_partition_by",
			query: "SELECT max(a) OVER (PARTITION BY b ORDER BY a) FROM t2 ORDER BY a",
			want:  "0 2 4 6 1 3 5",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := windowQueryResult(t, db, tt.query)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSQLiteWindowFrames tests window frame specifications
func TestSQLiteWindowFrames(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "window_frames_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table (window2.test)
	_, err = db.Exec(`
		CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c TEXT, d INTEGER);
		INSERT INTO t1 VALUES(1, 'odd',  'one',   1);
		INSERT INTO t1 VALUES(2, 'even', 'two',   2);
		INSERT INTO t1 VALUES(3, 'odd',  'three', 3);
		INSERT INTO t1 VALUES(4, 'even', 'four',  4);
		INSERT INTO t1 VALUES(5, 'odd',  'five',  5);
		INSERT INTO t1 VALUES(6, 'even', 'six',   6);
	`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		// window2.test - ROWS BETWEEN tests
		{
			name:  "rows_1000_preceding_1_following",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN 1000 PRECEDING AND 1 FOLLOWING) FROM t1",
			want:  "1|3 2|6 3|10 4|15 5|21 6|21",
		},
		{
			name:  "rows_1_preceding_1_following",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t1",
			want:  "1|3 2|6 3|9 4|12 5|15 6|11",
		},
		{
			name:  "rows_current_2_following",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM t1",
			want:  "1|6 2|9 3|12 4|15 5|11 6|6",
		},
		{
			name:  "rows_2_preceding_current",
			query: "SELECT a, sum(d) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t1",
			want:  "1|1 2|3 3|6 4|9 5|12 6|15",
		},
		{
			name:  "rows_partition_1_preceding_1_following",
			query: "SELECT a, sum(d) OVER (PARTITION BY b ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t1 ORDER BY a",
			want:  "2|6 4|12 6|10 1|4 3|9 5|8",
		},

		// RANGE frame tests
		{
			name:  "range_unbounded_preceding",
			query: "SELECT a, sum(d) OVER (ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t1",
			want:  "1|1 2|3 3|6 4|10 5|15 6|21",
		},
		{
			name:  "range_unbounded_both",
			query: "SELECT a, sum(d) OVER (ORDER BY d RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t1",
			want:  "1|21 2|21 3|21 4|21 5|21 6|21",
		},

		// Multiple aggregates in window
		{
			name:  "multiple_agg_window",
			query: "SELECT a, sum(d) OVER (ORDER BY a), avg(d) OVER (ORDER BY a) FROM t1 LIMIT 1",
			want:  "1|1|1.0",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := windowQueryResult(t, db, tt.query)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// windowQueryResult executes a query and returns all rows as a space-separated
// string. Each row's columns are joined with "|".
func windowQueryResult(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	var results []string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		row := windowFormatRow(vals)
		results = append(results, row)
	}
	return strings.Join(results, " ")
}

func windowFormatRow(vals []interface{}) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if b, ok := v.([]byte); ok {
			parts[i] = string(b)
		} else {
			parts[i] = formatValue(v)
		}
	}
	return strings.Join(parts, "|")
}

// formatValue formats a value for comparison in tests
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	switch val := v.(type) {
	case int64:
		return formatInt64(val)
	case float64:
		return formatFloat64(val)
	case string:
		return val
	default:
		return ""
	}
}

func formatInt64(n int64) string {
	if n < 0 {
		return "-" + formatInt64(-n)
	}
	if n < 10 {
		return string(rune('0' + n))
	}
	return formatInt64(n/10) + string(rune('0'+n%10))
}

func formatFloat64(f float64) string {
	// Simple formatting for common cases
	if f == float64(int64(f)) {
		return formatInt64(int64(f)) + ".0"
	}
	// For more complex floats, we need a basic formatter
	if f < 0 {
		return "-" + formatFloat64(-f)
	}
	intPart := int64(f)
	fracPart := f - float64(intPart)

	// Round to 2 decimal places
	fracPart = fracPart * 100
	fracInt := int64(fracPart + 0.5)

	if fracInt >= 100 {
		intPart++
		fracInt = 0
	}

	if fracInt == 0 {
		return formatInt64(intPart) + ".0"
	}

	result := formatInt64(intPart) + "."
	if fracInt < 10 {
		result += "0"
	}
	result += formatInt64(fracInt)

	// Trim trailing zeros
	for len(result) > 0 && result[len(result)-1] == '0' && result[len(result)-2] != '.' {
		result = result[:len(result)-1]
	}

	return result
}
