package driver

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestSQLiteDistinct contains comprehensive tests converted from SQLite TCL test suite
// Covers: distinct.test and distinctagg.test from contrib/sqlite/sqlite-src-3510200/test/
// Tests DISTINCT keyword in various contexts including SELECT, aggregates, and complex queries
//
// NOTE: Many tests are currently skipped as DISTINCT is not yet fully implemented.
// These tests serve as a comprehensive test suite for when DISTINCT support is added.
// See TODO.txt Phase 3 for feature completion roadmap.
//
// Test Coverage:
// - SELECT DISTINCT (basic and multi-column)
// - DISTINCT with NULL values
// - DISTINCT with ORDER BY (ASC/DESC)
// - DISTINCT with LIMIT/OFFSET
// - COUNT(DISTINCT column)
// - SUM(DISTINCT column)
// - DISTINCT in subqueries
// - DISTINCT with expressions
// - ALL keyword (opposite of DISTINCT)
// - DISTINCT with JOINs
// - Edge cases and error conditions
func TestSQLiteDistinct(t *testing.T) {
	const skipMsg = "DISTINCT not yet fully implemented - see TODO.txt Phase 3"

	tests := []struct {
		name    string
		setup   []string        // CREATE + INSERT test data
		query   string          // DISTINCT query
		want    [][]interface{} // Expected results
		wantErr bool            // Should query fail?
		skip    string          // Skip reason if not yet supported
	}{
		// ====================================================================
		// Basic SELECT DISTINCT tests (from distinct.test)
		// ====================================================================

		{
			name: "DISTINCT basic single column",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (5), (2), (6), (4), (5), (1), (3)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b TEXT, c TEXT)",
				"INSERT INTO t1 VALUES('a', 'b', 'c')",
				"INSERT INTO t1 VALUES('A', 'B', 'C')",
				"INSERT INTO t1 VALUES('a', 'b', 'c')",
				"INSERT INTO t1 VALUES('A', 'B', 'C')",
			},
			query: "SELECT DISTINCT a, b FROM t1 ORDER BY a, b",
			want:  [][]interface{}{{"A", "B"}, {"a", "b"}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with NULL values",
			setup: []string{
				"CREATE TABLE t3(a INTEGER, b INTEGER, c TEXT)",
				"INSERT INTO t3 VALUES(null, null, '1')",
				"INSERT INTO t3 VALUES(null, null, '2')",
				"INSERT INTO t3 VALUES(null, 3, '4')",
				"INSERT INTO t3 VALUES(null, 3, '5')",
				"INSERT INTO t3 VALUES(6, null, '7')",
				"INSERT INTO t3 VALUES(6, null, '8')",
			},
			query: "SELECT DISTINCT a, b FROM t3 ORDER BY a, b",
			want:  [][]interface{}{{nil, nil}, {nil, int64(3)}, {int64(6), nil}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT single NULL value",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(NULL), (NULL), (1), (2)",
			},
			query: "SELECT DISTINCT a FROM t1 ORDER BY a",
			want:  [][]interface{}{{nil}, {int64(1)}, {int64(2)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// DISTINCT with ORDER BY tests (from distinct.test 5.*)
		// ====================================================================

		{
			name: "DISTINCT with ORDER BY ASC",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (5), (2), (6), (4), (5), (1), (3)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x ASC",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (5), (2), (6), (4), (5), (1), (3)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x DESC",
			want:  [][]interface{}{{int64(6)}, {int64(5)}, {int64(4)}, {int64(3)}, {int64(2)}, {int64(1)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with ORDER BY implicit",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (5), (2), (6), (4), (5), (1), (3)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// DISTINCT with LIMIT and OFFSET
		// ====================================================================

		{
			name: "DISTINCT with LIMIT",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (2), (1), (4), (5)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x LIMIT 3",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with LIMIT and OFFSET",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (2), (1), (4), (5)",
			},
			query: "SELECT DISTINCT x FROM t1 ORDER BY x LIMIT 2 OFFSET 2",
			want:  [][]interface{}{{int64(3)}, {int64(4)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// DISTINCT with expressions
		// ====================================================================

		{
			name: "DISTINCT with arithmetic expressions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (1, 10), (3, 30)",
			},
			query: "SELECT DISTINCT a + b FROM t1 ORDER BY a + b",
			want:  [][]interface{}{{int64(11)}, {int64(22)}, {int64(33)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with CASE expression",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (1), (2), (3)",
			},
			query: "SELECT DISTINCT CASE WHEN a <= 2 THEN 'low' ELSE 'high' END FROM t1 ORDER BY 1",
			want:  [][]interface{}{{"high"}, {"low"}},
			skip:  skipMsg,
		},

		// ====================================================================
		// COUNT(DISTINCT column) tests (from distinctagg.test)
		// ====================================================================

		{
			name: "COUNT(DISTINCT column) basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(1, 3, 4)",
				"INSERT INTO t1 VALUES(1, 3, 5)",
			},
			query: "SELECT COUNT(DISTINCT a), COUNT(DISTINCT b), COUNT(DISTINCT c) FROM t1",
			want:  [][]interface{}{{int64(1), int64(2), int64(3)}},
			skip:  skipMsg,
		},
		{
			name: "COUNT(DISTINCT) with GROUP BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(1, 3, 4)",
				"INSERT INTO t1 VALUES(1, 3, 5)",
			},
			query: "SELECT b, COUNT(DISTINCT c) FROM t1 GROUP BY b ORDER BY b",
			want:  [][]interface{}{{int64(2), int64(1)}, {int64(3), int64(2)}},
			skip:  skipMsg,
		},
		{
			name: "COUNT(DISTINCT) with NULLs",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 'A', 1)",
				"INSERT INTO t1 VALUES(1, 'A', 1)",
				"INSERT INTO t1 VALUES(2, 'A', 2)",
				"INSERT INTO t1 VALUES(2, 'A', 2)",
				"INSERT INTO t1 VALUES(1, 'B', 1)",
				"INSERT INTO t1 VALUES(2, 'B', 2)",
				"INSERT INTO t1 VALUES(3, 'B', 3)",
				"INSERT INTO t1 VALUES(NULL, 'B', NULL)",
				"INSERT INTO t1 VALUES(NULL, 'C', NULL)",
			},
			query: "SELECT COUNT(DISTINCT a) FROM t1",
			want:  [][]interface{}{{int64(3)}},
			skip:  skipMsg,
		},
		{
			name: "COUNT(DISTINCT) grouped with NULLs",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 'A', 1)",
				"INSERT INTO t1 VALUES(1, 'A', 1)",
				"INSERT INTO t1 VALUES(2, 'A', 2)",
				"INSERT INTO t1 VALUES(2, 'A', 2)",
				"INSERT INTO t1 VALUES(1, 'B', 1)",
				"INSERT INTO t1 VALUES(2, 'B', 2)",
				"INSERT INTO t1 VALUES(3, 'B', 3)",
				"INSERT INTO t1 VALUES(NULL, 'B', NULL)",
				"INSERT INTO t1 VALUES(NULL, 'C', NULL)",
			},
			query: "SELECT COUNT(DISTINCT c) FROM t1 GROUP BY b ORDER BY b",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(0)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// SUM(DISTINCT column) tests
		// ====================================================================

		{
			name: "SUM(DISTINCT) basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (10), (30), (20)",
			},
			query: "SELECT SUM(DISTINCT value) FROM t1",
			want:  [][]interface{}{{int64(60)}}, // 10 + 20 + 30
			skip:  skipMsg,
		},
		{
			name: "SUM(DISTINCT) with NULLs",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (NULL), (20), (10), (NULL)",
			},
			query: "SELECT SUM(DISTINCT value) FROM t1",
			want:  [][]interface{}{{int64(30)}}, // 10 + 20
			skip:  skipMsg,
		},
		{
			name: "SUM(DISTINCT) vs SUM",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(5), (5), (10), (10), (15)",
			},
			query: "SELECT SUM(value), SUM(DISTINCT value) FROM t1",
			want:  [][]interface{}{{int64(45), int64(30)}}, // 45 vs 5+10+15=30
			skip:  skipMsg,
		},

		// ====================================================================
		// DISTINCT in subqueries
		// ====================================================================

		{
			name: "DISTINCT in subquery",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (1, 10), (3, 30)",
			},
			query: "SELECT * FROM (SELECT DISTINCT a, b FROM t1) ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), int64(30)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT in WHERE IN subquery",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"CREATE TABLE t2(value INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 20), (4, 30)",
				"INSERT INTO t2 VALUES(10), (20), (20)",
			},
			query: "SELECT id FROM t1 WHERE value IN (SELECT DISTINCT value FROM t2) ORDER BY id",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// ALL keyword (opposite of DISTINCT)
		// ====================================================================

		{
			name: "SELECT ALL (explicit - should return duplicates)",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (1), (3), (2)",
			},
			query: "SELECT ALL x FROM t1 ORDER BY x",
			want:  [][]interface{}{{int64(1)}, {int64(1)}, {int64(2)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "COUNT(ALL column)",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(1, 3, 4)",
				"INSERT INTO t1 VALUES(1, 3, 5)",
			},
			query: "SELECT COUNT(ALL a) FROM t1",
			want:  [][]interface{}{{int64(3)}},
			skip:  "ALL keyword not yet supported in aggregate functions",
		},

		// ====================================================================
		// DISTINCT with TEXT and collations
		// ====================================================================

		{
			name: "DISTINCT with case sensitive text",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b TEXT)",
				"INSERT INTO t1 VALUES('a', 'a')",
				"INSERT INTO t1 VALUES('a', 'b')",
				"INSERT INTO t1 VALUES('a', 'c')",
				"INSERT INTO t1 VALUES('b', 'a')",
				"INSERT INTO t1 VALUES('b', 'b')",
				"INSERT INTO t1 VALUES('b', 'c')",
				"INSERT INTO t1 VALUES('a', 'a')",
				"INSERT INTO t1 VALUES('b', 'b')",
				"INSERT INTO t1 VALUES('A', 'A')",
				"INSERT INTO t1 VALUES('B', 'B')",
			},
			query: "SELECT DISTINCT a, b FROM t1 ORDER BY a, b",
			want: [][]interface{}{
				{"A", "A"},
				{"B", "B"},
				{"a", "a"},
				{"a", "b"},
				{"a", "c"},
				{"b", "a"},
				{"b", "b"},
				{"b", "c"},
			},
			skip: skipMsg,
		},

		// ====================================================================
		// DISTINCT with JOINs
		// ====================================================================

		{
			name: "DISTINCT with CROSS JOIN",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(10), (20)",
			},
			query: "SELECT DISTINCT a FROM t1 CROSS JOIN t2 ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with LEFT JOIN and NULLs",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(1), (2)",
			},
			query: "SELECT DISTINCT t2.b FROM t1 LEFT JOIN t2 ON t1.a = t2.b ORDER BY t2.b",
			want:  [][]interface{}{{nil}, {int64(1)}, {int64(2)}},
			skip:  skipMsg,
		},
		{
			name: "COUNT(DISTINCT) with JOIN",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(10), (20)",
			},
			query: "SELECT COUNT(DISTINCT a) FROM t1, t2",
			want:  [][]interface{}{{int64(3)}},
			skip:  skipMsg,
		},

		// ====================================================================
		// Edge cases and special scenarios
		// ====================================================================

		{
			name: "DISTINCT on empty table",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
			},
			query: "SELECT DISTINCT x FROM t1",
			want:  [][]interface{}{},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT single row",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(42)",
			},
			query: "SELECT DISTINCT x FROM t1",
			want:  [][]interface{}{{int64(42)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT all same values",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(7), (7), (7), (7), (7)",
			},
			query: "SELECT DISTINCT x FROM t1",
			want:  [][]interface{}{{int64(7)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with rowid",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (1)",
			},
			query: "SELECT DISTINCT rowid FROM t1 ORDER BY rowid",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with partial unique index",
			setup: []string{
				"CREATE TABLE person(pid INT)",
				"CREATE UNIQUE INDEX idx ON person(pid) WHERE pid == 1",
				"INSERT INTO person VALUES(1), (10), (10)",
			},
			query: "SELECT DISTINCT pid FROM person WHERE pid = 10",
			want:  [][]interface{}{{int64(10)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with many constant columns",
			setup: []string{
				"CREATE TABLE dummy(x INTEGER)",
				"INSERT INTO dummy VALUES(1)",
			},
			query: `SELECT DISTINCT
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1
				FROM dummy`,
			want: [][]interface{}{
				{int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1),
					int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1),
					int64(1), int64(1), int64(1), int64(1), int64(1)},
			},
			skip: skipMsg,
		},

		// ====================================================================
		// Error cases
		// ====================================================================

		{
			name: "COUNT(DISTINCT) requires exactly one argument",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:   "SELECT COUNT(DISTINCT a, b) FROM t1",
			wantErr: true,
			skip:    skipMsg,
		},

		// ====================================================================
		// Complex scenarios
		// ====================================================================

		{
			name: "DISTINCT with GROUP BY and HAVING",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 1), ('A', 2), ('A', 1)",
				"INSERT INTO t1 VALUES('B', 3), ('B', 3), ('B', 4)",
			},
			query: "SELECT category, COUNT(DISTINCT value) FROM t1 GROUP BY category HAVING COUNT(DISTINCT value) > 1 ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}, {"B", int64(2)}},
			skip:  skipMsg,
		},
		{
			name: "DISTINCT with arithmetic in ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (1, 10), (3, 30)",
			},
			query: "SELECT DISTINCT a, b FROM t1 ORDER BY a * 2, b",
			want:  [][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), int64(30)}},
			skip:  skipMsg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			// Create a temporary database
			dbFile := fmt.Sprintf("test_distinct_%s.db", sanitizeFilenameDistinct(tt.name))
			defer os.Remove(dbFile)

			db, err := sql.Open(DriverName, dbFile)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			// Run setup statements
			for _, stmt := range tt.setup {
				_, err := db.Exec(stmt)
				if err != nil {
					t.Fatalf("setup failed for statement %q: %v", stmt, err)
				}
			}

			// Execute query
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()

			// Get column count
			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("failed to get columns: %v", err)
			}

			// Collect results
			var got [][]interface{}
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}

			// Compare results
			if len(got) != len(tt.want) {
				t.Errorf("row count mismatch: got %d, want %d\nGot: %v\nWant: %v", len(got), len(tt.want), got, tt.want)
				return
			}

			for i, wantRow := range tt.want {
				if len(got[i]) != len(wantRow) {
					t.Errorf("row %d column count mismatch: got %d, want %d", i, len(got[i]), len(wantRow))
					continue
				}
				for j, wantVal := range wantRow {
					if !compareDistinctValues(got[i][j], wantVal) {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareDistinctValues compares two values accounting for type conversions
func compareDistinctValues(got, want interface{}) bool {
	// Handle nil
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}

	// Handle byte slices (common for strings)
	if gotBytes, ok := got.([]byte); ok {
		if wantStr, ok := want.(string); ok {
			return string(gotBytes) == wantStr
		}
		if wantBytes, ok := want.([]byte); ok {
			if len(gotBytes) != len(wantBytes) {
				return false
			}
			for i := range gotBytes {
				if gotBytes[i] != wantBytes[i] {
					return false
				}
			}
			return true
		}
	}
	if wantBytes, ok := want.([]byte); ok {
		if gotStr, ok := got.(string); ok {
			return gotStr == string(wantBytes)
		}
	}

	// Handle numeric types - convert to float64 for comparison
	gotFloat, gotIsNum := toFloat64Distinct(got)
	wantFloat, wantIsNum := toFloat64Distinct(want)
	if gotIsNum && wantIsNum {
		return gotFloat == wantFloat
	}

	// Direct comparison
	return got == want
}

// toFloat64Distinct converts various numeric types to float64
func toFloat64Distinct(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// sanitizeFilenameDistinct removes characters that can't be used in filenames
func sanitizeFilenameDistinct(name string) string {
	result := ""
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result += string(c)
		} else if c == ' ' || c == '-' {
			result += "_"
		}
	}
	if len(result) > 50 {
		result = result[:50]
	}
	return result
}
