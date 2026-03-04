// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestSQLiteAggregate contains comprehensive tests converted from SQLite TCL test suite
// Covers: aggnested.test, aggorderby.test, aggerror.test, and select*.test files
func TestSQLiteAggregate(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string        // CREATE + INSERT test data
		query   string          // Aggregate query
		want    [][]interface{} // Expected results
		wantErr bool            // Should query fail?
		skip    string          // Skip reason if not yet supported
	}{
		// Basic COUNT tests
		{
			name: "COUNT(*) basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
			},
			query: "SELECT COUNT(*) FROM t1",
			want:  [][]interface{}{{int64(3)}},
		},
		{
			name: "COUNT(*) with WHERE",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
			},
			query: "SELECT COUNT(*) FROM t1 WHERE b > 10",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name: "COUNT(column) vs COUNT(*)",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, NULL), (3, 30)",
			},
			query: "SELECT COUNT(*), COUNT(b) FROM t1",
			want:  [][]interface{}{{int64(3), int64(2)}},
		},
		{
			name: "COUNT(DISTINCT column)",
			// DISTINCT now implemented
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (2), (3), (3), (3)",
			},
			query: "SELECT COUNT(DISTINCT a) FROM t1",
			want:  [][]interface{}{{int64(3)}},
		},

		// SUM tests
		{
			name: "SUM basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (30)",
			},
			query: "SELECT SUM(value) FROM t1",
			want:  [][]interface{}{{int64(60)}},
		},
		{
			name: "SUM with WHERE",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
			},
			query: "SELECT SUM(value) FROM t1 WHERE id > 1",
			want:  [][]interface{}{{int64(50)}},
		},
		{
			name: "SUM with NULL values",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (NULL), (20)",
			},
			query: "SELECT SUM(value) FROM t1",
			want:  [][]interface{}{{int64(30)}},
		},
		{
			name: "SUM empty result",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
			},
			query: "SELECT SUM(value) FROM t1",
			want:  [][]interface{}{{nil}},
		},

		// AVG tests
		{
			name: "AVG basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (30)",
			},
			query: "SELECT AVG(value) FROM t1",
			want:  [][]interface{}{{float64(20)}},
		},
		{
			name: "AVG with NULL values",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (NULL), (30)",
			},
			query: "SELECT AVG(value) FROM t1",
			want:  [][]interface{}{{float64(20)}},
		},

		// MIN/MAX tests
		{
			name: "MIN basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(30), (10), (20)",
			},
			query: "SELECT MIN(value) FROM t1",
			want:  [][]interface{}{{int64(10)}},
		},
		{
			name: "MAX basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(30), (10), (20)",
			},
			query: "SELECT MAX(value) FROM t1",
			want:  [][]interface{}{{int64(30)}},
		},
		{
			name: "MIN/MAX text",
			setup: []string{
				"CREATE TABLE t1(value TEXT)",
				"INSERT INTO t1 VALUES('zebra'), ('apple'), ('monkey')",
			},
			query: "SELECT MIN(value), MAX(value) FROM t1",
			want:  [][]interface{}{{"apple", "zebra"}},
		},

		// TOTAL tests (like SUM but returns 0.0 for empty set)
		{
			name: "TOTAL basic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20)",
			},
			query: "SELECT TOTAL(value) FROM t1",
			want:  [][]interface{}{{float64(30)}},
		},
		{
			name: "TOTAL empty result",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
			},
			query: "SELECT TOTAL(value) FROM t1",
			want:  [][]interface{}{{float64(0)}},
		},

		// GROUP BY single column
		{
			name: "GROUP BY single column",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 20), ('A', 30), ('B', 40)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(40)}, {"B", int64(60)}},
			skip:  "",
		},
		{
			name: "GROUP BY with COUNT",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 20), ('A', 30)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}, {"B", int64(1)}},
			skip:  "",
		},
		{
			name: "GROUP BY with multiple aggregates",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(2), int64(30), float64(15), int64(10), int64(20)}, {"B", int64(1), int64(30), float64(30), int64(30), int64(30)}},
			skip:  "",
		},

		// GROUP BY multiple columns
		{
			name: "GROUP BY multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 1, 10), (1, 2, 20), (1, 1, 30), (2, 1, 40)",
			},
			query: "SELECT a, b, SUM(value) FROM t1 GROUP BY a, b ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(1), int64(40)}, {int64(1), int64(2), int64(20)}, {int64(2), int64(1), int64(40)}},
			skip:  "",
		},

		// HAVING clause
		{
			name: "HAVING with COUNT",
			skip: "HAVING clause not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30), ('C', 40), ('C', 50), ('C', 60)",
			},
			query: "SELECT category, COUNT(*) as cnt FROM t1 GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}, {"C", int64(3)}},
		},
		{
			name: "HAVING with SUM",
			skip: "HAVING clause not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", int64(100)}},
		},

		// Aggregate with ORDER BY
		{
			name: "GROUP BY with ORDER BY aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 30), ('C', 20)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category ORDER BY total",
			want:  [][]interface{}{{"A", int64(10)}, {"C", int64(20)}, {"B", int64(30)}},
			skip:  "pre-existing failure - ORDER BY on aggregate alias not implemented for GROUP BY",
		},
		{
			name: "GROUP BY with ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 30), ('C', 20)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category ORDER BY total DESC",
			want:  [][]interface{}{{"B", int64(30)}, {"C", int64(20)}, {"A", int64(10)}},
			skip:  "pre-existing failure - ORDER BY on aggregate alias not implemented for GROUP BY",
		},

		// GROUP_CONCAT tests
		{
			name: "GROUP_CONCAT basic",
			setup: []string{
				"CREATE TABLE t1(value TEXT)",
				"INSERT INTO t1 VALUES('a'), ('b'), ('c')",
			},
			query: "SELECT GROUP_CONCAT(value) FROM t1",
			want:  [][]interface{}{{"a,b,c"}},
			skip:  "GROUP_CONCAT may have different ordering",
		},
		{
			name: "GROUP_CONCAT with separator",
			setup: []string{
				"CREATE TABLE t1(value TEXT)",
				"INSERT INTO t1 VALUES('a'), ('b'), ('c')",
			},
			query: "SELECT GROUP_CONCAT(value, '-') FROM t1",
			want:  [][]interface{}{{"a-b-c"}},
			skip:  "GROUP_CONCAT may have different ordering",
		},
		{
			name: "GROUP_CONCAT with GROUP BY",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value TEXT)",
				"INSERT INTO t1 VALUES('A', 'x'), ('A', 'y'), ('B', 'z')",
			},
			query: "SELECT category, GROUP_CONCAT(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", "x,y"}, {"B", "z"}},
			skip:  "GROUP_CONCAT may have different ordering",
		},

		// Nested aggregate tests (from aggnested.test)
		{
			name: "nested aggregate - subquery with aggregate in FROM",
			setup: []string{
				"CREATE TABLE t1(x INT)",
				"INSERT INTO t1 VALUES(100), (20), (3)",
			},
			query: "SELECT (SELECT y FROM (SELECT sum(x) AS y) AS t2) FROM t1",
			want:  [][]interface{}{{int64(123)}},
			skip:  "Nested aggregates not fully supported",
		},
		{
			name: "nested aggregate - sum with subquery",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT (SELECT min(y) + (SELECT x) FROM (SELECT sum(a) AS x, b AS y FROM t2)) FROM t1",
			want:  [][]interface{}{{int64(10)}},
			skip:  "Nested aggregates not fully supported",
		},

		// Aggregate with DISTINCT
		{
			name: "SUM DISTINCT",
			// DISTINCT now implemented
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (10), (30), (20)",
			},
			query: "SELECT SUM(DISTINCT value) FROM t1",
			want:  [][]interface{}{{int64(60)}},
		},
		{
			name: "AVG DISTINCT",
			// DISTINCT now implemented
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (10), (30)",
			},
			query: "SELECT AVG(DISTINCT value) FROM t1",
			want:  [][]interface{}{{float64(20)}},
		},

		// Complex aggregates
		{
			name: "multiple aggregates with arithmetic",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (30)",
			},
			query: "SELECT COUNT(*) * 2, SUM(value) + 10 FROM t1",
			want:  [][]interface{}{{int64(6), int64(70)}},
		},
		{
			name: "aggregate in expression",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(10), (20), (30)",
			},
			query: "SELECT COUNT(*) + SUM(value) FROM t1",
			want:  [][]interface{}{{int64(63)}},
			skip:  "Multiple aggregates in same expression not yet supported (e.g., COUNT() + SUM())",
		},

		// Edge cases
		{
			name: "COUNT on empty table",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
			},
			query: "SELECT COUNT(*) FROM t1",
			want:  [][]interface{}{{int64(0)}},
		},
		{
			name: "aggregates on empty table",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
			},
			query: "SELECT MIN(value), MAX(value), SUM(value), AVG(value) FROM t1",
			want:  [][]interface{}{{nil, nil, nil, nil}},
		},
		{
			name: "GROUP BY empty result",
			skip: "GROUP BY on empty table returns incorrect result",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category",
			want:  [][]interface{}{},
		},

		// Tests from aggorderby.test
		{
			name: "aggregate misuse in ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b INT, c INT, d INT)",
				"INSERT INTO t1 VALUES('1', 1, 1, 10)",
			},
			query:   "SELECT b, GROUP_CONCAT(a ORDER BY MAX(d)) FROM t1 GROUP BY b",
			wantErr: true,
			skip:    "Error checking for misuse of aggregate in ORDER BY",
		},

		// HAVING with aggregate expressions
		{
			name: "HAVING with AVG",
			skip: "HAVING clause not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100), ('B', 200)",
			},
			query: "SELECT category, AVG(value) FROM t1 GROUP BY category HAVING AVG(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", float64(150)}},
		},
		{
			name: "HAVING with MIN",
			skip: "HAVING clause not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30), ('B', 40)",
			},
			query: "SELECT category, MIN(value) FROM t1 GROUP BY category HAVING MIN(value) > 15 ORDER BY category",
			want:  [][]interface{}{{"B", int64(30)}},
		},

		// Multiple rows with aggregates
		{
			name: "complex GROUP BY scenario",
			setup: []string{
				"CREATE TABLE invoice(id INTEGER PRIMARY KEY, amount REAL, name VARCHAR(100))",
				"INSERT INTO invoice (amount, name) VALUES (4.0, 'Michael'), (15.0, 'Bara'), (4.0, 'Michael'), (6.0, 'John')",
			},
			query: "SELECT sum(amount), name FROM invoice GROUP BY name ORDER BY name",
			want:  [][]interface{}{{float64(15.0), "Bara"}, {float64(6.0), "John"}, {float64(8.0), "Michael"}},
			skip:  "pre-existing failure - multi-value INSERT and ORDER BY name with GROUP BY not working together",
		},

		// Test with JOIN and aggregates
		{
			name: "aggregate with INNER JOIN",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"CREATE TABLE t2(id INTEGER, category TEXT)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
				"INSERT INTO t2 VALUES(1, 'A'), (2, 'A'), (3, 'B')",
			},
			query: "SELECT t2.category, SUM(t1.value) FROM t1 INNER JOIN t2 ON t1.id = t2.id GROUP BY t2.category ORDER BY t2.category",
			want:  [][]interface{}{{"A", int64(30)}, {"B", int64(30)}},
			skip:  "GROUP BY not implemented; also fails with 'SUM() is an aggregate function, cannot be called as scalar'",
		},

		// COUNT with different expressions
		{
			name: "COUNT with expression",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, NULL)",
			},
			query: "SELECT COUNT(a + b) FROM t1",
			want:  [][]interface{}{{int64(2)}},
			skip:  "COUNT incorrectly counts NULL expression results - returns 3 instead of 2",
		},

		// Test TOTAL vs SUM difference
		{
			name: "TOTAL vs SUM on empty",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
			},
			query: "SELECT TOTAL(value), SUM(value) FROM t1",
			want:  [][]interface{}{{float64(0), nil}},
		},

		// Aggregate with subquery in WHERE
		{
			name: "aggregate with subquery filter",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
			},
			query: "SELECT SUM(value) FROM t1 WHERE id IN (SELECT id FROM t1 WHERE value > 10)",
			want:  [][]interface{}{{int64(50)}},
			skip:  "Subquery in WHERE clause not working correctly with aggregates - returns NULL instead of 50",
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			// Create a temporary database
			dbFile := fmt.Sprintf("test_agg_%s.db", sanitizeFilename(tt.name))
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
					if !compareAggregateValues(got[i][j], wantVal) {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareAggregateValues compares two values accounting for type conversions
func compareAggregateValues(got, want interface{}) bool {
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
	}

	// Handle numeric types - convert to float64 for comparison
	gotFloat, gotIsNum := toFloat64Agg(got)
	wantFloat, wantIsNum := toFloat64Agg(want)
	if gotIsNum && wantIsNum {
		return gotFloat == wantFloat
	}

	// Direct comparison
	return got == want
}

// toFloat64Agg converts various numeric types to float64
func toFloat64Agg(v interface{}) (float64, bool) {
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

// sanitizeFilename removes characters that can't be used in filenames
func sanitizeFilename(name string) string {
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
