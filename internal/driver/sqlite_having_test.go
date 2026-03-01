package driver

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestSQLiteHaving contains comprehensive tests converted from SQLite TCL test suite
// Covers: having.test, select3.test, select5.test, count.test, and e_select.test
// Tests HAVING clause functionality with various aggregate functions and conditions
func TestSQLiteHaving(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string        // CREATE + INSERT test data
		query   string          // Query with HAVING clause
		want    [][]interface{} // Expected results
		wantErr bool            // Should query fail?
		skip    string          // Skip reason if not yet supported
	}{
		// Basic HAVING with COUNT
		{
			name: "HAVING with COUNT basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING a=2",
			want:  [][]interface{}{{int64(2), int64(12)}},
		},
		{
			name: "HAVING with COUNT and aggregate condition",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING a=2 AND sum(b)>10",
			want:  [][]interface{}{{int64(2), int64(12)}},
		},
		{
			name: "HAVING with COUNT no match",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING sum(b)>12",
			want:  [][]interface{}{},
		},
		{
			name: "HAVING with COUNT(*) greater than",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30), ('C', 40), ('C', 50), ('C', 60)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}, {"C", int64(3)}},
		},
		{
			name: "HAVING with COUNT(*) equals",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category HAVING COUNT(*)=1 ORDER BY category",
			want:  [][]interface{}{{"B", int64(1)}},
		},
		{
			name: "HAVING with COUNT(*) in complex query",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(3, 5), (3, 6), (3, 7), (3, 8)",
				"INSERT INTO t1 VALUES(4, 9), (4, 10), (4, 11), (4, 12), (4, 13), (4, 14), (4, 15), (4, 16)",
				"INSERT INTO t1 VALUES(5, 17), (5, 18), (5, 19), (5, 20), (5, 21), (5, 22), (5, 23), (5, 24), (5, 25), (5, 26), (5, 27), (5, 28), (5, 29), (5, 30), (5, 31)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*)>=4 ORDER BY log",
			want:  [][]interface{}{{int64(3), int64(4)}, {int64(4), int64(8)}, {int64(5), int64(15)}},
		},

		// HAVING with SUM
		{
			name: "HAVING with SUM greater than",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", int64(100)}},
		},
		{
			name: "HAVING with SUM less than",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) < 50 ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}},
		},
		{
			name: "HAVING with SUM equals",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 15), ('A', 15), ('B', 100)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) = 30",
			want:  [][]interface{}{{"A", int64(30)}},
		},
		{
			name: "HAVING with SUM and multiple conditions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 20), (2, 5), (2, 5), (2, 5)",
			},
			query: "SELECT a, SUM(b), COUNT(*) FROM t1 GROUP BY a HAVING SUM(b) > 10 AND COUNT(*) > 2",
			want:  [][]interface{}{{int64(1), int64(30), int64(2)}},
		},

		// HAVING with AVG
		{
			name: "HAVING with AVG",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100), ('B', 200)",
			},
			query: "SELECT category, AVG(value) FROM t1 GROUP BY category HAVING AVG(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", float64(150)}},
		},
		{
			name: "HAVING with AVG equals",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('A', 30)",
			},
			query: "SELECT category, AVG(value) FROM t1 GROUP BY category HAVING AVG(value) = 20",
			want:  [][]interface{}{{"A", float64(20)}},
		},
		{
			name: "HAVING with AVG and COUNT",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100)",
			},
			query: "SELECT category, AVG(value), COUNT(*) FROM t1 GROUP BY category HAVING AVG(value) > 10 AND COUNT(*) > 1",
			want:  [][]interface{}{{"A", float64(15), int64(2)}},
		},

		// HAVING with MIN
		{
			name: "HAVING with MIN",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30), ('B', 40)",
			},
			query: "SELECT category, MIN(value) FROM t1 GROUP BY category HAVING MIN(value) > 15 ORDER BY category",
			want:  [][]interface{}{{"B", int64(30)}},
		},
		{
			name: "HAVING with MIN less than",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 5), ('A', 20), ('B', 30), ('B', 40)",
			},
			query: "SELECT category, MIN(value) FROM t1 GROUP BY category HAVING MIN(value) < 10 ORDER BY category",
			want:  [][]interface{}{{"A", int64(5)}},
		},
		{
			name: "HAVING with MIN and MAX",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 50), ('B', 20), ('B', 25)",
			},
			query: "SELECT category, MIN(value), MAX(value) FROM t1 GROUP BY category HAVING MIN(value) < 15 AND MAX(value) > 40",
			want:  [][]interface{}{{"A", int64(10), int64(50)}},
		},

		// HAVING with MAX
		{
			name: "HAVING with MAX",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30), ('B', 100)",
			},
			query: "SELECT category, MAX(value) FROM t1 GROUP BY category HAVING MAX(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", int64(100)}},
		},
		{
			name: "HAVING with MAX equals",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 50), ('B', 30)",
			},
			query: "SELECT category, MAX(value) FROM t1 GROUP BY category HAVING MAX(value) = 50",
			want:  [][]interface{}{{"A", int64(50)}},
		},

		// HAVING with complex conditions
		{
			name: "HAVING with AND combination",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 20), (2, 5), (2, 10), (3, 100)",
			},
			query: "SELECT a, SUM(b), COUNT(*) FROM t1 GROUP BY a HAVING SUM(b) > 10 AND COUNT(*) > 1 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(30), int64(2)}, {int64(2), int64(15), int64(2)}},
		},
		{
			name: "HAVING with OR combination",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 100), (2, 5), (2, 10), (3, 3)",
			},
			query: "SELECT a, SUM(b) FROM t1 GROUP BY a HAVING SUM(b) > 50 OR COUNT(*) > 1 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(100)}, {int64(2), int64(15)}},
		},
		{
			name: "HAVING with multiple aggregate comparisons",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('A', 30), ('B', 5), ('B', 15)",
			},
			query: "SELECT category, COUNT(*), SUM(value), AVG(value) FROM t1 GROUP BY category HAVING COUNT(*) >= 2 AND SUM(value) > 25 AND AVG(value) < 25 ORDER BY category",
			want:  [][]interface{}{{"A", int64(3), int64(60), float64(20)}},
		},

		// HAVING without GROUP BY (edge case)
		{
			name: "HAVING without GROUP BY - empty result",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
			},
			query: "SELECT log, count(*) FROM t1 HAVING log>=4",
			want:  [][]interface{}{},
		},
		{
			name: "HAVING without GROUP BY - with match",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
			},
			query: "SELECT count(*) FROM t1 HAVING count(*) > 2",
			want:  [][]interface{}{{int64(3)}},
		},
		{
			name: "HAVING without GROUP BY - aggregate condition",
			setup: []string{
				"CREATE TABLE t2(a INTEGER)",
				// Empty table
			},
			query: "SELECT count(*) FROM t2 HAVING count(*)>1",
			want:  [][]interface{}{},
		},
		{
			name: "HAVING without GROUP BY - aggregate condition with match",
			setup: []string{
				"CREATE TABLE t2(a INTEGER)",
				// Empty table
			},
			query: "SELECT count(*) FROM t2 HAVING count(*)<10",
			want:  [][]interface{}{{int64(0)}},
		},

		// HAVING with column aliases
		{
			name: "HAVING with column alias in GROUP BY",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(3, 5), (3, 6), (3, 7), (3, 8)",
				"INSERT INTO t1 VALUES(4, 9), (4, 10), (4, 11), (4, 12)",
			},
			query: "SELECT log AS x, count(*) AS y FROM t1 GROUP BY x HAVING y>=4 ORDER BY x",
			want:  [][]interface{}{{int64(3), int64(4)}, {int64(4), int64(4)}},
		},
		{
			name: "HAVING with alias - COUNT",
			setup: []string{
				"CREATE TABLE t1(category TEXT)",
				"INSERT INTO t1 VALUES('A'), ('A'), ('B')",
			},
			query: "SELECT category, COUNT(*) as cnt FROM t1 GROUP BY category HAVING cnt > 1",
			want:  [][]interface{}{{"A", int64(2)}},
		},
		{
			name: "HAVING with alias - SUM",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 5)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category HAVING total > 15 ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}},
		},

		// HAVING with NULL values
		{
			name: "HAVING with NULL in aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', NULL), ('A', 20), ('B', NULL), ('B', NULL)",
			},
			query: "SELECT category, COUNT(value) FROM t1 GROUP BY category HAVING COUNT(value) > 1 ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}},
		},
		{
			name: "HAVING with SUM and NULL values",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', NULL), ('A', 20), ('B', 5)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) > 10 ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}},
		},
		{
			name: "HAVING with AVG and NULL values",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', NULL), ('A', 30)",
			},
			query: "SELECT category, AVG(value) FROM t1 GROUP BY category HAVING AVG(value) = 20",
			want:  [][]interface{}{{"A", float64(20)}},
		},

		// HAVING with comparison operators
		{
			name: "HAVING with less than",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 10), ('B', 50), ('B', 50)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) < 50 ORDER BY category",
			want:  [][]interface{}{{"A", int64(20)}},
		},
		{
			name: "HAVING with less than or equal",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 25), ('A', 25), ('B', 60)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) <= 50 ORDER BY category",
			want:  [][]interface{}{{"A", int64(50)}},
		},
		{
			name: "HAVING with greater than or equal",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(4, 9), (4, 10), (5, 17)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING log>=4 ORDER BY log",
			want:  [][]interface{}{{int64(4), int64(2)}, {int64(5), int64(1)}},
		},
		{
			name: "HAVING with not equal",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 20), (2, 30)",
			},
			query: "SELECT a, COUNT(*) FROM t1 GROUP BY a HAVING COUNT(*) != 1 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(2)}},
		},

		// HAVING with subquery
		{
			name: "HAVING with subquery in WHERE",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"CREATE TABLE t2(x INTEGER, y INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (1, 2), (2, 3)",
			},
			query: "SELECT a, sum(b) FROM t1 WHERE a IN (SELECT 1) GROUP BY a HAVING sum(b) > 2",
			want:  [][]interface{}{{int64(1), int64(3)}},
		},
		{
			name: "HAVING in subquery with GROUP BY",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 5)",
			},
			query: "SELECT * FROM (SELECT category, SUM(value) as total FROM t1 GROUP BY category HAVING total > 10) ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}},
		},

		// HAVING with ORDER BY
		{
			name: "HAVING with ORDER BY aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 50), ('B', 50), ('C', 5), ('C', 10)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category HAVING total > 10 ORDER BY total",
			want:  [][]interface{}{{"C", int64(15)}, {"A", int64(30)}, {"B", int64(100)}},
		},
		{
			name: "HAVING with ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 50), ('B', 50), ('C', 5), ('C', 10)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category HAVING total > 10 ORDER BY total DESC",
			want:  [][]interface{}{{"B", int64(100)}, {"A", int64(30)}, {"C", int64(15)}},
		},
		{
			name: "HAVING with ORDER BY column position",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, n INTEGER)",
				"INSERT INTO t1 VALUES(3, 5), (3, 6), (4, 9), (4, 10), (4, 11), (4, 12)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*) >= 2 ORDER BY 2",
			want:  [][]interface{}{{int64(3), int64(2)}, {int64(4), int64(4)}},
		},

		// HAVING with multiple columns in GROUP BY
		{
			name: "HAVING with multiple GROUP BY columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 1, 10), (1, 1, 20), (1, 2, 30), (2, 1, 5)",
			},
			query: "SELECT a, b, SUM(value) FROM t1 GROUP BY a, b HAVING SUM(value) > 10 ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(1), int64(30)}, {int64(1), int64(2), int64(30)}},
		},
		{
			name: "HAVING with multiple GROUP BY and multiple conditions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 1, 5), (1, 1, 10), (1, 2, 15), (2, 1, 20)",
			},
			query: "SELECT a, b, COUNT(*), SUM(c) FROM t1 GROUP BY a, b HAVING COUNT(*) > 1 AND SUM(c) < 20 ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(1), int64(2), int64(15)}},
		},

		// HAVING with DISTINCT
		{
			name: "HAVING with COUNT DISTINCT",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 1), ('A', 1), ('A', 2), ('B', 1), ('B', 1), ('B', 1)",
			},
			query: "SELECT category, COUNT(DISTINCT value) FROM t1 GROUP BY category HAVING COUNT(DISTINCT value) > 1",
			want:  [][]interface{}{{"A", int64(2)}},
		},

		// HAVING with expressions
		{
			name: "HAVING with arithmetic expression",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 5), ('B', 10)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) * 2 > 50 ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}},
		},
		{
			name: "HAVING with multiple expressions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 20), (2, 5), (2, 5), (2, 5)",
			},
			query: "SELECT a, SUM(b), AVG(b) FROM t1 GROUP BY a HAVING SUM(b) > AVG(b) * 2 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(30), float64(15)}, {int64(2), int64(15), float64(5)}},
		},

		// HAVING with JOIN
		{
			name: "HAVING with JOIN",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"CREATE TABLE t2(id INTEGER, category TEXT)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30), (1, 15)",
				"INSERT INTO t2 VALUES(1, 'A'), (2, 'B'), (3, 'A')",
			},
			query: "SELECT t2.category, SUM(t1.value) FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY t2.category HAVING SUM(t1.value) > 30 ORDER BY t2.category",
			want:  [][]interface{}{{"A", int64(55)}},
		},

		// HAVING optimization tests (HAVING -> WHERE)
		{
			name: "HAVING optimization - column condition",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING a=2",
			want:  [][]interface{}{{int64(2), int64(12)}},
		},
		{
			name: "HAVING with both column and aggregate conditions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING sum(b)>5 AND a=2",
			want:  [][]interface{}{{int64(2), int64(12)}},
		},

		// HAVING with literal values
		{
			name: "HAVING with constant true",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING 1 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}},
		},
		{
			name: "HAVING with constant false",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20)",
			},
			query: "SELECT a, sum(b) FROM t1 GROUP BY a HAVING 0",
			want:  [][]interface{}{},
		},

		// Edge cases
		{
			name: "HAVING on empty table with GROUP BY",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category HAVING COUNT(*) > 0",
			want:  [][]interface{}{},
		},
		{
			name: "HAVING with all groups filtered out",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 5), (2, 10)",
			},
			query: "SELECT a, SUM(b) FROM t1 GROUP BY a HAVING SUM(b) > 100",
			want:  [][]interface{}{},
		},

		// Error cases
		{
			name: "HAVING with misuse of aliased aggregate",
			setup: []string{
				"CREATE TABLE test1(f1 INTEGER)",
				"INSERT INTO test1(f1) VALUES(1), (2), (3)",
			},
			query:   "SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING max(m+5)<10",
			wantErr: true,
			skip:    "Aliased aggregate misuse error not fully implemented",
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			// Create a temporary database
			dbFile := fmt.Sprintf("test_having_%s.db", sanitizeFilename(tt.name))
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
					if !compareHavingValues(got[i][j], wantVal) {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareHavingValues compares two values accounting for type conversions
func compareHavingValues(got, want interface{}) bool {
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
	gotFloat, gotIsNum := toFloat64Having(got)
	wantFloat, wantIsNum := toFloat64Having(want)
	if gotIsNum && wantIsNum {
		return gotFloat == wantFloat
	}

	// Direct comparison
	return got == want
}

// toFloat64Having converts various numeric types to float64
func toFloat64Having(v interface{}) (float64, bool) {
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
