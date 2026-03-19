// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import "testing"

func TestFilterClause(t *testing.T) {
	tests := []sqlTestCase{
		{
			name: "basic COUNT with FILTER",
			setup: []string{
				"CREATE TABLE t1 (x INTEGER)",
				"INSERT INTO t1 VALUES (-1), (0), (1), (2), (3)",
			},
			query:    "SELECT COUNT(*) FILTER (WHERE x > 0) FROM t1",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "multiple aggregates with different filters",
			setup: []string{
				"CREATE TABLE t2 (x INTEGER)",
				"INSERT INTO t2 VALUES (-5), (1), (3), (7), (15)",
			},
			query:    "SELECT COUNT(*) FILTER (WHERE x > 0), SUM(x) FILTER (WHERE x < 10) FROM t2",
			wantRows: [][]interface{}{{int64(4), int64(6)}},
		},
		{
			name: "FILTER with GROUP BY",
			setup: []string{
				"CREATE TABLE orders (category TEXT, amount INTEGER)",
				"INSERT INTO orders VALUES ('A', 50), ('A', 150), ('A', 200), ('B', 80), ('B', 120)",
			},
			query:    "SELECT category, COUNT(*) FILTER (WHERE amount > 100) FROM orders GROUP BY category",
			wantRows: [][]interface{}{{"A", int64(2)}, {"B", int64(1)}},
		},
		{
			name: "aggregate without FILTER alongside one with FILTER",
			setup: []string{
				"CREATE TABLE t3 (x INTEGER)",
				"INSERT INTO t3 VALUES (-2), (-1), (0), (1), (2)",
			},
			query:    "SELECT COUNT(*), COUNT(*) FILTER (WHERE x > 0) FROM t3",
			wantRows: [][]interface{}{{int64(5), int64(2)}},
		},
		{
			name: "SUM with FILTER and GROUP BY",
			setup: []string{
				"CREATE TABLE t4 (grp TEXT, val INTEGER)",
				"INSERT INTO t4 VALUES ('X', 10), ('X', 20), ('X', 30), ('Y', 5), ('Y', 50)",
			},
			query:    "SELECT grp, SUM(val) FILTER (WHERE val >= 20) FROM t4 GROUP BY grp",
			wantRows: [][]interface{}{{"X", int64(50)}, {"Y", int64(50)}},
		},
		{
			name: "FILTER that excludes all rows returns zero for COUNT",
			setup: []string{
				"CREATE TABLE t5 (x INTEGER)",
				"INSERT INTO t5 VALUES (1), (2), (3)",
			},
			query:    "SELECT COUNT(*) FILTER (WHERE x > 100) FROM t5",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}

	runSQLTestsFreshDB(t, tests)
}
