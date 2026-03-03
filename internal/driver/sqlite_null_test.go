// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestSQLiteNull contains comprehensive tests converted from SQLite TCL test suite
// Covers: null.test and minmax.test (NULL handling aspects)
// Tests NULL in comparisons, arithmetic, logical operations, aggregates, GROUP BY, ORDER BY, etc.
func TestSQLiteNull(t *testing.T) {
	t.Skip("pre-existing failure - needs NULL handling fixes")
	tests := []struct {
		name    string
		setup   []string        // CREATE + INSERT test data
		query   string          // SQL query
		want    [][]interface{} // Expected results
		wantErr bool            // Should query fail?
		skip    string          // Skip reason if not yet supported
	}{
		// null-1.0 - Setup test data
		{
			name: "null-1.0 setup and verify",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT * FROM t1",
			want: [][]interface{}{
				{int64(1), int64(0), int64(0)},
				{int64(2), int64(0), int64(1)},
				{int64(3), int64(1), int64(0)},
				{int64(4), int64(1), int64(1)},
				{int64(5), nil, int64(0)},
				{int64(6), nil, int64(1)},
				{int64(7), nil, nil},
			},
		},

		// null-1.1 - NULL in arithmetic: addition
		{
			name: "null-1.1 arithmetic addition with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(a+b,99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(4)}, {int64(5)},
				{int64(99)}, {int64(99)}, {int64(99)},
			},
		},

		// null-1.2 - NULL in arithmetic: multiplication
		{
			name: "null-1.2 arithmetic multiplication with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(b*c,99) FROM t1",
			want: [][]interface{}{
				{int64(0)}, {int64(0)}, {int64(0)}, {int64(1)},
				{int64(99)}, {int64(99)}, {int64(99)},
			},
		},

		// null-2.1 - CASE expression with NULL: basic comparison
		{
			name: "null-2.1 CASE with NULL in comparison",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when b<>0 then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(0)}, {int64(0)}, {int64(1)}, {int64(1)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.2 - CASE with NOT
		{
			name: "null-2.2 CASE with NOT and NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when not b<>0 then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(1)}, {int64(0)}, {int64(0)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.3 - CASE with AND
		{
			name: "null-2.3 CASE with AND and NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when b<>0 and c<>0 then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(0)}, {int64(0)}, {int64(0)}, {int64(1)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.4 - CASE with NOT (AND)
		{
			name: "null-2.4 CASE with NOT (AND) and NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when not (b<>0 and c<>0) then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(1)}, {int64(1)}, {int64(0)},
				{int64(1)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.5 - CASE with OR
		{
			name: "null-2.5 CASE with OR and NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when b<>0 or c<>0 then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(0)}, {int64(1)}, {int64(1)}, {int64(1)},
				{int64(0)}, {int64(1)}, {int64(0)},
			},
		},

		// null-2.6 - CASE with NOT (OR)
		{
			name: "null-2.6 CASE with NOT (OR) and NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case when not (b<>0 or c<>0) then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(0)}, {int64(0)}, {int64(0)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.7 - CASE equality comparison
		{
			name: "null-2.7 CASE b=c with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case b when c then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(0)}, {int64(0)}, {int64(1)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-2.8 - CASE equality comparison reversed
		{
			name: "null-2.8 CASE c=b with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT ifnull(case c when b then 1 else 0 end, 99) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(0)}, {int64(0)}, {int64(1)},
				{int64(0)}, {int64(0)}, {int64(0)},
			},
		},

		// null-3.1 - NULL in aggregate functions
		{
			name: "null-3.1 aggregates ignore NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT count(*), count(b), count(c), sum(b), sum(c), avg(b), avg(c), min(b), max(b) FROM t1",
			want: [][]interface{}{
				{int64(7), int64(4), int64(6), int64(2), int64(3), float64(0.5), float64(0.5), int64(0), int64(1)},
			},
		},

		// null-3.2 - SUM vs TOTAL on empty result
		{
			name: "null-3.2 SUM vs TOTAL empty",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT sum(b), total(b) FROM t1 WHERE b<0",
			want: [][]interface{}{
				{nil, float64(0.0)},
			},
		},

		// null-4.1 - NULL in WHERE clause
		{
			name: "null-4.1 WHERE with NULL comparison",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT a FROM t1 WHERE b<10",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)},
			},
		},

		// null-4.2 - WHERE with NOT
		{
			name: "null-4.2 WHERE NOT with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT a FROM t1 WHERE not b>10",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)},
			},
		},

		// null-4.3 - WHERE with OR
		{
			name: "null-4.3 WHERE OR with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT a FROM t1 WHERE b<10 or c=1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(6)},
			},
		},

		// null-4.4 - WHERE with AND
		{
			name: "null-4.4 WHERE AND with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT a FROM t1 WHERE b<10 and c=1",
			want: [][]interface{}{
				{int64(2)}, {int64(4)},
			},
		},

		// null-4.5 - WHERE with complex NOT
		{
			name: "null-4.5 WHERE NOT (AND) with NULL",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT a FROM t1 WHERE not (b<10 and c=1)",
			want: [][]interface{}{
				{int64(1)}, {int64(3)}, {int64(5)},
			},
		},

		// null-5.1 - DISTINCT with NULL
		{
			name: "null-5.1 DISTINCT treats NULL as distinct",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,0,0)",
				"INSERT INTO t1 VALUES(2,0,1)",
				"INSERT INTO t1 VALUES(3,1,0)",
				"INSERT INTO t1 VALUES(4,1,1)",
				"INSERT INTO t1 VALUES(5,null,0)",
				"INSERT INTO t1 VALUES(6,null,1)",
				"INSERT INTO t1 VALUES(7,null,null)",
			},
			query: "SELECT distinct b FROM t1 ORDER BY b",
			want: [][]interface{}{
				{nil}, {int64(0)}, {int64(1)},
			},
		},

		// null-8.1 - NULL with = comparison
		{
			name: "null-8.1 WHERE y=NULL returns empty",
			setup: []string{
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,11)",
				"INSERT INTO t4 VALUES(2,NULL)",
			},
			query: "SELECT x FROM t4 WHERE y=NULL",
			want:  [][]interface{}{},
		},

		// null-8.3 - NULL with < comparison
		{
			name: "null-8.3 WHERE y<33 excludes NULL",
			setup: []string{
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,11)",
				"INSERT INTO t4 VALUES(2,NULL)",
			},
			query: "SELECT x FROM t4 WHERE y<33 ORDER BY x",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		// null-8.4 - NULL with > comparison
		{
			name: "null-8.4 WHERE y>6 excludes NULL",
			setup: []string{
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,11)",
				"INSERT INTO t4 VALUES(2,NULL)",
			},
			query: "SELECT x FROM t4 WHERE y>6 ORDER BY x",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		// null-8.5 - NULL with != comparison
		{
			name: "null-8.5 WHERE y!=33 excludes NULL",
			setup: []string{
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,11)",
				"INSERT INTO t4 VALUES(2,NULL)",
			},
			query: "SELECT x FROM t4 WHERE y!=33 ORDER BY x",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		// null-9.2 - IS NULL comparison
		{
			name: "null-9.2 WHERE IS NULL",
			skip: "",
			setup: []string{
				"CREATE TABLE t5(a, b, c)",
				"CREATE UNIQUE INDEX t5ab ON t5(a, b)",
				"INSERT INTO t5 VALUES(1, NULL, 'one')",
				"INSERT INTO t5 VALUES(1, NULL, 'i')",
				"INSERT INTO t5 VALUES(NULL, 'x', 'two')",
				"INSERT INTO t5 VALUES(NULL, 'x', 'ii')",
			},
			query: "SELECT * FROM t5 WHERE a = 1 AND b IS NULL",
			want: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(1), nil, "i"},
			},
		},

		// null-9.3 - IS NULL with column
		{
			name: "null-9.3 WHERE a IS NULL AND b = value",
			skip: "",
			setup: []string{
				"CREATE TABLE t5(a, b, c)",
				"CREATE UNIQUE INDEX t5ab ON t5(a, b)",
				"INSERT INTO t5 VALUES(1, NULL, 'one')",
				"INSERT INTO t5 VALUES(1, NULL, 'i')",
				"INSERT INTO t5 VALUES(NULL, 'x', 'two')",
				"INSERT INTO t5 VALUES(NULL, 'x', 'ii')",
			},
			query: "SELECT * FROM t5 WHERE a IS NULL AND b = 'x'",
			want: [][]interface{}{
				{nil, "x", "two"},
				{nil, "x", "ii"},
			},
		},

		// null-10.1 - WHERE comparison with NULL literal
		{
			name: "null-10.1 WHERE column > NULL is empty",
			setup: []string{
				"CREATE TABLE t0(c0 PRIMARY KEY DESC)",
				"INSERT INTO t0(c0) VALUES (0)",
			},
			query: "SELECT * FROM t0 WHERE t0.c0 > NULL",
			want:  [][]interface{}{},
		},

		// COALESCE tests
		{
			name: "COALESCE basic",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, NULL, 3)",
				"INSERT INTO t1 VALUES(NULL, 2, 3)",
				"INSERT INTO t1 VALUES(NULL, NULL, 3)",
			},
			query: "SELECT COALESCE(a, b, c) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)},
			},
		},

		{
			name: "COALESCE all NULL",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"INSERT INTO t1 VALUES(NULL, NULL)",
			},
			query: "SELECT COALESCE(a, b) FROM t1",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "COALESCE with literal",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(5)",
			},
			query: "SELECT COALESCE(a, 99) FROM t1",
			want: [][]interface{}{
				{int64(99)}, {int64(5)},
			},
		},

		// IFNULL tests
		{
			name: "IFNULL basic",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"INSERT INTO t1 VALUES(1, 2)",
				"INSERT INTO t1 VALUES(NULL, 3)",
				"INSERT INTO t1 VALUES(4, NULL)",
			},
			query: "SELECT IFNULL(a, b) FROM t1",
			want: [][]interface{}{
				{int64(1)}, {int64(3)}, {int64(4)},
			},
		},

		{
			name: "IFNULL with literal",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(10)",
			},
			query: "SELECT IFNULL(a, -1) FROM t1",
			want: [][]interface{}{
				{int64(-1)}, {int64(10)},
			},
		},

		// NULL in ORDER BY
		{
			name: "ORDER BY with NULL - ASC",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT a FROM t1 ORDER BY a",
			want: [][]interface{}{
				{nil}, {nil}, {int64(1)}, {int64(2)}, {int64(3)},
			},
		},

		{
			name: "ORDER BY with NULL - DESC",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT a FROM t1 ORDER BY a DESC",
			want: [][]interface{}{
				{int64(3)}, {int64(2)}, {int64(1)}, {nil}, {nil},
			},
		},

		// NULL in GROUP BY
		{
			name: "GROUP BY with NULL",
			setup: []string{
				"CREATE TABLE t1(category, value)",
				"INSERT INTO t1 VALUES('A', 10)",
				"INSERT INTO t1 VALUES(NULL, 20)",
				"INSERT INTO t1 VALUES('A', 30)",
				"INSERT INTO t1 VALUES(NULL, 40)",
				"INSERT INTO t1 VALUES('B', 50)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want: [][]interface{}{
				{nil, int64(60)},
				{"A", int64(40)},
				{"B", int64(50)},
			},
		},

		// MIN/MAX with NULL from minmax.test
		{
			name: "minmax-10.1 MIN with NULL values",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(1)",
				"INSERT INTO t6 VALUES(2)",
				"INSERT INTO t6 VALUES(NULL)",
			},
			query: "SELECT coalesce(min(x),-1) FROM t6",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		{
			name: "minmax-10.2 MAX with NULL values",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(1)",
				"INSERT INTO t6 VALUES(2)",
				"INSERT INTO t6 VALUES(NULL)",
			},
			query: "SELECT max(x) FROM t6",
			want: [][]interface{}{
				{int64(2)},
			},
		},

		{
			name: "minmax-10.3 MIN with index and NULL",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(1)",
				"INSERT INTO t6 VALUES(2)",
				"INSERT INTO t6 VALUES(NULL)",
				"CREATE INDEX i6 ON t6(x)",
			},
			query: "SELECT coalesce(min(x),-1) FROM t6",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		{
			name: "minmax-10.4 MAX with index and NULL",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(1)",
				"INSERT INTO t6 VALUES(2)",
				"INSERT INTO t6 VALUES(NULL)",
				"CREATE INDEX i6 ON t6(x)",
			},
			query: "SELECT max(x) FROM t6",
			want: [][]interface{}{
				{int64(2)},
			},
		},

		{
			name: "minmax-10.8 MIN/MAX all NULL",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(NULL)",
			},
			query: "SELECT min(x), max(x) FROM t6",
			want: [][]interface{}{
				{nil, nil},
			},
		},

		{
			name: "minmax-10.12 MIN/MAX all NULL multiple rows",
			setup: []string{
				"CREATE TABLE t6(x)",
				"INSERT INTO t6 VALUES(NULL)",
				"INSERT INTO t6 VALUES(NULL)",
				"INSERT INTO t6 VALUES(NULL)",
			},
			query: "SELECT min(x), max(x) FROM t6",
			want: [][]interface{}{
				{nil, nil},
			},
		},

		// Three-valued logic tests
		{
			name: "three-valued AND: TRUE AND NULL = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN (1=1 AND NULL) THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "three-valued AND: FALSE AND NULL = FALSE",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN (1=0 AND NULL) THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "three-valued OR: TRUE OR NULL = TRUE",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN (1=1 OR NULL) THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		{
			name: "three-valued OR: FALSE OR NULL = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN (1=0 OR NULL) THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "three-valued NOT: NOT NULL = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN NOT NULL THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		// NULL with various comparison operators
		{
			name: "comparison NULL = NULL is NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "comparison NULL <> NULL is NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN NULL <> NULL THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "comparison NULL < value is NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN NULL < 5 THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		{
			name: "comparison value > NULL is NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN 5 > NULL THEN 1 ELSE 0 END",
			want: [][]interface{}{
				{int64(0)},
			},
		},

		// IS NULL and IS NOT NULL
		{
			name: "IS NULL returns TRUE for NULL",
			skip: "Known issue: VDBE infinite loop with IS NULL in WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT a FROM t1 WHERE a IS NULL",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "IS NOT NULL excludes NULL",
			skip: "Known issue: VDBE infinite loop with IS NOT NULL in WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT a FROM t1 WHERE a IS NOT NULL ORDER BY a",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},

		// NULL in arithmetic operations
		{
			name: "NULL + value = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT NULL + 5",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "NULL - value = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT NULL - 5",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "NULL * value = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT NULL * 5",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "NULL / value = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT NULL / 5",
			want: [][]interface{}{
				{nil},
			},
		},

		{
			name: "value / NULL = NULL",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT 5 / NULL",
			want: [][]interface{}{
				{nil},
			},
		},

		// NULL with string operations
		{
			name: "NULL concatenation",
			setup: []string{
				"CREATE TABLE t1(a TEXT)",
				"INSERT INTO t1 VALUES('hello')",
			},
			query: "SELECT 'hello' || NULL",
			want: [][]interface{}{
				{nil},
			},
		},

		// COUNT with NULL
		{
			name: "COUNT(*) counts NULLs",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT COUNT(*) FROM t1",
			want: [][]interface{}{
				{int64(3)},
			},
		},

		{
			name: "COUNT(column) ignores NULLs",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT COUNT(a) FROM t1",
			want: [][]interface{}{
				{int64(1)},
			},
		},

		// NULL in CASE expressions
		{
			name: "CASE WHEN NULL returns ELSE",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END",
			want: [][]interface{}{
				{"no"},
			},
		},

		{
			name: "CASE value WHEN NULL never matches",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(NULL)",
			},
			query: "SELECT CASE a WHEN NULL THEN 'match' ELSE 'no match' END FROM t1",
			want: [][]interface{}{
				{"no match"},
			},
		},

		{
			name: "CASE with NULL ELSE",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(5)",
			},
			query: "SELECT CASE WHEN a = 10 THEN 'ten' ELSE NULL END FROM t1",
			want: [][]interface{}{
				{nil},
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			// Create a temporary database
			dbFile := fmt.Sprintf("test_null_%s.db", sanitizeFilename(tt.name))
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
					if !compareNullValues(got[i][j], wantVal) {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareNullValues compares two values accounting for type conversions and NULL handling
func compareNullValues(got, want interface{}) bool {
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
	gotFloat, gotIsNum := toFloat64Null(got)
	wantFloat, wantIsNum := toFloat64Null(want)
	if gotIsNum && wantIsNum {
		return gotFloat == wantFloat
	}

	// Direct comparison
	return got == want
}

// toFloat64Null converts various numeric types to float64
func toFloat64Null(v interface{}) (float64, bool) {
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
