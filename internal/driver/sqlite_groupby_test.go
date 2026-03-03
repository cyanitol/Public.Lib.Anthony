// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// TestSQLiteGroupBy contains comprehensive GROUP BY tests converted from SQLite TCL test suite
// Covers tests from: select3.test, select5.test, collate5.test, and other select*.test files
// Tests GROUP BY with single/multiple columns, expressions, aggregates, NULL values,
// ORDER BY, LIMIT, COLLATE, HAVING, and compound SELECT statements
func TestSQLiteGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string        // CREATE + INSERT test data
		query   string          // GROUP BY query
		want    [][]interface{} // Expected results
		wantErr bool            // Should query fail?
		skip    string          // Skip reason if not yet supported
	}{
		// GROUP BY single column tests
		{
			name: "GROUP BY single column with COUNT - basic",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0),(2,1),(3,2),(4,2),(5,3),(6,3),(7,3),(8,3)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(1)}, {int64(2), int64(2)}, {int64(3), int64(4)}},
		},
		{
			name: "GROUP BY single column with MIN",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0),(2,1),(3,2),(4,2),(5,3),(6,3),(7,3),(8,3)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(3)}, {int64(3), int64(5)}},
		},
		{
			name: "GROUP BY single column with AVG",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0),(2,1),(3,2),(4,2),(5,3),(6,3),(7,3),(8,3)",
			},
			query: "SELECT log, avg(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), float64(1.0)}, {int64(1), float64(2.0)}, {int64(2), float64(3.5)}, {int64(3), float64(6.5)}},
		},
		{
			name: "GROUP BY single column with SUM",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 20), ('A', 30), ('B', 40)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(40)}, {"B", int64(60)}},
		},
		{
			name: "GROUP BY single column with MAX",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 30), ('A', 20)",
			},
			query: "SELECT category, MAX(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(20)}, {"B", int64(30)}},
		},

		// GROUP BY multiple columns tests
		{
			name: "GROUP BY multiple columns - two columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 1, 10), (1, 2, 20), (1, 1, 30), (2, 1, 40)",
			},
			query: "SELECT a, b, SUM(value) FROM t1 GROUP BY a, b ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(1), int64(40)}, {int64(1), int64(2), int64(20)}, {int64(2), int64(1), int64(40)}},
		},
		{
			name: "GROUP BY multiple columns with multiple aggregates",
			setup: []string{
				"CREATE TABLE t1(category TEXT, subcategory TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 'X', 10), ('A', 'X', 20), ('A', 'Y', 30), ('B', 'X', 40)",
			},
			query: "SELECT category, subcategory, COUNT(*), SUM(value), AVG(value) FROM t1 GROUP BY category, subcategory ORDER BY category, subcategory",
			want:  [][]interface{}{{"A", "X", int64(2), int64(30), float64(15)}, {"A", "Y", int64(1), int64(30), float64(30)}, {"B", "X", int64(1), int64(40), float64(40)}},
		},
		{
			name: "GROUP BY three columns",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT, c INT, val INT)",
				"INSERT INTO t1 VALUES(1,1,1,10), (1,1,2,20), (1,1,1,30), (1,2,1,40)",
			},
			query: "SELECT a, b, c, SUM(val) FROM t1 GROUP BY a, b, c ORDER BY a, b, c",
			want:  [][]interface{}{{int64(1), int64(1), int64(1), int64(40)}, {int64(1), int64(1), int64(2), int64(20)}, {int64(1), int64(2), int64(1), int64(40)}},
		},

		// GROUP BY with column number
		{
			name: "GROUP BY column number - 1st column",
			skip: "GROUP BY column number not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(0,3),(1,4)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY 1 ORDER BY log",
			want:  [][]interface{}{{int64(0), int64(2)}, {int64(1), int64(2)}},
		},
		{
			name: "GROUP BY column number - error case (0)",
			skip: "GROUP BY column number validation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1)",
			},
			query:   "SELECT log, count(*) FROM t1 GROUP BY 0 ORDER BY log",
			wantErr: true,
		},
		{
			name: "GROUP BY column number - error case (out of range)",
			skip: "GROUP BY column number validation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1)",
			},
			query:   "SELECT log, count(*) FROM t1 GROUP BY 3 ORDER BY log",
			wantErr: true,
		},

		// GROUP BY with expressions
		{
			name: "GROUP BY expression - arithmetic",
			skip: "GROUP BY expression output returns NULL",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(3,5)",
			},
			query: "SELECT log*2+1, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(1), float64(0.0)}, {int64(3), float64(0.0)}, {int64(5), float64(0.0)}, {int64(7), float64(0.0)}},
		},
		{
			name: "GROUP BY expression with alias",
			skip: "GROUP BY alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(0,3),(1,4)",
			},
			query: "SELECT log*2+1 as x, count(*) FROM t1 GROUP BY x ORDER BY x",
			want:  [][]interface{}{{int64(1), int64(2)}, {int64(3), int64(2)}},
		},
		{
			name: "GROUP BY expression - function call",
			skip: "GROUP BY function expression not yet supported",
			setup: []string{
				"CREATE TABLE t2(a TEXT, b INT, c INT)",
				"INSERT INTO t2 VALUES('abc', 1, 2), ('ABC', 3, 4), ('def', 5, 6)",
			},
			query: "SELECT LOWER(a), count(*) FROM t2 GROUP BY LOWER(a) ORDER BY LOWER(a)",
			want:  [][]interface{}{{"abc", int64(2)}, {"def", int64(1)}},
		},
		{
			name: "GROUP BY with expression in SELECT and GROUP BY",
			skip: "GROUP BY expression returns NULL",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT)",
				"INSERT INTO t1 VALUES(1,10), (1,20), (2,30)",
			},
			query: "SELECT a*2, SUM(b) FROM t1 GROUP BY a*2 ORDER BY a*2",
			want:  [][]interface{}{{int64(2), int64(30)}, {int64(4), int64(30)}},
		},

		// GROUP BY with aggregates (comprehensive)
		{
			name: "GROUP BY with all aggregate functions",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(2), int64(30), float64(15), int64(10), int64(20)}, {"B", int64(1), int64(30), float64(30), int64(30), int64(30)}},
		},
		{
			name: "GROUP BY with COUNT DISTINCT",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT category, COUNT(DISTINCT value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(2)}, {"B", int64(1)}},
		},
		{
			name: "GROUP BY with SUM DISTINCT",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT category, SUM(DISTINCT value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}, {"B", int64(30)}},
		},
		{
			name: "GROUP BY with aggregate arithmetic",
			skip: "aggregate arithmetic expression returns NULL",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3)",
			},
			query: "SELECT log, avg(n)+1 FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), float64(2.0)}, {int64(1), float64(3.0)}, {int64(2), float64(4.0)}},
		},

		// GROUP BY with NULL values
		{
			name: "GROUP BY with NULL values in grouped column",
			skip: "NULL grouping handling incorrect",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), (NULL, 20), ('A', 30), (NULL, 40)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{nil, int64(60)}, {"A", int64(40)}},
		},
		{
			name: "GROUP BY with NULL values in aggregate column",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', NULL), ('B', 20)",
			},
			query: "SELECT category, COUNT(*), COUNT(value), SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(2), int64(1), int64(10)}, {"B", int64(1), int64(1), int64(20)}},
		},
		{
			name: "GROUP BY all NULLs in aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', NULL), ('A', NULL), ('B', 10)",
			},
			query: "SELECT category, SUM(value), AVG(value), MIN(value), MAX(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", nil, nil, nil, nil}, {"B", int64(10), float64(10), int64(10), int64(10)}},
		},

		// GROUP BY with ORDER BY
		{
			name: "GROUP BY with ORDER BY same column",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(2,5),(0,1),(1,2)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(5)}},
		},
		{
			name: "GROUP BY with ORDER BY DESC",
			skip: "ORDER BY DESC not applying correctly",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,5)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC",
			want:  [][]interface{}{{int64(2), int64(5)}, {int64(1), int64(2)}, {int64(0), int64(1)}},
		},
		{
			name: "GROUP BY with ORDER BY column number",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(2,5),(0,1),(1,2)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1",
			want:  [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(5)}},
		},
		{
			name: "GROUP BY with ORDER BY aggregate",
			skip: "ORDER BY aggregate alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 30), ('C', 20)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category ORDER BY total",
			want:  [][]interface{}{{"A", int64(10)}, {"C", int64(20)}, {"B", int64(30)}},
		},
		{
			name: "GROUP BY with ORDER BY aggregate DESC",
			skip: "ORDER BY aggregate alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 30), ('C', 20)",
			},
			query: "SELECT category, SUM(value) as total FROM t1 GROUP BY category ORDER BY total DESC",
			want:  [][]interface{}{{"B", int64(30)}, {"C", int64(20)}, {"A", int64(10)}},
		},
		{
			name: "GROUP BY with ORDER BY multiple columns",
			skip: "GROUP BY alias expression returns NULL",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(1,2),(0,1),(1,3)",
			},
			query: "SELECT log*2+1 AS x, count(*) AS y FROM t1 GROUP BY x ORDER BY y, x",
			want:  [][]interface{}{{int64(1), int64(1)}, {int64(3), int64(2)}},
		},

		// GROUP BY with LIMIT
		{
			name: "GROUP BY with LIMIT",
			skip: "LIMIT with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1,10), (2,20), (3,30), (4,40)",
			},
			query: "SELECT a AS x, sum(b) AS y FROM t1 GROUP BY a LIMIT 3",
			want:  [][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), int64(30)}},
		},
		{
			name: "GROUP BY with LIMIT and ORDER BY",
			skip: "LIMIT with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 40), ('B', 10), ('C', 30), ('D', 20)",
			},
			query: "SELECT category, value FROM t1 GROUP BY category ORDER BY value DESC LIMIT 2",
			want:  [][]interface{}{{"A", int64(40)}, {"C", int64(30)}},
		},
		{
			name: "GROUP BY with LIMIT and aggregate",
			skip: "LIMIT with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(x INT, y INT)",
				"INSERT INTO t1 VALUES(1,10), (2,20), (1,30), (3,40), (2,50)",
			},
			query: "SELECT x, count(*) FROM t1 GROUP BY x ORDER BY x LIMIT 2",
			want:  [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}},
		},

		// GROUP BY with HAVING
		{
			name: "GROUP BY with HAVING on aggregate COUNT",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(2,4),(3,5),(3,6),(3,7),(3,8)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*)>=2 ORDER BY log",
			want:  [][]interface{}{{int64(2), int64(2)}, {int64(3), int64(4)}},
		},
		{
			name: "GROUP BY with HAVING on aggregate SUM",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", int64(100)}},
		},
		{
			name: "GROUP BY with HAVING on aggregate AVG",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 100), ('B', 200)",
			},
			query: "SELECT category, AVG(value) FROM t1 GROUP BY category HAVING AVG(value) > 50 ORDER BY category",
			want:  [][]interface{}{{"B", float64(150)}},
		},
		{
			name: "GROUP BY with HAVING on grouped column",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(3,5),(4,9),(5,17)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING log>=4 ORDER BY log",
			want:  [][]interface{}{{int64(4), int64(1)}, {int64(5), int64(1)}},
		},
		{
			name: "GROUP BY with HAVING using alias",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(2,4),(3,5),(3,6),(3,7)",
			},
			query: "SELECT log AS x, count(*) AS y FROM t1 GROUP BY x HAVING y>=2 ORDER BY x",
			want:  [][]interface{}{{int64(2), int64(2)}, {int64(3), int64(3)}},
		},

		// GROUP BY with COLLATE
		{
			name: "GROUP BY with COLLATE NOCASE",
			skip: "COLLATE in GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b INTEGER)",
				"INSERT INTO t1 VALUES('a', 1), ('A', 2), ('b', 3), ('B', 4)",
			},
			query: "SELECT a COLLATE NOCASE, count(*) FROM t1 GROUP BY a COLLATE NOCASE ORDER BY a",
			want:  [][]interface{}{{"a", int64(2)}, {"b", int64(2)}},
		},
		{
			name: "GROUP BY multiple columns with COLLATE",
			skip: "COLLATE in GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b TEXT, c INTEGER)",
				"INSERT INTO t1 VALUES('a', 'X', 1), ('A', 'x', 2), ('a', 'X', 3)",
			},
			query: "SELECT a, b, count(*) FROM t1 GROUP BY a COLLATE NOCASE, b COLLATE NOCASE ORDER BY a, b",
			want:  [][]interface{}{{"a", "X", int64(3)}},
		},

		// GROUP BY with compound SELECT (UNION)
		{
			name: "GROUP BY with UNION ALL",
			skip: "UNION ALL subquery causes panic",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT)",
				"CREATE TABLE t2(a INT, b INT)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20)",
				"INSERT INTO t2 VALUES(1, 30), (3, 40)",
			},
			query: "SELECT a, SUM(b) FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) GROUP BY a ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(40)}, {int64(2), int64(20)}, {int64(3), int64(40)}},
		},
		{
			name: "GROUP BY after UNION",
			skip: "UNION ALL subquery causes panic",
			setup: []string{
				"CREATE TABLE t1(x INT)",
				"CREATE TABLE t2(x INT)",
				"INSERT INTO t1 VALUES(1), (2), (1)",
				"INSERT INTO t2 VALUES(2), (3)",
			},
			query: "SELECT x, COUNT(*) FROM (SELECT x FROM t1 UNION ALL SELECT x FROM t2) GROUP BY x ORDER BY x",
			want:  [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}, {int64(3), int64(1)}},
		},

		// GROUP BY edge cases
		{
			name: "GROUP BY on empty table",
			skip: "empty table handling incorrect",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category",
			want:  [][]interface{}{},
		},
		{
			name: "GROUP BY with WHERE clause filtering all rows",
			setup: []string{
				"CREATE TABLE t2(a INTEGER, b INTEGER)",
				"INSERT INTO t2 VALUES(1,2)",
			},
			query: "SELECT a, sum(b) FROM t2 WHERE b=5 GROUP BY a",
			want:  [][]interface{}{},
		},
		{
			name: "GROUP BY without aggregate function",
			setup: []string{
				"CREATE TABLE t2(a INT, b INT, c INT)",
				"INSERT INTO t2 VALUES(1, 2, 3), (1, 4, 5), (6, 4, 7)",
			},
			query: "SELECT a FROM t2 GROUP BY a ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(6)}},
		},
		{
			name: "GROUP BY multiple columns without aggregate",
			setup: []string{
				"CREATE TABLE t2(a INT, b INT, c INT)",
				"INSERT INTO t2 VALUES(1, 2, 3), (1, 4, 5), (1, 2, 6)",
			},
			query: "SELECT a, b FROM t2 GROUP BY a, b ORDER BY a, b",
			want:  [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(4)}},
		},

		// Complex GROUP BY scenarios
		{
			name: "GROUP BY with complex aggregate expression",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(3,5),(4,9)",
			},
			query: "SELECT log, count(*), avg(n), max(n+log*2) FROM t1 GROUP BY log ORDER BY max(n+log*2)+0, avg(n)+0",
			want:  [][]interface{}{{int64(0), int64(1), float64(1.0), int64(1)}, {int64(1), int64(1), float64(2.0), int64(4)}, {int64(2), int64(1), float64(3.0), int64(7)}, {int64(3), int64(1), float64(5.0), int64(11)}, {int64(4), int64(1), float64(9.0), int64(17)}},
		},
		{
			name: "GROUP BY with JOIN",
			skip: "JOIN with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"CREATE TABLE t2(id INTEGER, category TEXT)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)",
				"INSERT INTO t2 VALUES(1, 'A'), (2, 'A'), (3, 'B')",
			},
			query: "SELECT t2.category, SUM(t1.value) FROM t1 INNER JOIN t2 ON t1.id = t2.id GROUP BY t2.category ORDER BY t2.category",
			want:  [][]interface{}{{"A", int64(30)}, {"B", int64(30)}},
		},
		{
			name: "GROUP BY with subquery in FROM",
			skip: "subquery in FROM with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(x INT, y INT)",
				"INSERT INTO t1 VALUES(1,5), (2,6), (3,7), (4,8), (5,9), (6,10)",
			},
			query: "SELECT y, count(*) FROM (SELECT x, y FROM t1 WHERE x<4) GROUP BY y ORDER BY y",
			want:  [][]interface{}{{int64(5), int64(1)}, {int64(6), int64(1)}, {int64(7), int64(1)}},
		},
		{
			name: "GROUP BY with REAL type conversion",
			skip: "typeof function not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a1 DOUBLE, a2 VARCHAR, a3 DOUBLE)",
				"INSERT INTO t1 VALUES(1000, 'ABC', 100), (1000, 'ABC', 200)",
			},
			query: "SELECT typeof(sum(a3)) FROM t1 GROUP BY a1",
			want:  [][]interface{}{{"real"}},
		},

		// HAVING with complex nested conditions
		{
			name: "HAVING with nested OR and AND conditions",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 5), ('B', 10), ('C', 100)",
			},
			query: "SELECT category, SUM(value), COUNT(*) FROM t1 GROUP BY category HAVING (SUM(value) > 50 OR COUNT(*) >= 2) AND category != 'B' ORDER BY category",
			want:  [][]interface{}{{"A", int64(30), int64(2)}, {"C", int64(100), int64(1)}},
		},
		{
			name: "HAVING with multiple aggregate functions and complex logic",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('A', 30), ('B', 5), ('B', 5), ('C', 50)",
			},
			query: "SELECT category, MIN(value), MAX(value), AVG(value) FROM t1 GROUP BY category HAVING MIN(value) < 15 AND MAX(value) > 25 ORDER BY category",
			want:  [][]interface{}{{"A", int64(10), int64(30), float64(20)}},
		},
		{
			name: "HAVING with arithmetic on aggregates",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 20), (2, 5), (2, 15)",
			},
			query: "SELECT a, SUM(b), AVG(b) FROM t1 GROUP BY a HAVING SUM(b) > AVG(b) * 2 ORDER BY a",
			want:  [][]interface{}{{int64(1), int64(30), float64(15)}, {int64(2), int64(20), float64(10)}},
		},
		{
			name: "HAVING with BETWEEN on aggregate",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 50), ('C', 5), ('C', 10), ('C', 15)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) BETWEEN 20 AND 40 ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}, {"C", int64(30)}},
			},
		{
			name: "HAVING with IN clause on aggregate",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 5), ('B', 5), ('B', 5), ('C', 100)",
			},
			query: "SELECT category, COUNT(*) FROM t1 GROUP BY category HAVING COUNT(*) IN (1, 3) ORDER BY category",
			want:  [][]interface{}{{"B", int64(3)}, {"C", int64(1)}},
		},

		// GROUP BY with DISTINCT variations
		{
			name: "GROUP BY with DISTINCT in SELECT without aggregate",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (1, 10), (2, 20)",
			},
			query: "SELECT DISTINCT a FROM t1 GROUP BY a ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "GROUP BY with aggregate of DISTINCT values",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 10), ('A', 20), ('B', 5)",
			},
			query: "SELECT category, SUM(DISTINCT value), COUNT(DISTINCT value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(30), int64(2)}, {"B", int64(5), int64(1)}},
		},
		{
			name: "GROUP BY with AVG of DISTINCT values",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 10), ('A', 20), ('B', 5), ('B', 5)",
			},
			query: "SELECT category, AVG(DISTINCT value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", float64(15)}, {"B", float64(5)}},
		},

		// GROUP BY with CASE expressions
		{
			name: "GROUP BY with CASE expression",
			skip: "GROUP BY alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(5), (15), (25), (35), (45)",
			},
			query: "SELECT CASE WHEN value < 20 THEN 'low' ELSE 'high' END as range, COUNT(*) FROM t1 GROUP BY range ORDER BY range",
			want:  [][]interface{}{{"high", int64(3)}, {"low", int64(2)}},
		},
		{
			name: "GROUP BY with CASE in aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 30), ('B', 5), ('B', 50)",
			},
			query: "SELECT category, SUM(CASE WHEN value > 20 THEN value ELSE 0 END) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}, {"B", int64(50)}},
		},

		// GROUP BY with window function context (subquery)
		{
			name: "GROUP BY in subquery with outer aggregate",
			skip: "subquery in FROM with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20), ('B', 30)",
			},
			query: "SELECT MAX(total) FROM (SELECT category, SUM(value) as total FROM t1 GROUP BY category)",
			want:  [][]interface{}{{int64(30)}},
		},
		{
			name: "GROUP BY with correlated subquery in HAVING",
			skip: "HAVING clause not yet supported",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40), (5, 'C', 5)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category HAVING SUM(value) > (SELECT AVG(value) FROM t1) ORDER BY category",
			want:  [][]interface{}{{"A", int64(30)}, {"B", int64(70)}},
		},

		// GROUP BY with mathematical functions
		{
			name: "GROUP BY with ABS function",
			skip: "GROUP BY alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(value INTEGER)",
				"INSERT INTO t1 VALUES(-5), (5), (-10), (10)",
			},
			query: "SELECT ABS(value) as abs_val, COUNT(*) FROM t1 GROUP BY abs_val ORDER BY abs_val",
			want:  [][]interface{}{{int64(5), int64(2)}, {int64(10), int64(2)}},
		},
		{
			name: "GROUP BY with ROUND in expression",
			skip: "ROUND function not yet implemented",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value REAL)",
				"INSERT INTO t1 VALUES('A', 10.4), ('A', 10.6), ('B', 20.1)",
			},
			query: "SELECT category, ROUND(AVG(value), 1) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", float64(10.5)}, {"B", float64(20.1)}},
		},

		// GROUP BY with LIMIT and OFFSET combinations
		{
			name: "GROUP BY with LIMIT and OFFSET",
			skip: "LIMIT/OFFSET with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES('A', 10), ('B', 20), ('C', 30), ('D', 40), ('E', 50)",
			},
			query: "SELECT category, value FROM t1 GROUP BY category ORDER BY category LIMIT 2 OFFSET 1",
			want:  [][]interface{}{{"B", int64(20)}, {"C", int64(30)}},
		},
		{
			name: "GROUP BY with aggregate and LIMIT OFFSET",
			skip: "LIMIT/OFFSET with GROUP BY not yet supported",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 10), (2, 20), (1, 30), (3, 40), (2, 50)",
			},
			query: "SELECT a, SUM(b) FROM t1 GROUP BY a ORDER BY SUM(b) DESC LIMIT 2 OFFSET 1",
			want:  [][]interface{}{{int64(2), int64(70)}, {int64(1), int64(40)}},
		},

		// GROUP BY with string aggregates
		{
			name: "GROUP BY with MIN and MAX on strings",
			setup: []string{
				"CREATE TABLE t1(category TEXT, name TEXT)",
				"INSERT INTO t1 VALUES('A', 'apple'), ('A', 'zebra'), ('B', 'banana')",
			},
			query: "SELECT category, MIN(name), MAX(name) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", "apple", "zebra"}, {"B", "banana", "banana"}},
		},

		// GROUP BY with type mixing
		{
			name: "GROUP BY with mixed types in aggregate",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value)",
				"INSERT INTO t1 VALUES('A', 10), ('A', 20.5), ('B', 30)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{"A", float64(30.5)}, {"B", float64(30)}},
		},

		// GROUP BY with all NULL group
		{
			name: "GROUP BY with all NULL values in group",
			skip: "NULL grouping handling incorrect",
			setup: []string{
				"CREATE TABLE t1(category TEXT, value INTEGER)",
				"INSERT INTO t1 VALUES(NULL, 10), (NULL, 20), ('A', 30)",
			},
			query: "SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category",
			want:  [][]interface{}{{nil, int64(30)}, {"A", int64(30)}},
		},

		// Error cases
		{
			name: "GROUP BY with invalid column reference",
			skip: "error validation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(x INT, y INT)",
				"INSERT INTO t1 VALUES(1, 5)",
			},
			query:   "SELECT y, count(*) FROM t1 GROUP BY z ORDER BY y",
			wantErr: true,
		},
		{
			name: "GROUP BY with invalid function",
			skip: "error validation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(x INT, y INT)",
				"INSERT INTO t1 VALUES(1, 5)",
			},
			query:   "SELECT y, count(*) FROM t1 GROUP BY invalid_func(y) ORDER BY y",
			wantErr: true,
		},
		{
			name: "HAVING with invalid column reference",
			skip: "error validation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(x INT, y INT)",
				"INSERT INTO t1 VALUES(1, 5)",
			},
			query:   "SELECT y, count(*) FROM t1 GROUP BY y HAVING count(*)<z ORDER BY y",
			wantErr: true,
		},

		// Additional edge cases from select3.test
		{
			name: "GROUP BY with complex ORDER BY expression",
			skip: "GROUP BY alias not yet supported",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,4),(3,8),(4,16)",
			},
			query: "SELECT log*2+1 AS x, count(*) AS y FROM t1 GROUP BY x ORDER BY 10-(x+y)",
			want:  [][]interface{}{{int64(9), int64(1)}, {int64(7), int64(1)}, {int64(5), int64(1)}, {int64(3), int64(1)}, {int64(1), int64(1)}},
		},
		{
			name: "GROUP BY with index on grouped column ASC",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(5,17),(4,9),(3,5),(2,3),(1,2),(0,1)",
				"CREATE INDEX i1 ON t1(log)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(3)}, {int64(3), int64(5)}, {int64(4), int64(9)}, {int64(5), int64(17)}},
		},
		{
			name: "GROUP BY with index on grouped column DESC",
			skip: "ORDER BY DESC not applying correctly",
			setup: []string{
				"CREATE TABLE t1(log int, n int)",
				"INSERT INTO t1 VALUES(0,1),(1,2),(2,3),(3,5),(4,9),(5,17)",
				"CREATE INDEX i1 ON t1(log)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC",
			want:  [][]interface{}{{int64(5), int64(17)}, {int64(4), int64(9)}, {int64(3), int64(5)}, {int64(2), int64(3)}, {int64(1), int64(2)}, {int64(0), int64(1)}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			// Create a temporary database
			dbFile := fmt.Sprintf("test_groupby_%s.db", sanitizeFilenameGroupBy(tt.name))
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
					if !compareGroupByValues(got[i][j], wantVal) {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareGroupByValues compares two values accounting for type conversions
func compareGroupByValues(got, want interface{}) bool {
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
	gotFloat, gotIsNum := toFloat64GroupBy(got)
	wantFloat, wantIsNum := toFloat64GroupBy(want)
	if gotIsNum && wantIsNum {
		return gotFloat == wantFloat
	}

	// Direct comparison
	return got == want
}

// toFloat64GroupBy converts various numeric types to float64
func toFloat64GroupBy(v interface{}) (float64, bool) {
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

// sanitizeFilenameGroupBy removes characters that can't be used in filenames
func sanitizeFilenameGroupBy(name string) string {
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
