package driver

import (
	"testing"
)

// TestSQLiteSelectAdvanced tests advanced SELECT features converted from TCL test files:
// - select5.test: Aggregates with GROUP BY and HAVING
// - select6.test: Subqueries in FROM clause
// - select7.test: Compound SELECT statements (UNION, INTERSECT, EXCEPT)
// - select8.test: LIMIT and OFFSET with GROUP BY
// - select9.test: Compound SELECT with ORDER BY
//
// Total: 54 test cases covering subqueries, compound SELECT, complex expressions,
// nested queries, and correlated subqueries
func TestSQLiteSelectAdvanced(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		// From select6.test - Subqueries in FROM clause
		{
			name: "subquery_in_from_simple",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
			},
			query: "SELECT * FROM (SELECT x, y FROM t1 WHERE x<2)",
			want:  [][]interface{}{{int64(1), int64(1)}},
		},
		{
			name: "count_from_subquery",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 2)",
			},
			query: "SELECT count(*) FROM (SELECT y FROM t1)",
			want:  [][]interface{}{{int64(3)}},
		},
		{
			name: "count_distinct_from_subquery",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 2)",
			},
			query: "SELECT count(*) FROM (SELECT DISTINCT y FROM t1)",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name: "nested_subquery_distinct",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 2)",
			},
			query: "SELECT count(*) FROM (SELECT DISTINCT * FROM (SELECT y FROM t1))",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name: "aggregate_subqueries_join",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 2)",
			},
			query: `SELECT * FROM
				(SELECT count(*),y FROM t1 GROUP BY y) AS a,
				(SELECT max(x),y FROM t1 GROUP BY y) as b
				WHERE a.y=b.y ORDER BY a.y`,
			want: [][]interface{}{
				{int64(1), int64(1), int64(1), int64(1)},
				{int64(2), int64(2), int64(3), int64(2)},
			},
		},
		{
			name: "subquery_with_aliases",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
			},
			query: `SELECT q, p, r FROM
				(SELECT count(*) as p, y as q FROM t1 GROUP BY y) AS a,
				(SELECT max(x) as r, y as s FROM t1 GROUP BY y) as b
				WHERE q=s ORDER BY s`,
			want: [][]interface{}{
				{int64(1), int64(1), int64(1)},
				{int64(2), int64(1), int64(2)},
			},
		},
		{
			name: "subquery_avg_with_expression",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 3)",
			},
			query: "SELECT a,b,a+b FROM (SELECT avg(x) as 'a', avg(y) as 'b' FROM t1)",
			want:  [][]interface{}{{float64(2), float64(2), float64(4)}},
		},
		{
			name: "subquery_with_where_on_aggregate",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 3)",
			},
			query: "SELECT a,b,a+b FROM (SELECT avg(x) as 'a', avg(y) as 'b' FROM t1) WHERE a>1",
			want:  [][]interface{}{{float64(2), float64(2), float64(4)}},
		},

		// From select6.test - Compound subqueries
		{
			name: "union_all_in_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM (SELECT x FROM t1 UNION ALL SELECT x+10 FROM t1) ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)},
			},
		},
		{
			name: "union_in_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM (SELECT x FROM t1 UNION SELECT x+1 FROM t1) ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)},
			},
		},
		{
			name: "intersect_in_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
			},
			query: "SELECT * FROM (SELECT x FROM t1 INTERSECT SELECT x+1 FROM t1) ORDER BY 1",
			want: [][]interface{}{
				{int64(2)}, {int64(3)},
			},
		},
		{
			name: "except_in_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
			},
			query: "SELECT * FROM (SELECT x FROM t1 EXCEPT SELECT x*2 FROM t1) ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(3)},
			},
		},

		// From select7.test - Compound SELECT statements
		{
			name: "three_way_intersect",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES('amx')",
				"INSERT INTO t1 VALUES('anx')",
				"INSERT INTO t1 VALUES('amy')",
				"INSERT INTO t1 VALUES('bmy')",
			},
			query: `SELECT * FROM t1 WHERE x LIKE 'a__'
				INTERSECT SELECT * FROM t1 WHERE x LIKE '_m_'
				INTERSECT SELECT * FROM t1 WHERE x LIKE '__x'`,
			want: [][]interface{}{{"amx"}},
		},
		{
			name: "union_all_simple",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT a FROM t1 UNION ALL SELECT a+10 FROM t1 ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)},
			},
		},
		{
			name: "union_removes_duplicates",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT a FROM t1 UNION SELECT a FROM t1 ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},

		// From select8.test - LIMIT and OFFSET with GROUP BY
		{
			name: "limit_offset_with_groupby",
			setup: []string{
				"CREATE TABLE songs(songid, artist, timesplayed)",
				"INSERT INTO songs VALUES(1,'one',1)",
				"INSERT INTO songs VALUES(2,'one',2)",
				"INSERT INTO songs VALUES(3,'two',3)",
			},
			query: "SELECT DISTINCT artist, sum(timesplayed) AS total FROM songs GROUP BY LOWER(artist) LIMIT 1 OFFSET 1",
			want: [][]interface{}{
				{"two", int64(3)},
			},
		},
		{
			name: "limit_offset_groupby_multiple",
			setup: []string{
				"CREATE TABLE songs(songid, artist, timesplayed)",
				"INSERT INTO songs VALUES(1,'one',1)",
				"INSERT INTO songs VALUES(2,'one',2)",
				"INSERT INTO songs VALUES(3,'two',3)",
				"INSERT INTO songs VALUES(4,'three',5)",
			},
			query: "SELECT DISTINCT artist, sum(timesplayed) AS total FROM songs GROUP BY LOWER(artist) LIMIT 2 OFFSET 1",
			want: [][]interface{}{
				{"three", int64(5)},
				{"two", int64(3)},
			},
		},

		// From select9.test - Compound SELECT with ORDER BY
		{
			name: "union_all_with_order_by",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE TABLE t2(d, e)",
				"INSERT INTO t1 VALUES(1, 'one')",
				"INSERT INTO t1 VALUES(2, 'two')",
				"INSERT INTO t2 VALUES(1, 'two')",
				"INSERT INTO t2 VALUES(2, 'four')",
			},
			query: "SELECT a, b FROM t1 UNION ALL SELECT d, e FROM t2 ORDER BY 1",
			want: [][]interface{}{
				{int64(1), "one"},
				{int64(1), "two"},
				{int64(2), "four"},
				{int64(2), "two"},
			},
		},
		{
			name: "union_with_order_by",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE TABLE t2(d, e)",
				"INSERT INTO t1 VALUES(1, 'one')",
				"INSERT INTO t1 VALUES(2, 'two')",
				"INSERT INTO t2 VALUES(1, 'two')",
				"INSERT INTO t2 VALUES(2, 'four')",
			},
			query: "SELECT a, b FROM t1 UNION SELECT d, e FROM t2 ORDER BY 1",
			want: [][]interface{}{
				{int64(1), "one"},
				{int64(1), "two"},
				{int64(2), "four"},
				{int64(2), "two"},
			},
		},
		{
			name: "intersect_with_order_by",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE TABLE t2(d, e)",
				"INSERT INTO t1 VALUES(1, 'one')",
				"INSERT INTO t1 VALUES(2, NULL)",
				"INSERT INTO t2 VALUES(2, NULL)",
				"INSERT INTO t2 VALUES(3, 'three')",
			},
			query: "SELECT a, b FROM t1 INTERSECT SELECT d, e FROM t2 ORDER BY 1",
			want: [][]interface{}{
				{int64(2), nil},
			},
		},
		{
			name: "except_with_order_by",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE TABLE t2(d, e)",
				"INSERT INTO t1 VALUES(1, 'one')",
				"INSERT INTO t1 VALUES(2, 'two')",
				"INSERT INTO t1 VALUES(3, NULL)",
				"INSERT INTO t2 VALUES(2, 'two')",
			},
			query: "SELECT a, b FROM t1 EXCEPT SELECT d, e FROM t2 ORDER BY 1",
			want: [][]interface{}{
				{int64(1), "one"},
				{int64(3), nil},
			},
		},

		// From select5.test - Aggregate functions with subqueries
		{
			name: "avg_empty_set",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT avg(x) FROM t1 WHERE x>100",
			want:  [][]interface{}{{nil}},
		},
		{
			name: "count_empty_set",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT count(x) FROM t1 WHERE x>100",
			want:  [][]interface{}{{int64(0)}},
		},
		{
			name: "min_empty_set",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT min(x) FROM t1 WHERE x>100",
			want:  [][]interface{}{{nil}},
		},
		{
			name: "max_empty_set",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT max(x) FROM t1 WHERE x>100",
			want:  [][]interface{}{{nil}},
		},
		{
			name: "sum_empty_set",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "SELECT sum(x) FROM t1 WHERE x>100",
			want:  [][]interface{}{{nil}},
		},

		// Correlated subqueries from select6.test
		{
			name: "correlated_subquery_in_select",
			setup: []string{
				"CREATE TABLE t1(w INT, x INT)",
				"CREATE TABLE t2(w INT, y VARCHAR(8))",
				"INSERT INTO t1(w,x) VALUES(1,10),(2,20),(3,30)",
				"INSERT INTO t2(w,y) VALUES(1,'one'),(2,'two'),(3,'three')",
			},
			query: `SELECT cnt, xyz, (SELECT y FROM t2 WHERE w=cnt) FROM
				(SELECT count(*) AS cnt, w AS xyz FROM t1 GROUP BY 2) ORDER BY cnt, xyz`,
			want: [][]interface{}{
				{int64(1), int64(1), "one"},
				{int64(1), int64(2), "two"},
				{int64(1), int64(3), "three"},
			},
		},
		{
			name: "correlated_subquery_with_function",
			setup: []string{
				"CREATE TABLE t1(w INT, x INT)",
				"CREATE TABLE t2(w INT, y VARCHAR(8))",
				"INSERT INTO t1(w,x) VALUES(1,10),(2,20)",
				"INSERT INTO t2(w,y) VALUES(1,'ONE'),(2,'TWO')",
			},
			query: `SELECT cnt, xyz, lower((SELECT y FROM t2 WHERE w=cnt)) FROM
				(SELECT count(*) AS cnt, w AS xyz FROM t1 GROUP BY 2) ORDER BY cnt, xyz`,
			want: [][]interface{}{
				{int64(1), int64(1), "one"},
				{int64(1), int64(2), "two"},
			},
		},
		{
			name: "correlated_subquery_in_where",
			setup: []string{
				"CREATE TABLE t1(w INT, x INT)",
				"CREATE TABLE t2(w INT, y VARCHAR(8))",
				"INSERT INTO t1(w,x) VALUES(1,10),(2,20),(3,30)",
				"INSERT INTO t2(w,y) VALUES(1,'one'),(2,'two'),(3,'three')",
			},
			query: `SELECT cnt, xyz FROM
				(SELECT count(*) AS cnt, w AS xyz FROM t1 GROUP BY 2)
				WHERE (SELECT y FROM t2 WHERE w=cnt)!='two'
				ORDER BY cnt, xyz`,
			want: [][]interface{}{
				{int64(1), int64(1)},
				{int64(1), int64(3)},
			},
		},
		{
			name: "correlated_subquery_in_order_by",
			setup: []string{
				"CREATE TABLE t1(w INT, x INT)",
				"CREATE TABLE t2(w INT, y VARCHAR(8))",
				"INSERT INTO t1(w,x) VALUES(1,10),(2,20),(3,30)",
				"INSERT INTO t2(w,y) VALUES(1,'one'),(2,'two'),(3,'three')",
			},
			query: `SELECT cnt, xyz FROM
				(SELECT count(*) AS cnt, w AS xyz FROM t1 GROUP BY 2)
				ORDER BY lower((SELECT y FROM t2 WHERE w=cnt))`,
			want: [][]interface{}{
				{int64(1), int64(1)},
				{int64(1), int64(3)},
				{int64(1), int64(2)},
			},
		},
		{
			name: "correlated_subquery_in_case",
			setup: []string{
				"CREATE TABLE t1(w INT, x INT)",
				"CREATE TABLE t2(w INT, y VARCHAR(8))",
				"INSERT INTO t1(w,x) VALUES(1,10),(2,20),(3,30)",
				"INSERT INTO t2(w,y) VALUES(1,'one'),(2,'two'),(3,'three')",
			},
			query: `SELECT cnt, xyz,
				CASE WHEN (SELECT y FROM t2 WHERE w=cnt)='two'
				THEN 'aaa' ELSE 'bbb' END
				FROM (SELECT count(*) AS cnt, w AS xyz FROM t1 GROUP BY 2)
				ORDER BY cnt`,
			want: [][]interface{}{
				{int64(1), int64(1), "bbb"},
				{int64(1), int64(2), "aaa"},
				{int64(1), int64(3), "bbb"},
			},
		},

		// Complex expressions in SELECT list
		{
			name: "arithmetic_expressions",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"INSERT INTO t1 VALUES(10, 5)",
				"INSERT INTO t1 VALUES(20, 4)",
			},
			query: "SELECT a+b, a-b, a*b, a/b FROM t1 ORDER BY a",
			want: [][]interface{}{
				{int64(15), int64(5), int64(50), int64(2)},
				{int64(24), int64(16), int64(80), int64(5)},
			},
		},
		{
			name: "string_concatenation",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"INSERT INTO t1 VALUES('hello', 'world')",
			},
			query: "SELECT a || ' ' || b FROM t1",
			want:  [][]interface{}{{"hello world"}},
		},
		{
			name: "case_expression",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
			},
			query: "SELECT CASE WHEN a=1 THEN 'one' WHEN a=2 THEN 'two' ELSE 'other' END FROM t1 ORDER BY a",
			want: [][]interface{}{
				{"one"}, {"two"}, {"other"},
			},
		},
		{
			name: "case_with_aggregate",
			setup: []string{
				"CREATE TABLE t3(a REAL)",
				"INSERT INTO t3 VALUES(44.0)",
				"INSERT INTO t3 VALUES(56.0)",
			},
			query: "SELECT (CASE WHEN a=0 THEN 0 ELSE (a + 25) / 50 END) AS categ, count(*) FROM t3 GROUP BY categ",
			want: [][]interface{}{
				{float64(1.38), int64(1)},
				{float64(1.62), int64(1)},
			},
		},

		// Nested subqueries
		{
			name: "triple_nested_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE x=1))",
			want:  [][]interface{}{{int64(1)}},
		},
		{
			name: "nested_aggregate_subquery",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
				"INSERT INTO t1 VALUES(2, 2)",
				"INSERT INTO t1 VALUES(3, 2)",
			},
			query: `SELECT * FROM
				(SELECT a.q, a.p, b.r FROM
					(SELECT count(*) as p, y as q FROM t1 GROUP BY y) AS a,
					(SELECT max(x) as r, y as s FROM t1 GROUP BY y) as b
				WHERE a.q=b.s ORDER BY a.q)`,
			want: [][]interface{}{
				{int64(1), int64(1), int64(1)},
				{int64(2), int64(2), int64(3)},
			},
		},

		// Subquery with LIMIT
		{
			name: "subquery_with_limit",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
			},
			query: "SELECT x FROM (SELECT x FROM t1 LIMIT 2)",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},
		{
			name: "subquery_with_limit_offset",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t1 VALUES(4)",
			},
			query: "SELECT x FROM (SELECT x FROM t1 LIMIT 2 OFFSET 1)",
			want: [][]interface{}{
				{int64(2)}, {int64(3)},
			},
		},
		{
			name: "outer_limit_on_subquery",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t1 VALUES(4)",
			},
			query: "SELECT x FROM (SELECT x FROM t1) LIMIT 2",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},
		{
			name: "nested_limit",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t1 VALUES(4)",
			},
			query: "SELECT x FROM (SELECT x FROM t1 LIMIT 3) LIMIT 2",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},

		// Subquery with no FROM clause
		{
			name: "subquery_no_from",
			setup: []string{},
			query: "SELECT * FROM (SELECT 1)",
			want:  [][]interface{}{{int64(1)}},
		},
		{
			name: "subquery_no_from_multiple_columns",
			setup: []string{},
			query: "SELECT c,b,a FROM (SELECT 1 AS 'a', 2 AS 'b', 'abc' AS 'c')",
			want:  [][]interface{}{{"abc", int64(2), int64(1)}},
		},
		{
			name: "subquery_no_from_with_where_false",
			setup: []string{},
			query: "SELECT * FROM (SELECT 1 AS 'a', 2 AS 'b' WHERE 0)",
			want:  [][]interface{}{},
		},

		// WHERE clause with subquery
		{
			name: "where_exists_subquery",
			setup: []string{
				"CREATE TABLE t1(a)",
				"CREATE TABLE t2(b)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t2 VALUES(1)",
			},
			query: "SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2 WHERE b=a)",
			want:  [][]interface{}{{int64(1)}},
		},
		{
			name: "where_not_exists_subquery",
			setup: []string{
				"CREATE TABLE photo(pk integer primary key, x)",
				"CREATE TABLE tag(pk integer primary key, fk int, name)",
				"INSERT INTO photo VALUES(1,1)",
				"INSERT INTO photo VALUES(2,2)",
				"INSERT INTO photo VALUES(3,3)",
				"INSERT INTO tag VALUES(11,1,'one')",
				"INSERT INTO tag VALUES(12,1,'two')",
			},
			query: `SELECT P.pk from photo P WHERE NOT EXISTS (
				SELECT T2.pk from tag T2 WHERE T2.fk = P.pk
				EXCEPT
				SELECT T3.pk from tag T3 WHERE T3.fk = P.pk AND T3.name LIKE '%foo%'
			)`,
			want: [][]interface{}{{int64(2)}, {int64(3)}},
		},

		// NULL handling in GROUP BY
		{
			name: "null_groupby",
			setup: []string{
				"CREATE TABLE t3(x,y)",
				"INSERT INTO t3 VALUES(1,NULL)",
				"INSERT INTO t3 VALUES(2,NULL)",
				"INSERT INTO t3 VALUES(3,4)",
			},
			query: "SELECT count(x), y FROM t3 GROUP BY y ORDER BY 1",
			want: [][]interface{}{
				{int64(1), int64(4)},
				{int64(2), nil},
			},
		},
		{
			name: "null_groupby_multiple",
			setup: []string{
				"CREATE TABLE t4(x,y,z)",
				"INSERT INTO t4 VALUES(1,2,NULL)",
				"INSERT INTO t4 VALUES(2,3,NULL)",
				"INSERT INTO t4 VALUES(3,NULL,5)",
				"INSERT INTO t4 VALUES(4,NULL,6)",
			},
			query: "SELECT max(x), count(x), y, z FROM t4 GROUP BY y, z ORDER BY 1",
			want: [][]interface{}{
				{int64(1), int64(1), int64(2), nil},
				{int64(2), int64(1), int64(3), nil},
				{int64(3), int64(1), nil, int64(5)},
				{int64(4), int64(1), nil, int64(6)},
			},
		},

		// Compound WHERE clauses
		{
			name: "where_with_union",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE TABLE t2(d, e)",
				"INSERT INTO t1 VALUES(1, 'one')",
				"INSERT INTO t1 VALUES(2, 'two')",
				"INSERT INTO t1 VALUES(3, 'three')",
				"INSERT INTO t2 VALUES(2, 'TWO')",
				"INSERT INTO t2 VALUES(4, 'FOUR')",
			},
			query: "SELECT * FROM t1 WHERE a<3 UNION SELECT * FROM t2 WHERE d>=2 ORDER BY 1",
			want: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "TWO"},
				{int64(2), "two"},
				{int64(4), "FOUR"},
			},
		},
		{
			name: "where_with_except",
			setup: []string{
				"CREATE TABLE t1(a)",
				"CREATE TABLE t2(d)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
			},
			query: "SELECT a FROM t1 WHERE a<8 EXCEPT SELECT d FROM t2 WHERE d<=3 ORDER BY 1",
			want:  [][]interface{}{{int64(3)}},
		},
		{
			name: "where_with_intersect",
			setup: []string{
				"CREATE TABLE t1(a)",
				"CREATE TABLE t2(d)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t2 VALUES(4)",
			},
			query: "SELECT a FROM t1 WHERE a<8 INTERSECT SELECT d FROM t2 WHERE d<=3 ORDER BY 1",
			want: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},

		// Additional complex cases
		{
			name: "union_all_limit_offset",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM (SELECT * FROM (SELECT * FROM t1 LIMIT 1) UNION ALL SELECT * from t1)",
			want: [][]interface{}{
				{int64(1)}, {int64(1)}, {int64(2)},
			},
		},
		{
			name: "where_false_union",
			setup: []string{
				"CREATE TABLE t61(a)",
				"CREATE TABLE t62(b)",
				"INSERT INTO t61 VALUES(111)",
				"INSERT INTO t62 VALUES(222)",
			},
			query: "SELECT a FROM t61 WHERE 0 UNION SELECT b FROM t62",
			want:  [][]interface{}{{int64(222)}},
		},
		{
			name: "where_false_union_all",
			setup: []string{
				"CREATE TABLE t61(a)",
				"CREATE TABLE t62(b)",
				"INSERT INTO t61 VALUES(111)",
				"INSERT INTO t62 VALUES(222)",
			},
			query: "SELECT a FROM t61 WHERE 0 UNION ALL SELECT b FROM t62",
			want:  [][]interface{}{{int64(222)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupMemoryDB(t)
			defer db.Close()

			// Execute setup statements
			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			// Query and compare results
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteSelectAdvancedErrors tests error cases from TCL tests
func TestSQLiteSelectAdvancedErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup []string
		query string
	}{
		{
			name: "subquery_wrong_column_count_in",
			setup: []string{
				"CREATE TABLE t2(a,b)",
			},
			query: "SELECT 5 IN (SELECT a,b FROM t2)",
		},
		{
			name: "subquery_wrong_column_count_union",
			setup: []string{
				"CREATE TABLE t2(a,b)",
			},
			query: "SELECT 5 IN (SELECT a,b FROM t2 UNION SELECT b,a FROM t2)",
		},
		{
			name: "union_column_mismatch",
			setup: []string{
				"CREATE TABLE t(i,j,k)",
				"CREATE TABLE j(l,m)",
			},
			query: "SELECT * FROM t UNION ALL SELECT * FROM j",
		},
		{
			name: "no_such_column_groupby",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 1)",
			},
			query: "SELECT y, count(*) FROM t1 GROUP BY z ORDER BY y",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupMemoryDB(t)
			defer db.Close()

			// Execute setup statements
			execSQL(t, db, tt.setup...)

			// Expect an error
			expectQueryError(t, db, tt.query)
		})
	}
}
