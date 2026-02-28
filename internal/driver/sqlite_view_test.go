package driver

import (
	"testing"
)

// TestSQLiteView tests VIEW operations converted from SQLite TCL tests
// Covers: view.test and view2.test
func TestSQLiteView(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
	}{
		// Basic view creation and selection (view-1.0, view-1.1)
		{
			name: "view-1.0 basic table setup",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(4,5,6)",
				"INSERT INTO t1 VALUES(7,8,9)",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(4), int64(5), int64(6)},
				{int64(7), int64(8), int64(9)},
			},
		},
		{
			name: "view-1.1 create view if not exists and select",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(4,5,6)",
				"INSERT INTO t1 VALUES(7,8,9)",
				"CREATE VIEW IF NOT EXISTS v1 AS SELECT a,b FROM t1",
			},
			query: "SELECT * FROM v1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
				{int64(4), int64(5)},
				{int64(7), int64(8)},
			},
		},
		{
			name: "view-1.3 basic view creation",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(4,5,6)",
				"INSERT INTO t1 VALUES(7,8,9)",
				"CREATE VIEW v1 AS SELECT a,b FROM t1",
			},
			query: "SELECT * FROM v1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
				{int64(4), int64(5)},
				{int64(7), int64(8)},
			},
		},
		// Views with expressions (view-3.3.1, view-3.3.2, view-3.3.3)
		{
			name: "view-3.3.1 view with expressions and aliases",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1",
			},
			query: "SELECT xyz, pqr FROM v1 ORDER BY xyz LIMIT 1",
			wantRows: [][]interface{}{
				{int64(2), int64(7)},
			},
		},
		{
			name: "view-3.3.2 view with table prefix and expression",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"CREATE VIEW v1b AS SELECT t1.a, b+c, t1.c FROM t1",
			},
			query: "SELECT * FROM v1b LIMIT 1",
			wantRows: [][]interface{}{
				{int64(2), int64(7), int64(4)},
			},
		},
		{
			name: "view-3.3.3 view with explicit column names",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"CREATE VIEW v1c(x,y,z) AS SELECT a, b+c, c-b FROM t1",
			},
			query: "SELECT * FROM v1c LIMIT 1",
			wantRows: [][]interface{}{
				{int64(2), int64(7), int64(1)},
			},
		},
		// Views with WHERE clause (view-2.1, view-2.5, view-2.6)
		{
			name: "view-2.1 view with where clause",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"CREATE VIEW v2 AS SELECT * FROM t1 WHERE a>5",
			},
			query: "SELECT * FROM v2",
			wantRows: [][]interface{}{
				{int64(7), int64(8), int64(9), int64(10)},
			},
		},
		{
			name: "view-2.5 additional data in view with where",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v2 AS SELECT * FROM t1 WHERE a>5",
			},
			query: "SELECT * FROM v2 ORDER BY x",
			wantRows: [][]interface{}{
				{int64(7), int64(8), int64(9), int64(10)},
				{int64(11), int64(12), int64(13), int64(14)},
			},
		},
		{
			name: "view-2.6 querying view with additional where",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v2 AS SELECT * FROM t1 WHERE a>5",
			},
			query: "SELECT x FROM v2 WHERE a>10",
			wantRows: [][]interface{}{
				{int64(11)},
			},
		},
		// Views with JOINs (view-5.2, view-7.1, view-7.3, view-7.5)
		{
			name: "view-5.2 view with inner join using",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"CREATE TABLE t2(y,a)",
				"INSERT INTO t2 VALUES(22,2)",
				"INSERT INTO t2 VALUES(55,5)",
				"CREATE VIEW v5 AS SELECT t1.x AS v, t2.y AS w FROM t1 JOIN t2 USING(a)",
			},
			query: "SELECT * FROM v5",
			wantRows: [][]interface{}{
				{int64(1), int64(22)},
				{int64(4), int64(55)},
			},
		},
		{
			name: "view-7.1 view with join on",
			setup: []string{
				"CREATE TABLE test1(id integer primary key, a)",
				"CREATE TABLE test2(id integer, b)",
				"INSERT INTO test1 VALUES(1,2)",
				"INSERT INTO test2 VALUES(1,3)",
				"CREATE VIEW test AS SELECT test1.id, a, b FROM test1 JOIN test2 ON test2.id=test1.id",
			},
			query: "SELECT * FROM test",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
			},
		},
		{
			name: "view-7.3 view with join using",
			setup: []string{
				"CREATE TABLE test1(id integer primary key, a)",
				"CREATE TABLE test2(id integer, b)",
				"INSERT INTO test1 VALUES(1,2)",
				"INSERT INTO test2 VALUES(1,3)",
				"CREATE VIEW test AS SELECT test1.id, a, b FROM test1 JOIN test2 USING(id)",
			},
			query: "SELECT * FROM test",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
			},
		},
		{
			name: "view-7.5 view with natural join",
			setup: []string{
				"CREATE TABLE test1(id integer primary key, a)",
				"CREATE TABLE test2(id integer, b)",
				"INSERT INTO test1 VALUES(1,2)",
				"INSERT INTO test2 VALUES(1,3)",
				"CREATE VIEW test AS SELECT test1.id, a, b FROM test1 NATURAL JOIN test2",
			},
			query: "SELECT * FROM test",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
			},
		},
		// Views with aggregates (view-6.1, view-6.2)
		{
			name: "view-6.1 min aggregate in view",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v2 AS SELECT * FROM t1 WHERE a>5",
			},
			query: "SELECT min(x), min(a), min(b), min(c), min(a+b+c) FROM v2",
			wantRows: [][]interface{}{
				{int64(7), int64(8), int64(9), int64(10), int64(27)},
			},
		},
		{
			name: "view-6.2 max aggregate in view",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v2 AS SELECT * FROM t1 WHERE a>5",
			},
			query: "SELECT max(x), max(a), max(b), max(c), max(a+b+c) FROM v2",
			wantRows: [][]interface{}{
				{int64(11), int64(12), int64(13), int64(14), int64(39)},
			},
		},
		// Nested views (view-8.1, view-8.3, view-18.1)
		{
			name: "view-8.1 view referencing another view",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1",
				"CREATE VIEW v6 AS SELECT pqr, xyz FROM v1",
			},
			query: "SELECT * FROM v6 ORDER BY xyz",
			wantRows: [][]interface{}{
				{int64(7), int64(2)},
				{int64(13), int64(5)},
				{int64(19), int64(8)},
				{int64(27), int64(12)},
			},
		},
		{
			name: "view-8.3 nested view with expression",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1",
				"CREATE VIEW v6 AS SELECT pqr, xyz FROM v1",
				"CREATE VIEW v7(a) AS SELECT pqr+xyz FROM v6",
			},
			query: "SELECT * FROM v7 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(9)},
				{int64(18)},
				{int64(27)},
				{int64(39)},
			},
		},
		{
			name: "view-18.1 deeply nested views (5 levels)",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(4, 5, 6)",
				"CREATE VIEW vv1 AS SELECT * FROM t1",
				"CREATE VIEW vv2 AS SELECT * FROM vv1",
				"CREATE VIEW vv3 AS SELECT * FROM vv2",
				"CREATE VIEW vv4 AS SELECT * FROM vv3",
				"CREATE VIEW vv5 AS SELECT * FROM vv4",
			},
			query: "SELECT * FROM vv5",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(4), int64(5), int64(6)},
			},
		},
		// Views with subqueries (view-8.4, view-8.6, view-8.7)
		{
			name: "view-8.4 view with subquery and group by",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"INSERT INTO t1 VALUES(4,5,6)",
				"CREATE VIEW v8 AS SELECT max(cnt) AS mx FROM (SELECT a%2 AS eo, count(*) AS cnt FROM t1 GROUP BY eo)",
			},
			query: "SELECT * FROM v8",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		{
			name: "view-8.6 join view with subquery",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1",
				"CREATE VIEW v6 AS SELECT pqr, xyz FROM v1",
				"CREATE TABLE t2(a,b,c)",
				"INSERT INTO t2 VALUES(1,2,3)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
				"INSERT INTO t2 VALUES(4,5,6)",
				"CREATE VIEW v8 AS SELECT max(cnt) AS mx FROM (SELECT a%2 AS eo, count(*) AS cnt FROM t2 GROUP BY eo)",
			},
			query: "SELECT mx+10, pqr FROM v6, v8 WHERE xyz=2",
			wantRows: [][]interface{}{
				{int64(12), int64(7)},
			},
		},
		{
			name: "view-8.7 join view with subquery multiple rows",
			setup: []string{
				"CREATE TABLE t1(x,a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3,4)",
				"INSERT INTO t1 VALUES(4,5,6,7)",
				"INSERT INTO t1 VALUES(7,8,9,10)",
				"INSERT INTO t1 VALUES(11,12,13,14)",
				"CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1",
				"CREATE VIEW v6 AS SELECT pqr, xyz FROM v1",
				"CREATE TABLE t2(a,b,c)",
				"INSERT INTO t2 VALUES(1,2,3)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
				"INSERT INTO t2 VALUES(4,5,6)",
				"CREATE VIEW v8 AS SELECT max(cnt) AS mx FROM (SELECT a%2 AS eo, count(*) AS cnt FROM t2 GROUP BY eo)",
			},
			query: "SELECT mx+10, pqr FROM v6, v8 WHERE xyz>2 ORDER BY pqr",
			wantRows: [][]interface{}{
				{int64(12), int64(13)},
				{int64(12), int64(19)},
				{int64(12), int64(27)},
			},
		},
		// Views with ORDER BY and LIMIT (view-9.3, view-9.4, view-9.5, view-9.6)
		{
			name: "view-9.3 view with order by and limit",
			setup: []string{
				"CREATE TABLE t2(y,a)",
				"INSERT INTO t2 VALUES(22,2)",
				"INSERT INTO t2 VALUES(33,3)",
				"INSERT INTO t2 VALUES(44,4)",
				"INSERT INTO t2 VALUES(55,5)",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<5",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<4",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<3",
				"CREATE VIEW v9 AS SELECT DISTINCT count(*) FROM t2 GROUP BY a ORDER BY 1 LIMIT 3",
			},
			query: "SELECT * FROM v9",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(4)},
			},
		},
		{
			name: "view-9.4 select from view with order by desc",
			setup: []string{
				"CREATE TABLE t2(y,a)",
				"INSERT INTO t2 VALUES(22,2)",
				"INSERT INTO t2 VALUES(33,3)",
				"INSERT INTO t2 VALUES(44,4)",
				"INSERT INTO t2 VALUES(55,5)",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<5",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<4",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<3",
				"CREATE VIEW v9 AS SELECT DISTINCT count(*) FROM t2 GROUP BY a ORDER BY 1 LIMIT 3",
			},
			query: "SELECT * FROM v9 ORDER BY 1 DESC",
			wantRows: [][]interface{}{
				{int64(4)},
				{int64(2)},
				{int64(1)},
			},
		},
		{
			name: "view-9.5 view with columns and order by",
			setup: []string{
				"CREATE TABLE t2(y,a)",
				"INSERT INTO t2 VALUES(22,2)",
				"INSERT INTO t2 VALUES(33,3)",
				"INSERT INTO t2 VALUES(44,4)",
				"INSERT INTO t2 VALUES(55,5)",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<5",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<4",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<3",
				"CREATE VIEW v10 AS SELECT DISTINCT a, count(*) FROM t2 GROUP BY a ORDER BY 2 LIMIT 3",
			},
			query: "SELECT * FROM v10",
			wantRows: [][]interface{}{
				{int64(5), int64(1)},
				{int64(4), int64(2)},
				{int64(3), int64(4)},
			},
		},
		{
			name: "view-9.6 select from view with different order",
			setup: []string{
				"CREATE TABLE t2(y,a)",
				"INSERT INTO t2 VALUES(22,2)",
				"INSERT INTO t2 VALUES(33,3)",
				"INSERT INTO t2 VALUES(44,4)",
				"INSERT INTO t2 VALUES(55,5)",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<5",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<4",
				"INSERT INTO t2 SELECT * FROM t2 WHERE a<3",
				"CREATE VIEW v10 AS SELECT DISTINCT a, count(*) FROM t2 GROUP BY a ORDER BY 2 LIMIT 3",
			},
			query: "SELECT * FROM v10 ORDER BY 1",
			wantRows: [][]interface{}{
				{int64(3), int64(4)},
				{int64(4), int64(2)},
				{int64(5), int64(1)},
			},
		},
		// Views with quoted column names (view-10.1, view-10.2)
		{
			name: "view-10.1 view with quoted column names",
			setup: []string{
				`CREATE TABLE t3("9" integer, [4] text)`,
				"INSERT INTO t3 VALUES(1,2)",
				`CREATE VIEW v_t3_a AS SELECT a.[9] FROM t3 AS a`,
			},
			query: "SELECT * FROM v_t3_a",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name: "view-10.2 view with bracket quoted column",
			setup: []string{
				`CREATE TABLE t3("9" integer, [4] text)`,
				"INSERT INTO t3 VALUES(1,2)",
				`CREATE VIEW v_t3_b AS SELECT "4" FROM t3`,
			},
			query: "SELECT * FROM v_t3_b",
			wantRows: [][]interface{}{
				{"2"},
			},
		},
		// Views with collation (view-11.3)
		{
			name: "view-11.3 view preserves collation",
			setup: []string{
				"CREATE TABLE t4(a COLLATE NOCASE)",
				"INSERT INTO t4 VALUES('This')",
				"INSERT INTO t4 VALUES('this')",
				"INSERT INTO t4 VALUES('THIS')",
				"CREATE VIEW v11 AS SELECT * FROM t4",
			},
			query: "SELECT * FROM v11 WHERE a = 'THIS'",
			wantRows: [][]interface{}{
				{"This"},
				{"this"},
				{"THIS"},
			},
		},
		// Views with ROWID (view-19.1)
		{
			name: "view-19.1 view with rowid",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(4, 5, 6)",
				"CREATE VIEW v3308a AS SELECT rowid, * FROM t1",
			},
			query: "SELECT * FROM v3308a",
			wantRows: [][]interface{}{
				{int64(1), int64(1), int64(2), int64(3)},
				{int64(2), int64(4), int64(5), int64(6)},
			},
		},
		// Views with empty column names (view-22.1)
		{
			name: "view-22.1 view with empty column names",
			setup: []string{
				"CREATE VIEW x1 AS SELECT 123 AS '', 234 AS '', 345 AS ''",
			},
			query: "SELECT * FROM x1",
			wantRows: [][]interface{}{
				{int64(123), int64(234), int64(345)},
			},
		},
		// Views with aggregates and group by (view-26.0)
		{
			name: "view-26.0 view with max/min and group by",
			setup: []string{
				"CREATE TABLE t16(a, b, c UNIQUE)",
				"INSERT INTO t16 VALUES(1, 1, 1)",
				"INSERT INTO t16 VALUES(2, 2, 2)",
				"INSERT INTO t16 VALUES(3, 3, 3)",
				"CREATE VIEW v16 AS SELECT max(a) AS mx, min(b) AS mn FROM t16 GROUP BY c",
			},
			query: "SELECT * FROM v16 AS one, v16 AS two WHERE one.mx=1",
			wantRows: [][]interface{}{
				{int64(1), int64(1), int64(1), int64(1)},
				{int64(1), int64(1), int64(2), int64(2)},
				{int64(1), int64(1), int64(3), int64(3)},
			},
		},
		// View with AVG aggregate (view-27.1, view-27.2, view-27.3)
		{
			name: "view-27.1 view with avg and type preservation",
			setup: []string{
				"CREATE TABLE t0(c0 TEXT, c1)",
				"INSERT INTO t0(c0, c1) VALUES (-1, 0)",
				"CREATE VIEW v0(c0, c1) AS SELECT t0.c0, AVG(t0.c1) FROM t0",
			},
			query: "SELECT c0, c1 FROM v0",
			wantRows: [][]interface{}{
				{"-1", 0.0},
			},
		},
		{
			name: "view-27.2 comparison in view result",
			setup: []string{
				"CREATE TABLE t0(c0 TEXT, c1)",
				"INSERT INTO t0(c0, c1) VALUES (-1, 0)",
				"CREATE VIEW v0(c0, c1) AS SELECT t0.c0, AVG(t0.c1) FROM t0",
			},
			query: "SELECT c0<c1 FROM v0",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name: "view-27.3 reverse comparison in view result",
			setup: []string{
				"CREATE TABLE t0(c0 TEXT, c1)",
				"INSERT INTO t0(c0, c1) VALUES (-1, 0)",
				"CREATE VIEW v0(c0, c1) AS SELECT t0.c0, AVG(t0.c1) FROM t0",
			},
			query: "SELECT c1<c0 FROM v0",
			wantRows: [][]interface{}{
				{int64(0)},
			},
		},
		// View with WHERE on text column (view-28.1, view-28.2)
		{
			name: "view-28.1 IN clause with text column from table",
			setup: []string{
				"CREATE TABLE t0(c0 TEXT)",
				"INSERT INTO t0(c0) VALUES ('0')",
				"CREATE VIEW v0(c0) AS SELECT t0.c0 FROM t0",
			},
			query: "SELECT 0 IN (c0) FROM t0",
			wantRows: [][]interface{}{
				{int64(0)},
			},
		},
		{
			name: "view-28.2 IN clause with text column from view",
			setup: []string{
				"CREATE TABLE t0(c0 TEXT)",
				"INSERT INTO t0(c0) VALUES ('0')",
				"CREATE VIEW v0(c0) AS SELECT t0.c0 FROM t0",
			},
			query: "SELECT 0 IN (c0) FROM v0",
			wantRows: [][]interface{}{
				{int64(0)},
			},
		},
		// View from view2.test with CTE (view2-1.1)
		{
			name: "view2-1.1 view with CTE",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 2)",
				"CREATE VIEW v1 AS SELECT * FROM (WITH x1 AS (SELECT y, x FROM t1) SELECT * FROM x1)",
			},
			query: "SELECT * FROM v1",
			wantRows: [][]interface{}{
				{int64(2), int64(1)},
			},
		},
		{
			name: "view2-1.2 view with main prefix ignores CTE",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 2)",
				"CREATE VIEW v3 AS SELECT * FROM main.t1",
			},
			query: "WITH t1(a, b) AS (SELECT 3, 4) SELECT * FROM v3",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "view2-1.3 view without main prefix ignores CTE",
			setup: []string{
				"CREATE TABLE t1(x, y)",
				"INSERT INTO t1 VALUES(1, 2)",
				"CREATE VIEW v2 AS SELECT * FROM t1",
			},
			query: "WITH t1(a, b) AS (SELECT 3, 4) SELECT * FROM v2",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := setupMemoryDB(t)
			defer db.Close()

			// Run setup statements
			execSQL(t, db, tt.setup...)

			// Execute the test query
			if tt.wantErr {
				_, err := db.Query(tt.query)
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			// Get results and compare
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// TestSQLiteViewDropIfExists tests DROP VIEW IF EXISTS
func TestSQLiteViewDropIfExists(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// view-16.3: DROP VIEW IF EXISTS on non-existent view should succeed
	_, err := db.Exec("DROP VIEW IF EXISTS nosuchview")
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS failed: %v", err)
	}

	// view-1.4: Create view, drop with IF EXISTS, verify it's gone
	_, err = db.Exec("CREATE TABLE t1(a,b,c)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("CREATE VIEW v1 AS SELECT a,b FROM t1")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	_, err = db.Exec("DROP VIEW IF EXISTS v1")
	if err != nil {
		t.Fatalf("DROP VIEW IF EXISTS failed: %v", err)
	}

	// Verify view is gone
	_, err = db.Query("SELECT * FROM v1")
	if err == nil {
		t.Error("Expected error querying dropped view")
	}
}

// TestSQLiteViewCreateIfNotExists tests CREATE VIEW IF NOT EXISTS
func TestSQLiteViewCreateIfNotExists(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// view-16.1: CREATE VIEW IF NOT EXISTS on existing view should succeed
	_, err := db.Exec("CREATE TABLE t1(a,b,c)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1,2,3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("CREATE VIEW v1 AS SELECT a AS 'xyz', b+c AS 'pqr', c-b FROM t1")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	_, err = db.Exec("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t1")
	if err != nil {
		t.Errorf("CREATE VIEW IF NOT EXISTS failed: %v", err)
	}

	// view-16.2: Verify the original view definition is unchanged
	var sql string
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE name='v1'").Scan(&sql)
	if err != nil {
		t.Fatalf("Failed to query sqlite_master: %v", err)
	}

	// The original view should still exist (IF NOT EXISTS doesn't replace)
	if !containsView(sql, "xyz") {
		t.Errorf("Expected original view definition with 'xyz', got: %s", sql)
	}
}

// TestSQLiteViewTableDrop tests that dropping a table affects dependent views
func TestSQLiteViewTableDrop(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// view-1.6, view-1.7: Drop table, recreate with different schema
	_, err := db.Exec("CREATE TABLE t1(a,b,c)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1,2,3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("CREATE VIEW v1 AS SELECT a,b FROM t1")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	_, err = db.Exec("DROP TABLE t1")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	// View still exists but querying should fail
	_, err = db.Query("SELECT * FROM v1")
	if err == nil {
		t.Error("Expected error querying view after table drop")
	}

	// Recreate table with different schema
	_, err = db.Exec("CREATE TABLE t1(x,a,b,c)")
	if err != nil {
		t.Fatalf("CREATE TABLE (2nd) failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1,2,3,4)")
	if err != nil {
		t.Fatalf("INSERT (2nd) failed: %v", err)
	}

	// Now the view should work with new schema
	rows, err := db.Query("SELECT * FROM v1 ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var a, b int64
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if a != 2 || b != 3 {
			t.Errorf("Expected a=2, b=3, got a=%d, b=%d", a, b)
		}
	} else {
		t.Error("Expected at least one row")
	}
}

// Helper function to check if a string contains a substring
func containsView(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && hasSubstring(s, substr)))
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
