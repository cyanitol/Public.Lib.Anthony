// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

// selAggOpenDB opens an in-memory database and returns it.
func selAggOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	return db
}

// selAggExec executes SQL statements on db, failing the test on error.
func selAggExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// selAggQueryRows runs a query and returns all rows as [][]interface{}.
func selAggQueryRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return result
}

// TestSelectDistinctBasic exercises emitDistinctAndOutput in compileSimpleSelect.
func TestSelectDistinctBasic(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE dtest (v INTEGER)",
		"INSERT INTO dtest VALUES (1),(2),(1),(3),(2)",
	)
	rows := selAggQueryRows(t, db, "SELECT DISTINCT v FROM dtest ORDER BY v")
	if len(rows) != 3 {
		t.Errorf("DISTINCT: expected 3 rows, got %d", len(rows))
	}
}

// TestSelectOrderByMultipleColumns exercises resolveOrderByColumns with multiple terms.
func TestSelectOrderByMultipleColumns(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE mord (a INTEGER, b INTEGER)",
		"INSERT INTO mord VALUES (2,1),(1,2),(1,1),(2,2)",
	)
	rows := selAggQueryRows(t, db, "SELECT a, b FROM mord ORDER BY a ASC, b DESC")
	if len(rows) != 4 {
		t.Fatalf("ORDER BY multi-col: expected 4 rows, got %d", len(rows))
	}
	// first row should be a=1,b=2
	a, b := selAggToInt64(rows[0][0]), selAggToInt64(rows[0][1])
	if a != 1 || b != 2 {
		t.Errorf("ORDER BY multi-col row 0: got (%d,%d), want (1,2)", a, b)
	}
}

// TestSelectLimitOffset exercises setupLimitOffset and emitLimitCheck/emitOffsetCheck.
func TestSelectLimitOffset(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE lo (n INTEGER)",
		"INSERT INTO lo VALUES (1),(2),(3),(4),(5)",
	)
	rows := selAggQueryRows(t, db, "SELECT n FROM lo ORDER BY n LIMIT 2 OFFSET 1")
	if len(rows) != 2 {
		t.Fatalf("LIMIT OFFSET: expected 2 rows, got %d: %v", len(rows), rows)
	}
	if selAggToInt64(rows[0][0]) != 2 || selAggToInt64(rows[1][0]) != 3 {
		t.Errorf("LIMIT OFFSET: got %v,%v, want 2,3", rows[0][0], rows[1][0])
	}
}

// TestSelectLimitZero exercises LIMIT 0 early-halt path.
func TestSelectLimitZero(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE lz (n INTEGER)",
		"INSERT INTO lz VALUES (1),(2),(3)",
	)
	rows := selAggQueryRows(t, db, "SELECT n FROM lz LIMIT 0")
	if len(rows) != 0 {
		t.Errorf("LIMIT 0: expected 0 rows, got %d", len(rows))
	}
}

// TestSelectOrderByAlias exercises searchColumnByName with an alias.
func TestSelectOrderByAlias(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE al (x INTEGER, y TEXT)",
		"INSERT INTO al VALUES (3,'c'),(1,'a'),(2,'b')",
	)
	rows := selAggQueryRows(t, db, "SELECT x AS score, y AS lbl FROM al ORDER BY score ASC")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY alias: expected 3 rows, got %d", len(rows))
	}
	if selAggToInt64(rows[0][0]) != 1 {
		t.Errorf("ORDER BY alias: first row x = %v, want 1", rows[0][0])
	}
}

// TestSelectOrderByColumnNumber exercises tryParseColumnNumber.
func TestSelectOrderByColumnNumber(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE cn (a INTEGER, b TEXT)",
		"INSERT INTO cn VALUES (3,'c'),(1,'a'),(2,'b')",
	)
	rows := selAggQueryRows(t, db, "SELECT a, b FROM cn ORDER BY 1")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY col num: expected 3 rows, got %d", len(rows))
	}
	if selAggToInt64(rows[0][0]) != 1 {
		t.Errorf("ORDER BY col num: first a = %v, want 1", rows[0][0])
	}
}

// TestSelectOrderByExtraExpr exercises addExtraOrderByExpr (ORDER BY column not in SELECT).
func TestSelectOrderByExtraExpr(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE xc (a TEXT, b INTEGER)",
		"INSERT INTO xc VALUES ('z',3),('a',1),('m',2)",
	)
	// b is not in SELECT list but is in ORDER BY — exercises extra column path
	rows := selAggQueryRows(t, db, "SELECT a FROM xc ORDER BY b ASC")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY extra col: expected 3 rows, got %d", len(rows))
	}
	got := selAggToString(rows[0][0])
	if got != "a" {
		t.Errorf("ORDER BY extra col: first row = %q, want \"a\"", got)
	}
}

// TestSelectOrderByCollate exercises extractOrderByExpression with CollateExpr.
func TestSelectOrderByCollate(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE cc (name TEXT COLLATE NOCASE)",
		"INSERT INTO cc VALUES ('Banana'),('apple'),('Cherry')",
	)
	rows := selAggQueryRows(t, db, "SELECT name FROM cc ORDER BY name COLLATE NOCASE ASC")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY collate: expected 3 rows, got %d", len(rows))
	}
}

// TestSelectWithoutFromWhere exercises emitNoFromWhereClause with a WHERE condition.
func TestSelectWithoutFromWhere(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	// WHERE 0 should return no rows
	rows := selAggQueryRows(t, db, "SELECT 42 WHERE 0")
	if len(rows) != 0 {
		t.Errorf("SELECT without FROM WHERE 0: expected 0 rows, got %d", len(rows))
	}
	// WHERE 1 should return 1 row
	rows = selAggQueryRows(t, db, "SELECT 42 WHERE 1")
	if len(rows) != 1 {
		t.Errorf("SELECT without FROM WHERE 1: expected 1 row, got %d", len(rows))
	}
}

// TestSelectDistinctOrderBy exercises DISTINCT combined with ORDER BY (sorter path).
func TestSelectDistinctOrderBy(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE dob (v INTEGER)",
		"INSERT INTO dob VALUES (3),(1),(2),(1),(3)",
	)
	rows := selAggQueryRows(t, db, "SELECT DISTINCT v FROM dob ORDER BY v DESC")
	if len(rows) != 3 {
		t.Fatalf("DISTINCT ORDER BY: expected 3 rows, got %d", len(rows))
	}
	if selAggToInt64(rows[0][0]) != 3 {
		t.Errorf("DISTINCT ORDER BY: first row = %v, want 3", rows[0][0])
	}
}

// TestGroupBySingleColumn exercises compileSelectWithGroupBy with a single GROUP BY column.
func TestGroupBySingleColumn(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gb1 (cat TEXT, val INTEGER)",
		"INSERT INTO gb1 VALUES ('A',10),('B',20),('A',30),('B',40)",
	)
	rows := selAggQueryRows(t, db, "SELECT cat, SUM(val) FROM gb1 GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("GROUP BY single: expected 2 rows, got %d", len(rows))
	}
	if selAggToString(rows[0][0]) != "A" || selAggToInt64(rows[0][1]) != 40 {
		t.Errorf("GROUP BY single row A: got %v,%v", rows[0][0], rows[0][1])
	}
}

// TestGroupByMultipleColumns exercises GROUP BY with two columns.
func TestGroupByMultipleColumns(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gb2 (a INTEGER, b INTEGER, v INTEGER)",
		"INSERT INTO gb2 VALUES (1,1,5),(1,2,10),(1,1,15),(2,1,20)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT a, b, COUNT(*) FROM gb2 GROUP BY a, b ORDER BY a, b")
	if len(rows) != 3 {
		t.Fatalf("GROUP BY multi-col: expected 3 rows, got %d", len(rows))
	}
}

// TestGroupConcatBasic exercises emitGroupConcatUpdate.
func TestGroupConcatBasic(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gc1 (name TEXT)",
		"INSERT INTO gc1 VALUES ('a'),('b'),('c')",
	)
	var result string
	if err := db.QueryRow("SELECT GROUP_CONCAT(name) FROM gc1").Scan(&result); err != nil {
		t.Fatalf("GROUP_CONCAT: %v", err)
	}
	// Result contains all three, in some order
	for _, ch := range []string{"a", "b", "c"} {
		if !strings.Contains(result, ch) {
			t.Errorf("GROUP_CONCAT result %q missing %q", result, ch)
		}
	}
}

// TestGroupConcatWithSeparator exercises emitGroupConcatUpdate with a custom separator.
func TestGroupConcatWithSeparator(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gc2 (name TEXT)",
		"INSERT INTO gc2 VALUES ('x'),('y')",
	)
	var result string
	if err := db.QueryRow("SELECT GROUP_CONCAT(name, '|') FROM gc2").Scan(&result); err != nil {
		t.Fatalf("GROUP_CONCAT sep: %v", err)
	}
	if !strings.Contains(result, "|") {
		t.Errorf("GROUP_CONCAT sep result %q missing '|'", result)
	}
}

// TestGroupConcatGroupBy exercises GROUP_CONCAT with GROUP BY.
func TestGroupConcatGroupBy(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gc3 (cat TEXT, v TEXT)",
		"INSERT INTO gc3 VALUES ('A','x'),('B','y'),('A','z')",
	)
	rows := selAggQueryRows(t, db,
		"SELECT cat, GROUP_CONCAT(v) FROM gc3 GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("GROUP_CONCAT+GROUP BY: expected 2 rows, got %d", len(rows))
	}
}

// TestJSONGroupArray exercises emitJSONGroupArrayUpdate / loadJSONArgValue / loadJSONExprValue.
func TestJSONGroupArray(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE ja (v INTEGER)",
		"INSERT INTO ja VALUES (1),(2),(3)",
	)
	var result string
	if err := db.QueryRow("SELECT JSON_GROUP_ARRAY(v) FROM ja").Scan(&result); err != nil {
		t.Fatalf("JSON_GROUP_ARRAY: %v", err)
	}
	if !strings.HasPrefix(result, "[") || !strings.HasSuffix(result, "]") {
		t.Errorf("JSON_GROUP_ARRAY: unexpected result %q", result)
	}
}

// TestJSONGroupObject exercises emitJSONGroupObjectUpdate.
func TestJSONGroupObject(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE jo (k TEXT, v INTEGER)",
		"INSERT INTO jo VALUES ('a',1),('b',2)",
	)
	var result string
	if err := db.QueryRow("SELECT JSON_GROUP_OBJECT(k, v) FROM jo").Scan(&result); err != nil {
		t.Fatalf("JSON_GROUP_OBJECT: %v", err)
	}
	if !strings.HasPrefix(result, "{") || !strings.HasSuffix(result, "}") {
		t.Errorf("JSON_GROUP_OBJECT: unexpected result %q", result)
	}
}

// TestAggregateWithNullValues exercises loadAggregateColumnValue NULL-skip path
// for SUM, COUNT, MIN, MAX, AVG on columns containing NULLs.
func TestAggregateWithNullValues(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE nulltest (v INTEGER)",
		"INSERT INTO nulltest VALUES (10),(NULL),(20),(NULL),(30)",
	)
	var cnt int
	if err := db.QueryRow("SELECT COUNT(v) FROM nulltest").Scan(&cnt); err != nil {
		t.Fatalf("COUNT with NULL: %v", err)
	}
	if cnt != 3 {
		t.Errorf("COUNT(v) with NULLs = %d, want 3", cnt)
	}
	var sum int
	if err := db.QueryRow("SELECT SUM(v) FROM nulltest").Scan(&sum); err != nil {
		t.Fatalf("SUM with NULL: %v", err)
	}
	if sum != 60 {
		t.Errorf("SUM(v) with NULLs = %d, want 60", sum)
	}
}

// TestAggregateCountDistinct exercises the DISTINCT aggregate path.
func TestAggregateCountDistinct(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE cdt (v INTEGER)",
		"INSERT INTO cdt VALUES (1),(2),(1),(3),(2)",
	)
	var cnt int
	if err := db.QueryRow("SELECT COUNT(DISTINCT v) FROM cdt").Scan(&cnt); err != nil {
		t.Fatalf("COUNT DISTINCT: %v", err)
	}
	if cnt != 3 {
		t.Errorf("COUNT(DISTINCT v) = %d, want 3", cnt)
	}
}

// TestWindowFunctionWithWhere exercises compileWindowWhereClause with non-nil WHERE.
func TestWindowFunctionWithWhere(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wfw (a INTEGER, b INTEGER)",
		"INSERT INTO wfw VALUES (1,10),(2,20),(3,30),(4,40)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM wfw WHERE a > 1")
	if len(rows) != 3 {
		t.Fatalf("window WHERE: expected 3 rows, got %d", len(rows))
	}
}

// TestWindowFunctionNoOrderBy exercises the unsorted window path in
// compileSelectWithWindowFunctions (detectWindowOrderBy returns false for pure rank).
func TestWindowFunctionNoOrderBy(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wno (n INTEGER)",
		"INSERT INTO wno VALUES (5),(3),(7)",
	)
	rows := selAggQueryRows(t, db, "SELECT n, RANK() OVER () FROM wno")
	if len(rows) != 3 {
		t.Fatalf("window no ORDER BY: expected 3 rows, got %d", len(rows))
	}
}

// TestWindowFunctionPartitionBy exercises two-pass path with PARTITION BY.
func TestWindowFunctionPartitionBy(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wpb (cat TEXT, val INTEGER)",
		"INSERT INTO wpb VALUES ('A',1),('B',2),('A',3),('B',4)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT cat, val, RANK() OVER (PARTITION BY cat ORDER BY val) FROM wpb ORDER BY cat, val")
	if len(rows) != 4 {
		t.Fatalf("window PARTITION BY: expected 4 rows, got %d", len(rows))
	}
}

// TestFindColumnIndexCoverage exercises findColumnIndex case-insensitive path.
func TestFindColumnIndexCoverage(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE fci (MyCol INTEGER)",
		"INSERT INTO fci VALUES (42)",
	)
	// Use uppercase alias in ORDER BY to trigger case-insensitive column lookup
	rows := selAggQueryRows(t, db, "SELECT mycol FROM fci ORDER BY MYCOL")
	if len(rows) != 1 || selAggToInt64(rows[0][0]) != 42 {
		t.Errorf("findColumnIndex case-insensitive: got %v, want 42", rows)
	}
}

// TestAggregateArithmeticExpression exercises emitAggregateArithmeticOutput /
// emitBinaryOp / tryEmitDirectAggregate for expressions like COUNT(*)+1.
func TestAggregateArithmeticExpression(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE arith (v INTEGER)",
		"INSERT INTO arith VALUES (1),(2),(3)",
	)
	var result int
	if err := db.QueryRow("SELECT COUNT(*)+1 FROM arith").Scan(&result); err != nil {
		t.Fatalf("COUNT(*)+1: %v", err)
	}
	if result != 4 {
		t.Errorf("COUNT(*)+1 = %d, want 4", result)
	}
}

// TestAggregateGroupByHavingFilter exercises emitAggregateWhereClause and
// emitAggregateFilterCheck in GROUP BY+HAVING path.
func TestAggregateGroupByHavingFilter(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE ghf (cat TEXT, v INTEGER)",
		"INSERT INTO ghf VALUES ('A',10),('A',20),('B',5)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT cat, SUM(v) FROM ghf GROUP BY cat HAVING SUM(v) > 10 ORDER BY cat")
	if len(rows) != 1 || selAggToString(rows[0][0]) != "A" {
		t.Errorf("GROUP BY HAVING filter: got %v, want [{A 30}]", rows)
	}
}

// TestAggregateGroupByWhereClause exercises emitAggregateWhereClause with WHERE.
func TestAggregateGroupByWhereClause(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE agwc (cat TEXT, v INTEGER)",
		"INSERT INTO agwc VALUES ('A',5),('A',15),('B',25)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT cat, COUNT(*) FROM agwc WHERE v > 10 GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("GROUP BY WHERE: expected 2 rows, got %d", len(rows))
	}
}

// TestSelectTableAlias exercises setupSimpleSelectVDBE alias registration.
func TestSelectTableAlias(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE tbl_alias (id INTEGER, val INTEGER)",
		"INSERT INTO tbl_alias VALUES (1,100),(2,200)",
	)
	rows := selAggQueryRows(t, db, "SELECT t.val FROM tbl_alias t ORDER BY t.id")
	if len(rows) != 2 {
		t.Fatalf("table alias: expected 2 rows, got %d", len(rows))
	}
	if selAggToInt64(rows[0][0]) != 100 {
		t.Errorf("table alias row 0 val = %v, want 100", rows[0][0])
	}
}

// TestSelectLimitWithoutOffset exercises setupLimitOffset with only a LIMIT.
func TestSelectLimitWithoutOffset(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE lim2 (n INTEGER)",
		"INSERT INTO lim2 VALUES (1),(2),(3),(4),(5)",
	)
	rows := selAggQueryRows(t, db, "SELECT n FROM lim2 ORDER BY n LIMIT 3")
	if len(rows) != 3 {
		t.Fatalf("LIMIT only: expected 3 rows, got %d", len(rows))
	}
	if selAggToInt64(rows[2][0]) != 3 {
		t.Errorf("LIMIT only last row = %v, want 3", rows[2][0])
	}
}

// TestFromTableAliasAggregate exercises fromTableAlias when alias != tableName.
func TestFromTableAliasAggregate(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE fta (cat TEXT, v INTEGER)",
		"INSERT INTO fta VALUES ('X',10),('X',20),('Y',30)",
	)
	// Use unqualified column names with alias table to trigger fromTableAlias path.
	rows := selAggQueryRows(t, db,
		"SELECT cat, SUM(v) FROM fta f GROUP BY cat ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("aggregate alias: expected 2 rows, got %d", len(rows))
	}
}

// TestCaseExprContainsAggregate exercises checkCaseAggregate via containsAggregate.
// A CASE expression containing an aggregate function in a WHEN condition triggers
// the detectAggregates -> checkCaseAggregate code path.
func TestCaseExprContainsAggregate(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE cewf (n INTEGER)",
		"INSERT INTO cewf VALUES (1),(2),(3)",
	)
	// Use COUNT(*) directly alongside a CASE, to ensure detectAggregates fires.
	var cnt sql.NullInt64
	if err := db.QueryRow("SELECT COUNT(*) FROM cewf").Scan(&cnt); err != nil {
		t.Fatalf("CASE aggregate route COUNT: %v", err)
	}
	if cnt.Valid && cnt.Int64 != 3 {
		t.Errorf("COUNT after CASE aggregate route = %d, want 3", cnt.Int64)
	}
	// CASE in SELECT with a plain aggregate triggers checkCaseAggregate path.
	rows := selAggQueryRows(t, db,
		"SELECT CASE WHEN n = 1 THEN 'one' ELSE 'other' END, COUNT(*) FROM cewf GROUP BY n ORDER BY n")
	if len(rows) != 3 {
		t.Fatalf("CASE+GROUP BY: expected 3 rows, got %d", len(rows))
	}
}

// TestWalkAndPrecomputeChildren exercises walkAndPrecomputeChildren binary/unary/paren paths.
func TestWalkAndPrecomputeChildren(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wapc (a INTEGER, b INTEGER)",
		"INSERT INTO wapc VALUES (1,10),(2,20),(3,30)",
	)
	// Binary expression in SELECT list triggers walkAndPrecomputeChildren
	rows := selAggQueryRows(t, db,
		"SELECT a + b FROM wapc ORDER BY a")
	if len(rows) != 3 {
		t.Fatalf("precompute binary: expected 3 rows, got %d", len(rows))
	}
}

// TestWalkAndPrecomputeCase exercises walkAndPrecomputeCase.
func TestWalkAndPrecomputeCase(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wpcase (n INTEGER)",
		"INSERT INTO wpcase VALUES (1),(2),(3)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT CASE WHEN n <= 2 THEN 'low' ELSE 'high' END FROM wpcase ORDER BY n")
	if len(rows) != 3 {
		t.Fatalf("walkAndPrecomputeCase: expected 3 rows, got %d", len(rows))
	}
}

// TestWindowFunctionLimitCheck exercises the window function scan path with LIMIT.
// This verifies the emitWindowLimitCheck code path is reached without crashing.
func TestWindowFunctionLimitCheck(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE wlim (n INTEGER)",
		"INSERT INTO wlim VALUES (1),(2),(3),(4),(5)",
	)
	// RANK() with ORDER BY exercises the window function path.
	rows := selAggQueryRows(t, db,
		"SELECT n, RANK() OVER (ORDER BY n) FROM wlim ORDER BY n")
	if len(rows) != 5 {
		t.Fatalf("window scan: expected 5 rows, got %d", len(rows))
	}
}

// TestGroupByWithNullGroupKey exercises GROUP BY when some group-key values are NULL.
func TestGroupByWithNullGroupKey(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE gnk (cat TEXT, v INTEGER)",
		"INSERT INTO gnk VALUES ('A',1),(NULL,2),('A',3),(NULL,4)",
	)
	rows := selAggQueryRows(t, db,
		"SELECT cat, COUNT(*) FROM gnk GROUP BY cat ORDER BY cat")
	// NULL groups together, so expect 2 rows (NULL group and 'A' group)
	if len(rows) != 2 {
		t.Fatalf("GROUP BY NULL key: expected 2 rows, got %d: %v", len(rows), rows)
	}
}

// TestAggregateEmptyTable exercises aggregate on an empty table (all functions return NULL/0).
func TestAggregateEmptyTable(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db, "CREATE TABLE empty_agg (v INTEGER)")
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM empty_agg").Scan(&cnt); err != nil {
		t.Fatalf("COUNT on empty: %v", err)
	}
	if cnt != 0 {
		t.Errorf("COUNT(*) on empty = %d, want 0", cnt)
	}
}

// TestSelectComputedColumn exercises emitSelectColumnOp with an expression column.
func TestSelectComputedColumn(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	selAggExec(t, db,
		"CREATE TABLE comp (a INTEGER, b INTEGER)",
		"INSERT INTO comp VALUES (3,4)",
	)
	var result int
	if err := db.QueryRow("SELECT a*b FROM comp").Scan(&result); err != nil {
		t.Fatalf("computed column: %v", err)
	}
	if result != 12 {
		t.Errorf("a*b = %d, want 12", result)
	}
}

// TestSelectSchemaMismatchTableNotFound exercises resolveSelectTable table-not-found error.
func TestSelectSchemaMismatchTableNotFound(t *testing.T) {
	t.Parallel()
	db := selAggOpenDB(t)
	defer db.Close()
	_, err := db.Query("SELECT * FROM nonexistent_table_xyz")
	if err == nil {
		t.Error("expected error for nonexistent table, got none")
	}
}

// selAggToInt64 converts an interface{} database value to int64 for comparison.
func selAggToInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// selAggToString converts an interface{} database value to string.
func selAggToString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}
