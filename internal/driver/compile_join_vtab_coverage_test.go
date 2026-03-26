// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openJVDB opens a fresh in-memory database for compile_join_vtab coverage tests.
func openJVDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("openJVDB: sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

// execJV runs a SQL statement and fails the test on error.
func execJV(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// queryJVRows runs a query and returns all rows as [][]interface{}.
func queryJVRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
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

	var out [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

// TestCompileJoinVtabCoverage is the umbrella test that exercises the targeted
// functions via SQL queries on an in-memory database.
func TestCompileJoinVtabCoverage(t *testing.T) {
	t.Run("resolveUsingJoin_single_col", testResolveUsingJoinSingleCol)
	t.Run("resolveUsingJoin_multi_col", testResolveUsingJoinMultiCol)
	t.Run("findColumnTableIndex_qualified", testFindColumnTableIndexQualified)
	t.Run("findColumnTableIndex_unqualified", testFindColumnTableIndexUnqualified)
	t.Run("emitLeafRowSorter_inner_orderby", testEmitLeafRowSorterInnerOrderBy)
	t.Run("emitLeafRowSorter_where_orderby", testEmitLeafRowSorterWhereOrderBy)
	t.Run("resolveExprCollationMultiTable_groupby", testResolveExprCollationMultiTableGroupBy)
	t.Run("resolveExprCollationMultiTable_paren", testResolveExprCollationMultiTableParen)
	t.Run("emitInterfaceValue_rtree_select", testEmitInterfaceValueRTreeSelect)
	t.Run("collectVTabRows_fts5", testCollectVTabRowsFTS5)
	t.Run("extractOrderByExpression_collate", testExtractOrderByExpressionCollate)
	t.Run("extractOrderByExpression_plain", testExtractOrderByExpressionPlain)
	t.Run("findColumnIndex_agg_alias", testFindColumnIndexAggAlias)
	t.Run("fromTableAlias_alias_query", testFromTableAliasAliasQuery)
	t.Run("emitGeneratedExpr_computed_col", testEmitGeneratedExprComputedCol)
	t.Run("ensureMasterPage_multiple_tables", testEnsureMasterPageMultipleTables)
	t.Run("registerBuiltinVirtualTables_rtree_fts5", testRegisterBuiltinVirtualTablesRtreeFTS5)
	t.Run("createMemoryConnection_multiple", testCreateMemoryConnectionMultiple)
	t.Run("MarkDirty_insert_update", testMarkDirtyInsertUpdate)
}

// --- resolveUsingJoin --------------------------------------------------

// testResolveUsingJoinSingleCol covers resolveUsingJoin with a single USING column.
func testResolveUsingJoinSingleCol(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE a (id INTEGER, val TEXT)")
	execJV(t, db, "CREATE TABLE b (id INTEGER, extra TEXT)")
	execJV(t, db, "INSERT INTO a VALUES (1, 'x')")
	execJV(t, db, "INSERT INTO b VALUES (1, 'y')")

	rows := queryJVRows(t, db, "SELECT a.val, b.extra FROM a JOIN b USING (id)")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// testResolveUsingJoinMultiCol covers resolveUsingJoin with multiple USING columns.
func testResolveUsingJoinMultiCol(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE p (id INTEGER, dept INTEGER, name TEXT)")
	execJV(t, db, "CREATE TABLE q (id INTEGER, dept INTEGER, score INTEGER)")
	execJV(t, db, "INSERT INTO p VALUES (1, 10, 'alice')")
	execJV(t, db, "INSERT INTO q VALUES (1, 10, 99)")

	rows := queryJVRows(t, db, "SELECT p.name, q.score FROM p JOIN q USING (id, dept)")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// --- findColumnTableIndex -----------------------------------------------

// testFindColumnTableIndexQualified covers the table-qualified branch of findColumnTableIndex.
func testFindColumnTableIndexQualified(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE left1 (id INTEGER, lval TEXT)")
	execJV(t, db, "CREATE TABLE right1 (id INTEGER, rval TEXT)")
	execJV(t, db, "INSERT INTO left1 VALUES (1, 'L')")
	execJV(t, db, "INSERT INTO right1 VALUES (1, 'R')")

	// ORDER BY forces the sorter path which calls findColumnTableIndex on the
	// SELECT columns to decide which cursor each column belongs to.
	rows := queryJVRows(t, db,
		"SELECT left1.lval, right1.rval FROM left1 JOIN right1 ON left1.id = right1.id ORDER BY left1.lval")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// testFindColumnTableIndexUnqualified covers the unqualified-name branch of findColumnTableIndex.
func testFindColumnTableIndexUnqualified(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE m1 (id INTEGER, mval TEXT)")
	execJV(t, db, "CREATE TABLE m2 (id INTEGER, nval TEXT)")
	execJV(t, db, "INSERT INTO m1 VALUES (2, 'M')")
	execJV(t, db, "INSERT INTO m2 VALUES (2, 'N')")

	// Unqualified column names in ORDER BY; findColumnTableIndex scans all tables.
	rows := queryJVRows(t, db,
		"SELECT mval, nval FROM m1 JOIN m2 ON m1.id = m2.id ORDER BY mval")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// --- emitLeafRowSorter --------------------------------------------------

// testEmitLeafRowSorterInnerOrderBy covers emitLeafRowSorter via an INNER JOIN with ORDER BY.
func testEmitLeafRowSorterInnerOrderBy(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE emp (eid INTEGER, name TEXT, did INTEGER)")
	execJV(t, db, "CREATE TABLE dept (did INTEGER, dname TEXT)")
	execJV(t, db, "INSERT INTO emp VALUES (1, 'Alice', 10)")
	execJV(t, db, "INSERT INTO emp VALUES (2, 'Bob', 10)")
	execJV(t, db, "INSERT INTO emp VALUES (3, 'Carol', 20)")
	execJV(t, db, "INSERT INTO dept VALUES (10, 'Eng')")
	execJV(t, db, "INSERT INTO dept VALUES (20, 'Mkt')")

	rows := queryJVRows(t, db,
		"SELECT emp.name, dept.dname FROM emp JOIN dept ON emp.did = dept.did ORDER BY emp.name")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// testEmitLeafRowSorterWhereOrderBy covers emitLeafRowSorter WHERE branch with ORDER BY.
func testEmitLeafRowSorterWhereOrderBy(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE t1 (id INTEGER, v TEXT)")
	execJV(t, db, "CREATE TABLE t2 (id INTEGER, w TEXT)")
	for i := 1; i <= 5; i++ {
		execJV(t, db, "INSERT INTO t1 VALUES (?, ?)", i, "v")
		execJV(t, db, "INSERT INTO t2 VALUES (?, ?)", i, "w")
	}

	rows := queryJVRows(t, db,
		"SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 2 ORDER BY t1.id DESC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// --- resolveExprCollationMultiTable ------------------------------------

// testResolveExprCollationMultiTableGroupBy covers resolveExprCollationMultiTable via
// a GROUP BY on a multi-table JOIN, hitting the IdentExpr branch.
func testResolveExprCollationMultiTableGroupBy(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE cats (cid INTEGER, cname TEXT COLLATE NOCASE)")
	execJV(t, db, "CREATE TABLE items (cid INTEGER, price INTEGER)")
	execJV(t, db, "INSERT INTO cats VALUES (1, 'food')")
	execJV(t, db, "INSERT INTO cats VALUES (2, 'drink')")
	execJV(t, db, "INSERT INTO items VALUES (1, 10)")
	execJV(t, db, "INSERT INTO items VALUES (1, 20)")
	execJV(t, db, "INSERT INTO items VALUES (2, 30)")

	rows := queryJVRows(t, db,
		"SELECT cats.cname, SUM(items.price) FROM cats JOIN items ON cats.cid = items.cid GROUP BY cats.cname")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// testResolveExprCollationMultiTableParen covers the ParenExpr branch of
// resolveExprCollationMultiTable.
func testResolveExprCollationMultiTableParen(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE grp (gid INTEGER, label TEXT)")
	execJV(t, db, "CREATE TABLE data (gid INTEGER, amount INTEGER)")
	execJV(t, db, "INSERT INTO grp VALUES (1, 'A')")
	execJV(t, db, "INSERT INTO grp VALUES (2, 'B')")
	execJV(t, db, "INSERT INTO data VALUES (1, 5)")
	execJV(t, db, "INSERT INTO data VALUES (2, 15)")

	// GROUP BY with a parenthesised expression triggers the ParenExpr case.
	rows := queryJVRows(t, db,
		"SELECT grp.label, COUNT(*) FROM grp JOIN data ON grp.gid = data.gid GROUP BY (grp.label)")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// --- emitInterfaceValue (compile_vtab.go) --------------------------------

// testEmitInterfaceValueRTreeSelect covers emitInterfaceValue by selecting rows
// back from an rtree virtual table.  The rtree cursor returns int64 and float64
// values, exercising several branches of the type switch.
func testEmitInterfaceValueRTreeSelect(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE VIRTUAL TABLE geo USING rtree(id, minX, maxX, minY, maxY)")
	if err != nil {
		t.Skipf("rtree not available: %v", err)
	}

	execJV(t, db, "INSERT INTO geo VALUES (1, 0.0, 1.0, 0.0, 1.0)")
	execJV(t, db, "INSERT INTO geo VALUES (2, 2.0, 3.0, 2.0, 3.0)")

	rows := queryJVRows(t, db, "SELECT id, minX, maxX FROM geo")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows from rtree, got %d", len(rows))
	}
}

// --- collectVTabRows (compile_vtab.go) -----------------------------------

// testCollectVTabRowsFTS5 covers collectVTabRows by scanning all rows from an
// FTS5 virtual table (which uses a virtual cursor internally).
func testCollectVTabRowsFTS5(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE VIRTUAL TABLE docs USING fts5(content)")
	if err != nil {
		t.Skipf("fts5 not available: %v", err)
	}

	execJV(t, db, "INSERT INTO docs VALUES ('hello world')")
	execJV(t, db, "INSERT INTO docs VALUES ('foo bar baz')")

	rows := queryJVRows(t, db, "SELECT content FROM docs")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows from fts5 table, got %d", len(rows))
	}
}

// --- extractOrderByExpression (compile_select.go) ------------------------

// testExtractOrderByExpressionCollate covers extractOrderByExpression when the
// ORDER BY term wraps a CollateExpr.
func testExtractOrderByExpressionCollate(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE words (w TEXT)")
	execJV(t, db, "INSERT INTO words VALUES ('Banana')")
	execJV(t, db, "INSERT INTO words VALUES ('apple')")
	execJV(t, db, "INSERT INTO words VALUES ('Cherry')")

	rows := queryJVRows(t, db, "SELECT w FROM words ORDER BY w COLLATE NOCASE")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// testExtractOrderByExpressionPlain covers extractOrderByExpression without a CollateExpr.
func testExtractOrderByExpressionPlain(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE nums (n INTEGER)")
	execJV(t, db, "INSERT INTO nums VALUES (3)")
	execJV(t, db, "INSERT INTO nums VALUES (1)")
	execJV(t, db, "INSERT INTO nums VALUES (2)")

	rows := queryJVRows(t, db, "SELECT n FROM nums ORDER BY n ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// --- findColumnIndex (compile_select_agg.go) -----------------------------

// testFindColumnIndexAggAlias covers findColumnIndex via aggregate queries where
// the column name may differ in case from the schema declaration.
func testFindColumnIndexAggAlias(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE sales (Product TEXT, Amount INTEGER)")
	execJV(t, db, "INSERT INTO sales VALUES ('A', 10)")
	execJV(t, db, "INSERT INTO sales VALUES ('A', 20)")
	execJV(t, db, "INSERT INTO sales VALUES ('B', 5)")

	rows := queryJVRows(t, db, "SELECT Product, SUM(Amount) FROM sales GROUP BY Product ORDER BY Product")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// --- fromTableAlias (compile_select_agg.go) ------------------------------

// testFromTableAliasAliasQuery covers fromTableAlias when a FROM table has an
// explicit alias, triggering the alias != tableName registration path.
func testFromTableAliasAliasQuery(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE employees (dept TEXT, salary INTEGER)")
	execJV(t, db, "INSERT INTO employees VALUES ('Eng', 100)")
	execJV(t, db, "INSERT INTO employees VALUES ('Eng', 200)")
	execJV(t, db, "INSERT INTO employees VALUES ('HR', 50)")

	// Use a table alias; fromTableAlias returns the alias and registerAggTableInfo
	// registers both the real name and the alias.
	rows := queryJVRows(t, db,
		"SELECT dept, SUM(salary) FROM employees e GROUP BY dept ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// --- emitGeneratedExpr (compile_select_agg.go) ---------------------------

// testEmitGeneratedExprComputedCol covers emitGeneratedExpr via a GROUP BY aggregate
// that projects a computed (non-identifier) expression in the SELECT list.
func testEmitGeneratedExprComputedCol(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE ledger (acct TEXT, val INTEGER)")
	execJV(t, db, "INSERT INTO ledger VALUES ('X', 3)")
	execJV(t, db, "INSERT INTO ledger VALUES ('X', 7)")
	execJV(t, db, "INSERT INTO ledger VALUES ('Y', 5)")

	// The expression (SUM(val) * 2) is not a plain identifier, so emitGeneratedExpr
	// generates it and copies the result register to the output column slot.
	rows := queryJVRows(t, db,
		"SELECT acct, SUM(val) * 2 FROM ledger GROUP BY acct ORDER BY acct")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// --- ensureMasterPage (conn.go) -----------------------------------------

// testEnsureMasterPageMultipleTables exercises ensureMasterPage by creating
// multiple tables on several fresh in-memory connections.  Each new connection
// initialises page 1 (sqlite_master) which calls ensureMasterPage.
func testEnsureMasterPageMultipleTables(t *testing.T) {
	for i := 0; i < 3; i++ {
		db := openJVDB(t)

		execJV(t, db, "CREATE TABLE first (id INTEGER)")
		execJV(t, db, "CREATE TABLE second (val TEXT)")
		execJV(t, db, "INSERT INTO first VALUES (42)")

		var v int
		if err := db.QueryRow("SELECT id FROM first").Scan(&v); err != nil {
			db.Close()
			t.Fatalf("iter %d SELECT: %v", i, err)
		}
		if v != 42 {
			db.Close()
			t.Errorf("iter %d: got %d want 42", i, v)
		}
		db.Close()
	}
}

// --- registerBuiltinVirtualTables (conn.go) ------------------------------

// testRegisterBuiltinVirtualTablesRtreeFTS5 exercises registerBuiltinVirtualTables
// by creating both an rtree and an fts5 virtual table on the same connection.
func testRegisterBuiltinVirtualTablesRtreeFTS5(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	// rtree
	if _, err := db.Exec("CREATE VIRTUAL TABLE spaces USING rtree(id, x1, x2, y1, y2)"); err != nil {
		t.Logf("rtree skipped: %v", err)
	}

	// fts5
	if _, err := db.Exec("CREATE VIRTUAL TABLE corpus USING fts5(body)"); err != nil {
		t.Logf("fts5 skipped: %v", err)
	}
}

// --- createMemoryConnection (driver.go) ----------------------------------

// testCreateMemoryConnectionMultiple exercises createMemoryConnection by opening
// several independent :memory: databases; each call creates a distinct db state.
func testCreateMemoryConnectionMultiple(t *testing.T) {
	const n = 4
	dbs := make([]*sql.DB, n)
	for i := range dbs {
		dbs[i] = openJVDB(t)
		execJV(t, dbs[i], "CREATE TABLE x (v INTEGER)")
		execJV(t, dbs[i], "INSERT INTO x VALUES (?)", i)
	}
	// Verify each db has its own isolated state.
	for i, db := range dbs {
		var v int
		if err := db.QueryRow("SELECT v FROM x").Scan(&v); err != nil {
			db.Close()
			t.Fatalf("db %d SELECT: %v", i, err)
		}
		if v != i {
			t.Errorf("db %d: got %d want %d", i, v, i)
		}
		db.Close()
	}
}

// --- MarkDirty (driver.go / memoryPagerProvider) -------------------------

// testMarkDirtyInsertUpdate exercises MarkDirty (memoryPagerProvider.MarkDirty)
// by performing a sequence of INSERT and UPDATE operations that write pages.
func testMarkDirtyInsertUpdate(t *testing.T) {
	db := openJVDB(t)
	defer db.Close()

	execJV(t, db, "CREATE TABLE counters (id INTEGER PRIMARY KEY, cnt INTEGER)")

	for i := 1; i <= 10; i++ {
		execJV(t, db, "INSERT INTO counters (cnt) VALUES (?)", i*100)
	}

	execJV(t, db, "UPDATE counters SET cnt = cnt + 1")

	var total int64
	if err := db.QueryRow("SELECT COUNT(*) FROM counters").Scan(&total); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if total != 10 {
		t.Errorf("want 10 rows, got %d", total)
	}
}
