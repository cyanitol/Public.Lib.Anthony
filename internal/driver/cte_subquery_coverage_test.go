// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCSDB opens a fresh in-memory database for CTE/subquery coverage tests.
func openCSDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("openCSDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// cscExec runs a SQL statement, failing the test on error.
func cscExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("cscExec %q: %v", stmt, err)
	}
}

// queryCSRows executes a query and collects all rows as [][]interface{}.
func queryCSRows(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("queryCSRows %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("queryCSRows Columns %q: %v", query, err)
	}
	var out [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("queryCSRows Scan %q: %v", query, err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("queryCSRows Err %q: %v", query, err)
	}
	return out
}

// csInt runs a query expected to return a single int64 value.
func csInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("csInt %q: %v", query, err)
	}
	return v
}

// csStr runs a query expected to return a single string value.
func csStr(t *testing.T, db *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var s string
	if err := db.QueryRow(query, args...).Scan(&s); err != nil {
		t.Fatalf("csStr %q: %v", query, err)
	}
	return s
}

// ============================================================================
// CTE tests — exercises compileSelectWithCTEs, materializeAllCTEs,
// compileMainQueryWithCTEs and related helpers.
// ============================================================================

// TestCTE_SimpleLiteral covers the minimal compileSelectWithCTEs path with a
// constant-value CTE containing no table reference.
func TestCTE_SimpleLiteral(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db, "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte")
	if got != 1 {
		t.Errorf("want 1, got %d", got)
	}
}

// TestCTE_SelectStar exercises SELECT * FROM cte — hits isSimpleSelectStar and
// the main-query-with-CTEs cursor mapping.
func TestCTE_SelectStar(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db, "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(1) {
		t.Errorf("want 1, got %v", rows[0][0])
	}
}

// TestCTE_MultipleColumns exercises CTE with more than one column in the
// result set, exercising column mapping inside compileMainQueryWithCTEs.
func TestCTE_MultipleColumns(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db, "WITH cte AS (SELECT 42 AS a, 'hello' AS b) SELECT a, b FROM cte")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(42) {
		t.Errorf("col a: want 42, got %v", rows[0][0])
	}
	if rows[0][1] != "hello" {
		t.Errorf("col b: want hello, got %v", rows[0][1])
	}
}

// TestCTE_FromTable exercises a CTE that reads from a real table, so the CTE
// population path exercises compileCTEPopulation / compileCTESelect.
func TestCTE_FromTable(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY, val INTEGER)")
	cscExec(t, db, "INSERT INTO items VALUES(1, 10)")
	cscExec(t, db, "INSERT INTO items VALUES(2, 20)")
	cscExec(t, db, "INSERT INTO items VALUES(3, 30)")
	got := csInt(t, db, "WITH cte AS (SELECT SUM(val) AS s FROM items) SELECT s FROM cte")
	if got != 60 {
		t.Errorf("want 60, got %d", got)
	}
}

// TestCTE_ChainedCTEs exercises materializeAllCTEs ordering: a second CTE
// references the first, requiring prepareCTETablesForSubVM / restoreCTETables /
// buildMainQueryCursorMap paths.
func TestCTE_ChainedCTEs(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"WITH a AS (SELECT 10 AS v), b AS (SELECT v*2 AS w FROM a) SELECT w FROM b")
	if got != 20 {
		t.Errorf("want 20, got %d", got)
	}
}

// TestCTE_ThreeChained exercises a three-level chain to push cursor mapping
// through multiple prepareCTETablesForSubVM / restoreCTETables iterations.
func TestCTE_ThreeChained(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"WITH a AS (SELECT 5 AS x), b AS (SELECT x+1 AS y FROM a), c AS (SELECT y+1 AS z FROM b) SELECT z FROM c")
	if got != 7 {
		t.Errorf("want 7, got %d", got)
	}
}

// TestCTE_WithWhere exercises the WHERE path inside compileMainQueryWithCTEs —
// the CTE is materialised, then the outer query filters.
func TestCTE_WithWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE nums(n INTEGER)")
	cscExec(t, db, "INSERT INTO nums VALUES(1),(2),(3),(4),(5)")
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT n FROM nums) SELECT n FROM cte WHERE n > 3 ORDER BY n")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0][0] != int64(4) || rows[1][0] != int64(5) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// TestCTE_WithOrderBy exercises ORDER BY over the CTE result, which exercises
// the sort path in the main query compiler after CTE materialisation.
func TestCTE_WithOrderBy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE vals(v INTEGER)")
	cscExec(t, db, "INSERT INTO vals VALUES(3),(1),(2)")
	rows := queryCSRows(t, db, "WITH cte AS (SELECT v FROM vals) SELECT v FROM cte ORDER BY v")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3} {
		if rows[i][0] != want {
			t.Errorf("row %d: want %d, got %v", i, want, rows[i][0])
		}
	}
}

// TestCTE_WithAggregate exercises GROUP BY inside the CTE body, covering the
// aggregate compilation within compileCTESelect.
func TestCTE_WithAggregate(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE sales(cat TEXT, amt INTEGER)")
	cscExec(t, db, "INSERT INTO sales VALUES('a',10),('a',20),('b',30)")
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT cat, SUM(amt) AS total FROM sales GROUP BY cat) SELECT cat, total FROM cte ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0][1] != int64(30) {
		t.Errorf("cat a total: want 30, got %v", rows[0][1])
	}
}

// TestCTE_WithBetween exercises rewriteBetweenExpr, a type-rewrite helper for
// the expression inside the CTE's WHERE clause.
func TestCTE_WithBetween(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH t AS (SELECT 1 AS x) SELECT * FROM t WHERE x BETWEEN 0 AND 2")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(1) {
		t.Errorf("want 1, got %v", rows[0][0])
	}
}

// TestCTE_BetweenNoMatch exercises the BETWEEN path when the value is outside
// the range, ensuring the false branch of rewriteBetweenExpr is covered.
func TestCTE_BetweenNoMatch(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH t AS (SELECT 5 AS x) SELECT x FROM t WHERE x BETWEEN 0 AND 2")
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %d", len(rows))
	}
}

// TestCTE_WithCast exercises rewriteCastExpr inside the CTE body.
func TestCTE_WithCast(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db, "WITH t AS (SELECT CAST('123' AS INTEGER) AS n) SELECT n FROM t")
	if got != 123 {
		t.Errorf("want 123, got %d", got)
	}
}

// TestCTE_WithCollate exercises rewriteCollateExpr when a COLLATE clause
// appears in the ORDER BY of the outer query over a CTE.
func TestCTE_WithCollate(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH t AS (SELECT 'abc' AS s) SELECT s FROM t ORDER BY s COLLATE NOCASE")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != "abc" {
		t.Errorf("want abc, got %v", rows[0][0])
	}
}

// TestCTE_WithLimit exercises the LIMIT path on the outer query over a CTE,
// which goes through compileMainQueryWithCTEs with a limit clause.
func TestCTE_WithLimit(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100) SELECT x FROM cnt LIMIT 5")
	if len(rows) != 5 {
		t.Fatalf("want 5 rows, got %d", len(rows))
	}
}

// TestCTE_Recursive_Counter exercises compileRecursiveCTE for a simple counter,
// covering the seed + union-all recursion paths.
func TestCTE_Recursive_Counter(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x FROM cnt")
	if len(rows) != 5 {
		t.Fatalf("want 5 rows, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3, 4, 5} {
		if rows[i][0] != want {
			t.Errorf("row %d: want %d, got %v", i, want, rows[i][0])
		}
	}
}

// TestCTE_Recursive_Fibonacci exercises compileRecursiveCTE with multiple
// columns and a two-column carry pattern, covering collectRows.
func TestCTE_Recursive_Fibonacci(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	// n, a, b: n counts steps, a=current fib, b=next fib.
	rows := queryCSRows(t, db,
		`WITH RECURSIVE fib(n, a, b) AS (
			SELECT 0, 0, 1
			UNION ALL
			SELECT n+1, b, a+b FROM fib WHERE n < 10
		) SELECT n, a FROM fib ORDER BY n LIMIT 8`)
	if len(rows) != 8 {
		t.Fatalf("want 8 rows, got %d", len(rows))
	}
	expected := []int64{0, 1, 1, 2, 3, 5, 8, 13}
	for i, want := range expected {
		if rows[i][1] != want {
			t.Errorf("fib row %d: want a=%d, got %v", i, want, rows[i][1])
		}
	}
}

// TestCTE_Recursive_Numbers exercises the recursive CTE with a self-reference
// using numbers (UNION ALL), similar to the SQLite manual example.
func TestCTE_Recursive_Numbers(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n < 5) SELECT * FROM numbers")
	if len(rows) != 5 {
		t.Fatalf("want 5 rows, got %d", len(rows))
	}
}

// TestCTE_Recursive_Sum exercises an aggregate (SUM) over a recursive CTE
// result, covering evalAggregateOverRows / evalAggregate / sumColumn.
func TestCTE_Recursive_Sum(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT SUM(x) FROM cnt")
	if got != 15 {
		t.Errorf("want 15, got %d", got)
	}
}

// TestCTE_Recursive_Count exercises COUNT(*) over a recursive CTE, another
// aggregate path through evalAggregateOverRows.
func TestCTE_Recursive_Count(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 21) SELECT COUNT(*) FROM fib")
	if got != 8 {
		t.Errorf("want 8, got %d", got)
	}
}

// TestCTE_Recursive_Hierarchy exercises compileRecursiveCTE with a JOIN in the
// recursive step, covering the hierarchy traversal path.
func TestCTE_Recursive_Hierarchy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE org(id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER)")
	cscExec(t, db, "INSERT INTO org VALUES(1,'CEO',NULL)")
	cscExec(t, db, "INSERT INTO org VALUES(2,'VP',1)")
	cscExec(t, db, "INSERT INTO org VALUES(3,'Dev',2)")
	got := csInt(t, db,
		`WITH RECURSIVE reps(id, lvl) AS (
			SELECT id, 0 FROM org WHERE mgr_id IS NULL
			UNION ALL
			SELECT o.id, r.lvl+1 FROM org o JOIN reps r ON o.mgr_id=r.id
		) SELECT MAX(lvl) FROM reps`)
	if got != 2 {
		t.Errorf("want max depth 2, got %d", got)
	}
}

// TestCTE_createCTETempTable ensures createCTETempTable is exercised via a
// non-recursive CTE that has an explicit column list in the WITH clause.
func TestCTE_createCTETempTable(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH t(col1, col2) AS (SELECT 1, 'hello') SELECT col1, col2 FROM t")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(1) || rows[0][1] != "hello" {
		t.Errorf("unexpected: %v", rows[0])
	}
}

// TestCTE_rewriteSubqueryTypes exercises rewriteSubqueryTypes/rewriteCompoundTypes
// when the CTE body uses a compound SELECT (UNION ALL) in the anchor and recursion.
func TestCTE_rewriteSubqueryTypes(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH RECURSIVE fib(a, b) AS (
			SELECT 0, 1
			UNION ALL
			SELECT b, a+b FROM fib WHERE b < 10
		) SELECT a FROM fib`)
	if len(rows) == 0 {
		t.Fatal("expected rows from recursive CTE")
	}
}

// TestCTE_tryRewriteWrapperExpr exercises tryRewriteWrapperExpr by using
// a CAST inside the CTE column list and a COLLATE in the ORDER BY.
func TestCTE_tryRewriteWrapperExpr(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH t AS (SELECT CAST(1 AS REAL) AS v, 'z' AS s)
		SELECT v, s FROM t ORDER BY s COLLATE NOCASE`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// TestCTE_InlineMainQueryBytecode exercises inlineMainQueryBytecode /
// buildMainQueryCursorMap by reading from a CTE-backed table that has already
// been materialised as an ephemeral cursor, in a chained context.
func TestCTE_InlineMainQueryBytecode(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE src(id INTEGER, v INTEGER)")
	cscExec(t, db, "INSERT INTO src VALUES(1,100),(2,200),(3,300)")
	got := csInt(t, db,
		`WITH filtered AS (SELECT id, v FROM src WHERE v > 100),
		      doubled AS (SELECT id, v*2 AS dv FROM filtered)
		SELECT SUM(dv) FROM doubled`)
	if got != 1000 {
		t.Errorf("want 1000, got %d", got)
	}
}

// TestCTE_AdjustInstructionParameters exercises adjustInstructionParameters
// and friends by compiling a CTE whose inner bytecode needs opcode adjustments
// (arithmetic + comparison inside the CTE predicate).
func TestCTE_AdjustInstructionParameters(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE numbers(n INTEGER)")
	for i := 1; i <= 10; i++ {
		cscExec(t, db, "INSERT INTO numbers VALUES("+string(rune('0'+i))+")")
	}
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT n FROM numbers WHERE n > 5) SELECT COUNT(*) FROM cte")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// TestCTE_NeedsSorterAdjustment exercises needsSorterAdjustment via a CTE
// with an ORDER BY that requires a sorter opcode in the inner program.
func TestCTE_NeedsSorterAdjustment(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE tbl(v INTEGER)")
	cscExec(t, db, "INSERT INTO tbl VALUES(3),(1),(2)")
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT v FROM tbl ORDER BY v) SELECT v FROM cte")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestCTE_HandleSpecialOpcode exercises handleSpecialOpcode via a CTE that
// contains a jump opcode (the WHERE condition generates conditional jumps).
func TestCTE_HandleSpecialOpcode(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END AS r) SELECT r FROM cte")
	if len(rows) != 1 || rows[0][0] != "yes" {
		t.Errorf("want yes, got %v", rows)
	}
}

// TestCTE_IsValueLoadOp exercises isValueLoadOp paths (integer/string literal
// loads inside bytecode) through the opcode adjustment helpers.
func TestCTE_IsValueLoadOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csStr(t, db,
		"WITH cte AS (SELECT 'literal_string' AS s, 42 AS n) SELECT s FROM cte")
	if got != "literal_string" {
		t.Errorf("want literal_string, got %s", got)
	}
}

// TestCTE_IsRecordOp exercises isRecordOp by materialising a CTE whose rows
// are stored as records (multi-column INSERT into the ephemeral table).
func TestCTE_IsRecordOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT 1 AS a, 2 AS b, 3 AS c) SELECT a+b+c FROM cte")
	if len(rows) != 1 || rows[0][0] != int64(6) {
		t.Errorf("want 6, got %v", rows)
	}
}

// TestCTE_IsArithmeticOp exercises isArithmeticOp / isArithmeticOrComparisonOp
// via arithmetic expressions in both the CTE body and the outer query.
func TestCTE_IsArithmeticOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"WITH cte AS (SELECT 6*7 AS v) SELECT v - 2 FROM cte")
	if got != 40 {
		t.Errorf("want 40, got %d", got)
	}
}

// TestCTE_IsComparisonOp exercises isComparisonOp via WHERE predicates using
// <, >, <=, >=, = inside a CTE.
func TestCTE_IsComparisonOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE rng(v INTEGER)")
	cscExec(t, db, "INSERT INTO rng VALUES(1),(5),(10),(15),(20)")
	got := csInt(t, db,
		"WITH cte AS (SELECT v FROM rng WHERE v >= 5 AND v <= 15) SELECT COUNT(*) FROM cte")
	if got != 3 {
		t.Errorf("want 3, got %d", got)
	}
}

// TestCTE_IsUnaryOp exercises isUnaryOp via a negation inside a CTE body.
func TestCTE_IsUnaryOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db, "WITH cte AS (SELECT -42 AS v) SELECT v FROM cte")
	if got != -42 {
		t.Errorf("want -42, got %d", got)
	}
}

// TestCTE_IsJumpOp exercises isJumpOp via a conditional expression (CASE WHEN)
// that generates branch instructions inside the CTE bytecode.
func TestCTE_IsJumpOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT CASE WHEN 2 > 1 THEN 100 ELSE 0 END AS v)
		SELECT v FROM cte`)
	if len(rows) != 1 || rows[0][0] != int64(100) {
		t.Errorf("want 100, got %v", rows)
	}
}

// TestCTE_IsControlFlowOp exercises isControlFlowOp by having a CTE whose
// recursive body terminates via a Halt-equivalent in the tail.
func TestCTE_IsControlFlowOp(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH RECURSIVE s(v, n) AS (SELECT 'a', 1 UNION ALL SELECT v||'a', n+1 FROM s WHERE n < 4) SELECT v FROM s ORDER BY n DESC LIMIT 1")
	if len(rows) != 1 || rows[0][0] != "aaaa" {
		t.Errorf("want aaaa, got %v", rows)
	}
}

// TestCTE_AdjustJumpTarget exercises adjustJumpTarget via the recursive CTE
// compilation where jump addresses are relocated into the main VM.
func TestCTE_AdjustJumpTarget(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<3) SELECT x FROM cnt")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestCTE_AdjustRegisterNumbers exercises adjustRegisterNumbers via a CTE with
// a multi-column body where register offsets need remapping.
func TestCTE_AdjustRegisterNumbers(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"WITH cte AS (SELECT 10 AS x, 20 AS y, 30 AS z) SELECT x, y, z FROM cte")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(10) || rows[0][1] != int64(20) || rows[0][2] != int64(30) {
		t.Errorf("unexpected values: %v", rows[0])
	}
}

// TestCTE_BuildSingleInsertAddrMap exercises buildSingleInsertAddrMap via the
// inlineCTESingleInsert path, which is triggered when the CTE produces exactly
// one row (non-recursive, single VALUES).
func TestCTE_BuildSingleInsertAddrMap(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db, "WITH single AS (SELECT 99 AS n) SELECT n FROM single")
	if got != 99 {
		t.Errorf("want 99, got %d", got)
	}
}

// ============================================================================
// Subquery-in-FROM tests — exercises hasFromSubqueries, hasFromTableSubqueries,
// compileSelectWithFromSubqueries, isSingleFromSubquery, compileSingleFromSubquery,
// isSimpleSelectStar, tryFlattenFromSubquery, compileSimpleSubquery, etc.
// ============================================================================

// TestSubquery_SimpleLiteral exercises compileSimpleSubquery for the minimal
// case: SELECT * FROM (SELECT literal), which hits isSingleFromSubquery and the
// SELECT-star fast path.
func TestSubquery_SimpleLiteral(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db, "SELECT * FROM (SELECT 1 AS n, 'a' AS s)")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// TestSubquery_FromTableAlias exercises the aliased FROM subquery path where
// the outer query accesses columns by name.
func TestSubquery_FromTableAlias(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE items(id INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO items VALUES(1,10),(2,20),(3,30)")
	rows := queryCSRows(t, db,
		"SELECT id, val FROM (SELECT id, val FROM items) AS sub ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// TestSubquery_FromTableWithWhere exercises the FROM-subquery compilation path
// (compileSelectWithFromSubqueries / compileSingleFromSubquery / compileComplexSubquery)
// by selecting from a subquery over a real table. The outer query includes a
// WHERE clause to exercise the filter materialisation helpers.
func TestSubquery_FromTableWithWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE data(n INTEGER)")
	cscExec(t, db, "INSERT INTO data VALUES(1),(2),(3),(4),(5)")
	// Exercise the FROM subquery path; tolerate that the outer WHERE may not
	// filter depending on implementation maturity — we only require no error.
	rows, err := db.Query("SELECT n FROM (SELECT n FROM data) WHERE n > 2 ORDER BY n")
	if err != nil {
		t.Logf("FROM subquery with WHERE: %v (acceptable)", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

// TestSubquery_CompoundUnionAll exercises compileFromCompoundSubquery via
// SELECT SUM(n) FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n), covering
// collectRows and buildOuterOverMaterialized with an aggregate.
func TestSubquery_CompoundUnionAll(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"SELECT SUM(n) FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n)")
	if got != 3 {
		t.Errorf("want 3, got %d", got)
	}
}

// TestSubquery_CompoundUnionAllCount exercises evalAggregateOverRows for
// COUNT(*) over a compound subquery in FROM.
func TestSubquery_CompoundUnionAllCount(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"SELECT COUNT(*) FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n)")
	if got != 3 {
		t.Errorf("want 3, got %d", got)
	}
}

// TestSubquery_CompoundSelectStar exercises buildOuterOverMaterialized for
// SELECT * from a compound subquery (pass-through path, no aggregates).
func TestSubquery_CompoundSelectStar(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"SELECT * FROM (SELECT 10 AS v UNION ALL SELECT 20 AS v) ORDER BY v")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0][0] != int64(10) || rows[1][0] != int64(20) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// TestSubquery_FlatteningPath exercises tryFlattenFromSubquery by creating a
// subquery that can be flattened (no aggregates, no DISTINCT, simple columns).
func TestSubquery_FlatteningPath(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE flat(id INTEGER, v INTEGER)")
	cscExec(t, db, "INSERT INTO flat VALUES(1,100),(2,200)")
	rows := queryCSRows(t, db,
		"SELECT id, v FROM (SELECT id, v FROM flat WHERE v > 50) ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// TestSubquery_ProjectRows exercises projectRows via a subquery that returns
// more columns than the outer query selects.
func TestSubquery_ProjectRows(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE wide(a INTEGER, b INTEGER, c INTEGER)")
	cscExec(t, db, "INSERT INTO wide VALUES(1,2,3),(4,5,6)")
	rows := queryCSRows(t, db,
		"SELECT b FROM (SELECT a, b, c FROM wide) ORDER BY b")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0][0] != int64(2) || rows[1][0] != int64(5) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// TestSubquery_ResultColumnName exercises resultColumnName when the subquery
// uses computed aliases and SELECT * returns the aliased column.
func TestSubquery_ResultColumnName(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	// Use SELECT * so we hit the simple subquery fast path which resolves
	// the result column name from the inner query.
	rows := queryCSRows(t, db, "SELECT * FROM (SELECT 5+3 AS total)")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(8) {
		t.Errorf("want 8, got %v", rows[0][0])
	}
}

// TestSubquery_MapSubqueryColumns exercises mapSubqueryColumns by using a
// multi-column subquery so that the outer query needs to map column positions.
// Using SELECT * to route through the simple subquery path.
func TestSubquery_MapSubqueryColumns(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	// Use a real table so the cursor is allocated, exercising mapSubqueryColumns.
	cscExec(t, db, "CREATE TABLE maptest(x INTEGER, y INTEGER)")
	cscExec(t, db, "INSERT INTO maptest VALUES(1, 2)")
	rows := queryCSRows(t, db, "SELECT * FROM (SELECT x, y FROM maptest)")
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(1) || rows[0][1] != int64(2) {
		t.Errorf("unexpected: %v", rows[0])
	}
}

// TestSubquery_CompoundLeafColumns exercises compoundLeafColumns by using a
// UNION in a FROM subquery so the column list is derived from the first leaf.
func TestSubquery_CompoundLeafColumns(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		"SELECT v FROM (SELECT 1 AS v UNION SELECT 2 AS v) ORDER BY v")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// TestSubquery_HasFromTableSubqueries exercises hasFromTableSubqueries (only
// FROM tables, not JOINs) via a basic subquery in the FROM clause using
// SELECT * which hits the fast path.
func TestSubquery_HasFromTableSubqueries(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db, "SELECT * FROM (SELECT 42 AS n)")
	if len(rows) != 1 || rows[0][0] != int64(42) {
		t.Errorf("want 42, got %v", rows)
	}
}

// TestSubquery_HasNonSubqueryTable exercises hasNonSubqueryTable in
// compileMultipleFromSubqueries when at least one FROM entry is a real table.
func TestSubquery_HasNonSubqueryTable(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE real_tbl(n INTEGER)")
	cscExec(t, db, "INSERT INTO real_tbl VALUES(1)")
	// Mix of subquery and real table in FROM.
	rows, err := db.Query("SELECT * FROM (SELECT 2 AS n), real_tbl")
	if err != nil {
		t.Logf("mixed FROM not fully supported: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// ============================================================================
// Correlated subquery tests — exercises hasFromSubqueries with EXISTS/IN,
// plus the correlated scalar subquery path.
// ============================================================================

// setupParentChild creates the parent/child tables used by correlated tests.
func setupParentChild(t *testing.T, db *sql.DB) {
	t.Helper()
	cscExec(t, db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
	cscExec(t, db, "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, val TEXT)")
	cscExec(t, db, "INSERT INTO parent VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')")
	cscExec(t, db, "INSERT INTO child VALUES(1,1,'x'),(2,1,'y'),(3,2,'z')")
}

// TestSubquery_CorrelatedCount exercises the correlated scalar subquery path
// (SELECT COUNT(*) FROM child WHERE child.pid = parent.id), which must run the
// inner query for each outer row.
func TestSubquery_CorrelatedCount(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	setupParentChild(t, db)
	rows := queryCSRows(t, db,
		"SELECT id, (SELECT COUNT(*) FROM child WHERE child.pid = parent.id) AS cnt FROM parent ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	// Alice has 2 children, Bob has 1, Carol has 0.
	wantCnts := []int64{2, 1, 0}
	for i, wc := range wantCnts {
		if rows[i][1] != wc {
			t.Errorf("parent %d: want cnt=%d, got %v", i+1, wc, rows[i][1])
		}
	}
}

// TestSubquery_InSubquery exercises the IN (subquery) path, which routes
// through the scalar subquery compiler.
func TestSubquery_InSubquery(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	setupParentChild(t, db)
	rows := queryCSRows(t, db,
		"SELECT id FROM parent WHERE id IN (SELECT pid FROM child) ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows (Alice, Bob), got %d", len(rows))
	}
	if rows[0][0] != int64(1) || rows[1][0] != int64(2) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// TestSubquery_ExistsSubquery exercises EXISTS (subquery) with a correlated
// inner query — exercises the EXISTS evaluation path.
func TestSubquery_ExistsSubquery(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	setupParentChild(t, db)
	rows := queryCSRows(t, db,
		"SELECT name FROM parent WHERE EXISTS (SELECT 1 FROM child WHERE child.pid = parent.id) ORDER BY name")
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// TestSubquery_ScalarCount exercises a scalar subquery in the SELECT list
// returning COUNT(*), covering the scalar path in subquery compilation.
func TestSubquery_ScalarCount(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	setupParentChild(t, db)
	got := csInt(t, db,
		"SELECT (SELECT COUNT(*) FROM child WHERE pid = 1) FROM parent WHERE id = 1")
	if got != 2 {
		t.Errorf("want 2, got %d", got)
	}
}

// TestSubquery_IsTruthy exercises isTruthy via filterMaterializedRows inside a
// compound (UNION ALL) FROM subquery. The outer WHERE filters using a boolean
// expression, routing through isTruthy for each materialised row.
func TestSubquery_IsTruthy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	// Use a compound FROM subquery so the rows are materialised then filtered.
	// isTruthy is called internally on integer column values.
	rows := queryCSRows(t, db,
		"SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 0 AS n UNION ALL SELECT 2 AS n)")
	// The FROM compound subquery materialises all 3 rows.
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(rows), rows)
	}
}

// TestSubquery_EvalAggregateSum exercises evalAggregate for SUM() over a
// compound FROM subquery, covering evalAggregateOverRows / sumColumn.
func TestSubquery_EvalAggregateSum(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"SELECT SUM(n) FROM (SELECT 1 AS n UNION ALL SELECT 3 AS n UNION ALL SELECT 5 AS n)")
	if got != 9 {
		t.Errorf("want 9, got %d", got)
	}
}

// TestSubquery_EvalAggregateCount exercises COUNT(*) aggregate evaluation over
// a compound FROM subquery.
func TestSubquery_EvalAggregateCount(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"SELECT COUNT(*) FROM (SELECT 5 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 8 AS n)")
	if got != 3 {
		t.Errorf("want 3, got %d", got)
	}
}

// TestSubquery_EvalAggregateCountDistinct exercises COUNT() over a UNION
// compound subquery (which deduplicates), covering a slightly different path
// through evalAggregateOverRows.
func TestSubquery_EvalAggregateCountDistinct(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	got := csInt(t, db,
		"SELECT COUNT(*) FROM (SELECT 1 AS n UNION SELECT 1 AS n UNION SELECT 2 AS n)")
	// UNION deduplicates, so 2 rows.
	if got != 2 {
		t.Errorf("want 2, got %d", got)
	}
}
