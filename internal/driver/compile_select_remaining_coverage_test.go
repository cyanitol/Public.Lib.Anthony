// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

// rcrOpenDB opens an in-memory database for remaining-coverage tests.
func rcrOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("rcrOpenDB: %v", err)
	}
	return db
}

// rcrExec executes SQL statements, fataling on error.
func rcrExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("rcrExec %q: %v", s, err)
		}
	}
}

// rcrQueryRows runs a query and returns all rows as [][]interface{}.
func rcrQueryRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("rcrQueryRows %q: %v", query, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("rcrQueryRows scan: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rcrQueryRows rows.Err: %v", err)
	}
	return out
}

// rcrQueryOneString runs a single-column, single-row string query.
func rcrQueryOneString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var s string
	if err := db.QueryRow(query).Scan(&s); err != nil {
		t.Fatalf("rcrQueryOneString %q: %v", query, err)
	}
	return s
}

// rcrQueryOneInt runs a single-column, single-row int64 query.
func rcrQueryOneInt(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("rcrQueryOneInt %q: %v", query, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// handleTVFSelect: correlated TVF cross-join path
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_TVFCorrelatedJoin exercises the hasCorrelatedTVF branch
// of handleTVFSelect by joining a table with a TVF that references a column.
func TestCompileSelectRemaining_TVFCorrelatedJoin(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE nums (n INTEGER)",
		"INSERT INTO nums VALUES (3)",
		"INSERT INTO nums VALUES (5)",
	)

	rows := rcrQueryRows(t, db,
		"SELECT n, value FROM nums, generate_series(1, n)",
	)
	if len(rows) == 0 {
		t.Fatal("expected rows from correlated TVF join, got none")
	}
}

// TestCompileSelectRemaining_TVFWithAggregate exercises the TVF + aggregate branch
// of handleTVFSelect (materializeTVFAsEphemeral path) using generate_series.
func TestCompileSelectRemaining_TVFWithAggregate(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	got := rcrQueryOneInt(t, db, "SELECT COUNT(*) FROM generate_series(1, 7)")
	if got != 7 {
		t.Errorf("COUNT(*) FROM generate_series(1,7) = %d, want 7", got)
	}
}

// ---------------------------------------------------------------------------
// extractOrderByExpression: COLLATE expression path
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_OrderByCollate exercises the collate-expression branch
// inside extractOrderByExpression by using ORDER BY col COLLATE NOCASE.
func TestCompileSelectRemaining_OrderByCollate(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE words (w TEXT)",
		"INSERT INTO words VALUES ('banana')",
		"INSERT INTO words VALUES ('Apple')",
		"INSERT INTO words VALUES ('cherry')",
	)

	rows := rcrQueryRows(t, db, "SELECT w FROM words ORDER BY w COLLATE NOCASE ASC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Case-insensitive order: Apple, banana, cherry
	first, _ := rows[0][0].(string)
	if !strings.EqualFold(first, "apple") {
		t.Errorf("first row after COLLATE NOCASE order = %q, want apple (case-insensitive)", first)
	}
}

// ---------------------------------------------------------------------------
// emitJSONGroupObjectUpdate / loadJSONExprValue
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_JSONGroupObjectColumnRef exercises
// emitJSONGroupObjectUpdate and loadJSONExprValue with a direct column reference
// (the IdentExpr branch of loadJSONExprValue).
func TestCompileSelectRemaining_JSONGroupObjectColumnRef(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE kv (k TEXT, v INTEGER)",
		"INSERT INTO kv VALUES ('x', 1)",
		"INSERT INTO kv VALUES ('y', 2)",
		"INSERT INTO kv VALUES ('z', 3)",
	)

	got := rcrQueryOneString(t, db, "SELECT json_group_object(k, v) FROM kv")
	if !strings.Contains(got, `"x"`) || !strings.Contains(got, `"y"`) || !strings.Contains(got, `"z"`) {
		t.Errorf("json_group_object unexpected result: %s", got)
	}
}

// TestCompileSelectRemaining_JSONGroupObjectExprValue exercises loadJSONExprValue
// with an expression argument (non-IdentExpr, the GenerateExpr branch).
func TestCompileSelectRemaining_JSONGroupObjectExprValue(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE nums2 (id INTEGER, val INTEGER)",
		"INSERT INTO nums2 VALUES (1, 10)",
		"INSERT INTO nums2 VALUES (2, 20)",
	)

	// The key is a string literal (non-ident expression), exercising GenerateExpr branch.
	got := rcrQueryOneString(t, db, "SELECT json_group_object('key'||id, val) FROM nums2")
	if !strings.Contains(got, "key1") || !strings.Contains(got, "key2") {
		t.Errorf("json_group_object with expr key unexpected result: %s", got)
	}
}

// ---------------------------------------------------------------------------
// findAggregateInExpr: nested aggregate paths
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_FindAggInBinaryExpr exercises the BinaryExpr path
// of findAggregateInExpr (aggregate nested in arithmetic expression).
func TestCompileSelectRemaining_FindAggInBinaryExpr(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE vals (n INTEGER)",
		"INSERT INTO vals VALUES (5)",
		"INSERT INTO vals VALUES (10)",
		"INSERT INTO vals VALUES (15)",
	)

	// COUNT(*) + 1 forces findAggregateInExpr to walk the BinaryExpr tree.
	got := rcrQueryOneInt(t, db, "SELECT COUNT(*) + 1 FROM vals")
	if got != 4 {
		t.Errorf("COUNT(*)+1 = %d, want 4", got)
	}
}

// TestCompileSelectRemaining_FindAggInUnaryExpr exercises the UnaryExpr path of
// findAggregateInExpr by wrapping a count in a parenthesized expression that
// leads into the unary/paren arms of the expression walker.
func TestCompileSelectRemaining_FindAggInUnaryExpr(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE negvals (n INTEGER)",
		"INSERT INTO negvals VALUES (3)",
		"INSERT INTO negvals VALUES (7)",
	)

	// (COUNT(*)) wraps aggregate in a ParenExpr, exercising the ParenExpr arm of
	// findAggregateInExpr.  The result must be 2.
	got := rcrQueryOneInt(t, db, "SELECT (COUNT(*)) FROM negvals")
	if got != 2 {
		t.Errorf("(COUNT(*)) = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// fromTableAlias: alias registration path
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_FromTableAlias exercises the alias != tableName branch
// of fromTableAlias / setupAggregateVDBE by querying an aggregate with a table alias.
func TestCompileSelectRemaining_FromTableAlias(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE items (id INTEGER, price INTEGER)",
		"INSERT INTO items VALUES (1, 100)",
		"INSERT INTO items VALUES (2, 200)",
		"INSERT INTO items VALUES (3, 150)",
	)

	// Table alias 'i' differs from table name 'items', exercising alias branch.
	got := rcrQueryOneInt(t, db, "SELECT SUM(i.price) FROM items AS i")
	if got != 450 {
		t.Errorf("SUM(i.price) = %d, want 450", got)
	}
}

// ---------------------------------------------------------------------------
// emitBinaryOp: all arithmetic operators in aggregate context
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_EmitBinaryOpPlus exercises OpPlus in aggregate arithmetic.
func TestCompileSelectRemaining_EmitBinaryOpPlus(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE addtab (n INTEGER)",
		"INSERT INTO addtab VALUES (10)",
		"INSERT INTO addtab VALUES (20)",
	)

	got := rcrQueryOneInt(t, db, "SELECT SUM(n) + 5 FROM addtab")
	if got != 35 {
		t.Errorf("SUM(n)+5 = %d, want 35", got)
	}
}

// TestCompileSelectRemaining_EmitBinaryOpMinus exercises OpMinus in aggregate arithmetic.
func TestCompileSelectRemaining_EmitBinaryOpMinus(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE subtab (n INTEGER)",
		"INSERT INTO subtab VALUES (100)",
		"INSERT INTO subtab VALUES (40)",
	)

	got := rcrQueryOneInt(t, db, "SELECT SUM(n) - 10 FROM subtab")
	if got != 130 {
		t.Errorf("SUM(n)-10 = %d, want 130", got)
	}
}

// TestCompileSelectRemaining_EmitBinaryOpMul exercises OpMul in aggregate arithmetic.
func TestCompileSelectRemaining_EmitBinaryOpMul(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE multab (n INTEGER)",
		"INSERT INTO multab VALUES (3)",
		"INSERT INTO multab VALUES (7)",
	)

	got := rcrQueryOneInt(t, db, "SELECT COUNT(*) * 4 FROM multab")
	if got != 8 {
		t.Errorf("COUNT(*)*4 = %d, want 8", got)
	}
}

// TestCompileSelectRemaining_EmitBinaryOpDiv exercises OpDiv in aggregate arithmetic.
func TestCompileSelectRemaining_EmitBinaryOpDiv(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE divtab (n INTEGER)",
		"INSERT INTO divtab VALUES (20)",
		"INSERT INTO divtab VALUES (30)",
		"INSERT INTO divtab VALUES (50)",
	)

	// SUM(n) / 2 = 100 / 2 = 50
	got := rcrQueryOneInt(t, db, "SELECT SUM(n) / 2 FROM divtab")
	if got != 50 {
		t.Errorf("SUM(n)/2 = %d, want 50", got)
	}
}

// ---------------------------------------------------------------------------
// findColumnIndex: case-insensitive and uppercase fallback paths
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_FindColumnIndexCaseInsensitive exercises the
// second loop (case-insensitive EqualFold) in findColumnIndex by using a window
// PARTITION BY with a column name whose case does not match the schema definition.
func TestCompileSelectRemaining_FindColumnIndexCaseInsensitive(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE ci_tbl (Category TEXT, Amount INTEGER)",
		"INSERT INTO ci_tbl VALUES ('A', 10)",
		"INSERT INTO ci_tbl VALUES ('A', 20)",
		"INSERT INTO ci_tbl VALUES ('B', 5)",
	)

	// ORDER BY uses 'category' (lowercase) while schema column is 'Category'.
	// This hits the case-insensitive branch of findColumnIndex.
	rows := rcrQueryRows(t, db,
		"SELECT Category, ROW_NUMBER() OVER (ORDER BY category) FROM ci_tbl",
	)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_FindColumnIndexGroupBy exercises findColumnIndex
// in a GROUP BY aggregate query where the column name differs in case from schema.
func TestCompileSelectRemaining_FindColumnIndexGroupBy(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE gb_tbl (dept TEXT, salary INTEGER)",
		"INSERT INTO gb_tbl VALUES ('eng', 100)",
		"INSERT INTO gb_tbl VALUES ('eng', 200)",
		"INSERT INTO gb_tbl VALUES ('hr', 150)",
	)

	rows := rcrQueryRows(t, db, "SELECT dept, SUM(salary) FROM gb_tbl GROUP BY dept")
	if len(rows) != 2 {
		t.Fatalf("GROUP BY dept: expected 2 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// compileWindowWhereClause: non-nil WHERE path
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_WindowWhereClause exercises compileWindowWhereClause
// with a non-nil WHERE expression on the simple (unsorted) window path.
func TestCompileSelectRemaining_WindowWhereClause(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE wintbl (id INTEGER, val INTEGER)",
		"INSERT INTO wintbl VALUES (1, 10)",
		"INSERT INTO wintbl VALUES (2, 20)",
		"INSERT INTO wintbl VALUES (3, 30)",
		"INSERT INTO wintbl VALUES (4, 40)",
	)

	// RANK() OVER () with WHERE clause exercises the no-sort path with a WHERE.
	rows := rcrQueryRows(t, db,
		"SELECT id, RANK() OVER () FROM wintbl WHERE val > 15",
	)
	if len(rows) != 3 {
		t.Fatalf("window WHERE val>15: expected 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_WindowWhereClauseNil exercises the nil WHERE branch
// of compileWindowWhereClause (returns -1, no skip emitted).
func TestCompileSelectRemaining_WindowWhereClauseNil(t *testing.T) {
	t.Parallel()
	db := rcrOpenDB(t)
	defer db.Close()

	rcrExec(t, db,
		"CREATE TABLE wintbl2 (id INTEGER)",
		"INSERT INTO wintbl2 VALUES (1)",
		"INSERT INTO wintbl2 VALUES (2)",
	)

	rows := rcrQueryRows(t, db, "SELECT id, RANK() OVER () FROM wintbl2")
	if len(rows) != 2 {
		t.Fatalf("window no WHERE: expected 2 rows, got %d", len(rows))
	}
}
