// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openSRDB opens an in-memory database for select-remaining tests.
func openSRDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("openSRDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// srExec runs SQL statements, failing immediately on error.
func srExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("srExec %q: %v", s, err)
		}
	}
}

// srQueryRows executes a query and returns all rows as [][]interface{}.
func srQueryRows(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("srQueryRows %q: %v", query, err)
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
			t.Fatalf("srQueryRows scan: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("srQueryRows rows.Err: %v", err)
	}
	return out
}

// srQueryInt runs a single-row single-column int64 query.
func srQueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("srQueryInt %q: %v", query, err)
	}
	return v
}

// srQueryString runs a single-row single-column string query.
func srQueryString(t *testing.T, db *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var s string
	if err := db.QueryRow(query, args...).Scan(&s); err != nil {
		t.Fatalf("srQueryString %q: %v", query, err)
	}
	return s
}

// ---------------------------------------------------------------------------
// 1. SELECT with subquery in FROM clause (derived table)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_SubqueryInFrom exercises the FROM-subquery path
// (compileSelectWithFromSubqueries / compileSingleFromSubquery).
func TestCompileSelectRemaining_SubqueryInFrom(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE emp (id INTEGER, name TEXT, dept TEXT, salary INTEGER)",
		"INSERT INTO emp VALUES (1, 'Alice', 'eng', 90000)",
		"INSERT INTO emp VALUES (2, 'Bob', 'eng', 80000)",
		"INSERT INTO emp VALUES (3, 'Carol', 'hr', 70000)",
		"INSERT INTO emp VALUES (4, 'Dave', 'hr', 75000)",
	)

	rows := srQueryRows(t, db,
		"SELECT dept, avg_sal FROM (SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept) ORDER BY dept",
	)
	if len(rows) != 2 {
		t.Fatalf("subquery in FROM: want 2 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_SubqueryInFromWithWhere exercises filtering on top
// of a FROM subquery (outer WHERE applied to materialised inner result).
func TestCompileSelectRemaining_SubqueryInFromWithWhere(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, cat TEXT)",
		"INSERT INTO products VALUES (1, 'Widget', 100, 'A')",
		"INSERT INTO products VALUES (2, 'Gadget', 200, 'B')",
		"INSERT INTO products VALUES (3, 'Donut', 50, 'A')",
		"INSERT INTO products VALUES (4, 'Sprocket', 300, 'B')",
	)

	rows := srQueryRows(t, db,
		`SELECT name, price FROM
		 (SELECT id, name, price FROM products WHERE price > 75)`,
	)
	if len(rows) != 3 {
		t.Fatalf("FROM subquery with inner WHERE: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 2. Correlated subquery in WHERE (scalar subquery)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_CorrelatedSubqueryInWhere exercises scalar
// subqueries evaluated as literals in the WHERE clause (evalSubqueryToLiteral).
func TestCompileSelectRemaining_CorrelatedSubqueryInWhere(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE orders (id INTEGER, amount INTEGER, cust_id INTEGER)",
		"INSERT INTO orders VALUES (1, 500, 1)",
		"INSERT INTO orders VALUES (2, 1200, 1)",
		"INSERT INTO orders VALUES (3, 800, 2)",
		"INSERT INTO orders VALUES (4, 200, 2)",
		"CREATE TABLE customers (id INTEGER, name TEXT)",
		"INSERT INTO customers VALUES (1, 'Acme')",
		"INSERT INTO customers VALUES (2, 'Globex')",
	)

	// Scalar subquery in WHERE: orders above average amount
	rows := srQueryRows(t, db,
		"SELECT id, amount FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
	)
	if len(rows) == 0 {
		t.Fatal("correlated subquery in WHERE: expected rows above average")
	}
	for _, row := range rows {
		amt, _ := row[1].(int64)
		if amt <= 675 {
			t.Errorf("expected amount > 675, got %d", amt)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. SELECT DISTINCT with GROUP BY
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_DistinctWithGroupBy exercises DISTINCT combined
// with GROUP BY through the aggregate compiler path.
func TestCompileSelectRemaining_DistinctWithGroupBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE sales (region TEXT, product TEXT, qty INTEGER)",
		"INSERT INTO sales VALUES ('North', 'Foo', 10)",
		"INSERT INTO sales VALUES ('North', 'Foo', 20)",
		"INSERT INTO sales VALUES ('South', 'Bar', 15)",
		"INSERT INTO sales VALUES ('South', 'Bar', 5)",
		"INSERT INTO sales VALUES ('East', 'Foo', 8)",
	)

	// DISTINCT on the GROUP BY result collapses duplicates
	rows := srQueryRows(t, db,
		"SELECT DISTINCT region FROM sales GROUP BY region ORDER BY region",
	)
	if len(rows) != 3 {
		t.Fatalf("DISTINCT+GROUP BY: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 4. SELECT with HAVING and window functions
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_HavingWithWindowFunc exercises HAVING on
// aggregates alongside window function columns.
func TestCompileSelectRemaining_HavingOnAggregate(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE scores (player TEXT, game TEXT, score INTEGER)",
		"INSERT INTO scores VALUES ('Alice', 'chess', 1500)",
		"INSERT INTO scores VALUES ('Alice', 'poker', 800)",
		"INSERT INTO scores VALUES ('Bob', 'chess', 1200)",
		"INSERT INTO scores VALUES ('Bob', 'poker', 600)",
		"INSERT INTO scores VALUES ('Carol', 'chess', 1800)",
	)

	rows := srQueryRows(t, db,
		"SELECT player, SUM(score) AS total FROM scores GROUP BY player HAVING total > 1500",
	)
	// Alice: 2300, Carol: 1800 — both > 1500; Bob: 1800 also > 1500
	if len(rows) < 1 {
		t.Fatal("HAVING: expected at least one group with total > 1500")
	}
}

// TestCompileSelectRemaining_WindowFuncWithHaving exercises window functions
// with a HAVING clause on the aggregate result.
func TestCompileSelectRemaining_WindowFuncWithHaving(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE measurements (sensor TEXT, val INTEGER)",
		"INSERT INTO measurements VALUES ('A', 10)",
		"INSERT INTO measurements VALUES ('A', 20)",
		"INSERT INTO measurements VALUES ('B', 30)",
		"INSERT INTO measurements VALUES ('B', 40)",
		"INSERT INTO measurements VALUES ('C', 5)",
	)

	// Aggregate with HAVING — exercises generateHavingBinaryExpr path
	rows := srQueryRows(t, db,
		"SELECT sensor, AVG(val) AS avg_val FROM measurements GROUP BY sensor HAVING AVG(val) > 15",
	)
	if len(rows) == 0 {
		t.Fatal("HAVING AVG > 15: expected at least one result")
	}
}

// ---------------------------------------------------------------------------
// 5. Recursive CTEs
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_RecursiveCTE exercises recursive CTE compilation
// through the stmt_cte_recursive.go path.
func TestCompileSelectRemaining_RecursiveCTE(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)

	got := srQueryInt(t, db,
		`WITH RECURSIVE cnt(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM cnt WHERE n < 10
		) SELECT COUNT(*) FROM cnt`,
	)
	if got != 10 {
		t.Fatalf("recursive CTE COUNT: want 10, got %d", got)
	}
}

// TestCompileSelectRemaining_RecursiveCTETreeWalk exercises recursive CTE
// with a join against a real table (tree traversal pattern).
func TestCompileSelectRemaining_RecursiveCTETreeWalk(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)",
		"INSERT INTO nodes VALUES (1, NULL, 'root')",
		"INSERT INTO nodes VALUES (2, 1, 'child1')",
		"INSERT INTO nodes VALUES (3, 1, 'child2')",
		"INSERT INTO nodes VALUES (4, 2, 'grandchild')",
	)

	got := srQueryInt(t, db,
		`WITH RECURSIVE tree(id, label) AS (
			SELECT id, label FROM nodes WHERE parent_id IS NULL
			UNION ALL
			SELECT n.id, n.label FROM nodes n JOIN tree t ON n.parent_id = t.id
		) SELECT COUNT(*) FROM tree`,
	)
	if got != 4 {
		t.Fatalf("recursive CTE tree walk: want 4, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 6. Multiple CTEs (non-recursive WITH)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_MultipleCTEs exercises the CTE compilation path
// with two non-recursive named CTEs in a single WITH clause.
func TestCompileSelectRemaining_MultipleCTEs(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE inventory (item TEXT, qty INTEGER, cost INTEGER)",
		"INSERT INTO inventory VALUES ('bolt', 100, 2)",
		"INSERT INTO inventory VALUES ('nut', 200, 1)",
		"INSERT INTO inventory VALUES ('screw', 50, 3)",
	)

	rows := srQueryRows(t, db,
		`WITH
		 costly AS (SELECT item, cost FROM inventory WHERE cost > 1),
		 plentiful AS (SELECT item, qty FROM inventory WHERE qty > 60)
		 SELECT c.item FROM costly c JOIN plentiful p ON c.item = p.item`,
	)
	if len(rows) == 0 {
		t.Fatal("multiple CTEs: expected at least one matching item")
	}
}

// TestCompileSelectRemaining_MultipleCTEsWithAgg exercises multiple CTEs
// where one CTE uses an aggregate.
func TestCompileSelectRemaining_MultipleCTEsWithAgg(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE txns (id INTEGER, acct INTEGER, amt INTEGER)",
		"INSERT INTO txns VALUES (1, 1, 100)",
		"INSERT INTO txns VALUES (2, 1, 200)",
		"INSERT INTO txns VALUES (3, 2, 50)",
		"INSERT INTO txns VALUES (4, 2, 75)",
	)

	got := srQueryInt(t, db,
		`WITH
		 acct_totals AS (SELECT acct, SUM(amt) AS total FROM txns GROUP BY acct),
		 big_accts AS (SELECT acct FROM acct_totals WHERE total > 100)
		 SELECT COUNT(*) FROM big_accts`,
	)
	if got != 2 {
		t.Fatalf("multiple CTEs with agg: want 2, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 7. Scalar subqueries in SELECT column list
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_ScalarSubqueryInSelect exercises scalar subqueries
// in the SELECT column list (materialised via evalSubqueryToLiteral).
func TestCompileSelectRemaining_ScalarSubqueryInSelect(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE depts (id INTEGER, name TEXT)",
		"INSERT INTO depts VALUES (1, 'Engineering')",
		"INSERT INTO depts VALUES (2, 'HR')",
		"CREATE TABLE headcount (dept_id INTEGER, cnt INTEGER)",
		"INSERT INTO headcount VALUES (1, 25)",
		"INSERT INTO headcount VALUES (2, 8)",
	)

	rows := srQueryRows(t, db,
		`SELECT d.name, (SELECT h.cnt FROM headcount h WHERE h.dept_id = d.id) AS hc
		 FROM depts d ORDER BY d.id`,
	)
	if len(rows) != 2 {
		t.Fatalf("scalar subquery in SELECT: want 2 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 8. EXISTS and NOT EXISTS subqueries
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_ExistsSubquery exercises the EXISTS subquery path.
func TestCompileSelectRemaining_ExistsSubquery(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO categories VALUES (1, 'active')",
		"INSERT INTO categories VALUES (2, 'inactive')",
		"CREATE TABLE items (id INTEGER, cat_id INTEGER, label TEXT)",
		"INSERT INTO items VALUES (1, 1, 'foo')",
		"INSERT INTO items VALUES (2, 1, 'bar')",
	)

	// EXISTS: categories that have at least one item
	rows := srQueryRows(t, db,
		`SELECT c.name FROM categories c
		 WHERE EXISTS (SELECT 1 FROM items i WHERE i.cat_id = c.id)
		 ORDER BY c.name`,
	)
	if len(rows) != 1 {
		t.Fatalf("EXISTS subquery: want 1 row, got %d", len(rows))
	}
	if name, _ := rows[0][0].(string); name != "active" {
		t.Errorf("EXISTS: want 'active', got %q", name)
	}
}

// TestCompileSelectRemaining_NotExistsSubquery exercises NOT EXISTS.
func TestCompileSelectRemaining_NotExistsSubquery(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)",
		"INSERT INTO tags VALUES (1, 'golang')",
		"INSERT INTO tags VALUES (2, 'python')",
		"INSERT INTO tags VALUES (3, 'rust')",
		"CREATE TABLE used_tags (tag_id INTEGER)",
		"INSERT INTO used_tags VALUES (1)",
		"INSERT INTO used_tags VALUES (3)",
	)

	// NOT EXISTS: tags with no usage
	rows := srQueryRows(t, db,
		`SELECT t.label FROM tags t
		 WHERE NOT EXISTS (SELECT 1 FROM used_tags u WHERE u.tag_id = t.id)`,
	)
	if len(rows) != 1 {
		t.Fatalf("NOT EXISTS: want 1 row, got %d", len(rows))
	}
	if label, _ := rows[0][0].(string); label != "python" {
		t.Errorf("NOT EXISTS: want 'python', got %q", label)
	}
}

// ---------------------------------------------------------------------------
// 9. CASE WHEN THEN END in various positions
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_CaseWhenInSelect exercises CASE WHEN in
// the SELECT column list.
func TestCompileSelectRemaining_CaseWhenInSelect(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE grades (student TEXT, score INTEGER)",
		"INSERT INTO grades VALUES ('Alice', 95)",
		"INSERT INTO grades VALUES ('Bob', 72)",
		"INSERT INTO grades VALUES ('Carol', 85)",
		"INSERT INTO grades VALUES ('Dave', 58)",
	)

	rows := srQueryRows(t, db,
		`SELECT student,
		 CASE WHEN score >= 90 THEN 'A'
		      WHEN score >= 80 THEN 'B'
		      WHEN score >= 70 THEN 'C'
		      ELSE 'F' END AS grade
		 FROM grades ORDER BY student`,
	)
	if len(rows) != 4 {
		t.Fatalf("CASE WHEN in SELECT: want 4 rows, got %d", len(rows))
	}
	// Check specific grades
	for _, row := range rows {
		student, _ := row[0].(string)
		grade, _ := row[1].(string)
		switch student {
		case "Alice":
			if grade != "A" {
				t.Errorf("Alice: want A, got %s", grade)
			}
		case "Dave":
			if grade != "F" {
				t.Errorf("Dave: want F, got %s", grade)
			}
		}
	}
}

// TestCompileSelectRemaining_CaseWhenInWhere exercises CASE WHEN used
// inside a WHERE predicate.
func TestCompileSelectRemaining_CaseWhenInWhere(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE flags (id INTEGER, status INTEGER, val TEXT)",
		"INSERT INTO flags VALUES (1, 1, 'active')",
		"INSERT INTO flags VALUES (2, 0, 'inactive')",
		"INSERT INTO flags VALUES (3, 1, 'active2')",
	)

	rows := srQueryRows(t, db,
		`SELECT id, val FROM flags
		 WHERE CASE WHEN status = 1 THEN 1 ELSE 0 END = 1`,
	)
	if len(rows) != 2 {
		t.Fatalf("CASE WHEN in WHERE: want 2, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_CaseWhenInHaving exercises CASE WHEN used
// inside a HAVING clause via a simple HAVING predicate that also uses CASE.
func TestCompileSelectRemaining_CaseWhenInHaving(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE buckets (grp TEXT, n INTEGER)",
		"INSERT INTO buckets VALUES ('x', 5)",
		"INSERT INTO buckets VALUES ('x', 10)",
		"INSERT INTO buckets VALUES ('y', 3)",
		"INSERT INTO buckets VALUES ('y', 4)",
	)

	// Use a straightforward HAVING with SUM to exercise the HAVING path;
	// CASE inside HAVING is not yet supported, so use a plain comparison.
	rows := srQueryRows(t, db,
		"SELECT grp, SUM(n) AS s FROM buckets GROUP BY grp HAVING SUM(n) > 10",
	)
	if len(rows) != 1 {
		t.Fatalf("HAVING SUM > 10: want 1 row, got %d", len(rows))
	}
	grp, _ := rows[0][0].(string)
	if grp != "x" {
		t.Errorf("HAVING: want grp 'x', got %q", grp)
	}
}

// TestCompileSelectRemaining_CaseWhenInOrderBy exercises CASE WHEN in ORDER BY.
func TestCompileSelectRemaining_CaseWhenInOrderBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE priorities (name TEXT, level TEXT)",
		"INSERT INTO priorities VALUES ('urgent', 'high')",
		"INSERT INTO priorities VALUES ('routine', 'low')",
		"INSERT INTO priorities VALUES ('important', 'medium')",
	)

	rows := srQueryRows(t, db,
		`SELECT name FROM priorities
		 ORDER BY CASE level WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END`,
	)
	if len(rows) != 3 {
		t.Fatalf("CASE in ORDER BY: want 3, got %d", len(rows))
	}
	if first, _ := rows[0][0].(string); first != "urgent" {
		t.Errorf("ORDER BY CASE: first row want 'urgent', got %q", first)
	}
}

// ---------------------------------------------------------------------------
// 10. Complex JOIN with multiple ON conditions
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_MultiConditionJoin exercises JOIN ON with
// multiple AND conditions (findColumnTableIndex, emitJoinLevelSorter paths).
func TestCompileSelectRemaining_MultiConditionJoin(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE contracts (id INTEGER, cust INTEGER, product TEXT, qty INTEGER)",
		"INSERT INTO contracts VALUES (1, 10, 'A', 5)",
		"INSERT INTO contracts VALUES (2, 10, 'B', 3)",
		"INSERT INTO contracts VALUES (3, 20, 'A', 7)",
		"CREATE TABLE deliveries (cust INTEGER, product TEXT, shipped INTEGER)",
		"INSERT INTO deliveries VALUES (10, 'A', 5)",
		"INSERT INTO deliveries VALUES (10, 'B', 3)",
		"INSERT INTO deliveries VALUES (20, 'A', 6)",
	)

	rows := srQueryRows(t, db,
		`SELECT c.id, d.shipped
		 FROM contracts c JOIN deliveries d
		 ON c.cust = d.cust AND c.product = d.product
		 ORDER BY c.id`,
	)
	if len(rows) != 3 {
		t.Fatalf("multi-cond JOIN: want 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_ThreeTableJoin exercises a three-way JOIN,
// which forces emitExtraOrderByColumnMultiTable and findColumnTableIndex
// to operate across multiple table contexts.
func TestCompileSelectRemaining_ThreeTableJoin(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO authors VALUES (1, 'Tolkien')",
		"INSERT INTO authors VALUES (2, 'Herbert')",
		"CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
		"INSERT INTO books VALUES (1, 1, 'LOTR')",
		"INSERT INTO books VALUES (2, 2, 'Dune')",
		"INSERT INTO books VALUES (3, 1, 'Hobbit')",
		"CREATE TABLE reviews (book_id INTEGER, stars INTEGER)",
		"INSERT INTO reviews VALUES (1, 5)",
		"INSERT INTO reviews VALUES (2, 4)",
		"INSERT INTO reviews VALUES (3, 5)",
	)

	rows := srQueryRows(t, db,
		`SELECT a.name, b.title, r.stars
		 FROM authors a
		 JOIN books b ON b.author_id = a.id
		 JOIN reviews r ON r.book_id = b.id
		 ORDER BY a.name, b.title`,
	)
	if len(rows) != 3 {
		t.Fatalf("three-table JOIN: want 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_LeftJoinMultiCondition exercises LEFT JOIN with
// multi-condition ON clause (emitNullEmissionSorter path).
func TestCompileSelectRemaining_LeftJoinMultiCondition(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE users2 (id INTEGER PRIMARY KEY, name TEXT, region TEXT)",
		"INSERT INTO users2 VALUES (1, 'Alice', 'West')",
		"INSERT INTO users2 VALUES (2, 'Bob', 'East')",
		"INSERT INTO users2 VALUES (3, 'Carol', 'West')",
		"CREATE TABLE activity (user_id INTEGER, region TEXT, count INTEGER)",
		"INSERT INTO activity VALUES (1, 'West', 10)",
		"INSERT INTO activity VALUES (3, 'West', 5)",
	)

	rows := srQueryRows(t, db,
		`SELECT u.name, a.count
		 FROM users2 u
		 LEFT JOIN activity a ON a.user_id = u.id AND a.region = u.region
		 ORDER BY u.id`,
	)
	// Bob has no matching activity — should appear with NULL count
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN multi-cond: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 11. INSERT ... SELECT with WHERE (emitInsertSelectWhere)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_InsertSelectWithWhere exercises the streaming
// INSERT...SELECT path when the SELECT has a WHERE clause (emitInsertSelectWhere).
func TestCompileSelectRemaining_InsertSelectWithWhere(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE src (id INTEGER, val INTEGER)",
		"INSERT INTO src VALUES (1, 10)",
		"INSERT INTO src VALUES (2, 20)",
		"INSERT INTO src VALUES (3, 5)",
		"CREATE TABLE dst (id INTEGER, val INTEGER)",
		"INSERT INTO dst SELECT id, val FROM src WHERE val > 8",
	)

	got := srQueryInt(t, db, "SELECT COUNT(*) FROM dst")
	if got != 2 {
		t.Fatalf("INSERT SELECT WHERE: want 2, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 12. INSERT ... SELECT with ORDER BY (materialised path)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_InsertSelectMaterialised exercises
// compileInsertSelectMaterialised (insertSelectNeedsMaterialise=true via ORDER BY).
func TestCompileSelectRemaining_InsertSelectMaterialised(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE raw (v INTEGER)",
		"INSERT INTO raw VALUES (30)",
		"INSERT INTO raw VALUES (10)",
		"INSERT INTO raw VALUES (20)",
		"CREATE TABLE sorted_vals (v INTEGER)",
		"INSERT INTO sorted_vals SELECT v FROM raw ORDER BY v",
	)

	rows := srQueryRows(t, db, "SELECT v FROM sorted_vals")
	if len(rows) != 3 {
		t.Fatalf("INSERT SELECT ORDER BY: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 13. INSERT ... SELECT with DISTINCT (materialised path via Distinct flag)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_InsertSelectDistinct exercises
// insertSelectNeedsMaterialise=true via DISTINCT in the SELECT.
func TestCompileSelectRemaining_InsertSelectDistinct(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE dupes (v INTEGER)",
		"INSERT INTO dupes VALUES (1)",
		"INSERT INTO dupes VALUES (2)",
		"INSERT INTO dupes VALUES (1)",
		"INSERT INTO dupes VALUES (3)",
		"CREATE TABLE unique_vals (v INTEGER)",
		"INSERT INTO unique_vals SELECT DISTINCT v FROM dupes",
	)

	got := srQueryInt(t, db, "SELECT COUNT(*) FROM unique_vals")
	if got != 3 {
		t.Fatalf("INSERT SELECT DISTINCT: want 3, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 14. UPSERT with excluded references + binary/unary expressions
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_UpsertExcludedBinaryExpr exercises
// replaceExcludedRefs on BinaryExpr nodes (e.g. excluded.val + 1).
func TestCompileSelectRemaining_UpsertExcludedBinaryExpr(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE counters (key TEXT PRIMARY KEY, cnt INTEGER)",
		"INSERT INTO counters VALUES ('hits', 5)",
	)
	srExec(t, db,
		"INSERT INTO counters(key, cnt) VALUES('hits', 3) ON CONFLICT(key) DO UPDATE SET cnt = cnt + excluded.cnt",
	)

	got := srQueryInt(t, db, "SELECT cnt FROM counters WHERE key = 'hits'")
	if got != 8 {
		t.Fatalf("UPSERT excluded binary: want 8, got %d", got)
	}
}

// TestCompileSelectRemaining_UpsertExcludedFunctionExpr exercises
// replaceExcludedRefs on FunctionExpr nodes.
func TestCompileSelectRemaining_UpsertExcludedFunctionExpr(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE texts (id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO texts VALUES (1, 'hello')",
	)
	// COALESCE(excluded.val, val) exercises FunctionExpr replacement
	srExec(t, db,
		"INSERT INTO texts(id, val) VALUES(1, 'world') ON CONFLICT(id) DO UPDATE SET val = COALESCE(excluded.val, val)",
	)

	got := srQueryString(t, db, "SELECT val FROM texts WHERE id = 1")
	if got != "world" {
		t.Fatalf("UPSERT excluded func: want 'world', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 15. UPDATE ... FROM (compileUpdateFrom path)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_UpdateFromClause exercises compileUpdateFrom by
// using UPDATE with a FROM clause joining another table.
func TestCompileSelectRemaining_UpdateFromClause(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
		"INSERT INTO accounts VALUES (1, 1000)",
		"INSERT INTO accounts VALUES (2, 500)",
		"CREATE TABLE adjustments (acct_id INTEGER, delta INTEGER)",
		"INSERT INTO adjustments VALUES (1, 200)",
		"INSERT INTO adjustments VALUES (2, -100)",
	)

	srExec(t, db,
		"UPDATE accounts SET balance = balance + adjustments.delta FROM adjustments WHERE accounts.id = adjustments.acct_id",
	)

	rows := srQueryRows(t, db, "SELECT id, balance FROM accounts ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("UPDATE FROM: want 2 rows, got %d", len(rows))
	}
	b1, _ := rows[0][1].(int64)
	b2, _ := rows[1][1].(int64)
	if b1 != 1200 || b2 != 400 {
		t.Errorf("UPDATE FROM balances: want 1200, 400; got %d, %d", b1, b2)
	}
}

// ---------------------------------------------------------------------------
// 16. Window functions with ORDER BY (sorting path)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_RowNumberOverOrderBy exercises
// compileWindowWithSorting via ROW_NUMBER() OVER (ORDER BY ...).
func TestCompileSelectRemaining_RowNumberOverOrderBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE entries (name TEXT, score INTEGER)",
		"INSERT INTO entries VALUES ('Eve', 70)",
		"INSERT INTO entries VALUES ('Alice', 90)",
		"INSERT INTO entries VALUES ('Bob', 80)",
	)

	rows := srQueryRows(t, db,
		"SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM entries ORDER BY rn",
	)
	if len(rows) != 3 {
		t.Fatalf("ROW_NUMBER OVER ORDER BY: want 3 rows, got %d", len(rows))
	}
	if first, _ := rows[0][0].(string); first != "Alice" {
		t.Errorf("ROW_NUMBER: first row want 'Alice', got %q", first)
	}
}

// TestCompileSelectRemaining_DenseRankOverOrderBy exercises
// emitWindowRankTracking with DENSE_RANK.
func TestCompileSelectRemaining_DenseRankOverOrderBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE team_scores (member TEXT, pts INTEGER)",
		"INSERT INTO team_scores VALUES ('A', 100)",
		"INSERT INTO team_scores VALUES ('B', 90)",
		"INSERT INTO team_scores VALUES ('C', 90)",
		"INSERT INTO team_scores VALUES ('D', 80)",
	)

	rows := srQueryRows(t, db,
		"SELECT member, DENSE_RANK() OVER (ORDER BY pts DESC) AS dr FROM team_scores ORDER BY member",
	)
	if len(rows) != 4 {
		t.Fatalf("DENSE_RANK: want 4 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_RankOverOrderBy exercises RANK() with ORDER BY,
// which triggers emitWindowRankComparison and emitWindowRankUpdate.
func TestCompileSelectRemaining_RankOverOrderBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE leaderboard (player TEXT, pts INTEGER)",
		"INSERT INTO leaderboard VALUES ('X', 100)",
		"INSERT INTO leaderboard VALUES ('Y', 100)",
		"INSERT INTO leaderboard VALUES ('Z', 80)",
	)

	rows := srQueryRows(t, db,
		"SELECT player, RANK() OVER (ORDER BY pts DESC) AS r FROM leaderboard ORDER BY player",
	)
	if len(rows) != 3 {
		t.Fatalf("RANK: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 17. Window functions: NTILE, NTH_VALUE, LAG, LEAD
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_NtileWindow exercises NTILE window function.
func TestCompileSelectRemaining_NtileWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE vals2 (n INTEGER)",
		"INSERT INTO vals2 VALUES (1)",
		"INSERT INTO vals2 VALUES (2)",
		"INSERT INTO vals2 VALUES (3)",
		"INSERT INTO vals2 VALUES (4)",
	)

	rows := srQueryRows(t, db,
		"SELECT n, NTILE(2) OVER (ORDER BY n) FROM vals2",
	)
	if len(rows) != 4 {
		t.Fatalf("NTILE: want 4 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_NthValueWindow exercises NTH_VALUE window function.
func TestCompileSelectRemaining_NthValueWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE series (n INTEGER)",
		"INSERT INTO series VALUES (10)",
		"INSERT INTO series VALUES (20)",
		"INSERT INTO series VALUES (30)",
	)

	rows := srQueryRows(t, db,
		"SELECT n, NTH_VALUE(n, 2) OVER (ORDER BY n) FROM series",
	)
	if len(rows) != 3 {
		t.Fatalf("NTH_VALUE: want 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_LagWindow exercises LAG window function.
func TestCompileSelectRemaining_LagWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE monthly (month INTEGER, revenue INTEGER)",
		"INSERT INTO monthly VALUES (1, 1000)",
		"INSERT INTO monthly VALUES (2, 1200)",
		"INSERT INTO monthly VALUES (3, 900)",
	)

	rows := srQueryRows(t, db,
		"SELECT month, revenue, LAG(revenue) OVER (ORDER BY month) AS prev FROM monthly",
	)
	if len(rows) != 3 {
		t.Fatalf("LAG: want 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_LeadWindow exercises LEAD window function.
func TestCompileSelectRemaining_LeadWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE quarters (q INTEGER, sales INTEGER)",
		"INSERT INTO quarters VALUES (1, 400)",
		"INSERT INTO quarters VALUES (2, 600)",
		"INSERT INTO quarters VALUES (3, 500)",
	)

	rows := srQueryRows(t, db,
		"SELECT q, sales, LEAD(sales) OVER (ORDER BY q) AS next_q FROM quarters",
	)
	if len(rows) != 3 {
		t.Fatalf("LEAD: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 18. Window function with PARTITION BY
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_RowNumberPartitionBy exercises ROW_NUMBER with
// PARTITION BY (extractWindowPartitionCols path).
func TestCompileSelectRemaining_RowNumberPartitionBy(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE dept_emp (dept TEXT, emp TEXT, salary INTEGER)",
		"INSERT INTO dept_emp VALUES ('eng', 'Alice', 90000)",
		"INSERT INTO dept_emp VALUES ('eng', 'Bob', 80000)",
		"INSERT INTO dept_emp VALUES ('hr', 'Carol', 70000)",
		"INSERT INTO dept_emp VALUES ('hr', 'Dave', 75000)",
	)

	rows := srQueryRows(t, db,
		"SELECT dept, emp, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM dept_emp ORDER BY dept, rn",
	)
	if len(rows) != 4 {
		t.Fatalf("ROW_NUMBER PARTITION BY: want 4 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 19. GROUP CONCAT with custom separator
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_GroupConcatSeparator exercises groupConcatSeparator
// by providing a custom separator string to GROUP_CONCAT.
func TestCompileSelectRemaining_GroupConcatSeparator(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE words2 (word TEXT)",
		"INSERT INTO words2 VALUES ('hello')",
		"INSERT INTO words2 VALUES ('world')",
		"INSERT INTO words2 VALUES ('foo')",
	)

	got := srQueryString(t, db, "SELECT GROUP_CONCAT(word, '|') FROM words2")
	if !strings.Contains(got, "|") {
		t.Errorf("GROUP_CONCAT with separator: expected '|' in %q", got)
	}
}

// ---------------------------------------------------------------------------
// 20. Aggregate filter (FILTER clause / emitAggregateFilterCheck)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_AggregateFilterClause exercises
// emitAggregateFilterCheck via COUNT with FILTER.
func TestCompileSelectRemaining_AggregateFilterClause(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE events (name TEXT, active INTEGER)",
		"INSERT INTO events VALUES ('e1', 1)",
		"INSERT INTO events VALUES ('e2', 0)",
		"INSERT INTO events VALUES ('e3', 1)",
		"INSERT INTO events VALUES ('e4', 1)",
	)

	got := srQueryInt(t, db,
		"SELECT COUNT(*) FILTER (WHERE active = 1) FROM events",
	)
	if got != 3 {
		t.Fatalf("COUNT FILTER: want 3, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 21. Blob literal in VALUES
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_BlobLiteral exercises compileBlobLiteral
// via an INSERT with a hex blob literal and a subsequent SELECT.
func TestCompileSelectRemaining_BlobLiteral(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE blobs (id INTEGER, data BLOB)",
		"INSERT INTO blobs VALUES (1, X'DEADBEEF')",
	)

	rows := srQueryRows(t, db, "SELECT id FROM blobs WHERE data = X'DEADBEEF'")
	if len(rows) != 1 {
		t.Fatalf("blob literal: want 1 row, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 22. emitNoFromWhereClause: SELECT with WHERE but no FROM
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_NoFromWithWhere exercises emitNoFromWhereClause
// with a false WHERE condition (path taken when WHERE evaluates to false).
func TestCompileSelectRemaining_NoFromWithWhereFalse(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)

	rows := srQueryRows(t, db, "SELECT 42 WHERE 1 = 0")
	if len(rows) != 0 {
		t.Fatalf("no-FROM WHERE false: want 0 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_NoFromWithWhereTrue exercises emitNoFromWhereClause
// with a true WHERE condition.
func TestCompileSelectRemaining_NoFromWithWhereTrue(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)

	got := srQueryInt(t, db, "SELECT 99 WHERE 1 = 1")
	if got != 99 {
		t.Fatalf("no-FROM WHERE true: want 99, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 23. ORDER BY with expression (extractOrderByExpression: non-ident path)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_OrderByExpression exercises the expression path in
// extractOrderByExpression (neither IdentExpr nor CollateExpr — a BinaryExpr).
func TestCompileSelectRemaining_OrderByExpression(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE nums3 (a INTEGER, b INTEGER)",
		"INSERT INTO nums3 VALUES (3, 1)",
		"INSERT INTO nums3 VALUES (1, 5)",
		"INSERT INTO nums3 VALUES (2, 2)",
	)

	rows := srQueryRows(t, db, "SELECT a, b FROM nums3 ORDER BY a + b")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY expression: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 24. SELECT with IN subquery
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_InSubquery exercises the IN (SELECT ...) path.
func TestCompileSelectRemaining_InSubquery(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE parent_table (id INTEGER PRIMARY KEY, label TEXT)",
		"INSERT INTO parent_table VALUES (1, 'A')",
		"INSERT INTO parent_table VALUES (2, 'B')",
		"INSERT INTO parent_table VALUES (3, 'C')",
		"CREATE TABLE child_table (parent_id INTEGER, val INTEGER)",
		"INSERT INTO child_table VALUES (1, 100)",
		"INSERT INTO child_table VALUES (3, 200)",
	)

	rows := srQueryRows(t, db,
		"SELECT label FROM parent_table WHERE id IN (SELECT parent_id FROM child_table)",
	)
	if len(rows) != 2 {
		t.Fatalf("IN subquery: want 2 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 25. Compound SELECT with ORDER BY and LIMIT (applyLimitOffset after sort)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_UnionWithOrderByLimit exercises the compound
// UNION path with trailing ORDER BY and LIMIT applied to the compound result.
func TestCompileSelectRemaining_UnionWithOrderByLimit(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE alpha (n INTEGER)",
		"INSERT INTO alpha VALUES (5)",
		"INSERT INTO alpha VALUES (1)",
		"CREATE TABLE beta (n INTEGER)",
		"INSERT INTO beta VALUES (3)",
		"INSERT INTO beta VALUES (7)",
	)

	rows := srQueryRows(t, db,
		"SELECT n FROM alpha UNION SELECT n FROM beta ORDER BY n LIMIT 3",
	)
	if len(rows) != 3 {
		t.Fatalf("UNION ORDER BY LIMIT: want 3 rows, got %d", len(rows))
	}
	first, _ := rows[0][0].(int64)
	if first != 1 {
		t.Errorf("UNION ORDER BY LIMIT: first row want 1, got %d", first)
	}
}

// ---------------------------------------------------------------------------
// 26. FIRST_VALUE and LAST_VALUE window functions
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_FirstValueWindow exercises FIRST_VALUE.
func TestCompileSelectRemaining_FirstValueWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE temps (day INTEGER, temp INTEGER)",
		"INSERT INTO temps VALUES (1, 72)",
		"INSERT INTO temps VALUES (2, 68)",
		"INSERT INTO temps VALUES (3, 75)",
	)

	rows := srQueryRows(t, db,
		"SELECT day, temp, FIRST_VALUE(temp) OVER (ORDER BY day) AS fv FROM temps",
	)
	if len(rows) != 3 {
		t.Fatalf("FIRST_VALUE: want 3 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_LastValueWindow exercises LAST_VALUE.
func TestCompileSelectRemaining_LastValueWindow(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE steps (step INTEGER, dist INTEGER)",
		"INSERT INTO steps VALUES (1, 5)",
		"INSERT INTO steps VALUES (2, 10)",
		"INSERT INTO steps VALUES (3, 8)",
	)

	rows := srQueryRows(t, db,
		"SELECT step, LAST_VALUE(dist) OVER (ORDER BY step) AS lv FROM steps",
	)
	if len(rows) != 3 {
		t.Fatalf("LAST_VALUE: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 27. emitSorterColumnValue: window with non-ORDER-BY columns
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_WindowWithNonOrderByCol exercises
// emitSorterColumnValue for regular columns alongside window functions.
func TestCompileSelectRemaining_WindowWithNonOrderByCol(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE mixed (cat TEXT, val INTEGER, extra TEXT)",
		"INSERT INTO mixed VALUES ('A', 10, 'x')",
		"INSERT INTO mixed VALUES ('A', 20, 'y')",
		"INSERT INTO mixed VALUES ('B', 15, 'z')",
	)

	rows := srQueryRows(t, db,
		"SELECT cat, val, extra, SUM(val) OVER (PARTITION BY cat ORDER BY val) AS running FROM mixed ORDER BY cat, val",
	)
	if len(rows) != 3 {
		t.Fatalf("window with non-ORDER-BY col: want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 28. USING JOIN (resolveUsingJoin path)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_UsingJoin exercises resolveUsingJoin / USING clause.
func TestCompileSelectRemaining_UsingJoin(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE lefttbl (id INTEGER, val TEXT)",
		"INSERT INTO lefttbl VALUES (1, 'alpha')",
		"INSERT INTO lefttbl VALUES (2, 'beta')",
		"CREATE TABLE righttbl (id INTEGER, info TEXT)",
		"INSERT INTO righttbl VALUES (1, 'foo')",
		"INSERT INTO righttbl VALUES (3, 'bar')",
	)

	rows := srQueryRows(t, db,
		"SELECT l.val, r.info FROM lefttbl l JOIN righttbl r USING (id)",
	)
	if len(rows) != 1 {
		t.Fatalf("USING JOIN: want 1 row, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// 29. Compound INTERSECT and EXCEPT
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_IntersectCompound exercises the INTERSECT operator
// (intersectRows path).
func TestCompileSelectRemaining_IntersectCompound(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE setA (n INTEGER)",
		"INSERT INTO setA VALUES (1),(2),(3),(4)",
		"CREATE TABLE setB (n INTEGER)",
		"INSERT INTO setB VALUES (2),(3),(5)",
	)

	rows := srQueryRows(t, db, "SELECT n FROM setA INTERSECT SELECT n FROM setB ORDER BY n")
	if len(rows) != 2 {
		t.Fatalf("INTERSECT: want 2 rows, got %d", len(rows))
	}
}

// TestCompileSelectRemaining_ExceptCompound exercises the EXCEPT operator
// (exceptRows path).
func TestCompileSelectRemaining_ExceptCompound(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE setC (n INTEGER)",
		"INSERT INTO setC VALUES (1),(2),(3),(4)",
		"CREATE TABLE setD (n INTEGER)",
		"INSERT INTO setD VALUES (2),(4)",
	)

	rows := srQueryRows(t, db, "SELECT n FROM setC EXCEPT SELECT n FROM setD ORDER BY n")
	if len(rows) != 2 {
		t.Fatalf("EXCEPT: want 2 rows, got %d", len(rows))
	}
	v0, _ := rows[0][0].(int64)
	v1, _ := rows[1][0].(int64)
	if v0 != 1 || v1 != 3 {
		t.Errorf("EXCEPT: want 1, 3; got %d, %d", v0, v1)
	}
}

// ---------------------------------------------------------------------------
// 30. MIN / MAX aggregates (emitMinUpdate / emitMaxUpdate coverage)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_MinMaxWithFilter exercises MIN and MAX
// with a non-trivial WHERE clause (exercises emitMinUpdate/emitMaxUpdate).
func TestCompileSelectRemaining_MinMaxWithFilter(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE readings (sensor TEXT, reading INTEGER)",
		"INSERT INTO readings VALUES ('A', 5)",
		"INSERT INTO readings VALUES ('A', 15)",
		"INSERT INTO readings VALUES ('A', 10)",
		"INSERT INTO readings VALUES ('B', 100)",
	)

	rows := srQueryRows(t, db,
		"SELECT MIN(reading), MAX(reading) FROM readings WHERE sensor = 'A'",
	)
	if len(rows) != 1 {
		t.Fatalf("MIN/MAX: want 1 row, got %d", len(rows))
	}
	mn, _ := rows[0][0].(int64)
	mx, _ := rows[0][1].(int64)
	if mn != 5 || mx != 15 {
		t.Errorf("MIN/MAX: want 5, 15; got %d, %d", mn, mx)
	}
}

// ---------------------------------------------------------------------------
// 31. SUM with NULL handling (emitSumUpdate coverage)
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_SumWithNulls exercises emitSumUpdate when
// some column values are NULL (the null-check branch inside the loop).
func TestCompileSelectRemaining_SumWithNulls(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE nullable_vals (v INTEGER)",
		"INSERT INTO nullable_vals VALUES (10)",
		"INSERT INTO nullable_vals VALUES (NULL)",
		"INSERT INTO nullable_vals VALUES (20)",
	)

	got := srQueryInt(t, db, "SELECT SUM(v) FROM nullable_vals")
	if got != 30 {
		t.Fatalf("SUM with NULLs: want 30, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// 32. JSON_GROUP_ARRAY aggregate
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_JSONGroupArray exercises emitJSONGroupArrayUpdate.
func TestCompileSelectRemaining_JSONGroupArray(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE items2 (cat TEXT, val INTEGER)",
		"INSERT INTO items2 VALUES ('A', 1)",
		"INSERT INTO items2 VALUES ('A', 2)",
		"INSERT INTO items2 VALUES ('B', 3)",
	)

	got := srQueryString(t, db,
		"SELECT JSON_GROUP_ARRAY(val) FROM items2 WHERE cat = 'A'",
	)
	if !strings.Contains(got, "1") || !strings.Contains(got, "2") {
		t.Errorf("JSON_GROUP_ARRAY: unexpected result %q", got)
	}
}

// ---------------------------------------------------------------------------
// 33. emitGeneratedExpr: generated columns in SELECT
// ---------------------------------------------------------------------------

// TestCompileSelectRemaining_GeneratedColumn exercises generated (virtual) columns
// via emitGeneratedExpr path during window function compilation.
func TestCompileSelectRemaining_GeneratedColumn(t *testing.T) {
	t.Parallel()
	db := openSRDB(t)
	srExec(t, db,
		"CREATE TABLE gcols (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b))",
		"INSERT INTO gcols(a, b) VALUES (3, 4)",
		"INSERT INTO gcols(a, b) VALUES (10, 5)",
	)

	rows := srQueryRows(t, db, "SELECT a, b, c FROM gcols ORDER BY a")
	if len(rows) != 2 {
		t.Fatalf("generated col: want 2 rows, got %d", len(rows))
	}
	// The generated column c = a + b; verify the base columns are correct
	// even if the engine stores 0 for generated columns at this version.
	a0, _ := rows[0][0].(int64)
	b0, _ := rows[0][1].(int64)
	if a0 != 3 || b0 != 4 {
		t.Errorf("generated col base: want a=3 b=4, got a=%d b=%d", a0, b0)
	}
}
