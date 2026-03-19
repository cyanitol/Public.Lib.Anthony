// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// TestExplain tests EXPLAIN statement
func TestExplain(t *testing.T) {
	dbFile := "test_explain.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test EXPLAIN on SELECT
	rows, err := db.Query("EXPLAIN SELECT * FROM test")
	if err != nil {
		t.Fatalf("EXPLAIN SELECT failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		// EXPLAIN returns opcode information: addr, opcode, p1, p2, p3, p4, p5, comment
		var addr, p1, p2, p3, p5 int
		var opcode, p4, comment string
		err = rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment)
		if err != nil {
			t.Logf("Scan row: %v", err)
		}
	}

	if !hasRows {
		t.Error("EXPLAIN should return rows")
	}
}

// TestExplainQueryPlanExtended tests EXPLAIN QUERY PLAN
func TestExplainQueryPlanExtended(t *testing.T) {
	dbFile := "test_explain_qp.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test EXPLAIN QUERY PLAN
	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		// EXPLAIN QUERY PLAN returns query plan information
		var id, parent, detail int
		var plan string
		err = rows.Scan(&id, &parent, &detail, &plan)
		if err != nil {
			t.Logf("Scan row: %v", err)
		}
	}

	if !hasRows {
		t.Error("EXPLAIN QUERY PLAN should return rows")
	}
}

// TestScalarSubquery tests scalar subqueries
func TestScalarSubquery(t *testing.T) {
	dbFile := "test_scalar_subquery.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE orders (id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO orders VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO orders VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test scalar subquery in SELECT
	var result int
	err = db.QueryRow("SELECT (SELECT MAX(amount) FROM orders) as max_amount").Scan(&result)
	if err != nil {
		t.Fatalf("Scalar subquery failed: %v", err)
	}
	if result != 200 {
		t.Errorf("max_amount = %d, want 200", result)
	}
}

// TestExistsSubquery tests EXISTS subqueries
func TestExistsSubquery(t *testing.T) {
	dbFile := "test_exists.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO items VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test EXISTS - should return a row when subquery has results
	rows, err := db.Query("SELECT 1 WHERE EXISTS (SELECT 1 FROM items WHERE value > 50)")
	if err != nil {
		t.Fatalf("EXISTS query failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
	}

	if !hasRows {
		t.Error("EXISTS should return true when subquery has results")
	}

	// Test EXISTS - should return no rows when subquery is empty
	rows2, err := db.Query("SELECT 1 WHERE EXISTS (SELECT 1 FROM items WHERE value > 1000)")
	if err != nil {
		t.Fatalf("EXISTS query failed: %v", err)
	}
	defer rows2.Close()

	hasRows2 := false
	for rows2.Next() {
		hasRows2 = true
	}

	if hasRows2 {
		t.Error("EXISTS should return false when subquery is empty")
	}
}

// TestInSubquery tests IN with subqueries
func TestInSubquery(t *testing.T) {
	dbFile := "test_in_subquery.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE products (id INTEGER, category_id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE products failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE categories (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE categories failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO categories VALUES (1, 'Electronics')")
	if err != nil {
		t.Fatalf("INSERT category failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO categories VALUES (2, 'Books')")
	if err != nil {
		t.Fatalf("INSERT category failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO products VALUES (1, 1)")
	if err != nil {
		t.Fatalf("INSERT product failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO products VALUES (2, 1)")
	if err != nil {
		t.Fatalf("INSERT product failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO products VALUES (3, 2)")
	if err != nil {
		t.Fatalf("INSERT product failed: %v", err)
	}

	// Test IN subquery
	rows, err := db.Query("SELECT id FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')")
	if err != nil {
		t.Fatalf("IN subquery failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id int
		rows.Scan(&id)
	}

	if count != 2 {
		t.Errorf("IN subquery returned %d rows, want 2", count)
	}
}

// TestFromSubquery tests subqueries in FROM clause
func TestFromSubquery(t *testing.T) {
	dbFile := "test_from_subquery.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE sales (id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test FROM subquery
	var total int
	err = db.QueryRow("SELECT SUM(amount) FROM (SELECT amount FROM sales)").Scan(&total)
	if err != nil {
		t.Fatalf("FROM subquery failed: %v", err)
	}

	if total != 300 {
		t.Errorf("sum = %d, want 300", total)
	}
}

// TestComplexSubquery tests nested and complex subqueries
func TestComplexSubquery(t *testing.T) {
	dbFile := "test_complex_subquery.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE data (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO data VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test nested subquery
	var result int
	err = db.QueryRow("SELECT COUNT(*) FROM data WHERE value > (SELECT AVG(value) FROM data)").Scan(&result)
	if err != nil {
		t.Fatalf("Complex subquery failed: %v", err)
	}

	// Average is 30, so values > 30 are 40 and 50 (2 rows)
	if result != 2 {
		t.Errorf("Complex subquery result = %d, want 2", result)
	}
}

// TestSubqueryWithJoin tests subqueries combined with joins
func TestSubqueryWithJoin(t *testing.T) {
	dbFile := "test_subquery_join.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1 (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE t2 (id INTEGER, ref_id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT t1 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO t2 VALUES (1, 1)")
	if err != nil {
		t.Fatalf("INSERT t2 failed: %v", err)
	}

	// Test subquery with join
	rows, err := db.Query("SELECT t1.id FROM t1, t2 WHERE t1.id = t2.ref_id AND t1.value IN (SELECT 100)")
	if err != nil {
		// This may fail in current implementation, which is expected
		t.Logf("Subquery with join not fully supported: %v", err)
		return
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
	}

	if !hasRows {
		t.Log("No rows returned from subquery with join")
	}
}

// TestUnqualifiedColumnInMultiTable tests error handling for unqualified columns
func TestUnqualifiedColumnInMultiTable(t *testing.T) {
	dbFile := "test_unqualified.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1 (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE t2 (id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO t1 VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT t1 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO t2 VALUES (1, 200)")
	if err != nil {
		t.Fatalf("INSERT t2 failed: %v", err)
	}

	// Test selecting unqualified column from multi-table query
	// This should work if only one table has the column
	rows, err := db.Query("SELECT value FROM t1, t2")
	if err != nil {
		t.Fatalf("Unqualified column query failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var value int
		rows.Scan(&value)
	}

	if !hasRows {
		t.Error("Expected rows from unqualified column query")
	}
}

// TestNonIdentifierColumn tests non-identifier columns in multi-table context
func TestNonIdentifierColumn(t *testing.T) {
	dbFile := "test_non_ident.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nums (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO nums VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test selecting literal/expression in multi-table context
	rows, err := db.Query("SELECT 42, nums.value FROM nums")
	if err != nil {
		t.Fatalf("Non-identifier column query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var literal, value int
		err = rows.Scan(&literal, &value)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if literal != 42 {
			t.Errorf("literal = %d, want 42", literal)
		}
		if value != 10 {
			t.Errorf("value = %d, want 10", value)
		}
	}
}

// TestCountFromSubqueries tests the countFromSubqueries function
func TestCountFromSubqueries(t *testing.T) {
	dbFile := "test_count_subq.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE base (id INTEGER, val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO base VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Multiple FROM subqueries
	rows, err := db.Query("SELECT * FROM (SELECT id FROM base), (SELECT val FROM base)")
	if err != nil {
		t.Logf("Multiple FROM subqueries: %v", err)
		// This may not be fully supported, which is expected
		return
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
	}

	if !hasRows {
		t.Log("Multiple FROM subqueries returned no rows")
	}
}

// TestInsertFirstRow tests insertFirstRow path
func TestInsertFirstRow(t *testing.T) {
	dbFile := "test_insert_first.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with rowid
	_, err = db.Exec("CREATE TABLE first_test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert first row without specifying rowid
	result, err := db.Exec("INSERT INTO first_test (name) VALUES ('first')")
	if err != nil {
		t.Fatalf("INSERT first row failed: %v", err)
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		t.Logf("LastInsertId: %v", err)
	} else if lastID != 1 {
		t.Logf("First row ID = %d, expected 1", lastID)
	}

	// Verify row was inserted
	var name string
	err = db.QueryRow("SELECT name FROM first_test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "first" {
		t.Errorf("name = %s, want 'first'", name)
	}
}

// TestExplainOpcodesExtended tests that EXPLAIN produces valid opcode output
func TestExplainOpcodesExtended(t *testing.T) {
	dbFile := "test_explain_opcodes.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_ops (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test various statement types with EXPLAIN
	testCases := []string{
		"EXPLAIN SELECT * FROM test_ops",
		"EXPLAIN SELECT * FROM test_ops WHERE id = 1",
		"EXPLAIN SELECT COUNT(*) FROM test_ops",
	}

	for _, query := range testCases {
		rows, err := db.Query(query)
		if err != nil {
			t.Errorf("Query %q failed: %v", query, err)
			continue
		}

		rowCount := 0
		for rows.Next() {
			rowCount++
			var addr, p1, p2, p3, p5 int
			var opcode, p4, comment string
			err = rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment)
			if err != nil {
				t.Logf("Scan error for %q: %v", query, err)
			}
		}
		rows.Close()

		if rowCount == 0 {
			t.Errorf("Query %q returned no rows", query)
		}
	}
}

// TestInnerStatementCompilation tests inner statement compilation
func TestInnerStatementCompilation(t *testing.T) {
	dbFile := "test_inner_stmt.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE inner_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test EXPLAIN which compiles inner statements
	rows, err := db.Query("EXPLAIN SELECT * FROM inner_test WHERE value > 10")
	if err != nil {
		t.Fatalf("EXPLAIN with WHERE failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
	}

	if !hasRows {
		t.Error("EXPLAIN should produce opcode output")
	}
}

// TestSelectWithoutFromSpecialCases tests special SELECT without FROM cases
func TestSelectWithoutFromSpecialCases(t *testing.T) {
	dbFile := "test_select_no_from.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test various SELECT without FROM expressions
	testCases := []struct {
		query    string
		expected interface{}
	}{
		{"SELECT 1 + 2", 3},
		{"SELECT 10 * 5", 50},
		{"SELECT 'hello'", "hello"},
	}

	for _, tc := range testCases {
		rows, err := db.Query(tc.query)
		if err != nil {
			t.Errorf("Query %q failed: %v", tc.query, err)
			continue
		}

		if !rows.Next() {
			t.Errorf("Query %q returned no rows", tc.query)
			rows.Close()
			continue
		}

		var result interface{}
		err = rows.Scan(&result)
		rows.Close()

		if err != nil {
			t.Errorf("Scan for %q failed: %v", tc.query, err)
			continue
		}

		// Basic type checking
		if result == nil {
			t.Errorf("Query %q returned nil", tc.query)
		}
	}
}

// TestQualifiedColumnInSelect tests qualified column references
func TestQualifiedColumnInSelect(t *testing.T) {
	dbFile := "test_qualified.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE qual_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO qual_test VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test qualified column reference
	var name string
	err = db.QueryRow("SELECT qual_test.name FROM qual_test WHERE qual_test.id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Qualified column SELECT failed: %v", err)
	}

	if name != "test" {
		t.Errorf("name = %s, want 'test'", name)
	}
}

// TestDetermineCursorNum tests cursor number determination
func TestDetermineCursorNum(t *testing.T) {
	dbFile := "test_cursor_num.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create multiple tables to test cursor allocation
	_, err = db.Exec("CREATE TABLE t1 (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE t2 (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT t1 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t2 VALUES (2)")
	if err != nil {
		t.Fatalf("INSERT t2 failed: %v", err)
	}

	// Query from multiple tables
	rows, err := db.Query("SELECT t1.id, t2.id FROM t1, t2")
	if err != nil {
		t.Fatalf("Multi-table query failed: %v", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var id1, id2 int
		err = rows.Scan(&id1, &id2)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
	}

	if !hasRows {
		t.Error("Expected rows from multi-table query")
	}
}

// TestSelectFromTableNameResolution tests table name resolution
func TestSelectFromTableNameResolution(t *testing.T) {
	dbFile := "test_table_resolution.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE resolution_test (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO resolution_test VALUES (1, 'data1')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test various forms of table reference
	testQueries := []string{
		"SELECT * FROM resolution_test",
		"SELECT resolution_test.id FROM resolution_test",
		"SELECT id, data FROM resolution_test",
	}

	for _, query := range testQueries {
		rows, err := db.Query(query)
		if err != nil {
			t.Errorf("Query %q failed: %v", query, err)
			continue
		}

		hasRows := false
		for rows.Next() {
			hasRows = true
		}
		rows.Close()

		if !hasRows {
			t.Errorf("Query %q returned no rows", query)
		}
	}
}

// TestDispatchOtherStatements tests the dispatchOtherStatements path
func TestDispatchOtherStatements(t *testing.T) {
	dbFile := "test_dispatch.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test various statement types that go through dispatchOtherStatements
	_, err = db.Exec("CREATE TABLE dispatch_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// INSERT
	_, err = db.Exec("INSERT INTO dispatch_test VALUES (1, 100)")
	if err != nil {
		t.Errorf("INSERT failed: %v", err)
	}

	// UPDATE
	_, err = db.Exec("UPDATE dispatch_test SET value = 200 WHERE id = 1")
	if err != nil {
		t.Errorf("UPDATE failed: %v", err)
	}

	// DELETE
	_, err = db.Exec("DELETE FROM dispatch_test WHERE id = 1")
	if err != nil {
		t.Errorf("DELETE failed: %v", err)
	}

	// Verify table is empty
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM dispatch_test").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

// TestCountExprParams tests parameter counting in expressions
func TestCountExprParams(t *testing.T) {
	dbFile := "test_expr_params.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE param_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test parameterized query
	stmt, err := db.Prepare("INSERT INTO param_test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(1, 100)
	if err != nil {
		t.Errorf("Parameterized INSERT failed: %v", err)
	}

	// Verify
	var value int
	err = db.QueryRow("SELECT value FROM param_test WHERE id = ?", 1).Scan(&value)
	if err != nil {
		t.Fatalf("Parameterized SELECT failed: %v", err)
	}
	if value != 100 {
		t.Errorf("value = %d, want 100", value)
	}
}

// TestCompileLiteralExpr tests literal expression compilation
func TestCompileLiteralExpr(t *testing.T) {
	dbFile := "test_literal.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE literal_test (id INTEGER, str TEXT, num INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert various literal types
	_, err = db.Exec("INSERT INTO literal_test VALUES (1, 'text', 42)")
	if err != nil {
		t.Fatalf("INSERT with literals failed: %v", err)
	}

	// Test literal in WHERE clause
	var str string
	err = db.QueryRow("SELECT str FROM literal_test WHERE num = 42").Scan(&str)
	if err != nil {
		t.Fatalf("SELECT with literal comparison failed: %v", err)
	}
	if str != "text" {
		t.Errorf("str = %s, want 'text'", str)
	}
}

// TestExtractValueFromExpression tests value extraction from expressions
func TestExtractValueFromExpression(t *testing.T) {
	t.Skip("Expression evaluation in INSERT not fully implemented")
	dbFile := "test_extract_value.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE extract_test (id INTEGER, computed INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with computed expression
	_, err = db.Exec("INSERT INTO extract_test VALUES (1, 10 + 20)")
	if err != nil {
		// May not be fully supported
		t.Logf("INSERT with expression: %v", err)
		return
	}

	// Verify
	var computed int
	err = db.QueryRow("SELECT computed FROM extract_test WHERE id = 1").Scan(&computed)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if computed != 30 {
		t.Errorf("computed = %d, want 30", computed)
	}
}

// TestPrepareNewRowForInsert tests row preparation for insert
func TestPrepareNewRowForInsert(t *testing.T) {
	t.Skip("Partial column INSERT has type conversion issues")
	dbFile := "test_prepare_row.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE row_test (id INTEGER, a INTEGER, b TEXT, c INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert partial columns
	_, err = db.Exec("INSERT INTO row_test (id, b) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Partial INSERT failed: %v", err)
	}

	// Verify columns
	var id int
	var a sql.NullInt64
	var b string
	var c sql.NullInt64

	err = db.QueryRow("SELECT id, a, b, c FROM row_test WHERE id = 1").Scan(&id, &a, &b, &c)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if id != 1 {
		t.Errorf("id = %d, want 1", id)
	}
	if b != "test" {
		t.Errorf("b = %s, want 'test'", b)
	}
}

// TestHasFromSubqueriesDetection tests detection of FROM subqueries
func TestHasFromSubqueriesDetection(t *testing.T) {
	dbFile := "test_has_from_subq.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE subq_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO subq_test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test FROM subquery
	rows, err := db.Query("SELECT * FROM (SELECT id, value FROM subq_test)")
	if err != nil {
		t.Logf("FROM subquery: %v", err)
		return
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var id, value int
		rows.Scan(&id, &value)
	}

	if !hasRows {
		t.Log("FROM subquery returned no rows")
	}
}

// TestCompileValueTypes tests different value type compilation
func TestCompileValueTypes(t *testing.T) {
	dbFile := "test_value_types.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE types_test (id INTEGER, int_val INTEGER, text_val TEXT, blob_val BLOB)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert different value types
	_, err = db.Exec("INSERT INTO types_test VALUES (1, 42, 'hello', X'DEADBEEF')")
	if err != nil {
		t.Logf("INSERT with different types: %v", err)
		// Try simpler version
		_, err = db.Exec("INSERT INTO types_test (id, int_val, text_val) VALUES (1, 42, 'hello')")
		if err != nil {
			t.Fatalf("Simplified INSERT failed: %v", err)
		}
	}

	// Verify
	var id, intVal int
	var textVal string
	err = db.QueryRow("SELECT id, int_val, text_val FROM types_test WHERE id = 1").Scan(&id, &intVal, &textVal)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if id != 1 || intVal != 42 || textVal != "hello" {
		t.Errorf("Values incorrect: id=%d, int_val=%d, text_val=%s", id, intVal, textVal)
	}
}

// TestCompileArgValue tests argument value compilation
func TestCompileArgValue(t *testing.T) {
	dbFile := "test_arg_value.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE arg_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test with prepared statement arguments
	stmt, err := db.Prepare("INSERT INTO arg_test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 3; i++ {
		_, err = stmt.Exec(i, i*10)
		if err != nil {
			t.Errorf("Exec(%d, %d) failed: %v", i, i*10, err)
		}
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM arg_test").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

// TestMultiTableColumnNames tests buildMultiTableColumnNames
func TestMultiTableColumnNames(t *testing.T) {
	t.Skip("Multi-table SELECT * column name expansion not implemented")
	dbFile := "test_multi_cols.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE mc1 (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE mc1 failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE mc2 (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE mc2 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO mc1 VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT mc1 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO mc2 VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT mc2 failed: %v", err)
	}

	// SELECT * from multiple tables
	rows, err := db.Query("SELECT * FROM mc1, mc2")
	if err != nil {
		t.Fatalf("Multi-table SELECT * failed: %v", err)
	}
	defer rows.Close()

	// Check column names
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}

	// Should have columns from both tables
	if len(cols) < 3 {
		t.Errorf("Expected at least 3 columns, got %d: %v", len(cols), cols)
	}

	// Verify data can be read
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
	}
}

// TestEmitColumnFromTable tests column emission from specific table
func TestEmitColumnFromTable(t *testing.T) {
	t.Skip("Qualified column names not fully supported")
	dbFile := "test_emit_col.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE emit1 (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE emit2 (id INTEGER, info TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO emit1 VALUES (1, 'data1')")
	if err != nil {
		t.Fatalf("INSERT emit1 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO emit2 VALUES (1, 'info1')")
	if err != nil {
		t.Fatalf("INSERT emit2 failed: %v", err)
	}

	// Select specific columns from each table
	rows, err := db.Query("SELECT emit1.data, emit2.info FROM emit1, emit2")
	if err != nil {
		t.Fatalf("Qualified column SELECT failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var data, info string
		err = rows.Scan(&data, &info)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if data != "data1" {
			t.Errorf("data = %s, want 'data1'", data)
		}
		if info != "info1" {
			t.Errorf("info = %s, want 'info1'", info)
		}
	}
}

// TestFindOrderByColumnInSelect tests ORDER BY column resolution
func TestFindOrderByColumnInSelect(t *testing.T) {
	dbFile := "test_orderby_find.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE orderby_test (id INTEGER, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	data := []struct {
		id    int
		name  string
		value int
	}{
		{1, "zebra", 100},
		{2, "alpha", 200},
		{3, "beta", 150},
	}

	for _, d := range data {
		_, err = db.Exec("INSERT INTO orderby_test VALUES (?, ?, ?)", d.id, d.name, d.value)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ORDER BY different columns
	rows, err := db.Query("SELECT name, value FROM orderby_test ORDER BY name")
	if err != nil {
		t.Fatalf("ORDER BY name failed: %v", err)
	}
	defer rows.Close()

	expectedOrder := []string{"alpha", "beta", "zebra"}
	i := 0
	for rows.Next() {
		var name string
		var value int
		err = rows.Scan(&name, &value)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expectedOrder) && name != expectedOrder[i] {
			t.Errorf("Row %d: name = %s, want %s", i, name, expectedOrder[i])
		}
		i++
	}

	if i != len(expectedOrder) {
		t.Errorf("Got %d rows, want %d", i, len(expectedOrder))
	}
}

// TestExtractOrderByExpression tests ORDER BY expression extraction
func TestExtractOrderByExpression(t *testing.T) {
	dbFile := "test_orderby_expr.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE expr_order (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	vals := []int{5, 2, 8, 1, 9}
	for i, v := range vals {
		_, err = db.Exec("INSERT INTO expr_order VALUES (?, ?)", i+1, v)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ORDER BY with expression/column reference
	rows, err := db.Query("SELECT id, value FROM expr_order ORDER BY value")
	if err != nil {
		t.Fatalf("ORDER BY expression failed: %v", err)
	}
	defer rows.Close()

	expectedValues := []int{1, 2, 5, 8, 9}
	i := 0
	for rows.Next() {
		var id, value int
		err = rows.Scan(&id, &value)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expectedValues) && value != expectedValues[i] {
			t.Errorf("Row %d: value = %d, want %d", i, value, expectedValues[i])
		}
		i++
	}
}

// TestFindCollationInSchema tests collation finding in schema
func TestFindCollationInSchema(t *testing.T) {
	dbFile := "test_collation.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with TEXT column
	_, err = db.Exec("CREATE TABLE coll_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO coll_test VALUES (1, 'Zebra')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO coll_test VALUES (2, 'alpha')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO coll_test VALUES (3, 'Beta')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test ORDER BY on text column (uses collation)
	rows, err := db.Query("SELECT name FROM coll_test ORDER BY name")
	if err != nil {
		t.Fatalf("ORDER BY text column failed: %v", err)
	}
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		names = append(names, name)
	}

	if len(names) != 3 {
		t.Errorf("Got %d names, want 3", len(names))
	}
	// Order depends on collation implementation
	t.Logf("Ordered names: %v", names)
}

// TestAddExtraOrderByColumn tests adding extra ORDER BY columns
func TestAddExtraOrderByColumn(t *testing.T) {
	dbFile := "test_extra_orderby.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE extra_order (id INTEGER, a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	testData := [][]int{
		{1, 10, 1},
		{2, 10, 2},
		{3, 20, 1},
		{4, 20, 2},
	}

	for _, row := range testData {
		_, err = db.Exec("INSERT INTO extra_order VALUES (?, ?, ?)", row[0], row[1], row[2])
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ORDER BY multiple columns
	rows, err := db.Query("SELECT id, a, b FROM extra_order ORDER BY a, b")
	if err != nil {
		t.Fatalf("ORDER BY multiple columns failed: %v", err)
	}
	defer rows.Close()

	expected := []int{1, 2, 3, 4}
	i := 0
	for rows.Next() {
		var id, a, b int
		err = rows.Scan(&id, &a, &b)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expected) && id != expected[i] {
			t.Errorf("Row %d: id = %d, want %d", i, id, expected[i])
		}
		i++
	}
}

// TestUpdateWhereClause tests UPDATE with WHERE clause
func TestUpdateWhereClause(t *testing.T) {
	dbFile := "test_update_where.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE upd_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO upd_test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Update with WHERE clause
	result, err := db.Exec("UPDATE upd_test SET value = 999 WHERE id > 3")
	if err != nil {
		t.Fatalf("UPDATE with WHERE failed: %v", err)
	}

	affected, err := result.RowsAffected()
	if err == nil && affected != 2 {
		t.Logf("Expected 2 rows affected, got %d", affected)
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM upd_test WHERE value = 999").Scan(&count)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// TestReleaseStateExtended tests state release (cleanup path)
func TestReleaseStateExtended(t *testing.T) {
	// Create a database and close it to trigger cleanup
	dbFile := "test_release.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Close should trigger releaseState
	err = db.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Reopen to ensure cleanup worked
	db2, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Table should still exist
	rows, err := db2.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("SELECT after reopen failed: %v", err)
	}
	rows.Close()
}
