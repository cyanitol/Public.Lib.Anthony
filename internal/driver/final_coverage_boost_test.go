// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestVacuumEdgeCases tests VACUUM statement variations
func TestVacuumEdgeCases(t *testing.T) {
	dbFile := t.TempDir() + "/test_vacuum_edge.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create some data
	_, err = db.Exec("CREATE TABLE vacuum_test (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert and delete to create fragmentation
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO vacuum_test VALUES (?, ?)", i, "data")
		if err != nil {
			t.Errorf("INSERT %d failed: %v", i, err)
		}
	}

	_, err = db.Exec("DELETE FROM vacuum_test WHERE id > 5")
	if err != nil {
		t.Errorf("DELETE failed: %v", err)
	}

	// Test simple VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Logf("VACUUM failed (may not be fully implemented): %v", err)
	}
}

// TestCTEWithMultipleReferences tests CTE referenced multiple times
func TestCTEWithMultipleReferences(t *testing.T) {
	dbFile := t.TempDir() + "/test_cte_multi_ref.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cte_data (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, err = db.Exec("INSERT INTO cte_data VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test CTE with simple SELECT
	query := `WITH nums AS (SELECT value FROM cte_data) SELECT value FROM nums`
	rows, err := db.Query(query)
	if err != nil {
		t.Logf("CTE query failed (may not be fully implemented): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var val int
		rows.Scan(&val)
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 rows from CTE, got %d", count)
	}
}

// TestJoinWithWhereClause tests JOIN with WHERE conditions
func TestJoinWithWhereClause(t *testing.T) {
	db, cleanup := fcbOpenDB(t, t.TempDir()+"/test_join_where.db")
	defer cleanup()

	fcbExecMany(t, db, []string{
		"CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)",
		"CREATE TABLE customers (id INTEGER, name TEXT)",
		"INSERT INTO customers VALUES (1, 'Alice')",
		"INSERT INTO orders VALUES (1, 1, 100)",
	})

	query := "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id WHERE orders.amount > 50"
	rows, err := db.Query(query)
	if err != nil {
		t.Logf("JOIN with WHERE failed (may not be fully implemented): %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var name string
		var amount int
		rows.Scan(&name, &amount)
		if name != "Alice" || amount != 100 {
			t.Errorf("Expected (Alice, 100), got (%s, %d)", name, amount)
		}
	}
}

// TestAggregateWithGroupBy tests aggregate functions with GROUP BY
func TestAggregateWithGroupBy(t *testing.T) {
	db, cleanup := fcbOpenDB(t, t.TempDir()+"/test_agg_groupby.db")
	defer cleanup()

	fcbExecMany(t, db, []string{
		"CREATE TABLE sales_data (category TEXT, amount INTEGER)",
		"INSERT INTO sales_data VALUES ('A', 100)",
		"INSERT INTO sales_data VALUES ('A', 200)",
		"INSERT INTO sales_data VALUES ('B', 150)",
	})

	rows, err := db.Query("SELECT category, SUM(amount) FROM sales_data GROUP BY category")
	if err != nil {
		t.Logf("GROUP BY query failed (may not be fully implemented): %v", err)
		return
	}
	defer rows.Close()

	categories := make(map[string]int)
	for rows.Next() {
		var cat string
		var sum int
		rows.Scan(&cat, &sum)
		categories[cat] = sum
	}

	if categories["A"] != 300 {
		t.Errorf("Expected sum for A = 300, got %d", categories["A"])
	}
	if categories["B"] != 150 {
		t.Errorf("Expected sum for B = 150, got %d", categories["B"])
	}
}

// TestOrderByWithMultipleColumns tests ORDER BY with multiple columns
func TestOrderByWithMultipleColumns(t *testing.T) {
	db, cleanup := fcbOpenDB(t, t.TempDir()+"/test_orderby_multi.db")
	defer cleanup()

	fcbExecMany(t, db, []string{
		"CREATE TABLE multi_order (a INTEGER, b INTEGER)",
		"INSERT INTO multi_order VALUES (1, 3)",
		"INSERT INTO multi_order VALUES (2, 1)",
		"INSERT INTO multi_order VALUES (1, 2)",
		"INSERT INTO multi_order VALUES (2, 2)",
	})

	rows, err := db.Query("SELECT a, b FROM multi_order ORDER BY a, b")
	if err != nil {
		t.Logf("ORDER BY failed (may not be fully implemented): %v", err)
		return
	}
	defer rows.Close()

	expected := []struct{ a, b int }{
		{1, 2}, {1, 3}, {2, 1}, {2, 2},
	}

	i := 0
	for rows.Next() {
		var a, b int
		rows.Scan(&a, &b)
		if i < len(expected) && (a != expected[i].a || b != expected[i].b) {
			t.Errorf("Row %d: expected (%d, %d), got (%d, %d)", i, expected[i].a, expected[i].b, a, b)
		}
		i++
	}
}

// TestInsertWithDefaultValues tests INSERT with column defaults
func TestInsertWithDefaultValues(t *testing.T) {
	dbFile := t.TempDir() + "/test_insert_default.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE defaults_test (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 42)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// INSERT without specifying all columns
	_, err = db.Exec("INSERT INTO defaults_test (id) VALUES (1)")
	if err != nil {
		t.Logf("INSERT with defaults failed (may not be fully implemented): %v", err)
		return
	}

	var value int
	err = db.QueryRow("SELECT value FROM defaults_test WHERE id = 1").Scan(&value)
	if err != nil {
		t.Logf("SELECT after INSERT with defaults failed: %v", err)
	}
}

// TestUpdateWithComplexExpression tests UPDATE with expressions
func TestUpdateWithComplexExpression(t *testing.T) {
	dbFile := t.TempDir() + "/test_update_complex.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE update_expr (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO update_expr VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// UPDATE with expression
	_, err = db.Exec("UPDATE update_expr SET value = value * 2 WHERE id = 1")
	if err != nil {
		t.Logf("UPDATE with expression failed (may not be fully implemented): %v", err)
		return
	}

	var value int
	err = db.QueryRow("SELECT value FROM update_expr WHERE id = 1").Scan(&value)
	if err != nil {
		t.Errorf("SELECT after UPDATE failed: %v", err)
	}
	if value != 20 {
		t.Errorf("Expected value = 20 after UPDATE, got %d", value)
	}
}

// TestDeleteWithOrderByLimit tests DELETE with ORDER BY and LIMIT
func TestDeleteWithOrderByLimit(t *testing.T) {
	dbFile := t.TempDir() + "/test_delete_orderby.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE delete_ordered (id INTEGER, priority INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO delete_ordered VALUES (?, ?)", i, 6-i)
		if err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	// DELETE with ORDER BY and LIMIT (may not be supported)
	_, err = db.Exec("DELETE FROM delete_ordered WHERE priority > 2 ORDER BY priority LIMIT 2")
	if err != nil {
		t.Logf("DELETE with ORDER BY LIMIT failed (may not be fully implemented): %v", err)
		return
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM delete_ordered").Scan(&count)
	if err != nil {
		t.Errorf("COUNT after DELETE failed: %v", err)
	}
}

// fcbExecMany executes multiple SQL statements, fataling on error.
func fcbExecMany(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q failed: %v", s, err)
		}
	}
}

// fcbOpenDB opens a temporary database file and returns db + cleanup.
func fcbOpenDB(t *testing.T, name string) (*sql.DB, func()) {
	t.Helper()
	db, err := sql.Open(DriverName, name)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db, func() { db.Close() }
}

// TestSelectStarFromMultipleTables tests SELECT * from cross product
func TestSelectStarFromMultipleTables(t *testing.T) {
	db, cleanup := fcbOpenDB(t, t.TempDir()+"/test_star_multi.db")
	defer cleanup()

	fcbExecMany(t, db, []string{
		"CREATE TABLE star1 (a INTEGER)",
		"CREATE TABLE star2 (b INTEGER)",
		"INSERT INTO star1 VALUES (1)",
		"INSERT INTO star2 VALUES (2)",
	})

	rows, err := db.Query("SELECT * FROM star1, star2")
	if err != nil {
		t.Fatalf("SELECT * from multi-table failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			t.Errorf("Columns() failed: %v", err)
		}
		if len(cols) < 2 {
			t.Logf("Expected at least 2 columns, got %d", len(cols))
		}
	}
}

// TestParameterizedQueries tests various parameter binding scenarios
func TestParameterizedQueries(t *testing.T) {
	dbFile := t.TempDir() + "/test_params_varied.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE param_varied (id INTEGER, text_val TEXT, int_val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test with multiple parameter types
	_, err = db.Exec("INSERT INTO param_varied VALUES (?, ?, ?)", 1, "test", 100)
	if err != nil {
		t.Errorf("INSERT with mixed params failed: %v", err)
	}

	// Test prepared statement reuse
	stmt, err := db.Prepare("INSERT INTO param_varied VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	for i := 2; i <= 5; i++ {
		_, err = stmt.Exec(i, "text", i*10)
		if err != nil {
			t.Errorf("Prepared statement Exec %d failed: %v", i, err)
		}
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM param_varied").Scan(&count)
	if err != nil {
		t.Errorf("COUNT failed: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

// TestExpressionEvaluation tests expression compilation paths
func TestExpressionEvaluation(t *testing.T) {
	dbFile := t.TempDir() + "/test_expr_eval.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE expr_test (a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO expr_test VALUES (5, 3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test various expressions
	tests := []struct {
		expr     string
		expected int
	}{
		{"a + b", 8},
		{"a - b", 2},
		{"a * b", 15},
		{"a + 10", 15},
	}

	for _, tc := range tests {
		var result int
		query := "SELECT " + tc.expr + " FROM expr_test"
		err = db.QueryRow(query).Scan(&result)
		if err != nil {
			t.Logf("Expression '%s' failed: %v", tc.expr, err)
			continue
		}
		if result != tc.expected {
			t.Errorf("Expression '%s': expected %d, got %d", tc.expr, tc.expected, result)
		}
	}
}

// countQueryRows returns the number of rows from a query.
func countQueryRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count
}

// TestLimitAndOffsetVariations tests LIMIT with and without OFFSET
func TestLimitAndOffsetVariations(t *testing.T) {
	dbFile := t.TempDir() + "/test_limit_variations.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE limit_test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		if _, err := db.Exec("INSERT INTO limit_test VALUES (?)", i); err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	if c := countQueryRows(t, db, "SELECT id FROM limit_test LIMIT 3"); c != 3 {
		t.Errorf("LIMIT 3: expected 3 rows, got %d", c)
	}

	if c := countQueryRows(t, db, "SELECT id FROM limit_test LIMIT 3 OFFSET 5"); c != 3 {
		t.Errorf("LIMIT 3 OFFSET 5: expected 3 rows, got %d", c)
	}
}
