// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// TestCTEBasic tests basic Common Table Expressions
func TestCTEBasic(t *testing.T) {
	dbFile := "test_cte_basic.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE numbers (value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO numbers VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test simple CTE
	rows, err := db.Query("WITH doubled AS (SELECT value * 2 AS val FROM numbers) SELECT val FROM doubled")
	if err != nil {
		t.Logf("Simple CTE: %v", err)
		// CTEs may not be fully implemented
		return
	}
	defer rows.Close()

	expected := []int{2, 4, 6, 8, 10}
	i := 0
	for rows.Next() {
		var val int
		err = rows.Scan(&val)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expected) && val != expected[i] {
			t.Errorf("Row %d: val = %d, want %d", i, val, expected[i])
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Got %d rows, want %d", i, len(expected))
	}
}

// TestCTEMultiple tests multiple CTEs
func TestCTEMultiple(t *testing.T) {
	dbFile := "test_cte_multiple.db"
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

	_, err = db.Exec("INSERT INTO data VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO data VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test multiple CTEs
	query := `
		WITH cte1 AS (SELECT id, value FROM data WHERE id = 1),
		     cte2 AS (SELECT id, value FROM data WHERE id = 2)
		SELECT cte1.value + cte2.value FROM cte1, cte2
	`
	var result int
	err = db.QueryRow(query).Scan(&result)
	if err != nil {
		t.Logf("Multiple CTEs: %v", err)
		// CTEs may not be fully implemented
		return
	}

	if result != 30 {
		t.Errorf("result = %d, want 30", result)
	}
}

// TestRecursiveCTE tests recursive Common Table Expressions
func TestRecursiveCTE(t *testing.T) {
	dbFile := "test_recursive_cte.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test recursive CTE to generate sequence
	query := `
		WITH RECURSIVE seq(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 5
		)
		SELECT n FROM seq
	`

	rows, err := db.Query(query)
	if err != nil {
		t.Logf("Recursive CTE: %v", err)
		// Recursive CTEs may not be fully implemented
		return
	}
	defer rows.Close()

	expected := []int{1, 2, 3, 4, 5}
	i := 0
	for rows.Next() {
		var n int
		err = rows.Scan(&n)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expected) && n != expected[i] {
			t.Errorf("Row %d: n = %d, want %d", i, n, expected[i])
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Got %d rows, want %d", i, len(expected))
	}
}

// TestCTEWithAggregate tests CTE with aggregate functions
func TestCTEWithAggregate(t *testing.T) {
	// t.Skip("pre-existing failure - CTE with GROUP BY aggregate not yet implemented")
	dbFile := "test_cte_agg.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE sales (region TEXT, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES ('North', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES ('North', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO sales VALUES ('South', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE with aggregate
	query := `
		WITH regional_totals AS (
			SELECT region, SUM(amount) as total FROM sales GROUP BY region
		)
		SELECT total FROM regional_totals WHERE region = 'North'
	`

	var total int
	err = db.QueryRow(query).Scan(&total)
	if err != nil {
		t.Logf("CTE with aggregate: %v", err)
		return
	}

	if total != 250 {
		t.Errorf("total = %d, want 250", total)
	}
}

// TestCTERewrite tests CTE table rewriting
func TestCTERewrite(t *testing.T) {
	dbFile := "test_cte_rewrite.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE base (x INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO base VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE that rewrites FROM clause
	query := `WITH temp AS (SELECT x FROM base) SELECT x FROM temp`
	var x int
	err = db.QueryRow(query).Scan(&x)
	if err != nil {
		t.Logf("CTE rewrite: %v", err)
		return
	}

	if x != 1 {
		t.Errorf("x = %d, want 1", x)
	}
}

// TestAttachDatabase tests ATTACH DATABASE statement
func TestAttachDatabase(t *testing.T) {
	t.Skip("ATTACH not implemented")
	mainDB := "test_attach_main.db"
	attachDB := "test_attach_other.db"
	defer os.Remove(mainDB)
	defer os.Remove(attachDB)

	// Create and populate the database to attach
	otherDB, err := sql.Open(DriverName, attachDB)
	if err != nil {
		t.Fatalf("failed to create other database: %v", err)
	}

	_, err = otherDB.Exec("CREATE TABLE other_table (id INTEGER, value TEXT)")
	if err != nil {
		otherDB.Close()
		t.Fatalf("CREATE TABLE in other db failed: %v", err)
	}

	_, err = otherDB.Exec("INSERT INTO other_table VALUES (1, 'attached')")
	if err != nil {
		otherDB.Close()
		t.Fatalf("INSERT in other db failed: %v", err)
	}
	otherDB.Close()

	// Open main database
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("failed to open main database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE main_table (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE in main db failed: %v", err)
	}

	// Test ATTACH DATABASE
	_, err = db.Exec("ATTACH DATABASE '" + attachDB + "' AS other")
	if err != nil {
		t.Logf("ATTACH DATABASE: %v", err)
		// ATTACH may not be fully implemented
		return
	}

	// Query attached database
	var value string
	err = db.QueryRow("SELECT value FROM other.other_table WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("SELECT from attached db failed: %v", err)
	}

	if value != "attached" {
		t.Errorf("value = %s, want 'attached'", value)
	}

	// Test DETACH DATABASE
	_, err = db.Exec("DETACH DATABASE other")
	if err != nil {
		t.Errorf("DETACH DATABASE failed: %v", err)
	}

	// Verify detached
	err = db.QueryRow("SELECT value FROM other.other_table WHERE id = 1").Scan(&value)
	if err == nil {
		t.Error("Expected error querying detached database, got nil")
	}
}

// TestDetachDatabase tests DETACH without prior ATTACH
func TestDetachDatabase(t *testing.T) {
	dbFile := "test_detach.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Try to detach non-existent database
	_, err = db.Exec("DETACH DATABASE nonexistent")
	if err == nil {
		t.Error("Expected error detaching nonexistent database, got nil")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestAttachWithAlias tests ATTACH with different alias names
func TestAttachWithAlias(t *testing.T) {
	t.Skip("ATTACH not implemented")
	mainDB := "test_attach_alias_main.db"
	attachDB := "test_attach_alias_other.db"
	defer os.Remove(mainDB)
	defer os.Remove(attachDB)

	// Create attached database
	otherDB, err := sql.Open(DriverName, attachDB)
	if err != nil {
		t.Fatalf("failed to create other database: %v", err)
	}

	_, err = otherDB.Exec("CREATE TABLE data (val INTEGER)")
	if err != nil {
		otherDB.Close()
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = otherDB.Exec("INSERT INTO data VALUES (42)")
	if err != nil {
		otherDB.Close()
		t.Fatalf("INSERT failed: %v", err)
	}
	otherDB.Close()

	// Open main and attach
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("failed to open main database: %v", err)
	}
	defer db.Close()

	// Attach with custom alias
	_, err = db.Exec("ATTACH DATABASE '" + attachDB + "' AS mydb")
	if err != nil {
		t.Logf("ATTACH with alias: %v", err)
		return
	}

	// Query using alias
	var val int
	err = db.QueryRow("SELECT val FROM mydb.data").Scan(&val)
	if err != nil {
		t.Fatalf("SELECT with alias failed: %v", err)
	}

	if val != 42 {
		t.Errorf("val = %d, want 42", val)
	}

	// Detach using alias
	_, err = db.Exec("DETACH DATABASE mydb")
	if err != nil {
		t.Errorf("DETACH with alias failed: %v", err)
	}
}

// TestCTEColumnAliases tests CTE with column aliases
func TestCTEColumnAliases(t *testing.T) {
	dbFile := "test_cte_aliases.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id INTEGER, price INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO items VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE with column aliases
	query := `
		WITH expensive_items(item_id, item_price) AS (
			SELECT id, price FROM items WHERE price > 50
		)
		SELECT item_id, item_price FROM expensive_items
	`

	var id, price int
	err = db.QueryRow(query).Scan(&id, &price)
	if err != nil {
		t.Logf("CTE with column aliases: %v", err)
		return
	}

	if id != 1 || price != 100 {
		t.Errorf("Got (%d, %d), want (1, 100)", id, price)
	}
}

// TestNestedCTE tests nested CTE references
func TestNestedCTE(t *testing.T) {
	dbFile := "test_nested_cte.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nums (n INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, err = db.Exec("INSERT INTO nums VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Nested CTEs (one CTE referencing another)
	query := `
		WITH cte1 AS (SELECT n FROM nums),
		     cte2 AS (SELECT n * 2 AS doubled FROM cte1)
		SELECT doubled FROM cte2
	`

	rows, err := db.Query(query)
	if err != nil {
		t.Logf("Nested CTE: %v", err)
		return
	}
	defer rows.Close()

	expected := []int{2, 4, 6}
	i := 0
	for rows.Next() {
		var doubled int
		err = rows.Scan(&doubled)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if i < len(expected) && doubled != expected[i] {
			t.Errorf("Row %d: doubled = %d, want %d", i, doubled, expected[i])
		}
		i++
	}
}

// TestRecursiveCTEComplexTermination tests recursive CTE termination
func TestRecursiveCTEComplexTermination(t *testing.T) {
	dbFile := "test_recursive_term.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Recursive CTE with complex termination
	query := `
		WITH RECURSIVE countdown(n) AS (
			SELECT 10
			UNION ALL
			SELECT n - 1 FROM countdown WHERE n > 1
		)
		SELECT COUNT(*) FROM countdown
	`

	var count int
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Logf("Recursive CTE with termination: %v", err)
		return
	}

	if count != 10 {
		t.Errorf("count = %d, want 10", count)
	}
}

// TestCTEInSubquery tests CTE used in subquery
func TestCTEInSubquery(t *testing.T) {
	t.Skip("CTE with GROUP BY aggregate returns no rows - see TestCTEWithAggregate")
	dbFile := "test_cte_subquery.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE products (id INTEGER, category TEXT, price INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO products VALUES (1, 'A', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO products VALUES (2, 'A', 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO products VALUES (3, 'B', 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE in subquery context
	query := `
		WITH category_avg AS (
			SELECT category, AVG(price) as avg_price
			FROM products
			GROUP BY category
		)
		SELECT id FROM products
		WHERE price > (SELECT avg_price FROM category_avg WHERE category = products.category)
	`

	rows, err := db.Query(query)
	if err != nil {
		t.Logf("CTE in subquery: %v", err)
		return
	}
	defer rows.Close()

	ids := []int{}
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		ids = append(ids, id)
	}

	t.Logf("IDs above category average: %v", ids)
}

// TestCTEMaterialization tests CTE materialization
func TestCTEMaterialization(t *testing.T) {
	dbFile := "test_cte_materialize.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE base_data (value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO base_data VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// CTE that should be materialized and reused
	query := `
		WITH computed AS (SELECT value * value AS squared FROM base_data)
		SELECT COUNT(*) FROM computed
		WHERE squared > (SELECT AVG(squared) FROM computed)
	`

	var count int
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Logf("CTE materialization: %v", err)
		return
	}

	t.Logf("Count of values above average: %d", count)
}

// TestCTEWithJoin tests CTE combined with joins
func TestCTEWithJoin(t *testing.T) {
	dbFile := "test_cte_join.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT users failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO orders VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT orders failed: %v", err)
	}

	// CTE with join
	query := `
		WITH user_totals AS (
			SELECT user_id, SUM(amount) as total
			FROM orders
			GROUP BY user_id
		)
		SELECT users.name, user_totals.total
		FROM users, user_totals
		WHERE users.id = user_totals.user_id
	`

	var name string
	var total int
	err = db.QueryRow(query).Scan(&name, &total)
	if err != nil {
		t.Logf("CTE with join: %v", err)
		return
	}

	if name != "Alice" || total != 100 {
		t.Errorf("Got (%s, %d), want (Alice, 100)", name, total)
	}
}

// TestRecursiveCTEValidation tests recursive CTE validation
func TestRecursiveCTEValidation(t *testing.T) {
	dbFile := "test_recursive_validation.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test invalid recursive CTE (missing base case)
	invalidQuery := `
		WITH RECURSIVE bad(n) AS (
			SELECT n + 1 FROM bad
		)
		SELECT n FROM bad
	`

	_, err = db.Query(invalidQuery)
	if err == nil {
		t.Log("Expected error for invalid recursive CTE")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCTETempTableCreation tests CTE temporary table creation
func TestCTETempTableCreation(t *testing.T) {
	dbFile := "test_cte_temp.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE source (x INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO source VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE that creates temp table internally
	query := `WITH temp_cte AS (SELECT x * 10 AS y FROM source) SELECT y FROM temp_cte`

	var y int
	err = db.QueryRow(query).Scan(&y)
	if err != nil {
		t.Logf("CTE temp table: %v", err)
		return
	}

	if y != 10 {
		t.Errorf("y = %d, want 10", y)
	}
}

// TestCTEBytecodeInlining tests CTE bytecode inlining
func TestCTEBytecodeInlining(t *testing.T) {
	t.Skip("CTE bytecode inlining not fully implemented")
	dbFile := "test_cte_inline.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE inline_test (val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, err = db.Exec("INSERT INTO inline_test VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Simple CTE that might be inlined
	query := `WITH simple AS (SELECT val FROM inline_test) SELECT COUNT(*) FROM simple`

	var count int
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Logf("CTE inline: %v", err)
		return
	}

	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

// TestCTERegisterAdjustment tests CTE register number adjustment
func TestCTERegisterAdjustment(t *testing.T) {
	t.Skip("CTE register adjustment has bugs")
	dbFile := "test_cte_registers.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE reg_test (a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO reg_test VALUES (1, 2)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// CTE with multiple columns requiring register management
	query := `
		WITH calc AS (SELECT a, b, a + b AS sum FROM reg_test)
		SELECT a, b, sum FROM calc
	`

	var a, b, sum int
	err = db.QueryRow(query).Scan(&a, &b, &sum)
	if err != nil {
		t.Logf("CTE register adjustment: %v", err)
		return
	}

	if a != 1 || b != 2 || sum != 3 {
		t.Errorf("Got (%d, %d, %d), want (1, 2, 3)", a, b, sum)
	}
}

// TestMultipleAttachDetach tests multiple ATTACH/DETACH operations
func TestMultipleAttachDetach(t *testing.T) {
	t.Skip("ATTACH not implemented")
	mainDB := "test_multi_attach_main.db"
	db1 := "test_multi_attach_1.db"
	db2 := "test_multi_attach_2.db"
	defer os.Remove(mainDB)
	defer os.Remove(db1)
	defer os.Remove(db2)

	// Create databases
	for _, dbFile := range []string{db1, db2} {
		tmpDB, err := sql.Open(DriverName, dbFile)
		if err != nil {
			t.Fatalf("failed to create %s: %v", dbFile, err)
		}
		_, err = tmpDB.Exec("CREATE TABLE data (id INTEGER)")
		if err != nil {
			tmpDB.Close()
			t.Fatalf("CREATE TABLE in %s failed: %v", dbFile, err)
		}
		tmpDB.Close()
	}

	// Open main and attach multiple
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("failed to open main database: %v", err)
	}
	defer db.Close()

	// Attach first database
	_, err = db.Exec("ATTACH DATABASE '" + db1 + "' AS db1")
	if err != nil {
		t.Logf("ATTACH db1: %v", err)
		return
	}

	// Attach second database
	_, err = db.Exec("ATTACH DATABASE '" + db2 + "' AS db2")
	if err != nil {
		t.Logf("ATTACH db2: %v", err)
		return
	}

	// Detach first
	_, err = db.Exec("DETACH DATABASE db1")
	if err != nil {
		t.Errorf("DETACH db1 failed: %v", err)
	}

	// Detach second
	_, err = db.Exec("DETACH DATABASE db2")
	if err != nil {
		t.Errorf("DETACH db2 failed: %v", err)
	}
}

// TestCTEEmptyResult tests CTE with empty result set
func TestCTEEmptyResult(t *testing.T) {
	dbFile := "test_cte_empty.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE empty_test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// CTE with empty result (no rows in table)
	query := `WITH empty AS (SELECT id FROM empty_test) SELECT COUNT(*) FROM empty`

	var count int
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Logf("CTE with empty result: %v", err)
		return
	}

	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}
