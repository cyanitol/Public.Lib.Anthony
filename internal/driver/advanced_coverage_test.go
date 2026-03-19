// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// advOpenDB opens a database file for testing.
func advOpenDB(t *testing.T, dbFile string) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// advExecAll executes a list of SQL statements.
func advExecAll(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s failed: %v", s, err)
		}
	}
}

// advQueryInts queries a single int column and returns all results.
func advQueryInts(t *testing.T, db *sql.DB, query string) []int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	return vals
}

func TestJoinQuery(t *testing.T) {
	dbFile := t.TempDir() + "/test_join.db"
	db := advOpenDB(t, dbFile)
	defer db.Close()

	advExecAll(t, db,
		"CREATE TABLE users (id INTEGER, name TEXT)",
		"CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)",
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO orders VALUES (1, 1, 100)",
	)

	rows, err := db.Query("SELECT users.id, users.name FROM users, orders")
	if err != nil {
		t.Errorf("multi-table query failed: %v", err)
		return
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
	}
	if !hasRows {
		t.Error("expected at least one row")
	}
}

func TestInsertWithSelectColumns(t *testing.T) {
	dbFile := t.TempDir() + "/test_insert_select_cols.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert specifying columns
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Errorf("INSERT with column list failed: %v", err)
	}

	// Verify
	var name string
	err = db.QueryRow("SELECT name FROM test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	if name != "test" {
		t.Errorf("name = %s, want test", name)
	}
}

func TestUpdateWithExpression(t *testing.T) {
	dbFile := t.TempDir() + "/test_update_expr.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Update with expression
	_, err = db.Exec("UPDATE test SET value = value + 5 WHERE id = 1")
	if err != nil {
		t.Errorf("UPDATE with expression failed: %v", err)
	}

	// Verify
	var value int
	err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	if value != 15 {
		t.Errorf("value = %d, want 15", value)
	}
}

func TestDeleteWithWhere(t *testing.T) {
	dbFile := t.TempDir() + "/test_delete_where.db"
	db := advOpenDB(t, dbFile)
	defer db.Close()

	advExecAll(t, db,
		"CREATE TABLE test (id INTEGER, value INTEGER)",
		"INSERT INTO test VALUES (1, 10)",
		"INSERT INTO test VALUES (2, 20)",
		"DELETE FROM test WHERE id = 1",
	)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	var value int
	if err := db.QueryRow("SELECT value FROM test").Scan(&value); err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	if value != 20 {
		t.Errorf("value = %d, want 20", value)
	}
}

func TestSelectWithQualifiedColumn(t *testing.T) {
	dbFile := t.TempDir() + "/test_qualified_col.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SELECT with table.column syntax
	var name string
	err = db.QueryRow("SELECT test.name FROM test WHERE test.id = 1").Scan(&name)
	if err != nil {
		t.Errorf("SELECT with qualified column failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}

func TestSelectAllColumns(t *testing.T) {
	dbFile := t.TempDir() + "/test_select_all.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 'Alice', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SELECT * with table prefix
	rows, err := db.Query("SELECT test.* FROM test")
	if err != nil {
		t.Errorf("SELECT test.* failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("expected one row")
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Errorf("Columns() failed: %v", err)
	}
	if len(cols) != 3 {
		t.Errorf("got %d columns, want 3", len(cols))
	}
}

func TestExpressionInOrderBy(t *testing.T) {
	dbFile := t.TempDir() + "/test_order_expr.db"
	db := advOpenDB(t, dbFile)
	defer db.Close()

	advExecAll(t, db,
		"CREATE TABLE test (id INTEGER, value INTEGER)",
		"INSERT INTO test VALUES (1, 10)",
		"INSERT INTO test VALUES (2, 5)",
	)

	ids := advQueryInts(t, db, "SELECT id FROM test ORDER BY value")
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 1 {
		t.Errorf("ids = %v, want [2 1]", ids)
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	dbFile := t.TempDir() + "/test_create_if_not_exists.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create again with IF NOT EXISTS should not error
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
	if err != nil {
		t.Errorf("CREATE TABLE IF NOT EXISTS failed: %v", err)
	}
}

func TestTransactionRollbackOnError(t *testing.T) {
	dbFile := t.TempDir() + "/test_tx_rollback_error.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert data
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Errorf("ROLLBACK failed: %v", err)
	}

	// Verify data was rolled back
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 (data should be rolled back)", count)
	}
}

func TestMultipleInserts(t *testing.T) {
	dbFile := t.TempDir() + "/test_multi_insert.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Multiple inserts
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, "test")
		if err != nil {
			t.Errorf("INSERT %d failed: %v", i, err)
		}
	}

	// Verify count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 10 {
		t.Errorf("count = %d, want 10", count)
	}
}

func TestOrderByColumnNumber(t *testing.T) {
	dbFile := t.TempDir() + "/test_order_colnum.db"
	db := advOpenDB(t, dbFile)
	defer db.Close()

	advExecAll(t, db,
		"CREATE TABLE test (id INTEGER, value INTEGER)",
		"INSERT INTO test VALUES (1, 10)",
		"INSERT INTO test VALUES (2, 5)",
	)

	rows, err := db.Query("SELECT id, value FROM test ORDER BY 1")
	if err != nil {
		t.Fatalf("ORDER BY column number failed: %v", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id, value int
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("ids = %v, want [1 2]", ids)
	}
}
