package driver

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// This file contains tests specifically to improve coverage of uncovered code paths

func TestTransactionControlStatements(t *testing.T) {
	dbFile := "test_tx_control.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test BEGIN statement
	_, err = db.Exec("BEGIN")
	if err != nil {
		t.Errorf("BEGIN failed: %v", err)
	}

	// Test COMMIT statement
	_, err = db.Exec("COMMIT")
	if err != nil {
		t.Errorf("COMMIT failed: %v", err)
	}

	// Test BEGIN TRANSACTION
	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		t.Errorf("BEGIN TRANSACTION failed: %v", err)
	}

	// Test ROLLBACK statement
	_, err = db.Exec("ROLLBACK")
	if err != nil {
		t.Errorf("ROLLBACK failed: %v", err)
	}
}

func TestDropTable(t *testing.T) {
	dbFile := "test_drop_table.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Drop the table
	_, err = db.Exec("DROP TABLE test")
	if err != nil {
		t.Errorf("DROP TABLE failed: %v", err)
	}

	// Drop if exists
	_, err = db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS failed: %v", err)
	}
}

func TestCountStar(t *testing.T) {
	dbFile := "test_count_star.db"
	defer os.Remove(dbFile)

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

	_, err = db.Exec("INSERT INTO test VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test COUNT(*)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("COUNT(*) failed: %v", err)
	}
	if count != 2 {
		t.Errorf("COUNT(*) = %d, want 2", count)
	}
}

func TestAggregateFunction(t *testing.T) {
	dbFile := "test_aggregate.db"
	defer os.Remove(dbFile)

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

	_, err = db.Exec("INSERT INTO test VALUES (2, 20)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (3, 30)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test SUM
	var sum int
	err = db.QueryRow("SELECT SUM(value) FROM test").Scan(&sum)
	if err != nil {
		t.Errorf("SUM failed: %v", err)
	}
	if sum != 60 {
		t.Errorf("SUM = %d, want 60", sum)
	}

	// Test AVG
	var avg float64
	err = db.QueryRow("SELECT AVG(value) FROM test").Scan(&avg)
	if err != nil {
		t.Errorf("AVG failed: %v", err)
	}
	if avg != 20.0 {
		t.Errorf("AVG = %f, want 20.0", avg)
	}

	// Test MIN
	var min int
	err = db.QueryRow("SELECT MIN(value) FROM test").Scan(&min)
	if err != nil {
		t.Errorf("MIN failed: %v", err)
	}
	if min != 10 {
		t.Errorf("MIN = %d, want 10", min)
	}

	// Test MAX
	var max int
	err = db.QueryRow("SELECT MAX(value) FROM test").Scan(&max)
	if err != nil {
		t.Errorf("MAX failed: %v", err)
	}
	if max != 30 {
		t.Errorf("MAX = %d, want 30", max)
	}
}

func TestReleaseState(t *testing.T) {
	dbFile := "test_release_state.db"
	defer os.Remove(dbFile)

	d := &Driver{}

	// Open first connection
	conn1, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open first connection: %v", err)
	}

	// Open second connection to same file
	conn2, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open second connection: %v", err)
	}

	// Close first connection - should decrease refCnt but not remove state
	conn1.Close()

	// Verify state still exists (implicitly by second connection still working)
	c2 := conn2.(*Conn)
	if err := c2.Ping(context.Background()); err != nil {
		t.Error("second connection should still work after first closes")
	}

	// Close second connection - should remove state
	conn2.Close()
}

func TestComplexAggregateQuery(t *testing.T) {
	dbFile := "test_complex_agg.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE sales (id INTEGER, amount INTEGER, region TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES (1, 100, 'North')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES (2, 200, 'South')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test aggregate with WHERE clause
	var total int
	err = db.QueryRow("SELECT SUM(amount) FROM sales WHERE amount > 50").Scan(&total)
	if err != nil {
		t.Errorf("SUM with WHERE failed: %v", err)
	}
	if total != 300 {
		t.Errorf("SUM = %d, want 300", total)
	}
}

// Note: Non-aggregate function tests are covered by function_test.go and integration tests

func TestOrderByWithLimit(t *testing.T) {
	dbFile := "test_order_limit.db"
	defer os.Remove(dbFile)

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

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ORDER BY with LIMIT
	rows, err := db.Query("SELECT value FROM test ORDER BY value DESC LIMIT 2")
	if err != nil {
		t.Errorf("ORDER BY LIMIT failed: %v", err)
	}
	defer rows.Close()

	var values []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		values = append(values, v)
	}

	if len(values) != 2 {
		t.Errorf("got %d values, want 2", len(values))
	}
	if len(values) >= 2 && (values[0] != 50 || values[1] != 40) {
		t.Errorf("values = %v, want [50, 40]", values)
	}
}

func TestOrderByWithOffset(t *testing.T) {
	dbFile := "test_order_offset.db"
	defer os.Remove(dbFile)

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

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test ORDER BY with OFFSET
	rows, err := db.Query("SELECT value FROM test ORDER BY value ASC LIMIT 2 OFFSET 2")
	if err != nil {
		t.Errorf("ORDER BY OFFSET failed: %v", err)
	}
	defer rows.Close()

	var values []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		values = append(values, v)
	}

	if len(values) != 2 {
		t.Errorf("got %d values, want 2", len(values))
	}
	if len(values) >= 2 && (values[0] != 30 || values[1] != 40) {
		t.Errorf("values = %v, want [30, 40]", values)
	}
}

func TestExecContextAutoCommit(t *testing.T) {
	dbFile := "test_autocommit.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Prepare and execute a statement that modifies data
	stmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Execute - should auto-commit since not in transaction
	_, err = s.ExecContext(context.Background(), nil)
	if err != nil {
		t.Errorf("ExecContext failed: %v", err)
	}

	// Verify auto-commit happened by checking that transaction is not active
	if c.inTx {
		t.Error("should not be in transaction after auto-commit")
	}
}

func TestParameterizedQuery(t *testing.T) {
	dbFile := "test_params.db"
	defer os.Remove(dbFile)

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

	// Insert with parameters
	_, err = db.Exec("INSERT INTO test VALUES (?, ?)", 1, "Alice")
	if err != nil {
		t.Errorf("INSERT with params failed: %v", err)
	}

	// Query with parameters
	var name string
	err = db.QueryRow("SELECT name FROM test WHERE id = ?", 1).Scan(&name)
	if err != nil {
		t.Errorf("SELECT with params failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("name = %s, want Alice", name)
	}
}
