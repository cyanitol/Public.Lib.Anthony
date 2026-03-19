// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

func TestMemoryDatabaseBasic(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	defer db.Close()

	// Verify the connection works
	if err := db.Ping(); err != nil {
		t.Errorf("ping failed: %v", err)
	}
}

// memOpenDB opens an in-memory database and fatals on error.
func memOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	return db
}

// memExec executes a SQL statement and fatals on error.
func memExec(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result {
	t.Helper()
	result, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("failed to exec %q: %v", query, err)
	}
	return result
}

// memScanCount scans a COUNT(*) query and returns the result.
func memScanCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("failed to query count: %v", err)
	}
	return count
}

func TestMemoryDatabaseCreateTable(t *testing.T) {
	db := memOpenDB(t)
	defer db.Close()

	memExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	memExec(t, db, "INSERT INTO users (name, age) VALUES ('Alice', 30)")
	memExec(t, db, "INSERT INTO users (name, age) VALUES ('Bob', 25)")

	rows, err := db.Query("SELECT name, age FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name string
		age  int
	}{
		{"Alice", 30},
		{"Bob", 25},
	}

	i := 0
	for rows.Next() {
		var name string
		var age int
		if err := rows.Scan(&name, &age); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if name != expected[i].name || age != expected[i].age {
			t.Errorf("row %d: got (%s, %d), want (%s, %d)", i, name, age, expected[i].name, expected[i].age)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

// memTxExecAndFinish begins a transaction, executes a statement, and either commits or rolls back.
func memTxExecAndFinish(t *testing.T, db *sql.DB, query string, commit bool) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if _, err := tx.Exec(query); err != nil {
		t.Fatalf("failed to exec in tx: %v", err)
	}
	if commit {
		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	} else {
		if err := tx.Rollback(); err != nil {
			t.Fatalf("failed to rollback: %v", err)
		}
	}
}

func TestMemoryDatabaseTransaction(t *testing.T) {
	db := memOpenDB(t)
	defer db.Close()

	memExec(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

	// Commit path
	memTxExecAndFinish(t, db, "INSERT INTO test (value) VALUES ('test1')", true)
	if c := memScanCount(t, db, "SELECT COUNT(*) FROM test"); c != 1 {
		t.Errorf("got count %d, want 1", c)
	}

	// Rollback path
	memTxExecAndFinish(t, db, "INSERT INTO test (value) VALUES ('test2')", false)
	if c := memScanCount(t, db, "SELECT COUNT(*) FROM test"); c != 1 {
		t.Errorf("got count %d, want 1 (rollback should not have committed)", c)
	}
}

func TestMemoryDatabaseUpdate(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (value) VALUES ('old')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Update
	result, err := db.Exec("UPDATE test SET value = 'new' WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}

	if rowsAffected != 1 {
		t.Errorf("got %d rows affected, want 1", rowsAffected)
	}

	// Verify update
	var value string
	err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if value != "new" {
		t.Errorf("got value %q, want %q", value, "new")
	}
}

func TestMemoryDatabaseDelete(t *testing.T) {
	db := memOpenDB(t)
	defer db.Close()

	memExec(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	memExec(t, db, "INSERT INTO test (value) VALUES ('test1')")
	memExec(t, db, "INSERT INTO test (value) VALUES ('test2')")

	result := memExec(t, db, "DELETE FROM test WHERE id = 1")
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("got %d rows affected, want 1", rowsAffected)
	}

	if c := memScanCount(t, db, "SELECT COUNT(*) FROM test"); c != 1 {
		t.Errorf("got count %d, want 1", c)
	}
}

func TestMemoryDatabaseIsolation(t *testing.T) {
	// Open two connections to :memory: - they should be isolated (standard SQLite behavior)
	db1, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open first database: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open second database: %v", err)
	}
	defer db2.Close()

	// Create table in first connection
	_, err = db1.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data in first connection
	_, err = db1.Exec("INSERT INTO test (value) VALUES ('isolated')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Table should NOT exist in second connection (isolated databases)
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}

	if count != 0 {
		t.Errorf("table should not exist in isolated database, but found %d tables", count)
	}
}

func TestMemoryDatabasePersistenceIsolation(t *testing.T) {
	// In-memory databases should not persist after closing
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (value) VALUES ('data')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Close the database
	db.Close()

	// Open a new connection with same name
	db2, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open new database: %v", err)
	}
	defer db2.Close()

	// The table should not exist in the new database
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}

	if count != 0 {
		t.Errorf("table should not exist in new database, but found %d tables", count)
	}
}

// memSetupMultiOpSchema creates users and posts tables for multi-operation tests.
func memSetupMultiOpSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	memExec(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)`)
	memExec(t, db, `CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, content TEXT, FOREIGN KEY(user_id) REFERENCES users(id))`)
}

func TestMemoryDatabaseMultipleOperations(t *testing.T) {
	db := memOpenDB(t)
	defer db.Close()

	memSetupMultiOpSchema(t, db)

	memExec(t, db, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")

	var userID int64
	if err := db.QueryRow("SELECT id FROM users WHERE name = 'Alice'").Scan(&userID); err != nil {
		t.Fatalf("failed to get user ID: %v", err)
	}

	memExec(t, db, "INSERT INTO posts (user_id, title, content) VALUES (?, 'First Post', 'Hello World')", userID)

	rows, err := db.Query(`SELECT users.name, posts.title, posts.content FROM posts JOIN users ON posts.user_id = users.id`)
	if err != nil {
		t.Fatalf("failed to query with join: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var name, title, content string
	if err := rows.Scan(&name, &title, &content); err != nil {
		t.Fatalf("failed to scan row: %v", err)
	}

	if name != "Alice" || title != "First Post" || content != "Hello World" {
		t.Errorf("got (%s, %s, %s), want (Alice, First Post, Hello World)", name, title, content)
	}
}

func TestMemoryDatabaseEmptyStringIsolation(t *testing.T) {
	// Empty string should create isolated databases
	db1, err := sql.Open(DriverName, "")
	if err != nil {
		t.Fatalf("failed to open first database: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open(DriverName, "")
	if err != nil {
		t.Fatalf("failed to open second database: %v", err)
	}
	defer db2.Close()

	// Create table in first connection
	_, err = db1.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table in db1: %v", err)
	}

	// Table should not exist in second connection (isolated database)
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query sqlite_master in db2: %v", err)
	}

	if count != 0 {
		t.Errorf("table should not exist in isolated database, but found %d tables", count)
	}
}

func TestMemoryDatabaseIndex(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	defer db.Close()

	// Create table with index
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_value ON test(value)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Insert data
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO test (value) VALUES (?)", i)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
	}

	// Query using index
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test WHERE value = '50'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if count != 1 {
		t.Errorf("got count %d, want 1", count)
	}
}
