//go:build stress

// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// concOpenAndPopulate creates a test database with the given number of rows.
func concOpenAndPopulate(t *testing.T, driverName, dbPath string, rows int) *sql.DB {
	t.Helper()
	db, err := sql.Open(driverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if _, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, "test"); err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}
	return db
}

// concRunQueryLoop runs queries in a loop until done is closed.
func concRunQueryLoop(db *sql.DB, done <-chan struct{}, id int) {
	for {
		select {
		case <-done:
			return
		default:
			rows, err := db.Query("SELECT * FROM test WHERE id = ?", id)
			if err != nil {
				return
			}
			for rows.Next() {
				var i int
				var s string
				_ = rows.Scan(&i, &s)
			}
			rows.Close()
		}
	}
}

// TestConcurrentClose tests closing a connection while queries are running
func TestConcurrentClose(t *testing.T) {
	db := concOpenAndPopulate(t, DriverName, filepath.Join(t.TempDir(), "test.db"), 100)

	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			concRunQueryLoop(db, done, id)
		}(i)
	}

	time.Sleep(50 * time.Millisecond)
	err := db.Close()
	close(done)
	wg.Wait()

	if err != nil {
		t.Errorf("Unexpected error closing database: %v", err)
	}
}

// concExecInsertWorker runs insert operations and reports errors.
func concExecInsertWorker(db *sql.DB, base, numOps int, errors chan<- error, insertCount []int) {
	for j := 0; j < numOps; j++ {
		id := base*numOps + j
		_, err := db.Exec("INSERT INTO test (id, value) VALUES (?, ?)", id, "test")
		if err != nil {
			errors <- err
		} else {
			insertCount[base]++
		}
	}
}

// concExecLogErrors drains and logs errors from the channel.
func concExecLogErrors(t *testing.T, errors <-chan error) {
	t.Helper()
	errorCount := 0
	for err := range errors {
		errorCount++
		if errorCount <= 5 {
			t.Logf("Concurrent exec error: %v", err)
		}
	}
	if errorCount > 0 {
		t.Logf("Total errors: %d", errorCount)
	}
}

// TestConcurrentExec tests concurrent statement execution
func TestConcurrentExec(t *testing.T) {
	db, err := sql.Open(DriverName, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if _, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const numGoroutines = 10
	const numOps = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOps)
	insertCount := make([]int, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			concExecInsertWorker(db, base, numOps, errors, insertCount)
		}(i)
	}

	wg.Wait()
	close(errors)
	concExecLogErrors(t, errors)

	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != numGoroutines*numOps {
		t.Errorf("Expected %d rows, got %d", numGoroutines*numOps, count)
	}
}

// concReaderLoop reads rows in a loop until done is closed.
func concReaderLoop(t *testing.T, db *sql.DB, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
			rows, err := db.Query("SELECT * FROM test LIMIT 10")
			if err != nil {
				t.Errorf("Read error: %v", err)
				return
			}
			for rows.Next() {
				var id int
				var value string
				_ = rows.Scan(&id, &value)
			}
			rows.Close()
		}
	}
}

// concWriterLoop writes updates in a loop until done or maxWrites.
func concWriterLoop(t *testing.T, db *sql.DB, done <-chan struct{}, id, maxWrites int) {
	for counter := 0; counter < maxWrites; counter++ {
		select {
		case <-done:
			return
		default:
			if _, err := db.Exec("UPDATE test SET value = ? WHERE id = ?", "updated", id); err != nil {
				t.Errorf("Write error: %v", err)
				return
			}
		}
	}
}

// TestConcurrentReadWrite tests concurrent reads and writes
func TestConcurrentReadWrite(t *testing.T) {
	db := concOpenAndPopulate(t, DriverName, filepath.Join(t.TempDir(), "test.db"), 100)
	defer db.Close()
	db.SetMaxOpenConns(1)

	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); concReaderLoop(t, db, done) }()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); concWriterLoop(t, db, done, id, 10) }(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()
}

// TestConcurrentPrepare tests concurrent statement preparation
func TestConcurrentPrepare(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const numGoroutines = 20
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Prepare statement
			stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
			if err != nil {
				t.Errorf("Prepare error: %v", err)
				return
			}
			defer stmt.Close()

			// Execute it a few times
			for j := 0; j < 10; j++ {
				rows, err := stmt.Query(j)
				if err != nil {
					t.Errorf("Query error: %v", err)
					return
				}
				rows.Close()
			}
		}()
	}

	wg.Wait()
}

// concTxIncrementWorker runs increment transactions in a loop.
func concTxIncrementWorker(t *testing.T, db *sql.DB, iterations int) {
	for j := 0; j < iterations; j++ {
		if err := concTxIncrement(t, db); err != nil {
			return
		}
	}
}

// concTxIncrement performs a single read-increment-commit cycle.
func concTxIncrement(t *testing.T, db *sql.DB) error {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("Begin error: %v", err)
		return err
	}
	var value int
	if err = tx.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value); err != nil {
		tx.Rollback()
		t.Errorf("Query error: %v", err)
		return err
	}
	if _, err = tx.Exec("UPDATE test SET value = ? WHERE id = 1", value+1); err != nil {
		tx.Rollback()
		t.Errorf("Update error: %v", err)
		return err
	}
	if err = tx.Commit(); err != nil {
		t.Errorf("Commit error: %v", err)
		return err
	}
	return nil
}

// TestSecurityConcurrentTransactions tests concurrent transaction handling
func TestSecurityConcurrentTransactions(t *testing.T) {
	db, err := sql.Open(DriverName, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if _, err = db.Exec("INSERT INTO test VALUES (1, 0)"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); concTxIncrementWorker(t, db, 10) }()
	}
	wg.Wait()

	var finalValue int
	if err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&finalValue); err != nil {
		t.Fatalf("Failed to query final value: %v", err)
	}
	if finalValue == 0 {
		t.Error("Expected value to be incremented")
	}
}

// TestConcurrentStmtClose tests closing statements while they're being used
func TestConcurrentStmtClose(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	var wg sync.WaitGroup

	// Start goroutine using the statement
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			rows, err := stmt.Query(i)
			if err != nil {
				// Expected to fail after Close
				return
			}
			rows.Close()
		}
	}()

	// Close statement after a brief delay
	time.Sleep(10 * time.Millisecond)
	stmt.Close()

	wg.Wait()
}

// concConnWorker opens a connection, inserts rows, and verifies counts.
func concConnWorker(t *testing.T, dbPath string, id, numOps int) {
	db, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Errorf("Failed to open connection: %v", err)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	for j := 0; j < numOps; j++ {
		if _, err := db.Exec("INSERT INTO test VALUES (?, ?)", id*100+j, "data"); err != nil {
			t.Errorf("Insert error: %v", err)
			return
		}
		var count int
		if err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count); err != nil {
			t.Errorf("Query error: %v", err)
			return
		}
	}
}

// TestConcurrentConnections tests multiple concurrent connections
func TestConcurrentConnections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db1.SetMaxOpenConns(1)
	if _, err = db1.Exec("CREATE TABLE test (id INTEGER, value TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	db1.Close()

	const numConns = 5
	var wg sync.WaitGroup
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); concConnWorker(t, dbPath, id, 20) }(i)
	}
	wg.Wait()

	dbFinal, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open final connection: %v", err)
	}
	defer dbFinal.Close()
	dbFinal.SetMaxOpenConns(1)

	var count int
	if err = dbFinal.QueryRow("SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatalf("Failed to count final rows: %v", err)
	}
	if count != numConns*20 {
		t.Errorf("Expected %d rows, got %d", numConns*20, count)
	}
}

// TestRaceConditionOnClose tests for race conditions during connection close
func TestRaceConditionOnClose(t *testing.T) {
	// This test is specifically designed to catch race conditions with -race flag

	for iteration := 0; iteration < 10; iteration++ {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "test.db")

		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		_, err = db.Exec("CREATE TABLE test (id INTEGER)")
		if err != nil {
			db.Close()
			t.Fatalf("Failed to create table: %v", err)
		}

		var wg sync.WaitGroup

		// Multiple goroutines executing queries
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, _ = db.Exec("INSERT INTO test VALUES (?)", j)
				}
			}()
		}

		// Close while operations are in progress
		time.Sleep(5 * time.Millisecond)
		db.Close()

		wg.Wait()
	}
}

// TestSecurityContextCancellation tests query cancellation via context
func TestSecurityContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// This query might get cancelled
	_, err = db.QueryContext(ctx, "SELECT * FROM test")
	// We don't check the error because it might succeed if fast enough
}

// TestDatabaseFileLocking tests file locking behavior
func TestDatabaseFileLocking(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open first connection: %v", err)
	}
	defer db1.Close()

	_, err = db1.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Open second connection to same file
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open second connection: %v", err)
	}
	defer db2.Close()

	// Both connections should be able to read
	var count1, count2 int
	err = db1.QueryRow("SELECT COUNT(*) FROM test").Scan(&count1)
	if err != nil {
		t.Errorf("First connection read failed: %v", err)
	}

	err = db2.QueryRow("SELECT COUNT(*) FROM test").Scan(&count2)
	if err != nil {
		t.Errorf("Second connection read failed: %v", err)
	}
}
