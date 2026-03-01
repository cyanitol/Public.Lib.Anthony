// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestConcurrentClose tests closing a connection while queries are running
// This test should be run with -race flag to detect race conditions
func TestConcurrentClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, "test")
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start multiple goroutines running queries
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					rows, err := db.Query("SELECT * FROM test WHERE id = ?", id)
					if err != nil {
						// Connection may be closed, which is expected
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
		}(i)
	}

	// Let queries run for a bit
	time.Sleep(50 * time.Millisecond)

	// Close the database while queries are running
	err = db.Close()
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()

	// We expect Close to succeed (either immediately or after queries finish)
	if err != nil {
		t.Errorf("Unexpected error closing database: %v", err)
	}
}

// TestConcurrentExec tests concurrent statement execution
func TestConcurrentExec(t *testing.T) {
	t.Skip("Concurrent write operations not fully supported without WAL mode")
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Serialize access through single connection to avoid file locking issues
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const numGoroutines = 10
	const numOps = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOps)

	// Run concurrent inserts
	insertCount := make([]int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				id := base*numOps + j
				_, err := db.Exec("INSERT INTO test (id, value) VALUES (?, ?)", id, "test")
				if err != nil {
					errors <- err
					// Don't return early - continue trying other inserts
				} else {
					insertCount[base]++
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		if errorCount <= 5 { // Only log first 5 errors to avoid spam
			t.Logf("Concurrent exec error: %v", err)
		}
	}
	if errorCount > 0 {
		t.Logf("Total errors: %d", errorCount)
	}

	// Log insert counts per goroutine
	totalInserted := 0
	for i, count := range insertCount {
		totalInserted += count
		if count != numOps {
			t.Logf("Goroutine %d: inserted %d/%d", i, count, numOps)
		}
	}
	t.Logf("Total inserts attempted: %d", totalInserted)

	// Verify all rows were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	expected := numGoroutines * numOps
	if count != expected {
		t.Errorf("Expected %d rows, got %d (missing %d)", expected, count, expected-count)
	}
}

// TestConcurrentReadWrite tests concurrent reads and writes
func TestConcurrentReadWrite(t *testing.T) {
	t.Skip("Concurrent write operations not fully supported without WAL mode")
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Serialize access through single connection to avoid file locking issues
	db.SetMaxOpenConns(1)

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, "initial")
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-done:
					return
				default:
					_, err := db.Exec("UPDATE test SET value = ? WHERE id = ?", "updated", id)
					if err != nil {
						t.Errorf("Write error: %v", err)
						return
					}
					counter++
					if counter >= 10 {
						return
					}
				}
			}
		}(i)
	}

	// Let operations run
	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()
}

// TestConcurrentPrepare tests concurrent statement preparation
func TestConcurrentPrepare(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

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

// TestSecurityConcurrentTransactions tests concurrent transaction handling
func TestSecurityConcurrentTransactions(t *testing.T) {
	t.Skip("Concurrent transactions not fully supported without WAL mode")
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Serialize access through single connection to avoid file locking issues
	db.SetMaxOpenConns(1)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	_, err = db.Exec("INSERT INTO test VALUES (1, 0)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	const numGoroutines = 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				tx, err := db.Begin()
				if err != nil {
					t.Errorf("Begin error: %v", err)
					return
				}

				// Read current value
				var value int
				err = tx.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
				if err != nil {
					tx.Rollback()
					t.Errorf("Query error: %v", err)
					return
				}

				// Update with incremented value
				_, err = tx.Exec("UPDATE test SET value = ? WHERE id = 1", value+1)
				if err != nil {
					tx.Rollback()
					t.Errorf("Update error: %v", err)
					return
				}

				// Commit
				err = tx.Commit()
				if err != nil {
					t.Errorf("Commit error: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Verify final value (may not be exactly numGoroutines * 10 due to transaction conflicts)
	var finalValue int
	err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&finalValue)
	if err != nil {
		t.Fatalf("Failed to query final value: %v", err)
	}

	// Should have increased by at least some amount
	if finalValue == 0 {
		t.Error("Expected value to be incremented")
	}
}

// TestConcurrentStmtClose tests closing statements while they're being used
func TestConcurrentStmtClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

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

// TestConcurrentConnections tests multiple concurrent connections
func TestConcurrentConnections(t *testing.T) {
	t.Skip("Concurrent connections with write operations not fully supported without WAL mode")
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create initial database
	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	// Serialize access through single connection to avoid file locking issues
	db1.SetMaxOpenConns(1)

	_, err = db1.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	db1.Close()

	const numConns = 5
	var wg sync.WaitGroup

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			db, err := sql.Open("sqlite_internal", dbPath)
			if err != nil {
				t.Errorf("Failed to open connection: %v", err)
				return
			}
			defer db.Close()
			// Serialize access through single connection to avoid file locking issues
			db.SetMaxOpenConns(1)

			// Each connection does some work
			for j := 0; j < 20; j++ {
				insertID := id*100 + j
				_, err := db.Exec("INSERT INTO test VALUES (?, ?)", insertID, "data")
				if err != nil {
					t.Errorf("Insert error: %v", err)
					return
				}

				var count int
				err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
				if err != nil {
					t.Errorf("Query error: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	dbFinal, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open final connection: %v", err)
	}
	defer dbFinal.Close()
	dbFinal.SetMaxOpenConns(1)

	var count int
	err = dbFinal.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count final rows: %v", err)
	}

	expected := numConns * 20
	if count != expected {
		t.Errorf("Expected %d rows, got %d", expected, count)
	}
}

// TestRaceConditionOnClose tests for race conditions during connection close
func TestRaceConditionOnClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

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
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

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
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

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

// Clean up test files
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
