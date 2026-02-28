package driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConnConcurrentClose tests that multiple goroutines can safely call Close concurrently
func TestConnConcurrentClose(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Get the underlying driver connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Launch multiple goroutines that all try to close the connection
	const numGoroutines = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := conn.Close(); err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	// Check for errors - at least one Close should succeed
	// database/sql wraps our driver and may return "connection is already closed"
	// for subsequent calls, which is acceptable
	var alreadyClosedCount int
	for err := range errChan {
		if err.Error() == "sql: connection is already closed" {
			alreadyClosedCount++
		} else {
			t.Errorf("Unexpected Close error: %v", err)
		}
	}

	// At least one close should have succeeded (the first one)
	if alreadyClosedCount == numGoroutines {
		t.Error("All Close calls reported already closed - at least one should succeed")
	}

	successCount := numGoroutines - alreadyClosedCount
	t.Logf("Close results: %d succeeded, %d already closed", successCount, alreadyClosedCount)
}

// TestCloseDuringActiveQuery tests closing a connection while queries are running
func TestCloseDuringActiveQuery(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create a table with some data
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err := db.Exec("INSERT INTO test (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Get a dedicated connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	var wg sync.WaitGroup
	var queryErrors int32
	var closeError error

	// Start multiple query goroutines
	const numQueries = 5
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func(queryID int) {
			defer wg.Done()

			// Keep querying until connection is closed
			for j := 0; j < 10; j++ {
				// Use standard database/sql query (no Raw)
				var id int
				var value string
				err := conn.QueryRowContext(context.Background(), "SELECT id, value FROM test WHERE id = ?", queryID).Scan(&id, &value)

				if err != nil {
					// Expected to fail after close
					atomic.AddInt32(&queryErrors, 1)
					return
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Give queries time to start
	time.Sleep(5 * time.Millisecond)

	// Close the connection while queries are running
	closeError = conn.Close()
	if closeError != nil {
		t.Errorf("Close failed: %v", closeError)
	}

	wg.Wait()

	// Some queries should have failed (that's expected and OK)
	t.Logf("Query errors (expected after close): %d", queryErrors)
}

// TestNoDeadlockOnConcurrentOperations tests for deadlock scenarios
func TestNoDeadlockOnConcurrentOperations(t *testing.T) {
	// Use a timeout to detect deadlocks
	done := make(chan bool)

	go func() {
		db, cleanup := setupConnTestDB(t)
		defer cleanup()

		// Create test table
		_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
			done <- false
			return
		}

		var wg sync.WaitGroup
		const numOps = 20

		// Concurrent connection creation and closing
		for i := 0; i < numOps; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				conn, err := db.Conn(context.Background())
				if err != nil {
					return
				}

				// Do some work - execute a simple query
				var result int
				_ = conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)

				// Close the connection
				conn.Close()
			}(i)
		}

		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - possible deadlock detected")
	}
}

// TestConcurrentPrepareAndClose tests concurrent Prepare and Close operations
func TestConcurrentPrepareAndClose(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create test table
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	var wg sync.WaitGroup
	stopPrepare := make(chan bool)

	// Goroutine that prepares statements
	const numPreparers = 3
	for i := 0; i < numPreparers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopPrepare:
					return
				default:
					// Prepare and execute a query
					stmt, err := db.Prepare("SELECT * FROM test")
					if err == nil {
						stmt.Close()
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Let preparers run for a bit
	time.Sleep(10 * time.Millisecond)

	// Close connection
	closeErr := conn.Close()

	// Stop preparers
	close(stopPrepare)
	wg.Wait()

	if closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}
}

// TestConcurrentStatementClose tests concurrent statement closing
func TestConcurrentStatementClose(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create test table
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare multiple statements
	const numStmts = 10
	stmts := make([]*sql.Stmt, numStmts)
	for i := 0; i < numStmts; i++ {
		stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		stmts[i] = stmt
	}

	// Close all statements concurrently
	var wg sync.WaitGroup
	for _, stmt := range stmts {
		wg.Add(1)
		go func(s *sql.Stmt) {
			defer wg.Done()
			if err := s.Close(); err != nil {
				t.Errorf("Failed to close statement: %v", err)
			}
		}(stmt)
	}

	wg.Wait()
}

// TestConnectionCloseIdempotency tests that Close() is idempotent at the driver level
func TestConnectionCloseIdempotency(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Close multiple times
	// Note: database/sql wraps our driver and only the first Close() succeeds
	// Subsequent calls return "sql: connection is already closed"
	// This test verifies that our driver's Close() is idempotent internally
	var firstCloseErr error
	var subsequentCloseErrs int

	for i := 0; i < 5; i++ {
		err := conn.Close()
		if i == 0 {
			firstCloseErr = err
		} else if err != nil {
			if err.Error() == "sql: connection is already closed" {
				subsequentCloseErrs++
			} else {
				t.Errorf("Unexpected error on close %d: %v", i+1, err)
			}
		}
	}

	if firstCloseErr != nil {
		t.Errorf("First Close() failed: %v", firstCloseErr)
	}

	// All subsequent closes should report "already closed"
	if subsequentCloseErrs != 4 {
		t.Errorf("Expected 4 subsequent closes to report already closed, got %d", subsequentCloseErrs)
	}
}

// TestStressTest performs stress testing with many concurrent operations
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create test table
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const (
		numWorkers   = 10
		opsPerWorker = 100
	)

	var wg sync.WaitGroup
	var successOps int64
	var failedOps int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWorker; i++ {
				conn, err := db.Conn(context.Background())
				if err != nil {
					atomic.AddInt64(&failedOps, 1)
					continue
				}

				// Perform some operations
				stmt, err := db.Prepare("INSERT INTO test (id, value) VALUES (?, ?)")
				if err == nil {
					stmt.Close()
				}

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}

				// Close connection
				conn.Close()
			}
		}(w)
	}

	wg.Wait()

	t.Logf("Stress test completed: %d successful ops, %d failed ops", successOps, failedOps)

	// At least some operations should succeed
	if successOps == 0 {
		t.Error("No operations succeeded in stress test")
	}
}

// setupConnTestDB creates a temporary test database
func setupConnTestDB(t *testing.T) (*sql.DB, func()) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dbPath := tmpfile.Name()

	// Open database
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		os.Remove(dbPath)
		t.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	return db, cleanup
}

// TestRaceConditionInClose uses the race detector to catch race conditions
func TestRaceConditionInClose(t *testing.T) {
	// This test is most effective when run with: go test -race
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create table and insert data
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err := db.Exec("INSERT INTO test (id, value) VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Get connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	var wg sync.WaitGroup

	// Start readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Execute a query
			var id int
			var value string
			conn.QueryRowContext(context.Background(), "SELECT id, value FROM test LIMIT 1").Scan(&id, &value)
		}()
	}

	// Start closers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.Close()
		}()
	}

	wg.Wait()
}
