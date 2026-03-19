// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// TestStmtCacheBasic tests basic statement caching functionality.
func TestStmtCacheBasic(t *testing.T) {
	db, dbPath := stmtCacheCreateDB(t)
	defer os.Remove(dbPath)
	defer db.Close()

	stmtCacheExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	stmtCacheExec(t, db, "INSERT INTO users VALUES (1, 'Alice')")

	conn := stmtCacheGetConn(t, db, dbPath)

	// Reset metrics after setup (CREATE TABLE and INSERT generate cache misses)
	conn.stmtCache.ResetMetrics()

	hits, misses := conn.stmtCache.GetMetrics()
	if hits != 0 || misses != 0 {
		t.Errorf("Expected empty cache, got hits=%d, misses=%d", hits, misses)
	}

	query := "SELECT * FROM users WHERE id = 1"
	stmtCacheRunQuery(t, db, query)
	stmtCacheAssertMetrics(t, conn, 0, 1)

	stmtCacheRunQuery(t, db, query)
	stmtCacheAssertMetrics(t, conn, 1, 1)

	stmtCacheRunQuery(t, db, query)
	stmtCacheAssertMetrics(t, conn, 2, 1)

	hitRate := conn.stmtCache.HitRate()
	expectedRate := float64(2) * 100.0 / float64(3)
	if hitRate < expectedRate-0.1 || hitRate > expectedRate+0.1 {
		t.Errorf("Expected hit rate ~%.2f%%, got %.2f%%", expectedRate, hitRate)
	}
}

func stmtCacheCreateDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test_cache_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	dbPath := tmpFile.Name()
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db, dbPath
}

func stmtCacheExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec failed: %v", err)
	}
}

func stmtCacheGetConn(t *testing.T, db *sql.DB, dbPath string) *Conn {
	t.Helper()
	drv := db.Driver().(*Driver)
	drv.mu.Lock()
	conn, ok := drv.conns[dbPath]
	drv.mu.Unlock()
	if !ok {
		t.Fatal("Failed to get connection")
	}
	return conn
}

func stmtCacheRunQuery(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	rows.Close()
}

func stmtCacheAssertMetrics(t *testing.T, conn *Conn, wantHits, wantMisses uint64) {
	t.Helper()
	hits, misses := conn.stmtCache.GetMetrics()
	if hits != wantHits {
		t.Errorf("Expected %d hits, got %d", wantHits, hits)
	}
	if misses != wantMisses {
		t.Errorf("Expected %d misses, got %d", wantMisses, misses)
	}
}

// TestStmtCacheInvalidation tests cache invalidation on schema changes.
func TestStmtCacheInvalidation(t *testing.T) {
	db, dbPath := stmtCacheCreateDB(t)
	defer os.Remove(dbPath)
	defer db.Close()

	stmtCacheExec(t, db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
	stmtCacheExec(t, db, "INSERT INTO products VALUES (1, 'Widget')")

	conn := stmtCacheGetConn(t, db, dbPath)

	// Reset metrics after setup
	conn.stmtCache.ResetMetrics()

	query := "SELECT * FROM products"
	stmtCacheRunQuery(t, db, query)
	stmtCacheAssertMetrics(t, conn, 0, 1)

	stmtCacheExec(t, db, "CREATE INDEX idx_products_name ON products(name)")
	stmtCacheRunQuery(t, db, query)
	// Misses: 1 (initial SELECT) + 1 (CREATE INDEX DDL) + 1 (SELECT after schema change) = 3
	stmtCacheAssertMetrics(t, conn, 0, 3)
}

// TestStmtCacheLRU tests LRU eviction.
func TestStmtCacheLRU(t *testing.T) {
	db, dbPath := stmtCacheCreateDB(t)
	defer os.Remove(dbPath)
	defer db.Close()

	stmtCacheExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	conn := stmtCacheGetConn(t, db, dbPath)
	conn.stmtCache.SetCapacity(3)

	// Reset metrics after setup
	conn.stmtCache.ResetMetrics()

	queries := []string{
		"SELECT * FROM items WHERE id = 1",
		"SELECT * FROM items WHERE id = 2",
		"SELECT * FROM items WHERE id = 3",
		"SELECT * FROM items WHERE id = 4",
	}
	for _, q := range queries {
		stmtCacheRunQuery(t, db, q)
	}

	size := conn.stmtCache.Size()
	if size > 3 {
		t.Errorf("Expected cache size <= 3, got %d", size)
	}

	stmtCacheRunQuery(t, db, queries[0])
	_, misses := conn.stmtCache.GetMetrics()
	if misses != 5 {
		t.Errorf("Expected 5 misses, got %d", misses)
	}
}

// TestStmtCacheParameterized tests that parameterized queries are not cached.
func TestStmtCacheParameterized(t *testing.T) {
	// Create temporary database file
	tmpFile, err := os.CreateTemp("", "test_cache_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	dbPath := tmpFile.Name()
	defer os.Remove(dbPath)

	// Create database
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get the connection
	// Get the connection and check cache stats
	drv := db.Driver().(*Driver)
	drv.mu.Lock()
	conn, ok := drv.conns[dbPath]
	drv.mu.Unlock()
	if !ok {
		t.Fatal("Failed to get connection")
	}

	// Execute parameterized query (should not be cached)
	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	rows.Close()

	rows, err = stmt.Query(2)
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}
	rows.Close()

	// Cache should not have any hits (parameterized queries aren't cached)
	hits, _ := conn.stmtCache.GetMetrics()
	if hits != 0 {
		t.Errorf("Expected 0 hits for parameterized queries, got %d", hits)
	}
}

// TestStmtCacheThreadSafety tests concurrent access to the cache.
func TestStmtCacheThreadSafety(t *testing.T) {
	db, dbPath := stmtCacheCreateDB(t)
	defer os.Remove(dbPath)
	defer db.Close()

	stmtCacheExec(t, db, "CREATE TABLE concurrent (id INTEGER PRIMARY KEY, data TEXT)")

	// Get conn reference and reset metrics before concurrent access
	conn := stmtCacheGetConn(t, db, dbPath)
	conn.stmtCache.ResetMetrics()

	const numGoroutines = 10
	const queriesPerGoroutine = 100

	errChan := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			query := "SELECT * FROM concurrent WHERE id = 1"
			for j := 0; j < queriesPerGoroutine; j++ {
				rows, err := db.Query(query)
				if err != nil {
					errChan <- err
					return
				}
				rows.Close()
			}
			errChan <- nil
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Goroutine error: %v", err)
		}
	}

	hits, misses := conn.stmtCache.GetMetrics()
	total := hits + misses
	if total == 0 {
		t.Errorf("Expected cache accesses, got 0")
	}
	// With concurrent access, at least some queries should hit the cache
	if hits == 0 {
		t.Errorf("Expected some cache hits, got 0 out of %d total", total)
	}
}
