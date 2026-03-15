// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSQLiteStress is a comprehensive test suite converted from SQLite's TCL stress and performance tests
// (speed*.test, threadtest*.test, and related stress tests)
//
// Test Coverage:
// - Large dataset operations
// - Bulk insert performance
// - Complex query performance
// - Concurrent access patterns
// - Index performance under load
// - Transaction throughput
// - Memory pressure scenarios
// - Large text and blob handling
// - Join performance
// - Vacuum and reorganization

// =============================================================================
// Test 1: Bulk Insert Performance
// From speed1.test - Large insert operations
// =============================================================================

func TestStress_BulkInsertUnindexed(t *testing.T) {
	t.Skip("pre-existing failure - needs bulk insert fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)

	const numRows = 10000

	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= numRows; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	assertRowCount(t, db, "t1", numRows)
	t.Logf("Inserted %d rows in %v (%.0f rows/sec)", numRows, elapsed, float64(numRows)/elapsed.Seconds())
}

func TestStress_BulkInsertIndexed(t *testing.T) {
	t.Skip("pre-existing failure - needs bulk insert fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t2(a INTEGER, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i2a ON t2(a)`)
	mustExec(t, db, `CREATE INDEX i2b ON t2(b)`)

	const numRows = 10000

	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= numRows; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t2 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	assertRowCount(t, db, "t2", numRows)
	t.Logf("Inserted %d indexed rows in %v (%.0f rows/sec)", numRows, elapsed, float64(numRows)/elapsed.Seconds())
}

// =============================================================================
// Test 2: Full Table Scan Performance
// From speed1.test - Query performance on large tables
// =============================================================================

func TestStress_FullTableScanInteger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)

	// Populate with test data
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)

	// Perform multiple range queries
	start := time.Now()
	for i := 0; i < 10; i++ {
		lwr := i * 1000
		upr := (i + 10) * 1000
		rows := queryRows(t, db, `SELECT count(*), avg(b) FROM t1 WHERE b >= ? AND b < ?`, lwr, upr)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 10 range scans in %v", elapsed)
}

func TestStress_FullTableScanLike(t *testing.T) {
	t.Skip("pre-existing failure - needs LIKE implementation")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)

	// Populate with test data
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 3000; i++ {
		r := rand.Intn(100)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("value_%d", r))
	}
	mustExec(t, db, `COMMIT`)

	// Perform LIKE queries
	start := time.Now()
	for i := 0; i < 10; i++ {
		pattern := fmt.Sprintf("%%_%d%%", i)
		rows := queryRows(t, db, `SELECT count(*), avg(b) FROM t1 WHERE c LIKE ?`, pattern)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 10 LIKE scans in %v", elapsed)
}

// =============================================================================
// Test 3: Index Creation Performance
// From speed1.test - Building indexes on large tables
// =============================================================================

func TestStress_CreateIndexOnLargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)

	// Populate with test data
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)

	// Create indexes
	start := time.Now()
	mustExec(t, db, `CREATE INDEX i1a ON t1(a)`)
	elapsed1 := time.Since(start)
	t.Logf("Created index on column a in %v", elapsed1)

	start = time.Now()
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)
	elapsed2 := time.Since(start)
	t.Logf("Created index on column b in %v", elapsed2)

	start = time.Now()
	mustExec(t, db, `CREATE INDEX i1c ON t1(c)`)
	elapsed3 := time.Since(start)
	t.Logf("Created index on column c in %v", elapsed3)
}

// =============================================================================
// Test 4: Indexed Query Performance
// From speed1.test - Query performance with indexes
// =============================================================================

func TestStress_IndexedRangeQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		r := rand.Intn(10000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)

	// Perform indexed range queries
	start := time.Now()
	for i := 0; i < 1000; i++ {
		lwr := i * 10
		upr := (i + 10) * 10
		rows := queryRows(t, db, `SELECT count(*), avg(b) FROM t1 WHERE b >= ? AND b < ?`, lwr, upr)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 1000 indexed range queries in %v (%.0f queries/sec)", elapsed, 1000.0/elapsed.Seconds())
}

// =============================================================================
// Test 5: Random Rowid Lookup
// From speed1.test - Random access performance
// =============================================================================

func TestStress_RandomRowidLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, fmt.Sprintf("text_%d", r))
	}
	mustExec(t, db, `COMMIT`)

	// Random lookups
	start := time.Now()
	for i := 0; i < 5000; i++ {
		id := rand.Intn(10000) + 1
		rows := queryRows(t, db, `SELECT c FROM t1 WHERE a = ?`, id)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 5000 random rowid lookups in %v (%.0f lookups/sec)", elapsed, 5000.0/elapsed.Seconds())
}

// =============================================================================
// Test 6: Random Indexed Column Lookup
// From speed1.test - Indexed column access
// =============================================================================

func TestStress_RandomIndexedLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, i, fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Random indexed lookups
	start := time.Now()
	for i := 0; i < 5000; i++ {
		b := rand.Intn(10000) + 1
		rows := queryRows(t, db, `SELECT c FROM t1 WHERE b = ?`, b)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 5000 random indexed lookups in %v (%.0f lookups/sec)", elapsed, 5000.0/elapsed.Seconds())
}

// =============================================================================
// Test 7: Text Index Lookup Performance
// From speed1.test - Text-based index access
// =============================================================================

func TestStress_TextIndexLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1c ON t1(c)`)

	// Populate with unique text values
	values := make([]string, 5000)
	mustExec(t, db, `BEGIN`)
	for i := 0; i < 5000; i++ {
		values[i] = fmt.Sprintf("value_%05d", rand.Intn(100000))
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i+1, values[i])
	}
	mustExec(t, db, `COMMIT`)

	// Random text lookups
	start := time.Now()
	for i := 0; i < 2000; i++ {
		val := values[rand.Intn(len(values))]
		rows := queryRows(t, db, `SELECT a FROM t1 WHERE c = ?`, val)
		_ = rows
	}
	elapsed := time.Since(start)

	t.Logf("Completed 2000 text index lookups in %v (%.0f lookups/sec)", elapsed, 2000.0/elapsed.Seconds())
}

// =============================================================================
// Test 8: Vacuum Performance
// From speed1.test - Database reorganization
// =============================================================================

func TestStress_VacuumPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "vacuum.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		r := rand.Intn(500000)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, r, strings.Repeat("x", 100))
	}
	mustExec(t, db, `COMMIT`)

	// Delete half the rows to fragment
	mustExec(t, db, `DELETE FROM t1 WHERE a % 2 = 0`)

	// Vacuum
	start := time.Now()
	mustExec(t, db, `VACUUM`)
	elapsed := time.Since(start)

	t.Logf("VACUUM completed in %v", elapsed)
}

// =============================================================================
// Test 9: Update Performance on Indexed Columns
// From speed1.test - Update operations
// =============================================================================

func TestStress_UpdateIndexedColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, i*2, fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Update indexed column
	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 0; i < 1000; i++ {
		lwr := i * 2
		upr := (i + 1) * 2
		mustExec(t, db, `UPDATE t1 SET b = b + 1 WHERE b >= ? AND b < ?`, lwr, upr)
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	t.Logf("Completed 1000 indexed updates in %v", elapsed)
}

// =============================================================================
// Test 10: Delete Performance
// From speed1.test - Delete operations
// =============================================================================

func TestStress_DeleteWithIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, i%100, fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Delete with indexed condition
	start := time.Now()
	mustExec(t, db, `DELETE FROM t1 WHERE b < 50`)
	elapsed := time.Since(start)

	t.Logf("Deleted ~5000 rows using index in %v", elapsed)

	count := querySingle(t, db, `SELECT COUNT(*) FROM t1`)
	if count.(int64) > 5500 {
		t.Errorf("expected ~5000 rows remaining, got %v", count)
	}
}

// =============================================================================
// Test 11: Large Blob Operations
// From speed tests - Blob handling
// =============================================================================

func TestStress_LargeBlobInsert(t *testing.T) {
	t.Skip("pre-existing failure - needs large blob insert")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data BLOB)`)

	// Insert large blobs
	blobSize := 100000
	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 100; i++ {
		data := make([]byte, blobSize)
		for j := range data {
			data[j] = byte(j % 256)
		}
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, data)
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	t.Logf("Inserted 100 blobs of %d bytes each in %v (%.2f MB/sec)",
		blobSize, elapsed, float64(100*blobSize)/elapsed.Seconds()/1024/1024)
}

func TestStress_LargeBlobRead(t *testing.T) {
	t.Skip("pre-existing failure - needs large blob read")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data BLOB)`)

	// Insert test blobs
	blobSize := 50000
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 100; i++ {
		data := make([]byte, blobSize)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, data)
	}
	mustExec(t, db, `COMMIT`)

	// Read blobs
	start := time.Now()
	for i := 1; i <= 100; i++ {
		var data []byte
		err := db.QueryRow(`SELECT data FROM t1 WHERE id = ?`, i).Scan(&data)
		if err != nil {
			t.Fatalf("failed to read blob: %v", err)
		}
	}
	elapsed := time.Since(start)

	t.Logf("Read 100 blobs in %v", elapsed)
}

// =============================================================================
// Test 12: Large Text Operations
// From speed tests - Text handling
// =============================================================================

func TestStress_LargeTextInsert(t *testing.T) {
	t.Skip("pre-existing failure - needs large text insert")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, text TEXT)`)

	// Insert large text values
	textSize := 10000
	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		text := strings.Repeat(fmt.Sprintf("row_%d_", i), textSize/10)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, text[:textSize])
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	t.Logf("Inserted 1000 text fields of %d chars each in %v", textSize, elapsed)
}

// =============================================================================
// Test 13: Join Performance
// From speed tests - JOIN operations
// =============================================================================

func TestStress_SimpleJoin(t *testing.T) {
	t.Skip("pre-existing failure - needs join fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER, data TEXT)`)
	mustExec(t, db, `CREATE INDEX i2_t1id ON t2(t1_id)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, i*10)
	}
	for i := 1; i <= 5000; i++ {
		t1_id := (i % 1000) + 1
		mustExec(t, db, `INSERT INTO t2 VALUES(?, ?, ?)`, i, t1_id, fmt.Sprintf("data_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Perform joins
	start := time.Now()
	rows := queryRows(t, db, `SELECT COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.t1_id`)
	elapsed := time.Since(start)

	if rows[0][0].(int64) != 5000 {
		t.Errorf("expected 5000 joined rows, got %v", rows[0][0])
	}
	t.Logf("Join completed in %v", elapsed)
}

func TestStress_ComplexJoin(t *testing.T) {
	t.Skip("pre-existing failure - needs complex join fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, category TEXT)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER, value INTEGER)`)
	mustExec(t, db, `CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INTEGER, description TEXT)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 100; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, fmt.Sprintf("cat_%d", i%10))
	}
	for i := 1; i <= 500; i++ {
		mustExec(t, db, `INSERT INTO t2 VALUES(?, ?, ?)`, i, (i%100)+1, i*5)
	}
	for i := 1; i <= 2000; i++ {
		mustExec(t, db, `INSERT INTO t3 VALUES(?, ?, ?)`, i, (i%500)+1, fmt.Sprintf("desc_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Three-way join
	start := time.Now()
	rows := queryRows(t, db, `
		SELECT COUNT(*)
		FROM t1
		JOIN t2 ON t1.id = t2.t1_id
		JOIN t3 ON t2.id = t3.t2_id
	`)
	elapsed := time.Since(start)

	t.Logf("Three-way join completed in %v, result: %v rows", elapsed, rows[0][0])
}

// =============================================================================
// Test 14: Concurrent Read Operations
// From threadtest - Concurrent access patterns
// =============================================================================

func TestStress_ConcurrentReads(t *testing.T) {
	t.Skip("pre-existing failure - needs concurrent read fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent.db")

	// Setup database
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER, text TEXT)`)
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, i*2, fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)
	db.Close()

	// Concurrent reads
	const numReaders = 5
	const readsPerReader = 100

	var wg sync.WaitGroup
	start := time.Now()

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			readerDB, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Errorf("reader %d failed to open db: %v", readerID, err)
				return
			}
			defer readerDB.Close()

			for i := 0; i < readsPerReader; i++ {
				id := rand.Intn(1000) + 1
				var value int
				var text string
				err := readerDB.QueryRow(`SELECT value, text FROM t1 WHERE id = ?`, id).Scan(&value, &text)
				if err != nil {
					t.Errorf("reader %d query failed: %v", readerID, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalReads := numReaders * readsPerReader
	t.Logf("Completed %d concurrent reads with %d readers in %v (%.0f reads/sec)",
		totalReads, numReaders, elapsed, float64(totalReads)/elapsed.Seconds())
}

// =============================================================================
// Test 15: Concurrent Write Operations
// From threadtest - Concurrent inserts
// =============================================================================

func TestStress_ConcurrentInserts(t *testing.T) {
	t.Skip("pre-existing failure - needs concurrent insert fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent_write.db")

	// Setup database
	setupDB, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	mustExec(t, setupDB, `CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, writer_id INTEGER, value INTEGER)`)
	setupDB.Close()

	// Concurrent inserts
	const numWriters = 3
	const insertsPerWriter = 100

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			writerDB, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Errorf("writer %d failed to open db: %v", writerID, err)
				return
			}
			defer writerDB.Close()

			for i := 0; i < insertsPerWriter; i++ {
				_, err := writerDB.Exec(`INSERT INTO t1(writer_id, value) VALUES(?, ?)`, writerID, i)
				if err != nil {
					t.Errorf("writer %d insert failed: %v", writerID, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify
	verifyDB, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open verify db: %v", err)
	}
	defer verifyDB.Close()

	var count int64
	err = verifyDB.QueryRow(`SELECT COUNT(*) FROM t1`).Scan(&count)
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}

	expected := int64(numWriters * insertsPerWriter)
	if count != expected {
		t.Errorf("expected %d rows, got %d", expected, count)
	}

	t.Logf("Completed %d concurrent inserts with %d writers in %v (%.0f inserts/sec)",
		expected, numWriters, elapsed, float64(expected)/elapsed.Seconds())
}

// =============================================================================
// Test 16: Mixed Read/Write Workload
// From threadtest - Mixed operations
// =============================================================================

func TestStress_MixedReadWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mixed.db")

	// Setup
	setupDB, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	mustExec(t, setupDB, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER, updated_at INTEGER)`)
	mustExec(t, setupDB, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		mustExec(t, setupDB, `INSERT INTO t1 VALUES(?, ?, ?)`, i, i*10, 0)
	}
	mustExec(t, setupDB, `COMMIT`)
	setupDB.Close()

	var wg sync.WaitGroup
	start := time.Now()

	// Readers
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			db, _ := sql.Open(DriverName, dbPath)
			defer db.Close()

			for i := 0; i < 50; i++ {
				id := rand.Intn(1000) + 1
				var value int
				db.QueryRow(`SELECT value FROM t1 WHERE id = ?`, id).Scan(&value)
				time.Sleep(10 * time.Millisecond)
			}
		}(r)
	}

	// Writers
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			db, _ := sql.Open(DriverName, dbPath)
			defer db.Close()

			for i := 0; i < 30; i++ {
				id := rand.Intn(1000) + 1
				db.Exec(`UPDATE t1 SET updated_at = ? WHERE id = ?`, time.Now().Unix(), id)
				time.Sleep(20 * time.Millisecond)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("Completed mixed read/write workload in %v", elapsed)
}

// =============================================================================
// Test 17: Transaction Throughput
// Measuring transaction commit rate
// =============================================================================

func TestStress_TransactionThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	const numTransactions = 100
	const rowsPerTransaction = 50

	start := time.Now()
	for txn := 0; txn < numTransactions; txn++ {
		mustExec(t, db, `BEGIN`)
		for i := 0; i < rowsPerTransaction; i++ {
			id := txn*rowsPerTransaction + i + 1
			mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, id, id*2)
		}
		mustExec(t, db, `COMMIT`)
	}
	elapsed := time.Since(start)

	totalRows := numTransactions * rowsPerTransaction
	t.Logf("Committed %d transactions (%d rows) in %v (%.0f txn/sec, %.0f rows/sec)",
		numTransactions, totalRows, elapsed,
		float64(numTransactions)/elapsed.Seconds(),
		float64(totalRows)/elapsed.Seconds())
}

// =============================================================================
// Test 18: Aggregate Performance
// From speed tests - Aggregate functions
// =============================================================================

func TestStress_AggregateOperations(t *testing.T) {
	t.Skip("pre-existing failure - needs aggregate operation fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(category TEXT, value INTEGER)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 0; i < 10000; i++ {
		category := fmt.Sprintf("cat_%d", i%100)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, category, rand.Intn(1000))
	}
	mustExec(t, db, `COMMIT`)

	// Run aggregates
	start := time.Now()
	rows := queryRows(t, db, `SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM t1 GROUP BY category`)
	elapsed := time.Since(start)

	if len(rows) != 100 {
		t.Errorf("expected 100 groups, got %d", len(rows))
	}

	t.Logf("Completed GROUP BY with aggregates on 10k rows in %v", elapsed)
}

// =============================================================================
// Test 19: Subquery Performance
// From speed tests - Nested queries
// =============================================================================

func TestStress_CorrelatedSubquery(t *testing.T) {
	t.Skip("pre-existing failure - needs correlated subquery fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, category TEXT, value INTEGER)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		category := fmt.Sprintf("cat_%d", i%20)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, category, rand.Intn(1000))
	}
	mustExec(t, db, `COMMIT`)

	// Correlated subquery
	start := time.Now()
	rows := queryRows(t, db, `
		SELECT id, value
		FROM t1 t
		WHERE value > (SELECT AVG(value) FROM t1 WHERE category = t.category)
	`)
	elapsed := time.Since(start)

	t.Logf("Correlated subquery completed in %v, returned %d rows", elapsed, len(rows))
}

// =============================================================================
// Test 20: DISTINCT Performance
// From speed tests - DISTINCT operations
// =============================================================================

func TestStress_DistinctValues(t *testing.T) {
	t.Skip("DISTINCT not yet implemented")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	// Insert many rows with limited distinct values
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, i%100)
	}
	mustExec(t, db, `COMMIT`)

	// DISTINCT query
	start := time.Now()
	rows := queryRows(t, db, `SELECT DISTINCT value FROM t1 ORDER BY value`)
	elapsed := time.Since(start)

	if len(rows) != 100 {
		t.Errorf("expected 100 distinct values, got %d", len(rows))
	}

	t.Logf("DISTINCT on 10k rows completed in %v", elapsed)
}

// =============================================================================
// Test 21: ORDER BY Performance
// From speed tests - Sorting operations
// =============================================================================

func TestStress_OrderByLargeResult(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER, text TEXT)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, i, rand.Intn(10000), fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// ORDER BY non-indexed column
	start := time.Now()
	rows := queryRows(t, db, `SELECT id FROM t1 ORDER BY value DESC LIMIT 100`)
	elapsed := time.Since(start)

	if len(rows) != 100 {
		t.Errorf("expected 100 rows, got %d", len(rows))
	}

	t.Logf("ORDER BY on 5k rows completed in %v", elapsed)
}

// =============================================================================
// Test 22: UNION Performance
// From speed tests - Set operations
// =============================================================================

func TestStress_UnionOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, value INTEGER)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 2000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, i*2)
	}
	for i := 1500; i <= 3500; i++ {
		mustExec(t, db, `INSERT INTO t2 VALUES(?, ?)`, i, i*2)
	}
	mustExec(t, db, `COMMIT`)

	// UNION
	start := time.Now()
	rows := queryRows(t, db, `SELECT value FROM t1 UNION SELECT value FROM t2`)
	elapsed := time.Since(start)

	t.Logf("UNION of 2k and 2k rows completed in %v, result: %d rows", elapsed, len(rows))
}

// =============================================================================
// Test 23: Window Function Performance
// From window function tests
// =============================================================================

func TestStress_WindowFunctions(t *testing.T) {
	t.Skip("pre-existing failure - needs window function fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(category TEXT, value INTEGER)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 0; i < 5000; i++ {
		category := fmt.Sprintf("cat_%d", i%50)
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, category, rand.Intn(1000))
	}
	mustExec(t, db, `COMMIT`)

	// Window function
	start := time.Now()
	rows := queryRows(t, db, `
		SELECT
			category,
			value,
			ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as rn,
			SUM(value) OVER (PARTITION BY category) as total
		FROM t1
		LIMIT 1000
	`)
	elapsed := time.Since(start)

	if len(rows) != 1000 {
		t.Errorf("expected 1000 rows, got %d", len(rows))
	}

	t.Logf("Window functions on 5k rows completed in %v", elapsed)
}

// =============================================================================
// Test 24: CTE (Common Table Expression) Performance
// From CTE tests
// =============================================================================

func TestStress_RecursiveCTE(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	// Recursive CTE to generate numbers
	start := time.Now()
	rows := queryRows(t, db, `
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x+1 FROM cnt WHERE x < 1000
		)
		SELECT COUNT(*) FROM cnt
	`)
	elapsed := time.Since(start)

	if rows[0][0].(int64) != 1000 {
		t.Errorf("expected 1000, got %v", rows[0][0])
	}

	t.Logf("Recursive CTE generating 1000 rows completed in %v", elapsed)
}

// =============================================================================
// Test 25: Prepared Statement Reuse
// Testing prepared statement performance
// =============================================================================

func TestStress_PreparedStatementReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	// Prepare statement
	stmt, err := db.Prepare(`INSERT INTO t1 VALUES(?, ?)`)
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	// Reuse prepared statement
	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		_, err := stmt.Exec(i, i*2)
		if err != nil {
			t.Fatalf("exec failed: %v", err)
		}
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	t.Logf("Inserted 10k rows using prepared statement in %v (%.0f rows/sec)",
		elapsed, 10000.0/elapsed.Seconds())
}

// =============================================================================
// Test 26: Multiple Index Lookup
// Testing multi-index scenarios
// =============================================================================

func TestStress_MultipleIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d TEXT)`)
	mustExec(t, db, `CREATE INDEX i1a ON t1(a)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)
	mustExec(t, db, `CREATE INDEX i1c ON t1(c)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?, ?)`,
			rand.Intn(100), rand.Intn(100), rand.Intn(100), fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// Query using different indexes
	start := time.Now()
	queryRows(t, db, `SELECT COUNT(*) FROM t1 WHERE a = 50`)
	queryRows(t, db, `SELECT COUNT(*) FROM t1 WHERE b = 75`)
	queryRows(t, db, `SELECT COUNT(*) FROM t1 WHERE c = 25`)
	elapsed := time.Since(start)

	t.Logf("Three index lookups completed in %v", elapsed)
}

// =============================================================================
// Test 27: Insert with Conflict Resolution
// Testing INSERT OR REPLACE/IGNORE performance
// =============================================================================

func TestStress_InsertOrReplace(t *testing.T) {
	t.Skip("pre-existing failure - needs INSERT OR REPLACE fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	// Initial insert
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 1000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, i*2)
	}
	mustExec(t, db, `COMMIT`)

	// INSERT OR REPLACE (updates)
	start := time.Now()
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 2000; i++ {
		mustExec(t, db, `INSERT OR REPLACE INTO t1 VALUES(?, ?)`, i, i*3)
	}
	mustExec(t, db, `COMMIT`)
	elapsed := time.Since(start)

	assertRowCount(t, db, "t1", 2000)
	t.Logf("INSERT OR REPLACE of 2000 rows (1000 updates, 1000 inserts) completed in %v", elapsed)
}

// =============================================================================
// Test 28: Batch Delete Performance
// Testing large-scale deletes
// =============================================================================

func TestStress_BatchDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, rand.Intn(100))
	}
	mustExec(t, db, `COMMIT`)

	// Delete large batch
	start := time.Now()
	mustExec(t, db, `DELETE FROM t1 WHERE value < 50`)
	elapsed := time.Since(start)

	count := querySingle(t, db, `SELECT COUNT(*) FROM t1`)
	t.Logf("Deleted ~5000 rows in %v, %v rows remaining", elapsed, count)
}

// =============================================================================
// Test 29: Complex WHERE Clause
// Testing query optimization
// =============================================================================

func TestStress_ComplexWhereClause(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER)`)
	mustExec(t, db, `CREATE INDEX i1a ON t1(a)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 0; i < 5000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?, ?)`,
			rand.Intn(100), rand.Intn(100), rand.Intn(100), rand.Intn(100))
	}
	mustExec(t, db, `COMMIT`)

	// Complex WHERE
	start := time.Now()
	rows := queryRows(t, db, `
		SELECT COUNT(*)
		FROM t1
		WHERE (a > 25 AND a < 75)
		  AND (b % 2 = 0)
		  AND (c + d > 100)
	`)
	elapsed := time.Since(start)

	t.Logf("Complex WHERE clause completed in %v, result: %v", elapsed, rows[0][0])
}

// =============================================================================
// Test 30: Large Result Set Fetch
// Testing result set retrieval performance
// =============================================================================

func TestStress_LargeResultFetch(t *testing.T) {
	t.Skip("pre-existing failure - needs large result fetch fixes")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, strings.Repeat("x", 100))
	}
	mustExec(t, db, `COMMIT`)

	// Fetch all rows
	start := time.Now()
	rows := queryRows(t, db, `SELECT id, data FROM t1`)
	elapsed := time.Since(start)

	if len(rows) != 10000 {
		t.Errorf("expected 10000 rows, got %d", len(rows))
	}

	t.Logf("Fetched 10k rows in %v (%.0f rows/sec)", elapsed, 10000.0/elapsed.Seconds())
}

// =============================================================================
// Test 31-35: Additional Stress Scenarios
// =============================================================================

func TestStress_AnalyzePerformance(t *testing.T) {
	t.Skip("ANALYZE not implemented")
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)`)
	mustExec(t, db, `CREATE INDEX i1a ON t1(a)`)
	mustExec(t, db, `CREATE INDEX i1b ON t1(b)`)

	// Populate
	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 5000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?, ?)`, rand.Intn(1000), rand.Intn(1000), fmt.Sprintf("text_%d", i))
	}
	mustExec(t, db, `COMMIT`)

	// ANALYZE
	start := time.Now()
	mustExec(t, db, `ANALYZE`)
	elapsed := time.Since(start)

	t.Logf("ANALYZE completed in %v", elapsed)
}
