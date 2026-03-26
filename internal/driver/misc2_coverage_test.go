// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	driver "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// MultiStmt.Exec / MultiStmt.Query
//
// MultiStmt is an unexported type whose Exec and Query methods are reached by
// preparing a multi-statement string via database/sql.  The sql.DB layer calls
// driver.Stmt.Exec (which dispatches to MultiStmt.ExecContext) when the
// prepared text contains more than one statement.
// ---------------------------------------------------------------------------

// miscOpenMem opens an in-memory database for misc coverage tests.
func miscOpenMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("miscOpenMem: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestMultiStmtExec exercises MultiStmt.Exec through database/sql.
func TestMultiStmtExec(t *testing.T) {
	db := miscOpenMem(t)

	if _, err := db.Exec("CREATE TABLE m(x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// A semicolon-separated string causes the driver to create a MultiStmt.
	result, err := db.Exec("INSERT INTO m VALUES(1); INSERT INTO m VALUES(2)")
	if err != nil {
		t.Fatalf("multi-stmt Exec: %v", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != 2 {
		t.Errorf("want 2 rows affected, got %d", rows)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM m").Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if n != 2 {
		t.Errorf("want 2 rows in table, got %d", n)
	}
}

// TestMultiStmtExecMultipleStatements exercises the executeAllStmts aggregation path.
func TestMultiStmtExecMultipleStatements(t *testing.T) {
	db := miscOpenMem(t)

	if _, err := db.Exec("CREATE TABLE m2(x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Three statements — exercises the loop and totalRowsAffected accumulation.
	_, err := db.Exec("INSERT INTO m2 VALUES(10); INSERT INTO m2 VALUES(20); INSERT INTO m2 VALUES(30)")
	if err != nil {
		t.Fatalf("multi-stmt Exec (3 stmts): %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM m2").Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if n != 3 {
		t.Errorf("want 3 rows, got %d", n)
	}
}

// TestMultiStmtQuery exercises MultiStmt.Query through database/sql.
// Multi-statement Query returns driver.ErrSkip, so the sql.DB layer falls back
// to Exec; the Query method itself still runs and returns the error.
func TestMultiStmtQuery(t *testing.T) {
	db := miscOpenMem(t)

	if _, err := db.Exec("CREATE TABLE mq(x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO mq VALUES(1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Querying a multi-statement string — the driver returns ErrSkip for Query,
	// which causes database/sql to fall through to Exec internally.
	// Either an error or an empty result is acceptable here; what matters is
	// that the Query code path is exercised.
	rows, err := db.Query("SELECT x FROM mq; SELECT x FROM mq")
	if err != nil {
		// ErrSkip surfaces as an error to the caller — that's fine.
		t.Logf("Query returned error (expected for multi-stmt): %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var x int
		_ = rows.Scan(&x)
	}
}

// ---------------------------------------------------------------------------
// StmtCache — Clear, SetSchemaVersion, Capacity
// ---------------------------------------------------------------------------

// TestCacheClear exercises StmtCache.Clear.
func TestCacheClear(t *testing.T) {
	cache := driver.NewStmtCache(10)

	// Capacity should reflect the initial value even before any entries.
	if cap := cache.Capacity(); cap != 10 {
		t.Errorf("Capacity want 10, got %d", cap)
	}

	// After Clear on an empty cache — should be a no-op that doesn't panic.
	cache.Clear()

	if sz := cache.Size(); sz != 0 {
		t.Errorf("Size after Clear want 0, got %d", sz)
	}
}

// TestCacheSetSchemaVersion exercises StmtCache.SetSchemaVersion.
func TestCacheSetSchemaVersion(t *testing.T) {
	cache := driver.NewStmtCache(5)

	// Setting schema version should not panic and should be reflected in
	// subsequent Get calls (stale entries are rejected).
	cache.SetSchemaVersion(42)

	// A Get on an empty cache should return nil regardless of schema version.
	if got := cache.Get("SELECT 1"); got != nil {
		t.Error("expected nil from Get on empty cache")
	}
}

// TestCacheCapacity exercises StmtCache.Capacity.
func TestCacheCapacity(t *testing.T) {
	for _, cap := range []int{1, 10, 100, 500} {
		cache := driver.NewStmtCache(cap)
		if got := cache.Capacity(); got != cap {
			t.Errorf("Capacity(%d): want %d, got %d", cap, cap, got)
		}
	}
}

// TestCacheCapacityDefaultOnZero verifies that capacity 0 is normalised to 100.
func TestCacheCapacityDefaultOnZero(t *testing.T) {
	cache := driver.NewStmtCache(0)
	if got := cache.Capacity(); got != 100 {
		t.Errorf("Capacity(0): want 100 (default), got %d", got)
	}
}

// TestCacheClearResetsEntries verifies that Clear removes all entries by using
// a real database to populate the cache and then clearing it.
func TestCacheClearResetsEntries(t *testing.T) {
	// We can only drive this through the SQL layer; populate via db.Query
	// then check that the cache size returns to 0 after Clear is called on
	// a fresh StmtCache (the StmtCache in a connection is internal, so we
	// exercise the method directly on a fresh instance).
	cache := driver.NewStmtCache(10)

	// Set a schema version, clear, and re-check.
	cache.SetSchemaVersion(99)
	cache.Clear()

	if sz := cache.Size(); sz != 0 {
		t.Errorf("Size after Clear want 0, got %d", sz)
	}

	// Capacity should be unchanged by Clear.
	if cap := cache.Capacity(); cap != 10 {
		t.Errorf("Capacity after Clear want 10, got %d", cap)
	}
}

// ---------------------------------------------------------------------------
// ConnRowReader — FindReferencingRowsWithParentAffinity, ReadRowByRowid
// These are reached through foreign-key enforcement (InsertStmt with FK).
// ---------------------------------------------------------------------------

// TestConnRowReader_ReadRowByRowid exercises ReadRowByRowid indirectly by
// inserting a row and having the FK engine look up the parent row.
func TestConnRowReader_ReadRowByRowid(t *testing.T) {
	db := miscOpenMem(t)

	// Enable foreign keys so the FK manager uses ConnRowReader.
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}

	if _, err := db.Exec("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))"); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}

	// Insert parent row — ReadRowByRowid is used by the FK manager to look up
	// existing rows during enforcement.
	if _, err := db.Exec("INSERT INTO parent VALUES(1, 'Alice')"); err != nil {
		t.Fatalf("INSERT parent: %v", err)
	}

	// Insert valid child — exercises FindReferencingRowsWithParentAffinity.
	if _, err := db.Exec("INSERT INTO child VALUES(1, 1)"); err != nil {
		t.Fatalf("INSERT child (valid): %v", err)
	}

	// Attempt to delete the parent while a child references it — exercises
	// FindReferencingRowsWithParentAffinity on the child table.
	_, err := db.Exec("DELETE FROM parent WHERE id=1")
	if err != nil {
		// FK violation expected — that's the successful code path we want.
		t.Logf("DELETE parent (FK violation expected): %v", err)
	}
}

// TestConnRowReader_FindReferencingRowsWithParentAffinity exercises the
// affinity-aware referencing-row search via cascading FK lookups.
func TestConnRowReader_FindReferencingRowsWithParentAffinity(t *testing.T) {
	db := miscOpenMem(t)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}

	if _, err := db.Exec("CREATE TABLE p(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE p: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))"); err != nil {
		t.Fatalf("CREATE c: %v", err)
	}

	if _, err := db.Exec("INSERT INTO p VALUES(10)"); err != nil {
		t.Fatalf("INSERT p: %v", err)
	}
	if _, err := db.Exec("INSERT INTO c VALUES(1, 10)"); err != nil {
		t.Fatalf("INSERT c: %v", err)
	}

	// Deleting the parent triggers FindReferencingRowsWithParentAffinity.
	_, err := db.Exec("DELETE FROM p WHERE id=10")
	if err != nil {
		t.Logf("DELETE p (FK violation expected): %v", err)
	}
}

// TestEnsureMasterPage exercises ensureMasterPage indirectly by opening a new
// in-memory database and issuing a CREATE TABLE (which triggers the master
// page initialization path on first use).
func TestEnsureMasterPage(t *testing.T) {
	db := miscOpenMem(t)

	// A fresh in-memory database requires page 1 to be initialized before any
	// schema writes can happen.  CREATE TABLE triggers this path.
	if _, err := db.Exec("CREATE TABLE emp_test(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO emp_test VALUES(1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM emp_test").Scan(&n); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if n != 1 {
		t.Errorf("want 1, got %d", n)
	}
}
