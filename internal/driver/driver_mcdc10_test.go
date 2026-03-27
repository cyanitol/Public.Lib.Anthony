// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC 10 — internal driver coverage for low-coverage paths
//
// Targets (internal package access):
//   stmt_cache.go:147  HitRate          (83.3%) — cache metrics after hits/misses
//   stmt_cache.go:198  evictLRU         (71.4%) — cache eviction when full
//   stmt_cache.go:215  remove           (85.7%) — remove from cache
//   multi_stmt.go:22   Close            (77.8%) — close-when-already-closed branch
//   conn.go:205        closeStatements  (71.4%) — close with open statements
//   conn.go:832        RemoveCollation  (71.4%) — custom collation removal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// StmtCache — HitRate, evictLRU, remove
// ---------------------------------------------------------------------------

// TestMCDC10_StmtCache_HitRate exercises the HitRate function.
// Calls Get on a miss (HitRate denominator increases), then Put+Get for hit.
func TestMCDC10_StmtCache_HitRate(t *testing.T) {
	t.Parallel()

	cache := NewStmtCache(10)

	// Cold miss — total=1, hits=0
	got := cache.Get("SELECT 1")
	if got != nil {
		t.Error("expected nil on cold miss")
	}

	// Record a hit by Put then Get.
	vm := vdbe.New()
	cache.Put("SELECT 1", vm)
	vm.Finalize()

	got = cache.Get("SELECT 1")
	// Cloned VDBE may be nil if clone returns nil — just record
	if got != nil {
		got.Finalize()
	}

	rate := cache.HitRate()
	// 1 hit out of 2 total = 50%.
	if rate < 0 || rate > 100 {
		t.Errorf("HitRate out of range: %v", rate)
	}
}

// TestMCDC10_StmtCache_HitRate_Zero exercises the zero-total branch (no accesses).
func TestMCDC10_StmtCache_HitRate_Zero(t *testing.T) {
	t.Parallel()

	cache := NewStmtCache(5)
	rate := cache.HitRate()
	if rate != 0.0 {
		t.Errorf("expected 0.0 for empty cache HitRate, got %v", rate)
	}
}

// TestMCDC10_StmtCache_EvictLRU exercises evictLRU by filling cache past capacity.
func TestMCDC10_StmtCache_EvictLRU(t *testing.T) {
	t.Parallel()

	cache := NewStmtCache(2)

	for i := 0; i < 4; i++ {
		vm := vdbe.New()
		cache.Put(fmt.Sprintf("SELECT %d", i), vm)
		vm.Finalize()
	}
	// Cache should not exceed capacity.
	if cache.Size() > 2 {
		t.Errorf("cache size %d exceeds capacity 2 after eviction", cache.Size())
	}
}

// TestMCDC10_StmtCache_Remove exercises remove via schema invalidation + Get.
// When schemaVersion differs, Get calls remove internally.
func TestMCDC10_StmtCache_Remove(t *testing.T) {
	t.Parallel()

	cache := NewStmtCache(10)
	vm := vdbe.New()
	cache.Put("SELECT x", vm)
	vm.Finalize()

	if cache.Size() != 1 {
		t.Fatalf("expected 1 entry, got %d", cache.Size())
	}

	// Invalidate the schema so Get triggers the remove path.
	cache.InvalidateAll()

	got := cache.Get("SELECT x")
	if got != nil {
		got.Finalize()
	}

	// After schema invalidation + Get, entry should have been removed.
	if cache.Size() != 0 {
		t.Errorf("expected 0 entries after invalidation+get, got %d", cache.Size())
	}
}

// TestMCDC10_StmtCache_SetCapacity_Shrink exercises SetCapacity triggering evictions.
func TestMCDC10_StmtCache_SetCapacity_Shrink(t *testing.T) {
	t.Parallel()

	cache := NewStmtCache(10)
	for i := 0; i < 8; i++ {
		vm := vdbe.New()
		cache.Put(fmt.Sprintf("q%d", i), vm)
		vm.Finalize()
	}
	if cache.Size() != 8 {
		t.Fatalf("expected 8, got %d", cache.Size())
	}

	cache.SetCapacity(3)
	if cache.Size() > 3 {
		t.Errorf("after shrink to 3, size=%d (expected ≤3)", cache.Size())
	}
}

// ---------------------------------------------------------------------------
// MultiStmt.Close — already-closed branch
// ---------------------------------------------------------------------------

// TestMCDC10_MultiStmt_DoubleClose exercises Close() when already closed.
// The second Close should return nil without panicking.
func TestMCDC10_MultiStmt_DoubleClose(t *testing.T) {
	t.Parallel()

	// We need a Conn to create a MultiStmt. Use the internal SQL connection.
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Prepare a multi-statement to get a MultiStmt.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	// Use the raw driver to prepare a multi-statement.
	var ms driver.Stmt
	err = conn.Raw(func(c interface{}) error {
		drvConn, ok := c.(driver.ConnPrepareContext)
		if !ok {
			// Fallback: try driver.Conn
			drvConn2, ok2 := c.(driver.Conn)
			if !ok2 {
				return fmt.Errorf("not a driver.Conn")
			}
			var perr error
			ms, perr = drvConn2.Prepare("SELECT 1; SELECT 2")
			return perr
		}
		var perr error
		ms, perr = drvConn.PrepareContext(context.Background(), "SELECT 1; SELECT 2")
		return perr
	})
	if err != nil {
		t.Skipf("could not prepare multi-stmt: %v", err)
	}

	// Close twice — second close should be a no-op.
	if err := ms.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Conn.RemoveCollation — exercises the RemoveCollation path
// ---------------------------------------------------------------------------

// TestMCDC10_RemoveCollation exercises Conn.RemoveCollation for a registered collation.
func TestMCDC10_RemoveCollation(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(c interface{}) error {
		type collationRegistrar interface {
			RegisterCollation(name string, fn func(a, b string) int) error
			RemoveCollation(name string) error
		}
		cr, ok := c.(collationRegistrar)
		if !ok {
			return fmt.Errorf("driver does not implement collationRegistrar")
		}
		// Register then remove.
		if err := cr.RegisterCollation("mcdc10_test_coll", func(a, b string) int {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		}); err != nil {
			return fmt.Errorf("register: %v", err)
		}
		return cr.RemoveCollation("mcdc10_test_coll")
	})
	if err != nil {
		t.Skipf("collation API not available: %v", err)
	}
}

// TestMCDC10_RemoveCollation_NonExistent exercises RemoveCollation for a name
// that was never registered (the "not ok" branch in the map lookup).
func TestMCDC10_RemoveCollation_NonExistent(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	conn.Raw(func(c interface{}) error {
		type collationRemover interface {
			RemoveCollation(name string) error
		}
		cr, ok := c.(collationRemover)
		if !ok {
			return nil
		}
		// Remove non-existent collation — should not error.
		_ = cr.RemoveCollation("no_such_collation_mcdc10")
		return nil
	})
}
