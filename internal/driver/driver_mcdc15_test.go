// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestMCDC15_RemoveCollation_NilRegistry covers the collRegistry == nil branch.
func TestMCDC15_RemoveCollation_NilRegistry(t *testing.T) {
	t.Parallel()
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	// Force registry to nil so the nil-registry path is taken.
	c.collRegistry = nil

	err = c.RemoveCollation("MY_COLL")
	if err == nil {
		t.Fatal("expected error when collRegistry is nil, got nil")
	}
}

// TestMCDC15_RemoveCollation_ClosedConn covers the closed-connection branch.
func TestMCDC15_RemoveCollation_ClosedConn(t *testing.T) {
	t.Parallel()
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	conn.Close()

	c := conn.(*Conn)
	err = c.RemoveCollation("MY_COLL")
	if err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got %v", err)
	}
}

// TestMCDC15_RemoveCollation_Registered covers the success path of RemoveCollation.
func TestMCDC15_RemoveCollation_Registered(t *testing.T) {
	t.Parallel()
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	collFn := func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}
	if err := c.CreateCollation("TEST_COLL", collFn); err != nil {
		t.Fatalf("CreateCollation: %v", err)
	}
	if err := c.RemoveCollation("TEST_COLL"); err != nil {
		t.Fatalf("RemoveCollation: %v", err)
	}
}

// TestMCDC15_EvictLRU_EmptyList covers the lruList.Len() == 0 guard in evictLRU.
func TestMCDC15_EvictLRU_EmptyList(t *testing.T) {
	t.Parallel()
	cache := NewStmtCache(3)
	// Call SetCapacity to 0 entries when list is empty; evictLRU is called but
	// the list is empty, so it should return without panic.
	cache.SetCapacity(1)
	if cache.Size() != 0 {
		t.Fatalf("expected empty cache, got %d entries", cache.Size())
	}
}

// TestMCDC15_EvictLRU_FillBeyondCapacity fills the cache beyond capacity and forces
// LRU eviction, exercising the main eviction path and element-removal logic.
func TestMCDC15_EvictLRU_FillBeyondCapacity(t *testing.T) {
	t.Parallel()
	const cap = 3
	cache := NewStmtCache(cap)

	for i := 0; i < cap+2; i++ {
		sql := "SELECT " + string(rune('0'+i))
		program := vdbe.New()
		cache.Put(sql, program)
		program.Finalize()
	}

	if cache.Size() > cap {
		t.Fatalf("cache size %d exceeds capacity %d after fill", cache.Size(), cap)
	}
}

// TestMCDC15_EvictLRU_SetCapacitySmaller triggers evictLRU via SetCapacity shrinkage.
func TestMCDC15_EvictLRU_SetCapacitySmaller(t *testing.T) {
	t.Parallel()
	cache := NewStmtCache(5)

	for i := 0; i < 5; i++ {
		sql := "SELECT " + string(rune('A'+i))
		program := vdbe.New()
		cache.Put(sql, program)
		program.Finalize()
	}

	// Shrink to 2; this triggers multiple evictLRU calls.
	cache.SetCapacity(2)
	if cache.Size() > 2 {
		t.Fatalf("cache size %d should be <= 2 after SetCapacity(2)", cache.Size())
	}
}

// TestMCDC15_LoadInitialSchema_NewDB exercises the pager.PageCount() <= 1 branch
// in loadInitialSchema by creating a new in-memory database (page count starts at 0).
func TestMCDC15_LoadInitialSchema_NewDB(t *testing.T) {
	t.Parallel()
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open new memory DB: %v", err)
	}
	defer conn.Close()

	// If we got here, loadInitialSchema ran the btree.CreateTable() branch
	// (page count <= 1 for a fresh DB).
	c := conn.(*Conn)
	if c.schema == nil {
		t.Fatal("schema should not be nil after opening new DB")
	}
}
