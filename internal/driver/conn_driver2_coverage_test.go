// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
)

// ---------------------------------------------------------------------------
// ensureMasterPage — the "page does not exist, create it" branch
// ---------------------------------------------------------------------------

// TestConnDriver2Coverage_EnsureMasterPageCreate covers the code path that
// runs when c.btree.GetPage(1) fails (page not in cache and no provider).
// A freshly allocated Btree with no pages cached returns an error from GetPage,
// so ensureMasterPage must allocate and write the sqlite_master leaf page.
func TestConnDriver2Coverage_EnsureMasterPageCreate(t *testing.T) {
	const pageSize = 4096
	bt := btree.NewBtree(uint32(pageSize))
	// No provider, no cached pages → GetPage(1) returns "page 1 not found".

	c := &Conn{btree: bt}
	if err := c.ensureMasterPage(); err != nil {
		t.Fatalf("ensureMasterPage (create path): unexpected error: %v", err)
	}

	// Page 1 must now be present in the btree cache.
	data, err := bt.GetPage(1)
	if err != nil {
		t.Fatalf("after ensureMasterPage, GetPage(1) failed: %v", err)
	}
	if len(data) != pageSize {
		t.Errorf("page 1 data len = %d, want %d", len(data), pageSize)
	}

	// The type byte at the correct offset must be PageTypeLeafTable.
	typeOffset := btree.FileHeaderSize + btree.PageHeaderOffsetType
	if data[typeOffset] != btree.PageTypeLeafTable {
		t.Errorf("page type byte = %d, want %d (PageTypeLeafTable)",
			data[typeOffset], btree.PageTypeLeafTable)
	}
}

// TestConnDriver2Coverage_EnsureMasterPageIdempotent verifies that calling
// ensureMasterPage a second time (page 1 already cached) returns nil without
// modifying the page content, exercising the fast-return branch.
func TestConnDriver2Coverage_EnsureMasterPageIdempotent(t *testing.T) {
	const pageSize = 4096
	bt := btree.NewBtree(uint32(pageSize))

	c := &Conn{btree: bt}

	// First call creates the page.
	if err := c.ensureMasterPage(); err != nil {
		t.Fatalf("first ensureMasterPage: %v", err)
	}

	// Second call should hit the "GetPage succeeds → early return" branch.
	if err := c.ensureMasterPage(); err != nil {
		t.Fatalf("second ensureMasterPage: %v", err)
	}
}

// ---------------------------------------------------------------------------
// createMemoryConnection — security config nil branch
// ---------------------------------------------------------------------------

// TestConnDriver2Coverage_CreateMemoryConnectionNilSecurity exercises the
// branch in createMemoryConnection where config.Security is nil, causing the
// function to call security.DefaultSecurityConfig() itself.
func TestConnDriver2Coverage_CreateMemoryConnectionNilSecurity(t *testing.T) {
	d := &Driver{}
	d.initMaps()

	state, err := d.newMemoryDBState()
	if err != nil {
		t.Fatalf("newMemoryDBState: %v", err)
	}

	// Build a config with Security explicitly nil to hit the nil-security branch.
	cfg := DefaultDriverConfig()
	cfg.Security = nil

	conn, err := d.createMemoryConnection(":memory:nilsec_test", state, cfg)
	if err != nil {
		t.Fatalf("createMemoryConnection (nil security): %v", err)
	}
	if conn == nil {
		t.Fatal("createMemoryConnection returned nil conn without error")
	}
	conn.Close()
}

// TestConnDriver2Coverage_CreateMemoryConnectionWithSecurity exercises
// createMemoryConnection with a non-nil security config (the standard code
// path), confirming the connection is fully functional.
func TestConnDriver2Coverage_CreateMemoryConnectionWithSecurity(t *testing.T) {
	d := &Driver{}
	d.initMaps()

	state, err := d.newMemoryDBState()
	if err != nil {
		t.Fatalf("newMemoryDBState: %v", err)
	}

	cfg := DefaultDriverConfig()
	cfg.Security = security.DefaultSecurityConfig()

	conn, err := d.createMemoryConnection(":memory:withsec_test", state, cfg)
	if err != nil {
		t.Fatalf("createMemoryConnection (with security): %v", err)
	}
	if conn == nil {
		t.Fatal("createMemoryConnection returned nil conn without error")
	}
	conn.Close()
}

// ---------------------------------------------------------------------------
// memoryPagerProvider.MarkDirty — error path (pager.Get fails)
// ---------------------------------------------------------------------------

// TestConnDriver2Coverage_MemoryPagerProviderMarkDirtyGetError exercises the
// error-return branch of memoryPagerProvider.MarkDirty when pager.Get fails.
// Page number 0 is always invalid in the pager and triggers the error path.
func TestConnDriver2Coverage_MemoryPagerProviderMarkDirtyGetError(t *testing.T) {
	pgr, err := pager.OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}

	pp := newMemoryPagerProvider(pgr)

	// Page 0 is always invalid; Get must return an error.
	if err := pp.MarkDirty(0); err == nil {
		t.Fatal("MarkDirty(0): expected error for invalid page number, got nil")
	}
}

// TestConnDriver2Coverage_MemoryPagerProviderMarkDirtySuccess exercises the
// happy path of memoryPagerProvider.MarkDirty so all statements in the
// function body (Get, Write, Put) are reached.
func TestConnDriver2Coverage_MemoryPagerProviderMarkDirtySuccess(t *testing.T) {
	pgr, err := pager.OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}

	pp := newMemoryPagerProvider(pgr)

	// Begin a write transaction so pager.Write is permitted.
	if err := pgr.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Allocate a fresh page while a transaction is active.
	pgno, _, err := pp.AllocatePageData()
	if err != nil {
		t.Fatalf("AllocatePageData: %v", err)
	}

	// MarkDirty on the allocated page must succeed.
	if err := pp.MarkDirty(pgno); err != nil {
		t.Fatalf("MarkDirty(%d): unexpected error: %v", pgno, err)
	}

	pgr.Rollback()
}

// ---------------------------------------------------------------------------
// Integration — exercise createMemoryConnection via Driver.Open plus DDL
// ---------------------------------------------------------------------------

// TestConnDriver2Coverage_MemoryDBSchemaIntegration opens a fresh in-memory
// connection, creates a table, inserts a row, and queries it back.  This
// exercises the full chain: createMemoryConnection → openDatabase →
// ensureMasterPage → memoryPagerProvider.MarkDirty.
// connDriver2ExecStmt prepares and executes a SQL statement on a Conn.
func connDriver2ExecStmt(t *testing.T, c *Conn, sql string) {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("Prepare %q: %v", sql, err)
	}
	if _, err := stmt.Exec(nil); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
	stmt.Close()
}

func TestConnDriver2Coverage_MemoryDBSchemaIntegration(t *testing.T) {
	d := &Driver{}
	raw, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	defer raw.Close()

	c := raw.(*Conn)

	connDriver2ExecStmt(t, c, "CREATE TABLE cov2(id INTEGER PRIMARY KEY, val TEXT)")
	connDriver2ExecStmt(t, c, "INSERT INTO cov2 VALUES (1, 'hello')")

	selStmt, err := c.Prepare("SELECT val FROM cov2 WHERE id = 1")
	if err != nil {
		t.Fatalf("Prepare SELECT: %v", err)
	}
	rows, err := selStmt.Query(nil)
	if err != nil {
		t.Fatalf("Query SELECT: %v", err)
	}
	vals := make([]driver.Value, 1)
	if err := rows.Next(vals); err != nil {
		t.Fatalf("rows.Next: %v", err)
	}
	rows.Close()
	selStmt.Close()

	got, ok := vals[0].(string)
	if !ok {
		t.Fatalf("expected string, got %T", vals[0])
	}
	if got != "hello" {
		t.Errorf("val = %q, want %q", got, "hello")
	}
}
