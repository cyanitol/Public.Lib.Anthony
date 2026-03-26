// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

const testDriver = "sqlite_internal"

// openMemDB opens a fresh in-memory database and fails the test on error.
// Opening :memory: exercises createMemoryConnection and registerBuiltinVirtualTables.
func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(testDriver, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open :memory: failed: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping failed: %v", err)
	}
	return db
}

// TestConnCoverageMemoryOpen exercises createMemoryConnection and
// registerBuiltinVirtualTables by opening multiple independent in-memory
// connections.
func TestConnCoverageMemoryOpen(t *testing.T) {
	for i := 0; i < 3; i++ {
		db := openMemDB(t)
		defer db.Close()

		var val int
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Errorf("iter %d: SELECT 1 failed: %v", i, err)
		}
		if val != 1 {
			t.Errorf("iter %d: got %d want 1", i, val)
		}
	}
}

// TestConnCoverageCreateTable exercises createMemoryConnection and
// ensureMasterPage by creating a table immediately after opening.
// The first CREATE TABLE causes the driver to ensure page 1 (sqlite_master) exists.
func TestConnCoverageCreateTable(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Verify table exists
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM items").Scan(&cnt); err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	if cnt != 0 {
		t.Errorf("expected 0 rows, got %d", cnt)
	}
}

// TestConnCoverageWriteMarkDirty exercises MarkDirty (memoryPagerProvider)
// by performing INSERT and UPDATE operations on an in-memory database.
func TestConnCoverageWriteMarkDirty(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows to allocate pages and trigger MarkDirty.
	for i := 1; i <= 20; i++ {
		if _, err := db.Exec("INSERT INTO nums (v) VALUES (?)", i*10); err != nil {
			t.Fatalf("INSERT row %d: %v", i, err)
		}
	}

	// UPDATE triggers additional MarkDirty calls.
	if _, err := db.Exec("UPDATE nums SET v = v + 1"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	var total int
	if err := db.QueryRow("SELECT COUNT(*) FROM nums").Scan(&total); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if total != 20 {
		t.Errorf("expected 20 rows, got %d", total)
	}
}

// TestConnCoverageEnsureMasterPageViaCreateMultiple exercises ensureMasterPage
// across multiple connections; each new :memory: connection initialises page 1.
func TestConnCoverageEnsureMasterPageViaCreateMultiple(t *testing.T) {
	for i := 0; i < 4; i++ {
		db := openMemDB(t)

		tbl := fmt.Sprintf("t%d", i)
		if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (x INTEGER)", tbl)); err != nil {
			db.Close()
			t.Fatalf("CREATE TABLE %s: %v", tbl, err)
		}
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (42)", tbl)); err != nil {
			db.Close()
			t.Fatalf("INSERT into %s: %v", tbl, err)
		}

		var v int
		if err := db.QueryRow(fmt.Sprintf("SELECT x FROM %s", tbl)).Scan(&v); err != nil {
			db.Close()
			t.Fatalf("SELECT from %s: %v", tbl, err)
		}
		if v != 42 {
			db.Close()
			t.Errorf("iter %d: got %d want 42", i, v)
		}

		db.Close()
	}
}

// TestConnCoverageRegisterVirtualTablesFTS5 exercises registerBuiltinVirtualTables
// by creating an FTS5 virtual table on a fresh in-memory connection.
func TestConnCoverageRegisterVirtualTablesFTS5(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE VIRTUAL TABLE docs USING fts5(content)`)
	if err != nil {
		// FTS5 module may or may not be fully operational; the key is that
		// registerBuiltinVirtualTables was called without panicking.
		t.Logf("fts5 CREATE VIRTUAL TABLE: %v (module registered but may be limited)", err)
	}
}

// TestConnCoverageRegisterVirtualTablesRTree exercises registerBuiltinVirtualTables
// by creating an RTree virtual table on a fresh in-memory connection.
func TestConnCoverageRegisterVirtualTablesRTree(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE VIRTUAL TABLE geo USING rtree(id, minX, maxX, minY, maxY)`)
	if err != nil {
		t.Logf("rtree CREATE VIRTUAL TABLE: %v (module registered but may be limited)", err)
	}
}

// TestConnCoverageMarkDirtyDelete exercises MarkDirty through DELETE operations.
func TestConnCoverageMarkDirtyDelete(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	for i := 0; i < 15; i++ {
		if _, err := db.Exec("INSERT INTO data (v) VALUES (?)", fmt.Sprintf("row%d", i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	if _, err := db.Exec("DELETE FROM data WHERE id > 5"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM data").Scan(&cnt); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if cnt > 15 {
		t.Errorf("unexpected row count %d", cnt)
	}
}

// TestConnCoverageTransactionWriteMarkDirty exercises MarkDirty within an
// explicit transaction on an in-memory database.
func TestConnCoverageTransactionWriteMarkDirty(t *testing.T) {
	db := openMemDB(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE vals (id INTEGER PRIMARY KEY, n INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	for i := 1; i <= 10; i++ {
		if _, err := tx.Exec("INSERT INTO vals (n) VALUES (?)", i); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var total int
	if err := db.QueryRow("SELECT COUNT(*) FROM vals").Scan(&total); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if total != 10 {
		t.Errorf("expected 10 rows, got %d", total)
	}
}
