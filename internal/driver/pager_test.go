// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"os"
	"path/filepath"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

func TestPagerIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-pager-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	// Open a connection directly through the driver
	conn, err := sqliteDriver.OpenConnector(dbPath)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Check initial state
	t.Logf("Initial state:")
	t.Logf("  btree.Pages count: %d", len(c.btree.Pages))
	t.Logf("  Provider: %v", c.btree.Provider != nil)
	if c.btree.Provider != nil {
		pp := c.btree.Provider.(*pagerProvider)
		t.Logf("  Provider nextPage: %d", pp.nextPage)
	}

	// Create table
	stmt, err := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to prepare CREATE TABLE: %v", err)
	}
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("failed to exec CREATE TABLE: %v", err)
	}
	stmt.Close()

	// Check state after CREATE TABLE
	t.Logf("After CREATE TABLE:")
	t.Logf("  btree.Pages count: %d", len(c.btree.Pages))
	table, ok := c.schema.GetTable("test")
	if !ok {
		t.Fatalf("table 'test' not found in schema")
	}
	t.Logf("  table.RootPage: %d", table.RootPage)

	// Verify page data exists
	pageData, err := c.btree.GetPage(table.RootPage)
	if err != nil {
		t.Fatalf("failed to get root page: %v", err)
	}
	t.Logf("  Page %d exists, len=%d, type=0x%02x", table.RootPage, len(pageData), pageData[0])

	// Insert data
	stmt, err = c.Prepare("INSERT INTO test (id, value) VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("failed to prepare INSERT: %v", err)
	}
	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatalf("failed to exec INSERT: %v", err)
	}
	stmt.Close()
	affected, _ := result.RowsAffected()
	t.Logf("After INSERT: rows affected = %d", affected)

	// Check page data after INSERT
	pageData, err = c.btree.GetPage(table.RootPage)
	if err != nil {
		t.Fatalf("failed to get root page after INSERT: %v", err)
	}
	// Parse page header to see if cells were added
	header, cells, err := c.btree.ParsePage(table.RootPage)
	if err != nil {
		t.Logf("  Failed to parse page: %v", err)
	} else {
		t.Logf("  Page %d: numCells=%d, pageType=0x%02x, isLeaf=%v", table.RootPage, header.NumCells, header.PageType, header.IsLeaf)
		for i, cell := range cells {
			t.Logf("    Cell %d: key=%d, payload len=%d", i, cell.Key, len(cell.Payload))
		}
	}

	// Test cursor directly
	t.Logf("Testing cursor directly:")
	cursor := btree.NewCursor(c.btree, table.RootPage)
	err = cursor.MoveToFirst()
	if err != nil {
		t.Logf("  MoveToFirst error: %v", err)
	} else {
		t.Logf("  Cursor state: %d, valid=%v", cursor.State, cursor.IsValid())
		if cursor.IsValid() {
			t.Logf("  Current key: %d, payload len: %d", cursor.GetKey(), len(cursor.GetPayload()))
		}
	}

	// Query data
	stmt, err = c.Prepare("SELECT value FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to prepare SELECT: %v", err)
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	// Try to read rows
	cols := rows.Columns()
	t.Logf("SELECT columns: %v", cols)

	driverValues := make([]driver.Value, len(cols))
	err = rows.Next(driverValues)
	if err != nil {
		t.Fatalf("no rows returned from SELECT: %v", err)
	}

	t.Logf("Got row: %v", driverValues)
	stmt.Close()
}
