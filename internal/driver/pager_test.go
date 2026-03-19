// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"os"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// pagerExecSQL prepares and executes a SQL statement on the given connection.
func pagerExecSQL(t *testing.T, c *Conn, sql string) {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("failed to prepare %q: %v", sql, err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("failed to exec %q: %v", sql, err)
	}
}

// pagerLogInitialState logs the initial state of a pager connection.
func pagerLogInitialState(t *testing.T, c *Conn) {
	t.Helper()
	t.Logf("Initial state:")
	t.Logf("  btree.Pages count: %d", len(c.btree.Pages))
	t.Logf("  Provider: %v", c.btree.Provider != nil)
	if c.btree.Provider != nil {
		pp := c.btree.Provider.(*pagerProvider)
		t.Logf("  Provider nextPage: %d", pp.nextPage)
	}
}

// pagerVerifyInsertAndParse verifies page data after insert and parses page cells.
func pagerVerifyInsertAndParse(t *testing.T, c *Conn, rootPage uint32) {
	t.Helper()
	_, err := c.btree.GetPage(rootPage)
	if err != nil {
		t.Fatalf("failed to get root page after INSERT: %v", err)
	}
	header, cells, err := c.btree.ParsePage(rootPage)
	if err != nil {
		t.Logf("  Failed to parse page: %v", err)
		return
	}
	t.Logf("  Page %d: numCells=%d, pageType=0x%02x, isLeaf=%v", rootPage, header.NumCells, header.PageType, header.IsLeaf)
	for i, cell := range cells {
		t.Logf("    Cell %d: key=%d, payload len=%d", i, cell.Key, len(cell.Payload))
	}
}

// pagerTestCursor tests cursor operations directly on the btree.
func pagerTestCursor(t *testing.T, c *Conn, rootPage uint32) {
	t.Helper()
	t.Logf("Testing cursor directly:")
	cursor := btree.NewCursor(c.btree, rootPage)
	err := cursor.MoveToFirst()
	if err != nil {
		t.Logf("  MoveToFirst error: %v", err)
		return
	}
	t.Logf("  Cursor state: %d, valid=%v", cursor.State, cursor.IsValid())
	if cursor.IsValid() {
		t.Logf("  Current key: %d, payload len: %d", cursor.GetKey(), len(cursor.GetPayload()))
	}
}

func TestPagerIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sqlite-pager-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")

	conn, err := sqliteDriver.OpenConnector(dbPath)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	pagerLogInitialState(t, c)

	pagerExecSQL(t, c, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

	t.Logf("After CREATE TABLE:")
	t.Logf("  btree.Pages count: %d", len(c.btree.Pages))
	table, ok := c.schema.GetTable("test")
	if !ok {
		t.Fatalf("table 'test' not found in schema")
	}
	t.Logf("  table.RootPage: %d", table.RootPage)

	pageData, err := c.btree.GetPage(table.RootPage)
	if err != nil {
		t.Fatalf("failed to get root page: %v", err)
	}
	t.Logf("  Page %d exists, len=%d, type=0x%02x", table.RootPage, len(pageData), pageData[0])

	pagerExecSQL(t, c, "INSERT INTO test (id, value) VALUES (1, 'hello')")

	pagerVerifyInsertAndParse(t, c, table.RootPage)
	pagerTestCursor(t, c, table.RootPage)

	stmt, err := c.Prepare("SELECT value FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to prepare SELECT: %v", err)
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

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
