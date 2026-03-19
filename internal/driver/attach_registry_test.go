// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// TestDatabaseRegistryResolveAttachedTable ensures that attaching a database
// populates the registry and allows qualified table resolution.
func TestDatabaseRegistryResolveAttachedTable(t *testing.T) {
	tmpDir := t.TempDir()
	mainPath := filepath.Join(tmpDir, "main.db")
	attachPath := filepath.Join(tmpDir, "attach.db")

	// Create attached database with a table.
	attachDB, err := sql.Open(DriverName, attachPath)
	if err != nil {
		t.Fatalf("failed to open attach database: %v", err)
	}
	if _, err := attachDB.Exec("CREATE TABLE t2(x, y)"); err != nil {
		t.Fatalf("failed to create table in attach database: %v", err)
	}
	var masterCount int
	if err := attachDB.QueryRow("SELECT count(*) FROM sqlite_master WHERE name = 't2'").Scan(&masterCount); err != nil {
		t.Fatalf("failed to verify table in attach database: %v", err)
	}
	if masterCount != 1 {
		t.Fatalf("expected table t2 in attach database, found %d", masterCount)
	}
	var typ, name, tbl, sqlText string
	var root int
	if err := attachDB.QueryRow("SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master WHERE name = 't2'").Scan(&typ, &name, &tbl, &root, &sqlText); err != nil {
		t.Fatalf("failed to read sqlite_master row: %v", err)
	}
	t.Logf("master row: type=%s name=%s tbl=%s root=%d sql=%s", typ, name, tbl, root, sqlText)
	attachDB.Close()

	driver := GetDriver()
	conn, err := driver.Open(mainPath)
	if err != nil {
		t.Fatalf("failed to open main connection: %v", err)
	}
	c := conn.(*Conn)
	defer c.Close()

	stmt, err := c.PrepareContext(context.Background(), fmt.Sprintf("ATTACH DATABASE '%s' AS two", attachPath))
	if err != nil {
		t.Fatalf("failed to prepare attach statement: %v", err)
	}
	if _, err := stmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to execute attach: %v", err)
	}
	defer stmt.Close()

	// Inspect registry entry for attached database.
	attachedDB, _ := c.dbRegistry.GetDatabase("two")
	attachedTables := 0
	pageCount := 0
	if attachedDB != nil && attachedDB.Schema != nil {
		attachedTables = len(attachedDB.Schema.Tables)
	}
	if attachedDB != nil && attachedDB.Pager != nil {
		pageCount = int(attachedDB.Pager.PageCount())
	}
	if attachedDB != nil && attachedDB.Schema != nil && attachedDB.Btree != nil {
		if err := attachedDB.Schema.LoadFromMaster(attachedDB.Btree); err != nil {
			t.Fatalf("failed to load schema from attached database: %v", err)
		}
		attachedTables = len(attachedDB.Schema.Tables)
	}

	// Resolve qualified table from attached database.
	table, dbEntry, _, ok := c.dbRegistry.ResolveTable("two", "t2")
	if !ok {
		t.Fatalf("failed to resolve attached table; databases=%v tables=%d pageCount=%d", c.dbRegistry.ListDatabases(), attachedTables, pageCount)
	}
	if table == nil || dbEntry == nil || dbEntry.Btree == nil {
		t.Fatalf("attached registry entry is incomplete: table=%v db=%v", table, dbEntry)
	}
}
