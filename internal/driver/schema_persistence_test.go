// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"path/filepath"
	"testing"
)

// TestDatabaseSchemaPersistence verifies that schema is persisted to sqlite_master
// and can be loaded on reopening the database.
func TestDatabaseSchemaPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	drv := GetDriver()

	conn1, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open initial connection: %v", err)
	}
	c1 := conn1.(*Conn)

	create := []string{
		"CREATE TABLE table1 (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE table2 (id INTEGER PRIMARY KEY, value INTEGER)",
		"CREATE INDEX idx_name ON table1(name)",
	}
	for _, sql := range create {
		stmt, err := c1.Prepare(sql)
		if err != nil {
			t.Fatalf("prepare %q: %v", sql, err)
		}
		if _, err := stmt.(*Stmt).ExecContext(ctxBackground(), nil); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
		stmt.Close()
	}
	c1.Close()

	conn2, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen connection: %v", err)
	}
	c2 := conn2.(*Conn)

	for _, sql := range []string{"SELECT * FROM table1", "SELECT * FROM table2"} {
		stmt, err := c2.Prepare(sql)
		if err != nil {
			t.Fatalf("prepare %q after reopen: %v", sql, err)
		}
		rows, err := stmt.(*Stmt).QueryContext(ctxBackground(), nil)
		if err != nil {
			t.Fatalf("query %q after reopen: %v", sql, err)
		}
		rows.Close()
		stmt.Close()
	}
	c2.Close()
}

func ctxBackground() context.Context {
	return context.Background()
}
