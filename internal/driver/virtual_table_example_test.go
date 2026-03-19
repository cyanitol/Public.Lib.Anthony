// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"testing"
)

// TestVirtualTableSQLComplete demonstrates complete CREATE VIRTUAL TABLE functionality.
// This test shows that FTS5 and R-Tree modules work via SQL CREATE VIRTUAL TABLE syntax.
// vtableSQLCheckTable verifies a virtual table exists with expected properties.
func vtableSQLCheckTable(t *testing.T, c *Conn, name, module string) {
	t.Helper()
	table, exists := c.schema.GetTable(name)
	if !exists {
		t.Errorf("virtual table %q not found", name)
		return
	}
	if !table.IsVirtual {
		t.Errorf("table %q not marked as virtual", name)
	}
	if table.Module != module {
		t.Errorf("table %q has wrong module: got %s, want %s", name, table.Module, module)
	}
	t.Logf("Virtual table created: %s with args %v", table.Name, table.ModuleArgs)
}

func TestVirtualTableSQLComplete(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE VIRTUAL TABLE documents USING fts5(title, content)")
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	_, err = db.Exec("CREATE VIRTUAL TABLE spatial_index USING rtree(id, minX, maxX, minY, maxY)")
	if err != nil {
		t.Fatalf("Failed to create R-Tree table: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return nil
		}
		vtableSQLCheckTable(t, c, "documents", "fts5")
		vtableSQLCheckTable(t, c, "spatial_index", "rtree")
		return nil
	})
	if err != nil {
		t.Fatalf("Schema verification failed: %v", err)
	}
}
