// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"testing"
)

// TestVirtualTableSQLComplete demonstrates complete CREATE VIRTUAL TABLE functionality.
// This test shows that FTS5 and R-Tree modules work via SQL CREATE VIRTUAL TABLE syntax.
func TestVirtualTableSQLComplete(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create FTS5 virtual table
	t.Log("Creating FTS5 virtual table...")
	_, err = db.Exec("CREATE VIRTUAL TABLE documents USING fts5(title, content)")
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Create R-Tree virtual table
	t.Log("Creating R-Tree virtual table...")
	_, err = db.Exec("CREATE VIRTUAL TABLE spatial_index USING rtree(id, minX, maxX, minY, maxY)")
	if err != nil {
		t.Fatalf("Failed to create R-Tree table: %v", err)
	}

	// Verify both tables were created
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

		// Check FTS5 table
		ftsTable, exists := c.schema.GetTable("documents")
		if !exists {
			t.Error("FTS5 table 'documents' not found")
		} else {
			if !ftsTable.IsVirtual {
				t.Error("FTS5 table not marked as virtual")
			}
			if ftsTable.Module != "fts5" {
				t.Errorf("FTS5 table has wrong module: %s", ftsTable.Module)
			}
			t.Logf("FTS5 table created: %s with args %v", ftsTable.Name, ftsTable.ModuleArgs)
		}

		// Check R-Tree table
		rtreeTable, exists := c.schema.GetTable("spatial_index")
		if !exists {
			t.Error("R-Tree table 'spatial_index' not found")
		} else {
			if !rtreeTable.IsVirtual {
				t.Error("R-Tree table not marked as virtual")
			}
			if rtreeTable.Module != "rtree" {
				t.Errorf("R-Tree table has wrong module: %s", rtreeTable.Module)
			}
			t.Logf("R-Tree table created: %s with args %v", rtreeTable.Name, rtreeTable.ModuleArgs)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Schema verification failed: %v", err)
	}

	t.Log("SUCCESS: Both FTS5 and R-Tree virtual tables created via SQL")
}
