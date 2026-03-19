// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"testing"
)

// TestCreateVirtualTableFTS5 tests creating an FTS5 virtual table via SQL.
func TestCreateVirtualTableFTS5(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test CREATE VIRTUAL TABLE with FTS5
	_, err = db.Exec("CREATE VIRTUAL TABLE t1 USING fts5(content)")
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Verify table was created by checking schema directly
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return nil // Skip if we can't access driver connection
		}

		// Check if table exists in schema
		table, exists := c.schema.GetTable("t1")
		if !exists {
			t.Fatal("Table 't1' not found in schema")
		}

		// Verify it's a virtual table
		if !table.IsVirtual {
			t.Error("Table 't1' is not marked as virtual")
		}

		// Verify module name
		if table.Module != "fts5" {
			t.Errorf("Expected module 'fts5', got %q", table.Module)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Schema verification failed: %v", err)
	}
}

// TestCreateVirtualTableRTree tests creating an RTree virtual table via SQL.
func TestCreateVirtualTableRTree(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test CREATE VIRTUAL TABLE with RTree
	_, err = db.Exec("CREATE VIRTUAL TABLE rt1 USING rtree(id, minx, maxx, miny, maxy)")
	if err != nil {
		t.Fatalf("Failed to create RTree table: %v", err)
	}

	// Verify table was created by checking schema directly
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

		table, exists := c.schema.GetTable("rt1")
		if !exists {
			t.Fatal("Table 'rt1' not found in schema")
		}

		if !table.IsVirtual {
			t.Error("Table 'rt1' is not marked as virtual")
		}

		if table.Module != "rtree" {
			t.Errorf("Expected module 'rtree', got %q", table.Module)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Schema verification failed: %v", err)
	}
}

// TestCreateVirtualTableIfNotExists tests IF NOT EXISTS clause.
func TestCreateVirtualTableIfNotExists(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first time
	_, err = db.Exec("CREATE VIRTUAL TABLE IF NOT EXISTS t1 USING fts5(content)")
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Create again with IF NOT EXISTS - should succeed
	_, err = db.Exec("CREATE VIRTUAL TABLE IF NOT EXISTS t1 USING fts5(content)")
	if err != nil {
		t.Fatalf("Expected IF NOT EXISTS to succeed, got error: %v", err)
	}

	// Create without IF NOT EXISTS - should fail
	_, err = db.Exec("CREATE VIRTUAL TABLE t1 USING fts5(content)")
	if err == nil {
		t.Fatal("Expected error when creating duplicate table, got nil")
	}
}

// TestCreateVirtualTableUnknownModule tests error handling for unknown modules.
func TestCreateVirtualTableUnknownModule(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Try to create table with non-existent module
	_, err = db.Exec("CREATE VIRTUAL TABLE t1 USING nonexistent(arg1)")
	if err == nil {
		t.Fatal("Expected error for unknown module, got nil")
	}
}

// TestCreateVirtualTableWithArgs tests module arguments.
// vtableSQLCheckArgs verifies a virtual table's module arguments.
func vtableSQLCheckArgs(t *testing.T, c *Conn, name string, expectedArgs []string) {
	t.Helper()
	table, exists := c.schema.GetTable(name)
	if !exists {
		t.Fatalf("Table %q not found in schema", name)
	}
	if !table.IsVirtual {
		t.Errorf("Table %q is not marked as virtual", name)
	}
	if len(table.ModuleArgs) != len(expectedArgs) {
		t.Errorf("Expected %d module args, got %d", len(expectedArgs), len(table.ModuleArgs))
	}
	for i, expected := range expectedArgs {
		if i < len(table.ModuleArgs) && table.ModuleArgs[i] != expected {
			t.Errorf("Arg %d: expected %q, got %q", i, expected, table.ModuleArgs[i])
		}
	}
}

func TestCreateVirtualTableWithArgs(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE VIRTUAL TABLE docs USING fts5(title, body, author)")
	if err != nil {
		t.Fatalf("Failed to create FTS5 table with multiple columns: %v", err)
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
		vtableSQLCheckArgs(t, c, "docs", []string{"title", "body", "author"})
		return nil
	})
	if err != nil {
		t.Fatalf("Schema verification failed: %v", err)
	}
}
