// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab/rtree"
)

// TestRTreeIntegrationBasic tests basic R-Tree operations through the SQL interface
func TestRTreeIntegrationBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rtree_basic.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get the connection and register rtree module
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	// Verify rtree module is registered
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		if !c.vtabRegistry.HasModule("rtree") {
			return fmt.Errorf("rtree module not registered")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RTree module check failed: %v", err)
	}

	// Test 1: Create virtual table directly through vtab API
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}

		// Get the rtree module
		module := c.vtabRegistry.GetModule("rtree")
		if module == nil {
			return fmt.Errorf("rtree module not found")
		}

		// Create the virtual table
		_, _, err := module.Create(
			nil, "rtree", "main", "t1",
			[]string{"id", "minX", "maxX", "minY", "maxY"},
		)
		if err != nil {
			return fmt.Errorf("failed to create rtree table: %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to create rtree table: %v", err)
	}

	t.Log("R-Tree module successfully registered and basic table creation works")
}

// TestRTreeModuleRegistration tests that RTree module is properly registered
func TestRTreeModuleRegistration(t *testing.T) {
	t.Parallel()

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Verify module is registered
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		if !c.vtabRegistry.HasModule("rtree") {
			return fmt.Errorf("rtree module not registered")
		}

		// Verify the module is actually an RTreeModule
		module := c.vtabRegistry.GetModule("rtree")
		if module == nil {
			return fmt.Errorf("rtree module is nil")
		}

		_, ok = module.(*rtree.RTreeModule)
		if !ok {
			return fmt.Errorf("rtree module is not *rtree.RTreeModule")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Module registration check failed: %v", err)
	}

	t.Log("RTree module successfully registered")
}

// TestRTreeModuleOperations tests basic operations on RTree module
func TestRTreeModuleOperations(t *testing.T) {
	t.Parallel()

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Test creating an RTree table through the module
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}

		module := c.vtabRegistry.GetModule("rtree")
		if module == nil {
			return fmt.Errorf("rtree module not found")
		}

		// Create a 2D R-Tree
		table, schema, err := module.Create(
			nil, "rtree", "main", "spatial_index",
			[]string{"id", "minX", "maxX", "minY", "maxY"},
		)
		if err != nil {
			return fmt.Errorf("failed to create rtree: %v", err)
		}

		if table == nil {
			return fmt.Errorf("created table is nil")
		}

		if schema == "" {
			return fmt.Errorf("schema is empty")
		}

		// Verify it's an RTree
		rt, ok := table.(*rtree.RTree)
		if !ok {
			return fmt.Errorf("table is not *rtree.RTree")
		}

		// Test inserting data
		_, err = rt.Update(7, []interface{}{
			nil,       // old rowid
			int64(1),  // new rowid
			int64(0),  // minX
			int64(10), // maxX
			int64(0),  // minY
			int64(10), // maxY
		})
		if err != nil {
			return fmt.Errorf("failed to insert: %v", err)
		}

		// Verify count
		if rt.Count() != 1 {
			return fmt.Errorf("expected 1 entry, got %d", rt.Count())
		}

		// Test querying
		cursor, err := rt.Open()
		if err != nil {
			return fmt.Errorf("failed to open cursor: %v", err)
		}
		defer cursor.Close()

		err = cursor.Filter(0, "", []interface{}{})
		if err != nil {
			return fmt.Errorf("failed to filter: %v", err)
		}

		// Count results
		count := 0
		for !cursor.EOF() {
			count++
			cursor.Next()
		}

		if count != 1 {
			return fmt.Errorf("expected 1 result, got %d", count)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Module operations test failed: %v", err)
	}

	t.Log("RTree module operations completed successfully")
}

// TestRTreeModuleMultipleDimensions tests creating R-Trees with different dimensions
func TestRTreeModuleMultipleDimensions(t *testing.T) {
	t.Parallel()

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	testCases := []struct {
		name    string
		columns []string
		wantDim int
		wantErr bool
	}{
		{
			name:    "2D R-Tree",
			columns: []string{"id", "minX", "maxX", "minY", "maxY"},
			wantDim: 2,
			wantErr: false,
		},
		{
			name:    "3D R-Tree",
			columns: []string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ"},
			wantDim: 3,
			wantErr: false,
		},
		{
			name:    "4D R-Tree",
			columns: []string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ", "minT", "maxT"},
			wantDim: 4,
			wantErr: false,
		},
		{
			name:    "Invalid - too few columns",
			columns: []string{"id", "min", "max"},
			wantDim: 0,
			wantErr: true,
		},
		{
			name:    "Invalid - odd columns",
			columns: []string{"id", "minX", "maxX", "minY"},
			wantDim: 0,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := conn.Raw(func(driverConn interface{}) error {
				c, ok := driverConn.(*Conn)
				if !ok {
					return fmt.Errorf("unexpected driver connection type")
				}

				module := c.vtabRegistry.GetModule("rtree")
				if module == nil {
					return fmt.Errorf("rtree module not found")
				}

				table, _, err := module.Create(
					nil, "rtree", "main", "test_table",
					tc.columns,
				)

				if tc.wantErr {
					if err == nil {
						return fmt.Errorf("expected error but got none")
					}
					return nil // Expected error, test passed
				}

				if err != nil {
					return fmt.Errorf("unexpected error: %v", err)
				}

				rt, ok := table.(*rtree.RTree)
				if !ok {
					return fmt.Errorf("table is not *rtree.RTree")
				}

				if rt.Count() != 0 {
					return fmt.Errorf("expected empty tree, got %d entries", rt.Count())
				}

				return nil
			})

			if err != nil {
				t.Fatalf("Test case failed: %v", err)
			}
		})
	}
}
