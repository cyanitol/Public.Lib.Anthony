// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
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

	// Verify rtree module is registered and create a table
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		if !c.vtabRegistry.HasModule("rtree") {
			return fmt.Errorf("rtree module not registered")
		}
		module := c.vtabRegistry.GetModule("rtree")
		if module == nil {
			return fmt.Errorf("rtree module not found")
		}
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

// rtreeCreateAndInsert creates an RTree table, inserts one entry, and returns the RTree.
func rtreeCreateAndInsert(c *Conn) (*rtree.RTree, error) {
	module := c.vtabRegistry.GetModule("rtree")
	if module == nil {
		return nil, fmt.Errorf("rtree module not found")
	}

	table, schema, err := module.Create(
		nil, "rtree", "main", "spatial_index",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create rtree: %v", err)
	}
	if table == nil {
		return nil, fmt.Errorf("created table is nil")
	}
	if schema == "" {
		return nil, fmt.Errorf("schema is empty")
	}

	rt, ok := table.(*rtree.RTree)
	if !ok {
		return nil, fmt.Errorf("table is not *rtree.RTree")
	}

	_, err = rt.Update(7, []interface{}{
		nil, int64(1), int64(0), int64(10), int64(0), int64(10),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to insert: %v", err)
	}
	return rt, nil
}

// rtreeVerifyCursorCount opens a cursor, filters, and verifies the result count.
func rtreeVerifyCursorCount(rt *rtree.RTree, want int) error {
	cursor, err := rt.Open()
	if err != nil {
		return fmt.Errorf("failed to open cursor: %v", err)
	}
	defer cursor.Close()

	err = cursor.Filter(0, "", []interface{}{})
	if err != nil {
		return fmt.Errorf("failed to filter: %v", err)
	}

	count := 0
	for !cursor.EOF() {
		count++
		cursor.Next()
	}
	if count != want {
		return fmt.Errorf("expected %d result, got %d", want, count)
	}
	return nil
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

	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}

		rt, err := rtreeCreateAndInsert(c)
		if err != nil {
			return err
		}

		if rt.Count() != 1 {
			return fmt.Errorf("expected 1 entry, got %d", rt.Count())
		}

		return rtreeVerifyCursorCount(rt, 1)
	})
	if err != nil {
		t.Fatalf("Module operations test failed: %v", err)
	}

	t.Log("RTree module operations completed successfully")
}

// rtreeTestCreateDimension tests creating an RTree with the given columns.
func rtreeTestCreateDimension(c *Conn, columns []string, wantErr bool) error {
	module := c.vtabRegistry.GetModule("rtree")
	if module == nil {
		return fmt.Errorf("rtree module not found")
	}

	table, _, err := module.Create(nil, "rtree", "main", "test_table", columns)
	if wantErr {
		if err == nil {
			return fmt.Errorf("expected error but got none")
		}
		return nil
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
				return rtreeTestCreateDimension(c, tc.columns, tc.wantErr)
			})
			if err != nil {
				t.Fatalf("Test case failed: %v", err)
			}
		})
	}
}
