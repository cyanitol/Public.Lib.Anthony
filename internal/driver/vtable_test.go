// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
)

var ctx = context.Background()

// TestRegisterVirtualTableModule tests the registration and use of virtual table modules.
func TestRegisterVirtualTableModule(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the underlying connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Register a test virtual table module
	module := &testVTableModule{}
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.RegisterVirtualTableModule("test_vtab", module)
	})
	if err != nil {
		t.Fatalf("Failed to register virtual table module: %v", err)
	}

	// Test: Verify module was registered
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		if !c.vtabRegistry.HasModule("test_vtab") {
			return fmt.Errorf("module not registered")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Module registration check failed: %v", err)
	}

	t.Log("Virtual table module registered successfully")
}

// TestVirtualTableModuleOperations tests various operations on virtual table modules.
func TestVirtualTableModuleOperations(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the underlying connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Register a test virtual table module
	module := &testVTableModule{}
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.RegisterVirtualTableModule("test_ops", module)
	})
	if err != nil {
		t.Fatalf("Failed to register virtual table module: %v", err)
	}

	// Test unregistering the module
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.UnregisterVirtualTableModule("test_ops")
	})
	if err != nil {
		t.Fatalf("Failed to unregister virtual table module: %v", err)
	}

	// Verify module was unregistered
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		if c.vtabRegistry.HasModule("test_ops") {
			return fmt.Errorf("module still registered after unregister")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Module unregistration check failed: %v", err)
	}

	t.Log("Virtual table module operations completed successfully")
}

// TestVirtualTableModuleDuplicateRegistration tests duplicate registration handling.
func TestVirtualTableModuleDuplicateRegistration(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the underlying connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Register a test virtual table module
	module := &testVTableModule{}
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.RegisterVirtualTableModule("test_dup", module)
	})
	if err != nil {
		t.Fatalf("Failed to register virtual table module: %v", err)
	}

	// Try to register the same module again
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.RegisterVirtualTableModule("test_dup", module)
	})
	if err == nil {
		t.Fatal("Expected error when registering duplicate module, got none")
	}

	t.Logf("Duplicate registration correctly rejected: %v", err)
}

// TestVirtualTableModuleUnregisterNonExistent tests unregistering a non-existent module.
func TestVirtualTableModuleUnregisterNonExistent(t *testing.T) {
	t.Parallel()

	// Create an in-memory database
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the underlying connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Try to unregister a non-existent module
	err = conn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type")
		}
		return c.UnregisterVirtualTableModule("nonexistent")
	})
	if err == nil {
		t.Fatal("Expected error when unregistering non-existent module, got none")
	}

	t.Logf("Unregister non-existent correctly rejected: %v", err)
}

// Test helpers

// testVTableModule is a simple test module implementation.
type testVTableModule struct {
	vtab.BaseModule
}

func (m *testVTableModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return &testVTable{
		data: []testVTableRow{
			{ID: 1, Name: "Alice", Age: 30},
			{ID: 2, Name: "Bob", Age: 25},
			{ID: 3, Name: "Charlie", Age: 35},
		},
	}, "CREATE TABLE x(id INTEGER, name TEXT, age INTEGER)", nil
}

func (m *testVTableModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.Create(db, moduleName, dbName, tableName, args)
}

// testVTable is a simple test virtual table implementation.
type testVTable struct {
	vtab.BaseVirtualTable
	data []testVTableRow
}

type testVTableRow struct {
	ID   int64
	Name string
	Age  int64
}

func (t *testVTable) BestIndex(info *vtab.IndexInfo) error {
	// Simple implementation: check if we have an ID constraint
	idxConstraint := info.FindConstraint(0, vtab.ConstraintEQ)
	if idxConstraint >= 0 {
		// We can use the ID constraint
		info.SetConstraintUsage(idxConstraint, 1, true)
		info.EstimatedCost = 1.0
		info.EstimatedRows = 1
		info.IdxNum = 1 // Indicates we're using ID constraint
	} else {
		// Full table scan
		info.EstimatedCost = 100.0
		info.EstimatedRows = int64(len(t.data))
		info.IdxNum = 0
	}
	return nil
}

func (t *testVTable) Open() (vtab.VirtualCursor, error) {
	return &testVTableCursor{
		table: t,
		pos:   -1,
	}, nil
}

// testVTableCursor is a simple test cursor implementation.
type testVTableCursor struct {
	vtab.BaseCursor
	table    *testVTable
	pos      int
	filtered []testVTableRow
}

func (c *testVTableCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	// Reset filtered data
	c.filtered = nil

	if idxNum == 1 && len(argv) > 0 {
		// ID constraint is active
		targetID, ok := argv[0].(int64)
		if !ok {
			// Try to convert from other numeric types
			switch v := argv[0].(type) {
			case int:
				targetID = int64(v)
			case int32:
				targetID = int64(v)
			default:
				return fmt.Errorf("invalid ID type: %T", argv[0])
			}
		}

		// Filter by ID
		for _, row := range c.table.data {
			if row.ID == targetID {
				c.filtered = append(c.filtered, row)
			}
		}
	} else {
		// No constraint, return all rows
		c.filtered = append([]testVTableRow(nil), c.table.data...)
	}

	// Position at first row
	if len(c.filtered) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}

	return nil
}

func (c *testVTableCursor) Next() error {
	c.pos++
	return nil
}

func (c *testVTableCursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.filtered)
}

func (c *testVTableCursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, fmt.Errorf("cursor is at EOF")
	}

	row := c.filtered[c.pos]
	switch index {
	case 0:
		return row.ID, nil
	case 1:
		return row.Name, nil
	case 2:
		return row.Age, nil
	default:
		return nil, fmt.Errorf("column index %d out of range", index)
	}
}

func (c *testVTableCursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, fmt.Errorf("cursor is at EOF")
	}
	return c.filtered[c.pos].ID, nil
}

func (c *testVTableCursor) Close() error {
	c.filtered = nil
	return nil
}

// TestVirtualTableWithData tests a complete virtual table with data operations.
func TestVirtualTableWithData(t *testing.T) {
	t.Parallel()

	// Create module and table
	module := &testVTableModule{}
	vtable, schema, err := module.Create(nil, "test_vtab", "main", "test_table", nil)
	if err != nil {
		t.Fatalf("Failed to create virtual table: %v", err)
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}
	t.Logf("Schema: %s", schema)

	// Test BestIndex with no constraints
	info := vtab.NewIndexInfo(0)
	err = vtable.BestIndex(info)
	if err != nil {
		t.Fatalf("BestIndex failed: %v", err)
	}
	if info.IdxNum != 0 {
		t.Errorf("Expected IdxNum 0 (full scan), got %d", info.IdxNum)
	}
	t.Logf("BestIndex (no constraints): cost=%.1f, rows=%d", info.EstimatedCost, info.EstimatedRows)

	// Test BestIndex with ID constraint
	info = vtab.NewIndexInfo(1)
	info.Constraints[0] = vtab.IndexConstraint{
		Column: 0,
		Op:     vtab.ConstraintEQ,
		Usable: true,
	}
	err = vtable.BestIndex(info)
	if err != nil {
		t.Fatalf("BestIndex failed: %v", err)
	}
	if info.IdxNum != 1 {
		t.Errorf("Expected IdxNum 1 (ID constraint), got %d", info.IdxNum)
	}
	t.Logf("BestIndex (ID constraint): cost=%.1f, rows=%d", info.EstimatedCost, info.EstimatedRows)

	// Open a cursor
	cursor, err := vtable.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	// Test full table scan
	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	count := 0
	for !cursor.EOF() {
		id, err := cursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		name, err := cursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}
		age, err := cursor.Column(2)
		if err != nil {
			t.Errorf("Column(2) failed: %v", err)
		}
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}

		t.Logf("Row %d: id=%v, name=%v, age=%v, rowid=%v", count, id, name, age, rowid)
		count++

		err = cursor.Next()
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// Test filtered query (ID = 2)
	cursor2, err := vtable.Open()
	if err != nil {
		t.Fatalf("Failed to open second cursor: %v", err)
	}
	defer cursor2.Close()

	err = cursor2.Filter(1, "", []interface{}{int64(2)})
	if err != nil {
		t.Fatalf("Filter with constraint failed: %v", err)
	}

	count = 0
	for !cursor2.EOF() {
		id, err := cursor2.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		name, err := cursor2.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}

		t.Logf("Filtered row: id=%v, name=%v", id, name)

		// Verify it's the correct row
		if id != int64(2) {
			t.Errorf("Expected ID 2, got %v", id)
		}
		if name != "Bob" {
			t.Errorf("Expected name 'Bob', got %v", name)
		}

		count++
		err = cursor2.Next()
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 filtered row, got %d", count)
	}
}
