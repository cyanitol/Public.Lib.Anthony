// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package constraint

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// createTestTablePK creates a test table with the given schema (simplified without btree initialization).
func createTestTablePK(columns []*schema.Column, primaryKey []string) *schema.Table {
	return &schema.Table{
		Name:       "test_table",
		RootPage:   2,
		Columns:    columns,
		PrimaryKey: primaryKey,
	}
}

// setupTestTable creates a test table with in-memory btree support.
func setupTestTable(t *testing.T, columns []*schema.Column, primaryKey []string) (*schema.Table, *btree.Btree, func()) {
	t.Helper()

	// Create in-memory btree
	bt := btree.NewBtree(4096)

	// Create table root page
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Build table schema
	table := &schema.Table{
		Name:       "test_table",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: primaryKey,
	}

	cleanup := func() {
		// Nothing to clean up for in-memory btree
	}

	return table, bt, cleanup
}

// TestPrimaryKeyConstraint_IntegerPrimaryKey tests INTEGER PRIMARY KEY validation.
func TestPrimaryKeyConstraint_IntegerPrimaryKey(t *testing.T) {
	columns := []*schema.Column{
		{
			Name:       "id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: "TEXT",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{"id"})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	t.Run("auto-generate rowid when not provided", func(t *testing.T) {
		values := map[string]interface{}{
			"name": "Alice",
		}

		rowid, err := pk.ValidateInsert(values, false, 0)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if rowid != 1 {
			t.Errorf("Expected auto-generated rowid 1, got: %d", rowid)
		}
	})

	t.Run("use explicit INTEGER PRIMARY KEY value", func(t *testing.T) {
		values := map[string]interface{}{
			"id":   int64(42),
			"name": "Bob",
		}

		rowid, err := pk.ValidateInsert(values, false, 0)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if rowid != 42 {
			t.Errorf("Expected rowid 42, got: %d", rowid)
		}
	})

	t.Run("reject duplicate INTEGER PRIMARY KEY", func(t *testing.T) {
		// First insert
		values1 := map[string]interface{}{
			"id":   int64(100),
			"name": "Charlie",
		}
		rowid1, err := pk.ValidateInsert(values1, false, 0)
		if err != nil {
			t.Fatalf("First insert failed: %v", err)
		}

		// Insert the row into btree
		cursor := btree.NewCursor(bt, table.RootPage)
		payload := []byte("test_payload")
		if err := cursor.Insert(rowid1, payload); err != nil {
			t.Fatalf("BTree insert failed: %v", err)
		}

		// Try to insert duplicate
		values2 := map[string]interface{}{
			"id":   int64(100),
			"name": "David",
		}
		_, err = pk.ValidateInsert(values2, false, 0)
		if err == nil {
			t.Error("Expected error for duplicate PRIMARY KEY, got nil")
		}
	})

	t.Run("handle different integer types", func(t *testing.T) {
		testCases := []struct {
			name  string
			value interface{}
			want  int64
		}{
			{"int", int(5), 5},
			{"int32", int32(6), 6},
			{"int64", int64(7), 7},
			{"uint32", uint32(8), 8},
			{"float64", float64(9.0), 9},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				values := map[string]interface{}{
					"id":   tc.value,
					"name": "Test",
				}

				rowid, err := pk.ValidateInsert(values, false, 0)
				if err != nil {
					t.Fatalf("Expected no error, got: %v", err)
				}

				if rowid != tc.want {
					t.Errorf("Expected rowid %d, got: %d", tc.want, rowid)
				}
			})
		}
	})
}

// TestPrimaryKeyConstraint_CompositePrimaryKey tests composite PRIMARY KEY validation.
func TestPrimaryKeyConstraint_CompositePrimaryKey(t *testing.T) {
	columns := []*schema.Column{
		{
			Name:       "dept_id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name:       "emp_id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: "TEXT",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{"dept_id", "emp_id"})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	t.Run("all PRIMARY KEY columns required", func(t *testing.T) {
		values := map[string]interface{}{
			"dept_id": int64(1),
			// Missing emp_id
			"name": "Alice",
		}

		_, err := pk.ValidateInsert(values, false, 0)
		if err == nil {
			t.Error("Expected error for missing PRIMARY KEY column, got nil")
		}
	})

	t.Run("NULL not allowed in PRIMARY KEY columns", func(t *testing.T) {
		values := map[string]interface{}{
			"dept_id": int64(1),
			"emp_id":  nil,
			"name":    "Bob",
		}

		_, err := pk.ValidateInsert(values, false, 0)
		if err == nil {
			t.Error("Expected error for NULL PRIMARY KEY column, got nil")
		}
	})

	t.Run("valid composite PRIMARY KEY", func(t *testing.T) {
		values := map[string]interface{}{
			"dept_id": int64(1),
			"emp_id":  int64(100),
			"name":    "Charlie",
		}

		rowid, err := pk.ValidateInsert(values, false, 0)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if rowid <= 0 {
			t.Errorf("Expected positive rowid, got: %d", rowid)
		}
	})
}

// TestPrimaryKeyConstraint_NoPrimaryKey tests tables without PRIMARY KEY.
func TestPrimaryKeyConstraint_NoPrimaryKey(t *testing.T) {
	columns := []*schema.Column{
		{
			Name: "name",
			Type: "TEXT",
		},
		{
			Name: "age",
			Type: "INTEGER",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	t.Run("auto-generate rowid for table without PRIMARY KEY", func(t *testing.T) {
		values := map[string]interface{}{
			"name": "Alice",
			"age":  int64(30),
		}

		rowid, err := pk.ValidateInsert(values, false, 0)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if rowid != 1 {
			t.Errorf("Expected auto-generated rowid 1, got: %d", rowid)
		}
	})

	t.Run("use explicit rowid when provided", func(t *testing.T) {
		values := map[string]interface{}{
			"name": "Bob",
			"age":  int64(25),
		}

		rowid, err := pk.ValidateInsert(values, true, 999)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if rowid != 999 {
			t.Errorf("Expected rowid 999, got: %d", rowid)
		}
	})
}

// TestPrimaryKeyConstraint_Update tests UPDATE validation.
func TestPrimaryKeyConstraint_Update(t *testing.T) {
	columns := []*schema.Column{
		{
			Name:       "id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: "TEXT",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{"id"})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// Insert initial row
	cursor := btree.NewCursor(bt, table.RootPage)
	if err := cursor.Insert(10, []byte("initial_payload")); err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	t.Run("update non-PRIMARY KEY column allowed", func(t *testing.T) {
		newValues := map[string]interface{}{
			"name": "Updated Name",
		}

		err := pk.ValidateUpdate(10, newValues)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("update PRIMARY KEY to same value allowed", func(t *testing.T) {
		newValues := map[string]interface{}{
			"id": int64(10),
		}

		err := pk.ValidateUpdate(10, newValues)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("update PRIMARY KEY to NULL not allowed", func(t *testing.T) {
		newValues := map[string]interface{}{
			"id": nil,
		}

		err := pk.ValidateUpdate(10, newValues)
		if err == nil {
			t.Error("Expected error for NULL PRIMARY KEY, got nil")
		}
	})

	t.Run("update PRIMARY KEY to new unique value allowed", func(t *testing.T) {
		newValues := map[string]interface{}{
			"id": int64(20),
		}

		err := pk.ValidateUpdate(10, newValues)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("update PRIMARY KEY to duplicate value not allowed", func(t *testing.T) {
		// Insert another row with id=30
		if err := cursor.Insert(30, []byte("another_payload")); err != nil {
			t.Fatalf("Failed to insert second row: %v", err)
		}

		// Try to update row 10 to id=30 (duplicate)
		newValues := map[string]interface{}{
			"id": int64(30),
		}

		err := pk.ValidateUpdate(10, newValues)
		if err == nil {
			t.Error("Expected error for duplicate PRIMARY KEY, got nil")
		}
	})
}

// TestPrimaryKeyConstraint_RowidGeneration tests automatic rowid generation.
func TestPrimaryKeyConstraint_RowidGeneration(t *testing.T) {
	columns := []*schema.Column{
		{
			Name: "data",
			Type: "TEXT",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	t.Run("sequential rowid generation", func(t *testing.T) {
		cursor := btree.NewCursor(bt, table.RootPage)

		// Generate and insert multiple rows
		for i := 1; i <= 5; i++ {
			values := map[string]interface{}{
				"data": "test",
			}

			rowid, err := pk.ValidateInsert(values, false, 0)
			if err != nil {
				t.Fatalf("Failed to generate rowid: %v", err)
			}

			if rowid != int64(i) {
				t.Errorf("Expected rowid %d, got: %d", i, rowid)
			}

			// Actually insert to update table state
			if err := cursor.Insert(rowid, []byte("payload")); err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}
	})

	t.Run("generate rowid with gaps - uses max+1", func(t *testing.T) {
		// Create table with gaps: 1, 2, 4, 5 (missing 3)
		// SQLite behavior: generateRowid() uses max+1, NOT gap filling
		// Gap filling only happens when max rowid hits int64 limit
		columns := []*schema.Column{
			{Name: "data", Type: "TEXT"},
		}
		table2, bt2, cleanup2 := setupTestTable(t, columns, []string{})
		defer cleanup2()

		cursor := btree.NewCursor(bt2, table2.RootPage)
		cursor.Insert(1, []byte("payload"))
		cursor.Insert(2, []byte("payload"))
		cursor.Insert(4, []byte("payload"))
		cursor.Insert(5, []byte("payload"))

		pk2 := NewPrimaryKeyConstraint(table2, bt2, nil)

		// Should return max+1 (6), not the gap (3)
		// This matches SQLite behavior
		rowid, err := pk2.generateRowid()
		if err != nil {
			t.Fatalf("Failed to generate rowid: %v", err)
		}

		if rowid != 6 {
			t.Errorf("Expected max+1 rowid 6, got: %d", rowid)
		}
	})
}

// TestPrimaryKeyConstraint_AutoIncrement tests AUTOINCREMENT behavior.
func TestPrimaryKeyConstraint_AutoIncrement(t *testing.T) {
	columns := []*schema.Column{
		{
			Name:          "id",
			Type:          "INTEGER",
			PrimaryKey:    true,
			Autoincrement: true,
		},
		{
			Name: "name",
			Type: "TEXT",
		},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{"id"})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	t.Run("has autoincrement", func(t *testing.T) {
		if !pk.HasAutoIncrement() {
			t.Error("Expected HasAutoIncrement to return true")
		}
	})

	t.Run("autoincrement prevents reusing deleted rowids", func(t *testing.T) {
		// This is a placeholder test - full AUTOINCREMENT implementation
		// would require tracking max rowid in sqlite_sequence table
		// For now, just verify the flag is recognized
		if !pk.HasAutoIncrement() {
			t.Error("Expected AUTOINCREMENT flag to be set")
		}
	})
}

// TestPrimaryKeyConstraint_Helpers tests helper methods.
func TestPrimaryKeyConstraint_Helpers(t *testing.T) {
	columns := []*schema.Column{
		{
			Name:       "dept_id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name:       "emp_id",
			Type:       "INTEGER",
			PrimaryKey: true,
		},
		{
			Name: "name",
			Type: "TEXT",
		},
	}

	table := createTestTablePK(columns, []string{"dept_id", "emp_id"})
	pk := NewPrimaryKeyConstraint(table, nil, nil)

	t.Run("GetPrimaryKeyColumns", func(t *testing.T) {
		pkCols := pk.GetPrimaryKeyColumns()
		if len(pkCols) != 2 {
			t.Errorf("Expected 2 PRIMARY KEY columns, got: %d", len(pkCols))
		}
		if pkCols[0] != "dept_id" || pkCols[1] != "emp_id" {
			t.Errorf("Unexpected PRIMARY KEY columns: %v", pkCols)
		}
	})

	t.Run("isIntegerPrimaryKey", func(t *testing.T) {
		if pk.isIntegerPrimaryKey() {
			t.Error("Expected isIntegerPrimaryKey to return false for composite key")
		}

		// Test with single INTEGER PRIMARY KEY
		singlePKCols := []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		}
		singlePKTable := createTestTablePK(singlePKCols, []string{"id"})
		singlePK := NewPrimaryKeyConstraint(singlePKTable, nil, nil)
		if !singlePK.isIntegerPrimaryKey() {
			t.Error("Expected isIntegerPrimaryKey to return true for single INTEGER PRIMARY KEY")
		}
	})
}

// TestPrimaryKeyConstraint_SimplifiedLogic tests constraint logic without btree.
func TestPrimaryKeyConstraint_SimplifiedLogic(t *testing.T) {
	t.Run("isIntegerPrimaryKey detection", func(t *testing.T) {
		testCases := []struct {
			name     string
			columns  []*schema.Column
			pkCols   []string
			expected bool
		}{
			{
				name: "single INTEGER PRIMARY KEY",
				columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
				},
				pkCols:   []string{"id"},
				expected: true,
			},
			{
				name: "single INT PRIMARY KEY",
				columns: []*schema.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
				},
				pkCols:   []string{"id"},
				expected: true,
			},
			{
				name: "single TEXT PRIMARY KEY",
				columns: []*schema.Column{
					{Name: "code", Type: "TEXT", PrimaryKey: true},
				},
				pkCols:   []string{"code"},
				expected: false,
			},
			{
				name: "composite PRIMARY KEY",
				columns: []*schema.Column{
					{Name: "dept", Type: "INTEGER", PrimaryKey: true},
					{Name: "emp", Type: "INTEGER", PrimaryKey: true},
				},
				pkCols:   []string{"dept", "emp"},
				expected: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				table := createTestTablePK(tc.columns, tc.pkCols)
				pk := NewPrimaryKeyConstraint(table, nil, nil)
				result := pk.isIntegerPrimaryKey()
				if result != tc.expected {
					t.Errorf("Expected isIntegerPrimaryKey=%v, got %v", tc.expected, result)
				}
			})
		}
	})

	t.Run("GetPrimaryKeyColumns", func(t *testing.T) {
		columns := []*schema.Column{
			{Name: "a", Type: "INTEGER", PrimaryKey: true},
			{Name: "b", Type: "TEXT", PrimaryKey: true},
			{Name: "c", Type: "TEXT"},
		}
		table := createTestTablePK(columns, []string{"a", "b"})
		pk := NewPrimaryKeyConstraint(table, nil, nil)

		pkCols := pk.GetPrimaryKeyColumns()
		if len(pkCols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(pkCols))
		}
		if pkCols[0] != "a" || pkCols[1] != "b" {
			t.Errorf("Expected [a, b], got %v", pkCols)
		}
	})

	t.Run("HasAutoIncrement", func(t *testing.T) {
		testCases := []struct {
			name     string
			columns  []*schema.Column
			pkCols   []string
			expected bool
		}{
			{
				name: "AUTOINCREMENT set",
				columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: true},
				},
				pkCols:   []string{"id"},
				expected: true,
			},
			{
				name: "no AUTOINCREMENT",
				columns: []*schema.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: false},
				},
				pkCols:   []string{"id"},
				expected: false,
			},
			{
				name: "composite key",
				columns: []*schema.Column{
					{Name: "a", Type: "INTEGER", PrimaryKey: true, Autoincrement: true},
					{Name: "b", Type: "INTEGER", PrimaryKey: true},
				},
				pkCols:   []string{"a", "b"},
				expected: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				table := createTestTablePK(tc.columns, tc.pkCols)
				pk := NewPrimaryKeyConstraint(table, nil, nil)
				result := pk.HasAutoIncrement()
				if result != tc.expected {
					t.Errorf("Expected HasAutoIncrement=%v, got %v", tc.expected, result)
				}
			})
		}
	})
}

// TestPrimaryKeyConstraint_TypeConversion tests type conversion for INTEGER PRIMARY KEY.
func TestPrimaryKeyConstraint_TypeConversion(t *testing.T) {
	columns := []*schema.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
	}

	table, bt, cleanup := setupTestTable(t, columns, []string{"id"})
	defer cleanup()

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	testCases := []struct {
		name      string
		value     interface{}
		wantValue int64
		wantError bool
	}{
		{"int64", int64(42), 42, false},
		{"int", int(42), 42, false},
		{"int32", int32(42), 42, false},
		{"uint32", uint32(42), 42, false},
		{"float64", float64(42.7), 42, false},
		{"string", "not a number", 0, true},
		{"bool", true, 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := pk.convertToInt64(tc.value)

			if tc.wantError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != tc.wantValue {
					t.Errorf("Expected %d, got %d", tc.wantValue, result)
				}
			}
		})
	}
}
