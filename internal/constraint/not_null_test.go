// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package constraint

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// createTestTable creates a test table with various column configurations
func createTestTable() *schema.Table {
	return &schema.Table{
		Name: "test_table",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true, PrimaryKey: true},
			{Name: "name", Type: "TEXT", NotNull: true},
			{Name: "email", Type: "TEXT", NotNull: true, Default: "unknown@example.com"},
			{Name: "age", Type: "INTEGER", NotNull: false},
			{Name: "status", Type: "TEXT", NotNull: true, Default: "active"},
			{Name: "description", Type: "TEXT", NotNull: false, Default: "No description"},
		},
	}
}

// TestNewNotNullConstraint tests the constructor
func TestNewNotNullConstraint(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	if nnc == nil {
		t.Fatal("NewNotNullConstraint returned nil")
	}

	if nnc.table != table {
		t.Error("NotNullConstraint table reference is incorrect")
	}
}

// TestValidateInsert_ValidData tests INSERT validation with valid data
func TestValidateInsert_ValidData(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name   string
		values map[string]interface{}
	}{
		{
			name: "all required columns provided",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
		},
		{
			name: "all columns including optional",
			values: map[string]interface{}{
				"id":          2,
				"name":        "Jane Smith",
				"email":       "jane@example.com",
				"age":         30,
				"status":      "inactive",
				"description": "A test user",
			},
		},
		{
			name: "optional columns as NULL",
			values: map[string]interface{}{
				"id":          3,
				"name":        "Bob Wilson",
				"email":       "bob@example.com",
				"age":         nil,
				"status":      "active",
				"description": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateInsert(tt.values)
			if err != nil {
				t.Errorf("ValidateInsert failed for valid data: %v", err)
			}
		})
	}
}

// TestValidateInsert_MissingNotNullColumn tests INSERT validation with missing NOT NULL columns
func TestValidateInsert_MissingNotNullColumn(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name          string
		values        map[string]interface{}
		expectedError string
	}{
		{
			name: "missing id column",
			values: map[string]interface{}{
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column id",
		},
		{
			name: "missing name column",
			values: map[string]interface{}{
				"id":     1,
				"email":  "john@example.com",
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column name",
		},
		{
			name: "missing email column",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column email",
		},
		{
			name: "missing status column",
			values: map[string]interface{}{
				"id":    1,
				"name":  "John Doe",
				"email": "john@example.com",
			},
			expectedError: "NOT NULL constraint failed: column status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateInsert(tt.values)
			if err == nil {
				t.Error("ValidateInsert should have failed for missing NOT NULL column")
			} else if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Expected error containing %q, got %q", tt.expectedError, err.Error())
			}
		})
	}
}

// TestValidateInsert_NullNotNullColumn tests INSERT validation with NULL in NOT NULL columns
func TestValidateInsert_NullNotNullColumn(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name          string
		values        map[string]interface{}
		expectedError string
	}{
		{
			name: "NULL id",
			values: map[string]interface{}{
				"id":     nil,
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column id",
		},
		{
			name: "NULL name",
			values: map[string]interface{}{
				"id":     1,
				"name":   nil,
				"email":  "john@example.com",
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column name",
		},
		{
			name: "NULL email",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  nil,
				"status": "active",
			},
			expectedError: "NOT NULL constraint failed: column email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateInsert(tt.values)
			if err == nil {
				t.Error("ValidateInsert should have failed for NULL in NOT NULL column")
			} else if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Expected error containing %q, got %q", tt.expectedError, err.Error())
			}
		})
	}
}

// TestValidateUpdate_ValidData tests UPDATE validation with valid data
func TestValidateUpdate_ValidData(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name    string
		updates map[string]interface{}
	}{
		{
			name: "update single column",
			updates: map[string]interface{}{
				"name": "Updated Name",
			},
		},
		{
			name: "update multiple columns",
			updates: map[string]interface{}{
				"name":  "Jane Doe",
				"email": "jane.doe@example.com",
			},
		},
		{
			name: "update nullable column to NULL",
			updates: map[string]interface{}{
				"age": nil,
			},
		},
		{
			name: "update nullable column",
			updates: map[string]interface{}{
				"description": "New description",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateUpdate(tt.updates)
			if err != nil {
				t.Errorf("ValidateUpdate failed for valid data: %v", err)
			}
		})
	}
}

// TestValidateUpdate_NullNotNullColumn tests UPDATE validation with NULL in NOT NULL columns
func TestValidateUpdate_NullNotNullColumn(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name          string
		updates       map[string]interface{}
		expectedError string
	}{
		{
			name: "update name to NULL",
			updates: map[string]interface{}{
				"name": nil,
			},
			expectedError: "NOT NULL constraint failed: column name",
		},
		{
			name: "update email to NULL",
			updates: map[string]interface{}{
				"email": nil,
			},
			expectedError: "NOT NULL constraint failed: column email",
		},
		{
			name: "update status to NULL",
			updates: map[string]interface{}{
				"status": nil,
			},
			expectedError: "NOT NULL constraint failed: column status",
		},
		{
			name: "update multiple columns with one NULL",
			updates: map[string]interface{}{
				"name":  "Valid Name",
				"email": nil,
			},
			expectedError: "NOT NULL constraint failed: column email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateUpdate(tt.updates)
			if err == nil {
				t.Error("ValidateUpdate should have failed for NULL in NOT NULL column")
			} else if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Expected error containing %q, got %q", tt.expectedError, err.Error())
			}
		})
	}
}

// TestNotNullApplyDefaults tests DEFAULT value application for NOT NULL constraints
func TestNotNullApplyDefaults(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name     string
		values   map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "apply default for missing email",
			values: map[string]interface{}{
				"id":   1,
				"name": "John Doe",
			},
			expected: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "unknown@example.com",
				"status": "active",
			},
		},
		{
			name: "apply default for missing status",
			values: map[string]interface{}{
				"id":    1,
				"name":  "John Doe",
				"email": "john@example.com",
			},
			expected: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
		},
		{
			name: "apply multiple defaults",
			values: map[string]interface{}{
				"id":   1,
				"name": "John Doe",
			},
			expected: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "unknown@example.com",
				"status": "active",
			},
		},
		{
			name: "do not override explicit values",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "custom@example.com",
				"status": "inactive",
			},
			expected: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "custom@example.com",
				"status": "inactive",
			},
		},
		{
			name: "apply default for NULL value",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  nil,
				"status": nil,
			},
			expected: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "unknown@example.com",
				"status": "active",
			},
		},
		{
			name: "apply default for nullable column",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
			expected: map[string]interface{}{
				"id":          1,
				"name":        "John Doe",
				"email":       "john@example.com",
				"status":      "active",
				"description": "No description",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ApplyDefaults(tt.values, true)
			if err != nil {
				t.Errorf("ApplyDefaults failed: %v", err)
			}

			// Check that all expected values are present
			for key, expectedVal := range tt.expected {
				actualVal, exists := tt.values[key]
				if !exists {
					t.Errorf("Expected key %q not found in values", key)
				} else if actualVal != expectedVal {
					t.Errorf("For key %q, expected %v, got %v", key, expectedVal, actualVal)
				}
			}
		})
	}
}

// TestValidateRow tests complete row validation with defaults
func TestValidateRow(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name        string
		values      map[string]interface{}
		shouldError bool
		errorMsg    string
	}{
		{
			name: "valid row with all required fields",
			values: map[string]interface{}{
				"id":     1,
				"name":   "John Doe",
				"email":  "john@example.com",
				"status": "active",
			},
			shouldError: false,
		},
		{
			name: "valid row with defaults applied",
			values: map[string]interface{}{
				"id":   2,
				"name": "Jane Smith",
			},
			shouldError: false,
		},
		{
			name: "invalid row missing non-default NOT NULL column",
			values: map[string]interface{}{
				"id": 3,
			},
			shouldError: true,
			errorMsg:    "NOT NULL constraint failed: column name",
		},
		{
			name: "invalid row with NULL in non-default NOT NULL column",
			values: map[string]interface{}{
				"id":   4,
				"name": nil,
			},
			shouldError: true,
			errorMsg:    "NOT NULL constraint failed: column name",
		},
		{
			name: "valid row defaults fill all missing NOT NULL columns",
			values: map[string]interface{}{
				"id":   5,
				"name": "Test User",
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateRow(tt.values)
			if tt.shouldError {
				if err == nil {
					t.Error("ValidateRow should have failed but didn't")
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("ValidateRow failed unexpectedly: %v", err)
				}
			}
		})
	}
}

// TestGetNotNullColumns tests retrieval of NOT NULL column names
func TestGetNotNullColumns(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	notNullCols := nnc.GetNotNullColumns()

	expectedCols := []string{"id", "name", "email", "status"}

	if len(notNullCols) != len(expectedCols) {
		t.Errorf("Expected %d NOT NULL columns, got %d", len(expectedCols), len(notNullCols))
	}

	// Check that all expected columns are present
	colMap := make(map[string]bool)
	for _, col := range notNullCols {
		colMap[col] = true
	}

	for _, expected := range expectedCols {
		if !colMap[expected] {
			t.Errorf("Expected NOT NULL column %q not found", expected)
		}
	}
}

// TestHasNotNullConstraint tests checking if a column has NOT NULL constraint
func TestHasNotNullConstraint(t *testing.T) {
	table := createTestTable()
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		columnName string
		expected   bool
	}{
		{"id", true},
		{"name", true},
		{"email", true},
		{"age", false},
		{"status", true},
		{"description", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.columnName, func(t *testing.T) {
			result := nnc.HasNotNullConstraint(tt.columnName)
			if result != tt.expected {
				t.Errorf("HasNotNullConstraint(%q) = %v, expected %v", tt.columnName, result, tt.expected)
			}
		})
	}
}

// TestValidateInsert_EmptyTable tests validation with a table that has no NOT NULL constraints
func TestValidateInsert_EmptyTable(t *testing.T) {
	table := &schema.Table{
		Name: "no_constraints",
		Columns: []*schema.Column{
			{Name: "col1", Type: "TEXT", NotNull: false},
			{Name: "col2", Type: "INTEGER", NotNull: false},
		},
	}
	nnc := NewNotNullConstraint(table)

	// Should succeed even with empty values
	err := nnc.ValidateInsert(map[string]interface{}{})
	if err != nil {
		t.Errorf("ValidateInsert should succeed for table with no NOT NULL constraints: %v", err)
	}

	// Should succeed with NULL values
	err = nnc.ValidateInsert(map[string]interface{}{
		"col1": nil,
		"col2": nil,
	})
	if err != nil {
		t.Errorf("ValidateInsert should succeed for NULL values when no NOT NULL constraints: %v", err)
	}
}

// TestValidateInsert_AllNotNull tests validation with a table where all columns are NOT NULL
func TestValidateInsert_AllNotNull(t *testing.T) {
	table := &schema.Table{
		Name: "all_not_null",
		Columns: []*schema.Column{
			{Name: "col1", Type: "TEXT", NotNull: true},
			{Name: "col2", Type: "INTEGER", NotNull: true},
			{Name: "col3", Type: "REAL", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)

	// Should fail with empty values
	err := nnc.ValidateInsert(map[string]interface{}{})
	if err == nil {
		t.Error("ValidateInsert should fail when all columns are NOT NULL and no values provided")
	}

	// Should fail with partial values
	err = nnc.ValidateInsert(map[string]interface{}{
		"col1": "value",
	})
	if err == nil {
		t.Error("ValidateInsert should fail when some NOT NULL columns are missing")
	}

	// Should succeed with all values
	err = nnc.ValidateInsert(map[string]interface{}{
		"col1": "value",
		"col2": 42,
		"col3": 3.14,
	})
	if err != nil {
		t.Errorf("ValidateInsert should succeed when all NOT NULL columns have values: %v", err)
	}
}
