// Package constraint provides constraint validation for SQLite operations.
package constraint

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// NotNullConstraint represents a NOT NULL constraint validator.
type NotNullConstraint struct {
	table *schema.Table
}

// NewNotNullConstraint creates a new NOT NULL constraint validator for the given table.
func NewNotNullConstraint(table *schema.Table) *NotNullConstraint {
	return &NotNullConstraint{
		table: table,
	}
}

// ValidateInsert validates that all NOT NULL columns have non-NULL values for INSERT operations.
// It takes a map of column names to their values and returns an error if validation fails.
// DEFAULT values should be applied before calling this function.
func (nnc *NotNullConstraint) ValidateInsert(values map[string]interface{}) error {
	for _, col := range nnc.table.Columns {
		if !col.NotNull {
			continue
		}

		// Check if value is provided
		val, exists := values[col.Name]
		if !exists {
			return fmt.Errorf("NOT NULL constraint failed: column %s", col.Name)
		}

		// Check if value is NULL
		if val == nil {
			return fmt.Errorf("NOT NULL constraint failed: column %s", col.Name)
		}
	}

	return nil
}

// ValidateUpdate validates that all NOT NULL columns have non-NULL values for UPDATE operations.
// It takes a map of column names to their new values and returns an error if validation fails.
// Only columns being updated need to be checked - existing values are assumed to be valid.
// DEFAULT values should be applied before calling this function.
func (nnc *NotNullConstraint) ValidateUpdate(updates map[string]interface{}) error {
	for colName, val := range updates {
		// Find the column in the table schema
		col, ok := nnc.table.GetColumn(colName)
		if !ok {
			// Unknown column - this should be caught elsewhere
			continue
		}

		// Check if column has NOT NULL constraint
		if !col.NotNull {
			continue
		}

		// Check if value is NULL
		if val == nil {
			return fmt.Errorf("NOT NULL constraint failed: column %s", col.Name)
		}
	}

	return nil
}

// ApplyDefaults applies DEFAULT values to columns that are NULL or missing.
// This should be called before ValidateInsert or ValidateUpdate.
// It modifies the values map in-place, filling in defaults where needed.
func (nnc *NotNullConstraint) ApplyDefaults(values map[string]interface{}, isInsert bool) error {
	for _, col := range nnc.table.Columns {
		// Skip if value is already provided and not NULL
		if val, exists := values[col.Name]; exists && val != nil {
			continue
		}

		// Skip if no default value is defined
		if col.Default == nil {
			continue
		}

		// Apply the default value
		// Note: col.Default is stored as interface{} which could be:
		// - A string representation of the default expression
		// - A literal value
		// For Phase 2, we'll handle simple literal defaults
		values[col.Name] = col.Default
	}

	return nil
}

// ValidateRow performs complete NOT NULL validation for a row.
// It first applies defaults, then validates NOT NULL constraints.
// This is a convenience method that combines ApplyDefaults and ValidateInsert.
func (nnc *NotNullConstraint) ValidateRow(values map[string]interface{}) error {
	// Apply defaults first
	if err := nnc.ApplyDefaults(values, true); err != nil {
		return err
	}

	// Validate NOT NULL constraints
	return nnc.ValidateInsert(values)
}

// GetNotNullColumns returns a list of column names that have NOT NULL constraints.
func (nnc *NotNullConstraint) GetNotNullColumns() []string {
	var notNullCols []string
	for _, col := range nnc.table.Columns {
		if col.NotNull {
			notNullCols = append(notNullCols, col.Name)
		}
	}
	return notNullCols
}

// HasNotNullConstraint checks if a specific column has a NOT NULL constraint.
func (nnc *NotNullConstraint) HasNotNullConstraint(columnName string) bool {
	col, ok := nnc.table.GetColumn(columnName)
	if !ok {
		return false
	}
	return col.NotNull
}
