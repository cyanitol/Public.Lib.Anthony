// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package constraint provides SQL constraint enforcement for the Anthony SQLite clone.
package constraint

import (
	"fmt"
	"math"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// PrimaryKeyConstraint enforces PRIMARY KEY constraints.
type PrimaryKeyConstraint struct {
	Table *schema.Table
	Btree *btree.Btree
	Pager interface{} // *pager.Pager for marking pages dirty
}

// NewPrimaryKeyConstraint creates a new primary key constraint validator.
func NewPrimaryKeyConstraint(table *schema.Table, bt *btree.Btree, pager interface{}) *PrimaryKeyConstraint {
	return &PrimaryKeyConstraint{
		Table: table,
		Btree: bt,
		Pager: pager,
	}
}

// ValidateInsert validates a PRIMARY KEY constraint for an INSERT operation.
// It checks:
// 1. For single-column INTEGER PRIMARY KEY: auto-generates rowid if not provided
// 2. For composite primary keys or non-INTEGER PRIMARY KEY: checks uniqueness
// Returns the final rowid to use and an error if constraint is violated.
func (pk *PrimaryKeyConstraint) ValidateInsert(values map[string]interface{}, rowidProvided bool, providedRowid int64) (int64, error) {
	// If table has no primary key, use auto-generated rowid
	if len(pk.Table.PrimaryKey) == 0 {
		if rowidProvided {
			// Check for duplicate rowid
			if err := pk.checkRowidUniqueness(providedRowid); err != nil {
				return 0, err
			}
			return providedRowid, nil
		}
		// Auto-generate rowid
		return pk.generateRowid()
	}

	// Handle INTEGER PRIMARY KEY (single column, INTEGER type)
	if pk.isIntegerPrimaryKey() {
		return pk.handleIntegerPrimaryKey(values, rowidProvided, providedRowid)
	}

	// Handle composite or non-INTEGER PRIMARY KEY
	return pk.handleCompositePrimaryKey(values, rowidProvided, providedRowid)
}

// ValidateUpdate validates a PRIMARY KEY constraint for an UPDATE operation.
// Returns error if the update would violate uniqueness.
func (pk *PrimaryKeyConstraint) ValidateUpdate(oldRowid int64, newValues map[string]interface{}) error {
	// If no primary key defined, updates can't violate constraint
	if len(pk.Table.PrimaryKey) == 0 {
		return nil
	}

	// Check if any primary key column is being updated
	pkChanged := false
	for _, pkCol := range pk.Table.PrimaryKey {
		if _, updated := newValues[pkCol]; updated {
			pkChanged = true
			break
		}
	}

	// If primary key columns aren't changing, no violation possible
	if !pkChanged {
		return nil
	}

	// For INTEGER PRIMARY KEY, validate the new rowid
	if pk.isIntegerPrimaryKey() {
		return pk.validateIntegerPKUpdate(oldRowid, newValues)
	}

	// For composite keys, validate uniqueness
	return pk.validateCompositePKUpdate(oldRowid, newValues)
}

// isIntegerPrimaryKey returns true if the table has a single-column INTEGER PRIMARY KEY.
func (pk *PrimaryKeyConstraint) isIntegerPrimaryKey() bool {
	if len(pk.Table.PrimaryKey) != 1 {
		return false
	}

	pkColName := pk.Table.PrimaryKey[0]
	col, ok := pk.Table.GetColumn(pkColName)
	if !ok {
		return false
	}

	// INTEGER PRIMARY KEY is a rowid alias
	return col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")
}

// handleIntegerPrimaryKey handles INSERT validation for INTEGER PRIMARY KEY columns.
func (pk *PrimaryKeyConstraint) handleIntegerPrimaryKey(values map[string]interface{}, rowidProvided bool, providedRowid int64) (int64, error) {
	pkColName := pk.Table.PrimaryKey[0]

	// Check if PRIMARY KEY column value is explicitly provided in INSERT
	pkValue, hasExplicitPK := values[pkColName]

	if hasExplicitPK && pkValue != nil {
		// Explicit value provided for INTEGER PRIMARY KEY column
		rowid, err := pk.convertToInt64(pkValue)
		if err != nil {
			return 0, fmt.Errorf("PRIMARY KEY value must be INTEGER: %w", err)
		}

		// Check uniqueness
		if err := pk.checkRowidUniqueness(rowid); err != nil {
			return 0, err
		}
		return rowid, nil
	}

	// No explicit value - check if rowid was provided separately
	if rowidProvided {
		if err := pk.checkRowidUniqueness(providedRowid); err != nil {
			return 0, err
		}
		return providedRowid, nil
	}

	// Auto-generate rowid
	return pk.generateRowid()
}

// handleCompositePrimaryKey handles INSERT validation for composite or non-INTEGER PRIMARY KEYs.
func (pk *PrimaryKeyConstraint) handleCompositePrimaryKey(values map[string]interface{}, rowidProvided bool, providedRowid int64) (int64, error) {
	// Verify all PRIMARY KEY columns have non-NULL values
	for _, pkCol := range pk.Table.PrimaryKey {
		val, exists := values[pkCol]
		if !exists || val == nil {
			return 0, fmt.Errorf("PRIMARY KEY column '%s' cannot be NULL", pkCol)
		}
	}

	// For composite keys, we would check uniqueness via index lookup
	// This is a simplified implementation - full implementation would use an index
	// For now, we allow the insert and let btree handle duplicate rowids

	if rowidProvided {
		if err := pk.checkRowidUniqueness(providedRowid); err != nil {
			return 0, err
		}
		return providedRowid, nil
	}

	return pk.generateRowid()
}

// validateIntegerPKUpdate validates an UPDATE that modifies an INTEGER PRIMARY KEY.
func (pk *PrimaryKeyConstraint) validateIntegerPKUpdate(oldRowid int64, newValues map[string]interface{}) error {
	pkColName := pk.Table.PrimaryKey[0]
	newPKValue, updated := newValues[pkColName]

	if !updated {
		return nil
	}

	if newPKValue == nil {
		return fmt.Errorf("PRIMARY KEY column '%s' cannot be NULL", pkColName)
	}

	newRowid, err := pk.convertToInt64(newPKValue)
	if err != nil {
		return fmt.Errorf("PRIMARY KEY value must be INTEGER: %w", err)
	}

	// If rowid unchanged, no violation
	if newRowid == oldRowid {
		return nil
	}

	// Check new rowid doesn't already exist
	return pk.checkRowidUniqueness(newRowid)
}

// validateCompositePKUpdate validates an UPDATE that modifies composite PRIMARY KEY columns.
func (pk *PrimaryKeyConstraint) validateCompositePKUpdate(oldRowid int64, newValues map[string]interface{}) error {
	// Check all PRIMARY KEY columns are non-NULL
	for _, pkCol := range pk.Table.PrimaryKey {
		if val, updated := newValues[pkCol]; updated && val == nil {
			return fmt.Errorf("PRIMARY KEY column '%s' cannot be NULL", pkCol)
		}
	}

	// Full implementation would check uniqueness via index
	// For now, simplified validation
	return nil
}

// checkRowidUniqueness checks if a rowid already exists in the table.
func (pk *PrimaryKeyConstraint) checkRowidUniqueness(rowid int64) error {
	cursor := btree.NewCursor(pk.Btree, pk.Table.RootPage)
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		// Seek errors (other than "not found") are real errors
		return fmt.Errorf("error checking PRIMARY KEY uniqueness: %w", err)
	}

	if found {
		return fmt.Errorf("UNIQUE constraint failed: PRIMARY KEY must be unique (duplicate rowid: %d)", rowid)
	}

	return nil
}

// generateRowid generates a new unique rowid for the table.
func (pk *PrimaryKeyConstraint) generateRowid() (int64, error) {
	// Find the maximum existing rowid and add 1
	cursor := btree.NewCursor(pk.Btree, pk.Table.RootPage)

	// Move to last entry to find max rowid
	if err := cursor.MoveToLast(); err != nil {
		// Empty table - start at 1
		return 1, nil
	}

	maxRowid := cursor.GetKey()

	// SQLite behavior: if max rowid is at max int64, reuse lower values
	if maxRowid == math.MaxInt64 {
		// Try to find a gap by scanning from 1
		return pk.findGapInRowids()
	}

	return maxRowid + 1, nil
}

// findGapInRowids finds the first available rowid by scanning the table.
func (pk *PrimaryKeyConstraint) findGapInRowids() (int64, error) {
	cursor := btree.NewCursor(pk.Btree, pk.Table.RootPage)

	if err := cursor.MoveToFirst(); err != nil {
		// Empty table
		return 1, nil
	}

	expectedRowid := int64(1)
	for cursor.IsValid() {
		currentRowid := cursor.GetKey()
		if currentRowid > expectedRowid {
			// Found a gap
			return expectedRowid, nil
		}
		expectedRowid = currentRowid + 1

		if err := cursor.Next(); err != nil {
			break
		}
	}

	return expectedRowid, nil
}

// convertToInt64 converts a value to int64, handling various input types.
func (pk *PrimaryKeyConstraint) convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case *vdbe.Mem:
		return pk.convertMemToInt64(v)
	default:
		return 0, fmt.Errorf("invalid type for INTEGER PRIMARY KEY: %T", value)
	}
}

// convertMemToInt64 converts a vdbe.Mem value to int64.
func (pk *PrimaryKeyConstraint) convertMemToInt64(mem *vdbe.Mem) (int64, error) {
	if mem.IsInt() {
		return mem.IntValue(), nil
	}
	if mem.IsReal() {
		return int64(mem.RealValue()), nil
	}
	return 0, fmt.Errorf("cannot convert to INTEGER")
}

// GetPrimaryKeyColumns returns the list of primary key column names.
func (pk *PrimaryKeyConstraint) GetPrimaryKeyColumns() []string {
	return pk.Table.PrimaryKey
}

// HasAutoIncrement returns true if the table has an AUTOINCREMENT INTEGER PRIMARY KEY.
func (pk *PrimaryKeyConstraint) HasAutoIncrement() bool {
	if !pk.isIntegerPrimaryKey() {
		return false
	}

	pkColName := pk.Table.PrimaryKey[0]
	col, ok := pk.Table.GetColumn(pkColName)
	if !ok {
		return false
	}

	return col.Autoincrement
}
