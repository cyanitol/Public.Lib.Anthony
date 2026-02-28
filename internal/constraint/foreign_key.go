// Package constraint provides constraint enforcement for SQLite databases.
// It implements CHECK, FOREIGN KEY, and other constraint validations.
package constraint

import (
	"fmt"
	"strings"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// ForeignKeyAction defines the action to take on DELETE/UPDATE.
type ForeignKeyAction int

const (
	FKActionNone ForeignKeyAction = iota
	FKActionSetNull
	FKActionSetDefault
	FKActionCascade
	FKActionRestrict
	FKActionNoAction
)

// DeferrableMode defines when constraint checking occurs.
type DeferrableMode int

const (
	DeferrableNone DeferrableMode = iota
	DeferrableInitiallyDeferred
	DeferrableInitiallyImmediate
)

// ForeignKeyConstraint represents a single FOREIGN KEY constraint.
type ForeignKeyConstraint struct {
	// Source table and columns
	Table   string   // Table this constraint belongs to
	Columns []string // Columns in this table

	// Referenced table and columns
	RefTable   string   // Referenced table name
	RefColumns []string // Referenced columns (empty means PRIMARY KEY)

	// Actions
	OnDelete ForeignKeyAction // Action on DELETE
	OnUpdate ForeignKeyAction // Action on UPDATE

	// Deferral mode
	Deferrable DeferrableMode

	// Name (optional)
	Name string
}

// ForeignKeyManager manages foreign key constraints for a database.
type ForeignKeyManager struct {
	constraints map[string][]*ForeignKeyConstraint // table name -> constraints
	enabled     bool                               // PRAGMA foreign_keys setting
	mu          sync.RWMutex
}

// NewForeignKeyManager creates a new foreign key constraint manager.
func NewForeignKeyManager() *ForeignKeyManager {
	return &ForeignKeyManager{
		constraints: make(map[string][]*ForeignKeyConstraint),
		enabled:     false, // Off by default in SQLite
	}
}

// SetEnabled sets the foreign_keys pragma value.
func (m *ForeignKeyManager) SetEnabled(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = enabled
}

// IsEnabled returns the current foreign_keys pragma value.
func (m *ForeignKeyManager) IsEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabled
}

// AddConstraint adds a foreign key constraint for a table.
func (m *ForeignKeyManager) AddConstraint(constraint *ForeignKeyConstraint) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableName := strings.ToLower(constraint.Table)
	m.constraints[tableName] = append(m.constraints[tableName], constraint)
}

// GetConstraints returns all foreign key constraints for a table.
func (m *ForeignKeyManager) GetConstraints(tableName string) []*ForeignKeyConstraint {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableName = strings.ToLower(tableName)
	return m.constraints[tableName]
}

// RemoveConstraints removes all foreign key constraints for a table.
// Called when a table is dropped.
func (m *ForeignKeyManager) RemoveConstraints(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableName = strings.ToLower(tableName)
	delete(m.constraints, tableName)
}

// ValidateInsert validates foreign key constraints for an INSERT operation.
// Returns an error if any constraint is violated.
func (m *ForeignKeyManager) ValidateInsert(
	tableName string,
	values map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
) error {
	if !m.IsEnabled() {
		return nil // Foreign keys disabled
	}

	constraints := m.GetConstraints(tableName)
	if len(constraints) == 0 {
		return nil // No constraints to check
	}

	for _, fk := range constraints {
		// Check if this constraint is deferred
		if fk.Deferrable == DeferrableInitiallyDeferred {
			// Deferred constraints are checked at COMMIT time
			continue
		}

		// Extract foreign key column values from the new row
		fkValues := make([]interface{}, len(fk.Columns))
		hasNull := false
		for i, col := range fk.Columns {
			val, ok := values[col]
			if !ok || val == nil {
				hasNull = true
				break
			}
			fkValues[i] = val
		}

		// NULL values in foreign key columns don't need to reference anything
		if hasNull {
			continue
		}

		// Check if referenced row exists
		if err := m.validateReference(fk, fkValues, schemaObj, rowReader); err != nil {
			return err
		}
	}

	return nil
}

// ValidateUpdate validates foreign key constraints for an UPDATE operation.
// Returns an error if any constraint is violated.
func (m *ForeignKeyManager) ValidateUpdate(
	tableName string,
	oldValues map[string]interface{},
	newValues map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowUpdater RowUpdater,
) error {
	if !m.IsEnabled() {
		return nil
	}

	// 1. Check outgoing foreign keys (this table references others)
	constraints := m.GetConstraints(tableName)
	for _, fk := range constraints {
		if fk.Deferrable == DeferrableInitiallyDeferred {
			continue
		}

		// Check if any FK columns were updated
		updated := false
		for _, col := range fk.Columns {
			if oldValues[col] != newValues[col] {
				updated = true
				break
			}
		}

		if !updated {
			continue // FK columns unchanged
		}

		// Extract new foreign key values
		fkValues := make([]interface{}, len(fk.Columns))
		hasNull := false
		for i, col := range fk.Columns {
			val, ok := newValues[col]
			if !ok || val == nil {
				hasNull = true
				break
			}
			fkValues[i] = val
		}

		if hasNull {
			continue // NULL is always valid
		}

		// Validate the new reference exists
		if err := m.validateReference(fk, fkValues, schemaObj, rowReader); err != nil {
			return err
		}
	}

	// 2. Check incoming foreign keys (other tables reference this one)
	// This is needed to enforce RESTRICT/NO ACTION and perform CASCADE/SET NULL/SET DEFAULT
	if err := m.validateIncomingReferences(tableName, oldValues, newValues, schemaObj, rowReader, rowUpdater); err != nil {
		return err
	}

	return nil
}

// ValidateDelete validates foreign key constraints for a DELETE operation.
// Returns an error if any constraint is violated, or performs cascade actions.
func (m *ForeignKeyManager) ValidateDelete(
	tableName string,
	values map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
) error {
	if !m.IsEnabled() {
		return nil
	}

	// Find all foreign keys that reference this table
	var referencingConstraints []*ForeignKeyConstraint
	for _, fks := range m.constraints {
		for _, fk := range fks {
			if strings.ToLower(fk.RefTable) == strings.ToLower(tableName) {
				referencingConstraints = append(referencingConstraints, fk)
			}
		}
	}

	if len(referencingConstraints) == 0 {
		return nil // Nothing references this table
	}

	// Get the table to find primary key
	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	// For each referencing constraint, handle based on ON DELETE action
	for _, fk := range referencingConstraints {
		// Determine which columns are being referenced
		refCols := fk.RefColumns
		if len(refCols) == 0 {
			refCols = table.PrimaryKey
		}

		// Extract the key values being deleted
		keyValues := make([]interface{}, len(refCols))
		for i, col := range refCols {
			keyValues[i] = values[col]
		}

		// Check if any rows reference this one
		referencingRows, err := rowReader.FindReferencingRows(fk.Table, fk.Columns, keyValues)
		if err != nil {
			return fmt.Errorf("failed to check foreign key references: %w", err)
		}

		if len(referencingRows) == 0 {
			continue // No references, OK to delete
		}

		// Handle based on ON DELETE action
		switch fk.OnDelete {
		case FKActionRestrict, FKActionNoAction:
			return fmt.Errorf("FOREIGN KEY constraint failed: table %s has referencing rows", fk.Table)

		case FKActionCascade:
			// Delete all referencing rows
			for _, rowID := range referencingRows {
				if err := rowDeleter.DeleteRow(fk.Table, rowID); err != nil {
					return fmt.Errorf("CASCADE DELETE failed: %w", err)
				}
			}

		case FKActionSetNull:
			// Set foreign key columns to NULL in referencing rows
			nullValues := make(map[string]interface{})
			for _, col := range fk.Columns {
				nullValues[col] = nil
			}
			for _, rowID := range referencingRows {
				if err := rowUpdater.UpdateRow(fk.Table, rowID, nullValues); err != nil {
					return fmt.Errorf("SET NULL failed: %w", err)
				}
			}

		case FKActionSetDefault:
			// Set foreign key columns to their DEFAULT values
			// This requires reading the column defaults from schema
			refTable, ok := schemaObj.GetTable(fk.Table)
			if !ok {
				return fmt.Errorf("referencing table not found: %s", fk.Table)
			}

			defaultValues := make(map[string]interface{})
			for _, col := range fk.Columns {
				column, ok := refTable.GetColumn(col)
				if !ok {
					return fmt.Errorf("column not found: %s", col)
				}
				defaultValues[col] = column.Default
			}

			for _, rowID := range referencingRows {
				if err := rowUpdater.UpdateRow(fk.Table, rowID, defaultValues); err != nil {
					return fmt.Errorf("SET DEFAULT failed: %w", err)
				}
			}
		}
	}

	return nil
}

// validateReference checks if a referenced row exists.
func (m *ForeignKeyManager) validateReference(
	fk *ForeignKeyConstraint,
	values []interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
) error {
	// Get referenced table
	refTable, ok := schemaObj.GetTable(fk.RefTable)
	if !ok {
		return fmt.Errorf("referenced table not found: %s", fk.RefTable)
	}

	// Determine referenced columns (PRIMARY KEY if not specified)
	refCols := fk.RefColumns
	if len(refCols) == 0 {
		refCols = refTable.PrimaryKey
	}

	if len(refCols) != len(values) {
		return fmt.Errorf("foreign key column count mismatch")
	}

	// Check if a row with these values exists in the referenced table
	exists, err := rowReader.RowExists(fk.RefTable, refCols, values)
	if err != nil {
		return fmt.Errorf("failed to check foreign key reference: %w", err)
	}

	if !exists {
		return fmt.Errorf("FOREIGN KEY constraint failed: no matching row in %s", fk.RefTable)
	}

	return nil
}

// validateIncomingReferences checks if updating this row would break foreign keys.
func (m *ForeignKeyManager) validateIncomingReferences(
	tableName string,
	oldValues map[string]interface{},
	newValues map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowUpdater RowUpdater,
) error {
	// Get the table
	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return nil
	}

	// Find all foreign keys that reference this table
	var referencingConstraints []*ForeignKeyConstraint
	for _, fks := range m.constraints {
		for _, fk := range fks {
			if strings.ToLower(fk.RefTable) == strings.ToLower(tableName) {
				referencingConstraints = append(referencingConstraints, fk)
			}
		}
	}

	if len(referencingConstraints) == 0 {
		return nil
	}

	// Check each referencing constraint
	for _, fk := range referencingConstraints {
		// Determine which columns are being referenced
		refCols := fk.RefColumns
		if len(refCols) == 0 {
			refCols = table.PrimaryKey
		}

		// Check if any referenced columns changed
		changed := false
		for _, col := range refCols {
			if oldValues[col] != newValues[col] {
				changed = true
				break
			}
		}

		if !changed {
			continue // Referenced columns unchanged
		}

		// Extract old and new key values
		oldKeyValues := make([]interface{}, len(refCols))
		newKeyValues := make([]interface{}, len(refCols))
		for i, col := range refCols {
			oldKeyValues[i] = oldValues[col]
			newKeyValues[i] = newValues[col]
		}

		// Check if any rows reference the old values
		referencingRows, err := rowReader.FindReferencingRows(fk.Table, fk.Columns, oldKeyValues)
		if err != nil {
			return fmt.Errorf("failed to check foreign key references: %w", err)
		}

		if len(referencingRows) == 0 {
			continue // No references
		}

		// Handle based on ON UPDATE action
		switch fk.OnUpdate {
		case FKActionRestrict, FKActionNoAction:
			// RESTRICT/NO ACTION: prevent the update if there are referencing rows
			return fmt.Errorf("FOREIGN KEY constraint failed: table %s has referencing rows", fk.Table)

		case FKActionCascade:
			// Update all referencing rows with the new key values
			updateValues := make(map[string]interface{})
			for i, col := range fk.Columns {
				updateValues[col] = newKeyValues[i]
			}
			for _, rowID := range referencingRows {
				if err := rowUpdater.UpdateRow(fk.Table, rowID, updateValues); err != nil {
					return fmt.Errorf("CASCADE UPDATE failed: %w", err)
				}
			}

		case FKActionSetNull:
			// Set foreign key columns to NULL in referencing rows
			nullValues := make(map[string]interface{})
			for _, col := range fk.Columns {
				nullValues[col] = nil
			}
			for _, rowID := range referencingRows {
				if err := rowUpdater.UpdateRow(fk.Table, rowID, nullValues); err != nil {
					return fmt.Errorf("SET NULL UPDATE failed: %w", err)
				}
			}

		case FKActionSetDefault:
			// Set foreign key columns to their DEFAULT values
			// This requires reading the column defaults from schema
			refTable, ok := schemaObj.GetTable(fk.Table)
			if !ok {
				return fmt.Errorf("referencing table not found: %s", fk.Table)
			}

			defaultValues := make(map[string]interface{})
			for _, col := range fk.Columns {
				column, ok := refTable.GetColumn(col)
				if !ok {
					return fmt.Errorf("column not found: %s", col)
				}
				defaultValues[col] = column.Default
			}

			for _, rowID := range referencingRows {
				if err := rowUpdater.UpdateRow(fk.Table, rowID, defaultValues); err != nil {
					return fmt.Errorf("SET DEFAULT UPDATE failed: %w", err)
				}
			}
		}
	}

	return nil
}

// RowReader defines the interface for reading rows from tables.
type RowReader interface {
	// RowExists checks if a row exists with the given column values
	RowExists(table string, columns []string, values []interface{}) (bool, error)

	// FindReferencingRows finds all rows that reference the given values
	FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error)
}

// RowDeleter defines the interface for deleting rows.
type RowDeleter interface {
	// DeleteRow deletes a row by its rowid
	DeleteRow(table string, rowid int64) error
}

// RowUpdater defines the interface for updating rows.
type RowUpdater interface {
	// UpdateRow updates specific columns in a row
	UpdateRow(table string, rowid int64, values map[string]interface{}) error
}

// CreateForeignKeyFromParser creates a ForeignKeyConstraint from parser AST.
func CreateForeignKeyFromParser(
	tableName string,
	columns []string,
	fk *parser.ForeignKeyConstraint,
	name string,
) *ForeignKeyConstraint {
	return &ForeignKeyConstraint{
		Table:      tableName,
		Columns:    columns,
		RefTable:   fk.Table,
		RefColumns: fk.Columns,
		OnDelete:   convertFKAction(fk.OnDelete),
		OnUpdate:   convertFKAction(fk.OnUpdate),
		Deferrable: convertDeferrableMode(fk.Deferrable),
		Name:       name,
	}
}

// convertFKAction converts parser.ForeignKeyAction to constraint.ForeignKeyAction.
func convertFKAction(action parser.ForeignKeyAction) ForeignKeyAction {
	switch action {
	case parser.FKActionSetNull:
		return FKActionSetNull
	case parser.FKActionSetDefault:
		return FKActionSetDefault
	case parser.FKActionCascade:
		return FKActionCascade
	case parser.FKActionRestrict:
		return FKActionRestrict
	case parser.FKActionNoAction:
		return FKActionNoAction
	default:
		return FKActionNone
	}
}

// convertDeferrableMode converts parser.DeferrableMode to constraint.DeferrableMode.
func convertDeferrableMode(mode parser.DeferrableMode) DeferrableMode {
	switch mode {
	case parser.DeferrableInitiallyDeferred:
		return DeferrableInitiallyDeferred
	case parser.DeferrableInitiallyImmediate:
		return DeferrableInitiallyImmediate
	default:
		return DeferrableNone
	}
}
