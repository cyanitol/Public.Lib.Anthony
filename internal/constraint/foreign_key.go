// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package constraint provides constraint enforcement for SQLite databases.
// It implements CHECK, FOREIGN KEY, and other constraint validations.
package constraint

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
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
		return nil
	}

	constraints := m.GetConstraints(tableName)
	if len(constraints) == 0 {
		return nil
	}

	for _, fk := range constraints {
		if fk.Deferrable == DeferrableInitiallyDeferred {
			continue
		}

		fkValues, hasNull := extractForeignKeyValues(values, fk.Columns)
		if hasNull {
			continue
		}

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
	if err := m.validateOutgoingReferences(tableName, oldValues, newValues, schemaObj, rowReader); err != nil {
		return err
	}

	// 2. Check incoming foreign keys (other tables reference this one)
	if err := m.validateIncomingReferences(tableName, oldValues, newValues, schemaObj, rowReader, rowUpdater); err != nil {
		return err
	}

	return nil
}

// validateOutgoingReferences validates that updated foreign key columns still reference valid rows.
func (m *ForeignKeyManager) validateOutgoingReferences(
	tableName string,
	oldValues map[string]interface{},
	newValues map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
) error {
	constraints := m.GetConstraints(tableName)

	for _, fk := range constraints {
		if fk.Deferrable == DeferrableInitiallyDeferred {
			continue
		}

		if !columnsChanged(fk.Columns, oldValues, newValues) {
			continue
		}

		fkValues, hasNull := extractForeignKeyValues(newValues, fk.Columns)
		if hasNull {
			continue
		}

		if err := m.validateReference(fk, fkValues, schemaObj, rowReader); err != nil {
			return err
		}
	}

	return nil
}

// extractForeignKeyValues extracts foreign key values and indicates if any are NULL.
func extractForeignKeyValues(values map[string]interface{}, columns []string) ([]interface{}, bool) {
	fkValues := make([]interface{}, len(columns))
	hasNull := false

	for i, col := range columns {
		val, ok := values[col]
		if !ok || val == nil {
			hasNull = true
			break
		}
		fkValues[i] = val
	}

	return fkValues, hasNull
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

	referencingConstraints := m.findReferencingConstraints(tableName)
	if len(referencingConstraints) == 0 {
		return nil
	}

	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	for _, fk := range referencingConstraints {
		if err := m.handleDeleteConstraint(fk, table, values, schemaObj, rowReader, rowDeleter, rowUpdater); err != nil {
			return err
		}
	}

	return nil
}

// findReferencingConstraints finds all foreign keys that reference the given table.
func (m *ForeignKeyManager) findReferencingConstraints(tableName string) []*ForeignKeyConstraint {
	var result []*ForeignKeyConstraint
	tableLower := strings.ToLower(tableName)

	for _, fks := range m.constraints {
		for _, fk := range fks {
			if strings.ToLower(fk.RefTable) == tableLower {
				result = append(result, fk)
			}
		}
	}

	return result
}

// handleDeleteConstraint processes a single foreign key constraint during delete.
func (m *ForeignKeyManager) handleDeleteConstraint(
	fk *ForeignKeyConstraint,
	table *schema.Table,
	values map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
) error {
	refCols := fk.RefColumns
	if len(refCols) == 0 {
		refCols = table.PrimaryKey
	}

	keyValues := extractKeyValues(values, refCols)

	referencingRows, err := rowReader.FindReferencingRows(fk.Table, fk.Columns, keyValues)
	if err != nil {
		return fmt.Errorf("failed to check foreign key references: %w", err)
	}

	if len(referencingRows) == 0 {
		return nil
	}

	return m.applyDeleteAction(fk, referencingRows, schemaObj, rowDeleter, rowUpdater)
}

// applyDeleteAction applies the appropriate ON DELETE action.
func (m *ForeignKeyManager) applyDeleteAction(
	fk *ForeignKeyConstraint,
	referencingRows []int64,
	schemaObj *schema.Schema,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
) error {
	switch fk.OnDelete {
	case FKActionRestrict, FKActionNoAction:
		return fmt.Errorf("FOREIGN KEY constraint failed: table %s has referencing rows", fk.Table)

	case FKActionCascade:
		return m.cascadeDelete(fk.Table, referencingRows, rowDeleter)

	case FKActionSetNull:
		return m.setNullOnRows(fk.Table, fk.Columns, referencingRows, rowUpdater)

	case FKActionSetDefault:
		return m.setDefaultOnRows(fk.Table, fk.Columns, referencingRows, schemaObj, rowUpdater)
	}

	return nil
}

// cascadeDelete deletes all referencing rows.
func (m *ForeignKeyManager) cascadeDelete(table string, rowIDs []int64, rowDeleter RowDeleter) error {
	for _, rowID := range rowIDs {
		if err := rowDeleter.DeleteRow(table, rowID); err != nil {
			return fmt.Errorf("CASCADE DELETE failed: %w", err)
		}
	}
	return nil
}

// setNullOnRows sets foreign key columns to NULL in the given rows.
func (m *ForeignKeyManager) setNullOnRows(table string, columns []string, rowIDs []int64, rowUpdater RowUpdater) error {
	nullValues := make(map[string]interface{})
	for _, col := range columns {
		nullValues[col] = nil
	}

	for _, rowID := range rowIDs {
		if err := rowUpdater.UpdateRow(table, rowID, nullValues); err != nil {
			return fmt.Errorf("SET NULL failed: %w", err)
		}
	}
	return nil
}

// setDefaultOnRows sets foreign key columns to their DEFAULT values in the given rows.
func (m *ForeignKeyManager) setDefaultOnRows(
	table string,
	columns []string,
	rowIDs []int64,
	schemaObj *schema.Schema,
	rowUpdater RowUpdater,
) error {
	defaultValues, err := m.getDefaultValues(table, columns, schemaObj)
	if err != nil {
		return err
	}

	for _, rowID := range rowIDs {
		if err := rowUpdater.UpdateRow(table, rowID, defaultValues); err != nil {
			return fmt.Errorf("SET DEFAULT failed: %w", err)
		}
	}
	return nil
}

// getDefaultValues retrieves default values for the given columns.
func (m *ForeignKeyManager) getDefaultValues(
	tableName string,
	columns []string,
	schemaObj *schema.Schema,
) (map[string]interface{}, error) {
	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("referencing table not found: %s", tableName)
	}

	defaultValues := make(map[string]interface{})
	for _, col := range columns {
		column, ok := table.GetColumn(col)
		if !ok {
			return nil, fmt.Errorf("column not found: %s", col)
		}
		defaultValues[col] = column.Default
	}

	return defaultValues, nil
}

// extractKeyValues extracts values from the given map based on column names.
func extractKeyValues(values map[string]interface{}, columns []string) []interface{} {
	result := make([]interface{}, len(columns))
	for i, col := range columns {
		result[i] = values[col]
	}
	return result
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
	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return nil
	}

	referencingConstraints := m.findReferencingConstraints(tableName)
	if len(referencingConstraints) == 0 {
		return nil
	}

	for _, fk := range referencingConstraints {
		if err := m.handleUpdateConstraint(fk, table, oldValues, newValues, schemaObj, rowReader, rowUpdater); err != nil {
			return err
		}
	}

	return nil
}

// handleUpdateConstraint processes a single foreign key constraint during update.
func (m *ForeignKeyManager) handleUpdateConstraint(
	fk *ForeignKeyConstraint,
	table *schema.Table,
	oldValues map[string]interface{},
	newValues map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowUpdater RowUpdater,
) error {
	refCols := fk.RefColumns
	if len(refCols) == 0 {
		refCols = table.PrimaryKey
	}

	if !columnsChanged(refCols, oldValues, newValues) {
		return nil
	}

	oldKeyValues := extractKeyValues(oldValues, refCols)
	newKeyValues := extractKeyValues(newValues, refCols)

	referencingRows, err := rowReader.FindReferencingRows(fk.Table, fk.Columns, oldKeyValues)
	if err != nil {
		return fmt.Errorf("failed to check foreign key references: %w", err)
	}

	if len(referencingRows) == 0 {
		return nil
	}

	return m.applyUpdateAction(fk, newKeyValues, referencingRows, schemaObj, rowUpdater)
}

// columnsChanged checks if any of the specified columns changed between old and new values.
func columnsChanged(columns []string, oldValues, newValues map[string]interface{}) bool {
	for _, col := range columns {
		if oldValues[col] != newValues[col] {
			return true
		}
	}
	return false
}

// applyUpdateAction applies the appropriate ON UPDATE action.
func (m *ForeignKeyManager) applyUpdateAction(
	fk *ForeignKeyConstraint,
	newKeyValues []interface{},
	referencingRows []int64,
	schemaObj *schema.Schema,
	rowUpdater RowUpdater,
) error {
	switch fk.OnUpdate {
	case FKActionRestrict, FKActionNoAction:
		return fmt.Errorf("FOREIGN KEY constraint failed: table %s has referencing rows", fk.Table)

	case FKActionCascade:
		return m.cascadeUpdate(fk, newKeyValues, referencingRows, rowUpdater)

	case FKActionSetNull:
		return m.setNullOnRows(fk.Table, fk.Columns, referencingRows, rowUpdater)

	case FKActionSetDefault:
		return m.setDefaultOnRows(fk.Table, fk.Columns, referencingRows, schemaObj, rowUpdater)
	}

	return nil
}

// cascadeUpdate updates all referencing rows with new key values.
func (m *ForeignKeyManager) cascadeUpdate(
	fk *ForeignKeyConstraint,
	newKeyValues []interface{},
	rowIDs []int64,
	rowUpdater RowUpdater,
) error {
	updateValues := make(map[string]interface{})
	for i, col := range fk.Columns {
		updateValues[col] = newKeyValues[i]
	}

	for _, rowID := range rowIDs {
		if err := rowUpdater.UpdateRow(fk.Table, rowID, updateValues); err != nil {
			return fmt.Errorf("CASCADE UPDATE failed: %w", err)
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
