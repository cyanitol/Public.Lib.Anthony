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

// DeferredViolation represents a deferred foreign key constraint violation.
type DeferredViolation struct {
	Constraint *ForeignKeyConstraint
	Values     []interface{}
	Table      string
}

// ForeignKeyManager manages foreign key constraints for a database.
type ForeignKeyManager struct {
	constraints        map[string][]*ForeignKeyConstraint // table name -> constraints
	enabled            bool                               // PRAGMA foreign_keys setting
	deferredViolations []*DeferredViolation               // violations to check at commit time
	mu                 sync.RWMutex
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
	schemaObj interface{},
	rowReader interface{},
) error {
	if !m.IsEnabled() {
		return nil
	}

	constraints := m.GetConstraints(tableName)
	if len(constraints) == 0 {
		return nil
	}

	// Type assert the schema
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return nil // Can't validate without proper schema
	}

	// Type assert the row reader
	reader, ok := rowReader.(RowReader)
	if !ok {
		return nil // Can't validate without proper row reader
	}

	for _, fk := range constraints {
		if err := m.validateInsertConstraint(fk, tableName, values, schemaTyped, reader); err != nil {
			return err
		}
	}

	return nil
}

// validateInsertConstraint validates a single foreign key constraint for an INSERT operation.
func (m *ForeignKeyManager) validateInsertConstraint(
	fk *ForeignKeyConstraint,
	tableName string,
	values map[string]interface{},
	schemaTyped *schema.Schema,
	reader RowReader,
) error {
	fkValues, hasNull := extractForeignKeyValues(values, fk.Columns)
	if hasNull {
		return nil
	}

	// Check for self-referencing row (row references itself)
	if strings.EqualFold(fk.Table, fk.RefTable) {
		if selfReferenceMatches(values, fk.Columns, fk.RefColumns) {
			return nil // Row references itself, constraint satisfied
		}
	}

	// For deferred constraints, record violation for later checking at COMMIT
	if fk.Deferrable == DeferrableInitiallyDeferred {
		m.recordDeferredViolation(fk, fkValues, tableName)
		return nil
	}

	return m.validateReference(fk, fkValues, schemaTyped, reader)
}

// ValidateUpdate validates foreign key constraints for an UPDATE operation.
// Returns an error if any constraint is violated.
func (m *ForeignKeyManager) ValidateUpdate(
	tableName string,
	oldValues map[string]interface{},
	newValues map[string]interface{},
	schemaObj interface{},
	rowReader interface{},
	rowUpdater interface{},
) error {
	if !m.IsEnabled() {
		return nil
	}

	// Type assertions
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return nil
	}
	reader, ok := rowReader.(RowReader)
	if !ok {
		return nil
	}
	updater, ok := rowUpdater.(RowUpdater)
	if !ok {
		return nil
	}

	// 1. Check outgoing foreign keys (this table references others)
	if err := m.validateOutgoingReferences(tableName, oldValues, newValues, schemaTyped, reader); err != nil {
		return err
	}

	// 2. Check incoming foreign keys (other tables reference this one)
	if err := m.validateIncomingReferences(tableName, oldValues, newValues, schemaTyped, reader, updater); err != nil {
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
		if !columnsChanged(fk.Columns, oldValues, newValues) {
			continue
		}

		fkValues, hasNull := extractForeignKeyValues(newValues, fk.Columns)
		if hasNull {
			continue
		}

		// For deferred constraints, record violation for later checking at COMMIT
		if fk.Deferrable == DeferrableInitiallyDeferred {
			m.recordDeferredViolation(fk, fkValues, tableName)
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

// selfReferenceMatches checks if a self-referencing row references itself.
// This happens when the FK column values match the parent key column values.
func selfReferenceMatches(values map[string]interface{}, fkColumns, parentColumns []string) bool {
	if len(fkColumns) != len(parentColumns) {
		return false
	}

	for i := range fkColumns {
		fkVal, fkOK := values[strings.ToLower(fkColumns[i])]
		if !fkOK {
			fkVal, fkOK = values[fkColumns[i]]
		}
		parentVal, parentOK := values[strings.ToLower(parentColumns[i])]
		if !parentOK {
			parentVal, parentOK = values[parentColumns[i]]
		}

		if !fkOK || !parentOK {
			return false
		}

		if !valuesEqual(fkVal, parentVal) {
			return false
		}
	}

	return true
}

// validateDeleteTypeAssertions performs type assertions for ValidateDelete.
// Returns the typed objects or nil if any assertion fails.
func (m *ForeignKeyManager) validateDeleteTypeAssertions(
	schemaObj, rowReader, rowDeleter, rowUpdater interface{},
) (*schema.Schema, RowReader, RowDeleter, RowUpdater, bool) {
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return nil, nil, nil, nil, false
	}
	reader, ok := rowReader.(RowReader)
	if !ok {
		return nil, nil, nil, nil, false
	}
	deleter, ok := rowDeleter.(RowDeleter)
	if !ok {
		return nil, nil, nil, nil, false
	}
	updater, ok := rowUpdater.(RowUpdater)
	if !ok {
		return nil, nil, nil, nil, false
	}
	return schemaTyped, reader, deleter, updater, true
}

// ValidateDelete validates foreign key constraints for a DELETE operation.
// Returns an error if any constraint is violated, or performs cascade actions.
func (m *ForeignKeyManager) ValidateDelete(
	tableName string,
	values map[string]interface{},
	schemaObj interface{},
	rowReader interface{},
	rowDeleter interface{},
	rowUpdater interface{},
) error {
	if !m.IsEnabled() {
		return nil
	}

	schemaTyped, reader, deleter, updater, ok := m.validateDeleteTypeAssertions(
		schemaObj, rowReader, rowDeleter, rowUpdater)
	if !ok {
		return nil
	}

	referencingConstraints := m.FindReferencingConstraints(tableName)
	if len(referencingConstraints) == 0 {
		return nil
	}

	table, tableOk := schemaTyped.GetTable(tableName)
	if !tableOk {
		return fmt.Errorf("table not found: %s", tableName)
	}

	for _, fk := range referencingConstraints {
		if err := m.handleDeleteConstraint(fk, table, values, schemaTyped, reader, deleter, updater); err != nil {
			return err
		}
	}

	return nil
}

// FindReferencingConstraints finds all foreign keys that reference the given table.
func (m *ForeignKeyManager) FindReferencingConstraints(tableName string) []*ForeignKeyConstraint {
	m.mu.RLock()
	defer m.mu.RUnlock()

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

	// For self-referencing FKs, filter out the row being deleted if it references itself
	if strings.EqualFold(fk.Table, fk.RefTable) && selfReferenceMatches(values, fk.Columns, refCols) {
		referencingRows = m.filterSelfReference(referencingRows, values, refCols)
		if len(referencingRows) == 0 {
			return nil
		}
	}

	return m.applyDeleteAction(fk, referencingRows, schemaObj, rowDeleter, rowUpdater, rowReader)
}

// applyDeleteActionRestrict returns an error for RESTRICT/NO ACTION/default behavior.
func (m *ForeignKeyManager) applyDeleteActionRestrict(fk *ForeignKeyConstraint) error {
	return fmt.Errorf("FOREIGN KEY constraint failed: table %s has referencing rows", fk.Table)
}

// applyDeleteAction applies the appropriate ON DELETE action.
func (m *ForeignKeyManager) applyDeleteAction(
	fk *ForeignKeyConstraint,
	referencingRows []int64,
	schemaObj *schema.Schema,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
	rowReader RowReader,
) error {
	if len(referencingRows) == 0 {
		return nil
	}

	switch fk.OnDelete {
	case FKActionNone, FKActionRestrict, FKActionNoAction:
		return m.applyDeleteActionRestrict(fk)
	case FKActionCascade:
		return m.cascadeDelete(fk.Table, referencingRows, schemaObj, rowDeleter, rowUpdater, rowReader)
	case FKActionSetNull:
		return m.setNullOnRows(fk.Table, fk.Columns, referencingRows, rowUpdater)
	case FKActionSetDefault:
		return m.setDefaultOnRows(fk.Table, fk.Columns, referencingRows, schemaObj, rowUpdater)
	}

	return nil
}

// cascadeDelete deletes all referencing rows, recursively handling grandchildren.
func (m *ForeignKeyManager) cascadeDelete(
	table string,
	rowIDs []int64,
	schemaObj *schema.Schema,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
	rowReader RowReader,
) error {
	for _, rowID := range rowIDs {
		// Read the row values before deletion (needed for recursive FK checking)
		rowValues, err := rowReader.ReadRowByRowid(table, rowID)
		if err != nil {
			// Row might already be deleted by a previous cascade
			continue
		}

		// Recursively handle any grandchildren that reference this row
		if err := m.validateDeleteRecursive(table, rowValues, schemaObj, rowReader, rowDeleter, rowUpdater); err != nil {
			return err
		}

		// Now delete the row
		if err := rowDeleter.DeleteRow(table, rowID); err != nil {
			return fmt.Errorf("CASCADE DELETE failed: %w", err)
		}
	}
	return nil
}

// validateDeleteRecursive handles FK validation for rows being cascade-deleted.
func (m *ForeignKeyManager) validateDeleteRecursive(
	tableName string,
	values map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
) error {
	// Find FK constraints where this table is the parent (RefTable)
	referencingConstraints := m.FindReferencingConstraints(tableName)

	table, exists := schemaObj.Tables[tableName]
	if !exists {
		return nil
	}

	for _, fk := range referencingConstraints {
		if err := m.handleDeleteConstraint(fk, table, values, schemaObj, rowReader, rowDeleter, rowUpdater); err != nil {
			return err
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

// filterSelfReference removes the row being deleted from referencingRows if it matches the PK values.
func (m *ForeignKeyManager) filterSelfReference(rowIDs []int64, values map[string]interface{}, pkCols []string) []int64 {
	// Extract the rowid of the row being deleted from the primary key
	if len(pkCols) == 0 {
		return rowIDs
	}

	// Check if first PK column contains the rowid (for INTEGER PRIMARY KEY)
	pkVal, ok := values[pkCols[0]]
	if !ok {
		return rowIDs
	}

	var deleteRowID int64
	switch v := pkVal.(type) {
	case int64:
		deleteRowID = v
	case int:
		deleteRowID = int64(v)
	default:
		return rowIDs // Can't determine rowid, return all
	}

	// Filter out the row being deleted
	filtered := make([]int64, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		if rowID != deleteRowID {
			filtered = append(filtered, rowID)
		}
	}
	return filtered
}

// columnsAreUnique checks if the given columns have a UNIQUE constraint.
func columnsAreUnique(table *schema.Table, columns []string, schemaObj *schema.Schema) bool {
	// Check if columns are the primary key
	if len(columns) == len(table.PrimaryKey) && columnsMatch(columns, table.PrimaryKey) {
		return true
	}

	// For single column, check if it has column-level UNIQUE constraint
	if len(columns) == 1 {
		if col, ok := table.GetColumn(columns[0]); ok && col.Unique {
			return true
		}
	}

	// Check table-level UNIQUE constraints
	if hasUniqueConstraint(table, columns) {
		return true
	}

	// Check UNIQUE indexes
	return hasUniqueIndex(schemaObj, table.Name, columns)
}

// columnsMatch checks if two column lists are equivalent (order matters).
func columnsMatch(cols1, cols2 []string) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	for i, col := range cols1 {
		if !strings.EqualFold(col, cols2[i]) {
			return false
		}
	}
	return true
}

// columnsMatchUnordered checks if two column lists contain the same columns (order doesn't matter).
func columnsMatchUnordered(cols1, cols2 []string) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	// Create a map of columns in cols2
	colSet := make(map[string]bool)
	for _, col := range cols2 {
		colSet[strings.ToLower(col)] = true
	}
	// Check that all columns in cols1 are in the set
	for _, col := range cols1 {
		if !colSet[strings.ToLower(col)] {
			return false
		}
	}
	return true
}

// hasUniqueConstraint checks if the table has a UNIQUE constraint on the given columns.
func hasUniqueConstraint(table *schema.Table, columns []string) bool {
	for _, tc := range table.Constraints {
		if tc.Type == schema.ConstraintUnique && columnsMatch(columns, tc.Columns) {
			return true
		}
	}
	return false
}

// hasUniqueIndex checks if there's a UNIQUE index covering the given columns.
// The index columns can be in any order as long as they cover the same columns.
func hasUniqueIndex(schemaObj *schema.Schema, tableName string, columns []string) bool {
	for _, idx := range schemaObj.Indexes {
		if idx.Unique && strings.EqualFold(idx.Table, tableName) && columnsMatchUnordered(columns, idx.Columns) {
			return true
		}
	}
	return false
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

	// Check if referenced columns have a unique constraint
	if !columnsAreUnique(refTable, refCols, schemaObj) {
		return fmt.Errorf("foreign key mismatch - \"%s\" referencing \"%s\"", fk.Table, fk.RefTable)
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

	referencingConstraints := m.FindReferencingConstraints(tableName)
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
	// No referencing rows means no action needed
	if len(referencingRows) == 0 {
		return nil
	}
	switch fk.OnUpdate {
	case FKActionNone, FKActionRestrict, FKActionNoAction:
		// FKActionNone is the default when no ON UPDATE is specified, which is equivalent to NO ACTION
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

	// ReadRowByRowid reads a row's values by its rowid (needed for recursive CASCADE)
	ReadRowByRowid(table string, rowid int64) (map[string]interface{}, error)
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

// recordDeferredViolation records a potential deferred constraint violation.
func (m *ForeignKeyManager) recordDeferredViolation(
	fk *ForeignKeyConstraint,
	values []interface{},
	tableName string,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deferredViolations = append(m.deferredViolations, &DeferredViolation{
		Constraint: fk,
		Values:     values,
		Table:      tableName,
	})
}

// CheckDeferredViolations validates all deferred constraints at commit time.
// Returns an error if any deferred constraint is violated.
func (m *ForeignKeyManager) CheckDeferredViolations(
	schemaObj interface{},
	rowReader interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.deferredViolations) == 0 {
		return nil
	}

	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return nil
	}

	reader, ok := rowReader.(RowReader)
	if !ok {
		return nil
	}

	return m.validateAllDeferredViolations(schemaTyped, reader)
}

// validateAllDeferredViolations checks each deferred violation.
func (m *ForeignKeyManager) validateAllDeferredViolations(
	schemaObj *schema.Schema,
	reader RowReader,
) error {
	for _, violation := range m.deferredViolations {
		if err := m.validateReference(violation.Constraint, violation.Values, schemaObj, reader); err != nil {
			return err
		}
	}
	return nil
}

// ClearDeferredViolations clears all recorded deferred violations.
// Called on ROLLBACK or after successful commit.
func (m *ForeignKeyManager) ClearDeferredViolations() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deferredViolations = nil
}

// ForeignKeyViolation represents a single foreign key constraint violation.
type ForeignKeyViolation struct {
	Table  string // Child table name
	Rowid  int64  // Rowid of the violating row
	Parent string // Parent table name
	FKid   int    // Foreign key constraint index (0-based)
}

// FindViolations finds all foreign key violations in the database.
// If tableName is empty, checks all tables. Otherwise, checks only the specified table.
// Works even when foreign key enforcement is OFF.
func (m *ForeignKeyManager) FindViolations(
	tableName string,
	schemaObj interface{},
	rowReader interface{},
) ([]ForeignKeyViolation, error) {
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return nil, fmt.Errorf("invalid schema object")
	}

	// Allow rowReader to be invalid - schema mismatches can be detected without scanning rows
	reader, _ := rowReader.(RowReader)

	if tableName != "" {
		return m.findViolationsForTable(tableName, schemaTyped, reader)
	}

	return m.findViolationsAllTables(schemaTyped, reader)
}

// findViolationsAllTables checks all tables for foreign key violations.
func (m *ForeignKeyManager) findViolationsAllTables(
	schemaObj *schema.Schema,
	reader RowReader,
) ([]ForeignKeyViolation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var violations []ForeignKeyViolation
	for tableName := range m.constraints {
		tableViolations, err := m.checkTableViolations(tableName, schemaObj, reader)
		if err != nil {
			return nil, err
		}
		violations = append(violations, tableViolations...)
	}

	return violations, nil
}

// findViolationsForTable checks a specific table for foreign key violations.
func (m *ForeignKeyManager) findViolationsForTable(
	tableName string,
	schemaObj *schema.Schema,
	reader RowReader,
) ([]ForeignKeyViolation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.checkTableViolations(tableName, schemaObj, reader)
}

// checkTableViolations checks all rows in a table for FK violations.
func (m *ForeignKeyManager) checkTableViolations(
	tableName string,
	schemaObj *schema.Schema,
	reader RowReader,
) ([]ForeignKeyViolation, error) {
	tableLower := strings.ToLower(tableName)
	constraints := m.constraints[tableLower]
	if len(constraints) == 0 {
		return nil, nil
	}

	table, ok := schemaObj.GetTable(tableName)
	if !ok {
		return nil, nil
	}

	var violations []ForeignKeyViolation
	for fkid, fk := range constraints {
		fkViolations, err := m.checkConstraintViolations(fk, fkid, table, reader, schemaObj)
		if err != nil {
			return nil, err
		}
		violations = append(violations, fkViolations...)
	}

	return violations, nil
}

// checkConstraintViolations checks all rows for a specific FK constraint.
func (m *ForeignKeyManager) checkConstraintViolations(
	fk *ForeignKeyConstraint,
	fkid int,
	table *schema.Table,
	reader RowReader,
	schemaObj *schema.Schema,
) ([]ForeignKeyViolation, error) {
	refTable, ok := schemaObj.GetTable(fk.RefTable)
	if !ok {
		// Missing parent table - scan child table to report violations for non-NULL FKs
		return m.scanMissingParentViolations(fk, fkid, reader)
	}

	refCols := fk.RefColumns
	if len(refCols) == 0 {
		refCols = refTable.PrimaryKey
	}

	// Check for schema mismatch (referenced columns must be unique)
	if !columnsAreUnique(refTable, refCols, schemaObj) {
		return []ForeignKeyViolation{{
			Table:  fk.Table,
			Rowid:  0,
			Parent: fk.RefTable,
			FKid:   fkid,
		}}, nil
	}

	return m.scanTableForViolations(fk, fkid, refCols, reader, schemaObj)
}

// scanMissingParentViolations scans child table when parent table doesn't exist.
// Reports violations for all rows with non-NULL FK values.
func (m *ForeignKeyManager) scanMissingParentViolations(
	fk *ForeignKeyConstraint,
	fkid int,
	reader RowReader,
) ([]ForeignKeyViolation, error) {
	if reader == nil {
		return nil, nil
	}

	var violations []ForeignKeyViolation
	rowids, err := reader.FindReferencingRows(fk.Table, []string{}, []interface{}{})
	if err != nil {
		return nil, err
	}

	for _, rowid := range rowids {
		rowValues, err := reader.ReadRowByRowid(fk.Table, rowid)
		if err != nil {
			return nil, err
		}

		// Check if FK columns have NULL values (partial NULL is allowed)
		_, hasNull := extractForeignKeyValues(rowValues, fk.Columns)
		if hasNull {
			continue // Partial NULL is not a violation
		}

		// Non-NULL FK with missing parent table is a violation
		violations = append(violations, ForeignKeyViolation{
			Table:  fk.Table,
			Rowid:  rowid,
			Parent: fk.RefTable,
			FKid:   fkid,
		})
	}

	return violations, nil
}

// scanTableForViolations scans all rows in a table and checks for violations.
func (m *ForeignKeyManager) scanTableForViolations(
	fk *ForeignKeyConstraint,
	fkid int,
	refCols []string,
	reader RowReader,
	schemaObj *schema.Schema,
) ([]ForeignKeyViolation, error) {
	// If no reader provided, can't scan rows - return empty violations
	if reader == nil {
		return nil, nil
	}

	var violations []ForeignKeyViolation

	rowids, err := reader.FindReferencingRows(fk.Table, []string{}, []interface{}{})
	if err != nil {
		return nil, err
	}

	for _, rowid := range rowids {
		violation, err := m.checkRowForViolation(fk, fkid, rowid, refCols, reader, schemaObj)
		if err != nil {
			return nil, err
		}
		if violation != nil {
			violations = append(violations, *violation)
		}
	}

	return violations, nil
}

// checkRowForViolation checks if a specific row violates an FK constraint.
func (m *ForeignKeyManager) checkRowForViolation(
	fk *ForeignKeyConstraint,
	fkid int,
	rowid int64,
	refCols []string,
	reader RowReader,
	schemaObj *schema.Schema,
) (*ForeignKeyViolation, error) {
	rowValues, err := reader.ReadRowByRowid(fk.Table, rowid)
	if err != nil {
		return nil, err
	}

	fkValues, hasNull := extractForeignKeyValues(rowValues, fk.Columns)
	if hasNull {
		return nil, nil
	}

	if isViolation(fk, fkValues, refCols, reader) {
		return &ForeignKeyViolation{
			Table:  fk.Table,
			Rowid:  rowid,
			Parent: fk.RefTable,
			FKid:   fkid,
		}, nil
	}

	return nil, nil
}

// isViolation checks if FK values violate the constraint.
func isViolation(
	fk *ForeignKeyConstraint,
	fkValues []interface{},
	refCols []string,
	reader RowReader,
) bool {
	exists, err := reader.RowExists(fk.RefTable, refCols, fkValues)
	if err != nil {
		return true
	}
	return !exists
}
