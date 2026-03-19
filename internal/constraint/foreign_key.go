// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package constraint provides constraint enforcement for SQLite databases.
// It implements CHECK, FOREIGN KEY, and other constraint validations.
package constraint

import (
	"fmt"
	"strconv"
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
	inTransaction      bool                               // true when inside a transaction
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

// SetInTransaction sets whether we're currently in a transaction.
// Deferred constraints only defer when inside a transaction.
func (m *ForeignKeyManager) SetInTransaction(inTx bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inTransaction = inTx
}

// IsInTransaction returns whether we're currently in a transaction.
func (m *ForeignKeyManager) IsInTransaction() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.inTransaction
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
	// But only defer if we're inside a transaction
	if fk.Deferrable == DeferrableInitiallyDeferred && m.IsInTransaction() {
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

	// 3. Clear any deferred violations for this table since the row was updated
	//    and may no longer violate the constraint
	m.ClearDeferredViolationsForTable(tableName)

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
		// But only defer if we're inside a transaction
		if fk.Deferrable == DeferrableInitiallyDeferred && m.IsInTransaction() {
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

	// Clear any deferred violations for this table since the row was deleted
	m.ClearDeferredViolationsForTable(tableName)

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

	// Check if child table is WITHOUT ROWID
	childTable, ok := schemaObj.GetTable(fk.Table)
	if ok && childTable.WithoutRowID {
		return m.handleDeleteConstraintWithoutRowID(fk, childTable, keyValues, refCols, values, schemaObj, rowReader, rowDeleter, rowUpdater)
	}

	referencingRows, err := m.findReferencingRowsWithAffinity(rowReader, fk, table.Name, refCols, keyValues)
	if err != nil {
		return fmt.Errorf("failed to check foreign key references: %w", err)
	}

	referencingRows = m.filterDeleteSelfRef(fk, referencingRows, values, refCols)
	if len(referencingRows) == 0 {
		return nil
	}

	if m.shouldDeferDeleteViolation(fk, keyValues) {
		return nil
	}

	return m.applyDeleteAction(fk, referencingRows, schemaObj, rowDeleter, rowUpdater, rowReader)
}

// filterDeleteSelfRef filters out self-referencing rows from referencingRows when applicable.
func (m *ForeignKeyManager) filterDeleteSelfRef(
	fk *ForeignKeyConstraint,
	referencingRows []int64,
	values map[string]interface{},
	refCols []string,
) []int64 {
	if len(referencingRows) == 0 {
		return referencingRows
	}
	if strings.EqualFold(fk.Table, fk.RefTable) && selfReferenceMatches(values, fk.Columns, refCols) {
		return m.filterSelfReference(referencingRows, values, refCols)
	}
	return referencingRows
}

// shouldDeferDeleteViolation checks if the violation should be deferred and records it if so.
func (m *ForeignKeyManager) shouldDeferDeleteViolation(fk *ForeignKeyConstraint, keyValues []interface{}) bool {
	if fk.Deferrable != DeferrableInitiallyDeferred || !m.IsInTransaction() {
		return false
	}
	action := fk.OnDelete
	if action != FKActionNone && action != FKActionNoAction {
		return false
	}
	m.recordDeferredViolation(fk, keyValues, fk.Table)
	return true
}

// handleDeleteConstraintWithoutRowID handles FK constraint for WITHOUT ROWID child tables.
func (m *ForeignKeyManager) handleDeleteConstraintWithoutRowID(
	fk *ForeignKeyConstraint,
	childTable *schema.Table,
	parentValues []interface{},
	parentCols []string,
	deletedRowValues map[string]interface{},
	schemaObj *schema.Schema,
	rowReader RowReader,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
) error {
	// Try to use extended interface
	readerExt, ok := rowReader.(RowReaderExtended)
	if !ok {
		return fmt.Errorf("RowReaderExtended interface required for WITHOUT ROWID CASCADE")
	}

	// Find all matching rows and get their complete data
	rowDataList, err := readerExt.FindReferencingRowsWithData(fk.Table, fk.Columns, parentValues)
	if err != nil {
		return fmt.Errorf("failed to find referencing rows: %w", err)
	}

	if len(rowDataList) == 0 {
		return nil
	}

	// For self-referencing FKs, filter out the row being deleted
	if strings.EqualFold(fk.Table, fk.RefTable) && selfReferenceMatches(deletedRowValues, fk.Columns, parentCols) {
		rowDataList = m.filterSelfReferenceWithoutRowID(rowDataList, deletedRowValues, childTable.PrimaryKey)
		if len(rowDataList) == 0 {
			return nil
		}
	}

	return m.applyDeleteActionWithoutRowID(fk, childTable, rowDataList, schemaObj, rowDeleter, rowUpdater, readerExt)
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

// applyDeleteActionWithoutRowID applies DELETE actions for WITHOUT ROWID tables.
func (m *ForeignKeyManager) applyDeleteActionWithoutRowID(
	fk *ForeignKeyConstraint,
	childTable *schema.Table,
	rowDataList []map[string]interface{},
	schemaObj *schema.Schema,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
	readerExt RowReaderExtended,
) error {
	if len(rowDataList) == 0 {
		return nil
	}

	switch fk.OnDelete {
	case FKActionNone, FKActionRestrict, FKActionNoAction:
		return m.applyDeleteActionRestrict(fk)
	case FKActionCascade:
		return m.cascadeDeleteWithoutRowID(fk.Table, childTable, rowDataList, schemaObj, rowDeleter, rowUpdater, readerExt)
	case FKActionSetNull:
		// For SET NULL, we'd need to update rows by their primary keys
		return fmt.Errorf("SET NULL not yet supported for WITHOUT ROWID tables")
	case FKActionSetDefault:
		// For SET DEFAULT, we'd need to update rows by their primary keys
		return fmt.Errorf("SET DEFAULT not yet supported for WITHOUT ROWID tables")
	}

	return nil
}

// cascadeDeleteWithoutRowID deletes referencing rows from WITHOUT ROWID tables.
func (m *ForeignKeyManager) cascadeDeleteWithoutRowID(
	tableName string,
	childTable *schema.Table,
	rowDataList []map[string]interface{},
	schemaObj *schema.Schema,
	rowDeleter RowDeleter,
	rowUpdater RowUpdater,
	readerExt RowReaderExtended,
) error {
	deleterExt, ok := rowDeleter.(RowDeleterExtended)
	if !ok {
		return fmt.Errorf("RowDeleterExtended interface required for WITHOUT ROWID CASCADE DELETE")
	}

	for _, rowData := range rowDataList {
		// Recursively handle grandchildren that reference this row
		if err := m.validateDeleteRecursive(tableName, rowData, schemaObj, readerExt, rowDeleter, rowUpdater); err != nil {
			return err
		}

		// Extract primary key values
		pkValues := extractKeyValues(rowData, childTable.PrimaryKey)

		// Delete the row by its primary key
		if err := deleterExt.DeleteRowByKey(tableName, pkValues); err != nil {
			return fmt.Errorf("CASCADE DELETE failed: %w", err)
		}
	}
	return nil
}

// filterSelfReferenceWithoutRowID removes the deleted row from the list if it matches.
func (m *ForeignKeyManager) filterSelfReferenceWithoutRowID(
	rowDataList []map[string]interface{},
	deletedRowValues map[string]interface{},
	pkCols []string,
) []map[string]interface{} {
	// Extract PK values from the deleted row
	deletedPK := extractKeyValues(deletedRowValues, pkCols)

	// Filter out rows that match the deleted row's PK
	filtered := make([]map[string]interface{}, 0, len(rowDataList))
	for _, rowData := range rowDataList {
		rowPK := extractKeyValues(rowData, pkCols)
		if !valuesMatch(rowPK, deletedPK) {
			filtered = append(filtered, rowData)
		}
	}
	return filtered
}

// valuesMatch checks if two slices of values are equal.
func valuesMatch(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
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
	// Check if table is WITHOUT ROWID
	tableObj, ok := schemaObj.GetTable(table)
	if !ok {
		return fmt.Errorf("table not found: %s", table)
	}

	// Try to use extended interfaces for WITHOUT ROWID support
	deleterExt, hasDeleterExt := rowDeleter.(RowDeleterExtended)

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
		if tableObj.WithoutRowID && hasDeleterExt {
			// Extract PK values for deletion
			pkValues := extractKeyValues(rowValues, tableObj.PrimaryKey)
			if err := deleterExt.DeleteRowByKey(table, pkValues); err != nil {
				return fmt.Errorf("CASCADE DELETE failed: %w", err)
			}
		} else {
			if err := rowDeleter.DeleteRow(table, rowID); err != nil {
				return fmt.Errorf("CASCADE DELETE failed: %w", err)
			}
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

	// Note: This function currently only supports regular tables with rowids.
	// For WITHOUT ROWID tables, rowIDs would need to be replaced with primary key values.
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
		defaultValues[col] = applyAffinityToDefault(column.Default, column.Type)
	}

	return defaultValues, nil
}

// applyAffinityToDefault converts a string default value to the appropriate Go
// type based on the column's declared type affinity (INTEGER, REAL, etc.).
func applyAffinityToDefault(val interface{}, colType string) interface{} {
	s, ok := val.(string)
	if !ok {
		return val
	}
	upper := strings.ToUpper(colType)
	if strings.Contains(upper, "INT") {
		return tryParseInt(s, val)
	}
	if hasRealAffinity(upper) {
		return tryParseFloat(s, val)
	}
	if hasNumericAffinity(upper) {
		return tryParseNumeric(s, val)
	}
	return val
}

// hasRealAffinity checks if the type string indicates REAL affinity.
func hasRealAffinity(upper string) bool {
	return strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOA") || strings.Contains(upper, "DOUB")
}

// hasNumericAffinity checks if the type string indicates NUMERIC affinity.
func hasNumericAffinity(upper string) bool {
	return strings.Contains(upper, "NUM") || strings.Contains(upper, "DEC")
}

// tryParseInt tries to parse a string as int64, returning fallback on failure.
func tryParseInt(s string, fallback interface{}) interface{} {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	return fallback
}

// tryParseFloat tries to parse a string as float64, returning fallback on failure.
func tryParseFloat(s string, fallback interface{}) interface{} {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return fallback
}

// tryParseNumeric tries to parse as int first, then float, returning fallback on failure.
func tryParseNumeric(s string, fallback interface{}) interface{} {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return fallback
}

// extractKeyValues extracts values from the given map based on column names.
func extractKeyValues(values map[string]interface{}, columns []string) []interface{} {
	result := make([]interface{}, len(columns))
	for i, col := range columns {
		result[i] = values[col]
	}
	return result
}

// findReferencingRowsWithAffinity finds referencing rows using affinity-aware comparison.
// It tries to use an extended interface if available, otherwise falls back to standard comparison.
func (m *ForeignKeyManager) findReferencingRowsWithAffinity(
	rowReader RowReader,
	fk *ForeignKeyConstraint,
	parentTableName string,
	parentColumns []string,
	parentValues []interface{},
) ([]int64, error) {
	// Try to use affinity-aware interface if available
	type affinityRowReader interface {
		FindReferencingRowsWithParentAffinity(
			childTableName string,
			childColumns []string,
			parentValues []interface{},
			parentTableName string,
			parentColumns []string,
		) ([]int64, error)
	}

	if ar, ok := rowReader.(affinityRowReader); ok {
		return ar.FindReferencingRowsWithParentAffinity(
			fk.Table,
			fk.Columns,
			parentValues,
			parentTableName,
			parentColumns,
		)
	}

	// Fall back to standard comparison
	return rowReader.FindReferencingRows(fk.Table, fk.Columns, parentValues)
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

	// Extract collations from parent table columns
	collations := extractCollations(refTable, refCols)

	// Check if a row with these values exists in the referenced table
	// Use the parent table's collation for comparison (SQLite behavior)
	exists, err := rowReader.RowExistsWithCollation(fk.RefTable, refCols, values, collations)
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

	referencingRows, err := m.findReferencingRowsWithAffinity(rowReader, fk, table.Name, refCols, oldKeyValues)
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

	// Note: This function currently only supports regular tables with rowids.
	// For WITHOUT ROWID tables, rowIDs would need to be replaced with primary key values.
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

	// RowExistsWithCollation checks if a row exists using specified collations per column
	RowExistsWithCollation(table string, columns []string, values []interface{}, collations []string) (bool, error)

	// FindReferencingRows finds all rows that reference the given values
	FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error)

	// ReadRowByRowid reads a row's values by its rowid (needed for recursive CASCADE)
	ReadRowByRowid(table string, rowid int64) (map[string]interface{}, error)
}

// RowReaderExtended extends RowReader with support for WITHOUT ROWID tables.
type RowReaderExtended interface {
	RowReader
	// ReadRowByKey reads a row by primary key values (for WITHOUT ROWID tables)
	ReadRowByKey(table string, keyValues []interface{}) (map[string]interface{}, error)

	// FindReferencingRowsWithData finds rows and returns their full data (for WITHOUT ROWID support)
	// Returns a slice of row data maps, where each map contains all column values
	FindReferencingRowsWithData(table string, columns []string, values []interface{}) ([]map[string]interface{}, error)
}

// RowDeleter defines the interface for deleting rows.
type RowDeleter interface {
	// DeleteRow deletes a row by its rowid
	DeleteRow(table string, rowid int64) error
}

// RowDeleterExtended extends RowDeleter with support for WITHOUT ROWID tables.
type RowDeleterExtended interface {
	RowDeleter
	// DeleteRowByKey deletes a row by primary key values (for WITHOUT ROWID tables)
	DeleteRowByKey(table string, keyValues []interface{}) error
}

// RowUpdater defines the interface for updating rows.
type RowUpdater interface {
	// UpdateRow updates specific columns in a row
	UpdateRow(table string, rowid int64, values map[string]interface{}) error
}

// RowUpdaterExtended extends RowUpdater with support for WITHOUT ROWID tables.
type RowUpdaterExtended interface {
	RowUpdater
	// UpdateRowByKey updates specific columns by primary key values (for WITHOUT ROWID tables)
	UpdateRowByKey(table string, keyValues []interface{}, values map[string]interface{}) error
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

// DeferredViolationCount returns the number of pending deferred violations.
func (m *ForeignKeyManager) DeferredViolationCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.deferredViolations)
}

// ClearDeferredViolationsForTable clears deferred violations for a specific table.
// Called when a row in the table is updated or deleted, since the violation may no longer apply.
func (m *ForeignKeyManager) ClearDeferredViolationsForTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableLower := strings.ToLower(tableName)
	filtered := make([]*DeferredViolation, 0, len(m.deferredViolations))
	for _, v := range m.deferredViolations {
		if !strings.EqualFold(v.Table, tableLower) {
			filtered = append(filtered, v)
		}
	}
	m.deferredViolations = filtered
}

// ForeignKeyViolation represents a single foreign key constraint violation.
type ForeignKeyViolation struct {
	Table  string // Child table name
	Rowid  int64  // Rowid of the violating row
	Parent string // Parent table name
	FKid   int    // Foreign key constraint index (0-based)
}

// CheckSchemaMismatch checks if any FK has a schema mismatch.
// Returns an error if a FK references non-existent table/columns or non-unique columns.
func (m *ForeignKeyManager) CheckSchemaMismatch(
	tableName string,
	schemaObj interface{},
) error {
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return fmt.Errorf("invalid schema object")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if tableName != "" {
		return m.checkTableSchemaMismatch(tableName, schemaTyped)
	}

	return m.checkAllTablesSchemaMismatch(schemaTyped)
}

// checkAllTablesSchemaMismatch checks all tables for schema mismatches.
func (m *ForeignKeyManager) checkAllTablesSchemaMismatch(schemaObj *schema.Schema) error {
	for tableName := range m.constraints {
		if err := m.checkTableSchemaMismatch(tableName, schemaObj); err != nil {
			return err
		}
	}
	return nil
}

// checkTableSchemaMismatch checks a specific table for schema mismatches.
func (m *ForeignKeyManager) checkTableSchemaMismatch(tableName string, schemaObj *schema.Schema) error {
	tableLower := strings.ToLower(tableName)
	constraints := m.constraints[tableLower]
	if len(constraints) == 0 {
		return nil
	}

	for fkid, fk := range constraints {
		if err := m.validateFKSchema(fk, fkid, schemaObj); err != nil {
			return err
		}
	}

	return nil
}

// validateFKSchema validates that a FK constraint's schema is correct.
func (m *ForeignKeyManager) validateFKSchema(fk *ForeignKeyConstraint, fkid int, schemaObj *schema.Schema) error {
	// Check if parent table exists
	refTable, ok := schemaObj.GetTable(fk.RefTable)
	if !ok {
		// Parent table doesn't exist
		// This is only an error if the child table has data (checked by FindViolations)
		// Not a schema mismatch error at this level
		return nil
	}

	// Determine referenced columns (PRIMARY KEY if not specified)
	refCols := fk.RefColumns
	if len(refCols) == 0 {
		refCols = refTable.PrimaryKey
	}

	// Check column count mismatch (parent exists but wrong column count)
	if len(fk.Columns) != len(refCols) {
		return fmt.Errorf("foreign key mismatch - \"%s\" referencing \"%s\"", fk.Table, fk.RefTable)
	}

	// Check if referenced columns exist (parent exists but columns don't)
	for _, colName := range refCols {
		if _, ok := refTable.GetColumn(colName); !ok {
			return fmt.Errorf("foreign key mismatch - \"%s\" referencing \"%s\"", fk.Table, fk.RefTable)
		}
	}

	// Check if referenced columns have a unique constraint (parent exists but not unique)
	if !columnsAreUnique(refTable, refCols, schemaObj) {
		return fmt.Errorf("foreign key mismatch - \"%s\" referencing \"%s\"", fk.Table, fk.RefTable)
	}

	return nil
}

// ValidateFKAtCreateTime validates FK constraints at CREATE TABLE time.
// Only checks errors that should prevent table creation:
// - Column count mismatch
// - FK references a view instead of a table
// Does NOT check for non-unique columns (that's a PRAGMA foreign_key_check error, not a CREATE TABLE error)
func (m *ForeignKeyManager) ValidateFKAtCreateTime(tableName string, schemaObj interface{}) error {
	schemaTyped, ok := schemaObj.(*schema.Schema)
	if !ok {
		return fmt.Errorf("invalid schema object")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tableLower := strings.ToLower(tableName)
	constraints := m.constraints[tableLower]

	for _, fk := range constraints {
		// Check if referenced "table" is actually a view
		if schemaTyped.IsView(fk.RefTable) {
			return fmt.Errorf("foreign key on %s references view %s", fk.Table, fk.RefTable)
		}

		// Check if parent table exists
		refTable, ok := schemaTyped.GetTable(fk.RefTable)
		if !ok {
			// Parent table doesn't exist - not an error at CREATE TABLE time
			// (the FK will be checked later when the parent table is created or data is inserted)
			continue
		}

		// Determine referenced columns
		refCols := fk.RefColumns
		if len(refCols) == 0 {
			refCols = refTable.PrimaryKey
		}

		// Check column count mismatch
		if len(fk.Columns) != len(refCols) {
			return fmt.Errorf("number of columns in foreign key does not match")
		}
	}

	return nil
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

	// Schema mismatches should be caught by CheckSchemaMismatch before calling FindViolations
	// If we get here with a schema mismatch, just skip scanning (no violations to find)
	if !columnsAreUnique(refTable, refCols, schemaObj) {
		return nil, nil
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

// extractCollations extracts collation names for the given columns from a table.
// Returns a slice of collation names, one per column.
// If a column has no explicit collation, returns "BINARY" (the default).
func extractCollations(table *schema.Table, columns []string) []string {
	collations := make([]string, len(columns))
	for i, colName := range columns {
		col, ok := table.GetColumn(colName)
		if !ok {
			collations[i] = "BINARY"
			continue
		}
		if col.Collation == "" {
			collations[i] = "BINARY"
		} else {
			collations[i] = col.Collation
		}
	}
	return collations
}

// Note: Affinity conversion functions have been removed as they are currently
// unused. FK value comparison is handled through collation-aware comparison
// in the RowReader implementations.
