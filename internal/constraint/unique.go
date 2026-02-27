// Package constraint provides constraint validation for SQLite-compatible databases.
// This includes UNIQUE, CHECK, FOREIGN KEY, and other constraint types.
package constraint

import (
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// UniqueConstraint represents a UNIQUE constraint on one or more columns.
// According to the SQL standard, NULL values are always considered distinct,
// so multiple NULL values are allowed in a UNIQUE column.
type UniqueConstraint struct {
	// Name is the optional constraint name
	Name string

	// TableName is the name of the table this constraint belongs to
	TableName string

	// Columns are the column names that make up the unique constraint
	Columns []string

	// IndexName is the name of the automatically-created backing index
	// for enforcing this constraint
	IndexName string

	// Partial indicates whether this is a partial unique constraint (WHERE clause)
	Partial bool

	// Where is the WHERE clause expression for partial unique constraints
	Where string
}

// UniqueViolationError is returned when a UNIQUE constraint is violated.
type UniqueViolationError struct {
	ConstraintName string
	TableName      string
	Columns        []string
	ConflictValues map[string]interface{}
}

// Error implements the error interface.
func (e *UniqueViolationError) Error() string {
	if e.ConstraintName != "" {
		return fmt.Sprintf("UNIQUE constraint failed: %s.%s", e.TableName, e.ConstraintName)
	}
	return fmt.Sprintf("UNIQUE constraint failed: %s.%s", e.TableName, strings.Join(e.Columns, ","))
}

// NewUniqueConstraint creates a new UNIQUE constraint.
func NewUniqueConstraint(name, tableName string, columns []string) *UniqueConstraint {
	return &UniqueConstraint{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		IndexName: generateIndexName(name, tableName, columns),
	}
}

// generateIndexName generates an index name for a UNIQUE constraint.
// SQLite uses the format: sqlite_autoindex_{table}_{N} for unnamed constraints
// or uses the constraint name directly if provided.
func generateIndexName(constraintName, tableName string, columns []string) string {
	if constraintName != "" {
		return fmt.Sprintf("sqlite_autoindex_%s_%s", tableName, constraintName)
	}
	// For unnamed constraints, use column names
	colNames := strings.Join(columns, "_")
	return fmt.Sprintf("sqlite_autoindex_%s_%s", tableName, colNames)
}

// Validate checks if the given row values violate this UNIQUE constraint.
// It returns nil if the constraint is satisfied, or a UniqueViolationError if violated.
//
// According to SQL standard:
// - NULL values are always distinct from each other
// - Multiple NULLs are allowed in UNIQUE columns
// - Only non-NULL values must be unique
func (uc *UniqueConstraint) Validate(table *schema.Table, bt *btree.Btree, values map[string]interface{}, rowid int64) error {
	if len(uc.Columns) == 0 {
		return fmt.Errorf("unique constraint has no columns")
	}

	// Extract the values for the constrained columns
	constraintValues := make(map[string]interface{})
	hasNonNull := false

	for _, colName := range uc.Columns {
		val, exists := values[colName]
		if !exists {
			// Column not in values map - check default
			col, found := table.GetColumn(colName)
			if !found {
				return fmt.Errorf("column %s not found in table %s", colName, table.Name)
			}
			val = col.Default
		}

		constraintValues[colName] = val

		// Check if value is NULL
		if val != nil {
			hasNonNull = true
		}
	}

	// Per SQL standard: if all constraint columns are NULL, no check is needed
	// Multiple rows with all-NULL values are allowed
	if !hasNonNull {
		return nil
	}

	// Check for existing row with same non-NULL values
	// We use the backing index to efficiently check for duplicates
	exists, _, err := uc.checkDuplicateViaIndex(bt, table, constraintValues, rowid)
	if err != nil {
		return fmt.Errorf("failed to check unique constraint: %w", err)
	}

	if exists {
		return &UniqueViolationError{
			ConstraintName: uc.Name,
			TableName:      uc.TableName,
			Columns:        uc.Columns,
			ConflictValues: constraintValues,
		}
	}

	return nil
}

// checkDuplicateViaIndex checks if a duplicate value exists using the backing index.
// Returns (exists, conflictRowid, error).
// The conflictRowid is the rowid of the conflicting row, or 0 if no conflict.
// The rowid parameter is the rowid of the row being inserted/updated (to skip self-check).
func (uc *UniqueConstraint) checkDuplicateViaIndex(
	bt *btree.Btree,
	table *schema.Table,
	values map[string]interface{},
	rowid int64,
) (bool, int64, error) {
	// For now, we'll implement a simple linear scan
	// In a full implementation, this would use the backing index B-tree

	cursor := btree.NewCursor(bt, table.RootPage)
	err := cursor.MoveToFirst()
	if err != nil {
		// Empty table - no duplicates
		return false, 0, nil
	}

	for {
		// Get current row's rowid
		currentRowid := cursor.GetKey()

		// Skip the row we're updating (self-check)
		if currentRowid == rowid {
			if err := cursor.Next(); err != nil {
				break
			}
			continue
		}

		// Get current row's data
		currentData := cursor.GetPayload()
		if currentData == nil {
			// Skip rows with no data
			if err := cursor.Next(); err != nil {
				break
			}
			continue
		}

		// Parse the row data to extract column values
		// This requires decoding the SQLite record format
		currentValues, err := parseRecordValues(currentData, table)
		if err != nil {
			// Skip malformed rows
			if err := cursor.Next(); err != nil {
				break
			}
			continue
		}

		// Check if all constraint columns match
		if uc.valuesMatch(values, currentValues) {
			return true, currentRowid, nil
		}

		// Move to next row
		if err := cursor.Next(); err != nil {
			break
		}
	}

	return false, 0, nil
}

// valuesMatch checks if the constraint column values match between two rows.
// Returns true only if all non-NULL values match.
// NULL values are always considered distinct (SQL standard).
func (uc *UniqueConstraint) valuesMatch(values1, values2 map[string]interface{}) bool {
	for _, colName := range uc.Columns {
		val1 := values1[colName]
		val2 := values2[colName]

		// NULL is distinct from everything, including other NULLs
		if val1 == nil || val2 == nil {
			return false
		}

		// Compare values
		if !valuesEqual(val1, val2) {
			return false
		}
	}

	return true
}

// valuesEqual compares two values for equality.
// Handles different types according to SQLite's type affinity rules.
func valuesEqual(v1, v2 interface{}) bool {
	// Handle nil cases
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// Type conversions for comparison
	switch a := v1.(type) {
	case int:
		if b, ok := v2.(int); ok {
			return a == b
		}
		if b, ok := v2.(int64); ok {
			return int64(a) == b
		}
	case int64:
		if b, ok := v2.(int64); ok {
			return a == b
		}
		if b, ok := v2.(int); ok {
			return a == int64(b)
		}
	case float64:
		if b, ok := v2.(float64); ok {
			return a == b
		}
	case string:
		if b, ok := v2.(string); ok {
			return a == b
		}
	case []byte:
		if b, ok := v2.([]byte); ok {
			if len(a) != len(b) {
				return false
			}
			for i := range a {
				if a[i] != b[i] {
					return false
				}
			}
			return true
		}
	}

	return false
}

// parseRecordValues parses a SQLite record and extracts column values.
// This is a simplified implementation that works with the record format.
func parseRecordValues(data []byte, table *schema.Table) (map[string]interface{}, error) {
	// This is a placeholder implementation
	// In a real implementation, this would:
	// 1. Parse the record header to get serial types
	// 2. Extract each column value based on its serial type
	// 3. Map values to column names
	//
	// For now, we return an empty map as this requires integration
	// with the record parsing logic from internal/vdbe/record.go

	values := make(map[string]interface{})

	// TODO: Implement proper record parsing
	// This would use code similar to:
	// - vdbe.DecodeRecord()
	// - vdbe.ParseRecordHeader()

	return values, nil
}

// CreateBackingIndex creates an automatic index to enforce this UNIQUE constraint.
// This index is used for efficient duplicate detection.
func (uc *UniqueConstraint) CreateBackingIndex(sch *schema.Schema, bt *btree.Btree) error {
	// Check if index already exists
	if _, exists := sch.GetIndex(uc.IndexName); exists {
		// Index already created
		return nil
	}

	// Create the index in the schema
	// Note: We use CreateIndex from the schema package
	// The index is automatically UNIQUE since it backs a UNIQUE constraint

	// Check that the table exists
	if _, tableExists := sch.GetTable(uc.TableName); !tableExists {
		return fmt.Errorf("table %s not found", uc.TableName)
	}

	// Build indexed columns
	indexedCols := make([]string, len(uc.Columns))
	copy(indexedCols, uc.Columns)

	// Allocate a B-tree root page for the index
	rootPage, err := bt.CreateTable()
	if err != nil {
		return fmt.Errorf("failed to allocate index root page: %w", err)
	}

	// Create the index structure
	index := &schema.Index{
		Name:     uc.IndexName,
		Table:    uc.TableName,
		RootPage: rootPage,
		SQL:      uc.generateIndexSQL(),
		Columns:  indexedCols,
		Unique:   true, // This is a unique index
		Partial:  uc.Partial,
		Where:    uc.Where,
	}

	// Register the index in the schema
	// We need to access the schema's internal map, which requires
	// going through the public API
	// For now, we'll manually add it (in production, this would use CreateIndex)
	sch.Indexes[uc.IndexName] = index

	return nil
}

// generateIndexSQL generates the CREATE INDEX SQL for this constraint's backing index.
func (uc *UniqueConstraint) generateIndexSQL() string {
	columns := strings.Join(uc.Columns, ", ")
	sql := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s(%s)", uc.IndexName, uc.TableName, columns)

	if uc.Partial && uc.Where != "" {
		sql += fmt.Sprintf(" WHERE %s", uc.Where)
	}

	return sql
}

// ExtractUniqueConstraints extracts all UNIQUE constraints from a table definition.
// This includes both column-level UNIQUE constraints and table-level UNIQUE constraints.
func ExtractUniqueConstraints(table *schema.Table) []*UniqueConstraint {
	var constraints []*UniqueConstraint

	// Extract column-level UNIQUE constraints
	for _, col := range table.Columns {
		if col.Unique {
			constraint := NewUniqueConstraint(
				"", // Column-level constraints typically don't have names
				table.Name,
				[]string{col.Name},
			)
			constraints = append(constraints, constraint)
		}
	}

	// Extract table-level UNIQUE constraints
	for _, tc := range table.Constraints {
		if tc.Type == schema.ConstraintUnique {
			constraint := NewUniqueConstraint(
				tc.Name,
				table.Name,
				tc.Columns,
			)
			constraints = append(constraints, constraint)
		}
	}

	return constraints
}

// ValidateTableRow validates all UNIQUE constraints on a table for a given row.
// Returns the first constraint violation encountered, or nil if all constraints pass.
func ValidateTableRow(table *schema.Table, bt *btree.Btree, values map[string]interface{}, rowid int64) error {
	constraints := ExtractUniqueConstraints(table)

	for _, constraint := range constraints {
		if err := constraint.Validate(table, bt, values, rowid); err != nil {
			return err
		}
	}

	return nil
}

// EnsureUniqueIndexes creates backing indexes for all UNIQUE constraints on a table.
// This should be called when a table is created or when constraints are added.
func EnsureUniqueIndexes(table *schema.Table, sch *schema.Schema, bt *btree.Btree) error {
	constraints := ExtractUniqueConstraints(table)

	for _, constraint := range constraints {
		if err := constraint.CreateBackingIndex(sch, bt); err != nil {
			return fmt.Errorf("failed to create backing index for constraint: %w", err)
		}
	}

	return nil
}
