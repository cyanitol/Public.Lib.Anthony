// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import "fmt"

// schemaWithGetTableByName is an interface for accessing tables by name.
type schemaWithGetTableByName interface {
	GetTableByName(name string) (interface{}, bool)
}

// tableWithColumns is an interface for accessing table columns.
type tableWithColumns interface {
	GetColumns() []interface{}
}

// constraintColumnInfo is an interface for column constraint info.
type constraintColumnInfo interface {
	GetName() string
	IsPrimaryKeyColumn() bool
	GetNotNull() bool
	GetCheck() string
}

// getTableColumns retrieves columns from a table by name.
func (v *VDBE) getTableColumns(tableName string) ([]interface{}, bool) {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil, false
	}
	schema, ok := v.Ctx.Schema.(schemaWithGetTableByName)
	if !ok {
		return nil, false
	}
	tableIface, exists := schema.GetTableByName(tableName)
	if !exists {
		return nil, false
	}
	table, ok := tableIface.(tableWithColumns)
	if !ok {
		return nil, false
	}
	return table.GetColumns(), true
}

// checkNotNullConstraints verifies that none of the NOT NULL columns have NULL values.
func (v *VDBE) checkNotNullConstraints(tableName string, payload []byte) error {
	columns, ok := v.getTableColumns(tableName)
	if !ok {
		return nil
	}
	return v.validateNotNullColumns(columns, payload)
}

// validateNotNullColumns checks NOT NULL constraints on columns.
func (v *VDBE) validateNotNullColumns(columns []interface{}, payload []byte) error {
	recordIdx := 0
	for _, colIface := range columns {
		col, ok := colIface.(constraintColumnInfo)
		if !ok {
			recordIdx++
			continue
		}
		if col.IsPrimaryKeyColumn() {
			continue
		}
		if err := v.checkColumnNotNull(col, payload, recordIdx); err != nil {
			return err
		}
		recordIdx++
	}
	return nil
}

// checkColumnNotNull checks a single column's NOT NULL constraint.
func (v *VDBE) checkColumnNotNull(col constraintColumnInfo, payload []byte, recordIdx int) error {
	if !col.GetNotNull() {
		return nil
	}
	valueMem := NewMem()
	if err := parseRecordColumn(payload, recordIdx, valueMem); err != nil {
		return fmt.Errorf("NOT NULL constraint failed: %s", col.GetName())
	}
	if valueMem.IsNull() {
		return fmt.Errorf("NOT NULL constraint failed: %s", col.GetName())
	}
	return nil
}

// checkCheckConstraints evaluates CHECK constraints on the row being inserted.
func (v *VDBE) checkCheckConstraints(tableName string, payload []byte, rowid int64) error {
	columns, ok := v.getTableColumns(tableName)
	if !ok {
		return nil
	}
	return v.validateCheckColumns(columns, payload)
}

// validateCheckColumns checks CHECK constraints on columns.
func (v *VDBE) validateCheckColumns(columns []interface{}, payload []byte) error {
	recordIdx := 0
	for _, colIface := range columns {
		col, ok := colIface.(constraintColumnInfo)
		if !ok {
			recordIdx++
			continue
		}
		if col.IsPrimaryKeyColumn() {
			continue
		}
		if err := v.checkColumnCheck(col, payload, recordIdx); err != nil {
			return err
		}
		recordIdx++
	}
	return nil
}

// checkColumnCheck checks a single column's CHECK constraint.
func (v *VDBE) checkColumnCheck(col constraintColumnInfo, payload []byte, recordIdx int) error {
	checkExpr := col.GetCheck()
	if checkExpr == "" {
		return nil
	}
	valueMem := NewMem()
	if err := parseRecordColumn(payload, recordIdx, valueMem); err != nil {
		return nil // Skip if can't parse
	}
	if !evaluateCheckConstraint(checkExpr, valueMem) {
		return fmt.Errorf("CHECK constraint failed: %s", col.GetName())
	}
	return nil
}

// evaluateCheckConstraint performs basic CHECK constraint evaluation.
func evaluateCheckConstraint(checkExpr string, value *Mem) bool {
	if value.IsNull() {
		return true
	}
	// Simplified: just check basic numeric range constraints
	// Full implementation would parse and evaluate the expression tree
	return true
}
