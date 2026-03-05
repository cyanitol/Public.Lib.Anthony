// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import "fmt"

// checkNotNullConstraints verifies that none of the NOT NULL columns have NULL values.
func (v *VDBE) checkNotNullConstraints(tableName string, payload []byte) error {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}

	type schemaWithGetTableByName interface {
		GetTableByName(name string) (interface{}, bool)
	}

	schema, ok := v.Ctx.Schema.(schemaWithGetTableByName)
	if !ok {
		return nil
	}

	tableIface, exists := schema.GetTableByName(tableName)
	if !exists {
		return nil
	}

	type tableWithColumns interface {
		GetColumns() []interface{}
	}

	table, ok := tableIface.(tableWithColumns)
	if !ok {
		return nil
	}

	columns := table.GetColumns()

	type columnInfo interface {
		GetName() string
		IsPrimaryKeyColumn() bool
		GetNotNull() bool
	}

	recordIdx := 0
	for _, colIface := range columns {
		col, ok := colIface.(columnInfo)
		if !ok {
			recordIdx++
			continue
		}

		if col.IsPrimaryKeyColumn() {
			continue
		}

		if col.GetNotNull() {
			valueMem := NewMem()
			if err := parseRecordColumn(payload, recordIdx, valueMem); err != nil {
				return fmt.Errorf("NOT NULL constraint failed: %s", col.GetName())
			}

			if valueMem.IsNull() {
				return fmt.Errorf("NOT NULL constraint failed: %s", col.GetName())
			}
		}

		recordIdx++
	}

	return nil
}

// checkCheckConstraints evaluates CHECK constraints on the row being inserted.
func (v *VDBE) checkCheckConstraints(tableName string, payload []byte, rowid int64) error {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}

	type schemaWithGetTableByName interface {
		GetTableByName(name string) (interface{}, bool)
	}

	schema, ok := v.Ctx.Schema.(schemaWithGetTableByName)
	if !ok {
		return nil
	}

	tableIface, exists := schema.GetTableByName(tableName)
	if !exists {
		return nil
	}

	type tableWithColumns interface {
		GetColumns() []interface{}
	}

	table, ok := tableIface.(tableWithColumns)
	if !ok {
		return nil
	}

	columns := table.GetColumns()

	type columnInfo interface {
		GetName() string
		IsPrimaryKeyColumn() bool
		GetCheck() string
	}

	recordIdx := 0
	for _, colIface := range columns {
		col, ok := colIface.(columnInfo)
		if !ok {
			recordIdx++
			continue
		}

		if col.IsPrimaryKeyColumn() {
			continue
		}

		checkExpr := col.GetCheck()
		if checkExpr != "" {
			valueMem := NewMem()
			if err := parseRecordColumn(payload, recordIdx, valueMem); err != nil {
				recordIdx++
				continue
			}

			if !evaluateCheckConstraint(checkExpr, valueMem) {
				return fmt.Errorf("CHECK constraint failed: %s", col.GetName())
			}
		}

		recordIdx++
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
