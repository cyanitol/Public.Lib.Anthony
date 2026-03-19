// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

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

// evaluateCheckConstraint parses and evaluates a CHECK constraint expression
// against a single column value. NULL always passes (SQL standard).
func evaluateCheckConstraint(checkExpr string, value *Mem) bool {
	if value.IsNull() {
		return true
	}
	p := parser.NewParser(checkExpr)
	expr, err := p.ParseExpression()
	if err != nil {
		return true // Cannot parse — skip
	}
	return evalCheckExpr(expr, value)
}

// evalCheckExpr evaluates a parsed expression using the column value for any
// identifier reference. Returns true when the constraint is satisfied.
func evalCheckExpr(expr parser.Expression, value *Mem) bool {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return evalBinaryCheck(e, value)
	case *parser.UnaryExpr:
		return evalUnaryCheck(e, value)
	default:
		return true // Unknown expression types pass
	}
}

// evalBinaryCheck evaluates a binary comparison in a CHECK expression.
func evalBinaryCheck(e *parser.BinaryExpr, value *Mem) bool {
	if e.Op == parser.OpAnd {
		return evalCheckExpr(e.Left, value) && evalCheckExpr(e.Right, value)
	}
	if e.Op == parser.OpOr {
		return evalCheckExpr(e.Left, value) || evalCheckExpr(e.Right, value)
	}
	left := resolveCheckOperand(e.Left, value)
	right := resolveCheckOperand(e.Right, value)
	if left == nil || right == nil {
		return true // NULL in comparison — pass
	}
	return compareCheckValues(left, right, e.Op)
}

// evalUnaryCheck evaluates a unary expression in a CHECK context.
func evalUnaryCheck(e *parser.UnaryExpr, value *Mem) bool {
	if e.Op == parser.OpNot {
		return !evalCheckExpr(e.Expr, value)
	}
	return true
}

// resolveCheckOperand returns an int64 or float64 for a literal or column ref.
func resolveCheckOperand(expr parser.Expression, colValue *Mem) interface{} {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return literalToNumber(e)
	case *parser.IdentExpr:
		return memToNumber(colValue)
	case *parser.UnaryExpr:
		return resolveUnaryOperand(e, colValue)
	default:
		return nil
	}
}

// resolveUnaryOperand handles negated literals like -1.
func resolveUnaryOperand(e *parser.UnaryExpr, colValue *Mem) interface{} {
	if e.Op != parser.OpNeg {
		return nil
	}
	inner := resolveCheckOperand(e.Expr, colValue)
	switch v := inner.(type) {
	case int64:
		return -v
	case float64:
		return -v
	}
	return nil
}

// literalToNumber converts a LiteralExpr to int64 or float64.
func literalToNumber(lit *parser.LiteralExpr) interface{} {
	switch lit.Type {
	case parser.LiteralInteger:
		v, err := strconv.ParseInt(lit.Value, 10, 64)
		if err != nil {
			return nil
		}
		return v
	case parser.LiteralFloat:
		v, err := strconv.ParseFloat(lit.Value, 64)
		if err != nil {
			return nil
		}
		return v
	default:
		return nil
	}
}

// memToNumber extracts a numeric value from a Mem cell.
func memToNumber(m *Mem) interface{} {
	if m.IsNull() {
		return nil
	}
	if m.IsInt() {
		return m.IntValue()
	}
	if m.IsReal() {
		return m.RealValue()
	}
	return nil
}

// compareCheckValues compares two numeric operands with the given operator.
func compareCheckValues(left, right interface{}, op parser.BinaryOp) bool {
	lf := toFloat(left)
	rf := toFloat(right)
	switch op {
	case parser.OpGe:
		return lf >= rf
	case parser.OpGt:
		return lf > rf
	case parser.OpLe:
		return lf <= rf
	case parser.OpLt:
		return lf < rf
	case parser.OpEq:
		return lf == rf
	case parser.OpNe:
		return lf != rf
	default:
		return true
	}
}

// toFloat converts an int64 or float64 to float64.
func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

// autoincrementColumnChecker is used to detect if a table has AUTOINCREMENT.
type autoincrementColumnChecker interface {
	GetAutoincrementColumnIndex() int
}

// sequenceManagerAccessor provides access to the sequence manager.
type sequenceManagerAccessor interface {
	GetSequences() interface{}
}

// sequenceUpdater provides methods for updating sequence values.
type sequenceUpdater interface {
	NextSequence(tableName string, currentMaxRowid int64) (int64, error)
	UpdateSequence(tableName string, rowid int64)
	HasSequence(tableName string) bool
}

// isAutoincrementTable checks if the given table has an AUTOINCREMENT column.
func (v *VDBE) isAutoincrementTable(tableName string) bool {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return false
	}
	sch, ok := v.Ctx.Schema.(schemaWithGetTableByName)
	if !ok {
		return false
	}
	tableIface, exists := sch.GetTableByName(tableName)
	if !exists {
		return false
	}
	checker, ok := tableIface.(autoincrementColumnChecker)
	if !ok {
		return false
	}
	return checker.GetAutoincrementColumnIndex() >= 0
}

// getSequenceManager retrieves the sequence manager from the schema.
func (v *VDBE) getSequenceManager() sequenceUpdater {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}
	accessor, ok := v.Ctx.Schema.(sequenceManagerAccessor)
	if !ok {
		return nil
	}
	sm, ok := accessor.GetSequences().(sequenceUpdater)
	if !ok {
		return nil
	}
	return sm
}

// schemaIndexProvider gives access to unique indexes and PK info for a table.
type schemaIndexProvider interface {
	GetTableIndexes(tableName string) []uniqueIndexInfo
	GetTablePrimaryKey(tableName string) ([]string, bool)
	GetRecordColumnIndex(tableName, colName string) int
	GetColumnCollation(tableName, colName string) string
}

// uniqueIndexInfo describes a UNIQUE index from the schema.
type uniqueIndexInfo interface {
	IsUnique() bool
	GetColumns() []string
}

// checkSchemaUniqueConstraints checks unique indexes and composite PK
// constraints from the schema that are not covered by column-level checks.
func (v *VDBE) checkSchemaUniqueConstraints(tableName string, payload []byte, btCursor *btree.BtCursor, newRowid int64) error {
	provider := v.getSchemaIndexProvider(tableName)
	if provider == nil {
		return nil
	}
	if err := v.checkIndexUniqueConstraints(provider, tableName, payload, btCursor, newRowid); err != nil {
		return err
	}
	return v.checkCompositePKUnique(provider, tableName, payload, btCursor, newRowid)
}

// getSchemaIndexProvider retrieves a schemaIndexProvider from the VDBE context.
func (v *VDBE) getSchemaIndexProvider(tableName string) schemaIndexProvider {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}
	return newSchemaIndexAdapter(v.Ctx.Schema)
}

// newSchemaIndexAdapter creates an adapter if the schema supports the needed interfaces.
func newSchemaIndexAdapter(sch interface{}) schemaIndexProvider {
	type tableGetter interface {
		GetTableByName(string) (interface{}, bool)
	}
	if _, ok := sch.(tableGetter); !ok {
		return nil
	}
	return &schemaIndexAdapterImpl{schema: sch}
}

// schemaIndexAdapterImpl implements schemaIndexProvider using interface assertions.
type schemaIndexAdapterImpl struct {
	schema interface{}
}

func (a *schemaIndexAdapterImpl) GetTableIndexes(tableName string) []uniqueIndexInfo {
	type indexListGetter interface {
		ListIndexesForTable(string) []interface{}
	}
	// The schema stores indexes; we need to iterate them for this table.
	// Use the schema's GetTableIndexes which returns []*schema.Index.
	// We access it via a method that returns []interface{} to avoid import cycle.
	if getter, ok := a.schema.(indexListGetter); ok {
		raw := getter.ListIndexesForTable(tableName)
		return wrapIndexInterfaces(raw)
	}
	return nil
}

// wrapIndexInterfaces converts a slice of index interfaces to uniqueIndexInfo.
func wrapIndexInterfaces(items []interface{}) []uniqueIndexInfo {
	var result []uniqueIndexInfo
	for _, item := range items {
		if idx, ok := item.(uniqueIndexInfo); ok {
			result = append(result, idx)
		}
	}
	return result
}

func (a *schemaIndexAdapterImpl) GetTablePrimaryKey(tableName string) ([]string, bool) {
	tblIface := a.getTable(tableName)
	if tblIface == nil {
		return nil, false
	}
	type pkGetter interface {
		GetPrimaryKey() []string
	}
	if pk, ok := tblIface.(pkGetter); ok {
		cols := pk.GetPrimaryKey()
		return cols, len(cols) > 0
	}
	return nil, false
}

func (a *schemaIndexAdapterImpl) GetRecordColumnIndex(tableName, colName string) int {
	tblIface := a.getTable(tableName)
	if tblIface == nil {
		return -1
	}
	type recordColGetter interface {
		GetRecordColumnNames() []string
	}
	getter, ok := tblIface.(recordColGetter)
	if !ok {
		return -1
	}
	for i, name := range getter.GetRecordColumnNames() {
		if strings.EqualFold(name, colName) {
			return i
		}
	}
	return -1
}

func (a *schemaIndexAdapterImpl) GetColumnCollation(tableName, colName string) string {
	tblIface := a.getTable(tableName)
	if tblIface == nil {
		return ""
	}
	type collationGetter interface {
		GetColumnCollationByName(string) string
	}
	if getter, ok := tblIface.(collationGetter); ok {
		return getter.GetColumnCollationByName(colName)
	}
	return ""
}

func (a *schemaIndexAdapterImpl) getTable(tableName string) interface{} {
	type tableGetter interface {
		GetTableByName(string) (interface{}, bool)
	}
	getter, ok := a.schema.(tableGetter)
	if !ok {
		return nil
	}
	tbl, found := getter.GetTableByName(tableName)
	if !found {
		return nil
	}
	return tbl
}

// checkIndexUniqueConstraints checks UNIQUE indexes from CREATE UNIQUE INDEX.
func (v *VDBE) checkIndexUniqueConstraints(provider schemaIndexProvider, tableName string, payload []byte, btCursor *btree.BtCursor, newRowid int64) error {
	indexes := provider.GetTableIndexes(tableName)
	for _, idx := range indexes {
		if !idx.IsUnique() {
			continue
		}
		cols := idx.GetColumns()
		if err := v.checkMultiColUnique(provider, tableName, cols, payload, btCursor, newRowid); err != nil {
			return err
		}
	}
	return nil
}

// checkCompositePKUnique enforces uniqueness on composite primary keys
// and non-integer single-column primary keys.
func (v *VDBE) checkCompositePKUnique(provider schemaIndexProvider, tableName string, payload []byte, btCursor *btree.BtCursor, newRowid int64) error {
	pkCols, hasPK := provider.GetTablePrimaryKey(tableName)
	if !hasPK || len(pkCols) == 0 {
		return nil
	}
	return v.checkMultiColUnique(provider, tableName, pkCols, payload, btCursor, newRowid)
}

// checkMultiColUnique checks that a set of columns is unique across existing rows.
func (v *VDBE) checkMultiColUnique(provider schemaIndexProvider, tableName string, cols []string, payload []byte, btCursor *btree.BtCursor, newRowid int64) error {
	// Extract new values for the constraint columns
	newValues := make([]*Mem, len(cols))
	allNull := true
	for i, col := range cols {
		recIdx := provider.GetRecordColumnIndex(tableName, col)
		if recIdx < 0 {
			return nil // Column not in record, skip
		}
		m := NewMem()
		if err := parseRecordColumn(payload, recIdx, m); err != nil {
			return nil
		}
		newValues[i] = m
		if !m.IsNull() {
			allNull = false
		}
	}
	if allNull {
		return nil // All NULL — always distinct
	}
	return v.scanForMultiColDuplicate(btCursor, cols, newValues, newRowid, tableName, provider)
}

// scanForMultiColDuplicate scans all rows checking for matching values on the
// specified columns.
func (v *VDBE) scanForMultiColDuplicate(cursor *btree.BtCursor, cols []string, newValues []*Mem, skipRowid int64, tableName string, provider schemaIndexProvider) error {
	scanCursor := btree.NewCursor(cursor.Btree, cursor.RootPage)
	if err := scanCursor.MoveToFirst(); err != nil {
		return nil
	}
	for scanCursor.IsValid() {
		if err := v.checkMultiColRow(scanCursor, cols, newValues, skipRowid, tableName, provider); err != nil {
			return err
		}
		if err := scanCursor.Next(); err != nil {
			break
		}
	}
	return nil
}

// checkMultiColRow checks a single existing row for multi-column duplicate.
func (v *VDBE) checkMultiColRow(scanCursor *btree.BtCursor, cols []string, newValues []*Mem, skipRowid int64, tableName string, provider schemaIndexProvider) error {
	if scanCursor.GetKey() == skipRowid {
		return nil
	}
	recordData, err := scanCursor.GetPayloadWithOverflow()
	if err != nil {
		return nil
	}
	for i, col := range cols {
		recIdx := provider.GetRecordColumnIndex(tableName, col)
		if recIdx < 0 {
			return nil
		}
		existing := NewMem()
		if err := parseRecordColumn(recordData, recIdx, existing); err != nil {
			return nil
		}
		if existing.IsNull() || newValues[i].IsNull() {
			return nil // NULL is always distinct
		}
		collation := provider.GetColumnCollation(tableName, col)
		if v.compareMemValuesWithCollation(existing, newValues[i], collation) != 0 {
			return nil // Mismatch on this column
		}
	}
	return fmt.Errorf("UNIQUE constraint failed: %s.%s", tableName, strings.Join(cols, ", "))
}
