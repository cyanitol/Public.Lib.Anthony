// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
)

// VDBERowReader implements the constraint.RowReader interface for foreign key validation.
// It uses VDBE cursor operations to query the database.
type VDBERowReader struct {
	vdbe *VDBE
}

// NewVDBERowReader creates a new VDBERowReader adapter.
func NewVDBERowReader(v *VDBE) *VDBERowReader {
	return &VDBERowReader{vdbe: v}
}

// VDBERowModifier implements RowDeleter and RowUpdater for FK cascades.
type VDBERowModifier struct {
	reader *VDBERowReader
}

// NewVDBERowModifier creates a new modifier using VDBE cursor operations.
func NewVDBERowModifier(v *VDBE) *VDBERowModifier {
	return &VDBERowModifier{reader: NewVDBERowReader(v)}
}

// withReadCursor validates context, resolves a table, and opens a temporary read cursor.
// The callback receives the cursor number and table info; the cursor is closed on return.
func (r *VDBERowReader) withReadCursor(tableName string, fn func(int, *tableInfo) error) error {
	if err := r.validateContext(); err != nil {
		return err
	}
	table, err := r.getTable(tableName)
	if err != nil {
		return err
	}
	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)
	if err := r.openReadCursorForTable(cursorNum, table); err != nil {
		return err
	}
	return fn(cursorNum, table)
}

// RowExists checks if a row exists with the given column values in the referenced table.
// It returns true if a matching row is found, false otherwise.
func (r *VDBERowReader) RowExists(tableName string, columns []string, values []interface{}) (bool, error) {
	var result bool
	err := r.withReadCursor(tableName, func(cur int, t *tableInfo) error {
		var e error
		result, e = r.findMatchingRow(cur, t, columns, values)
		return e
	})
	return result, err
}

// RowExistsWithCollation checks if a row exists using specified collations per column.
func (r *VDBERowReader) RowExistsWithCollation(tableName string, columns []string, values []interface{}, collations []string) (bool, error) {
	var result bool
	err := r.withReadCursor(tableName, func(cur int, t *tableInfo) error {
		var e error
		result, e = r.findMatchingRowWithCollation(cur, t, columns, values, collations)
		return e
	})
	return result, err
}

// FindReferencingRows finds all rowids of rows that reference the given values.
// This is used for ON DELETE/UPDATE CASCADE operations.
func (r *VDBERowReader) FindReferencingRows(tableName string, columns []string, values []interface{}) ([]int64, error) {
	var result []int64
	err := r.withReadCursor(tableName, func(cur int, t *tableInfo) error {
		var e error
		result, e = r.collectMatchingRowids(cur, t, columns, values)
		return e
	})
	return result, err
}

// FindReferencingRowsWithData finds all rows that match and returns their complete data.
// This is used for CASCADE operations on WITHOUT ROWID tables where we need primary key values.
func (r *VDBERowReader) FindReferencingRowsWithData(tableName string, columns []string, values []interface{}) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	err := r.withReadCursor(tableName, func(cur int, t *tableInfo) error {
		var e error
		result, e = r.collectMatchingRowData(cur, t, columns, values)
		return e
	})
	return result, err
}

// FindReferencingRowsWithParentAffinity finds all rowids with affinity and collation-aware matching.
// This applies the parent column's affinity and collation to child values before comparison.
func (r *VDBERowReader) FindReferencingRowsWithParentAffinity(
	childTableName string,
	childColumns []string,
	parentValues []interface{},
	parentTableName string,
	parentColumns []string,
) ([]int64, error) {
	if err := r.validateContext(); err != nil {
		return nil, err
	}
	parentTable, err := r.getTable(parentTableName)
	if err != nil {
		return nil, err
	}
	var result []int64
	err = r.withReadCursor(childTableName, func(cur int, childTable *tableInfo) error {
		var e error
		result, e = r.collectMatchingRowidsWithAffinityAndCollation(cur, childTable, childColumns, parentValues, parentTable, parentColumns)
		return e
	})
	return result, err
}

// ReadRowByRowid reads a row's values by its rowid.
// Used for recursive CASCADE operations.
// For WITHOUT ROWID tables, rowid is encoded as a composite key of primary key columns.
func (r *VDBERowReader) ReadRowByRowid(tableName string, rowid int64) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := r.withReadCursor(tableName, func(cur int, table *tableInfo) error {
		if table.WithoutRowID {
			return fmt.Errorf("ReadRowByRowid not supported for WITHOUT ROWID table: %s (use ReadRowByKey)", tableName)
		}
		cursor, e := r.getBTreeCursor(cur)
		if e != nil {
			return e
		}
		if e = r.seekToRowid(cursor, rowid); e != nil {
			return e
		}
		result, e = r.readRowValuesFromCursor(r.vdbe.Cursors[cur], table)
		return e
	})
	return result, err
}

// seekByKeyValues seeks a cursor to the row identified by keyValues.
func (r *VDBERowReader) seekByKeyValues(cursor *btree.BtCursor, table *tableInfo, keyValues []interface{}) error {
	if table.WithoutRowID {
		keyBytes := encodeCompositeKey(keyValues)
		found, err := cursor.SeekComposite(keyBytes)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("row not found with key %v", keyValues)
		}
		return nil
	}
	if len(keyValues) != 1 {
		return fmt.Errorf("regular tables expect single rowid value")
	}
	rowid, ok := keyValues[0].(int64)
	if !ok {
		return fmt.Errorf("rowid must be int64")
	}
	return r.seekToRowid(cursor, rowid)
}

// ReadRowByKey reads a row's values by its primary key values.
// Used for recursive CASCADE operations on WITHOUT ROWID tables.
func (r *VDBERowReader) ReadRowByKey(tableName string, keyValues []interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := r.withReadCursor(tableName, func(cur int, table *tableInfo) error {
		cursor, e := r.getBTreeCursor(cur)
		if e != nil {
			return e
		}
		if e = r.seekByKeyValues(cursor, table, keyValues); e != nil {
			return e
		}
		result, e = r.readRowValuesFromCursor(r.vdbe.Cursors[cur], table)
		return e
	})
	return result, err
}

// validateContext checks if VDBE context is available
func (r *VDBERowReader) validateContext() error {
	if r.vdbe == nil || r.vdbe.Ctx == nil {
		return fmt.Errorf("vdbe context not available")
	}
	return nil
}

// getBTreeCursor retrieves and validates the btree cursor at the given cursor number
func (r *VDBERowReader) getBTreeCursor(cursorNum int) (*btree.BtCursor, error) {
	cursor := r.vdbe.Cursors[cursorNum]
	if cursor == nil {
		return nil, fmt.Errorf("cursor not found")
	}

	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil, fmt.Errorf("invalid cursor type")
	}

	return btCursor, nil
}

// seekToRowid seeks to a specific rowid and validates it was found
func (r *VDBERowReader) seekToRowid(btCursor *btree.BtCursor, rowid int64) error {
	found, err := btCursor.SeekRowid(rowid)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("row not found: rowid %d", rowid)
	}
	return nil
}

// readRowValuesFromCursor reads all column values from the current cursor position.
func (r *VDBERowReader) readRowValuesFromCursor(cursor *Cursor, table *tableInfo) (map[string]interface{}, error) {
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil, fmt.Errorf("invalid cursor type")
	}

	rowid := btCursor.GetKey()
	payload, err := btCursor.GetCompletePayload()
	if err != nil {
		return nil, fmt.Errorf("failed to get payload: %w", err)
	}

	result := make(map[string]interface{})
	for _, col := range table.Columns {
		if col.IsIntegerPK {
			result[col.Name] = rowid
		} else {
			mem := NewMem()
			if err := parseRecordColumn(payload, col.PayloadColIndex, mem); err != nil {
				return nil, fmt.Errorf("read column %s: %w", col.Name, err)
			}
			result[col.Name] = memToInterface(mem)
		}
	}
	return result, nil
}

// tableInfo represents table metadata needed for FK validation
type tableInfo struct {
	RootPage        uint32
	Columns         []columnInfo
	WithoutRowID    bool  // true for WITHOUT ROWID tables
	PKColumnIndices []int // indices of primary key columns in Columns
}

// columnInfo represents column metadata
type columnInfo struct {
	Name            string
	Type            string // Column type for affinity determination
	Collation       string // Column collation (e.g., BINARY, NOCASE, RTRIM)
	IsIntegerPK     bool   // true if INTEGER PRIMARY KEY (rowid alias)
	PayloadColIndex int    // index in payload (-1 if stored as rowid)
}

// getTable retrieves table metadata from the schema.
func (r *VDBERowReader) getTable(tableName string) (*tableInfo, error) {
	// Try GetTableByName first (returns interface{}, bool)
	type schemaWithGetTableByName interface {
		GetTableByName(name string) (interface{}, bool)
	}

	if schemaObj, ok := r.vdbe.Ctx.Schema.(schemaWithGetTableByName); ok {
		tableIface, found := schemaObj.GetTableByName(tableName)
		if !found {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
		return r.extractTableInfo(tableIface)
	}

	// Fallback: try GetTable with *Table return type using reflection
	val := reflect.ValueOf(r.vdbe.Ctx.Schema)
	method := val.MethodByName("GetTable")
	if !method.IsValid() {
		return nil, fmt.Errorf("invalid schema type: no GetTable method")
	}

	results := method.Call([]reflect.Value{reflect.ValueOf(tableName)})
	if len(results) != 2 || !results[1].Bool() {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return r.extractTableInfo(results[0].Interface())
}

// extractTableInfo extracts table information from the schema table object
func (r *VDBERowReader) extractTableInfo(tableIface interface{}) (*tableInfo, error) {
	// Type assert to access table properties
	type tableWithColumns interface {
		GetColumns() []interface{}
	}

	table, ok := tableIface.(tableWithColumns)
	if !ok {
		return nil, fmt.Errorf("invalid table type")
	}

	rootPage, withoutRowID, err := r.extractTableMetadata(tableIface)
	if err != nil {
		return nil, err
	}

	info := &tableInfo{
		RootPage:        rootPage,
		Columns:         make([]columnInfo, 0),
		WithoutRowID:    withoutRowID,
		PKColumnIndices: make([]int, 0),
	}

	// Extract column info and track PK columns
	payloadIdx := 0
	for colIdx, colIface := range table.GetColumns() {
		colInfo, payloadIncrement := r.buildColumnInfo(colIface, withoutRowID, payloadIdx)
		info.Columns = append(info.Columns, colInfo)

		// Track primary key column indices
		if r.isPKColumn(colIface) {
			info.PKColumnIndices = append(info.PKColumnIndices, colIdx)
		}

		payloadIdx += payloadIncrement
	}

	return info, nil
}

// isPKColumn checks if a column is part of the primary key
func (r *VDBERowReader) isPKColumn(colIface interface{}) bool {
	type columnWithPK interface {
		IsPrimaryKeyColumn() bool
	}
	if col, ok := colIface.(columnWithPK); ok {
		return col.IsPrimaryKeyColumn()
	}
	return false
}

// extractTableMetadata extracts RootPage and WithoutRowID from table using reflection
func (r *VDBERowReader) extractTableMetadata(tableIface interface{}) (uint32, bool, error) {
	val := reflect.ValueOf(tableIface)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	rootPageField := val.FieldByName("RootPage")
	if !rootPageField.IsValid() {
		return 0, false, fmt.Errorf("table type does not have RootPage field")
	}
	rootPage := uint32(rootPageField.Uint())

	withoutRowID := false
	if wField := val.FieldByName("WithoutRowID"); wField.IsValid() {
		withoutRowID = wField.Bool()
	}

	return rootPage, withoutRowID, nil
}

// buildColumnInfo creates columnInfo from column interface and returns payload increment
func (r *VDBERowReader) buildColumnInfo(colIface interface{}, withoutRowID bool, payloadIdx int) (columnInfo, int) {
	type columnWithInfo interface {
		GetName() string
		IsPrimaryKeyColumn() bool
		GetType() string
	}

	type columnWithCollation interface {
		GetCollation() string
	}

	col, ok := colIface.(columnWithInfo)
	if !ok {
		return r.buildMinimalColumnInfo(colIface, payloadIdx)
	}

	colType := col.GetType()

	// Try to get collation if available
	var collation string
	if colWithColl, ok := colIface.(columnWithCollation); ok {
		collation = colWithColl.GetCollation()
	}

	// INTEGER PRIMARY KEY only acts as rowid alias for regular tables (not WITHOUT ROWID)
	isIPK := !withoutRowID && col.IsPrimaryKeyColumn() && (colType == "INTEGER" || colType == "INT")

	if isIPK {
		return columnInfo{
			Name:            col.GetName(),
			Type:            colType,
			Collation:       collation,
			IsIntegerPK:     true,
			PayloadColIndex: -1,
		}, 0
	}

	return columnInfo{
		Name:            col.GetName(),
		Type:            colType,
		Collation:       collation,
		IsIntegerPK:     false,
		PayloadColIndex: payloadIdx,
	}, 1
}

// buildMinimalColumnInfo handles columns with minimal interface
func (r *VDBERowReader) buildMinimalColumnInfo(colIface interface{}, payloadIdx int) (columnInfo, int) {
	type columnWithName interface {
		GetName() string
	}

	if minCol, ok := colIface.(columnWithName); ok {
		return columnInfo{
			Name:            minCol.GetName(),
			Type:            "",
			IsIntegerPK:     false,
			PayloadColIndex: payloadIdx,
		}, 1
	}

	return columnInfo{}, 0
}

// allocTempCursor allocates a temporary cursor number.
func (r *VDBERowReader) allocTempCursor() int {
	// Use a high cursor number to avoid conflicts with existing cursors
	return len(r.vdbe.Cursors) + 1000
}

// openReadCursorForTable opens a cursor for reading, handling both regular and WITHOUT ROWID tables.
func (r *VDBERowReader) openReadCursorForTable(cursorNum int, table *tableInfo) error {
	bt, ok := r.vdbe.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree type")
	}

	// Ensure we have enough cursor slots
	if err := r.vdbe.AllocCursors(cursorNum + 1); err != nil {
		return err
	}

	// Create cursor with appropriate options for WITHOUT ROWID
	btCursor := btree.NewCursorWithOptions(bt, table.RootPage, table.WithoutRowID)
	r.vdbe.Cursors[cursorNum] = &Cursor{
		CurType:      CursorBTree,
		IsTable:      true,
		RootPage:     table.RootPage,
		BtreeCursor:  btCursor,
		CachedCols:   make([][]byte, 0),
		CacheStatus:  0,
		WithoutRowID: table.WithoutRowID,
	}

	return nil
}

// closeTempCursor closes and removes a temporary cursor.
func (r *VDBERowReader) closeTempCursor(cursorNum int) {
	if cursorNum < len(r.vdbe.Cursors) && r.vdbe.Cursors[cursorNum] != nil {
		r.vdbe.Cursors[cursorNum] = nil
	}
}

// findMatchingRow scans the table to find a row matching the given values.
func (r *VDBERowReader) findMatchingRow(cursorNum int, table *tableInfo, columns []string, values []interface{}) (bool, error) {
	cursor, btCursor, empty, err := r.prepareTableScan(cursorNum)
	if err != nil || empty {
		return false, err
	}
	return r.scanForMatch(cursor, btCursor, table, columns, values)
}

// findMatchingRowWithCollation scans the table to find a row matching the given values with collation.
func (r *VDBERowReader) findMatchingRowWithCollation(cursorNum int, table *tableInfo, columns []string, values []interface{}, collations []string) (bool, error) {
	cursor, btCursor, empty, err := r.prepareTableScan(cursorNum)
	if err != nil || empty {
		return false, err
	}
	return r.scanForMatchWithCollation(cursor, btCursor, table, columns, values, collations)
}

// moveToFirstRow moves cursor to the first row, returns (isEmpty, error).
// isEmpty is true if the table is empty (cursor not valid), false if positioned at first row.
func (r *VDBERowReader) moveToFirstRow(btCursor *btree.BtCursor) (bool, error) {
	if err := btCursor.MoveToFirst(); err != nil {
		if r.isEmptyTableError(err) {
			return true, nil // Table is empty
		}
		return false, err
	}
	return false, nil // Cursor is at first row
}

// prepareTableScan gets the btree cursor, moves to the first row, and returns scan state.
// Returns (cursor, btCursor, isEmpty, error). If isEmpty is true, the table has no rows.
func (r *VDBERowReader) prepareTableScan(cursorNum int) (*Cursor, *btree.BtCursor, bool, error) {
	btCursor, err := r.getBTreeCursor(cursorNum)
	if err != nil {
		return nil, nil, false, err
	}
	isEmpty, err := r.moveToFirstRow(btCursor)
	if err != nil {
		return nil, nil, false, err
	}
	return r.vdbe.Cursors[cursorNum], btCursor, isEmpty, nil
}

// scanForMatch scans through rows looking for a match
func (r *VDBERowReader) scanForMatch(cursor *Cursor, btCursor *btree.BtCursor, table *tableInfo, columns []string, values []interface{}) (bool, error) {
	for {
		match, err := r.checkRowMatch(cursor, table, columns, values)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return false, nil
}

// scanForMatchWithCollation scans through rows looking for a match with collation
func (r *VDBERowReader) scanForMatchWithCollation(cursor *Cursor, btCursor *btree.BtCursor, table *tableInfo, columns []string, values []interface{}, collations []string) (bool, error) {
	for {
		match, err := r.checkRowMatchWithCollation(cursor, table, columns, values, collations)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return false, nil
}

// collectMatchingRowids finds all rowids that match the given column values.
func (r *VDBERowReader) collectMatchingRowids(cursorNum int, table *tableInfo, columns []string, values []interface{}) ([]int64, error) {
	cursor, btCursor, empty, err := r.prepareTableScan(cursorNum)
	if err != nil {
		return nil, err
	}
	if empty {
		return []int64{}, nil
	}
	return r.collectAllMatchingRowids(cursor, btCursor, table, columns, values)
}

// collectMatchingRowData finds all rows that match and returns their complete data.
func (r *VDBERowReader) collectMatchingRowData(cursorNum int, table *tableInfo, columns []string, values []interface{}) ([]map[string]interface{}, error) {
	cursor, btCursor, empty, err := r.prepareTableScan(cursorNum)
	if err != nil {
		return nil, err
	}
	if empty {
		return []map[string]interface{}{}, nil
	}
	return r.collectAllMatchingRowData(cursor, btCursor, table, columns, values)
}

// collectAllMatchingRowData scans all rows and collects full row data for matches.
func (r *VDBERowReader) collectAllMatchingRowData(cursor *Cursor, btCursor *btree.BtCursor, table *tableInfo, columns []string, values []interface{}) ([]map[string]interface{}, error) {
	var rowData []map[string]interface{}

	for {
		match, err := r.checkRowMatch(cursor, table, columns, values)
		if err != nil {
			return nil, err
		}
		if match {
			// Read the complete row data
			rowValues, err := r.readRowValuesFromCursor(cursor, table)
			if err != nil {
				return nil, err
			}
			rowData = append(rowData, rowValues)
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return rowData, nil
}

// collectMatchingRowidsWithAffinityAndCollation collects rowids using parent column affinity and collation.
func (r *VDBERowReader) collectMatchingRowidsWithAffinityAndCollation(
	cursorNum int,
	childTable *tableInfo,
	childColumns []string,
	parentValues []interface{},
	parentTable *tableInfo,
	parentColumns []string,
) ([]int64, error) {
	cursor, btCursor, empty, err := r.prepareTableScan(cursorNum)
	if err != nil {
		return nil, err
	}
	if empty {
		return []int64{}, nil
	}
	return r.collectAllMatchingRowidsWithAffinityAndCollation(
		cursor, btCursor, childTable, childColumns, parentValues, parentTable, parentColumns)
}

// collectAllMatchingRowidsWithAffinityAndCollation scans rows with parent affinity and collation.
func (r *VDBERowReader) collectAllMatchingRowidsWithAffinityAndCollation(
	cursor *Cursor,
	btCursor *btree.BtCursor,
	childTable *tableInfo,
	childColumns []string,
	parentValues []interface{},
	parentTable *tableInfo,
	parentColumns []string,
) ([]int64, error) {
	var rowids []int64

	for {
		match, err := r.checkRowMatchWithParentAffinityAndCollation(
			cursor, childTable, childColumns, parentValues, parentTable, parentColumns)
		if err != nil {
			return nil, err
		}
		if match {
			rowids = append(rowids, btCursor.GetKey())
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return rowids, nil
}

// collectAllMatchingRowids scans all rows and collects matching rowids
func (r *VDBERowReader) collectAllMatchingRowids(cursor *Cursor, btCursor *btree.BtCursor, table *tableInfo, columns []string, values []interface{}) ([]int64, error) {
	var rowids []int64

	for {
		match, err := r.checkRowMatch(cursor, table, columns, values)
		if err != nil {
			return nil, err
		}
		if match {
			rowids = append(rowids, btCursor.GetKey())
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return rowids, nil
}

// isEmptyTableError checks if error indicates an empty table
func (r *VDBERowReader) isEmptyTableError(err error) bool {
	errMsg := err.Error()
	return errMsg == "empty table" || errMsg == "empty leaf" || strings.Contains(errMsg, "empty leaf page")
}

// cursorPayload extracts the btree cursor, payload, and rowid from a Cursor.
func cursorPayload(cursor *Cursor) (*btree.BtCursor, []byte, int64, error) {
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil, nil, 0, fmt.Errorf("invalid cursor type")
	}
	payload, err := btCursor.GetPayloadWithOverflow()
	if err != nil {
		return nil, nil, 0, err
	}
	return btCursor, payload, btCursor.GetKey(), nil
}

// checkRowMatch checks if the current row matches the given column values.
func (r *VDBERowReader) checkRowMatch(cursor *Cursor, table *tableInfo, columns []string, values []interface{}) (bool, error) {
	_, payload, rowid, err := cursorPayload(cursor)
	if err != nil {
		return false, err
	}
	for i, colName := range columns {
		colValue, err := r.extractColumnValueFromRow(table, colName, payload, rowid)
		if err != nil {
			return false, err
		}
		colIdx := r.findColumnIndex(table, colName)
		if !r.valuesEqualWithAffinity(colValue, values[i], table.Columns[colIdx].Type) {
			return false, nil
		}
	}
	return true, nil
}

// getCollationForColumn returns the collation for a column at the given index.
func getCollationForColumn(collations []string, idx int) string {
	if idx < len(collations) && collations[idx] != "" {
		return collations[idx]
	}
	return "BINARY"
}

// checkRowMatchWithCollation checks if the current row matches the given column values with collation.
func (r *VDBERowReader) checkRowMatchWithCollation(cursor *Cursor, table *tableInfo, columns []string, values []interface{}, collations []string) (bool, error) {
	_, payload, rowid, err := cursorPayload(cursor)
	if err != nil {
		return false, err
	}
	for i, colName := range columns {
		colValue, err := r.extractColumnValueFromRow(table, colName, payload, rowid)
		if err != nil {
			return false, err
		}
		colIdx := r.findColumnIndex(table, colName)
		colCollation := getCollationForColumn(collations, i)
		if !r.valuesEqualWithAffinityAndCollation(colValue, values[i], table.Columns[colIdx].Type, colCollation) {
			return false, nil
		}
	}
	return true, nil
}

// extractColumnValueFromRow extracts a column value from the current row, handling INTEGER PK.
func (r *VDBERowReader) extractColumnValueFromRow(table *tableInfo, colName string, payload []byte, rowid int64) (*Mem, error) {
	colIdx := r.findColumnIndex(table, colName)
	if colIdx < 0 {
		return nil, fmt.Errorf("column not found: %s", colName)
	}
	colInfo := table.Columns[colIdx]
	value := NewMem()
	if colInfo.IsIntegerPK {
		value.SetInt(rowid)
	} else {
		if err := parseRecordColumn(payload, colInfo.PayloadColIndex, value); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// getParentColumnTypeAndCollation retrieves the type and collation for a parent column.
func (r *VDBERowReader) getParentColumnTypeAndCollation(parentTable *tableInfo, parentColumns []string, idx int) (string, string) {
	if idx >= len(parentColumns) || parentTable == nil {
		return "", ""
	}
	parentColIdx := r.findColumnIndex(parentTable, parentColumns[idx])
	if parentColIdx < 0 {
		return "", ""
	}
	return parentTable.Columns[parentColIdx].Type, parentTable.Columns[parentColIdx].Collation
}

// checkRowMatchWithParentAffinityAndCollation checks if child row matches parent values.
// It applies parent column affinity and collation to the comparison.
func (r *VDBERowReader) checkRowMatchWithParentAffinityAndCollation(
	cursor *Cursor,
	childTable *tableInfo,
	childColumns []string,
	parentValues []interface{},
	parentTable *tableInfo,
	parentColumns []string,
) (bool, error) {
	_, payload, rowid, err := cursorPayload(cursor)
	if err != nil {
		return false, err
	}

	for i, childColName := range childColumns {
		childValue, err := r.extractColumnValueFromRow(childTable, childColName, payload, rowid)
		if err != nil {
			return false, err
		}

		parentColType, parentCollation := r.getParentColumnTypeAndCollation(parentTable, parentColumns, i)

		if parentCollation != "" {
			if !r.valuesEqualWithCollation(childValue, parentValues[i], parentCollation) {
				return false, nil
			}
		} else {
			if !r.valuesEqualWithAffinity(childValue, parentValues[i], parentColType) {
				return false, nil
			}
		}
	}

	return true, nil
}

// findColumnIndex finds the index of a column in the table.
func (r *VDBERowReader) findColumnIndex(table *tableInfo, colName string) int {
	colNameLower := strings.ToLower(colName)
	for i, col := range table.Columns {
		if strings.ToLower(col.Name) == colNameLower {
			return i
		}
	}
	return -1
}

// valuesEqual compares a Mem value with an interface{} value.
func (r *VDBERowReader) valuesEqual(mem *Mem, value interface{}) bool {
	if mem.IsNull() {
		return value == nil
	}
	return compareMemToInterface(mem, value)
}

// compareMemToInterface compares a non-null Mem value with an interface{} value.
func compareMemToInterface(mem *Mem, value interface{}) bool {
	handlers := []func(*Mem, interface{}) (bool, bool){
		compareMemToInt,
		compareMemToInt64,
		compareMemToFloat64Handler,
		compareMemToString,
		compareMemToBlob,
	}

	for _, handler := range handlers {
		if matched, handled := handler(mem, value); handled {
			return matched
		}
	}

	return false
}

// compareMemToInt checks if mem matches an int value
func compareMemToInt(mem *Mem, value interface{}) (bool, bool) {
	v, ok := value.(int)
	if !ok {
		return false, false
	}
	return mem.IsInt() && mem.IntValue() == int64(v), true
}

// compareMemToInt64 checks if mem matches an int64 value
func compareMemToInt64(mem *Mem, value interface{}) (bool, bool) {
	v, ok := value.(int64)
	if !ok {
		return false, false
	}
	return mem.IsInt() && mem.IntValue() == v, true
}

// compareMemToFloat64Handler checks if mem matches a float64 value
func compareMemToFloat64Handler(mem *Mem, value interface{}) (bool, bool) {
	v, ok := value.(float64)
	if !ok {
		return false, false
	}
	return compareMemToFloat64(mem, v), true
}

// compareMemToString checks if mem matches a string value
func compareMemToString(mem *Mem, value interface{}) (bool, bool) {
	v, ok := value.(string)
	if !ok {
		return false, false
	}
	return mem.IsString() && mem.StringValue() == v, true
}

// compareMemToBlob checks if mem matches a []byte value
func compareMemToBlob(mem *Mem, value interface{}) (bool, bool) {
	v, ok := value.([]byte)
	if !ok {
		return false, false
	}
	return mem.IsBlob() && string(mem.BlobValue()) == string(v), true
}

// compareMemToFloat64 compares a Mem value with a float64.
func compareMemToFloat64(mem *Mem, v float64) bool {
	if mem.IsReal() {
		return mem.RealValue() == v
	}
	if mem.IsInt() {
		return float64(mem.IntValue()) == v
	}
	return false
}

// valuesEqualWithAffinity compares mem with value, applying parent column affinity for FK matching.
func (r *VDBERowReader) valuesEqualWithAffinity(mem *Mem, value interface{}, columnType string) bool {
	if mem.IsNull() {
		return value == nil
	}

	// Apply affinity to both sides for consistent comparison
	if columnType != "" {
		value = applyColumnAffinity(value, columnType)
		// Also apply affinity to the stored mem value
		memValue := memToInterface(mem)
		memValue = applyColumnAffinity(memValue, columnType)
		// Now compare the affinity-converted values directly
		return valuesEqualDirect(memValue, value)
	}

	return compareMemToInterface(mem, value)
}

// valuesEqualDirect compares two interface{} values directly.
func valuesEqualDirect(v1, v2 interface{}) bool {
	// After affinity conversion, types should match exactly
	// Just handle the common cases
	if v1 == v2 {
		return true
	}

	// Handle int/int64 equivalence
	n1, ok1 := toInt64(v1)
	n2, ok2 := toInt64(v2)
	if ok1 && ok2 {
		return n1 == n2
	}

	return false
}

// valuesEqualWithCollation compares mem with value using the specified collation.
func (r *VDBERowReader) valuesEqualWithCollation(mem *Mem, value interface{}, collationName string) bool {
	if mem.IsNull() {
		return value == nil
	}

	// For numeric types, collation doesn't apply - use regular comparison
	if mem.IsInt() || mem.IsReal() {
		return compareMemToInterface(mem, value)
	}

	// For strings, use collation-aware comparison
	if mem.IsString() {
		s1 := mem.StringValue()
		s2 := fmt.Sprintf("%v", value)
		return collation.Compare(s1, s2, collationName) == 0
	}

	// For other types, use regular comparison
	return compareMemToInterface(mem, value)
}

// valuesEqualWithAffinityAndCollation compares values with affinity conversion and collation.
// This applies the column's affinity to both values, then uses collation for string comparison.
func (r *VDBERowReader) valuesEqualWithAffinityAndCollation(mem *Mem, value interface{}, columnType string, collationName string) bool {
	if mem.IsNull() {
		return value == nil
	}

	// Apply affinity to both values
	memValue := memToInterface(mem)
	memValue = applyColumnAffinity(memValue, columnType)
	value = applyColumnAffinity(value, columnType)

	// After affinity, compare based on result types
	// If both are numeric, use numeric comparison
	n1, ok1 := toInt64(memValue)
	n2, ok2 := toInt64(value)
	if ok1 && ok2 {
		return n1 == n2
	}

	// If both are strings, use collation
	s1, ok1 := memValue.(string)
	s2, ok2 := value.(string)
	if ok1 && ok2 {
		return collation.Compare(s1, s2, collationName) == 0
	}

	// Fall back to direct equality
	return valuesEqualDirect(memValue, value)
}

// toInt64 converts various integer types to int64.
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// affinityRule defines a type affinity matching rule
type affinityRule struct {
	keywords []string
	apply    func(interface{}) interface{}
}

// applyColumnAffinity applies SQLite type affinity to value based on column type.
func applyColumnAffinity(value interface{}, columnType string) interface{} {
	if columnType == "" {
		return value
	}

	upper := strings.ToUpper(columnType)

	rules := []affinityRule{
		{keywords: []string{"INT"}, apply: applyIntegerAffinity},
		{keywords: []string{"CHAR", "CLOB", "TEXT"}, apply: applyTextAffinity},
		{keywords: []string{"REAL", "FLOA", "DOUB"}, apply: applyRealAffinity},
	}

	for _, rule := range rules {
		if matchesAnyKeyword(upper, rule.keywords) {
			return rule.apply(value)
		}
	}

	// NUMERIC affinity (default for non-empty types)
	return applyNumericAffinity(value)
}

// matchesAnyKeyword checks if str contains any of the keywords
func matchesAnyKeyword(str string, keywords []string) bool {
	for _, keyword := range keywords {
		if strings.Contains(str, keyword) {
			return true
		}
	}
	return false
}

// applyIntegerAffinity converts value to int64 when possible.
func applyIntegerAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f)
		}
		return v
	default:
		return value
	}
}

// applyRealAffinity converts value to float64 when possible.
func applyRealAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return v
	default:
		return value
	}
}

// applyNumericAffinity converts value to numeric type when possible.
func applyNumericAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		return v
	default:
		return value
	}
}

// applyTextAffinity converts value to string when possible.
func applyTextAffinity(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return value
	}
}

// DeleteRow deletes a row by rowid using a writable cursor.
// For WITHOUT ROWID tables, rowid is actually encoded key data.
func (m *VDBERowModifier) DeleteRow(table string, rowid int64) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	if tableInfo.WithoutRowID {
		return fmt.Errorf("DeleteRow not supported for WITHOUT ROWID table: %s (use DeleteRowByKey)", table)
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage, tableInfo.WithoutRowID)
	if err != nil {
		return err
	}

	found, err := btCursor.SeekRowid(rowid)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("rowid %d not found in table %s", rowid, table)
	}

	return btCursor.Delete()
}

// DeleteRowByKey deletes a row by its primary key values.
// Used for WITHOUT ROWID tables.
func (m *VDBERowModifier) DeleteRowByKey(table string, keyValues []interface{}) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage, tableInfo.WithoutRowID)
	if err != nil {
		return err
	}

	found, err := m.seekByKey(btCursor, tableInfo, keyValues)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("row not found with key %v in table %s", keyValues, table)
	}

	return btCursor.Delete()
}

// UpdateRow updates specific columns on a rowid using a delete/insert cycle.
// For WITHOUT ROWID tables, use UpdateRowByKey instead.
func (m *VDBERowModifier) UpdateRow(table string, rowid int64, values map[string]interface{}) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	if tableInfo.WithoutRowID {
		return fmt.Errorf("UpdateRow not supported for WITHOUT ROWID table: %s (use UpdateRowByKey)", table)
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage, tableInfo.WithoutRowID)
	if err != nil {
		return err
	}

	currentValues, err := m.fetchAndMergeValues(btCursor, tableInfo, rowid, table, values)
	if err != nil {
		return err
	}

	return m.replaceRow(btCursor, tableInfo, rowid, currentValues)
}

// seekByKey seeks to a row using either composite key or rowid.
func (m *VDBERowModifier) seekByKey(btCursor *btree.BtCursor, tableInfo *tableInfo, keyValues []interface{}) (bool, error) {
	if tableInfo.WithoutRowID {
		keyBytes := encodeCompositeKey(keyValues)
		return btCursor.SeekComposite(keyBytes)
	}
	if len(keyValues) != 1 {
		return false, fmt.Errorf("regular tables expect single rowid value")
	}
	rowid, ok := keyValues[0].(int64)
	if !ok {
		return false, fmt.Errorf("rowid must be int64")
	}
	return btCursor.SeekRowid(rowid)
}

// readAndMergeRowByKey reads the current row and merges in update values.
func (m *VDBERowModifier) readAndMergeRowByKey(btCursor *btree.BtCursor, tableInfo *tableInfo, values map[string]interface{}) ([]interface{}, int64, error) {
	payload, err := btCursor.GetPayloadWithOverflow()
	if err != nil {
		return nil, 0, err
	}
	var rowid int64
	if !tableInfo.WithoutRowID {
		rowid = btCursor.GetKey()
	}
	currentValues, err := m.readRowValues(tableInfo, payload, rowid)
	if err != nil {
		return nil, 0, err
	}
	for colIdx, col := range tableInfo.Columns {
		if newVal, ok := values[col.Name]; ok {
			currentValues[colIdx] = newVal
		}
	}
	return currentValues, rowid, nil
}

// UpdateRowByKey updates specific columns on a row identified by primary key values.
// Used for WITHOUT ROWID tables.
func (m *VDBERowModifier) UpdateRowByKey(table string, keyValues []interface{}, values map[string]interface{}) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage, tableInfo.WithoutRowID)
	if err != nil {
		return err
	}

	found, err := m.seekByKey(btCursor, tableInfo, keyValues)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("row not found with key %v in table %s", keyValues, table)
	}

	currentValues, rowid, err := m.readAndMergeRowByKey(btCursor, tableInfo, values)
	if err != nil {
		return err
	}

	if tableInfo.WithoutRowID {
		return m.replaceRowWithoutRowID(btCursor, tableInfo, keyValues, currentValues)
	}
	return m.replaceRow(btCursor, tableInfo, rowid, currentValues)
}

// fetchAndMergeValues retrieves current row values and merges with update values
func (m *VDBERowModifier) fetchAndMergeValues(btCursor *btree.BtCursor, tableInfo *tableInfo, rowid int64, tableName string, values map[string]interface{}) ([]interface{}, error) {
	found, err := btCursor.SeekRowid(rowid)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("rowid %d not found in table %s", rowid, tableName)
	}
	merged, _, err := m.readAndMergeRowByKey(btCursor, tableInfo, values)
	return merged, err
}

// replaceRow deletes the current row and inserts updated values
func (m *VDBERowModifier) replaceRow(btCursor *btree.BtCursor, tableInfo *tableInfo, rowid int64, currentValues []interface{}) error {
	if err := btCursor.Delete(); err != nil {
		return err
	}

	payloadValues := m.buildPayloadValues(tableInfo, currentValues)
	newPayload := encodeSimpleRecord(payloadValues)
	return btCursor.Insert(rowid, newPayload)
}

// buildPayloadValues creates payload excluding INTEGER PRIMARY KEY columns
func (m *VDBERowModifier) buildPayloadValues(tableInfo *tableInfo, currentValues []interface{}) []interface{} {
	payloadValues := make([]interface{}, 0, len(currentValues))
	for i, col := range tableInfo.Columns {
		if !col.IsIntegerPK {
			payloadValues = append(payloadValues, currentValues[i])
		}
	}
	return payloadValues
}

// openWriteCursor opens a writable cursor on the given root page.
func (m *VDBERowModifier) openWriteCursor(cursorNum int, rootPage uint32, withoutRowID bool) (*btree.BtCursor, error) {
	bt, ok := m.reader.vdbe.Ctx.Btree.(*btree.Btree)
	if !ok {
		return nil, fmt.Errorf("invalid btree type")
	}

	if err := m.reader.vdbe.AllocCursors(cursorNum + 1); err != nil {
		return nil, err
	}

	btCursor := btree.NewCursorWithOptions(bt, rootPage, withoutRowID)
	m.reader.vdbe.Cursors[cursorNum] = &Cursor{
		CurType:      CursorBTree,
		IsTable:      true,
		Writable:     true,
		RootPage:     rootPage,
		BtreeCursor:  btCursor,
		CachedCols:   make([][]byte, 0),
		CacheStatus:  0,
		WithoutRowID: withoutRowID,
	}

	return btCursor, nil
}

// readRowValues decodes all column values for a row payload.
func (m *VDBERowModifier) readRowValues(table *tableInfo, payload []byte, rowid int64) ([]interface{}, error) {
	values := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		if col.IsIntegerPK {
			// INTEGER PRIMARY KEY is stored as rowid, not in payload
			values[i] = rowid
		} else {
			mem := NewMem()
			if err := parseRecordColumn(payload, col.PayloadColIndex, mem); err != nil {
				return nil, fmt.Errorf("read column %s: %w", col.Name, err)
			}
			values[i] = memToInterface(mem)
		}
	}
	return values, nil
}

// replaceRowWithoutRowID handles row replacement for WITHOUT ROWID tables.
// This is more complex because the key might change.
func (m *VDBERowModifier) replaceRowWithoutRowID(btCursor *btree.BtCursor, tableInfo *tableInfo, oldKeyValues []interface{}, newValues []interface{}) error {
	// Delete the old row
	if err := btCursor.Delete(); err != nil {
		return err
	}

	// Build new key from primary key columns
	newKeyValues := m.extractPrimaryKeyValues(tableInfo, newValues)
	keyBytes := encodeCompositeKey(newKeyValues)

	// Build payload (all columns)
	payload := encodeSimpleRecord(newValues)

	// Insert with new key
	return btCursor.InsertWithComposite(0, keyBytes, payload)
}

// extractPrimaryKeyValues extracts primary key column values from a full row.
// This uses PKColumnIndices from tableInfo to identify which columns are PK columns.
func (m *VDBERowModifier) extractPrimaryKeyValues(tableInfo *tableInfo, rowValues []interface{}) []interface{} {
	// Use PKColumnIndices to extract only primary key values
	if len(tableInfo.PKColumnIndices) > 0 {
		pkValues := make([]interface{}, 0, len(tableInfo.PKColumnIndices))
		for _, idx := range tableInfo.PKColumnIndices {
			if idx < len(rowValues) {
				pkValues = append(pkValues, rowValues[idx])
			}
		}
		return pkValues
	}

	// Fallback: if no PK columns identified, return all values
	// This handles edge cases where schema info is incomplete
	return rowValues
}

// encodeCompositeKey is a local implementation matching withoutrowid.EncodeCompositeKey.
func encodeCompositeKey(values []interface{}) []byte {
	var buf []byte
	for _, v := range values {
		switch val := v.(type) {
		case nil:
			buf = append(buf, 0x00)
		case int:
			buf = append(buf, 0x10)
			buf = append(buf, encodeInt64ForKey(int64(val))...)
		case int64:
			buf = append(buf, 0x10)
			buf = append(buf, encodeInt64ForKey(val)...)
		case float64:
			buf = append(buf, 0x20)
			buf = append(buf, encodeFloat64ForKey(val)...)
		case string:
			buf = append(buf, 0x30)
			buf = append(buf, []byte(val)...)
			buf = append(buf, 0x00)
		case []byte:
			buf = append(buf, 0x40)
			buf = append(buf, val...)
			buf = append(buf, 0x00)
		default:
			buf = append(buf, 0x50)
			buf = append(buf, []byte(fmt.Sprintf("%v", val))...)
			buf = append(buf, 0x00)
		}
	}
	return buf
}

// encodeInt64ForKey encodes an int64 in sortable big-endian format.
func encodeInt64ForKey(v int64) []byte {
	u := uint64(v) ^ (1 << 63)
	return []byte{
		byte(u >> 56), byte(u >> 48), byte(u >> 40), byte(u >> 32),
		byte(u >> 24), byte(u >> 16), byte(u >> 8), byte(u),
	}
}

// encodeFloat64ForKey encodes a float64 in sortable big-endian format.
func encodeFloat64ForKey(v float64) []byte {
	bits := math.Float64bits(v)
	if v >= 0 {
		bits |= (1 << 63)
	} else {
		bits = ^bits
	}
	return []byte{
		byte(bits >> 56), byte(bits >> 48), byte(bits >> 40), byte(bits >> 32),
		byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits),
	}
}
