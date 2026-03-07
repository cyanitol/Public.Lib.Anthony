// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
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

// RowExists checks if a row exists with the given column values in the referenced table.
// It returns true if a matching row is found, false otherwise.
func (r *VDBERowReader) RowExists(tableName string, columns []string, values []interface{}) (bool, error) {
	if r.vdbe == nil || r.vdbe.Ctx == nil {
		return false, fmt.Errorf("vdbe context not available")
	}

	// Get the table from schema
	table, err := r.getTable(tableName)
	if err != nil {
		return false, err
	}

	// Find the root page for the table
	rootPage := table.RootPage

	// Open a temporary cursor for reading
	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)

	if err := r.openReadCursor(cursorNum, rootPage); err != nil {
		return false, err
	}

	// Search for a matching row
	return r.findMatchingRow(cursorNum, table, columns, values)
}

// FindReferencingRows finds all rowids of rows that reference the given values.
// This is used for ON DELETE/UPDATE CASCADE operations.
func (r *VDBERowReader) FindReferencingRows(tableName string, columns []string, values []interface{}) ([]int64, error) {
	if r.vdbe == nil || r.vdbe.Ctx == nil {
		return nil, fmt.Errorf("vdbe context not available")
	}

	table, err := r.getTable(tableName)
	if err != nil {
		return nil, err
	}

	rootPage := table.RootPage
	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)

	if err := r.openReadCursor(cursorNum, rootPage); err != nil {
		return nil, err
	}

	return r.collectMatchingRowids(cursorNum, table, columns, values)
}

// tableInfo represents table metadata needed for FK validation
type tableInfo struct {
	RootPage uint32
	Columns  []columnInfo
}

// columnInfo represents column metadata
type columnInfo struct {
	Name            string
	IsIntegerPK     bool // true if INTEGER PRIMARY KEY (rowid alias)
	PayloadColIndex int  // index in payload (-1 if stored as rowid)
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

	// Extract RootPage using reflection since it's a field, not a method
	val := reflect.ValueOf(tableIface)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	rootPageField := val.FieldByName("RootPage")
	if !rootPageField.IsValid() {
		return nil, fmt.Errorf("table type does not have RootPage field")
	}

	rootPage := uint32(rootPageField.Uint())

	info := &tableInfo{
		RootPage: rootPage,
		Columns:  make([]columnInfo, 0),
	}

	// Extract column info including INTEGER PRIMARY KEY detection
	payloadIdx := 0
	for _, colIface := range table.GetColumns() {
		type columnWithInfo interface {
			GetName() string
			IsPrimaryKeyColumn() bool
			GetType() string
		}

		col, ok := colIface.(columnWithInfo)
		if !ok {
			// Fallback for minimal interface
			type columnWithName interface {
				GetName() string
			}
			if minCol, minOk := colIface.(columnWithName); minOk {
				info.Columns = append(info.Columns, columnInfo{
					Name:            minCol.GetName(),
					IsIntegerPK:     false,
					PayloadColIndex: payloadIdx,
				})
				payloadIdx++
			}
			continue
		}

		// Check if this is INTEGER PRIMARY KEY (rowid alias)
		isIPK := col.IsPrimaryKeyColumn() && (col.GetType() == "INTEGER" || col.GetType() == "INT")

		if isIPK {
			info.Columns = append(info.Columns, columnInfo{
				Name:            col.GetName(),
				IsIntegerPK:     true,
				PayloadColIndex: -1, // Not in payload, stored as rowid
			})
		} else {
			info.Columns = append(info.Columns, columnInfo{
				Name:            col.GetName(),
				IsIntegerPK:     false,
				PayloadColIndex: payloadIdx,
			})
			payloadIdx++
		}
	}

	return info, nil
}

// allocTempCursor allocates a temporary cursor number.
func (r *VDBERowReader) allocTempCursor() int {
	// Use a high cursor number to avoid conflicts with existing cursors
	return len(r.vdbe.Cursors) + 1000
}

// openReadCursor opens a cursor for reading on the given root page.
func (r *VDBERowReader) openReadCursor(cursorNum int, rootPage uint32) error {
	bt, ok := r.vdbe.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree type")
	}

	// Ensure we have enough cursor slots
	if err := r.vdbe.AllocCursors(cursorNum + 1); err != nil {
		return err
	}

	// Create and store the cursor
	btCursor := btree.NewCursor(bt, rootPage)
	r.vdbe.Cursors[cursorNum] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		RootPage:    rootPage,
		BtreeCursor: btCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
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
	cursor := r.vdbe.Cursors[cursorNum]
	if cursor == nil {
		return false, fmt.Errorf("cursor not found")
	}

	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return false, fmt.Errorf("invalid cursor type")
	}

	// Rewind to start
	if err := btCursor.MoveToFirst(); err != nil {
		// Empty table/leaf means no rows - not an error for FK check, just return false
		errStr := err.Error()
		if strings.Contains(errStr, "empty") {
			return false, nil
		}
		return false, err
	}

	// Scan all rows
	for {
		if match, err := r.checkRowMatch(cursor, table, columns, values); err != nil {
			return false, err
		} else if match {
			return true, nil
		}

		// Move to next row
		if err := btCursor.Next(); err != nil {
			break // End of table
		}
	}

	return false, nil
}

// collectMatchingRowids finds all rowids that match the given column values.
func (r *VDBERowReader) collectMatchingRowids(cursorNum int, table *tableInfo, columns []string, values []interface{}) ([]int64, error) {
	cursor := r.vdbe.Cursors[cursorNum]
	if cursor == nil {
		return nil, fmt.Errorf("cursor not found")
	}

	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil, fmt.Errorf("invalid cursor type")
	}

	var rowids []int64

	if err := btCursor.MoveToFirst(); err != nil {
		if err.Error() == "empty table" {
			return rowids, nil
		}
		return nil, err
	}

	for {
		if match, err := r.checkRowMatch(cursor, table, columns, values); err != nil {
			return nil, err
		} else if match {
			rowids = append(rowids, btCursor.GetKey())
		}

		if err := btCursor.Next(); err != nil {
			break
		}
	}

	return rowids, nil
}

// checkRowMatch checks if the current row matches the given column values.
func (r *VDBERowReader) checkRowMatch(cursor *Cursor, table *tableInfo, columns []string, values []interface{}) (bool, error) {
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return false, fmt.Errorf("invalid cursor type")
	}

	payload, err := btCursor.GetPayloadWithOverflow()
	if err != nil {
		return false, err
	}

	rowid := btCursor.GetKey()

	// Parse the record and check column values
	for i, colName := range columns {
		colIdx := r.findColumnIndex(table, colName)
		if colIdx < 0 {
			return false, fmt.Errorf("column not found: %s", colName)
		}

		colInfo := table.Columns[colIdx]
		colValue := NewMem()

		if colInfo.IsIntegerPK {
			// INTEGER PRIMARY KEY is stored as rowid, not in payload
			colValue.SetInt(rowid)
		} else {
			// Regular column: extract from payload using PayloadColIndex
			if err := parseRecordColumn(payload, colInfo.PayloadColIndex, colValue); err != nil {
				return false, err
			}
		}

		// Compare with expected value
		if !r.valuesEqual(colValue, values[i]) {
			return false, nil
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
	switch v := value.(type) {
	case int:
		return mem.IsInt() && mem.IntValue() == int64(v)
	case int64:
		return mem.IsInt() && mem.IntValue() == v
	case float64:
		return compareMemToFloat64(mem, v)
	case string:
		return mem.IsString() && mem.StringValue() == v
	case []byte:
		return mem.IsBlob() && string(mem.BlobValue()) == string(v)
	default:
		return false
	}
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

// DeleteRow deletes a row by rowid using a writable cursor.
func (m *VDBERowModifier) DeleteRow(table string, rowid int64) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage)
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

// UpdateRow updates specific columns on a rowid using a delete/insert cycle.
func (m *VDBERowModifier) UpdateRow(table string, rowid int64, values map[string]interface{}) error {
	tableInfo, err := m.reader.getTable(table)
	if err != nil {
		return err
	}

	cursorNum := m.reader.allocTempCursor()
	defer m.reader.closeTempCursor(cursorNum)

	btCursor, err := m.openWriteCursor(cursorNum, tableInfo.RootPage)
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

	payload, err := btCursor.GetPayloadWithOverflow()
	if err != nil {
		return err
	}

	currentValues, err := m.readRowValues(tableInfo, payload, rowid)
	if err != nil {
		return err
	}

	for colIdx, col := range tableInfo.Columns {
		if newVal, ok := values[col.Name]; ok {
			currentValues[colIdx] = newVal
		}
	}

	if err := btCursor.Delete(); err != nil {
		return err
	}

	// Build payload excluding INTEGER PRIMARY KEY columns (they're stored as rowid)
	payloadValues := make([]interface{}, 0, len(currentValues))
	for i, col := range tableInfo.Columns {
		if !col.IsIntegerPK {
			payloadValues = append(payloadValues, currentValues[i])
		}
	}

	newPayload := encodeSimpleRecord(payloadValues)
	return btCursor.Insert(rowid, newPayload)
}

// openWriteCursor opens a writable cursor on the given root page.
func (m *VDBERowModifier) openWriteCursor(cursorNum int, rootPage uint32) (*btree.BtCursor, error) {
	bt, ok := m.reader.vdbe.Ctx.Btree.(*btree.Btree)
	if !ok {
		return nil, fmt.Errorf("invalid btree type")
	}

	if err := m.reader.vdbe.AllocCursors(cursorNum + 1); err != nil {
		return nil, err
	}

	btCursor := btree.NewCursor(bt, rootPage)
	m.reader.vdbe.Cursors[cursorNum] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		Writable:    true,
		RootPage:    rootPage,
		BtreeCursor: btCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
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
