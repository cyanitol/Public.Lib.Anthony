// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"strconv"
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

	// Open a temporary cursor for reading
	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)

	if err := r.openReadCursorForTable(cursorNum, table); err != nil {
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

	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)

	if err := r.openReadCursorForTable(cursorNum, table); err != nil {
		return nil, err
	}

	return r.collectMatchingRowids(cursorNum, table, columns, values)
}

// ReadRowByRowid reads a row's values by its rowid.
// Used for recursive CASCADE operations.
func (r *VDBERowReader) ReadRowByRowid(tableName string, rowid int64) (map[string]interface{}, error) {
	if r.vdbe == nil || r.vdbe.Ctx == nil {
		return nil, fmt.Errorf("vdbe context not available")
	}

	table, err := r.getTable(tableName)
	if err != nil {
		return nil, err
	}

	// WITHOUT ROWID tables don't have rowids, so this function only works for regular tables
	if table.WithoutRowID {
		return nil, fmt.Errorf("ReadRowByRowid not supported for WITHOUT ROWID table: %s", tableName)
	}

	cursorNum := r.allocTempCursor()
	defer r.closeTempCursor(cursorNum)

	if err := r.openReadCursorForTable(cursorNum, table); err != nil {
		return nil, err
	}

	cursor := r.vdbe.Cursors[cursorNum]
	if cursor == nil {
		return nil, fmt.Errorf("cursor not found")
	}

	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil, fmt.Errorf("invalid cursor type")
	}

	// Seek to the row with the given rowid
	found, err := btCursor.SeekRowid(rowid)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("row not found: rowid %d", rowid)
	}

	// Read the row values using the existing readRowValuesFromCursor method
	return r.readRowValuesFromCursor(cursor, table)
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
	RootPage     uint32
	Columns      []columnInfo
	WithoutRowID bool // true for WITHOUT ROWID tables
}

// columnInfo represents column metadata
type columnInfo struct {
	Name            string
	Type            string // Column type for affinity determination
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
		RootPage:     rootPage,
		Columns:      make([]columnInfo, 0),
		WithoutRowID: withoutRowID,
	}

	// Extract column info
	payloadIdx := 0
	for _, colIface := range table.GetColumns() {
		colInfo, payloadIncrement := r.buildColumnInfo(colIface, withoutRowID, payloadIdx)
		info.Columns = append(info.Columns, colInfo)
		payloadIdx += payloadIncrement
	}

	return info, nil
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

	col, ok := colIface.(columnWithInfo)
	if !ok {
		return r.buildMinimalColumnInfo(colIface, payloadIdx)
	}

	colType := col.GetType()
	// INTEGER PRIMARY KEY only acts as rowid alias for regular tables (not WITHOUT ROWID)
	isIPK := !withoutRowID && col.IsPrimaryKeyColumn() && (colType == "INTEGER" || colType == "INT")

	if isIPK {
		return columnInfo{
			Name:            col.GetName(),
			Type:            colType,
			IsIntegerPK:     true,
			PayloadColIndex: -1,
		}, 0
	}

	return columnInfo{
		Name:            col.GetName(),
		Type:            colType,
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
		if r.isEmptyTableError(err) {
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

// isEmptyTableError checks if error indicates an empty table
func (r *VDBERowReader) isEmptyTableError(err error) bool {
	errMsg := err.Error()
	return errMsg == "empty table" || errMsg == "empty leaf" || strings.Contains(errMsg, "empty leaf page")
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
		if !r.valuesEqualWithAffinity(colValue, values[i], colInfo.Type) {
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

// applyColumnAffinity applies SQLite type affinity to value based on column type.
func applyColumnAffinity(value interface{}, columnType string) interface{} {
	upper := strings.ToUpper(columnType)

	// INTEGER affinity
	if strings.Contains(upper, "INT") {
		return applyIntegerAffinity(value)
	}

	// TEXT affinity
	if strings.Contains(upper, "CHAR") || strings.Contains(upper, "CLOB") || strings.Contains(upper, "TEXT") {
		return applyTextAffinity(value)
	}

	// REAL affinity
	if strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOA") || strings.Contains(upper, "DOUB") {
		return applyRealAffinity(value)
	}

	// NUMERIC affinity (default for non-empty types)
	if columnType != "" {
		return applyNumericAffinity(value)
	}

	return value
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

	currentValues, err := m.fetchAndMergeValues(btCursor, tableInfo, rowid, table, values)
	if err != nil {
		return err
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

	payload, err := btCursor.GetPayloadWithOverflow()
	if err != nil {
		return nil, err
	}

	currentValues, err := m.readRowValues(tableInfo, payload, rowid)
	if err != nil {
		return nil, err
	}

	for colIdx, col := range tableInfo.Columns {
		if newVal, ok := values[col.Name]; ok {
			currentValues[colIdx] = newVal
		}
	}

	return currentValues, nil
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
