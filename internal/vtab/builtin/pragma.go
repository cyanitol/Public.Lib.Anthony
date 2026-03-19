// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package builtin

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// PragmaModule implements virtual table modules for various PRAGMA tables.
// SQLite exposes many PRAGMA commands as virtual tables for easy querying.
type PragmaModule struct {
	vtab.BaseModule
	pragmaType string // "table_info", "index_list", "foreign_key_list", etc.
}

// NewPragmaTableInfoModule creates a module for pragma_table_info(table_name).
func NewPragmaTableInfoModule() *PragmaModule {
	return &PragmaModule{
		pragmaType: "table_info",
	}
}

// NewPragmaIndexListModule creates a module for pragma_index_list(table_name).
func NewPragmaIndexListModule() *PragmaModule {
	return &PragmaModule{
		pragmaType: "index_list",
	}
}

// Connect creates a connection to the pragma virtual table.
func (m *PragmaModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	var schema string

	switch m.pragmaType {
	case "table_info":
		schema = `CREATE TABLE pragma_table_info(
			cid INTEGER,
			name TEXT,
			type TEXT,
			notnull INTEGER,
			dflt_value TEXT,
			pk INTEGER
		)`
	case "index_list":
		schema = `CREATE TABLE pragma_index_list(
			seq INTEGER,
			name TEXT,
			unique INTEGER,
			origin TEXT,
			partial INTEGER
		)`
	default:
		return nil, "", fmt.Errorf("unknown pragma type: %s", m.pragmaType)
	}

	return &PragmaTable{
		db:         db,
		pragmaType: m.pragmaType,
		tableName:  "", // Will be set via constraints
	}, schema, nil
}

// Create is the same as Connect for pragma tables.
func (m *PragmaModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.Connect(db, moduleName, dbName, tableName, args)
}

// PragmaTable represents a pragma virtual table instance.
type PragmaTable struct {
	vtab.BaseVirtualTable
	db         interface{}
	pragmaType string
	tableName  string
}

// BestIndex analyzes constraints for the pragma table.
// Pragma tables typically require a table name constraint.
func (t *PragmaTable) BestIndex(info *vtab.IndexInfo) error {
	// For pragma tables, we typically need the table name as a hidden argument
	// This is usually passed as arg[0] in the table-valued function syntax:
	// SELECT * FROM pragma_table_info('table_name')

	// Simple cost estimation
	info.EstimatedCost = 10.0
	info.EstimatedRows = 10

	return nil
}

// Open creates a cursor for the pragma table.
func (t *PragmaTable) Open() (vtab.VirtualCursor, error) {
	return &PragmaCursor{
		table:      t,
		pragmaType: t.pragmaType,
		rows:       [][]interface{}{},
		pos:        -1,
	}, nil
}

// PragmaCursor represents a cursor for pragma tables.
type PragmaCursor struct {
	vtab.BaseCursor
	table      *PragmaTable
	pragmaType string
	tableName  string
	rows       [][]interface{}
	pos        int
}

// Filter initializes the cursor with the given table name.
func (c *PragmaCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	// The table name should be in argv[0] for table-valued functions
	// For now, we'll use a default table for demonstration
	c.tableName = "example_table"
	if len(argv) > 0 && argv[0] != nil {
		if str, ok := argv[0].(string); ok {
			c.tableName = str
		}
	}

	// Generate rows based on pragma type
	switch c.pragmaType {
	case "table_info":
		c.rows = c.generateTableInfo()
	case "index_list":
		c.rows = c.generateIndexList()
	default:
		c.rows = [][]interface{}{}
	}

	// Position at first row or EOF
	if len(c.rows) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}

	return nil
}

// generateTableInfo generates table_info rows for the current table.
func (c *PragmaCursor) generateTableInfo() [][]interface{} {
	// In a real implementation, this would query the schema for the table
	// For now, return sample data
	if c.tableName == "" {
		return [][]interface{}{}
	}

	// Example table_info output for a simple table
	return [][]interface{}{
		{int64(0), "id", "INTEGER", int64(0), nil, int64(1)},
		{int64(1), "name", "TEXT", int64(0), nil, int64(0)},
		{int64(2), "value", "REAL", int64(0), nil, int64(0)},
	}
}

// generateIndexList generates index_list rows for the current table.
func (c *PragmaCursor) generateIndexList() [][]interface{} {
	// In a real implementation, this would query indexes for the table
	// For now, return sample data
	if c.tableName == "" {
		return [][]interface{}{}
	}

	// Example index_list output
	return [][]interface{}{
		{int64(0), "idx_" + c.tableName + "_name", int64(0), "c", int64(0)},
	}
}

// Next advances to the next row.
func (c *PragmaCursor) Next() error {
	c.pos++
	return nil
}

// EOF returns true if we're at the end.
func (c *PragmaCursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.rows)
}

// Column returns the value of the specified column.
func (c *PragmaCursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, fmt.Errorf("cursor is at EOF")
	}

	row := c.rows[c.pos]
	if index < 0 || index >= len(row) {
		return nil, fmt.Errorf("column index %d out of range", index)
	}

	return row[index], nil
}

// Rowid returns the rowid of the current row.
func (c *PragmaCursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, fmt.Errorf("cursor is at EOF")
	}
	return int64(c.pos), nil
}

// Close closes the cursor.
func (c *PragmaCursor) Close() error {
	c.rows = nil
	return nil
}

// PragmaFunctionModule implements pragma functions that can be called like:
// SELECT * FROM pragma_table_info('tablename')
type PragmaFunctionModule struct {
	vtab.BaseModule
}

// NewPragmaFunctionModule creates a new pragma function module.
func NewPragmaFunctionModule() *PragmaFunctionModule {
	return &PragmaFunctionModule{}
}

// Connect handles connection for pragma functions.
func (m *PragmaFunctionModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	// Determine which pragma function based on the module name
	pragmaType := "table_info" // default

	// Parse module name to determine pragma type
	// e.g., "pragma_table_info" -> "table_info"
	if strings.HasPrefix(moduleName, "pragma_") {
		pragmaType = strings.TrimPrefix(moduleName, "pragma_")
	}

	var schema string
	switch pragmaType {
	case "table_info":
		schema = `CREATE TABLE x(cid, name, type, notnull, dflt_value, pk, hidden)`
	case "index_list":
		schema = `CREATE TABLE x(seq, name, unique, origin, partial, hidden)`
	case "foreign_key_list":
		schema = `CREATE TABLE x(id, seq, table, from, to, on_update, on_delete, match, hidden)`
	default:
		schema = `CREATE TABLE x(value, hidden)`
	}

	return &PragmaTable{
		db:         db,
		pragmaType: pragmaType,
	}, schema, nil
}

// Create is the same as Connect.
func (m *PragmaFunctionModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.Connect(db, moduleName, dbName, tableName, args)
}
