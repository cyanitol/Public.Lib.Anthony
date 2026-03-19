// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package builtin

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// SQLiteMasterModule implements a virtual table module for sqlite_master.
// This provides access to the schema metadata table.
type SQLiteMasterModule struct {
	vtab.BaseModule
}

// NewSQLiteMasterModule creates a new sqlite_master virtual table module.
func NewSQLiteMasterModule() *SQLiteMasterModule {
	return &SQLiteMasterModule{}
}

// Connect creates a connection to the sqlite_master virtual table.
func (m *SQLiteMasterModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	schema := `CREATE TABLE sqlite_master(
		type TEXT,
		name TEXT,
		tbl_name TEXT,
		rootpage INTEGER,
		sql TEXT
	)`

	return &SQLiteMasterTable{
		db: db,
	}, schema, nil
}

// Create creates a new sqlite_master virtual table.
// This is the same as Connect since sqlite_master is read-only and eponymous.
func (m *SQLiteMasterModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.Connect(db, moduleName, dbName, tableName, args)
}

// SQLiteMasterTable represents an instance of the sqlite_master virtual table.
type SQLiteMasterTable struct {
	vtab.BaseVirtualTable
	db interface{}
}

// BestIndex analyzes the query and determines the best index strategy.
func (t *SQLiteMasterTable) BestIndex(info *vtab.IndexInfo) error {
	// Simple implementation: we can use constraints on type and name columns
	argvIndex := 1

	for i, constraint := range info.Constraints {
		if !constraint.Usable {
			continue
		}

		// We can handle equality constraints on type (column 0) and name (column 1)
		if (constraint.Column == 0 || constraint.Column == 1) && constraint.Op == vtab.ConstraintEQ {
			info.SetConstraintUsage(i, argvIndex, true)
			argvIndex++
		}
	}

	// Estimate cost based on whether we have constraints
	if argvIndex > 1 {
		// We have some constraints, so the query will be more efficient
		info.EstimatedCost = 10.0
		info.EstimatedRows = 10
	} else {
		// Full table scan
		info.EstimatedCost = 100.0
		info.EstimatedRows = 100
	}

	return nil
}

// Open creates a new cursor for scanning the sqlite_master table.
func (t *SQLiteMasterTable) Open() (vtab.VirtualCursor, error) {
	return &SQLiteMasterCursor{
		table: t,
		rows:  []MasterRow{},
		pos:   -1,
	}, nil
}

// MasterRow represents a row in the sqlite_master table.
type MasterRow struct {
	Type     string
	Name     string
	TblName  string
	RootPage int64
	SQL      string
}

// SQLiteMasterCursor represents a cursor for iterating over sqlite_master rows.
type SQLiteMasterCursor struct {
	vtab.BaseCursor
	table      *SQLiteMasterTable
	rows       []MasterRow
	pos        int
	typeFilter string
	nameFilter string
}

// parseFilterConstraints extracts type and name filters from argv.
func (c *SQLiteMasterCursor) parseFilterConstraints(argv []interface{}) {
	c.typeFilter = ""
	c.nameFilter = ""

	for _, arg := range argv {
		if arg == nil {
			continue
		}
		c.assignFilterValue(arg)
	}
}

// assignFilterValue assigns a filter value to the appropriate field.
func (c *SQLiteMasterCursor) assignFilterValue(arg interface{}) {
	str, ok := arg.(string)
	if !ok {
		return
	}

	if c.typeFilter == "" {
		c.typeFilter = str
	} else if c.nameFilter == "" {
		c.nameFilter = str
	}
}

// loadSchemaRows loads rows from the database schema.
func (c *SQLiteMasterCursor) loadSchemaRows() {
	c.rows = []MasterRow{}

	// Try to cast db to *schema.Schema
	if sch, ok := c.table.db.(*schema.Schema); ok && sch != nil {
		// Load all tables from schema
		for _, table := range sch.Tables {
			c.rows = append(c.rows, MasterRow{
				Type:     "table",
				Name:     table.Name,
				TblName:  table.Name,
				RootPage: int64(table.RootPage),
				SQL:      table.SQL,
			})
		}

		// Load all indexes from schema
		for _, index := range sch.Indexes {
			c.rows = append(c.rows, MasterRow{
				Type:     "index",
				Name:     index.Name,
				TblName:  index.Table,
				RootPage: int64(index.RootPage),
				SQL:      index.SQL,
			})
		}

		// Load all views from schema
		for _, view := range sch.Views {
			c.rows = append(c.rows, MasterRow{
				Type:     "view",
				Name:     view.Name,
				TblName:  view.Name,
				RootPage: 0,
				SQL:      view.SQL,
			})
		}

		// Load all triggers from schema
		for _, trigger := range sch.Triggers {
			c.rows = append(c.rows, MasterRow{
				Type:     "trigger",
				Name:     trigger.Name,
				TblName:  trigger.Table,
				RootPage: 0,
				SQL:      trigger.SQL,
			})
		}
		return
	}

	// Fallback: return just sqlite_master itself when no schema is available
	c.rows = []MasterRow{
		{
			Type:     "table",
			Name:     "sqlite_master",
			TblName:  "sqlite_master",
			RootPage: 1,
			SQL:      "CREATE TABLE sqlite_master(type text,name text,tbl_name text,rootpage integer,sql text)",
		},
	}
}

// applyFilters filters rows based on type and name constraints.
func (c *SQLiteMasterCursor) applyFilters() {
	filtered := make([]MasterRow, 0, len(c.rows))
	for _, row := range c.rows {
		if c.matchesFilters(row) {
			filtered = append(filtered, row)
		}
	}
	c.rows = filtered
}

// matchesFilters returns true if the row matches all active filters.
func (c *SQLiteMasterCursor) matchesFilters(row MasterRow) bool {
	if c.typeFilter != "" && row.Type != c.typeFilter {
		return false
	}
	if c.nameFilter != "" && row.Name != c.nameFilter {
		return false
	}
	return true
}

// positionCursor sets the cursor position to the first row or EOF.
func (c *SQLiteMasterCursor) positionCursor() {
	if len(c.rows) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}
}

// Filter initializes the cursor with the given constraints.
func (c *SQLiteMasterCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	c.parseFilterConstraints(argv)
	c.loadSchemaRows()
	c.applyFilters()
	c.positionCursor()
	return nil
}

// Next advances to the next row.
func (c *SQLiteMasterCursor) Next() error {
	c.pos++
	return nil
}

// EOF returns true if we're past the last row.
func (c *SQLiteMasterCursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.rows)
}

// Column returns the value of the specified column for the current row.
func (c *SQLiteMasterCursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, fmt.Errorf("cursor is at EOF")
	}

	row := c.rows[c.pos]
	switch index {
	case 0: // type
		return row.Type, nil
	case 1: // name
		return row.Name, nil
	case 2: // tbl_name
		return row.TblName, nil
	case 3: // rootpage
		return row.RootPage, nil
	case 4: // sql
		return row.SQL, nil
	default:
		return nil, fmt.Errorf("column index %d out of range", index)
	}
}

// Rowid returns the rowid of the current row.
func (c *SQLiteMasterCursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, fmt.Errorf("cursor is at EOF")
	}
	return int64(c.pos), nil
}

// Close closes the cursor.
func (c *SQLiteMasterCursor) Close() error {
	c.rows = nil
	return nil
}
