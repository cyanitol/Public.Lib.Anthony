// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package schema

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// sqlite_master table schema:
//
// CREATE TABLE sqlite_master (
//   type TEXT,      -- "table", "index", "trigger", "view"
//   name TEXT,      -- object name
//   tbl_name TEXT,  -- table name (for indexes/triggers)
//   rootpage INT,   -- root B-tree page
//   sql TEXT        -- CREATE statement
// );
//
// The sqlite_master table is always stored on page 1 of the database.

// MasterRow represents a row in the sqlite_master table.
type MasterRow struct {
	Type     string // "table", "index", "trigger", "view"
	Name     string // Object name
	TblName  string // Associated table name
	RootPage uint32 // Root page number
	SQL      string // CREATE statement
}

// isInternalTable reports whether name is an SQLite-internal table that should
// not be loaded into the user-visible schema.
func isInternalTable(name string) bool {
	return name == "sqlite_master" || name == "sqlite_sequence"
}

// isAutoIndex reports whether name is an automatically generated index
// (sqlite_autoindex_*) that carries no user-visible SQL definition.
func isAutoIndex(name string) bool {
	const prefix = "sqlite_autoindex"
	return len(name) > len(prefix) && name[:len(prefix)] == prefix
}

// processMasterTableRow parses and registers a single "table" master row.
// Internal tables are silently skipped.
func (s *Schema) processMasterTableRow(row MasterRow) error {
	if isInternalTable(row.Name) {
		return nil
	}
	table, err := s.parseTableSQL(row)
	if err != nil {
		return fmt.Errorf("failed to parse table %s: %w", row.Name, err)
	}
	s.Tables[table.Name] = table
	return nil
}

// processMasterIndexRow parses and registers a single "index" master row.
// Auto-generated indexes are silently skipped.
func (s *Schema) processMasterIndexRow(row MasterRow) error {
	if isAutoIndex(row.Name) {
		return nil
	}
	index, err := s.parseIndexSQL(row)
	if err != nil {
		return fmt.Errorf("failed to parse index %s: %w", row.Name, err)
	}
	s.Indexes[index.Name] = index
	return nil
}

// processMasterViewRow parses and registers a single "view" master row.
func (s *Schema) processMasterViewRow(row MasterRow) error {
	view, err := s.parseViewSQL(row)
	if err != nil {
		return fmt.Errorf("failed to parse view %s: %w", row.Name, err)
	}
	s.Views[view.Name] = view
	return nil
}

// processMasterRow dispatches a single sqlite_master row to the appropriate
// handler based on its type.  Unknown types (trigger, …) are ignored.
func (s *Schema) processMasterRow(row MasterRow) error {
	switch row.Type {
	case "table":
		return s.processMasterTableRow(row)
	case "index":
		return s.processMasterIndexRow(row)
	case "view":
		return s.processMasterViewRow(row)
	default:
		return nil
	}
}

// LoadFromMaster loads the schema from the sqlite_master table.
// This reads all table and index definitions from page 1 of the database.
func (s *Schema) LoadFromMaster(bt *btree.Btree) error {
	if bt == nil {
		return fmt.Errorf("nil btree")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// sqlite_master is on page 1
	const masterPageNum = 1

	rows, err := s.parseMasterPage(bt, masterPageNum)
	if err != nil {
		return fmt.Errorf("failed to parse sqlite_master: %w", err)
	}

	for _, row := range rows {
		if err := s.processMasterRow(row); err != nil {
			return err
		}
	}

	return nil
}

// SaveToMaster saves the current schema to the sqlite_master table.
// This writes all table and index definitions to page 1 of the database.
func (s *Schema) SaveToMaster(bt *btree.Btree) error {
	if bt == nil {
		return fmt.Errorf("nil btree")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build master rows from current schema
	rows := s.buildMasterRows()

	// Clear existing sqlite_master content and write new rows
	if err := s.clearMasterTable(bt); err != nil {
		return fmt.Errorf("failed to clear sqlite_master: %w", err)
	}

	// Write all rows to sqlite_master
	for _, row := range rows {
		if err := s.writeMasterRow(bt, row); err != nil {
			return fmt.Errorf("failed to write master row for %s: %w", row.Name, err)
		}
	}

	return nil
}

// parseMasterPage reads and parses the sqlite_master page.
// This is a simplified implementation - a full version would use the btree
// cursor to iterate through all cells in the page.
func (s *Schema) parseMasterPage(bt *btree.Btree, pageNum uint32) ([]MasterRow, error) {
	// In a real implementation, this would:
	// 1. Create a cursor on page 1
	// 2. Iterate through all cells
	// 3. Parse each record as a MasterRow
	// 4. Return the list of rows

	cur := btree.NewCursor(bt, pageNum)
	if err := cur.MoveToFirst(); err != nil {
		return []MasterRow{}, nil
	}

	var rows []MasterRow
	for cur.IsValid() {
		payload, err := cur.GetCompletePayload()
		if err != nil {
			break
		}
		if row, err := decodeMasterRow(payload); err == nil {
			rows = append(rows, row)
		}
		if err := cur.Next(); err != nil {
			break
		}
	}
	return rows, nil
}

// writeMasterPage writes rows to the sqlite_master page.
func (s *Schema) writeMasterPage(bt *btree.Btree, pageNum uint32, rows []MasterRow) error {
	return nil
}

// buildMasterRows builds a list of MasterRow entries from the current schema.
func (s *Schema) buildMasterRows() []MasterRow {
	var rows []MasterRow

	// Add tables (except internal tables)
	for name, table := range s.Tables {
		if !isInternalTable(name) {
			rows = append(rows, MasterRow{
				Type:     "table",
				Name:     table.Name,
				TblName:  table.Name,
				RootPage: table.RootPage,
				SQL:      table.SQL,
			})
		}
	}

	// Add indexes (except auto-indexes without SQL)
	for name, index := range s.Indexes {
		if !isAutoIndex(name) || index.SQL != "" {
			rows = append(rows, MasterRow{
				Type:     "index",
				Name:     index.Name,
				TblName:  index.Table,
				RootPage: index.RootPage,
				SQL:      index.SQL,
			})
		}
	}

	// Add views
	for _, view := range s.Views {
		rows = append(rows, MasterRow{
			Type:     "view",
			Name:     view.Name,
			TblName:  view.Name,
			RootPage: 0, // Views don't have root pages
			SQL:      view.SQL,
		})
	}

	// Add triggers
	for _, trigger := range s.Triggers {
		rows = append(rows, MasterRow{
			Type:     "trigger",
			Name:     trigger.Name,
			TblName:  trigger.Table,
			RootPage: 0, // Triggers don't have root pages
			SQL:      trigger.SQL,
		})
	}

	return rows
}

// clearMasterTable clears all existing content from sqlite_master table.
// This is used during VACUUM to rebuild the table from scratch.
func (s *Schema) clearMasterTable(bt *btree.Btree) error {
	// Create a cursor on sqlite_master (page 1)
	cur := btree.NewCursor(bt, 1)

	// Move to first entry
	if err := cur.MoveToFirst(); err != nil {
		// If no entries exist, that's fine
		return nil
	}

	// Collect all rowids to delete
	var rowids []int64
	for cur.IsValid() {
		if cur.CurrentCell != nil {
			rowids = append(rowids, cur.CurrentCell.Key)
		}
		if err := cur.Next(); err != nil {
			break
		}
	}

	// Delete all entries
	for _, rowid := range rowids {
		found, err := cur.SeekRowid(rowid)
		if err != nil {
			return fmt.Errorf("failed to seek to rowid %d: %w", rowid, err)
		}
		if !found {
			return fmt.Errorf("rowid %d not found", rowid)
		}
		if err := cur.Delete(); err != nil {
			return fmt.Errorf("failed to delete rowid %d: %w", rowid, err)
		}
	}

	return nil
}

// writeMasterRow writes a single row to the sqlite_master table.
func (s *Schema) writeMasterRow(bt *btree.Btree, row MasterRow) error {
	// Encode the row as a payload
	payload := encodeMasterRow(row)

	// Create a cursor on sqlite_master (page 1)
	cur := btree.NewCursor(bt, 1)

	// Find the insertion point (we use sequential rowids)
	// Move to the end and get the last rowid
	var nextRowid int64 = 1
	if err := cur.MoveToLast(); err == nil && cur.IsValid() {
		if cur.CurrentCell != nil {
			nextRowid = cur.CurrentCell.Key + 1
		}
	}

	// Insert the row
	if err := cur.Insert(nextRowid, payload); err != nil {
		return fmt.Errorf("failed to insert master row: %w", err)
	}

	return nil
}

// parseTableSQL parses a CREATE TABLE statement from a master row.
func (s *Schema) parseTableSQL(row MasterRow) (*Table, error) {
	if row.SQL == "" {
		return tableWithNoSQL(row), nil
	}

	createTable, err := parseSingleCreateTable(row.SQL)
	if err != nil {
		return nil, err
	}

	return buildTableFromStmt(createTable, row), nil
}

// tableWithNoSQL returns a bare Table for system rows that carry no SQL text.
func tableWithNoSQL(row MasterRow) *Table {
	return &Table{
		Name:     row.Name,
		RootPage: row.RootPage,
		SQL:      row.SQL,
		Columns:  []*Column{},
	}
}

// encodeMasterRow encodes a MasterRow as a simple length-prefixed record.
func encodeMasterRow(row MasterRow) []byte {
	fields := [][]byte{
		[]byte(row.Type),
		[]byte(row.Name),
		[]byte(row.TblName),
		intToVarintBytes(uint64(row.RootPage)),
		[]byte(row.SQL),
	}
	var out []byte
	for _, f := range fields {
		out = append(out, intToVarintBytes(uint64(len(f)))...)
		out = append(out, f...)
	}
	return out
}

// decodeMasterRow decodes the simplified record produced by encodeMasterRow.
func decodeMasterRow(payload []byte) (MasterRow, error) {
	var row MasterRow
	parts := make([][]byte, 0, 5)
	data := payload
	for len(parts) < 5 && len(data) > 0 {
		length, n := btree.GetVarint(data)
		if n == 0 || len(data) < n+int(length) {
			return row, fmt.Errorf("invalid master row encoding")
		}
		data = data[n:]
		parts = append(parts, data[:length])
		data = data[length:]
	}
	if len(parts) != 5 {
		return row, fmt.Errorf("invalid master row field count")
	}
	row.Type = string(parts[0])
	row.Name = string(parts[1])
	row.TblName = string(parts[2])
	row.RootPage = uint32(varintBytesToUint(parts[3]))
	row.SQL = string(parts[4])
	return row, nil
}

func intToVarintBytes(v uint64) []byte {
	buf := make([]byte, btree.VarintLen(v))
	n := btree.PutVarint(buf, v)
	return buf[:n]
}

func varintBytesToUint(b []byte) uint64 {
	v, _ := btree.GetVarint(b)
	return v
}

// parseSingleCreateTable parses sql, validates it contains exactly one
// CREATE TABLE statement, and returns that statement.
func parseSingleCreateTable(sql string) (*parser.CreateTableStmt, error) {
	stmts, err := parser.NewParser(sql).Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected 1 statement, got %d", len(stmts))
	}

	createTable, ok := stmts[0].(*parser.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE TABLE, got %T", stmts[0])
	}

	return createTable, nil
}

// buildTableFromStmt assembles a *Table from a parsed CREATE TABLE statement
// and the originating master row (used for the authoritative RootPage and SQL).
func buildTableFromStmt(stmt *parser.CreateTableStmt, row MasterRow) *Table {
	// Re-use the existing helpers from schema.go — convertColumns handles the
	// per-column constraint switch and processColumnConstraint handles each
	// individual constraint, so no duplicate decision logic lives here.
	columns, primaryKeyColumns := convertColumns(stmt.Columns)

	return &Table{
		Name:         stmt.Name,
		RootPage:     row.RootPage, // Use the one from sqlite_master
		SQL:          row.SQL,
		Columns:      columns,
		PrimaryKey:   uniqueStrings(primaryKeyColumns),
		WithoutRowID: stmt.WithoutRowID,
		Strict:       stmt.Strict,
		Temp:         stmt.Temp,
	}
}

// parseViewSQL parses a CREATE VIEW statement from a master row.
func (s *Schema) parseViewSQL(row MasterRow) (*View, error) {
	if row.SQL == "" {
		return &View{
			Name: row.Name,
			SQL:  row.SQL,
			Columns: []string{},
		}, nil
	}

	// Parse the SQL statement
	p := parser.NewParser(row.SQL)
	stmts, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Should have exactly one statement
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected 1 statement, got %d", len(stmts))
	}

	// Ensure it's a CREATE VIEW statement
	createView, ok := stmts[0].(*parser.CreateViewStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE VIEW, got %T", stmts[0])
	}

	// Create the view
	view := &View{
		Name:      createView.Name,
		Columns:   createView.Columns,
		Select:    createView.Select,
		SQL:       row.SQL,
		Temporary: createView.Temporary,
	}

	return view, nil
}

// parseIndexSQL parses a CREATE INDEX statement from a master row.
func (s *Schema) parseIndexSQL(row MasterRow) (*Index, error) {
	if row.SQL == "" {
		// Some auto-indexes don't have SQL
		return &Index{
			Name:     row.Name,
			Table:    row.TblName,
			RootPage: row.RootPage,
			SQL:      row.SQL,
			Columns:  []string{},
		}, nil
	}

	// Parse the SQL statement
	p := parser.NewParser(row.SQL)
	stmts, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Should have exactly one statement
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected 1 statement, got %d", len(stmts))
	}

	// Ensure it's a CREATE INDEX statement
	createIndex, ok := stmts[0].(*parser.CreateIndexStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE INDEX, got %T", stmts[0])
	}

	// Extract column names
	columns := make([]string, len(createIndex.Columns))
	for i, col := range createIndex.Columns {
		columns[i] = col.Column
	}

	// Create the index
	index := &Index{
		Name:     createIndex.Name,
		Table:    createIndex.Table,
		RootPage: row.RootPage, // Use the one from sqlite_master
		SQL:      row.SQL,
		Columns:  columns,
		Unique:   createIndex.Unique,
		Partial:  createIndex.Where != nil,
	}

	if createIndex.Where != nil {
		index.Where = createIndex.Where.String()
	}

	return index, nil
}

// InitializeMaster creates the sqlite_master table in a new database.
// This should be called when creating a new database file.
func (s *Schema) InitializeMaster() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create the sqlite_master table
	masterTable := &Table{
		Name:     "sqlite_master",
		RootPage: 1,
		SQL:      "CREATE TABLE sqlite_master(type text,name text,tbl_name text,rootpage integer,sql text)",
		Columns: []*Column{
			{
				Name:     "type",
				Type:     "text",
				Affinity: AffinityText,
			},
			{
				Name:     "name",
				Type:     "text",
				Affinity: AffinityText,
			},
			{
				Name:     "tbl_name",
				Type:     "text",
				Affinity: AffinityText,
			},
			{
				Name:     "rootpage",
				Type:     "integer",
				Affinity: AffinityInteger,
			},
			{
				Name:     "sql",
				Type:     "text",
				Affinity: AffinityText,
			},
		},
		PrimaryKey:   []string{},
		WithoutRowID: false,
		Strict:       false,
		Temp:         false,
	}

	s.Tables["sqlite_master"] = masterTable
	return nil
}
