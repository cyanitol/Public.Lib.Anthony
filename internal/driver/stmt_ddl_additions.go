// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileCreateIndex compiles a CREATE INDEX statement.
func (s *Stmt) compileCreateIndex(vm *vdbe.VDBE, stmt *parser.CreateIndexStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the index in the schema
	index, err := s.conn.schema.CreateIndex(stmt)
	if err != nil {
		return nil, err
	}

	// Allocate a root page for the index btree
	if s.conn.btree != nil {
		rootPage, err := s.conn.btree.CreateTable()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate index root page: %w", err)
		}
		index.RootPage = rootPage
	} else {
		// For in-memory databases without btree, use a placeholder
		// Use higher page numbers for indexes to avoid conflicts
		index.RootPage = uint32(1000 + s.conn.schema.IndexCount())
	}

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Populate the index with existing table data
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
}

// compileDropIndex compiles a DROP INDEX statement.
func (s *Stmt) compileDropIndex(vm *vdbe.VDBE, stmt *parser.DropIndexStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if index exists
	_, exists := s.conn.schema.GetIndex(stmt.Name)
	if !exists {
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("index not found: %s", stmt.Name)
	}

	// Drop the index from the schema
	if err := s.conn.schema.DropIndex(stmt.Name); err != nil {
		return nil, err
	}

	// In a full implementation, this would:
	// 1. Delete entry from sqlite_master table
	// 2. Free all pages used by the index
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
}

// compileAlterTable compiles an ALTER TABLE statement.
func (s *Stmt) compileAlterTable(vm *vdbe.VDBE, stmt *parser.AlterTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Get the table
	table, exists := s.conn.schema.GetTable(stmt.Table)
	if !exists {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Handle different ALTER TABLE actions
	switch action := stmt.Action.(type) {
	case *parser.RenameTableAction:
		// Rename the table
		return s.compileAlterTableRename(vm, stmt.Table, action.NewName)

	case *parser.RenameColumnAction:
		// Rename a column
		return s.compileAlterTableRenameColumn(vm, table, action.OldName, action.NewName)

	case *parser.AddColumnAction:
		// Add a column
		return s.compileAlterTableAddColumn(vm, table, &action.Column)

	case *parser.DropColumnAction:
		// Drop a column
		return s.compileAlterTableDropColumn(vm, table, action.ColumnName)

	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action: %T", action)
	}
}

// compileAlterTableRename handles ALTER TABLE ... RENAME TO ...
func (s *Stmt) compileAlterTableRename(vm *vdbe.VDBE, oldName, newName string) (*vdbe.VDBE, error) {
	// Check if new name already exists
	// Rename the table in schema
	if err := s.conn.schema.RenameTable(oldName, newName); err != nil {
		return nil, err
	}

	// In a full implementation, this would also:
	// 1. Update sqlite_master table
	// 2. Update all indexes and triggers that reference this table
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileAlterTableRenameColumn handles ALTER TABLE ... RENAME COLUMN ... TO ...
func (s *Stmt) compileAlterTableRenameColumn(vm *vdbe.VDBE, table *schema.Table, oldName, newName string) (*vdbe.VDBE, error) {
	// Find the column
	col, exists := table.GetColumn(oldName)
	if !exists {
		return nil, fmt.Errorf("column %q not found in table %q", oldName, table.Name)
	}

	// Check if new name already exists
	if _, exists := table.GetColumn(newName); exists {
		return nil, fmt.Errorf("column %q already exists in table %q", newName, table.Name)
	}

	// Update column name
	col.Name = newName

	// In a full implementation, this would:
	// 1. Update sqlite_master table
	// 2. Update all indexes, triggers, and views that reference this column
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileAlterTableAddColumn handles ALTER TABLE ... ADD COLUMN ...
func (s *Stmt) compileAlterTableAddColumn(vm *vdbe.VDBE, table *schema.Table, colDef *parser.ColumnDef) (*vdbe.VDBE, error) {
	if err := s.validateColumnAddition(table, colDef); err != nil {
		return nil, err
	}

	newCol := s.createNewColumn(colDef)
	table.Columns = append(table.Columns, newCol)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// validateColumnAddition checks if a column can be added to the table.
func (s *Stmt) validateColumnAddition(table *schema.Table, colDef *parser.ColumnDef) error {
	if _, exists := table.GetColumn(colDef.Name); exists {
		return fmt.Errorf("column %q already exists in table %q", colDef.Name, table.Name)
	}
	return nil
}

// createNewColumn creates a new column from a column definition.
func (s *Stmt) createNewColumn(colDef *parser.ColumnDef) *schema.Column {
	newCol := &schema.Column{
		Name:     colDef.Name,
		Type:     colDef.Type,
		Affinity: schema.DetermineAffinity(colDef.Type),
	}

	s.applyColumnConstraints(newCol, colDef.Constraints)
	return newCol
}

// applyColumnConstraints applies constraints to a column.
func (s *Stmt) applyColumnConstraints(col *schema.Column, constraints []parser.ColumnConstraint) {
	for _, constraint := range constraints {
		s.applyColumnConstraint(col, constraint)
	}
}

// applyColumnConstraint applies a single constraint to a column.
func (s *Stmt) applyColumnConstraint(col *schema.Column, constraint parser.ColumnConstraint) {
	switch constraint.Type {
	case parser.ConstraintNotNull:
		col.NotNull = true
	case parser.ConstraintUnique:
		col.Unique = true
	case parser.ConstraintDefault:
		if constraint.Default != nil {
			col.Default = constraint.Default.String()
		}
	case parser.ConstraintCollate:
		col.Collation = constraint.Collate
	}
}

// compileAlterTableDropColumn handles ALTER TABLE ... DROP COLUMN ...
func (s *Stmt) compileAlterTableDropColumn(vm *vdbe.VDBE, table *schema.Table, columnName string) (*vdbe.VDBE, error) {
	// Find the column index
	colIdx := table.GetColumnIndex(columnName)
	if colIdx == -1 {
		return nil, fmt.Errorf("column %q not found in table %q", columnName, table.Name)
	}

	// Check if it's the last column (SQLite doesn't allow dropping the last column)
	if len(table.Columns) == 1 {
		return nil, fmt.Errorf("cannot drop the last column of table %q", table.Name)
	}

	// Remove the column
	table.Columns = append(table.Columns[:colIdx], table.Columns[colIdx+1:]...)

	// In a full implementation, this would:
	// 1. Update sqlite_master table
	// 2. Rebuild the table data without the dropped column
	// 3. Update all indexes and triggers that reference this column
	// 4. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compilePragma compiles a PRAGMA statement.
func (s *Stmt) compilePragma(vm *vdbe.VDBE, stmt *parser.PragmaStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Most PRAGMAs can be read-only queries
	vm.SetReadOnly(true)
	vm.AllocMemory(10)

	pragmaName := strings.ToLower(stmt.Name)

	switch pragmaName {
	case "table_info":
		return s.compilePragmaTableInfo(vm, stmt)
	case "foreign_keys":
		return s.compilePragmaForeignKeys(vm, stmt)
	case "journal_mode":
		return s.compilePragmaJournalMode(vm, stmt)
	case "page_count":
		return s.compilePragmaPageCount(vm)
	default:
		// For unsupported PRAGMAs, return empty result
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		return vm, nil
	}
}

// compilePragmaTableInfo compiles PRAGMA table_info(tablename)
func (s *Stmt) compilePragmaTableInfo(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	tableName, err := extractTableNameFromPragma(stmt)
	if err != nil {
		return nil, err
	}

	table, exists := s.conn.schema.GetTable(tableName)
	if !exists {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	// PRAGMA table_info returns:
	// cid, name, type, notnull, dflt_value, pk
	vm.ResultCols = []string{"cid", "name", "type", "notnull", "dflt_value", "pk"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Use registers 1-6 for the result row (reused for each column)
	const baseReg = 1

	// Generate rows for each column
	for i, col := range table.Columns {
		emitTableInfoRow(vm, baseReg, i, col, table)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// extractTableNameFromPragma extracts the table name from a PRAGMA statement value.
func extractTableNameFromPragma(stmt *parser.PragmaStmt) (string, error) {
	if stmt.Value == nil {
		return "", fmt.Errorf("PRAGMA table_info requires a table name")
	}

	if lit, ok := stmt.Value.(*parser.LiteralExpr); ok {
		return lit.Value, nil
	}

	if ident, ok := stmt.Value.(*parser.IdentExpr); ok {
		return ident.Name, nil
	}

	return "", fmt.Errorf("invalid table name in PRAGMA table_info")
}

// emitTableInfoRow generates VDBE opcodes for a single table_info row.
func emitTableInfoRow(vm *vdbe.VDBE, baseReg, index int, col *schema.Column, table *schema.Table) {
	// cid (column index)
	vm.AddOpWithP4Int(vdbe.OpInteger, index, baseReg, 0, int32(index))

	// name
	vm.AddOpWithP4Str(vdbe.OpString, 0, baseReg+1, 0, col.Name)

	// type
	vm.AddOpWithP4Str(vdbe.OpString, 0, baseReg+2, 0, col.Type)

	// notnull (0 or 1)
	emitNotNullValue(vm, baseReg+3, col.NotNull)

	// dflt_value (default value or NULL)
	emitDefaultValue(vm, baseReg+4, col.Default)

	// pk (primary key index, 0 if not primary key)
	pk := calculatePrimaryKeyIndex(col, table)
	vm.AddOpWithP4Int(vdbe.OpInteger, pk, baseReg+5, 0, int32(pk))

	// Create result row
	vm.AddOp(vdbe.OpResultRow, baseReg, 6, 0)
}

// emitNotNullValue generates the VDBE opcode for the notnull column value.
func emitNotNullValue(vm *vdbe.VDBE, reg int, notNull bool) {
	value := 0
	if notNull {
		value = 1
	}
	vm.AddOpWithP4Int(vdbe.OpInteger, value, reg, 0, int32(value))
}

// emitDefaultValue generates the VDBE opcode for the default value column.
func emitDefaultValue(vm *vdbe.VDBE, reg int, defaultVal interface{}) {
	if defaultVal == nil {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}

	if defStr, ok := defaultVal.(string); ok {
		vm.AddOpWithP4Str(vdbe.OpString, 0, reg, 0, defStr)
	} else {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// calculatePrimaryKeyIndex returns the primary key index for a column.
// Returns 0 if the column is not a primary key, or the position (1-based) if it is.
func calculatePrimaryKeyIndex(col *schema.Column, table *schema.Table) int {
	if !col.PrimaryKey {
		return 0
	}

	// Find position in composite primary key
	for j, pkCol := range table.PrimaryKey {
		if pkCol == col.Name {
			return j + 1
		}
	}

	// Single column primary key
	return 1
}

// compilePragmaForeignKeys compiles PRAGMA foreign_keys or PRAGMA foreign_keys = value
func (s *Stmt) compilePragmaForeignKeys(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	if stmt.Value != nil {
		return s.compilePragmaForeignKeysSet(vm, stmt)
	}
	return s.compilePragmaForeignKeysGet(vm)
}

// compilePragmaForeignKeysSet handles SET operation: PRAGMA foreign_keys = value
func (s *Stmt) compilePragmaForeignKeysSet(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	enabled, err := parseForeignKeysValue(stmt.Value)
	if err != nil {
		return nil, err
	}

	s.conn.foreignKeysEnabled = enabled
	if s.conn.fkManager != nil {
		s.conn.fkManager.SetEnabled(enabled)
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compilePragmaForeignKeysGet handles GET operation: PRAGMA foreign_keys
func (s *Stmt) compilePragmaForeignKeysGet(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"foreign_keys"}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	value := 0
	if s.conn.foreignKeysEnabled {
		value = 1
	}
	vm.AddOpWithP4Int(vdbe.OpInteger, value, 1, 0, int32(value))
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// parseForeignKeysValue parses a PRAGMA foreign_keys value into a boolean
func parseForeignKeysValue(value parser.Expression) (bool, error) {
	// Map of valid values to their boolean equivalents
	validValues := map[string]bool{
		"ON":    true,
		"TRUE":  true,
		"1":     true,
		"OFF":   false,
		"FALSE": false,
		"0":     false,
	}

	valueStr := extractPragmaValueString(value)
	if valueStr == "" {
		return false, fmt.Errorf("invalid value for PRAGMA foreign_keys")
	}

	enabled, ok := validValues[strings.ToUpper(valueStr)]
	if !ok {
		return false, fmt.Errorf("invalid value for PRAGMA foreign_keys: %s", valueStr)
	}

	return enabled, nil
}

// extractPragmaValueString extracts a string value from a pragma expression
func extractPragmaValueString(value parser.Expression) string {
	if lit, ok := value.(*parser.LiteralExpr); ok {
		return lit.Value
	}
	if ident, ok := value.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return ""
}

// compilePragmaJournalMode compiles PRAGMA journal_mode or PRAGMA journal_mode = value
func (s *Stmt) compilePragmaJournalMode(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	if stmt.Value != nil {
		return s.compilePragmaJournalModeSet(vm, stmt)
	}
	return s.compilePragmaJournalModeGet(vm)
}

// compilePragmaJournalModeSet handles SET operation: PRAGMA journal_mode = value
func (s *Stmt) compilePragmaJournalModeSet(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	mode := extractJournalModeValue(stmt.Value)
	pagerMode, err := mapJournalModeToPager(mode)
	if err != nil {
		return nil, err
	}

	if err := s.setJournalModeInPager(pagerMode); err != nil {
		return nil, err
	}

	s.conn.journalMode = mode
	emitJournalModeResult(vm, strings.ToLower(mode))
	return vm, nil
}

// compilePragmaJournalModeGet handles GET operation: PRAGMA journal_mode
func (s *Stmt) compilePragmaJournalModeGet(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"journal_mode"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	mode := s.getCurrentJournalMode()
	vm.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, mode)
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compilePragmaPageCount handles PRAGMA page_count
func (s *Stmt) compilePragmaPageCount(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"page_count"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Get page count from pager
	pageCount := int64(s.conn.pager.PageCount())
	vm.AddOpWithP4Int64(vdbe.OpInt64, 0, 1, 0, pageCount)
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// extractJournalModeValue extracts the journal mode string from a pragma value expression.
func extractJournalModeValue(value parser.Expression) string {
	if lit, ok := value.(*parser.LiteralExpr); ok {
		return strings.ToUpper(lit.Value)
	}
	if ident, ok := value.(*parser.IdentExpr); ok {
		return strings.ToUpper(ident.Name)
	}
	return ""
}

// mapJournalModeToPager maps a journal mode string to a pager constant.
func mapJournalModeToPager(mode string) (int, error) {
	modeMap := map[string]int{
		"DELETE":   0, // JournalModeDelete
		"TRUNCATE": 3, // JournalModeTruncate
		"PERSIST":  1, // JournalModePersist
		"MEMORY":   4, // JournalModeMemory
		"WAL":      5, // JournalModeWAL
		"OFF":      2, // JournalModeOff
	}

	pagerMode, valid := modeMap[mode]
	if !valid {
		return 0, fmt.Errorf("invalid journal mode: %s", mode)
	}
	return pagerMode, nil
}

// setJournalModeInPager sets the journal mode in the pager if it's a concrete pager.
func (s *Stmt) setJournalModeInPager(pagerMode int) error {
	concretePager, ok := s.conn.pager.(*pager.Pager)
	if !ok {
		return nil
	}
	if err := concretePager.SetJournalMode(pagerMode); err != nil {
		return fmt.Errorf("failed to set journal mode: %w", err)
	}
	return nil
}

// getCurrentJournalMode retrieves the current journal mode.
func (s *Stmt) getCurrentJournalMode() string {
	mode := s.conn.journalMode
	if concretePager, ok := s.conn.pager.(*pager.Pager); ok {
		pagerModeInt := concretePager.GetJournalMode()
		modeNames := []string{"delete", "persist", "off", "truncate", "memory", "wal"}
		if pagerModeInt >= 0 && pagerModeInt < len(modeNames) {
			mode = modeNames[pagerModeInt]
		}
	}

	if mode == "" {
		return "delete"
	}
	return strings.ToLower(mode)
}

// emitJournalModeResult emits VDBE opcodes for returning a journal mode result.
func emitJournalModeResult(vm *vdbe.VDBE, mode string) {
	vm.ResultCols = []string{"journal_mode"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, mode)
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
}
