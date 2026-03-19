// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
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
	// Preserve original SQL for persistence/loading.
	index.SQL = s.query

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

	// Persist schema to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

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

	// Persist schema to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

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
	if err := s.conn.schema.RenameTable(oldName, newName); err != nil {
		return nil, err
	}

	// Regenerate stored SQL for the table and its indexes
	s.conn.schema.UpdateRenameTableSQL(newName)

	// Update trigger table references
	s.updateTriggerTableRefs(oldName, newName)

	// Persist to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()

	return vm, nil
}

// updateTriggerTableRefs updates trigger table references after a table rename.
func (s *Stmt) updateTriggerTableRefs(oldName, newName string) {
	lowerOld := strings.ToLower(oldName)
	for _, trigger := range s.conn.schema.Triggers {
		if strings.ToLower(trigger.Table) == lowerOld {
			trigger.Table = newName
		}
	}
}

// compileAlterTableRenameColumn handles ALTER TABLE ... RENAME COLUMN ... TO ...
func (s *Stmt) compileAlterTableRenameColumn(vm *vdbe.VDBE, table *schema.Table, oldName, newName string) (*vdbe.VDBE, error) {
	if err := s.conn.schema.RenameColumn(table.Name, oldName, newName); err != nil {
		return nil, err
	}

	// Persist to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()

	return vm, nil
}

// compileAlterTableAddColumn handles ALTER TABLE ... ADD COLUMN ...
func (s *Stmt) compileAlterTableAddColumn(vm *vdbe.VDBE, table *schema.Table, colDef *parser.ColumnDef) (*vdbe.VDBE, error) {
	if err := s.validateColumnAddition(table, colDef); err != nil {
		return nil, err
	}

	newCol := s.createNewColumn(colDef)
	table.Columns = append(table.Columns, newCol)

	// Regenerate stored SQL and persist
	s.conn.schema.UpdateTableSQL(table.Name)
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()

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
	if err := s.conn.schema.DropColumn(table.Name, columnName); err != nil {
		return nil, err
	}

	// Persist to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			_ = err
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()

	return vm, nil
}

// pragmaCompiler is a function that compiles a specific PRAGMA statement.
type pragmaCompiler func(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error)

// compilePragma compiles a PRAGMA statement.
func (s *Stmt) compilePragma(vm *vdbe.VDBE, stmt *parser.PragmaStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)
	vm.AllocMemory(10)

	dispatch := map[string]pragmaCompiler{
		"table_info":        s.compilePragmaTableInfo,
		"foreign_keys":      s.compilePragmaForeignKeys,
		"foreign_key_check": s.compilePragmaForeignKeyCheck,
		"foreign_key_list":  s.compilePragmaForeignKeyList,
		"journal_mode":      s.compilePragmaJournalMode,
		"index_list":        s.compilePragmaIndexList,
		"cache_size":        s.compilePragmaCacheSize,
	}

	pragmaName := strings.ToLower(stmt.Name)

	if compiler, ok := dispatch[pragmaName]; ok {
		return compiler(vm, stmt)
	}

	// Handle pragmas with different signatures
	switch pragmaName {
	case "page_count":
		return s.compilePragmaPageCount(vm)
	case "database_list":
		return s.compilePragmaDatabaseList(vm)
	default:
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
// For table-level PRIMARY KEY(a, b), columns may not have col.PrimaryKey set,
// so we also check membership in table.PrimaryKey.
func calculatePrimaryKeyIndex(col *schema.Column, table *schema.Table) int {
	// Check table.PrimaryKey list (covers both inline and table-level PKs)
	for j, pkCol := range table.PrimaryKey {
		if strings.EqualFold(pkCol, col.Name) {
			return j + 1
		}
	}

	// Fallback for inline PRIMARY KEY when table.PrimaryKey is empty
	if col.PrimaryKey {
		return 1
	}

	return 0
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

// extractPragmaValueString extracts a string value from a pragma expression.
// Handles literals, identifiers, and unary negation (e.g., -4000).
func extractPragmaValueString(value parser.Expression) string {
	if lit, ok := value.(*parser.LiteralExpr); ok {
		return lit.Value
	}
	if ident, ok := value.(*parser.IdentExpr); ok {
		return ident.Name
	}
	if unary, ok := value.(*parser.UnaryExpr); ok {
		if unary.Op == parser.OpNeg {
			inner := extractPragmaValueString(unary.Expr)
			if inner != "" {
				return "-" + inner
			}
		}
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

// compilePragmaDatabaseList handles PRAGMA database_list.
// Returns rows with columns: seq, name, file.
func (s *Stmt) compilePragmaDatabaseList(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"seq", "name", "file"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	databases := s.conn.dbRegistry.ListDatabasesOrdered()
	for i, db := range databases {
		vm.AddOpWithP4Int(vdbe.OpInteger, i, 1, 0, int32(i))
		vm.AddOpWithP4Str(vdbe.OpString, 0, 2, 0, db.Name)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 3, 0, db.Path)
		vm.AddOp(vdbe.OpResultRow, 1, 3, 0)
	}

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

// compilePragmaForeignKeyCheck compiles PRAGMA foreign_key_check or PRAGMA foreign_key_check(table)
func (s *Stmt) compilePragmaForeignKeyCheck(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	tableName := extractOptionalTableName(stmt)

	if s.conn.fkManager == nil {
		return s.emptyForeignKeyCheckResult(vm)
	}

	// First check for schema mismatches (returns error if FK definition is invalid)
	if err := s.conn.fkManager.CheckSchemaMismatch(tableName, s.conn.schema); err != nil {
		return nil, err
	}

	// Create a RowReader to scan tables
	rowReader := newDriverRowReader(s.conn)
	violations, err := s.conn.fkManager.FindViolations(tableName, s.conn.schema, rowReader)
	if err != nil {
		return nil, fmt.Errorf("foreign_key_check failed: %w", err)
	}

	return s.emitForeignKeyCheckResults(vm, violations)
}

// extractOptionalTableName extracts the optional table name from PRAGMA foreign_key_check.
func extractOptionalTableName(stmt *parser.PragmaStmt) string {
	if stmt.Value == nil {
		return ""
	}

	if lit, ok := stmt.Value.(*parser.LiteralExpr); ok {
		return lit.Value
	}

	if ident, ok := stmt.Value.(*parser.IdentExpr); ok {
		return ident.Name
	}

	return ""
}

// emptyForeignKeyCheckResult returns an empty result set.
func (s *Stmt) emptyForeignKeyCheckResult(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"table", "rowid", "parent", "fkid"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitForeignKeyCheckResults emits violations as VDBE result rows.
func (s *Stmt) emitForeignKeyCheckResults(vm *vdbe.VDBE, violations []constraint.ForeignKeyViolation) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"table", "rowid", "parent", "fkid"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, v := range violations {
		s.emitViolationRow(vm, v)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitViolationRow emits a single violation row.
func (s *Stmt) emitViolationRow(vm *vdbe.VDBE, v constraint.ForeignKeyViolation) {
	vm.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, v.Table)
	vm.AddOpWithP4Int64(vdbe.OpInt64, 0, 2, 0, v.Rowid)
	vm.AddOpWithP4Str(vdbe.OpString, 0, 3, 0, v.Parent)
	vm.AddOpWithP4Int(vdbe.OpInteger, v.FKid, 4, 0, int32(v.FKid))
	vm.AddOp(vdbe.OpResultRow, 1, 4, 0)
}

// compilePragmaForeignKeyList compiles PRAGMA foreign_key_list(tablename)
func (s *Stmt) compilePragmaForeignKeyList(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	tableName := extractOptionalTableName(stmt)
	if tableName == "" {
		return nil, fmt.Errorf("PRAGMA foreign_key_list requires a table name")
	}

	vm.ResultCols = []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	if s.conn.fkManager == nil {
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		return vm, nil
	}

	constraints := s.conn.fkManager.GetConstraints(tableName)
	for id, fk := range constraints {
		s.emitForeignKeyListRows(vm, id, fk)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitForeignKeyListRows emits rows for a single FK constraint (one per column).
func (s *Stmt) emitForeignKeyListRows(vm *vdbe.VDBE, id int, fk *constraint.ForeignKeyConstraint) {
	onUpdate := fkActionToString(fk.OnUpdate)
	onDelete := fkActionToString(fk.OnDelete)

	for seq, col := range fk.Columns {
		toCol := ""
		if seq < len(fk.RefColumns) {
			toCol = fk.RefColumns[seq]
		}

		vm.AddOpWithP4Int(vdbe.OpInteger, id, 1, 0, int32(id))
		vm.AddOpWithP4Int(vdbe.OpInteger, seq, 2, 0, int32(seq))
		vm.AddOpWithP4Str(vdbe.OpString, 0, 3, 0, fk.RefTable)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 4, 0, col)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 5, 0, toCol)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 6, 0, onUpdate)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 7, 0, onDelete)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 8, 0, "NONE")
		vm.AddOp(vdbe.OpResultRow, 1, 8, 0)
	}
}

// fkActionToString converts a foreign key action to its string representation.
func fkActionToString(action constraint.ForeignKeyAction) string {
	switch action {
	case constraint.FKActionCascade:
		return "CASCADE"
	case constraint.FKActionSetNull:
		return "SET NULL"
	case constraint.FKActionSetDefault:
		return "SET DEFAULT"
	case constraint.FKActionRestrict:
		return "RESTRICT"
	case constraint.FKActionNoAction:
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

// driverRowReader implements constraint.RowReader for PRAGMA foreign_key_check.
// It uses the connection's btree and schema directly.
type driverRowReader struct {
	conn *Conn
}

// newDriverRowReader creates a new driverRowReader.
func newDriverRowReader(conn *Conn) *driverRowReader {
	return &driverRowReader{conn: conn}
}

// RowExists checks if a row exists with the given column values.
func (r *driverRowReader) RowExists(tableName string, columns []string, values []interface{}) (bool, error) {
	cursor, err := r.initializeCursor(tableName)
	if err != nil {
		return false, err
	}

	table, _ := r.conn.schema.GetTable(tableName)
	return r.scanForMatch(cursor, table, columns, values)
}

// RowExistsWithCollation checks if a row exists using specified collations per column.
func (r *driverRowReader) RowExistsWithCollation(tableName string, columns []string, values []interface{}, collations []string) (bool, error) {
	cursor, err := r.initializeCursor(tableName)
	if err != nil {
		return false, err
	}

	table, _ := r.conn.schema.GetTable(tableName)
	return r.scanForMatchWithCollation(cursor, table, columns, values, collations)
}

// initializeCursor initializes and positions a cursor for table scanning.
func (r *driverRowReader) initializeCursor(tableName string) (*btree.BtCursor, error) {
	table, ok := r.conn.schema.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	if r.conn.btree == nil {
		return nil, fmt.Errorf("btree not available")
	}

	cursor := btree.NewCursor(r.conn.btree, table.RootPage)
	if err := cursor.MoveToFirst(); err != nil {
		// Empty table
		if strings.Contains(err.Error(), "empty") {
			return nil, nil
		}
		return nil, err
	}

	return cursor, nil
}

// scanForMatch scans all rows looking for a match.
func (r *driverRowReader) scanForMatch(cursor *btree.BtCursor, table *schema.Table, columns []string, values []interface{}) (bool, error) {
	if cursor == nil {
		return false, nil
	}

	for {
		if match, err := r.checkRowMatch(cursor, table, columns, values); err != nil {
			return false, err
		} else if match {
			return true, nil
		}

		if err := cursor.Next(); err != nil {
			break
		}
	}

	return false, nil
}

// scanForMatchWithCollation scans all rows looking for a match using specified collations.
func (r *driverRowReader) scanForMatchWithCollation(cursor *btree.BtCursor, table *schema.Table, columns []string, values []interface{}, collations []string) (bool, error) {
	if cursor == nil {
		return false, nil
	}

	for {
		if match, err := r.checkRowMatchWithCollation(cursor, table, columns, values, collations); err != nil {
			return false, err
		} else if match {
			return true, nil
		}

		if err := cursor.Next(); err != nil {
			break
		}
	}

	return false, nil
}

// FindReferencingRows finds all rowids of rows with matching column values.
func (r *driverRowReader) FindReferencingRows(tableName string, columns []string, values []interface{}) ([]int64, error) {
	cursor, err := r.initializeCursor(tableName)
	if err != nil {
		return nil, err
	}

	if cursor == nil {
		return []int64{}, nil
	}

	table, _ := r.conn.schema.GetTable(tableName)
	return r.collectMatchingRowids(cursor, table, columns, values)
}

// FindReferencingRowsWithParentAffinity finds all rowids with affinity-aware matching.
// This is used for FK checks where we need to apply parent column affinity to child values.
func (r *driverRowReader) FindReferencingRowsWithParentAffinity(
	childTableName string,
	childColumns []string,
	parentValues []interface{},
	parentTableName string,
	parentColumns []string,
) ([]int64, error) {
	cursor, err := r.initializeCursor(childTableName)
	if err != nil {
		return nil, err
	}

	if cursor == nil {
		return []int64{}, nil
	}

	childTable, _ := r.conn.schema.GetTable(childTableName)
	parentTable, _ := r.conn.schema.GetTable(parentTableName)

	return r.collectMatchingRowidsWithAffinity(cursor, childTable, childColumns, parentValues, parentTable, parentColumns)
}

// collectMatchingRowids scans all rows and collects rowids that match.
func (r *driverRowReader) collectMatchingRowids(cursor *btree.BtCursor, table *schema.Table, columns []string, values []interface{}) ([]int64, error) {
	var rowids []int64
	for {
		if match, err := r.checkRowMatch(cursor, table, columns, values); err != nil {
			return nil, err
		} else if match {
			rowids = append(rowids, cursor.GetKey())
		}

		if err := cursor.Next(); err != nil {
			break
		}
	}

	return rowids, nil
}

// collectMatchingRowidsWithAffinity scans rows and collects matches using parent column affinity.
func (r *driverRowReader) collectMatchingRowidsWithAffinity(
	cursor *btree.BtCursor,
	childTable *schema.Table,
	childColumns []string,
	parentValues []interface{},
	parentTable *schema.Table,
	parentColumns []string,
) ([]int64, error) {
	var rowids []int64
	for {
		match, err := r.checkRowMatchWithParentAffinity(cursor, childTable, childColumns, parentValues, parentTable, parentColumns)
		if err != nil {
			return nil, err
		}
		if match {
			rowids = append(rowids, cursor.GetKey())
		}

		if err := cursor.Next(); err != nil {
			break
		}
	}

	return rowids, nil
}

// ReadRowByRowid reads a row's values by its rowid.
func (r *driverRowReader) ReadRowByRowid(tableName string, rowid int64) (map[string]interface{}, error) {
	table, ok := r.conn.schema.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	if r.conn.btree == nil {
		return nil, fmt.Errorf("btree not available")
	}

	cursor := btree.NewCursor(r.conn.btree, table.RootPage)
	found, err := cursor.SeekRowid(rowid)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("row not found: rowid %d", rowid)
	}

	return r.readRowValues(cursor, table)
}

// checkRowMatch checks if the current row matches the given column values.
func (r *driverRowReader) checkRowMatch(cursor *btree.BtCursor, table *schema.Table, columns []string, values []interface{}) (bool, error) {
	// If no columns specified, return all rows (used for full table scan)
	if len(columns) == 0 {
		return true, nil
	}

	payload, err := cursor.GetPayloadWithOverflow()
	if err != nil {
		return false, err
	}

	rowid := cursor.GetKey()

	for i, colName := range columns {
		colVal, err := r.getColumnValue(table, colName, payload, rowid)
		if err != nil {
			return false, err
		}

		// Get parent column for affinity-aware comparison
		col, _ := table.GetColumn(colName)
		if !r.valuesEqualWithAffinity(colVal, values[i], col) {
			return false, nil
		}
	}

	return true, nil
}

// checkRowMatchWithCollation checks if the current row matches using specified collations.
func (r *driverRowReader) checkRowMatchWithCollation(cursor *btree.BtCursor, table *schema.Table, columns []string, values []interface{}, collations []string) (bool, error) {
	// If no columns specified, return all rows (used for full table scan)
	if len(columns) == 0 {
		return true, nil
	}

	payload, err := cursor.GetPayloadWithOverflow()
	if err != nil {
		return false, err
	}

	rowid := cursor.GetKey()

	for i, colName := range columns {
		colVal, err := r.getColumnValue(table, colName, payload, rowid)
		if err != nil {
			return false, err
		}

		collation := "BINARY"
		if i < len(collations) && collations[i] != "" {
			collation = collations[i]
		}

		if !r.valuesEqualWithCollation(colVal, values[i], collation) {
			return false, nil
		}
	}

	return true, nil
}

// checkRowMatchWithParentAffinity checks if child row matches parent values using parent affinity.
func (r *driverRowReader) checkRowMatchWithParentAffinity(
	cursor *btree.BtCursor,
	childTable *schema.Table,
	childColumns []string,
	parentValues []interface{},
	parentTable *schema.Table,
	parentColumns []string,
) (bool, error) {
	if len(childColumns) == 0 {
		return true, nil
	}

	payload, err := cursor.GetPayloadWithOverflow()
	if err != nil {
		return false, err
	}

	rowid := cursor.GetKey()

	for i, childColName := range childColumns {
		childVal, err := r.getColumnValue(childTable, childColName, payload, rowid)
		if err != nil {
			return false, err
		}

		// Get parent column for affinity and collation
		var parentCol *schema.Column
		if i < len(parentColumns) && parentTable != nil {
			parentCol, _ = parentTable.GetColumn(parentColumns[i])
		}

		if !r.valuesEqualWithAffinity(parentValues[i], childVal, parentCol) {
			return false, nil
		}
	}

	return true, nil
}

// getColumnValue extracts a column value from the row.
func (r *driverRowReader) getColumnValue(table *schema.Table, colName string, payload []byte, rowid int64) (interface{}, error) {
	// Find the column index
	payloadIdx := 0
	for _, col := range table.Columns {
		if strings.EqualFold(col.Name, colName) {
			// Check if it's an INTEGER PRIMARY KEY (rowid alias)
			if col.PrimaryKey && strings.EqualFold(col.Type, "INTEGER") {
				return rowid, nil
			}
			// Extract from payload
			mem := vdbe.NewMem()
			if err := vdbe.ParseRecordColumn(payload, payloadIdx, mem); err != nil {
				return nil, err
			}
			return vdbe.MemToInterface(mem), nil
		}
		// Count payload columns (skip INTEGER PRIMARY KEY)
		if !(col.PrimaryKey && strings.EqualFold(col.Type, "INTEGER")) {
			payloadIdx++
		}
	}
	return nil, fmt.Errorf("column not found: %s", colName)
}

// readRowValues reads all column values from the current row.
func (r *driverRowReader) readRowValues(cursor *btree.BtCursor, table *schema.Table) (map[string]interface{}, error) {
	payload, err := cursor.GetPayloadWithOverflow()
	if err != nil {
		return nil, err
	}

	rowid := cursor.GetKey()
	result := make(map[string]interface{})
	payloadIdx := 0

	for _, col := range table.Columns {
		if col.PrimaryKey && strings.EqualFold(col.Type, "INTEGER") {
			result[col.Name] = rowid
		} else {
			mem := vdbe.NewMem()
			if err := vdbe.ParseRecordColumn(payload, payloadIdx, mem); err != nil {
				return nil, err
			}
			result[col.Name] = vdbe.MemToInterface(mem)
			payloadIdx++
		}
	}

	return result, nil
}

// valuesEqual compares two values for FK matching.
func (r *driverRowReader) valuesEqual(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// Handle numeric comparisons
	if n1, ok := toInt64Value(v1); ok {
		if n2, ok := toInt64Value(v2); ok {
			return n1 == n2
		}
	}

	// String comparison
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	return s1 == s2
}

// valuesEqualWithCollation compares two values using the specified collation.
func (r *driverRowReader) valuesEqualWithCollation(v1, v2 interface{}, collationName string) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// Handle numeric comparisons (collation doesn't apply to numbers)
	if n1, ok := toInt64Value(v1); ok {
		if n2, ok := toInt64Value(v2); ok {
			return n1 == n2
		}
	}

	// String comparison with collation
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)

	// Use collation-aware comparison from the collation package
	return collation.Compare(s1, s2, collationName) == 0
}

// valuesEqualWithAffinity compares two values using the parent column's affinity and collation.
// According to SQLite FK rules, the parent column's affinity is applied to the
// child value before comparison, and the parent column's collation is used for string comparison.
func (r *driverRowReader) valuesEqualWithAffinity(parentVal, childVal interface{}, parentCol *schema.Column) bool {
	if parentVal == nil && childVal == nil {
		return true
	}
	if parentVal == nil || childVal == nil {
		return false
	}

	// Apply parent column's affinity to child value
	if parentCol != nil {
		childVal = expr.ApplyAffinity(childVal, parentCol.Affinity)
	}

	// Get parent column's collation (defaults to BINARY if not specified)
	collationName := "BINARY"
	if parentCol != nil && parentCol.Collation != "" {
		collationName = strings.ToUpper(parentCol.Collation)
	}

	// Compare after applying affinity and collation
	return r.compareAfterAffinityWithCollation(parentVal, childVal, collationName)
}

// compareAfterAffinityWithCollation compares values after affinity has been applied,
// using the specified collation for string comparisons.
func (r *driverRowReader) compareAfterAffinityWithCollation(v1, v2 interface{}, collationName string) bool {
	// Handle numeric comparisons (collation doesn't apply to numbers)
	if n1, ok := toInt64Value(v1); ok {
		if n2, ok := toInt64Value(v2); ok {
			return n1 == n2
		}
	}

	// Handle float comparisons
	if f1, ok := toFloat64Value(v1); ok {
		if f2, ok := toFloat64Value(v2); ok {
			return f1 == f2
		}
	}

	// String comparison with collation
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	return collation.Compare(s1, s2, collationName) == 0
}

// toFloat64Value converts a value to float64 if possible.
func toFloat64Value(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// toInt64Value converts a value to int64 if possible.
func toInt64Value(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}

// compilePragmaCacheSize compiles PRAGMA cache_size or PRAGMA cache_size = value.
func (s *Stmt) compilePragmaCacheSize(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	if stmt.Value != nil {
		return s.compilePragmaCacheSizeSet(vm, stmt)
	}
	return s.compilePragmaCacheSizeGet(vm)
}

// compilePragmaCacheSizeGet returns the current cache_size value.
func (s *Stmt) compilePragmaCacheSizeGet(vm *vdbe.VDBE) (*vdbe.VDBE, error) {
	vm.ResultCols = []string{"cache_size"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOpWithP4Int64(vdbe.OpInt64, 0, 1, 0, s.conn.cacheSize)
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compilePragmaCacheSizeSet sets the cache_size value.
func (s *Stmt) compilePragmaCacheSizeSet(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	valueStr := extractPragmaValueString(stmt.Value)
	var val int64
	if _, err := fmt.Sscanf(valueStr, "%d", &val); err != nil {
		return nil, fmt.Errorf("invalid value for PRAGMA cache_size: %s", valueStr)
	}
	s.conn.cacheSize = val
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compilePragmaIndexList compiles PRAGMA index_list(tablename).
func (s *Stmt) compilePragmaIndexList(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	tableName := extractOptionalTableName(stmt)
	if tableName == "" {
		return nil, fmt.Errorf("PRAGMA index_list requires a table name")
	}

	vm.ResultCols = []string{"seq", "name", "unique", "origin", "partial"}
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	indexes := s.conn.schema.GetTableIndexes(tableName)
	for i, idx := range indexes {
		isUnique := 0
		if idx.Unique {
			isUnique = 1
		}
		isPartial := 0
		if idx.Partial {
			isPartial = 1
		}
		vm.AddOpWithP4Int(vdbe.OpInteger, i, 1, 0, int32(i))
		vm.AddOpWithP4Str(vdbe.OpString, 0, 2, 0, idx.Name)
		vm.AddOpWithP4Int(vdbe.OpInteger, isUnique, 3, 0, int32(isUnique))
		vm.AddOpWithP4Str(vdbe.OpString, 0, 4, 0, "c")
		vm.AddOpWithP4Int(vdbe.OpInteger, isPartial, 5, 0, int32(isPartial))
		vm.AddOp(vdbe.OpResultRow, 1, 5, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// simpleRowReader is a minimal RowReader implementation for foreign_key_check.
