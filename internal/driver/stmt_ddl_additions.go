package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
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
		index.RootPage = uint32(1000 + len(s.conn.schema.Indexes))
	}

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Populate the index with existing table data
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

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
	// Check if column already exists
	if _, exists := table.GetColumn(colDef.Name); exists {
		return nil, fmt.Errorf("column %q already exists in table %q", colDef.Name, table.Name)
	}

	// Create new column
	newCol := &schema.Column{
		Name:     colDef.Name,
		Type:     colDef.Type,
		Affinity: schema.DetermineAffinity(colDef.Type),
	}

	// Apply constraints
	for _, constraint := range colDef.Constraints {
		switch constraint.Type {
		case parser.ConstraintNotNull:
			newCol.NotNull = true
		case parser.ConstraintUnique:
			newCol.Unique = true
		case parser.ConstraintDefault:
			if constraint.Default != nil {
				newCol.Default = constraint.Default.String()
			}
		case parser.ConstraintCollate:
			newCol.Collation = constraint.Collate
		}
	}

	// Add column to table
	table.Columns = append(table.Columns, newCol)

	// In a full implementation, this would:
	// 1. Update sqlite_master table
	// 2. Add default values to all existing rows
	// 3. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
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
		// SET operation: PRAGMA foreign_keys = value
		vm.SetReadOnly(false)

		// Parse the value (ON/OFF, 1/0, true/false)
		var enabled bool
		if lit, ok := stmt.Value.(*parser.LiteralExpr); ok {
			switch strings.ToUpper(lit.Value) {
			case "ON", "TRUE", "1":
				enabled = true
			case "OFF", "FALSE", "0":
				enabled = false
			default:
				return nil, fmt.Errorf("invalid value for PRAGMA foreign_keys: %s", lit.Value)
			}
		} else if ident, ok := stmt.Value.(*parser.IdentExpr); ok {
			switch strings.ToUpper(ident.Name) {
			case "ON", "TRUE":
				enabled = true
			case "OFF", "FALSE":
				enabled = false
			default:
				return nil, fmt.Errorf("invalid value for PRAGMA foreign_keys: %s", ident.Name)
			}
		}

		// Store the setting in the connection
		s.conn.foreignKeysEnabled = enabled

		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		return vm, nil
	}

	// GET operation: PRAGMA foreign_keys
	vm.ResultCols = []string{"foreign_keys"}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Return current value
	value := 0
	if s.conn.foreignKeysEnabled {
		value = 1
	}
	vm.AddOpWithP4Int(vdbe.OpInteger, value, 1, 0, int32(value))
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compilePragmaJournalMode compiles PRAGMA journal_mode or PRAGMA journal_mode = value
func (s *Stmt) compilePragmaJournalMode(vm *vdbe.VDBE, stmt *parser.PragmaStmt) (*vdbe.VDBE, error) {
	if stmt.Value != nil {
		// SET operation: PRAGMA journal_mode = value
		vm.SetReadOnly(false)

		var mode string
		if lit, ok := stmt.Value.(*parser.LiteralExpr); ok {
			mode = strings.ToUpper(lit.Value)
		} else if ident, ok := stmt.Value.(*parser.IdentExpr); ok {
			mode = strings.ToUpper(ident.Name)
		}

		// Validate mode (DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF)
		validModes := map[string]bool{
			"DELETE":   true,
			"TRUNCATE": true,
			"PERSIST":  true,
			"MEMORY":   true,
			"WAL":      true,
			"OFF":      true,
		}

		if !validModes[mode] {
			return nil, fmt.Errorf("invalid journal mode: %s", mode)
		}

		// Store the setting in the connection
		s.conn.journalMode = mode

		// Return the mode that was set
		vm.ResultCols = []string{"journal_mode"}
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, strings.ToLower(mode))
		vm.AddOp(vdbe.OpResultRow, 1, 1, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		return vm, nil
	}

	// GET operation: PRAGMA journal_mode
	vm.ResultCols = []string{"journal_mode"}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Return current value (default is DELETE if not set)
	mode := s.conn.journalMode
	if mode == "" {
		mode = "delete"
	} else {
		mode = strings.ToLower(mode)
	}
	vm.AddOpWithP4Str(vdbe.OpString, 0, 1, 0, mode)
	vm.AddOp(vdbe.OpResultRow, 1, 1, 0)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}
