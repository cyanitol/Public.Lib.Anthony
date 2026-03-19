// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// Table DDL Compilation
// ============================================================================

// compileCreateTable compiles a CREATE TABLE statement.
func (s *Stmt) compileCreateTable(vm *vdbe.VDBE, stmt *parser.CreateTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	targetSchema, targetBtree, err := s.resolveTargetDatabase(stmt.Schema)
	if err != nil {
		return nil, err
	}

	table, err := targetSchema.CreateTable(stmt)
	if err != nil {
		return nil, err
	}
	table.SQL = s.query

	if err := s.initializeNewTable(table, stmt, targetSchema, targetBtree); err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// initializeNewTable allocates storage, registers constraints, and persists the schema for a new table.
func (s *Stmt) initializeNewTable(table *schema.Table, stmt *parser.CreateTableStmt, targetSchema *schema.Schema, targetBtree *btree.Btree) error {
	if targetBtree != nil {
		rootPage, err := s.allocateTablePage(targetBtree, stmt.WithoutRowID)
		if err != nil {
			return err
		}
		table.RootPage = rootPage
	} else {
		table.RootPage = 2
	}

	if err := s.registerForeignKeyConstraints(table, stmt); err != nil {
		return err
	}

	if _, hasAutoincrement := table.HasAutoincrementColumn(); hasAutoincrement {
		if err := s.ensureSqliteSequenceTable(); err != nil {
			return err
		}
	}

	if targetBtree != nil {
		if err := targetSchema.SaveToMaster(targetBtree); err != nil {
			return fmt.Errorf("failed to persist schema: %w", err)
		}
	}

	return nil
}

// resolveTargetDatabase resolves the target schema and btree for a DDL statement.
// If schemaName is empty, uses the main database.
func (s *Stmt) resolveTargetDatabase(schemaName string) (*schema.Schema, *btree.Btree, error) {
	if schemaName == "" {
		return s.conn.schema, s.conn.btree, nil
	}
	db, ok := s.conn.dbRegistry.GetDatabase(schemaName)
	if !ok {
		return nil, nil, fmt.Errorf("unknown database %s", schemaName)
	}
	return db.Schema, db.Btree, nil
}

// allocateTablePage allocates a root page for a new table.
func (s *Stmt) allocateTablePage(bt *btree.Btree, withoutRowID bool) (uint32, error) {
	if withoutRowID {
		return bt.CreateWithoutRowidTable()
	}
	return bt.CreateTable()
}

// ensureSqliteSequenceTable creates the sqlite_sequence table if it does not exist.
// This table is required for AUTOINCREMENT support and is automatically created
// when the first table with an AUTOINCREMENT column is created.
func (s *Stmt) ensureSqliteSequenceTable() error {
	if _, exists := s.conn.schema.GetTable("sqlite_sequence"); exists {
		return nil
	}

	var rootPage uint32
	if s.conn.btree != nil {
		var err error
		rootPage, err = s.conn.btree.CreateTable()
		if err != nil {
			return fmt.Errorf("failed to create sqlite_sequence table: %w", err)
		}
	} else {
		rootPage = 3 // placeholder for in-memory databases
	}

	s.conn.schema.EnsureSqliteSequenceTable(rootPage)
	return nil
}

// registerForeignKeyConstraints registers foreign key constraints from a CREATE TABLE statement
// with the connection's ForeignKeyManager.
func (s *Stmt) registerForeignKeyConstraints(_ interface{}, stmt *parser.CreateTableStmt) error {
	if s.conn.fkManager == nil {
		return nil
	}
	s.registerTableLevelFKs(stmt)
	s.registerColumnLevelFKs(stmt)

	// Validate FK constraints at CREATE TABLE time
	// Only checks errors that should prevent table creation (column count mismatch, FK to view)
	// Does NOT check for non-unique columns (that's reported via PRAGMA foreign_key_check)
	if err := s.conn.fkManager.ValidateFKAtCreateTime(stmt.Name, s.conn.schema); err != nil {
		// Remove the constraints we just added since they're invalid
		s.conn.fkManager.RemoveConstraints(stmt.Name)
		return err
	}
	return nil
}

// registerTableLevelFKs registers table-level FOREIGN KEY constraints.
func (s *Stmt) registerTableLevelFKs(stmt *parser.CreateTableStmt) {
	for _, tableConstraint := range stmt.Constraints {
		if tableConstraint.ForeignKey == nil {
			continue
		}
		fkTableConstraint := tableConstraint.ForeignKey
		fk := constraint.CreateForeignKeyFromParser(
			stmt.Name,
			fkTableConstraint.Columns,
			&fkTableConstraint.ForeignKey,
			tableConstraint.Name,
		)
		s.conn.fkManager.AddConstraint(fk)
	}
}

// registerColumnLevelFKs registers column-level FOREIGN KEY constraints.
func (s *Stmt) registerColumnLevelFKs(stmt *parser.CreateTableStmt) {
	for _, col := range stmt.Columns {
		for _, colConstraint := range col.Constraints {
			if colConstraint.Type == parser.ConstraintForeignKey && colConstraint.ForeignKey != nil {
				fk := constraint.CreateForeignKeyFromParser(stmt.Name, []string{col.Name}, colConstraint.ForeignKey, "")
				s.conn.fkManager.AddConstraint(fk)
			}
		}
	}
}

// compileDropTable compiles a DROP TABLE statement.
func (s *Stmt) compileDropTable(vm *vdbe.VDBE, stmt *parser.DropTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if table exists
	table, exists := s.conn.schema.GetTable(stmt.Name)
	if !exists {
		return s.handleMissingTable(vm, stmt)
	}

	// Check foreign key constraints
	if err := s.checkDropTableForeignKeys(stmt.Name); err != nil {
		return nil, err
	}

	// Drop table and cleanup
	if err := s.performDropTable(stmt.Name, table); err != nil {
		return nil, err
	}

	// Emit bytecode
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()
	return vm, nil
}

// knownVirtualTableModules lists the supported virtual table modules.
var knownVirtualTableModules = map[string]bool{
	"fts5":      true,
	"fts4":      true,
	"fts3":      true,
	"rtree":     true,
	"rtree_i32": true,
}

// compileCreateVirtualTable compiles a CREATE VIRTUAL TABLE statement
func (s *Stmt) compileCreateVirtualTable(vm *vdbe.VDBE, stmt *parser.CreateVirtualTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check for known module
	moduleName := strings.ToLower(stmt.Module)
	if !knownVirtualTableModules[moduleName] {
		return nil, fmt.Errorf("no such module: %s", stmt.Module)
	}

	// Check if table already exists
	if _, exists := s.conn.schema.GetTable(stmt.Name); exists {
		if stmt.IfNotExists {
			// IF NOT EXISTS - silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("table %s already exists", stmt.Name)
	}

	// Get the module from registry and create the virtual table instance
	// Pass s.conn as the database executor so modules can create shadow tables.
	var vtabInstance interface{}
	if s.conn.vtabRegistry != nil {
		module := s.conn.vtabRegistry.GetModule(moduleName)
		if module != nil {
			vtab, _, err := module.Create(s.conn, moduleName, "main", stmt.Name, stmt.Args)
			if err != nil {
				return nil, fmt.Errorf("failed to create virtual table: %w", err)
			}
			vtabInstance = vtab
		}
	}

	// Register the virtual table in the schema with the created instance
	err := s.conn.schema.CreateVirtualTable(stmt.Name, stmt.Module, stmt.Args, vtabInstance, stmt.String())
	if err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	s.invalidateStmtCache()
	return vm, nil
}

// handleMissingTable handles the case when a table doesn't exist.
func (s *Stmt) handleMissingTable(vm *vdbe.VDBE, stmt *parser.DropTableStmt) (*vdbe.VDBE, error) {
	if stmt.IfExists {
		// IF EXISTS was specified, silently succeed
		vm.AddOp(vdbe.OpInit, 0, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		return vm, nil
	}
	return nil, fmt.Errorf("table not found: %s", stmt.Name)
}

// checkDropTableForeignKeys checks if foreign key constraints prevent dropping the table.
func (s *Stmt) checkDropTableForeignKeys(tableName string) error {
	if s.conn.fkManager == nil || !s.conn.fkManager.IsEnabled() {
		return nil
	}

	referencingConstraints := s.conn.fkManager.FindReferencingConstraints(tableName)
	if len(referencingConstraints) > 0 {
		return fmt.Errorf("FOREIGN KEY constraint failed: cannot drop table %s, referenced by foreign key constraint", tableName)
	}
	return nil
}

// performDropTable removes the table from schema and performs cleanup.
func (s *Stmt) performDropTable(tableName string, _ *schema.Table) error {
	// Drop the table from the schema
	if err := s.conn.schema.DropTable(tableName); err != nil {
		return err
	}

	// Remove FK constraints that belonged to this table
	if s.conn.fkManager != nil {
		s.conn.fkManager.RemoveConstraints(tableName)
	}

	// Free table pages if btree is available
	// In a full implementation, would call btree.FreePage(table.RootPage)
	// and recursively free all pages in the table's btree

	// Persist schema updates
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			return fmt.Errorf("failed to persist schema: %w", err)
		}
	}

	return nil
}

// ============================================================================
// View DDL Compilation
// ============================================================================

// compileCreateView compiles a CREATE VIEW statement.
func (s *Stmt) compileCreateView(vm *vdbe.VDBE, stmt *parser.CreateViewStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the view in the schema
	_, err := s.conn.schema.CreateView(stmt)
	if err != nil {
		return nil, err
	}

	// Persist schema to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			return nil, fmt.Errorf("failed to persist schema: %w", err)
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
}

// compileDropView compiles a DROP VIEW statement.
func (s *Stmt) compileDropView(vm *vdbe.VDBE, stmt *parser.DropViewStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if view exists
	_, exists := s.conn.schema.GetView(stmt.Name)
	if !exists {
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("no such view: %s", stmt.Name)
	}

	// Drop the view from the schema
	if err := s.conn.schema.DropView(stmt.Name); err != nil {
		return nil, err
	}

	// Persist schema to sqlite_master
	if s.conn.btree != nil {
		if err := s.conn.schema.SaveToMaster(s.conn.btree); err != nil {
			return nil, fmt.Errorf("failed to persist schema: %w", err)
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
}

// ============================================================================
// Trigger DDL Compilation
// ============================================================================

// compileCreateTrigger compiles a CREATE TRIGGER statement.
func (s *Stmt) compileCreateTrigger(vm *vdbe.VDBE, stmt *parser.CreateTriggerStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the trigger in the schema
	_, err := s.conn.schema.CreateTrigger(stmt)
	if err != nil {
		if stmt.IfNotExists && err.Error() == fmt.Sprintf("trigger already exists: %s", stmt.Name) {
			// IF NOT EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, err
	}

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
}

// compileDropTrigger compiles a DROP TRIGGER statement.
func (s *Stmt) compileDropTrigger(vm *vdbe.VDBE, stmt *parser.DropTriggerStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if trigger exists
	_, exists := s.conn.schema.GetTrigger(stmt.Name)
	if !exists {
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("trigger not found: %s", stmt.Name)
	}

	// Drop the trigger from the schema
	if err := s.conn.schema.DropTrigger(stmt.Name); err != nil {
		return nil, err
	}

	// In a full implementation, this would:
	// 1. Delete entry from sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// ============================================================================
// Transaction Control Compilation
// ============================================================================

// compileBegin compiles a BEGIN statement.
func (s *Stmt) compileBegin(vm *vdbe.VDBE, stmt *parser.BeginStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	if s.conn.inTx {
		return nil, fmt.Errorf("cannot start a transaction within a transaction")
	}

	vm.SetReadOnly(false)
	vm.InTxn = true

	// Set FK manager's transaction state for deferred constraint handling
	if s.conn.fkManager != nil {
		s.conn.fkManager.SetInTransaction(true)
	}

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileCommit compiles a COMMIT statement.
func (s *Stmt) compileCommit(vm *vdbe.VDBE, stmt *parser.CommitStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	if !s.conn.inTx {
		return nil, fmt.Errorf("cannot commit - no transaction is active")
	}

	vm.SetReadOnly(false)

	// Check deferred FK constraints before commit
	if err := s.conn.checkDeferredFKConstraints(); err != nil {
		return nil, err
	}

	// Reset FK manager's transaction state
	if s.conn.fkManager != nil {
		s.conn.fkManager.SetInTransaction(false)
	}
	s.conn.clearDeferredFKViolations()

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOp(vdbe.OpCommit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRollback compiles a ROLLBACK statement.
func (s *Stmt) compileRollback(vm *vdbe.VDBE, stmt *parser.RollbackStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// ROLLBACK TO savepoint does not require an explicit transaction
	if stmt.Savepoint != "" {
		return s.compileRollbackTo(vm, stmt.Savepoint)
	}

	if !s.conn.inTx {
		return nil, fmt.Errorf("cannot rollback - no transaction is active")
	}

	vm.SetReadOnly(false)

	// Reset FK manager's transaction state (no deferred check on rollback)
	if s.conn.fkManager != nil {
		s.conn.fkManager.SetInTransaction(false)
	}
	s.conn.clearDeferredFKViolations()

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOp(vdbe.OpRollback, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRollbackTo compiles a ROLLBACK TO savepoint statement.
func (s *Stmt) compileRollbackTo(vm *vdbe.VDBE, name string) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOpWithP4Str(vdbe.OpSavepoint, 2, 0, 0, name)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileSavepoint compiles a SAVEPOINT statement.
func (s *Stmt) compileSavepoint(vm *vdbe.VDBE, stmt *parser.SavepointStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Ensure a write transaction is active (needed for pager savepoint support)
	if !s.conn.pager.InWriteTransaction() {
		if err := s.conn.pager.BeginWrite(); err != nil {
			return nil, fmt.Errorf("failed to begin write transaction: %w", err)
		}
	}

	if !s.conn.inTx {
		s.conn.inTx = true
		s.conn.sqlTx = true
		s.conn.savepointOnly = true
		vm.InTxn = true
		if s.conn.fkManager != nil {
			s.conn.fkManager.SetInTransaction(true)
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOpWithP4Str(vdbe.OpSavepoint, 0, 0, 0, stmt.Name)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRelease compiles a RELEASE [SAVEPOINT] statement.
func (s *Stmt) compileRelease(vm *vdbe.VDBE, stmt *parser.ReleaseStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 1, 0)
	vm.AddOpWithP4Str(vdbe.OpSavepoint, 1, 0, 0, stmt.Name)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// ============================================================================
// REINDEX Statement
// ============================================================================

// compileReindex compiles a REINDEX statement.
// REINDEX is used to rebuild indexes. In this implementation:
// - For in-memory databases, REINDEX is a no-op (indexes don't get corrupted)
// - For disk databases, we validate the target exists but don't rebuild
// - The statement always succeeds unless the target doesn't exist
func (s *Stmt) compileReindex(vm *vdbe.VDBE, stmt *parser.ReindexStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	// If a name is specified, validate that it exists (either as a table or index)
	if stmt.Name != "" {
		// Check if it's a table
		_, isTable := s.conn.schema.GetTable(stmt.Name)

		// Check if it's an index
		_, isIndex := s.conn.schema.GetIndex(stmt.Name)

		// Must be one or the other
		if !isTable && !isIndex {
			return nil, fmt.Errorf("no such table or index: %s", stmt.Name)
		}
	}
	// If no name is specified, REINDEX all databases (which we support as a no-op)

	// REINDEX is essentially a no-op in this implementation
	// In a full SQLite implementation, this would:
	// 1. Drop and recreate the index(es)
	// 2. Rebuild the index b-tree structure
	// 3. Update statistics
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}
