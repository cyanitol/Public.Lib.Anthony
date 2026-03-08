// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// Table DDL Compilation
// ============================================================================

// compileCreateTable compiles a CREATE TABLE statement.
func (s *Stmt) compileCreateTable(vm *vdbe.VDBE, stmt *parser.CreateTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the table in the schema
	// This simplified implementation registers the table in memory
	// A full implementation would also persist to sqlite_master
	table, err := s.conn.schema.CreateTable(stmt)
	if err != nil {
		return nil, err
	}

	// Allocate a root page for the table btree
	if s.conn.btree != nil {
		var rootPage uint32
		var err error
		if stmt.WithoutRowID {
			// WITHOUT ROWID tables use a different page type for composite keys
			rootPage, err = s.conn.btree.CreateWithoutRowidTable()
		} else {
			rootPage, err = s.conn.btree.CreateTable()
		}
		if err != nil {
			return nil, fmt.Errorf("failed to allocate table root page: %w", err)
		}
		table.RootPage = rootPage
	} else {
		// For in-memory databases without btree, use a placeholder
		table.RootPage = 2
	}

	// Register foreign key constraints with the FK manager
	if err := s.registerForeignKeyConstraints(table, stmt); err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// registerForeignKeyConstraints registers foreign key constraints from a CREATE TABLE statement
// with the connection's ForeignKeyManager.
func (s *Stmt) registerForeignKeyConstraints(_ interface{}, stmt *parser.CreateTableStmt) error {
	if s.conn.fkManager == nil {
		return nil
	}
	s.registerTableLevelFKs(stmt)
	s.registerColumnLevelFKs(stmt)
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
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("table not found: %s", stmt.Name)
	}

	// Check if foreign keys are enabled and any FK constraints reference this table
	if s.conn.fkManager != nil && s.conn.fkManager.IsEnabled() {
		referencingConstraints := s.conn.fkManager.FindReferencingConstraints(stmt.Name)
		if len(referencingConstraints) > 0 {
			return nil, fmt.Errorf("FOREIGN KEY constraint failed: cannot drop table %s, referenced by foreign key constraint", stmt.Name)
		}
	}

	// Drop the table from the schema
	// This simplified implementation removes the table from memory
	// A full implementation would also:
	// 1. Delete entry from sqlite_master table
	// 2. Free all pages used by the table
	// 3. Update the schema cookie
	if err := s.conn.schema.DropTable(stmt.Name); err != nil {
		return nil, err
	}

	// Remove FK constraints that belonged to this table
	if s.conn.fkManager != nil {
		s.conn.fkManager.RemoveConstraints(stmt.Name)
	}

	// Free table pages if btree is available
	if s.conn.btree != nil && table.RootPage > 0 {
		// In a full implementation, would call btree.FreePage(table.RootPage)
		// and recursively free all pages in the table's btree
		// For now, we just note that the page should be freed
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	// In a full implementation with sqlite_master:
	// - OpOpenWrite on sqlite_master cursor
	// - OpSeek to find the table entry
	// - OpDelete to remove it
	// - OpSetCookie to update schema version
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Invalidate statement cache since schema has changed
	s.invalidateStmtCache()

	return vm, nil
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

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Update the schema cookie

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
		return nil, fmt.Errorf("view not found: %s", stmt.Name)
	}

	// Drop the view from the schema
	if err := s.conn.schema.DropView(stmt.Name); err != nil {
		return nil, err
	}

	// In a full implementation, this would:
	// 1. Delete entry from sqlite_master table
	// 2. Update the schema cookie

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
	vm.SetReadOnly(false)
	vm.InTxn = true

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileCommit compiles a COMMIT statement.
func (s *Stmt) compileCommit(vm *vdbe.VDBE, stmt *parser.CommitStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	vm.AddOp(vdbe.OpCommit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRollback compiles a ROLLBACK statement.
func (s *Stmt) compileRollback(vm *vdbe.VDBE, stmt *parser.RollbackStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	vm.AddOp(vdbe.OpRollback, 0, 0, 0)
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
