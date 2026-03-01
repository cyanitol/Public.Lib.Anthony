// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
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
		rootPage, err := s.conn.btree.CreateTable()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate table root page: %w", err)
		}
		table.RootPage = rootPage
	} else {
		// For in-memory databases without btree, use a placeholder
		table.RootPage = 2
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileDropTable compiles a DROP TABLE statement.
func (s *Stmt) compileDropTable(vm *vdbe.VDBE, stmt *parser.DropTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// In a real implementation, this would:
	// 1. Remove entry from sqlite_master
	// 2. Free all pages used by the table
	// 3. Update the schema in memory

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// TODO: Generate bytecode to:
	// - Delete from sqlite_master table
	// - Free table pages
	// - Update schema cookie

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
	// TODO: Add commit opcode
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRollback compiles a ROLLBACK statement.
func (s *Stmt) compileRollback(vm *vdbe.VDBE, stmt *parser.RollbackStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	// TODO: Add rollback opcode
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}
