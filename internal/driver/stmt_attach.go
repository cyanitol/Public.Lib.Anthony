// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileAttach compiles an ATTACH DATABASE statement.
func (s *Stmt) compileAttach(vm *vdbe.VDBE, stmt *parser.AttachStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	filename, schemaName, err := s.extractAttachParameters(stmt)
	if err != nil {
		return nil, err
	}

	validatedPath, err := s.validateDatabasePath(filename)
	if err != nil {
		return nil, fmt.Errorf("invalid database path: %w", err)
	}

	if err := s.performDatabaseAttach(schemaName, validatedPath); err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// extractAttachParameters extracts and validates filename and schema name from ATTACH statement.
func (s *Stmt) extractAttachParameters(stmt *parser.AttachStmt) (string, string, error) {
	filename, err := s.extractFilename(stmt)
	if err != nil {
		return "", "", err
	}

	schemaName, err := s.validateSchemaName(stmt.SchemaName)
	if err != nil {
		return "", "", err
	}

	return filename, schemaName, nil
}

// extractFilename extracts the filename from the ATTACH statement.
func (s *Stmt) extractFilename(stmt *parser.AttachStmt) (string, error) {
	litExpr, ok := stmt.Filename.(*parser.LiteralExpr)
	if !ok {
		return "", fmt.Errorf("ATTACH DATABASE requires a string literal filename")
	}
	return litExpr.Value, nil
}

// validateSchemaName validates that the schema name is valid and not reserved.
func (s *Stmt) validateSchemaName(schemaName string) (string, error) {
	if schemaName == "" {
		return "", fmt.Errorf("ATTACH DATABASE requires a schema name")
	}

	if schemaName == "main" || schemaName == "temp" {
		return "", fmt.Errorf("cannot ATTACH database with reserved name: %s", schemaName)
	}

	return schemaName, nil
}

// performDatabaseAttach opens and attaches the database.
func (s *Stmt) performDatabaseAttach(schemaName, validatedPath string) error {
	p, bt, err := s.openDatabase(validatedPath)
	if err != nil {
		return err
	}

	if err := s.conn.dbRegistry.AttachDatabase(schemaName, validatedPath, p, bt); err != nil {
		p.Close()
		return fmt.Errorf("failed to attach database: %w", err)
	}

	return nil
}

// openDatabase opens a database file and creates its btree.
func (s *Stmt) openDatabase(validatedPath string) (*pager.Pager, *btree.Btree, error) {
	p, err := pager.Open(validatedPath, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database file %s: %w", validatedPath, err)
	}

	bt := btree.NewBtree(uint32(p.PageSize()))
	return p, bt, nil
}

// compileDetach compiles a DETACH DATABASE statement.
func (s *Stmt) compileDetach(vm *vdbe.VDBE, stmt *parser.DetachStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Validate schema name
	schemaName := stmt.SchemaName
	if schemaName == "" {
		return nil, fmt.Errorf("DETACH DATABASE requires a schema name")
	}

	// Detach the database from the connection's registry
	if err := s.conn.dbRegistry.DetachDatabase(schemaName); err != nil {
		return nil, fmt.Errorf("failed to detach database: %w", err)
	}

	// Generate simple bytecode that succeeds
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}
