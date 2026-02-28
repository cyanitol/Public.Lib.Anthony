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

	// Evaluate the filename expression to get the database file path
	var filename string
	if litExpr, ok := stmt.Filename.(*parser.LiteralExpr); ok {
		filename = litExpr.Value
	} else {
		return nil, fmt.Errorf("ATTACH DATABASE requires a string literal filename")
	}

	// Validate schema name
	schemaName := stmt.SchemaName
	if schemaName == "" {
		return nil, fmt.Errorf("ATTACH DATABASE requires a schema name")
	}

	// Check for reserved schema names
	if schemaName == "main" || schemaName == "temp" {
		return nil, fmt.Errorf("cannot ATTACH database with reserved name: %s", schemaName)
	}

	// Open the database file
	p, err := pager.Open(filename, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file %s: %w", filename, err)
	}

	// Create btree for the attached database
	bt := btree.NewBtree(uint32(p.PageSize()))

	// Attach the database to the connection's registry
	if err := s.conn.dbRegistry.AttachDatabase(schemaName, filename, p, bt); err != nil {
		p.Close()
		return nil, fmt.Errorf("failed to attach database: %w", err)
	}

	// Generate simple bytecode that succeeds
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
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
