// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

func TestDebugAggBytecode(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "debug_agg.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE numbers(value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for _, v := range []int{1, 2, 3, 4, 5} {
		_, err = db.Exec("INSERT INTO numbers(value) VALUES(?)", v)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Get the underlying connection
	conn, err := db.Driver().Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open conn: %v", err)
	}
	defer conn.Close()

	driverConn := conn.(*Conn)

	// Parse the SQL
	query := "SELECT count(*) FROM numbers"
	p := parser.NewParser(query)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	stmt := &Stmt{
		conn:  driverConn,
		query: query,
		ast:   stmts[0],
	}

	vm, err := stmt.compile(nil)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	t.Logf("Bytecode for SELECT count(*) FROM numbers:")
	for i, instr := range vm.Program {
		name := vdbe.OpcodeNames[instr.Opcode]
		if name == "" {
			name = "???"
		}
		t.Logf("  [%3d] %-15s P1=%-4d P2=%-4d P3=%-4d", i, name, instr.P1, instr.P2, instr.P3)
	}

	// Check what the parser produced
	selectStmt := stmt.ast.(*parser.SelectStmt)
	fnExpr := selectStmt.Columns[0].Expr.(*parser.FunctionExpr)
	t.Logf("Function name: %q, Star: %v", fnExpr.Name, fnExpr.Star)
	t.Logf("detectAggregates: %v", stmt.detectAggregates(selectStmt))
	t.Logf("isAggregateExpr: %v", stmt.isAggregateExpr(fnExpr))

	// Now step through and get the result
	hasRow, err := vm.Step()
	if err != nil {
		t.Fatalf("Step error: %v", err)
	}
	if hasRow {
		mem, err := vm.GetMem(0)
		if err != nil {
			t.Fatalf("GetMem error: %v", err)
		}
		t.Logf("Result register 0: int=%d", mem.IntValue())
	} else {
		t.Logf("No row returned")
	}
}
