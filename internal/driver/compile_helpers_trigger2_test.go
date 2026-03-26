// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// Tests for the "has triggers" execution paths in the six execute*Triggers
// helper functions (executeBeforeInsertTriggers, executeAfterInsertTriggers,
// executeBeforeUpdateTriggers, executeAfterUpdateTriggers,
// executeBeforeDeleteTriggers, executeAfterDeleteTriggers).
//
// The "no triggers → return nil" branch is already covered by existing tests.
// These tests register a real trigger in the schema so that GetTableTriggers
// returns a non-empty slice, forcing execution past the early return.

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// setupTriggerConn creates a connection with a table "t" already present in
// the schema.  It returns the connection and the *schema.Table.
func setupTriggerConn(t *testing.T) (*Conn, *schema.Table) {
	t.Helper()
	conn := openMemConn(t)
	if err := conn.ExecDDL("CREATE TABLE t (id INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	tbl, ok := conn.schema.GetTable("t")
	if !ok {
		t.Fatal("table 't' not found in schema after CREATE TABLE")
	}
	return conn, tbl
}

// minimalSelectBody returns a trigger body containing a single no-op
// SelectStmt.  When executeTriggerBody processes it, it creates a fresh VDBE,
// calls vm.Run() on an empty program (which returns nil immediately), and
// returns nil — so the trigger body never fails.
func minimalSelectBody() []parser.Statement {
	return []parser.Statement{
		&parser.SelectStmt{},
	}
}

// addTrigger registers a trigger directly in conn.schema.
func addTrigger(conn *Conn, name, table string, timing parser.TriggerTiming, event parser.TriggerEvent) {
	conn.schema.AddTriggerDirect(&schema.Trigger{
		Name:       name,
		Table:      table,
		Timing:     timing,
		Event:      event,
		ForEachRow: true,
		Body:       minimalSelectBody(),
	})
}

// ---------------------------------------------------------------------------
// TestCompileHelpersTrigger2 — six sub-tests, one per function
// ---------------------------------------------------------------------------

func TestCompileHelpersTrigger2(t *testing.T) {
	t.Run("executeBeforeInsertTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_bi", "t", parser.TriggerBefore, parser.TriggerInsert)
		s := stmtFor(conn)

		stmt := &parser.InsertStmt{
			Table:   "t",
			Columns: []string{"id", "val"},
			Values: [][]parser.Expression{
				{
					&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
					&parser.LiteralExpr{Type: parser.LiteralString, Value: "a"},
				},
			},
		}

		if err := s.executeBeforeInsertTriggers(stmt, tbl); err != nil {
			t.Errorf("executeBeforeInsertTriggers: unexpected error: %v", err)
		}
	})

	t.Run("executeAfterInsertTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_ai", "t", parser.TriggerAfter, parser.TriggerInsert)
		s := stmtFor(conn)

		stmt := &parser.InsertStmt{
			Table:   "t",
			Columns: []string{"id", "val"},
			Values: [][]parser.Expression{
				{
					&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
					&parser.LiteralExpr{Type: parser.LiteralString, Value: "b"},
				},
			},
		}

		if err := s.executeAfterInsertTriggers(stmt, tbl); err != nil {
			t.Errorf("executeAfterInsertTriggers: unexpected error: %v", err)
		}
	})

	t.Run("executeBeforeUpdateTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_bu", "t", parser.TriggerBefore, parser.TriggerUpdate)
		s := stmtFor(conn)

		stmt := &parser.UpdateStmt{
			Table: "t",
			Sets: []parser.Assignment{
				{Column: "val", Value: &parser.LiteralExpr{Type: parser.LiteralString, Value: "x"}},
			},
		}

		if err := s.executeBeforeUpdateTriggers(stmt, tbl, []string{"val"}); err != nil {
			t.Errorf("executeBeforeUpdateTriggers: unexpected error: %v", err)
		}
	})

	t.Run("executeAfterUpdateTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_au", "t", parser.TriggerAfter, parser.TriggerUpdate)
		s := stmtFor(conn)

		stmt := &parser.UpdateStmt{
			Table: "t",
			Sets: []parser.Assignment{
				{Column: "val", Value: &parser.LiteralExpr{Type: parser.LiteralString, Value: "y"}},
			},
		}

		if err := s.executeAfterUpdateTriggers(stmt, tbl, []string{"val"}); err != nil {
			t.Errorf("executeAfterUpdateTriggers: unexpected error: %v", err)
		}
	})

	t.Run("executeBeforeDeleteTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_bd", "t", parser.TriggerBefore, parser.TriggerDelete)
		s := stmtFor(conn)

		stmt := &parser.DeleteStmt{Table: "t"}

		if err := s.executeBeforeDeleteTriggers(stmt, tbl); err != nil {
			t.Errorf("executeBeforeDeleteTriggers: unexpected error: %v", err)
		}
	})

	t.Run("executeAfterDeleteTriggers_hasTrigger", func(t *testing.T) {
		conn, tbl := setupTriggerConn(t)
		addTrigger(conn, "trg_ad", "t", parser.TriggerAfter, parser.TriggerDelete)
		s := stmtFor(conn)

		stmt := &parser.DeleteStmt{Table: "t"}

		if err := s.executeAfterDeleteTriggers(stmt, tbl); err != nil {
			t.Errorf("executeAfterDeleteTriggers: unexpected error: %v", err)
		}
	})
}
