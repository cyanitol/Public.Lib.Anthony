// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// ============================================================================
// substituteSelect (trigger_runtime.go:263)
// Direct unit tests against the unexported triggerSubstitutor.
// ============================================================================

// TestTriggerRuntime3_SubstituteSelectNoWhere exercises substituteSelect when
// stmt.Where is nil – the returned stmt should be unchanged.
func TestTriggerRuntime3Coverage_SubstituteSelectNoWhere(t *testing.T) {
	s := &triggerSubstitutor{
		newRow: map[string]interface{}{"id": int64(1)},
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Star: true},
		},
	}
	got, err := s.substituteSelect(stmt)
	if err != nil {
		t.Fatalf("substituteSelect no-where: unexpected error: %v", err)
	}
	if got.Where != nil {
		t.Errorf("substituteSelect no-where: expected Where=nil, got non-nil")
	}
}

// TestTriggerRuntime3_SubstituteSelectWithWhere exercises the Where != nil
// branch of substituteSelect. The WHERE clause references NEW.id, which gets
// replaced with the literal value from newRow.
func TestTriggerRuntime3Coverage_SubstituteSelectWithWhere(t *testing.T) {
	s := &triggerSubstitutor{
		newRow: map[string]interface{}{"id": int64(42)},
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		Where: &parser.IdentExpr{
			Table: "NEW",
			Name:  "id",
		},
	}
	got, err := s.substituteSelect(stmt)
	if err != nil {
		t.Fatalf("substituteSelect with-where: unexpected error: %v", err)
	}
	if got.Where == nil {
		t.Fatalf("substituteSelect with-where: expected Where non-nil after substitution")
	}
	lit, ok := got.Where.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("substituteSelect with-where: expected *LiteralExpr after substitution, got %T", got.Where)
	}
	if lit.Value != "42" {
		t.Errorf("substituteSelect with-where: expected literal '42', got %q", lit.Value)
	}
}

// TestTriggerRuntime3_SubstituteSelectWhereError exercises the error path of
// substituteSelect: the WHERE refers to OLD.missing when oldRow is nil.
func TestTriggerRuntime3Coverage_SubstituteSelectWhereError(t *testing.T) {
	s := &triggerSubstitutor{
		oldRow: nil,
		newRow: map[string]interface{}{"id": int64(1)},
	}
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		Where: &parser.IdentExpr{
			Table: "OLD",
			Name:  "col",
		},
	}
	_, err := s.substituteSelect(stmt)
	if err == nil {
		t.Error("substituteSelect error path: expected error when OLD is nil, got nil")
	}
}

// ============================================================================
// valueToLiteral (trigger_runtime.go:485)
// Direct unit tests covering every type branch.
// ============================================================================

func TestTriggerRuntime3Coverage_ValueToLiteral_Nil(t *testing.T) {
	lit := valueToLiteral(nil)
	if lit.Type != parser.LiteralNull {
		t.Errorf("nil: expected LiteralNull, got %v", lit.Type)
	}
	if lit.Value != "NULL" {
		t.Errorf("nil: expected 'NULL', got %q", lit.Value)
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_Int64(t *testing.T) {
	lit := valueToLiteral(int64(1234567890))
	if lit.Type != parser.LiteralInteger {
		t.Errorf("int64: expected LiteralInteger, got %v", lit.Type)
	}
	if lit.Value != "1234567890" {
		t.Errorf("int64: expected '1234567890', got %q", lit.Value)
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_Int(t *testing.T) {
	lit := valueToLiteral(int(99))
	if lit.Type != parser.LiteralInteger {
		t.Errorf("int: expected LiteralInteger, got %v", lit.Type)
	}
	if lit.Value != "99" {
		t.Errorf("int: expected '99', got %q", lit.Value)
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_Float64(t *testing.T) {
	lit := valueToLiteral(float64(3.14))
	if lit.Type != parser.LiteralFloat {
		t.Errorf("float64: expected LiteralFloat, got %v", lit.Type)
	}
	if lit.Value == "" {
		t.Errorf("float64: expected non-empty Value")
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_String(t *testing.T) {
	lit := valueToLiteral("hello world")
	if lit.Type != parser.LiteralString {
		t.Errorf("string: expected LiteralString, got %v", lit.Type)
	}
	if lit.Value != "hello world" {
		t.Errorf("string: expected 'hello world', got %q", lit.Value)
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_BoolTrue(t *testing.T) {
	lit := valueToLiteral(true)
	if lit.Type != parser.LiteralInteger {
		t.Errorf("bool(true): expected LiteralInteger, got %v", lit.Type)
	}
	if lit.Value != "1" {
		t.Errorf("bool(true): expected '1', got %q", lit.Value)
	}
}

func TestTriggerRuntime3Coverage_ValueToLiteral_BoolFalse(t *testing.T) {
	lit := valueToLiteral(false)
	if lit.Type != parser.LiteralInteger {
		t.Errorf("bool(false): expected LiteralInteger, got %v", lit.Type)
	}
	if lit.Value != "0" {
		t.Errorf("bool(false): expected '0', got %q", lit.Value)
	}
}

// TestTriggerRuntime3_ValueToLiteral_Default covers the default branch with a
// []byte value, which is not a handled type and falls through to NULL.
func TestTriggerRuntime3Coverage_ValueToLiteral_Default(t *testing.T) {
	lit := valueToLiteral([]byte("blob"))
	if lit.Type != parser.LiteralNull {
		t.Errorf("[]byte default: expected LiteralNull, got %v", lit.Type)
	}
}

// ============================================================================
// conn.go:ensureMasterPage (line 542)
// ============================================================================

// TestTriggerRuntime3_EnsureMasterPageNilBtree covers the early-return branch
// where c.btree == nil.
func TestTriggerRuntime3Coverage_EnsureMasterPageNilBtree(t *testing.T) {
	c := &Conn{btree: nil}
	if err := c.ensureMasterPage(); err != nil {
		t.Fatalf("ensureMasterPage nil btree: unexpected error: %v", err)
	}
}

// TestTriggerRuntime3_EnsureMasterPagePageExists exercises the branch where
// page 1 already exists (GetPage succeeds → early return nil).
func TestTriggerRuntime3Coverage_EnsureMasterPagePageExists(t *testing.T) {
	c := openMemConn(t)
	// A freshly opened memory connection has its master page created during
	// openDatabase → page 1 always exists, so GetPage(1) succeeds → early return.
	if err := c.ensureMasterPage(); err != nil {
		t.Fatalf("ensureMasterPage page exists: unexpected error: %v", err)
	}
}

// ============================================================================
// conn.go:registerBuiltinVirtualTables (line 525)
// ============================================================================

// TestTriggerRuntime3_RegisterBuiltinVirtualTables_Success exercises the happy
// path by calling registerBuiltinVirtualTables on a fresh registry.
func TestTriggerRuntime3Coverage_RegisterBuiltinVirtualTables_Success(t *testing.T) {
	c := openMemConn(t)
	// Replace the registry with a fresh one to ensure fts5/rtree are not yet
	// registered, then call the function under test.
	c.vtabRegistry = vtab.NewModuleRegistry()
	if err := c.registerBuiltinVirtualTables(); err != nil {
		t.Fatalf("registerBuiltinVirtualTables: unexpected error: %v", err)
	}
}

// TestTriggerRuntime3_RegisterBuiltinVirtualTables_DuplicateError exercises
// the error return paths by registering the modules a second time.
func TestTriggerRuntime3Coverage_RegisterBuiltinVirtualTables_DuplicateError(t *testing.T) {
	c := openMemConn(t)
	// First call succeeds (already called by openDatabase internally).
	// Second call should return an error (duplicate module name).
	err := c.registerBuiltinVirtualTables()
	if err == nil {
		t.Error("registerBuiltinVirtualTables duplicate: expected error for duplicate registration, got nil")
	}
}

// ============================================================================
// driver.go:createMemoryConnection (line 232)
// ============================================================================

// TestTriggerRuntime3_CreateMemoryConnection_Success exercises the happy path
// of createMemoryConnection by creating a full memory connection via Driver.Open.
func TestTriggerRuntime3Coverage_CreateMemoryConnection_Success(t *testing.T) {
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("createMemoryConnection success: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	// Verify the connection is functional.
	stmt, err := c.Prepare("CREATE TABLE t(id INTEGER)")
	if err != nil {
		t.Fatalf("createMemoryConnection: Prepare failed: %v", err)
	}
	if _, err := stmt.Exec(nil); err != nil {
		t.Fatalf("createMemoryConnection: Exec failed: %v", err)
	}
	stmt.Close()
}

// TestTriggerRuntime3_CreateMemoryConnection_WithConfig exercises the security
// config nil branch inside createMemoryConnection (config.Security == nil →
// DefaultSecurityConfig() is used).
func TestTriggerRuntime3Coverage_CreateMemoryConnection_WithConfig(t *testing.T) {
	d := &Driver{}
	// Open via DSN – config.Security is nil by default, exercising the nil branch.
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("createMemoryConnection with-config: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	if c.securityConfig == nil {
		t.Error("createMemoryConnection with-config: expected securityConfig to be non-nil")
	}
}

// ============================================================================
// driver.go:MarkDirty on memoryPagerProvider (line 378)
// ============================================================================

// markDirtyExecSQL prepares and executes a SQL statement on the given Conn,
// closing the prepared statement afterward.
func markDirtyExecSQL(t *testing.T, c *Conn, sql string, args []driver.Value) {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("MarkDirty: Prepare %q: %v", sql, err)
	}
	if _, err := stmt.(*Stmt).Exec(args); err != nil {
		t.Fatalf("MarkDirty: Exec %q: %v", sql, err)
	}
	stmt.Close()
}

// markDirtyQueryInt64 prepares, queries, and returns a single int64 value.
func markDirtyQueryInt64(t *testing.T, c *Conn, sql string) int64 {
	t.Helper()
	q, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("MarkDirty: Prepare %q: %v", sql, err)
	}
	rows, err := q.(*Stmt).Query(nil)
	if err != nil {
		t.Fatalf("MarkDirty: Query %q: %v", sql, err)
	}
	vals := make([]driver.Value, 1)
	if err := rows.Next(vals); err != nil {
		t.Fatalf("MarkDirty: Next: %v", err)
	}
	rows.Close()
	q.Close()
	v, ok := vals[0].(int64)
	if !ok {
		t.Fatalf("MarkDirty: expected int64, got %T", vals[0])
	}
	return v
}

// TestTriggerRuntime3_MarkDirty exercises memoryPagerProvider.MarkDirty by
// performing a sequence of INSERT + UPDATE + DELETE operations on an in-memory
// database, which force pages to be dirtied/journaled through MarkDirty.
func TestTriggerRuntime3Coverage_MarkDirty(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)
	_ = s

	// Setup table.
	markDirtyExecSQL(t, c, "CREATE TABLE items(id INTEGER PRIMARY KEY, val INTEGER)", nil)

	// INSERT rows to allocate pages and trigger MarkDirty.
	for i := 1; i <= 30; i++ {
		markDirtyExecSQL(t, c, "INSERT INTO items VALUES (?, ?)", []driver.Value{int64(i), int64(i * 10)})
	}

	// UPDATE to dirty existing pages.
	markDirtyExecSQL(t, c, "UPDATE items SET val = val + 1 WHERE id <= 15", nil)

	// DELETE to dirty pages further.
	markDirtyExecSQL(t, c, "DELETE FROM items WHERE id > 20", nil)

	// Verify final state.
	count := markDirtyQueryInt64(t, c, "SELECT COUNT(*) FROM items")
	if count != 20 {
		t.Errorf("MarkDirty: expected 20 remaining rows, got %d", count)
	}
}
