// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// Helpers shared by multiple tests in this file
// ============================================================================

// newInternalTestConn opens a fresh in-memory database and returns the
// underlying *Conn so unexported methods can be called directly.
func newInternalTestConn(t *testing.T) *Conn {
	t.Helper()
	d := &Driver{}
	rawConn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	conn, ok := rawConn.(*Conn)
	if !ok {
		t.Fatalf("expected *Conn, got %T", rawConn)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// newInternalTestStmt returns a *Stmt wired to conn with a trivial parsed AST.
// The stmt is not registered in conn.stmts — it is only used to exercise
// internal methods that need a valid *Stmt.conn chain.
func newInternalTestStmt(conn *Conn) *Stmt {
	return &Stmt{conn: conn, query: "SELECT 1"}
}

// newMinimalTable builds a *schema.Table with the given column names.
func newMinimalTable(name string, cols ...string) *schema.Table {
	columns := make([]*schema.Column, len(cols))
	for i, c := range cols {
		columns[i] = &schema.Column{Name: c}
	}
	return &schema.Table{Name: name, Columns: columns}
}

// ============================================================================
// StmtCache — Clear, SetSchemaVersion, Capacity
// ============================================================================

func TestInternalStmtCache_Clear(t *testing.T) {
	c := NewStmtCache(10)

	// Put a synthetic (nil) entry via the exported Put method.
	// cloneVdbe handles nil VDBE gracefully.
	c.Put("SELECT 1", nil)
	if c.Size() != 1 {
		t.Fatalf("expected Size=1 after Put, got %d", c.Size())
	}

	c.Clear()
	if c.Size() != 0 {
		t.Errorf("expected Size=0 after Clear, got %d", c.Size())
	}
}

func TestInternalStmtCache_SetSchemaVersion(t *testing.T) {
	c := NewStmtCache(10)
	c.Put("SELECT 1", nil)

	// Advance schema version so the cached entry is stale.
	c.SetSchemaVersion(99)
	got := c.Get("SELECT 1") // stale entry → miss
	if got != nil {
		t.Errorf("expected nil from Get after SetSchemaVersion, got non-nil VDBE")
	}
}

func TestInternalStmtCache_Capacity(t *testing.T) {
	c := NewStmtCache(5)
	if cap := c.Capacity(); cap != 5 {
		t.Errorf("Capacity() = %d, want 5", cap)
	}
}

func TestInternalStmtCache_SetCapacityEvicts(t *testing.T) {
	c := NewStmtCache(5)
	for i := 0; i < 5; i++ {
		key := string(rune('A' + i))
		c.Put(key, nil)
	}
	if c.Size() != 5 {
		t.Fatalf("expected Size=5, got %d", c.Size())
	}

	// Shrink to 2 — should evict 3 entries.
	c.SetCapacity(2)
	if c.Size() != 2 {
		t.Errorf("after SetCapacity(2) expected Size=2, got %d", c.Size())
	}
	if c.Capacity() != 2 {
		t.Errorf("after SetCapacity(2) Capacity()=%d, want 2", c.Capacity())
	}
}

func TestInternalStmtCache_DefaultCapacity(t *testing.T) {
	// capacity ≤ 0 should be normalized to 100
	c := NewStmtCache(0)
	if c.Capacity() != 100 {
		t.Errorf("expected default capacity 100 for NewStmtCache(0), got %d", c.Capacity())
	}
}

// ============================================================================
// setVdbeContext
// ============================================================================

func TestInternalSetVdbeContext(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)
	vm := vdbe.New()

	// Must not panic; sets vm.Ctx with connection info.
	s.setVdbeContext(vm)

	if vm.Ctx == nil {
		t.Error("expected vm.Ctx to be set, got nil")
	}
}

// ============================================================================
// Trigger execution — no triggers registered (all early-return paths)
// ============================================================================

func TestInternalExecuteBeforeInsertTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.InsertStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id", "name")

	if err := s.executeBeforeInsertTriggers(stmt, table); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInternalExecuteAfterInsertTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.InsertStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id")

	if err := s.executeAfterInsertTriggers(stmt, table); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInternalExecuteBeforeUpdateTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.UpdateStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id")

	if err := s.executeBeforeUpdateTriggers(stmt, table, []string{"id"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInternalExecuteAfterUpdateTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.UpdateStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id")

	if err := s.executeAfterUpdateTriggers(stmt, table, []string{"id"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInternalExecuteBeforeDeleteTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.DeleteStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id")

	if err := s.executeBeforeDeleteTriggers(stmt, table); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInternalExecuteAfterDeleteTriggers_NoTriggers(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.DeleteStmt{Table: "no_such_table"}
	table := newMinimalTable("no_such_table", "id")

	if err := s.executeAfterDeleteTriggers(stmt, table); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ============================================================================
// prepareNewRowForInsert & extractValueFromExpression / parseLiteralValue
// ============================================================================

func TestInternalPrepareNewRowForInsert_NoValues(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.InsertStmt{Table: "t", Columns: []string{"id"}}
	// stmt.Values is empty → function returns empty map immediately.
	table := newMinimalTable("t", "id")

	row := s.prepareNewRowForInsert(stmt, table)
	if len(row) != 0 {
		t.Errorf("expected empty map, got %v", row)
	}
}

func TestInternalPrepareNewRowForInsert_WithColumns(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.InsertStmt{
		Table:   "t",
		Columns: []string{"id", "name"},
		Values: [][]parser.Expression{
			{
				&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
				&parser.LiteralExpr{Type: parser.LiteralString, Value: "alice"},
			},
		},
	}
	table := newMinimalTable("t", "id", "name")

	row := s.prepareNewRowForInsert(stmt, table)
	if row["id"] != int64(42) {
		t.Errorf("expected id=42, got %v (%T)", row["id"], row["id"])
	}
	if row["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", row["name"])
	}
}

func TestInternalPrepareNewRowForInsert_ImpliedColumns(t *testing.T) {
	// When Columns is empty the function uses all table columns.
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	stmt := &parser.InsertStmt{
		Table: "t",
		// No Columns specified
		Values: [][]parser.Expression{
			{
				&parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"},
				&parser.LiteralExpr{Type: parser.LiteralNull, Value: ""},
			},
		},
	}
	table := newMinimalTable("t", "x", "y")

	row := s.prepareNewRowForInsert(stmt, table)
	if row["x"] != float64(3.14) {
		t.Errorf("expected x=3.14, got %v (%T)", row["x"], row["x"])
	}
	if row["y"] != nil {
		t.Errorf("expected y=nil, got %v", row["y"])
	}
}

func TestInternalExtractValueFromExpression_Variable(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	// VariableExpr (bound parameter) → nil placeholder
	val := s.extractValueFromExpression(&parser.VariableExpr{Name: "?"})
	if val != nil {
		t.Errorf("expected nil for VariableExpr, got %v", val)
	}
}

func TestInternalExtractValueFromExpression_ComplexExpr(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	// Any other expression type → nil placeholder (default branch).
	// Use a BinaryExpr as a representative "complex" expression.
	val := s.extractValueFromExpression(&parser.BinaryExpr{
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Op:    parser.OpPlus,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	})
	if val != nil {
		t.Errorf("expected nil for complex expr, got %v", val)
	}
}

func TestInternalParseLiteralValue_DefaultBranch(t *testing.T) {
	conn := newInternalTestConn(t)
	s := newInternalTestStmt(conn)

	// Use a LiteralType value that does not match any known case
	// (e.g. a zero value that is not Integer/Float/String/Null).
	// LiteralType is an int; 99 is outside the defined constants.
	expr := &parser.LiteralExpr{Type: parser.LiteralType(99), Value: "raw"}
	val := s.parseLiteralValue(expr)
	if val != "raw" {
		t.Errorf("expected raw value %q, got %v", "raw", val)
	}
}

// ============================================================================
// sqlite_test_helpers.go coverage — exercise functions from inside package
// ============================================================================

func TestInternalSetupDiskDB(t *testing.T) {
	db := setupDiskDB(t)
	defer db.Close()
	if db == nil {
		t.Fatal("setupDiskDB returned nil")
	}
}

func TestInternalCompareRowsUnordered_Match(t *testing.T) {
	got := [][]interface{}{{int64(1), "a"}, {int64(2), "b"}}
	want := [][]interface{}{{int64(2), "b"}, {int64(1), "a"}}
	compareRowsUnordered(t, got, want)
}

func TestInternalMatchGotRows_NoMatch(t *testing.T) {
	// findMatchingRow returns false when nothing matches.
	row := []interface{}{int64(99)}
	want := [][]interface{}{{int64(1)}, {int64(2)}}
	matched := make([]bool, len(want))
	found := findMatchingRow(row, want, matched)
	if found {
		t.Error("expected false from findMatchingRow for non-matching row")
	}
}

func TestInternalRowsEqual_LengthMismatch(t *testing.T) {
	if rowsEqual([]interface{}{1}, []interface{}{1, 2}) {
		t.Error("expected false for rows with different column counts")
	}
}

func TestInternalRowsEqual_Equal(t *testing.T) {
	if !rowsEqual([]interface{}{int64(1), "x"}, []interface{}{int64(1), "x"}) {
		t.Error("expected true for equal rows")
	}
}

func TestInternalEqualInt_Conversions(t *testing.T) {
	if !equalInt(5, int64(5)) {
		t.Error("equalInt(5, int64(5)) should be true")
	}
	if !equalInt(5, 5) {
		t.Error("equalInt(5, 5) should be true")
	}
	if !equalInt(5, float64(5)) {
		t.Error("equalInt(5, float64(5)) should be true")
	}
	if equalInt(5, "5") {
		t.Error("equalInt(5, \"5\") should be false")
	}
}

func TestInternalMustQuery(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE x (v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := mustQuery(t, db, "SELECT v FROM x")
	defer rows.Close()
	// No rows — just exercise the happy path.
	if err := rows.Err(); err != nil {
		t.Errorf("unexpected rows.Err: %v", err)
	}
}

// ============================================================================
// ensureMasterPage — exercise via a real connection open sequence
// ============================================================================

func TestInternalEnsureMasterPage(t *testing.T) {
	// ensureMasterPage is called during openDatabase; exercising it via a
	// freshly created Conn (which goes through the full open path) is the
	// most reliable approach since calling it directly would require a
	// partially-initialised btree.
	conn := newInternalTestConn(t)

	// Call it directly on the already-opened connection to cover both branches:
	// 1. Page 1 already exists → returns nil immediately.
	err := conn.ensureMasterPage()
	if err != nil {
		t.Errorf("ensureMasterPage returned unexpected error: %v", err)
	}
}

// ============================================================================
// ConnRowReader — FindReferencingRowsWithParentAffinity, ReadRowByRowid
// ============================================================================

func TestInternalConnRowReader_FindReferencingRowsWithParentAffinity(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`CREATE TABLE parent (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE child (pid INTEGER REFERENCES parent(id))`); err != nil {
		t.Fatalf("create child: %v", err)
	}

	// Get the underlying *Conn to call methods directly.
	rawConn, err := db.Driver().Open(":memory:")
	if err != nil {
		t.Fatalf("open raw conn: %v", err)
	}
	defer rawConn.Close()

	// Use a fresh conn from the global driver so it shares the DriverName registration.
	d2 := &Driver{}
	rc2, err := d2.Open(":memory:")
	if err != nil {
		t.Fatalf("open d2: %v", err)
	}
	defer rc2.Close()
	conn2 := rc2.(*Conn)

	rr := &ConnRowReader{conn: conn2}

	// Table doesn't exist in conn2 — call just exercises the method path.
	_, err = rr.FindReferencingRowsWithParentAffinity(
		"child", []string{"pid"}, []interface{}{int64(1)},
		"parent", []string{"id"},
	)
	// Error is expected because the tables don't exist in conn2.
	// We only care that the method runs without panicking.
	_ = err
}

func TestInternalConnRowReader_ReadRowByRowid(t *testing.T) {
	d := &Driver{}
	rc, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	conn := rc.(*Conn)

	rr := &ConnRowReader{conn: conn}
	// Table doesn't exist — just exercises the code path without panicking.
	_, err = rr.ReadRowByRowid("no_table", 1)
	_ = err // error expected; we only verify no panic
}
