// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC test coverage for the driver package, batch 19b.
// Targets:
//   stmt_attach.go:  extractFilename — !ok error path and success path
//   conn.go:         hasAttachedDatabases — nil registry and empty registry paths
//   driver.go:       AllocatePageData, MarkDirty (pagerProvider)
//   compile_tvf.go:  evalTVFUnary — default branch (unknown op returns true)
//   multi_stmt.go:   buildResult — nil lastResult path

import (
	"database/sql/driver"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// extractFilename
// ---------------------------------------------------------------------------

// TestMCDC19b_ExtractFilename_NotLiteral covers the !ok branch in
// extractFilename when stmt.Filename is not a *parser.LiteralExpr.
func TestMCDC19b_ExtractFilename_NotLiteral(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	s := &Stmt{conn: c}

	stmt := &parser.AttachStmt{
		Filename:   &parser.IdentExpr{Name: "mydb"}, // not a LiteralExpr
		SchemaName: "aux",
	}

	filename, err := s.extractFilename(stmt)
	if err == nil {
		t.Error("extractFilename with IdentExpr should return error, got nil")
	}
	if filename != "" {
		t.Errorf("expected empty filename on error, got %q", filename)
	}
}

// TestMCDC19b_ExtractFilename_Literal covers the success path in
// extractFilename when stmt.Filename is a *parser.LiteralExpr.
func TestMCDC19b_ExtractFilename_Literal(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	s := &Stmt{conn: c}

	stmt := &parser.AttachStmt{
		Filename:   &parser.LiteralExpr{Type: parser.LiteralString, Value: "mydb.db"},
		SchemaName: "aux",
	}

	filename, err := s.extractFilename(stmt)
	if err != nil {
		t.Fatalf("extractFilename with LiteralExpr: %v", err)
	}
	if filename != "mydb.db" {
		t.Errorf("filename = %q, want %q", filename, "mydb.db")
	}
}

// ---------------------------------------------------------------------------
// hasAttachedDatabases
// ---------------------------------------------------------------------------

// TestMCDC19b_HasAttachedDatabases_NilRegistry covers the c.dbRegistry == nil
// branch of hasAttachedDatabases.
func TestMCDC19b_HasAttachedDatabases_NilRegistry(t *testing.T) {
	t.Parallel()

	c := &Conn{dbRegistry: nil}
	if c.hasAttachedDatabases() {
		t.Error("hasAttachedDatabases with nil registry should return false")
	}
}

// TestMCDC19b_HasAttachedDatabases_EmptyRegistry covers the len(dbs) <= 1 path
// when the registry exists but has only the "main" database (no attachments).
func TestMCDC19b_HasAttachedDatabases_EmptyRegistry(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	// A freshly opened :memory: connection has only "main" registered.
	if c.hasAttachedDatabases() {
		t.Error("freshly opened connection should not have attached databases")
	}
}

// ---------------------------------------------------------------------------
// AllocatePageData (pagerProvider)
// ---------------------------------------------------------------------------

// TestMCDC19b_AllocatePageData_Basic creates a real file-based pager, wraps it
// in a pagerProvider, allocates a page, and verifies the returned page number
// and data slice.
func TestMCDC19b_AllocatePageData_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "alloc_page.db")

	pgr, err := pager.Open(dbFile, false)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	defer pgr.Close()

	// Begin a write transaction so that Get/Write succeed.
	if err := pgr.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	pp := newPagerProvider(pgr)

	pgno, data, err := pp.AllocatePageData()
	if err != nil {
		t.Fatalf("AllocatePageData: %v", err)
	}
	if pgno == 0 {
		t.Error("AllocatePageData returned page number 0")
	}
	if len(data) == 0 {
		t.Error("AllocatePageData returned empty data slice")
	}

	_ = pgr.Rollback()
}

// ---------------------------------------------------------------------------
// MarkDirty (pagerProvider)
// ---------------------------------------------------------------------------

// TestMCDC19b_MarkDirty_Basic allocates a page via pagerProvider and then
// marks it dirty, verifying that no error is returned.
func TestMCDC19b_MarkDirty_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "mark_dirty.db")

	pgr, err := pager.Open(dbFile, false)
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	defer pgr.Close()

	if err := pgr.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	pp := newPagerProvider(pgr)

	pgno, _, err := pp.AllocatePageData()
	if err != nil {
		t.Fatalf("AllocatePageData: %v", err)
	}

	if err := pp.MarkDirty(pgno); err != nil {
		t.Errorf("MarkDirty(%d): %v", pgno, err)
	}

	_ = pgr.Rollback()
}

// ---------------------------------------------------------------------------
// evalTVFUnary — default branch (unknown op)
// ---------------------------------------------------------------------------

// TestMCDC19b_EvalTVFUnary_Default passes an op that is neither OpIsNull nor
// OpNotNull.  The default branch returns true (conservative include-all).
func TestMCDC19b_EvalTVFUnary_Default(t *testing.T) {
	t.Parallel()

	// Use OpNot as the unknown op — it is not handled by evalTVFUnary.
	e := &parser.UnaryExpr{
		Op:   parser.OpNot,
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}

	result := evalTVFUnary(e, []functions.Value{}, []string{})
	if !result {
		t.Error("evalTVFUnary with unknown op should return true (conservative), got false")
	}
}

// ---------------------------------------------------------------------------
// MultiStmt buildResult — nil lastResult path
// ---------------------------------------------------------------------------

// TestMCDC19b_BuildResult_NilLastResult calls buildResult with nil as
// lastResult, verifying that the returned Result has the expected rowsAffected
// and a zero lastInsertID.
func TestMCDC19b_BuildResult_NilLastResult(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	m := &MultiStmt{conn: c}

	const totalRows int64 = 42
	result := m.buildResult(nil, totalRows)

	if result == nil {
		t.Fatal("buildResult returned nil")
	}

	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != totalRows {
		t.Errorf("rowsAffected = %d, want %d", rows, totalRows)
	}

	lid, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if lid != 0 {
		t.Errorf("lastInsertID = %d, want 0", lid)
	}
}

// TestMCDC19b_BuildResult_WithLastResult calls buildResult with a non-nil
// lastResult, verifying that its LastInsertId is propagated.
func TestMCDC19b_BuildResult_WithLastResult(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	m := &MultiStmt{conn: c}

	last := &Result{lastInsertID: 99, rowsAffected: 1}
	const totalRows int64 = 5

	result := m.buildResult(last, totalRows)
	if result == nil {
		t.Fatal("buildResult returned nil")
	}

	lid, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if lid != 99 {
		t.Errorf("lastInsertID = %d, want 99", lid)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != totalRows {
		t.Errorf("rowsAffected = %d, want %d", rows, totalRows)
	}
}

// Ensure the driver.Result interface is used (import guard).
var _ driver.Result = (*Result)(nil)

// Ensure schema package import is used.
var _ = (*schema.DatabaseRegistry)(nil)
