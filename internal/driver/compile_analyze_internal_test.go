// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// openMemConn opens a fresh in-memory Conn for internal testing.
func openMemConn(t *testing.T) *Conn {
	t.Helper()
	d := &Driver{}
	raw, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("openMemConn: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	return raw.(*Conn)
}

// stmtFor builds a minimal Stmt backed by the given Conn.
func stmtFor(c *Conn) *Stmt {
	return &Stmt{conn: c, query: ""}
}

// ------------------------------------------------------------------ helpers

// execOnConn executes a SQL string directly on the connection (no mutex).
func execOnConn(t *testing.T, c *Conn, sql string) {
	t.Helper()
	if err := c.ExecDDL(sql); err != nil {
		t.Fatalf("execOnConn(%q): %v", sql, err)
	}
}

// ensureStat1Exists creates sqlite_stat1 on c if it doesn't already exist.
func ensureStat1Exists(t *testing.T, c *Conn) {
	t.Helper()
	if _, ok := c.schema.GetTable("sqlite_stat1"); !ok {
		execOnConn(t, c, "CREATE TABLE sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT)")
	}
}

// ------------------------------------------------------------------ tests

// TestCompileAnalyzeInternalEnsureStat1Error covers the error branch of
// ensureSqliteStat1Table: sqlite_stat1 is absent from the schema, and ExecDDL
// fails because the connection is closed.
func TestCompileAnalyzeInternalEnsureStat1Error(t *testing.T) {
	c := openMemConn(t)

	// Make sure sqlite_stat1 is NOT in the schema so the create path is taken.
	if _, ok := c.schema.GetTable("sqlite_stat1"); ok {
		t.Skip("sqlite_stat1 already exists; cannot test creation-error path")
	}

	// Closing the connection makes ExecDDL fail.
	c.closed = true

	s := stmtFor(c)
	err := s.ensureSqliteStat1Table()
	if err == nil {
		t.Error("ensureSqliteStat1Table: expected error when conn is closed, got nil")
	}
}

// TestCompileAnalyzeInternalCompileAnalyzeEnsureStat1Error covers the early-return
// error branch in compileAnalyze that is triggered when ensureSqliteStat1Table fails.
func TestCompileAnalyzeInternalCompileAnalyzeEnsureStat1Error(t *testing.T) {
	c := openMemConn(t)

	// sqlite_stat1 must NOT exist in schema.
	if _, ok := c.schema.GetTable("sqlite_stat1"); ok {
		t.Skip("sqlite_stat1 already exists; cannot test this branch")
	}

	// Close the connection so ExecDDL (called inside ensureSqliteStat1Table) fails.
	c.closed = true

	s := stmtFor(c)
	vm := s.newVDBE()
	analyzeStmt := &parser.AnalyzeStmt{}
	_, err := s.compileAnalyze(vm, analyzeStmt, nil)
	if err == nil {
		t.Error("compileAnalyze: expected error propagated from ensureSqliteStat1Table, got nil")
	}
}

// TestCompileAnalyzeInternalCollectStatsClearError covers the error branch in
// collectAndStoreStatistics where clearStatistics fails.
// clearStatistics runs "DELETE FROM sqlite_stat1"; it will fail when the
// connection is closed after sqlite_stat1 has been created in the schema.
func TestCompileAnalyzeInternalCollectStatsClearError(t *testing.T) {
	c := openMemConn(t)
	ensureStat1Exists(t, c)

	// Re-confirm stat1 is present so the DELETE path is taken, then break the conn.
	if _, ok := c.schema.GetTable("sqlite_stat1"); !ok {
		t.Fatal("sqlite_stat1 not in schema after ensureStat1Exists")
	}

	// Close the connection so ExecDML (DELETE FROM sqlite_stat1) fails.
	c.closed = true

	s := stmtFor(c)
	analyzeStmt := &parser.AnalyzeStmt{} // Name == "" → DELETE FROM sqlite_stat1
	err := s.collectAndStoreStatistics(analyzeStmt)
	if err == nil {
		t.Error("collectAndStoreStatistics: expected error from clearStatistics, got nil")
	}
}

// TestCompileAnalyzeInternalCollectStatsClearNamedError covers clearStatistics
// with a named table (DELETE FROM sqlite_stat1 WHERE tbl = ?).
func TestCompileAnalyzeInternalCollectStatsClearNamedError(t *testing.T) {
	c := openMemConn(t)
	ensureStat1Exists(t, c)

	c.closed = true

	s := stmtFor(c)
	analyzeStmt := &parser.AnalyzeStmt{Name: "mytable"}
	err := s.collectAndStoreStatistics(analyzeStmt)
	if err == nil {
		t.Error("collectAndStoreStatistics (named): expected error from clearStatistics, got nil")
	}
}

// TestCompileAnalyzeInternalAnalyzeTableInsertError covers the ExecDML INSERT
// error branch inside analyzeTable.  We set up sqlite_stat1 (so
// ensureSqliteStat1Table passes), insert rows into a user table, then drop
// sqlite_stat1 and call analyzeTable directly so that the INSERT fails.
func TestCompileAnalyzeInternalAnalyzeTableInsertError(t *testing.T) {
	c := openMemConn(t)

	// Create a real user table.
	execOnConn(t, c, "CREATE TABLE t_analyze_target (id INTEGER, val TEXT)")
	// sqlite_stat1 is needed for the countTableRows step but must be missing for INSERT.
	// Approach: create stat1 for counting, then drop it before INSERT.
	ensureStat1Exists(t, c)

	// Grab the schema table object for our user table.
	tbl, ok := c.schema.GetTable("t_analyze_target")
	if !ok {
		t.Fatal("t_analyze_target not in schema")
	}

	// Drop sqlite_stat1 so the INSERT into it fails.
	execOnConn(t, c, "DROP TABLE sqlite_stat1")
	// Remove from schema so the next ANALYZE doesn't think stat1 exists.
	c.schema.DropTable("sqlite_stat1")

	s := stmtFor(c)
	err := s.analyzeTable(tbl)
	if err == nil {
		t.Error("analyzeTable: expected error when sqlite_stat1 missing for INSERT, got nil")
	}
}

// TestCompileAnalyzeInternalCountTableRowsSuccess covers the normal (success)
// path of countTableRows, confirming it returns 0 for an empty table and a
// positive count for a populated table.
func TestCompileAnalyzeInternalCountTableRowsSuccess(t *testing.T) {
	c := openMemConn(t)
	ensureStat1Exists(t, c)

	execOnConn(t, c, "CREATE TABLE ctr_test (x INTEGER)")

	s := stmtFor(c)

	// Empty table → COUNT(*) = 0.
	n, err := s.countTableRows("ctr_test")
	if err != nil {
		t.Fatalf("countTableRows (empty): %v", err)
	}
	if n != 0 {
		t.Errorf("countTableRows (empty): got %d, want 0", n)
	}

	// Insert a row and recount.
	if _, err := c.ExecDML("INSERT INTO ctr_test VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	n, err = s.countTableRows("ctr_test")
	if err != nil {
		t.Fatalf("countTableRows (1 row): %v", err)
	}
	if n != 1 {
		t.Errorf("countTableRows (1 row): got %d, want 1", n)
	}
}

// TestCompileAnalyzeInternalCountTableRowsError covers the error branch of
// countTableRows where conn.Query fails (table does not exist).
func TestCompileAnalyzeInternalCountTableRowsError(t *testing.T) {
	c := openMemConn(t)

	s := stmtFor(c)
	_, err := s.countTableRows("no_such_table_xyz")
	if err == nil {
		t.Error("countTableRows: expected error for non-existent table, got nil")
	}
}

// TestCompileAnalyzeInternalEnsureStat1AlreadyExists covers the early-return
// (no-op) branch of ensureSqliteStat1Table when the table is already present.
func TestCompileAnalyzeInternalEnsureStat1AlreadyExists(t *testing.T) {
	c := openMemConn(t)
	ensureStat1Exists(t, c)

	s := stmtFor(c)
	if err := s.ensureSqliteStat1Table(); err != nil {
		t.Errorf("ensureSqliteStat1Table: expected nil (already exists), got %v", err)
	}
}

// TestCompileAnalyzeInternalCollectStatsAnalyzeTableError covers the
// analyzeTable error branch inside collectAndStoreStatistics.
// We create stat1 and a user table, then drop stat1 so the INSERT inside
// analyzeTable fails, while clearStatistics (DELETE) succeeds on a fresh stat1.
func TestCompileAnalyzeInternalCollectStatsAnalyzeTableError(t *testing.T) {
	c := openMemConn(t)

	// Create user table and sqlite_stat1.
	execOnConn(t, c, "CREATE TABLE tbl_for_err (a INTEGER)")
	ensureStat1Exists(t, c)

	// Run clearStatistics first to prime the path, then drop stat1 to break INSERT.
	s := stmtFor(c)
	// clearStatistics succeeds (stat1 exists, DELETE is a no-op on empty table).
	if _, err := c.ExecDML("DELETE FROM sqlite_stat1"); err != nil {
		t.Fatalf("pre-clear: %v", err)
	}

	// Drop stat1 so the subsequent INSERT (in analyzeTable) fails.
	execOnConn(t, c, "DROP TABLE sqlite_stat1")
	c.schema.DropTable("sqlite_stat1")

	// Re-add stat1 to schema (but not to btree) so ensureSqliteStat1Table skips creation.
	fakeStat1 := &schema.Table{Name: "sqlite_stat1"}
	c.schema.AddTableDirect(fakeStat1)

	analyzeStmt := &parser.AnalyzeStmt{Name: "tbl_for_err"}
	err := s.collectAndStoreStatistics(analyzeStmt)
	if err == nil {
		t.Error("collectAndStoreStatistics: expected error from analyzeTable, got nil")
	}
}

// TestCompileAnalyzeInternalAnalyzeTableCountRowsError covers the error branch
// in analyzeTable where countTableRows fails because the connection is closed
// before the SELECT COUNT(*) query can execute.
func TestCompileAnalyzeInternalAnalyzeTableCountRowsError(t *testing.T) {
	c := openMemConn(t)
	ensureStat1Exists(t, c)
	execOnConn(t, c, "CREATE TABLE ctr_err_tbl (id INTEGER)")

	tbl, ok := c.schema.GetTable("ctr_err_tbl")
	if !ok {
		t.Fatal("ctr_err_tbl not in schema")
	}

	// Close the connection so the SELECT COUNT(*) inside countTableRows fails.
	c.closed = true

	s := stmtFor(c)
	err := s.analyzeTable(tbl)
	if err == nil {
		t.Error("analyzeTable: expected error from countTableRows on closed conn, got nil")
	}
}

// TestCompileAnalyzeInternalAnalyzeTableIndexesInsertError covers the ExecDML
// INSERT error branch inside analyzeTableIndexes. The table and its index exist
// in the schema but sqlite_stat1 has been dropped from storage so the INSERT fails.
func TestCompileAnalyzeInternalAnalyzeTableIndexesInsertError(t *testing.T) {
	c := openMemConn(t)

	// Create a real table with an index so analyzeTableIndexes has work to do.
	execOnConn(t, c, "CREATE TABLE tidx_tbl (x INTEGER, y TEXT)")
	execOnConn(t, c, "CREATE INDEX tidx_tbl_x ON tidx_tbl (x)")

	tbl, ok := c.schema.GetTable("tidx_tbl")
	if !ok {
		t.Fatal("tidx_tbl not in schema")
	}

	// Do NOT create sqlite_stat1 – the INSERT inside analyzeTableIndexes will fail.
	s := stmtFor(c)
	err := s.analyzeTableIndexes(tbl, 0)
	if err == nil {
		t.Error("analyzeTableIndexes: expected INSERT error when sqlite_stat1 missing, got nil")
	}
}

// TestCompileAnalyzeInternalCompileAnalyzeCollectStatsError covers the second
// early-return branch in compileAnalyze where collectAndStoreStatistics fails.
// We arrange for stat1 to exist (so ensureSqliteStat1Table passes), add a ghost
// user table to the schema so collectAndStoreStatistics tries to analyze it, and
// then close the connection so the clearStatistics DELETE fails, propagating the
// error back through compileAnalyze.
func TestCompileAnalyzeInternalCompileAnalyzeCollectStatsError(t *testing.T) {
	c := openMemConn(t)

	// Ensure sqlite_stat1 exists so ensureSqliteStat1Table is a no-op.
	ensureStat1Exists(t, c)

	// Add a real user table to the schema.
	execOnConn(t, c, "CREATE TABLE real_user_tbl (v INTEGER)")

	// Close the connection so ExecDML (DELETE FROM sqlite_stat1 in clearStatistics) fails.
	c.closed = true

	s := stmtFor(c)
	vm := s.newVDBE()
	analyzeStmt := &parser.AnalyzeStmt{} // empty Name → targets all tables
	_, err := s.compileAnalyze(vm, analyzeStmt, nil)
	if err == nil {
		t.Error("compileAnalyze: expected error propagated from collectAndStoreStatistics, got nil")
	}
}
