// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// multi_stmt_coverage_test.go exercises MultiStmt.Exec (line 44) and
// MultiStmt.Query (line 163) via the legacy database/sql/driver.Stmt
// interface. These methods delegate to ExecContext / QueryContext
// respectively, but the Exec and Query wrapper lines themselves must be
// entered to be counted as covered.
//
// Strategy: obtain a driver.Conn via Driver.Open, call Prepare with a
// multi-statement SQL string (which returns a *MultiStmt as driver.Stmt),
// then call Exec and Query directly on that driver.Stmt value.

import (
	"database/sql/driver"
	"testing"

	internaldriver "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// mscOpenConn opens a raw driver.Conn for multi_stmt coverage tests.
func mscOpenConn(t *testing.T) driver.Conn {
	t.Helper()
	d := &internaldriver.Driver{}
	dbFile := t.TempDir() + "/multi_stmt_cov.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("mscOpenConn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// mscPrepare prepares a SQL string and returns the driver.Stmt.
func mscPrepare(t *testing.T, conn driver.Conn, sql string) driver.Stmt {
	t.Helper()
	stmt, err := conn.Prepare(sql)
	if err != nil {
		t.Fatalf("mscPrepare %q: %v", sql, err)
	}
	t.Cleanup(func() { stmt.Close() })
	return stmt
}

// mscExecSetup creates the schema needed for multi-stmt exec tests.
func mscExecSetup(t *testing.T, conn driver.Conn, ddl string) {
	t.Helper()
	stmt, err := conn.Prepare(ddl)
	if err != nil {
		t.Fatalf("mscExecSetup Prepare %q: %v", ddl, err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec([]driver.Value{}); err != nil {
		t.Fatalf("mscExecSetup Exec %q: %v", ddl, err)
	}
}

// TestMultiStmtCoverage_Exec calls MultiStmt.Exec([]driver.Value{}) directly
// via the driver.Stmt interface. This covers multi_stmt.go line 44.
func TestMultiStmtCoverage_Exec(t *testing.T) {
	conn := mscOpenConn(t)

	// Create table via a single-statement prepare so we can later use
	// multi-statement INSERT.
	mscExecSetup(t, conn, "CREATE TABLE msc_exec(x INTEGER)")

	// Prepare a multi-statement INSERT — Prepare returns a *MultiStmt.
	stmt := mscPrepare(t, conn, "INSERT INTO msc_exec VALUES(1); INSERT INTO msc_exec VALUES(2)")

	// Call the legacy Exec method directly (line 44 in multi_stmt.go).
	result, err := stmt.Exec([]driver.Value{})
	if err != nil {
		t.Fatalf("MultiStmt.Exec: %v", err)
	}
	if result == nil {
		t.Fatal("MultiStmt.Exec returned nil result")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != 2 {
		t.Errorf("want 2 rows affected, got %d", rows)
	}
}

// TestMultiStmtCoverage_ExecMultiple calls MultiStmt.Exec with three
// statements to exercise the executeAllStmts loop path via the legacy method.
func TestMultiStmtCoverage_ExecMultiple(t *testing.T) {
	conn := mscOpenConn(t)

	mscExecSetup(t, conn, "CREATE TABLE msc_multi(n INTEGER)")

	stmt := mscPrepare(t, conn,
		"INSERT INTO msc_multi VALUES(10); INSERT INTO msc_multi VALUES(20); INSERT INTO msc_multi VALUES(30)")

	result, err := stmt.Exec([]driver.Value{})
	if err != nil {
		t.Fatalf("MultiStmt.Exec (3 stmts): %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if rows != 3 {
		t.Errorf("want 3 rows affected, got %d", rows)
	}
}

// TestMultiStmtCoverage_Query calls MultiStmt.Query([]driver.Value{}) directly
// via the driver.Stmt interface. This covers multi_stmt.go line 163.
// The method always returns (nil, driver.ErrSkip).
func TestMultiStmtCoverage_Query(t *testing.T) {
	conn := mscOpenConn(t)

	mscExecSetup(t, conn, "CREATE TABLE msc_query(v INTEGER)")
	mscExecSetup(t, conn, "INSERT INTO msc_query VALUES(42)")

	// Prepare a multi-statement SELECT — returns a *MultiStmt.
	stmt := mscPrepare(t, conn, "SELECT v FROM msc_query; SELECT v FROM msc_query")

	// Call the legacy Query method directly (line 163 in multi_stmt.go).
	// It always returns driver.ErrSkip.
	rows, err := stmt.Query([]driver.Value{})
	if rows != nil {
		rows.Close()
		t.Error("MultiStmt.Query: expected nil rows")
	}
	if err != driver.ErrSkip {
		t.Errorf("MultiStmt.Query: want driver.ErrSkip, got %v", err)
	}
}

// TestMultiStmtCoverage_ExecOnClosed verifies that calling Exec on a closed
// MultiStmt returns an error (exercises checkClosed fast path via Exec).
func TestMultiStmtCoverage_ExecOnClosed(t *testing.T) {
	conn := mscOpenConn(t)

	mscExecSetup(t, conn, "CREATE TABLE msc_closed(x INTEGER)")

	stmt, err := (&internaldriver.Driver{}).Open(t.TempDir() + "/closed.db")
	if err != nil {
		t.Fatalf("open second conn: %v", err)
	}
	defer stmt.Close()

	multiStmt, err := stmt.Prepare("INSERT INTO msc_closed VALUES(1); INSERT INTO msc_closed VALUES(2)")
	if err != nil {
		t.Fatalf("Prepare multi-stmt on second conn: %v", err)
	}

	// Close the stmt before calling Exec.
	if err := multiStmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Now Exec should fail (ErrBadConn from checkClosed).
	_, err = multiStmt.Exec([]driver.Value{})
	if err == nil {
		t.Error("expected error from Exec on closed MultiStmt, got nil")
	}
}
