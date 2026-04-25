// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func TestParserMultiStatement(t *testing.T) {
	query := `INSERT INTO t1 VALUES(2); INSERT INTO t1 VALUES(3)`
	p := parser.NewParser(query)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	t.Logf("Parsed %d statements", len(stmts))
	for i, stmt := range stmts {
		t.Logf("  Statement %d: %T", i, stmt)
	}
	if len(stmts) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(stmts))
	}
}

func TestMultiStatementExec(t *testing.T) {
	db := multiStmtSetupDB(t)
	defer db.Close()

	multiStmtSeedRows(t, db)
	multiStmtAssertRows(t, db, 3)
}

func multiStmtSetupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return db
}

func multiStmtSeedRows(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE t1(x INT)"); err != nil {
		t.Fatalf("CREATE TABLE error: %v", err)
	}
	t.Log("CREATE TABLE succeeded")
	if _, err := db.Exec("INSERT INTO t1 VALUES(1)"); err != nil {
		t.Fatalf("INSERT 1 error: %v", err)
	}
	t.Log("INSERT 1 succeeded")
	result, err := db.Exec(`INSERT INTO t1 VALUES(2); INSERT INTO t1 VALUES(3)`)
	if err != nil {
		t.Fatalf("Multi-statement exec error: %v", err)
	}
	if result != nil {
		rows, _ := result.RowsAffected()
		t.Logf("Multi-stmt rows affected: %d", rows)
	}
}

func multiStmtAssertRows(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	rows, err := db.Query("SELECT x FROM t1 ORDER BY x")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	var results []int
	for rows.Next() {
		var x int
		rows.Scan(&x)
		results = append(results, x)
	}

	t.Logf("Results: %v", results)
	if len(results) != want {
		t.Errorf("Expected %d results (1,2,3), got %d: %v", want, len(results), results)
	}
}
