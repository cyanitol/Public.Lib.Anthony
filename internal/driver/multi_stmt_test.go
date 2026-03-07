// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
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
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer db.Close()

	// Execute statements one at a time first
	_, err = db.Exec("CREATE TABLE t1(x INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE error: %v", err)
	}
	t.Log("CREATE TABLE succeeded")

	_, err = db.Exec("INSERT INTO t1 VALUES(1)")
	if err != nil {
		t.Fatalf("INSERT 1 error: %v", err)
	}
	t.Log("INSERT 1 succeeded")

	// Now try multi-statement
	result, err := db.Exec(`INSERT INTO t1 VALUES(2); INSERT INTO t1 VALUES(3)`)
	if err != nil {
		t.Fatalf("Multi-statement exec error: %v", err)
	}
	if result != nil {
		rows, _ := result.RowsAffected()
		t.Logf("Multi-stmt rows affected: %d", rows)
	}

	// Verify data
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
	if len(results) != 3 {
		t.Errorf("Expected 3 results (1,2,3), got %d: %v", len(results), results)
	}
}
