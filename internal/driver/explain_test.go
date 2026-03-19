// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"strings"
	"testing"
)

// createTestDB creates a temporary database file and returns an open connection.
// The caller should call the returned cleanup function when done.
func createTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	tmpfile, err := os.CreateTemp("", "anthony_explain_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	db, err := sql.Open(DriverName, tmpfile.Name())
	if err != nil {
		os.Remove(tmpfile.Name())
		t.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpfile.Name())
	}

	return db, cleanup
}

// explainCheckColumns verifies column names from an explain result set.
func explainCheckColumns(t *testing.T, rows *sql.Rows, expectedCols []string) {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	if len(cols) != len(expectedCols) {
		t.Errorf("Expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, col := range cols {
		if i < len(expectedCols) && col != expectedCols[i] {
			t.Errorf("Column %d: expected '%s', got '%s'", i, expectedCols[i], col)
		}
	}
}

// explainScanPlanRows scans all EXPLAIN QUERY PLAN rows and returns the detail strings.
func explainScanPlanRows(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	var details []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
	return details
}

// TestExplainQueryPlan tests EXPLAIN QUERY PLAN functionality end-to-end.
func TestExplainQueryPlan(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	explainCheckColumns(t, rows, []string{"id", "parent", "notused", "detail"})

	details := explainScanPlanRows(t, rows)
	if len(details) == 0 {
		t.Error("Expected at least one row in explain output")
	}

	foundUsers := false
	for _, d := range details {
		if strings.Contains(d, "users") {
			foundUsers = true
			break
		}
	}
	if !foundUsers {
		t.Error("Expected plan to mention 'users' table")
	}
}

// TestExplainQueryPlanWithWhere tests EXPLAIN QUERY PLAN with WHERE clause.
func TestExplainQueryPlanWithWhere(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	foundScanOrSearch := false
	for rows.Next() {
		var id, parent, notused int
		var detail string

		err = rows.Scan(&id, &parent, &notused, &detail)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Check that it mentions SCAN or SEARCH
		if strings.Contains(detail, "SCAN") || strings.Contains(detail, "SEARCH") {
			foundScanOrSearch = true
		}

		t.Logf("Plan: %s", detail)
	}

	if !foundScanOrSearch {
		t.Error("Expected plan to mention SCAN or SEARCH")
	}
}

// TestExplainQueryPlanWithJoin tests EXPLAIN QUERY PLAN with JOIN.
func TestExplainQueryPlanWithJoin(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	details := explainScanPlanRows(t, rows)
	for _, table := range []string{"users", "orders"} {
		found := false
		for _, d := range details {
			if strings.Contains(d, table) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected plan to mention '%s' table", table)
		}
	}
}

// scanExplainOpcodes scans EXPLAIN rows and returns a set of found opcodes.
func scanExplainOpcodes(t *testing.T, rows *sql.Rows) map[string]bool {
	t.Helper()
	found := make(map[string]bool)
	for rows.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode, p4, comment string
		if err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		found[opcode] = true
		t.Logf("Opcode %d: %s p1=%d p2=%d p3=%d p4=%s p5=%d", addr, opcode, p1, p2, p3, p4, p5)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
	return found
}

// TestExplainOpcodes tests basic EXPLAIN (shows VDBE opcodes).
func TestExplainOpcodes(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN SELECT * FROM items")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	defer rows.Close()

	explainCheckColumns(t, rows, []string{"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"})

	found := scanExplainOpcodes(t, rows)
	if len(found) == 0 {
		t.Error("Expected at least one row in explain output")
	}

	for _, name := range []string{"Init", "OpenRead", "Rewind", "ResultRow", "Halt"} {
		if !found[name] {
			t.Errorf("Expected to find %s opcode", name)
		}
	}
}

// TestExplainInsert tests EXPLAIN QUERY PLAN for INSERT.
func TestExplainInsert(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN INSERT INTO data (value) VALUES ('test')")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	foundInsert := false
	for rows.Next() {
		var id, parent, notused int
		var detail string

		err = rows.Scan(&id, &parent, &notused, &detail)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if strings.Contains(detail, "INSERT") {
			foundInsert = true
		}

		t.Logf("Plan: %s", detail)
	}

	if !foundInsert {
		t.Error("Expected plan to mention INSERT")
	}
}

// TestExplainUpdate tests EXPLAIN QUERY PLAN for UPDATE.
func TestExplainUpdate(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, status TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN UPDATE records SET status = 'active' WHERE id = 1")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	foundUpdate := false
	for rows.Next() {
		var id, parent, notused int
		var detail string

		err = rows.Scan(&id, &parent, &notused, &detail)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if strings.Contains(detail, "UPDATE") {
			foundUpdate = true
		}

		t.Logf("Plan: %s", detail)
	}

	if !foundUpdate {
		t.Error("Expected plan to mention UPDATE")
	}
}

// TestExplainDelete tests EXPLAIN QUERY PLAN for DELETE.
func TestExplainDelete(t *testing.T) {

	db, cleanup := createTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("EXPLAIN QUERY PLAN DELETE FROM logs WHERE id < 100")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	foundDelete := false
	for rows.Next() {
		var id, parent, notused int
		var detail string

		err = rows.Scan(&id, &parent, &notused, &detail)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if strings.Contains(detail, "DELETE") {
			foundDelete = true
		}

		t.Logf("Plan: %s", detail)
	}

	if !foundDelete {
		t.Error("Expected plan to mention DELETE")
	}
}
