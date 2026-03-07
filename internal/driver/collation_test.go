// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"testing"
)

func TestCollationBinary(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with different cases
	testData := []string{"alice", "ALICE", "Bob", "bob", "Charlie"}
	for i, name := range testData {
		_, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}

	// Query with BINARY collation (case-sensitive, default)
	rows, err := db.Query("SELECT name FROM users ORDER BY name COLLATE BINARY")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	// BINARY collation should sort: ALICE < Bob < Charlie < alice < bob
	// (uppercase letters come before lowercase in ASCII)
	expected := []string{"ALICE", "Bob", "Charlie", "alice", "bob"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

func TestCollationNoCase(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with different cases
	testData := []string{"alice", "ALICE", "Bob", "bob", "Charlie"}
	for i, name := range testData {
		_, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}

	// Query with NOCASE collation (case-insensitive)
	rows, err := db.Query("SELECT name FROM users ORDER BY name COLLATE NOCASE")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	// NOCASE collation treats alice and ALICE as equal, should group them
	// Order: alice/ALICE (either first), Bob/bob (either first), Charlie
	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	// Check that all alice/ALICE come before Bob/bob
	var aliceCount, bobCount int
	for i, name := range results {
		if name == "alice" || name == "ALICE" {
			aliceCount++
			if i >= 2 {
				t.Errorf("alice/ALICE at position %d, should be in first 2", i)
			}
		} else if name == "Bob" || name == "bob" {
			bobCount++
			if i < 2 || i >= 4 {
				t.Errorf("Bob/bob at position %d, should be in positions 2-3", i)
			}
		} else if name == "Charlie" {
			if i != 4 {
				t.Errorf("Charlie at position %d, should be last (position 4)", i)
			}
		}
	}

	if aliceCount != 2 {
		t.Errorf("Expected 2 alice/ALICE entries, got %d", aliceCount)
	}
	if bobCount != 2 {
		t.Errorf("Expected 2 Bob/bob entries, got %d", bobCount)
	}
}

func TestCollationRTrim(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with trailing spaces
	testData := []struct {
		id    int
		value string
	}{
		{1, "apple"},
		{2, "apple  "},   // 2 trailing spaces
		{3, "banana   "}, // 3 trailing spaces
		{4, "banana"},
		{5, "cherry    "}, // 4 trailing spaces
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO data (id, value) VALUES (?, ?)", data.id, data.value)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query with RTRIM collation
	rows, err := db.Query("SELECT value FROM data ORDER BY value COLLATE RTRIM")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, value)
	}

	// With RTRIM, trailing spaces are ignored for comparison
	// So "apple" and "apple  " should be considered equal
	// Expected order: apple/apple  (2 entries), banana/banana   (2 entries), cherry    (1 entry)
	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	// Count entries for each base value
	appleCount := 0
	bananaCount := 0
	cherryCount := 0

	for i, value := range results {
		if value == "apple" || value == "apple  " {
			appleCount++
			if i >= 2 {
				t.Errorf("apple entry at position %d, should be in first 2", i)
			}
		} else if value == "banana" || value == "banana   " {
			bananaCount++
			if i < 2 || i >= 4 {
				t.Errorf("banana entry at position %d, should be in positions 2-3", i)
			}
		} else if value == "cherry    " {
			cherryCount++
			if i != 4 {
				t.Errorf("cherry at position %d, should be last (position 4)", i)
			}
		}
	}

	if appleCount != 2 {
		t.Errorf("Expected 2 apple entries, got %d", appleCount)
	}
	if bananaCount != 2 {
		t.Errorf("Expected 2 banana entries, got %d", bananaCount)
	}
	if cherryCount != 1 {
		t.Errorf("Expected 1 cherry entry, got %d", cherryCount)
	}
}

func TestColumnCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table with column-level COLLATE
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{"alice", "ALICE", "Bob", "bob"}
	for i, name := range testData {
		_, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}

	// Query - should use column's default collation (NOCASE)
	rows, err := db.Query("SELECT name FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(results))
	}

	// Should be case-insensitive sorted
	// Check that alice/ALICE come before Bob/bob
	for i := 0; i < 2; i++ {
		if results[i] != "alice" && results[i] != "ALICE" {
			t.Errorf("Expected alice/ALICE at position %d, got %s", i, results[i])
		}
	}
	for i := 2; i < 4; i++ {
		if results[i] != "Bob" && results[i] != "bob" {
			t.Errorf("Expected Bob/bob at position %d, got %s", i, results[i])
		}
	}
}

func TestCollationInWhereClause(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'JOHN'), (3, 'Jane')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with NOCASE in WHERE clause should match both 'John' and 'JOHN'
	rows, err := db.Query("SELECT name FROM users WHERE name COLLATE NOCASE = 'john'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		if name != "John" && name != "JOHN" {
			t.Errorf("Unexpected result: %s", name)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 results with NOCASE, got %d", count)
	}
}

func TestCollationDescending(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{"alice", "Bob", "Charlie"}
	for i, name := range testData {
		_, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}

	// Query with NOCASE DESC
	rows, err := db.Query("SELECT name FROM users ORDER BY name COLLATE NOCASE DESC")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	// Should be reverse alphabetical order (case-insensitive)
	expected := []string{"Charlie", "Bob", "alice"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

func TestMultipleCollationsInOrderBy(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, lastname TEXT, firstname TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []struct {
		id        int
		lastname  string
		firstname string
	}{
		{1, "Smith", "alice"},
		{2, "SMITH", "Bob"},
		{3, "jones", "Charlie"},
		{4, "Jones", "alice"},
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO users (id, lastname, firstname) VALUES (?, ?, ?)",
			data.id, data.lastname, data.firstname)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Order by lastname (NOCASE), then firstname (BINARY)
	rows, err := db.Query("SELECT lastname, firstname FROM users ORDER BY lastname COLLATE NOCASE, firstname COLLATE BINARY")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		lastname  string
		firstname string
	}

	for rows.Next() {
		var lastname, firstname string
		if err := rows.Scan(&lastname, &firstname); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, struct {
			lastname  string
			firstname string
		}{lastname, firstname})
	}

	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(results))
	}

	// jones/Jones should come before Smith/SMITH (case-insensitive)
	// Within jones/Jones, alice comes before Charlie (case-sensitive BINARY)
	// Within Smith/SMITH, Bob comes before alice (case-sensitive BINARY)
	expectedOrder := []struct {
		lastname  string
		firstname string
	}{
		{"jones", "Charlie"}, // jones group, Charlie < alice (uppercase C < lowercase a)
		{"Jones", "alice"},   // jones group
		{"SMITH", "Bob"},     // Smith group, Bob < alice (uppercase B < lowercase a)
		{"Smith", "alice"},   // Smith group
	}

	for i, exp := range expectedOrder {
		if results[i].lastname != exp.lastname || results[i].firstname != exp.firstname {
			t.Errorf("Result[%d]: expected (%q, %q), got (%q, %q)",
				i, exp.lastname, exp.firstname, results[i].lastname, results[i].firstname)
		}
	}
}

// setupTestDB creates an in-memory test database
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}
