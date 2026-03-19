// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// collInsertNames inserts name data into a users table with sequential IDs.
func collInsertNames(t *testing.T, db *sql.DB, names []string) {
	t.Helper()
	for i, name := range names {
		_, err := db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}
}

// collQueryNames executes a query and returns all string results.
func collQueryNames(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
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
	return results
}

// collAssertResults checks that results match expected values exactly.
func collAssertResults(t *testing.T, results, expected []string) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

func TestCollationBinary(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	collInsertNames(t, db, []string{"alice", "ALICE", "Bob", "bob", "Charlie"})
	results := collQueryNames(t, db, "SELECT name FROM users ORDER BY name COLLATE BINARY")
	collAssertResults(t, results, []string{"ALICE", "Bob", "Charlie", "alice", "bob"})
}

// collNoCaseGroupOf returns the group name for NOCASE ordering.
func collNoCaseGroupOf(name string) string {
	groups := map[string]string{
		"alice": "alice", "ALICE": "alice",
		"Bob": "bob", "bob": "bob",
		"Charlie": "charlie",
	}
	return groups[name]
}

// collNoCaseCheckPosition validates that a name is in the correct position for NOCASE ordering.
func collNoCaseCheckPosition(t *testing.T, name string, pos int) {
	t.Helper()
	type posRange struct{ min, max int }
	ranges := map[string]posRange{
		"alice":   {0, 1},
		"bob":     {2, 3},
		"charlie": {4, 4},
	}
	group := collNoCaseGroupOf(name)
	r, ok := ranges[group]
	if ok && (pos < r.min || pos > r.max) {
		t.Errorf("%s at position %d, should be in [%d,%d]", name, pos, r.min, r.max)
	}
}

func TestCollationNoCase(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	collInsertNames(t, db, []string{"alice", "ALICE", "Bob", "bob", "Charlie"})
	results := collQueryNames(t, db, "SELECT name FROM users ORDER BY name COLLATE NOCASE")

	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	for i, name := range results {
		collNoCaseCheckPosition(t, name, i)
	}

	collNoCaseVerifyCounts(t, results)
}

// collNoCaseVerifyCounts checks that the expected number of entries per group exist.
func collNoCaseVerifyCounts(t *testing.T, results []string) {
	t.Helper()
	counts := make(map[string]int)
	for _, name := range results {
		counts[collNoCaseGroupOf(name)]++
	}
	if counts["alice"] != 2 {
		t.Errorf("Expected 2 alice/ALICE entries, got %d", counts["alice"])
	}
	if counts["bob"] != 2 {
		t.Errorf("Expected 2 Bob/bob entries, got %d", counts["bob"])
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

	// Verify grouped ordering with prefix-specific helpers
	collRtrimVerifyGroupedOrder(t, results)
}

// collRtrimVerifyGroupedOrder verifies that results are properly grouped by base value
func collRtrimVerifyGroupedOrder(t *testing.T, results []string) {
	t.Helper()

	counts := collRtrimCountGroups(results)

	collRtrimVerifyCount(t, counts["apple"], 2, "apple")
	collRtrimVerifyCount(t, counts["banana"], 2, "banana")
	collRtrimVerifyCount(t, counts["cherry"], 1, "cherry")

	collRtrimVerifyPositions(t, results)
}

// collRtrimCountGroups counts entries by base value
func collRtrimCountGroups(results []string) map[string]int {
	counts := make(map[string]int)
	for _, value := range results {
		if value == "apple" || value == "apple  " {
			counts["apple"]++
		} else if value == "banana" || value == "banana   " {
			counts["banana"]++
		} else if value == "cherry    " {
			counts["cherry"]++
		}
	}
	return counts
}

// collRtrimVerifyCount checks if count matches expected value
func collRtrimVerifyCount(t *testing.T, got, want int, label string) {
	t.Helper()
	if got != want {
		t.Errorf("Expected %d %s entries, got %d", want, label, got)
	}
}

// collRtrimVerifyPositions checks that each value is in its expected position range
func collRtrimVerifyPositions(t *testing.T, results []string) {
	t.Helper()
	for i, value := range results {
		collRtrimCheckPosition(t, value, i)
	}
}

// collRtrimGroupOf returns the base group name for RTRIM ordering.
func collRtrimGroupOf(value string) string {
	groups := map[string]string{
		"apple": "apple", "apple  ": "apple",
		"banana": "banana", "banana   ": "banana",
		"cherry    ": "cherry",
	}
	return groups[value]
}

// collRtrimCheckPosition validates that a value is in the correct position
func collRtrimCheckPosition(t *testing.T, value string, pos int) {
	t.Helper()
	type posRange struct{ min, max int }
	ranges := map[string]posRange{
		"apple":  {0, 1},
		"banana": {2, 3},
		"cherry": {4, 4},
	}
	group := collRtrimGroupOf(value)
	r, ok := ranges[group]
	if ok && (pos < r.min || pos > r.max) {
		t.Errorf("%s entry at position %d, should be in [%d,%d]", group, pos, r.min, r.max)
	}
}

// collAssertGroup checks that results in [start, end) belong to the allowedSet.
func collAssertGroup(t *testing.T, results []string, start, end int, allowedSet []string, label string) {
	t.Helper()
	allowed := make(map[string]bool)
	for _, s := range allowedSet {
		allowed[s] = true
	}
	for i := start; i < end; i++ {
		if !allowed[results[i]] {
			t.Errorf("Expected %s at position %d, got %s", label, i, results[i])
		}
	}
}

func TestColumnCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	collInsertNames(t, db, []string{"alice", "ALICE", "Bob", "bob"})
	results := collQueryNames(t, db, "SELECT name FROM users ORDER BY name")

	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(results))
	}

	collAssertGroup(t, results, 0, 2, []string{"alice", "ALICE"}, "alice/ALICE")
	collAssertGroup(t, results, 2, 4, []string{"Bob", "bob"}, "Bob/bob")
}

func TestCollationInWhereClause(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'JOHN'), (3, 'Jane')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	results := collQueryNames(t, db, "SELECT name FROM users WHERE name COLLATE NOCASE = 'john'")
	if len(results) != 2 {
		t.Errorf("Expected 2 results with NOCASE, got %d", len(results))
	}
	for _, name := range results {
		if name != "John" && name != "JOHN" {
			t.Errorf("Unexpected result: %s", name)
		}
	}
}

func TestCollationDescending(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	collInsertNames(t, db, []string{"alice", "Bob", "Charlie"})
	results := collQueryNames(t, db, "SELECT name FROM users ORDER BY name COLLATE NOCASE DESC")
	collAssertResults(t, results, []string{"Charlie", "Bob", "alice"})
}

// collNamePair is a helper type for multi-column collation results.
type collNamePair struct {
	lastname  string
	firstname string
}

// collQueryNamePairs queries two-column results.
func collQueryNamePairs(t *testing.T, db *sql.DB, query string) []collNamePair {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []collNamePair
	for rows.Next() {
		var p collNamePair
		if err := rows.Scan(&p.lastname, &p.firstname); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, p)
	}
	return results
}

func TestMultipleCollationsInOrderBy(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, lastname TEXT, firstname TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	insertData := []collNamePair{
		{"Smith", "alice"}, {"SMITH", "Bob"}, {"jones", "Charlie"}, {"Jones", "alice"},
	}
	for i, d := range insertData {
		_, err := db.Exec("INSERT INTO users (id, lastname, firstname) VALUES (?, ?, ?)", i+1, d.lastname, d.firstname)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	results := collQueryNamePairs(t, db, "SELECT lastname, firstname FROM users ORDER BY lastname COLLATE NOCASE, firstname COLLATE BINARY")

	expected := []collNamePair{
		{"jones", "Charlie"}, {"Jones", "alice"}, {"SMITH", "Bob"}, {"Smith", "alice"},
	}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %v, got %v", i, exp, results[i])
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
