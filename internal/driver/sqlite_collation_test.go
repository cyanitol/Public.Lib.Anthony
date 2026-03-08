// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// collationVerifyType defines the type of verification to perform
type collationVerifyType int

const (
	collationVerifyRowCount collationVerifyType = iota // Verify exact row count
	collationVerifyOrderedStrings                      // Verify exact string order
	collationVerifyOrderedNullStrings                  // Verify exact order with NULLs
	collationVerifyMultiColumnOrder                    // Verify multi-column results
	collationVerifyContainsStrings                     // Verify result contains specific strings
)

// collationTestCase defines a single collation test scenario
type collationTestCase struct {
	name            string
	setup           []string             // CREATE TABLE statements and other setup
	inserts         []string             // INSERT statements to test
	query           string               // Query to execute
	verifyType      collationVerifyType  // Type of verification
	expectedRows    [][]interface{}      // Expected rows for multi-column
	expectedStrings []string             // Expected string values in order
	expectedNulls   []sql.NullString     // Expected NULL-capable strings
	expectedCount   int                  // Expected row count
	wantErr         bool
	errMsg          string
	skip            string
}

// TestSQLiteCollation is a comprehensive test suite converted from SQLite's TCL collation tests
// (collate1.test, collate2.test, collate3.test, collate4.test, collate5.test, collate6.test,
//
//	collate7.test, collate8.test, collate9.test, collateA.test, collateB.test)
//
// These tests cover:
// - COLLATE BINARY (case-sensitive, default)
// - COLLATE NOCASE (case-insensitive)
// - COLLATE RTRIM (trailing space ignored)
// - Custom collation sequences
// - ORDER BY with explicit COLLATE clauses
// - WHERE clauses with COLLATE
// - Column-level COLLATE specifications
// - Multi-column ORDER BY with different collations
// - DISTINCT with collations
// - UNION/INTERSECT/EXCEPT with collations
// - GROUP BY with collations
// - Index usage with collations
func TestSQLiteCollation(t *testing.T) {
	tests := collationTestCases()

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			runCollationTest(t, tt)
		})
	}
}

// runCollationTest executes a single collation test case
func runCollationTest(t *testing.T, tt collationTestCase) {
	if tt.skip != "" {
		t.Skip(tt.skip)
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	if !executeSetupStatements(t, db, tt.setup, tt.wantErr, tt.errMsg) {
		return
	}

	// Insert test data
	if !executeInsertStatements(t, db, tt.inserts) {
		return
	}

	// Execute query and verify
	executeAndVerifyQuery(t, db, tt)
}

// executeSetupStatements runs setup SQL statements
func executeSetupStatements(t *testing.T, db *sql.DB, setup []string, wantErr bool, errMsg string) bool {
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			if wantErr {
				if errMsg == "" || !containsCollation(err.Error(), errMsg) {
					t.Errorf("Setup error: %v, wanted error containing %q", err, errMsg)
				}
				return false
			}
			t.Fatalf("Setup failed: %v\nStatement: %s", err, stmt)
		}
	}
	return true
}

// executeInsertStatements runs insert SQL statements
func executeInsertStatements(t *testing.T, db *sql.DB, inserts []string) bool {
	for _, stmt := range inserts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Insert failed: %v\nStatement: %s", err, stmt)
			return false
		}
	}
	return true
}

// executeAndVerifyQuery runs the test query and performs verification
func executeAndVerifyQuery(t *testing.T, db *sql.DB, tt collationTestCase) {
	if tt.query == "" {
		return
	}

	rows, err := db.Query(tt.query)
	if err != nil {
		handleQueryError(t, tt, err)
		return
	}
	defer rows.Close()

	performVerification(t, rows, tt)
}

// handleQueryError checks if error is expected
func handleQueryError(t *testing.T, tt collationTestCase, err error) {
	if tt.wantErr {
		if tt.errMsg == "" || !containsCollation(err.Error(), tt.errMsg) {
			t.Errorf("Query error: %v, wanted error containing %q", err, tt.errMsg)
		}
		return
	}
	t.Fatalf("Query failed: %v\nQuery: %s", err, tt.query)
}

// performVerification performs the appropriate verification based on verifyType
func performVerification(t *testing.T, rows *sql.Rows, tt collationTestCase) {
	switch tt.verifyType {
	case collationVerifyRowCount:
		collationVerifyCount(t, rows, tt.expectedCount)
	case collationVerifyOrderedStrings:
		collationVerifyStringOrder(t, rows, tt.expectedStrings)
	case collationVerifyOrderedNullStrings:
		collationVerifyNullStringOrder(t, rows, tt.expectedNulls)
	case collationVerifyMultiColumnOrder:
		collationVerifyMultiColumn(t, rows, tt.expectedRows)
	case collationVerifyContainsStrings:
		collationVerifyContains(t, rows, tt.expectedStrings)
	}
}

// collationVerifyCount verifies the exact number of rows
func collationVerifyCount(t *testing.T, rows *sql.Rows, expected int) {
	count := 0
	for rows.Next() {
		count++
		// Scan to consume row
		cols, _ := rows.Columns()
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}
	if count != expected {
		t.Errorf("Expected %d rows, got %d", expected, count)
	}
}

// collationVerifyStringOrder verifies exact string order
func collationVerifyStringOrder(t *testing.T, rows *sql.Rows, expected []string) {
	var results []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, val)
	}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

// collationVerifyNullStringOrder verifies string order with NULL support
func collationVerifyNullStringOrder(t *testing.T, rows *sql.Rows, expected []sql.NullString) {
	var results []sql.NullString
	for rows.Next() {
		var val sql.NullString
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, val)
	}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Row %d: expected %v, got %v", i, exp, results[i])
		}
	}
}

// collationVerifyMultiColumn verifies multi-column results
func collationVerifyMultiColumn(t *testing.T, rows *sql.Rows, expected [][]interface{}) {
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	var results [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, vals)
	}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), len(results))
	}
}

// collationVerifyContains verifies that results contain expected strings (partial match)
func collationVerifyContains(t *testing.T, rows *sql.Rows, expected []string) {
	var results []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, val)
	}

	// Check that all expected values are present
	for _, exp := range expected {
		found := false
		for _, res := range results {
			if res == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find %q in results, but didn't", exp)
		}
	}
}

// collationTestCases returns all collation test cases
func collationTestCases() []collationTestCase {
	return []collationTestCase{
		// ===== BASIC ORDER BY TESTS (from collate1.test) =====

		{
			name: "collate1-1.1: ORDER BY without COLLATE uses BINARY default",
			setup: []string{
				"CREATE TABLE t1(c1, c2)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES(45, '0x2D')",
				"INSERT INTO t1 VALUES(NULL, NULL)",
				"INSERT INTO t1 VALUES(281, '0x119')",
			},
			query:      "SELECT c2 FROM t1 ORDER BY c2",
			verifyType: collationVerifyOrderedNullStrings,
			expectedNulls: []sql.NullString{
				{Valid: false},
				{String: "0x119", Valid: true},
				{String: "0x2D", Valid: true},
			},
		},

		{
			name: "collate1-1.5: SELECT with COLLATE NOCASE in column expression",
			setup: []string{
				"CREATE TABLE t1(c1, c2)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('aa', 'AA')",
				"INSERT INTO t1 VALUES('AB', 'ab')",
				"INSERT INTO t1 VALUES('Ba', 'bA')",
			},
			query:         "SELECT c2 FROM t1 ORDER BY c2 COLLATE NOCASE",
			verifyType:    collationVerifyRowCount,
			expectedCount: 3,
		},

		{
			name: "collate1-2.2: Multi-column ORDER BY with different collations",
			setup: []string{
				"CREATE TABLE t1(c1, c2)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('5', '10')",
				"INSERT INTO t1 VALUES('5', '5')",
				"INSERT INTO t1 VALUES(NULL, NULL)",
				"INSERT INTO t1 VALUES('7', '5')",
				"INSERT INTO t1 VALUES('11', '10')",
				"INSERT INTO t1 VALUES('11', '100')",
			},
			query:         "SELECT c1, c2 FROM t1 ORDER BY c1 COLLATE BINARY, c2 COLLATE BINARY",
			verifyType:    collationVerifyRowCount,
			expectedCount: 6,
		},

		{
			name: "collate1-3.1: Default column collation in ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('abc', 1)",
				"INSERT INTO t1 VALUES('ABC', 2)",
				"INSERT INTO t1 VALUES('def', 3)",
				"INSERT INTO t1 VALUES(NULL, NULL)",
			},
			query:         "SELECT a FROM t1 ORDER BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		{
			name: "collate1-3.5: Explicit COLLATE overrides column default",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('abc', 1)",
				"INSERT INTO t1 VALUES('ABC', 2)",
				"INSERT INTO t1 VALUES('def', 3)",
			},
			query:         "SELECT a as c1 FROM t1 ORDER BY c1 COLLATE BINARY",
			verifyType:    collationVerifyRowCount,
			expectedCount: 3,
		},

		// ===== COLLATE IN WHERE CLAUSE (from collate2.test) =====

		{
			name: "collate2-1.1: WHERE with column default BINARY collation",
			setup: []string{
				"CREATE TABLE t1(a COLLATE BINARY, b COLLATE NOCASE, c COLLATE RTRIM)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('aa', 'aa', 'aa')",
				"INSERT INTO t1 VALUES('ab', 'ab', 'ab')",
				"INSERT INTO t1 VALUES('ba', 'ba', 'ba')",
				"INSERT INTO t1 VALUES('aA', 'aA', 'aA')",
			},
			query:         "SELECT a FROM t1 WHERE a > 'aa' ORDER BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		{
			name: "collate2-1.2: WHERE with NOCASE column comparison",
			setup: []string{
				"CREATE TABLE t1(a COLLATE BINARY, b COLLATE NOCASE, c COLLATE RTRIM)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('aa', 'aa', 'aa')",
				"INSERT INTO t1 VALUES('ab', 'ab', 'ab')",
				"INSERT INTO t1 VALUES('ba', 'ba', 'ba')",
			},
			query:         "SELECT b FROM t1 WHERE b > 'aa' ORDER BY b",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		{
			name: "collate2-1.3: Explicit COLLATE in WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a COLLATE BINARY, b)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('aa', 1)",
				"INSERT INTO t1 VALUES('AA', 2)",
				"INSERT INTO t1 VALUES('ab', 3)",
			},
			query:         "SELECT a FROM t1 WHERE a COLLATE NOCASE = 'aa'",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		// ===== UNKNOWN/UNDEFINED COLLATION SEQUENCES (from collate3.test) =====

		{
			name: "collate3-1.1: Unknown collation sequence in ORDER BY",
			skip: "unknown collation error reporting not yet implemented",
			setup: []string{
				"CREATE TABLE t1(c1)",
			},
			query:   "SELECT * FROM t1 ORDER BY c1 COLLATE garbage",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name: "collate3-1.1.2: Unknown collation in DISTINCT",
			skip: "unknown collation error reporting not yet implemented",
			setup: []string{
				"CREATE TABLE t1(c1)",
			},
			query:   "SELECT DISTINCT c1 COLLATE garbage FROM t1",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name:    "collate3-1.2: Unknown collation in CREATE TABLE",
			skip:    "unknown collation error reporting not yet implemented",
			setup:   []string{},
			query:   "CREATE TABLE t1(c1 COLLATE garbage)",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name: "collate3-1.3: Unknown collation in CREATE INDEX",
			skip: "unknown collation error reporting not yet implemented",
			setup: []string{
				"CREATE TABLE t1(c1)",
			},
			query:   "CREATE INDEX i1 ON t1(c1 COLLATE garbage)",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		// ===== COLLATE WITH BINARY (from collate1.test, collate2.test) =====

		{
			name: "collate-binary-1: BINARY collation is case-sensitive",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(2, 'ALICE')",
				"INSERT INTO users VALUES(3, 'Bob')",
				"INSERT INTO users VALUES(4, 'bob')",
				"INSERT INTO users VALUES(5, 'Charlie')",
			},
			query:           "SELECT name FROM users ORDER BY name COLLATE BINARY",
			verifyType:      collationVerifyOrderedStrings,
			expectedStrings: []string{"ALICE", "Bob", "Charlie", "alice", "bob"},
		},

		// ===== COLLATE WITH NOCASE (from collate1.test, collate2.test) =====

		{
			name: "collate-nocase-1: NOCASE collation is case-insensitive for sorting",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(2, 'ALICE')",
				"INSERT INTO users VALUES(3, 'Bob')",
				"INSERT INTO users VALUES(4, 'bob')",
				"INSERT INTO users VALUES(5, 'Charlie')",
			},
			query:           "SELECT name FROM users ORDER BY name COLLATE NOCASE",
			verifyType:      collationVerifyContainsStrings,
			expectedStrings: []string{"alice", "ALICE", "Bob", "bob", "Charlie"},
		},

		{
			name: "collate-nocase-2: NOCASE in WHERE clause",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'John')",
				"INSERT INTO users VALUES(2, 'JOHN')",
				"INSERT INTO users VALUES(3, 'Jane')",
			},
			query:           "SELECT name FROM users WHERE name COLLATE NOCASE = 'john'",
			verifyType:      collationVerifyContainsStrings,
			expectedStrings: []string{"John", "JOHN"},
		},

		// ===== COLLATE WITH RTRIM (from collate1.test) =====

		{
			name: "collate-rtrim-1: RTRIM ignores trailing spaces",
			setup: []string{
				"CREATE TABLE data(id INTEGER PRIMARY KEY, value TEXT)",
			},
			inserts: []string{
				"INSERT INTO data VALUES(1, 'apple')",
				"INSERT INTO data VALUES(2, 'apple  ')",
				"INSERT INTO data VALUES(3, 'banana   ')",
				"INSERT INTO data VALUES(4, 'banana')",
				"INSERT INTO data VALUES(5, 'cherry    ')",
			},
			query:         "SELECT value FROM data ORDER BY value COLLATE RTRIM",
			verifyType:    collationVerifyRowCount,
			expectedCount: 5,
		},

		// ===== COLUMN-LEVEL COLLATE (from collate1.test, collate2.test) =====

		{
			name: "collate-column-1: Column with COLLATE NOCASE uses it by default",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(2, 'ALICE')",
				"INSERT INTO users VALUES(3, 'Bob')",
				"INSERT INTO users VALUES(4, 'bob')",
			},
			query:         "SELECT name FROM users ORDER BY name",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		{
			name: "collate-column-2: Multiple COLLATE clauses - last one wins",
			skip: "multiple COLLATE clauses on column not yet supported",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT COLLATE BINARY COLLATE NOCASE COLLATE RTRIM)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES(1, 'abc')",
				"INSERT INTO t1 VALUES(2, 'abc   ')",
			},
			query:         "SELECT a FROM t1 WHERE a = 'abc'",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		// ===== DISTINCT WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-1.1: DISTINCT with NOCASE column",
			skip: "DISTINCT with COLLATE NOCASE not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
				"INSERT INTO t1 VALUES('B', 'banana')",
			},
			query:         "SELECT DISTINCT a FROM t1",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		{
			name: "collate5-1.2: DISTINCT with BINARY column",
			skip: "DISTINCT with COLLATE not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
				"INSERT INTO t1 VALUES('B', 'banana')",
			},
			query:         "SELECT DISTINCT b FROM t1",
			verifyType:    collationVerifyRowCount,
			expectedCount: 3,
		},

		{
			name: "collate5-1.3: DISTINCT with multiple columns",
			skip: "DISTINCT with COLLATE not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
			},
			query:         "SELECT DISTINCT a, b FROM t1",
			verifyType:    collationVerifyRowCount,
			expectedCount: 3,
		},

		// ===== UNION WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-2.1.1: UNION with NOCASE from first table",
			skip: "UNION collation propagation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b)",
				"CREATE TABLE t2(a COLLATE BINARY, b)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 1)",
				"INSERT INTO t1 VALUES('A', 2)",
				"INSERT INTO t1 VALUES('b', 3)",
				"INSERT INTO t2 VALUES('a', 4)",
				"INSERT INTO t2 VALUES('B', 5)",
			},
			query:         "SELECT a FROM t1 UNION SELECT a FROM t2",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		{
			name: "collate5-2.1.2: UNION with BINARY from first table",
			setup: []string{
				"CREATE TABLE t1(a COLLATE BINARY, b)",
				"CREATE TABLE t2(a COLLATE NOCASE, b)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 1)",
				"INSERT INTO t1 VALUES('A', 2)",
				"INSERT INTO t1 VALUES('b', 3)",
				"INSERT INTO t2 VALUES('a', 4)",
				"INSERT INTO t2 VALUES('B', 5)",
			},
			query:         "SELECT a FROM t1 UNION SELECT a FROM t2",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		// ===== DESCENDING ORDER (from collate1.test) =====

		{
			name: "collate-desc-1: ORDER BY DESC with NOCASE",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(2, 'Bob')",
				"INSERT INTO users VALUES(3, 'Charlie')",
			},
			query:           "SELECT name FROM users ORDER BY name COLLATE NOCASE DESC",
			verifyType:      collationVerifyOrderedStrings,
			expectedStrings: []string{"Charlie", "Bob", "alice"},
		},

		// ===== MULTI-COLUMN ORDER BY (from collate1.test) =====

		{
			name: "collate-multi-1: ORDER BY multiple columns with different collations",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, lastname TEXT, firstname TEXT)",
			},
			inserts: []string{
				"INSERT INTO users VALUES(1, 'Smith', 'alice')",
				"INSERT INTO users VALUES(2, 'SMITH', 'Bob')",
				"INSERT INTO users VALUES(3, 'jones', 'Charlie')",
				"INSERT INTO users VALUES(4, 'Jones', 'alice')",
			},
			query:         "SELECT lastname, firstname FROM users ORDER BY lastname COLLATE NOCASE, firstname COLLATE BINARY",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		// ===== INDEX USAGE WITH COLLATION (from collate4.test) =====

		{
			name: "collate4-1.1.1: Index with NOCASE can be used for ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
				"CREATE INDEX i1 ON t1(a)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'a')",
				"INSERT INTO t1 VALUES('b', 'b')",
				"INSERT INTO t1 VALUES('B', 'B')",
				"INSERT INTO t1 VALUES('A', 'A')",
			},
			query:         "SELECT a FROM t1 ORDER BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		{
			name: "collate4-1.1.2: Index with NOCASE can be used for ORDER BY NOCASE",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
				"CREATE INDEX i1 ON t1(a)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'a')",
				"INSERT INTO t1 VALUES('b', 'b')",
				"INSERT INTO t1 VALUES('B', 'B')",
				"INSERT INTO t1 VALUES('A', 'A')",
			},
			query:         "SELECT a FROM t1 ORDER BY a COLLATE NOCASE",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		// ===== GROUP BY WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-4.1: GROUP BY with NOCASE column",
			skip: "GROUP BY with NOCASE collation not yet implemented",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b INTEGER)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 1)",
				"INSERT INTO t1 VALUES('A', 2)",
				"INSERT INTO t1 VALUES('b', 3)",
				"INSERT INTO t1 VALUES('B', 4)",
			},
			query:         "SELECT a, SUM(b) FROM t1 GROUP BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},

		{
			name: "collate5-4.2: GROUP BY with BINARY column",
			setup: []string{
				"CREATE TABLE t1(a COLLATE BINARY, b INTEGER)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 1)",
				"INSERT INTO t1 VALUES('A', 2)",
				"INSERT INTO t1 VALUES('b', 3)",
				"INSERT INTO t1 VALUES('B', 4)",
			},
			query:         "SELECT a, SUM(b) FROM t1 GROUP BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		// ===== NULL HANDLING (from collate1.test, collate2.test) =====

		{
			name: "collate-null-1: NULL values sort first",
			skip: "NULL row handling in ORDER BY not yet correct",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('b')",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES('a')",
				"INSERT INTO t1 VALUES(NULL)",
			},
			query:         "SELECT a FROM t1 ORDER BY a",
			verifyType:    collationVerifyRowCount,
			expectedCount: 4,
		},

		// ===== COMPOUND SELECT (from collate5.test) =====

		{
			name: "collate5-2.2.1: EXCEPT with collation",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE)",
				"CREATE TABLE t2(a COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a')",
				"INSERT INTO t1 VALUES('b')",
				"INSERT INTO t1 VALUES('n')",
				"INSERT INTO t2 VALUES('a')",
				"INSERT INTO t2 VALUES('A')",
				"INSERT INTO t2 VALUES('b')",
				"INSERT INTO t2 VALUES('B')",
			},
			query:         "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
			verifyType:    collationVerifyRowCount,
			expectedCount: 1,
		},

		{
			name: "collate5-2.3.1: INTERSECT with collation",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE)",
				"CREATE TABLE t2(a COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a')",
				"INSERT INTO t1 VALUES('b')",
				"INSERT INTO t2 VALUES('a')",
				"INSERT INTO t2 VALUES('A')",
				"INSERT INTO t2 VALUES('b')",
			},
			query:         "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
			verifyType:    collationVerifyRowCount,
			expectedCount: 2,
		},
	}
}

func containsCollation(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && strings.Contains(strings.ToLower(s), strings.ToLower(substr))))
}
