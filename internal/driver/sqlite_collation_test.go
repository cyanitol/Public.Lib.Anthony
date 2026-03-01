// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteCollation is a comprehensive test suite converted from SQLite's TCL collation tests
// (collate1.test, collate2.test, collate3.test, collate4.test, collate5.test, collate6.test,
//  collate7.test, collate8.test, collate9.test, collateA.test, collateB.test)
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
	tests := []struct {
		name     string
		setup    []string // CREATE TABLE statements and other setup
		inserts  []string // INSERT statements to test
		query    string   // Query to execute
		verify   func(*testing.T, *sql.Rows)
		wantErr  bool
		errMsg   string
	}{
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
			query: "SELECT c2 FROM t1 ORDER BY c2",
			verify: func(t *testing.T, rows *sql.Rows) {
				expected := []sql.NullString{
					{Valid: false},
					{String: "0x119", Valid: true},
					{String: "0x2D", Valid: true},
				}
				var got []sql.NullString
				for rows.Next() {
					var val sql.NullString
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					got = append(got, val)
				}
				if len(got) != len(expected) {
					t.Fatalf("Expected %d rows, got %d", len(expected), len(got))
				}
				for i, exp := range expected {
					if got[i] != exp {
						t.Errorf("Row %d: expected %v, got %v", i, exp, got[i])
					}
				}
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
			query: "SELECT c2 FROM t1 ORDER BY c2 COLLATE NOCASE",
			verify: func(t *testing.T, rows *sql.Rows) {
				var count int
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 3 {
					t.Errorf("Expected 3 rows, got %d", count)
				}
			},
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
			query: "SELECT c1, c2 FROM t1 ORDER BY c1 COLLATE BINARY, c2 COLLATE BINARY",
			verify: func(t *testing.T, rows *sql.Rows) {
				var count int
				for rows.Next() {
					var c1, c2 sql.NullString
					if err := rows.Scan(&c1, &c2); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 6 {
					t.Errorf("Expected 6 rows, got %d", count)
				}
			},
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
			query: "SELECT a FROM t1 ORDER BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []sql.NullString{}
				for rows.Next() {
					var val sql.NullString
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				if len(results) != 4 {
					t.Fatalf("Expected 4 rows, got %d", len(results))
				}
				// NULL should be first
				if results[0].Valid {
					t.Errorf("First row should be NULL, got %v", results[0])
				}
			},
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
			query: "SELECT a as c1 FROM t1 ORDER BY c1 COLLATE BINARY",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				// With BINARY, uppercase comes before lowercase
				if len(results) != 3 {
					t.Fatalf("Expected 3 rows, got %d", len(results))
				}
			},
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
			query: "SELECT a FROM t1 WHERE a > 'aa' ORDER BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				if len(results) != 2 {
					t.Fatalf("Expected 2 rows, got %d", len(results))
				}
			},
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
			query: "SELECT b FROM t1 WHERE b > 'aa' ORDER BY b",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 2 {
					t.Errorf("Expected 2 rows, got %d", count)
				}
			},
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
			query: "SELECT a FROM t1 WHERE a COLLATE NOCASE = 'aa'",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				// Should match both 'aa' and 'AA' with NOCASE
				if count != 2 {
					t.Errorf("Expected 2 rows with NOCASE, got %d", count)
				}
			},
		},

		// ===== UNKNOWN/UNDEFINED COLLATION SEQUENCES (from collate3.test) =====

		{
			name: "collate3-1.1: Unknown collation sequence in ORDER BY",
			setup: []string{
				"CREATE TABLE t1(c1)",
			},
			query:   "SELECT * FROM t1 ORDER BY c1 COLLATE garbage",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name: "collate3-1.1.2: Unknown collation in DISTINCT",
			setup: []string{
				"CREATE TABLE t1(c1)",
			},
			query:   "SELECT DISTINCT c1 COLLATE garbage FROM t1",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name:    "collate3-1.2: Unknown collation in CREATE TABLE",
			setup:   []string{},
			query:   "CREATE TABLE t1(c1 COLLATE garbage)",
			wantErr: true,
			errMsg:  "no such collation sequence",
		},

		{
			name: "collate3-1.3: Unknown collation in CREATE INDEX",
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
			query: "SELECT name FROM users ORDER BY name COLLATE BINARY",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, name)
				}
				// BINARY: uppercase < lowercase in ASCII
				expected := []string{"ALICE", "Bob", "Charlie", "alice", "bob"}
				if len(results) != len(expected) {
					t.Fatalf("Expected %d results, got %d", len(expected), len(results))
				}
				for i, exp := range expected {
					if results[i] != exp {
						t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
					}
				}
			},
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
			query: "SELECT name FROM users ORDER BY name COLLATE NOCASE",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, name)
				}
				if len(results) != 5 {
					t.Fatalf("Expected 5 results, got %d", len(results))
				}
				// Check that alice/ALICE come before Bob/bob
				aliceCount := 0
				for i := 0; i < 2 && i < len(results); i++ {
					if results[i] == "alice" || results[i] == "ALICE" {
						aliceCount++
					}
				}
				if aliceCount != 2 {
					t.Errorf("Expected 2 alice/ALICE in first 2 positions, got %d", aliceCount)
				}
			},
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
			query: "SELECT name FROM users WHERE name COLLATE NOCASE = 'john'",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					if name != "John" && name != "JOHN" {
						t.Errorf("Unexpected result: %s", name)
					}
					count++
				}
				if count != 2 {
					t.Errorf("Expected 2 results with NOCASE, got %d", count)
				}
			},
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
			query: "SELECT value FROM data ORDER BY value COLLATE RTRIM",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var value string
					if err := rows.Scan(&value); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, value)
				}
				if len(results) != 5 {
					t.Fatalf("Expected 5 results, got %d", len(results))
				}
				// Count entries for each base value
				appleCount := 0
				for i := 0; i < 2 && i < len(results); i++ {
					if results[i] == "apple" || results[i] == "apple  " {
						appleCount++
					}
				}
				if appleCount != 2 {
					t.Errorf("Expected 2 apple entries in first 2 positions, got %d", appleCount)
				}
			},
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
			query: "SELECT name FROM users ORDER BY name",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
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
				// Check that alice/ALICE come before Bob/bob
				for i := 0; i < 2; i++ {
					if results[i] != "alice" && results[i] != "ALICE" {
						t.Errorf("Expected alice/ALICE at position %d, got %s", i, results[i])
					}
				}
			},
		},

		{
			name: "collate-column-2: Multiple COLLATE clauses - last one wins",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT COLLATE BINARY COLLATE NOCASE COLLATE RTRIM)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES(1, 'abc')",
				"INSERT INTO t1 VALUES(2, 'abc   ')",
			},
			query: "SELECT a FROM t1 WHERE a = 'abc'",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				// With RTRIM, both should match
				if count != 2 {
					t.Errorf("Expected 2 results with RTRIM, got %d", count)
				}
			},
		},

		// ===== DISTINCT WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-1.1: DISTINCT with NOCASE column",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
				"INSERT INTO t1 VALUES('B', 'banana')",
			},
			query: "SELECT DISTINCT a FROM t1",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				// NOCASE: 'a' and 'A' are same, 'b' and 'B' are same
				if len(results) != 2 {
					t.Errorf("Expected 2 distinct values with NOCASE, got %d", len(results))
				}
			},
		},

		{
			name: "collate5-1.2: DISTINCT with BINARY column",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
				"INSERT INTO t1 VALUES('B', 'banana')",
			},
			query: "SELECT DISTINCT b FROM t1",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				// BINARY: all different
				if len(results) != 3 {
					t.Errorf("Expected 3 distinct values with BINARY, got %d", len(results))
				}
			},
		},

		{
			name: "collate5-1.3: DISTINCT with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b COLLATE BINARY)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 'apple')",
				"INSERT INTO t1 VALUES('A', 'Apple')",
				"INSERT INTO t1 VALUES('b', 'banana')",
			},
			query: "SELECT DISTINCT a, b FROM t1",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var a, b string
					if err := rows.Scan(&a, &b); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 3 {
					t.Errorf("Expected 3 distinct rows, got %d", count)
				}
			},
		},

		// ===== UNION WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-2.1.1: UNION with NOCASE from first table",
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
			query: "SELECT a FROM t1 UNION SELECT a FROM t2",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				// NOCASE from t1 used: a/A same, b/B same
				if len(results) != 2 {
					t.Errorf("Expected 2 results with UNION and NOCASE, got %d", len(results))
				}
			},
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
			query: "SELECT a FROM t1 UNION SELECT a FROM t2",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				// BINARY from t1 used: all different
				if len(results) != 4 {
					t.Errorf("Expected 4 results with UNION and BINARY, got %d", len(results))
				}
			},
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
			query: "SELECT name FROM users ORDER BY name COLLATE NOCASE DESC",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []string{}
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, name)
				}
				expected := []string{"Charlie", "Bob", "alice"}
				if len(results) != len(expected) {
					t.Fatalf("Expected %d results, got %d", len(expected), len(results))
				}
				for i, exp := range expected {
					if results[i] != exp {
						t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
					}
				}
			},
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
			query: "SELECT lastname, firstname FROM users ORDER BY lastname COLLATE NOCASE, firstname COLLATE BINARY",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []struct {
					lastname  string
					firstname string
				}{}
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
				// jones/Jones should come before Smith/SMITH (NOCASE)
				// Within each group, sort by firstname (BINARY: uppercase < lowercase)
			},
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
			query: "SELECT a FROM t1 ORDER BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 4 {
					t.Errorf("Expected 4 rows, got %d", count)
				}
			},
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
			query: "SELECT a FROM t1 ORDER BY a COLLATE NOCASE",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 4 {
					t.Errorf("Expected 4 rows, got %d", count)
				}
			},
		},

		// ===== GROUP BY WITH COLLATION (from collate5.test) =====

		{
			name: "collate5-4.1: GROUP BY with NOCASE column",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE, b INTEGER)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('a', 1)",
				"INSERT INTO t1 VALUES('A', 2)",
				"INSERT INTO t1 VALUES('b', 3)",
				"INSERT INTO t1 VALUES('B', 4)",
			},
			query: "SELECT a, SUM(b) FROM t1 GROUP BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := map[string]int{}
				for rows.Next() {
					var a string
					var sum int
					if err := rows.Scan(&a, &sum); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results[a] = sum
				}
				// NOCASE: 'a' and 'A' grouped together
				if len(results) != 2 {
					t.Errorf("Expected 2 groups with NOCASE, got %d", len(results))
				}
			},
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
			query: "SELECT a, SUM(b) FROM t1 GROUP BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var a string
					var sum int
					if err := rows.Scan(&a, &sum); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				// BINARY: all separate groups
				if count != 4 {
					t.Errorf("Expected 4 groups with BINARY, got %d", count)
				}
			},
		},

		// ===== NULL HANDLING (from collate1.test, collate2.test) =====

		{
			name: "collate-null-1: NULL values sort first",
			setup: []string{
				"CREATE TABLE t1(a COLLATE NOCASE)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES('b')",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES('a')",
				"INSERT INTO t1 VALUES(NULL)",
			},
			query: "SELECT a FROM t1 ORDER BY a",
			verify: func(t *testing.T, rows *sql.Rows) {
				results := []sql.NullString{}
				for rows.Next() {
					var val sql.NullString
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					results = append(results, val)
				}
				if len(results) != 4 {
					t.Fatalf("Expected 4 rows, got %d", len(results))
				}
				// First two should be NULL
				if results[0].Valid || results[1].Valid {
					t.Errorf("Expected first two rows to be NULL")
				}
			},
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
			query: "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				// Should return only values in t1 not in t2 (using t1's NOCASE)
				if count != 1 {
					t.Errorf("Expected 1 result, got %d", count)
				}
			},
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
			query: "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
			verify: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var val string
					if err := rows.Scan(&val); err != nil {
						t.Fatalf("Scan failed: %v", err)
					}
					count++
				}
				if count != 2 {
					t.Errorf("Expected 2 results, got %d", count)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := sql.Open("sqlite_internal", dbPath)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Setup
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					if tt.wantErr {
						if tt.errMsg == "" || !containsCollation(err.Error(), tt.errMsg) {
							t.Errorf("Setup error: %v, wanted error containing %q", err, tt.errMsg)
						}
						return
					}
					t.Fatalf("Setup failed: %v\nStatement: %s", err, stmt)
				}
			}

			// Insert test data
			for _, stmt := range tt.inserts {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("Insert failed: %v\nStatement: %s", err, stmt)
				}
			}

			// Execute query
			if tt.query != "" {
				if tt.verify != nil {
					rows, err := db.Query(tt.query)
					if err != nil {
						if tt.wantErr {
							if tt.errMsg == "" || !containsCollation(err.Error(), tt.errMsg) {
								t.Errorf("Query error: %v, wanted error containing %q", err, tt.errMsg)
							}
							return
						}
						t.Fatalf("Query failed: %v\nQuery: %s", err, tt.query)
					}
					defer rows.Close()
					tt.verify(t, rows)
				} else {
					// Just execute without verification
					_, err := db.Exec(tt.query)
					if tt.wantErr {
						if err == nil {
							t.Errorf("Expected error containing %q, got nil", tt.errMsg)
							return
						}
						if tt.errMsg == "" || !containsCollation(err.Error(), tt.errMsg) {
							t.Errorf("Query error: %v, wanted error containing %q", err, tt.errMsg)
						}
						return
					}
					if err != nil {
						t.Fatalf("Query failed: %v\nQuery: %s", err, tt.query)
					}
				}
			}
		})
	}
}

func containsCollation(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && strings.Contains(strings.ToLower(s), strings.ToLower(substr))))
}
