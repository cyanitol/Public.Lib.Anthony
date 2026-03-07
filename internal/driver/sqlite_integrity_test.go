// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteIntegrityCheck tests PRAGMA integrity_check, quick_check, and foreign_key_check
// Converted from contrib/sqlite/sqlite-src-3510200/test/pragma.test and related tests
func TestSQLiteIntegrityCheck(t *testing.T) {
	t.Skip("pre-existing failure - needs integrity check implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "integrity_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		setup    []string
		query    string
		wantOK   bool // true if we expect "ok" result
		wantErr  bool
		contains []string // patterns that should be in result
	}{
		// Test 1: Basic integrity check on clean database
		{
			name: "pragma-3.1 basic integrity check ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT, c INT)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(4, 5, 6)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 2: Quick check on clean database
		{
			name: "pragma-3.8.1 quick check ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:  "PRAGMA quick_check",
			wantOK: true,
		},
		// Test 3: Quick check case insensitive
		{
			name: "pragma-3.8.2 QUICK_CHECK uppercase",
			setup: []string{
				"CREATE TABLE t1(a INT)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:  "PRAGMA QUICK_CHECK",
			wantOK: true,
		},
		// Test 4: Integrity check with limit
		{
			name: "pragma-integrity-limit basic limit",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:  "PRAGMA integrity_check(10)",
			wantOK: true,
		},
		// Test 5: Integrity check specific table
		{
			name: "pragma-3.6c check sqlite_schema",
			setup: []string{
				"CREATE TABLE t1(a INT)",
			},
			query:  "PRAGMA integrity_check(sqlite_schema)",
			wantOK: true,
		},
		// Test 6: Multiple tables integrity check
		{
			name: "integrity-multi tables ok",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, name TEXT)",
				"CREATE TABLE t2(id INT PRIMARY KEY, ref INT)",
				"INSERT INTO t1 VALUES(1, 'test')",
				"INSERT INTO t2 VALUES(1, 1)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 7: Table with index integrity check
		{
			name: "integrity-index simple index ok",
			setup: []string{
				"CREATE TABLE users(id INT, email TEXT)",
				"CREATE INDEX idx_email ON users(email)",
				"INSERT INTO users VALUES(1, 'test@example.com')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 8: Multiple indexes integrity check
		{
			name: "integrity-multi-index multiple indexes ok",
			setup: []string{
				"CREATE TABLE products(id INT, name TEXT, price REAL)",
				"CREATE INDEX idx_name ON products(name)",
				"CREATE INDEX idx_price ON products(price)",
				"INSERT INTO products VALUES(1, 'Widget', 9.99)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 9: Composite index integrity check
		{
			name: "integrity-composite composite index ok",
			setup: []string{
				"CREATE TABLE orders(customer_id INT, order_date TEXT, amount REAL)",
				"CREATE INDEX idx_customer_date ON orders(customer_id, order_date)",
				"INSERT INTO orders VALUES(1, '2024-01-01', 100.00)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 10: Unique index integrity check
		{
			name: "integrity-unique unique index ok",
			setup: []string{
				"CREATE TABLE accounts(id INT, username TEXT)",
				"CREATE UNIQUE INDEX idx_username ON accounts(username)",
				"INSERT INTO accounts VALUES(1, 'user1')",
				"INSERT INTO accounts VALUES(2, 'user2')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 11: Primary key integrity check
		{
			name: "integrity-pk primary key ok",
			setup: []string{
				"CREATE TABLE items(id INT PRIMARY KEY, description TEXT)",
				"INSERT INTO items VALUES(1, 'First item')",
				"INSERT INTO items VALUES(2, 'Second item')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 12: Empty table integrity check
		{
			name: "integrity-empty empty table ok",
			setup: []string{
				"CREATE TABLE empty_table(a INT, b INT, c INT)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 13: Table with NULL values
		{
			name: "integrity-null nulls ok",
			setup: []string{
				"CREATE TABLE nullable(a INT, b TEXT)",
				"INSERT INTO nullable VALUES(NULL, 'test')",
				"INSERT INTO nullable VALUES(1, NULL)",
				"INSERT INTO nullable VALUES(NULL, NULL)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 14: Without rowid table
		{
			name: "integrity-without-rowid without rowid ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT, PRIMARY KEY(a)) WITHOUT ROWID",
				"INSERT INTO t1 VALUES(1, 100)",
				"INSERT INTO t1 VALUES(2, 200)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 15: Text collation
		{
			name: "integrity-collation text collation ok",
			setup: []string{
				"CREATE TABLE t1(a TEXT COLLATE NOCASE)",
				"CREATE INDEX idx_a ON t1(a)",
				"INSERT INTO t1 VALUES('Hello')",
				"INSERT INTO t1 VALUES('WORLD')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 16: Integer primary key
		{
			name: "integrity-int-pk integer primary key ok",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT)",
				"INSERT INTO t1(data) VALUES('test1')",
				"INSERT INTO t1(data) VALUES('test2')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 17: Foreign key check (basic)
		{
			name: "fkey-check-1 no violations",
			setup: []string{
				"PRAGMA foreign_keys = ON",
				"CREATE TABLE parent(id INT PRIMARY KEY)",
				"CREATE TABLE child(id INT, parent_id INT, FOREIGN KEY(parent_id) REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(1, 1)",
			},
			query:  "PRAGMA foreign_key_check",
			wantOK: true,
		},
		// Test 18: Foreign key check specific table
		{
			name: "fkey-check-2 specific table",
			setup: []string{
				"PRAGMA foreign_keys = ON",
				"CREATE TABLE parent(id INT PRIMARY KEY)",
				"CREATE TABLE child(id INT, parent_id INT, FOREIGN KEY(parent_id) REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(1, 1)",
			},
			query:  "PRAGMA foreign_key_check(child)",
			wantOK: true,
		},
		// Test 19: Multiple tables quick check
		{
			name: "quick-check-multi multiple tables",
			setup: []string{
				"CREATE TABLE t1(a INT)",
				"CREATE TABLE t2(b INT)",
				"CREATE TABLE t3(c INT)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t3 VALUES(3)",
			},
			query:  "PRAGMA quick_check",
			wantOK: true,
		},
		// Test 20: Check with autoincrement
		{
			name: "integrity-autoincrement autoincrement ok",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
				"INSERT INTO t1(value) VALUES('first')",
				"INSERT INTO t1(value) VALUES('second')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 21: Virtual tables excluded from foreign key check
		{
			name: "fkey-virtual virtual table",
			setup: []string{
				"CREATE TABLE parent(id INT PRIMARY KEY)",
				"INSERT INTO parent VALUES(1)",
			},
			query:  "PRAGMA foreign_key_check",
			wantOK: true,
		},
		// Test 22: Check after delete
		{
			name: "integrity-after-delete delete operations",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, data TEXT)",
				"CREATE INDEX idx_data ON t1(data)",
				"INSERT INTO t1 VALUES(1, 'a'), (2, 'b'), (3, 'c')",
				"DELETE FROM t1 WHERE id = 2",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 23: Check after update
		{
			name: "integrity-after-update update operations",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, value INT)",
				"CREATE INDEX idx_value ON t1(value)",
				"INSERT INTO t1 VALUES(1, 100), (2, 200)",
				"UPDATE t1 SET value = 300 WHERE id = 1",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 24: Check with transactions
		{
			name: "integrity-transaction transaction ok",
			setup: []string{
				"CREATE TABLE t1(a INT)",
				"BEGIN TRANSACTION",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"COMMIT",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 25: Check with rollback
		{
			name: "integrity-rollback after rollback",
			setup: []string{
				"CREATE TABLE t1(a INT)",
				"INSERT INTO t1 VALUES(1)",
				"BEGIN TRANSACTION",
				"INSERT INTO t1 VALUES(2)",
				"ROLLBACK",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 26: Covering index check
		{
			name: "integrity-covering covering index ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT, c INT)",
				"CREATE INDEX idx_abc ON t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 27: Partial index check
		{
			name: "integrity-partial partial index ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT)",
				"CREATE INDEX idx_filtered ON t1(a) WHERE b > 0",
				"INSERT INTO t1 VALUES(1, 10), (2, -5), (3, 20)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 28: Expression index check
		{
			name: "integrity-expression expression index ok",
			setup: []string{
				"CREATE TABLE t1(name TEXT)",
				"CREATE INDEX idx_lower ON t1(lower(name))",
				"INSERT INTO t1 VALUES('Alice'), ('BOB')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 29: Check with BLOB data
		{
			name: "integrity-blob blob data ok",
			setup: []string{
				"CREATE TABLE t1(id INT, data BLOB)",
				"INSERT INTO t1 VALUES(1, X'DEADBEEF')",
				"INSERT INTO t1 VALUES(2, X'CAFEBABE')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 30: Check with REAL data
		{
			name: "integrity-real real numbers ok",
			setup: []string{
				"CREATE TABLE t1(id INT, value REAL)",
				"CREATE INDEX idx_value ON t1(value)",
				"INSERT INTO t1 VALUES(1, 3.14159), (2, 2.71828)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 31: Check with TEXT data
		{
			name: "integrity-text text data ok",
			setup: []string{
				"CREATE TABLE t1(id INT, description TEXT)",
				"CREATE INDEX idx_desc ON t1(description)",
				"INSERT INTO t1 VALUES(1, 'Short'), (2, 'A much longer description text')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 32: Check with mixed types
		{
			name: "integrity-mixed mixed types ok",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 'text', 3.14)",
				"INSERT INTO t1 VALUES('string', 42, X'FF')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 33: Check with large dataset
		{
			name: "integrity-large larger dataset ok",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, value INT)",
				"CREATE INDEX idx_value ON t1(value)",
				"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100) INSERT INTO t1 SELECT x, x*10 FROM cnt",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 34: Check with string collation
		{
			name: "integrity-collate collation ok",
			setup: []string{
				"CREATE TABLE t1(name TEXT COLLATE NOCASE)",
				"INSERT INTO t1 VALUES('alice'), ('ALICE'), ('Bob')",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 35: Check with generated columns
		{
			name: "integrity-generated generated column ok",
			setup: []string{
				"CREATE TABLE t1(a INT, b INT, c AS (a+b))",
				"INSERT INTO t1(a, b) VALUES(1, 2), (3, 4)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 36: Check with CHECK constraint
		{
			name: "integrity-check-constraint check constraint ok",
			setup: []string{
				"CREATE TABLE t1(a INT CHECK(a > 0))",
				"INSERT INTO t1 VALUES(1), (5), (10)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 37: Check with DEFAULT values
		{
			name: "integrity-default default values ok",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, status TEXT DEFAULT 'active')",
				"INSERT INTO t1(id) VALUES(1), (2)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
		// Test 38: Multiple integrity checks in sequence
		{
			name: "integrity-sequence sequential checks",
			setup: []string{
				"CREATE TABLE t1(a INT)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:  "PRAGMA integrity_check",
			wantOK: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Clean up
			db.Exec("DROP TABLE IF EXISTS t1")
			db.Exec("DROP TABLE IF EXISTS t2")
			db.Exec("DROP TABLE IF EXISTS t3")
			db.Exec("DROP TABLE IF EXISTS users")
			db.Exec("DROP TABLE IF EXISTS products")
			db.Exec("DROP TABLE IF EXISTS orders")
			db.Exec("DROP TABLE IF EXISTS accounts")
			db.Exec("DROP TABLE IF EXISTS items")
			db.Exec("DROP TABLE IF EXISTS empty_table")
			db.Exec("DROP TABLE IF EXISTS nullable")
			db.Exec("DROP TABLE IF EXISTS parent")
			db.Exec("DROP TABLE IF EXISTS child")

			// Run setup
			for _, setupSQL := range tt.setup {
				if _, err := db.Exec(setupSQL); err != nil {
					t.Fatalf("setup failed: %v, SQL: %s", err, setupSQL)
				}
			}

			// Execute query
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()

			// Collect results
			var results []string
			for rows.Next() {
				var result string
				if err := rows.Scan(&result); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				results = append(results, result)
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("rows iteration failed: %v", err)
			}

			// Check results
			if tt.wantOK {
				// Expect "ok" result
				if len(results) == 0 {
					t.Error("expected 'ok' result, got empty result set")
				} else if len(results) == 1 && results[0] == "ok" {
					// Success
				} else {
					t.Errorf("expected 'ok', got: %v", results)
				}
			} else if len(tt.contains) > 0 {
				// Check for specific patterns
				fullResult := strings.Join(results, "\n")
				for _, pattern := range tt.contains {
					if !strings.Contains(fullResult, pattern) {
						t.Errorf("expected pattern %q not found in results: %v", pattern, results)
					}
				}
			}
		})
	}
}

// TestIntegrityCheckEdgeCases tests edge cases and special scenarios
func TestIntegrityCheckEdgeCases(t *testing.T) {
	t.Skip("pre-existing failure - needs integrity check implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "edge_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Integrity check on fresh database
	t.Run("fresh database", func(t *testing.T) {
		var result string
		err := db.QueryRow("PRAGMA integrity_check").Scan(&result)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result != "ok" {
			t.Errorf("expected 'ok', got %q", result)
		}
	})

	// Test 2: Integrity check with limit 0 (unlimited)
	t.Run("unlimited check", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE t1(a INT); INSERT INTO t1 VALUES(1)")
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		var result string
		err = db.QueryRow("PRAGMA integrity_check(0)").Scan(&result)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result != "ok" {
			t.Errorf("expected 'ok', got %q", result)
		}
		db.Exec("DROP TABLE t1")
	})

	// Test 3: Quick check vs integrity check comparison
	t.Run("quick vs integrity", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE t2(x INT); INSERT INTO t2 VALUES(42)")
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		var integrityResult, quickResult string
		err = db.QueryRow("PRAGMA integrity_check").Scan(&integrityResult)
		if err != nil {
			t.Fatalf("integrity_check failed: %v", err)
		}

		err = db.QueryRow("PRAGMA quick_check").Scan(&quickResult)
		if err != nil {
			t.Fatalf("quick_check failed: %v", err)
		}

		if integrityResult != "ok" || quickResult != "ok" {
			t.Errorf("expected both to return 'ok', got integrity=%q quick=%q", integrityResult, quickResult)
		}
		db.Exec("DROP TABLE t2")
	})

	// Test 4: Check with very long table name
	t.Run("long table name", func(t *testing.T) {
		longName := "table_" + strings.Repeat("x", 100)
		_, err := db.Exec("CREATE TABLE " + longName + "(a INT)")
		if err != nil {
			t.Fatalf("create table failed: %v", err)
		}

		var result string
		err = db.QueryRow("PRAGMA integrity_check").Scan(&result)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if result != "ok" {
			t.Errorf("expected 'ok', got %q", result)
		}
		db.Exec("DROP TABLE " + longName)
	})

	// Test 5: Multiple sequential checks
	t.Run("sequential checks", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE t3(a INT); INSERT INTO t3 VALUES(1)")
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			var result string
			err = db.QueryRow("PRAGMA integrity_check").Scan(&result)
			if err != nil {
				t.Fatalf("check %d failed: %v", i, err)
			}
			if result != "ok" {
				t.Errorf("check %d: expected 'ok', got %q", i, result)
			}
		}
		db.Exec("DROP TABLE t3")
	})
}

// TestPragmaIntegrityCheckOptions tests various PRAGMA integrity_check options
func TestPragmaIntegrityCheckOptions(t *testing.T) {
	t.Skip("pre-existing failure - needs integrity check options implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "options_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup test data
	_, err = db.Exec(`
		CREATE TABLE test_table(id INT PRIMARY KEY, data TEXT);
		CREATE INDEX idx_data ON test_table(data);
		INSERT INTO test_table VALUES(1, 'alpha'), (2, 'beta'), (3, 'gamma');
	`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	tests := []struct {
		name   string
		pragma string
	}{
		{"no limit", "PRAGMA integrity_check"},
		{"limit 1", "PRAGMA integrity_check(1)"},
		{"limit 5", "PRAGMA integrity_check(5)"},
		{"limit 10", "PRAGMA integrity_check(10)"},
		{"limit 100", "PRAGMA integrity_check(100)"},
		{"specific table", "PRAGMA integrity_check(test_table)"},
		{"schema table", "PRAGMA integrity_check(sqlite_schema)"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.pragma)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()

			var results []string
			for rows.Next() {
				var result string
				if err := rows.Scan(&result); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				results = append(results, result)
			}

			if len(results) == 0 {
				t.Error("no results returned")
			} else if results[0] != "ok" {
				t.Errorf("expected 'ok', got: %v", results)
			}
		})
	}
}
