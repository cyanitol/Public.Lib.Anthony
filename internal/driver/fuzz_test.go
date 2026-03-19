// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// FuzzSQL tests the driver with random SQL statements to ensure it doesn't panic
func FuzzSQL(f *testing.F) {
	// Add seed corpus with various SQL statements
	seeds := []string{
		// Basic queries
		"SELECT 1",
		"SELECT * FROM sqlite_master",
		"SELECT 1 + 1",
		"SELECT 'hello'",

		// Table operations
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
		"DROP TABLE IF EXISTS t",
		"INSERT INTO t VALUES (1, 'test')",
		"UPDATE t SET name = 'updated' WHERE id = 1",
		"DELETE FROM t WHERE id = 1",

		// Queries with various clauses
		"SELECT * FROM t WHERE id = 1",
		"SELECT * FROM t ORDER BY id",
		"SELECT * FROM t LIMIT 10",
		"SELECT * FROM t LIMIT 10 OFFSET 5",
		"SELECT COUNT(*) FROM t GROUP BY name",
		"SELECT * FROM t WHERE id IN (1, 2, 3)",

		// Joins
		"SELECT * FROM a JOIN b ON a.id = b.id",
		"SELECT * FROM a LEFT JOIN b ON a.id = b.id",
		"SELECT * FROM a CROSS JOIN b",

		// Subqueries
		"SELECT * FROM (SELECT 1 AS x)",
		"SELECT * FROM t WHERE id IN (SELECT id FROM t)",

		// Aggregates
		"SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM t",

		// Transactions
		"BEGIN TRANSACTION",
		"COMMIT",
		"ROLLBACK",

		// Indexes
		"CREATE INDEX idx ON t(id)",
		"DROP INDEX IF EXISTS idx",

		// PRAGMA statements
		"PRAGMA foreign_keys",
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode",
		"PRAGMA table_info(t)",

		// Empty and whitespace
		"",
		" ",
		"\n",
		"\t",
		"  \n  \t  ",

		// Comments
		"SELECT 1 -- comment",
		"SELECT 1 /* comment */",
		"/* multi\nline\ncomment */ SELECT 1",

		// String literals with special characters
		"SELECT 'test'",
		"SELECT 'test''s'", // escaped quote
		"SELECT \"test\"",
		"SELECT 'line1\nline2'",
		"SELECT '\x00'",

		// Prepared statement placeholders
		"SELECT * FROM t WHERE id = ?",
		"SELECT * FROM t WHERE id = :id",
		"SELECT * FROM t WHERE id = $1",

		// Complex expressions
		"SELECT 1 + 2 * 3 - 4 / 5",
		"SELECT (1 + 2) * (3 - 4)",
		"SELECT 1 AND 2 OR 3",
		"SELECT NOT (1 = 2)",
		"SELECT 1 BETWEEN 0 AND 10",
		"SELECT 1 IS NULL",
		"SELECT 1 IS NOT NULL",
		"SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END",

		// Malformed SQL (should error gracefully, not panic)
		"SELECT",
		"FROM",
		"WHERE",
		"SELECT FROM",
		"SELECT * FROM",
		"INSERT INTO",
		"CREATE TABLE",
		"DROP",
		"UPDATE",
		"DELETE",

		// SQL injection attempts (should be safe)
		"SELECT * FROM t WHERE id = 1; DROP TABLE t",
		"SELECT * FROM t WHERE name = '' OR '1'='1",
		"SELECT * FROM t WHERE name = '\\' OR 1=1--",

		// Unicode
		"SELECT '你好'",
		"SELECT 'Привет'",
		"SELECT '🔥'",

		// Long inputs
		string(make([]byte, 1000)),
		strings.Repeat("SELECT 1 UNION ", 10) + "SELECT 1",

		// Nested subqueries
		"SELECT * FROM (SELECT * FROM (SELECT 1))",

		// CTEs
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	// Fuzz function - should never panic
	f.Fuzz(func(t *testing.T, sqlStr string) {
		// Skip extremely long inputs to prevent timeout
		if len(sqlStr) > 100000 {
		}

		// Create in-memory database connection
		db, err := sql.Open(DriverName, ":memory:")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Test should not panic, even with malformed input
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Driver panicked on input: %q\nPanic: %v", sqlStr, r)
			}
		}()

		// Try to execute the SQL
		// We don't care if it returns an error, just that it doesn't panic
		ctx := context.Background()

		// Try as query first
		rows, err := db.QueryContext(ctx, sqlStr)
		if err == nil {
			// If query succeeded, consume rows
			for rows.Next() {
				// Scan into empty interface to avoid type errors
				var dummy interface{}
				_ = rows.Scan(&dummy)
			}
			rows.Close()
		} else {
			// If query failed, try as exec
			_, _ = db.ExecContext(ctx, sqlStr)
		}
	})
}

// FuzzPreparedStatement tests prepared statements with random SQL and parameters
func FuzzPreparedStatement(f *testing.F) {
	seeds := []struct {
		sql   string
		param string
	}{
		{"SELECT ?", "1"},
		{"SELECT ?", "test"},
		{"SELECT ?", ""},
		{"SELECT ? + ?", "1"},
		{"SELECT * FROM t WHERE id = ?", "1"},
		{"INSERT INTO t VALUES (?)", "test"},
		{"UPDATE t SET name = ? WHERE id = 1", "updated"},
		{"DELETE FROM t WHERE id = ?", "1"},
	}

	for _, seed := range seeds {
		f.Add(seed.sql, seed.param)
	}

	f.Fuzz(func(t *testing.T, sqlStr string, param string) {
		// Skip extremely long inputs
		if len(sqlStr) > 10000 || len(param) > 10000 {
		}

		// Create in-memory database
		db, err := sql.Open(DriverName, ":memory:")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Prepared statement panicked\nSQL: %q\nParam: %q\nPanic: %v",
					sqlStr, param, r)
			}
		}()

		// Try to prepare and execute
		stmt, err := db.Prepare(sqlStr)
		if err == nil {
			defer stmt.Close()

			// Try to execute with parameter
			rows, err := stmt.Query(param)
			if err == nil {
				for rows.Next() {
					var dummy interface{}
					_ = rows.Scan(&dummy)
				}
				rows.Close()
			} else {
				// Try as exec
				_, _ = stmt.Exec(param)
			}
		}
	})
}

// FuzzTransaction tests transaction operations with random SQL sequences
func FuzzTransaction(f *testing.F) {
	seeds := []string{
		"BEGIN; SELECT 1; COMMIT",
		"BEGIN; SELECT 1; ROLLBACK",
		"BEGIN; CREATE TABLE t (id INT); COMMIT",
		"BEGIN; INSERT INTO t VALUES (1); COMMIT",
		"BEGIN; UPDATE t SET id = 2; ROLLBACK",
		"BEGIN; DELETE FROM t; COMMIT",
		"SELECT 1",             // No transaction
		"COMMIT",               // Commit without begin
		"ROLLBACK",             // Rollback without begin
		"BEGIN; BEGIN; COMMIT", // Nested begin
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sqlStr string) {
		// Skip extremely long inputs
		if len(sqlStr) > 10000 {
		}

		// Create in-memory database
		db, err := sql.Open(DriverName, ":memory:")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Transaction panicked on: %q\nPanic: %v", sqlStr, r)
			}
		}()

		// Execute SQL (may be multiple statements)
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, sqlStr)
	})
}

// FuzzConcurrentAccess tests concurrent database access with random SQL
func FuzzConcurrentAccess(f *testing.F) {
	seeds := []string{
		"SELECT 1",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET id = 2",
		"DELETE FROM t",
		"CREATE TABLE t (id INT)",
		"DROP TABLE t",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sqlStr string) {
		// Skip extremely long inputs
		if len(sqlStr) > 1000 {
		}

		// Create in-memory database
		db, err := sql.Open(DriverName, ":memory:")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		// Should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Concurrent access panicked on: %q\nPanic: %v", sqlStr, r)
			}
		}()

		// Execute same SQL from multiple goroutines
		done := make(chan bool)
		for i := 0; i < 3; i++ {
			go func() {
				defer func() { done <- true }()
				ctx := context.Background()
				_, _ = db.ExecContext(ctx, sqlStr)
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 3; i++ {
			<-done
		}
	})
}

// TestFuzzRegressionDriver tests against known problematic inputs
func TestFuzzRegressionDriver(t *testing.T) {
	regressionInputs := []string{
		"",
		"\x00",
		strings.Repeat("\x00", 100),
		strings.Repeat("SELECT 1 UNION ", 100) + "SELECT 1",
		"SELECT * FROM (",
		"SELECT * FROM (SELECT * FROM",
		"CREATE TABLE t (",
		"INSERT INTO t VALUES (",
		strings.Repeat("(", 100),
		strings.Repeat(")", 100),
	}

	for i, input := range regressionInputs {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic on regression input %d: %v\nInput: %q", i, r, input)
				}
			}()

			db, err := sql.Open(DriverName, ":memory:")
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			ctx := context.Background()
			_, _ = db.ExecContext(ctx, input)
		})
	}
}
