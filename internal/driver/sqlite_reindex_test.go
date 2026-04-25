// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func reindexExecAll(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Errorf("exec %q failed: %v", stmt, err)
		}
	}
}

func reindexAssertInts(t *testing.T, db *sql.DB, query string, want []int) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, v)
	}
	if len(got) != len(want) {
		t.Errorf("expected %d rows, got %d", len(want), len(got))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: expected %d, got %d", i, want[i], got[i])
		}
	}
}

func reindexAssertCount(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var count int64
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != want {
		t.Errorf("expected count %d, got %d", want, count)
	}
}

// TestSQLiteReindex tests the REINDEX command
// Converted from contrib/sqlite/sqlite-src-3510200/test/reindex.test
func TestSQLiteReindex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Basic sanity checks (reindex-1.1-1.8)
	t.Run("basic_reindex", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE t1(a, b);
			INSERT INTO t1 VALUES(1, 2);
			INSERT INTO t1 VALUES(3, 4);
			CREATE INDEX i1 ON t1(a);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		reindexExecAll(t, db, []string{
			"REINDEX",
			"REINDEX t1",
			"REINDEX i1",
			"REINDEX main.t1",
			"REINDEX main.i1",
		})

		reindexAssertCount(t, db, "SELECT COUNT(*) FROM t1", 2)
	})

	// Test 2: REINDEX on non-existent object
	t.Run("reindex_bogus", func(t *testing.T) {
		_, err := db.Exec("REINDEX bogus")
		if err == nil {
			t.Error("expected error for REINDEX on non-existent object")
		}
	})

	// Test 3: Verify data after reindex
	t.Run("verify_data_after_reindex", func(t *testing.T) {
		reindexAssertInts(t, db, "SELECT a FROM t1 ORDER BY a", []int{1, 3})
	})
}

// TestReindexWithCollation tests REINDEX with custom collation
// Based on reindex-2.* tests
func TestReindexWithCollation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_collation_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("reindex_nocase_collation", func(t *testing.T) {
		// Create table with NOCASE collation
		_, err := db.Exec(`
			CREATE TABLE t2(
				a TEXT PRIMARY KEY COLLATE nocase,
				b TEXT UNIQUE COLLATE nocase,
				c TEXT COLLATE nocase,
				d TEXT COLLATE binary
			);
			INSERT INTO t2 VALUES('abc', 'abc', 'abc', 'abc');
			INSERT INTO t2 VALUES('ABCD', 'ABCD', 'ABCD', 'ABCD');
			INSERT INTO t2 VALUES('bcd', 'bcd', 'bcd', 'bcd');
			INSERT INTO t2 VALUES('BCDE', 'BCDE', 'BCDE', 'BCDE');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// REINDEX the database
		_, err = db.Exec("REINDEX")
		if err != nil {
			t.Fatalf("REINDEX failed: %v", err)
		}

		// Verify order with NOCASE collation on column c
		rows, err := db.Query("SELECT c FROM t2 ORDER BY c")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		var results []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			results = append(results, c)
		}

		// With NOCASE, order should be case-insensitive
		if len(results) != 4 {
			t.Errorf("expected 4 rows, got %d", len(results))
		}
	})
}

// TestReindexMultipleIndexes tests REINDEX with multiple indexes
func TestReindexMultipleIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_multi_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("multiple_indexes", func(t *testing.T) {
		// Create table with multiple indexes
		_, err := db.Exec(`
			CREATE TABLE t3(id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
			CREATE INDEX idx_name ON t3(name);
			CREATE INDEX idx_age ON t3(age);
			CREATE INDEX idx_name_age ON t3(name, age);

			INSERT INTO t3(name, age) VALUES('Alice', 30);
			INSERT INTO t3(name, age) VALUES('Bob', 25);
			INSERT INTO t3(name, age) VALUES('Charlie', 35);
		`)
		if err != nil {
			t.Fatalf("failed to create table and indexes: %v", err)
		}

		// REINDEX all
		_, err = db.Exec("REINDEX")
		if err != nil {
			t.Errorf("REINDEX failed: %v", err)
		}

		// REINDEX specific index
		_, err = db.Exec("REINDEX idx_name")
		if err != nil {
			t.Errorf("REINDEX idx_name failed: %v", err)
		}

		_, err = db.Exec("REINDEX idx_age")
		if err != nil {
			t.Errorf("REINDEX idx_age failed: %v", err)
		}

		// Verify data is still accessible via indexes
		var name string
		err = db.QueryRow("SELECT name FROM t3 WHERE age = 25").Scan(&name)
		if err != nil {
			t.Fatalf("query with index failed: %v", err)
		}
		if name != "Bob" {
			t.Errorf("expected 'Bob', got %q", name)
		}
	})
}

// TestReindexAfterInsert tests REINDEX after inserting data
func TestReindexAfterInsert(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_insert_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("reindex_after_bulk_insert", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE t4(value INTEGER);
			CREATE INDEX idx_value ON t4(value);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err = db.Exec("INSERT INTO t4(value) VALUES(?)", i); err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		reindexExecAll(t, db, []string{"REINDEX t4"})
		reindexAssertCount(t, db, "SELECT COUNT(*) FROM t4", 100)
		reindexAssertCount(t, db, "SELECT SUM(value) FROM t4 WHERE value >= 50 AND value < 60",
			int64(50+51+52+53+54+55+56+57+58+59))
	})
}

// TestReindexWithoutRowid tests REINDEX on WITHOUT ROWID tables
// Based on reindex-4.* tests
func TestReindexWithoutRowid(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_without_rowid_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("without_rowid_desc", func(t *testing.T) {
		_, err := db.Exec(`
			CREATE TABLE t5(
				c0 INTEGER PRIMARY KEY DESC,
				c1 UNIQUE DEFAULT NULL
			) WITHOUT ROWID;
			INSERT INTO t5(c0) VALUES (1), (2), (3), (4), (5);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		query := "SELECT c0 FROM t5 WHERE c1 IS NULL ORDER BY c0"
		expected := []int{1, 2, 3, 4, 5}

		reindexAssertInts(t, db, query, expected)

		if _, err = db.Exec("REINDEX"); err != nil {
			t.Fatalf("REINDEX failed: %v", err)
		}

		reindexAssertInts(t, db, query, expected)
	})
}

// TestReindexPartialIndex tests REINDEX with partial indexes
func TestReindexPartialIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_partial_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("partial_index", func(t *testing.T) {
		// Create table with partial index
		_, err := db.Exec(`
			CREATE TABLE t6(id INTEGER, status TEXT);
			CREATE INDEX idx_active ON t6(id) WHERE status = 'active';

			INSERT INTO t6 VALUES(1, 'active');
			INSERT INTO t6 VALUES(2, 'inactive');
			INSERT INTO t6 VALUES(3, 'active');
			INSERT INTO t6 VALUES(4, 'inactive');
			INSERT INTO t6 VALUES(5, 'active');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// REINDEX
		_, err = db.Exec("REINDEX idx_active")
		if err != nil {
			t.Errorf("REINDEX idx_active failed: %v", err)
		}

		// Verify partial index works
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE status = 'active'").Scan(&count)
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3 active rows, got %d", count)
		}
	})
}

// TestReindexCompositeIndex tests REINDEX with composite indexes
func TestReindexCompositeIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_composite_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("composite_index", func(t *testing.T) {
		// Create table with composite index
		_, err := db.Exec(`
			CREATE TABLE t7(a INTEGER, b INTEGER, c TEXT);
			CREATE INDEX idx_ab ON t7(a, b);
			CREATE INDEX idx_ba ON t7(b, a);

			INSERT INTO t7 VALUES(1, 10, 'row1');
			INSERT INTO t7 VALUES(2, 20, 'row2');
			INSERT INTO t7 VALUES(1, 20, 'row3');
			INSERT INTO t7 VALUES(2, 10, 'row4');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// REINDEX both composite indexes
		_, err = db.Exec("REINDEX idx_ab")
		if err != nil {
			t.Errorf("REINDEX idx_ab failed: %v", err)
		}

		_, err = db.Exec("REINDEX idx_ba")
		if err != nil {
			t.Errorf("REINDEX idx_ba failed: %v", err)
		}

		// Verify queries work correctly
		var c string
		err = db.QueryRow("SELECT c FROM t7 WHERE a = 1 AND b = 20").Scan(&c)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if c != "row3" {
			t.Errorf("expected 'row3', got %q", c)
		}
	})
}

// TestReindexAfterUpdate tests REINDEX after UPDATE operations
func TestReindexAfterUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_update_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("reindex_after_update", func(t *testing.T) {
		// Create table
		_, err := db.Exec(`
			CREATE TABLE t8(id INTEGER PRIMARY KEY, value TEXT);
			CREATE INDEX idx_value ON t8(value);

			INSERT INTO t8(value) VALUES('alpha');
			INSERT INTO t8(value) VALUES('beta');
			INSERT INTO t8(value) VALUES('gamma');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Update values
		_, err = db.Exec("UPDATE t8 SET value = 'ALPHA' WHERE value = 'alpha'")
		if err != nil {
			t.Fatalf("update failed: %v", err)
		}

		// REINDEX
		_, err = db.Exec("REINDEX t8")
		if err != nil {
			t.Errorf("REINDEX failed: %v", err)
		}

		// Verify updated value
		var value string
		err = db.QueryRow("SELECT value FROM t8 WHERE id = 1").Scan(&value)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if value != "ALPHA" {
			t.Errorf("expected 'ALPHA', got %q", value)
		}
	})
}

// TestReindexAfterDelete tests REINDEX after DELETE operations
func TestReindexAfterDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_delete_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("reindex_after_delete", func(t *testing.T) {
		reindexDeleteSetup(t, db)
		reindexDeletePopulate(t, db)
		reindexDeleteVerify(t, db)
	})
}

func reindexDeleteSetup(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE t9(id INTEGER PRIMARY KEY, value INTEGER);
		CREATE INDEX idx_value ON t9(value);
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

func reindexDeletePopulate(t *testing.T, db *sql.DB) {
	t.Helper()
	for i := 1; i <= 20; i++ {
		if _, err := db.Exec("INSERT INTO t9(value) VALUES(?)", i*10); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	_, err := db.Exec("DELETE FROM t9 WHERE value > 100")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = db.Exec("REINDEX t9")
	if err != nil {
		t.Errorf("REINDEX failed: %v", err)
	}
}

func reindexDeleteVerify(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t9").Scan(&count); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10 rows, got %d", count)
	}
}

// TestReindexEmptyTable tests REINDEX on an empty table
func TestReindexEmptyTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reindex_empty_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("empty_table", func(t *testing.T) {
		// Create empty table with index
		_, err := db.Exec(`
			CREATE TABLE t10(id INTEGER PRIMARY KEY, data TEXT);
			CREATE INDEX idx_data ON t10(data);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// REINDEX empty table
		_, err = db.Exec("REINDEX t10")
		if err != nil {
			t.Errorf("REINDEX on empty table failed: %v", err)
		}

		// Verify it's still empty
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM t10").Scan(&count)
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 rows, got %d", count)
		}
	})
}
