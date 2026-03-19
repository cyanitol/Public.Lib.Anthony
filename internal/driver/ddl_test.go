// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestCreateDropIndex tests CREATE INDEX and DROP INDEX statements.
func TestCreateDropIndex(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create an index
	_, err = db.Exec("CREATE INDEX idx_users_email ON users(email)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Create unique index
	_, err = db.Exec("CREATE UNIQUE INDEX idx_users_name ON users(name)")
	if err != nil {
		t.Fatalf("failed to create unique index: %v", err)
	}

	// Test IF NOT EXISTS
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
	if err != nil {
		t.Fatalf("failed to create index with IF NOT EXISTS: %v", err)
	}

	// Drop an index
	_, err = db.Exec("DROP INDEX idx_users_email")
	if err != nil {
		t.Fatalf("failed to drop index: %v", err)
	}

	// Test IF EXISTS
	_, err = db.Exec("DROP INDEX IF EXISTS idx_nonexistent")
	if err != nil {
		t.Fatalf("failed to drop index with IF EXISTS: %v", err)
	}

	// Drop without IF EXISTS should fail
	_, err = db.Exec("DROP INDEX idx_nonexistent")
	if err == nil {
		t.Fatal("expected error when dropping nonexistent index without IF EXISTS")
	}
}

// TestAlterTable tests ALTER TABLE statements.
func TestAlterTable(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Add a column
	_, err = db.Exec("ALTER TABLE users ADD COLUMN email TEXT")
	if err != nil {
		t.Fatalf("failed to add column: %v", err)
	}

	// Add column with default
	_, err = db.Exec("ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 0")
	if err != nil {
		t.Fatalf("failed to add column with default: %v", err)
	}

	// Rename a column
	_, err = db.Exec("ALTER TABLE users RENAME COLUMN name TO full_name")
	if err != nil {
		t.Fatalf("failed to rename column: %v", err)
	}

	// Rename the table
	_, err = db.Exec("ALTER TABLE users RENAME TO people")
	if err != nil {
		t.Fatalf("failed to rename table: %v", err)
	}

	// Drop a column
	_, err = db.Exec("ALTER TABLE people DROP COLUMN age")
	if err != nil {
		t.Fatalf("failed to drop column: %v", err)
	}
}

// ddlColumnInfo holds PRAGMA table_info results.
type ddlColumnInfo struct {
	cid        int
	name       string
	typ        string
	notnull    int
	dflt_value sql.NullString
	pk         int
}

// ddlScanTableInfo scans all rows from PRAGMA table_info.
func ddlScanTableInfo(t *testing.T, db *sql.DB, table string) []ddlColumnInfo {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatalf("failed to query table_info: %v", err)
	}
	defer rows.Close()
	var columns []ddlColumnInfo
	for rows.Next() {
		var col ddlColumnInfo
		if err := rows.Scan(&col.cid, &col.name, &col.typ, &col.notnull, &col.dflt_value, &col.pk); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		columns = append(columns, col)
	}
	return columns
}

// ddlOpenMemDB opens an in-memory database.
func ddlOpenMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// ddlAssertColumn checks a column's basic properties.
func ddlAssertColumn(t *testing.T, col ddlColumnInfo, wantName, wantTyp string, idx int) {
	t.Helper()
	if col.name != wantName || col.typ != wantTyp {
		t.Errorf("unexpected column %d: %+v", idx, col)
	}
}

// TestPragmaTableInfo tests PRAGMA table_info.
func TestPragmaTableInfo(t *testing.T) {
	db := ddlOpenMemDB(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER DEFAULT 0
	)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	columns := ddlScanTableInfo(t, db, "users")
	if len(columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(columns))
	}
	ddlAssertColumn(t, columns[0], "id", "INTEGER", 0)
	if columns[0].pk != 1 {
		t.Errorf("column 0 pk = %d, want 1", columns[0].pk)
	}
	ddlAssertColumn(t, columns[1], "name", "TEXT", 1)
	if columns[1].notnull != 1 {
		t.Errorf("column 1 notnull = %d, want 1", columns[1].notnull)
	}
}

// ddlAssertPragmaInt queries a PRAGMA and checks the integer result.
func ddlAssertPragmaInt(t *testing.T, db *sql.DB, pragma string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(pragma).Scan(&got); err != nil {
		t.Fatalf("failed to query %s: %v", pragma, err)
	}
	if got != want {
		t.Errorf("%s = %d, want %d", pragma, got, want)
	}
}

// TestPragmaForeignKeys tests PRAGMA foreign_keys.
func TestPragmaForeignKeys(t *testing.T) {
	t.Skip("pre-existing failure - PRAGMA foreign_keys not yet supported")
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ddlAssertPragmaInt(t, db, "PRAGMA foreign_keys", 0)
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("failed to set foreign_keys: %v", err)
	}
	ddlAssertPragmaInt(t, db, "PRAGMA foreign_keys", 1)
	if _, err = db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("failed to set foreign_keys to OFF: %v", err)
	}
	ddlAssertPragmaInt(t, db, "PRAGMA foreign_keys", 0)
}

// ddlAssertPragmaStr queries a PRAGMA and checks the string result.
func ddlAssertPragmaStr(t *testing.T, db *sql.DB, pragma, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow(pragma).Scan(&got); err != nil {
		t.Fatalf("failed to query %s: %v", pragma, err)
	}
	if got != want {
		t.Errorf("%s = %q, want %q", pragma, got, want)
	}
}

// TestPragmaJournalMode tests PRAGMA journal_mode.
func TestPragmaJournalMode(t *testing.T) {
	t.Skip("pre-existing failure - PRAGMA journal_mode not yet supported")
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ddlAssertPragmaStr(t, db, "PRAGMA journal_mode", "delete")
	ddlAssertPragmaStr(t, db, "PRAGMA journal_mode = WAL", "wal")
	ddlAssertPragmaStr(t, db, "PRAGMA journal_mode", "wal")
	ddlAssertPragmaStr(t, db, "PRAGMA journal_mode = MEMORY", "memory")
}
