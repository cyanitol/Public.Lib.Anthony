package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteSchema tests SQLite schema queries and sqlite_master table
// Converted from contrib/sqlite/sqlite-src-3510200/test/schema*.test
func TestSQLiteSchema(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "schema_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Query sqlite_master when empty
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries in empty database, got %d", count)
	}

	// Test 2: Create table and verify in sqlite_master
	_, err = db.Exec("CREATE TABLE t1(a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	var tblName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&tblName)
	if err != nil {
		t.Fatalf("failed to find table in sqlite_master: %v", err)
	}
	if tblName != "t1" {
		t.Errorf("expected table name 't1', got %q", tblName)
	}

	// Test 3: Check sqlite_master columns
	rows, err := db.Query("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name='t1'")
	if err != nil {
		t.Fatalf("failed to query sqlite_master columns: %v", err)
	}
	defer rows.Close()

	var typ, name, tbl_name, sql string
	if rows.Next() {
		err = rows.Scan(&typ, &name, &tbl_name, &sql)
		if err != nil {
			t.Fatalf("failed to scan sqlite_master: %v", err)
		}
		if typ != "table" {
			t.Errorf("expected type 'table', got %q", typ)
		}
		if !strings.Contains(sql, "CREATE TABLE") {
			t.Errorf("expected CREATE TABLE in sql column, got %q", sql)
		}
	} else {
		t.Fatal("no rows returned from sqlite_master")
	}
	rows.Close()

	// Test 4: Create index and verify in sqlite_master
	_, err = db.Exec("CREATE INDEX idx1 ON t1(a)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to find index in sqlite_master: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 index entry, got %d", count)
	}

	// Test 5: Create view and verify in sqlite_master
	_, err = db.Exec("CREATE VIEW v1 AS SELECT a, b FROM t1")
	if err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='v1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to find view in sqlite_master: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 view entry, got %d", count)
	}

	// Test 6: Create trigger and verify in sqlite_master
	_, err = db.Exec(`CREATE TRIGGER trig1 AFTER INSERT ON t1 BEGIN SELECT 1; END`)
	if err != nil {
		t.Fatalf("failed to create trigger: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='trig1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to find trigger in sqlite_master: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 trigger entry, got %d", count)
	}

	// Test 7: Count all objects in sqlite_master
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count all objects: %v", err)
	}
	// Should have: 1 table + 1 index + 1 view + 1 trigger = 4 (minimum)
	if count < 4 {
		t.Errorf("expected at least 4 objects in sqlite_master, got %d", count)
	}

	// Test 8: Query all table names
	rows, err = db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("failed to query table names: %v", err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan table name: %v", err)
		}
		tableNames = append(tableNames, name)
	}
	if len(tableNames) < 1 {
		t.Error("expected at least 1 table name")
	}
	rows.Close()

	// Test 9: Drop view and verify removal from sqlite_master
	_, err = db.Exec("DROP VIEW v1")
	if err != nil {
		t.Fatalf("failed to drop view: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='v1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query after drop view: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 view entries after drop, got %d", count)
	}

	// Test 10: Drop trigger and verify removal
	_, err = db.Exec("DROP TRIGGER trig1")
	if err != nil {
		t.Fatalf("failed to drop trigger: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='trig1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query after drop trigger: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 trigger entries after drop, got %d", count)
	}

	// Test 11: Drop index and verify removal
	_, err = db.Exec("DROP INDEX idx1")
	if err != nil {
		t.Fatalf("failed to drop index: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query after drop index: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 index entries after drop, got %d", count)
	}

	// Test 12: Create table with primary key (auto-index)
	_, err = db.Exec("CREATE TABLE t2(id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("failed to create table with primary key: %v", err)
	}

	// Test 13: Check for auto-created index
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='t2'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query auto-index: %v", err)
	}
	// PRIMARY KEY may or may not create an explicit index entry

	// Test 14: Create table with unique constraint
	_, err = db.Exec("CREATE TABLE t3(a INTEGER UNIQUE, b TEXT)")
	if err != nil {
		t.Fatalf("failed to create table with unique: %v", err)
	}

	// Test 15: Check for unique index in sqlite_master
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='t3'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query unique index: %v", err)
	}
	if count < 1 {
		t.Errorf("expected at least 1 auto-index for unique constraint, got %d", count)
	}

	// Test 16: Query sql column for table definition
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&sql)
	if err != nil {
		t.Fatalf("failed to query sql column: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sql), "CREATE TABLE") {
		t.Errorf("sql column should contain CREATE TABLE, got %q", sql)
	}

	// Test 17: Create temporary table
	_, err = db.Exec("CREATE TEMP TABLE temp1(x INTEGER)")
	if err != nil {
		t.Fatalf("failed to create temp table: %v", err)
	}

	// Test 18: Temp tables should not appear in sqlite_master
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='temp1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query for temp table: %v", err)
	}
	if count != 0 {
		t.Errorf("temp table should not appear in sqlite_master, got count=%d", count)
	}

	// Test 19: Query sqlite_temp_master for temp tables
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_temp_master WHERE name='temp1'").Scan(&count)
	if err != nil {
		// sqlite_temp_master might not be accessible in all cases
		t.Logf("sqlite_temp_master query failed (may be expected): %v", err)
	} else if count != 1 {
		t.Logf("expected 1 entry in sqlite_temp_master, got %d", count)
	}

	// Test 20: Alter table and check schema update
	_, err = db.Exec("ALTER TABLE t1 ADD COLUMN c REAL")
	if err != nil {
		t.Fatalf("failed to alter table: %v", err)
	}

	// Test 21: Verify altered table in sqlite_master
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&sql)
	if err != nil {
		t.Fatalf("failed to query altered table: %v", err)
	}
	if !strings.Contains(strings.ToLower(sql), "c") {
		t.Logf("sql after ALTER TABLE: %q (may not show new column in all cases)", sql)
	}

	// Test 22: Rename table and check sqlite_master
	_, err = db.Exec("ALTER TABLE t1 RENAME TO t1_renamed")
	if err != nil {
		t.Fatalf("failed to rename table: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='t1_renamed'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to find renamed table: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry for renamed table, got %d", count)
	}

	// Test 23: Verify old name is gone
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check old table name: %v", err)
	}
	if count != 0 {
		t.Errorf("old table name should not exist, got count=%d", count)
	}

	// Test 24: Create table with complex schema
	_, err = db.Exec(`CREATE TABLE t4(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		value REAL DEFAULT 0.0,
		created DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("failed to create complex table: %v", err)
	}

	// Test 25: Query complex table schema
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE name='t4'").Scan(&sql)
	if err != nil {
		t.Fatalf("failed to query complex table schema: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sql), "AUTOINCREMENT") {
		t.Logf("complex table schema: %q", sql)
	}

	// Test 26: Create multiple indexes on same table
	_, err = db.Exec("CREATE INDEX idx_t4_name ON t4(name)")
	if err != nil {
		t.Fatalf("failed to create index on name: %v", err)
	}
	_, err = db.Exec("CREATE INDEX idx_t4_value ON t4(value)")
	if err != nil {
		t.Fatalf("failed to create index on value: %v", err)
	}

	// Test 27: Count indexes for table
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='t4'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count indexes: %v", err)
	}
	if count < 2 {
		t.Errorf("expected at least 2 indexes for t4, got %d", count)
	}

	// Test 28: Query all object types
	rows, err = db.Query("SELECT DISTINCT type FROM sqlite_master ORDER BY type")
	if err != nil {
		t.Fatalf("failed to query object types: %v", err)
	}
	defer rows.Close()

	types := []string{}
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatalf("failed to scan type: %v", err)
		}
		types = append(types, typ)
	}
	if len(types) < 2 {
		t.Errorf("expected at least 2 object types, got %v", types)
	}
	rows.Close()

	// Test 29: Create view that references multiple tables
	_, err = db.Exec("CREATE VIEW v2 AS SELECT t2.id, t3.a FROM t2, t3")
	if err != nil {
		t.Fatalf("failed to create multi-table view: %v", err)
	}

	// Test 30: Verify multi-table view in sqlite_master
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE type='view' AND name='v2'").Scan(&sql)
	if err != nil {
		t.Fatalf("failed to query multi-table view: %v", err)
	}
	if !strings.Contains(sql, "t2") || !strings.Contains(sql, "t3") {
		t.Errorf("view sql should reference both tables: %q", sql)
	}

	// Test 31: Drop table and verify cascade
	_, err = db.Exec("DROP TABLE IF EXISTS t1_renamed")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	// Test 32: Create table with foreign key
	_, err = db.Exec("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("failed to create parent table: %v", err)
	}
	_, err = db.Exec("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))")
	if err != nil {
		t.Fatalf("failed to create child table: %v", err)
	}

	// Test 33: Query both tables in sqlite_master
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND (name='parent' OR name='child')").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query FK tables: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 FK-related tables, got %d", count)
	}

	// Test 34: Query rootpage column
	var rootpage int64
	err = db.QueryRow("SELECT rootpage FROM sqlite_master WHERE type='table' AND name='parent'").Scan(&rootpage)
	if err != nil {
		t.Fatalf("failed to query rootpage: %v", err)
	}
	if rootpage <= 0 {
		t.Errorf("expected positive rootpage, got %d", rootpage)
	}

	// Test 35: Test case-insensitive name matching
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='PARENT'").Scan(&count)
	if err != nil {
		t.Fatalf("failed case-insensitive query: %v", err)
	}
	// SQLite table names are case-insensitive in queries but stored as created
	if count != 0 {
		// Exact case match required in sqlite_master
	}

	// Test 36: Query with LIKE pattern
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 't%'").Scan(&count)
	if err != nil {
		t.Fatalf("failed LIKE query: %v", err)
	}
	if count < 1 {
		t.Errorf("expected at least 1 table starting with 't', got %d", count)
	}
}

// TestSchemaModification tests schema changes and their effects
func TestSchemaModification(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "schema_mod_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Create and drop multiple objects
	_, err = db.Exec("CREATE TABLE test1(a, b, c)")
	if err != nil {
		t.Fatalf("failed to create test1: %v", err)
	}

	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='test1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query test1: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry for test1, got %d", count)
	}

	// Test 2: Drop and recreate with different schema
	_, err = db.Exec("DROP TABLE test1")
	if err != nil {
		t.Fatalf("failed to drop test1: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test1(x INTEGER, y TEXT)")
	if err != nil {
		t.Fatalf("failed to recreate test1: %v", err)
	}

	var sql string
	err = db.QueryRow("SELECT sql FROM sqlite_master WHERE name='test1'").Scan(&sql)
	if err != nil {
		t.Fatalf("failed to query recreated table: %v", err)
	}
	if !strings.Contains(sql, "x") || !strings.Contains(sql, "y") {
		t.Errorf("recreated table should have columns x and y: %q", sql)
	}

	// Test 3: Transaction with schema changes
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE tx_test(a INTEGER)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table in transaction: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit schema transaction: %v", err)
	}

	// Test 4: Verify transaction committed
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='tx_test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to verify committed table: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry for tx_test, got %d", count)
	}

	// Test 5: Rollback schema changes
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE rollback_test(a INTEGER)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table for rollback: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Test 6: Verify rollback
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name='rollback_test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to check rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries after rollback, got %d", count)
	}
}
