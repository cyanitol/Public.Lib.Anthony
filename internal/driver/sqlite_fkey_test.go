// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

// TestForeignKey_PragmaForeignKeys tests the PRAGMA foreign_keys setting.
// Based on fkey1.test and fkey2.test.
func TestForeignKey_PragmaForeignKeys(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Foreign keys should be off by default
	var enabled int
	err := db.QueryRow("PRAGMA foreign_keys").Scan(&enabled)
	if err != nil {
		t.Fatalf("Failed to query foreign_keys pragma: %v", err)
	}
	if enabled != 0 {
		t.Errorf("Expected foreign_keys to be 0 (disabled) by default, got %d", enabled)
	}

	// Enable foreign keys
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign_keys: %v", err)
	}

	err = db.QueryRow("PRAGMA foreign_keys").Scan(&enabled)
	if err != nil {
		t.Fatalf("Failed to query foreign_keys pragma: %v", err)
	}
	if enabled != 1 {
		t.Errorf("Expected foreign_keys to be 1 (enabled), got %d", enabled)
	}

	// Disable foreign keys
	_, err = db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		t.Fatalf("Failed to disable foreign_keys: %v", err)
	}

	err = db.QueryRow("PRAGMA foreign_keys").Scan(&enabled)
	if err != nil {
		t.Fatalf("Failed to query foreign_keys pragma: %v", err)
	}
	if enabled != 0 {
		t.Errorf("Expected foreign_keys to be 0 (disabled), got %d", enabled)
	}
}

// TestForeignKey_BasicDefinition tests basic foreign key definition.
// Based on fkey1-1.* tests.
func TestForeignKey_BasicDefinition(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Create table with inline foreign key
	_, err := db.Exec(`
		CREATE TABLE t1(
			a INTEGER PRIMARY KEY,
			b INTEGER REFERENCES t1 ON DELETE CASCADE,
			c TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table with inline FK: %v", err)
	}

	// Create table with table-level foreign key
	_, err = db.Exec(`
		CREATE TABLE t2(
			x INTEGER PRIMARY KEY,
			y TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t3(
			a INTEGER REFERENCES t2,
			b INTEGER REFERENCES t1,
			FOREIGN KEY (a, b) REFERENCES t2(x, y)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table with multi-column FK: %v", err)
	}
}

// TestForeignKey_ForeignKeyList tests PRAGMA foreign_key_list.
// Based on fkey1-3.* tests.
func TestForeignKey_ForeignKeyList(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec(`
		CREATE TABLE t5(a PRIMARY KEY, b, c);
		CREATE TABLE t6(
			d REFERENCES t5,
			e REFERENCES t5(c)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Query foreign key list
	rows, err := db.Query("PRAGMA foreign_key_list(t6)")
	if err != nil {
		t.Fatalf("Failed to query foreign_key_list: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id, seq int
		var table, from, to, onUpdate, onDelete, match string
		err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match)
		if err != nil {
			t.Fatalf("Failed to scan foreign_key_list row: %v", err)
		}

		if table != "t5" {
			t.Errorf("Expected table 't5', got '%s'", table)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 foreign keys, got %d", count)
	}
}

// TestForeignKey_OnDeleteActions tests ON DELETE actions.
// Based on fkey1-3.* and fkey2.test.
func TestForeignKey_OnDeleteActions(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	tests := []struct {
		name   string
		action string
	}{
		{"CASCADE", "CASCADE"},
		{"SET NULL", "SET NULL"},
		{"SET DEFAULT", "SET DEFAULT"},
		{"RESTRICT", "RESTRICT"},
		{"NO ACTION", "NO ACTION"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Drop in correct order: child before parent (child may reference parent)
			db.Exec(`DROP TABLE IF EXISTS child`)
			db.Exec(`DROP TABLE IF EXISTS parent`)

			_, err := db.Exec(`CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)`)
			if err != nil {
				t.Fatalf("Failed to create parent table: %v", err)
			}

			sql := `CREATE TABLE child(
				id INTEGER PRIMARY KEY,
				parent_id INTEGER REFERENCES parent ON DELETE ` + tt.action + `
			)`
			_, err = db.Exec(sql)
			if err != nil {
				t.Fatalf("Failed to create child table with %s: %v", tt.action, err)
			}
		})
	}
}

// TestForeignKey_OnUpdateActions tests ON UPDATE actions.
// Based on fkey1-3.* tests.
func TestForeignKey_OnUpdateActions(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	tests := []struct {
		name   string
		action string
	}{
		{"CASCADE", "CASCADE"},
		{"SET NULL", "SET NULL"},
		{"SET DEFAULT", "SET DEFAULT"},
		{"RESTRICT", "RESTRICT"},
		{"NO ACTION", "NO ACTION"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Drop in correct order: child before parent (child may reference parent)
			db.Exec(`DROP TABLE IF EXISTS child`)
			db.Exec(`DROP TABLE IF EXISTS parent`)

			_, err := db.Exec(`CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)`)
			if err != nil {
				t.Fatalf("Failed to create parent table: %v", err)
			}

			sql := `CREATE TABLE child(
				id INTEGER PRIMARY KEY,
				parent_id INTEGER REFERENCES parent ON UPDATE ` + tt.action + `
			)`
			_, err = db.Exec(sql)
			if err != nil {
				t.Fatalf("Failed to create child table with %s: %v", tt.action, err)
			}
		})
	}
}

// TestForeignKey_SimpleInsertViolation tests basic FK constraint violation on INSERT.
// Based on fkey2-1.1.* tests.
func TestForeignKey_SimpleInsertViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(a PRIMARY KEY, b);
		CREATE TABLE t2(c REFERENCES t1(a), d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Try to insert with non-existent foreign key
	_, err = db.Exec("INSERT INTO t2 VALUES(1, 3)")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}

	// Insert parent record
	_, err = db.Exec("INSERT INTO t1 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("Failed to insert parent record: %v", err)
	}

	// Now insert should succeed
	_, err = db.Exec("INSERT INTO t2 VALUES(1, 3)")
	if err != nil {
		t.Errorf("Expected successful insert, got: %v", err)
	}

	// NULL values should be allowed
	_, err = db.Exec("INSERT INTO t2 VALUES(NULL, 4)")
	if err != nil {
		t.Errorf("Expected NULL FK value to succeed, got: %v", err)
	}
}

// TestForeignKey_SimpleDeleteViolation tests FK constraint violation on DELETE.
// Based on fkey2-1.1.* tests.
func TestForeignKey_SimpleDeleteViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(a PRIMARY KEY, b);
		CREATE TABLE t2(c REFERENCES t1(a), d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO t2 VALUES(1, 3)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to delete parent - should fail
	_, err = db.Exec("DELETE FROM t1 WHERE a=1")
	if err == nil {
		t.Fatal("Expected FK constraint error on delete, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}
}

// TestForeignKey_SimpleUpdateViolation tests FK constraint violation on UPDATE.
// Based on fkey2-1.1.* tests.
func TestForeignKey_SimpleUpdateViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(a PRIMARY KEY, b);
		CREATE TABLE t2(c REFERENCES t1(a), d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO t2 VALUES(1, 3)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to update child to invalid FK - should fail
	_, err = db.Exec("UPDATE t2 SET c=2 WHERE d=3")
	if err == nil {
		t.Error("Expected FK constraint error on update, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}

	// Try to update parent key - should fail (child references it)
	_, err = db.Exec("UPDATE t1 SET a=2 WHERE a=1")
	if err == nil {
		t.Error("Expected FK constraint error on parent update, got nil")
	}
}

// TestForeignKey_DeferredConstraints tests deferred foreign key constraints.
// Based on fkey2-2.* tests.
func TestForeignKey_DeferredConstraints(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE node(
			nodeid PRIMARY KEY,
			parent REFERENCES node DEFERRABLE INITIALLY DEFERRED
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Without transaction, should fail immediately
	_, err = db.Exec("INSERT INTO node VALUES(1, 0)")
	if err == nil {
		t.Error("Expected immediate FK error outside transaction, got nil")
	}

	// Within transaction, should be deferred
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// This should succeed (constraint is deferred)
	_, err = tx.Exec("INSERT INTO node VALUES(1, 0)")
	if err != nil {
		t.Errorf("Expected deferred constraint to allow INSERT, got: %v", err)
	}

	// Fix the constraint violation
	_, err = tx.Exec("UPDATE node SET parent = NULL WHERE nodeid = 1")
	if err != nil {
		t.Fatalf("Failed to update node: %v", err)
	}

	// Now commit should succeed
	err = tx.Commit()
	if err != nil {
		t.Errorf("Expected successful commit after fixing constraint, got: %v", err)
	}
}

// TestForeignKey_DeferredConstraintViolation tests deferred constraint fails at commit.
// Based on fkey2-2.* tests.
func TestForeignKey_DeferredConstraintViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE node(
			nodeid PRIMARY KEY,
			parent REFERENCES node DEFERRABLE INITIALLY DEFERRED
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// This should succeed (constraint is deferred)
	_, err = tx.Exec("INSERT INTO node VALUES(1, 0)")
	if err != nil {
		t.Errorf("Expected deferred constraint to allow INSERT, got: %v", err)
	}

	// Don't fix the constraint - commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("Expected FK constraint error on commit, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}
}

// TestForeignKey_OnDeleteCascade tests CASCADE delete action.
// Based on fkey2-11.* tests.
func TestForeignKey_OnDeleteCascade(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO parent VALUES(1, 'parent1')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(20, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent - should cascade to children
	_, err = db.Exec("DELETE FROM parent WHERE id=1")
	if err != nil {
		t.Errorf("Expected CASCADE delete to succeed, got: %v", err)
	}

	// Verify children were deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM child").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count children: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 children after CASCADE delete, got %d", count)
	}
}

// TestForeignKey_OnDeleteSetNull tests SET NULL delete action.
// Based on fkey2-9.* tests.
func TestForeignKey_OnDeleteSetNull(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES parent(id) ON DELETE SET NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO parent VALUES(1, 'parent1')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent - should set child FK to NULL
	_, err = db.Exec("DELETE FROM parent WHERE id=1")
	if err != nil {
		t.Errorf("Expected SET NULL delete to succeed, got: %v", err)
	}

	// Verify child FK was set to NULL
	var parentID sql.NullInt64
	err = db.QueryRow("SELECT parent_id FROM child WHERE id=10").Scan(&parentID)
	if err != nil {
		t.Fatalf("Failed to query child: %v", err)
	}
	if parentID.Valid {
		t.Errorf("Expected parent_id to be NULL, got %d", parentID.Int64)
	}
}

// TestForeignKey_OnUpdateCascade tests CASCADE update action.
// Based on fkey2-11.* tests.
func TestForeignKey_OnUpdateCascade(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES parent(id) ON UPDATE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO parent VALUES(1, 'parent1')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Update parent key - should cascade to child
	_, err = db.Exec("UPDATE parent SET id=2 WHERE id=1")
	if err != nil {
		t.Errorf("Expected CASCADE update to succeed, got: %v", err)
	}

	// Verify child FK was updated
	var parentID int
	err = db.QueryRow("SELECT parent_id FROM child WHERE id=10").Scan(&parentID)
	if err != nil {
		t.Fatalf("Failed to query child: %v", err)
	}
	if parentID != 2 {
		t.Errorf("Expected parent_id to be 2, got %d", parentID)
	}
}

// TestForeignKey_OnUpdateSetNull tests SET NULL update action.
// Based on fkey3-2.* tests.
func TestForeignKey_OnUpdateSetNull(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES parent(id) ON UPDATE SET NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO parent VALUES(1, 'parent1')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Update parent key - should set child FK to NULL
	_, err = db.Exec("UPDATE parent SET id=2 WHERE id=1")
	if err != nil {
		t.Errorf("Expected SET NULL update to succeed, got: %v", err)
	}

	// Verify child FK was set to NULL
	var parentID sql.NullInt64
	err = db.QueryRow("SELECT parent_id FROM child WHERE id=10").Scan(&parentID)
	if err != nil {
		t.Fatalf("Failed to query child: %v", err)
	}
	if parentID.Valid {
		t.Errorf("Expected parent_id to be NULL, got %d", parentID.Int64)
	}
}

// TestForeignKey_SelfReferencing tests self-referencing foreign keys.
// Based on fkey1-5.* and fkey3-3.* tests.
func TestForeignKey_SelfReferencing(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t11(
			x INTEGER PRIMARY KEY,
			parent REFERENCES t11 ON DELETE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create self-referencing table: %v", err)
	}

	// Insert root node (NULL parent)
	_, err = db.Exec("INSERT INTO t11 VALUES(1, NULL)")
	if err != nil {
		t.Fatalf("Failed to insert root: %v", err)
	}

	// Insert child node
	_, err = db.Exec("INSERT INTO t11 VALUES(2, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Insert grandchild node
	_, err = db.Exec("INSERT INTO t11 VALUES(3, 2)")
	if err != nil {
		t.Fatalf("Failed to insert grandchild: %v", err)
	}

	// Delete root - should cascade
	_, err = db.Exec("DELETE FROM t11 WHERE x=1")
	if err != nil {
		t.Errorf("Expected CASCADE delete to succeed, got: %v", err)
	}

	// All nodes should be deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t11").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after CASCADE delete, got %d", count)
	}
}

// TestForeignKey_SelfReferencingInsert tests inserting self-referencing rows.
// Based on fkey3-3.* tests.
func TestForeignKey_SelfReferencingInsert(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t5(
			a INTEGER PRIMARY KEY,
			b REFERENCES t5(a)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with NULL parent (autoincrement will assign rowid)
	_, err = db.Exec("INSERT INTO t5 VALUES(NULL, 1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Try to insert with non-existent parent
	_, err = db.Exec("INSERT INTO t5 VALUES(NULL, 3)")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}
}

// TestForeignKey_MultiColumn tests multi-column foreign keys.
// Based on fkey1-3.* and fkey3-3.* tests.
func TestForeignKey_MultiColumn(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(
			a INTEGER,
			b TEXT,
			c TEXT,
			UNIQUE(a, b)
		);
		CREATE TABLE child(
			x INTEGER,
			y TEXT,
			FOREIGN KEY(x, y) REFERENCES parent(a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Insert parent
	_, err = db.Exec("INSERT INTO parent VALUES(1, 'alpha', 'data')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child with valid FK
	_, err = db.Exec("INSERT INTO child VALUES(1, 'alpha')")
	if err != nil {
		t.Errorf("Expected successful insert, got: %v", err)
	}

	// Try to insert child with invalid FK
	_, err = db.Exec("INSERT INTO child VALUES(1, 'beta')")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}

	// NULL in any column should be allowed
	_, err = db.Exec("INSERT INTO child VALUES(NULL, 'alpha')")
	if err != nil {
		t.Errorf("Expected NULL FK to succeed, got: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(1, NULL)")
	if err != nil {
		t.Errorf("Expected NULL FK to succeed, got: %v", err)
	}
}

// TestForeignKey_DropTableWithReferences tests dropping tables with FK references.
// Based on fkey3-1.* tests.
func TestForeignKey_DropTableWithReferences(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(x INTEGER PRIMARY KEY);
		CREATE TABLE t2(y INTEGER REFERENCES t1(x))
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(100)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO t2 VALUES(100)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to drop parent table with references
	_, err = db.Exec("DROP TABLE t1")
	if err == nil {
		t.Error("Expected FK constraint error when dropping referenced table, got nil")
	}

	// Drop child table first
	_, err = db.Exec("DROP TABLE t2")
	if err != nil {
		t.Fatalf("Failed to drop child table: %v", err)
	}

	// Now dropping parent should succeed
	_, err = db.Exec("DROP TABLE t1")
	if err != nil {
		t.Errorf("Expected successful drop after removing references, got: %v", err)
	}
}

// TestForeignKey_ForeignKeyCheck tests PRAGMA foreign_key_check.
// Based on fkey5.test.
func TestForeignKey_ForeignKeyCheck(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Disable FK enforcement to insert invalid data
	_, err := db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		t.Fatalf("Failed to disable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE p1(a INTEGER PRIMARY KEY);
		CREATE TABLE c1(x INTEGER PRIMARY KEY REFERENCES p1)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO p1 VALUES(88), (89)")
	if err != nil {
		t.Fatalf("Failed to insert parents: %v", err)
	}

	// Insert with valid and invalid FKs
	_, err = db.Exec("INSERT INTO c1 VALUES(90), (87), (88)")
	if err != nil {
		t.Fatalf("Failed to insert children: %v", err)
	}

	// Check for FK violations
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check: %v", err)
	}
	defer rows.Close()

	violations := 0
	for rows.Next() {
		violations++
		var table string
		var rowid sql.NullInt64
		var parent string
		var fkid int
		err := rows.Scan(&table, &rowid, &parent, &fkid)
		if err != nil {
			t.Fatalf("Failed to scan violation: %v", err)
		}

		if table != "c1" {
			t.Errorf("Expected table 'c1', got '%s'", table)
		}
		if parent != "p1" {
			t.Errorf("Expected parent 'p1', got '%s'", parent)
		}
	}

	if violations != 2 {
		t.Errorf("Expected 2 FK violations, got %d", violations)
	}
}

// TestForeignKey_ForeignKeyCheckSpecificTable tests PRAGMA foreign_key_check(table).
// Based on fkey5-1.3.
func TestForeignKey_ForeignKeyCheckSpecificTable(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		t.Fatalf("Failed to disable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE p1(a INTEGER PRIMARY KEY);
		CREATE TABLE c1(x INTEGER PRIMARY KEY REFERENCES p1);
		CREATE TABLE c2(y INTEGER PRIMARY KEY REFERENCES p1)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO p1 VALUES(88)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO c1 VALUES(90)")
	if err != nil {
		t.Fatalf("Failed to insert c1: %v", err)
	}

	// Check specific table
	rows, err := db.Query("PRAGMA foreign_key_check(c1)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check(c1): %v", err)
	}
	defer rows.Close()

	violations := 0
	for rows.Next() {
		violations++
		var table string
		var rowid sql.NullInt64
		var parent string
		var fkid int
		err := rows.Scan(&table, &rowid, &parent, &fkid)
		if err != nil {
			t.Fatalf("Failed to scan violation: %v", err)
		}
	}

	if violations != 1 {
		t.Errorf("Expected 1 FK violation, got %d", violations)
	}

	// Check c2 (should have no violations)
	rows2, err := db.Query("PRAGMA foreign_key_check(c2)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check(c2): %v", err)
	}
	defer rows2.Close()

	violations2 := 0
	for rows2.Next() {
		violations2++
	}

	if violations2 != 0 {
		t.Errorf("Expected 0 FK violations for c2, got %d", violations2)
	}
}

// TestForeignKey_NoActionOnInsert tests that FK checks happen on INSERT.
// Based on fkey2-1.1.* tests.
func TestForeignKey_NoActionOnInsert(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t3(a PRIMARY KEY, b);
		CREATE TABLE t4(c REFERENCES t3, d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Insert with non-existent FK should fail
	_, err = db.Exec("INSERT INTO t4 VALUES(1, 3)")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}

	// Insert parent
	_, err = db.Exec("INSERT INTO t3 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Now insert should succeed
	_, err = db.Exec("INSERT INTO t4 VALUES(1, 3)")
	if err != nil {
		t.Errorf("Expected successful insert, got: %v", err)
	}
}

// TestForeignKey_IntegerPrimaryKey tests FK with INTEGER PRIMARY KEY.
// Based on fkey2-4.* tests.
func TestForeignKey_IntegerPrimaryKey(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t7(a, b INTEGER PRIMARY KEY);
		CREATE TABLE t8(c REFERENCES t7, d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Insert parent with INTEGER PRIMARY KEY
	_, err = db.Exec("INSERT INTO t7 VALUES(2, 1)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child referencing rowid
	_, err = db.Exec("INSERT INTO t8 VALUES(1, 3)")
	if err != nil {
		t.Errorf("Expected successful insert, got: %v", err)
	}

	// Try to delete parent
	_, err = db.Exec("DELETE FROM t7 WHERE b=1")
	if err == nil {
		t.Error("Expected FK constraint error on delete, got nil")
	}
}

// TestForeignKey_CollationHandling tests that FK uses parent key collation.
// Based on fkey2-1.7.* tests.
func TestForeignKey_CollationHandling(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE i(i TEXT COLLATE nocase PRIMARY KEY);
		CREATE TABLE j(j TEXT COLLATE binary REFERENCES i(i))
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO i VALUES('SQLite')")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert with different case - should use parent's nocase collation
	_, err = db.Exec("INSERT INTO j VALUES('sqlite')")
	if err != nil {
		t.Errorf("Expected insert to succeed with nocase collation, got: %v", err)
	}

	// Try to delete parent - should be prevented
	_, err = db.Exec("DELETE FROM i")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}
}

// TestForeignKey_RecursiveCascade tests recursive CASCADE operations.
// Based on fkey2-4.* tests.
func TestForeignKey_RecursiveCascade(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(
			node PRIMARY KEY,
			parent REFERENCES t1 ON DELETE CASCADE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Build a tree: 1 -> 2 -> 4
	//                  -> 3 -> 6
	//                     -> 7
	//               -> 5
	_, err = db.Exec(`
		INSERT INTO t1 VALUES(1, NULL);
		INSERT INTO t1 VALUES(2, 1);
		INSERT INTO t1 VALUES(3, 1);
		INSERT INTO t1 VALUES(4, 2);
		INSERT INTO t1 VALUES(5, 2);
		INSERT INTO t1 VALUES(6, 3);
		INSERT INTO t1 VALUES(7, 3)
	`)
	if err != nil {
		t.Fatalf("Failed to build tree: %v", err)
	}

	// Delete root - should cascade to all descendants
	_, err = db.Exec("DELETE FROM t1 WHERE node = 1")
	if err != nil {
		t.Errorf("Expected CASCADE delete to succeed, got: %v", err)
	}

	// All rows should be deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after recursive CASCADE, got %d", count)
	}
}

// TestForeignKey_ReplaceViolation tests INSERT OR REPLACE with FK violation.
// Based on fkey1-5.2.
func TestForeignKey_ReplaceViolation(t *testing.T) {
	t.Skip("INSERT OR REPLACE FK cascade not yet implemented")
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t11(
			x INTEGER PRIMARY KEY,
			parent REFERENCES t11 ON DELETE CASCADE
		);
		INSERT INTO t11 VALUES(1, NULL), (2, 1), (3, 2)
	`)
	if err != nil {
		t.Fatalf("Failed to create table and data: %v", err)
	}

	// REPLACE deletes (2,1) which cascades to delete (3,2)
	// Then tries to insert (2,3) but 3 doesn't exist anymore
	_, err = db.Exec("INSERT OR REPLACE INTO t11 VALUES(2, 3)")
	if err == nil {
		t.Error("Expected FK constraint error from REPLACE cascade, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}
}

// TestForeignKey_QuotedTableNames tests FK with quoted table names.
// Based on fkey1-4.* tests.
func TestForeignKey_QuotedTableNames(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE "xx1"("xx2" TEXT PRIMARY KEY, "xx3" TEXT);
		INSERT INTO "xx1"("xx2", "xx3") VALUES('abc', 'def');
		CREATE TABLE "xx4"("xx5" TEXT REFERENCES "xx1" ON DELETE CASCADE);
		INSERT INTO "xx4"("xx5") VALUES('abc')
	`)
	if err != nil {
		t.Fatalf("Failed to create tables with quoted names: %v", err)
	}

	// Insert another parent
	_, err = db.Exec(`INSERT INTO "xx1"("xx2", "xx3") VALUES('uvw', 'xyz')`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Verify child exists
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM "xx4"`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count children: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 child, got %d", count)
	}

	// Delete all parents - should cascade to child
	_, err = db.Exec(`DELETE FROM "xx1"`)
	if err != nil {
		t.Errorf("Expected CASCADE delete to succeed, got: %v", err)
	}

	// Child should be deleted
	err = db.QueryRow(`SELECT COUNT(*) FROM "xx4"`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count children: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 children after CASCADE delete, got %d", count)
	}
}

// TestForeignKey_MissingParentTable tests FK referencing non-existent table.
// Based on fkey5-9.* tests.
func TestForeignKey_MissingParentTable(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		t.Fatalf("Failed to disable foreign keys: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE k1(x REFERENCES s1)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Check with NULL FK (should pass)
	_, err = db.Exec("INSERT INTO k1 VALUES(NULL)")
	if err != nil {
		t.Fatalf("Failed to insert NULL FK: %v", err)
	}

	rows, err := db.Query("PRAGMA foreign_key_check(k1)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check: %v", err)
	}
	defer rows.Close()

	violations := 0
	for rows.Next() {
		violations++
	}

	if violations != 0 {
		t.Errorf("Expected 0 violations with NULL FK, got %d", violations)
	}

	// Insert non-NULL FK to missing table
	_, err = db.Exec("INSERT INTO k1 VALUES(1)")
	if err != nil {
		t.Fatalf("Failed to insert FK: %v", err)
	}

	// Should report violation
	rows2, err := db.Query("PRAGMA foreign_key_check(k1)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check: %v", err)
	}
	defer rows2.Close()

	violations2 := 0
	for rows2.Next() {
		violations2++
		var table, parent string
		var rowid sql.NullInt64
		var fkid int
		err := rows2.Scan(&table, &rowid, &parent, &fkid)
		if err != nil {
			t.Fatalf("Failed to scan violation: %v", err)
		}

		if parent != "s1" {
			t.Errorf("Expected parent table 's1', got '%s'", parent)
		}
	}

	if violations2 != 1 {
		t.Errorf("Expected 1 violation with missing parent table, got %d", violations2)
	}
}

// TestForeignKey_PartialNullMultiColumn tests partial NULL in multi-column FK.
// Based on fkey5-9.2.
func TestForeignKey_PartialNullMultiColumn(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = OFF")
	if err != nil {
		t.Fatalf("Failed to disable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE k2(
			x, y,
			FOREIGN KEY(x, y) REFERENCES s1(a, b)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with partial NULL (should be allowed)
	_, err = db.Exec("INSERT INTO k2 VALUES(NULL, 'five')")
	if err != nil {
		t.Fatalf("Failed to insert partial NULL: %v", err)
	}

	_, err = db.Exec("INSERT INTO k2 VALUES('one', NULL)")
	if err != nil {
		t.Fatalf("Failed to insert partial NULL: %v", err)
	}

	// Check - partial NULLs should not be violations
	rows, err := db.Query("PRAGMA foreign_key_check(k2)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check: %v", err)
	}
	defer rows.Close()

	violations := 0
	for rows.Next() {
		violations++
	}

	if violations != 0 {
		t.Errorf("Expected 0 violations with partial NULL FK, got %d", violations)
	}

	// Insert with both non-NULL (should be violation)
	_, err = db.Exec("INSERT INTO k2 VALUES('six', 'seven')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows2, err := db.Query("PRAGMA foreign_key_check(k2)")
	if err != nil {
		t.Fatalf("Failed to run foreign_key_check: %v", err)
	}
	defer rows2.Close()

	violations2 := 0
	for rows2.Next() {
		violations2++
	}

	if violations2 != 1 {
		t.Errorf("Expected 1 violation with non-NULL FK, got %d", violations2)
	}
}

// TestForeignKey_Restrict tests RESTRICT action.
// Based on fkey2-12.* tests.
func TestForeignKey_Restrict(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER REFERENCES parent(id) ON DELETE RESTRICT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO parent VALUES(1)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Try to delete parent - RESTRICT should prevent it
	_, err = db.Exec("DELETE FROM parent WHERE id=1")
	if err == nil {
		t.Error("Expected RESTRICT to prevent delete, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}
}

// TestForeignKey_AffinityHandling tests that affinity doesn't break FK checks.
// Based on fkey2-1.5.* tests.
func TestForeignKey_AffinityHandling(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE i(i INTEGER PRIMARY KEY);
		CREATE TABLE j(j REFERENCES i)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	_, err = db.Exec("INSERT INTO i VALUES(35)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert with text '35.0' (should match integer 35 with affinity)
	_, err = db.Exec("INSERT INTO j VALUES('35.0')")
	if err != nil {
		t.Errorf("Expected insert to succeed, got: %v", err)
	}

	// Verify the value is stored as text
	var val string
	var typ string
	err = db.QueryRow("SELECT j, typeof(j) FROM j").Scan(&val, &typ)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if val != "35.0" {
		t.Errorf("Expected value '35.0', got '%s'", val)
	}
	if typ != "text" {
		t.Errorf("Expected type 'text', got '%s'", typ)
	}

	// Try to delete parent - should fail
	_, err = db.Exec("DELETE FROM i")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}
}

// TestForeignKey_SelfReferencingUpdate tests updating self-referencing rows.
// Based on fkey3-3.6.* tests.
func TestForeignKey_SelfReferencingUpdate(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t8(a, b, c, d, e, FOREIGN KEY(c, d) REFERENCES t8(a, b));
		CREATE UNIQUE INDEX t8i1 ON t8(a, b);
		CREATE UNIQUE INDEX t8i2 ON t8(c);
		INSERT INTO t8 VALUES(1, 1, 1, 1, 1)
	`)
	if err != nil {
		t.Fatalf("Failed to create table and data: %v", err)
	}

	// Try to update to invalid FK
	_, err = db.Exec("UPDATE t8 SET d = 2")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}

	// Update to valid FK
	_, err = db.Exec("UPDATE t8 SET d = 1")
	if err != nil {
		t.Errorf("Expected successful update, got: %v", err)
	}

	// Update non-FK column
	_, err = db.Exec("UPDATE t8 SET e = 2")
	if err != nil {
		t.Errorf("Expected successful update of non-FK column, got: %v", err)
	}
}

// TestForeignKey_MatchingSelf tests self-referencing row matching itself.
// Based on fkey3-3.4.* tests.
func TestForeignKey_MatchingSelf(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t6(
			a INTEGER PRIMARY KEY,
			b, c, d,
			FOREIGN KEY(c, d) REFERENCES t6(a, b)
		);
		CREATE UNIQUE INDEX t6i ON t6(b, a)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert row that references itself (autoincrement)
	_, err = db.Exec("INSERT INTO t6 VALUES(NULL, 'a', 1, 'a')")
	if err != nil {
		t.Errorf("Expected self-referencing insert to succeed, got: %v", err)
	}

	// Insert row referencing existing row
	_, err = db.Exec("INSERT INTO t6 VALUES(2, 'a', 2, 'a')")
	if err != nil {
		t.Errorf("Expected insert to succeed, got: %v", err)
	}

	// Another self-referencing insert
	_, err = db.Exec("INSERT INTO t6 VALUES(NULL, 'a', 1, 'a')")
	if err != nil {
		t.Errorf("Expected self-referencing insert to succeed, got: %v", err)
	}

	// Insert with invalid FK
	_, err = db.Exec("INSERT INTO t6 VALUES(NULL, 'a', 65, 'a')")
	if err == nil {
		t.Error("Expected FK constraint error, got nil")
	}
}

// TestForeignKey_DeleteSelfReferencing tests deleting and updating self-referencing rows.
// Based on fkey3-3.4.7-8 tests.
func TestForeignKey_DeleteSelfReferencing(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t6(
			a INTEGER PRIMARY KEY,
			b, c, d,
			FOREIGN KEY(c, d) REFERENCES t6(a, b)
		);
		CREATE UNIQUE INDEX t6i ON t6(a, b)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert and delete self-referencing row
	_, err = db.Exec("INSERT INTO t6 VALUES(100, 'one', 100, 'one')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec("DELETE FROM t6 WHERE a = 100")
	if err != nil {
		t.Errorf("Expected delete to succeed, got: %v", err)
	}

	// Insert, update, and delete
	_, err = db.Exec("INSERT INTO t6 VALUES(100, 'one', 100, 'one')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// First insert a row to reference
	_, err = db.Exec("INSERT INTO t6 VALUES(1, 'a', 1, 'a')")
	if err != nil {
		t.Fatalf("Failed to insert reference row: %v", err)
	}

	_, err = db.Exec("UPDATE t6 SET c = 1, d = 'a' WHERE a = 100")
	if err != nil {
		t.Errorf("Expected update to succeed, got: %v", err)
	}

	_, err = db.Exec("DELETE FROM t6 WHERE a = 100")
	if err != nil {
		t.Errorf("Expected delete to succeed, got: %v", err)
	}
}

// TestForeignKey_ForeignKeyMismatch tests "foreign key mismatch" error.
// Based on fkey2-5.2 and fkey5-11.* tests.
func TestForeignKey_ForeignKeyMismatch(t *testing.T) {
	t.Skip("FK mismatch detection not yet implemented")
	db := setupMemoryDB(t)
	defer db.Close()

	// Create table with FK to column without unique constraint
	_, err := db.Exec(`
		CREATE TABLE tt(y);
		CREATE TABLE c11(x REFERENCES tt(y))
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// PRAGMA foreign_key_check should report mismatch
	_, err = db.Query("PRAGMA foreign_key_check")
	if err == nil {
		t.Error("Expected 'foreign key mismatch' error, got nil")
	}
	if !strings.Contains(err.Error(), "foreign key mismatch") {
		t.Errorf("Expected 'foreign key mismatch' error, got: %v", err)
	}
}

// TestForeignKey_SetDefault tests SET DEFAULT action.
// Based on fkey2-9.* tests.
func TestForeignKey_SetDefault(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY);
		CREATE TABLE child(
			id INTEGER PRIMARY KEY,
			parent_id INTEGER DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Insert default parent
	_, err = db.Exec("INSERT INTO parent VALUES(0)")
	if err != nil {
		t.Fatalf("Failed to insert default parent: %v", err)
	}

	// Insert actual parent
	_, err = db.Exec("INSERT INTO parent VALUES(1)")
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Insert child referencing parent 1
	_, err = db.Exec("INSERT INTO child VALUES(10, 1)")
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Delete parent 1 - should set child FK to default (0)
	_, err = db.Exec("DELETE FROM parent WHERE id=1")
	if err != nil {
		t.Errorf("Expected SET DEFAULT delete to succeed, got: %v", err)
	}

	// Verify child FK was set to default
	var parentID int
	err = db.QueryRow("SELECT parent_id FROM child WHERE id=10").Scan(&parentID)
	if err != nil {
		t.Fatalf("Failed to query child: %v", err)
	}
	if parentID != 0 {
		t.Errorf("Expected parent_id to be 0 (default), got %d", parentID)
	}
}

// TestForeignKey_DeferredInitiallyImmediate tests DEFERRABLE INITIALLY IMMEDIATE.
// Based on fkey4.test.
func TestForeignKey_DeferredInitiallyImmediate(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatalf("Failed to enable foreign keys: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE t1(a PRIMARY KEY, b);
		CREATE TABLE t2(c REFERENCES t1 DEFERRABLE INITIALLY IMMEDIATE, d)
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Even in transaction, should fail immediately
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT INTO t2 VALUES(2, 4)")
	if err == nil {
		t.Error("Expected immediate FK constraint error, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Errorf("Expected 'FOREIGN KEY constraint failed', got: %v", err)
	}
}
