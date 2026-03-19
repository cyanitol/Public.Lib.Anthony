// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setupAttachTestDB creates a test database for ATTACH/DETACH tests
func setupAttachTestDB(t *testing.T, name string) (*sql.DB, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, name)

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database %s: %v", name, err)
	}

	return db, dbPath
}

// setupAttachTestDBInDir creates a test database in a specific directory
// (for ATTACH tests where both databases must be in the same sandbox)
func setupAttachTestDBInDir(t *testing.T, tmpDir, name string) (*sql.DB, string) {
	dbPath := filepath.Join(tmpDir, name)

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database %s: %v", name, err)
	}

	return db, dbPath
}

// TestAttachBasic tests basic ATTACH DATABASE functionality (attach.test 1.1-1.7)
func TestAttachBasic(t *testing.T) {
	// Use same temp dir for both databases (security sandbox requirement)
	tmpDir := t.TempDir()

	db, _ := setupAttachTestDBInDir(t, tmpDir, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDBInDir(t, tmpDir, "test2.db")
	defer db2.Close()

	// Create table in main database
	_, err := db.Exec("CREATE TABLE t1(a,b)")
	if err != nil {
		t.Fatalf("failed to create t1: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(1,2)")
	if err != nil {
		t.Fatalf("failed to insert into t1: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(3,4)")
	if err != nil {
		t.Fatalf("failed to insert into t1: %v", err)
	}

	// Create table in second database
	_, err = db2.Exec("CREATE TABLE t2(x,y)")
	if err != nil {
		t.Fatalf("failed to create t2: %v", err)
	}

	_, err = db2.Exec("INSERT INTO t2 VALUES(1,'x')")
	if err != nil {
		t.Fatalf("failed to insert into t2: %v", err)
	}

	_, err = db2.Exec("INSERT INTO t2 VALUES(2,'y')")
	if err != nil {
		t.Fatalf("failed to insert into t2: %v", err)
	}

	db2.Close()

	// Attach test2.db to main database
	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS two", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Query attached database with qualified name
	rows, err := db.Query("SELECT * FROM two.t2")
	if err != nil {
		t.Fatalf("failed to query attached database: %v", err)
	}
	defer rows.Close()

	var results []struct {
		x int
		y string
	}
	for rows.Next() {
		var r struct {
			x int
			y string
		}
		if err := rows.Scan(&r.x, &r.y); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 rows, got %d", len(results))
	}

	// Query attached database without qualifier (should work)
	rows2, err := db.Query("SELECT * FROM t2")
	if err != nil {
		t.Fatalf("failed to query t2 without qualifier: %v", err)
	}
	rows2.Close()

	// DETACH database
	_, err = db.Exec("DETACH DATABASE two")
	if err != nil {
		t.Fatalf("failed to detach database: %v", err)
	}

	// Query main database after detach (should work)
	rows3, err := db.Query("SELECT * FROM t1")
	if err != nil {
		t.Fatalf("failed to query t1 after detach: %v", err)
	}
	rows3.Close()

	// Query t2 after detach (should fail)
	_, err = db.Query("SELECT * FROM t2")
	if err == nil {
		t.Error("expected error querying t2 after detach, got nil")
	}

	// Query two.t2 after detach (should fail)
	_, err = db.Query("SELECT * FROM two.t2")
	if err == nil {
		t.Error("expected error querying two.t2 after detach, got nil")
	}
}

// TestAttachNonExistent tests attaching a non-existent database (attach.test 1.8-1.10)
func TestAttachNonExistent(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test3.db")

	// Attach non-existent database (should create it)
	_, err := db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS three", dbPath))
	if err != nil {
		t.Fatalf("failed to attach non-existent database: %v", err)
	}

	// Query sqlite_master of attached database
	rows, err := db.Query("SELECT * FROM three.sqlite_master")
	if err != nil {
		t.Fatalf("failed to query three.sqlite_master: %v", err)
	}
	rows.Close()

	// Detach with square brackets
	_, err = db.Exec("DETACH DATABASE [three]")
	if err != nil {
		t.Fatalf("failed to detach database with brackets: %v", err)
	}
}

// TestAttachMultiple tests attaching multiple databases (attach.test 1.11-1.19)
func TestAttachMultiple(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, dbPath := setupAttachTestDB(t, "test.db")
	defer db.Close()

	// Attach multiple databases
	for i := 2; i <= 9; i++ {
		alias := fmt.Sprintf("db%d", i)
		_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS %s", dbPath, alias))
		if err != nil {
			t.Fatalf("failed to attach %s: %v", alias, err)
		}
	}

	// Try to attach with duplicate name (should fail)
	_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS db2", dbPath))
	if err == nil {
		t.Error("expected error when attaching with duplicate name, got nil")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("expected 'already in use' error, got: %v", err)
	}

	// Try to attach as 'main' (should fail)
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS main", dbPath))
	if err == nil {
		t.Error("expected error when attaching as 'main', got nil")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("expected 'already in use' error, got: %v", err)
	}

	// Try to attach as 'MAIN' (should fail - case insensitive)
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS MAIN", dbPath))
	if err == nil {
		t.Error("expected error when attaching as 'MAIN', got nil")
	}
}

// TestDetachErrors tests DETACH error cases (attach.test 1.23-1.27)
func TestDetachErrors(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	// Try to detach non-existent database
	_, err := db.Exec("DETACH DATABASE db14")
	if err == nil {
		t.Error("expected error when detaching non-existent database, got nil")
	}
	if !strings.Contains(err.Error(), "no such database") {
		t.Errorf("expected 'no such database' error, got: %v", err)
	}

	// Try to detach main database
	_, err = db.Exec("DETACH DATABASE main")
	if err == nil {
		t.Error("expected error when detaching main, got nil")
	}
	if !strings.Contains(err.Error(), "cannot detach database main") {
		t.Errorf("expected 'cannot detach database main' error, got: %v", err)
	}
}

// TestAttachSameSchema tests attaching databases with identical schemas (attach2.test 1.1)
func TestAttachSameSchema(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	defer db2.Close()

	// Create identical schema in both databases
	schema := "CREATE TABLE t1(a,b); CREATE INDEX x1 ON t1(a)"

	_, err := db.Exec(schema)
	if err != nil {
		t.Fatalf("failed to create schema in db: %v", err)
	}

	_, err = db2.Exec(schema)
	if err != nil {
		t.Fatalf("failed to create schema in db2: %v", err)
	}

	db2.Close()

	// Attach test2.db (should succeed even with same schema)
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS t2", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database with identical schema: %v", err)
	}
}

// TestAttachCrossDatabase tests cross-database queries (attach.test 4.1-4.5)
func TestAttachCrossDatabase(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	// Create table in main database
	_, err := db.Exec("CREATE TABLE t3(a,b)")
	if err != nil {
		t.Fatalf("failed to create t3 in main: %v", err)
	}

	_, err = db.Exec("CREATE UNIQUE INDEX t3i1b ON t3(a)")
	if err != nil {
		t.Fatalf("failed to create index on main.t3: %v", err)
	}

	_, err = db.Exec("INSERT INTO t3 VALUES(9,10)")
	if err != nil {
		t.Fatalf("failed to insert into main.t3: %v", err)
	}

	// Create table in second database
	_, err = db2.Exec("CREATE TABLE t3(x,y)")
	if err != nil {
		t.Fatalf("failed to create t3 in db2: %v", err)
	}

	_, err = db2.Exec("CREATE UNIQUE INDEX t3i1 ON t3(x)")
	if err != nil {
		t.Fatalf("failed to create index on db2.t3: %v", err)
	}

	_, err = db2.Exec("INSERT INTO t3 VALUES(1,2)")
	if err != nil {
		t.Fatalf("failed to insert into db2.t3: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS db2", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Query attached database
	var x, y int
	err = db.QueryRow("SELECT * FROM db2.t3").Scan(&x, &y)
	if err != nil {
		t.Fatalf("failed to query db2.t3: %v", err)
	}
	if x != 1 || y != 2 {
		t.Errorf("expected (1,2), got (%d,%d)", x, y)
	}

	// Query main database
	var a, b int
	err = db.QueryRow("SELECT * FROM main.t3").Scan(&a, &b)
	if err != nil {
		t.Fatalf("failed to query main.t3: %v", err)
	}
	if a != 9 || b != 10 {
		t.Errorf("expected (9,10), got (%d,%d)", a, b)
	}

	// Insert into attached database
	_, err = db.Exec("INSERT INTO db2.t3 VALUES(9,10)")
	if err != nil {
		t.Fatalf("failed to insert into db2.t3: %v", err)
	}
}

// TestAttachCreateSchema tests creating schema objects in attached databases (attach3.test 1.1-1.5)
func TestAttachCreateSchema(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	// Create tables in both databases
	_, err := db.Exec("CREATE TABLE t1(a, b); CREATE TABLE t2(c, d)")
	if err != nil {
		t.Fatalf("failed to create tables in main: %v", err)
	}

	_, err = db2.Exec("CREATE TABLE t1(a, b); CREATE TABLE t2(c, d)")
	if err != nil {
		t.Fatalf("failed to create tables in db2: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS aux", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Create table in attached database
	_, err = db.Exec("CREATE TABLE aux.t3(e, f)")
	if err != nil {
		t.Fatalf("failed to create table in attached database: %v", err)
	}

	// Verify table exists in aux, not in main
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 't3'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query main sqlite_master: %v", err)
	}
	if count != 0 {
		t.Error("table t3 should not exist in main database")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 't3'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query aux sqlite_master: %v", err)
	}
	if count != 1 {
		t.Error("table t3 should exist in aux database")
	}

	// Insert into attached table
	_, err = db.Exec("INSERT INTO t3 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("failed to insert into t3: %v", err)
	}

	// Query attached table
	var e, f int
	err = db.QueryRow("SELECT * FROM t3").Scan(&e, &f)
	if err != nil {
		t.Fatalf("failed to query t3: %v", err)
	}
	if e != 1 || f != 2 {
		t.Errorf("expected (1,2), got (%d,%d)", e, f)
	}
}

// TestAttachCreateIndex tests creating indexes in attached databases (attach3.test 2.1-3.3)
func TestAttachCreateIndex(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	_, err := db2.Exec("CREATE TABLE t3(e, f)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS aux", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Create index in attached database
	_, err = db.Exec("CREATE INDEX aux.i1 ON t3(e)")
	if err != nil {
		t.Fatalf("failed to create index in attached database: %v", err)
	}

	// Verify index exists in aux, not in main
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 'i1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query main sqlite_master: %v", err)
	}
	if count != 0 {
		t.Error("index i1 should not exist in main database")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 'i1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query aux sqlite_master: %v", err)
	}
	if count != 1 {
		t.Error("index i1 should exist in aux database")
	}

	// Drop index from attached database
	_, err = db.Exec("DROP INDEX aux.i1")
	if err != nil {
		t.Fatalf("failed to drop index: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 'i1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query aux sqlite_master: %v", err)
	}
	if count != 0 {
		t.Error("index i1 should not exist after drop")
	}

	// Create index again without qualifier
	_, err = db.Exec("CREATE INDEX aux.i1 ON t3(e)")
	if err != nil {
		t.Fatalf("failed to recreate index: %v", err)
	}

	// Drop index without database qualifier
	_, err = db.Exec("DROP INDEX i1")
	if err != nil {
		t.Fatalf("failed to drop index without qualifier: %v", err)
	}
}

// TestAttachDropTable tests dropping tables in attached databases (attach3.test 4.1-4.3)
func TestAttachDropTable(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	// Create tables
	_, err := db.Exec("CREATE TABLE t2(c, d)")
	if err != nil {
		t.Fatalf("failed to create main.t2: %v", err)
	}

	_, err = db2.Exec("CREATE TABLE t1(a, b); CREATE TABLE t2(c, d); CREATE TABLE t3(e, f)")
	if err != nil {
		t.Fatalf("failed to create tables in db2: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS aux", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Drop table from attached database
	_, err = db.Exec("DROP TABLE aux.t1")
	if err != nil {
		t.Fatalf("failed to drop aux.t1: %v", err)
	}

	// Verify remaining tables in aux
	rows, err := db.Query("SELECT name FROM aux.sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("failed to query aux tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan table name: %v", err)
		}
		tables = append(tables, name)
	}

	expected := []string{"t2", "t3"}
	if len(tables) != len(expected) {
		t.Errorf("expected %v tables, got %v", expected, tables)
	}

	// Drop t2 without qualifier (should drop main.t2, not aux.t2)
	_, err = db.Exec("DROP TABLE t2")
	if err != nil {
		t.Fatalf("failed to drop t2: %v", err)
	}

	// Verify aux.t2 still exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 't2'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query aux sqlite_master: %v", err)
	}
	if count != 1 {
		t.Error("aux.t2 should still exist")
	}
}

// TestAttachReadOnlyMaster tests that sqlite_master is read-only (attach3.test 10.0)
func TestAttachReadOnlyMaster(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	db2.Close()

	// Attach db2
	_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS aux", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Try to insert into aux.sqlite_master (should fail)
	_, err = db.Exec("INSERT INTO aux.sqlite_master VALUES(1, 2, 3, 4, 5)")
	if err == nil {
		t.Error("expected error when inserting into sqlite_master, got nil")
	}
	if !strings.Contains(err.Error(), "may not be modified") {
		t.Errorf("expected 'may not be modified' error, got: %v", err)
	}
}

// TestAttachInvalidFile tests attaching invalid database files (attach.test 8.1-8.2)
func TestAttachInvalidFile(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	tmpDir := t.TempDir()
	invalidPath := filepath.Join(tmpDir, "test2.db")

	// Create invalid database file
	if err := os.WriteFile(invalidPath, []byte("This file is not a valid SQLite database"), 0600); err != nil {
		t.Fatalf("failed to create invalid file: %v", err)
	}

	// Try to attach invalid database
	_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS t2", invalidPath))
	if err == nil {
		t.Error("expected error when attaching invalid database, got nil")
	}
	if !strings.Contains(err.Error(), "not a database") {
		t.Errorf("expected 'not a database' error, got: %v", err)
	}
}

// TestAttachSameFileTwice tests attaching the same file multiple times (attach.test 9.1-9.3)
func TestAttachSameFileTwice(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	tmpDir := t.TempDir()
	test4Path := filepath.Join(tmpDir, "test4.db")

	// Attach same file twice with different aliases
	_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS aux1", test4Path))
	if err != nil {
		t.Fatalf("failed to attach as aux1: %v", err)
	}

	_, err = db.Exec("CREATE TABLE aux1.t1(a, b)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO aux1.t1 VALUES(1, 2)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS aux2", test4Path))
	if err != nil {
		t.Fatalf("failed to attach as aux2: %v", err)
	}

	// Query via second alias
	var a, b int
	err = db.QueryRow("SELECT * FROM aux2.t1").Scan(&a, &b)
	if err != nil {
		t.Fatalf("failed to query aux2.t1: %v", err)
	}
	if a != 1 || b != 2 {
		t.Errorf("expected (1,2), got (%d,%d)", a, b)
	}

	// Concurrent writes to same file via different aliases should fail
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO aux1.t1 VALUES(3, 4)")
	if err != nil {
		t.Fatalf("failed to insert into aux1: %v", err)
	}

	_, err = tx.Exec("INSERT INTO aux2.t1 VALUES(5, 6)")
	if err != nil {
		tx.Rollback()
		// This is expected - writing to the same database via different aliases
		// in a transaction should cause a lock error
		if !strings.Contains(err.Error(), "locked") {
			t.Errorf("expected 'locked' error, got: %v", err)
		}
	} else {
		tx.Rollback()
	}
}

// TestAttachMemoryDatabases tests attaching memory databases (attach.test 10.1-10.2)
func TestAttachMemoryDatabases(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	// Attach empty string as memory database
	_, err := db.Exec("ATTACH '' AS noname")
	if err != nil {
		t.Fatalf("failed to attach empty string: %v", err)
	}

	// Attach :memory: as memory database
	_, err = db.Exec("ATTACH ':memory:' AS inmem")
	if err != nil {
		t.Fatalf("failed to attach :memory:: %v", err)
	}

	// Create tables in attached memory databases
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE noname.noname(x)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table in noname: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE inmem.inmem(y)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table in inmem: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE main.main(z)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to create table in main: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify tables exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM noname.sqlite_master WHERE name='noname'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query noname tables: %v", err)
	}
	if count != 1 {
		t.Error("table noname should exist in noname database")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM inmem.sqlite_master WHERE name='inmem'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query inmem tables: %v", err)
	}
	if count != 1 {
		t.Error("table inmem should exist in inmem database")
	}
}

// TestAttachMaxDatabases tests the maximum number of attached databases (attach4.test 1.1-1.2)
func TestAttachMaxDatabases(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, dbPath := setupAttachTestDB(t, "test.db")
	defer db.Close()

	tmpDir := t.TempDir()

	// Attach many databases
	var attached []string
	for i := 0; i < 10; i++ {
		alias := fmt.Sprintf("aux%d", i)
		attachPath := filepath.Join(tmpDir, fmt.Sprintf("test%d.db", i))

		_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS %s", attachPath, alias))
		if err != nil {
			// Hit the limit
			if !strings.Contains(err.Error(), "too many attached") {
				t.Errorf("expected 'too many attached' error at %d attachments, got: %v", i, err)
			}
			break
		}
		attached = append(attached, alias)
	}

	// Verify we could attach at least a few databases
	if len(attached) < 3 {
		t.Errorf("expected to attach at least 3 databases, got %d", len(attached))
	}

	// Try to attach one more (should fail if at limit)
	oneMore := filepath.Join(tmpDir, "testone_more.db")
	_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS one_more", oneMore))
	if err != nil {
		if !strings.Contains(err.Error(), "too many attached") {
			t.Logf("attachment limit reached at %d databases", len(attached))
		}
	}

	// Detach one and verify we can attach again
	if len(attached) > 0 {
		_, err = db.Exec(fmt.Sprintf("DETACH %s", attached[0]))
		if err != nil {
			t.Fatalf("failed to detach: %v", err)
		}

		_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS reattached", dbPath))
		if err != nil {
			t.Fatalf("failed to reattach after detach: %v", err)
		}
	}
}

// TestAttachEmptyNames tests attaching with empty database names (attach3.test 12.1-12.14)
func TestAttachEmptyNames(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	// Attach with empty name
	_, err := db.Exec("ATTACH DATABASE '' AS ''")
	if err != nil {
		t.Fatalf("failed to attach with empty name: %v", err)
	}

	// Detach empty name
	_, err = db.Exec("DETACH ''")
	if err != nil {
		t.Fatalf("failed to detach empty name: %v", err)
	}

	// Attach again with question mark placeholder (test parameter binding)
	_, err = db.Exec("ATTACH DATABASE '' AS ''")
	if err != nil {
		t.Fatalf("failed to attach with empty name again: %v", err)
	}

	// Try to attach with same empty name (should fail)
	_, err = db.Exec("ATTACH DATABASE '' AS ''")
	if err == nil {
		t.Error("expected error when attaching duplicate empty name, got nil")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("expected 'already in use' error, got: %v", err)
	}

	// Detach again
	_, err = db.Exec("DETACH ''")
	if err != nil {
		t.Fatalf("failed to detach empty name: %v", err)
	}
}

// TestAttachQualifiedTableNames tests using qualified table names (attach.test 4.12-4.13)
func TestAttachQualifiedTableNames(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	// Create tables in both databases
	_, err := db.Exec("CREATE TABLE t3(a,b)")
	if err != nil {
		t.Fatalf("failed to create main.t3: %v", err)
	}

	_, err = db.Exec("INSERT INTO t3 VALUES(9,10)")
	if err != nil {
		t.Fatalf("failed to insert into main.t3: %v", err)
	}

	_, err = db2.Exec("CREATE TABLE t3(x,y)")
	if err != nil {
		t.Fatalf("failed to create db2.t3: %v", err)
	}

	_, err = db2.Exec("INSERT INTO t3 VALUES(1,2)")
	if err != nil {
		t.Fatalf("failed to insert into db2.t3: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS db2", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Query with qualified names
	var x, y int
	err = db.QueryRow("SELECT * FROM db2.t3").Scan(&x, &y)
	if err != nil {
		t.Fatalf("failed to query db2.t3: %v", err)
	}
	if x != 1 || y != 2 {
		t.Errorf("expected (1,2) from db2.t3, got (%d,%d)", x, y)
	}

	var a, b int
	err = db.QueryRow("SELECT * FROM main.t3").Scan(&a, &b)
	if err != nil {
		t.Fatalf("failed to query main.t3: %v", err)
	}
	if a != 9 || b != 10 {
		t.Errorf("expected (9,10) from main.t3, got (%d,%d)", a, b)
	}
}

// TestAttachSchemaNamespace tests that schemas in different databases are independent (attach.test 2.7-2.16)
func TestAttachSchemaNamespace(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	// Create table in main
	_, err := db.Exec("CREATE TABLE tx(x1,x2,y1,y2)")
	if err != nil {
		t.Fatalf("failed to create main.tx: %v", err)
	}

	// Create table and index in db2
	_, err = db2.Exec("CREATE TABLE t2(x,y)")
	if err != nil {
		t.Fatalf("failed to create db2.t2: %v", err)
	}

	_, err = db2.Exec("INSERT INTO t2 VALUES(1,'x')")
	if err != nil {
		t.Fatalf("failed to insert into db2.t2: %v", err)
	}

	_, err = db2.Exec("CREATE TABLE tx(x1,x2,y1,y2)")
	if err != nil {
		t.Fatalf("failed to create db2.tx: %v", err)
	}

	db2.Close()

	// Attach db2
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS db2", db2Path))
	if err != nil {
		t.Fatalf("failed to attach database: %v", err)
	}

	// Query main.tx (should be empty)
	rows, err := db.Query("SELECT * FROM main.tx")
	if err != nil {
		t.Fatalf("failed to query main.tx: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 rows in main.tx, got %d", count)
	}

	// Verify schema objects exist in db2
	var schemaCount int
	err = db.QueryRow("SELECT COUNT(*) FROM db2.sqlite_master WHERE type IN ('table','index')").Scan(&schemaCount)
	if err != nil {
		t.Fatalf("failed to query db2 schema: %v", err)
	}
	if schemaCount < 2 {
		t.Errorf("expected at least 2 schema objects in db2, got %d", schemaCount)
	}
}

// TestAttachUnknownDatabase tests error for unknown database (attach.test 6.3)
func TestAttachUnknownDatabase(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db := openAttachTestDB(t)
	defer db.Close()

	// Try to create table in non-existent database
	_, err := db.Exec("CREATE TABLE no_such_db.t1(a, b, c)")
	if err == nil {
		t.Error("expected error for unknown database, got nil")
	}
	if !strings.Contains(err.Error(), "unknown database") && !strings.Contains(err.Error(), "no such") {
		t.Errorf("expected 'unknown database' error, got: %v", err)
	}
}

// openAttachTestDB creates an in-memory test database
func openAttachTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}
