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

func attachExecAll(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec failed: %v\nSQL: %s", err, s)
		}
	}
}

func attachAssertCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != want {
		t.Errorf("got %d rows, want %d", count, want)
	}
}

func attachAssertQueryFails(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	rows, err := db.Query(query)
	if err == nil {
		rows.Close()
		t.Errorf("expected error for query %q, got nil", query)
	}
}

func attachAssertQuerySucceeds(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v\nSQL: %s", err, query)
	}
	rows.Close()
}

// TestAttachBasic tests basic ATTACH DATABASE functionality (attach.test 1.1-1.7)
func TestAttachBasic(t *testing.T) {
	tmpDir := t.TempDir()

	db, _ := setupAttachTestDBInDir(t, tmpDir, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDBInDir(t, tmpDir, "test2.db")

	attachExecAll(t, db, []string{
		"CREATE TABLE t1(a,b)",
		"INSERT INTO t1 VALUES(1,2)",
		"INSERT INTO t1 VALUES(3,4)",
	})
	attachExecAll(t, db2, []string{
		"CREATE TABLE t2(x,y)",
		"INSERT INTO t2 VALUES(1,'x')",
		"INSERT INTO t2 VALUES(2,'y')",
	})
	db2.Close()

	attachExecAll(t, db, []string{fmt.Sprintf("ATTACH DATABASE '%s' AS two", db2Path)})
	attachAssertCount(t, db, "SELECT * FROM two.t2", 2)
	attachAssertQuerySucceeds(t, db, "SELECT * FROM t2")

	attachExecAll(t, db, []string{"DETACH DATABASE two"})
	attachAssertQuerySucceeds(t, db, "SELECT * FROM t1")
	attachAssertQueryFails(t, db, "SELECT * FROM t2")
	attachAssertQueryFails(t, db, "SELECT * FROM two.t2")
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

func attachAssertRow2Int(t *testing.T, db *sql.DB, query string, wantA, wantB int) {
	t.Helper()
	var a, b int
	if err := db.QueryRow(query).Scan(&a, &b); err != nil {
		t.Fatalf("query failed: %v\nSQL: %s", err, query)
	}
	if a != wantA || b != wantB {
		t.Errorf("got (%d,%d), want (%d,%d)", a, b, wantA, wantB)
	}
}

// TestAttachCrossDatabase tests cross-database queries (attach.test 4.1-4.5)
func TestAttachCrossDatabase(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")

	attachExecAll(t, db, []string{
		"CREATE TABLE t3(a,b)",
		"CREATE UNIQUE INDEX t3i1b ON t3(a)",
		"INSERT INTO t3 VALUES(9,10)",
	})
	attachExecAll(t, db2, []string{
		"CREATE TABLE t3(x,y)",
		"CREATE UNIQUE INDEX t3i1 ON t3(x)",
		"INSERT INTO t3 VALUES(1,2)",
	})
	db2.Close()

	attachExecAll(t, db, []string{fmt.Sprintf("ATTACH DATABASE '%s' AS db2", db2Path)})
	attachAssertRow2Int(t, db, "SELECT * FROM db2.t3", 1, 2)
	attachAssertRow2Int(t, db, "SELECT * FROM main.t3", 9, 10)
	attachExecAll(t, db, []string{"INSERT INTO db2.t3 VALUES(9,10)"})
}

func attachAssertIntCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v\nSQL: %s", err, query)
	}
	if count != want {
		t.Errorf("got count %d, want %d\nSQL: %s", count, want, query)
	}
}

// TestAttachCreateSchema tests creating schema objects in attached databases (attach3.test 1.1-1.5)
func TestAttachCreateSchema(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	attachExecAll(t, db, []string{"CREATE TABLE t1(a, b)", "CREATE TABLE t2(c, d)"})
	attachExecAll(t, db2, []string{"CREATE TABLE t1(a, b)", "CREATE TABLE t2(c, d)"})
	db2.Close()

	attachExecAll(t, db, []string{
		fmt.Sprintf("ATTACH '%s' AS aux", db2Path),
		"CREATE TABLE aux.t3(e, f)",
	})

	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM sqlite_master WHERE name = 't3'", 0)
	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 't3'", 1)

	attachExecAll(t, db, []string{"INSERT INTO t3 VALUES(1, 2)"})
	attachAssertRow2Int(t, db, "SELECT * FROM t3", 1, 2)
}

// TestAttachCreateIndex tests creating indexes in attached databases (attach3.test 2.1-3.3)
func TestAttachCreateIndex(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	attachExecAll(t, db2, []string{"CREATE TABLE t3(e, f)"})
	db2.Close()

	attachExecAll(t, db, []string{
		fmt.Sprintf("ATTACH '%s' AS aux", db2Path),
		"CREATE INDEX aux.i1 ON t3(e)",
	})

	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'i1'", 0)
	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 'i1'", 1)

	attachExecAll(t, db, []string{"DROP INDEX aux.i1"})
	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 'i1'", 0)

	attachExecAll(t, db, []string{
		"CREATE INDEX aux.i1 ON t3(e)",
		"DROP INDEX i1",
	})
}

// TestAttachDropTable tests dropping tables in attached databases (attach3.test 4.1-4.3)
func TestAttachDropTable(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	attachExecAll(t, db, []string{"CREATE TABLE t2(c, d)"})
	attachExecAll(t, db2, []string{"CREATE TABLE t1(a, b)", "CREATE TABLE t2(c, d)", "CREATE TABLE t3(e, f)"})
	db2.Close()

	attachExecAll(t, db, []string{
		fmt.Sprintf("ATTACH '%s' AS aux", db2Path),
		"DROP TABLE aux.t1",
	})

	attachAssertStringList(t, db, "SELECT name FROM aux.sqlite_master WHERE type='table' ORDER BY name",
		[]string{"t2", "t3"})

	attachExecAll(t, db, []string{"DROP TABLE t2"})
	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM aux.sqlite_master WHERE name = 't2'", 1)
}

func attachAssertStringList(t *testing.T, db *sql.DB, query string, want []string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, s)
	}
	if len(got) != len(want) {
		t.Errorf("got %v, want %v", got, want)
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

	attachExecAll(t, db, []string{
		fmt.Sprintf("ATTACH '%s' AS aux1", test4Path),
		"CREATE TABLE aux1.t1(a, b)",
		"INSERT INTO aux1.t1 VALUES(1, 2)",
		fmt.Sprintf("ATTACH '%s' AS aux2", test4Path),
	})

	attachAssertRow2Int(t, db, "SELECT * FROM aux2.t1", 1, 2)
	attachSameFileTwiceWriteTest(t, db)
}

func attachSameFileTwiceWriteTest(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if _, err = tx.Exec("INSERT INTO aux1.t1 VALUES(3, 4)"); err != nil {
		t.Fatalf("failed to insert into aux1: %v", err)
	}
	_, err = tx.Exec("INSERT INTO aux2.t1 VALUES(5, 6)")
	if err != nil {
		tx.Rollback()
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

	attachExecAll(t, db, []string{
		"ATTACH '' AS noname",
		"ATTACH ':memory:' AS inmem",
	})

	attachMemoryCreateTables(t, db)

	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM noname.sqlite_master WHERE name='noname'", 1)
	attachAssertIntCount(t, db, "SELECT COUNT(*) FROM inmem.sqlite_master WHERE name='inmem'", 1)
}

func attachMemoryCreateTables(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE noname.noname(x)",
		"CREATE TABLE inmem.inmem(y)",
		"CREATE TABLE main.main(z)",
	} {
		if _, err = tx.Exec(stmt); err != nil {
			tx.Rollback()
			t.Fatalf("exec failed: %v\nSQL: %s", err, stmt)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func attachMany(t *testing.T, db *sql.DB, tmpDir string, count int) []string {
	t.Helper()
	var attached []string
	for i := 0; i < count; i++ {
		alias := fmt.Sprintf("aux%d", i)
		attachPath := filepath.Join(tmpDir, fmt.Sprintf("test%d.db", i))
		_, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS %s", attachPath, alias))
		if err != nil {
			if !strings.Contains(err.Error(), "too many attached") {
				t.Errorf("expected 'too many attached' error at %d attachments, got: %v", i, err)
			}
			break
		}
		attached = append(attached, alias)
	}
	return attached
}

// TestAttachMaxDatabases tests the maximum number of attached databases (attach4.test 1.1-1.2)
func TestAttachMaxDatabases(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, dbPath := setupAttachTestDB(t, "test.db")
	defer db.Close()

	tmpDir := t.TempDir()
	attached := attachMany(t, db, tmpDir, 10)

	if len(attached) < 3 {
		t.Errorf("expected to attach at least 3 databases, got %d", len(attached))
	}

	oneMore := filepath.Join(tmpDir, "testone_more.db")
	if _, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS one_more", oneMore)); err != nil {
		t.Logf("attachment limit reached at %d databases", len(attached))
	}

	if len(attached) > 0 {
		if _, err := db.Exec(fmt.Sprintf("DETACH %s", attached[0])); err != nil {
			t.Fatalf("failed to detach: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf("ATTACH '%s' AS reattached", dbPath)); err != nil {
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
	attachExecAll(t, db, []string{"CREATE TABLE t3(a,b)", "INSERT INTO t3 VALUES(9,10)"})
	attachExecAll(t, db2, []string{"CREATE TABLE t3(x,y)", "INSERT INTO t3 VALUES(1,2)"})
	db2.Close()

	attachExecAll(t, db, []string{fmt.Sprintf("ATTACH DATABASE '%s' AS db2", db2Path)})
	attachAssertRow2Int(t, db, "SELECT * FROM db2.t3", 1, 2)
	attachAssertRow2Int(t, db, "SELECT * FROM main.t3", 9, 10)
}

// TestAttachSchemaNamespace tests that schemas in different databases are independent (attach.test 2.7-2.16)
func TestAttachSchemaNamespace(t *testing.T) {
	t.Skip("ATTACH not implemented")
	db, _ := setupAttachTestDB(t, "test.db")
	defer db.Close()

	db2, db2Path := setupAttachTestDB(t, "test2.db")
	attachExecAll(t, db, []string{"CREATE TABLE tx(x1,x2,y1,y2)"})
	attachExecAll(t, db2, []string{
		"CREATE TABLE t2(x,y)",
		"INSERT INTO t2 VALUES(1,'x')",
		"CREATE TABLE tx(x1,x2,y1,y2)",
	})
	db2.Close()

	attachExecAll(t, db, []string{fmt.Sprintf("ATTACH '%s' AS db2", db2Path)})
	attachAssertCount(t, db, "SELECT * FROM main.tx", 0)

	var schemaCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM db2.sqlite_master WHERE type IN ('table','index')").Scan(&schemaCount); err != nil {
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
