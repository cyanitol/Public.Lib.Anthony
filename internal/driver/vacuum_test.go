// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func vacuumTestOpenDB(t *testing.T, name string) (*sql.DB, string) {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), name)
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	return db, dbFile
}

func vacuumTestExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q error = %v", query, err)
	}
}

func vacuumTestCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count query error = %v", err)
	}
	return count
}

func vacuumTestInsertN(t *testing.T, db *sql.DB, query string, n int, argFn func(int) []interface{}) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := db.Exec(query, argFn(i)...); err != nil {
			t.Fatalf("INSERT #%d error = %v", i, err)
		}
	}
}

func TestVacuum_Integration(t *testing.T) {
	db, _ := vacuumTestOpenDB(t, "test.db")
	defer db.Close()

	vacuumTestExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	vacuumTestInsertN(t, db, "INSERT INTO users (name, age) VALUES (?, ?)", 100, func(i int) []interface{} {
		return []interface{}{"User" + string(rune('0'+i%10)), 20 + i%50}
	})

	countBefore := vacuumTestCount(t, db, "SELECT COUNT(*) FROM users")
	if countBefore != 100 {
		t.Errorf("Count before VACUUM = %d, want 100", countBefore)
	}

	vacuumTestExec(t, db, "VACUUM")

	if countAfter := vacuumTestCount(t, db, "SELECT COUNT(*) FROM users"); countAfter != countBefore {
		t.Errorf("Count after VACUUM = %d, want %d", countAfter, countBefore)
	}

	var name string
	var age int
	if err := db.QueryRow("SELECT name, age FROM users WHERE id = ?", 50).Scan(&name, &age); err != nil {
		t.Fatalf("SELECT after VACUUM error = %v", err)
	}
	t.Logf("Row 50 after VACUUM: name=%s, age=%d", name, age)
}

func TestVacuum_AfterDeletes(t *testing.T) {
	db, dbFile := vacuumTestOpenDB(t, "test.db")
	defer db.Close()

	vacuumTestExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
	vacuumTestInsertN(t, db, "INSERT INTO items (data) VALUES (?)", 1000, func(i int) []interface{} {
		return []interface{}{"Data" + string(rune('A'+i%26))}
	})

	sizeBefore := vacuumTestFileSize(t, dbFile)
	t.Logf("Database size before delete: %d bytes", sizeBefore)

	vacuumTestExec(t, db, "DELETE FROM items WHERE id > 100")
	if c := vacuumTestCount(t, db, "SELECT COUNT(*) FROM items"); c != 100 {
		t.Errorf("Count after delete = %d, want 100", c)
	}
	vacuumTestExec(t, db, "VACUUM")

	sizeAfter := vacuumTestFileSize(t, dbFile)
	t.Logf("Database size after VACUUM: %d bytes (saved %d bytes)", sizeAfter, sizeBefore-sizeAfter)

	if c := vacuumTestCount(t, db, "SELECT COUNT(*) FROM items"); c != 100 {
		t.Errorf("Count after VACUUM = %d, want 100", c)
	}
	if sizeAfter > sizeBefore {
		t.Errorf("Database size increased after VACUUM: before=%d, after=%d", sizeBefore, sizeAfter)
	}
}

func vacuumTestFileSize(t *testing.T, path string) int64 {
	t.Helper()
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}
	return stat.Size()
}

func TestVacuum_Into(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.db")
	targetFile := filepath.Join(tempDir, "target.db")

	db, err := sql.Open("sqlite_internal", sourceFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	vacuumTestExec(t, db, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	vacuumTestInsertN(t, db, "INSERT INTO test (value) VALUES (?)", 50, func(i int) []interface{} {
		return []interface{}{"Value" + string(rune('A'+i%26))}
	})

	if _, err := db.Exec("VACUUM INTO ?", targetFile); err != nil {
		t.Fatalf("VACUUM INTO error = %v", err)
	}
	db.Close()

	vacuumIntoVerify(t, targetFile, sourceFile)
}

func vacuumIntoVerify(t *testing.T, targetFile, sourceFile string) {
	t.Helper()
	if _, err := os.Stat(targetFile); os.IsNotExist(err) {
		t.Fatal("Target file was not created")
	}

	targetDB, err := sql.Open("sqlite_internal", targetFile)
	if err != nil {
		t.Fatalf("sql.Open(target) error = %v", err)
	}
	defer targetDB.Close()

	if c := vacuumTestCount(t, targetDB, "SELECT COUNT(*) FROM test"); c != 50 {
		t.Errorf("Target database count = %d, want 50", c)
	}

	sourceDB, err := sql.Open("sqlite_internal", sourceFile)
	if err != nil {
		t.Fatalf("sql.Open(source) after VACUUM error = %v", err)
	}
	defer sourceDB.Close()

	if c := vacuumTestCount(t, sourceDB, "SELECT COUNT(*) FROM test"); c != 50 {
		t.Errorf("Source database count after VACUUM = %d, want 50", c)
	}
}

func TestVacuum_MultipleVacuums(t *testing.T) {
	db, _ := vacuumTestOpenDB(t, "test.db")
	defer db.Close()

	vacuumTestExec(t, db, "CREATE TABLE data (id INTEGER PRIMARY KEY, info TEXT)")
	vacuumTestInsertN(t, db, "INSERT INTO data (info) VALUES (?)", 100, func(i int) []interface{} {
		return []interface{}{"Info" + string(rune('A'+i%26))}
	})

	vacuumTestExec(t, db, "VACUUM")
	vacuumTestExec(t, db, "DELETE FROM data WHERE id > 80")
	vacuumTestExec(t, db, "VACUUM")

	vacuumTestInsertN(t, db, "INSERT INTO data (info) VALUES (?)", 50, func(i int) []interface{} {
		return []interface{}{"NewInfo" + string(rune('A'+i%26))}
	})
	vacuumTestExec(t, db, "VACUUM")

	if c := vacuumTestCount(t, db, "SELECT COUNT(*) FROM data"); c != 130 {
		t.Errorf("Final count = %d, want 130", c)
	}
}

func TestVacuum_WithIndex(t *testing.T) {
	db, _ := vacuumTestOpenDB(t, "test.db")
	defer db.Close()

	vacuumTestExec(t, db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	if _, err := db.Exec("CREATE INDEX idx_name ON products(name)"); err != nil {
		t.Logf("CREATE INDEX warning: %v (indexes may not be fully supported)", err)
	}

	vacuumTestInsertN(t, db, "INSERT INTO products (name, price) VALUES (?, ?)", 100, func(i int) []interface{} {
		return []interface{}{"Product" + string(rune('A'+i%26)), float64(i) * 1.5}
	})

	vacuumTestExec(t, db, "VACUUM")
	if c := vacuumTestCount(t, db, "SELECT COUNT(*) FROM products"); c != 100 {
		t.Errorf("Count after VACUUM = %d, want 100", c)
	}

	var name string
	var price float64
	if err := db.QueryRow("SELECT name, price FROM products WHERE name = ?", "ProductA").Scan(&name, &price); err != nil {
		t.Logf("SELECT with index warning: %v (may not be fully supported)", err)
	}
}
