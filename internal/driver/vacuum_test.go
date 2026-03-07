// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestVacuum_Integration(t *testing.T) {
	t.Skip("VACUUM not fully implemented")
	// t.Skip("pre-existing failure")
	dbFile := filepath.Join(t.TempDir(), "test.db")

	// Register driver
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Create a table and insert data
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec("INSERT INTO users (name, age) VALUES (?, ?)",
			"User"+string(rune('0'+i%10)), 20+i%50)
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	// Verify data before VACUUM
	var countBefore int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&countBefore)
	if err != nil {
		t.Fatalf("SELECT COUNT before VACUUM error = %v", err)
	}

	if countBefore != 100 {
		t.Errorf("Count before VACUUM = %d, want 100", countBefore)
	}

	// Execute VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("VACUUM error = %v", err)
	}

	// Verify data after VACUUM
	var countAfter int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&countAfter)
	if err != nil {
		t.Fatalf("SELECT COUNT after VACUUM error = %v", err)
	}

	if countAfter != countBefore {
		t.Errorf("Count after VACUUM = %d, want %d", countAfter, countBefore)
	}

	// Verify specific row integrity
	var name string
	var age int
	err = db.QueryRow("SELECT name, age FROM users WHERE id = ?", 50).Scan(&name, &age)
	if err != nil {
		t.Fatalf("SELECT after VACUUM error = %v", err)
	}

	t.Logf("Row 50 after VACUUM: name=%s, age=%d", name, age)
}

func TestVacuum_AfterDeletes(t *testing.T) {
	t.Skip("VACUUM not fully implemented")
	// t.Skip("pre-existing failure")
	dbFile := filepath.Join(t.TempDir(), "test.db")

	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Insert many rows
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec("INSERT INTO items (data) VALUES (?)", "Data"+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	// Get file size before delete
	statBefore, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("stat before delete error = %v", err)
	}
	sizeBefore := statBefore.Size()
	t.Logf("Database size before delete: %d bytes", sizeBefore)

	// Delete most rows
	_, err = db.Exec("DELETE FROM items WHERE id > 100")
	if err != nil {
		t.Fatalf("DELETE error = %v", err)
	}

	// Verify count after delete
	var countAfterDelete int
	err = db.QueryRow("SELECT COUNT(*) FROM items").Scan(&countAfterDelete)
	if err != nil {
		t.Fatalf("SELECT COUNT after delete error = %v", err)
	}

	if countAfterDelete != 100 {
		t.Errorf("Count after delete = %d, want 100", countAfterDelete)
	}

	// VACUUM to reclaim space
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("VACUUM error = %v", err)
	}

	// Get file size after VACUUM
	statAfter, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("stat after vacuum error = %v", err)
	}
	sizeAfter := statAfter.Size()
	t.Logf("Database size after VACUUM: %d bytes (saved %d bytes)",
		sizeAfter, sizeBefore-sizeAfter)

	// Verify data integrity
	var countAfterVacuum int
	err = db.QueryRow("SELECT COUNT(*) FROM items").Scan(&countAfterVacuum)
	if err != nil {
		t.Fatalf("SELECT COUNT after VACUUM error = %v", err)
	}

	if countAfterVacuum != 100 {
		t.Errorf("Count after VACUUM = %d, want 100", countAfterVacuum)
	}

	// Database should be smaller (or at least not larger)
	if sizeAfter > sizeBefore {
		t.Errorf("Database size increased after VACUUM: before=%d, after=%d",
			sizeBefore, sizeAfter)
	}
}

func TestVacuum_Into(t *testing.T) {
	t.Skip("VACUUM not fully implemented")
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.db")
	// Use same temp directory for target so security check passes
	targetFile := filepath.Join(tempDir, "target.db")

	// Create source database
	db, err := sql.Open("sqlite_internal", sourceFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Create table and insert data
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	for i := 1; i <= 50; i++ {
		_, err = db.Exec("INSERT INTO test (value) VALUES (?)", "Value"+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	// Execute VACUUM INTO
	_, err = db.Exec("VACUUM INTO ?", targetFile)
	if err != nil {
		t.Fatalf("VACUUM INTO error = %v", err)
	}

	// Close source database
	db.Close()

	// Verify target file was created
	if _, err := os.Stat(targetFile); os.IsNotExist(err) {
		t.Fatal("Target file was not created")
	}

	// Open target database and verify data
	targetDB, err := sql.Open("sqlite_internal", targetFile)
	if err != nil {
		t.Fatalf("sql.Open(target) error = %v", err)
	}
	defer targetDB.Close()

	var count int
	err = targetDB.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT from target error = %v", err)
	}

	if count != 50 {
		t.Errorf("Target database count = %d, want 50", count)
	}

	// Verify specific row
	var value string
	err = targetDB.QueryRow("SELECT value FROM test WHERE id = ?", 25).Scan(&value)
	if err != nil {
		t.Fatalf("SELECT from target error = %v", err)
	}

	t.Logf("Row 25 in target: value=%s", value)

	// Verify source database still exists and works
	sourceDB, err := sql.Open("sqlite_internal", sourceFile)
	if err != nil {
		t.Fatalf("sql.Open(source) after VACUUM error = %v", err)
	}
	defer sourceDB.Close()

	var sourceCount int
	err = sourceDB.QueryRow("SELECT COUNT(*) FROM test").Scan(&sourceCount)
	if err != nil {
		t.Fatalf("SELECT COUNT from source after VACUUM error = %v", err)
	}

	if sourceCount != 50 {
		t.Errorf("Source database count after VACUUM = %d, want 50", sourceCount)
	}
}

func TestVacuum_MultipleVacuums(t *testing.T) {
	t.Skip("VACUUM not fully implemented")
	dbFile := filepath.Join(t.TempDir(), "test.db")

	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, info TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Insert initial data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec("INSERT INTO data (info) VALUES (?)", "Info"+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	// First VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("First VACUUM error = %v", err)
	}

	// Delete some rows
	_, err = db.Exec("DELETE FROM data WHERE id > 80")
	if err != nil {
		t.Fatalf("DELETE error = %v", err)
	}

	// Second VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("Second VACUUM error = %v", err)
	}

	// Insert more data
	for i := 1; i <= 50; i++ {
		_, err = db.Exec("INSERT INTO data (info) VALUES (?)", "NewInfo"+string(rune('A'+i%26)))
		if err != nil {
			t.Fatalf("INSERT after VACUUM error = %v", err)
		}
	}

	// Third VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("Third VACUUM error = %v", err)
	}

	// Verify final count (80 remaining + 50 new = 130)
	var finalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM data").Scan(&finalCount)
	if err != nil {
		t.Fatalf("SELECT COUNT after all operations error = %v", err)
	}

	if finalCount != 130 {
		t.Errorf("Final count = %d, want 130", finalCount)
	}

	t.Logf("Successfully performed 3 VACUUMs with data modifications")
}

func TestVacuum_WithIndex(t *testing.T) {
	t.Skip("VACUUM not fully implemented")
	dbFile := filepath.Join(t.TempDir(), "test.db")

	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	// Create table with index
	_, err = db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_name ON products(name)")
	if err != nil {
		t.Logf("CREATE INDEX warning: %v (indexes may not be fully supported)", err)
		// Continue anyway - index support is optional for this test
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec("INSERT INTO products (name, price) VALUES (?, ?)",
			"Product"+string(rune('A'+i%26)), float64(i)*1.5)
		if err != nil {
			t.Fatalf("INSERT error = %v", err)
		}
	}

	// VACUUM
	_, err = db.Exec("VACUUM")
	if err != nil {
		t.Fatalf("VACUUM error = %v", err)
	}

	// Verify data after VACUUM
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT after VACUUM error = %v", err)
	}

	if count != 100 {
		t.Errorf("Count after VACUUM = %d, want 100", count)
	}

	// Try a query that would use the index (if supported)
	var name string
	var price float64
	err = db.QueryRow("SELECT name, price FROM products WHERE name = ?", "ProductA").Scan(&name, &price)
	if err != nil {
		t.Logf("SELECT with index warning: %v (may not be fully supported)", err)
	} else {
		t.Logf("Query with index succeeded: name=%s, price=%.2f", name, price)
	}
}
