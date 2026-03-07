// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ExampleParseDSN demonstrates how to parse DSN strings
func ExampleParseDSN() {
	// Parse a simple filename
	dsn1, err := driver.ParseDSN("test.db")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Simple filename:", dsn1.Filename)

	// Parse with read-only mode
	dsn2, err := driver.ParseDSN("test.db?mode=ro")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Read-only:", dsn2.Config.Pager.ReadOnly)

	// Parse with multiple parameters
	dsn3, err := driver.ParseDSN("test.db?journal_mode=wal&cache_size=10000&foreign_keys=on")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Journal mode:", dsn3.Config.Pager.JournalMode)
	fmt.Println("Cache size:", dsn3.Config.Pager.CacheSize)
	fmt.Println("Foreign keys:", dsn3.Config.EnableForeignKeys)

	// Output:
	// Simple filename: test.db
	// Read-only: true
	// Journal mode: wal
	// Cache size: 10000
	// Foreign keys: true
}

// ExampleDSN_withDatabase demonstrates using DSN parameters with sql.Open
func ExampleDSN_withDatabase() {
	// Create a temporary database for the example
	tmpDir, err := os.MkdirTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "example.db")

	// Open database with WAL mode and foreign keys enabled
	dsn := dbPath + "?journal_mode=wal&foreign_keys=on&cache_size=5000"
	db, err := sql.Open(driver.DriverName, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some data
	_, err = db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
	if err != nil {
		log.Fatal(err)
	}

	// Query the data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("User count:", count)

	// Output:
	// User count: 1
}

// ExampleDSN_readOnly demonstrates read-only mode
func ExampleDSN_readOnly() {
	// Create a temporary database
	tmpDir, err := os.MkdirTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "example.db")

	// Create and populate the database
	db, err := sql.Open(driver.DriverName, dbPath)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE data (value INTEGER)")
	if err != nil {
		db.Close()
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO data VALUES (42)")
	if err != nil {
		db.Close()
		log.Fatal(err)
	}
	db.Close()

	// Open in read-only mode
	db, err = sql.Open(driver.DriverName, dbPath+"?mode=ro")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Read data
	var value int
	err = db.QueryRow("SELECT value FROM data").Scan(&value)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Value:", value)

	// Attempting to write will fail
	_, err = db.Exec("INSERT INTO data VALUES (100)")
	if err != nil {
		fmt.Println("Write failed (as expected)")
	}

	// Output:
	// Value: 42
	// Write failed (as expected)
}

// ExampleDSN_memory demonstrates in-memory database with parameters
func ExampleDSN_memory() {
	// Open an in-memory database with foreign keys enabled
	db, err := sql.Open(driver.DriverName, ":memory:?foreign_keys=on")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables with foreign key constraint
	_, err = db.Exec(`
		CREATE TABLE authors (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE books (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			author_id INTEGER,
			FOREIGN KEY (author_id) REFERENCES authors(id)
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert an author
	_, err = db.Exec("INSERT INTO authors (id, name) VALUES (1, 'J.K. Rowling')")
	if err != nil {
		log.Fatal(err)
	}

	// Insert a book with valid author
	_, err = db.Exec("INSERT INTO books (title, author_id) VALUES ('Harry Potter', 1)")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Valid insert succeeded")

	// Try to insert a book with invalid author
	// NOTE: Foreign key constraints are not yet implemented, so this succeeds
	_, err = db.Exec("INSERT INTO books (title, author_id) VALUES ('Unknown Book', 999)")
	if err != nil {
		fmt.Println("Foreign key constraint enforced")
	} else {
		fmt.Println("Insert succeeded (foreign keys not yet enforced)")
	}

	// Output:
	// Valid insert succeeded
	// Insert succeeded (foreign keys not yet enforced)
}
