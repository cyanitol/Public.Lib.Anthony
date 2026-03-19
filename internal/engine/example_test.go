// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/cyanitol/Public.Lib.Anthony/internal/engine"
)

// Example_basic demonstrates basic database operations.
func Example_basic() {
	// Create a temporary database
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example.db")
	defer os.Remove(dbPath)

	// Open the database
	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Execute(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some data
	_, err = db.Execute(`INSERT INTO products (name, price) VALUES ('Widget', 9.99)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`INSERT INTO products (name, price) VALUES ('Gadget', 19.99)`)
	if err != nil {
		log.Fatal(err)
	}

	// Query the data
	rows, err := db.Query(`SELECT id, name, price FROM products`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		var price float64

		if err := rows.Scan(&id, &name, &price); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("ID: %d, Name: %s, Price: $%.2f\n", id, name, price)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// Example_transaction demonstrates transaction usage.
func Example_transaction() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_tx.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert initial data
	_, err = db.Execute(`INSERT INTO accounts (balance) VALUES (100.0)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`INSERT INTO accounts (balance) VALUES (200.0)`)
	if err != nil {
		log.Fatal(err)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Update accounts within transaction
	_, err = tx.Execute(`UPDATE accounts SET balance = balance - 50 WHERE id = 1`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	_, err = tx.Execute(`UPDATE accounts SET balance = balance + 50 WHERE id = 2`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Transaction committed successfully")
}

// Example_preparedStatement demonstrates prepared statement usage.
func Example_preparedStatement() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_stmt.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute(`CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare a statement
	stmt, err := db.Prepare(`INSERT INTO logs (message) VALUES ('Log entry')`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Execute the statement multiple times
	for i := 0; i < 3; i++ {
		_, err := stmt.Execute()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Query to verify
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM logs`).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted %d log entries\n", count)
}

// Example_rollback demonstrates rolling back a transaction.
func Example_rollback() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_rollback.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table and insert initial data
	_, err = db.Execute(`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`INSERT INTO items (name) VALUES ('Item 1')`)
	if err != nil {
		log.Fatal(err)
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Insert more data in transaction
	_, err = tx.Execute(`INSERT INTO items (name) VALUES ('Item 2')`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Rollback the transaction
	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	// Count items (should only be 1)
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Items after rollback: %d\n", count)
}

// Example_multipleSelects demonstrates executing multiple SELECT queries.
func Example_multipleSelects() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_multi.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Execute(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert data
	_, err = db.Execute(`INSERT INTO users (name) VALUES ('Alice')`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`INSERT INTO posts (title) VALUES ('First Post')`)
	if err != nil {
		log.Fatal(err)
	}

	// Query users
	rows, err := db.Query(`SELECT id, name FROM users`)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User: %d - %s\n", id, name)
	}
	rows.Close()

	// Query posts
	rows, err = db.Query(`SELECT id, title FROM posts`)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var id int64
		var title string
		if err := rows.Scan(&id, &title); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Post: %d - %s\n", id, title)
	}
	rows.Close()
}

// Example_createIndex demonstrates creating and dropping an index.
func Example_createIndex() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_index.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute(`CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// Create index on title
	_, err = db.Execute(`CREATE INDEX idx_title ON books (title)`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Index created successfully")

	// Create another index on author
	_, err = db.Execute(`CREATE INDEX idx_author ON books (author)`)
	if err != nil {
		log.Fatal(err)
	}

	// Drop an index
	_, err = db.Execute(`DROP INDEX idx_author`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Index dropped successfully")
}

// Example_schema demonstrates accessing schema information.
func Example_schema() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_schema.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create some tables
	_, err = db.Execute(`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)`)
	if err != nil {
		log.Fatal(err)
	}

	// Get schema
	schema := db.GetSchema()
	tables := schema.ListTables()

	fmt.Println("Tables in database:")
	for _, tableName := range tables {
		table, ok := schema.GetTable(tableName)
		if ok {
			fmt.Printf("  - %s (%d columns)\n", table.Name, len(table.Columns))
		}
	}
}

// Example_queryRow demonstrates the QueryRow convenience method.
func Example_queryRow() {
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example_qrow.db")
	defer os.Remove(dbPath)

	db, err := engine.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Execute(`CREATE TABLE config (key TEXT, value TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Execute(`INSERT INTO config (key, value) VALUES ('version', '1.0.0')`)
	if err != nil {
		log.Fatal(err)
	}

	// Query single value
	var version string
	err = db.QueryRow(`SELECT value FROM config WHERE key = 'version'`).Scan(&version)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Version: %s\n", version)
}
