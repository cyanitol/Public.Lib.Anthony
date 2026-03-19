// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ExampleDriver demonstrates basic usage of the SQLite driver.
func ExampleDriver() {
	// Open a database connection
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Database connected successfully")

	// Output:
	// Database connected successfully
}

// ExampleDriver_transaction demonstrates transaction usage.
func ExampleDriver_transaction() {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Do some work...
	// (In this example, we just commit immediately)

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Transaction committed")

	// Output:
	// Transaction committed
}

// ExampleDriver_rollback demonstrates transaction rollback.
func ExampleDriver_rollback() {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Rollback transaction
	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Transaction rolled back")

	// Output:
	// Transaction rolled back
}

// ExampleDriver_prepare demonstrates prepared statements.
func ExampleDriver_prepare() {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Prepare a statement
	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	fmt.Println("Statement prepared")

	// Output:
	// Statement prepared
}
