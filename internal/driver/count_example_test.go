package driver

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

// Example_preparedStatementCount demonstrates that COUNT(*) works correctly
// with prepared statements, returning the actual count instead of NULL.
func Example_preparedStatementCount() {
	// Create a temporary database
	dbFile := "example_count.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		log.Fatal(err)
	}

	// Insert some data
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO items (id, name) VALUES (?, ?)", i, fmt.Sprintf("Item %d", i))
		if err != nil {
			log.Fatal(err)
		}
	}

	// Use a prepared statement to count rows
	stmt, err := db.Prepare("SELECT COUNT(*) FROM items")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	var count int
	err = stmt.QueryRow().Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total items: %d\n", count)

	// Output: Total items: 5
}

// Example_preparedStatementCountEmpty demonstrates that COUNT(*) returns 0
// for empty tables instead of NULL.
func Example_preparedStatementCountEmpty() {
	// Create a temporary database
	dbFile := "example_count_empty.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create an empty table
	_, err = db.Exec("CREATE TABLE empty_items (id INTEGER PRIMARY KEY)")
	if err != nil {
		log.Fatal(err)
	}

	// Use a prepared statement to count rows
	stmt, err := db.Prepare("SELECT COUNT(*) FROM empty_items")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	var count int
	err = stmt.QueryRow().Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total items: %d\n", count)

	// Output: Total items: 0
}
