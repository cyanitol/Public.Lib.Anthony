// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func main() {
	db := setupDatabase()
	defer db.Close()

	runQueryPlanTests(db)
	runOpcodeTests(db)
	runDMLTests(db)

	fmt.Println("All tests completed successfully!")
}

func setupDatabase() *sql.DB {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err := createTestTables(db); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	return db
}

func createTestTables(db *sql.DB) error {
	tables := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return err
		}
	}

	return nil
}

func runQueryPlanTests(db *sql.DB) {
	fmt.Println("=== Testing EXPLAIN QUERY PLAN ===")
	fmt.Println()

	tests := []struct {
		description string
		query       string
	}{
		{"1. EXPLAIN QUERY PLAN SELECT * FROM users:", "EXPLAIN QUERY PLAN SELECT * FROM users"},
		{"2. EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1:", "EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1"},
		{"3. EXPLAIN QUERY PLAN SELECT * FROM users ORDER BY name:", "EXPLAIN QUERY PLAN SELECT * FROM users ORDER BY name"},
		{"4. EXPLAIN QUERY PLAN SELECT * FROM users JOIN orders ON users.id = orders.user_id:", "EXPLAIN QUERY PLAN SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"},
	}

	for _, test := range tests {
		executeQueryPlanTest(db, test.description, test.query)
	}
}

func runOpcodeTests(db *sql.DB) {
	fmt.Println("=== Testing EXPLAIN (VDBE opcodes) ===")
	fmt.Println()

	fmt.Println("5. EXPLAIN SELECT * FROM users:")
	rows, err := db.Query("EXPLAIN SELECT * FROM users")
	if err != nil {
		log.Fatalf("EXPLAIN failed: %v", err)
	}
	printOpcodes(rows)
	rows.Close()
	fmt.Println()
}

func runDMLTests(db *sql.DB) {
	tests := []struct {
		description string
		query       string
	}{
		{"6. EXPLAIN QUERY PLAN INSERT INTO users (name, age) VALUES ('Alice', 25):", "EXPLAIN QUERY PLAN INSERT INTO users (name, age) VALUES ('Alice', 25)"},
		{"7. EXPLAIN QUERY PLAN UPDATE users SET age = 30 WHERE id = 1:", "EXPLAIN QUERY PLAN UPDATE users SET age = 30 WHERE id = 1"},
		{"8. EXPLAIN QUERY PLAN DELETE FROM users WHERE age < 18:", "EXPLAIN QUERY PLAN DELETE FROM users WHERE age < 18"},
	}

	for _, test := range tests {
		executeQueryPlanTest(db, test.description, test.query)
	}
}

func executeQueryPlanTest(db *sql.DB, description, query string) {
	fmt.Println(description)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()
}

func printQueryPlan(rows *sql.Rows) {
	fmt.Printf("%-4s  %-8s  %-8s  %s\n", "id", "parent", "notused", "detail")
	fmt.Println("----  --------  --------  " + "------------------------------------------------")

	for rows.Next() {
		var id, parent, notused int
		var detail string

		err := rows.Scan(&id, &parent, &notused, &detail)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Printf("%-4d  %-8d  %-8d  %s\n", id, parent, notused, detail)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}
}

func printOpcodes(rows *sql.Rows) {
	fmt.Printf("%-4s  %-13s  %-4s  %-4s  %-4s  %-12s  %-2s  %s\n",
		"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment")
	fmt.Println("----  -------------  ----  ----  ----  ------------  --  -------")

	count := 0
	for rows.Next() {
		var addr, p1, p2, p3, p5 int
		var opcode, p4, comment string

		err := rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Printf("%-4d  %-13s  %-4d  %-4d  %-4d  %-12s  %-2d  %s\n",
			addr, opcode, p1, p2, p3, p4, p5, comment)
		count++

		// Limit output to first 15 opcodes for readability
		if count >= 15 {
			fmt.Println("... (output truncated)")
			break
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}
}
