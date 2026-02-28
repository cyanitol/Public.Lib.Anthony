package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
	// Open an in-memory database
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("=== Testing EXPLAIN QUERY PLAN ===")
	fmt.Println()

	// Test 1: Simple SELECT
	fmt.Println("1. EXPLAIN QUERY PLAN SELECT * FROM users:")
	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM users")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	// Test 2: SELECT with WHERE
	fmt.Println("2. EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1:")
	rows, err = db.Query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	// Test 3: SELECT with ORDER BY
	fmt.Println("3. EXPLAIN QUERY PLAN SELECT * FROM users ORDER BY name:")
	rows, err = db.Query("EXPLAIN QUERY PLAN SELECT * FROM users ORDER BY name")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	// Create another table for JOIN test
	_, err = db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")
	if err != nil {
		log.Fatalf("Failed to create orders table: %v", err)
	}

	// Test 4: JOIN
	fmt.Println("4. EXPLAIN QUERY PLAN SELECT * FROM users JOIN orders ON users.id = orders.user_id:")
	rows, err = db.Query("EXPLAIN QUERY PLAN SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	fmt.Println("=== Testing EXPLAIN (VDBE opcodes) ===")
	fmt.Println()

	// Test 5: EXPLAIN opcodes
	fmt.Println("5. EXPLAIN SELECT * FROM users:")
	rows, err = db.Query("EXPLAIN SELECT * FROM users")
	if err != nil {
		log.Fatalf("EXPLAIN failed: %v", err)
	}
	printOpcodes(rows)
	rows.Close()
	fmt.Println()

	// Test 6: INSERT
	fmt.Println("6. EXPLAIN QUERY PLAN INSERT INTO users (name, age) VALUES ('Alice', 25):")
	rows, err = db.Query("EXPLAIN QUERY PLAN INSERT INTO users (name, age) VALUES ('Alice', 25)")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN INSERT failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	// Test 7: UPDATE
	fmt.Println("7. EXPLAIN QUERY PLAN UPDATE users SET age = 30 WHERE id = 1:")
	rows, err = db.Query("EXPLAIN QUERY PLAN UPDATE users SET age = 30 WHERE id = 1")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN UPDATE failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	// Test 8: DELETE
	fmt.Println("8. EXPLAIN QUERY PLAN DELETE FROM users WHERE age < 18:")
	rows, err = db.Query("EXPLAIN QUERY PLAN DELETE FROM users WHERE age < 18")
	if err != nil {
		log.Fatalf("EXPLAIN QUERY PLAN DELETE failed: %v", err)
	}
	printQueryPlan(rows)
	rows.Close()
	fmt.Println()

	fmt.Println("All tests completed successfully!")
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
