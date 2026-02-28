package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
	// Open database
	db, err := sql.Open("sqlite_internal", "demo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer os.Remove("demo.db")

	fmt.Println("SQLite JSON Functions Demo")
	fmt.Println("===========================\n")

	// Test 1: json_valid
	fmt.Println("1. Testing json_valid():")
	testJSONValid(db)
	fmt.Println()

	// Test 2: json_type
	fmt.Println("2. Testing json_type():")
	testJSONType(db)
	fmt.Println()

	// Test 3: json_extract
	fmt.Println("3. Testing json_extract():")
	testJSONExtract(db)
	fmt.Println()

	// Test 4: json()
	fmt.Println("4. Testing json() - minify:")
	testJSON(db)
	fmt.Println()

	// Test 5: json_array
	fmt.Println("5. Testing json_array():")
	testJSONArray(db)
	fmt.Println()

	// Test 6: json_object
	fmt.Println("6. Testing json_object():")
	testJSONObject(db)
	fmt.Println()

	// Test 7: Real-world example with table
	fmt.Println("7. Real-world example with table:")
	testRealWorld(db)
}

func testJSONValid(db *sql.DB) {
	tests := []string{
		`SELECT json_valid('{"a":1}')`,
		`SELECT json_valid('[1,2,3]')`,
		`SELECT json_valid('{invalid}')`,
	}

	for _, query := range tests {
		var result int64
		err := db.QueryRow(query).Scan(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   %s => %d\n", query, result)
	}
}

func testJSONType(db *sql.DB) {
	tests := []string{
		`SELECT json_type('123')`,
		`SELECT json_type('{"a":1}')`,
		`SELECT json_type('[1,2,3]')`,
		`SELECT json_type('"hello"')`,
		`SELECT json_type('null')`,
	}

	for _, query := range tests {
		var result string
		err := db.QueryRow(query).Scan(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   %s => %s\n", query, result)
	}
}

func testJSONExtract(db *sql.DB) {
	// Test extracting different types
	var intResult int64
	err := db.QueryRow(`SELECT json_extract('{"a":1}', '$.a')`).Scan(&intResult)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_extract('{\"a\":1}', '$.a') => %d\n", intResult)

	var strResult string
	err = db.QueryRow(`SELECT json_extract('{"name":"John"}', '$.name')`).Scan(&strResult)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_extract('{\"name\":\"John\"}', '$.name') => %s\n", strResult)

	// Nested extraction
	err = db.QueryRow(`SELECT json_extract('{"a":{"b":{"c":42}}}', '$.a.b.c')`).Scan(&intResult)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_extract('{\"a\":{\"b\":{\"c\":42}}}', '$.a.b.c') => %d\n", intResult)

	// Array extraction
	err = db.QueryRow(`SELECT json_extract('[1,2,3]', '$[1]')`).Scan(&intResult)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_extract('[1,2,3]', '$[1]') => %d\n", intResult)
}

func testJSON(db *sql.DB) {
	var result string

	err := db.QueryRow(`SELECT json('{"a": 1, "b": 2}')`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json('{\"a\": 1, \"b\": 2}') => %s\n", result)

	err = db.QueryRow(`SELECT json('[1, 2, 3]')`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json('[1, 2, 3]') => %s\n", result)
}

func testJSONArray(db *sql.DB) {
	var result string

	err := db.QueryRow(`SELECT json_array()`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_array() => %s\n", result)

	err = db.QueryRow(`SELECT json_array(1, 2, 3)`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_array(1, 2, 3) => %s\n", result)

	err = db.QueryRow(`SELECT json_array(1, 'hello', 3.14)`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_array(1, 'hello', 3.14) => %s\n", result)
}

func testJSONObject(db *sql.DB) {
	var result string

	err := db.QueryRow(`SELECT json_object()`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_object() => %s\n", result)

	err = db.QueryRow(`SELECT json_object('x', 1)`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_object('x', 1) => %s\n", result)

	err = db.QueryRow(`SELECT json_object('name', 'Alice', 'age', 30)`).Scan(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   SELECT json_object('name', 'Alice', 'age', 30) => %s\n", result)
}

func testRealWorld(db *sql.DB) {
	// Create a table with JSON data
	_, err := db.Exec(`CREATE TABLE users (id INTEGER, profile TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some users with JSON profiles
	_, err = db.Exec(`INSERT INTO users VALUES (1, '{"name":"Alice","age":30,"city":"NYC"}')`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO users VALUES (2, '{"name":"Bob","age":25,"city":"LA"}')`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO users VALUES (3, '{"name":"Charlie","age":35,"city":"SF"}')`)
	if err != nil {
		log.Fatal(err)
	}

	// Query 1: Extract all names
	fmt.Println("   Query: SELECT id, json_extract(profile, '$.name') AS name FROM users")
	rows, err := db.Query(`SELECT id, json_extract(profile, '$.name') AS name FROM users ORDER BY id`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("      User %d: %s\n", id, name)
	}

	// Query 2: Filter by age > 28
	fmt.Println("\n   Query: SELECT id, json_extract(profile, '$.name'), json_extract(profile, '$.age') FROM users WHERE json_extract(profile, '$.age') > 28")
	rows, err = db.Query(`SELECT id, json_extract(profile, '$.name'), json_extract(profile, '$.age') FROM users WHERE json_extract(profile, '$.age') > 28 ORDER BY id`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		var age int
		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("      User %d: %s (age: %d)\n", id, name, age)
	}

	// Query 3: Validate all profiles
	fmt.Println("\n   Query: SELECT COUNT(*) FROM users WHERE json_valid(profile) = 1")
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM users WHERE json_valid(profile) = 1`).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("      Valid JSON profiles: %d\n", count)
}
