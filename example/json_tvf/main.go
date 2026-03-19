// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// JSON functions and table-valued functions (json_each, json_tree) including
// correlated cross-joins where TVF arguments reference outer table columns.
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func main() {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	jsonScalarFunctions(db)
	jsonEachBasic(db)
	jsonTreeBasic(db)
	correlatedTVFJoin(db)
	jsonAggregates(db)
}

// jsonScalarFunctions shows json_extract, json_type, json_valid, and json_object.
func jsonScalarFunctions(db *sql.DB) {
	fmt.Println("=== JSON Scalar Functions ===")

	// json_extract: pull values from JSON documents
	var name string
	db.QueryRow(`SELECT json_extract('{"user":"alice","score":42}', '$.user')`).Scan(&name)
	fmt.Printf("  json_extract(doc, '$.user') = %s\n", name)

	// json_type: inspect value types
	var typ string
	db.QueryRow(`SELECT json_type('[1, "two", null, true]', '$[0]')`).Scan(&typ)
	fmt.Printf("  json_type(array, '$[0]')    = %s\n", typ)

	// json_object: build JSON from key/value pairs
	var obj string
	db.QueryRow(`SELECT json_object('name', 'Bob', 'active', 1)`).Scan(&obj)
	fmt.Printf("  json_object(...)            = %s\n", obj)

	// json_array: build JSON arrays
	var arr string
	db.QueryRow(`SELECT json_array(10, 'hello', NULL)`).Scan(&arr)
	fmt.Printf("  json_array(10,'hello',NULL) = %s\n", arr)

	fmt.Println()
}

// jsonEachBasic demonstrates json_each as a table-valued function in FROM.
func jsonEachBasic(db *sql.DB) {
	fmt.Println("=== json_each() — Iterate Arrays and Objects ===")

	// Iterate an array
	fmt.Println("  Array [10, 20, 30]:")
	rows, _ := db.Query(`SELECT key, value, type FROM json_each('[10,20,30]') ORDER BY key`)
	defer rows.Close()
	for rows.Next() {
		var key int
		var value, typ string
		rows.Scan(&key, &value, &typ)
		fmt.Printf("    [%d] = %s (%s)\n", key, value, typ)
	}

	// Iterate an object
	fmt.Println("  Object {\"a\":1, \"b\":2}:")
	rows2, _ := db.Query(`SELECT key, value FROM json_each('{"a":1,"b":2}') ORDER BY key`)
	defer rows2.Close()
	for rows2.Next() {
		var key, value string
		rows2.Scan(&key, &value)
		fmt.Printf("    .%s = %s\n", key, value)
	}

	// With path: drill into nested structure
	fmt.Println("  Nested path $.config.ports:")
	rows3, _ := db.Query(`
		SELECT value FROM json_each('{"config":{"ports":[80,443,8080]}}', '$.config.ports')
		ORDER BY key
	`)
	defer rows3.Close()
	for rows3.Next() {
		var val int
		rows3.Scan(&val)
		fmt.Printf("    %d\n", val)
	}

	fmt.Println()
}

// jsonTreeBasic demonstrates json_tree for recursive traversal.
func jsonTreeBasic(db *sql.DB) {
	fmt.Println("=== json_tree() — Recursive Traversal ===")

	rows, _ := db.Query(`
		SELECT key, value, type, atom
		FROM json_tree('{"a":{"b":[1,2]},"c":3}')
		WHERE atom IS NOT NULL
		ORDER BY id
	`)
	defer rows.Close()

	fmt.Println("  Leaf nodes of {\"a\":{\"b\":[1,2]},\"c\":3}:")
	for rows.Next() {
		var key, value, typ string
		var atom sql.NullString
		rows.Scan(&key, &value, &typ, &atom)
		fmt.Printf("    key=%-4s value=%-4s type=%s\n", key, value, typ)
	}
	fmt.Println()
}

// correlatedTVFJoin demonstrates the correlated TVF cross-join pattern:
// FROM table, json_each(table.column) — the TVF argument references a column
// from the outer table, evaluated per-row.
func correlatedTVFJoin(db *sql.DB) {
	fmt.Println("=== Correlated TVF Cross-Join ===")
	fmt.Println("  (FROM table, json_each(table.json_col) — per-row evaluation)")
	fmt.Println()

	// Create a table where each row has a JSON array of tags
	db.Exec(`CREATE TABLE repos (
		id   INTEGER PRIMARY KEY,
		name TEXT,
		tags TEXT
	)`)
	db.Exec(`INSERT INTO repos VALUES(1, 'anthony',   '["go","sqlite","database"]')`)
	db.Exec(`INSERT INTO repos VALUES(2, 'webserver', '["go","http","rest"]')`)
	db.Exec(`INSERT INTO repos VALUES(3, 'ml-kit',    '["python","ml","data"]')`)

	// Expand tags — json_each reads from repos.tags for EACH repo row
	fmt.Println("  All repo tags (expanded):")
	rows, _ := db.Query(`
		SELECT r.name, je.value
		FROM repos r, json_each(r.tags) je
		ORDER BY r.id, je.key
	`)
	defer rows.Close()
	for rows.Next() {
		var name, tag string
		rows.Scan(&name, &tag)
		fmt.Printf("    %-10s %s\n", name, tag)
	}
	fmt.Println()

	// Filter: find repos tagged with "go"
	fmt.Println("  Repos tagged 'go':")
	rows2, _ := db.Query(`
		SELECT r.name, je.value
		FROM repos r, json_each(r.tags) je
		WHERE je.value = 'go'
		ORDER BY r.id
	`)
	defer rows2.Close()
	for rows2.Next() {
		var name, tag string
		rows2.Scan(&name, &tag)
		fmt.Printf("    %s\n", name)
	}
	fmt.Println()

	// Aggregate: count tags per repo
	fmt.Println("  Tag count per repo:")
	rows3, _ := db.Query(`
		SELECT r.id, COUNT(je.value) AS tag_count
		FROM repos r, json_each(r.tags) je
		GROUP BY r.id
		ORDER BY r.id
	`)
	defer rows3.Close()
	for rows3.Next() {
		var id, count int
		rows3.Scan(&id, &count)
		fmt.Printf("    repo %d: %d tags\n", id, count)
	}
	fmt.Println()

	// Count distinct tags across all repos
	var distinctCount int
	db.QueryRow(`
		SELECT COUNT(DISTINCT je.value)
		FROM repos r, json_each(r.tags) je
	`).Scan(&distinctCount)
	fmt.Printf("  Total distinct tags: %d\n", distinctCount)
	fmt.Println()
}

// jsonAggregates demonstrates json_group_array and json_group_object.
func jsonAggregates(db *sql.DB) {
	fmt.Println("=== JSON Aggregate Functions ===")

	db.Exec(`CREATE TABLE scores (player TEXT, points INTEGER)`)
	db.Exec(`INSERT INTO scores VALUES('alice', 10)`)
	db.Exec(`INSERT INTO scores VALUES('bob', 20)`)
	db.Exec(`INSERT INTO scores VALUES('alice', 30)`)

	// json_group_array: collect values into a JSON array
	var arr string
	db.QueryRow(`SELECT json_group_array(points) FROM scores ORDER BY points`).Scan(&arr)
	fmt.Printf("  json_group_array(points)  = %s\n", arr)

	// json_group_object: collect key/value pairs into a JSON object
	var obj string
	db.QueryRow(`SELECT json_group_object(player, points) FROM scores`).Scan(&obj)
	fmt.Printf("  json_group_object(player, points) = %s\n", obj)
}
