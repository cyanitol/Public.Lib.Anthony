// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Compound queries: UNION, UNION ALL, INTERSECT, and EXCEPT for combining
// result sets from multiple SELECT statements.
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

	seedData(db)
	unionExample(db)
	unionAllExample(db)
	intersectExample(db)
	exceptExample(db)
	compoundWithOrderLimit(db)
}

func seedData(db *sql.DB) {
	db.Exec(`CREATE TABLE east_customers (name TEXT, city TEXT)`)
	db.Exec(`INSERT INTO east_customers VALUES('Alice', 'New York')`)
	db.Exec(`INSERT INTO east_customers VALUES('Bob', 'Boston')`)
	db.Exec(`INSERT INTO east_customers VALUES('Carol', 'Miami')`)
	db.Exec(`INSERT INTO east_customers VALUES('Dave', 'New York')`)

	db.Exec(`CREATE TABLE west_customers (name TEXT, city TEXT)`)
	db.Exec(`INSERT INTO west_customers VALUES('Eve', 'Seattle')`)
	db.Exec(`INSERT INTO west_customers VALUES('Frank', 'Portland')`)
	db.Exec(`INSERT INTO west_customers VALUES('Carol', 'Miami')`)
	db.Exec(`INSERT INTO west_customers VALUES('Grace', 'Denver')`)
}

func unionExample(db *sql.DB) {
	fmt.Println("=== UNION — All Unique Customers ===")

	rows, err := db.Query(`
		SELECT name, city FROM east_customers
		UNION
		SELECT name, city FROM west_customers
		ORDER BY name
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, city string
		rows.Scan(&name, &city)
		fmt.Printf("  %-8s %s\n", name, city)
	}
	fmt.Println()
}

func unionAllExample(db *sql.DB) {
	fmt.Println("=== UNION ALL — All Rows Including Duplicates ===")

	rows, err := db.Query(`
		SELECT name, city FROM east_customers
		UNION ALL
		SELECT name, city FROM west_customers
		ORDER BY name
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, city string
		rows.Scan(&name, &city)
		fmt.Printf("  %-8s %s\n", name, city)
	}
	fmt.Println()
}

func intersectExample(db *sql.DB) {
	fmt.Println("=== INTERSECT — Customers in Both Regions ===")

	rows, err := db.Query(`
		SELECT name, city FROM east_customers
		INTERSECT
		SELECT name, city FROM west_customers
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, city string
		rows.Scan(&name, &city)
		fmt.Printf("  %-8s %s\n", name, city)
	}
	fmt.Println()
}

func exceptExample(db *sql.DB) {
	fmt.Println("=== EXCEPT — East-Only Customers ===")

	rows, err := db.Query(`
		SELECT name, city FROM east_customers
		EXCEPT
		SELECT name, city FROM west_customers
		ORDER BY name
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, city string
		rows.Scan(&name, &city)
		fmt.Printf("  %-8s %s\n", name, city)
	}
	fmt.Println()
}

func compoundWithOrderLimit(db *sql.DB) {
	fmt.Println("=== Compound with ORDER BY + LIMIT ===")

	rows, err := db.Query(`
		SELECT name, city FROM east_customers
		UNION
		SELECT name, city FROM west_customers
		ORDER BY city
		LIMIT 4
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("  First 4 customers by city:")
	for rows.Next() {
		var name, city string
		rows.Scan(&name, &city)
		fmt.Printf("    %-8s %s\n", name, city)
	}
}
