// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Common Table Expressions: non-recursive CTEs for readability, subquery
// factoring, and multi-step data transformations.
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
	simpleCTE(db)
	cteWithFilter(db)
	cteWithAggregate(db)
	subqueryExample(db)
}

func seedData(db *sql.DB) {
	db.Exec(`CREATE TABLE orders (
		id       INTEGER PRIMARY KEY,
		customer TEXT,
		product  TEXT,
		qty      INTEGER,
		price    REAL
	)`)

	orders := []struct {
		customer, product string
		qty               int
		price             float64
	}{
		{"Alice", "Widget", 10, 2.50},
		{"Alice", "Gadget", 2, 15.00},
		{"Bob", "Widget", 5, 2.50},
		{"Bob", "Widget", 8, 2.50},
		{"Carol", "Gadget", 1, 15.00},
		{"Carol", "Gizmo", 3, 8.00},
		{"Carol", "Widget", 20, 2.50},
	}

	for _, o := range orders {
		db.Exec(`INSERT INTO orders(customer, product, qty, price) VALUES(?,?,?,?)`,
			o.customer, o.product, o.qty, o.price)
	}
}

func simpleCTE(db *sql.DB) {
	fmt.Println("=== Simple CTE — Top Customers by Revenue ===")

	rows, err := db.Query(`
		WITH revenue AS (
			SELECT customer, SUM(qty * price) AS total
			FROM orders
			GROUP BY customer
		)
		SELECT customer, total
		FROM revenue
		ORDER BY total DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var customer string
		var total float64
		rows.Scan(&customer, &total)
		fmt.Printf("  %-8s $%.2f\n", customer, total)
	}
	fmt.Println()
}

func cteWithFilter(db *sql.DB) {
	fmt.Println("=== CTE with WHERE — High-Value Orders ===")

	rows, err := db.Query(`
		WITH order_totals AS (
			SELECT id, customer, product, qty * price AS total
			FROM orders
		)
		SELECT customer, product, total
		FROM order_totals
		WHERE total > 20
		ORDER BY total DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var customer, product string
		var total float64
		rows.Scan(&customer, &product, &total)
		fmt.Printf("  %-8s %-8s $%.2f\n", customer, product, total)
	}
	fmt.Println()
}

func cteWithAggregate(db *sql.DB) {
	fmt.Println("=== CTE with Aggregate — Product Revenue Summary ===")

	rows, err := db.Query(`
		WITH product_rev AS (
			SELECT product, SUM(qty * price) AS revenue, SUM(qty) AS total_qty
			FROM orders
			GROUP BY product
		)
		SELECT product, total_qty, revenue
		FROM product_rev
		ORDER BY revenue DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Printf("  %-8s %5s %9s\n", "Product", "Qty", "Revenue")
	for rows.Next() {
		var product string
		var qty int
		var revenue float64
		rows.Scan(&product, &qty, &revenue)
		fmt.Printf("  %-8s %5d %9.2f\n", product, qty, revenue)
	}
	fmt.Println()
}

func subqueryExample(db *sql.DB) {
	fmt.Println("=== Derived Tables (FROM subquery) ===")

	rows, err := db.Query(`
		SELECT customer, order_count
		FROM (
			SELECT customer, COUNT(*) AS order_count
			FROM orders
			GROUP BY customer
		)
		ORDER BY order_count DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var customer string
		var count int
		rows.Scan(&customer, &count)
		fmt.Printf("  %-8s %d orders\n", customer, count)
	}
}
