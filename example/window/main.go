// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD,
// SUM/COUNT OVER, PARTITION BY, and named WINDOW clauses.
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
	rankingFunctions(db)
	aggregateWindows(db)
	navigationFunctions(db)
	namedWindows(db)
}

func seedData(db *sql.DB) {
	db.Exec(`CREATE TABLE sales (
		id     INTEGER PRIMARY KEY,
		rep    TEXT,
		region TEXT,
		amount INTEGER
	)`)

	data := []struct {
		rep, region string
		amount      int
	}{
		{"Alice", "East", 500},
		{"Alice", "East", 300},
		{"Alice", "East", 700},
		{"Bob", "East", 400},
		{"Bob", "East", 600},
		{"Carol", "West", 800},
		{"Carol", "West", 200},
		{"Dave", "West", 550},
	}

	for _, d := range data {
		db.Exec(`INSERT INTO sales(rep, region, amount) VALUES(?, ?, ?)`,
			d.rep, d.region, d.amount)
	}
}

func rankingFunctions(db *sql.DB) {
	fmt.Println("=== Ranking Functions ===")

	rows, _ := db.Query(`
		SELECT rep, amount,
			ROW_NUMBER() OVER (ORDER BY amount DESC) AS row_num,
			RANK()       OVER (ORDER BY amount DESC) AS rnk
		FROM sales
		ORDER BY amount DESC
	`)
	defer rows.Close()

	fmt.Printf("  %-8s %6s %4s %4s\n", "Rep", "Amount", "Row#", "Rank")
	for rows.Next() {
		var rep string
		var amount, rowNum, rnk int
		rows.Scan(&rep, &amount, &rowNum, &rnk)
		fmt.Printf("  %-8s %6d %4d %4d\n", rep, amount, rowNum, rnk)
	}
	fmt.Println()
}

func aggregateWindows(db *sql.DB) {
	fmt.Println("=== SUM and COUNT Over Partitions ===")

	rows, _ := db.Query(`
		SELECT rep, amount,
			SUM(amount)   OVER (PARTITION BY rep) AS rep_total,
			COUNT(amount) OVER (PARTITION BY rep) AS rep_count
		FROM sales
		ORDER BY rep, amount
	`)
	defer rows.Close()

	fmt.Printf("  %-8s %6s %9s %5s\n", "Rep", "Amount", "RepTotal", "Count")
	for rows.Next() {
		var rep string
		var amount, total, count int
		rows.Scan(&rep, &amount, &total, &count)
		fmt.Printf("  %-8s %6d %9d %5d\n", rep, amount, total, count)
	}
	fmt.Println()
}

func navigationFunctions(db *sql.DB) {
	fmt.Println("=== LAG / LEAD — Compare With Adjacent Rows ===")

	rows, _ := db.Query(`
		SELECT rep, amount,
			LAG(amount, 1)  OVER (ORDER BY id) AS prev_amount,
			LEAD(amount, 1) OVER (ORDER BY id) AS next_amount
		FROM sales
		ORDER BY id
	`)
	defer rows.Close()

	fmt.Printf("  %-8s %6s %6s %6s\n", "Rep", "Amount", "Prev", "Next")
	for rows.Next() {
		var rep string
		var amount int
		var prev, next sql.NullInt64
		rows.Scan(&rep, &amount, &prev, &next)
		prevStr, nextStr := "NULL", "NULL"
		if prev.Valid {
			prevStr = fmt.Sprintf("%d", prev.Int64)
		}
		if next.Valid {
			nextStr = fmt.Sprintf("%d", next.Int64)
		}
		fmt.Printf("  %-8s %6d %6s %6s\n", rep, amount, prevStr, nextStr)
	}
	fmt.Println()
}

func namedWindows(db *sql.DB) {
	fmt.Println("=== Named WINDOW Clause ===")
	fmt.Println("  (Define once, reuse across columns)")

	rows, _ := db.Query(`
		SELECT rep, amount,
			ROW_NUMBER() OVER w AS row_num,
			SUM(amount)  OVER w AS running
		FROM sales
		WINDOW w AS (ORDER BY id)
		ORDER BY id
	`)
	defer rows.Close()

	fmt.Printf("  %-8s %6s %4s %7s\n", "Rep", "Amount", "Row#", "Running")
	for rows.Next() {
		var rep string
		var amount, rowNum, running int
		rows.Scan(&rep, &amount, &rowNum, &running)
		fmt.Printf("  %-8s %6d %4d %7d\n", rep, amount, rowNum, running)
	}
}
