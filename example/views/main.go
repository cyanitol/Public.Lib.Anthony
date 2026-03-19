// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Views: CREATE VIEW, querying views with WHERE/ORDER BY, and views built
// on aggregates for reusable summary queries.
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
	simpleView(db)
	aggregateView(db)
	viewWithFilter(db)
}

func seedData(db *sql.DB) {
	db.Exec(`CREATE TABLE employees (
		id     INTEGER PRIMARY KEY,
		name   TEXT NOT NULL,
		dept   TEXT NOT NULL,
		salary INTEGER NOT NULL
	)`)

	staff := []struct {
		name, dept string
		salary     int
	}{
		{"Alice", "Engineering", 95000},
		{"Bob", "Engineering", 88000},
		{"Carol", "Engineering", 102000},
		{"Dave", "Sales", 72000},
		{"Eve", "Sales", 68000},
		{"Frank", "Marketing", 78000},
		{"Grace", "Marketing", 82000},
	}

	for _, s := range staff {
		db.Exec(`INSERT INTO employees(name, dept, salary) VALUES(?,?,?)`,
			s.name, s.dept, s.salary)
	}
}

func simpleView(db *sql.DB) {
	fmt.Println("=== Simple View — High Earners ===")

	db.Exec(`CREATE VIEW high_earners AS
		SELECT name, dept, salary
		FROM employees
		WHERE salary >= 80000
	`)

	rows, err := db.Query(`SELECT name, dept, salary FROM high_earners ORDER BY salary DESC`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, dept string
		var salary int
		rows.Scan(&name, &dept, &salary)
		fmt.Printf("  %-8s %-12s $%d\n", name, dept, salary)
	}
	fmt.Println()
}

func aggregateView(db *sql.DB) {
	fmt.Println("=== Aggregate View — Department Summary ===")

	db.Exec(`CREATE VIEW dept_summary AS
		SELECT dept, COUNT(*) AS headcount, SUM(salary) AS total_salary
		FROM employees
		GROUP BY dept
	`)

	rows, err := db.Query(`SELECT dept, headcount, total_salary FROM dept_summary ORDER BY total_salary DESC`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Printf("  %-12s %5s %12s\n", "Department", "Count", "Total Salary")
	for rows.Next() {
		var dept string
		var count, total int
		rows.Scan(&dept, &count, &total)
		fmt.Printf("  %-12s %5d %12d\n", dept, count, total)
	}
	fmt.Println()
}

func viewWithFilter(db *sql.DB) {
	fmt.Println("=== Query View with Additional WHERE ===")

	rows, err := db.Query(`
		SELECT name, salary
		FROM high_earners
		WHERE dept = 'Engineering'
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("  Engineering high earners:")
	for rows.Next() {
		var name string
		var salary int
		rows.Scan(&name, &salary)
		fmt.Printf("    %-8s $%d\n", name, salary)
	}
}
