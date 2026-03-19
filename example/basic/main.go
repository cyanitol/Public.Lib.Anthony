// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Basic CRUD operations, transactions, constraints, and prepared statements.
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

	createSchema(db)
	insertRows(db)
	queryWithFilters(db)
	updateAndDelete(db)
	transactionExample(db)
	preparedStatementExample(db)
	aggregateExample(db)
}

func createSchema(db *sql.DB) {
	fmt.Println("=== Schema Creation ===")

	_, err := db.Exec(`
		CREATE TABLE employees (
			id    INTEGER PRIMARY KEY,
			name  TEXT NOT NULL,
			dept  TEXT NOT NULL DEFAULT 'Engineering',
			salary REAL CHECK(salary > 0),
			UNIQUE(name, dept)
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE INDEX idx_dept ON employees(dept)`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Created employees table with CHECK, UNIQUE, and index")
	fmt.Println()
}

func insertRows(db *sql.DB) {
	fmt.Println("=== INSERT ===")

	inserts := []struct {
		name   string
		dept   string
		salary float64
	}{
		{"Alice", "Engineering", 120000},
		{"Bob", "Engineering", 115000},
		{"Carol", "Marketing", 105000},
		{"Dave", "Marketing", 98000},
		{"Eve", "Sales", 92000},
		{"Frank", "Sales", 88000},
	}

	for _, e := range inserts {
		_, err := db.Exec(
			`INSERT INTO employees(name, dept, salary) VALUES(?, ?, ?)`,
			e.name, e.dept, e.salary,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Demonstrate constraint violation
	_, err := db.Exec(`INSERT INTO employees(name, dept, salary) VALUES('Alice', 'Engineering', 130000)`)
	if err != nil {
		fmt.Printf("UNIQUE constraint caught: %v\n", err)
	}

	_, err = db.Exec(`INSERT INTO employees(name, dept, salary) VALUES('Ghost', 'Ops', -1)`)
	if err != nil {
		fmt.Printf("CHECK constraint caught: %v\n", err)
	}

	fmt.Println()
}

func queryWithFilters(db *sql.DB) {
	fmt.Println("=== SELECT with WHERE, ORDER BY, LIMIT ===")

	rows, err := db.Query(`
		SELECT name, dept, salary
		FROM employees
		WHERE salary >= 100000
		ORDER BY salary DESC
		LIMIT 3
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, dept string
		var salary float64
		if err := rows.Scan(&name, &dept, &salary); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %-8s %-12s $%.0f\n", name, dept, salary)
	}
	fmt.Println()
}

func updateAndDelete(db *sql.DB) {
	fmt.Println("=== UPDATE and DELETE ===")

	res, err := db.Exec(`UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Sales'`)
	if err != nil {
		log.Fatal(err)
	}
	n, _ := res.RowsAffected()
	fmt.Printf("Gave Sales a 10%% raise: %d rows updated\n", n)

	res, err = db.Exec(`DELETE FROM employees WHERE name = 'Frank'`)
	if err != nil {
		log.Fatal(err)
	}
	n, _ = res.RowsAffected()
	fmt.Printf("Removed Frank: %d rows deleted\n", n)
	fmt.Println()
}

func transactionExample(db *sql.DB) {
	fmt.Println("=== Transactions ===")

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	_, err = tx.Exec(`INSERT INTO employees(name, dept, salary) VALUES('Grace', 'Engineering', 125000)`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	_, err = tx.Exec(`UPDATE employees SET salary = salary + 5000 WHERE name = 'Eve'`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transaction committed: added Grace, bumped Eve's salary")
	fmt.Println()
}

func preparedStatementExample(db *sql.DB) {
	fmt.Println("=== Prepared Statements ===")

	stmt, err := db.Prepare(`SELECT name, salary FROM employees WHERE dept = ? ORDER BY salary DESC`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, dept := range []string{"Engineering", "Marketing", "Sales"} {
		fmt.Printf("  %s:\n", dept)
		rows, err := stmt.Query(dept)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var name string
			var salary float64
			if err := rows.Scan(&name, &salary); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("    %-8s $%.0f\n", name, salary)
		}
		rows.Close()
	}
	fmt.Println()
}

func aggregateExample(db *sql.DB) {
	fmt.Println("=== Aggregates with GROUP BY and HAVING ===")

	rows, err := db.Query(`
		SELECT dept,
			   COUNT(*) AS headcount,
			   AVG(salary) AS avg_salary,
			   MIN(salary) AS min_salary,
			   MAX(salary) AS max_salary
		FROM employees
		GROUP BY dept
		HAVING COUNT(*) > 0
		ORDER BY avg_salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Printf("  %-12s %5s %10s %10s %10s\n", "Dept", "Count", "Avg", "Min", "Max")
	for rows.Next() {
		var dept string
		var count int
		var avg, min, max float64
		if err := rows.Scan(&dept, &count, &avg, &min, &max); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %-12s %5d %10.0f %10.0f %10.0f\n", dept, count, avg, min, max)
	}
}
