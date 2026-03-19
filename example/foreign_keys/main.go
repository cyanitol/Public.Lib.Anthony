// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Foreign Keys: referential integrity enforcement with ON DELETE CASCADE,
// ON DELETE SET NULL, and constraint violation detection.
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

	db.Exec(`PRAGMA foreign_keys = ON`)

	cascadeDelete(db)
	setNullDelete(db)
	constraintViolation(db)
}

func cascadeDelete(db *sql.DB) {
	fmt.Println("=== ON DELETE CASCADE ===")

	db.Exec(`CREATE TABLE departments (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)

	db.Exec(`CREATE TABLE employees (
		id      INTEGER PRIMARY KEY,
		name    TEXT NOT NULL,
		dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE
	)`)

	db.Exec(`INSERT INTO departments VALUES(1, 'Engineering')`)
	db.Exec(`INSERT INTO departments VALUES(2, 'Sales')`)
	db.Exec(`INSERT INTO employees VALUES(1, 'Alice', 1)`)
	db.Exec(`INSERT INTO employees VALUES(2, 'Bob', 1)`)
	db.Exec(`INSERT INTO employees VALUES(3, 'Carol', 2)`)

	fmt.Println("  Before deleting Engineering:")
	printEmployees(db)

	db.Exec(`DELETE FROM departments WHERE id = 1`)

	fmt.Println("  After deleting Engineering (CASCADE):")
	printEmployees(db)

	db.Exec(`DROP TABLE employees`)
	db.Exec(`DROP TABLE departments`)
	fmt.Println()
}

func setNullDelete(db *sql.DB) {
	fmt.Println("=== ON DELETE SET NULL ===")

	db.Exec(`CREATE TABLE teams (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)

	db.Exec(`CREATE TABLE members (
		id      INTEGER PRIMARY KEY,
		name    TEXT NOT NULL,
		team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL
	)`)

	db.Exec(`INSERT INTO teams VALUES(1, 'Alpha')`)
	db.Exec(`INSERT INTO teams VALUES(2, 'Beta')`)
	db.Exec(`INSERT INTO members VALUES(1, 'Dave', 1)`)
	db.Exec(`INSERT INTO members VALUES(2, 'Eve', 1)`)
	db.Exec(`INSERT INTO members VALUES(3, 'Frank', 2)`)

	fmt.Println("  Before deleting team Alpha:")
	printMembers(db)

	db.Exec(`DELETE FROM teams WHERE id = 1`)

	fmt.Println("  After deleting team Alpha (SET NULL):")
	printMembers(db)

	db.Exec(`DROP TABLE members`)
	db.Exec(`DROP TABLE teams`)
	fmt.Println()
}

func constraintViolation(db *sql.DB) {
	fmt.Println("=== Constraint Violation ===")

	db.Exec(`CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)`)
	db.Exec(`CREATE TABLE children (
		id        INTEGER PRIMARY KEY,
		name      TEXT,
		parent_id INTEGER REFERENCES parents(id)
	)`)

	db.Exec(`INSERT INTO parents VALUES(1, 'Parent A')`)

	// Try inserting a child with a non-existent parent
	_, err := db.Exec(`INSERT INTO children VALUES(1, 'Orphan', 999)`)
	if err != nil {
		fmt.Printf("  Expected error: %v\n", err)
	}

	// Valid insert works fine
	_, err = db.Exec(`INSERT INTO children VALUES(1, 'Child A', 1)`)
	if err != nil {
		fmt.Printf("  Unexpected error: %v\n", err)
	} else {
		fmt.Println("  Valid FK insert succeeded")
	}
}

func printEmployees(db *sql.DB) {
	rows, _ := db.Query(`SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.id`)
	defer rows.Close()
	for rows.Next() {
		var emp, dept string
		rows.Scan(&emp, &dept)
		fmt.Printf("    %-8s -> %s\n", emp, dept)
	}
}

func printMembers(db *sql.DB) {
	rows, _ := db.Query(`SELECT name, team_id FROM members ORDER BY id`)
	defer rows.Close()
	for rows.Next() {
		var name string
		var teamID sql.NullInt64
		rows.Scan(&name, &teamID)
		if teamID.Valid {
			fmt.Printf("    %-8s team %d\n", name, teamID.Int64)
		} else {
			fmt.Printf("    %-8s team NULL\n", name)
		}
	}
}
