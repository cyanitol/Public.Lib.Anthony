// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Date/time functions: date(), time(), datetime(), strftime(), julianday(),
// date arithmetic with modifiers, and practical calendar queries.
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

	basicFunctions(db)
	dateArithmetic(db)
	strftimeFormats(db)
	practicalQueries(db)
}

func basicFunctions(db *sql.DB) {
	fmt.Println("=== Basic Date/Time Functions ===")

	var d string
	db.QueryRow(`SELECT date('2026-03-16')`).Scan(&d)
	fmt.Printf("  date('2026-03-16')     = %s\n", d)

	var t string
	db.QueryRow(`SELECT time('14:30:00')`).Scan(&t)
	fmt.Printf("  time('14:30:00')       = %s\n", t)

	var dt string
	db.QueryRow(`SELECT datetime('2026-03-16 14:30:00')`).Scan(&dt)
	fmt.Printf("  datetime(...)          = %s\n", dt)

	var jd float64
	db.QueryRow(`SELECT julianday('2026-03-16')`).Scan(&jd)
	fmt.Printf("  julianday('2026-03-16') = %.4f\n", jd)

	fmt.Println()
}

func dateArithmetic(db *sql.DB) {
	fmt.Println("=== Date Arithmetic with Modifiers ===")

	queries := []struct {
		label, sql string
	}{
		{"+ 7 days", `SELECT date('2026-03-16', '+7 days')`},
		{"- 1 month", `SELECT date('2026-03-16', '-1 month')`},
		{"start of year", `SELECT date('2026-03-16', 'start of year')`},
		{"start of month", `SELECT date('2026-03-16', 'start of month')`},
		{"+ 1 year, start of year", `SELECT date('2026-03-16', '+1 year', 'start of year')`},
	}

	for _, q := range queries {
		var result string
		db.QueryRow(q.sql).Scan(&result)
		fmt.Printf("  %-28s = %s\n", q.label, result)
	}
	fmt.Println()
}

func strftimeFormats(db *sql.DB) {
	fmt.Println("=== strftime() Format Specifiers ===")

	base := "2026-03-16 14:30:45"
	specs := []struct {
		label, fmt string
	}{
		{"Year-Month-Day", "%Y-%m-%d"},
		{"Hour:Min:Sec", "%H:%M:%S"},
		{"Day of week (0=Sun)", "%w"},
		{"Day of year", "%j"},
		{"Week number", "%W"},
		{"Unix epoch", "%s"},
	}

	for _, s := range specs {
		var result string
		db.QueryRow(fmt.Sprintf(`SELECT strftime('%s', '%s')`, s.fmt, base)).Scan(&result)
		fmt.Printf("  %-22s (%s) = %s\n", s.label, s.fmt, result)
	}
	fmt.Println()
}

func practicalQueries(db *sql.DB) {
	fmt.Println("=== Practical: Days Between Dates ===")

	var days float64
	db.QueryRow(`SELECT julianday('2026-12-25') - julianday('2026-03-16')`).Scan(&days)
	fmt.Printf("  Days from 2026-03-16 to Christmas: %.0f\n", days)

	fmt.Println()
	fmt.Println("=== Practical: Generate Date Parts ===")

	db.Exec(`CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, ts TEXT)`)
	db.Exec(`INSERT INTO events VALUES(1, 'Launch', '2026-03-16 09:00:00')`)
	db.Exec(`INSERT INTO events VALUES(2, 'Review', '2026-04-01 14:30:00')`)
	db.Exec(`INSERT INTO events VALUES(3, 'Ship',   '2026-06-15 17:00:00')`)

	rows, err := db.Query(`
		SELECT name,
			strftime('%Y', ts) AS year,
			strftime('%m', ts) AS month,
			strftime('%d', ts) AS day
		FROM events
		ORDER BY ts
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Printf("  %-8s %4s %2s %2s\n", "Event", "Year", "Mo", "Dy")
	for rows.Next() {
		var name, year, month, day string
		rows.Scan(&name, &year, &month, &day)
		fmt.Printf("  %-8s %4s %2s %2s\n", name, year, month, day)
	}
}
