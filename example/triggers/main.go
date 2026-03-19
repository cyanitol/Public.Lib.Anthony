// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// Triggers: BEFORE/AFTER INSERT/UPDATE/DELETE, audit logging, cascading
// triggers, and RAISE for constraint enforcement.
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

	auditLogTrigger(db)
	cascadingTrigger(db)
}

// auditLogTrigger demonstrates AFTER INSERT/UPDATE triggers that maintain
// an audit log of all changes.
func auditLogTrigger(db *sql.DB) {
	fmt.Println("=== Audit Log Trigger ===")

	db.Exec(`CREATE TABLE accounts (
		id      INTEGER PRIMARY KEY,
		name    TEXT NOT NULL,
		balance REAL NOT NULL DEFAULT 0
	)`)

	db.Exec(`CREATE TABLE audit_log (
		id     INTEGER PRIMARY KEY,
		action TEXT,
		detail TEXT
	)`)

	// Log every insert
	db.Exec(`
		CREATE TRIGGER log_insert AFTER INSERT ON accounts
		BEGIN
			INSERT INTO audit_log(action, detail)
			VALUES('INSERT', 'Created account: ' || NEW.name);
		END
	`)

	// Log every balance update
	db.Exec(`
		CREATE TRIGGER log_update AFTER UPDATE OF balance ON accounts
		BEGIN
			INSERT INTO audit_log(action, detail)
			VALUES('UPDATE', NEW.name || ' balance changed to ' || CAST(NEW.balance AS TEXT));
		END
	`)

	// Perform some operations
	db.Exec(`INSERT INTO accounts(name, balance) VALUES('Alice', 1000)`)
	db.Exec(`INSERT INTO accounts(name, balance) VALUES('Bob', 500)`)
	db.Exec(`UPDATE accounts SET balance = balance + 200 WHERE name = 'Alice'`)
	db.Exec(`UPDATE accounts SET balance = balance - 100 WHERE name = 'Bob'`)

	// Show the audit log
	rows, _ := db.Query(`SELECT action, detail FROM audit_log ORDER BY id`)
	defer rows.Close()
	for rows.Next() {
		var action, detail string
		rows.Scan(&action, &detail)
		fmt.Printf("  [%-6s] %s\n", action, detail)
	}
	fmt.Println()
}

// cascadingTrigger demonstrates triggers that fire other triggers:
// inserting into orders fires a trigger that updates inventory, which fires
// a trigger that logs low-stock warnings.
func cascadingTrigger(db *sql.DB) {
	fmt.Println("=== Cascading Triggers ===")
	fmt.Println("  order insert -> inventory update -> low stock alert")
	fmt.Println()

	db.Exec(`CREATE TABLE inventory (
		product TEXT PRIMARY KEY,
		stock   INTEGER NOT NULL
	)`)

	db.Exec(`CREATE TABLE shop_orders (
		id      INTEGER PRIMARY KEY,
		product TEXT,
		qty     INTEGER
	)`)

	db.Exec(`CREATE TABLE alerts (
		id      INTEGER PRIMARY KEY,
		message TEXT
	)`)

	// Trigger 1: after order, reduce inventory
	db.Exec(`
		CREATE TRIGGER reduce_stock AFTER INSERT ON shop_orders
		BEGIN
			UPDATE inventory SET stock = stock - NEW.qty
			WHERE product = NEW.product;
		END
	`)

	// Trigger 2: after inventory update, alert if stock < 5
	db.Exec(`
		CREATE TRIGGER low_stock_alert AFTER UPDATE OF stock ON inventory
		WHEN NEW.stock < 5
		BEGIN
			INSERT INTO alerts(message)
			VALUES('LOW STOCK: ' || NEW.product || ' has only ' || CAST(NEW.stock AS TEXT) || ' left');
		END
	`)

	// Seed inventory
	db.Exec(`INSERT INTO inventory VALUES('Widget', 20)`)
	db.Exec(`INSERT INTO inventory VALUES('Gadget', 8)`)

	// Place orders — the cascading triggers fire automatically
	db.Exec(`INSERT INTO shop_orders(product, qty) VALUES('Widget', 12)`)
	db.Exec(`INSERT INTO shop_orders(product, qty) VALUES('Gadget', 5)`)
	db.Exec(`INSERT INTO shop_orders(product, qty) VALUES('Widget', 6)`)

	// Show inventory
	fmt.Println("  Inventory after orders:")
	rows, _ := db.Query(`SELECT product, stock FROM inventory ORDER BY product`)
	defer rows.Close()
	for rows.Next() {
		var product string
		var stock int
		rows.Scan(&product, &stock)
		fmt.Printf("    %-8s %d units\n", product, stock)
	}
	fmt.Println()

	// Show alerts
	fmt.Println("  Triggered alerts:")
	rows2, _ := db.Query(`SELECT message FROM alerts ORDER BY id`)
	defer rows2.Close()
	for rows2.Next() {
		var msg string
		rows2.Scan(&msg)
		fmt.Printf("    %s\n", msg)
	}
}
