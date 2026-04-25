// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func openBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return db
}

func seedRows(b *testing.B, db *sql.DB, n int) {
	b.Helper()
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO bench_items (label) VALUES (?)")
	if err != nil {
		b.Fatalf("prepare seed: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err = stmt.Exec(fmt.Sprintf("item-%d", i)); err != nil {
			b.Fatalf("seed exec: %v", err)
		}
	}
	stmt.Close()
	if err = tx.Commit(); err != nil {
		b.Fatalf("seed commit: %v", err)
	}
}

// BenchmarkInsert measures INSERT one row at a time with no transaction batching.
func BenchmarkInsert(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO bench_items (label) VALUES (?)", "x"); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

// BenchmarkBatchInsert measures INSERT of N rows inside a single transaction.
func BenchmarkBatchInsert(b *testing.B) {
	const batchSize = 100
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE batch_tbl (label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		for j := 0; j < batchSize; j++ {
			if _, err = tx.Exec("INSERT INTO batch_tbl (label) VALUES (?)", "y"); err != nil {
				tx.Rollback()
				b.Fatalf("batch insert: %v", err)
			}
		}
		if err = tx.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

// BenchmarkSelectAll measures SELECT of all rows from a 1000-row table.
func BenchmarkSelectAll(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	seedRows(b, db, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, label FROM bench_items")
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		for rows.Next() {
			var id int
			var label string
			_ = rows.Scan(&id, &label)
		}
		rows.Close()
	}
}

// BenchmarkSelectPK measures point lookup by primary key.
func BenchmarkSelectPK(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	seedRows(b, db, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int
		var label string
		row := db.QueryRow("SELECT id, label FROM bench_items WHERE id = ?", i%1000+1)
		_ = row.Scan(&id, &label)
	}
}

// BenchmarkUpdate measures UPDATE of a single row by primary key.
func BenchmarkUpdate(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	seedRows(b, db, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("UPDATE bench_items SET label = ? WHERE id = ?", "updated", i%1000+1); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

// BenchmarkDelete measures DELETE of a single row by primary key.
func BenchmarkDelete(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if _, err := db.Exec("INSERT INTO bench_items (id, label) VALUES (1, 'row')"); err != nil {
			b.Fatalf("re-insert: %v", err)
		}
		b.StartTimer()

		if _, err := db.Exec("DELETE FROM bench_items WHERE id = 1"); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

// BenchmarkPreparedInsert measures INSERT using a prepared statement.
func BenchmarkPreparedInsert(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE bench_items (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO bench_items (label) VALUES (?)")
	if err != nil {
		b.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec("z"); err != nil {
			b.Fatalf("exec: %v", err)
		}
	}
}

// insertUsersAndOrders populates bench_users and bench_orders within the
// provided transaction.
func insertUsersAndOrders(b *testing.B, tx *sql.Tx, n int) {
	b.Helper()
	uStmt, err := tx.Prepare("INSERT INTO bench_users (name) VALUES (?)")
	if err != nil {
		b.Fatalf("prepare users: %v", err)
	}
	oStmt, err := tx.Prepare("INSERT INTO bench_orders (user_id, amount) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("prepare orders: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err = uStmt.Exec(fmt.Sprintf("user-%d", i)); err != nil {
			b.Fatalf("insert user: %v", err)
		}
		if _, err = oStmt.Exec(i+1, float64(i)*1.5); err != nil {
			b.Fatalf("insert order: %v", err)
		}
	}
	uStmt.Close()
	oStmt.Close()
}

// seedComplexQueryTables creates and populates bench_users and bench_orders for
// the complex-query benchmark.
func seedComplexQueryTables(b *testing.B, db *sql.DB) {
	b.Helper()

	_, err := db.Exec("CREATE TABLE bench_users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		b.Fatalf("create bench_users: %v", err)
	}
	_, err = db.Exec("CREATE TABLE bench_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
	if err != nil {
		b.Fatalf("create bench_orders: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin seed: %v", err)
	}
	insertUsersAndOrders(b, tx, 100)
	if err = tx.Commit(); err != nil {
		b.Fatalf("seed commit: %v", err)
	}
}

// BenchmarkComplexQuery measures a JOIN across two tables with WHERE and ORDER BY.
func BenchmarkComplexQuery(b *testing.B) {
	db := openBenchDB(b)
	defer db.Close()

	seedComplexQueryTables(b, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(
			"SELECT u.name, o.amount FROM bench_users u JOIN bench_orders o ON u.id = o.user_id WHERE o.amount > ? ORDER BY o.amount DESC",
			10.0,
		)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		for rows.Next() {
			var name string
			var amount float64
			_ = rows.Scan(&name, &amount)
		}
		rows.Close()
	}
}
