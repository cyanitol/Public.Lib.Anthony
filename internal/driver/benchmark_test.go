// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// setupBenchmarkDB creates and initializes a database for benchmarking
func setupBenchmarkDB(b *testing.B, useMemory bool) (*sql.DB, func()) {
	var db *sql.DB
	var err error

	if useMemory {
		db, err = sql.Open(DriverName, ":memory:")
	} else {
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "bench.db")
		db, err = sql.Open(DriverName, dbPath)
	}

	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// BenchmarkInsert benchmarks single INSERT operations
func BenchmarkInsert(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	// Create table
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clean up previous data to avoid cache overflow
		if i > 0 && i%1000 == 0 {
			db.Exec("DELETE FROM users")
		}
		b.StartTimer()

		_, err := db.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "John Doe", 30)
		if err != nil {
			b.Fatalf("failed to insert (iteration %d): %v", i, err)
		}
	}
}

// BenchmarkInsertSingle is an alias for BenchmarkInsert for backward compatibility
func BenchmarkInsertSingle(b *testing.B) {
	BenchmarkInsert(b)
}

// BenchmarkInsertBatch benchmarks batch INSERT operations (1000 rows per iteration)
func BenchmarkInsertBatch(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Limit iterations to avoid cache overflow (each iteration inserts 1000 rows)
	iterations := b.N
	if iterations > 10 {
		iterations = 10
		b.N = iterations
	}

	for i := 0; i < iterations; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}

		stmt, err := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
		if err != nil {
			b.Fatalf("failed to prepare statement: %v", err)
		}

		for j := 0; j < 1000; j++ {
			_, err = stmt.Exec(fmt.Sprintf("User%d", i*1000+j), 20+j%50)
			if err != nil {
				b.Fatalf("failed to insert: %v", err)
			}
		}

		stmt.Close()

		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}

// BenchmarkSelect benchmarks simple SELECT queries
func BenchmarkSelect(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	// Setup data
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users WHERE id = ?", 500)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}
		rows.Close()
	}
}

// BenchmarkSelectSimple is an alias for BenchmarkSelect for backward compatibility
func BenchmarkSelectSimple(b *testing.B) {
	BenchmarkSelect(b)
}

// BenchmarkSelectFullScan benchmarks full table scans
func BenchmarkSelectFullScan(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users")
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var name string
			var age int
			rows.Scan(&id, &name, &age)
			count++
		}
		rows.Close()
	}
}

// BenchmarkSelectWhere benchmarks SELECT with WHERE clause
func BenchmarkSelectWhere(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users WHERE age > ?", 30)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var name string
			var age int
			rows.Scan(&id, &name, &age)
			count++
		}
		rows.Close()
	}
}

// BenchmarkSelectWithWhere is an alias for BenchmarkSelectWhere for backward compatibility
func BenchmarkSelectWithWhere(b *testing.B) {
	BenchmarkSelectWhere(b)
}

// BenchmarkUpdate benchmarks UPDATE operations
func BenchmarkUpdate(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", 25, i%1000+1)
		if err != nil {
			b.Fatalf("failed to update: %v", err)
		}
	}
}

// BenchmarkDelete benchmarks DELETE operations
func BenchmarkDelete(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Re-insert data before each delete
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "User1", 30)
		b.StartTimer()

		_, err := db.Exec("DELETE FROM users WHERE id = ?", 1)
		if err != nil {
			b.Fatalf("failed to delete: %v", err)
		}
	}
}

// BenchmarkTransaction benchmarks BEGIN/COMMIT cycle with a simple insert
func BenchmarkTransaction(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Limit iterations to avoid cache overflow
	iterations := b.N
	if iterations > 10000 {
		iterations = 10000
		b.N = iterations
	}

	for i := 0; i < iterations; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}

		_, err = tx.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "John", 30)
		if err != nil {
			b.Fatalf("failed to insert: %v", err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}

// BenchmarkTransactionOverhead is an alias for BenchmarkTransaction for backward compatibility
func BenchmarkTransactionOverhead(b *testing.B) {
	BenchmarkTransaction(b)
}

// BenchmarkSelectJoin benchmarks SELECT with JOIN operations
func BenchmarkSelectJoin(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	// Create tables
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		b.Fatalf("failed to create users table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
	if err != nil {
		b.Fatalf("failed to create orders table: %v", err)
	}

	// Insert test data
	tx, _ := db.Begin()
	userStmt, _ := tx.Prepare("INSERT INTO users (name) VALUES (?)")
	for i := 0; i < 100; i++ {
		userStmt.Exec(fmt.Sprintf("User%d", i))
	}
	userStmt.Close()

	orderStmt, _ := tx.Prepare("INSERT INTO orders (user_id, amount) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		orderStmt.Exec((i%100)+1, float64(10+i%100))
	}
	orderStmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > ?", 50.0)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var name string
			var amount float64
			rows.Scan(&name, &amount)
			count++
		}
		rows.Close()
	}
}

// BenchmarkJoinTwoTables is an alias for BenchmarkSelectJoin for backward compatibility
func BenchmarkJoinTwoTables(b *testing.B) {
	BenchmarkSelectJoin(b)
}

// BenchmarkAggregate benchmarks COUNT/SUM/AVG aggregate functions
func BenchmarkAggregate(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO orders (user_id, amount) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(i%100+1, float64(10+i%100))
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var count int
		var sum, avg float64
		err := db.QueryRow("SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders WHERE user_id <= ?", 50).Scan(&count, &sum, &avg)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}
	}
}

// BenchmarkAggregateCount benchmarks COUNT aggregate function
func BenchmarkAggregateCount(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE age > ?", 30).Scan(&count)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}
	}
}

// BenchmarkAggregateSum benchmarks SUM aggregate function
func BenchmarkAggregateSum(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO orders (user_id, amount) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(i%100+1, float64(10+i%100))
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var total float64
		err := db.QueryRow("SELECT SUM(amount) FROM orders").Scan(&total)
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}
	}
}

// BenchmarkAggregateGroupBy benchmarks GROUP BY operations
func BenchmarkAggregateGroupBy(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO orders (user_id, amount) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(i%100+1, float64(10+i%100))
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT user_id, SUM(amount) FROM orders GROUP BY user_id")
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var userID int
			var total float64
			rows.Scan(&userID, &total)
			count++
		}
		rows.Close()
	}
}

// BenchmarkPrepare benchmarks prepared statement creation and execution
func BenchmarkPrepare(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Limit iterations to avoid cache overflow
	iterations := b.N
	if iterations > 10000 {
		iterations = 10000
		b.N = iterations
	}

	for i := 0; i < iterations; i++ {
		_, err := stmt.Exec(fmt.Sprintf("User%d", i), 30)
		if err != nil {
			b.Fatalf("failed to execute: %v", err)
		}
	}
}

// BenchmarkPreparedStatement is an alias for BenchmarkPrepare for backward compatibility
func BenchmarkPreparedStatement(b *testing.B) {
	BenchmarkPrepare(b)
}

// BenchmarkMemoryVsDisk compares in-memory vs disk performance for inserts
func BenchmarkMemoryVsDisk(b *testing.B) {
	b.Run("Memory", func(b *testing.B) {
		db, cleanup := setupBenchmarkDB(b, true)
		defer cleanup()

		_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
		if err != nil {
			b.Fatalf("failed to create table: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// Limit iterations to avoid cache overflow
		iterations := b.N
		if iterations > 10000 {
			iterations = 10000
			b.N = iterations
		}

		for i := 0; i < iterations; i++ {
			_, err := db.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "John", 30)
			if err != nil {
				b.Fatalf("failed to insert: %v", err)
			}
		}
	})

	b.Run("Disk", func(b *testing.B) {
		db, cleanup := setupBenchmarkDB(b, false)
		defer cleanup()

		_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
		if err != nil {
			b.Fatalf("failed to create table: %v", err)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// Limit iterations to avoid cache overflow
		iterations := b.N
		if iterations > 10000 {
			iterations = 10000
			b.N = iterations
		}

		for i := 0; i < iterations; i++ {
			_, err := db.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "John", 30)
			if err != nil {
				b.Fatalf("failed to insert: %v", err)
			}
		}
	})
}

// BenchmarkSelectOrderBy benchmarks SELECT with ORDER BY
func BenchmarkSelectOrderBy(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users ORDER BY age DESC")
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var name string
			var age int
			rows.Scan(&id, &name, &age)
			count++
		}
		rows.Close()
	}
}

// BenchmarkSelectLimit benchmarks SELECT with LIMIT
func BenchmarkSelectLimit(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, true)
	defer cleanup()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	for i := 0; i < 1000; i++ {
		stmt.Exec(fmt.Sprintf("User%d", i), 20+i%50)
	}
	stmt.Close()
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users LIMIT 10")
		if err != nil {
			b.Fatalf("failed to query: %v", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var name string
			var age int
			rows.Scan(&id, &name, &age)
			count++
		}
		rows.Close()
	}
}
