// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// BenchmarkDriverOpen benchmarks opening a database connection
func BenchmarkDriverOpen(b *testing.B) {
	tmpfile := tempFile(b)
	defer os.Remove(tmpfile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := sql.Open("sqlite_internal", tmpfile)
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
}

// BenchmarkDriverPrepare benchmarks statement preparation
func BenchmarkDriverPrepare(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
		if err != nil {
			b.Fatal(err)
		}
		stmt.Close()
	}
}

// BenchmarkDriverSimpleQuery benchmarks a simple SELECT query
func BenchmarkDriverSimpleQuery(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert test data
	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO users (name, email) VALUES (?, ?)",
			"User"+string(rune(i)), "user"+string(rune(i))+"@example.com")
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, name, email FROM users WHERE id < 50")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var id int
			var name, email string
			_ = rows.Scan(&id, &name, &email)
		}
		rows.Close()
	}
}

// BenchmarkDriverInsert benchmarks INSERT operations
func BenchmarkDriverInsert(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO bench_insert (value) VALUES (?)", "test value")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDriverUpdate benchmarks UPDATE operations
func BenchmarkDriverUpdate(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_update (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	// Insert initial data
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO bench_update (value) VALUES (?)", "initial")
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("UPDATE bench_update SET value = ? WHERE id = ?", "updated", i%100+1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDriverDelete benchmarks DELETE operations
func BenchmarkDriverDelete(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_delete (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Re-insert data for each iteration
		_, err = db.Exec("INSERT INTO bench_delete (id, value) VALUES (?, ?)", 1, "test")
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		_, err = db.Exec("DELETE FROM bench_delete WHERE id = 1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDriverTransaction benchmarks transaction overhead
func BenchmarkDriverTransaction(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_tx (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		_, err = tx.Exec("INSERT INTO bench_tx (value) VALUES (?)", "test")
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDriverBatchInsert benchmarks batch INSERT in a transaction
func BenchmarkDriverBatchInsert(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_batch (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for j := 0; j < 100; j++ {
			_, err = tx.Exec("INSERT INTO bench_batch (value) VALUES (?)", "test value")
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}

		b.StopTimer()
		err = tx.Commit()
		if err != nil {
			b.Fatal(err)
		}
		// Clean up for next iteration
		_, _ = db.Exec("DELETE FROM bench_batch")
		b.StartTimer()
	}
}

// BenchmarkDriverJoin benchmarks JOIN operations
func BenchmarkDriverJoin(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	benchSetupJoinData(b, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchQueryJoinRows(b, db)
	}
}

// BenchmarkDriverAggregate benchmarks aggregate functions
func BenchmarkDriverAggregate(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench_agg (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)")
	if err != nil {
		b.Fatal(err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		category := "cat" + string(rune(i%5))
		_, err = db.Exec("INSERT INTO bench_agg (category, value) VALUES (?, ?)", category, i)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT category, COUNT(*), SUM(value), AVG(value) FROM bench_agg GROUP BY category")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var category string
			var count int
			var sum, avg float64
			_ = rows.Scan(&category, &count, &sum, &avg)
		}
		rows.Close()
	}
}

// setupBenchDB creates a temporary in-memory database for benchmarking
func setupBenchDB(b *testing.B) *sql.DB {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	return db
}

// tempFile creates a temporary file for testing
func tempFile(b *testing.B) string {
	f, err := os.CreateTemp("", "bench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)
	return name
}

func benchSetupJoinData(b *testing.B, db *sql.DB) {
	b.Helper()
	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Exec("INSERT INTO users (name) VALUES (?)", "User"+string(rune(i))); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO orders (user_id, amount) VALUES (?, ?)", i+1, float64(i)*10.5); err != nil {
			b.Fatal(err)
		}
	}
}

func benchQueryJoinRows(b *testing.B, db *sql.DB) {
	b.Helper()
	rows, err := db.Query("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		b.Fatal(err)
	}
	for rows.Next() {
		var name string
		var amount float64
		_ = rows.Scan(&name, &amount)
	}
	rows.Close()
}
