// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import "testing"

// BenchmarkParseSelectComplex benchmarks parsing a SELECT with WHERE clause
func BenchmarkParseSelectComplex(b *testing.B) {
	sql := "SELECT id, name, email FROM users WHERE status = 'active' AND created_at > '2024-01-01'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseInsertMultiColumn benchmarks parsing an INSERT statement
func BenchmarkParseInsertMultiColumn(b *testing.B) {
	sql := "INSERT INTO users (name, email, status) VALUES ('John', 'john@example.com', 'active')"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseComplexJoin benchmarks parsing a complex SELECT with JOIN, GROUP BY, HAVING
func BenchmarkParseComplexJoin(b *testing.B) {
	sql := "SELECT u.id, u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' GROUP BY u.id HAVING COUNT(o.id) > 5 ORDER BY u.name LIMIT 10"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseUpdate benchmarks parsing an UPDATE statement
func BenchmarkParseUpdate(b *testing.B) {
	sql := "UPDATE users SET status = 'inactive', updated_at = '2024-01-01' WHERE id = 123"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseDelete benchmarks parsing a DELETE statement
func BenchmarkParseDelete(b *testing.B) {
	sql := "DELETE FROM users WHERE status = 'inactive' AND created_at < '2023-01-01'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseCreateTable benchmarks parsing a CREATE TABLE statement
func BenchmarkParseCreateTable(b *testing.B) {
	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, created_at TEXT)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseCreateIndex benchmarks parsing a CREATE INDEX statement
func BenchmarkParseCreateIndex(b *testing.B) {
	sql := "CREATE INDEX idx_users_email ON users(email)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseSubquery benchmarks parsing SELECT with subquery
func BenchmarkParseSubquery(b *testing.B) {
	sql := "SELECT * FROM (SELECT id, name FROM users WHERE status = 'active') WHERE name LIKE 'John%'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseCTE benchmarks parsing SELECT with Common Table Expression
func BenchmarkParseCTE(b *testing.B) {
	sql := "WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM active_users WHERE created_at > '2024-01-01'"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}

// BenchmarkParseWindowFunction benchmarks parsing SELECT with window function
func BenchmarkParseWindowFunction(b *testing.B) {
	sql := "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY status ORDER BY created_at) as rn FROM users"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		_, _ = p.Parse()
	}
}
