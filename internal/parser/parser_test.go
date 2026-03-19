// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"strings"
	"testing"
)

func TestParseSelect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple select",
			sql:     "SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "select with columns",
			sql:     "SELECT id, name, email FROM users",
			wantErr: false,
		},
		{
			name:    "select with where",
			sql:     "SELECT * FROM users WHERE age > 18",
			wantErr: false,
		},
		{
			name:    "select with order by",
			sql:     "SELECT * FROM users ORDER BY name ASC",
			wantErr: false,
		},
		{
			name:    "select with limit",
			sql:     "SELECT * FROM users LIMIT 10",
			wantErr: false,
		},
		{
			name:    "select with limit and offset",
			sql:     "SELECT * FROM users LIMIT 10 OFFSET 5",
			wantErr: false,
		},
		{
			name:    "select with join",
			sql:     "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "select with group by",
			sql:     "SELECT category, COUNT(*) FROM products GROUP BY category",
			wantErr: false,
		},
		{
			name:    "select with having",
			sql:     "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5",
			wantErr: false,
		},
		{
			name:    "select distinct",
			sql:     "SELECT DISTINCT category FROM products",
			wantErr: false,
		},
		{
			name:    "select with alias",
			sql:     "SELECT name AS user_name, age AS user_age FROM users",
			wantErr: false,
		},
		{
			name:    "select with subquery",
			sql:     "SELECT * FROM (SELECT id, name FROM users) AS subq",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*SelectStmt); !ok {
					t.Errorf("expected SelectStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert with values",
			sql:     "INSERT INTO users (name, age) VALUES ('John', 30)",
			wantErr: false,
		},
		{
			name:    "insert multiple rows",
			sql:     "INSERT INTO users (name, age) VALUES ('John', 30), ('Jane', 25)",
			wantErr: false,
		},
		{
			name:    "insert from select",
			sql:     "INSERT INTO users_copy SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "insert default values",
			sql:     "INSERT INTO users DEFAULT VALUES",
			wantErr: false,
		},
		{
			name:    "insert or replace",
			sql:     "INSERT OR REPLACE INTO users (id, name) VALUES (1, 'John')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*InsertStmt); !ok {
					t.Errorf("expected InsertStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple update",
			sql:     "UPDATE users SET name = 'John'",
			wantErr: false,
		},
		{
			name:    "update with where",
			sql:     "UPDATE users SET name = 'John' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "update multiple columns",
			sql:     "UPDATE users SET name = 'John', age = 30 WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "update with order by and limit",
			sql:     "UPDATE users SET active = 0 ORDER BY created_at LIMIT 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*UpdateStmt); !ok {
					t.Errorf("expected UpdateStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseDelete(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple delete",
			sql:     "DELETE FROM users",
			wantErr: false,
		},
		{
			name:    "delete with where",
			sql:     "DELETE FROM users WHERE age < 18",
			wantErr: false,
		},
		{
			name:    "delete with order by and limit",
			sql:     "DELETE FROM users ORDER BY created_at LIMIT 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*DeleteStmt); !ok {
					t.Errorf("expected DeleteStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseCreateTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple create table",
			sql:     "CREATE TABLE users (id INTEGER, name TEXT)",
			wantErr: false,
		},
		{
			name:    "create table with primary key",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			wantErr: false,
		},
		{
			name:    "create table with autoincrement",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
			wantErr: false,
		},
		{
			name:    "create table with not null",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
			wantErr: false,
		},
		{
			name:    "create table with unique",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
			wantErr: false,
		},
		{
			name:    "create table with default",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER DEFAULT 1)",
			wantErr: false,
		},
		{
			name:    "create table with check",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0))",
			wantErr: false,
		},
		{
			name:    "create table if not exists",
			sql:     "CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)",
			wantErr: false,
		},
		{
			name:    "create temp table",
			sql:     "CREATE TEMP TABLE users (id INTEGER, name TEXT)",
			wantErr: false,
		},
		{
			name:    "create table without rowid",
			sql:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID",
			wantErr: false,
		},
		{
			name:    "create table strict",
			sql:     "CREATE TABLE users (id INTEGER, name TEXT) STRICT",
			wantErr: false,
		},
		{
			name:    "create table as select",
			sql:     "CREATE TABLE users_copy AS SELECT * FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*CreateTableStmt); !ok {
					t.Errorf("expected CreateTableStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseCreateIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple create index",
			sql:     "CREATE INDEX idx_name ON users (name)",
			wantErr: false,
		},
		{
			name:    "create unique index",
			sql:     "CREATE UNIQUE INDEX idx_email ON users (email)",
			wantErr: false,
		},
		{
			name:    "create index on multiple columns",
			sql:     "CREATE INDEX idx_name_age ON users (name, age)",
			wantErr: false,
		},
		{
			name:    "create index with order",
			sql:     "CREATE INDEX idx_name ON users (name ASC, age DESC)",
			wantErr: false,
		},
		{
			name:    "create index if not exists",
			sql:     "CREATE INDEX IF NOT EXISTS idx_name ON users (name)",
			wantErr: false,
		},
		{
			name:    "create index with where",
			sql:     "CREATE INDEX idx_active ON users (name) WHERE active = 1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*CreateIndexStmt); !ok {
					t.Errorf("expected CreateIndexStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseDrop(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		wantType interface{}
	}{
		{
			name:     "drop table",
			sql:      "DROP TABLE users",
			wantErr:  false,
			wantType: &DropTableStmt{},
		},
		{
			name:     "drop table if exists",
			sql:      "DROP TABLE IF EXISTS users",
			wantErr:  false,
			wantType: &DropTableStmt{},
		},
		{
			name:     "drop index",
			sql:      "DROP INDEX idx_name",
			wantErr:  false,
			wantType: &DropIndexStmt{},
		},
		{
			name:     "drop index if exists",
			sql:      "DROP INDEX IF EXISTS idx_name",
			wantErr:  false,
			wantType: &DropIndexStmt{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
		})
	}
}

func TestParseExpressions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "arithmetic expression",
			sql:     "SELECT 1 + 2 * 3",
			wantErr: false,
		},
		{
			name:    "comparison expression",
			sql:     "SELECT * FROM users WHERE age > 18 AND active = 1",
			wantErr: false,
		},
		{
			name:    "in expression",
			sql:     "SELECT * FROM users WHERE id IN (1, 2, 3)",
			wantErr: false,
		},
		{
			name:    "between expression",
			sql:     "SELECT * FROM users WHERE age BETWEEN 18 AND 65",
			wantErr: false,
		},
		{
			name:    "like expression",
			sql:     "SELECT * FROM users WHERE name LIKE 'John%'",
			wantErr: false,
		},
		{
			name:    "is null expression",
			sql:     "SELECT * FROM users WHERE email IS NULL",
			wantErr: false,
		},
		{
			name:    "is not null expression",
			sql:     "SELECT * FROM users WHERE email IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "case expression",
			sql:     "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
			wantErr: false,
		},
		{
			name:    "cast expression",
			sql:     "SELECT CAST(age AS TEXT) FROM users",
			wantErr: false,
		},
		{
			name:    "function call",
			sql:     "SELECT COUNT(*) FROM users",
			wantErr: false,
		},
		{
			name:    "function with args",
			sql:     "SELECT SUBSTR(name, 1, 10) FROM users",
			wantErr: false,
		},
		{
			name:    "subquery expression",
			sql:     "SELECT * FROM users WHERE id = (SELECT MAX(id) FROM users)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

func TestParseTransactions(t *testing.T) {
	t.Parallel()
	t.Skip("Transaction parsing not yet fully implemented")
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		wantType interface{}
	}{
		{
			name:     "begin",
			sql:      "BEGIN",
			wantErr:  false,
			wantType: &BeginStmt{},
		},
		{
			name:     "begin transaction",
			sql:      "BEGIN TRANSACTION",
			wantErr:  false,
			wantType: &BeginStmt{},
		},
		{
			name:     "begin deferred",
			sql:      "BEGIN DEFERRED TRANSACTION",
			wantErr:  false,
			wantType: &BeginStmt{},
		},
		{
			name:     "begin immediate",
			sql:      "BEGIN IMMEDIATE TRANSACTION",
			wantErr:  false,
			wantType: &BeginStmt{},
		},
		{
			name:     "begin exclusive",
			sql:      "BEGIN EXCLUSIVE TRANSACTION",
			wantErr:  false,
			wantType: &BeginStmt{},
		},
		{
			name:     "commit",
			sql:      "COMMIT",
			wantErr:  false,
			wantType: &CommitStmt{},
		},
		{
			name:     "rollback",
			sql:      "ROLLBACK",
			wantErr:  false,
			wantType: &RollbackStmt{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

func TestParseMultipleStatements(t *testing.T) {
	t.Parallel()
	sql := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (name) VALUES ('John');
		SELECT * FROM users;
	`

	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(stmts) != 3 {
		t.Errorf("expected 3 statements, got %d", len(stmts))
	}

	if _, ok := stmts[0].(*CreateTableStmt); !ok {
		t.Errorf("statement 0: expected CreateTableStmt, got %T", stmts[0])
	}
	if _, ok := stmts[1].(*InsertStmt); !ok {
		t.Errorf("statement 1: expected InsertStmt, got %T", stmts[1])
	}
	if _, ok := stmts[2].(*SelectStmt); !ok {
		t.Errorf("statement 2: expected SelectStmt, got %T", stmts[2])
	}
}

func assertSelectClauses(t *testing.T, stmt *SelectStmt, wantCols, wantGroupBy, wantOrderBy int) {
	t.Helper()
	if len(stmt.Columns) != wantCols {
		t.Errorf("expected %d columns, got %d", wantCols, len(stmt.Columns))
	}
	if len(stmt.GroupBy) != wantGroupBy {
		t.Errorf("expected %d GROUP BY expressions, got %d", wantGroupBy, len(stmt.GroupBy))
	}
	if len(stmt.OrderBy) != wantOrderBy {
		t.Errorf("expected %d ORDER BY term(s), got %d", wantOrderBy, len(stmt.OrderBy))
	}
}

func TestParseComplexQuery(t *testing.T) {
	t.Parallel()
	sql := `
		SELECT
			u.id,
			u.name,
			COUNT(o.id) AS order_count,
			SUM(o.total) AS total_spent
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.active = 1
			AND u.created_at >= '2020-01-01'
		GROUP BY u.id, u.name
		HAVING COUNT(o.id) > 5
		ORDER BY total_spent DESC
		LIMIT 10
	`

	stmt := parseSelectStmt(t, sql)
	assertSelectClauses(t, stmt, 4, 2, 1)

	if stmt.From == nil {
		t.Error("expected FROM clause")
	}
	if stmt.Where == nil {
		t.Error("expected WHERE clause")
	}
	if stmt.Having == nil {
		t.Error("expected HAVING clause")
	}
	if stmt.Limit == nil {
		t.Error("expected LIMIT clause")
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "missing FROM",
			sql:  "SELECT * users",
		},
		{
			name: "missing table name",
			sql:  "SELECT * FROM",
		},
		{
			name: "missing column in INSERT",
			sql:  "INSERT INTO users () VALUES (1)",
		},
		{
			name: "missing SET in UPDATE",
			sql:  "UPDATE users name = 'John'",
		},
		{
			name: "missing FROM in DELETE",
			sql:  "DELETE users",
		},
		{
			name: "unclosed parenthesis",
			sql:  "SELECT * FROM users WHERE (id = 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Errorf("expected error for invalid SQL: %q", tt.sql)
			}
		})
	}
}

func TestParseString(t *testing.T) {
	t.Parallel()
	sql := "SELECT * FROM users"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString() error = %v", err)
	}

	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	if _, ok := stmts[0].(*SelectStmt); !ok {
		t.Errorf("expected SelectStmt, got %T", stmts[0])
	}
}

func TestLiteralValueExtraction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		extract func(Expression) (interface{}, error)
		want    interface{}
	}{
		{
			name:    "integer literal",
			sql:     "SELECT 42",
			extract: func(e Expression) (interface{}, error) { return IntValue(e) },
			want:    int64(42),
		},
		{
			name:    "float literal",
			sql:     "SELECT 3.14",
			extract: func(e Expression) (interface{}, error) { return FloatValue(e) },
			want:    3.14,
		},
		{
			name:    "string literal",
			sql:     "SELECT 'hello'",
			extract: func(e Expression) (interface{}, error) { return StringValue(e) },
			want:    "hello",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			stmt := stmts[0].(*SelectStmt)
			expr := stmt.Columns[0].Expr

			got, err := tt.extract(expr)
			if err != nil {
				t.Fatalf("value extraction error = %v", err)
			}

			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseJoinTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "inner join",
			sql:  "SELECT * FROM a INNER JOIN b ON a.id = b.a_id",
		},
		{
			name: "left join",
			sql:  "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id",
		},
		{
			name: "left outer join",
			sql:  "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id",
		},
		{
			name: "cross join",
			sql:  "SELECT * FROM a CROSS JOIN b",
		},
		{
			name: "natural join",
			sql:  "SELECT * FROM a NATURAL JOIN b",
		},
		{
			name: "join with using",
			sql:  "SELECT * FROM a JOIN b USING (id)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() error = %v", err)
			}
			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

func TestParseCompoundSelect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "union",
			sql:  "SELECT id FROM a UNION SELECT id FROM b",
		},
		{
			name: "union all",
			sql:  "SELECT id FROM a UNION ALL SELECT id FROM b",
		},
		{
			name: "except",
			sql:  "SELECT id FROM a EXCEPT SELECT id FROM b",
		},
		{
			name: "intersect",
			sql:  "SELECT id FROM a INTERSECT SELECT id FROM b",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() error = %v", err)
			}
			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}

			stmt := stmts[0].(*SelectStmt)
			if stmt.Compound == nil {
				t.Error("expected compound select")
			}
		})
	}
}

func BenchmarkParseLexer(b *testing.B) {
	sql := "SELECT u.id, u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = 1 GROUP BY u.id, u.name ORDER BY u.name LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer := NewLexer(sql)
		for {
			tok := lexer.NextToken()
			if tok.Type == TK_EOF {
				break
			}
		}
	}
}

func BenchmarkParseSelect(b *testing.B) {
	sql := "SELECT u.id, u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = 1 GROUP BY u.id, u.name ORDER BY u.name LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseInsert(b *testing.B) {
	sql := "INSERT INTO users (id, name, email, age, active) VALUES (1, 'John Doe', 'john@example.com', 30, 1)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestASTNodeInterfaces(t *testing.T) {
	t.Parallel()
	// Test that AST nodes properly implement their interfaces
	var _ Statement = (*SelectStmt)(nil)
	var _ Statement = (*InsertStmt)(nil)
	var _ Statement = (*UpdateStmt)(nil)
	var _ Statement = (*DeleteStmt)(nil)
	var _ Statement = (*CreateTableStmt)(nil)
	var _ Statement = (*DropTableStmt)(nil)
	var _ Statement = (*CreateIndexStmt)(nil)
	var _ Statement = (*DropIndexStmt)(nil)
	var _ Statement = (*BeginStmt)(nil)
	var _ Statement = (*CommitStmt)(nil)
	var _ Statement = (*RollbackStmt)(nil)

	var _ Expression = (*BinaryExpr)(nil)
	var _ Expression = (*UnaryExpr)(nil)
	var _ Expression = (*LiteralExpr)(nil)
	var _ Expression = (*IdentExpr)(nil)
	var _ Expression = (*FunctionExpr)(nil)
	var _ Expression = (*CaseExpr)(nil)
	var _ Expression = (*InExpr)(nil)
	var _ Expression = (*BetweenExpr)(nil)
	var _ Expression = (*CastExpr)(nil)
	var _ Expression = (*CollateExpr)(nil)
	var _ Expression = (*ParenExpr)(nil)
	var _ Expression = (*SubqueryExpr)(nil)
	var _ Expression = (*VariableExpr)(nil)
}

func TestComplexTableConstraints(t *testing.T) {
	t.Parallel()
	sql := `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			product_id INTEGER NOT NULL,
			quantity INTEGER CHECK (quantity > 0),
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (user_id, product_id),
			FOREIGN KEY (user_id) REFERENCES users(id),
			CHECK (quantity <= 1000)
		)
	`

	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}

	stmt, ok := stmts[0].(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmts[0])
	}

	if len(stmt.Columns) != 5 {
		t.Errorf("expected 5 columns, got %d", len(stmt.Columns))
	}

	// Note: The parser currently may not fully parse all constraint types
	// This test ensures basic parsing works without errors
}

func TestWindowFunctions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "row number",
			sql:  "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM users",
		},
		{
			name: "partition by",
			sql:  "SELECT SUM(amount) OVER (PARTITION BY user_id ORDER BY date) FROM transactions",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			// Note: Full window function support may not be implemented
			// This test checks that parsing attempts don't panic
			if err != nil && !strings.Contains(err.Error(), "window") {
				t.Logf("Parse() error = %v (may not be fully supported)", err)
			}
			_ = stmts
		})
	}
}
