// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// Ultra-specific tests for remaining uncovered paths

func TestParseIdentExprWithDot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "qualified column reference",
			sql:  "SELECT t.col FROM t",
		},
		{
			name: "qualified column with table alias",
			sql:  "SELECT t1.x, t2.y FROM t1, t2",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseTableStarColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "table.* in SELECT",
			sql:  "SELECT t.* FROM t",
		},
		{
			name: "multiple table.* columns",
			sql:  "SELECT t1.*, t2.* FROM t1, t2",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseSubqueryInFrom(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "subquery without alias",
			sql:     "SELECT * FROM (SELECT 1)",
			wantErr: false,
		},
		{
			name:    "nested subquery",
			sql:     "SELECT * FROM (SELECT * FROM (SELECT 1))",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseOrderByWithDirection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "order by asc",
			sql:  "SELECT * FROM t ORDER BY x ASC",
		},
		{
			name: "order by desc",
			sql:  "SELECT * FROM t ORDER BY x DESC",
		},
		{
			name: "order by multiple with mixed directions",
			sql:  "SELECT * FROM t ORDER BY x ASC, y DESC, z",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseIndexedColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "index column with desc",
			sql:  "CREATE INDEX idx ON t(id DESC)",
		},
		{
			name: "index column with asc",
			sql:  "CREATE INDEX idx ON t(id ASC)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseColumnConstraintPrimaryKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "primary key with asc",
			sql:  "CREATE TABLE t (id INTEGER PRIMARY KEY ASC)",
		},
		{
			name: "primary key with desc",
			sql:  "CREATE TABLE t (id INTEGER PRIMARY KEY DESC)",
		},
		{
			name: "primary key autoincrement",
			sql:  "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseExpressionInParen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "complex expression in paren",
			sql:  "SELECT (1 + 2) * 3 FROM t",
		},
		{
			name: "nested parens",
			sql:  "SELECT ((1 + 2) * (3 + 4)) FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseMultiplicativeOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "division",
			sql:  "SELECT x / 2 FROM t",
		},
		{
			name: "modulo",
			sql:  "SELECT x % 10 FROM t",
		},
		{
			name: "multiplication",
			sql:  "SELECT x * y FROM t",
		},
		{
			name: "mixed operators",
			sql:  "SELECT x * y / z % 10 FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseAdditiveOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "string concatenation",
			sql:  "SELECT 'hello' || ' ' || 'world' FROM t",
		},
		{
			name: "subtraction",
			sql:  "SELECT x - y FROM t",
		},
		{
			name: "addition",
			sql:  "SELECT x + y FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseComparisonOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "not equal !=",
			sql:  "SELECT * FROM t WHERE x != 5",
		},
		{
			name: "not equal <>",
			sql:  "SELECT * FROM t WHERE x <> 5",
		},
		{
			name: "less than or equal",
			sql:  "SELECT * FROM t WHERE x <= 5",
		},
		{
			name: "greater than or equal",
			sql:  "SELECT * FROM t WHERE x >= 5",
		},
		{
			name: "less than",
			sql:  "SELECT * FROM t WHERE x < 5",
		},
		{
			name: "greater than",
			sql:  "SELECT * FROM t WHERE x > 5",
		},
		{
			name: "equal",
			sql:  "SELECT * FROM t WHERE x = 5",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseIsExpressions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "is null",
			sql:  "SELECT * FROM t WHERE x IS NULL",
		},
		{
			name: "is not null",
			sql:  "SELECT * FROM t WHERE x IS NOT NULL",
		},
		{
			name: "is true",
			sql:  "SELECT * FROM t WHERE x IS TRUE",
		},
		{
			name: "is false",
			sql:  "SELECT * FROM t WHERE x IS FALSE",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}


func TestParseExplainVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "explain",
			sql:  "EXPLAIN SELECT * FROM t",
		},
		{
			name: "explain query plan",
			sql:  "EXPLAIN QUERY PLAN SELECT * FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseTransactionStatements(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "begin transaction",
			sql:  "BEGIN TRANSACTION",
		},
		{
			name: "begin deferred",
			sql:  "BEGIN DEFERRED",
		},
		{
			name: "begin immediate",
			sql:  "BEGIN IMMEDIATE",
		},
		{
			name: "begin exclusive",
			sql:  "BEGIN EXCLUSIVE",
		},
		{
			name: "commit",
			sql:  "COMMIT",
		},
		{
			name: "rollback",
			sql:  "ROLLBACK",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseAttachDetach(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "attach database",
			sql:  "ATTACH DATABASE 'file.db' AS mydb",
		},
		{
			name: "attach with expression",
			sql:  "ATTACH 'file.db' AS mydb",
		},
		{
			name: "detach database",
			sql:  "DETACH DATABASE mydb",
		},
		{
			name: "detach",
			sql:  "DETACH mydb",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}
