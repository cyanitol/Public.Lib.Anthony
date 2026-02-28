package parser

import (
	"testing"
)

// Final set of tests to improve coverage to near 99%

func TestParseMultipleErrors(t *testing.T) {
	// Test that multiple parse errors are collected and reported
	sql := "SELECT FROM; INSERT INTO;"
	parser := NewParser(sql)
	_, err := parser.Parse()
	if err == nil {
		t.Error("Expected error for invalid SQL with multiple statements")
	}
}

func TestParseSelectWithAllClauses(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "select with all optional clauses",
			sql:  "SELECT DISTINCT x, y FROM t WHERE a > 5 GROUP BY x HAVING COUNT(*) > 1 ORDER BY x DESC LIMIT 10 OFFSET 5",
		},
		{
			name: "select with compound operator",
			sql:  "SELECT 1 UNION ALL SELECT 2 UNION SELECT 3",
		},
		{
			name: "select with CTE",
			sql:  "WITH cte AS (SELECT 1) SELECT * FROM cte",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseCompoundSelectChain(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "except followed by union",
			sql:     "SELECT 1 EXCEPT SELECT 2 UNION SELECT 3",
			wantErr: false,
		},
		{
			name:    "intersect followed by except",
			sql:     "SELECT 1 INTERSECT SELECT 2 EXCEPT SELECT 3",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseCTEWithMultiple(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "multiple CTEs",
			sql:     "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2",
			wantErr: false,
		},
		{
			name:    "CTE with column list",
			sql:     "WITH cte (a, b, c) AS (SELECT 1, 2, 3) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "CTE error - missing AS",
			sql:     "WITH cte (SELECT 1) SELECT * FROM cte",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseFromClauseComplexJoins(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "multiple joins",
			sql:     "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id",
			wantErr: false,
		},
		{
			name:    "left join",
			sql:     "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
			wantErr: false,
		},
		{
			name:    "cross join",
			sql:     "SELECT * FROM t1 CROSS JOIN t2",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseTableOrSubqueryVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "table with alias",
			sql:     "SELECT * FROM table1 t1",
			wantErr: false,
		},
		{
			name:    "subquery with alias",
			sql:     "SELECT * FROM (SELECT 1) sub",
			wantErr: false,
		},
		{
			name:    "multiple tables comma-separated",
			sql:     "SELECT * FROM t1, t2, t3",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseInsertVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert or replace",
			sql:     "INSERT OR REPLACE INTO t VALUES (1)",
			wantErr: false,
		},
		{
			name:    "insert or ignore",
			sql:     "INSERT OR IGNORE INTO t VALUES (1)",
			wantErr: false,
		},
		{
			name:    "insert or abort",
			sql:     "INSERT OR ABORT INTO t VALUES (1)",
			wantErr: false,
		},
		{
			name:    "insert or rollback",
			sql:     "INSERT OR ROLLBACK INTO t VALUES (1)",
			wantErr: false,
		},
		{
			name:    "insert or fail",
			sql:     "INSERT OR FAIL INTO t VALUES (1)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseUpdateVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "update or replace",
			sql:     "UPDATE OR REPLACE t SET x = 1",
			wantErr: false,
		},
		{
			name:    "update or ignore",
			sql:     "UPDATE OR IGNORE t SET x = 1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseDeleteVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "delete with where and order",
			sql:     "DELETE FROM t WHERE id > 100 ORDER BY id LIMIT 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseCreateTableVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create table if not exists",
			sql:     "CREATE TABLE IF NOT EXISTS t (id INTEGER)",
			wantErr: false,
		},
		{
			name:    "create table without rowid",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY) WITHOUT ROWID",
			wantErr: false,
		},
		{
			name:    "create table strict",
			sql:     "CREATE TABLE t (id INTEGER) STRICT",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseCreateIndexVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create unique index",
			sql:     "CREATE UNIQUE INDEX idx ON t(x)",
			wantErr: false,
		},
		{
			name:    "create index with where clause",
			sql:     "CREATE INDEX idx ON t(x) WHERE x > 0",
			wantErr: false,
		},
		{
			name:    "create index with multiple columns",
			sql:     "CREATE INDEX idx ON t(x, y, z)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseFunctionCallVariants(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "function with distinct",
			sql:     "SELECT COUNT(DISTINCT x) FROM t",
			wantErr: false,
		},
		{
			name:    "function with star",
			sql:     "SELECT COUNT(*) FROM t",
			wantErr: false,
		},
		{
			name:    "function with multiple args",
			sql:     "SELECT SUBSTR(name, 1, 5) FROM t",
			wantErr: false,
		},
		{
			name:    "function with filter",
			sql:     "SELECT SUM(x) FILTER (WHERE x > 0) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseExpressionPrecedence(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "arithmetic precedence",
			sql:  "SELECT 1 + 2 * 3 FROM t",
		},
		{
			name: "comparison with arithmetic",
			sql:  "SELECT * FROM t WHERE x + 5 > 10",
		},
		{
			name: "logical with comparison",
			sql:  "SELECT * FROM t WHERE x > 5 AND y < 10 OR z = 0",
		},
		{
			name: "bitwise operations",
			sql:  "SELECT x & 255 | y << 8 FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseUnaryOperators(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "unary not",
			sql:  "SELECT * FROM t WHERE NOT x",
		},
		{
			name: "unary minus",
			sql:  "SELECT -x FROM t",
		},
		{
			name: "unary plus",
			sql:  "SELECT +x FROM t",
		},
		{
			name: "bitwise not",
			sql:  "SELECT ~x FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}

func TestParseLiteralTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "null literal",
			sql:  "SELECT NULL FROM t",
		},
		{
			name: "true literal",
			sql:  "SELECT TRUE FROM t",
		},
		{
			name: "false literal",
			sql:  "SELECT FALSE FROM t",
		},
		{
			name: "blob literal",
			sql:  "SELECT X'48656C6C6F' FROM t",
		},
		{
			name: "current_time",
			sql:  "SELECT CURRENT_TIME FROM t",
		},
		{
			name: "current_date",
			sql:  "SELECT CURRENT_DATE FROM t",
		},
		{
			name: "current_timestamp",
			sql:  "SELECT CURRENT_TIMESTAMP FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse() unexpected error = %v", err)
			}
		})
	}
}
