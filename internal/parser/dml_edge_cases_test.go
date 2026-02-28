package parser

import (
	"testing"
)

// Test edge cases in DML parsing

func TestParseInsertEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert from select",
			sql:     "INSERT INTO t SELECT * FROM other",
			wantErr: false,
		},
		{
			name:    "insert from select with columns",
			sql:     "INSERT INTO t (a, b) SELECT x, y FROM other",
			wantErr: false,
		},
		{
			name:    "insert default values",
			sql:     "INSERT INTO t DEFAULT VALUES",
			wantErr: false,
		},
		{
			name:    "insert with columns error - missing comma",
			sql:     "INSERT INTO t (a b) VALUES (1, 2)",
			wantErr: true,
		},
		{
			name:    "insert values error - missing values",
			sql:     "INSERT INTO t (a, b) VALUES",
			wantErr: true,
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

func TestParseUpsertEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "upsert with conflict target",
			sql:     "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
			wantErr: false,
		},
		{
			name:    "upsert with where clause in conflict target",
			sql:     "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) WHERE id > 0 DO UPDATE SET name = excluded.name",
			wantErr: false,
		},
		{
			name:    "upsert with where clause in do update",
			sql:     "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT DO UPDATE SET name = excluded.name WHERE id > 0",
			wantErr: false,
		},
		{
			name:    "upsert do nothing",
			sql:     "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT DO NOTHING",
			wantErr: false,
		},
		{
			name:    "upsert conflict target error - missing paren",
			sql:     "INSERT INTO t (id) VALUES (1) ON CONFLICT id DO NOTHING",
			wantErr: true,
		},
		{
			name:    "upsert do update error - missing set",
			sql:     "INSERT INTO t (id) VALUES (1) ON CONFLICT DO UPDATE name = 1",
			wantErr: true,
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

func TestParseUpdateEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "update with order by",
			sql:     "UPDATE t SET x = 1 ORDER BY y",
			wantErr: false,
		},
		{
			name:    "update with limit",
			sql:     "UPDATE t SET x = 1 LIMIT 10",
			wantErr: false,
		},
		{
			name:    "update with order by and limit",
			sql:     "UPDATE t SET x = 1 ORDER BY y LIMIT 10",
			wantErr: false,
		},
		{
			name:    "update order by error - missing column",
			sql:     "UPDATE t SET x = 1 ORDER BY",
			wantErr: true,
		},
		{
			name:    "update multiple columns",
			sql:     "UPDATE t SET x = 1, y = 2, z = 3",
			wantErr: false,
		},
		{
			name:    "update assignments error - missing comma",
			sql:     "UPDATE t SET x = 1 y = 2",
			wantErr: true,
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

func TestParseDeleteEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "delete with order by",
			sql:     "DELETE FROM t ORDER BY x",
			wantErr: false,
		},
		{
			name:    "delete with limit",
			sql:     "DELETE FROM t LIMIT 10",
			wantErr: false,
		},
		{
			name:    "delete with order by and limit",
			sql:     "DELETE FROM t WHERE x > 5 ORDER BY y LIMIT 10",
			wantErr: false,
		},
		{
			name:    "delete order by error - missing column",
			sql:     "DELETE FROM t ORDER BY",
			wantErr: true,
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

func TestParseSelectEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "select with group by having",
			sql:     "SELECT x, COUNT(*) FROM t GROUP BY x HAVING COUNT(*) > 1",
			wantErr: false,
		},
		{
			name:    "select group by error - missing column",
			sql:     "SELECT * FROM t GROUP BY",
			wantErr: true,
		},
		{
			name:    "select order by error - missing column",
			sql:     "SELECT * FROM t ORDER BY",
			wantErr: true,
		},
		{
			name:    "select limit with offset",
			sql:     "SELECT * FROM t LIMIT 10 OFFSET 5",
			wantErr: false,
		},
		{
			name:    "select limit error - missing value",
			sql:     "SELECT * FROM t LIMIT",
			wantErr: true,
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

func TestParseJoinEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "join using clause",
			sql:     "SELECT * FROM t1 JOIN t2 USING (id)",
			wantErr: false,
		},
		{
			name:    "join using multiple columns",
			sql:     "SELECT * FROM t1 JOIN t2 USING (id, name)",
			wantErr: false,
		},
		{
			name:    "join on clause",
			sql:     "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
			wantErr: false,
		},
		{
			name:    "join on error - missing condition",
			sql:     "SELECT * FROM t1 JOIN t2 ON",
			wantErr: true,
		},
		{
			name:    "join using error - missing paren",
			sql:     "SELECT * FROM t1 JOIN t2 USING id",
			wantErr: true,
		},
		{
			name:    "join using error - missing column",
			sql:     "SELECT * FROM t1 JOIN t2 USING ()",
			wantErr: true,
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

func TestParseSubqueryEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "subquery in from clause",
			sql:     "SELECT * FROM (SELECT x FROM t)",
			wantErr: false,
		},
		{
			name:    "subquery with alias",
			sql:     "SELECT * FROM (SELECT x FROM t) AS sub",
			wantErr: false,
		},
		{
			name:    "subquery error - missing select",
			sql:     "SELECT * FROM (x FROM t)",
			wantErr: true,
		},
		{
			name:    "subquery error - missing closing paren",
			sql:     "SELECT * FROM (SELECT x FROM t",
			wantErr: true,
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

func TestParseCTEEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "cte with column list",
			sql:     "WITH cte(a, b) AS (SELECT x, y FROM t) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "recursive cte",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "cte error - missing select",
			sql:     "WITH cte AS () SELECT * FROM cte",
			wantErr: true,
		},
		{
			name:    "cte column list error - missing comma",
			sql:     "WITH cte(a b) AS (SELECT x, y FROM t) SELECT * FROM cte",
			wantErr: true,
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

func TestParseCompoundSelectEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "union",
			sql:     "SELECT 1 UNION SELECT 2",
			wantErr: false,
		},
		{
			name:    "union all",
			sql:     "SELECT 1 UNION ALL SELECT 2",
			wantErr: false,
		},
		{
			name:    "except",
			sql:     "SELECT 1 EXCEPT SELECT 2",
			wantErr: false,
		},
		{
			name:    "intersect",
			sql:     "SELECT 1 INTERSECT SELECT 2",
			wantErr: false,
		},
		{
			name:    "multiple unions",
			sql:     "SELECT 1 UNION SELECT 2 UNION SELECT 3",
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
