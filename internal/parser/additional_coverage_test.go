package parser

import (
	"testing"
)

// Additional tests to improve coverage on specific functions

func TestParseTableIndexedBy(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "table with indexed by",
			sql:     "SELECT * FROM t INDEXED BY idx_name",
			wantErr: false,
		},
		{
			name:    "table indexed by error - missing by",
			sql:     "SELECT * FROM t INDEXED idx_name",
			wantErr: true,
		},
		{
			name:    "table indexed by error - missing index name",
			sql:     "SELECT * FROM t INDEXED BY",
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

func TestParseConflictTargetEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "conflict target with multiple columns",
			sql:     "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a, b) DO NOTHING",
			wantErr: false,
		},
		{
			name:    "conflict target error - missing comma",
			sql:     "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a b) DO NOTHING",
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

func TestParseDoUpdateEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "do update multiple columns",
			sql:     "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT DO UPDATE SET a = excluded.a, b = excluded.b",
			wantErr: false,
		},
		{
			name:    "do update with where clause",
			sql:     "INSERT INTO t (a) VALUES (1) ON CONFLICT DO UPDATE SET a = excluded.a WHERE a < 10",
			wantErr: false,
		},
		{
			name:    "do update error - missing equal",
			sql:     "INSERT INTO t (a) VALUES (1) ON CONFLICT DO UPDATE SET a excluded.a",
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

func TestParseSelectWithEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "select with error - missing from",
			sql:     "SELECT * FROM",
			wantErr: true,
		},
		{
			name:    "select group by error - missing having expr",
			sql:     "SELECT COUNT(*) FROM t GROUP BY x HAVING",
			wantErr: true,
		},
		{
			name:    "select limit with comma offset (old syntax)",
			sql:     "SELECT * FROM t LIMIT 10, 5",
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

func TestParseInsertValuesEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "insert multiple value rows",
			sql:     "INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)",
			wantErr: false,
		},
		{
			name:    "insert values error - missing closing paren",
			sql:     "INSERT INTO t (a) VALUES (1, (2, 3)",
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

func TestParseTriggerBodyStatementEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "trigger body with delete",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 BEGIN DELETE FROM log WHERE id = OLD.id; END",
			wantErr: false,
		},
		{
			name:    "trigger body with update",
			sql:     "CREATE TRIGGER t AFTER INSERT ON table1 BEGIN UPDATE stats SET count = count + 1; END",
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

func TestParseExpressionOperatorChains(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "expression with or operators",
			sql:     "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3",
			wantErr: false,
		},
		{
			name:    "expression with and operators",
			sql:     "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3",
			wantErr: false,
		},
		{
			name:    "in expression with empty list error",
			sql:     "SELECT * FROM t WHERE x IN ()",
			wantErr: true,
		},
		{
			name:    "parenthesized expression error",
			sql:     "SELECT (1 + 2 FROM t",
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

func TestParseAliasEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "column alias with as",
			sql:     "SELECT x AS column_name FROM t",
			wantErr: false,
		},
		{
			name:    "column alias without as",
			sql:     "SELECT x column_name FROM t",
			wantErr: false,
		},
		{
			name:    "table alias with as",
			sql:     "SELECT * FROM table1 AS t1",
			wantErr: false,
		},
		{
			name:    "table alias without as",
			sql:     "SELECT * FROM table1 t1",
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

func TestParseCaseExpressionEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "case with no when clauses error",
			sql:     "SELECT CASE END FROM t",
			wantErr: true,
		},
		{
			name:    "case with base expression and multiple when",
			sql:     "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t",
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

func TestParseCreateViewColumnListEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create view error - empty column list",
			sql:     "CREATE VIEW v () AS SELECT * FROM t",
			wantErr: true,
		},
		{
			name:    "create view with single column",
			sql:     "CREATE VIEW v (col1) AS SELECT x FROM t",
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

func TestParseDropIndexEdgeCases2(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "drop index error - missing index name",
			sql:     "DROP INDEX",
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

func TestParseUpdateAssignmentsEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "update assignment error - missing value",
			sql:     "UPDATE t SET x =",
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

func TestParseCreateTableAsSelectEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create table as select error - missing select",
			sql:     "CREATE TABLE t AS",
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

func TestParseColumnOrConstraintEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "table constraint without name",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "multiple column definitions",
			sql:     "CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)",
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
