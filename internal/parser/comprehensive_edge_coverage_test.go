package parser

import (
	"testing"
)

// TestInsertDefaultValues tests INSERT with DEFAULT VALUES
func TestInsertDefaultValues(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "INSERT DEFAULT VALUES",
			sql:       "INSERT INTO t DEFAULT VALUES;",
			wantError: false,
		},
		{
			name:      "INSERT DEFAULT without VALUES",
			sql:       "INSERT INTO t DEFAULT;",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestInsertSelectSource tests INSERT with SELECT
func TestInsertSelectSource(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "INSERT with SELECT",
			sql:       "INSERT INTO t SELECT * FROM other;",
			wantError: false,
		},
		{
			name:      "INSERT with columns and SELECT",
			sql:       "INSERT INTO t (id, name) SELECT id, name FROM other;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestOnConflictConstraintName tests ON CONFLICT with constraint name
func TestOnConflictConstraintName(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "ON CONFLICT ON CONSTRAINT",
			sql:       "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT pk_id DO NOTHING;",
			wantError: false,
		},
		{
			name:      "ON CONFLICT ON CONSTRAINT DO UPDATE",
			sql:       "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT pk_id DO UPDATE SET id = 2;",
			wantError: false,
		},
		{
			name:      "ON CONFLICT ON without CONSTRAINT",
			sql:       "INSERT INTO t VALUES (1) ON CONFLICT ON DO NOTHING;",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v for SQL: %s", tt.wantError, err, tt.sql)
			}
		})
	}
}

// TestSelectWithCompoundOperators tests compound SELECT operators
func TestSelectWithCompoundOperators(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "UNION",
			sql:       "SELECT 1 UNION SELECT 2;",
			wantError: false,
		},
		{
			name:      "UNION ALL",
			sql:       "SELECT 1 UNION ALL SELECT 2;",
			wantError: false,
		},
		{
			name:      "EXCEPT",
			sql:       "SELECT 1 EXCEPT SELECT 2;",
			wantError: false,
		},
		{
			name:      "INTERSECT",
			sql:       "SELECT 1 INTERSECT SELECT 2;",
			wantError: false,
		},
		{
			name:      "multiple UNION",
			sql:       "SELECT 1 UNION SELECT 2 UNION SELECT 3;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestWithClauseRecursive tests WITH RECURSIVE
func TestWithClauseRecursive(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "WITH RECURSIVE",
			sql:       "WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte;",
			wantError: false,
		},
		{
			name:      "WITH RECURSIVE with columns",
			sql:       "WITH RECURSIVE cte(n) AS (SELECT 1) SELECT * FROM cte;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestFromClauseMultipleTables tests FROM with multiple tables
func TestFromClauseMultipleTables(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "FROM with two tables",
			sql:       "SELECT * FROM t1, t2;",
			wantError: false,
		},
		{
			name:      "FROM with three tables",
			sql:       "SELECT * FROM t1, t2, t3;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestTableAliasWithAs tests table alias with and without AS
func TestTableAliasWithAs(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "table alias with AS",
			sql:       "SELECT * FROM users AS u;",
			wantError: false,
		},
		{
			name:      "table alias without AS",
			sql:       "SELECT * FROM users u;",
			wantError: false,
		},
		{
			name:      "subquery alias with AS",
			sql:       "SELECT * FROM (SELECT * FROM t) AS sub;",
			wantError: false,
		},
		{
			name:      "subquery alias without AS",
			sql:       "SELECT * FROM (SELECT * FROM t) sub;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestJoinTypes tests different JOIN types
func TestJoinTypes(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "INNER JOIN",
			sql:       "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;",
			wantError: false,
		},
		{
			name:      "LEFT JOIN",
			sql:       "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id;",
			wantError: false,
		},
		{
			name:      "LEFT OUTER JOIN",
			sql:       "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;",
			wantError: false,
		},
		{
			name:      "CROSS JOIN",
			sql:       "SELECT * FROM t1 CROSS JOIN t2;",
			wantError: false,
		},
		{
			name:      "simple JOIN",
			sql:       "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestJoinUsingClause tests JOIN with USING clause
func TestJoinUsingClause(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "JOIN USING single column",
			sql:       "SELECT * FROM t1 JOIN t2 USING (id);",
			wantError: false,
		},
		{
			name:      "JOIN USING multiple columns",
			sql:       "SELECT * FROM t1 JOIN t2 USING (id, name);",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestInsertMultipleRows tests INSERT with multiple value rows
func TestInsertMultipleRows(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "INSERT single row",
			sql:       "INSERT INTO t VALUES (1, 'a');",
			wantError: false,
		},
		{
			name:      "INSERT two rows",
			sql:       "INSERT INTO t VALUES (1, 'a'), (2, 'b');",
			wantError: false,
		},
		{
			name:      "INSERT three rows",
			sql:       "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestDoUpdateClauseWithWhere tests DO UPDATE SET with WHERE
func TestDoUpdateClauseWithWhere(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "DO UPDATE without WHERE",
			sql:       "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET value = 10;",
			wantError: false,
		},
		{
			name:      "DO UPDATE with WHERE",
			sql:       "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET value = 10 WHERE id > 0;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestCreateTableConstraints tests various table constraints
func TestCreateTableConstraints(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "UNIQUE table constraint",
			sql:       "CREATE TABLE t (id INTEGER, name TEXT, UNIQUE (id));",
			wantError: false,
		},
		{
			name:      "CHECK table constraint",
			sql:       "CREATE TABLE t (id INTEGER, CHECK (id > 0));",
			wantError: false,
		},
		{
			name:      "FOREIGN KEY table constraint",
			sql:       "CREATE TABLE t (id INTEGER, fk INTEGER, FOREIGN KEY (fk) REFERENCES other(id));",
			wantError: false,
		},
		{
			name:      "PRIMARY KEY table constraint single column",
			sql:       "CREATE TABLE t (id INTEGER, name TEXT, PRIMARY KEY (id));",
			wantError: false,
		},
		{
			name:      "PRIMARY KEY table constraint multiple columns",
			sql:       "CREATE TABLE t (id1 INTEGER, id2 INTEGER, PRIMARY KEY (id1, id2));",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v for SQL: %s", tt.wantError, err, tt.sql)
			}
		})
	}
}

// TestCreateTableColumnConstraints tests column-level constraints
func TestCreateTableColumnConstraints(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "CHECK column constraint",
			sql:       "CREATE TABLE t (id INTEGER CHECK (id > 0));",
			wantError: false,
		},
		{
			name:      "NOT NULL column constraint",
			sql:       "CREATE TABLE t (id INTEGER NOT NULL);",
			wantError: false,
		},
		{
			name:      "UNIQUE column constraint",
			sql:       "CREATE TABLE t (id INTEGER UNIQUE);",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestAlterTableActions tests different ALTER TABLE actions
func TestAlterTableActions(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "ALTER TABLE RENAME TO",
			sql:       "ALTER TABLE old_name RENAME TO new_name;",
			wantError: false,
		},
		{
			name:      "ALTER TABLE RENAME COLUMN",
			sql:       "ALTER TABLE t RENAME COLUMN old_col TO new_col;",
			wantError: false,
		},
		{
			name:      "ALTER TABLE ADD COLUMN",
			sql:       "ALTER TABLE t ADD COLUMN new_col TEXT;",
			wantError: false,
		},
		{
			name:      "ALTER TABLE DROP COLUMN",
			sql:       "ALTER TABLE t DROP COLUMN old_col;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseExpressionBitwiseOps tests various expression edge cases
func TestParseExpressionBitwiseOps(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "OR expression",
			sql:       "SELECT * FROM t WHERE a = 1 OR b = 2;",
			wantError: false,
		},
		{
			name:      "AND expression",
			sql:       "SELECT * FROM t WHERE a = 1 AND b = 2;",
			wantError: false,
		},
		{
			name:      "IS NULL",
			sql:       "SELECT * FROM t WHERE a IS NULL;",
			wantError: false,
		},
		{
			name:      "IS NOT NULL",
			sql:       "SELECT * FROM t WHERE a IS NOT NULL;",
			wantError: false,
		},
		{
			name:      "bitwise AND",
			sql:       "SELECT a & b FROM t;",
			wantError: false,
		},
		{
			name:      "bitwise OR",
			sql:       "SELECT a | b FROM t;",
			wantError: false,
		},
		{
			name:      "left shift",
			sql:       "SELECT a << 2 FROM t;",
			wantError: false,
		},
		{
			name:      "right shift",
			sql:       "SELECT a >> 2 FROM t;",
			wantError: false,
		},
		{
			name:      "string concatenation",
			sql:       "SELECT 'hello' || ' ' || 'world';",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseFunctionFilterClause tests function FILTER clause
func TestParseFunctionFilterClause(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "COUNT with FILTER",
			sql:       "SELECT COUNT(*) FILTER (WHERE id > 0) FROM t;",
			wantError: false,
		},
		{
			name:      "SUM with FILTER",
			sql:       "SELECT SUM(value) FILTER (WHERE active = 1) FROM t;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseCaseExprWithMultipleWhen tests CASE with multiple WHEN clauses
func TestParseCaseExprWithMultipleWhen(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "CASE with multiple WHEN",
			sql:       "SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' WHEN id = 3 THEN 'three' END FROM t;",
			wantError: false,
		},
		{
			name:      "CASE with ELSE",
			sql:       "SELECT CASE WHEN id > 0 THEN 'positive' ELSE 'non-positive' END FROM t;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseSubqueryInFromClause tests subqueries in FROM clause
func TestParseSubqueryInFromClause(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "simple subquery",
			sql:       "SELECT * FROM (SELECT id FROM t);",
			wantError: false,
		},
		{
			name:      "subquery with alias",
			sql:       "SELECT * FROM (SELECT id FROM t) AS sub;",
			wantError: false,
		},
		{
			name:      "nested subquery",
			sql:       "SELECT * FROM (SELECT * FROM (SELECT id FROM t));",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseCreateTriggerTiming tests trigger timing variations
func TestParseCreateTriggerTiming(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "BEFORE INSERT",
			sql:       "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END;",
			wantError: false,
		},
		{
			name:      "AFTER UPDATE",
			sql:       "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN SELECT 1; END;",
			wantError: false,
		},
		{
			name:      "INSTEAD OF DELETE",
			sql:       "CREATE TRIGGER tr INSTEAD OF DELETE ON t BEGIN SELECT 1; END;",
			wantError: false,
		},
		{
			name:      "UPDATE OF columns",
			sql:       "CREATE TRIGGER tr AFTER UPDATE OF col1, col2 ON t BEGIN SELECT 1; END;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseForeignKeyReferences tests foreign key reference clauses
func TestParseForeignKeyReferences(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "FOREIGN KEY table constraint basic",
			sql:       "CREATE TABLE t (id INTEGER, fk INTEGER, FOREIGN KEY (fk) REFERENCES other(id));",
			wantError: false,
		},
		{
			name:      "FOREIGN KEY with multiple columns",
			sql:       "CREATE TABLE t (id1 INTEGER, id2 INTEGER, fk1 INTEGER, fk2 INTEGER, FOREIGN KEY (fk1, fk2) REFERENCES other(id1, id2));",
			wantError: false,
		},
		// Note: ON DELETE/UPDATE clauses may not be fully supported in this parser
		// so we're testing basic FOREIGN KEY functionality
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseOrderByCollation tests ORDER BY with COLLATE
func TestParseOrderByCollation(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "ORDER BY with COLLATE",
			sql:       "SELECT * FROM t ORDER BY name COLLATE NOCASE;",
			wantError: false,
		},
		{
			name:      "ORDER BY with COLLATE ASC",
			sql:       "SELECT * FROM t ORDER BY name COLLATE NOCASE ASC;",
			wantError: false,
		},
		{
			name:      "ORDER BY with COLLATE DESC",
			sql:       "SELECT * FROM t ORDER BY name COLLATE NOCASE DESC;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseGroupByHavingClause tests GROUP BY with HAVING
func TestParseGroupByHavingClause(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "GROUP BY without HAVING",
			sql:       "SELECT COUNT(*) FROM t GROUP BY category;",
			wantError: false,
		},
		{
			name:      "GROUP BY with HAVING",
			sql:       "SELECT COUNT(*) FROM t GROUP BY category HAVING COUNT(*) > 5;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseLimitOffset tests LIMIT with OFFSET
func TestParseLimitOffset(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "LIMIT only",
			sql:       "SELECT * FROM t LIMIT 10;",
			wantError: false,
		},
		{
			name:      "LIMIT with OFFSET",
			sql:       "SELECT * FROM t LIMIT 10 OFFSET 5;",
			wantError: false,
		},
		{
			name:      "LIMIT with comma syntax",
			sql:       "SELECT * FROM t LIMIT 5, 10;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseVacuumInto tests VACUUM INTO
func TestParseVacuumInto(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "VACUUM",
			sql:       "VACUUM;",
			wantError: false,
		},
		{
			name:      "VACUUM schema",
			sql:       "VACUUM main;",
			wantError: false,
		},
		{
			name:      "VACUUM INTO",
			sql:       "VACUUM INTO 'backup.db';",
			wantError: false,
		},
		{
			name:      "VACUUM schema INTO",
			sql:       "VACUUM main INTO 'backup.db';",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseParenthesizedExpression tests parenthesized vs subquery
func TestParseParenthesizedExpression(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "simple paren",
			sql:       "SELECT (1 + 2);",
			wantError: false,
		},
		{
			name:      "nested paren",
			sql:       "SELECT ((1 + 2) * 3);",
			wantError: false,
		},
		{
			name:      "scalar subquery",
			sql:       "SELECT (SELECT MAX(id) FROM t);",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}

// TestParseIdentOrFunctionQualified tests qualified identifiers
func TestParseIdentOrFunctionQualified(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "qualified identifier",
			sql:       "SELECT t.id FROM t;",
			wantError: false,
		},
		{
			name:      "qualified star",
			sql:       "SELECT t.* FROM t;",
			wantError: false,
		},
		{
			name:      "unqualified identifier",
			sql:       "SELECT id FROM t;",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantError {
				t.Errorf("wantError=%v, got error=%v", tt.wantError, err)
			}
		})
	}
}
