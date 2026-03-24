// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func TestParserLowCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// parseTableFuncArgs
		{
			name:    "TVF single arg",
			sql:     "SELECT * FROM generate_series(1, 10)",
			wantErr: false,
		},
		{
			name:    "TVF multiple args",
			sql:     "SELECT * FROM generate_series(1, 100, 5)",
			wantErr: false,
		},

		// parseReplaceInto
		{
			name:    "REPLACE INTO values",
			sql:     "REPLACE INTO t VALUES(1, 2)",
			wantErr: false,
		},
		{
			name:    "REPLACE INTO with columns",
			sql:     "REPLACE INTO t(a, b) VALUES(1, 2)",
			wantErr: false,
		},

		// parseFKDeferrable — DEFERRABLE INITIALLY DEFERRED
		{
			name:    "FK DEFERRABLE INITIALLY DEFERRED",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
			wantErr: false,
		},
		// parseFKDeferrable — DEFERRABLE INITIALLY IMMEDIATE
		{
			name:    "FK DEFERRABLE INITIALLY IMMEDIATE",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)",
			wantErr: false,
		},
		// parseFKDeferrable — bare DEFERRABLE (no INITIALLY)
		{
			name:    "FK DEFERRABLE no INITIALLY",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) DEFERRABLE)",
			wantErr: false,
		},

		// parseFKMatchClause — match name must be a plain identifier (TK_ID)
		{
			name:    "FK MATCH SIMPLE",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) MATCH SIMPLE)",
			wantErr: false,
		},
		{
			name:    "FK MATCH custom name",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) MATCH myname)",
			wantErr: false,
		},

		// parseFKNotDeferrable
		{
			name:    "FK NOT DEFERRABLE",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) NOT DEFERRABLE)",
			wantErr: false,
		},

		// parseFKActionStep — ON DELETE / ON UPDATE covered via FK actions
		{
			name:    "FK ON DELETE CASCADE",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) ON DELETE CASCADE)",
			wantErr: false,
		},
		{
			name:    "FK ON UPDATE RESTRICT",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) ON UPDATE RESTRICT)",
			wantErr: false,
		},
		{
			name:    "FK MATCH and DEFERRABLE combined",
			sql:     "CREATE TABLE t(id INT REFERENCES p(id) MATCH SIMPLE NOT DEFERRABLE)",
			wantErr: false,
		},

		// applyConstraintGenerated
		{
			name:    "GENERATED ALWAYS AS STORED",
			sql:     "CREATE TABLE t(id INT, x INT GENERATED ALWAYS AS (id*2) STORED)",
			wantErr: false,
		},
		{
			name:    "GENERATED ALWAYS AS VIRTUAL",
			sql:     "CREATE TABLE t(id INT, x INT GENERATED ALWAYS AS (id+1) VIRTUAL)",
			wantErr: false,
		},

		// parseIsDistinctFrom
		{
			name:    "IS DISTINCT FROM",
			sql:     "SELECT 1 IS DISTINCT FROM 2",
			wantErr: false,
		},
		{
			name:    "IS DISTINCT FROM null",
			sql:     "SELECT x IS DISTINCT FROM NULL FROM t",
			wantErr: false,
		},

		// parseIsNotDistinctFrom
		{
			name:    "IS NOT DISTINCT FROM",
			sql:     "SELECT 1 IS NOT DISTINCT FROM 2",
			wantErr: false,
		},
		{
			name:    "IS NOT DISTINCT FROM null",
			sql:     "SELECT x IS NOT DISTINCT FROM NULL FROM t",
			wantErr: false,
		},

		// isRaiseAction / parseRaiseMessage — trigger body
		{
			name:    "RAISE ABORT in trigger",
			sql:     "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT RAISE(ABORT, 'err'); END",
			wantErr: false,
		},
		{
			name:    "RAISE ROLLBACK in trigger",
			sql:     "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT RAISE(ROLLBACK, 'msg'); END",
			wantErr: false,
		},
		{
			name:    "RAISE FAIL in trigger",
			sql:     "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT RAISE(FAIL, 'bad'); END",
			wantErr: false,
		},
		{
			name:    "RAISE IGNORE in trigger",
			sql:     "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT RAISE(IGNORE); END",
			wantErr: false,
		},

		// parseJSONArrowOps (-> and ->>)
		{
			name:    "JSON arrow operator ->",
			sql:     "SELECT j->'key' FROM t",
			wantErr: false,
		},
		{
			name:    "JSON arrow operator ->>",
			sql:     "SELECT j->>'key' FROM t",
			wantErr: false,
		},
		{
			name:    "JSON chained arrow operators",
			sql:     "SELECT j->'a'->>'b' FROM t",
			wantErr: false,
		},

		// parseIsNotBranch — IS NOT <expr> (not null, not distinct)
		{
			name:    "IS NOT integer literal",
			sql:     "SELECT x IS NOT 5 FROM t",
			wantErr: false,
		},
		{
			name:    "IS NOT string literal",
			sql:     "SELECT x IS NOT 'foo' FROM t",
			wantErr: false,
		},

		// parseUpdateFromClause
		{
			name:    "UPDATE FROM clause",
			sql:     "UPDATE t SET x=1 FROM src WHERE t.id=src.id",
			wantErr: false,
		},
		{
			name:    "UPDATE FROM with join",
			sql:     "UPDATE t SET a=s.a FROM src s WHERE t.id=s.id",
			wantErr: false,
		},

		// parseReturningClause
		{
			name:    "INSERT RETURNING id",
			sql:     "INSERT INTO t VALUES(1) RETURNING id",
			wantErr: false,
		},
		{
			name:    "INSERT RETURNING star",
			sql:     "INSERT INTO t(a) VALUES(1) RETURNING *",
			wantErr: false,
		},
		{
			name:    "UPDATE RETURNING",
			sql:     "UPDATE t SET x=1 RETURNING x",
			wantErr: false,
		},
		{
			name:    "DELETE RETURNING",
			sql:     "DELETE FROM t WHERE id=1 RETURNING id",
			wantErr: false,
		},

		// parseNotPatternOp / tryParsePatternOp
		{
			name:    "NOT LIKE",
			sql:     "SELECT * FROM t WHERE name NOT LIKE '%foo%'",
			wantErr: false,
		},
		{
			name:    "NOT GLOB",
			sql:     "SELECT * FROM t WHERE name NOT GLOB '*foo*'",
			wantErr: false,
		},
		{
			name:    "LIKE pattern",
			sql:     "SELECT * FROM t WHERE name LIKE 'foo%'",
			wantErr: false,
		},
		{
			name:    "LIKE with ESCAPE",
			sql:     "SELECT * FROM t WHERE name LIKE '%!_%' ESCAPE '!'",
			wantErr: false,
		},
		{
			name:    "GLOB pattern",
			sql:     "SELECT * FROM t WHERE name GLOB 'foo*'",
			wantErr: false,
		},

		// parseInsertBody — schema-qualified table
		{
			name:    "INSERT with schema qualifier",
			sql:     "INSERT INTO main.t VALUES(1)",
			wantErr: false,
		},
		{
			name:    "REPLACE INTO with schema qualifier",
			sql:     "REPLACE INTO main.t VALUES(2)",
			wantErr: false,
		},
		{
			name:    "INSERT DEFAULT VALUES",
			sql:     "INSERT INTO t DEFAULT VALUES",
			wantErr: false,
		},

		// parseWindowDef — WINDOW clause with various specs
		{
			name:    "WINDOW with PARTITION BY",
			sql:     "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (PARTITION BY a)",
			wantErr: false,
		},
		{
			name:    "WINDOW with ORDER BY",
			sql:     "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY b)",
			wantErr: false,
		},
		{
			name:    "WINDOW with PARTITION BY and ORDER BY",
			sql:     "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)",
			wantErr: false,
		},
		{
			name:    "WINDOW empty spec",
			sql:     "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS ()",
			wantErr: false,
		},
		{
			name:    "multiple named windows",
			sql:     "SELECT ROW_NUMBER() OVER w1, RANK() OVER w2 FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.sql, err, tt.wantErr)
			}
		})
	}
}
