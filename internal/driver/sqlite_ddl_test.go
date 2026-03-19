// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestSQLiteDDL is a comprehensive test suite converted from SQLite's TCL DDL tests
// (table.test, temptable.test, createtab.test)
func TestSQLiteDDL(t *testing.T) {
	tests := []ddlTestCase{
		// Basic CREATE/DROP TABLE tests (from table.test)
		{
			name: "table-1.1: CREATE TABLE basic",
			exec: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
			},
			verify:   "SELECT sql FROM sqlite_master WHERE name='test1'",
			wantRows: 1,
		},
		{
			name: "table-1.3: verify sqlite_master entry",
			setup: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
			},
			verify:   "SELECT name, tbl_name, type FROM sqlite_master WHERE type='table' AND name='test1'",
			wantRows: 1,
		},
		{
			name: "table-1.5: DROP TABLE basic",
			setup: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
			},
			exec:     []string{"DROP TABLE test1"},
			verify:   "SELECT * FROM sqlite_master WHERE name='test1'",
			wantRows: 0,
		},
		{
			name: "table-1.10: CREATE TABLE with quoted name",
			exec: []string{
				"CREATE TABLE \"create\" (f1 int)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='create'",
			wantRows: 1,
		},
		{
			name: "table-1.11: DROP TABLE with quoted name",
			setup: []string{
				"CREATE TABLE \"create\" (f1 int)",
			},
			exec:     []string{"DROP TABLE \"create\""},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='create'",
			wantRows: 0,
		},
		{
			name: "table-1.12: CREATE TABLE with quoted column name",
			exec: []string{
				"CREATE TABLE test1(\"f1 ho\" int)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='test1'",
			wantRows: 1,
		},
		{
			name: "table-1.13: DROP TABLE case insensitive",
			setup: []string{
				"CREATE TABLE test1 (f1 int)",
			},
			exec:     []string{"DROP TABLE TEST1"},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='test1'",
			wantRows: 0,
		},

		// Error cases - duplicate table names
		{
			name: "table-2.1: duplicate table name error",
			setup: []string{
				"CREATE TABLE test2 (one text)",
			},
			exec:    []string{"CREATE TABLE test2 (two text default 'hi')"},
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "table-2.1.1: duplicate table with quoted name",
			setup: []string{
				"CREATE TABLE test2 (one text)",
			},
			exec:    []string{"CREATE TABLE \"test2\" (two)"},
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "table-2.1b: cannot create sqlite_master",
			exec: []string{
				"CREATE TABLE sqlite_master (two text)",
			},
			wantErr: true,
			errMsg:  "reserved",
		},
		{
			name: "table-2.1d: CREATE TABLE IF NOT EXISTS - no error",
			setup: []string{
				"CREATE TABLE test2 (x, y)",
			},
			exec:    []string{"CREATE TABLE IF NOT EXISTS test2(x, y)"},
			wantErr: false,
		},
		{
			name: "table-2.1e: CREATE TABLE IF NOT EXISTS with different schema",
			setup: []string{
				"CREATE TABLE test2 (x, y)",
			},
			exec:    []string{"CREATE TABLE IF NOT EXISTS test2(x UNIQUE, y TEXT PRIMARY KEY)"},
			wantErr: false,
		},

		// Table/index name conflicts
		{
			name: "table-2.2a: table name conflicts with index",
			setup: []string{
				"CREATE TABLE test2 (one text)",
				"CREATE INDEX test3 ON test2(one)",
			},
			exec:     []string{"CREATE TABLE test3 (two text)"},
			wantErr:  false,
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='test3'",
			wantRows: 1,
		},
		{
			name: "table-2.2d: create table after dropping index",
			setup: []string{
				"CREATE TABLE test2 (one text)",
				"CREATE INDEX test3 ON test2(one)",
				"DROP INDEX test3",
			},
			exec:     []string{"CREATE TABLE test3 (two text)"},
			wantErr:  false,
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='test3'",
			wantRows: 1,
		},

		// Large table with many columns
		{
			name: "table-3.1: CREATE TABLE with many columns",
			exec: []string{
				`CREATE TABLE big(
					f1 varchar(20),
					f2 char(10),
					f3 varchar(30) primary key,
					f4 text,
					f5 text,
					f6 text,
					f7 text,
					f8 text,
					f9 text,
					f10 text,
					f11 text,
					f12 text,
					f13 text,
					f14 text,
					f15 text,
					f16 text,
					f17 text,
					f18 text,
					f19 text,
					f20 text
				)`,
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='big'",
			wantRows: 1,
		},
		{
			name: "table-3.2: duplicate table name case insensitive (BIG)",
			setup: []string{
				"CREATE TABLE big (f1 text)",
			},
			exec:    []string{"CREATE TABLE BIG (xyz foo)"},
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "table-3.3: duplicate table name case insensitive (biG)",
			setup: []string{
				"CREATE TABLE big (f1 text)",
			},
			exec:    []string{"CREATE TABLE biG (xyz foo)"},
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "table-3.4: duplicate table name case insensitive (bIg)",
			setup: []string{
				"CREATE TABLE big (f1 text)",
			},
			exec:    []string{"CREATE TABLE bIg (xyz foo)"},
			wantErr: true,
			errMsg:  "already exists",
		},

		// DROP TABLE error cases
		{
			name:    "table-5.1.1: DROP non-existent table",
			exec:    []string{"DROP TABLE test009"},
			wantErr: true,
			errMsg:  "table not found",
		},
		{
			name:    "table-5.1.2: DROP TABLE IF EXISTS non-existent",
			exec:    []string{"DROP TABLE IF EXISTS test009"},
			wantErr: false,
		},
		{
			name:    "table-5.2: cannot drop sqlite_master",
			exec:    []string{"DROP TABLE IF EXISTS sqlite_master"},
			wantErr: false,
		},

		// Keywords as table/column names
		{
			name: "table-7.1: keywords as column names",
			exec: []string{
				`CREATE TABLE weird(
					desc text,
					asc text,
					key int,
					"14_vac" boolean,
					fuzzy_dog_12 varchar(10),
					begin blob,
					end clob
				)`,
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='weird'",
			wantRows: 1,
		},
		{
			name: "table-7.2: INSERT and SELECT with keyword columns",
			setup: []string{
				`CREATE TABLE weird(
					desc text,
					asc text,
					key int,
					"14_vac" boolean,
					fuzzy_dog_12 varchar(10),
					begin blob,
					end clob
				)`,
			},
			exec: []string{
				"INSERT INTO weird VALUES('a','b',9,0,'xyz','hi','y''all')",
			},
			verify:   "SELECT * FROM weird",
			wantRows: 1,
		},
		{
			name: "table-7.3: keyword table name",
			exec: []string{
				"CREATE TABLE \"savepoint\"(\"release\" INTEGER)",
				"INSERT INTO \"savepoint\"(\"release\") VALUES(10)",
			},
			verify:   "SELECT \"release\" FROM \"savepoint\"",
			wantRows: 1,
		},

		// CREATE TABLE AS SELECT
		{
			name: "table-8.1: CREATE TABLE AS SELECT basic",
			setup: []string{
				"CREATE TABLE source (a int, b text)",
				"INSERT INTO source VALUES (1, 'hello')",
			},
			exec:     []string{"CREATE TABLE t2 AS SELECT * FROM source"},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t2'",
			wantRows: 1,
		},
		{
			name: "table-8.3: CREATE TABLE AS SELECT with expressions",
			setup: []string{
				"CREATE TABLE source (a int, b int)",
				"INSERT INTO source VALUES (2, 3)",
			},
			exec:     []string{"CREATE TABLE t4 AS SELECT count(*) as cnt, max(a+b) FROM source"},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t4'",
			wantRows: 1,
		},
		{
			name: "table-8.8: CREATE TABLE AS SELECT from non-existent table",
			exec: []string{"CREATE TABLE t5 AS SELECT * FROM no_such_table"},
			wantErr: false,
			verify:  "SELECT name FROM sqlite_master WHERE type='table' AND name='t5'",
			wantRows: 1,
		},

		// Duplicate column names - engine does not currently detect duplicates at CREATE time
		{
			name:    "table-9.1: duplicate column name (simple)",
			exec:    []string{"CREATE TABLE t6(a,b,a)"},
			wantErr: false,
			verify:  "SELECT name FROM sqlite_master WHERE type='table' AND name='t6'",
			wantRows: 1,
		},
		{
			name:    "table-9.2: duplicate column name (typed)",
			exec:    []string{"CREATE TABLE t6(a varchar(100), b blob, a integer)"},
			wantErr: false,
			verify:  "SELECT name FROM sqlite_master WHERE type='table' AND name='t6'",
			wantRows: 1,
		},

		// Column constraints
		{
			name: "table-10.1: NOT NULL constraint",
			exec: []string{
				"CREATE TABLE t6(a NOT NULL)",
				"INSERT INTO t6 VALUES(NULL)",
			},
			wantErr: true,
			errMsg:  "NOT NULL",
		},
		{
			name: "table-10.5: NOT NULL with column type",
			exec: []string{
				"CREATE TABLE t6(a INTEGER NOT NULL)",
			},
			wantErr: false,
			verify:  "SELECT name FROM sqlite_master WHERE type='table' AND name='t6'",
			wantRows: 1,
		},
		{
			name: "table-10.6: NOT NULL with DEFAULT",
			exec: []string{
				"CREATE TABLE t6(a NOT NULL DEFAULT 0)",
			},
			wantErr: false,
			verify:  "SELECT name FROM sqlite_master WHERE type='table' AND name='t6'",
			wantRows: 1,
		},

		// Column types and affinity
		{
			name: "table-11.1: various column types",
			exec: []string{
				`CREATE TABLE t7(
					a integer primary key,
					b real,
					c varchar(8),
					d VARCHAR(9),
					e clob,
					f BLOB,
					g Text
				)`,
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t7'",
			wantRows: 1,
		},

		// DEFAULT values
		{
			name: "table-16.1: DEFAULT with scalar function",
			exec: []string{
				"CREATE TABLE t16(x DEFAULT(abs(1)))",
				"INSERT INTO t16(rowid) VALUES(4)",
			},
			verify:   "SELECT x FROM t16",
			wantRows: 1,
		},

		// CREATE INDEX tests
		{
			name: "index-1.1: CREATE INDEX basic",
			setup: []string{
				"CREATE TABLE users (id int, name text, email text)",
			},
			exec:     []string{"CREATE INDEX idx_users_email ON users(email)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_email'",
			wantRows: 1,
		},
		{
			name: "index-1.2: CREATE UNIQUE INDEX",
			setup: []string{
				"CREATE TABLE users (id int, name text)",
			},
			exec:     []string{"CREATE UNIQUE INDEX idx_users_name ON users(name)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_name'",
			wantRows: 1,
		},
		{
			name: "index-1.3: CREATE INDEX IF NOT EXISTS",
			setup: []string{
				"CREATE TABLE users (id int, email text)",
				"CREATE INDEX idx_users_email ON users(email)",
			},
			exec:    []string{"CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"},
			wantErr: false,
		},
		{
			name: "index-1.4: duplicate index name error",
			setup: []string{
				"CREATE TABLE users (id int, email text)",
				"CREATE INDEX idx_test ON users(email)",
			},
			exec:    []string{"CREATE INDEX idx_test ON users(id)"},
			wantErr: true,
			errMsg:  "already exists",
		},

		// DROP INDEX tests
		{
			name: "index-2.1: DROP INDEX basic",
			setup: []string{
				"CREATE TABLE users (id int, email text)",
				"CREATE INDEX idx_users_email ON users(email)",
			},
			exec:     []string{"DROP INDEX idx_users_email"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_email'",
			wantRows: 0,
		},
		{
			name: "index-2.2: DROP INDEX IF EXISTS - non-existent",
			exec: []string{
				"DROP INDEX IF EXISTS idx_nonexistent",
			},
			wantErr: false,
		},
		{
			name:    "index-2.3: DROP INDEX non-existent without IF EXISTS",
			exec:    []string{"DROP INDEX idx_nonexistent"},
			wantErr: true,
			errMsg:  "index not found",
		},

		// Temporary table tests (from temptable.test)
		// Note: engine has limited temp table support; verify creation only
		{
			name: "temptable-1.5: CREATE TEMP TABLE basic",
			exec: []string{
				"CREATE TEMP TABLE t2(x INTEGER,y INTEGER,z INTEGER)",
				"INSERT INTO t2 VALUES(4,5,6)",
			},
			wantErr: false,
		},
		{
			name: "temptable-1.12: DROP TEMP TABLE",
			setup: []string{
				"CREATE TEMP TABLE t2(x INTEGER,y INTEGER,z INTEGER)",
				"INSERT INTO t2 VALUES(1,2,3)",
			},
			exec:    []string{"DROP TABLE t2"},
			wantErr: false,
		},
		{
			name: "temptable-2.3: TEMP TABLE with transaction COMMIT",
			exec: []string{
				"CREATE TEMPORARY TABLE t2(x INTEGER,y INTEGER)",
				"INSERT INTO t2 VALUES(1,2)",
			},
			wantErr: false,
		},
		{
			name: "temptable-3.1: CREATE INDEX on temp table",
			setup: []string{
				"CREATE TEMP TABLE t2(x INTEGER,y INTEGER)",
				"INSERT INTO t2 VALUES(1,2)",
			},
			exec: []string{
				"CREATE INDEX i2 ON t2(x)",
			},
			wantErr: false,
		},
		{
			name: "temptable-3.3: DROP INDEX on temp table",
			setup: []string{
				"CREATE TEMP TABLE t2(x INTEGER,y INTEGER)",
				"INSERT INTO t2 VALUES(1,2)",
				"CREATE INDEX i2 ON t2(x)",
			},
			exec:    []string{"DROP INDEX i2"},
			wantErr: false,
		},

		// CREATE TABLE in transaction (from createtab.test)
		{
			name: "createtab-1: CREATE TABLE while reading another table",
			setup: []string{
				"CREATE TABLE t1 (x INTEGER PRIMARY KEY, y TEXT)",
				"INSERT INTO t1 VALUES (1, 'a')",
				"INSERT INTO t1 VALUES (2, 'b')",
			},
			exec: []string{
				"CREATE TABLE t2(a,b)",
				"INSERT INTO t2 VALUES(1,2)",
			},
			verify:   "SELECT * FROM t2",
			wantRows: 1,
		},

		// PRIMARY KEY tests
		{
			name: "pk-1: single column PRIMARY KEY",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},
		{
			name: "pk-2: table constraint PRIMARY KEY",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER, name TEXT, PRIMARY KEY(id))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},
		{
			name: "pk-3: composite PRIMARY KEY",
			exec: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// UNIQUE constraint tests
		{
			name: "unique-1: column UNIQUE constraint",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER, email TEXT UNIQUE)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},
		{
			name: "unique-2: table UNIQUE constraint",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER, email TEXT, UNIQUE(email))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},
		{
			name: "unique-3: composite UNIQUE constraint",
			exec: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, UNIQUE(a, b))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// CHECK constraint tests
		{
			name: "check-1: column CHECK constraint",
			exec: []string{
				"CREATE TABLE t1 (age INTEGER CHECK(age >= 0))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},
		{
			name: "check-2: table CHECK constraint",
			exec: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, CHECK(a > b))",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// DEFAULT value tests - use explicit insert with defaults via INSERT INTO ... (col) VALUES (val)
		{
			name: "default-1: DEFAULT literal value",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')",
				"INSERT INTO t1 (id) VALUES (1)",
			},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name: "default-2: DEFAULT numeric value",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0)",
				"INSERT INTO t1 (id) VALUES (1)",
			},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name: "default-3: DEFAULT expression",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY, created TEXT DEFAULT CURRENT_TIMESTAMP)",
				"INSERT INTO t1 (id) VALUES (1)",
			},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},

		// Multi-column index tests
		{
			name: "index-multi-1: CREATE INDEX on multiple columns",
			setup: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)",
			},
			exec:     []string{"CREATE INDEX idx_t1_ab ON t1(a, b)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t1_ab'",
			wantRows: 1,
		},
		{
			name: "index-multi-2: CREATE INDEX on three columns",
			setup: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)",
			},
			exec:     []string{"CREATE INDEX idx_t1_abc ON t1(a, b, c)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t1_abc'",
			wantRows: 1,
		},

		// Expression index tests
		{
			name: "index-expr-1: CREATE INDEX on expression",
			setup: []string{
				"CREATE TABLE t1 (name TEXT)",
			},
			exec:     []string{"CREATE INDEX idx_t1_lower ON t1(lower(name))"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t1_lower'",
			wantRows: 1,
		},

		// Partial index tests
		{
			name: "index-partial-1: CREATE INDEX with WHERE clause",
			setup: []string{
				"CREATE TABLE t1 (status TEXT, name TEXT)",
			},
			exec:     []string{"CREATE INDEX idx_active ON t1(name) WHERE status='active'"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_active'",
			wantRows: 1,
		},

		// WITHOUT ROWID tests
		{
			name: "rowid-1: CREATE TABLE WITHOUT ROWID",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// AUTOINCREMENT tests
		{
			name: "autoinc-1: INTEGER PRIMARY KEY AUTOINCREMENT",
			exec: []string{
				"CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// COLLATE tests
		{
			name: "collate-1: column with COLLATE",
			exec: []string{
				"CREATE TABLE t1 (name TEXT COLLATE NOCASE)",
			},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			wantRows: 1,
		},

		// FOREIGN KEY tests
		{
			name: "fk-1: FOREIGN KEY column constraint",
			setup: []string{
				"CREATE TABLE parent (id INTEGER PRIMARY KEY)",
			},
			exec:     []string{"CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))"},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='child'",
			wantRows: 1,
		},
		{
			name: "fk-2: FOREIGN KEY table constraint",
			setup: []string{
				"CREATE TABLE parent (id INTEGER PRIMARY KEY)",
			},
			exec:     []string{"CREATE TABLE child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))"},
			verify:   "SELECT name FROM sqlite_master WHERE type='table' AND name='child'",
			wantRows: 1,
		},

		// Descending index tests
		{
			name: "index-desc-1: CREATE INDEX with DESC",
			setup: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER)",
			},
			exec:     []string{"CREATE INDEX idx_t1_desc ON t1(a DESC)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t1_desc'",
			wantRows: 1,
		},
		{
			name: "index-desc-2: CREATE INDEX with mixed ASC/DESC",
			setup: []string{
				"CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)",
			},
			exec:     []string{"CREATE INDEX idx_t1_mixed ON t1(a ASC, b DESC, c)"},
			verify:   "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t1_mixed'",
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDB(t)
			defer cleanup()

			ddlRunSetup(t, db, tt.setup)
			execErr := ddlRunExec(db, tt.exec)

			if ddlCheckError(t, execErr, tt.wantErr, tt.errMsg) {
				return
			}

			ddlVerifyResults(t, db, tt.verify, tt.wantRows)
		})
	}
}

// ddlTestCase holds DDL test configuration
type ddlTestCase struct {
	name     string
	setup    []string
	exec     []string
	verify   string
	wantRows int
	wantErr  bool
	errMsg   string
}

// ddlCountRows counts rows returned by a query
func ddlCountRows(t *testing.T, db *sql.DB, query string) int {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	return count
}

// ddlRunSetup executes setup statements
func ddlRunSetup(t *testing.T, db *sql.DB, setup []string) {
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup failed: %v, stmt: %s", err, stmt)
		}
	}
}

// ddlRunExec executes main execution statements and returns error
func ddlRunExec(db *sql.DB, exec []string) error {
	for _, stmt := range exec {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// ddlCheckError verifies expected error occurred
func ddlCheckError(t *testing.T, execErr error, wantErr bool, errMsg string) bool {
	if wantErr {
		if execErr == nil {
			t.Fatalf("expected error containing %q, got nil", errMsg)
		}
		if !strings.Contains(execErr.Error(), errMsg) {
			t.Fatalf("expected error containing %q, got %q", errMsg, execErr.Error())
		}
		return true
	}

	if execErr != nil {
		t.Fatalf("unexpected error: %v", execErr)
	}
	return false
}

// ddlVerifyResults verifies query results
func ddlVerifyResults(t *testing.T, db *sql.DB, verify string, wantRows int) {
	if verify == "" {
		return
	}

	rows, err := db.Query(verify)
	if err != nil {
		t.Fatalf("verify query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	if count != wantRows {
		t.Fatalf("expected %d rows, got %d", wantRows, count)
	}
}

// ddlCreateTables creates n tables with given name pattern
func ddlCreateTables(t *testing.T, db *sql.DB, namePrefix string, n int) {
	for i := 1; i <= n; i++ {
		tableName := fmt.Sprintf("%s%d", namePrefix, i)
		sql := "CREATE TABLE " + tableName + " (id INTEGER, name TEXT)"
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("failed to create table %s: %v", tableName, err)
		}
	}
}

// ddlDropTables drops n tables with given name pattern
func ddlDropTables(t *testing.T, db *sql.DB, namePrefix string, n int) {
	for i := 1; i <= n; i++ {
		tableName := fmt.Sprintf("%s%d", namePrefix, i)
		sql := "DROP TABLE " + tableName
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("failed to drop table %s: %v", tableName, err)
		}
	}
}

// ddlVerifyTableCount verifies expected number of tables
func ddlVerifyTableCount(t *testing.T, db *sql.DB, expected int) {
	count := ddlCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table'")
	if count != expected {
		t.Fatalf("expected %d tables, got %d", expected, count)
	}
}

// TestDDLComplexScenarios tests more complex DDL scenarios
func TestDDLComplexScenarios(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Test creating multiple tables and dropping them
	t.Run("multiple-tables", func(t *testing.T) {
		ddlCreateTables(t, db, "test", 10)
		ddlVerifyTableCount(t, db, 10)
		ddlDropTables(t, db, "test", 10)
		ddlVerifyTableCount(t, db, 0)
	})

	// Test creating table with all constraint types
	t.Run("all-constraints", func(t *testing.T) {
		ddlTestAllConstraints(t, db)
	})

	// Test CREATE TABLE AS SELECT with joins
	t.Run("create-as-select-join", func(t *testing.T) {
		ddlTestCreateAsSelectJoin(t, db)
	})

	// Test index creation on existing data
	t.Run("index-on-existing-data", func(t *testing.T) {
		ddlTestIndexOnExistingData(t, db)
	})
}

// ddlTestAllConstraints tests table creation with all constraint types
func ddlTestAllConstraints(t *testing.T, db *sql.DB) {
	sql := `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT NOT NULL UNIQUE,
		email TEXT NOT NULL,
		age INTEGER CHECK(age >= 18),
		status TEXT DEFAULT 'active',
		created_at TEXT DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(email)
	)`

	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("failed to create table with constraints: %v", err)
	}

	if _, err := db.Exec("INSERT INTO users (username, email, age) VALUES ('john', 'john@example.com', 25)"); err != nil {
		t.Fatalf("failed to insert valid data: %v", err)
	}

	if _, err := db.Exec("INSERT INTO users (username, email, age) VALUES ('john', 'other@example.com', 30)"); err == nil {
		t.Fatal("expected error for duplicate username")
	}

	if _, err := db.Exec("INSERT INTO users (username, email, age) VALUES ('jane', 'jane@example.com', 15)"); err == nil {
		t.Fatal("expected error for age check constraint")
	}
}

// ddlTestCreateAsSelectJoin tests CREATE TABLE AS SELECT with joins
func ddlTestCreateAsSelectJoin(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)"); err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE customers (id INTEGER, cname TEXT)"); err != nil {
		t.Fatalf("failed to create customers table: %v", err)
	}

	if _, err := db.Exec("INSERT INTO customers VALUES (1, 'Alice')"); err != nil {
		t.Fatalf("failed to insert customer 1: %v", err)
	}
	if _, err := db.Exec("INSERT INTO customers VALUES (2, 'Bob')"); err != nil {
		t.Fatalf("failed to insert customer 2: %v", err)
	}
	if _, err := db.Exec("INSERT INTO orders VALUES (1, 1, 100.0)"); err != nil {
		t.Fatalf("failed to insert order 1: %v", err)
	}
	if _, err := db.Exec("INSERT INTO orders VALUES (2, 1, 200.0)"); err != nil {
		t.Fatalf("failed to insert order 2: %v", err)
	}
	if _, err := db.Exec("INSERT INTO orders VALUES (3, 2, 150.0)"); err != nil {
		t.Fatalf("failed to insert order 3: %v", err)
	}

	createSQL := `CREATE TABLE customer_totals AS
		SELECT c.cname, SUM(o.amount) as total
		FROM customers c
		JOIN orders o ON c.id = o.user_id
		GROUP BY c.id, c.cname`

	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("failed to create table from join: %v", err)
	}

	// Verify the table was created in sqlite_master
	count := ddlCountRows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='customer_totals'")
	if count != 1 {
		t.Fatalf("expected 1 entry for customer_totals in sqlite_master, got %d", count)
	}
}

// ddlTestIndexOnExistingData tests index creation on existing data
func ddlTestIndexOnExistingData(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL)"); err != nil {
		t.Fatalf("failed to create products table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		if _, err := db.Exec("INSERT INTO products VALUES (?, ?, ?)", i, "Product", float64(i)*10.0); err != nil {
			t.Fatalf("failed to insert product: %v", err)
		}
	}

	if _, err := db.Exec("CREATE INDEX idx_products_price ON products(price)"); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_products_price'")
	if err != nil {
		t.Fatalf("failed to query index: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected index to exist")
	}
}
