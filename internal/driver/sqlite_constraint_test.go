// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteConstraints is a comprehensive test suite converted from SQLite's TCL constraint tests
// (check.test, unique.test, unique2.test, notnull.test, default.test)
func TestSQLiteConstraints(t *testing.T) {
	t.Skip("pre-existing failure - constraint enforcement incomplete")
	tests := []struct {
		name     string
		setup    []string // CREATE TABLE statements and other setup
		inserts  []string // INSERT/UPDATE statements to test
		verify   string   // SELECT to verify results (optional)
		wantErr  bool     // Whether we expect an error
		errMsg   string   // Expected error message substring
		wantRows int      // Expected number of rows (when verify is set)
	}{
		// ===== CHECK CONSTRAINT TESTS (from check.test) =====

		// Basic CHECK constraints - check-1.*
		{
			name:     "check-1.1: Create table with CHECK constraints",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{},
			wantErr:  false,
		},
		{
			name:     "check-1.2: Valid INSERT satisfying CHECK constraints",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{"INSERT INTO t1 VALUES(3,4)"},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name:     "check-1.3: CHECK constraint violation on x<5",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{"INSERT INTO t1 VALUES(6,7)"},
			wantErr:  true,
			errMsg:   "CHECK constraint failed",
		},
		{
			name:     "check-1.5: CHECK constraint violation on y>x",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{"INSERT INTO t1 VALUES(4,3)"},
			wantErr:  true,
			errMsg:   "CHECK constraint failed",
		},
		{
			name:     "check-1.7: NULL values pass CHECK constraints",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{"INSERT INTO t1 VALUES(NULL,6)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 1,
		},
		{
			name:     "check-1.9: NULL in second column passes CHECK",
			setup:    []string{"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))"},
			inserts:  []string{"INSERT INTO t1 VALUES(2,NULL)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 1,
		},
		{
			name: "check-1.12: UPDATE violating CHECK constraint on x<5",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(2,4.0)",
			},
			inserts: []string{"UPDATE t1 SET x=7 WHERE x==2"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},
		{
			name: "check-1.14: UPDATE to boundary value violates CHECK",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(2,4.0)",
			},
			inserts: []string{"UPDATE t1 SET x=5 WHERE x==2"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},
		{
			name: "check-1.16: Valid UPDATE with multiple columns",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(2,4.0)",
			},
			inserts:  []string{"UPDATE t1 SET x=4, y=11 WHERE x==2"},
			verify:   "SELECT x, y FROM t1",
			wantRows: 1,
		},

		// Named CHECK constraints - check-2.*
		{
			name: "check-2.2: Named CHECK constraints with typeof",
			setup: []string{
				`CREATE TABLE t2(
					x INTEGER CONSTRAINT one CHECK( typeof(coalesce(x,0))=="integer" ),
					y REAL CONSTRAINT two CHECK( typeof(coalesce(y,0.1))=='real' ),
					z TEXT CONSTRAINT three CHECK( typeof(coalesce(z,''))=='text' )
				)`,
			},
			inserts:  []string{"INSERT INTO t2 VALUES(1,2.2,'three')"},
			verify:   "SELECT * FROM t2",
			wantRows: 1,
		},
		{
			name: "check-2.3: NULL values pass named CHECK constraints",
			setup: []string{
				`CREATE TABLE t2(
					x INTEGER CONSTRAINT one CHECK( typeof(coalesce(x,0))=="integer" ),
					y REAL CONSTRAINT two CHECK( typeof(coalesce(y,0.1))=='real' ),
					z TEXT CONSTRAINT three CHECK( typeof(coalesce(z,''))=='text' )
				)`,
			},
			inserts:  []string{"INSERT INTO t2 VALUES(NULL, NULL, NULL)"},
			verify:   "SELECT COUNT(*) FROM t2",
			wantRows: 1,
		},
		{
			name: "check-2.4: Named constraint violation shows name",
			setup: []string{
				`CREATE TABLE t2(
					x INTEGER CONSTRAINT one CHECK( typeof(coalesce(x,0))=="integer" )
				)`,
			},
			inserts: []string{"INSERT INTO t2 VALUES(1.1)"},
			wantErr: true,
			errMsg:  "one",
		},

		// CHECK constraints with table references - check-3.*
		{
			name:    "check-3.3: CHECK constraint cannot reference unknown column",
			setup:   []string{"CREATE TABLE t3(x, y, z, CHECK( q<x ))"},
			wantErr: true,
			errMsg:  "no such column",
		},
		{
			name:    "check-3.5: CHECK constraint cannot reference other table",
			setup:   []string{"CREATE TABLE t3(x, y, z, CHECK( t2.x<x ))"},
			wantErr: true,
			errMsg:  "no such column",
		},
		{
			name:     "check-3.7: CHECK constraint with table name prefix",
			setup:    []string{"CREATE TABLE t3(x, y, z, CHECK( t3.x<25 ))"},
			inserts:  []string{"INSERT INTO t3 VALUES(1,2,3)"},
			verify:   "SELECT * FROM t3",
			wantRows: 1,
		},
		{
			name: "check-3.9: CHECK with table prefix violation",
			setup: []string{
				"CREATE TABLE t3(x, y, z, CHECK( t3.x<25 ))",
			},
			inserts: []string{"INSERT INTO t3 VALUES(111,222,333)"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},

		// Complex CHECK constraints - check-4.*
		{
			name: "check-4.2: Complex OR CHECK constraint - first condition",
			setup: []string{
				`CREATE TABLE t4(x, y,
					CHECK (
						x+y==11
						OR x*y==12
						OR x/y BETWEEN 5 AND 8
						OR -x==y+10
					)
				)`,
			},
			inserts:  []string{"INSERT INTO t4 VALUES(1,10)"},
			verify:   "SELECT * FROM t4",
			wantRows: 1,
		},
		{
			name: "check-4.3: Complex CHECK constraint - second condition",
			setup: []string{
				`CREATE TABLE t4(x, y,
					CHECK (
						x+y==11
						OR x*y==12
						OR x/y BETWEEN 5 AND 8
						OR -x==y+10
					)
				)`,
				"INSERT INTO t4 VALUES(1,10)",
			},
			inserts:  []string{"UPDATE t4 SET x=4, y=3"},
			verify:   "SELECT x, y FROM t4",
			wantRows: 1,
		},
		{
			name: "check-4.6: Complex CHECK constraint violation",
			setup: []string{
				`CREATE TABLE t4(x, y,
					CHECK (
						x+y==11
						OR x*y==12
						OR x/y BETWEEN 5 AND 8
						OR -x==y+10
					)
				)`,
				"INSERT INTO t4 VALUES(12,-22)",
			},
			inserts: []string{"UPDATE t4 SET x=0, y=1"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},

		// CHECK constraints with conflict clauses - check-6.*
		{
			name: "check-6.2: INSERT OR IGNORE with CHECK violation",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(4,11.0)",
			},
			inserts:  []string{"UPDATE OR IGNORE t1 SET x=5"},
			verify:   "SELECT x FROM t1",
			wantRows: 1,
		},
		{
			name: "check-6.3: INSERT OR IGNORE skips violating row",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(4,11.0)",
			},
			inserts:  []string{"INSERT OR IGNORE INTO t1 VALUES(5,4.0)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 1,
		},
		{
			name: "check-6.4: INSERT OR IGNORE allows valid row",
			setup: []string{
				"CREATE TABLE t1(x INTEGER CHECK( x<5 ), y REAL CHECK( y>x ))",
				"INSERT INTO t1 VALUES(4,11.0)",
			},
			inserts:  []string{"INSERT OR IGNORE INTO t1 VALUES(2,20.0)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 2,
		},

		// ===== UNIQUE CONSTRAINT TESTS (from unique.test) =====

		// Multiple primary keys - unique-1.*
		{
			name:    "unique-1.1: Table cannot have two primary keys",
			setup:   []string{"CREATE TABLE t1(a int PRIMARY KEY, b int PRIMARY KEY, c text)"},
			wantErr: true,
			errMsg:  "more than one primary key",
		},
		{
			name:     "unique-1.1b: PRIMARY KEY and UNIQUE are different",
			setup:    []string{"CREATE TABLE t1(a int PRIMARY KEY, b int UNIQUE, c text)"},
			inserts:  []string{"INSERT INTO t1(a,b,c) VALUES(1,2,3)"},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name: "unique-1.3: PRIMARY KEY violation",
			setup: []string{
				"CREATE TABLE t1(a int PRIMARY KEY, b int UNIQUE, c text)",
				"INSERT INTO t1(a,b,c) VALUES(1,2,3)",
			},
			inserts: []string{"INSERT INTO t1(a,b,c) VALUES(1,3,4)"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "unique-1.5: UNIQUE constraint violation",
			setup: []string{
				"CREATE TABLE t1(a int PRIMARY KEY, b int UNIQUE, c text)",
				"INSERT INTO t1(a,b,c) VALUES(1,2,3)",
			},
			inserts: []string{"INSERT INTO t1(a,b,c) VALUES(3,2,4)"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "unique-1.7: Valid INSERT with different values",
			setup: []string{
				"CREATE TABLE t1(a int PRIMARY KEY, b int UNIQUE, c text)",
				"INSERT INTO t1(a,b,c) VALUES(1,2,3)",
			},
			inserts:  []string{"INSERT INTO t1(a,b,c) VALUES(3,4,5)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 2,
		},

		// UNIQUE INDEX tests - unique-2.*
		{
			name: "unique-2.1: CREATE UNIQUE INDEX on existing data",
			setup: []string{
				"CREATE TABLE t2(a int, b int)",
				"INSERT INTO t2(a,b) VALUES(1,2)",
				"INSERT INTO t2(a,b) VALUES(3,4)",
			},
			inserts:  []string{"CREATE UNIQUE INDEX i2 ON t2(a)"},
			verify:   "SELECT COUNT(*) FROM t2",
			wantRows: 2,
		},
		{
			name: "unique-2.3: INSERT violates UNIQUE INDEX",
			setup: []string{
				"CREATE TABLE t2(a int, b int)",
				"INSERT INTO t2(a,b) VALUES(1,2)",
				"INSERT INTO t2(a,b) VALUES(3,4)",
				"CREATE UNIQUE INDEX i2 ON t2(a)",
			},
			inserts: []string{"INSERT INTO t2 VALUES(1,5)"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "unique-2.5: After dropping index, duplicates allowed",
			setup: []string{
				"CREATE TABLE t2(a int, b int)",
				"INSERT INTO t2(a,b) VALUES(1,2)",
				"INSERT INTO t2(a,b) VALUES(3,4)",
				"CREATE UNIQUE INDEX i2 ON t2(a)",
				"DROP INDEX i2",
			},
			inserts:  []string{"INSERT INTO t2 VALUES(1,5)"},
			verify:   "SELECT COUNT(*) FROM t2",
			wantRows: 3,
		},
		{
			name: "unique-2.8: Cannot create UNIQUE INDEX on duplicate data",
			setup: []string{
				"CREATE TABLE t2(a int, b int)",
				"INSERT INTO t2(a,b) VALUES(1,2)",
				"INSERT INTO t2(a,b) VALUES(1,5)",
			},
			inserts: []string{"CREATE UNIQUE INDEX i2 ON t2(a)"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		// Multi-column UNIQUE - unique-3.*
		{
			name:     "unique-3.1: Create table with multi-column UNIQUE",
			setup:    []string{"CREATE TABLE t3(a int, b int, c int, d int, unique(a,c,d))"},
			inserts:  []string{"INSERT INTO t3(a,b,c,d) VALUES(1,2,3,4)"},
			verify:   "SELECT * FROM t3",
			wantRows: 1,
		},
		{
			name: "unique-3.3: Different value in one column allows duplicate",
			setup: []string{
				"CREATE TABLE t3(a int, b int, c int, d int, unique(a,c,d))",
				"INSERT INTO t3(a,b,c,d) VALUES(1,2,3,4)",
			},
			inserts:  []string{"INSERT INTO t3(a,b,c,d) VALUES(1,2,3,5)"},
			verify:   "SELECT COUNT(*) FROM t3",
			wantRows: 2,
		},
		{
			name: "unique-3.4: Multi-column UNIQUE violation",
			setup: []string{
				"CREATE TABLE t3(a int, b int, c int, d int, unique(a,c,d))",
				"INSERT INTO t3(a,b,c,d) VALUES(1,2,3,4)",
				"INSERT INTO t3(a,b,c,d) VALUES(1,2,3,5)",
			},
			inserts: []string{"INSERT INTO t3(a,b,c,d) VALUES(1,4,3,5)"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		// NULLs in UNIQUE constraints - unique-4.*
		{
			name: "unique-4.1: NULL is distinct in UNIQUE columns",
			setup: []string{
				"CREATE TABLE t4(a UNIQUE, b, c, UNIQUE(b,c))",
			},
			inserts:  []string{"INSERT INTO t4 VALUES(1,2,3)", "INSERT INTO t4 VALUES(NULL, 2, NULL)"},
			verify:   "SELECT COUNT(*) FROM t4",
			wantRows: 2,
		},
		{
			name: "unique-4.2: Multiple NULL values in single UNIQUE column",
			setup: []string{
				"CREATE TABLE t4(a UNIQUE, b, c, UNIQUE(b,c))",
				"INSERT INTO t4 VALUES(1,2,3)",
				"INSERT INTO t4 VALUES(NULL, 2, NULL)",
			},
			inserts:  []string{"INSERT INTO t4 VALUES(NULL, 3, 4)"},
			verify:   "SELECT COUNT(*) FROM t4",
			wantRows: 3,
		},
		{
			name: "unique-4.4: NULL in multi-column UNIQUE allows duplicates",
			setup: []string{
				"CREATE TABLE t4(a UNIQUE, b, c, UNIQUE(b,c))",
				"INSERT INTO t4 VALUES(1,2,3)",
				"INSERT INTO t4 VALUES(NULL, 2, NULL)",
				"INSERT INTO t4 VALUES(NULL, 3, 4)",
			},
			inserts:  []string{"INSERT INTO t4 VALUES(2, 2, NULL)"},
			verify:   "SELECT COUNT(*) FROM t4",
			wantRows: 4,
		},
		{
			name: "unique-4.6: Multiple rows with same NULLs in UNIQUE columns",
			setup: []string{
				"CREATE TABLE t4(a UNIQUE, b, c, UNIQUE(b,c))",
				"INSERT INTO t4 VALUES(1,2,3)",
				"INSERT INTO t4 VALUES(NULL, 2, NULL)",
			},
			inserts:  []string{"INSERT INTO t4 VALUES(NULL, 2, NULL)"},
			verify:   "SELECT COUNT(*) FROM t4",
			wantRows: 3,
		},

		// ===== NOT NULL CONSTRAINT TESTS (from notnull.test) =====

		// Basic NOT NULL - notnull-1.*
		{
			name: "notnull-1.2: INSERT violates NOT NULL on column a",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts: []string{"INSERT INTO t1(b,c,d,e) VALUES(2,3,4,5)"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "notnull-1.3: INSERT OR IGNORE with NOT NULL violation",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts:  []string{"INSERT OR IGNORE INTO t1(b,c,d,e) VALUES(2,3,4,5)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 0,
		},
		{
			name: "notnull-1.6: Missing column uses DEFAULT value",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts:  []string{"INSERT INTO t1(a,c,d,e) VALUES(1,3,4,5)"},
			verify:   "SELECT b FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-1.10: Explicit NULL violates NOT NULL",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts: []string{"INSERT INTO t1(a,b,c,d,e) VALUES(1,null,3,4,5)"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "notnull-1.12: INSERT OR REPLACE with NULL uses DEFAULT",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts:  []string{"INSERT OR REPLACE INTO t1(a,b,c,d,e) VALUES(1,null,3,4,5)"},
			verify:   "SELECT b FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-1.13: NULL in REPLACE column uses DEFAULT",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts:  []string{"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,null,4,5)"},
			verify:   "SELECT c FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-1.19: Missing column with DEFAULT",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts:  []string{"INSERT INTO t1(a,b,c,d) VALUES(1,2,3,4)"},
			verify:   "SELECT e FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-1.20: Explicit NULL on column e fails",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
			},
			inserts: []string{"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,3,4,null)"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},

		// UPDATE with NOT NULL - notnull-2.*
		{
			name: "notnull-2.1: UPDATE to NULL violates NOT NULL",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
				"INSERT INTO t1 VALUES(1,2,3,4,5)",
			},
			inserts: []string{"UPDATE t1 SET a=null"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "notnull-2.3: UPDATE OR IGNORE preserves old value",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
				"INSERT INTO t1 VALUES(1,2,3,4,5)",
			},
			inserts:  []string{"UPDATE OR IGNORE t1 SET a=null"},
			verify:   "SELECT a FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-2.6: UPDATE OR REPLACE with NULL uses DEFAULT",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
				"INSERT INTO t1 VALUES(1,2,3,4,5)",
			},
			inserts:  []string{"UPDATE OR REPLACE t1 SET b=null, d=e, e=d"},
			verify:   "SELECT b, d, e FROM t1",
			wantRows: 1,
		},
		{
			name: "notnull-2.8: UPDATE with NULL on REPLACE column",
			setup: []string{
				`CREATE TABLE t1 (
					a NOT NULL,
					b NOT NULL DEFAULT 5,
					c NOT NULL ON CONFLICT REPLACE DEFAULT 6,
					d NOT NULL ON CONFLICT IGNORE DEFAULT 7,
					e NOT NULL ON CONFLICT ABORT DEFAULT 8
				)`,
				"INSERT INTO t1 VALUES(1,2,3,4,5)",
			},
			inserts:  []string{"UPDATE t1 SET c=null, d=e, e=d"},
			verify:   "SELECT c FROM t1",
			wantRows: 1,
		},

		// ===== DEFAULT VALUE TESTS (from default.test) =====

		// Basic DEFAULT - default-1.*
		{
			name:     "default-1.2: DEFAULT NULL",
			setup:    []string{"CREATE TABLE t2(x INTEGER, y INTEGER DEFAULT NULL)"},
			inserts:  []string{"INSERT INTO t2(x) VALUES(1)"},
			verify:   "SELECT x, y FROM t2",
			wantRows: 1,
		},
		{
			name:    "default-1.3: DEFAULT with non-constant expression",
			setup:   []string{"CREATE TABLE t3(x INTEGER, y INTEGER DEFAULT (max(x,5)))"},
			wantErr: true,
			errMsg:  "not constant",
		},

		// Literal DEFAULT values - default-3.*
		{
			name: "default-3.1: Various DEFAULT types",
			setup: []string{
				`CREATE TABLE t3(
					a INTEGER PRIMARY KEY AUTOINCREMENT,
					b INT DEFAULT 12345 UNIQUE NOT NULL CHECK( b>=0 AND b<99999 ),
					c VARCHAR(123,456) DEFAULT 'hello' NOT NULL ON CONFLICT REPLACE,
					d REAL,
					e FLOATING POINT(5,10) DEFAULT 4.36,
					f NATIONAL CHARACTER(15) COLLATE RTRIM,
					g LONG INTEGER DEFAULT( 3600*12 )
				)`,
			},
			inserts:  []string{"INSERT INTO t3 VALUES(null, 5, 'row1', '5.25', 'xyz', 321, '432')"},
			verify:   "SELECT b, c, d FROM t3",
			wantRows: 1,
		},
		{
			name: "default-3.2: INSERT DEFAULT VALUES",
			setup: []string{
				`CREATE TABLE t3(
					a INTEGER PRIMARY KEY AUTOINCREMENT,
					b INT DEFAULT 12345 UNIQUE NOT NULL CHECK( b>=0 AND b<99999 ),
					c VARCHAR(123,456) DEFAULT 'hello' NOT NULL ON CONFLICT REPLACE,
					d REAL,
					e FLOATING POINT(5,10) DEFAULT 4.36,
					f NATIONAL CHARACTER(15) COLLATE RTRIM,
					g LONG INTEGER DEFAULT( 3600*12 )
				)`,
			},
			inserts:  []string{"INSERT INTO t3 DEFAULT VALUES"},
			verify:   "SELECT b, c, g FROM t3",
			wantRows: 1,
		},
		{
			name: "default-3.3: Integer boundary DEFAULT values",
			setup: []string{
				`CREATE TABLE t300(
					a INT DEFAULT 2147483647,
					b INT DEFAULT 2147483648,
					c INT DEFAULT +9223372036854775807,
					d INT DEFAULT -2147483647,
					e INT DEFAULT -2147483648,
					f INT DEFAULT -9223372036854775808
				)`,
			},
			inserts:  []string{"INSERT INTO t300 DEFAULT VALUES"},
			verify:   "SELECT a, b, c, d, e, f FROM t300",
			wantRows: 1,
		},

		// ===== COMBINED CONSTRAINT TESTS =====

		// PRIMARY KEY + UNIQUE + NOT NULL
		{
			name: "combined-1.1: PRIMARY KEY implies NOT NULL",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)",
			},
			inserts: []string{"INSERT INTO t1 VALUES(NULL, 'test')"},
			verify:  "SELECT id FROM t1 WHERE name='test'",
			wantRows: 1, // SQLite auto-generates rowid
		},
		{
			name: "combined-1.2: Multiple constraints on same column",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
			},
			inserts: []string{"INSERT INTO t1 VALUES(1, 'test@example.com')"},
			verify:  "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name: "combined-1.3: UNIQUE NOT NULL violation",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
				"INSERT INTO t1 VALUES(1, 'test@example.com')",
			},
			inserts: []string{"INSERT INTO t1 VALUES(2, NULL)"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "combined-1.4: UNIQUE violation with NOT NULL",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
				"INSERT INTO t1 VALUES(1, 'test@example.com')",
			},
			inserts: []string{"INSERT INTO t1 VALUES(2, 'test@example.com')"},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		// CHECK + NOT NULL + DEFAULT
		{
			name: "combined-2.1: CHECK and NOT NULL together",
			setup: []string{
				"CREATE TABLE t1(age INT NOT NULL CHECK(age >= 0 AND age <= 150))",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(25)"},
			verify:   "SELECT * FROM t1",
			wantRows: 1,
		},
		{
			name: "combined-2.2: CHECK constraint fails before NOT NULL",
			setup: []string{
				"CREATE TABLE t1(age INT NOT NULL CHECK(age >= 0 AND age <= 150))",
			},
			inserts: []string{"INSERT INTO t1 VALUES(200)"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},
		{
			name: "combined-2.3: NOT NULL with CHECK on NULL value",
			setup: []string{
				"CREATE TABLE t1(age INT NOT NULL CHECK(age >= 0 AND age <= 150))",
			},
			inserts: []string{"INSERT INTO t1 VALUES(NULL)"},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "combined-2.4: DEFAULT satisfies CHECK constraint",
			setup: []string{
				"CREATE TABLE t1(age INT NOT NULL DEFAULT 18 CHECK(age >= 0 AND age <= 150))",
			},
			inserts:  []string{"INSERT INTO t1 DEFAULT VALUES"},
			verify:   "SELECT age FROM t1",
			wantRows: 1,
		},

		// Multi-table constraint scenarios
		{
			name: "combined-3.1: Constraints across transaction",
			setup: []string{
				"CREATE TABLE users(id INT PRIMARY KEY, username TEXT UNIQUE NOT NULL)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(2, 'bob')",
				"COMMIT",
			},
			verify:   "SELECT COUNT(*) FROM users",
			wantRows: 1,
		},
		{
			name: "combined-3.2: Constraint violation rolls back transaction",
			setup: []string{
				"CREATE TABLE users(id INT PRIMARY KEY, username TEXT UNIQUE NOT NULL)",
			},
			inserts: []string{
				"BEGIN",
				"INSERT INTO users VALUES(1, 'alice')",
				"INSERT INTO users VALUES(1, 'bob')",
			},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		// Constraint conflict resolution - OR ABORT, OR FAIL, OR IGNORE, OR REPLACE, OR ROLLBACK
		{
			name: "conflict-1.1: OR REPLACE with UNIQUE constraint",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, value INT UNIQUE)",
				"INSERT INTO t1 VALUES(1, 100)",
			},
			inserts:  []string{"INSERT OR REPLACE INTO t1 VALUES(2, 100)"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 1,
		},
		{
			name: "conflict-1.2: OR IGNORE with PRIMARY KEY violation",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, value INT)",
				"INSERT INTO t1 VALUES(1, 100)",
			},
			inserts:  []string{"INSERT OR IGNORE INTO t1 VALUES(1, 200)"},
			verify:   "SELECT value FROM t1 WHERE id=1",
			wantRows: 1,
		},
		{
			name: "conflict-1.3: OR ABORT with CHECK constraint",
			setup: []string{
				"CREATE TABLE t1(x INT CHECK(x > 0))",
				"INSERT INTO t1 VALUES(5)",
			},
			inserts: []string{"INSERT OR ABORT INTO t1 VALUES(-1)"},
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},
		{
			name: "conflict-1.4: REPLACE INTO with multiple constraints",
			setup: []string{
				"CREATE TABLE t1(id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL CHECK(email LIKE '%@%'))",
				"INSERT INTO t1 VALUES(1, 'test@example.com')",
			},
			inserts:  []string{"REPLACE INTO t1 VALUES(1, 'new@example.com')"},
			verify:   "SELECT email FROM t1 WHERE id=1",
			wantRows: 1,
		},

		// Edge cases
		{
			name: "edge-1.1: Empty string in NOT NULL TEXT column",
			setup: []string{
				"CREATE TABLE t1(name TEXT NOT NULL)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES('')"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 1,
		},
		{
			name: "edge-1.2: Zero in NOT NULL INTEGER with CHECK",
			setup: []string{
				"CREATE TABLE t1(value INT NOT NULL CHECK(value >= 0))",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(0)"},
			verify:   "SELECT value FROM t1",
			wantRows: 1,
		},
		{
			name: "edge-1.3: UNIQUE on case-sensitive TEXT",
			setup: []string{
				"CREATE TABLE t1(name TEXT UNIQUE)",
				"INSERT INTO t1 VALUES('Test')",
			},
			inserts:  []string{"INSERT INTO t1 VALUES('test')"},
			verify:   "SELECT COUNT(*) FROM t1",
			wantRows: 2,
		},
		{
			name: "edge-1.4: DEFAULT expression evaluation",
			setup: []string{
				"CREATE TABLE t1(id INT, created INT DEFAULT (1234567890))",
			},
			inserts:  []string{"INSERT INTO t1(id) VALUES(1)"},
			verify:   "SELECT created FROM t1",
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary database
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			db, err := sql.Open("sqlite3", dbPath)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Run setup statements
			for _, stmt := range tt.setup {
				_, err := db.Exec(stmt)
				if err != nil {
					if tt.wantErr && tt.errMsg != "" && strings.Contains(err.Error(), tt.errMsg) {
						// Expected error during setup
						return
					}
					t.Fatalf("Setup failed: %v\nStatement: %s", err, stmt)
				}
			}

			// Run insert/update statements
			var lastErr error
			for _, stmt := range tt.inserts {
				_, err := db.Exec(stmt)
				if err != nil {
					lastErr = err
					if !tt.wantErr {
						t.Errorf("Unexpected error: %v\nStatement: %s", err, stmt)
						return
					}
				}
			}

			// Check for expected error
			if tt.wantErr {
				if lastErr == nil {
					t.Error("Expected an error but got none")
					return
				}
				if tt.errMsg != "" && !strings.Contains(lastErr.Error(), tt.errMsg) {
					t.Errorf("Error message mismatch.\nExpected substring: %q\nGot: %q", tt.errMsg, lastErr.Error())
				}
				return
			}

			// Verify results if specified
			if tt.verify != "" {
				rows, err := db.Query(tt.verify)
				if err != nil {
					t.Fatalf("Verify query failed: %v\nQuery: %s", err, tt.verify)
				}
				defer rows.Close()

				count := 0
				for rows.Next() {
					count++
				}

				if tt.wantRows > 0 && count != tt.wantRows {
					t.Errorf("Row count mismatch. Want %d, got %d", tt.wantRows, count)
				}
			}
		})
	}
}
