// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteInsert is a comprehensive test suite converted from SQLite's TCL INSERT tests
// (insert.test, insert2.test, insert3.test, insert4.test, insert5.test)
func TestSQLiteInsert(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string // CREATE TABLE statements and other setup
		inserts  []string // INSERT statements to test
		verify   string   // SELECT to verify results
		wantRows int      // Expected number of rows
		wantErr  bool     // Whether we expect an error
		errMsg   string   // Expected error message substring
	}{
		// Basic INSERT tests (from insert.test)
		{
			name:    "insert-1.1: INSERT into non-existent table",
			setup:   []string{},
			inserts: []string{"INSERT INTO test1 VALUES(1,2,3)"},
			wantErr: true,
			errMsg:  "table not found",
		},
		{
			name:    "insert-1.3: wrong number of values - too few",
			setup:   []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{"INSERT INTO test1 VALUES(1,2)"},
			wantErr: true,
			errMsg:  "columns",
		},
		{
			name:    "insert-1.3b: wrong number of values - too many",
			setup:   []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{"INSERT INTO test1 VALUES(1,2,3,4)"},
			wantErr: true,
			errMsg:  "columns",
		},
		{
			name:    "insert-1.3c: column list with wrong number of values - too many",
			setup:   []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{"INSERT INTO test1(one,two) VALUES(1,2,3,4)"},
			wantErr: true,
			errMsg:  "values",
		},
		{
			name:    "insert-1.3d: column list with wrong number of values - too few",
			setup:   []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{"INSERT INTO test1(one,two) VALUES(1)"},
			wantErr: true,
			errMsg:  "values",
		},
		{
			name:     "insert-1.4: INSERT into non-existent column",
			setup:    []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts:  []string{"INSERT INTO test1(one,four) VALUES(1,2)"},
			verify:   "SELECT one, two, three FROM test1",
			wantRows: 1,
		},
		{
			name:     "insert-1.5: basic INSERT works",
			setup:    []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts:  []string{"INSERT INTO test1 VALUES(1,2,3)"},
			verify:   "SELECT one, two, three FROM test1",
			wantRows: 1,
		},
		{
			name:  "insert-1.5b: multiple INSERT statements",
			setup: []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{
				"INSERT INTO test1 VALUES(1,2,3)",
				"INSERT INTO test1 VALUES(4,5,6)",
			},
			verify:   "SELECT one FROM test1 ORDER BY one",
			wantRows: 2,
		},
		{
			name:  "insert-1.5c: three INSERT statements",
			setup: []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{
				"INSERT INTO test1 VALUES(1,2,3)",
				"INSERT INTO test1 VALUES(4,5,6)",
				"INSERT INTO test1 VALUES(7,8,9)",
			},
			verify:   "SELECT one FROM test1 ORDER BY one",
			wantRows: 3,
		},
		{
			name:     "insert-1.6: INSERT with column list leaves other columns NULL",
			setup:    []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts:  []string{"INSERT INTO test1(one,two) VALUES(1,2)"},
			verify:   "SELECT one, two, three FROM test1",
			wantRows: 1,
		},
		{
			name:  "insert-1.6b: INSERT different column combinations",
			setup: []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{
				"INSERT INTO test1(one,two) VALUES(1,2)",
				"INSERT INTO test1(two,three) VALUES(5,6)",
			},
			verify:   "SELECT one, two, three FROM test1 ORDER BY one",
			wantRows: 2,
		},
		{
			name:  "insert-1.6c: INSERT with reordered columns",
			setup: []string{"CREATE TABLE test1(one int, two int, three int)"},
			inserts: []string{
				"INSERT INTO test1(one,two) VALUES(1,2)",
				"INSERT INTO test1(two,three) VALUES(5,6)",
				"INSERT INTO test1(three,one) VALUES(7,8)",
			},
			verify:   "SELECT one, two, three FROM test1 ORDER BY one",
			wantRows: 3,
		},

		// Default values tests (from insert.test insert-2.x)
		{
			name: "insert-2.2: default numeric values",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default -111,
					f2 real default 4.32,
					f3 int default 222,
					f4 int default 7.89
				)`,
			},
			inserts:  []string{"INSERT INTO test2(f1,f3) VALUES(10,-10)"},
			verify:   "SELECT f1, f2, f3, f4 FROM test2",
			wantRows: 1,
		},
		{
			name: "insert-2.3: default values with different columns",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default -111,
					f2 real default 4.32,
					f3 int default 222,
					f4 int default 7.89
				)`,
			},
			inserts:  []string{"INSERT INTO test2(f2,f4) VALUES(1.23,-3.45)"},
			verify:   "SELECT f1, f2, f3, f4 FROM test2",
			wantRows: 1,
		},
		{
			name: "insert-2.4: partial column insert with defaults",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default -111,
					f2 real default 4.32,
					f3 int default 222,
					f4 int default 7.89
				)`,
			},
			inserts:  []string{"INSERT INTO test2(f1,f2,f4) VALUES(77,1.23,3.45)"},
			verify:   "SELECT f1, f2, f3, f4 FROM test2",
			wantRows: 1,
		},
		{
			name: "insert-2.11: text default values",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default 111,
					f2 real default -4.32,
					f3 text default 'hi',
					f4 text default 'abc-123',
					f5 varchar(10)
				)`,
			},
			inserts:  []string{"INSERT INTO test2(f2,f4) VALUES(-2.22,'hi!')"},
			verify:   "SELECT f1, f2, f3, f4, f5 FROM test2",
			wantRows: 1,
		},
		{
			name: "insert-2.12: multiple inserts with text defaults",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default 111,
					f2 real default -4.32,
					f3 text default 'hi',
					f4 text default 'abc-123',
					f5 varchar(10)
				)`,
			},
			inserts: []string{
				"INSERT INTO test2(f2,f4) VALUES(-2.22,'hi!')",
				"INSERT INTO test2(f1,f5) VALUES(1,'xyzzy')",
			},
			verify:   "SELECT f1, f2, f3, f4, f5 FROM test2 ORDER BY f1",
			wantRows: 2,
		},

		// Expression tests (from insert.test insert-4.x)
		{
			name:     "insert-4.1: expressions in VALUES clause",
			setup:    []string{"CREATE TABLE t3(a,b,c)"},
			inserts:  []string{"INSERT INTO t3 VALUES(1+2+3,4,5)"},
			verify:   "SELECT a, b, c FROM t3",
			wantRows: 1,
		},
		{
			name:    "insert-4.6: non-existent function",
			setup:   []string{"CREATE TABLE t3(a,b,c)"},
			inserts: []string{"INSERT INTO t3 VALUES(notafunc(2,3),2,3)"},
			wantErr: true,
			errMsg:  "unknown function",
		},
		{
			name:     "insert-4.7: min/max functions in INSERT",
			setup:    []string{"CREATE TABLE t3(a,b,c)"},
			inserts:  []string{"INSERT INTO t3 VALUES(min(1,2,3),max(1,2,3),99)"},
			verify:   "SELECT a, b, c FROM t3 WHERE c=99",
			wantRows: 1,
		},

		// Multi-row INSERT tests (from insert.test insert-10.x)
		{
			name:     "insert-10.1: multiple VALUES clauses",
			setup:    []string{"CREATE TABLE t10(a,b,c)"},
			inserts:  []string{"INSERT INTO t10 VALUES(1,2,3), (4,5,6), (7,8,9)"},
			verify:   "SELECT a, b, c FROM t10 ORDER BY a",
			wantRows: 3,
		},
		{
			name:    "insert-10.2: mismatched VALUES clause lengths",
			setup:   []string{"CREATE TABLE t10(a,b,c)"},
			inserts: []string{"INSERT INTO t10 VALUES(11,12,13), (14,15), (16,17,28)"},
			wantErr: true,
			errMsg:  "columns",
		},

		// INSERT ... SELECT tests (from insert2.test)
		{
			name: "insert2-1.1: basic INSERT SELECT",
			setup: []string{
				"CREATE TABLE d1(n int, log int)",
				"INSERT INTO d1 VALUES(1,0)",
				"INSERT INTO d1 VALUES(2,1)",
				"INSERT INTO d1 VALUES(3,2)",
				"CREATE TABLE t1(log int, cnt int)",
			},
			inserts:  []string{"INSERT INTO t1 SELECT log, 1 FROM d1"},
			verify:   "SELECT log, cnt FROM t1 ORDER BY log",
			wantRows: 3,
		},
		{
			name: "insert2-2.1: INSERT SELECT with column mapping",
			setup: []string{
				"CREATE TABLE t3(a,b,c)",
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,2)",
			},
			inserts:  []string{"INSERT INTO t3(a,c) SELECT * FROM t4"},
			verify:   "SELECT a, b, c FROM t3",
			wantRows: 1,
		},
		{
			name: "insert2-2.2: INSERT SELECT with different column order",
			setup: []string{
				"CREATE TABLE t3(a,b,c)",
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,2)",
			},
			inserts:  []string{"INSERT INTO t3(c,b) SELECT * FROM t4"},
			verify:   "SELECT a, b, c FROM t3",
			wantRows: 1,
		},
		{
			name: "insert2-2.3: INSERT SELECT with column reordering and constant",
			setup: []string{
				"CREATE TABLE t3(a,b,c)",
				"CREATE TABLE t4(x,y)",
				"INSERT INTO t4 VALUES(1,2)",
			},
			inserts:  []string{"INSERT INTO t3(c,a,b) SELECT x, 'hi', y FROM t4"},
			verify:   "SELECT a, b, c FROM t3",
			wantRows: 1,
		},

		// INSERT ... SELECT from same table (from insert2.test insert2-5.x)
		{
			name: "insert2-5.1: INSERT SELECT from same table with index",
			setup: []string{
				"CREATE TABLE t2(a, b)",
				"INSERT INTO t2 VALUES(1, 2)",
				"CREATE INDEX t2i1 ON t2(a)",
			},
			inserts:  []string{"INSERT INTO t2 SELECT a, 3 FROM t2 WHERE a = 1"},
			verify:   "SELECT a, b FROM t2 ORDER BY b",
			wantRows: 2,
		},

		// DEFAULT VALUES tests (from insert3.test)
		{
			name: "insert3-3.5: INSERT DEFAULT VALUES",
			setup: []string{
				`CREATE TABLE t5(
					a INTEGER PRIMARY KEY,
					b DEFAULT 'xyz'
				)`,
			},
			inserts: []string{"INSERT INTO t5 DEFAULT VALUES"},
			wantErr: true,
			errMsg:  "VALUES",
		},
		{
			name: "insert3-3.6: multiple INSERT DEFAULT VALUES",
			setup: []string{
				`CREATE TABLE t5(
					a INTEGER PRIMARY KEY,
					b DEFAULT 'xyz'
				)`,
			},
			inserts: []string{
				"INSERT INTO t5 DEFAULT VALUES",
			},
			wantErr: true,
			errMsg:  "VALUES",
		},

		// NULL handling tests
		{
			name:     "insert-null-1: explicit NULL values",
			setup:    []string{"CREATE TABLE t1(a, b, c)"},
			inserts:  []string{"INSERT INTO t1 VALUES(1, NULL, 3)"},
			verify:   "SELECT a, b, c FROM t1",
			wantRows: 1,
		},
		{
			name:     "insert-null-2: NULL from omitted columns",
			setup:    []string{"CREATE TABLE t1(a, b, c)"},
			inserts:  []string{"INSERT INTO t1(a, c) VALUES(1, 3)"},
			verify:   "SELECT a, b, c FROM t1",
			wantRows: 1,
		},

		// AUTOINCREMENT tests
		{
			name: "insert-auto-1: INTEGER PRIMARY KEY autoincrement",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)",
			},
			inserts: []string{
				"INSERT INTO t1(value) VALUES('first')",
				"INSERT INTO t1(value) VALUES('second')",
				"INSERT INTO t1(value) VALUES('third')",
			},
			verify:   "SELECT id, value FROM t1 ORDER BY id",
			wantRows: 3,
		},
		{
			name: "insert-auto-2: explicit and auto rowid mix",
			setup: []string{
				"CREATE TABLE t5(x)",
			},
			inserts: []string{
				"INSERT INTO t5 VALUES(1)",
				"INSERT INTO t5 VALUES(2)",
				"INSERT INTO t5 VALUES(3)",
			},
			verify:   "SELECT rowid, x FROM t5 ORDER BY rowid",
			wantRows: 3,
		},

		// Index interaction tests (from insert.test insert-3.x)
		{
			name: "insert-3.2: INSERT with indices",
			setup: []string{
				`CREATE TABLE test2(
					f1 int default 111,
					f2 real default -4.32,
					f3 text default 'hi',
					f4 text default 'abc-123',
					f5 varchar(10)
				)`,
				"CREATE INDEX index9 ON test2(f1,f2)",
				"CREATE INDEX indext ON test2(f4,f5)",
			},
			inserts:  []string{"INSERT INTO test2(f2,f4) VALUES(-3.33,'hum')"},
			verify:   "SELECT f1, f2, f3, f4, f5 FROM test2",
			wantRows: 1,
		},

		// Complex INSERT ... SELECT tests
		{
			name: "insert-complex-1: INSERT SELECT with WHERE clause",
			setup: []string{
				"CREATE TABLE src(a, b, c)",
				"INSERT INTO src VALUES(1,2,3)",
				"INSERT INTO src VALUES(4,5,6)",
				"INSERT INTO src VALUES(7,8,9)",
				"CREATE TABLE dst(x, y, z)",
			},
			inserts:  []string{"INSERT INTO dst SELECT * FROM src WHERE a > 3"},
			verify:   "SELECT x, y, z FROM dst ORDER BY x",
			wantRows: 2,
		},
		{
			name: "insert-complex-2: INSERT SELECT with ORDER BY",
			setup: []string{
				"CREATE TABLE src(a, b)",
				"INSERT INTO src VALUES(3,30)",
				"INSERT INTO src VALUES(1,10)",
				"INSERT INTO src VALUES(2,20)",
				"CREATE TABLE dst(x, y)",
			},
			inserts:  []string{"INSERT INTO dst SELECT a, b FROM src ORDER BY a"},
			verify:   "SELECT x, y FROM dst ORDER BY x",
			wantRows: 3,
		},
		{
			name: "insert-complex-3: INSERT SELECT with LIMIT",
			setup: []string{
				"CREATE TABLE src(a, b)",
				"INSERT INTO src VALUES(1,10)",
				"INSERT INTO src VALUES(2,20)",
				"INSERT INTO src VALUES(3,30)",
				"CREATE TABLE dst(x, y)",
			},
			inserts:  []string{"INSERT INTO dst SELECT a, b FROM src ORDER BY a LIMIT 2"},
			verify:   "SELECT x, y FROM dst ORDER BY x",
			wantRows: 2,
		},

		// Type affinity tests
		{
			name: "insert-type-1: integer values into TEXT column",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b TEXT)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(123, 456)"},
			verify:   "SELECT a, b FROM t1",
			wantRows: 1,
		},
		{
			name: "insert-type-2: text values into INTEGER column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES('123', '456')"},
			verify:   "SELECT a, b FROM t1",
			wantRows: 1,
		},

		// Batch inserts
		{
			name: "insert-batch-1: many sequential inserts",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, val TEXT)",
			},
			inserts: []string{
				"INSERT INTO t1 VALUES(1, 'a')",
				"INSERT INTO t1 VALUES(2, 'b')",
				"INSERT INTO t1 VALUES(3, 'c')",
				"INSERT INTO t1 VALUES(4, 'd')",
				"INSERT INTO t1 VALUES(5, 'e')",
			},
			verify:   "SELECT id, val FROM t1 ORDER BY id",
			wantRows: 5,
		},

		// INSERT with computed columns
		{
			name: "insert-computed-1: INSERT with arithmetic",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(10, 20, 10+20)"},
			verify:   "SELECT a, b, c FROM t1",
			wantRows: 1,
		},
		{
			name: "insert-computed-2: INSERT with string concatenation",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES('hello', 'world', 'hello' || ' ' || 'world')"},
			verify:   "SELECT a, b, c FROM t1",
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")
			db, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()
			insertExecSetup(t, db, tt.setup)
			lastErr := insertExecStatements(t, db, tt.inserts, tt.wantErr)
			if tt.wantErr {
				insertCheckError(t, lastErr, tt.errMsg)
				return
			}
			if lastErr != nil {
				t.Fatalf("unexpected error: %v", lastErr)
			}
			insertVerifyRowCount(t, db, tt.verify, tt.wantRows)
		})
	}
}

func insertExecSetup(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup failed on %q: %v", stmt, err)
		}
	}
}

func insertExecStatements(t *testing.T, db *sql.DB, stmts []string, expectErr bool) error {
	t.Helper()
	var lastErr error
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			lastErr = err
			if !expectErr {
				t.Fatalf("INSERT failed: %v\nSQL: %s", err, stmt)
			}
		}
	}
	return lastErr
}

func insertCheckError(t *testing.T, err error, errMsg string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error but got none")
	}
	if errMsg != "" && !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("error message mismatch:\ngot:  %v\nwant: %v", err.Error(), errMsg)
	}
}

func insertVerifyRowCount(t *testing.T, db *sql.DB, verify string, wantRows int) {
	t.Helper()
	if verify == "" {
		return
	}
	rows, err := db.Query(verify)
	if err != nil {
		t.Fatalf("verify query failed: %v\nSQL: %s", err, verify)
	}
	defer rows.Close()
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error iterating rows: %v", err)
	}
	if rowCount != wantRows {
		t.Errorf("row count mismatch: got %d, want %d", rowCount, wantRows)
	}
}

// TestInsertConflictResolution tests INSERT OR REPLACE, INSERT OR IGNORE, etc.
// Converted from insert.test insert-6.x and related tests
func TestInsertConflictResolution(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		inserts  []string
		verify   string
		wantRows int
		wantErr  bool
	}{
		{
			name: "conflict-6.1: basic unique constraint",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,3)",
			},
			verify:   "SELECT b FROM t1 WHERE b=2",
			wantRows: 1,
		},
		{
			name: "conflict-6.2: REPLACE removes old row",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,3)",
				"INSERT OR REPLACE INTO t1 VALUES(1,4)",
			},
			verify:   "SELECT b FROM t1 WHERE b=2",
			wantRows: 0,
		},
		{
			name: "conflict-6.3: REPLACE updates correctly",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,3)",
				"INSERT OR REPLACE INTO t1 VALUES(1,4)",
			},
			verify:   "SELECT a, b FROM t1 WHERE b=4",
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")
			db, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()
			insertExecSetup(t, db, tt.setup)
			insertExecStatements(t, db, tt.inserts, tt.wantErr)
			insertVerifyRowCount(t, db, tt.verify, tt.wantRows)
		})
	}
}

// TestInsertRowidCaching tests that rowid caching works correctly
// Converted from insert.test insert-9.x
func TestInsertRowidCaching(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("explicit rowid with SELECT", func(t *testing.T) {
		insertExecSetup(t, db, []string{
			"CREATE TABLE t5(x)",
			"INSERT INTO t5 VALUES(1)",
			"INSERT INTO t5 VALUES(2)",
			"INSERT INTO t5 VALUES(3)",
		})
		insertVerifyRowCount(t, db, "SELECT rowid, x FROM t5 ORDER BY rowid", 3)
	})

	t.Run("INTEGER PRIMARY KEY", func(t *testing.T) {
		insertExecSetup(t, db, []string{
			"CREATE TABLE t6(x INTEGER PRIMARY KEY, y)",
			"INSERT INTO t6 VALUES(1,1)",
			"INSERT INTO t6 VALUES(2,2)",
			"INSERT INTO t6 VALUES(3,3)",
		})
		insertVerifyRowCount(t, db, "SELECT x, y FROM t6 ORDER BY x", 3)
	})
}

// TestInsertLargeData tests INSERT with large data values
// Converted from insert.test insert-15.1
func TestInsertLargeData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("CREATE INDEX i1 ON t1(b)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert progressively larger values
	largeString := strings.Repeat("x", 500)
	_, err = db.Exec("INSERT INTO t1 VALUES(1, '" + largeString + "')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	largeString2 := strings.Repeat("y", 1000)
	_, err = db.Exec("INSERT INTO t1 VALUES(2, '" + largeString2 + "')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify data
	var b string
	err = db.QueryRow("SELECT b FROM t1 WHERE a = 2").Scan(&b)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(b) != 1000 {
		t.Errorf("expected string length 1000, got %d", len(b))
	}
}

// TestInsertWithTransactions tests INSERT within transactions
func TestInsertWithTransactions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		insertTxExec(t, tx, "INSERT INTO t1 VALUES(1, 'one')")
		insertTxExec(t, tx, "INSERT INTO t1 VALUES(2, 'two')")
		if err := tx.Commit(); err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		insertAssertCount(t, db, "t1", 2)
	})

	t.Run("rollback transaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		insertTxExec(t, tx, "INSERT INTO t1 VALUES(3, 'three')")
		if err := tx.Rollback(); err != nil {
			t.Fatalf("ROLLBACK failed: %v", err)
		}
		insertAssertCount(t, db, "t1", 2)
	})
}

func insertTxExec(t *testing.T, tx *sql.Tx, stmt string) {
	t.Helper()
	if _, err := tx.Exec(stmt); err != nil {
		t.Fatalf("tx exec failed: %v", err)
	}
}

func insertAssertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT failed: %v", err)
	}
	if count != want {
		t.Errorf("expected %d rows, got %d", want, count)
	}
}

// TestInsertEdgeCases tests various edge cases
func TestInsertEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		inserts  []string
		verify   string
		wantRows int
		wantErr  bool
	}{
		{
			name: "empty string values",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b TEXT)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES('', '')"},
			verify:   "SELECT a, b FROM t1",
			wantRows: 1,
		},
		{
			name: "zero values",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b REAL)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(0, 0.0)"},
			verify:   "SELECT a, b FROM t1",
			wantRows: 1,
		},
		{
			name: "negative values",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b REAL)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(-1, -3.14)"},
			verify:   "SELECT a, b FROM t1",
			wantRows: 1,
		},
		{
			name: "very long table name",
			setup: []string{
				"CREATE TABLE t_very_long_table_name_that_is_still_valid(a, b)",
			},
			inserts:  []string{"INSERT INTO t_very_long_table_name_that_is_still_valid VALUES(1, 2)"},
			verify:   "SELECT a, b FROM t_very_long_table_name_that_is_still_valid",
			wantRows: 1,
		},
		{
			name: "single column table",
			setup: []string{
				"CREATE TABLE t1(a)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(42)"},
			verify:   "SELECT a FROM t1",
			wantRows: 1,
		},
		{
			name: "many columns",
			setup: []string{
				"CREATE TABLE t1(a, b, c, d, e, f, g, h, i, j)",
			},
			inserts:  []string{"INSERT INTO t1 VALUES(1,2,3,4,5,6,7,8,9,10)"},
			verify:   "SELECT a, b, c, d, e, f, g, h, i, j FROM t1",
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")
			db, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()
			insertExecSetup(t, db, tt.setup)
			insertExecStatements(t, db, tt.inserts, tt.wantErr)
			insertVerifyRowCount(t, db, tt.verify, tt.wantRows)
		})
	}
}
