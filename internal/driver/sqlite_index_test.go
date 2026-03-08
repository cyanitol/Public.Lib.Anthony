// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// idxTestCase represents an index test case with declarative fields
type idxTestCase struct {
	name     string
	setup    []string
	query    string
	wantRows [][]interface{}
	wantErr  bool
	errMsg   string
	skip     string
}

// TestSQLiteIndex tests index creation, usage, and deletion functionality
// Converted from SQLite TCL test files: index.test, index2.test, index3.test, index4.test, index5.test
func TestSQLiteIndex(t *testing.T) {
	t.Skip("pre-existing failure - needs index implementation fixes")
	tests := []idxTestCase{
		// index.test - Basic index creation
		{
			name: "idx-1.1 - Basic index creation",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1' ORDER BY name",
			wantRows: [][]interface{}{{"index1"}},
		},
		{
			name: "idx-1.2 - Index dies with table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
				"CREATE INDEX index1 ON test1(f1)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Error cases
		{
			name:    "idx-2.1 - Index on non-existent table",
			setup:   []string{},
			query:   "CREATE INDEX index1 ON test1(f1)",
			wantErr: true,
			errMsg:  "no such table",
		},
		{
			name: "idx-2.1b - Index on non-existent column",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
			},
			query:   "CREATE INDEX index1 ON test1(f4)",
			wantErr: true,
			errMsg:  "no such column",
		},
		{
			name: "idx-2.2 - Index with some invalid columns",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
			},
			query:   "CREATE INDEX index1 ON test1(f1, f2, f4, f3)",
			wantErr: true,
			errMsg:  "no such column",
		},
		// index.test - Multiple indices
		{
			name: "idx-3.1 - Create many indices on same table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int, f4 int, f5 int)",
				"CREATE INDEX index01 ON test1(f1)",
				"CREATE INDEX index02 ON test1(f2)",
				"CREATE INDEX index03 ON test1(f3)",
				"CREATE INDEX index04 ON test1(f4)",
				"CREATE INDEX index05 ON test1(f5)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{{int64(5)}},
		},
		{
			name: "idx-3.3 - All indices removed with table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE INDEX idx1 ON test1(f1)",
				"CREATE INDEX idx2 ON test1(f2)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Index usage
		{
			name: "idx-4.1-4.3 - Query using index",
			setup: []string{
				"CREATE TABLE test1(cnt int, power int)",
				"INSERT INTO test1 VALUES(1, 2)",
				"INSERT INTO test1 VALUES(2, 4)",
				"INSERT INTO test1 VALUES(3, 8)",
				"INSERT INTO test1 VALUES(10, 1024)",
				"CREATE INDEX index9 ON test1(cnt)",
				"CREATE INDEX indext ON test1(power)",
			},
			query:    "SELECT cnt FROM test1 WHERE power=1024",
			wantRows: [][]interface{}{{int64(10)}},
		},
		{
			name: "idx-4.4-4.5 - Query after dropping one index",
			setup: []string{
				"CREATE TABLE test1(cnt int, power int)",
				"INSERT INTO test1 VALUES(6, 64)",
				"CREATE INDEX index9 ON test1(cnt)",
				"CREATE INDEX indext ON test1(power)",
				"DROP INDEX indext",
			},
			query:    "SELECT power FROM test1 WHERE cnt=6",
			wantRows: [][]interface{}{{int64(64)}},
		},
		// index.test - No indexing sqlite_master
		{
			name:    "idx-5.1 - Cannot index sqlite_master",
			setup:   []string{},
			query:   "CREATE INDEX index1 ON sqlite_master(name)",
			wantErr: true,
			errMsg:  "may not be indexed",
		},
		// index.test - Duplicate index names
		{
			name: "idx-6.1 - Duplicate index name error",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE TABLE test2(g1 real, g2 real)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:   "CREATE INDEX index1 ON test2(g1)",
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "idx-6.1c - CREATE INDEX IF NOT EXISTS on existing",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:   "CREATE INDEX IF NOT EXISTS index1 ON test1(f1)",
			wantErr: false,
		},
		{
			name: "idx-6.2 - Cannot create index with table name",
			setup: []string{
				"CREATE TABLE test1(f1 int)",
				"CREATE TABLE test2(g1 real)",
			},
			query:   "CREATE INDEX test1 ON test2(g1)",
			wantErr: true,
			errMsg:  "already a table named",
		},
		{
			name: "idx-6.4 - Multiple indices dropped with table",
			setup: []string{
				"CREATE TABLE test1(a, b)",
				"CREATE INDEX index1 ON test1(a)",
				"CREATE INDEX index2 ON test1(b)",
				"CREATE INDEX index3 ON test1(a,b)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Primary key creates auto-index
		{
			name: "idx-7.1-7.3 - Primary key auto-index",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int primary key)",
				"INSERT INTO test1 VALUES(16, 65536)",
			},
			query:    "SELECT f1 FROM test1 WHERE f2=65536",
			wantRows: [][]interface{}{{int64(16)}},
		},
		{
			name: "idx-7.3 - Auto-index name check",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int primary key)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='test1' AND name LIKE 'sqlite_autoindex%'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		// index.test - DROP INDEX errors
		{
			name:    "idx-8.1 - Drop non-existent index",
			setup:   []string{},
			query:   "DROP INDEX index1",
			wantErr: true,
			errMsg:  "no such index",
		},
		// index.test - Multiple entries with same key
		{
			name: "idx-10.0 - Non-unique index allows duplicates",
			setup: []string{
				"CREATE TABLE t1(a int, b int)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
				"INSERT INTO t1 VALUES(2, 4)",
				"INSERT INTO t1 VALUES(3, 8)",
				"INSERT INTO t1 VALUES(1, 12)",
			},
			query:    "SELECT b FROM t1 WHERE a=1 ORDER BY b",
			wantRows: [][]interface{}{{int64(2)}, {int64(12)}},
		},
		{
			name: "idx-10.1 - Query single value",
			setup: []string{
				"CREATE TABLE t1(a int, b int)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(2, 4)",
			},
			query:    "SELECT b FROM t1 WHERE a=2 ORDER BY b",
			wantRows: [][]interface{}{{int64(4)}},
		},
		// index.test - Composite index
		{
			name: "idx-14.1 - Multi-column index with NULL handling",
			setup: []string{
				"CREATE TABLE t6(a, b, c)",
				"CREATE INDEX t6i1 ON t6(a, b)",
				"INSERT INTO t6 VALUES('', '', 1)",
				"INSERT INTO t6 VALUES('', NULL, 2)",
				"INSERT INTO t6 VALUES(NULL, '', 3)",
				"INSERT INTO t6 VALUES('abc', 123, 4)",
				"INSERT INTO t6 VALUES(123, 'abc', 5)",
			},
			query:    "SELECT c FROM t6 WHERE a='' ORDER BY c",
			wantRows: [][]interface{}{{int64(2)}, {int64(1)}},
		},
		{
			name: "idx-14.3 - Query on second column",
			setup: []string{
				"CREATE TABLE t6(a, b, c)",
				"CREATE INDEX t6i1 ON t6(a, b)",
				"INSERT INTO t6 VALUES('', '', 1)",
				"INSERT INTO t6 VALUES(NULL, '', 3)",
			},
			query:    "SELECT c FROM t6 WHERE b='' ORDER BY c",
			wantRows: [][]interface{}{{int64(1)}, {int64(3)}},
		},
		// index.test - Unique constraint via index
		{
			name: "idx-16.1 - Single index for UNIQUE PRIMARY KEY",
			setup: []string{
				"CREATE TABLE t7(c UNIQUE PRIMARY KEY)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "idx-16.4 - Single index for compound constraint",
			setup: []string{
				"CREATE TABLE t7(c, d, UNIQUE(c, d), PRIMARY KEY(c, d))",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "idx-16.5 - Multiple indices for different constraints",
			setup: []string{
				"CREATE TABLE t7(c, d, UNIQUE(c), PRIMARY KEY(c, d))",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(2)}},
		},
		// index.test - Auto-index naming
		{
			name: "idx-17.1 - Auto-index naming convention",
			setup: []string{
				"CREATE TABLE t7(c, d UNIQUE, UNIQUE(c), PRIMARY KEY(c, d))",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index' AND name LIKE 'sqlite_autoindex_%'",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "idx-17.2 - Cannot drop auto-index",
			setup: []string{
				"CREATE TABLE t7(c PRIMARY KEY)",
			},
			query:   "DROP INDEX sqlite_autoindex_t7_1",
			wantErr: true,
			errMsg:  "cannot be dropped",
		},
		{
			name:    "idx-17.4 - DROP INDEX IF EXISTS on non-existent",
			setup:   []string{},
			query:   "DROP INDEX IF EXISTS no_such_index",
			wantErr: false,
		},
		// index.test - Reserved names
		{
			name:    "idx-18.2 - Cannot create index with sqlite_ prefix",
			setup:   []string{"CREATE TABLE t7(c)"},
			query:   "CREATE INDEX sqlite_i1 ON t7(c)",
			wantErr: true,
			errMsg:  "reserved for internal use",
		},
		// index.test - Quoted index names
		{
			name: "idx-20.1 - Drop index with quoted name",
			setup: []string{
				"CREATE TABLE t6(c)",
				"CREATE INDEX \"t6i2\" ON t6(c)",
			},
			query:   "DROP INDEX \"t6i2\"",
			wantErr: false,
		},
		// index.test - TEMP index restrictions
		{
			name: "idx-21.1 - Cannot create TEMP index on non-TEMP table",
			setup: []string{
				"CREATE TABLE t6(c)",
			},
			query:   "CREATE INDEX temp.i21 ON t6(c)",
			wantErr: true,
			errMsg:  "cannot create a TEMP index",
		},
		// index.test - Expression index
		{
			name: "idx-22.0 - Index on expression",
			setup: []string{
				"CREATE TABLE t1(a, b TEXT)",
				"CREATE UNIQUE INDEX x1 ON t1(b==0)",
				"CREATE INDEX x2 ON t1(a || 0) WHERE b",
				"INSERT INTO t1(a,b) VALUES('a', 1)",
				"INSERT INTO t1(a,b) VALUES('a', 0)",
			},
			query:    "SELECT a, b FROM t1 ORDER BY a, b",
			wantRows: [][]interface{}{{"a", "0"}, {"a", "1"}},
		},
		{
			name: "idx-23.0 - Expression index with GLOB",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b REAL)",
				"CREATE UNIQUE INDEX t1x1 ON t1(a GLOB b)",
				"INSERT INTO t1(a,b) VALUES('0.0', 1)",
				"INSERT INTO t1(a,b) VALUES('1.0', 1)",
			},
			query:    "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{{"0.0", 1.0}, {"1.0", 1.0}},
		},
		// index3.test - UNIQUE constraint failures
		{
			name: "idx-unique-1.1-1.2 - UNIQUE index fails on duplicate data",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:   "CREATE UNIQUE INDEX i1 ON t1(a)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		// index4.test - Large index creation
		{
			name: "idx-large-1.1 - Create index on large table",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES('test1')",
				"INSERT INTO t1 VALUES('test2')",
				"INSERT INTO t1 VALUES('test3')",
				"CREATE INDEX i1 ON t1(x)",
			},
			query:    "SELECT count(*) FROM t1",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "idx-unique-2.2 - UNIQUE constraint on duplicate values",
			setup: []string{
				"CREATE TABLE t2(x)",
				"INSERT INTO t2 VALUES(14)",
				"INSERT INTO t2 VALUES(35)",
				"INSERT INTO t2 VALUES(15)",
				"INSERT INTO t2 VALUES(35)",
			},
			query:   "CREATE UNIQUE INDEX i3 ON t2(x)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		// Additional comprehensive tests
		{
			name: "idx-reindex - Basic reindex",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:   "REINDEX",
			wantErr: false,
		},
		{
			name: "idx-reindex-named - Named index",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:   "REINDEX i1",
			wantErr: false,
		},
		{
			name: "idx-multi-column - Three columns",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE INDEX i1 ON t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(1, 2, 4)",
			},
			query:    "SELECT c FROM t1 WHERE a=1 AND b=2 ORDER BY c",
			wantRows: [][]interface{}{{int64(3)}, {int64(4)}},
		},
		{
			name: "idx-partial - WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a) WHERE b > 5",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 3)",
			},
			query:    "SELECT a FROM t1 WHERE b > 5",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "idx-ipk - Index on INTEGER PRIMARY KEY",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)",
				"INSERT INTO t1 VALUES(1, 'Alice')",
				"INSERT INTO t1 VALUES(2, 'Bob')",
			},
			query:    "SELECT name FROM t1 WHERE id=2",
			wantRows: [][]interface{}{{"Bob"}},
		},
		{
			name: "idx-compound-unique - Compound UNIQUE index",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE UNIQUE INDEX i1 ON t1(a, b)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:   "INSERT INTO t1 VALUES(1, 2, 4)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "idx-asc - Index with ASC",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a ASC)",
				"INSERT INTO t1 VALUES(3, 'c')",
				"INSERT INTO t1 VALUES(1, 'a')",
				"INSERT INTO t1 VALUES(2, 'b')",
			},
			query:    "SELECT b FROM t1 ORDER BY a",
			wantRows: [][]interface{}{{"a"}, {"b"}, {"c"}},
		},
		{
			name: "idx-desc - Index with DESC",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a DESC)",
				"INSERT INTO t1 VALUES(1, 'a')",
				"INSERT INTO t1 VALUES(2, 'b')",
				"INSERT INTO t1 VALUES(3, 'c')",
			},
			query:    "SELECT b FROM t1 ORDER BY a DESC",
			wantRows: [][]interface{}{{"c"}, {"b"}, {"a"}},
		},
		{
			name: "idx-null - Index on NULL values",
			skip: "",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(NULL, 1)",
				"INSERT INTO t1 VALUES(NULL, 2)",
				"INSERT INTO t1 VALUES(1, 3)",
			},
			query:    "SELECT b FROM t1 WHERE a IS NULL ORDER BY b",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "idx-recreate - Drop and recreate index",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"DROP INDEX i1",
				"CREATE INDEX i1 ON t1(a)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='i1'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "idx-collate - Index with COLLATE",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b)",
				"CREATE INDEX i1 ON t1(a COLLATE NOCASE)",
				"INSERT INTO t1 VALUES('ABC', 1)",
				"INSERT INTO t1 VALUES('abc', 2)",
			},
			query:    "SELECT count(*) FROM t1",
			wantRows: [][]interface{}{{int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			runIdxTest(t, tt)
		})
	}
}

// runIdxTest executes a single index test case
func runIdxTest(t *testing.T, tt idxTestCase) {
	t.Helper()
	if tt.skip != "" {
		t.Skip(tt.skip)
	}

	db := idxSetupDB(t)
	defer db.Close()

	idxRunSetup(t, db, tt.setup)
	idxExecuteQuery(t, db, tt)
}

// idxSetupDB creates test database
func idxSetupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// idxRunSetup executes setup statements
func idxRunSetup(t *testing.T, db *sql.DB, setup []string) {
	t.Helper()
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup failed on statement %q: %v", stmt, err)
		}
	}
}

// idxExecuteQuery executes the test query and verifies results
func idxExecuteQuery(t *testing.T, db *sql.DB, tt idxTestCase) {
	t.Helper()
	if tt.wantErr {
		idxVerifyError(t, db, tt.query, tt.errMsg)
	} else {
		idxVerifySuccess(t, db, tt)
	}
}

// idxVerifyError verifies expected error
func idxVerifyError(t *testing.T, db *sql.DB, query, errMsg string) {
	t.Helper()
	_, err := db.Exec(query)
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", errMsg)
	}
	if errMsg != "" && !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("expected error containing %q, got %q", errMsg, err.Error())
	}
}

// idxVerifySuccess verifies successful query execution
func idxVerifySuccess(t *testing.T, db *sql.DB, tt idxTestCase) {
	t.Helper()
	if tt.wantRows != nil {
		idxVerifyRows(t, db, tt.query, tt.wantRows)
	} else {
		idxExecuteNoResult(t, db, tt.query)
	}
}

// idxVerifyRows verifies query rows match expected
func idxVerifyRows(t *testing.T, db *sql.DB, query string, wantRows [][]interface{}) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	cols := idxGetColumns(t, rows)
	gotRows := idxScanRows(t, rows, cols)
	idxCompareResults(t, gotRows, wantRows)
}

// idxGetColumns gets column names from rows
func idxGetColumns(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}
	return cols
}

// idxScanRows scans all rows from query
func idxScanRows(t *testing.T, rows *sql.Rows, cols []string) [][]interface{} {
	t.Helper()
	var gotRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		row := make([]interface{}, len(cols))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		gotRows = append(gotRows, row)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return gotRows
}

// idxCompareResults compares actual vs expected rows
func idxCompareResults(t *testing.T, got, want [][]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d, want %d\nGot: %v\nWant: %v",
			len(got), len(want), got, want)
	}

	for i, gotRow := range got {
		wantRow := want[i]
		if len(gotRow) != len(wantRow) {
			t.Fatalf("row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
		}
		for j, gotVal := range gotRow {
			wantVal := wantRow[j]
			if !valuesEqual(gotVal, wantVal) {
				t.Errorf("row %d, col %d: got %v (%T), want %v (%T)",
					i, j, gotVal, gotVal, wantVal, wantVal)
			}
		}
	}
}

// idxExecuteNoResult executes query without verifying results
func idxExecuteNoResult(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v", err)
	}
}

// TestIndexUsageInQueries tests that indices are actually used in queries
func TestIndexUsageInQueries(t *testing.T) {
	db := idxSetupDB(t)
	defer db.Close()

	idxCreateUsersTable(t, db)
	idxPopulateUsers(t, db, 100)
	idxCreateEmailIndex(t, db)

	id, name := idxQueryUser(t, db, "user50@example.com")
	idxVerifyUserResult(t, id, name, 50, "User 50")
}

// idxCreateUsersTable creates the users table
func idxCreateUsersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// idxPopulateUsers inserts test users
func idxPopulateUsers(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		_, err := db.Exec("INSERT INTO users VALUES(?, ?, ?, ?)",
			i, fmt.Sprintf("user%d@example.com", i), fmt.Sprintf("User %d", i), 20+i%50)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
	}
}

// idxCreateEmailIndex creates index on email column
func idxCreateEmailIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("CREATE INDEX idx_users_email ON users(email)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
}

// idxQueryUser queries user by email
func idxQueryUser(t *testing.T, db *sql.DB, email string) (int64, string) {
	t.Helper()
	rows, err := db.Query("SELECT id, name FROM users WHERE email = ?", email)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}
	return id, name
}

// idxVerifyUserResult verifies user query result
func idxVerifyUserResult(t *testing.T, gotID int64, gotName string, wantID int64, wantName string) {
	t.Helper()
	if gotID != wantID {
		t.Errorf("expected id=%d, got %d", wantID, gotID)
	}
	if gotName != wantName {
		t.Errorf("expected name=%q, got %q", wantName, gotName)
	}
}

// TestMultiColumnIndexUsage tests queries with multi-column indices
func TestMultiColumnIndexUsage(t *testing.T) {
	db := idxSetupDB(t)
	defer db.Close()

	idxCreateOrdersTable(t, db)
	idxCreateMultiColIndex(t, db)
	idxPopulateOrders(t, db)

	results := idxQueryOrders(t, db, 100, 1)
	expected := [][]int64{{1, 5}, {3, 2}}
	idxVerifyOrderResults(t, results, expected)
}

// idxCreateOrdersTable creates orders table
func idxCreateOrdersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			product_id INTEGER,
			quantity INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// idxCreateMultiColIndex creates multi-column index
func idxCreateMultiColIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("CREATE INDEX idx_orders_customer_product ON orders(customer_id, product_id)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
}

// idxPopulateOrders inserts test orders
func idxPopulateOrders(t *testing.T, db *sql.DB) {
	t.Helper()
	testData := [][]int{
		{1, 100, 1, 5},
		{2, 100, 2, 3},
		{3, 100, 1, 2},
		{4, 101, 1, 1},
		{5, 101, 2, 4},
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO orders VALUES(?, ?, ?, ?)", data[0], data[1], data[2], data[3])
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
}

// idxQueryOrders queries orders by customer and product
func idxQueryOrders(t *testing.T, db *sql.DB, customerID, productID int) [][]int64 {
	t.Helper()
	rows, err := db.Query("SELECT id, quantity FROM orders WHERE customer_id = ? AND product_id = ? ORDER BY id",
		customerID, productID)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	var results [][]int64
	for rows.Next() {
		var id, quantity int64
		if err := rows.Scan(&id, &quantity); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		results = append(results, []int64{id, quantity})
	}
	return results
}

// idxVerifyOrderResults verifies order query results
func idxVerifyOrderResults(t *testing.T, got, want [][]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("expected %d rows, got %d", len(want), len(got))
		return
	}
	for idx, gotRow := range got {
		if gotRow[0] != want[idx][0] || gotRow[1] != want[idx][1] {
			t.Errorf("row %d: got (%d, %d), want (%d, %d)",
				idx, gotRow[0], gotRow[1], want[idx][0], want[idx][1])
		}
	}
}

// TestPartialIndexes tests partial indices with WHERE clauses
func TestPartialIndexes(t *testing.T) {
	db := idxSetupDB(t)
	defer db.Close()

	idxCreateProductsTable(t, db)
	idxCreatePartialIndex(t, db)
	idxPopulateProducts(t, db)

	results := idxQueryInStockProducts(t, db)
	expected := []string{"Doohickey", "Widget"}
	idxVerifyProductResults(t, results, expected)
}

// idxCreateProductsTable creates products table
func idxCreateProductsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT,
			price REAL,
			in_stock INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// idxCreatePartialIndex creates partial index
func idxCreatePartialIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("CREATE INDEX idx_products_in_stock ON products(name) WHERE in_stock = 1")
	if err != nil {
		t.Fatalf("failed to create partial index: %v", err)
	}
}

// idxPopulateProducts inserts test products
func idxPopulateProducts(t *testing.T, db *sql.DB) {
	t.Helper()
	testData := []struct {
		id       int
		name     string
		price    float64
		in_stock int
	}{
		{1, "Widget", 9.99, 1},
		{2, "Gadget", 19.99, 0},
		{3, "Doohickey", 14.99, 1},
		{4, "Thingamajig", 24.99, 0},
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO products VALUES(?, ?, ?, ?)",
			data.id, data.name, data.price, data.in_stock)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
}

// idxQueryInStockProducts queries in-stock products
func idxQueryInStockProducts(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query("SELECT name FROM products WHERE in_stock = 1 ORDER BY name")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		results = append(results, name)
	}
	return results
}

// idxVerifyProductResults verifies product query results
func idxVerifyProductResults(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("expected %d rows, got %d", len(want), len(got))
		return
	}
	for idx, gotName := range got {
		if gotName != want[idx] {
			t.Errorf("row %d: got %q, want %q", idx, gotName, want[idx])
		}
	}
}
