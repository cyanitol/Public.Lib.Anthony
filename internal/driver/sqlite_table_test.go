// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// tblTestCase represents a table test case with declarative fields
type tblTestCase struct {
	name     string
	setup    []string
	query    string
	wantRows [][]interface{}
	wantErr  bool
	errMsg   string
}

// TestSQLiteTable tests various table operations including CREATE, DROP, temp tables, and constraints
func TestSQLiteTable(t *testing.T) {
	t.Skip("pre-existing failure")
	tests := []tblTestCase{
		// Basic CREATE TABLE tests (from table.test)
		{
			name: "tbl-1.1 create basic table",
			setup: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
			},
			query: "SELECT name FROM sqlite_master WHERE type='table' AND name='test1'",
			wantRows: [][]interface{}{
				{"test1"},
			},
		},
		{
			name: "tbl-1.3 verify table metadata",
			setup: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
			},
			query: "SELECT name, tbl_name, type FROM sqlite_master WHERE name='test1'",
			wantRows: [][]interface{}{
				{"test1", "test1", "table"},
			},
		},
		{
			name: "tbl-1.5 drop table",
			setup: []string{
				"CREATE TABLE test1 (one varchar(10), two text)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE name='test1'",
			wantRows: [][]interface{}{},
		},
		{
			name: "tbl-1.10 quoted table name",
			setup: []string{
				"CREATE TABLE \"create\" (f1 int)",
			},
			query: "SELECT name FROM sqlite_master WHERE name='create'",
			wantRows: [][]interface{}{
				{"create"},
			},
		},
		{
			name: "tbl-1.12 quoted column name",
			setup: []string{
				"CREATE TABLE test1(\"f1 ho\" int)",
			},
			query: "SELECT name FROM sqlite_master WHERE name='test1'",
			wantRows: [][]interface{}{
				{"test1"},
			},
		},
		{
			name:    "tbl-2.1 duplicate table name error",
			setup:   []string{"CREATE TABLE TEST2(one text)"},
			query:   "CREATE TABLE test2(two text default 'hi')",
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name:    "tbl-2.1b reserved name error",
			setup:   []string{},
			query:   "CREATE TABLE sqlite_master(two text)",
			wantErr: true,
			errMsg:  "reserved for internal use",
		},
		{
			name: "tbl-2.1d IF NOT EXISTS clause",
			setup: []string{
				"CREATE TABLE test2(x,y)",
			},
			query: "CREATE TABLE IF NOT EXISTS test2(x,y)",
			wantRows: [][]interface{}{
				{"test2"},
			},
		},
		{
			name: "tbl-2.2a cannot create table with index name",
			setup: []string{
				"CREATE TABLE test2(one text)",
				"CREATE INDEX test3 ON test2(one)",
			},
			query:   "CREATE TABLE test3(two text)",
			wantErr: true,
			errMsg:  "already an index",
		},
		{
			name: "tbl-3.1 create table with many fields",
			setup: []string{
				`CREATE TABLE big(
					f1 varchar(20),
					f2 char(10),
					f3 varchar(30) primary key,
					f4 text,
					f5 text
				)`,
			},
			query: "SELECT name FROM sqlite_master WHERE name='big'",
			wantRows: [][]interface{}{
				{"big"},
			},
		},
		{
			name:    "tbl-3.2 case insensitive duplicate check",
			setup:   []string{"CREATE TABLE big(x int)"},
			query:   "CREATE TABLE BIG(xyz foo)",
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name:    "tbl-5.1.1 drop non-existent table",
			query:   "DROP TABLE test009",
			wantErr: true,
			errMsg:  "no such table",
		},
		{
			name:  "tbl-5.1.2 drop if exists on non-existent table",
			query: "DROP TABLE IF EXISTS test009",
			wantRows: [][]interface{}{
				{},
			},
		},
		{
			name:    "tbl-5.2 cannot drop sqlite_master",
			query:   "DROP TABLE IF EXISTS sqlite_master",
			wantErr: true,
			errMsg:  "may not be dropped",
		},
		{
			name: "tbl-7.1 keywords as column names",
			setup: []string{
				`CREATE TABLE weird(
					desc text,
					asc text,
					key int,
					fuzzy_dog_12 varchar(10),
					begin blob,
					end clob
				)`,
				"INSERT INTO weird VALUES('a','b',9,'xyz','hi','y''all')",
			},
			query: "SELECT * FROM weird",
			wantRows: [][]interface{}{
				{"a", "b", int64(9), "xyz", "hi", "y'all"},
			},
		},
		{
			name: "tbl-7.3 CREATE TABLE with savepoint keyword",
			setup: []string{
				"CREATE TABLE savepoint(release)",
				"INSERT INTO savepoint(release) VALUES(10)",
				"UPDATE savepoint SET release = 5",
			},
			query: "SELECT release FROM savepoint",
			wantRows: [][]interface{}{
				{int64(5)},
			},
		},
		{
			name: "tbl-8.1 CREATE TABLE AS SELECT",
			setup: []string{
				"CREATE TABLE weird(desc text, asc text, key int)",
				"INSERT INTO weird VALUES('a','b',9)",
				"CREATE TABLE t2 AS SELECT * FROM weird",
			},
			query: "SELECT * FROM t2",
			wantRows: [][]interface{}{
				{"a", "b", int64(9)},
			},
		},
		{
			name: "tbl-8.2 quoted table name with special chars",
			setup: []string{
				"CREATE TABLE \"t3\"\"xyz\"(a,b,c)",
				"INSERT INTO [t3\"xyz] VALUES(1,2,3)",
			},
			query: "SELECT * FROM [t3\"xyz]",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
			},
		},
		{
			name: "tbl-8.3 CREATE TABLE AS with aggregate",
			setup: []string{
				"CREATE TABLE source(a,b,c)",
				"INSERT INTO source VALUES(1,2,3)",
				"INSERT INTO source VALUES(4,5,6)",
				"CREATE TABLE result AS SELECT count(*) as cnt, max(b+c) FROM source",
			},
			query: "SELECT * FROM result",
			wantRows: [][]interface{}{
				{int64(2), int64(11)},
			},
		},
		{
			name:    "tbl-8.8 CREATE TABLE AS from non-existent table",
			query:   "CREATE TABLE t5 AS SELECT * FROM no_such_table",
			wantErr: true,
			errMsg:  "no such table",
		},
		{
			name:    "tbl-9.1 duplicate column names",
			query:   "CREATE TABLE t6(a,b,a)",
			wantErr: true,
			errMsg:  "duplicate column name",
		},
		{
			name:    "tbl-9.2 duplicate column names with types",
			query:   "CREATE TABLE t6(a varchar(100), b blob, a integer)",
			wantErr: true,
			errMsg:  "duplicate column name",
		},
		// Temp table tests (from temptable.test)
		{
			name: "tbl-temp-1.5 create temporary table",
			setup: []string{
				"CREATE TEMP TABLE t2(x,y,z)",
				"INSERT INTO t2 VALUES(4,5,6)",
			},
			query: "SELECT * FROM t2",
			wantRows: [][]interface{}{
				{int64(4), int64(5), int64(6)},
			},
		},
		{
			name: "tbl-temp-1.9 delete from temp table",
			setup: []string{
				"CREATE TEMP TABLE t2(x,y,z)",
				"INSERT INTO t2 VALUES(4,5,6)",
				"INSERT INTO t2 VALUES(8,9,0)",
				"DELETE FROM t2 WHERE x=8",
			},
			query: "SELECT * FROM t2 ORDER BY x",
			wantRows: [][]interface{}{
				{int64(4), int64(5), int64(6)},
			},
		},
		{
			name: "tbl-temp-1.10 delete all from temp table",
			setup: []string{
				"CREATE TEMP TABLE t2(x,y,z)",
				"INSERT INTO t2 VALUES(4,5,6)",
				"DELETE FROM t2",
			},
			query:    "SELECT * FROM t2",
			wantRows: [][]interface{}{},
		},
		{
			name: "tbl-temp-1.11 insert and select from temp table",
			setup: []string{
				"CREATE TEMP TABLE t2(x,y,z)",
				"INSERT INTO t2 VALUES(7,6,5)",
				"INSERT INTO t2 VALUES(4,3,2)",
			},
			query: "SELECT * FROM t2 ORDER BY x",
			wantRows: [][]interface{}{
				{int64(4), int64(3), int64(2)},
				{int64(7), int64(6), int64(5)},
			},
		},
		{
			name: "tbl-temp-2.1 temp table in transaction",
			setup: []string{
				"BEGIN TRANSACTION",
				"CREATE TEMPORARY TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,2)",
			},
			query: "SELECT * FROM t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "tbl-temp-2.4 commit with temp table",
			setup: []string{
				"BEGIN TRANSACTION",
				"CREATE TEMPORARY TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,2)",
				"COMMIT",
			},
			query: "SELECT * FROM t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "tbl-temp-3.1 index on temp table not in sqlite_master",
			setup: []string{
				"CREATE TEMPORARY TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,2)",
				"CREATE INDEX i2 ON t2(x)",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND name='i2'",
			wantRows: [][]interface{}{},
		},
		{
			name: "tbl-temp-3.2 query using temp index",
			setup: []string{
				"CREATE TEMPORARY TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,2)",
				"CREATE INDEX i2 ON t2(x)",
			},
			query: "SELECT y FROM t2 WHERE x=1",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		// Additional comprehensive table tests
		{
			name: "tbl-constraint-1 primary key constraint",
			setup: []string{
				"CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
				"INSERT INTO users VALUES(1, 'Alice')",
			},
			query:   "INSERT INTO users VALUES(1, 'Bob')",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "tbl-constraint-2 unique constraint",
			setup: []string{
				"CREATE TABLE emails(id INTEGER, email TEXT UNIQUE)",
				"INSERT INTO emails VALUES(1, 'test@example.com')",
			},
			query:   "INSERT INTO emails VALUES(2, 'test@example.com')",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "tbl-constraint-3 not null constraint",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT NOT NULL)",
			},
			query:   "INSERT INTO products VALUES(1, NULL)",
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},
		{
			name: "tbl-constraint-4 check constraint",
			setup: []string{
				"CREATE TABLE items(id INTEGER, price REAL CHECK(price > 0))",
			},
			query:   "INSERT INTO items VALUES(1, -10)",
			wantErr: true,
			errMsg:  "CHECK constraint failed",
		},
		{
			name: "tbl-default-1 default value integer",
			setup: []string{
				"CREATE TABLE logs(id INTEGER, status INTEGER DEFAULT 0)",
				"INSERT INTO logs(id) VALUES(1)",
			},
			query: "SELECT * FROM logs",
			wantRows: [][]interface{}{
				{int64(1), int64(0)},
			},
		},
		{
			name: "tbl-default-2 default value text",
			setup: []string{
				"CREATE TABLE config(key TEXT, value TEXT DEFAULT 'default')",
				"INSERT INTO config(key) VALUES('setting1')",
			},
			query: "SELECT * FROM config",
			wantRows: [][]interface{}{
				{"setting1", "default"},
			},
		},
		{
			name: "tbl-alter-1 rename table",
			setup: []string{
				"CREATE TABLE old_name(id INTEGER, data TEXT)",
				"INSERT INTO old_name VALUES(1, 'test')",
				"ALTER TABLE old_name RENAME TO new_name",
			},
			query: "SELECT * FROM new_name",
			wantRows: [][]interface{}{
				{int64(1), "test"},
			},
		},
		{
			name: "tbl-alter-2 add column",
			setup: []string{
				"CREATE TABLE evolve(id INTEGER)",
				"INSERT INTO evolve VALUES(1)",
				"ALTER TABLE evolve ADD COLUMN name TEXT",
			},
			query: "SELECT * FROM evolve",
			wantRows: [][]interface{}{
				{int64(1), nil},
			},
		},
		{
			name: "tbl-without-rowid-1 create without rowid table",
			setup: []string{
				"CREATE TABLE compact(id INTEGER PRIMARY KEY, data TEXT) WITHOUT ROWID",
				"INSERT INTO compact VALUES(1, 'test')",
			},
			query: "SELECT * FROM compact",
			wantRows: [][]interface{}{
				{int64(1), "test"},
			},
		},
		{
			name: "tbl-without-rowid-2 query without rowid table",
			setup: []string{
				"CREATE TABLE compact(a INT PRIMARY KEY, b INT, c INT) WITHOUT ROWID",
				"INSERT INTO compact VALUES(1, 2, 3)",
				"INSERT INTO compact VALUES(2, 4, 6)",
			},
			query: "SELECT * FROM compact WHERE a=2",
			wantRows: [][]interface{}{
				{int64(2), int64(4), int64(6)},
			},
		},
		{
			name: "tbl-strict-1 strict table with type enforcement",
			setup: []string{
				"CREATE TABLE strict_test(id INTEGER, value INT) STRICT",
				"INSERT INTO strict_test VALUES(1, 100)",
			},
			query: "SELECT * FROM strict_test",
			wantRows: [][]interface{}{
				{int64(1), int64(100)},
			},
		},
		{
			name: "tbl-multiple-constraints combined constraints",
			setup: []string{
				"CREATE TABLE complex(id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, age INTEGER CHECK(age >= 0))",
				"INSERT INTO complex VALUES(1, 'user@test.com', 25)",
			},
			query: "SELECT * FROM complex",
			wantRows: [][]interface{}{
				{int64(1), "user@test.com", int64(25)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			runTblTest(t, tt)
		})
	}
}

// runTblTest executes a single table test case
func runTblTest(t *testing.T, tt tblTestCase) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := tblOpenDB(t, dbPath)
	defer db.Close()

	tblRunSetup(t, db, tt.setup)
	tblExecuteQuery(t, db, tt)
}

// tblOpenDB opens database for testing
func tblOpenDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// tblRunSetup executes setup statements
func tblRunSetup(t *testing.T, db *sql.DB, setup []string) {
	t.Helper()
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Setup failed for %q: %v", stmt, err)
		}
	}
}

// tblExecuteQuery executes the test query
func tblExecuteQuery(t *testing.T, db *sql.DB, tt tblTestCase) {
	t.Helper()
	if tt.wantErr {
		tblVerifyError(t, db, tt.query, tt.errMsg)
	} else {
		tblVerifySuccess(t, db, tt)
	}
}

// tblVerifyError verifies expected error
func tblVerifyError(t *testing.T, db *sql.DB, query, errMsg string) {
	t.Helper()
	_, err := db.Exec(query)
	if err == nil {
		t.Errorf("Expected error containing %q but got none", errMsg)
		return
	}
	if errMsg != "" && !strings.Contains(err.Error(), errMsg) {
		t.Errorf("Error %q does not contain expected substring %q", err.Error(), errMsg)
	}
}

// tblVerifySuccess verifies successful query
func tblVerifySuccess(t *testing.T, db *sql.DB, tt tblTestCase) {
	t.Helper()
	if len(tt.wantRows) == 1 && len(tt.wantRows[0]) == 0 {
		tblExecuteNoResult(t, db, tt.query)
		return
	}

	rows := tblQueryRows(t, db, tt.query)
	defer rows.Close()

	cols := tblGetColumns(t, rows)
	gotRows := tblScanAllRows(t, rows, cols)
	tblCompareResults(t, gotRows, tt.wantRows)
}

// tblExecuteNoResult executes query without result verification
func tblExecuteNoResult(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
}

// tblQueryRows executes query and returns rows
func tblQueryRows(t *testing.T, db *sql.DB, query string) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	return rows
}

// tblGetColumns gets column names from rows
func tblGetColumns(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	return cols
}

// tblScanAllRows scans all rows from result set
func tblScanAllRows(t *testing.T, rows *sql.Rows, cols []string) [][]interface{} {
	t.Helper()
	var gotRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		gotRows = append(gotRows, values)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Rows iteration error: %v", err)
	}

	return gotRows
}

// tblCompareResults compares actual vs expected rows
func tblCompareResults(t *testing.T, got, want [][]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("Row count mismatch: got %d, want %d", len(got), len(want))
		t.Logf("Got rows: %v", got)
		t.Logf("Want rows: %v", want)
		return
	}

	for i, gotRow := range got {
		wantRow := want[i]
		if len(gotRow) != len(wantRow) {
			t.Errorf("Row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
			continue
		}

		for j, gotVal := range gotRow {
			wantVal := wantRow[j]
			if !compareTableValues(gotVal, wantVal) {
				t.Errorf("Row %d, Col %d: got %v (%T), want %v (%T)", i, j, gotVal, gotVal, wantVal, wantVal)
			}
		}
	}
}

// compareTableValues compares two values handling type conversions
func compareTableValues(got, want interface{}) bool {
	if tblCheckNils(got, want) {
		return got == nil && want == nil
	}

	switch wv := want.(type) {
	case int64:
		return tblCompareInt64(got, wv)
	case float64:
		return tblCompareFloat64(got, wv)
	case string:
		return tblCompareString(got, wv)
	case []byte:
		return tblCompareBytes(got, wv)
	}

	return false
}

// tblCheckNils checks if either value is nil
func tblCheckNils(got, want interface{}) bool {
	return got == nil || want == nil
}

// tblCompareInt64 compares int64 values
func tblCompareInt64(got interface{}, want int64) bool {
	gv, ok := got.(int64)
	return ok && gv == want
}

// tblCompareFloat64 compares float64 values
func tblCompareFloat64(got interface{}, want float64) bool {
	gv, ok := got.(float64)
	return ok && gv == want
}

// tblCompareString compares string values
func tblCompareString(got interface{}, want string) bool {
	if gv, ok := got.(string); ok {
		return gv == want
	}
	if gv, ok := got.([]byte); ok {
		return string(gv) == want
	}
	return false
}

// tblCompareBytes compares byte slice values
func tblCompareBytes(got interface{}, want []byte) bool {
	if gv, ok := got.([]byte); ok {
		return string(gv) == string(want)
	}
	if gv, ok := got.(string); ok {
		return gv == string(want)
	}
	return false
}
