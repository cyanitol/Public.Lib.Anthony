// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// setupMemoryDB creates a new in-memory test database
func setupMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// setupDiskDB creates a new file-based test database
func setupDiskDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create initial database file
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0600); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db
}

// execSQL executes SQL statements and fails on error
func execSQL(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q failed: %v", stmt, err)
		}
	}
}

// queryRows executes query and returns all rows as [][]interface{}
func queryRows(t *testing.T, db *sql.DB, query string, args ...interface{}) [][]interface{} {
	t.Helper()
	rows := mustOpenRows(t, db, query, args...)
	defer rows.Close()

	result := scanAllRows(t, rows)

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return result
}

// mustOpenRows opens query rows or fails test
func mustOpenRows(t *testing.T, db *sql.DB, query string, args ...interface{}) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	return rows
}

// mustGetColumns gets column names or fails test
func mustGetColumns(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}
	return cols
}

// scanAllRows scans all rows from query result
func scanAllRows(t *testing.T, rows *sql.Rows) [][]interface{} {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		row := scanSingleRow(t, rows, len(cols))
		result = append(result, row)
	}
	return result
}

// scanSingleRow scans a single row and converts byte slices to strings
func scanSingleRow(t *testing.T, rows *sql.Rows, numCols int) []interface{} {
	t.Helper()
	values := make([]interface{}, numCols)
	valuePtrs := make([]interface{}, numCols)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		t.Fatalf("failed to scan row: %v", err)
	}

	return convertBytesToStrings(values)
}

// convertBytesToStrings converts []byte values to strings in a row
func convertBytesToStrings(values []interface{}) []interface{} {
	row := make([]interface{}, len(values))
	for i, v := range values {
		if b, ok := v.([]byte); ok {
			row[i] = string(b)
		} else {
			row[i] = v
		}
	}
	return row
}

// queryRow executes a query that returns a single row
func queryRow(t *testing.T, db *sql.DB, query string, args ...interface{}) []interface{} {
	t.Helper()
	rows := queryRows(t, db, query, args...)
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > 1 {
		t.Fatalf("queryRow expected 1 row, got %d", len(rows))
	}
	return rows[0]
}

// querySingle executes a query that returns a single value
func querySingle(t *testing.T, db *sql.DB, query string, args ...interface{}) interface{} {
	t.Helper()
	row := queryRow(t, db, query, args...)
	if row == nil || len(row) == 0 {
		return nil
	}
	return row[0]
}

// assertRowCount verifies expected row count
func assertRowCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var count int
	// nosec: table name is a test constant, not user input
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != want {
		t.Errorf("row count mismatch for %s: got %d, want %d", table, count, want)
	}
}

// compareRows compares actual vs expected rows
func compareRows(t *testing.T, got, want [][]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d rows, want %d rows\ngot:  %v\nwant: %v",
			len(got), len(want), got, want)
	}

	for i := range got {
		if len(got[i]) != len(want[i]) {
			t.Errorf("row %d column count mismatch: got %d, want %d", i, len(got[i]), len(want[i]))
			continue
		}

		for j := range got[i] {
			if !valuesEqual(got[i][j], want[i][j]) {
				t.Errorf("row %d col %d mismatch: got %v (%T), want %v (%T)",
					i, j, got[i][j], got[i][j], want[i][j], want[i][j])
			}
		}
	}
}

// compareRowsOrdered compares rows without caring about order
func compareRowsUnordered(t *testing.T, got, want [][]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d rows, want %d rows", len(got), len(want))
	}

	matched := make([]bool, len(want))
	matchGotRows(t, got, want, matched)
	reportMissingRows(t, want, matched)
}

// matchGotRows matches each got row against want rows
func matchGotRows(t *testing.T, got, want [][]interface{}, matched []bool) {
	t.Helper()
	for _, gotRow := range got {
		if !findMatchingRow(gotRow, want, matched) {
			t.Errorf("unexpected row: %v", gotRow)
		}
	}
}

// findMatchingRow finds a matching want row for the got row
func findMatchingRow(gotRow []interface{}, want [][]interface{}, matched []bool) bool {
	for i, wantRow := range want {
		if matched[i] {
			continue
		}
		if rowsEqual(gotRow, wantRow) {
			matched[i] = true
			return true
		}
	}
	return false
}

// reportMissingRows reports any want rows that weren't matched
func reportMissingRows(t *testing.T, want [][]interface{}, matched []bool) {
	t.Helper()
	for i, m := range matched {
		if !m {
			t.Errorf("missing expected row: %v", want[i])
		}
	}
}

// rowsEqual compares two rows for equality
func rowsEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// valuesEqual compares two values for equality, handling type conversions
func valuesEqual(a, b interface{}) bool {
	if bothNil := a == nil && b == nil; bothNil {
		return true
	}
	if eitherNil := a == nil || b == nil; eitherNil {
		return false
	}

	if equal, ok := tryTypeSpecificComparison(a, b); ok {
		return equal
	}

	return reflect.DeepEqual(a, b)
}

// tryTypeSpecificComparison attempts type-specific comparison
func tryTypeSpecificComparison(a, b interface{}) (bool, bool) {
	switch aVal := a.(type) {
	case int64:
		return equalInt64(aVal, b), true
	case int:
		return equalInt(aVal, b), true
	case float64:
		return equalFloat64(aVal, b), true
	case string:
		return equalString(aVal, b), true
	}
	return false, false
}

// equalInt64 compares an int64 with another value
func equalInt64(a int64, b interface{}) bool {
	switch bVal := b.(type) {
	case int64:
		return a == bVal
	case int:
		return a == int64(bVal)
	case float64:
		return float64(a) == bVal
	}
	return false
}

// equalInt compares an int with another value
func equalInt(a int, b interface{}) bool {
	switch bVal := b.(type) {
	case int64:
		return int64(a) == bVal
	case int:
		return a == bVal
	case float64:
		return float64(a) == bVal
	}
	return false
}

// equalFloat64 compares a float64 with another value
func equalFloat64(a float64, b interface{}) bool {
	switch bVal := b.(type) {
	case int64:
		return a == float64(bVal)
	case int:
		return a == float64(bVal)
	case float64:
		return a == bVal
	}
	return false
}

// equalString compares a string with another value
func equalString(a string, b interface{}) bool {
	bVal, ok := b.(string)
	return ok && a == bVal
}

// mustExec executes a statement and returns the result, failing the test on error
func mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result {
	t.Helper()
	result, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("exec failed: %v\nquery: %s\nargs: %v", err, query, args)
	}
	return result
}

// mustQuery executes a query and returns the rows, failing the test on error
func mustQuery(t *testing.T, db *sql.DB, query string, args ...interface{}) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query failed: %v\nquery: %s\nargs: %v", err, query, args)
	}
	return rows
}

// expectError executes a statement expecting it to fail
func expectError(t *testing.T, db *sql.DB, query string, args ...interface{}) error {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err == nil {
		t.Fatalf("expected error but got none for: %s", query)
	}
	return err
}

// expectQueryError executes a query expecting it to fail
func expectQueryError(t *testing.T, db *sql.DB, query string, args ...interface{}) error {
	t.Helper()
	_, err := db.Query(query, args...)
	if err == nil {
		t.Fatalf("expected error but got none for query: %s", query)
	}
	return err
}

// createTableWithData is a helper to create a table and insert data in one step
func createTableWithData(t *testing.T, db *sql.DB, createStmt string, insertStmts ...string) {
	t.Helper()
	mustExec(t, db, createStmt)
	for _, stmt := range insertStmts {
		mustExec(t, db, stmt)
	}
}

// ============================================================================
// Universal SQL Test Case Infrastructure
// ============================================================================

// sqlTestCase defines a declarative, table-driven SQL test case.
type sqlTestCase struct {
	name     string          // t.Run name
	setup    []string        // DDL/DML to run before the test
	exec     string          // statement to execute (non-query)
	query    string          // SELECT to run and compare results
	args     []interface{}   // bind parameters for query/exec
	wantRows [][]interface{} // expected rows from query
	wantErr  bool            // expect an error
	errLike  string          // error message substring match
}

// runSQLTests runs a slice of sqlTestCase against a shared database.
func runSQLTests(t *testing.T, db *sql.DB, tests []sqlTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSingleSQLTest(t, db, tt)
		})
	}
}

// runSQLTestsFreshDB runs each test case with a fresh in-memory database.
func runSQLTestsFreshDB(t *testing.T, tests []sqlTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupMemoryDB(t)
			defer db.Close()
			runSingleSQLTest(t, db, tt)
		})
	}
}

// runSingleSQLTest executes a single sqlTestCase.
func runSingleSQLTest(t *testing.T, db *sql.DB, tt sqlTestCase) {
	t.Helper()
	for _, s := range tt.setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q failed: %v", s, err)
		}
	}
	if tt.exec != "" {
		runSQLTestExec(t, db, tt)
		return
	}
	if tt.query != "" {
		runSQLTestQuery(t, db, tt)
	}
}

// runSQLTestExec handles the exec path of a SQL test case.
func runSQLTestExec(t *testing.T, db *sql.DB, tt sqlTestCase) {
	t.Helper()
	_, err := db.Exec(tt.exec, tt.args...)
	if tt.wantErr {
		if err == nil {
			t.Fatalf("expected error but got none for: %s", tt.exec)
		}
		checkErrLike(t, err, tt.errLike)
		return
	}
	if err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, tt.exec)
	}
}

// runSQLTestQuery handles the query path of a SQL test case.
func runSQLTestQuery(t *testing.T, db *sql.DB, tt sqlTestCase) {
	t.Helper()
	rows, err := db.Query(tt.query, tt.args...)
	if tt.wantErr {
		if err != nil {
			checkErrLike(t, err, tt.errLike)
			return
		}
		// Error may surface during row iteration (e.g. function errors).
		defer rows.Close()
		_ = scanAllRows(t, rows)
		if iterErr := rows.Err(); iterErr != nil {
			checkErrLike(t, iterErr, tt.errLike)
			return
		}
		t.Fatalf("expected error but got none for: %s", tt.query)
	}
	if err != nil {
		t.Fatalf("query failed: %v\nquery: %s", err, tt.query)
	}
	defer rows.Close()
	got := scanAllRows(t, rows)
	compareRows(t, got, tt.wantRows)
}

// checkErrLike checks that an error message contains the expected substring.
func checkErrLike(t *testing.T, err error, like string) {
	t.Helper()
	if like != "" && err != nil {
		msg := err.Error()
		lowerMsg := strings.ToLower(msg)
		lowerLike := strings.ToLower(like)
		if !strings.Contains(lowerMsg, lowerLike) {
			t.Errorf("error %q does not contain %q", msg, like)
		}
	}
}
