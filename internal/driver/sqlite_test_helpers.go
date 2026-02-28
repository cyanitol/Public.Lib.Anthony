package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// setupMemoryDB creates a new in-memory test database
func setupMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
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
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	var result [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the row values
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		// Convert []byte to string for comparison
		row := make([]interface{}, len(values))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return result
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

	// Create a map to track matched rows
	matched := make([]bool, len(want))

	for _, gotRow := range got {
		found := false
		for i, wantRow := range want {
			if matched[i] {
				continue
			}
			if rowsEqual(gotRow, wantRow) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			t.Errorf("unexpected row: %v", gotRow)
		}
	}

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
	// Handle nil
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle numeric conversions
	switch aVal := a.(type) {
	case int64:
		switch bVal := b.(type) {
		case int64:
			return aVal == bVal
		case int:
			return aVal == int64(bVal)
		case float64:
			return float64(aVal) == bVal
		}
	case int:
		switch bVal := b.(type) {
		case int64:
			return int64(aVal) == bVal
		case int:
			return aVal == bVal
		case float64:
			return float64(aVal) == bVal
		}
	case float64:
		switch bVal := b.(type) {
		case int64:
			return aVal == float64(bVal)
		case int:
			return aVal == float64(bVal)
		case float64:
			return aVal == bVal
		}
	case string:
		if bVal, ok := b.(string); ok {
			return aVal == bVal
		}
	}

	// Fallback to reflect.DeepEqual
	return reflect.DeepEqual(a, b)
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
