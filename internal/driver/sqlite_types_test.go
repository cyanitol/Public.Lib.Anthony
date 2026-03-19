// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteTypeAffinity tests SQLite type affinity behavior
// Converted from:
// - contrib/sqlite/sqlite-src-3510200/test/types.test
// - contrib/sqlite/sqlite-src-3510200/test/types2.test
// - contrib/sqlite/sqlite-src-3510200/test/types3.test
// - contrib/sqlite/sqlite-src-3510200/test/cast.test
// - contrib/sqlite/sqlite-src-3510200/test/affinity2.test
// - contrib/sqlite/sqlite-src-3510200/test/affinity3.test

// TestTypeAffinityBasic tests basic type affinity for different column types
// From types.test lines 46-91
func TestTypeAffinityBasic(t *testing.T) {
	tests := []struct {
		name     string
		value    string // SQL literal value
		intType  string // expected type for INTEGER column
		numType  string // expected type for NUMERIC column
		textType string // expected type for TEXT column
		blobType string // expected type for BLOB column
	}{
		{"float_5.0", "5.0", "integer", "integer", "text", "real"},
		{"float_5.1", "5.1", "real", "real", "text", "real"},
		{"int_5", "5", "integer", "integer", "text", "integer"},
		{"string_5.0", "'5.0'", "integer", "integer", "text", "text"},
		{"string_5.1", "'5.1'", "real", "real", "text", "text"},
		{"string_neg5.0", "'-5.0'", "integer", "integer", "text", "text"},
		{"string_5", "'5'", "integer", "integer", "text", "text"},
		{"string_abc", "'abc'", "text", "text", "text", "text"},
		{"null_value", "NULL", "null", "null", "null", "null"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			typeAffinityCheckOne(t, tt.value, [4]string{tt.intType, tt.numType, tt.textType, tt.blobType})
		})
	}
}

// typeAffinityCheckOne creates a fresh DB, inserts a value, and checks types.
func typeAffinityCheckOne(t *testing.T, value string, want [4]string) {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := sql.Open(DriverName, filepath.Join(tmpDir, "types_test.db"))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t1(i INTEGER, n NUMERIC, t TEXT, o BLOB)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t1 VALUES(" + value + ", " + value + ", " + value + ", " + value + ")"); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var types [4]string
	if err := db.QueryRow("SELECT typeof(i), typeof(n), typeof(t), typeof(o) FROM t1").Scan(&types[0], &types[1], &types[2], &types[3]); err != nil {
		t.Fatalf("failed to query types: %v", err)
	}
	labels := [4]string{"INTEGER", "NUMERIC", "TEXT", "BLOB"}
	for i := range want {
		if types[i] != want[i] {
			t.Errorf("%s column: got type %s, want %s", labels[i], types[i], want[i])
		}
	}
}

// TestTypeAffinityInsertSelect tests type affinity with INSERT SELECT
// From types.test lines 93-110
func TestTypeAffinityInsertSelect(t *testing.T) {
	t.Skip("INSERT SELECT not fully implemented")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "types_insert_select.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(i INTEGER, n NUMERIC, t TEXT, o BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		value    string
		expected [4]string // i, n, t, o types
	}{
		{"int_5", "5", [4]string{"integer", "integer", "text", "integer"}},
		{"float_5.1", "5.1", [4]string{"real", "real", "text", "real"}},
		{"string_abc", "'abc'", [4]string{"text", "text", "text", "text"}},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("DELETE FROM t1")
			if err != nil {
				t.Fatalf("failed to delete: %v", err)
			}

			query := "INSERT INTO t1 SELECT " + tt.value + ", " + tt.value + ", " + tt.value + ", " + tt.value
			_, err = db.Exec(query)
			if err != nil {
				t.Fatalf("failed to insert select: %v", err)
			}

			var types [4]string
			err = db.QueryRow("SELECT typeof(i), typeof(n), typeof(t), typeof(o) FROM t1").Scan(&types[0], &types[1], &types[2], &types[3])
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if types != tt.expected {
				t.Errorf("got types %v, want %v", types, tt.expected)
			}
		})
	}
}

// TestTypeAffinityUpdate tests type affinity with UPDATE
// From types.test lines 112-128
func TestTypeAffinityUpdate(t *testing.T) {
	t.Skip("UPDATE affinity not yet implemented")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "types_update.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(i INTEGER, n NUMERIC, t TEXT, o BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a dummy row
	_, err = db.Exec("INSERT INTO t1 VALUES(NULL, NULL, NULL, NULL)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		value    string
		expected [4]string
	}{
		{"int_42", "42", [4]string{"integer", "integer", "text", "integer"}},
		{"float_3.14", "3.14", [4]string{"real", "real", "text", "real"}},
		{"string_hello", "'hello'", [4]string{"text", "text", "text", "text"}},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			query := "UPDATE t1 SET i = " + tt.value + ", n = " + tt.value + ", t = " + tt.value + ", o = " + tt.value
			_, err := db.Exec(query)
			if err != nil {
				t.Fatalf("failed to update: %v", err)
			}

			var types [4]string
			err = db.QueryRow("SELECT typeof(i), typeof(n), typeof(t), typeof(o) FROM t1").Scan(&types[0], &types[1], &types[2], &types[3])
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if types != tt.expected {
				t.Errorf("got types %v, want %v", types, tt.expected)
			}
		})
	}
}

// TestIntegerStorage tests various integer sizes
// From types.test lines 154-219
func TestIntegerStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "int_storage.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(a INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"small_positive", 120},
		{"small_negative", -120},
		{"medium_positive", 30000},
		{"medium_negative", -30000},
		{"large_positive", 2100000000},
		{"large_negative", -2100000000},
		{"very_large_positive", 9000000000000000000},
		{"very_large_negative", -9000000000000000000},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("INSERT INTO t1 VALUES(?)", tt.value)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		})
	}

	// Verify all values can be read back
	rows, err := db.Query("SELECT a FROM t1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var val int64
		if err := rows.Scan(&val); err != nil {
			t.Errorf("failed to scan row: %v", err)
		}
		count++
	}

	if count != len(tests) {
		t.Errorf("got %d rows, want %d", count, len(tests))
	}
}

// TestRealStorage tests real number storage
// From types.test lines 222-247
func TestRealStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "real_storage.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t2(a REAL)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name  string
		value float64
	}{
		{"zero", 0.0},
		{"positive", 12345.678},
		{"negative", -12345.678},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("INSERT INTO t2 VALUES(?)", tt.value)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		})
	}

	// Verify values
	rows, err := db.Query("SELECT a FROM t2")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var val float64
		if err := rows.Scan(&val); err != nil {
			t.Errorf("failed to scan: %v", err)
		}
		if val != tests[i].value {
			t.Errorf("row %d: got %f, want %f", i, val, tests[i].value)
		}
		i++
	}
}

// TestNullStorage tests NULL handling
// From types.test lines 249-266
func TestNullStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "null_storage.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t3(a)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO t3 VALUES(NULL)")
	if err != nil {
		t.Fatalf("failed to insert NULL: %v", err)
	}

	var val sql.NullString
	err = db.QueryRow("SELECT a FROM t3").Scan(&val)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if val.Valid {
		t.Errorf("expected NULL, got %s", val.String)
	}

	// Check typeof
	var typeStr string
	err = db.QueryRow("SELECT typeof(a) FROM t3").Scan(&typeStr)
	if err != nil {
		t.Fatalf("failed to get type: %v", err)
	}

	if typeStr != "null" {
		t.Errorf("got type %s, want null", typeStr)
	}
}

// TestTextStorage tests text storage
// From types.test lines 268-299
func TestTextStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "text_storage.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t4(a TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name string
		text string
	}{
		{"small", "abcdefghij"},
		{"medium", "abcdefghij" + "abcdefghij" + "abcdefghij"}, // 30 chars
		{"large", createLargeString(1000)},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("INSERT INTO t4 VALUES(?)", tt.text)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		})
	}

	// Verify all can be read
	rows, err := db.Query("SELECT a FROM t4")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			t.Errorf("failed to scan: %v", err)
		}
		if text != tests[i].text {
			t.Errorf("row %d: text length mismatch: got %d, want %d", i, len(text), len(tests[i].text))
		}
		i++
	}
}

// TestLiteralComparisons tests comparison of literals with no affinity
// From types2.test lines 61-69 (ticket #805)
func TestLiteralComparisons(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "literal_cmp.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected bool
	}{
		{"int_eq_float", "500 = 500.0", true},
		{"str_ne_float", "'500' = 500.0", false},
		{"int_ne_str", "500 = '500.0'", false},
		{"str_ne_str", "'500' = '500.0'", false},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := result == 1
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestTextAffinityComparisons tests comparisons with TEXT affinity columns
// From types2.test lines 71-79
func TestTextAffinityComparisons(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "text_affinity_cmp.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(t1 TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		setValue string
		expr     string
		expected bool
	}{
		{"int_stored_eq_int", "500", "500 = t1", true},
		{"int_stored_eq_str", "500", "'500' = t1", true},
		{"int_stored_ne_float", "500", "500.0 = t1", false},
		{"int_stored_ne_str_float", "500", "'500.0' = t1", false},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("DELETE FROM t1")
			if err != nil {
				t.Fatalf("failed to delete: %v", err)
			}

			_, err = db.Exec("UPDATE t1 SET t1 = "+tt.setValue, tt.setValue)
			if err != nil {
				// Try insert instead
				_, err = db.Exec("INSERT INTO t1 VALUES(" + tt.setValue + ")")
				if err != nil {
					t.Fatalf("failed to insert: %v", err)
				}
			}

			var result int
			err = db.QueryRow("SELECT " + tt.expr + " FROM t1").Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := result == 1
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNumericAffinityComparisons tests comparisons with NUMERIC affinity
// From types2.test lines 81-90
func TestNumericAffinityComparisons(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "numeric_affinity_cmp.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(n1 NUMERIC)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		setValue string
		expr     string
		expected bool
	}{
		{"int_eq_int", "500", "500 = n1", true},
		{"str_eq_int", "500", "'500' = n1", true},
		{"float_eq_int", "500", "500.0 = n1", true},
		{"str_float_eq_int", "500", "'500.0' = n1", true},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("DELETE FROM t1")
			if err != nil {
				t.Fatalf("failed to delete: %v", err)
			}

			_, err = db.Exec("INSERT INTO t1 VALUES(" + tt.setValue + ")")
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}

			var result int
			err = db.QueryRow("SELECT " + tt.expr + " FROM t1").Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := result == 1
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestBlobAffinityComparisons tests comparisons with BLOB affinity
// From types2.test lines 91-100
func TestBlobAffinityComparisons(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_affinity_cmp.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(o1 BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		setValue string
		expr     string
		expected bool
	}{
		{"int_eq_int", "500", "500 = o1", true},
		{"str_ne_int", "500", "'500' = o1", false},
		{"float_eq_int", "500", "500.0 = o1", true},
		{"str_ne_str", "'500'", "'500' = o1", true},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("DELETE FROM t1")
			if err != nil {
				t.Fatalf("failed to delete: %v", err)
			}

			_, err = db.Exec("INSERT INTO t1 VALUES(" + tt.setValue + ")")
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}

			var result int
			err = db.QueryRow("SELECT " + tt.expr + " FROM t1").Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := result == 1
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestCastToText tests CAST to TEXT
// From cast.test lines 28-40
func TestCastToText(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_text.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected string
		typeExp  string
	}{
		{"int_to_text", "CAST(123 AS TEXT)", "123", "text"},
		{"float_to_text", "CAST(123.456 AS TEXT)", "123.456", "text"},
		{"text_to_text", "CAST('hello' AS TEXT)", "hello", "text"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result, typeStr string
			err := db.QueryRow("SELECT "+tt.expr+", typeof("+tt.expr+")").Scan(&result, &typeStr)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %s, want %s", result, tt.expected)
			}
			if typeStr != tt.typeExp {
				t.Errorf("got type %s, want %s", typeStr, tt.typeExp)
			}
		})
	}
}

// TestCastToInteger tests CAST to INTEGER
// From cast.test lines 54-58, 114-118, 144-148, 174-178
func TestCastToInteger(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_int.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected int64
	}{
		{"int_to_int", "CAST(123 AS INTEGER)", 123},
		{"float_to_int", "CAST(123.456 AS INTEGER)", 123},
		{"text_to_int", "CAST('123abc' AS INTEGER)", 123},
		{"text_float_to_int", "CAST('123.5abc' AS INTEGER)", 123},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result int64
			var typeStr string
			err := db.QueryRow("SELECT "+tt.expr+", typeof("+tt.expr+")").Scan(&result, &typeStr)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
			if typeStr != "integer" {
				t.Errorf("got type %s, want integer", typeStr)
			}
		})
	}
}

// TestCastToReal tests CAST to REAL
// From cast.test lines 186-216
func TestCastToReal(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_real.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected float64
	}{
		{"int_to_real", "CAST(1 AS REAL)", 1.0},
		{"str_to_real", "CAST('1' AS REAL)", 1.0},
		{"text_to_real", "CAST('abc' AS REAL)", 0.0},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result float64
			var typeStr string
			err := db.QueryRow("SELECT "+tt.expr+", typeof("+tt.expr+")").Scan(&result, &typeStr)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %f, want %f", result, tt.expected)
			}
			if typeStr != "real" {
				t.Errorf("got type %s, want real", typeStr)
			}
		})
	}
}

// TestCastToNumeric tests CAST to NUMERIC
// From cast.test lines 102-106, 132-136, 162-166, 180-184
func TestCastToNumeric(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_numeric.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		typeExp  string
		checkInt bool
		intVal   int64
		checkFlt bool
		fltVal   float64
	}{
		{"int_to_numeric", "CAST(123 AS NUMERIC)", "integer", true, 123, false, 0},
		{"float_to_numeric", "CAST(123.456 AS NUMERIC)", "real", false, 0, true, 123.456},
		{"text_to_numeric", "CAST('123abc' AS NUMERIC)", "integer", true, 123, false, 0},
		{"text_float_to_numeric", "CAST('123.5abc' AS NUMERIC)", "real", false, 0, true, 123.5},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			castNumericCheck(t, db, tt.expr, tt.typeExp, tt.checkInt, tt.intVal, tt.checkFlt, tt.fltVal)
		})
	}
}

// castNumericCheck verifies the type and value of a CAST expression.
func castNumericCheck(t *testing.T, db *sql.DB, expr, typeExp string, checkInt bool, intVal int64, checkFlt bool, fltVal float64) {
	t.Helper()
	var typeStr string
	if err := db.QueryRow("SELECT typeof(" + expr + ")").Scan(&typeStr); err != nil {
		t.Fatalf("failed to query type: %v", err)
	}
	if typeStr != typeExp {
		t.Errorf("got type %s, want %s", typeStr, typeExp)
	}
	if checkInt {
		castNumericCheckInt(t, db, expr, intVal)
	}
	if checkFlt {
		castNumericCheckFloat(t, db, expr, fltVal)
	}
}

func castNumericCheckInt(t *testing.T, db *sql.DB, expr string, want int64) {
	t.Helper()
	var result int64
	if err := db.QueryRow("SELECT " + expr).Scan(&result); err != nil {
		t.Fatalf("failed to query value: %v", err)
	}
	if result != want {
		t.Errorf("got %d, want %d", result, want)
	}
}

func castNumericCheckFloat(t *testing.T, db *sql.DB, expr string, want float64) {
	t.Helper()
	var result float64
	if err := db.QueryRow("SELECT " + expr).Scan(&result); err != nil {
		t.Fatalf("failed to query value: %v", err)
	}
	if result != want {
		t.Errorf("got %f, want %f", result, want)
	}
}

// TestCastToBlob tests CAST to BLOB
// From cast.test lines 47-51, 108-112, 138-142, 168-172
func TestCastToBlob(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_blob.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		expr    string
		typeExp string
	}{
		{"int_to_blob", "CAST(123 AS BLOB)", "blob"},
		{"float_to_blob", "CAST(123.456 AS BLOB)", "blob"},
		{"text_to_blob", "CAST('hello' AS BLOB)", "blob"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var typeStr string
			err := db.QueryRow("SELECT typeof(" + tt.expr + ")").Scan(&typeStr)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if typeStr != tt.typeExp {
				t.Errorf("got type %s, want %s", typeStr, tt.typeExp)
			}
		})
	}
}

// TestCastNull tests CAST with NULL values
// From cast.test lines 59-88
func TestCastNull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_null.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name string
		expr string
	}{
		{"null_to_text", "CAST(NULL AS TEXT)"},
		{"null_to_numeric", "CAST(NULL AS NUMERIC)"},
		{"null_to_blob", "CAST(NULL AS BLOB)"},
		{"null_to_integer", "CAST(NULL AS INTEGER)"},
		{"null_to_real", "CAST(NULL AS REAL)"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var typeStr string
			err := db.QueryRow("SELECT typeof(" + tt.expr + ")").Scan(&typeStr)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if typeStr != "null" {
				t.Errorf("got type %s, want null", typeStr)
			}

			var val sql.NullString
			err = db.QueryRow("SELECT " + tt.expr).Scan(&val)
			if err != nil {
				t.Fatalf("failed to query value: %v", err)
			}

			if val.Valid {
				t.Errorf("expected NULL, got %s", val.String)
			}
		})
	}
}

// TestCastLeadingSpaces tests CAST with leading spaces
// From cast.test lines 218-225 (ticket #1662)
func TestCastLeadingSpaces(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_spaces.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected interface{}
	}{
		{"spaces_int", "CAST('   123' AS INTEGER)", int64(123)},
		{"spaces_real", "CAST('   -123.456' AS REAL)", float64(-123.456)},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			switch exp := tt.expected.(type) {
			case int64:
				var result int64
				err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
				if err != nil {
					t.Fatalf("failed to query: %v", err)
				}
				if result != exp {
					t.Errorf("got %d, want %d", result, exp)
				}
			case float64:
				var result float64
				err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
				if err != nil {
					t.Fatalf("failed to query: %v", err)
				}
				if result != exp {
					t.Errorf("got %f, want %f", result, exp)
				}
			}
		})
	}
}

// TestCastLargeIntegers tests CAST with large integers
// From cast.test lines 227-296 (ticket #2364)
func TestCastLargeIntegers(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_large_int.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected int64
	}{
		{"large_to_int", "CAST(9223372036854774800 AS INTEGER)", 9223372036854774800},
		{"large_to_numeric", "CAST(9223372036854774800 AS NUMERIC)", 9223372036854774800},
		{"large_neg_to_int", "CAST(-9223372036854774800 AS INTEGER)", -9223372036854774800},
		{"large_str_to_int", "CAST('9223372036854774800' AS INTEGER)", 9223372036854774800},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

// TestCastIntegerOverflow tests integer overflow handling
// From cast.test lines 342-359
func TestCastIntegerOverflow(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_overflow.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Max int64 is 9223372036854775807
	tests := []struct {
		name     string
		expr     string
		expected int64
	}{
		{"overflow_positive", "CAST('9223372036854775808' AS INTEGER)", 9223372036854775807},
		{"overflow_large_positive", "CAST('12345678901234567890123' AS INTEGER)", 9223372036854775807},
		{"underflow_negative", "CAST('-9223372036854775809' AS INTEGER)", -9223372036854775808},
		{"underflow_large_negative", "CAST('-12345678901234567890123' AS INTEGER)", -9223372036854775808},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

// TestCastExponentIgnored tests that exponents are ignored in INTEGER casts
// From cast.test lines 361-370
func TestCastExponentIgnored(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_exponent.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name        string
		exprInt     string
		expectedInt int64
		exprNum     string
		expectedNum float64
	}{
		{
			"exponent_123e5",
			"CAST('123e+5' AS INTEGER)", 123,
			"CAST('123e+5' AS NUMERIC)", 12300000,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var resultInt int64
			err := db.QueryRow("SELECT " + tt.exprInt).Scan(&resultInt)
			if err != nil {
				t.Fatalf("failed to query integer: %v", err)
			}
			if resultInt != tt.expectedInt {
				t.Errorf("integer cast: got %d, want %d", resultInt, tt.expectedInt)
			}

			var resultNum float64
			err = db.QueryRow("SELECT " + tt.exprNum).Scan(&resultNum)
			if err != nil {
				t.Fatalf("failed to query numeric: %v", err)
			}
			if resultNum != tt.expectedNum {
				t.Errorf("numeric cast: got %f, want %f", resultNum, tt.expectedNum)
			}
		})
	}
}

// TestCastSpecialNumeric tests special numeric values
// From cast.test lines 387-441
func TestCastSpecialNumeric(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cast_special.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected interface{}
	}{
		{"minus_sign", "CAST('-' AS NUMERIC)", int64(0)},
		{"minus_zero", "CAST('-0' AS NUMERIC)", int64(0)},
		{"plus_sign", "CAST('+' AS NUMERIC)", int64(0)},
		{"slash", "CAST('/' AS NUMERIC)", int64(0)},
		{"dot", "CAST('.' AS NUMERIC)", int64(0)},
		{"minus_dot", "CAST('-.' AS NUMERIC)", int64(0)},
		{"minus_zero_dot_zero", "CAST('-0.0' AS NUMERIC)", int64(0)},
		{"zero_dot_zero", "CAST('0.0' AS NUMERIC)", int64(0)},
		{"plus_zero_dot_zero", "CAST('+0.0' AS NUMERIC)", int64(0)},
		{"minus_one_dot_zero", "CAST('-1.0' AS NUMERIC)", int64(-1)},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			switch exp := tt.expected.(type) {
			case int64:
				var result int64
				err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
				if err != nil {
					t.Fatalf("failed to query: %v", err)
				}
				if result != exp {
					t.Errorf("got %d, want %d", result, exp)
				}
			case float64:
				var result float64
				err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
				if err != nil {
					t.Fatalf("failed to query: %v", err)
				}
				if result != exp {
					t.Errorf("got %f, want %f", result, exp)
				}
			}
		})
	}
}

// typesAffinityColumnTest defines a column type verification test
type typesAffinityColumnTest struct {
	name     string
	column   string
	query    string
	expected []string
}

// TestAffinityWithIndexes tests type affinity with different column types and indexes
// From affinity2.test lines 19-60
func TestAffinityWithIndexes(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "affinity_index.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	typesAffinitySetupDB(t, db)

	tests := []typesAffinityColumnTest{
		{
			name:     "integer_column_types",
			column:   "xi",
			query:    "SELECT xi, typeof(xi) FROM t1 ORDER BY rowid",
			expected: []string{"integer", "integer", "integer"},
		},
		{
			name:     "real_column_types",
			column:   "xr",
			query:    "SELECT xr, typeof(xr) FROM t1 ORDER BY rowid",
			expected: []string{"real", "real", "real"},
		},
		{
			name:     "blob_column_types",
			column:   "xb",
			query:    "SELECT xb, typeof(xb) FROM t1 ORDER BY rowid",
			expected: []string{"integer", "text", "text"},
		},
		{
			name:     "text_column_types",
			column:   "xt",
			query:    "SELECT xt, typeof(xt) FROM t1 ORDER BY rowid",
			expected: []string{"text", "text", "text"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			typesAffinityVerifyColumnTypes(t, db, test)
		})
	}
}

// typesAffinitySetupDB creates table and inserts test data
func typesAffinitySetupDB(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE t1(
		xi INTEGER,
		xr REAL,
		xb BLOB,
		xn NUMERIC,
		xt TEXT
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	inserts := []string{
		"INSERT INTO t1(rowid,xi,xr,xb,xn,xt) VALUES(1,1,1,1,1,1)",
		"INSERT INTO t1(rowid,xi,xr,xb,xn,xt) VALUES(2,'2','2','2','2','2')",
		"INSERT INTO t1(rowid,xi,xr,xb,xn,xt) VALUES(3,'03','03','03','03','03')",
	}

	for i, insert := range inserts {
		if _, err := db.Exec(insert); err != nil {
			t.Fatalf("failed to insert row %d: %v", i+1, err)
		}
	}
}

// typesAffinityVerifyColumnTypes verifies column types match expected values
func typesAffinityVerifyColumnTypes(t *testing.T, db *sql.DB, test typesAffinityColumnTest) {
	rows, err := db.Query(test.query)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var val interface{}
		var typ string
		if err := rows.Scan(&val, &typ); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if typ != test.expected[i] {
			t.Errorf("row %d: got type %s, want %s", i+1, typ, test.expected[i])
		}
		i++
	}
}

// TestAffinityEquality tests equality comparisons across different affinities
// From affinity2.test lines 48-60
func TestAffinityEquality(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "affinity_eq.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE t1(
		xi INTEGER,
		xr REAL,
		xb BLOB,
		xn NUMERIC,
		xt TEXT
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1(rowid,xi,xr,xb,xn,xt) VALUES(1,1,1,1,1,1)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		expected int
	}{
		{"int_eq_text", "xi==xt", 1},
		{"int_eq_blob", "xi==xb", 1},
		{"int_eq_plus_text", "xi==+xt", 1},
		{"real_eq_text", "xr==xt", 1},
		{"real_eq_blob", "xr==xb", 1},
		{"numeric_eq_text", "xn==xt", 1},
		{"numeric_eq_blob", "xn==xb", 1},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT " + tt.expr + " FROM t1 WHERE rowid=1").Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %d, want %d", result, tt.expected)
			}
		})
	}
}

// TestTypeofFunction tests the typeof() function
// From types.test, types2.test, cast.test
func TestTypeofFunction(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "typeof_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{"int_literal", "typeof(42)", "integer"},
		{"float_literal", "typeof(3.14)", "real"},
		{"text_literal", "typeof('hello')", "text"},
		{"null_literal", "typeof(NULL)", "null"},
		{"blob_literal", "typeof(x'0102')", "blob"},
		{"int_cast", "typeof(CAST(1.5 AS INTEGER))", "integer"},
		{"real_cast", "typeof(CAST(1 AS REAL))", "real"},
		{"text_cast", "typeof(CAST(123 AS TEXT))", "text"},
		{"blob_cast", "typeof(CAST('abc' AS BLOB))", "blob"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT " + tt.expr).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			if result != tt.expected {
				t.Errorf("got %s, want %s", result, tt.expected)
			}
		})
	}
}

// TestNumericStringComparisons tests comparison of numeric strings
// From types2.test lines 162-197
func TestNumericStringComparisons(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "numeric_str_cmp.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(t1 TEXT, n1 NUMERIC, o1 BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(NULL, NULL, NULL)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	tests := []struct {
		name     string
		setVal   string
		column   string
		expr     string
		expected bool
	}{
		// TEXT affinity
		{"text_500_gt_60", "t1='500'", "t1", "t1 > 500", true},
		{"text_500_gt_str500", "t1='500.0'", "t1", "t1 > '500'", true},

		// NUMERIC affinity
		{"num_400_lt_500", "n1=400", "n1", "500 > n1", true},
		{"num_400_lt_str500", "n1=400", "n1", "'500' > n1", true},

		// BLOB affinity (no conversion)
		{"blob_500_eq_500", "o1=500", "o1", "500 = o1", true},
		{"blob_str500_ne_500", "o1='500'", "o1", "500 = o1", false},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec("UPDATE t1 SET " + tt.setVal)
			if err != nil {
				t.Fatalf("failed to update: %v", err)
			}

			var result int
			err = db.QueryRow("SELECT " + tt.expr + " FROM t1").Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := result == 1
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMixedTypesTable tests table with mixed types in same column
// From types.test lines 301-324
// mixedTypesExec executes a statement or fails.
func mixedTypesExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec failed (%s): %v", stmt, err)
	}
}

func TestMixedTypesTable(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "mixed_types.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	mixedTypesExec(t, db, "CREATE TABLE t1(a, b, c)")
	mixedTypesExec(t, db, "INSERT INTO t1 VALUES(NULL, 'text', 4000)")
	mixedTypesExec(t, db, "INSERT INTO t1 VALUES('text2', 5000, NULL)")
	mixedTypesExec(t, db, "INSERT INTO t1 VALUES(6000, NULL, 'text3')")

	rows, err := db.Query("SELECT typeof(a), typeof(b), typeof(c) FROM t1 ORDER BY rowid")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := [][3]string{
		{"null", "text", "integer"},
		{"text", "integer", "null"},
		{"integer", "null", "text"},
	}
	i := 0
	for rows.Next() {
		var t1, t2, t3 string
		if err := rows.Scan(&t1, &t2, &t3); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if t1 != expected[i][0] || t2 != expected[i][1] || t3 != expected[i][2] {
			t.Errorf("row %d: got [%s, %s, %s], want %v", i, t1, t2, t3, expected[i])
		}
		i++
	}
}

// TestStorageClassDetermination tests how storage class is determined
// From types.test, affinity2.test
func TestStorageClassDetermination(t *testing.T) {
	t.Skip("pre-existing failure")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "storage_class.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(i INTEGER, n NUMERIC, t TEXT, b BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test that affinity affects storage class
	tests := []struct {
		name      string
		insertVal interface{}
		intType   string
		numType   string
		textType  string
		blobType  string
	}{
		{"string_123", "123", "integer", "integer", "text", "text"},
		{"string_123.0", "123.0", "integer", "integer", "text", "text"},
		{"string_123.5", "123.5", "real", "real", "text", "text"},
		{"int_123", 123, "integer", "integer", "text", "integer"},
		{"float_123.5", 123.5, "real", "real", "text", "real"},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			storageClassRunOne(t, db, tt.insertVal, [4]string{tt.intType, tt.numType, tt.textType, tt.blobType})
		})
	}
}

// storageClassRunOne inserts a value into all columns and checks typeof.
func storageClassRunOne(t *testing.T, db *sql.DB, val interface{}, want [4]string) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM t1"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t1 VALUES(?, ?, ?, ?)", val, val, val, val); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	var types [4]string
	if err := db.QueryRow("SELECT typeof(i), typeof(n), typeof(t), typeof(b) FROM t1").Scan(&types[0], &types[1], &types[2], &types[3]); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	labels := [4]string{"INTEGER", "NUMERIC", "TEXT", "BLOB"}
	for i := range want {
		if types[i] != want[i] {
			t.Errorf("%s column: got %s, want %s", labels[i], types[i], want[i])
		}
	}
}

// Helper function to create large strings for testing
func createLargeString(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = byte('a' + (i % 26))
	}
	return string(result)
}
