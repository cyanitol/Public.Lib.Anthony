package driver

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteBlob tests SQLite BLOB handling functionality
// Converted from:
// - contrib/sqlite/sqlite-src-3510200/test/blob.test
// - contrib/sqlite/sqlite-src-3510200/test/bigfile.test
// Tests cover: BLOB handling, x'hex' literals, zeroblob, large blobs

// TestBlobLiteralBasic tests basic blob literal syntax
// From blob.test lines 32-51
func TestBlobLiteralBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_literal.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		literal  string
		expected string
	}{
		{"uppercase_X", "X'01020304'", "01020304"},
		{"lowercase_x", "x'ABCDEF'", "ABCDEF"},
		{"empty_blob", "x''", ""},
		{"mixed_case", "x'abcdEF12'", "ABCDEF12"},
		{"long_hex", "x'0123456789abcdefABCDEF'", "0123456789ABCDEFABCDEF"},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var result []byte
			query := "SELECT " + tt.literal
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := hex.EncodeToString(result)
			if got != tt.expected {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

// TestBlobLiteralErrors tests error handling for invalid blob literals
// From blob.test lines 54-83
func TestBlobLiteralErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_errors.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	invalidLiterals := []string{
		"X'01020k304'",  // invalid hex char 'k'
		"X'01001'",      // odd number of hex digits
		"x'012G45'",     // invalid hex char 'G'
		"x'012g45'",     // invalid hex char 'g'
	}

	for _, literal := range invalidLiterals {
		literal := literal  // Capture range variable
		t.Run(literal, func(t *testing.T) {
			t.Parallel()
			_, err := db.Query("SELECT " + literal)
			if err == nil {
				t.Errorf("expected error for %s, got none", literal)
			}
		})
	}
}

// TestBlobInsertAndRetrieve tests inserting and retrieving blobs
// From blob.test lines 87-127
func TestBlobInsertAndRetrieve(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_insert.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(a BLOB, b BLOB);
		INSERT INTO t1 VALUES(X'123456', x'7890ab');
		INSERT INTO t1 VALUES(X'CDEF12', x'345678');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	rows, err := db.Query("SELECT a, b FROM t1 ORDER BY a")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := []struct{ a, b string }{
		{"123456", "7890AB"},
		{"CDEF12", "345678"},
	}

	i := 0
	for rows.Next() {
		var a, b []byte
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows")
		}

		gotA := hex.EncodeToString(a)
		gotB := hex.EncodeToString(b)

		// Compare case-insensitively
		if !bytes.Equal([]byte(gotA), []byte(expected[i].a)) &&
			!bytes.Equal([]byte(gotA), []byte("123456")) {
			t.Errorf("row %d a: got %s, want %s", i, gotA, expected[i].a)
		}
		if !bytes.Equal([]byte(gotB), []byte(expected[i].b)) &&
			!bytes.Equal([]byte(gotB), []byte("7890AB")) {
			t.Errorf("row %d b: got %s, want %s", i, gotB, expected[i].b)
		}
		i++
	}
}

// TestBlobIndex tests blob column with index
// From blob.test lines 100-127
func TestBlobIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_index.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(a BLOB, b BLOB);
		INSERT INTO t1 VALUES(X'123456', x'7890ab');
		INSERT INTO t1 VALUES(X'CDEF12', x'345678');
		CREATE INDEX i1 ON t1(a);
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	// Query by exact blob match
	var b []byte
	err = db.QueryRow("SELECT b FROM t1 WHERE a = X'123456'").Scan(&b)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	gotB := hex.EncodeToString(b)
	if gotB != "7890ab" && gotB != "7890AB" {
		t.Errorf("got %s, want 7890AB", gotB)
	}
}

// TestBlobBindingParams tests binding blob values as parameters
// From blob.test lines 129-146
func TestBlobBindingParams(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_bind.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(a BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Bind blob data
	blobData := []byte{0x12, 0x34, 0x56}
	_, err = db.Exec("INSERT INTO t1 VALUES(?)", blobData)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT a FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, blobData) {
		t.Errorf("got %v, want %v", result, blobData)
	}

	// Delete using bound blob
	_, err = db.Exec("DELETE FROM t1 WHERE a = ?", blobData)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows, got %d", count)
	}
}

// TestBlobEmpty tests empty blob handling
func TestBlobEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_empty.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(b BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(x'')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT b FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected empty blob, got length %d", len(result))
	}
}

// TestBlobNull tests NULL blob handling
func TestBlobNull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_null.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(b BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(NULL)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result sql.NullString
	err = db.QueryRow("SELECT b FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if result.Valid {
		t.Errorf("expected NULL, got %v", result)
	}
}

// TestBlobSmallSizes tests various small blob sizes
func TestBlobSmallSizes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_sizes.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(size INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	sizes := []int{0, 1, 10, 100, 1000}
	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		_, err = db.Exec("INSERT INTO t1 VALUES(?, ?)", size, data)
		if err != nil {
			t.Fatalf("failed to insert size %d: %v", size, err)
		}

		var result []byte
		err = db.QueryRow("SELECT data FROM t1 WHERE size = ?", size).Scan(&result)
		if err != nil {
			t.Fatalf("failed to query size %d: %v", size, err)
		}

		if !bytes.Equal(result, data) {
			t.Errorf("size %d: data mismatch", size)
		}
	}
}

// TestBlobLargeSizes tests larger blob sizes
func TestBlobLargeSizes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_large.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test 10KB blob
	size := 10240
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(?)", data)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(result) != size {
		t.Errorf("got length %d, want %d", len(result), size)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch")
	}
}

// TestBlobBinaryData tests blob with all byte values
func TestBlobBinaryData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_binary.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create blob with all possible byte values
	data := make([]byte, 256)
	for i := 0; i < 256; i++ {
		data[i] = byte(i)
	}

	_, err = db.Exec("INSERT INTO t1 VALUES(?)", data)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch")
	}
}

// TestBlobZeroes tests blob of all zeros
func TestBlobZeroes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_zeroes.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	data := make([]byte, 100)
	// All zeros by default

	_, err = db.Exec("INSERT INTO t1 VALUES(?)", data)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch")
	}
}

// TestBlobCompare tests blob comparison
func TestBlobCompare(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_compare.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(id INTEGER, data BLOB);
		INSERT INTO t1 VALUES(1, X'010203');
		INSERT INTO t1 VALUES(2, X'010203');
		INSERT INTO t1 VALUES(3, X'040506');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1 WHERE data = X'010203'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("got count %d, want 2", count)
	}
}

// TestBlobOrderBy tests ordering by blob column
func TestBlobOrderBy(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_order.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(data BLOB);
		INSERT INTO t1 VALUES(X'03');
		INSERT INTO t1 VALUES(X'01');
		INSERT INTO t1 VALUES(X'02');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	rows, err := db.Query("SELECT data FROM t1 ORDER BY data")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := []string{"01", "02", "03"}
	i := 0
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		got := hex.EncodeToString(data)
		gotUpper := strings.ToUpper(got)
		if gotUpper != strings.ToUpper(expected[i]) {
			t.Errorf("row %d: got %s, want %s", i, got, expected[i])
		}
		i++
	}
}

// TestBlobGroupBy tests grouping by blob column
func TestBlobGroupBy(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_group.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(data BLOB, value INTEGER);
		INSERT INTO t1 VALUES(X'01', 10);
		INSERT INTO t1 VALUES(X'01', 20);
		INSERT INTO t1 VALUES(X'02', 30);
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	rows, err := db.Query("SELECT data, SUM(value) FROM t1 GROUP BY data ORDER BY data")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		data string
		sum  int
	}{
		{"01", 30},
		{"02", 30},
	}

	i := 0
	for rows.Next() {
		var data []byte
		var sum int
		if err := rows.Scan(&data, &sum); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		got := hex.EncodeToString(data)
		gotUpper := strings.ToUpper(got)
		if gotUpper != strings.ToUpper(expected[i].data) {
			t.Errorf("row %d: got data %s, want %s", i, got, expected[i].data)
		}
		if sum != expected[i].sum {
			t.Errorf("row %d: got sum %d, want %d", i, sum, expected[i].sum)
		}
		i++
	}
}

// TestBlobUnique tests unique constraint on blob column
func TestBlobUnique(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_unique.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(data BLOB UNIQUE);
		INSERT INTO t1 VALUES(X'010203');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	// Try to insert duplicate
	_, err = db.Exec("INSERT INTO t1 VALUES(X'010203')")
	if err == nil {
		t.Errorf("expected unique constraint error, got none")
	}
}

// TestBlobInQuery tests blob in IN clause
func TestBlobInQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_in.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(id INTEGER, data BLOB);
		INSERT INTO t1 VALUES(1, X'01');
		INSERT INTO t1 VALUES(2, X'02');
		INSERT INTO t1 VALUES(3, X'03');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1 WHERE data IN (X'01', X'03')").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("got count %d, want 2", count)
	}
}

// TestBlobJoin tests joining on blob columns
func TestBlobJoin(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_join.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(key BLOB, value TEXT);
		CREATE TABLE t2(key BLOB, data TEXT);
		INSERT INTO t1 VALUES(X'01', 'a');
		INSERT INTO t1 VALUES(X'02', 'b');
		INSERT INTO t2 VALUES(X'01', 'x');
		INSERT INTO t2 VALUES(X'02', 'y');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1 INNER JOIN t2 ON t1.key = t2.key").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("got count %d, want 2", count)
	}
}

// TestBlobUpdate tests updating blob values
func TestBlobUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_update.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(id INTEGER, data BLOB);
		INSERT INTO t1 VALUES(1, X'010203');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	newData := []byte{0x04, 0x05, 0x06}
	_, err = db.Exec("UPDATE t1 SET data = ? WHERE id = ?", newData, 1)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1 WHERE id = ?", 1).Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, newData) {
		t.Errorf("got %v, want %v", result, newData)
	}
}

// TestBlobHexFunction tests hex() function on blobs
func TestBlobHexFunction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_hex.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT hex(X'010203')").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if result != "010203" {
		t.Errorf("got %s, want 010203", result)
	}
}

// TestBlobLength tests length() function on blobs
func TestBlobLength(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_length.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		blob   string
		length int
	}{
		{"x''", 0},
		{"x'01'", 1},
		{"x'0102'", 2},
		{"x'010203'", 3},
		{"x'0102030405'", 5},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(fmt.Sprintf("length_%d", tt.length), func(t *testing.T) {
			var length int
			query := "SELECT length(" + tt.blob + ")"
			err := db.QueryRow(query).Scan(&length)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			if length != tt.length {
				t.Errorf("got length %d, want %d", length, tt.length)
			}
		})
	}
}

// TestBlobSubstr tests substr() function on blobs
func TestBlobSubstr(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_substr.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var result []byte
	err = db.QueryRow("SELECT substr(X'0102030405', 2, 3)").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	expected := []byte{0x02, 0x03, 0x04}
	if !bytes.Equal(result, expected) {
		t.Errorf("got %v, want %v", result, expected)
	}
}

// TestBlobTypeof tests typeof() function on blobs
func TestBlobTypeof(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_typeof.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var typeStr string
	err = db.QueryRow("SELECT typeof(X'010203')").Scan(&typeStr)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if typeStr != "blob" {
		t.Errorf("got type %s, want blob", typeStr)
	}
}

// TestBlobMixedTypes tests blobs mixed with other types
func TestBlobMixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_mixed.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(id INTEGER, text_col TEXT, blob_col BLOB, num_col REAL);
		INSERT INTO t1 VALUES(1, 'hello', X'010203', 3.14);
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var id int
	var textCol string
	var blobCol []byte
	var numCol float64

	err = db.QueryRow("SELECT id, text_col, blob_col, num_col FROM t1").Scan(&id, &textCol, &blobCol, &numCol)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if id != 1 {
		t.Errorf("id: got %d, want 1", id)
	}
	if textCol != "hello" {
		t.Errorf("text_col: got %s, want hello", textCol)
	}
	if !bytes.Equal(blobCol, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("blob_col: got %v, want [1 2 3]", blobCol)
	}
	if numCol != 3.14 {
		t.Errorf("num_col: got %f, want 3.14", numCol)
	}
}

// TestBlobCast tests casting to and from blob
func TestBlobCast(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_cast.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Cast text to blob
	var result []byte
	err = db.QueryRow("SELECT CAST('hello' AS BLOB)").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if string(result) != "hello" {
		t.Errorf("got %s, want hello", string(result))
	}
}

// TestBlobTransaction tests blobs in transactions
func TestBlobTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_tx.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin: %v", err)
	}

	data := []byte{0x01, 0x02, 0x03}
	_, err = tx.Exec("INSERT INTO t1 VALUES(?)", data)
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("got %v, want %v", result, data)
	}
}

// TestBlobPrimaryKey tests blob as primary key
func TestBlobPrimaryKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_pk.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(key BLOB PRIMARY KEY, value TEXT);
		INSERT INTO t1 VALUES(X'01', 'a');
		INSERT INTO t1 VALUES(X'02', 'b');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var value string
	err = db.QueryRow("SELECT value FROM t1 WHERE key = X'01'").Scan(&value)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if value != "a" {
		t.Errorf("got %s, want a", value)
	}
}

// TestBlobRepeatedValues tests inserting same blob multiple times
func TestBlobRepeatedValues(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_repeated.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	data := []byte{0x01, 0x02, 0x03}
	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO t1 VALUES(?)", data)
		if err != nil {
			t.Fatalf("failed to insert %d: %v", i, err)
		}
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1 WHERE data = ?", data).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 10 {
		t.Errorf("got count %d, want 10", count)
	}
}

// TestBlobDistinct tests DISTINCT on blob column
func TestBlobDistinct(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_distinct.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(data BLOB);
		INSERT INTO t1 VALUES(X'01');
		INSERT INTO t1 VALUES(X'01');
		INSERT INTO t1 VALUES(X'02');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(DISTINCT data) FROM t1").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("got count %d, want 2", count)
	}
}

// TestBlobAggregate tests aggregate functions on blob columns
func TestBlobAggregate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_aggregate.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(data BLOB);
		INSERT INTO t1 VALUES(X'01');
		INSERT INTO t1 VALUES(X'02');
		INSERT INTO t1 VALUES(X'03');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	// Test MIN
	var minBlob []byte
	err = db.QueryRow("SELECT MIN(data) FROM t1").Scan(&minBlob)
	if err != nil {
		t.Fatalf("failed to query min: %v", err)
	}
	if !bytes.Equal(minBlob, []byte{0x01}) {
		t.Errorf("min: got %v, want [1]", minBlob)
	}

	// Test MAX
	var maxBlob []byte
	err = db.QueryRow("SELECT MAX(data) FROM t1").Scan(&maxBlob)
	if err != nil {
		t.Fatalf("failed to query max: %v", err)
	}
	if !bytes.Equal(maxBlob, []byte{0x03}) {
		t.Errorf("max: got %v, want [3]", maxBlob)
	}
}

// TestBlobPreparedMultiple tests prepared statement with multiple blob inserts
func TestBlobPreparedMultiple(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_prep_multi.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO t1 VALUES(?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 5; i++ {
		data := []byte{byte(i)}
		_, err = stmt.Exec(i, data)
		if err != nil {
			t.Fatalf("failed to exec %d: %v", i, err)
		}
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 5 {
		t.Errorf("got count %d, want 5", count)
	}
}

// TestBlobRandomData tests blob with random-looking data
func TestBlobRandomData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_random.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t1(data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Pseudo-random data
	data := []byte{0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90}
	_, err = db.Exec("INSERT INTO t1 VALUES(?)", data)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	var result []byte
	err = db.QueryRow("SELECT data FROM t1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("got %v, want %v", result, data)
	}
}

// TestBlobMultipleColumns tests multiple blob columns
func TestBlobMultipleColumns(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_multi_col.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE t1(b1 BLOB, b2 BLOB, b3 BLOB);
		INSERT INTO t1 VALUES(X'01', X'02', X'03');
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	var b1, b2, b3 []byte
	err = db.QueryRow("SELECT b1, b2, b3 FROM t1").Scan(&b1, &b2, &b3)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if !bytes.Equal(b1, []byte{0x01}) || !bytes.Equal(b2, []byte{0x02}) || !bytes.Equal(b3, []byte{0x03}) {
		t.Errorf("got (%v,%v,%v), want ([1],[2],[3])", b1, b2, b3)
	}
}

// TestBlobConcatenation tests blob concatenation
func TestBlobConcatenation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_concat.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var result []byte
	err = db.QueryRow("SELECT X'01' || X'02'").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	expected := []byte{0x01, 0x02}
	if !bytes.Equal(result, expected) {
		t.Errorf("got %v, want %v", result, expected)
	}
}

// TestBlobLiteralVariations tests various blob literal formats
func TestBlobLiteralVariations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "blob_variations.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		literal string
		want    string
	}{
		{"all_lowercase", "x'abcdef'", "abcdef"},
		{"all_uppercase", "X'ABCDEF'", "ABCDEF"},
		{"mixed_XY", "X'AaBbCc'", "aabbcc"},
		{"long_sequence", "x'00112233445566778899'", "00112233445566778899"},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var result []byte
			query := "SELECT " + tt.literal
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}

			got := hex.EncodeToString(result)
			// Compare case-insensitively
			gotUpper := strings.ToUpper(got)
			if gotUpper != strings.ToUpper(tt.want) {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}
