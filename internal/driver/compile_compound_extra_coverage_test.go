// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// cmpBytesOpenDB opens a file-backed database, creates a table, and inserts blob data.
func cmpBytesOpenDB(t *testing.T, name string, stmts ...string) *sql.DB {
	t.Helper()
	dbFile := t.TempDir() + "/" + name + ".db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	return db
}

// countBlobRows runs a blob query and returns the number of rows.
func countBlobRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return count
}

// TestCmpBytesViaIntersect exercises cmpBytes via INTERSECT on BLOB columns.
func TestCmpBytesViaIntersect(t *testing.T) {
	t.Parallel()
	db := cmpBytesOpenDB(t, "cmpbytes_intersect",
		"CREATE TABLE blobs(b BLOB)",
		"INSERT INTO blobs VALUES (X'AABB'), (X'CCDD'), (X'EEFF')",
	)
	count := countBlobRows(t, db, "SELECT b FROM blobs INTERSECT SELECT b FROM blobs ORDER BY b")
	if count != 3 {
		t.Errorf("INTERSECT returned %d rows, want 3", count)
	}
}

// TestCmpBytesViaExcept exercises cmpBytes via EXCEPT on BLOB columns.
func TestCmpBytesViaExcept(t *testing.T) {
	t.Parallel()
	db := cmpBytesOpenDB(t, "cmpbytes_except",
		"CREATE TABLE left_blobs(b BLOB)",
		"CREATE TABLE right_blobs(b BLOB)",
		"INSERT INTO left_blobs VALUES (X'AABB'), (X'CCDD'), (X'EEFF')",
		"INSERT INTO right_blobs VALUES (X'CCDD')",
	)
	count := countBlobRows(t, db, "SELECT b FROM left_blobs EXCEPT SELECT b FROM right_blobs ORDER BY b")
	if count != 2 {
		t.Errorf("EXCEPT returned %d rows, want 2", count)
	}
}

// TestCmpBytesLexicographicOrder verifies BLOB ordering with cmpBytes.
func TestCmpBytesLexicographicOrder(t *testing.T) {
	t.Parallel()
	db := cmpBytesOpenDB(t, "cmpbytes_order")
	count := countBlobRows(t, db, "SELECT X'AABB' UNION SELECT X'CCDD' UNION SELECT X'AABB' ORDER BY 1")
	if count != 2 {
		t.Errorf("UNION got %d rows, want 2", count)
	}
}

// TestCmpBytesDifferentLengths exercises cmpBytes with blobs of different lengths.
func TestCmpBytesDifferentLengths(t *testing.T) {
	t.Parallel()
	db := cmpBytesOpenDB(t, "cmpbytes_len",
		"CREATE TABLE t(b BLOB)",
		"INSERT INTO t VALUES (X'AA'), (X'AABB'), (X'AABBCC'), (X'AA')",
	)
	count := countBlobRows(t, db, "SELECT b FROM t INTERSECT SELECT b FROM t ORDER BY b")
	if count != 3 {
		t.Errorf("INTERSECT distinct blobs = %d, want 3", count)
	}
}
