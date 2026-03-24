// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestCmpBytesViaIntersect exercises cmpBytes via INTERSECT on BLOB columns.
func TestCmpBytesViaIntersect(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/cmpbytes_intersect.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE blobs(b BLOB)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO blobs VALUES (X'AABB'), (X'CCDD'), (X'EEFF')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// INTERSECT forces deduplication comparison using cmpBytes for BLOB columns.
	rows, err := db.Query("SELECT b FROM blobs INTERSECT SELECT b FROM blobs ORDER BY b")
	if err != nil {
		t.Fatalf("query intersect: %v", err)
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
	if count != 3 {
		t.Errorf("INTERSECT returned %d rows, want 3", count)
	}
}

// TestCmpBytesViaExcept exercises cmpBytes via EXCEPT on BLOB columns.
func TestCmpBytesViaExcept(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/cmpbytes_except.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE left_blobs(b BLOB)")
	if err != nil {
		t.Fatalf("create left_blobs: %v", err)
	}
	_, err = db.Exec("CREATE TABLE right_blobs(b BLOB)")
	if err != nil {
		t.Fatalf("create right_blobs: %v", err)
	}
	_, err = db.Exec("INSERT INTO left_blobs VALUES (X'AABB'), (X'CCDD'), (X'EEFF')")
	if err != nil {
		t.Fatalf("insert left: %v", err)
	}
	_, err = db.Exec("INSERT INTO right_blobs VALUES (X'CCDD')")
	if err != nil {
		t.Fatalf("insert right: %v", err)
	}

	// EXCEPT uses cmpBytes to compare BLOB values and remove matching rows.
	rows, err := db.Query("SELECT b FROM left_blobs EXCEPT SELECT b FROM right_blobs ORDER BY b")
	if err != nil {
		t.Fatalf("query except: %v", err)
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
	if count != 2 {
		t.Errorf("EXCEPT returned %d rows, want 2", count)
	}
}

// TestCmpBytesLexicographicOrder verifies BLOB ordering with cmpBytes.
func TestCmpBytesLexicographicOrder(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/cmpbytes_order.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// UNION on BLOB columns with ORDER BY triggers blob comparison in sorting.
	rows, err := db.Query(
		"SELECT X'AABB' UNION SELECT X'CCDD' UNION SELECT X'AABB' ORDER BY 1",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var results [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	// UNION removes duplicates: X'AABB' appears once, X'CCDD' once
	if len(results) != 2 {
		t.Errorf("UNION got %d rows, want 2", len(results))
	}
}

// TestCmpBytesDifferentLengths exercises cmpBytes with blobs of different lengths.
func TestCmpBytesDifferentLengths(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/cmpbytes_len.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t(b BLOB)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO t VALUES (X'AA'), (X'AABB'), (X'AABBCC'), (X'AA')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// INTERSECT with itself forces cmpBytes across different-length blobs.
	rows, err := db.Query("SELECT b FROM t INTERSECT SELECT b FROM t ORDER BY b")
	if err != nil {
		t.Fatalf("query: %v", err)
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
	if count != 3 {
		t.Errorf("INTERSECT distinct blobs = %d, want 3", count)
	}
}
