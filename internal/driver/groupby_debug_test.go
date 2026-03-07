// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"testing"
)

func TestGroupBySimple(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec(`CREATE TABLE t1(a INT, b TEXT)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO t1 VALUES(1, 'x')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t1 VALUES(1, 'y')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t1 VALUES(2, 'z')`)
	if err != nil {
		t.Fatal(err)
	}

	// Simple GROUP BY without subquery
	rows, err := db.Query(`SELECT a, COUNT(*) as cnt FROM t1 GROUP BY a ORDER BY a`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	var results []struct {
		a   int
		cnt int
	}
	for rows.Next() {
		var r struct {
			a   int
			cnt int
		}
		if err := rows.Scan(&r.a, &r.cnt); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		results = append(results, r)
	}

	t.Logf("Got results: %v", results)
	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}
	if len(results) >= 2 {
		if results[0].a != 1 || results[0].cnt != 2 {
			t.Errorf("Expected (1, 2), got (%d, %d)", results[0].a, results[0].cnt)
		}
		if results[1].a != 2 || results[1].cnt != 1 {
			t.Errorf("Expected (2, 1), got (%d, %d)", results[1].a, results[1].cnt)
		}
	}
}

func TestGroupByWithSUM(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec(`CREATE TABLE t1(category TEXT, value INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert rows one at a time to avoid multi-value insert
	_, err = db.Exec(`INSERT INTO t1 VALUES('A', 10)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t1 VALUES('B', 20)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t1 VALUES('A', 30)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t1 VALUES('B', 40)`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify data first
	rows, err := db.Query(`SELECT category, value FROM t1 ORDER BY category, value`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	t.Log("Raw data:")
	for rows.Next() {
		var cat string
		var val int
		rows.Scan(&cat, &val)
		t.Logf("  %s, %d", cat, val)
	}
	rows.Close()

	// Now test GROUP BY with SUM
	rows, err = db.Query(`SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category`)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	var results []struct {
		cat string
		sum int64
	}
	for rows.Next() {
		var r struct {
			cat string
			sum int64
		}
		if err := rows.Scan(&r.cat, &r.sum); err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		results = append(results, r)
		t.Logf("Got row: category=%s, sum=%d", r.cat, r.sum)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}
	if len(results) >= 1 && (results[0].cat != "A" || results[0].sum != 40) {
		t.Errorf("Expected (A, 40), got (%s, %d)", results[0].cat, results[0].sum)
	}
	if len(results) >= 2 && (results[1].cat != "B" || results[1].sum != 60) {
		t.Errorf("Expected (B, 60), got (%s, %d)", results[1].cat, results[1].sum)
	}
}
