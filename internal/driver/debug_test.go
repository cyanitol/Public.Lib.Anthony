// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestDebugCrossJoin(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE aa(a INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE bb(b INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO aa VALUES(1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO aa VALUES(3)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO bb VALUES(2)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO bb VALUES(4)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO bb VALUES(0)`)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Simple cross join without WHERE
	t.Run("cross-join-no-where", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM aa, bb")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		t.Logf("Columns: %v", cols)

		count := 0
		for rows.Next() {
			var a, b sql.NullInt64
			if err := rows.Scan(&a, &b); err != nil {
				t.Logf("Row %d scan error: %v", count, err)
				count++
				continue
			}
			t.Logf("Row %d: a=%v, b=%v", count, a, b)
			count++
		}
		t.Logf("Total rows: %d (expected 6)", count)
	})

	// Test 2: Cross join with simple WHERE
	t.Run("cross-join-with-where", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM aa, bb WHERE a=1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var a, b sql.NullInt64
			if err := rows.Scan(&a, &b); err != nil {
				t.Logf("Row %d scan error: %v", count, err)
				count++
				continue
			}
			t.Logf("Row %d: a=%v, b=%v", count, a, b)
			count++
		}
		t.Logf("Total rows: %d (expected 3)", count)
	})

	// Test 3: Cross join with CASE in WHERE
	t.Run("cross-join-case-where", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM aa, bb WHERE CASE WHEN a=b-1 THEN 1 END")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var a, b sql.NullInt64
			if err := rows.Scan(&a, &b); err != nil {
				t.Logf("Row %d scan error: %v", count, err)
				count++
				continue
			}
			fmt.Printf("Row %d: a=%v, b=%v\n", count, a, b)
			count++
		}
		t.Logf("Total rows: %d (expected 2)", count)
	})
}
