// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

func TestFrameExcludeNoOthers(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t1(val INTEGER)")
	mustExec(t, db, "INSERT INTO t1 VALUES (1),(2),(3),(4),(5)")

	// EXCLUDE NO OTHERS is the default — should include all frame rows
	rows, err := db.Query("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) FROM t1")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct{ val, sum int }{
		{1, 3}, {2, 6}, {3, 9}, {4, 12}, {5, 9},
	}

	i := 0
	for rows.Next() {
		var val, sum int
		if err := rows.Scan(&val, &sum); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if val != expected[i].val || sum != expected[i].sum {
			t.Errorf("row %d: got val=%d sum=%d, want val=%d sum=%d", i, val, sum, expected[i].val, expected[i].sum)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func TestFrameExcludeCurrentRow(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t2(val INTEGER)")
	mustExec(t, db, "INSERT INTO t2 VALUES (1),(2),(3),(4),(5)")

	// EXCLUDE CURRENT ROW removes the current row from the frame
	rows, err := db.Query("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t2")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct{ val, sum int }{
		{1, 2}, {2, 4}, {3, 6}, {4, 8}, {5, 4},
	}

	i := 0
	for rows.Next() {
		var val, sum int
		if err := rows.Scan(&val, &sum); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if val != expected[i].val || sum != expected[i].sum {
			t.Errorf("row %d: got val=%d sum=%d, want val=%d sum=%d", i, val, sum, expected[i].val, expected[i].sum)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func TestFrameExcludeGroup(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t3(val INTEGER)")
	mustExec(t, db, "INSERT INTO t3 VALUES (1),(2),(2),(3),(4)")

	// EXCLUDE GROUP removes all peers (same ORDER BY value) from the frame
	rows, err := db.Query("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM t3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	// Rows ordered: 1, 2, 2, 3, 4
	// Row 0 (val=1): frame [0,1]=[1,2], exclude group(val=1)=[1] -> sum=2
	// Row 1 (val=2): frame [0,2]=[1,2,2], exclude group(val=2)=[2,2] -> sum=1
	// Row 2 (val=2): frame [1,3]=[2,2,3], exclude group(val=2)=[2,2] -> sum=3
	// Row 3 (val=3): frame [2,4]=[2,3,4], exclude group(val=3)=[3] -> sum=6
	// Row 4 (val=4): frame [3,4]=[3,4], exclude group(val=4)=[4] -> sum=3
	expected := []struct{ val, sum int }{
		{1, 2}, {2, 1}, {2, 3}, {3, 6}, {4, 3},
	}

	i := 0
	for rows.Next() {
		var val, sum int
		if err := rows.Scan(&val, &sum); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if val != expected[i].val || sum != expected[i].sum {
			t.Errorf("row %d: got val=%d sum=%d, want val=%d sum=%d", i, val, sum, expected[i].val, expected[i].sum)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func TestFrameExcludeTies(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE t4(val INTEGER)")
	mustExec(t, db, "INSERT INTO t4 VALUES (1),(2),(2),(3),(4)")

	// EXCLUDE TIES removes peers but keeps the current row
	rows, err := db.Query("SELECT val, SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) FROM t4")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	// Rows ordered: 1, 2, 2, 3, 4
	// Row 0 (val=1): frame [0,1]=[1,2], no ties for 1 -> sum=3
	// Row 1 (val=2): frame [0,2]=[1,2,2], ties of 2 exclude idx2 but keep idx1(current) -> 1+2=3
	// Row 2 (val=2): frame [1,3]=[2,2,3], ties of 2 exclude idx1 but keep idx2(current) -> 2+3=5
	// Row 3 (val=3): frame [2,4]=[2,3,4], no ties for 3 -> sum=9
	// Row 4 (val=4): frame [3,4]=[3,4], no ties for 4 -> sum=7
	expected := []struct{ val, sum int }{
		{1, 3}, {2, 3}, {2, 5}, {3, 9}, {4, 7},
	}

	i := 0
	for rows.Next() {
		var val, sum int
		if err := rows.Scan(&val, &sum); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if val != expected[i].val || sum != expected[i].sum {
			t.Errorf("row %d: got val=%d sum=%d, want val=%d sum=%d", i, val, sum, expected[i].val, expected[i].sum)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func TestFrameExcludeParseAllVariants(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE tp(val INTEGER)")
	mustExec(t, db, "INSERT INTO tp VALUES (1),(2),(3)")

	queries := []string{
		"SELECT SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) FROM tp",
		"SELECT SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM tp",
		"SELECT SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM tp",
		"SELECT SUM(val) OVER (ORDER BY val ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) FROM tp",
	}

	for _, q := range queries {
		rows, err := db.Query(q)
		if err != nil {
			t.Errorf("query %q failed: %v", q, err)
			continue
		}
		rows.Close()
	}
}
