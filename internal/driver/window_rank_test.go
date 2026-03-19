// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

type rankRow struct {
	name  string
	score int
	rank  int
}

func rankSetupScoresDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rank_test.db")
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE scores (name TEXT, score INTEGER)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	data := []struct {
		name  string
		score int
	}{
		{"Alice", 100}, {"Bob", 90}, {"Charlie", 90},
		{"David", 85}, {"Eve", 85}, {"Frank", 85},
	}
	for _, row := range data {
		if _, err := db.Exec("INSERT INTO scores VALUES (?, ?)", row.name, row.score); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	return db
}

func rankVerifyRows(t *testing.T, rows *sql.Rows, expected []rankRow) {
	t.Helper()
	i := 0
	for rows.Next() {
		var name string
		var score, rank int
		if err := rows.Scan(&name, &score, &rank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if i < len(expected) && (name != expected[i].name || score != expected[i].score || rank != expected[i].rank) {
			t.Errorf("row %d: got (%s, %d, %d), want (%s, %d, %d)",
				i, name, score, rank, expected[i].name, expected[i].score, expected[i].rank)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

// TestRankFunction tests basic RANK() window function with ties
func TestRankFunction(t *testing.T) {
	db := rankSetupScoresDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY score DESC, name")
	if err != nil {
		t.Fatalf("RANK query failed: %v", err)
	}
	defer rows.Close()

	rankVerifyRows(t, rows, []rankRow{
		{"Alice", 100, 1}, {"Bob", 90, 2}, {"Charlie", 90, 2},
		{"David", 85, 4}, {"Eve", 85, 4}, {"Frank", 85, 4},
	})
}

// TestDenseRankFunction tests basic DENSE_RANK() window function
func TestDenseRankFunction(t *testing.T) {
	db := rankSetupScoresDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores ORDER BY score DESC, name")
	if err != nil {
		t.Fatalf("DENSE_RANK query failed: %v", err)
	}
	defer rows.Close()

	rankVerifyRows(t, rows, []rankRow{
		{"Alice", 100, 1}, {"Bob", 90, 2}, {"Charlie", 90, 2},
		{"David", 85, 3}, {"Eve", 85, 3}, {"Frank", 85, 3},
	})
}

// TestRankAndDenseRankTogether tests both RANK() and DENSE_RANK() in the same query
func TestRankAndDenseRankTogether(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "both_ranks_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	rankTogetherSetup(t, db)
	rankTogetherVerify(t, db)
}

func rankTogetherSetup(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE data (value INTEGER)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for _, v := range []int{10, 10, 8, 8, 8, 5} {
		if _, err := db.Exec("INSERT INTO data VALUES (?)", v); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
}

func rankTogetherVerify(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`
		SELECT value, ROW_NUMBER() OVER (ORDER BY value DESC) as rn,
		       RANK() OVER (ORDER BY value DESC) as rnk,
		       DENSE_RANK() OVER (ORDER BY value DESC) as drnk
		FROM data ORDER BY value DESC`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := [][4]int{{10, 1, 1, 1}, {10, 2, 1, 1}, {8, 3, 3, 2}, {8, 4, 3, 2}, {8, 5, 3, 2}, {5, 6, 6, 3}}
	i := 0
	for rows.Next() {
		var value, rowNumber, rank, denseRank int
		if err := rows.Scan(&value, &rowNumber, &rank, &denseRank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if i < len(expected) {
			rankTogetherCheckRow(t, i, [4]int{value, rowNumber, rank, denseRank}, expected[i])
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func rankTogetherCheckRow(t *testing.T, i int, got, exp [4]int) {
	t.Helper()
	if got != exp {
		t.Errorf("row %d: got (%d, rn=%d, rnk=%d, drnk=%d), want (%d, rn=%d, rnk=%d, drnk=%d)",
			i, got[0], got[1], got[2], got[3], exp[0], exp[1], exp[2], exp[3])
	}
}
