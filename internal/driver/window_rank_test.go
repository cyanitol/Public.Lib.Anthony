// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestRankFunction tests basic RANK() window function with ties
func TestRankFunction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rank_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE scores (name TEXT, score INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data with duplicates to test rank behavior
	testData := []struct {
		name  string
		score int
	}{
		{"Alice", 100},
		{"Bob", 90},
		{"Charlie", 90},  // Tied with Bob at rank 2
		{"David", 85},    // Rank should be 4, not 3
		{"Eve", 85},
		{"Frank", 85},
	}

	for _, row := range testData {
		_, err = db.Exec("INSERT INTO scores VALUES (?, ?)", row.name, row.score)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Test RANK() with ORDER BY score DESC
	rows, err := db.Query("SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY score DESC, name")
	if err != nil {
		t.Fatalf("RANK query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name  string
		score int
		rank  int
	}{
		{"Alice", 100, 1},
		{"Bob", 90, 2},
		{"Charlie", 90, 2},   // Same rank as Bob
		{"David", 85, 4},     // Rank jumps to 4 (not 3) because of the tie
		{"Eve", 85, 4},
		{"Frank", 85, 4},
	}

	i := 0
	for rows.Next() {
		var name string
		var score, rank int
		if err := rows.Scan(&name, &score, &rank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: %s, %d, %d", name, score, rank)
		}

		if name != expected[i].name || score != expected[i].score || rank != expected[i].rank {
			t.Errorf("row %d: got (%s, %d, %d), want (%s, %d, %d)",
				i, name, score, rank, expected[i].name, expected[i].score, expected[i].rank)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

// TestDenseRankFunction tests basic DENSE_RANK() window function
func TestDenseRankFunction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "dense_rank_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE scores (name TEXT, score INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data with duplicates to test dense_rank behavior
	testData := []struct {
		name  string
		score int
	}{
		{"Alice", 100},
		{"Bob", 90},
		{"Charlie", 90},  // Tied with Bob
		{"David", 85},    // Dense rank should be 3, not 4
		{"Eve", 85},
		{"Frank", 85},
	}

	for _, row := range testData {
		_, err = db.Exec("INSERT INTO scores VALUES (?, ?)", row.name, row.score)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Test DENSE_RANK() with ORDER BY score DESC
	rows, err := db.Query("SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores ORDER BY score DESC, name")
	if err != nil {
		t.Fatalf("DENSE_RANK query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name  string
		score int
		rank  int
	}{
		{"Alice", 100, 1},
		{"Bob", 90, 2},
		{"Charlie", 90, 2},   // Same rank as Bob
		{"David", 85, 3},     // Dense rank is 3 (no gap)
		{"Eve", 85, 3},
		{"Frank", 85, 3},
	}

	i := 0
	for rows.Next() {
		var name string
		var score, rank int
		if err := rows.Scan(&name, &score, &rank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: %s, %d, %d", name, score, rank)
		}

		if name != expected[i].name || score != expected[i].score || rank != expected[i].rank {
			t.Errorf("row %d: got (%s, %d, %d), want (%s, %d, %d)",
				i, name, score, rank, expected[i].name, expected[i].score, expected[i].rank)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
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

	// Create test table
	_, err = db.Exec("CREATE TABLE data (value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data: 10, 10, 8, 8, 8, 5
	values := []int{10, 10, 8, 8, 8, 5}
	for _, v := range values {
		_, err = db.Exec("INSERT INTO data VALUES (?)", v)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Test both RANK() and DENSE_RANK() together
	rows, err := db.Query(`
		SELECT value,
		       ROW_NUMBER() OVER (ORDER BY value DESC) as rn,
		       RANK() OVER (ORDER BY value DESC) as rnk,
		       DENSE_RANK() OVER (ORDER BY value DESC) as drnk
		FROM data
		ORDER BY value DESC
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		value     int
		rowNumber int
		rank      int
		denseRank int
	}{
		{10, 1, 1, 1},
		{10, 2, 1, 1},
		{8, 3, 3, 2},
		{8, 4, 3, 2},
		{8, 5, 3, 2},
		{5, 6, 6, 3},
	}

	i := 0
	for rows.Next() {
		var value, rowNumber, rank, denseRank int
		if err := rows.Scan(&value, &rowNumber, &rank, &denseRank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: %d, %d, %d, %d", value, rowNumber, rank, denseRank)
		}

		exp := expected[i]
		if value != exp.value || rowNumber != exp.rowNumber || rank != exp.rank || denseRank != exp.denseRank {
			t.Errorf("row %d: got (%d, rn=%d, rnk=%d, drnk=%d), want (%d, rn=%d, rnk=%d, drnk=%d)",
				i, value, rowNumber, rank, denseRank,
				exp.value, exp.rowNumber, exp.rank, exp.denseRank)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}
