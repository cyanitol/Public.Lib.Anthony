// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestOrderByASC tests ORDER BY with ASC ordering
func TestOrderByASC(t *testing.T) {
	t.Skip("ORDER BY not yet fully implemented in internal driver")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open database
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0600); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE scores (player TEXT, score INT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	scores := []struct {
		player string
		score  int
	}{
		{"Alice", 100},
		{"Bob", 250},
		{"Charlie", 150},
		{"David", 200},
		{"Eve", 175},
	}

	for _, s := range scores {
		_, err := db.Exec("INSERT INTO scores VALUES (?, ?)", s.player, s.score)
		if err != nil {
			t.Fatalf("failed to insert %s: %v", s.player, err)
		}
	}

	// Test ORDER BY ASC
	rows, err := db.Query(`SELECT player FROM scores ORDER BY score ASC`)
	if err != nil {
		t.Fatalf("ORDER BY ASC failed: %v", err)
	}
	defer rows.Close()

	expectedAsc := []string{"Alice", "Charlie", "Eve", "David", "Bob"}
	idx := 0
	for rows.Next() {
		var player string
		if err := rows.Scan(&player); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if idx >= len(expectedAsc) || player != expectedAsc[idx] {
			t.Errorf("ORDER BY ASC row %d: expected %s, got %s", idx, expectedAsc[idx], player)
		}
		idx++
	}

	if idx != len(expectedAsc) {
		t.Errorf("expected %d rows, got %d", len(expectedAsc), idx)
	}
}

// TestOrderByDESC tests ORDER BY with DESC ordering
func TestOrderByDESC(t *testing.T) {
	t.Skip("ORDER BY not yet fully implemented in internal driver")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open database
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0600); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE scores (player TEXT, score INT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	scores := []struct {
		player string
		score  int
	}{
		{"Alice", 100},
		{"Bob", 250},
		{"Charlie", 150},
	}

	for _, s := range scores {
		_, err := db.Exec("INSERT INTO scores VALUES (?, ?)", s.player, s.score)
		if err != nil {
			t.Fatalf("failed to insert %s: %v", s.player, err)
		}
	}

	// Test ORDER BY DESC
	rows, err := db.Query(`SELECT player, score FROM scores ORDER BY score DESC`)
	if err != nil {
		t.Fatalf("ORDER BY DESC failed: %v", err)
	}
	defer rows.Close()

	expectedDesc := []struct {
		player string
		score  int
	}{
		{"Bob", 250},
		{"Charlie", 150},
		{"Alice", 100},
	}

	idx := 0
	for rows.Next() {
		var player string
		var score int
		if err := rows.Scan(&player, &score); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if idx >= len(expectedDesc) {
			t.Fatalf("too many rows returned")
		}
		if player != expectedDesc[idx].player || score != expectedDesc[idx].score {
			t.Errorf("ORDER BY DESC row %d: expected (%s, %d), got (%s, %d)",
				idx, expectedDesc[idx].player, expectedDesc[idx].score, player, score)
		}
		idx++
	}

	if idx != len(expectedDesc) {
		t.Errorf("expected %d rows, got %d", len(expectedDesc), idx)
	}
}
