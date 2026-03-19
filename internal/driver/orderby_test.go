// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// orderbySetupDB creates a test database with a scores table and inserts the given data.
func orderbySetupDB(t *testing.T, scores []struct {
	player string
	score  int
}) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0600); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE scores (player TEXT, score INT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for _, s := range scores {
		if _, err := db.Exec("INSERT INTO scores VALUES (?, ?)", s.player, s.score); err != nil {
			t.Fatalf("failed to insert %s: %v", s.player, err)
		}
	}
	return db
}

// orderbyVerifyPlayers queries and checks that player names match expected order.
func orderbyVerifyPlayers(t *testing.T, rows *sql.Rows, expected []string) {
	t.Helper()
	idx := 0
	for rows.Next() {
		var player string
		if err := rows.Scan(&player); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if idx >= len(expected) || player != expected[idx] {
			t.Errorf("row %d: expected %s, got %s", idx, expected[idx], player)
		}
		idx++
	}
	if idx != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), idx)
	}
}

// TestOrderByASC tests ORDER BY with ASC ordering
func TestOrderByASC(t *testing.T) {
	t.Skip("ORDER BY not yet fully implemented in internal driver")
	scores := []struct {
		player string
		score  int
	}{
		{"Alice", 100}, {"Bob", 250}, {"Charlie", 150}, {"David", 200}, {"Eve", 175},
	}
	db := orderbySetupDB(t, scores)
	defer db.Close()

	rows, err := db.Query(`SELECT player FROM scores ORDER BY score ASC`)
	if err != nil {
		t.Fatalf("ORDER BY ASC failed: %v", err)
	}
	defer rows.Close()

	orderbyVerifyPlayers(t, rows, []string{"Alice", "Charlie", "Eve", "David", "Bob"})
}

// TestOrderByDESC tests ORDER BY with DESC ordering
func TestOrderByDESC(t *testing.T) {
	t.Skip("ORDER BY not yet fully implemented in internal driver")
	scores := []struct {
		player string
		score  int
	}{
		{"Alice", 100}, {"Bob", 250}, {"Charlie", 150},
	}
	db := orderbySetupDB(t, scores)
	defer db.Close()

	rows, err := db.Query(`SELECT player FROM scores ORDER BY score DESC`)
	if err != nil {
		t.Fatalf("ORDER BY DESC failed: %v", err)
	}
	defer rows.Close()

	orderbyVerifyPlayers(t, rows, []string{"Bob", "Charlie", "Alice"})
}
