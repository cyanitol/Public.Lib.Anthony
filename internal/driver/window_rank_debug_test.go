package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestRankDebug is a debug test to see what values we're actually getting
func TestRankDebug(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rank_debug.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert simple test data
	_, err = db.Exec("INSERT INTO test VALUES (3)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO test VALUES (2)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test RANK() - see what we get
	t.Log("Testing RANK() to see actual values...")
	rows, err := db.Query("SELECT value, RANK() OVER (ORDER BY value) as rnk FROM test")
	if err != nil {
		t.Fatalf("RANK query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var value interface{}
		var rank interface{}
		if err := rows.Scan(&value, &rank); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		t.Logf("value=%v (type %T), rank=%v (type %T)", value, value, rank, rank)
	}
}
