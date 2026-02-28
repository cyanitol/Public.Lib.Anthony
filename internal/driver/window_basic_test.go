package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestBasicRowNumber tests basic ROW_NUMBER() window function
func TestBasicRowNumber(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "window_basic.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE t1(id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO t1 VALUES(1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert row 1: %v", err)
	}
	_, err = db.Exec("INSERT INTO t1 VALUES(2, 'Bob')")
	if err != nil {
		t.Fatalf("failed to insert row 2: %v", err)
	}
	_, err = db.Exec("INSERT INTO t1 VALUES(3, 'Charlie')")
	if err != nil {
		t.Fatalf("failed to insert row 3: %v", err)
	}

	// Test ROW_NUMBER() OVER ()
	rows, err := db.Query("SELECT id, name, ROW_NUMBER() OVER () as rn FROM t1")
	if err != nil {
		t.Fatalf("failed to execute window query: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		id   int
		name string
		rn   int
	}{
		{1, "Alice", 1},
		{2, "Bob", 2},
		{3, "Charlie", 3},
	}

	i := 0
	for rows.Next() {
		var id, rn int
		var name string
		if err := rows.Scan(&id, &name, &rn); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: id=%d, name=%s, rn=%d", id, name, rn)
		}

		if id != expected[i].id || name != expected[i].name || rn != expected[i].rn {
			t.Errorf("row %d: got (%d, %s, %d), want (%d, %s, %d)",
				i, id, name, rn, expected[i].id, expected[i].name, expected[i].rn)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

// TestBasicRowNumberWithOrderBy tests ROW_NUMBER() OVER (ORDER BY col)
func TestBasicRowNumberWithOrderBy(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "window_order.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE t1(id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data in non-sequential order
	inserts := []string{
		"INSERT INTO t1 VALUES(3, 300)",
		"INSERT INTO t1 VALUES(1, 100)",
		"INSERT INTO t1 VALUES(2, 200)",
	}
	for _, insert := range inserts {
		if _, err := db.Exec(insert); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Test ROW_NUMBER() OVER (ORDER BY value)
	rows, err := db.Query("SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM t1")
	if err != nil {
		t.Fatalf("failed to execute window query: %v", err)
	}
	defer rows.Close()

	// Expected results: ordered by value
	expected := []struct {
		id    int
		value int
		rn    int
	}{
		{1, 100, 1},
		{2, 200, 2},
		{3, 300, 3},
	}

	i := 0
	for rows.Next() {
		var id, value, rn int
		if err := rows.Scan(&id, &value, &rn); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: id=%d, value=%d, rn=%d", id, value, rn)
		}

		if id != expected[i].id || value != expected[i].value || rn != expected[i].rn {
			t.Errorf("row %d: got (%d, %d, %d), want (%d, %d, %d)",
				i, id, value, rn, expected[i].id, expected[i].value, expected[i].rn)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}
