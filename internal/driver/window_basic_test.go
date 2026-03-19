// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func windowBasicSetupDB(t *testing.T, dbName, createSQL string, inserts []string) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, dbName)
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for _, ins := range inserts {
		if _, err := db.Exec(ins); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	return db
}

func windowBasicVerifyRows(t *testing.T, rows *sql.Rows, expected [][3]int) {
	t.Helper()
	i := 0
	for rows.Next() {
		var a, b, c int
		if err := rows.Scan(&a, &b, &c); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if i < len(expected) && (a != expected[i][0] || b != expected[i][1] || c != expected[i][2]) {
			t.Errorf("row %d: got (%d, %d, %d), want (%d, %d, %d)",
				i, a, b, c, expected[i][0], expected[i][1], expected[i][2])
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

func windowBasicVerifyRowsStr(t *testing.T, rows *sql.Rows, expected []struct {
	id   int
	name string
	rn   int
}) {
	t.Helper()
	i := 0
	for rows.Next() {
		var id, rn int
		var name string
		if err := rows.Scan(&id, &name, &rn); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if i < len(expected) && (id != expected[i].id || name != expected[i].name || rn != expected[i].rn) {
			t.Errorf("row %d: got (%d, %s, %d), want (%d, %s, %d)",
				i, id, name, rn, expected[i].id, expected[i].name, expected[i].rn)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
}

// TestBasicRowNumber tests basic ROW_NUMBER() window function
func TestBasicRowNumber(t *testing.T) {
	db := windowBasicSetupDB(t, "window_basic.db",
		"CREATE TABLE t1(id INTEGER, name TEXT)",
		[]string{
			"INSERT INTO t1 VALUES(1, 'Alice')",
			"INSERT INTO t1 VALUES(2, 'Bob')",
			"INSERT INTO t1 VALUES(3, 'Charlie')",
		})
	defer db.Close()

	rows, err := db.Query("SELECT id, name, ROW_NUMBER() OVER () as rn FROM t1")
	if err != nil {
		t.Fatalf("failed to execute window query: %v", err)
	}
	defer rows.Close()

	windowBasicVerifyRowsStr(t, rows, []struct {
		id   int
		name string
		rn   int
	}{
		{1, "Alice", 1}, {2, "Bob", 2}, {3, "Charlie", 3},
	})
}

// TestBasicRowNumberWithOrderBy tests ROW_NUMBER() OVER (ORDER BY col)
func TestBasicRowNumberWithOrderBy(t *testing.T) {
	db := windowBasicSetupDB(t, "window_order.db",
		"CREATE TABLE t1(id INTEGER, value INTEGER)",
		[]string{
			"INSERT INTO t1 VALUES(3, 300)",
			"INSERT INTO t1 VALUES(1, 100)",
			"INSERT INTO t1 VALUES(2, 200)",
		})
	defer db.Close()

	rows, err := db.Query("SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM t1")
	if err != nil {
		t.Fatalf("failed to execute window query: %v", err)
	}
	defer rows.Close()

	windowBasicVerifyRows(t, rows, [][3]int{{1, 100, 1}, {2, 200, 2}, {3, 300, 3}})
}
