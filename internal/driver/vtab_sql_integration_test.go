// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"testing"
)

var vtabCtx = context.Background()

// vtabExec executes a statement and fatals on error.
func vtabExec(t *testing.T, db *sql.DB, desc, query string) {
	t.Helper()
	_, err := db.Exec(query)
	if err != nil {
		t.Fatalf("%s failed: %v", desc, err)
	}
}

// vtabCountRows queries and returns the row count, fataling on error.
func vtabCountRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count
}

// vtabQueryStrings queries two string columns and returns the count, fataling on error.
func vtabQueryStrings(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		t.Logf("Row %d: %q, %q", count, a, b)
	}
	return count
}

func openVtabDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func TestFTS5SQLIntegration(t *testing.T) {
	db := openVtabDB(t)
	defer db.Close()

	vtabExec(t, db, "CREATE VIRTUAL TABLE", "CREATE VIRTUAL TABLE docs USING fts5(title, body)")

	t.Run("Insert", func(t *testing.T) {
		vtabExec(t, db, "INSERT", "INSERT INTO docs VALUES ('Hello World', 'This is a test document about hello')")
		vtabExec(t, db, "INSERT", "INSERT INTO docs VALUES ('Go Programming', 'Go is a systems programming language')")
		vtabExec(t, db, "INSERT", "INSERT INTO docs VALUES ('Database Design', 'SQL databases use indexes for fast lookups')")
	})

	t.Run("FullScan", func(t *testing.T) {
		count := vtabQueryStrings(t, db, "SELECT * FROM docs")
		if count != 3 {
			t.Errorf("Expected 3 rows, got %d", count)
		}
	})

	t.Run("Match", func(t *testing.T) {
		count := vtabQueryStrings(t, db, "SELECT title, body FROM docs WHERE docs MATCH 'hello'")
		if count != 1 {
			t.Errorf("Expected 1 MATCH result, got %d", count)
		}
	})

	t.Run("WhereEqual", func(t *testing.T) {
		count := vtabCountRows(t, db, "SELECT title FROM docs WHERE title = 'Database Design'")
		t.Logf("WHERE= matched %d rows", count)
	})

	t.Run("Delete", func(t *testing.T) {
		conn, err := db.Conn(vtabCtx)
		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}
		_, err = conn.ExecContext(vtabCtx, "DELETE FROM docs WHERE title = 'Database Design'")
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}
		conn.Close()

		remaining := vtabCountRows(t, db, "SELECT * FROM docs")
		if remaining != 2 {
			t.Errorf("Expected 2 rows after DELETE, got %d", remaining)
		}
	})
}

func TestRTreeSQLIntegration(t *testing.T) {
	db := openVtabDB(t)
	defer db.Close()

	vtabExec(t, db, "CREATE VIRTUAL TABLE", "CREATE VIRTUAL TABLE spatial USING rtree(id, minX, maxX, minY, maxY)")

	t.Run("Insert", func(t *testing.T) {
		vtabExec(t, db, "INSERT 1", "INSERT INTO spatial VALUES (1, 0.0, 10.0, 0.0, 10.0)")
		vtabExec(t, db, "INSERT 2", "INSERT INTO spatial VALUES (2, 5.0, 15.0, 5.0, 15.0)")
		vtabExec(t, db, "INSERT 3", "INSERT INTO spatial VALUES (3, 20.0, 30.0, 20.0, 30.0)")
	})

	t.Run("FullScan", func(t *testing.T) {
		count := vtabCountRows(t, db, "SELECT * FROM spatial")
		if count != 3 {
			t.Errorf("Expected 3 rows, got %d", count)
		}
	})

	t.Run("RangeFilter", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM spatial WHERE minX <= 12 AND maxX >= 3")
		if err != nil {
			t.Fatalf("SELECT with range filter failed: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
			t.Logf("Range query result: id=%d", id)
		}
		if count < 2 {
			t.Errorf("Expected at least 2 spatial results, got %d", count)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		vtabExec(t, db, "DELETE", "DELETE FROM spatial WHERE id = 3")
		count := vtabCountRows(t, db, "SELECT * FROM spatial")
		if count != 2 {
			t.Errorf("Expected 2 rows after DELETE, got %d", count)
		}
	})
}
