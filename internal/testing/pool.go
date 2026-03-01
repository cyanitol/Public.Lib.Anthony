// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package testing provides test utilities for the Anthony SQLite driver.
package testing

import (
	"database/sql"
	"sync"
	"testing"

	_ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver" // Register SQLite driver
)

// testDBPool provides a pool of reusable in-memory database connections.
var testDBPool = sync.Pool{
	New: func() interface{} {
		db, err := sql.Open("sqlite_internal", ":memory:")
		if err != nil {
			return nil
		}
		return db
	},
}

// GetTestDB returns a pooled in-memory database connection.
// The connection is automatically returned to the pool when the test completes.
func GetTestDB(t *testing.T) *sql.DB {
	t.Helper()

	pooled := testDBPool.Get()
	if pooled == nil {
		t.Fatal("failed to get database from pool")
	}

	db := pooled.(*sql.DB)

	t.Cleanup(func() {
		// Reset database state before returning to pool
		_, _ = db.Exec("PRAGMA writable_schema = RESET")

		// Get list of tables and drop them
		rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
		if err == nil {
			var tables []string
			for rows.Next() {
				var name string
				rows.Scan(&name)
				tables = append(tables, name)
			}
			rows.Close()
			for _, table := range tables {
				db.Exec("DROP TABLE IF EXISTS " + table)
			}
		}

		testDBPool.Put(db)
	})

	return db
}

// GetFreshDB returns a new in-memory database connection (not pooled).
// Use this when you need a clean database without any pooling.
func GetFreshDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}
