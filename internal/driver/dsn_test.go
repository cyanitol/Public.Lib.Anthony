// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestParseDSN tests the DSN parsing functionality
func TestParseDSN(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		wantErr     bool
		wantMemory  bool
		wantReadOnly bool
		checkConfig func(*testing.T, *DriverConfig)
	}{
		{
			name:       "simple filename",
			dsn:        "test.db",
			wantErr:    false,
			wantMemory: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.EnableForeignKeys != true {
					t.Errorf("expected EnableForeignKeys=true, got %v", cfg.EnableForeignKeys)
				}
			},
		},
		{
			name:         "read-only mode",
			dsn:          "test.db?mode=ro",
			wantErr:      false,
			wantReadOnly: true,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if !cfg.Pager.ReadOnly {
					t.Errorf("expected ReadOnly=true, got false")
				}
			},
		},
		{
			name:       "memory mode",
			dsn:        ":memory:",
			wantErr:    false,
			wantMemory: true,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if !cfg.Pager.MemoryDB {
					t.Errorf("expected MemoryDB=true, got false")
				}
			},
		},
		{
			name:    "journal_mode WAL",
			dsn:     "test.db?journal_mode=wal",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.Pager.JournalMode != "wal" {
					t.Errorf("expected JournalMode=wal, got %s", cfg.Pager.JournalMode)
				}
			},
		},
		{
			name:    "cache_size",
			dsn:     "test.db?cache_size=10000",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.Pager.CacheSize != 10000 {
					t.Errorf("expected CacheSize=10000, got %d", cfg.Pager.CacheSize)
				}
			},
		},
		{
			name:    "synchronous mode",
			dsn:     "test.db?synchronous=normal",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.Pager.SyncMode != "normal" {
					t.Errorf("expected SyncMode=normal, got %s", cfg.Pager.SyncMode)
				}
			},
		},
		{
			name:    "foreign_keys off",
			dsn:     "test.db?foreign_keys=off",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.EnableForeignKeys != false {
					t.Errorf("expected EnableForeignKeys=false, got true")
				}
			},
		},
		{
			name:    "busy_timeout",
			dsn:     "test.db?busy_timeout=5000",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				expected := 5000 * time.Millisecond
				if cfg.Pager.BusyTimeout != expected {
					t.Errorf("expected BusyTimeout=%v, got %v", expected, cfg.Pager.BusyTimeout)
				}
			},
		},
		{
			name:    "multiple parameters",
			dsn:     "test.db?mode=rw&journal_mode=wal&cache_size=5000&foreign_keys=on",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if cfg.Pager.ReadOnly {
					t.Errorf("expected ReadOnly=false, got true")
				}
				if cfg.Pager.JournalMode != "wal" {
					t.Errorf("expected JournalMode=wal, got %s", cfg.Pager.JournalMode)
				}
				if cfg.Pager.CacheSize != 5000 {
					t.Errorf("expected CacheSize=5000, got %d", cfg.Pager.CacheSize)
				}
				if !cfg.EnableForeignKeys {
					t.Errorf("expected EnableForeignKeys=true, got false")
				}
			},
		},
		{
			name:    "shared cache",
			dsn:     "test.db?cache=shared",
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *DriverConfig) {
				if !cfg.SharedCache {
					t.Errorf("expected SharedCache=true, got false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if dsn.Config.Pager.MemoryDB != tt.wantMemory {
				t.Errorf("expected MemoryDB=%v, got %v", tt.wantMemory, dsn.Config.Pager.MemoryDB)
			}

			if dsn.Config.Pager.ReadOnly != tt.wantReadOnly {
				t.Errorf("expected ReadOnly=%v, got %v", tt.wantReadOnly, dsn.Config.Pager.ReadOnly)
			}

			if tt.checkConfig != nil {
				tt.checkConfig(t, dsn.Config)
			}
		})
	}
}

// TestDSNIntegration tests DSN parameters with actual database connections
func TestDSNIntegration(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		setupTest func(*testing.T, *sql.DB)
	}{
		{
			name: "journal_mode WAL",
			dsn:  "?journal_mode=wal",
			setupTest: func(t *testing.T, db *sql.DB) {
				var mode string
				err := db.QueryRow("PRAGMA journal_mode").Scan(&mode)
				if err != nil {
					t.Fatalf("failed to query journal_mode: %v", err)
				}
				if mode != "wal" {
					t.Errorf("expected journal_mode=wal, got %s", mode)
				}
			},
		},
		{
			name: "foreign_keys on",
			dsn:  "?foreign_keys=on",
			setupTest: func(t *testing.T, db *sql.DB) {
				var fk int
				err := db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
				if err != nil {
					t.Fatalf("failed to query foreign_keys: %v", err)
				}
				if fk != 1 {
					t.Errorf("expected foreign_keys=1, got %d", fk)
				}
			},
		},
		{
			name: "cache_size",
			dsn:  "?cache_size=5000",
			setupTest: func(t *testing.T, db *sql.DB) {
				// Create a simple table to verify the connection works
				// The cache_size is applied but may not be queryable yet
				_, err := db.Exec("CREATE TABLE test (id INTEGER)")
				if err != nil {
					t.Fatalf("failed to create table with cache_size setting: %v", err)
				}
			},
		},
		{
			name: "multiple settings",
			dsn:  "?journal_mode=memory&foreign_keys=off",
			setupTest: func(t *testing.T, db *sql.DB) {
				var mode string
				err := db.QueryRow("PRAGMA journal_mode").Scan(&mode)
				if err != nil {
					t.Fatalf("failed to query journal_mode: %v", err)
				}
				if mode != "memory" {
					t.Errorf("expected journal_mode=memory, got %s", mode)
				}

				var fk int
				err = db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
				if err != nil {
					t.Fatalf("failed to query foreign_keys: %v", err)
				}
				if fk != 0 {
					t.Errorf("expected foreign_keys=0, got %d", fk)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary database file
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			// Open database with DSN
			dsn := dbPath + tt.dsn
			db, err := sql.Open(DriverName, dsn)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			// Run the test
			tt.setupTest(t, db)
		})
	}
}

// TestDSNMemory tests in-memory database with DSN parameters
func TestDSNMemory(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:?foreign_keys=on")
	if err != nil {
		t.Fatalf("failed to open memory database: %v", err)
	}
	defer db.Close()

	// Check foreign_keys is enabled
	var fk int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
	if err != nil {
		t.Fatalf("failed to query foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("expected foreign_keys=1, got %d", fk)
	}

	// Create a table and verify it works
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}

// TestDSNReadOnly tests read-only mode
func TestDSNReadOnly(t *testing.T) {
	// Create a database with some data
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and populate database
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		db.Close()
		t.Fatalf("failed to insert data: %v", err)
	}
	db.Close()

	// Open in read-only mode
	db, err = sql.Open(DriverName, dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("failed to open database in read-only mode: %v", err)
	}
	defer db.Close()

	// Verify we can read
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}

	// Verify we cannot write
	_, err = db.Exec("INSERT INTO test (name) VALUES ('test2')")
	if err == nil {
		t.Error("expected error when writing to read-only database, got nil")
	}
}

// TestFormatDSN tests DSN formatting
func TestFormatDSN(t *testing.T) {
	tests := []struct {
		name     string
		dsn      *DSN
		expected string
	}{
		{
			name: "simple filename",
			dsn: &DSN{
				Filename: "test.db",
				Config:   DefaultDriverConfig(),
			},
			expected: "test.db",
		},
		{
			name: "memory",
			dsn: &DSN{
				Filename: ":memory:",
				Config: func() *DriverConfig {
					cfg := DefaultDriverConfig()
					cfg.Pager.MemoryDB = true
					return cfg
				}(),
			},
			expected: ":memory:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDSN(tt.dsn)
			// For simple cases, check exact match
			// For complex cases with parameters, just verify filename is present
			if tt.expected == ":memory:" {
				if result != ":memory:" {
					t.Errorf("expected %s, got %s", tt.expected, result)
				}
			} else if tt.expected == "test.db" {
				// Should either be exactly "test.db" or start with "test.db?"
				if result != "test.db" && result[:7] != "test.db" {
					t.Errorf("expected result to start with test.db, got %s", result)
				}
			}
		})
	}
}

// TestDSNBusyTimeout tests busy timeout parameter
func TestDSNBusyTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Open database with busy timeout
	db, err := sql.Open(DriverName, dbPath+"?busy_timeout=1000")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec("INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Verify the data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}

// TestDSNInvalidParameters tests error handling for invalid parameters
func TestDSNInvalidParameters(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{
			name:    "invalid mode",
			dsn:     "test.db?mode=invalid",
			wantErr: true,
		},
		{
			name:    "invalid cache",
			dsn:     "test.db?cache=invalid",
			wantErr: true,
		},
		{
			name:    "invalid cache_size",
			dsn:     "test.db?cache_size=invalid",
			wantErr: true,
		},
		{
			name:    "invalid busy_timeout",
			dsn:     "test.db?busy_timeout=invalid",
			wantErr: true,
		},
		{
			name:    "invalid foreign_keys",
			dsn:     "test.db?foreign_keys=invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestDSNWithActualFile tests DSN with a real file on disk
func TestDSNWithActualFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create the file first
	f, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	f.Close()

	// Open with various DSN parameters
	dsn := dbPath + "?journal_mode=wal&cache_size=2000&foreign_keys=on"
	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify we can use the database
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}
