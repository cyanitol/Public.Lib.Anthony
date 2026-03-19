// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// dsnParsedVerify checks the parsed DSN fields common to all test cases.
func dsnParsedVerify(t *testing.T, dsn *DSN, wantMemory, wantReadOnly bool) {
	t.Helper()
	if dsn.Config.Pager.MemoryDB != wantMemory {
		t.Errorf("expected MemoryDB=%v, got %v", wantMemory, dsn.Config.Pager.MemoryDB)
	}
	if dsn.Config.Pager.ReadOnly != wantReadOnly {
		t.Errorf("expected ReadOnly=%v, got %v", wantReadOnly, dsn.Config.Pager.ReadOnly)
	}
}

// TestParseDSN tests the DSN parsing functionality
type dsnTestCase struct {
	name         string
	dsn          string
	wantErr      bool
	wantMemory   bool
	wantReadOnly bool
	checkConfig  func(*testing.T, *DriverConfig)
}

func dsnCheckSimpleFilename(t *testing.T, cfg *DriverConfig) {
	if cfg.EnableForeignKeys != false {
		t.Errorf("expected EnableForeignKeys=false (SQLite default), got %v", cfg.EnableForeignKeys)
	}
}

func dsnCheckReadOnly(t *testing.T, cfg *DriverConfig) {
	if !cfg.Pager.ReadOnly {
		t.Errorf("expected ReadOnly=true, got false")
	}
}

func dsnCheckMemory(t *testing.T, cfg *DriverConfig) {
	if !cfg.Pager.MemoryDB {
		t.Errorf("expected MemoryDB=true, got false")
	}
}

func dsnCheckMultipleParams(t *testing.T, cfg *DriverConfig) {
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
}

func dsnParseCases() []dsnTestCase {
	return []dsnTestCase{
		{"simple filename", "test.db", false, false, false, dsnCheckSimpleFilename},
		{"read-only mode", "test.db?mode=ro", false, false, true, dsnCheckReadOnly},
		{"memory mode", ":memory:", false, true, false, dsnCheckMemory},
		{"journal_mode WAL", "test.db?journal_mode=wal", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if cfg.Pager.JournalMode != "wal" {
				t.Errorf("expected JournalMode=wal, got %s", cfg.Pager.JournalMode)
			}
		}},
		{"cache_size", "test.db?cache_size=10000", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if cfg.Pager.CacheSize != 10000 {
				t.Errorf("expected CacheSize=10000, got %d", cfg.Pager.CacheSize)
			}
		}},
		{"synchronous mode", "test.db?synchronous=normal", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if cfg.Pager.SyncMode != "normal" {
				t.Errorf("expected SyncMode=normal, got %s", cfg.Pager.SyncMode)
			}
		}},
		{"foreign_keys off", "test.db?foreign_keys=off", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if cfg.EnableForeignKeys != false {
				t.Errorf("expected EnableForeignKeys=false, got true")
			}
		}},
		{"busy_timeout", "test.db?busy_timeout=5000", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if cfg.Pager.BusyTimeout != 5000*time.Millisecond {
				t.Errorf("expected BusyTimeout=%v, got %v", 5000*time.Millisecond, cfg.Pager.BusyTimeout)
			}
		}},
		{"multiple parameters", "test.db?mode=rw&journal_mode=wal&cache_size=5000&foreign_keys=on", false, false, false, dsnCheckMultipleParams},
		{"shared cache", "test.db?cache=shared", false, false, false, func(t *testing.T, cfg *DriverConfig) {
			if !cfg.SharedCache {
				t.Errorf("expected SharedCache=true, got false")
			}
		}},
	}
}

func TestParseDSN(t *testing.T) {
	for _, tt := range dsnParseCases() {
		t.Run(tt.name, func(t *testing.T) {
			dsnRunParseCase(t, tt.dsn, tt.wantErr, tt.wantMemory, tt.wantReadOnly, tt.checkConfig)
		})
	}
}

// dsnRunParseCase runs a single ParseDSN test case.
func dsnRunParseCase(t *testing.T, dsnStr string, wantErr, wantMemory, wantReadOnly bool, checkConfig func(*testing.T, *DriverConfig)) {
	t.Helper()
	dsn, err := ParseDSN(dsnStr)
	if (err != nil) != wantErr {
		t.Errorf("ParseDSN() error = %v, wantErr %v", err, wantErr)
		return
	}
	if err != nil {
		return
	}
	dsnParsedVerify(t, dsn, wantMemory, wantReadOnly)
	if checkConfig != nil {
		checkConfig(t, dsn.Config)
	}
}

// dsnOpenTemp opens a temp database with the given DSN suffix.
func dsnOpenTemp(t *testing.T, dsnSuffix string) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := sql.Open(DriverName, dbPath+dsnSuffix)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// dsnIntegrationCase holds a DSN integration test case.
type dsnIntegrationCase struct {
	name      string
	dsn       string
	setupTest func(*testing.T, *sql.DB)
}

func dsnIntCheckJournalWAL(t *testing.T, db *sql.DB) {
	t.Helper()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("expected journal_mode=wal, got %s", mode)
	}
}

func dsnIntCheckForeignKeysOn(t *testing.T, db *sql.DB) {
	t.Helper()
	var fk int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("failed to query foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("expected foreign_keys=1, got %d", fk)
	}
}

func dsnIntCheckCacheSize(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE test (id INTEGER)"); err != nil {
		t.Fatalf("failed to create table with cache_size setting: %v", err)
	}
}

func dsnIntCheckMultipleSettings(t *testing.T, db *sql.DB) {
	t.Helper()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}
	if mode != "memory" {
		t.Errorf("expected journal_mode=memory, got %s", mode)
	}
	var fk int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("failed to query foreign_keys: %v", err)
	}
	if fk != 0 {
		t.Errorf("expected foreign_keys=0, got %d", fk)
	}
}

func dsnIntegrationCases() []dsnIntegrationCase {
	return []dsnIntegrationCase{
		{"journal_mode WAL", "?journal_mode=wal", dsnIntCheckJournalWAL},
		{"foreign_keys on", "?foreign_keys=on", dsnIntCheckForeignKeysOn},
		{"cache_size", "?cache_size=5000", dsnIntCheckCacheSize},
		{"multiple settings", "?journal_mode=memory&foreign_keys=off", dsnIntCheckMultipleSettings},
	}
}

// TestDSNIntegration tests DSN parameters with actual database connections
func TestDSNIntegration(t *testing.T) {
	for _, tt := range dsnIntegrationCases() {
		dsn := tt.dsn
		setup := tt.setupTest
		t.Run(tt.name, func(t *testing.T) {
			db := dsnOpenTemp(t, dsn)
			defer db.Close()
			setup(t, db)
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
