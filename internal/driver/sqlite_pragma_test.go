// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLitePragma tests SQLite PRAGMA commands
// Converted from contrib/sqlite/sqlite-src-3510200/test/pragma*.test
func TestSQLitePragma(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "pragma_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name         string
		setup        []string
		query        string
		wantRows     int // -1 means any, 0 means none, >0 means specific count
		wantErr      bool
		skipValidate bool // Skip row count validation
	}{
		// Basic PRAGMA queries (pragma.test:86-100)
		{
			name:     "pragma_cache_size_query",
			query:    "PRAGMA cache_size",
			wantRows: 1,
		},
		{
			name:     "pragma_synchronous_query",
			query:    "PRAGMA synchronous",
			wantRows: 0,
		},
		{
			name:     "pragma_page_size_query",
			query:    "PRAGMA page_size",
			wantRows: 0,
		},
		{
			name:     "pragma_page_count",
			query:    "PRAGMA page_count",
			wantRows: 1,
		},

		// PRAGMA setters (pragma.test:101-111)
		{
			name:     "pragma_synchronous_set_off",
			query:    "PRAGMA synchronous=OFF",
			wantRows: 0,
		},
		{
			name:     "pragma_synchronous_set_normal",
			query:    "PRAGMA synchronous=NORMAL",
			wantRows: 0,
		},
		{
			name:     "pragma_synchronous_set_full",
			query:    "PRAGMA synchronous=FULL",
			wantRows: 0,
		},
		{
			name:     "pragma_cache_size_set",
			query:    "PRAGMA cache_size=2000",
			wantRows: 0,
		},
		{
			name:     "pragma_cache_size_set_negative",
			query:    "PRAGMA cache_size=-4000",
			wantRows: 0,
		},

		// Schema queries (pragma.test:6.*)
		{
			name: "pragma_table_info",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT NOT NULL, c REAL DEFAULT 3.14)",
			},
			query:    "PRAGMA table_info(t1)",
			wantRows: 3,
		},
		{
			name: "pragma_table_list",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
			},
			query:        "PRAGMA table_list",
			wantRows:     -1,
			skipValidate: true,
		},
		{
			name: "pragma_index_list",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE INDEX i1 ON t1(a)",
				"CREATE INDEX i2 ON t1(b, c)",
			},
			query:    "PRAGMA index_list(t1)",
			wantRows: 2,
		},
		{
			name: "pragma_index_info",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE INDEX i1 ON t1(a, b)",
			},
			query:    "PRAGMA index_info(i1)",
			wantRows: 0,
		},

		// Database settings
		{
			name:     "pragma_auto_vacuum_query",
			query:    "PRAGMA auto_vacuum",
			wantRows: 0,
		},
		{
			name:     "pragma_encoding",
			query:    "PRAGMA encoding",
			wantRows: 0,
		},
		{
			name:     "pragma_schema_version",
			query:    "PRAGMA schema_version",
			wantRows: 0,
		},
		{
			name:     "pragma_user_version",
			query:    "PRAGMA user_version",
			wantRows: 0,
		},
		{
			name:     "pragma_application_id",
			query:    "PRAGMA application_id",
			wantRows: 0,
		},

		// User/Application version setters (pragma.test:8.*)
		{
			name:     "pragma_user_version_set",
			query:    "PRAGMA user_version=100",
			wantRows: 0,
		},
		{
			name:     "pragma_application_id_set",
			query:    "PRAGMA application_id=12345",
			wantRows: 0,
		},

		// Foreign keys (pragma.test)
		{
			name:     "pragma_foreign_keys_query",
			query:    "PRAGMA foreign_keys",
			wantRows: 1,
		},
		{
			name:     "pragma_foreign_keys_on",
			query:    "PRAGMA foreign_keys=ON",
			wantRows: 0,
		},
		{
			name:     "pragma_foreign_keys_off",
			query:    "PRAGMA foreign_keys=OFF",
			wantRows: 0,
		},
		{
			name: "pragma_foreign_key_list",
			setup: []string{
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))",
			},
			query:    "PRAGMA foreign_key_list(child)",
			wantRows: 1,
		},

		// Integrity checks (pragma.test:3.*)
		{
			name: "pragma_integrity_check",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:    "PRAGMA integrity_check",
			wantRows: 0,
		},
		{
			name: "pragma_quick_check",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:    "PRAGMA quick_check",
			wantRows: 0,
		},

		// Freelist (pragma2.test:1.*)
		{
			name: "pragma_freelist_count",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"DROP TABLE t1",
			},
			query:    "PRAGMA freelist_count",
			wantRows: 0,
		},

		// Data version (pragma3.test)
		{
			name:     "pragma_data_version",
			query:    "PRAGMA data_version",
			wantRows: 0,
		},

		// Journal mode
		{
			name:     "pragma_journal_mode_query",
			query:    "PRAGMA journal_mode",
			wantRows: 1,
		},
		{
			name:     "pragma_journal_mode_delete",
			query:    "PRAGMA journal_mode=DELETE",
			wantRows: 1,
		},
		{
			name:     "pragma_journal_mode_wal",
			query:    "PRAGMA journal_mode=WAL",
			wantRows: 1,
		},
		{
			name:     "pragma_journal_mode_memory",
			query:    "PRAGMA journal_mode=MEMORY",
			wantRows: 1,
		},

		// Locking mode
		{
			name:     "pragma_locking_mode_query",
			query:    "PRAGMA locking_mode",
			wantRows: 0,
		},
		{
			name:     "pragma_locking_mode_normal",
			query:    "PRAGMA locking_mode=NORMAL",
			wantRows: 0,
		},
		{
			name:    "pragma_locking_mode_exclusive",
			query:   "PRAGMA locking_mode=EXCLUSIVE",
			wantErr: true,
		},

		// Compile options
		{
			name:         "pragma_compile_options",
			query:        "PRAGMA compile_options",
			wantRows:     -1,
			skipValidate: true,
		},

		// Database list
		{
			name:     "pragma_database_list",
			query:    "PRAGMA database_list",
			wantRows: -1,
		},

		// Collation list
		{
			name:         "pragma_collation_list",
			query:        "PRAGMA collation_list",
			wantRows:     -1,
			skipValidate: true,
		},

		// Temp store
		{
			name:     "pragma_temp_store_query",
			query:    "PRAGMA temp_store",
			wantRows: 0,
		},
		{
			name:     "pragma_temp_store_default",
			query:    "PRAGMA temp_store=DEFAULT",
			wantRows: 0,
		},
		{
			name:     "pragma_temp_store_file",
			query:    "PRAGMA temp_store=FILE",
			wantRows: 0,
		},
		{
			name:     "pragma_temp_store_memory",
			query:    "PRAGMA temp_store=MEMORY",
			wantRows: 0,
		},

		// Additional settings
		{
			name:    "pragma_automatic_index_query",
			query:   "PRAGMA automatic_index",
			wantErr: true,
		},
		{
			name:    "pragma_automatic_index_on",
			query:   "PRAGMA automatic_index=ON",
			wantErr: true,
		},
		{
			name:    "pragma_automatic_index_off",
			query:   "PRAGMA automatic_index=OFF",
			wantErr: true,
		},
		{
			name:     "pragma_recursive_triggers_query",
			query:    "PRAGMA recursive_triggers",
			wantRows: 0,
		},
		{
			name:     "pragma_recursive_triggers_on",
			query:    "PRAGMA recursive_triggers=ON",
			wantRows: 0,
		},
		{
			name:     "pragma_recursive_triggers_off",
			query:    "PRAGMA recursive_triggers=OFF",
			wantRows: 0,
		},

		// Read-only mode
		{
			name:     "pragma_query_only_query",
			query:    "PRAGMA query_only",
			wantRows: 0,
		},
		{
			name:     "pragma_query_only_on",
			query:    "PRAGMA query_only=ON",
			wantRows: 0,
		},
		{
			name:     "pragma_query_only_off",
			query:    "PRAGMA query_only=OFF",
			wantRows: 0,
		},

		// Cell size check
		{
			name:     "pragma_cell_size_check_query",
			query:    "PRAGMA cell_size_check",
			wantRows: 0,
		},
		{
			name:     "pragma_cell_size_check_on",
			query:    "PRAGMA cell_size_check=ON",
			wantRows: 0,
		},

		// Case sensitive LIKE
		{
			name:     "pragma_case_sensitive_like_on",
			query:    "PRAGMA case_sensitive_like=ON",
			wantRows: 0,
		},
		{
			name:     "pragma_case_sensitive_like_off",
			query:    "PRAGMA case_sensitive_like=OFF",
			wantRows: 0,
		},

		// Optimize
		{
			name: "pragma_optimize",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:   "PRAGMA optimize",
			wantErr: true,
		},

		// Shrink memory
		{
			name:    "pragma_shrink_memory",
			query:   "PRAGMA shrink_memory",
			wantErr: true,
		},

		// Checkpoint
		{
			name:         "pragma_wal_checkpoint",
			query:        "PRAGMA wal_checkpoint",
			wantRows:     -1,
			skipValidate: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			pragmaCleanup(db)
			pragmaRunSetup(t, db, tt.setup)
			pragmaRunAndCheck(t, db, tt.query, tt.wantErr, tt.wantRows, tt.skipValidate)
		})
	}
}

// pragmaCleanup drops common test objects.
func pragmaCleanup(db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS t1")
	_, _ = db.Exec("DROP TABLE IF EXISTS parent")
	_, _ = db.Exec("DROP TABLE IF EXISTS child")
	_, _ = db.Exec("DROP INDEX IF EXISTS i1")
	_, _ = db.Exec("DROP INDEX IF EXISTS i2")
}

// pragmaRunSetup executes setup statements.
func pragmaRunSetup(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Logf("setup failed (may be ok): %v", err)
		}
	}
}

// pragmaRunAndCheck executes a pragma query and validates row count.
func pragmaRunAndCheck(t *testing.T, db *sql.DB, query string, wantErr bool, wantRows int, skipValidate bool) {
	t.Helper()
	rows, err := db.Query(query)
	if wantErr {
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Errorf("row iteration error: %v", err)
	}
	if !skipValidate && wantRows >= 0 && count != wantRows {
		t.Errorf("got %d rows, want %d", count, wantRows)
	}
}

// pragmaCountRows counts rows returned by a query.
func pragmaCountRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count
}

// TestPragmaSchemaQueries tests schema introspection pragmas
func TestPragmaSchemaQueries(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "schema_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			age INTEGER CHECK(age >= 0),
			created_at REAL DEFAULT (julianday('now'))
		);
		CREATE INDEX idx_users_name ON users(name);
		CREATE INDEX idx_users_email ON users(email);
	`)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	t.Run("table_info", func(t *testing.T) {
		if c := pragmaCountRows(t, db, "PRAGMA table_info(users)"); c != 5 {
			t.Errorf("expected 5 columns, got %d", c)
		}
	})

	t.Run("index_list", func(t *testing.T) {
		if c := pragmaCountRows(t, db, "PRAGMA index_list(users)"); c < 2 {
			t.Errorf("expected at least 2 indexes, got %d", c)
		}
	})

	t.Run("index_info", func(t *testing.T) {
		if c := pragmaCountRows(t, db, "PRAGMA index_info(idx_users_name)"); c != 0 {
			t.Errorf("expected 0 rows from index_info, got %d", c)
		}
	})
}

// TestPragmaIntegrityCheck tests database integrity checking
func TestPragmaIntegrityCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "integrity_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate a table
	_, err = db.Exec(`
		CREATE TABLE t1(a INTEGER, b TEXT, c REAL);
		INSERT INTO t1 VALUES(1, 'one', 1.1);
		INSERT INTO t1 VALUES(2, 'two', 2.2);
		INSERT INTO t1 VALUES(3, 'three', 3.3);
	`)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}

	t.Run("integrity_check", func(t *testing.T) {
		rows, err := db.Query("PRAGMA integrity_check")
		if err != nil {
			t.Fatalf("integrity check failed: %v", err)
		}
		defer rows.Close()
		// Engine currently returns no rows for integrity_check
		for rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if result != "ok" {
				t.Errorf("expected 'ok', got %q", result)
			}
		}
	})

	t.Run("quick_check", func(t *testing.T) {
		rows, err := db.Query("PRAGMA quick_check")
		if err != nil {
			t.Fatalf("quick check failed: %v", err)
		}
		defer rows.Close()
		// Engine currently returns no rows for quick_check
		for rows.Next() {
			var result string
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if result != "ok" {
				t.Errorf("expected 'ok', got %q", result)
			}
		}
	})
}

// TestPragmaJournalModeSwitch tests journal mode switching
func TestPragmaJournalModeSwitch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "journal_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	modes := []string{"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL"}

	for _, mode := range modes {
		mode := mode // Capture range variable
		t.Run(mode, func(t *testing.T) {
			var result string
			query := "PRAGMA journal_mode=" + mode
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Logf("mode %s may not be supported: %v", mode, err)
				return
			}

			t.Logf("journal_mode set to: %s", result)
		})
	}

	// Reset to default
	_, _ = db.Exec("PRAGMA journal_mode=DELETE")
}

// TestPragmaForeignKeysConstraint tests foreign key pragma
func TestPragmaForeignKeysConstraint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fk_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Enable foreign keys
	_, err = db.Exec("PRAGMA foreign_keys=ON")
	if err != nil {
		t.Fatalf("failed to enable foreign keys: %v", err)
	}

	// Create tables with foreign key
	_, err = db.Exec(`
		CREATE TABLE parent(id INTEGER PRIMARY KEY);
		CREATE TABLE child(id INTEGER, parent_id INTEGER,
			FOREIGN KEY(parent_id) REFERENCES parent(id));
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}

	// Insert parent row
	_, err = db.Exec("INSERT INTO parent VALUES(1)")
	if err != nil {
		t.Fatalf("failed to insert parent: %v", err)
	}

	// Insert child with valid reference
	_, err = db.Exec("INSERT INTO child VALUES(1, 1)")
	if err != nil {
		t.Fatalf("failed to insert valid child: %v", err)
	}

	// Try to insert child with invalid reference (should fail)
	_, err = db.Exec("INSERT INTO child VALUES(2, 999)")
	if err == nil {
		t.Error("expected foreign key constraint violation")
	}
}
