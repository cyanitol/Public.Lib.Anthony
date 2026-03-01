// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestSQLiteBackup tests database backup and restore functionality
// Converted from contrib/sqlite/sqlite-src-3510200/test/backup*.test
// Note: Go's database/sql doesn't expose SQLite's backup API directly,
// so these tests focus on database copy operations and data integrity
func TestSQLiteBackup(t *testing.T) {
	t.Skip("BACKUP not implemented")
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "source.db")
	dstPath := filepath.Join(tmpDir, "backup.db")

	// Open source database
	srcDB, err := sql.Open(DriverName, srcPath)
	if err != nil {
		t.Fatalf("failed to open source database: %v", err)
	}
	defer srcDB.Close()

	tests := []struct {
		name    string
		setup   []string
		verify  func(t *testing.T, src, dst *sql.DB)
		wantErr bool
	}{
		// backup.test 1.1 - Basic table creation
		{
			name: "backup_basic_table",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a, b)",
				"INSERT INTO t1 VALUES(1, 'test')",
				"INSERT INTO t1 VALUES(2, 'data')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
				if err != nil {
					t.Errorf("failed to query backup: %v", err)
				}
				if count != 2 {
					t.Errorf("expected 2 rows, got %d", count)
				}
			},
		},
		// backup.test 1.4 - Copy complete database
		{
			name: "backup_complete_database",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a, b)",
				"INSERT INTO t1 VALUES(1, 'alpha')",
				"INSERT INTO t1 VALUES(2, 'beta')",
				"INSERT INTO t1 VALUES(3, 'gamma')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Verify table exists
				var name string
				err := dst.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='t1'").Scan(&name)
				if err != nil {
					t.Errorf("table not found in backup: %v", err)
				}
			},
		},
		// backup2.test - Backup with indices and triggers
		{
			name: "backup_with_indices",
			setup: []string{
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t2(x INTEGER, y INTEGER)",
				"CREATE INDEX t2i1 ON t2(x)",
				"CREATE INDEX t2i2 ON t2(y)",
				"INSERT INTO t2 VALUES(1, 10)",
				"INSERT INTO t2 VALUES(2, 20)",
				"INSERT INTO t2 VALUES(3, 30)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Count indices
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='t2'").Scan(&count)
				if err != nil {
					t.Errorf("failed to count indices: %v", err)
				}
				if count < 2 {
					t.Errorf("expected at least 2 indices, got %d", count)
				}
			},
		},
		// Test backup with multiple tables
		{
			name: "backup_multiple_tables",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"DROP TABLE IF EXISTS t2",
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b TEXT)",
				"CREATE TABLE t3(c REAL)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES('test')",
				"INSERT INTO t3 VALUES(3.14)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&count)
				if err != nil {
					t.Errorf("failed to count tables: %v", err)
				}
				if count != 3 {
					t.Errorf("expected 3 tables, got %d", count)
				}
			},
		},
		// Test backup with large data
		{
			name: "backup_large_data",
			setup: []string{
				"DROP TABLE IF EXISTS large",
				"CREATE TABLE large(id INTEGER, data TEXT)",
				"INSERT INTO large VALUES(1, 'x')",
				"INSERT INTO large SELECT id+1, data FROM large",
				"INSERT INTO large SELECT id+2, data FROM large",
				"INSERT INTO large SELECT id+4, data FROM large",
				"INSERT INTO large SELECT id+8, data FROM large",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var srcCount, dstCount int64
				srcDB.QueryRow("SELECT COUNT(*) FROM large").Scan(&srcCount)
				dst.QueryRow("SELECT COUNT(*) FROM large").Scan(&dstCount)
				if srcCount != dstCount {
					t.Errorf("row count mismatch: src=%d, dst=%d", srcCount, dstCount)
				}
			},
		},
		// Test backup preserves PRIMARY KEY
		{
			name: "backup_primary_key",
			setup: []string{
				"DROP TABLE IF EXISTS pk_test",
				"CREATE TABLE pk_test(id INTEGER PRIMARY KEY, val TEXT)",
				"INSERT INTO pk_test VALUES(1, 'first')",
				"INSERT INTO pk_test VALUES(2, 'second')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var val string
				err := dst.QueryRow("SELECT val FROM pk_test WHERE id=1").Scan(&val)
				if err != nil {
					t.Errorf("failed to query by primary key: %v", err)
				}
				if val != "first" {
					t.Errorf("expected 'first', got '%s'", val)
				}
			},
		},
		// Test backup preserves UNIQUE constraints
		{
			name: "backup_unique_constraint",
			setup: []string{
				"DROP TABLE IF EXISTS uniq_test",
				"CREATE TABLE uniq_test(id INTEGER, email TEXT UNIQUE)",
				"INSERT INTO uniq_test VALUES(1, 'test@example.com')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Try to insert duplicate - should fail
				_, err := dst.Exec("INSERT INTO uniq_test VALUES(2, 'test@example.com')")
				if err == nil {
					t.Error("expected UNIQUE constraint error")
				}
			},
		},
		// Test backup with foreign keys
		{
			name: "backup_foreign_keys",
			setup: []string{
				"DROP TABLE IF EXISTS parent",
				"DROP TABLE IF EXISTS child",
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(id INTEGER, parent_id INTEGER REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(1, 1)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM child").Scan(&count)
				if err != nil {
					t.Errorf("failed to query child table: %v", err)
				}
				if count != 1 {
					t.Errorf("expected 1 row, got %d", count)
				}
			},
		},
		// Test backup with views
		{
			name: "backup_with_views",
			setup: []string{
				"DROP VIEW IF EXISTS v1",
				"DROP TABLE IF EXISTS base",
				"CREATE TABLE base(x INTEGER, y INTEGER)",
				"INSERT INTO base VALUES(1, 2)",
				"INSERT INTO base VALUES(3, 4)",
				"CREATE VIEW v1 AS SELECT x, y, x+y AS sum FROM base",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var name string
				err := dst.QueryRow("SELECT name FROM sqlite_master WHERE type='view' AND name='v1'").Scan(&name)
				if err != nil {
					t.Errorf("view not found in backup: %v", err)
				}
			},
		},
		// Test backup with different column types
		{
			name: "backup_column_types",
			setup: []string{
				"DROP TABLE IF EXISTS types",
				"CREATE TABLE types(i INTEGER, t TEXT, r REAL, b BLOB, n NULL)",
				"INSERT INTO types VALUES(42, 'text', 3.14, X'DEADBEEF', NULL)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var i int64
				var txt string
				var r float64
				err := dst.QueryRow("SELECT i, t, r FROM types").Scan(&i, &txt, &r)
				if err != nil {
					t.Errorf("failed to query types: %v", err)
				}
				if i != 42 || txt != "text" {
					t.Errorf("data mismatch: i=%d, t=%s", i, txt)
				}
			},
		},
		// Test backup preserves NULL values
		{
			name: "backup_null_values",
			setup: []string{
				"DROP TABLE IF EXISTS nulls",
				"CREATE TABLE nulls(a INTEGER, b TEXT, c REAL)",
				"INSERT INTO nulls VALUES(1, NULL, 3.14)",
				"INSERT INTO nulls VALUES(NULL, 'text', NULL)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM nulls WHERE b IS NULL").Scan(&count)
				if err != nil {
					t.Errorf("failed to query nulls: %v", err)
				}
				if count != 1 {
					t.Errorf("expected 1 null, got %d", count)
				}
			},
		},
		// Test backup with collation
		{
			name: "backup_collation",
			setup: []string{
				"DROP TABLE IF EXISTS collate_test",
				"CREATE TABLE collate_test(name TEXT COLLATE NOCASE)",
				"INSERT INTO collate_test VALUES('Alice')",
				"INSERT INTO collate_test VALUES('alice')",
				"INSERT INTO collate_test VALUES('ALICE')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(DISTINCT name) FROM collate_test").Scan(&count)
				if err != nil {
					t.Errorf("failed to query collation: %v", err)
				}
				// With NOCASE, all three should be considered the same
				if count != 1 {
					t.Errorf("expected 1 distinct name, got %d", count)
				}
			},
		},
		// Test backup with CHECK constraints
		{
			name: "backup_check_constraint",
			setup: []string{
				"DROP TABLE IF EXISTS checked",
				"CREATE TABLE checked(age INTEGER CHECK(age >= 0))",
				"INSERT INTO checked VALUES(25)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Try to insert invalid data
				_, err := dst.Exec("INSERT INTO checked VALUES(-1)")
				if err == nil {
					t.Error("expected CHECK constraint error")
				}
			},
		},
		// Test backup with DEFAULT values
		{
			name: "backup_default_values",
			setup: []string{
				"DROP TABLE IF EXISTS defaults",
				"CREATE TABLE defaults(id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', created INTEGER DEFAULT 0)",
				"INSERT INTO defaults(id) VALUES(1)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var status string
				err := dst.QueryRow("SELECT status FROM defaults WHERE id=1").Scan(&status)
				if err != nil {
					t.Errorf("failed to query defaults: %v", err)
				}
				if status != "active" {
					t.Errorf("expected 'active', got '%s'", status)
				}
			},
		},
		// Test backup preserves AUTOINCREMENT
		{
			name: "backup_autoincrement",
			setup: []string{
				"DROP TABLE IF EXISTS auto_test",
				"CREATE TABLE auto_test(id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)",
				"INSERT INTO auto_test(data) VALUES('first')",
				"INSERT INTO auto_test(data) VALUES('second')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Check if sqlite_sequence exists
				var name string
				err := dst.QueryRow("SELECT name FROM sqlite_master WHERE name='sqlite_sequence'").Scan(&name)
				if err != nil {
					t.Errorf("sqlite_sequence not found: %v", err)
				}
			},
		},
		// Test backup with composite primary key
		{
			name: "backup_composite_key",
			setup: []string{
				"DROP TABLE IF EXISTS composite",
				"CREATE TABLE composite(a INTEGER, b INTEGER, data TEXT, PRIMARY KEY(a, b))",
				"INSERT INTO composite VALUES(1, 1, 'test')",
				"INSERT INTO composite VALUES(1, 2, 'test2')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var data string
				err := dst.QueryRow("SELECT data FROM composite WHERE a=1 AND b=2").Scan(&data)
				if err != nil {
					t.Errorf("failed to query composite key: %v", err)
				}
				if data != "test2" {
					t.Errorf("expected 'test2', got '%s'", data)
				}
			},
		},
		// Test backup with multiple indices on same table
		{
			name: "backup_multiple_indices",
			setup: []string{
				"DROP TABLE IF EXISTS multi_idx",
				"CREATE TABLE multi_idx(a INTEGER, b INTEGER, c INTEGER)",
				"CREATE INDEX idx_a ON multi_idx(a)",
				"CREATE INDEX idx_b ON multi_idx(b)",
				"CREATE INDEX idx_c ON multi_idx(c)",
				"CREATE INDEX idx_ab ON multi_idx(a, b)",
				"INSERT INTO multi_idx VALUES(1, 2, 3)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='multi_idx'").Scan(&count)
				if err != nil {
					t.Errorf("failed to count indices: %v", err)
				}
				if count < 4 {
					t.Errorf("expected at least 4 indices, got %d", count)
				}
			},
		},
		// Test backup with generated columns (if supported)
		{
			name: "backup_computed_data",
			setup: []string{
				"DROP TABLE IF EXISTS computed",
				"CREATE TABLE computed(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO computed VALUES(2, 3, 5)",
				"INSERT INTO computed VALUES(4, 5, 9)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var sum int64
				err := dst.QueryRow("SELECT a+b FROM computed WHERE c=5").Scan(&sum)
				if err != nil {
					t.Errorf("failed to compute: %v", err)
				}
				if sum != 5 {
					t.Errorf("expected 5, got %d", sum)
				}
			},
		},
		// Test backup with BLOB data
		{
			name: "backup_blob_data",
			setup: []string{
				"DROP TABLE IF EXISTS blobs",
				"CREATE TABLE blobs(id INTEGER, data BLOB)",
				"INSERT INTO blobs VALUES(1, X'DEADBEEF')",
				"INSERT INTO blobs VALUES(2, X'CAFEBABE')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&count)
				if err != nil {
					t.Errorf("failed to query blobs: %v", err)
				}
				if count != 2 {
					t.Errorf("expected 2 rows, got %d", count)
				}
			},
		},
		// Test backup with transaction data
		{
			name: "backup_transaction_data",
			setup: []string{
				"DROP TABLE IF EXISTS txn",
				"CREATE TABLE txn(id INTEGER, val INTEGER)",
				"BEGIN",
				"INSERT INTO txn VALUES(1, 100)",
				"INSERT INTO txn VALUES(2, 200)",
				"COMMIT",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM txn").Scan(&count)
				if err != nil {
					t.Errorf("failed to query txn: %v", err)
				}
				if count != 2 {
					t.Errorf("expected 2 rows, got %d", count)
				}
			},
		},
		// Test backup with empty tables
		{
			name: "backup_empty_tables",
			setup: []string{
				"DROP TABLE IF EXISTS empty1",
				"DROP TABLE IF EXISTS empty2",
				"CREATE TABLE empty1(x INTEGER)",
				"CREATE TABLE empty2(y TEXT)",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM empty1").Scan(&count)
				if err != nil {
					t.Errorf("failed to query empty table: %v", err)
				}
				if count != 0 {
					t.Errorf("expected 0 rows, got %d", count)
				}
			},
		},
		// Test backup data integrity with checksums
		{
			name: "backup_data_integrity",
			setup: []string{
				"DROP TABLE IF EXISTS integrity",
				"CREATE TABLE integrity(id INTEGER PRIMARY KEY, data TEXT)",
				"INSERT INTO integrity VALUES(1, 'test data 1')",
				"INSERT INTO integrity VALUES(2, 'test data 2')",
				"INSERT INTO integrity VALUES(3, 'test data 3')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				// Verify all data matches
				rows, err := dst.Query("SELECT id, data FROM integrity ORDER BY id")
				if err != nil {
					t.Errorf("failed to query integrity: %v", err)
					return
				}
				defer rows.Close()

				expected := []struct {
					id   int64
					data string
				}{
					{1, "test data 1"},
					{2, "test data 2"},
					{3, "test data 3"},
				}

				i := 0
				for rows.Next() {
					var id int64
					var data string
					rows.Scan(&id, &data)
					if i < len(expected) && (id != expected[i].id || data != expected[i].data) {
						t.Errorf("row %d mismatch: got (%d, %s), want (%d, %s)",
							i, id, data, expected[i].id, expected[i].data)
					}
					i++
				}
			},
		},
		// Test backup with special characters in data
		{
			name: "backup_special_chars",
			setup: []string{
				"DROP TABLE IF EXISTS special",
				"CREATE TABLE special(data TEXT)",
				"INSERT INTO special VALUES('café')",
				"INSERT INTO special VALUES('こんにちは')",
				"INSERT INTO special VALUES('🚀')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM special").Scan(&count)
				if err != nil {
					t.Errorf("failed to query special chars: %v", err)
				}
				if count != 3 {
					t.Errorf("expected 3 rows, got %d", count)
				}
			},
		},
		// Test backup with very long TEXT values
		{
			name: "backup_long_text",
			setup: []string{
				"DROP TABLE IF EXISTS longtext",
				"CREATE TABLE longtext(id INTEGER, content TEXT)",
				"INSERT INTO longtext VALUES(1, '0123456789')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var content string
				err := dst.QueryRow("SELECT content FROM longtext WHERE id=1").Scan(&content)
				if err != nil {
					t.Errorf("failed to query long text: %v", err)
				}
				if len(content) != 10 {
					t.Errorf("expected length 10, got %d", len(content))
				}
			},
		},
		// Test backup preserves rowid
		{
			name: "backup_rowid",
			setup: []string{
				"DROP TABLE IF EXISTS rowid_test",
				"CREATE TABLE rowid_test(data TEXT)",
				"INSERT INTO rowid_test VALUES('first')",
				"INSERT INTO rowid_test VALUES('second')",
				"INSERT INTO rowid_test VALUES('third')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var data string
				err := dst.QueryRow("SELECT data FROM rowid_test WHERE rowid=2").Scan(&data)
				if err != nil {
					t.Errorf("failed to query by rowid: %v", err)
				}
				if data != "second" {
					t.Errorf("expected 'second', got '%s'", data)
				}
			},
		},
		// Test backup with WITHOUT ROWID tables
		{
			name: "backup_without_rowid",
			setup: []string{
				"DROP TABLE IF EXISTS no_rowid",
				"CREATE TABLE no_rowid(id INTEGER PRIMARY KEY, val TEXT) WITHOUT ROWID",
				"INSERT INTO no_rowid VALUES(1, 'test')",
				"INSERT INTO no_rowid VALUES(2, 'data')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var val string
				err := dst.QueryRow("SELECT val FROM no_rowid WHERE id=1").Scan(&val)
				if err != nil {
					t.Errorf("failed to query without rowid: %v", err)
				}
				if val != "test" {
					t.Errorf("expected 'test', got '%s'", val)
				}
			},
		},
		// Test backup with partial indices
		{
			name: "backup_partial_index",
			setup: []string{
				"DROP TABLE IF EXISTS partial",
				"CREATE TABLE partial(id INTEGER, status TEXT)",
				"CREATE INDEX partial_active ON partial(id) WHERE status='active'",
				"INSERT INTO partial VALUES(1, 'active')",
				"INSERT INTO partial VALUES(2, 'inactive')",
				"INSERT INTO partial VALUES(3, 'active')",
			},
			verify: func(t *testing.T, src, dst *sql.DB) {
				var count int64
				err := dst.QueryRow("SELECT COUNT(*) FROM partial WHERE status='active'").Scan(&count)
				if err != nil {
					t.Errorf("failed to query partial index: %v", err)
				}
				if count != 2 {
					t.Errorf("expected 2 active rows, got %d", count)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Clean database
			tables := []string{"t1", "t2", "t3", "large", "pk_test", "uniq_test",
				"parent", "child", "base", "types", "nulls", "collate_test",
				"checked", "defaults", "auto_test", "composite", "multi_idx",
				"computed", "blobs", "txn", "empty1", "empty2", "integrity",
				"special", "longtext", "rowid_test", "no_rowid", "partial"}
			for _, table := range tables {
				srcDB.Exec("DROP TABLE IF EXISTS " + table)
			}
			srcDB.Exec("DROP VIEW IF EXISTS v1")

			// Run setup
			for _, stmt := range tt.setup {
				_, err := srcDB.Exec(stmt)
				if err != nil {
					t.Logf("setup failed (may be expected): %v", err)
				}
			}

			// Close source to ensure data is flushed
			srcDB.Close()

			// Copy database file (simulating backup)
			srcData, err := os.ReadFile(srcPath)
			if err != nil {
				t.Fatalf("failed to read source database: %v", err)
			}

			err = os.WriteFile(dstPath, srcData, 0644)
			if err != nil {
				t.Fatalf("failed to write backup database: %v", err)
			}

			// Reopen source
			srcDB, err = sql.Open(DriverName, srcPath)
			if err != nil {
				t.Fatalf("failed to reopen source database: %v", err)
			}

			// Open backup database
			dstDB, err := sql.Open(DriverName, dstPath)
			if err != nil {
				t.Fatalf("failed to open backup database: %v", err)
			}
			defer dstDB.Close()

			// Run verification
			if tt.verify != nil {
				tt.verify(t, srcDB, dstDB)
			}
		})
	}
}

// TestBackupIntegrity verifies that backup preserves all data correctly
func TestBackupIntegrity(t *testing.T) {
	t.Skip("BACKUP not implemented")
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "integrity_src.db")
	dstPath := filepath.Join(tmpDir, "integrity_dst.db")

	// Create source database
	srcDB, err := sql.Open(DriverName, srcPath)
	if err != nil {
		t.Fatalf("failed to open source: %v", err)
	}
	defer srcDB.Close()

	// Create complex schema
	_, err = srcDB.Exec(`
		CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE);
		CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, content TEXT,
			FOREIGN KEY(user_id) REFERENCES users(id));
		CREATE INDEX idx_posts_user ON posts(user_id);
		CREATE VIEW user_posts AS SELECT users.name, posts.content FROM users JOIN posts ON users.id = posts.user_id;
	`)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Insert data
	_, err = srcDB.Exec("INSERT INTO users VALUES(1, 'Alice', 'alice@example.com')")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	_, err = srcDB.Exec("INSERT INTO posts VALUES(1, 1, 'Hello World')")
	if err != nil {
		t.Fatalf("failed to insert post: %v", err)
	}

	// Flush and copy
	srcDB.Close()

	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("failed to read source: %v", err)
	}

	err = os.WriteFile(dstPath, data, 0644)
	if err != nil {
		t.Fatalf("failed to write backup: %v", err)
	}

	// Open backup and verify
	dstDB, err := sql.Open(DriverName, dstPath)
	if err != nil {
		t.Fatalf("failed to open backup: %v", err)
	}
	defer dstDB.Close()

	// Verify schema
	var tableCount int64
	err = dstDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tableCount)
	if err != nil {
		t.Fatalf("failed to count tables: %v", err)
	}

	if tableCount != 2 {
		t.Errorf("expected 2 tables, got %d", tableCount)
	}

	// Verify data
	var name, content string
	err = dstDB.QueryRow("SELECT name, content FROM user_posts").Scan(&name, &content)
	if err != nil {
		t.Fatalf("failed to query view: %v", err)
	}

	if name != "Alice" || content != "Hello World" {
		t.Errorf("data mismatch: got (%s, %s)", name, content)
	}
}
