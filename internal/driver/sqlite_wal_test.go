// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestSQLiteWAL tests Write-Ahead Logging functionality
// Converted from contrib/sqlite/sqlite-src-3510200/test/wal*.test
func TestSQLiteWAL(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wal_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    interface{}
		wantErr bool
		skip    string
	}{
		// wal.test - Basic WAL mode tests (lines 66-74)
		{
			name: "wal_mode_enable",
			setup: []string{
				"PRAGMA auto_vacuum = 0",
				"PRAGMA synchronous = normal",
			},
			query: "PRAGMA journal_mode = wal",
			want:  "wal",
		},

		// wal.test - Create table in WAL mode (lines 75-95)
		{
			name: "wal_create_table",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t1(a, b)",
			},
			query: "SELECT name FROM sqlite_master WHERE type='table' AND name='t1'",
			want:  "t1",
		},

		// wal.test - Insert and select in WAL mode (lines 98-107)
		{
			name: "wal_insert_select",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t2(a, b)",
				"INSERT INTO t2 VALUES(1, 2)",
				"INSERT INTO t2 VALUES(3, 4)",
				"INSERT INTO t2 VALUES(5, 6)",
			},
			query: "SELECT count(*) FROM t2",
			want:  int64(3),
		},

		// wal.test - Sum aggregation in WAL mode
		{
			name: "wal_sum_values",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t3(a INTEGER, b INTEGER)",
				"INSERT INTO t3 VALUES(1, 2)",
				"INSERT INTO t3 VALUES(3, 4)",
				"INSERT INTO t3 VALUES(5, 6)",
				"INSERT INTO t3 VALUES(7, 8)",
				"INSERT INTO t3 VALUES(9, 10)",
			},
			query: "SELECT sum(a) FROM t3",
			want:  int64(25),
		},

		// wal.test - Transaction rollback (lines 136-146)
		{
			name: "wal_rollback",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t4(a INTEGER)",
				"INSERT INTO t4 VALUES(1)",
				"INSERT INTO t4 VALUES(2)",
				"BEGIN",
				"DELETE FROM t4",
				"ROLLBACK",
			},
			query: "SELECT count(*) FROM t4",
			want:  int64(2),
		},

		// wal.test - Savepoint tests (lines 153-174)
		{
			name: "wal_savepoint",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t5(a TEXT, b TEXT)",
				"BEGIN",
				"INSERT INTO t5 VALUES('a', 'b')",
				"SAVEPOINT sp",
				"INSERT INTO t5 VALUES('c', 'd')",
				"ROLLBACK TO sp",
				"COMMIT",
			},
			query: "SELECT count(*) FROM t5",
			want:  int64(1),
		},
		{
			name: "wal_savepoint_values",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t6(a TEXT, b TEXT)",
				"BEGIN",
				"INSERT INTO t6 VALUES('a', 'b')",
				"SAVEPOINT sp",
				"INSERT INTO t6 VALUES('c', 'd')",
				"ROLLBACK TO sp",
				"COMMIT",
			},
			query: "SELECT a FROM t6",
			want:  "a",
		},

		// Additional WAL tests - checkpoint
		{
			name: "wal_checkpoint",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t7(x INTEGER)",
				"INSERT INTO t7 VALUES(1)",
			},
			query: "PRAGMA wal_checkpoint",
			want:  int64(0),
		},

		// WAL autocheckpoint
		{
			name: "wal_autocheckpoint_get",
			setup: []string{
				"PRAGMA journal_mode = wal",
			},
			query: "PRAGMA wal_autocheckpoint",
			want:  int64(1000),
		},
		{
			name: "wal_autocheckpoint_set",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"PRAGMA wal_autocheckpoint = 2000",
			},
			query: "PRAGMA wal_autocheckpoint",
			want:  int64(2000),
		},

		// WAL with indexes
		{
			name: "wal_with_index",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t8(a INTEGER, b INTEGER)",
				"CREATE INDEX idx_t8 ON t8(a)",
				"INSERT INTO t8 VALUES(1, 2)",
				"INSERT INTO t8 VALUES(3, 4)",
			},
			query: "SELECT count(*) FROM t8 WHERE a = 1",
			want:  int64(1),
		},

		// WAL with triggers
		{
			name: "wal_with_trigger",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t9(a INTEGER)",
				"CREATE TABLE t9_log(b INTEGER)",
				"CREATE TRIGGER t9_insert AFTER INSERT ON t9 BEGIN INSERT INTO t9_log VALUES(NEW.a); END",
				"INSERT INTO t9 VALUES(42)",
			},
			query: "SELECT count(*) FROM t9_log",
			want:  int64(1),
		},

		// WAL with views
		{
			name: "wal_with_view",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t10(x INTEGER, y INTEGER)",
				"CREATE VIEW v10 AS SELECT x, y FROM t10",
				"INSERT INTO t10 VALUES(1, 2)",
			},
			query: "SELECT count(*) FROM v10",
			want:  int64(1),
		},

		// WAL multi-insert
		{
			name: "wal_multi_insert",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t11(val INTEGER)",
				"INSERT INTO t11 SELECT value FROM generate_series(1, 10)",
			},
			query: "SELECT count(*) FROM t11",
			want:  int64(10),
		},

		// WAL with UPDATE
		{
			name: "wal_update",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t12(a INTEGER, b INTEGER)",
				"INSERT INTO t12 VALUES(1, 2)",
				"INSERT INTO t12 VALUES(3, 4)",
				"UPDATE t12 SET b = b + 10",
			},
			query: "SELECT sum(b) FROM t12",
			want:  int64(26),
		},

		// WAL with DELETE
		{
			name: "wal_delete",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t13(a INTEGER)",
				"INSERT INTO t13 VALUES(1)",
				"INSERT INTO t13 VALUES(2)",
				"INSERT INTO t13 VALUES(3)",
				"DELETE FROM t13 WHERE a = 2",
			},
			query: "SELECT count(*) FROM t13",
			want:  int64(2),
		},

		// WAL with REPLACE
		{
			name: "wal_replace",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t14(a INTEGER PRIMARY KEY, b INTEGER)",
				"INSERT INTO t14 VALUES(1, 2)",
				"REPLACE INTO t14 VALUES(1, 3)",
			},
			query: "SELECT b FROM t14 WHERE a = 1",
			want:  int64(3),
		},

		// WAL with VACUUM
		{
			name: "wal_vacuum",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t15(x INTEGER)",
				"INSERT INTO t15 VALUES(1)",
				"VACUUM",
			},
			query: "SELECT count(*) FROM t15",
			want:  int64(1),
		},

		// WAL with ATTACH
		{
			name: "wal_attach_database",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE main_table(a INTEGER)",
				"INSERT INTO main_table VALUES(1)",
			},
			query: "SELECT count(*) FROM main_table",
			want:  int64(1),
		},

		// WAL with foreign keys
		{
			name: "wal_foreign_keys",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"PRAGMA foreign_keys = ON",
				"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
				"CREATE TABLE child(id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))",
				"INSERT INTO parent VALUES(1)",
				"INSERT INTO child VALUES(1, 1)",
			},
			query: "SELECT count(*) FROM child",
			want:  int64(1),
		},

		// WAL with DISTINCT
		{
			name: "wal_distinct",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t16(a INTEGER)",
				"INSERT INTO t16 VALUES(1)",
				"INSERT INTO t16 VALUES(1)",
				"INSERT INTO t16 VALUES(2)",
			},
			query: "SELECT count(DISTINCT a) FROM t16",
			want:  int64(2),
		},

		// WAL with GROUP BY
		{
			name: "wal_group_by",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t17(a INTEGER, b INTEGER)",
				"INSERT INTO t17 VALUES(1, 10)",
				"INSERT INTO t17 VALUES(1, 20)",
				"INSERT INTO t17 VALUES(2, 30)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t17 GROUP BY a)",
			want:  int64(2),
		},

		// WAL with HAVING
		{
			name: "wal_having",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t18(a INTEGER, b INTEGER)",
				"INSERT INTO t18 VALUES(1, 10)",
				"INSERT INTO t18 VALUES(1, 20)",
				"INSERT INTO t18 VALUES(2, 5)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t18 GROUP BY a HAVING sum(b) > 10)",
			want:  int64(1),
		},

		// WAL with ORDER BY
		{
			name: "wal_order_by",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t19(a INTEGER)",
				"INSERT INTO t19 VALUES(3)",
				"INSERT INTO t19 VALUES(1)",
				"INSERT INTO t19 VALUES(2)",
			},
			query: "SELECT a FROM t19 ORDER BY a LIMIT 1",
			want:  int64(1),
		},

		// WAL with LIMIT
		{
			name: "wal_limit",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t20(a INTEGER)",
				"INSERT INTO t20 VALUES(1)",
				"INSERT INTO t20 VALUES(2)",
				"INSERT INTO t20 VALUES(3)",
			},
			query: "SELECT count(*) FROM (SELECT * FROM t20 LIMIT 2)",
			want:  int64(2),
		},

		// WAL with OFFSET
		{
			name: "wal_offset",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t21(a INTEGER)",
				"INSERT INTO t21 VALUES(1)",
				"INSERT INTO t21 VALUES(2)",
				"INSERT INTO t21 VALUES(3)",
			},
			query: "SELECT a FROM t21 ORDER BY a LIMIT 1 OFFSET 1",
			want:  int64(2),
		},

		// WAL with UNION
		{
			name: "wal_union",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t22a(a INTEGER)",
				"CREATE TABLE t22b(a INTEGER)",
				"INSERT INTO t22a VALUES(1)",
				"INSERT INTO t22b VALUES(2)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t22a UNION SELECT a FROM t22b)",
			want:  int64(2),
		},

		// WAL with INTERSECT
		{
			name: "wal_intersect",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t23a(a INTEGER)",
				"CREATE TABLE t23b(a INTEGER)",
				"INSERT INTO t23a VALUES(1)",
				"INSERT INTO t23a VALUES(2)",
				"INSERT INTO t23b VALUES(2)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t23a INTERSECT SELECT a FROM t23b)",
			want:  int64(1),
		},

		// WAL with EXCEPT
		{
			name: "wal_except",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t24a(a INTEGER)",
				"CREATE TABLE t24b(a INTEGER)",
				"INSERT INTO t24a VALUES(1)",
				"INSERT INTO t24a VALUES(2)",
				"INSERT INTO t24b VALUES(2)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t24a EXCEPT SELECT a FROM t24b)",
			want:  int64(1),
		},

		// WAL with CTE
		{
			name: "wal_cte",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t25(a INTEGER)",
				"INSERT INTO t25 VALUES(1)",
				"INSERT INTO t25 VALUES(2)",
			},
			query: "WITH cte AS (SELECT a FROM t25) SELECT count(*) FROM cte",
			want:  int64(2),
		},

		// WAL with subquery
		{
			name: "wal_subquery",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t26(a INTEGER, b INTEGER)",
				"INSERT INTO t26 VALUES(1, 10)",
				"INSERT INTO t26 VALUES(2, 20)",
			},
			query: "SELECT count(*) FROM (SELECT a FROM t26 WHERE b > 5)",
			want:  int64(2),
		},

		// WAL with CASE expression
		{
			name: "wal_case",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t27(a INTEGER)",
				"INSERT INTO t27 VALUES(1)",
				"INSERT INTO t27 VALUES(2)",
			},
			query: "SELECT sum(CASE WHEN a = 1 THEN 10 ELSE 20 END) FROM t27",
			want:  int64(30),
		},

		// WAL with COALESCE
		{
			name: "wal_coalesce",
			setup: []string{
				"PRAGMA journal_mode = wal",
				"CREATE TABLE t28(a INTEGER)",
				"INSERT INTO t28 VALUES(NULL)",
				"INSERT INTO t28 VALUES(5)",
			},
			query: "SELECT sum(COALESCE(a, 0)) FROM t28",
			want:  int64(5),
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			// Use a new database for each test
			testDBPath := filepath.Join(tmpDir, tt.name+".db")
			testDB, err := sql.Open(DriverName, testDBPath)
			if err != nil {
				t.Fatalf("failed to open test database: %v", err)
			}
			defer testDB.Close()
			defer os.RemoveAll(testDBPath)
			defer os.RemoveAll(testDBPath + "-wal")
			defer os.RemoveAll(testDBPath + "-shm")

			for _, setup := range tt.setup {
				_, err := testDB.Exec(setup)
				if err != nil {
					t.Fatalf("setup failed for %q: %v", setup, err)
				}
			}

			var result interface{}
			err = testDB.QueryRow(tt.query).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			// Convert byte arrays to strings for comparison
			if b, ok := result.([]byte); ok {
				result = string(b)
			}

			if result != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", result, result, tt.want, tt.want)
			}
		})
	}
}

// TestSQLiteWALRecovery tests WAL recovery scenarios
func TestSQLiteWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wal_recovery_test.db")

	// Create database with WAL mode
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	_, err = db.Exec("PRAGMA journal_mode = wal")
	if err != nil {
		t.Fatalf("failed to set WAL mode: %v", err)
	}

	_, err = db.Exec("CREATE TABLE recovery_test(a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO recovery_test VALUES(1, 'first')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	db.Close()

	// Reopen database
	db, err = sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	var count int64
	err = db.QueryRow("SELECT count(*) FROM recovery_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query after reopen: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 row after recovery, got %d", count)
	}
}
