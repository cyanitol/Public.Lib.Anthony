package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteRowid tests the rowid, _rowid_, and oid columns
// Converted from contrib/sqlite/sqlite-src-3510200/test/rowid.test
func TestSQLiteRowid(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Basic rowid functionality (rowid-1.*)
	t.Run("basic_rowid_access", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t1(x int, y int);
			INSERT INTO t1 VALUES(1, 2);
			INSERT INTO t1 VALUES(3, 4);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Test selecting rowid, oid, _rowid_
		var rowid1, rowid2 int64
		err = db.QueryRow("SELECT rowid FROM t1 WHERE x = 1").Scan(&rowid1)
		if err != nil {
			t.Fatalf("failed to get rowid: %v", err)
		}

		err = db.QueryRow("SELECT oid FROM t1 WHERE x = 1").Scan(&rowid2)
		if err != nil {
			t.Fatalf("failed to get oid: %v", err)
		}

		if rowid1 != rowid2 {
			t.Errorf("rowid and oid should be equal: %d != %d", rowid1, rowid2)
		}

		var rowid3 int64
		err = db.QueryRow("SELECT _rowid_ FROM t1 WHERE x = 1").Scan(&rowid3)
		if err != nil {
			t.Fatalf("failed to get _rowid_: %v", err)
		}

		if rowid1 != rowid3 {
			t.Errorf("rowid and _rowid_ should be equal: %d != %d", rowid1, rowid3)
		}

		// Test using rowid in WHERE clause
		var x int
		err = db.QueryRow("SELECT x FROM t1 WHERE rowid = ?", rowid1).Scan(&x)
		if err != nil {
			t.Fatalf("failed to query by rowid: %v", err)
		}
		if x != 1 {
			t.Errorf("expected x=1, got x=%d", x)
		}
	})

	// Test 2: Inserting and updating rowid (rowid-2.*)
	t.Run("insert_update_rowid", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec("DELETE FROM t1")
		if err != nil {
			t.Fatalf("failed to delete: %v", err)
		}

		// Insert with explicit rowid
		_, err = db.Exec("INSERT INTO t1(rowid, x, y) VALUES(1234, 5, 6)")
		if err != nil {
			t.Fatalf("failed to insert with rowid: %v", err)
		}

		var rowid int64
		var x, y int
		err = db.QueryRow("SELECT rowid, x, y FROM t1 WHERE x = 5").Scan(&rowid, &x, &y)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if rowid != 1234 || x != 5 || y != 6 {
			t.Errorf("expected (1234, 5, 6), got (%d, %d, %d)", rowid, x, y)
		}

		// Update rowid
		_, err = db.Exec("UPDATE t1 SET rowid = 12345 WHERE x = 5")
		if err != nil {
			t.Fatalf("failed to update rowid: %v", err)
		}

		err = db.QueryRow("SELECT rowid FROM t1 WHERE x = 5").Scan(&rowid)
		if err != nil {
			t.Fatalf("failed to query after update: %v", err)
		}
		if rowid != 12345 {
			t.Errorf("expected rowid 12345, got %d", rowid)
		}

		// Insert with oid
		_, err = db.Exec("INSERT INTO t1(y, x, oid) VALUES(8, 7, 1235)")
		if err != nil {
			t.Fatalf("failed to insert with oid: %v", err)
		}

		err = db.QueryRow("SELECT oid FROM t1 WHERE x = 7").Scan(&rowid)
		if err != nil {
			t.Fatalf("failed to query oid: %v", err)
		}
		if rowid != 1235 {
			t.Errorf("expected oid 1235, got %d", rowid)
		}

		// Insert with _rowid_
		_, err = db.Exec("INSERT INTO t1(x, _rowid_, y) VALUES(9, 1236, 10)")
		if err != nil {
			t.Fatalf("failed to insert with _rowid_: %v", err)
		}

		err = db.QueryRow("SELECT _rowid_ FROM t1 WHERE x = 9").Scan(&rowid)
		if err != nil {
			t.Fatalf("failed to query _rowid_: %v", err)
		}
		if rowid != 1236 {
			t.Errorf("expected _rowid_ 1236, got %d", rowid)
		}
	})

	// Test 3: User-defined column named rowid (rowid-3.*)
	t.Run("user_defined_rowid_column", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t2(rowid int, x int, y int);
			INSERT INTO t2 VALUES(0, 2, 3);
			INSERT INTO t2 VALUES(4, 5, 6);
			INSERT INTO t2 VALUES(7, 8, 9);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// When a table has a column named 'rowid', it shadows the special rowid
		rows, err := db.Query("SELECT rowid, x FROM t2 ORDER BY rowid")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		defer rows.Close()

		expected := []struct {
			rowid int
			x     int
		}{
			{0, 2},
			{4, 5},
			{7, 8},
		}

		i := 0
		for rows.Next() {
			var rowid, x int
			if err := rows.Scan(&rowid, &x); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			if i >= len(expected) {
				t.Fatalf("too many rows")
			}
			if rowid != expected[i].rowid || x != expected[i].x {
				t.Errorf("row %d: expected (%d, %d), got (%d, %d)",
					i, expected[i].rowid, expected[i].x, rowid, x)
			}
			i++
		}
	})

	// Test 4: Joins using rowid (rowid-4.*)
	t.Run("joins_with_rowid", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t3(a INTEGER);
			CREATE TABLE t4(b INTEGER);
		`)
		if err != nil {
			t.Fatalf("failed to create tables: %v", err)
		}

		// Insert 10 rows into t3
		for i := 1; i <= 10; i++ {
			_, err = db.Exec("INSERT INTO t3(a) VALUES(?)", i*i)
			if err != nil {
				t.Fatalf("failed to insert into t3: %v", err)
			}
		}

		// Insert into t4 using rowids from t3
		_, err = db.Exec("INSERT INTO t4 SELECT _rowid_ FROM t3 WHERE a = 16")
		if err != nil {
			t.Fatalf("failed to insert into t4: %v", err)
		}

		// Join tables on rowid
		var result int
		err = db.QueryRow("SELECT t3.a FROM t3, t4 WHERE t3.rowid = t4.b").Scan(&result)
		if err != nil {
			t.Fatalf("failed to join: %v", err)
		}
		if result != 16 {
			t.Errorf("expected 16, got %d", result)
		}
	})
}

// TestRowidWithIntegerPrimaryKey tests INTEGER PRIMARY KEY behavior
// Based on rowid-7.* tests
func TestRowidWithIntegerPrimaryKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_ipk_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("integer_primary_key_auto_increment", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t5(a INTEGER PRIMARY KEY, b);
			INSERT INTO t5(b) VALUES(55);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// First auto-generated rowid should be 1
		var a int64
		err = db.QueryRow("SELECT a FROM t5 WHERE b = 55").Scan(&a)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if a != 1 {
			t.Errorf("expected auto-generated a=1, got a=%d", a)
		}

		// Insert another row
		_, err = db.Exec("INSERT INTO t5(b) VALUES(66)")
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		err = db.QueryRow("SELECT a FROM t5 WHERE b = 66").Scan(&a)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if a != 2 {
			t.Errorf("expected auto-generated a=2, got a=%d", a)
		}

		// Insert with explicit large value
		_, err = db.Exec("INSERT INTO t5(a, b) VALUES(1000000, 77)")
		if err != nil {
			t.Fatalf("failed to insert with explicit a: %v", err)
		}

		// Next auto-generated should be 1000001
		_, err = db.Exec("INSERT INTO t5(b) VALUES(88)")
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		err = db.QueryRow("SELECT a FROM t5 WHERE b = 88").Scan(&a)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if a != 1000001 {
			t.Errorf("expected auto-generated a=1000001, got a=%d", a)
		}
	})
}

// TestRowidComparisons tests rowid comparisons with different types
// Based on rowid-9.* and rowid-10.* tests
func TestRowidComparisons(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_compare_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("rowid_float_comparison", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t6(a INTEGER PRIMARY KEY, b);
			INSERT INTO t6 VALUES(123, 'x');
			INSERT INTO t6 VALUES(124, 'y');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Test comparisons with float values
		var count int64

		// a < 123.5 should include only 123
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a < 123.5").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 row where a < 123.5, got %d", count)
		}

		// a < 124.5 should include both
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a < 124.5").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 rows where a < 124.5, got %d", count)
		}

		// a > 123.5 should include only 124
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a > 123.5").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 row where a > 123.5, got %d", count)
		}

		// a == 123.5 should match nothing
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a == 123.5").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 rows where a == 123.5, got %d", count)
		}

		// a == 123.0 should match 123
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a == 123.0").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 row where a == 123.0, got %d", count)
		}

		// Range query
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE a > 100.5 AND a < 200.5").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 rows in range, got %d", count)
		}
	})

	t.Run("rowid_string_comparison", func(t *testing.T) {
		t.Parallel()
		// rowid compared to string should handle type mismatch
		var count int64

		// rowid > 'abc' should return no rows (string is greater in SQLite's type ordering)
		err := db.QueryRow("SELECT COUNT(*) FROM t6 WHERE rowid > 'abc'").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 rows where rowid > 'abc', got %d", count)
		}

		// rowid < 'abc' should return all rows
		err = db.QueryRow("SELECT COUNT(*) FROM t6 WHERE rowid < 'abc'").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 rows where rowid < 'abc', got %d", count)
		}
	})
}

// TestRowidRangeQueries tests rowid with range queries
// Based on rowid-10.* tests
func TestRowidRangeQueries(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_range_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("rowid_range_with_floats", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t7(a);
			INSERT INTO t7(a) VALUES(1);
			INSERT INTO t7(a) SELECT a+1 FROM t7;
			INSERT INTO t7(a) SELECT a+2 FROM t7;
			INSERT INTO t7(a) SELECT a+4 FROM t7;
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Should have 8 rows with rowid 1-8
		tests := []struct {
			name  string
			where string
			want  int64
		}{
			{"rowid >= 5.5", "rowid >= 5.5", 3}, // 6, 7, 8
			{"rowid >= 5.0", "rowid >= 5.0", 4}, // 5, 6, 7, 8
			{"rowid > 5.5", "rowid > 5.5", 3},   // 6, 7, 8
			{"rowid > 5.0", "rowid > 5.0", 3},   // 6, 7, 8
			{"rowid <= 5.5", "rowid <= 5.5", 5}, // 1, 2, 3, 4, 5
			{"rowid < 5.5", "rowid < 5.5", 5},   // 1, 2, 3, 4, 5
			{"5.5 <= rowid", "5.5 <= rowid", 3}, // 6, 7, 8
			{"5.5 < rowid", "5.5 < rowid", 3},   // 6, 7, 8
			{"5.5 >= rowid", "5.5 >= rowid", 5}, // 1, 2, 3, 4, 5
			{"5.5 > rowid", "5.5 > rowid", 5},   // 1, 2, 3, 4, 5
		}

		for _, tt := range tests {
			tt := tt  // Capture range variable
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var count int64
				query := "SELECT COUNT(*) FROM t7 WHERE " + tt.where
				err := db.QueryRow(query).Scan(&count)
				if err != nil {
					t.Fatalf("query failed: %v", err)
				}
				if count != tt.want {
					t.Errorf("expected %d rows, got %d", tt.want, count)
				}
			})
		}
	})
}

// TestRowidWithNegativeValues tests rowid with negative values
// Based on rowid-10.30+ tests
func TestRowidWithNegativeValues(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_negative_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("negative_rowid_comparisons", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t8(a);
			INSERT INTO t8(rowid, a) VALUES(-8, 8);
			INSERT INTO t8(rowid, a) VALUES(-7, 7);
			INSERT INTO t8(rowid, a) VALUES(-6, 6);
			INSERT INTO t8(rowid, a) VALUES(-5, 5);
			INSERT INTO t8(rowid, a) VALUES(-4, 4);
			INSERT INTO t8(rowid, a) VALUES(-3, 3);
			INSERT INTO t8(rowid, a) VALUES(-2, 2);
			INSERT INTO t8(rowid, a) VALUES(-1, 1);
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		tests := []struct {
			name  string
			where string
			want  int64
		}{
			{"rowid >= -5.5", "rowid >= -5.5", 5}, // -5, -4, -3, -2, -1
			{"rowid >= -5.0", "rowid >= -5.0", 5}, // -5, -4, -3, -2, -1
			{"rowid > -5.5", "rowid > -5.5", 5},   // -5, -4, -3, -2, -1
			{"rowid > -5.0", "rowid > -5.0", 4},   // -4, -3, -2, -1
			{"rowid <= -5.5", "rowid <= -5.5", 3}, // -8, -7, -6
			{"rowid < -5.5", "rowid < -5.5", 3},   // -8, -7, -6
			{"-5.5 <= rowid", "-5.5 <= rowid", 5}, // -5, -4, -3, -2, -1
			{"-5.5 < rowid", "-5.5 < rowid", 5},   // -5, -4, -3, -2, -1
			{"-5.5 >= rowid", "-5.5 >= rowid", 3}, // -8, -7, -6
			{"-5.5 > rowid", "-5.5 > rowid", 3},   // -8, -7, -6
		}

		for _, tt := range tests {
			tt := tt  // Capture range variable
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				var count int64
				query := "SELECT COUNT(*) FROM t8 WHERE " + tt.where
				err := db.QueryRow(query).Scan(&count)
				if err != nil {
					t.Fatalf("query failed: %v", err)
				}
				if count != tt.want {
					t.Errorf("expected %d rows, got %d", tt.want, count)
				}
			})
		}
	})
}

// TestRowidOrdering tests ORDER BY with rowid
func TestRowidOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_order_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("order_by_rowid_asc", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t9(value TEXT);
			INSERT INTO t9 VALUES('third');
			INSERT INTO t9 VALUES('first');
			INSERT INTO t9 VALUES('second');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		rows, err := db.Query("SELECT rowid, value FROM t9 ORDER BY rowid ASC")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		expected := []string{"third", "first", "second"}
		i := 0
		for rows.Next() {
			var rowid int64
			var value string
			if err := rows.Scan(&rowid, &value); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if i >= len(expected) {
				t.Fatalf("too many rows")
			}
			if value != expected[i] {
				t.Errorf("row %d: expected %q, got %q", i, expected[i], value)
			}
			i++
		}
	})

	t.Run("order_by_rowid_desc", func(t *testing.T) {
		t.Parallel()
		rows, err := db.Query("SELECT rowid, value FROM t9 ORDER BY rowid DESC")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		expected := []string{"second", "first", "third"}
		i := 0
		for rows.Next() {
			var rowid int64
			var value string
			if err := rows.Scan(&rowid, &value); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if i >= len(expected) {
				t.Fatalf("too many rows")
			}
			if value != expected[i] {
				t.Errorf("row %d: expected %q, got %q", i, expected[i], value)
			}
			i++
		}
	})
}

// TestRowidWithoutRowid tests tables without rowid
// Based on rowid-16.* tests
func TestRowidWithoutRowid(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_without_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("without_rowid_table", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t10(x INTEGER PRIMARY KEY, y TEXT) WITHOUT ROWID;
			INSERT INTO t10 VALUES(1, 'one');
			INSERT INTO t10 VALUES(2, 'two');
			INSERT INTO t10 VALUES(3, 'three');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Query using the primary key as if it were rowid
		var y string
		err = db.QueryRow("SELECT y FROM t10 WHERE x = 2").Scan(&y)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if y != "two" {
			t.Errorf("expected 'two', got %q", y)
		}

		// Count rows
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM t10").Scan(&count)
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}
	})
}

// TestRowidMaxValue tests behavior when approaching maximum rowid
func TestRowidMaxValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_max_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("large_rowid_values", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t11(x INTEGER PRIMARY KEY, y);
			INSERT INTO t11 VALUES(9223372036854775807, 'max');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		var y string
		err = db.QueryRow("SELECT y FROM t11 WHERE x = 9223372036854775807").Scan(&y)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if y != "max" {
			t.Errorf("expected 'max', got %q", y)
		}

		// Inserting NULL should generate a new rowid (will be random when max is taken)
		_, err = db.Exec("INSERT INTO t11 VALUES(NULL, 'auto')")
		if err != nil {
			t.Fatalf("failed to insert with NULL primary key: %v", err)
		}

		// Should have 2 rows now
		var count int64
		err = db.QueryRow("SELECT COUNT(*) FROM t11").Scan(&count)
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 rows, got %d", count)
		}
	})
}

// TestRowidAmbiguousInJoin tests ambiguous rowid in joins
// Based on rowid-16.9
func TestRowidAmbiguousInJoin(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_ambiguous_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("ambiguous_rowid_in_join", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t12(x);
			CREATE TABLE t13(y);
			INSERT INTO t12 VALUES(1);
			INSERT INTO t13 VALUES(2);
		`)
		if err != nil {
			t.Fatalf("failed to create tables: %v", err)
		}

		// This should fail because rowid is ambiguous
		_, err = db.Query("SELECT rowid FROM t12, t13")
		if err == nil {
			t.Error("expected error for ambiguous rowid in join")
		}
	})
}

// TestRowidUpdateConstraints tests updating rowid with constraints
func TestRowidUpdateConstraints(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_constraints_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("duplicate_rowid_update", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t14(a);
			INSERT INTO t14 VALUES('first');
			INSERT INTO t14 VALUES('second');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Get the rowid of first row
		var rowid1 int64
		err = db.QueryRow("SELECT rowid FROM t14 WHERE a = 'first'").Scan(&rowid1)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Try to update second row to have same rowid as first (should fail)
		_, err = db.Exec("UPDATE t14 SET rowid = ? WHERE a = 'second'", rowid1)
		if err == nil {
			t.Error("expected error when updating to duplicate rowid")
		}
	})
}

// TestRowidDeleteAndReuse tests rowid reuse after deletion
func TestRowidDeleteAndReuse(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rowid_reuse_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("rowid_after_delete", func(t *testing.T) {
		t.Parallel()
		_, err := db.Exec(`
			CREATE TABLE t15(value TEXT);
			INSERT INTO t15 VALUES('one');
			INSERT INTO t15 VALUES('two');
			INSERT INTO t15 VALUES('three');
		`)
		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Delete middle row
		_, err = db.Exec("DELETE FROM t15 WHERE value = 'two'")
		if err != nil {
			t.Fatalf("delete failed: %v", err)
		}

		// Insert new row - should get rowid 4, not reuse 2
		_, err = db.Exec("INSERT INTO t15 VALUES('four')")
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}

		var rowid int64
		err = db.QueryRow("SELECT rowid FROM t15 WHERE value = 'four'").Scan(&rowid)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if rowid != 4 {
			t.Errorf("expected rowid 4, got %d", rowid)
		}
	})
}
