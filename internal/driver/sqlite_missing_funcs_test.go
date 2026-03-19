// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"math"
	"testing"
)

func openMissingFuncsDB(t *testing.T) *sql.DB {
	t.Helper()
	dbFile := t.TempDir() + "/test_missing_funcs.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSQLiteVersion(t *testing.T) {
	db := openMissingFuncsDB(t)

	var version string
	err := db.QueryRow("SELECT sqlite_version()").Scan(&version)
	if err != nil {
		t.Fatalf("sqlite_version() query failed: %v", err)
	}
	if version != "3.46.0" {
		t.Errorf("sqlite_version() = %q, want %q", version, "3.46.0")
	}
}

func TestSoundex(t *testing.T) {
	db := openMissingFuncsDB(t)

	tests := []struct {
		input string
		want  string
	}{
		{"Robert", "R163"},
		{"Rupert", "R163"},
		{"Rubin", "R150"},
		{"Ashcraft", "A261"},
		{"Ashcroft", "A261"},
		{"Tymczak", "T522"},
		{"Pfister", "P236"},
		{"Honeyman", "H555"},
		{"", "?000"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var got string
			var err error
			if tt.input == "" {
				err = db.QueryRow("SELECT soundex('')").Scan(&got)
			} else {
				err = db.QueryRow("SELECT soundex(?)", tt.input).Scan(&got)
			}
			if err != nil {
				t.Fatalf("soundex(%q) query failed: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("soundex(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSoundexNull(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got string
	err := db.QueryRow("SELECT soundex(NULL)").Scan(&got)
	if err != nil {
		t.Fatalf("soundex(NULL) query failed: %v", err)
	}
	if got != "?000" {
		t.Errorf("soundex(NULL) = %q, want %q", got, "?000")
	}
}

func TestLogOneArg(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got float64
	err := db.QueryRow("SELECT log(2.718281828459045)").Scan(&got)
	if err != nil {
		t.Fatalf("log(e) query failed: %v", err)
	}
	if math.Abs(got-1.0) > 0.0001 {
		t.Errorf("log(e) = %v, want ~1.0", got)
	}
}

func TestLogTwoArgs(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got float64
	err := db.QueryRow("SELECT log(10, 100)").Scan(&got)
	if err != nil {
		t.Fatalf("log(10, 100) query failed: %v", err)
	}
	if math.Abs(got-2.0) > 0.0001 {
		t.Errorf("log(10, 100) = %v, want 2.0", got)
	}
}

func TestLogTwoArgsBase2(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got float64
	err := db.QueryRow("SELECT log(2, 8)").Scan(&got)
	if err != nil {
		t.Fatalf("log(2, 8) query failed: %v", err)
	}
	if math.Abs(got-3.0) > 0.0001 {
		t.Errorf("log(2, 8) = %v, want 3.0", got)
	}
}

func TestLogNullArgs(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got *float64
	err := db.QueryRow("SELECT log(NULL)").Scan(&got)
	if err != nil {
		t.Fatalf("log(NULL) query failed: %v", err)
	}
	if got != nil {
		t.Errorf("log(NULL) = %v, want NULL", *got)
	}
}

func TestLastInsertRowid(t *testing.T) {
	db := openMissingFuncsDB(t)
	// Use a single connection to ensure connection-level state is visible
	db.SetMaxOpenConns(1)

	_, err := db.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t1 (name) VALUES ('alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var rowid int64
	err = db.QueryRow("SELECT last_insert_rowid()").Scan(&rowid)
	if err != nil {
		t.Fatalf("last_insert_rowid() query failed: %v", err)
	}
	if rowid < 1 {
		t.Errorf("last_insert_rowid() = %d, want >= 1", rowid)
	}
}

func TestChanges(t *testing.T) {
	db := openMissingFuncsDB(t)
	db.SetMaxOpenConns(1)

	_, err := db.Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t2 (val) VALUES ('a')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var changes int64
	err = db.QueryRow("SELECT changes()").Scan(&changes)
	if err != nil {
		t.Fatalf("changes() query failed: %v", err)
	}
	if changes != 1 {
		t.Errorf("changes() = %d, want 1", changes)
	}
}

func TestTotalChanges(t *testing.T) {
	db := openMissingFuncsDB(t)
	db.SetMaxOpenConns(1)

	_, err := db.Exec("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t3 (val) VALUES ('a')")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t3 (val) VALUES ('b')")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	var total int64
	err = db.QueryRow("SELECT total_changes()").Scan(&total)
	if err != nil {
		t.Fatalf("total_changes() query failed: %v", err)
	}
	if total < 2 {
		t.Errorf("total_changes() = %d, want >= 2", total)
	}
}

func TestChangesAfterUpdate(t *testing.T) {
	db := openMissingFuncsDB(t)
	db.SetMaxOpenConns(1)

	_, err := db.Exec("CREATE TABLE t4 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t4 (val) VALUES ('a')")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO t4 (val) VALUES ('b')")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	_, err = db.Exec("UPDATE t4 SET val = 'c' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	var changes int64
	err = db.QueryRow("SELECT changes()").Scan(&changes)
	if err != nil {
		t.Fatalf("changes() query failed: %v", err)
	}
	if changes != 1 {
		t.Errorf("changes() after UPDATE = %d, want 1", changes)
	}
}

func TestChangesAfterDelete(t *testing.T) {
	db := openMissingFuncsDB(t)
	db.SetMaxOpenConns(1)

	_, err := db.Exec("CREATE TABLE t5 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO t5 (val) VALUES ('a')")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO t5 (val) VALUES ('b')")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	_, err = db.Exec("DELETE FROM t5 WHERE id = 2")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	var changes int64
	err = db.QueryRow("SELECT changes()").Scan(&changes)
	if err != nil {
		t.Fatalf("changes() query failed: %v", err)
	}
	if changes != 1 {
		t.Errorf("changes() after DELETE = %d, want 1", changes)
	}
}

func TestLogInvalidBase(t *testing.T) {
	db := openMissingFuncsDB(t)

	// log(1, 100) should return NaN (base 1 is undefined)
	var got float64
	err := db.QueryRow("SELECT log(1, 100)").Scan(&got)
	if err != nil {
		t.Fatalf("log(1, 100) query failed: %v", err)
	}
	if !math.IsNaN(got) {
		t.Errorf("log(1, 100) = %v, want NaN", got)
	}
}

func TestLogNegativeValue(t *testing.T) {
	db := openMissingFuncsDB(t)

	var got float64
	err := db.QueryRow("SELECT log(-1)").Scan(&got)
	if err != nil {
		t.Fatalf("log(-1) query failed: %v", err)
	}
	if !math.IsNaN(got) {
		t.Errorf("log(-1) = %v, want NaN", got)
	}
}

func TestSoundexSpecialCases(t *testing.T) {
	db := openMissingFuncsDB(t)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"single_letter", "SELECT soundex('A')", "A000"},
		{"numbers_only", "SELECT soundex('123')", "?000"},
		{"lowercase", "SELECT soundex('robert')", "R163"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			err := db.QueryRow(tt.query).Scan(&got)
			if err != nil {
				t.Fatalf("%s query failed: %v", tt.name, err)
			}
			if got != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}
