// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"strings"
	"testing"
)

// freshDB opens a fresh in-memory database with a single connection for
// ATTACH/DETACH tests (attached databases are per-connection state).
func freshDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestAttachGenMemory(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("ATTACH ':memory:' AS aux failed: %v", err)
	}
}

func TestAttachGenDetach(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("ATTACH failed: %v", err)
	}

	_, err = db.Exec("DETACH DATABASE aux")
	if err != nil {
		t.Fatalf("DETACH aux failed: %v", err)
	}
}

func TestAttachGenDetachMain(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("DETACH DATABASE main")
	if err == nil {
		t.Fatal("expected error detaching main, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "cannot detach") &&
		!strings.Contains(strings.ToLower(err.Error()), "cannot detach database main") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttachGenDetachTemp(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("DETACH DATABASE temp")
	if err == nil {
		t.Fatal("expected error detaching temp, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "cannot detach") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttachGenCreateTableInAttached(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("ATTACH failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE aux.t1(a)")
	if err != nil {
		t.Fatalf("CREATE TABLE in attached db failed: %v", err)
	}
}

func TestAttachGenCrossDBSelect(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("ATTACH failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE aux.t1(a)")
	if err != nil {
		t.Fatalf("CREATE TABLE in attached db failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO aux.t1 VALUES(42)")
	if err != nil {
		t.Fatalf("INSERT into attached db failed: %v", err)
	}

	var val int
	err = db.QueryRow("SELECT a FROM aux.t1").Scan(&val)
	if err != nil {
		t.Fatalf("SELECT from attached db failed: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestAttachGenDuplicateName(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("first ATTACH failed: %v", err)
	}

	_, err = db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err == nil {
		t.Fatal("expected error on duplicate ATTACH, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "already") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttachGenPragmaDatabaseList(t *testing.T) {
	db := freshDB(t)

	_, err := db.Exec("ATTACH DATABASE ':memory:' AS aux")
	if err != nil {
		t.Fatalf("ATTACH failed: %v", err)
	}

	rows, err := db.Query("PRAGMA database_list")
	if err != nil {
		t.Fatalf("PRAGMA database_list failed: %v", err)
	}
	defer rows.Close()

	names := collectDBNames(t, rows)

	if len(names) == 0 {
		t.Fatal("PRAGMA database_list returned no rows")
	}

	assertDBNamePresent(t, names, "main")
	assertDBNamePresent(t, names, "aux")
}

// collectDBNames scans PRAGMA database_list rows and returns the name column.
func collectDBNames(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	var names []string
	for rows.Next() {
		name := scanDBListName(t, rows)
		if name != "" {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}
	return names
}

// scanDBListName scans a single PRAGMA database_list row and returns the name.
func scanDBListName(t *testing.T, rows *sql.Rows) string {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(vals) >= 2 {
		if name, ok := asString(vals[1]); ok {
			return name
		}
	}
	return ""
}

// assertDBNamePresent checks that a database name appears in the list.
func assertDBNamePresent(t *testing.T, names []string, want string) {
	t.Helper()
	for _, n := range names {
		if strings.ToLower(n) == strings.ToLower(want) {
			return
		}
	}
	t.Errorf("PRAGMA database_list missing %q", want)
}

// asString converts a value from database/sql scanning to a string.
func asString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}
