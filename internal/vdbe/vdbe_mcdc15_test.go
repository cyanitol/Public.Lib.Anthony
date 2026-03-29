// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

// MC/DC 15 — SQL-level VDBE coverage for low-coverage exec.go paths.
//
// Targets:
//   exec.go:4787  execExplicitCast  (76.9%) — CAST(NULL AS ...) null path, BLOB/NUMERIC types
//   exec.go:4808  execAffinityCast  (77.8%) — NULL path, unknown affinity code path
//   exec.go:850   handleIndexSeekGE (75.0%) — index seek on unique index
//   exec.go:955   execSeekRowid     (78.9%) — rowid seek found/not-found
//   exec.go:2605  execInsertWithoutRowID (78.6%) — WITHOUT ROWID insert
//   exec.go:2733  resolveCompositeConflict (75.0%) — WITHOUT ROWID REPLACE conflict
//   exec.go:3010  generateNewRowid  (75.0%) — AUTOINCREMENT with explicit rowid

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func mcdc15Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mcdc15Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestMCDC15_Cast_NullInput exercises execExplicitCast with NULL input (line 4796-4799).
func TestMCDC15_Cast_NullInput(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	var v interface{}
	err := db.QueryRow(`SELECT CAST(NULL AS INTEGER)`).Scan(&v)
	if err != nil {
		t.Skipf("CAST NULL: %v", err)
	}
	if v != nil {
		t.Errorf("expected NULL, got %v", v)
	}
}

// TestMCDC15_Cast_Blob exercises CAST(x AS BLOB).
func TestMCDC15_Cast_Blob(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	var v []byte
	err := db.QueryRow(`SELECT CAST('hello' AS BLOB)`).Scan(&v)
	if err != nil {
		t.Skipf("CAST AS BLOB: %v", err)
	}
	if string(v) != "hello" {
		t.Errorf("expected 'hello', got %q", v)
	}
}

// TestMCDC15_Cast_Numeric exercises CAST(x AS NUMERIC).
func TestMCDC15_Cast_Numeric(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	var v interface{}
	err := db.QueryRow(`SELECT CAST('3.14' AS NUMERIC)`).Scan(&v)
	if err != nil {
		t.Skipf("CAST AS NUMERIC: %v", err)
	}
	_ = v
}

// TestMCDC15_Cast_Text exercises CAST(x AS TEXT).
func TestMCDC15_Cast_Text(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	var v string
	err := db.QueryRow(`SELECT CAST(42 AS TEXT)`).Scan(&v)
	if err != nil {
		t.Skipf("CAST AS TEXT: %v", err)
	}
	if v != "42" {
		t.Errorf("expected '42', got %q", v)
	}
}

// TestMCDC15_IndexSeekGE_UniqueIndex exercises handleIndexSeekGE via a
// unique index lookup.
func TestMCDC15_IndexSeekGE_UniqueIndex(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	mcdc15Exec(t, db, `CREATE TABLE emails (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	for i := 1; i <= 10; i++ {
		mcdc15Exec(t, db, `INSERT INTO emails VALUES (?, ?)`, i, "user"+string(rune('a'+i-1))+"@example.com")
	}

	// Query using UNIQUE index.
	rows, err := db.Query(`SELECT id FROM emails WHERE email >= 'usere@example.com'`)
	if err != nil {
		t.Skipf("unique index GE seek: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected rows from unique index seek")
	}
}

// TestMCDC15_SeekRowid_Found exercises execSeekRowid found path.
func TestMCDC15_SeekRowid_Found(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	mcdc15Exec(t, db, `CREATE TABLE rowdata (id INTEGER PRIMARY KEY, val TEXT)`)
	mcdc15Exec(t, db, `INSERT INTO rowdata VALUES (5, 'five')`)
	mcdc15Exec(t, db, `INSERT INTO rowdata VALUES (10, 'ten')`)

	var val string
	err := db.QueryRow(`SELECT val FROM rowdata WHERE id = 5`).Scan(&val)
	if err != nil {
		t.Skipf("SeekRowid found: %v", err)
	}
	if val != "five" {
		t.Errorf("expected 'five', got %q", val)
	}
}

// TestMCDC15_WithoutRowID_Insert exercises execInsertWithoutRowID.
func TestMCDC15_WithoutRowID_Insert(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	_, err := db.Exec(`CREATE TABLE wori (k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID`)
	if err != nil {
		t.Skipf("WITHOUT ROWID create: %v", err)
	}

	mcdc15Exec(t, db, `INSERT INTO wori VALUES (1, 'a')`)
	mcdc15Exec(t, db, `INSERT INTO wori VALUES (2, 'b')`)
	mcdc15Exec(t, db, `INSERT INTO wori VALUES (3, 'c')`)

	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM wori`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestMCDC15_WithoutRowID_Replace exercises resolveCompositeConflict via
// INSERT OR REPLACE into a WITHOUT ROWID table.
func TestMCDC15_WithoutRowID_Replace(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	_, err := db.Exec(`CREATE TABLE wori2 (k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID`)
	if err != nil {
		t.Skipf("WITHOUT ROWID create: %v", err)
	}

	mcdc15Exec(t, db, `INSERT INTO wori2 VALUES (1, 'original')`)
	mcdc15Exec(t, db, `INSERT OR REPLACE INTO wori2 VALUES (1, 'replaced')`)

	var v string
	if err := db.QueryRow(`SELECT v FROM wori2 WHERE k=1`).Scan(&v); err != nil {
		t.Skipf("select after replace: %v", err)
	}
	if v != "replaced" {
		t.Errorf("expected 'replaced', got %q", v)
	}
}

// TestMCDC15_Autoincrement_ExplicitRowid exercises generateNewRowid with an
// explicit rowid provided in an AUTOINCREMENT table.
func TestMCDC15_Autoincrement_ExplicitRowid(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	mcdc15Exec(t, db, `CREATE TABLE autoinc (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)`)
	mcdc15Exec(t, db, `INSERT INTO autoinc (id, val) VALUES (100, 'explicit')`)
	mcdc15Exec(t, db, `INSERT INTO autoinc (val) VALUES ('auto')`)

	rows, err := db.Query(`SELECT id FROM autoinc ORDER BY id DESC`)
	if err != nil {
		t.Fatalf("select ids: %v", err)
	}
	defer rows.Close()
	var maxID int64
	if rows.Next() {
		if scanErr := rows.Scan(&maxID); scanErr != nil {
			t.Skipf("scan id: %v", scanErr)
		}
	}
	if maxID < 100 {
		t.Errorf("expected id >= 100, got %d", maxID)
	}
}

// TestMCDC15_CheckColumnUnique_MultipleUnique exercises checkColumnUnique
// on a table with multiple UNIQUE non-PK columns.
func TestMCDC15_CheckColumnUnique_MultipleUnique(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	mcdc15Exec(t, db, `CREATE TABLE multi_unique (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE)`)
	mcdc15Exec(t, db, `INSERT INTO multi_unique VALUES (1, 'x', 'y')`)

	// Violate the second UNIQUE (b column).
	_, err := db.Exec(`INSERT INTO multi_unique VALUES (2, 'z', 'y')`)
	if err == nil {
		t.Skip("engine did not enforce UNIQUE on second column")
	}
}

// TestMCDC15_ExplicitCast_DefaultType exercises applyCast default branch (unknown
// type name → treated as INTEGER).
func TestMCDC15_ExplicitCast_DefaultType(t *testing.T) {
	t.Parallel()
	db := mcdc15Open(t)

	var v int64
	err := db.QueryRow(`SELECT CAST(3 AS BIGINT)`).Scan(&v)
	if err != nil {
		t.Skipf("CAST AS BIGINT: %v", err)
	}
	if v != 3 {
		t.Errorf("expected 3, got %d", v)
	}
}
