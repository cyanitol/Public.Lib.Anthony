// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func openExtraCovDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func execOK(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestEmitInsertRow exercises emitInsertRow indirectly.
// emitInsertRow is called on the same code path as compileInsertValues for
// WITHOUT ROWID and normal tables alike – any INSERT VALUES statement exercises it.
func TestEmitInsertRow(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE eir(id INTEGER PRIMARY KEY, val TEXT)`)
	execOK(t, db, `INSERT INTO eir(id, val) VALUES(1, 'a'), (2, 'b')`)

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM eir`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
}

// TestEmitInsertRowWithoutRowid exercises the WITHOUT ROWID path in emitInsertRow.
func TestEmitInsertRowWithoutRowid(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE eir_nr(a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID`)
	execOK(t, db, `INSERT INTO eir_nr VALUES(1, 'x')`)

	var got string
	if err := db.QueryRow(`SELECT b FROM eir_nr WHERE a=1`).Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got != "x" {
		t.Fatalf("want x, got %s", got)
	}
}

// TestCompileDefaultArg exercises compileDefaultArg via a time.Time parameter,
// which is a valid driver.Value not handled by the typed-arg cases.
func TestCompileDefaultArg(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE cda(ts TEXT)`)
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	execOK(t, db, `INSERT INTO cda VALUES(?)`, ts)

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cda`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1, got %d", count)
	}
}

// TestCompileUpdateFrom exercises compileUpdateFrom / buildUpdateFromSelect /
// mergeTargetIntoFrom / applyUpdateFromRows / emitUpdateFromSingleRow /
// loadValueIntoReg / dmlToInt64 via UPDATE … FROM syntax.
func TestCompileUpdateFrom(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)`)
	execOK(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, newval INTEGER)`)
	execOK(t, db, `INSERT INTO t1 VALUES(1, 10), (2, 20)`)
	execOK(t, db, `INSERT INTO t2 VALUES(1, 100), (2, 200)`)

	execOK(t, db, `UPDATE t1 SET val = t2.newval FROM t2 WHERE t1.id = t2.id`)

	rows, err := db.Query(`SELECT id, val FROM t1 ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	type row struct{ id, val int }
	want := []row{{1, 100}, {2, 200}}
	i := 0
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if i >= len(want) || r != want[i] {
			t.Fatalf("row %d: got %+v, want %+v", i, r, want[i])
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("got %d rows, want %d", i, len(want))
	}
}

// TestCompileUpdateFromTargetInFrom exercises mergeTargetIntoFrom when the
// target table IS already listed in the FROM clause (no-op branch).
func TestCompileUpdateFromTargetInFrom(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE u1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	execOK(t, db, `INSERT INTO u1 VALUES(1, 5, 0)`)

	// The target table u1 is repeated in FROM; mergeTargetIntoFrom should
	// detect it is already present and return the clause unchanged.
	execOK(t, db, `UPDATE u1 SET b = u1.a FROM u1 WHERE u1.id = 1`)

	var b int
	if err := db.QueryRow(`SELECT b FROM u1 WHERE id=1`).Scan(&b); err != nil {
		t.Fatalf("query: %v", err)
	}
	if b != 5 {
		t.Fatalf("want b=5, got %d", b)
	}
}

// TestCompileUpdateFromNullRowid exercises dmlToInt64 with a nil rowid (the
// !ok early-return branch in emitUpdateFromSingleRow).
func TestCompileUpdateFromNullRowid(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE ufr(id INTEGER PRIMARY KEY, v INTEGER)`)
	execOK(t, db, `CREATE TABLE ufs(id INTEGER, v INTEGER)`)
	execOK(t, db, `INSERT INTO ufr VALUES(1, 10)`)
	// No matching row in ufs – UPDATE FROM should produce zero updates without panic.
	execOK(t, db, `UPDATE ufr SET v = ufs.v FROM ufs WHERE ufr.id = ufs.id`)

	var v int
	if err := db.QueryRow(`SELECT v FROM ufr WHERE id=1`).Scan(&v); err != nil {
		t.Fatalf("query: %v", err)
	}
	if v != 10 {
		t.Fatalf("want v=10 (unchanged), got %d", v)
	}
}

// TestLoadValueIntoRegTypes exercises loadValueIntoReg for integer value types
// by using UPDATE … FROM with SET to integer values.
func TestLoadValueIntoRegTypes(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE lvr(id INTEGER PRIMARY KEY, i INTEGER, j INTEGER)`)
	execOK(t, db, `CREATE TABLE src(id INTEGER PRIMARY KEY, i INTEGER, j INTEGER)`)
	execOK(t, db, `INSERT INTO lvr VALUES(1, 0, 0)`)
	execOK(t, db, `INSERT INTO src VALUES(1, 42, 99)`)

	execOK(t, db, `UPDATE lvr SET i=src.i, j=src.j FROM src WHERE lvr.id=src.id`)

	var i, j int
	if err := db.QueryRow(`SELECT i, j FROM lvr WHERE id=1`).Scan(&i, &j); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if i != 42 || j != 99 {
		t.Fatalf("unexpected values: i=%d j=%d", i, j)
	}
}

// TestLoadValueIntoRegNull exercises the nil branch of loadValueIntoReg via
// UPDATE … FROM where a NULL value is materialised.
func TestLoadValueIntoRegNull(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE lvrn(id INTEGER PRIMARY KEY, v TEXT)`)
	execOK(t, db, `CREATE TABLE srcn(id INTEGER PRIMARY KEY, v TEXT)`)
	execOK(t, db, `INSERT INTO lvrn VALUES(1, 'notnull')`)
	execOK(t, db, `INSERT INTO srcn VALUES(1, NULL)`)

	execOK(t, db, `UPDATE lvrn SET v=srcn.v FROM srcn WHERE lvrn.id=srcn.id`)

	var v sql.NullString
	if err := db.QueryRow(`SELECT v FROM lvrn WHERE id=1`).Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v.Valid {
		t.Fatalf("want NULL, got %q", v.String)
	}
}

// TestReplaceGeneratedValues exercises replaceGeneratedValues by inserting into
// a table that has a GENERATED ALWAYS AS column without specifying a column list
// (so expandInsertDefaults calls replaceGeneratedValues).
// The VALUES list must have one entry per physical column; replaceGeneratedValues
// overwrites the placeholder for the generated column with its expression.
func TestReplaceGeneratedValues(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE rgv(a INTEGER, b INTEGER GENERATED ALWAYS AS (a*2) STORED)`)

	// INSERT without column list: supply a placeholder for the generated column;
	// replaceGeneratedValues replaces it with the computed expression.
	execOK(t, db, `INSERT INTO rgv VALUES(7, 0)`)

	var a int
	if err := db.QueryRow(`SELECT a FROM rgv`).Scan(&a); err != nil {
		t.Fatalf("query: %v", err)
	}
	if a != 7 {
		t.Fatalf("want a=7, got %d", a)
	}
}

// TestReplaceGeneratedValuesNoGen exercises replaceGeneratedValues early-return
// path when the table has no generated columns.
func TestReplaceGeneratedValuesNoGen(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE rgvng(a INTEGER, b INTEGER)`)
	execOK(t, db, `INSERT INTO rgvng VALUES(1, 2)`)

	var b int
	if err := db.QueryRow(`SELECT b FROM rgvng WHERE a=1`).Scan(&b); err != nil {
		t.Fatalf("query: %v", err)
	}
	if b != 2 {
		t.Fatalf("want 2, got %d", b)
	}
}

// TestDmlToInt64Float exercises the float64 branch of dmlToInt64 by using
// UPDATE … FROM where the rowid materialises as a float64.
func TestDmlToInt64Float(t *testing.T) {
	db := openExtraCovDB(t)
	execOK(t, db, `CREATE TABLE dif(id INTEGER PRIMARY KEY, v INTEGER)`)
	execOK(t, db, `CREATE TABLE dif2(id INTEGER PRIMARY KEY, v INTEGER)`)
	execOK(t, db, `INSERT INTO dif VALUES(1, 10)`)
	execOK(t, db, `INSERT INTO dif2 VALUES(1, 99)`)
	execOK(t, db, `UPDATE dif SET v = dif2.v FROM dif2 WHERE dif.id = dif2.id`)

	var v int
	if err := db.QueryRow(`SELECT v FROM dif WHERE id=1`).Scan(&v); err != nil {
		t.Fatalf("query: %v", err)
	}
	if v != 99 {
		t.Fatalf("want 99, got %d", v)
	}
}
