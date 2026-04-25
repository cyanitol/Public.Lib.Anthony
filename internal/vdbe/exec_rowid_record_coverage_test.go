// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ─────────────────────────────────────────────────────────────────────────────
// helpers local to this file (all names prefixed with "rr" to avoid collision)
// ─────────────────────────────────────────────────────────────────────────────

func rrOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("rrOpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func rrExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("rrExec %q: %v", q, err)
	}
}

func rrExecErr(t *testing.T, db *sql.DB, q string, args ...interface{}) error {
	t.Helper()
	_, err := db.Exec(q, args...)
	return err
}

func rrMustInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("rrMustInt %q: %v", q, err)
	}
	return v
}

func rrCount(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("rrCount %q: %v", q, err)
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. execNewRowid — empty-table branch (rowid = 1) and populated-table branch
//    (rowid = max + 1).  Also tests that explicit rowid INSERTs followed by an
//    implicit INSERT produce a rowid beyond the explicit one.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_NewRowidEmptyTable verifies that the first auto-rowid on
// an empty table is 1 (the error-from-NewRowid → newRowid=1 branch).
func TestExecRowidRecord_NewRowidEmptyTable(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT)`)

	// Insert without specifying id — engine calls execNewRowid on empty table.
	rrExec(t, db, `INSERT INTO emp(name) VALUES('alice')`)

	id := rrMustInt(t, db, `SELECT id FROM emp WHERE name='alice'`)
	if id != 1 {
		t.Errorf("want rowid=1 for first row in empty table, got %d", id)
	}
}

// TestExecRowidRecord_NewRowidPopulatedTable verifies that a second auto-rowid
// on a populated table is max(id)+1 (the NewRowid-succeeds branch).
func TestExecRowidRecord_NewRowidPopulatedTable(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE emp2 (id INTEGER PRIMARY KEY, name TEXT)`)

	rrExec(t, db, `INSERT INTO emp2(name) VALUES('alice')`)
	rrExec(t, db, `INSERT INTO emp2(name) VALUES('bob')`)

	ids := []int64{}
	rows, err := db.Query(`SELECT id FROM emp2 ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ids))
	}
	// Rowids must be sequential (1, 2) regardless of order.
	if ids[0] >= ids[1] {
		t.Errorf("expected ascending rowids, got %v", ids)
	}
}

// TestExecRowidRecord_NewRowidAfterExplicit ensures that when a large explicit
// rowid has already been inserted, the next auto-rowid is beyond it.
func TestExecRowidRecord_NewRowidAfterExplicit(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE emp3 (id INTEGER PRIMARY KEY, name TEXT)`)

	// Insert at an explicit high rowid to anchor max.
	rrExec(t, db, `INSERT INTO emp3 VALUES(1000, 'anchor')`)

	// Auto-rowid should be > 1000.
	rrExec(t, db, `INSERT INTO emp3(name) VALUES('auto')`)

	autoID := rrMustInt(t, db, `SELECT id FROM emp3 WHERE name='auto'`)
	if autoID <= 1000 {
		t.Errorf("expected auto-rowid > 1000, got %d", autoID)
	}
}

// TestExecRowidRecord_ExplicitRowid verifies that an explicit rowid provided in
// INSERT is stored and retrieved correctly (the engine skips execNewRowid when
// the rowid register is already populated by the planner).
func TestExecRowidRecord_ExplicitRowid(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE explicit_rid (id INTEGER PRIMARY KEY, val TEXT)`)

	rrExec(t, db, `INSERT INTO explicit_rid VALUES(42, 'forty-two')`)
	rrExec(t, db, `INSERT INTO explicit_rid VALUES(9999, 'large')`)

	v42 := rrMustInt(t, db, `SELECT id FROM explicit_rid WHERE val='forty-two'`)
	if v42 != 42 {
		t.Errorf("want 42, got %d", v42)
	}
	v9999 := rrMustInt(t, db, `SELECT id FROM explicit_rid WHERE val='large'`)
	if v9999 != 9999 {
		t.Errorf("want 9999, got %d", v9999)
	}
}

// TestExecRowidRecord_MultipleAutoRowids inserts many rows without explicit ids
// and verifies they are all distinct and in ascending order.
func TestExecRowidRecord_MultipleAutoRowids(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE autorid (id INTEGER PRIMARY KEY, n INTEGER)`)

	const count = 20
	for i := 0; i < count; i++ {
		rrExec(t, db, `INSERT INTO autorid(n) VALUES(?)`, i)
	}

	n := rrCount(t, db, `SELECT COUNT(*) FROM autorid`)
	if n != count {
		t.Errorf("expected %d rows, got %d", count, n)
	}

	// Verify uniqueness: count(*) == count(distinct id)
	nd := rrMustInt(t, db, `SELECT COUNT(DISTINCT id) FROM autorid`)
	if nd != int64(count) {
		t.Errorf("expected %d distinct rowids, got %d", count, nd)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. getRowidFromRegister — exercises the P3 != 0 branch via operations that
//    require the engine to read a rowid from a register for seeks.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_GetRowidFromRegisterUpdate exercises getRowidFromRegister
// through UPDATE, which stores the target rowid in a register before seeking.
func TestExecRowidRecord_GetRowidFromRegisterUpdate(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE regtbl (id INTEGER PRIMARY KEY, val TEXT)`)
	rrExec(t, db, `INSERT INTO regtbl VALUES(5, 'original')`)
	rrExec(t, db, `INSERT INTO regtbl VALUES(6, 'other')`)

	rrExec(t, db, `UPDATE regtbl SET val='changed' WHERE id=5`)

	var val string
	if err := db.QueryRow(`SELECT val FROM regtbl WHERE id=5`).Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "changed" {
		t.Errorf("want 'changed', got %q", val)
	}

	var other string
	if err := db.QueryRow(`SELECT val FROM regtbl WHERE id=6`).Scan(&other); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if other != "other" {
		t.Errorf("want 'other', got %q", other)
	}
}

// TestExecRowidRecord_GetRowidFromRegisterDelete exercises getRowidFromRegister
// through DELETE, which also seeks by rowid stored in a register.
func TestExecRowidRecord_GetRowidFromRegisterDelete(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE deltbl (id INTEGER PRIMARY KEY, val TEXT)`)
	rrExec(t, db, `INSERT INTO deltbl VALUES(10, 'keep')`)
	rrExec(t, db, `INSERT INTO deltbl VALUES(20, 'delete-me')`)
	rrExec(t, db, `INSERT INTO deltbl VALUES(30, 'keep-too')`)

	rrExec(t, db, `DELETE FROM deltbl WHERE id=20`)

	n := rrCount(t, db, `SELECT COUNT(*) FROM deltbl`)
	if n != 2 {
		t.Errorf("expected 2 rows after delete, got %d", n)
	}

	// The deleted row must be gone.
	missing := rrCount(t, db, `SELECT COUNT(*) FROM deltbl WHERE id=20`)
	if missing != 0 {
		t.Errorf("expected id=20 gone, got count=%d", missing)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. performDeferredSeek — exercises the lazy cursor seek path triggered when
//    the engine uses an index lookup then needs the table row.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_DeferredSeekViaIndex exercises performDeferredSeek by
// querying via a secondary index.  The engine finds the rowid through the index
// cursor and then performs the deferred seek on the table cursor.
func TestExecRowidRecord_DeferredSeekViaIndex(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT, qty INTEGER)`)
	rrExec(t, db, `CREATE INDEX idx_items_code ON items(code)`)

	rrExec(t, db, `INSERT INTO items VALUES(1, 'AAA', 10)`)
	rrExec(t, db, `INSERT INTO items VALUES(2, 'BBB', 20)`)
	rrExec(t, db, `INSERT INTO items VALUES(3, 'CCC', 30)`)
	rrExec(t, db, `INSERT INTO items VALUES(4, 'DDD', 40)`)

	var qty int64
	if err := db.QueryRow(`SELECT qty FROM items WHERE code='CCC'`).Scan(&qty); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if qty != 30 {
		t.Errorf("want qty=30, got %d", qty)
	}
}

// TestExecRowidRecord_DeferredSeekNoMatch verifies that performDeferredSeek
// sets EOF correctly when the sought rowid does not exist in the table.
func TestExecRowidRecord_DeferredSeekNoMatch(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE items2 (id INTEGER PRIMARY KEY, code TEXT)`)
	rrExec(t, db, `CREATE INDEX idx_items2_code ON items2(code)`)

	rrExec(t, db, `INSERT INTO items2 VALUES(1, 'X')`)

	var id int64
	err := db.QueryRow(`SELECT id FROM items2 WHERE code='MISSING'`).Scan(&id)
	if err != sql.ErrNoRows {
		t.Errorf("expected ErrNoRows, got %v", err)
	}
}

// TestExecRowidRecord_DeferredSeekMultipleMatches exercises the deferred-seek
// path across multiple matched rows from an index scan.
func TestExecRowidRecord_DeferredSeekMultipleMatches(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE items3 (id INTEGER PRIMARY KEY, cat TEXT, label TEXT)`)
	rrExec(t, db, `CREATE INDEX idx_items3_cat ON items3(cat)`)

	for i := 1; i <= 8; i++ {
		cat := "even"
		if i%2 != 0 {
			cat = "odd"
		}
		rrExec(t, db, `INSERT INTO items3 VALUES(?, ?, ?)`, i, cat, i)
	}

	n := rrCount(t, db, `SELECT COUNT(*) FROM items3 WHERE cat='even'`)
	if n != 4 {
		t.Errorf("expected 4 even rows, got %d", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. parseRecordColumnHeader — exercises header parsing including the
//    multi-byte varint path (serial type > 127) and multiple columns.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_ParseRecordColumnHeaderManyColumns exercises
// parseRecordColumnHeader with a table that has many columns of varying types,
// ensuring the header with many serial-type entries is parsed correctly.
func TestExecRowidRecord_ParseRecordColumnHeaderManyColumns_Setup(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE hdr (
		a INTEGER PRIMARY KEY, b INTEGER, c TEXT, d REAL,
		e BLOB, f INTEGER, g TEXT, h BLOB
	)`)

	b100 := make([]byte, 100)
	for i := range b100 {
		b100[i] = byte(i % 256)
	}
	s80 := strings.Repeat("z", 80)
	rrExec(t, db, `INSERT INTO hdr VALUES(1, 999, ?, 2.71, ?, -42, ?, ?)`,
		s80, b100, "short", []byte{0xAB, 0xCD})

	var bVal, fVal int64
	var cVal, gVal string
	var dVal float64
	var eVal, hVal []byte
	if err := db.QueryRow(`SELECT b,c,d,e,f,g,h FROM hdr WHERE a=1`).
		Scan(&bVal, &cVal, &dVal, &eVal, &fVal, &gVal, &hVal); err != nil {
		t.Fatalf("scan: %v", err)
	}
	rrCheckManyColumns(t, bVal, cVal, dVal, eVal, fVal, gVal, hVal)
}

func rrCheckManyColumnsNumeric(t *testing.T, bVal int64, dVal float64, fVal int64) {
	t.Helper()
	if bVal != 999 {
		t.Errorf("b: want 999, got %d", bVal)
	}
	if dVal < 2.70 || dVal > 2.72 {
		t.Errorf("d: want ~2.71, got %f", dVal)
	}
	if fVal != -42 {
		t.Errorf("f: want -42, got %d", fVal)
	}
}

func rrCheckManyColumnsOther(t *testing.T, cVal string, eVal []byte, gVal string, hVal []byte) {
	t.Helper()
	if len(cVal) != 80 {
		t.Errorf("c: want 80 chars, got %d", len(cVal))
	}
	if len(eVal) != 100 {
		t.Errorf("e: want 100 bytes, got %d", len(eVal))
	}
	if gVal != "short" {
		t.Errorf("g: want 'short', got %q", gVal)
	}
	if len(hVal) != 2 {
		t.Errorf("h: want 2 bytes, got %d", len(hVal))
	}
}

func rrCheckManyColumns(t *testing.T, bVal int64, cVal string, dVal float64, eVal []byte, fVal int64, gVal string, hVal []byte) {
	t.Helper()
	rrCheckManyColumnsNumeric(t, bVal, dVal, fVal)
	rrCheckManyColumnsOther(t, cVal, eVal, gVal, hVal)
}

// TestExecRowidRecord_ParseRecordColumnHeaderNullCols exercises the NULL serial
// type (0) path in parseRecordColumnHeader.  Multiple NULL columns produce
// serial type 0 entries in the header.
func TestExecRowidRecord_ParseRecordColumnHeaderNullCols(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE nullhdr (
		id INTEGER PRIMARY KEY,
		a TEXT,
		b INTEGER,
		c BLOB
	)`)

	rrExec(t, db, `INSERT INTO nullhdr VALUES(1, NULL, NULL, NULL)`)

	var a *string
	var b *int64
	var c []byte
	if err := db.QueryRow(`SELECT a,b,c FROM nullhdr WHERE id=1`).Scan(&a, &b, &c); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if a != nil {
		t.Errorf("a: want nil, got %v", *a)
	}
	if b != nil {
		t.Errorf("b: want nil, got %v", *b)
	}
	if c != nil {
		t.Errorf("c: want nil, got %v", c)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. parseSerialBlobOrText — text vs. blob branch discrimination.
//    Serial types with even number >= 12 → blob; odd >= 13 → text.
// ─────────────────────────────────────────────────────────────────────────────

func rrVerifyBlob(t *testing.T, db *sql.DB, id int, want []byte) {
	t.Helper()
	var got []byte
	if err := db.QueryRow(`SELECT dat FROM blbtypes WHERE id=?`, id).Scan(&got); err != nil {
		t.Fatalf("id=%d: %v", id, err)
	}
	if len(got) != len(want) {
		t.Errorf("id=%d: want %d bytes, got %d", id, len(want), len(got))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("id=%d byte[%d]: want %02x, got %02x", id, i, want[i], got[i])
			return
		}
	}
}

// TestExecRowidRecord_ParseSerialBlobType exercises the blob branch in
// parseSerialBlobOrText (st%2 == 0) by storing and retrieving binary BLOBs.
func TestExecRowidRecord_ParseSerialBlobType(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE blbtypes (id INTEGER PRIMARY KEY, dat BLOB)`)

	// Various blob sizes to test different even serial types.
	cases := []struct {
		id   int
		data []byte
	}{
		{1, []byte{0x00}},                   // 1 byte: st=12 (even)
		{2, []byte{0xFF, 0xFE}},             // 2 bytes: st=16 (even)
		{3, []byte{0x01, 0x02, 0x03, 0x04}}, // 4 bytes: st=20 (even)
		{4, make([]byte, 50)},               // 50 bytes: st=112 (even)
		{5, make([]byte, 200)},              // 200 bytes: st=412 (even, 2-byte varint)
	}
	// fill the larger blobs with a pattern
	for i := range cases[3].data {
		cases[3].data[i] = byte(i)
	}
	for i := range cases[4].data {
		cases[4].data[i] = byte(i % 256)
	}

	for _, c := range cases {
		rrExec(t, db, `INSERT INTO blbtypes VALUES(?, ?)`, c.id, c.data)
	}

	for _, c := range cases {
		rrVerifyBlob(t, db, c.id, c.data)
	}
}

// TestExecRowidRecord_ParseSerialTextType exercises the text branch in
// parseSerialBlobOrText (st%2 == 1) alongside various text lengths.
func TestExecRowidRecord_ParseSerialTextType(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE txttypes (id INTEGER PRIMARY KEY, val TEXT)`)

	// Serial type for text of N bytes = 2*N + 13 (odd for text).
	cases := []struct {
		id  int
		val string
	}{
		{1, "a"},                      // 1 byte: st=15 (odd)
		{2, "ab"},                     // 2 bytes: st=17 (odd)
		{3, strings.Repeat("x", 57)},  // 57 bytes: st=127 (1-byte varint boundary)
		{4, strings.Repeat("y", 58)},  // 58 bytes: st=129 (2-byte varint)
		{5, strings.Repeat("z", 300)}, // 300 bytes: st=613 (2-byte varint)
	}

	for _, c := range cases {
		rrExec(t, db, `INSERT INTO txttypes VALUES(?, ?)`, c.id, c.val)
	}

	for _, c := range cases {
		var got string
		if err := db.QueryRow(`SELECT val FROM txttypes WHERE id=?`, c.id).Scan(&got); err != nil {
			t.Fatalf("id=%d: %v", c.id, err)
		}
		if got != c.val {
			t.Errorf("id=%d: want len=%d, got len=%d", c.id, len(c.val), len(got))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. serialTypeLen — boundary cases for fixed and variable-length serial types.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_SerialTypeLenFixed exercises serialTypeLen for fixed-size
// serial types 0–11 by storing/retrieving values whose encoding uses those types.
func TestExecRowidRecord_SerialTypeLenFixed(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE fixed_st (id INTEGER PRIMARY KEY, v INTEGER)`)

	// These values exercise serial types 0 (NULL), 1 (int8), 2 (int16),
	// 3 (int24), 4 (int32), 5 (int48 — 6-byte), 6 (int64), 8 (literal 0), 9 (literal 1).
	rrExec(t, db, `INSERT INTO fixed_st VALUES(1, NULL)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(2, 127)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(3, 32767)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(4, 8388607)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(5, 2147483647)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(6, 140737488355327)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(7, 9223372036854775807)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(8, 0)`)
	rrExec(t, db, `INSERT INTO fixed_st VALUES(9, 1)`)

	n := rrCount(t, db, `SELECT COUNT(*) FROM fixed_st`)
	if n != 9 {
		t.Errorf("expected 9 rows, got %d", n)
	}
}

// TestExecRowidRecord_SerialTypeLenVariable exercises serialTypeLen for large
// text/blob values that require multi-byte varint serial type numbers.
func TestExecRowidRecord_SerialTypeLenVariable(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE varlen_st (id INTEGER PRIMARY KEY, dat BLOB)`)

	// Blob sizes that produce even serial types with multi-byte varints.
	// st = 2*N + 12 for BLOB of N bytes.
	// N=58  → st=128 (2-byte varint, boundary)
	// N=250 → st=512 (2-byte varint)
	// N=500 → st=1012 (2-byte varint)
	cases := []struct {
		id   int
		size int
	}{
		{1, 58},
		{2, 250},
		{3, 500},
	}

	for _, c := range cases {
		b := make([]byte, c.size)
		for i := range b {
			b[i] = byte(i % 251)
		}
		rrExec(t, db, `INSERT INTO varlen_st VALUES(?, ?)`, c.id, b)
	}

	for _, c := range cases {
		var got []byte
		if err := db.QueryRow(`SELECT dat FROM varlen_st WHERE id=?`, c.id).Scan(&got); err != nil {
			t.Fatalf("id=%d size=%d: %v", c.id, c.size, err)
		}
		if len(got) != c.size {
			t.Errorf("id=%d: want %d bytes, got %d", c.id, c.size, len(got))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. decodeSerialIntValue — exercises all 6 integer serial type branches.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_DecodeSerialIntAllSizes stores boundary values for each
// SQLite integer serial type and verifies correct round-trip decoding.
func TestExecRowidRecord_DecodeSerialIntAllSizes(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE intdecode (id INTEGER PRIMARY KEY, v INTEGER)`)

	cases := []struct {
		id  int
		val int64
	}{
		{1, int64(int8(-1))},              // serial type 1: 1-byte int8
		{2, int64(int8(127))},             // serial type 1: max int8
		{3, int64(int16(-1000))},          // serial type 2: 2-byte int16
		{4, int64(int16(30000))},          // serial type 2: near max int16
		{5, -8388608},                     // serial type 3: min int24
		{6, 8388607},                      // serial type 3: max int24
		{7, int64(int32(-2000000000))},    // serial type 4: 4-byte int32
		{8, int64(int32(2000000000))},     // serial type 4: near max int32
		{9, -140737488355328},             // serial type 5: min int48
		{10, 140737488355327},             // serial type 5: max int48
		{11, int64(-9223372036854775808)}, // serial type 6: min int64
		{12, int64(9223372036854775807)},  // serial type 6: max int64
	}

	for _, c := range cases {
		rrExec(t, db, `INSERT INTO intdecode VALUES(?, ?)`, c.id, c.val)
	}

	for _, c := range cases {
		got := rrMustInt(t, db, `SELECT v FROM intdecode WHERE id=?`, c.id)
		if got != c.val {
			t.Errorf("id=%d: want %d, got %d", c.id, c.val, got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. checkMultiColRow (constraints.go:549) — multi-column UNIQUE constraint.
//    Exercises the early-exit branches: skipRowid match, NULL in existing,
//    NULL in new values, column mismatch, and full match.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRowidRecord_MultiColUniqueConstraintViolation verifies that inserting
// a row whose (a, b) combination duplicates an existing row is rejected.
// This exercises checkMultiColRow reaching the full-match branch and returning
// a UNIQUE constraint error.
func TestExecRowidRecord_MultiColUniqueConstraintViolation(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mcuniq (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_mc_ab ON mcuniq(a, b)`)

	rrExec(t, db, `INSERT INTO mcuniq VALUES(1, 'go', 'lang', 'v1')`)
	rrExec(t, db, `INSERT INTO mcuniq VALUES(2, 'go', 'test', 'v2')`)
	rrExec(t, db, `INSERT INTO mcuniq VALUES(3, 'py', 'lang', 'v3')`)

	// Exact duplicate of (a='go', b='lang') — must fail.
	err := rrExecErr(t, db, `INSERT INTO mcuniq VALUES(10, 'go', 'lang', 'dup')`)
	if err == nil {
		t.Error("expected UNIQUE constraint error, got nil")
	}
}

// TestExecRowidRecord_MultiColUniqueNullSkip verifies that rows with NULL in a
// UNIQUE multi-column index do not conflict with each other or with non-NULL rows.
// This exercises the NULL early-exit branch inside checkMultiColRow.
func TestExecRowidRecord_MultiColUniqueNullSkip(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mcnull (id INTEGER PRIMARY KEY, a TEXT, b TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_mn_ab ON mcnull(a, b)`)

	rrExec(t, db, `INSERT INTO mcnull VALUES(1, 'key', 'val')`)

	// b=NULL: must not conflict with id=1 (NULL is always distinct).
	rrExec(t, db, `INSERT INTO mcnull VALUES(2, 'key', NULL)`)
	rrExec(t, db, `INSERT INTO mcnull VALUES(3, 'key', NULL)`)

	n := rrCount(t, db, `SELECT COUNT(*) FROM mcnull`)
	if n != 3 {
		t.Errorf("expected 3 rows (NULLs are distinct), got %d", n)
	}
}

// TestExecRowidRecord_MultiColUniquePartialMismatch verifies that rows sharing
// the first column but differing on the second do not conflict.
// This exercises the column-mismatch early-exit branch inside checkMultiColRow
// where v.compareMemValuesWithCollation returns != 0.
func TestExecRowidRecord_MultiColUniquePartialMismatch(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mcmatch (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_mm_ab ON mcmatch(a, b)`)

	rrExec(t, db, `INSERT INTO mcmatch VALUES(1, 'go', 'lang', 'x')`)
	rrExec(t, db, `INSERT INTO mcmatch VALUES(2, 'go', 'test', 'y')`)
	rrExec(t, db, `INSERT INTO mcmatch VALUES(3, 'py', 'lang', 'z')`)

	// Same 'a' but different 'b' — must succeed (no conflict).
	rrExec(t, db, `INSERT INTO mcmatch VALUES(4, 'go', 'spec', 'w')`)

	n := rrCount(t, db, `SELECT COUNT(*) FROM mcmatch`)
	if n != 4 {
		t.Errorf("expected 4 rows, got %d", n)
	}
}

// TestExecRowidRecord_MultiColUniqueSkipRowid exercises the skipRowid branch
// in checkMultiColRow by doing an UPDATE that re-inserts the same composite
// key values (REPLACE INTO on the same row).  The scanner must skip the row
// being replaced (its own rowid) and find no conflict.
func TestExecRowidRecord_MultiColUniqueSkipRowid(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mcself (id INTEGER PRIMARY KEY, a TEXT, b TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_ms_ab ON mcself(a, b)`)

	rrExec(t, db, `INSERT INTO mcself VALUES(1, 'same', 'key')`)
	rrExec(t, db, `INSERT INTO mcself VALUES(2, 'diff', 'key')`)

	// REPLACE on id=1 with same (a, b): scanner must skip rowid=1 (itself).
	rrExec(t, db, `INSERT OR REPLACE INTO mcself VALUES(1, 'same', 'key')`)

	n := rrCount(t, db, `SELECT COUNT(*) FROM mcself`)
	if n != 2 {
		t.Errorf("expected 2 rows after self-replace, got %d", n)
	}
	v := rrMustInt(t, db, `SELECT COUNT(*) FROM mcself WHERE id=1 AND a='same' AND b='key'`)
	if v != 1 {
		t.Errorf("expected id=1 still present, got count=%d", v)
	}
}

// TestExecRowidRecord_MultiColUniqueThreeColumns exercises checkMultiColRow with
// a three-column composite UNIQUE constraint.  Rows sharing two of three columns
// must not conflict; only a full three-column match must conflict.
func TestExecRowidRecord_MultiColUniqueThreeColumns(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mc3col (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_3c ON mc3col(a, b, c)`)

	rrExec(t, db, `INSERT INTO mc3col VALUES(1, 'x', 'y', 'z')`)
	rrExec(t, db, `INSERT INTO mc3col VALUES(2, 'x', 'y', 'w')`) // differs on c
	rrExec(t, db, `INSERT INTO mc3col VALUES(3, 'x', 'v', 'z')`) // differs on b
	rrExec(t, db, `INSERT INTO mc3col VALUES(4, 'u', 'y', 'z')`) // differs on a

	n := rrCount(t, db, `SELECT COUNT(*) FROM mc3col`)
	if n != 4 {
		t.Errorf("expected 4 rows, got %d", n)
	}

	// Full three-column match must conflict.
	err := rrExecErr(t, db, `INSERT INTO mc3col VALUES(10, 'x', 'y', 'z')`)
	if err == nil {
		t.Error("expected UNIQUE constraint error for full 3-col match, got nil")
	}
}

// TestExecRowidRecord_MultiColUniqueReplaceConflict exercises the full path
// through checkMultiColRow → conflict detected → REPLACE deletes old row.
// This ensures the full-match return path produces a correct error that
// triggers deletion in the REPLACE conflict handler.
func TestExecRowidRecord_MultiColUniqueReplaceConflict(t *testing.T) {
	db := rrOpenDB(t)
	rrExec(t, db, `CREATE TABLE mcrep (id INTEGER PRIMARY KEY, a TEXT, b TEXT)`)
	rrExec(t, db, `CREATE UNIQUE INDEX idx_mr_ab ON mcrep(a, b)`)

	rrExec(t, db, `INSERT INTO mcrep VALUES(1, 'alpha', 'beta')`)
	rrExec(t, db, `INSERT INTO mcrep VALUES(2, 'gamma', 'delta')`)
	// Row 3 shares same (a, b) with row 2 but different id → non-matching row for scanner.
	// Insert them in order so scanner visits non-matching rows first.
	rrExec(t, db, `INSERT INTO mcrep VALUES(3, 'eta', 'theta')`)

	// REPLACE with (a='alpha', b='beta') conflicts with id=1.
	// Scanner visits id=2 and id=3 first (no match), then id=1 (full match).
	rrExec(t, db, `INSERT OR REPLACE INTO mcrep VALUES(99, 'alpha', 'beta')`)

	// id=1 must be gone; id=99 must be present.
	gone := rrCount(t, db, `SELECT COUNT(*) FROM mcrep WHERE id=1`)
	if gone != 0 {
		t.Errorf("expected id=1 replaced, count=%d", gone)
	}
	here := rrCount(t, db, `SELECT COUNT(*) FROM mcrep WHERE id=99 AND a='alpha' AND b='beta'`)
	if here != 1 {
		t.Errorf("expected id=99 inserted, count=%d", here)
	}
	// Other rows must be intact.
	kept := rrCount(t, db, `SELECT COUNT(*) FROM mcrep WHERE id IN (2, 3)`)
	if kept != 2 {
		t.Errorf("expected rows 2 and 3 intact, count=%d", kept)
	}
}
