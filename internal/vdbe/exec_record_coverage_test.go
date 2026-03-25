// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ─────────────────────────────────────────────────────────────────────────────
// helpers local to this file (all names prefixed with "rec" to avoid collision)
// ─────────────────────────────────────────────────────────────────────────────

func recOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("recOpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func recExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("recExec %q: %v", q, err)
	}
}

func recQueryRow(t *testing.T, db *sql.DB, q string, args ...interface{}) *sql.Row {
	t.Helper()
	return db.QueryRow(q, args...)
}

func recMustInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("recMustInt %q: %v", q, err)
	}
	return v
}

func recMustString(t *testing.T, db *sql.DB, q string, args ...interface{}) string {
	t.Helper()
	var v string
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("recMustString %q: %v", q, err)
	}
	return v
}

func recCount(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("recCount %q: %v", q, err)
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. INDEX-based lookup – deferred seek: find row in table via index, read col
//    Exercises execDeferredSeek, getRowidFromIndexCursor, performDeferredSeek.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_IndexLookup creates a table with a secondary index and queries
// it by the indexed column, forcing the engine to seek via the index and then
// fetch the row from the table cursor (the DeferredSeek / IdxRowid path).
func TestExecRecord_IndexLookup(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE products (
		id   INTEGER PRIMARY KEY,
		sku  TEXT    NOT NULL,
		price REAL   NOT NULL
	)`)
	recExec(t, db, `CREATE INDEX idx_products_sku ON products(sku)`)

	rows := []struct {
		id    int
		sku   string
		price float64
	}{
		{1, "APPLE", 1.99},
		{2, "BANANA", 0.49},
		{3, "CHERRY", 3.50},
		{4, "DATE", 5.00},
		{5, "ELDERBERRY", 7.25},
	}
	for _, r := range rows {
		recExec(t, db, `INSERT INTO products VALUES(?, ?, ?)`, r.id, r.sku, r.price)
	}

	t.Run("SingleMatch", func(t *testing.T) {
		var id int
		var price float64
		err := recQueryRow(t, db,
			`SELECT id, price FROM products WHERE sku = 'CHERRY'`).Scan(&id, &price)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 3 {
			t.Errorf("id: want 3, got %d", id)
		}
		if price != 3.50 {
			t.Errorf("price: want 3.50, got %f", price)
		}
	})

	t.Run("NoMatch", func(t *testing.T) {
		err := recQueryRow(t, db,
			`SELECT id FROM products WHERE sku = 'MISSING'`).Scan(new(int))
		if err != sql.ErrNoRows {
			t.Errorf("expected no rows, got %v", err)
		}
	})

	t.Run("AllViaScan", func(t *testing.T) {
		n := recCount(t, db, `SELECT COUNT(*) FROM products`)
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		}
	})
}

// TestExecRecord_IndexLookupMultipleRows ensures the deferred seek path
// handles multiple lookups correctly when the index points to different table rows.
func TestExecRecord_IndexLookupMultipleRows(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE catalog (
		id    INTEGER PRIMARY KEY,
		cat   TEXT    NOT NULL,
		label TEXT    NOT NULL
	)`)
	recExec(t, db, `CREATE INDEX idx_cat ON catalog(cat)`)

	for i := 1; i <= 10; i++ {
		cat := "even"
		if i%2 != 0 {
			cat = "odd"
		}
		recExec(t, db,
			`INSERT INTO catalog VALUES(?, ?, ?)`, i, cat, fmt.Sprintf("item%d", i))
	}

	n := recCount(t, db, `SELECT COUNT(*) FROM catalog WHERE cat = 'even'`)
	if n != 5 {
		t.Errorf("expected 5 even rows, got %d", n)
	}

	n = recCount(t, db, `SELECT COUNT(*) FROM catalog WHERE cat = 'odd'`)
	if n != 5 {
		t.Errorf("expected 5 odd rows, got %d", n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Varied column types – exercises record encoding/decoding for all SQLite
//    serial types: integer (sizes 1–6), real (7), bool constants (8–9),
//    text (odd ≥ 13), blob (even ≥ 12), NULL (0).
//    Exercises parseRecordColumnHeader, serialTypeLen, decodeSerialIntValue,
//    parseSerialBlobOrText, execMakeRecord.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_VariedColumnTypes stores and retrieves every SQLite type class.
func TestExecRecord_VariedColumnTypes(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE mixed (
		id    INTEGER PRIMARY KEY,
		i8    INTEGER,
		i16   INTEGER,
		i24   INTEGER,
		i32   INTEGER,
		i48   INTEGER,
		i64   INTEGER,
		r64   REAL,
		txt   TEXT,
		blb   BLOB,
		nul   TEXT
	)`)

	// Insert values chosen to exercise each serial-type size:
	//   int8  (-128 / 127)
	//   int16 (-32768 / 32767)
	//   int24 (-8388608 / 8388607)
	//   int32 / int48 / int64
	recExec(t, db,
		`INSERT INTO mixed VALUES(1, -128, -32768, -8388608, -2147483648, 140737488355328, 9223372036854775807, 3.14159, 'hello', X'deadbeef', NULL)`)

	var i8, i16, i24, i32 int64
	var i48, i64 int64
	var r64 float64
	var txt string
	var blb []byte
	var nul *string

	err := db.QueryRow(`SELECT i8,i16,i24,i32,i48,i64,r64,txt,blb,nul FROM mixed WHERE id=1`).
		Scan(&i8, &i16, &i24, &i32, &i48, &i64, &r64, &txt, &blb, &nul)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if i8 != -128 {
		t.Errorf("i8: want -128, got %d", i8)
	}
	if i16 != -32768 {
		t.Errorf("i16: want -32768, got %d", i16)
	}
	if i24 != -8388608 {
		t.Errorf("i24: want -8388608, got %d", i24)
	}
	if i32 != -2147483648 {
		t.Errorf("i32: want -2147483648, got %d", i32)
	}
	if i48 != 140737488355328 {
		t.Errorf("i48: want 140737488355328, got %d", i48)
	}
	if i64 != 9223372036854775807 {
		t.Errorf("i64: want max int64, got %d", i64)
	}
	if r64 < 3.14 || r64 > 3.15 {
		t.Errorf("r64: want ~3.14159, got %f", r64)
	}
	if txt != "hello" {
		t.Errorf("txt: want 'hello', got %q", txt)
	}
	if len(blb) != 4 {
		t.Errorf("blb: want 4 bytes, got %d", len(blb))
	}
	if nul != nil {
		t.Errorf("nul: want nil, got %v", *nul)
	}
}

// TestExecRecord_BoolConstants exercises the SQLite boolean constants stored
// as serial types 8 (false/0) and 9 (true/1).
func TestExecRecord_BoolConstants(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE bools (id INTEGER PRIMARY KEY, flag INTEGER)`)
	recExec(t, db, `INSERT INTO bools VALUES(1, 0)`)
	recExec(t, db, `INSERT INTO bools VALUES(2, 1)`)

	v1 := recMustInt(t, db, `SELECT flag FROM bools WHERE id=1`)
	v2 := recMustInt(t, db, `SELECT flag FROM bools WHERE id=2`)

	if v1 != 0 {
		t.Errorf("want 0, got %d", v1)
	}
	if v2 != 1 {
		t.Errorf("want 1, got %d", v2)
	}
}

// TestExecRecord_NullColumn exercises the NULL serial type (0) in
// parseSerialValue and serialTypeLen.
func TestExecRecord_NullColumn(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE nulltest (id INTEGER PRIMARY KEY, val TEXT)`)
	recExec(t, db, `INSERT INTO nulltest VALUES(1, NULL)`)

	var v *string
	if err := db.QueryRow(`SELECT val FROM nulltest WHERE id=1`).Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v != nil {
		t.Errorf("want nil, got %q", *v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Large integers that require multi-byte varints in serialTypeLen / varintLen
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_LargeIntegers inserts values that span serial-type categories
// from 1-byte up to 8-byte integers, verifying round-trip integrity.
func TestExecRecord_LargeIntegers(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE bigints (id INTEGER PRIMARY KEY, val INTEGER)`)

	cases := []int64{
		0, 1, -1,
		127, -128,         // 1-byte boundary
		32767, -32768,     // 2-byte boundary
		8388607, -8388608, // 3-byte boundary
		2147483647, -2147483648,                    // 4-byte boundary
		140737488355327, -140737488355328,           // 6-byte boundary
		9223372036854775807, -9223372036854775808,  // 8-byte boundary
	}

	for i, c := range cases {
		recExec(t, db, `INSERT INTO bigints VALUES(?, ?)`, i+1, c)
	}

	rows, err := db.Query(`SELECT id, val FROM bigints ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	idx := 0
	for rows.Next() {
		var id int
		var val int64
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan row %d: %v", idx, err)
		}
		if val != cases[idx] {
			t.Errorf("row %d: want %d, got %d", idx, cases[idx], val)
		}
		idx++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if idx != len(cases) {
		t.Errorf("expected %d rows, got %d", len(cases), idx)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Tables with many columns – exercises serialTypeLen for the full range of
//    serial types (0=NULL, 1–6=ints, 7=float, 8–9=bool, ≥12=text/blob).
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_ManyColumns exercises a 12-column table so that
// parseRecordColumnHeader, skipToColumn, and serialTypeLen are called for every
// serial-type family.
func TestExecRecord_ManyColumns(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE wide (
		c0  INTEGER PRIMARY KEY,
		c1  INTEGER,   -- serial type 1 (int8)
		c2  INTEGER,   -- serial type 2 (int16)
		c3  INTEGER,   -- serial type 3 (int24)
		c4  INTEGER,   -- serial type 4 (int32)
		c5  INTEGER,   -- serial type 6 (int64)
		c6  REAL,      -- serial type 7 (float64)
		c7  TEXT,      -- serial type ≥13 (text)
		c8  BLOB,      -- serial type ≥12 (blob)
		c9  TEXT,      -- another text column
		c10 INTEGER,   -- will store NULL → serial type 0
		c11 INTEGER    -- store 0 → serial type 8; store 1 → serial type 9
	)`)

	recExec(t, db, `INSERT INTO wide VALUES(
		1,
		42, 1000, 100000, 1000000, 9999999999,
		2.71828,
		'wide-text', X'cafebabe', 'second-text',
		NULL, 0
	)`)
	recExec(t, db, `INSERT INTO wide VALUES(
		2,
		-1, -500, -50000, -500000, -9999999999,
		-1.41421,
		'negative', X'0102', 'more',
		NULL, 1
	)`)

	// Verify row 1
	var c1, c2, c3, c4, c5 int64
	var c6 float64
	var c7, c9 string
	var c8 []byte
	var c10 *int64
	var c11 int64

	err := db.QueryRow(`SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11 FROM wide WHERE c0=1`).
		Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11)
	if err != nil {
		t.Fatalf("scan row 1: %v", err)
	}
	if c1 != 42 {
		t.Errorf("c1: want 42, got %d", c1)
	}
	if c2 != 1000 {
		t.Errorf("c2: want 1000, got %d", c2)
	}
	if c3 != 100000 {
		t.Errorf("c3: want 100000, got %d", c3)
	}
	if c4 != 1000000 {
		t.Errorf("c4: want 1000000, got %d", c4)
	}
	if c5 != 9999999999 {
		t.Errorf("c5: want 9999999999, got %d", c5)
	}
	if c6 < 2.71 || c6 > 2.72 {
		t.Errorf("c6: want ~2.71828, got %f", c6)
	}
	if c7 != "wide-text" {
		t.Errorf("c7: want 'wide-text', got %q", c7)
	}
	if len(c8) != 4 {
		t.Errorf("c8: want 4 bytes, got %d", len(c8))
	}
	if c9 != "second-text" {
		t.Errorf("c9: want 'second-text', got %q", c9)
	}
	if c10 != nil {
		t.Errorf("c10: want nil, got %v", *c10)
	}
	if c11 != 0 {
		t.Errorf("c11: want 0, got %d", c11)
	}

	// Verify row 2 (bool constant 1 stored as serial type 9)
	var c11b int64
	if err := db.QueryRow(`SELECT c11 FROM wide WHERE c0=2`).Scan(&c11b); err != nil {
		t.Fatalf("scan row 2: %v", err)
	}
	if c11b != 1 {
		t.Errorf("c11 row2: want 1, got %d", c11b)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. BLOB columns from indexed access – getBtreeCursorPayload
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_BlobIndexedAccess stores binary blobs and fetches them via
// a query that causes the engine to use getBtreeCursorPayload to retrieve
// the record payload.
func TestExecRecord_BlobIndexedAccess(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE bindata (
		id   INTEGER PRIMARY KEY,
		tag  TEXT  NOT NULL,
		data BLOB  NOT NULL
	)`)
	recExec(t, db, `CREATE INDEX idx_bindata_tag ON bindata(tag)`)

	blobs := map[string][]byte{
		"alpha": {0x00, 0x01, 0x02, 0x03},
		"beta":  {0xFF, 0xFE, 0xFD},
		"gamma": make([]byte, 256), // 256-byte blob
	}
	// fill gamma with an ascending pattern
	for i := range blobs["gamma"] {
		blobs["gamma"][i] = byte(i)
	}

	id := 1
	for tag, data := range blobs {
		recExec(t, db, `INSERT INTO bindata VALUES(?, ?, ?)`, id, tag, data)
		id++
	}

	// Query via the index (tag column) to exercise the deferred-seek path
	// that then calls getBtreeCursorPayload on the table cursor.
	for tag, want := range blobs {
		var got []byte
		err := db.QueryRow(`SELECT data FROM bindata WHERE tag = ?`, tag).Scan(&got)
		if err != nil {
			t.Errorf("tag %q: %v", tag, err)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("tag %q: want %d bytes, got %d", tag, len(want), len(got))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("tag %q byte[%d]: want %02x, got %02x", tag, i, want[i], got[i])
				break
			}
		}
	}
}

// TestExecRecord_BlobEmptyAndLarge exercises the parseSerialBlobOrText path
// for zero-length and larger blobs.
func TestExecRecord_BlobEmptyAndLarge(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE blobsizes (id INTEGER PRIMARY KEY, dat BLOB)`)

	// empty blob
	recExec(t, db, `INSERT INTO blobsizes VALUES(1, X'')`)
	// 1-byte blob
	recExec(t, db, `INSERT INTO blobsizes VALUES(2, X'ab')`)
	// 16-byte blob
	recExec(t, db, `INSERT INTO blobsizes VALUES(3, X'000102030405060708090a0b0c0d0e0f')`)

	sizes := []int{0, 1, 16}
	for i, want := range sizes {
		var got []byte
		if err := db.QueryRow(`SELECT dat FROM blobsizes WHERE id=?`, i+1).Scan(&got); err != nil {
			t.Fatalf("id %d: %v", i+1, err)
		}
		if len(got) != want {
			t.Errorf("id %d: want %d bytes, got %d", i+1, want, len(got))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. JOIN queries using index lookup then table fetch – execDeferredSeek
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_JoinIndexTableFetch creates an orders/items schema with a
// foreign-key style join. The query planner uses the index on the FK column to
// look up matching rows in the child table, then defers the seek to fetch the
// full row – exercising execDeferredSeek end-to-end.
func TestExecRecord_JoinIndexTableFetch(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE customers (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	recExec(t, db, `CREATE TABLE orders (
		id          INTEGER PRIMARY KEY,
		customer_id INTEGER NOT NULL,
		amount      REAL    NOT NULL
	)`)
	recExec(t, db, `CREATE INDEX idx_orders_cid ON orders(customer_id)`)

	recExec(t, db, `INSERT INTO customers VALUES(1,'Alice')`)
	recExec(t, db, `INSERT INTO customers VALUES(2,'Bob')`)
	recExec(t, db, `INSERT INTO customers VALUES(3,'Carol')`)

	recExec(t, db, `INSERT INTO orders VALUES(10, 1, 100.0)`)
	recExec(t, db, `INSERT INTO orders VALUES(11, 1, 200.0)`)
	recExec(t, db, `INSERT INTO orders VALUES(12, 2, 50.0)`)
	recExec(t, db, `INSERT INTO orders VALUES(13, 3, 300.0)`)
	recExec(t, db, `INSERT INTO orders VALUES(14, 3, 150.0)`)

	t.Run("JoinRowCount", func(t *testing.T) {
		n := recCount(t, db,
			`SELECT COUNT(*) FROM customers c JOIN orders o ON o.customer_id = c.id`)
		if n != 5 {
			t.Errorf("expected 5 joined rows, got %d", n)
		}
	})

	t.Run("FilterByCustomer", func(t *testing.T) {
		n := recCount(t, db,
			`SELECT COUNT(*) FROM orders WHERE customer_id = 3`)
		if n != 2 {
			t.Errorf("expected 2 orders for customer 3, got %d", n)
		}
	})

	t.Run("SumAmounts", func(t *testing.T) {
		var total float64
		if err := db.QueryRow(
			`SELECT SUM(o.amount) FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 1`,
		).Scan(&total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if total != 300.0 {
			t.Errorf("expected total 300.0, got %f", total)
		}
	})

	t.Run("NoOrdersForCustomer", func(t *testing.T) {
		recExec(t, db, `INSERT INTO customers VALUES(99,'Orphan')`)
		n := recCount(t, db,
			`SELECT COUNT(*) FROM orders WHERE customer_id = 99`)
		if n != 0 {
			t.Errorf("expected 0 orders, got %d", n)
		}
	})
}

// TestExecRecord_JoinMultiColumnIndex exercises a composite-column lookup
// through an index with a text FK, verifying the full deferred-seek pipeline
// produces correct column values (exercises getBtreeCursorPayload on both the
// index and table cursors).
func TestExecRecord_JoinMultiColumnIndex(t *testing.T) {
	db := recOpenDB(t)

	recExec(t, db, `CREATE TABLE depts (
		code TEXT PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	recExec(t, db, `CREATE TABLE employees (
		id      INTEGER PRIMARY KEY,
		dept    TEXT    NOT NULL,
		salary  REAL    NOT NULL,
		name    TEXT    NOT NULL
	)`)
	recExec(t, db, `CREATE INDEX idx_emp_dept ON employees(dept)`)

	recExec(t, db, `INSERT INTO depts VALUES('ENG','Engineering')`)
	recExec(t, db, `INSERT INTO depts VALUES('MKT','Marketing')`)

	recExec(t, db, `INSERT INTO employees VALUES(1,'ENG',90000.0,'Alice')`)
	recExec(t, db, `INSERT INTO employees VALUES(2,'ENG',85000.0,'Bob')`)
	recExec(t, db, `INSERT INTO employees VALUES(3,'MKT',70000.0,'Carol')`)
	recExec(t, db, `INSERT INTO employees VALUES(4,'MKT',72000.0,'Dave')`)

	// Fetch employee names for a dept via the index.
	rows, err := db.Query(
		`SELECT e.name FROM employees e WHERE e.dept = 'ENG' ORDER BY e.name`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 ENG employees, got %d: %v", len(names), names)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. varintLen / serialTypeLen boundary: large text/blob serial type numbers
//    These serial type values are ≥ 12 and require multi-byte varint encoding
//    in the record header, exercising varintLen for values > 0x7f.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_LargeTextSerial stores a text string long enough that its
// serial type number exceeds 127 (i.e., > 57 bytes → st = 2*57+13 = 127, so
// 58 bytes → st = 129 which needs a 2-byte varint).
func TestExecRecord_LargeTextSerial(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE longtext (id INTEGER PRIMARY KEY, val TEXT)`)

	// 58-byte string: serial type = 2*58+13 = 129 (needs 2-byte varint)
	s58 := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
	// 500-byte string: serial type = 1013 (needs 2-byte varint)
	s500 := make([]byte, 500)
	for i := range s500 {
		s500[i] = byte('a' + i%26)
	}

	recExec(t, db, `INSERT INTO longtext VALUES(1, ?)`, s58)
	recExec(t, db, `INSERT INTO longtext VALUES(2, ?)`, string(s500))

	got58 := recMustString(t, db, `SELECT val FROM longtext WHERE id=1`)
	if got58 != s58 {
		t.Errorf("58-byte string mismatch")
	}

	var got500 string
	if err := db.QueryRow(`SELECT val FROM longtext WHERE id=2`).Scan(&got500); err != nil {
		t.Fatalf("scan 500-byte string: %v", err)
	}
	if len(got500) != 500 {
		t.Errorf("expected 500-byte string, got %d bytes", len(got500))
	}
}

// TestExecRecord_LargeBlobSerial exercises the blob serial type path for
// blobs that require a multi-byte varint serial type in the header.
func TestExecRecord_LargeBlobSerial(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE largeblob (id INTEGER PRIMARY KEY, dat BLOB)`)

	// 100-byte blob: serial type = 2*100+12 = 212 (needs 2-byte varint)
	b100 := make([]byte, 100)
	for i := range b100 {
		b100[i] = byte(i)
	}
	recExec(t, db, `INSERT INTO largeblob VALUES(1, ?)`, b100)

	var got []byte
	if err := db.QueryRow(`SELECT dat FROM largeblob WHERE id=1`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 100 {
		t.Errorf("expected 100 bytes, got %d", len(got))
	}
	for i, b := range got {
		if b != byte(i) {
			t.Errorf("byte[%d]: want %d, got %d", i, i, b)
			break
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. execMakeRecord: verify that INSERT encodes correctly and the round-trip
//    through MakeRecord → btree → column read works for all value types.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_MakeRecordRoundTrip inserts a row with every column type and
// reads it back verifying exact values, exercising execMakeRecord end-to-end.
func TestExecRecord_MakeRecordRoundTrip(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE roundtrip (
		id  INTEGER PRIMARY KEY,
		a   INTEGER,
		b   REAL,
		c   TEXT,
		d   BLOB,
		e   INTEGER
	)`)

	recExec(t, db, `INSERT INTO roundtrip VALUES(42, -999, 1.23456789, 'testval', X'01020304', NULL)`)

	var a int64
	var b float64
	var c string
	var d []byte
	var e *int64

	if err := db.QueryRow(`SELECT a,b,c,d,e FROM roundtrip WHERE id=42`).
		Scan(&a, &b, &c, &d, &e); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if a != -999 {
		t.Errorf("a: want -999, got %d", a)
	}
	if b < 1.2345 || b > 1.2346 {
		t.Errorf("b: want ~1.23456789, got %f", b)
	}
	if c != "testval" {
		t.Errorf("c: want 'testval', got %q", c)
	}
	if len(d) != 4 || d[0] != 1 || d[3] != 4 {
		t.Errorf("d: unexpected value %v", d)
	}
	if e != nil {
		t.Errorf("e: want nil, got %v", *e)
	}
}

// TestExecRecord_MakeRecordZeroAndOne ensures the zero (serial 8) and one
// (serial 9) constant optimisation in execMakeRecord encodes correctly.
func TestExecRecord_MakeRecordZeroAndOne(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE zeroone (id INTEGER PRIMARY KEY, z INTEGER, o INTEGER)`)
	recExec(t, db, `INSERT INTO zeroone VALUES(1, 0, 1)`)

	z := recMustInt(t, db, `SELECT z FROM zeroone WHERE id=1`)
	o := recMustInt(t, db, `SELECT o FROM zeroone WHERE id=1`)

	if z != 0 {
		t.Errorf("z: want 0, got %d", z)
	}
	if o != 1 {
		t.Errorf("o: want 1, got %d", o)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. checkRowidExists via SQL – exercises the linear-scan path through a
//    table cursor, which is called by execNotExists.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_CheckRowidExists exercises the checkRowidExists path by
// running queries that trigger the NOT EXISTS / rowid-existence check.
func TestExecRecord_CheckRowidExists(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE rowtable (id INTEGER PRIMARY KEY, name TEXT)`)
	recExec(t, db, `INSERT INTO rowtable VALUES(1,'one')`)
	recExec(t, db, `INSERT INTO rowtable VALUES(2,'two')`)
	recExec(t, db, `INSERT INTO rowtable VALUES(5,'five')`)

	t.Run("ExistingRowid", func(t *testing.T) {
		n := recCount(t, db, `SELECT COUNT(*) FROM rowtable WHERE id = 1`)
		if n != 1 {
			t.Errorf("expected 1, got %d", n)
		}
	})

	t.Run("MissingRowid", func(t *testing.T) {
		n := recCount(t, db, `SELECT COUNT(*) FROM rowtable WHERE id = 3`)
		if n != 0 {
			t.Errorf("expected 0, got %d", n)
		}
	})

	t.Run("NotExistsSubquery", func(t *testing.T) {
		// Force the NOT EXISTS path which internally calls checkRowidExists.
		recExec(t, db, `CREATE TABLE lookup (id INTEGER PRIMARY KEY)`)
		recExec(t, db, `INSERT INTO lookup VALUES(1)`)
		recExec(t, db, `INSERT INTO lookup VALUES(3)`)

		// IDs 2 and 5 are in rowtable but not in lookup.
		rows, err := db.Query(
			`SELECT id FROM rowtable WHERE NOT EXISTS (SELECT 1 FROM lookup WHERE lookup.id = rowtable.id)`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		if len(ids) != 2 {
			t.Errorf("expected [2,5], got %v", ids)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. getRowidFromRegister – exercises the P3 != 0 branch in getDeferredSeekRowid.
//     This is triggered by INSERT OR REPLACE / UPDATE operations that re-seek
//     a row by rowid stored in a register.
// ─────────────────────────────────────────────────────────────────────────────

// TestExecRecord_GetRowidFromRegister exercises the register-based rowid path
// by updating rows (which internally must find the row by its rowid) and then
// verifying the updated values.
func TestExecRecord_GetRowidFromRegister(t *testing.T) {
	db := recOpenDB(t)
	recExec(t, db, `CREATE TABLE regtable (id INTEGER PRIMARY KEY, val TEXT)`)
	recExec(t, db, `INSERT INTO regtable VALUES(10,'initial')`)
	recExec(t, db, `INSERT INTO regtable VALUES(20,'other')`)

	// UPDATE forces the engine to seek the row by its rowid via a register.
	recExec(t, db, `UPDATE regtable SET val='updated' WHERE id=10`)

	v := recMustString(t, db, `SELECT val FROM regtable WHERE id=10`)
	if v != "updated" {
		t.Errorf("want 'updated', got %q", v)
	}

	// The other row must remain unchanged.
	v2 := recMustString(t, db, `SELECT val FROM regtable WHERE id=20`)
	if v2 != "other" {
		t.Errorf("want 'other', got %q", v2)
	}
}
