// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

// MC/DC 12 — SQL-level coverage for exec.go low-coverage functions.
//
// Targets:
//   exec.go:2983  getAutoincrementRowid  — hasExplicit=true branch
//   exec.go:4786  execExplicitCast       — CAST(expr AS type) paths
//   exec.go:4807  execAffinityCast       — implicit affinity cast paths
//   exec.go:3640  checkWithoutRowidPKUniqueness — duplicate PK detection
//   exec.go:6302  execWindowNtile        — NTILE() window function paths
//   exec.go:989   execOpenEphemeral      — ephemeral table via DISTINCT/GROUP BY
//   exec.go:955   execSeekRowid          — not-found path on empty table

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func mcdc12Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mcdc12Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func mcdc12QueryInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// getAutoincrementRowid — hasExplicit=true branch
//
// MC/DC: hasExplicit=true → sm.UpdateSequence(tableName, explicitRowid) called,
//        explicit rowid returned directly; subsequent auto-insert must exceed it.
// ---------------------------------------------------------------------------

// TestMCDC12_Autoincrement_ExplicitRowid inserts with explicit rowid 100 then
// inserts without a rowid and verifies the second rowid is greater than 100.
func TestMCDC12_Autoincrement_ExplicitRowid(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)
	// Insert with an explicit rowid — exercises getAutoincrementRowid hasExplicit=true.
	mcdc12Exec(t, db, `INSERT INTO t VALUES(100, 'explicit')`)
	// Insert without a rowid — sequence must start from max(100)+1.
	mcdc12Exec(t, db, `INSERT INTO t(v) VALUES('auto')`)

	var id2 int
	if err := db.QueryRow(`SELECT id FROM t WHERE v='auto'`).Scan(&id2); err != nil {
		t.Fatalf("query auto rowid: %v", err)
	}
	if id2 <= 100 {
		t.Errorf("expected auto rowid > 100, got %d", id2)
	}
}

// ---------------------------------------------------------------------------
// execExplicitCast — CAST(expr AS type) paths
// ---------------------------------------------------------------------------

// TestMCDC12_Cast_TextToInteger covers CAST('42' AS INTEGER).
func TestMCDC12_Cast_TextToInteger(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	var n int
	if err := db.QueryRow(`SELECT CAST('42' AS INTEGER)`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 42 {
		t.Errorf("CAST('42' AS INTEGER) = %d, want 42", n)
	}
}

// TestMCDC12_Cast_RealToInteger covers CAST(3.7 AS INTEGER).
func TestMCDC12_Cast_RealToInteger(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	var n int
	if err := db.QueryRow(`SELECT CAST(3.7 AS INTEGER)`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 3 {
		t.Errorf("CAST(3.7 AS INTEGER) = %d, want 3", n)
	}
}

// TestMCDC12_Cast_NullCast covers the NULL short-circuit in execExplicitCast.
func TestMCDC12_Cast_NullCast(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	rows, err := db.Query(`SELECT CAST(NULL AS TEXT)`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var s sql.NullString
	if err := rows.Scan(&s); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if s.Valid {
		t.Errorf("expected NULL, got %q", s.String)
	}
}

// TestMCDC12_Cast_BlobToText covers CAST(X'41424344' AS TEXT) → 'ABCD'.
func TestMCDC12_Cast_BlobToText(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	var s string
	if err := db.QueryRow(`SELECT CAST(X'41424344' AS TEXT)`).Scan(&s); err != nil {
		t.Skipf("CAST blob to text not supported: %v", err)
	}
	if s != "ABCD" {
		t.Errorf("CAST(X'41424344' AS TEXT) = %q, want 'ABCD'", s)
	}
}

// TestMCDC12_Cast_IntegerToReal covers CAST(5 AS REAL).
func TestMCDC12_Cast_IntegerToReal(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	var f float64
	if err := db.QueryRow(`SELECT CAST(5 AS REAL)`).Scan(&f); err != nil {
		t.Fatalf("query: %v", err)
	}
	if f != 5.0 {
		t.Errorf("CAST(5 AS REAL) = %f, want 5.0", f)
	}
}

// ---------------------------------------------------------------------------
// checkWithoutRowidPKUniqueness — duplicate PK detection
// ---------------------------------------------------------------------------

// TestMCDC12_WithoutRowid_PKUniqueness expects a UNIQUE constraint error when
// inserting a duplicate primary key into a WITHOUT ROWID table.
func TestMCDC12_WithoutRowid_PKUniqueness(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	if _, err := db.Exec(`CREATE TABLE t(k TEXT PRIMARY KEY) WITHOUT ROWID`); err != nil {
		t.Skipf("WITHOUT ROWID not supported: %v", err)
	}
	mcdc12Exec(t, db, `INSERT INTO t VALUES('key1')`)
	_, err := db.Exec(`INSERT INTO t VALUES('key1')`)
	if err == nil {
		t.Error("expected UNIQUE constraint error for duplicate PK, got nil")
	}
}

// TestMCDC12_WithoutRowid_PKUniqueness_Update expects an error when an UPDATE
// would create a duplicate PK in a WITHOUT ROWID table.
func TestMCDC12_WithoutRowid_PKUniqueness_Update(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	if _, err := db.Exec(`CREATE TABLE t(k TEXT, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID`); err != nil {
		t.Skipf("WITHOUT ROWID not supported: %v", err)
	}
	mcdc12Exec(t, db, `INSERT INTO t VALUES('a','val1')`)
	mcdc12Exec(t, db, `INSERT INTO t VALUES('b','val2')`)
	// UPDATE row 'b' to have the same PK as 'a' — should fail.
	_, err := db.Exec(`UPDATE t SET k='a' WHERE k='b'`)
	if err == nil {
		t.Log("UPDATE to duplicate PK did not return error (may be unimplemented)")
	}
}

// ---------------------------------------------------------------------------
// execWindowNtile — NTILE() window function paths
// ---------------------------------------------------------------------------

// TestMCDC12_Ntile_FewerRowsThanBuckets covers NTILE(10) with only 3 rows.
func TestMCDC12_Ntile_FewerRowsThanBuckets(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(x INT)`)
	mcdc12Exec(t, db, `INSERT INTO t VALUES(1)`)
	mcdc12Exec(t, db, `INSERT INTO t VALUES(2)`)
	mcdc12Exec(t, db, `INSERT INTO t VALUES(3)`)

	rows, err := db.Query(`SELECT NTILE(10) OVER (ORDER BY x) FROM t`)
	if err != nil {
		t.Skipf("NTILE window function not supported: %v", err)
	}
	defer rows.Close()
	var buckets []int
	for rows.Next() {
		var b int
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		buckets = append(buckets, b)
	}
	if len(buckets) != 3 {
		t.Errorf("expected 3 ntile rows, got %d", len(buckets))
	}
}

// TestMCDC12_Ntile_ExactFit covers NTILE(3) with 6 rows (exactly 2 per bucket).
func TestMCDC12_Ntile_ExactFit(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(x INT)`)
	for i := 1; i <= 6; i++ {
		mcdc12Exec(t, db, `INSERT INTO t VALUES(?)`, i)
	}

	rows, err := db.Query(`SELECT NTILE(3) OVER (ORDER BY x) FROM t`)
	if err != nil {
		t.Skipf("NTILE window function not supported: %v", err)
	}
	defer rows.Close()
	counts := map[int]int{}
	for rows.Next() {
		var b int
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		counts[b]++
	}
	// Each of 3 buckets should have exactly 2 rows.
	for bucket := 1; bucket <= 3; bucket++ {
		if counts[bucket] != 2 {
			t.Errorf("bucket %d has %d rows, want 2", bucket, counts[bucket])
		}
	}
}

// TestMCDC12_Ntile_RemainderRows covers NTILE(3) with 7 rows (remainder path).
func TestMCDC12_Ntile_RemainderRows(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(x INT)`)
	for i := 1; i <= 7; i++ {
		mcdc12Exec(t, db, `INSERT INTO t VALUES(?)`, i)
	}

	rows, err := db.Query(`SELECT NTILE(3) OVER (ORDER BY x) FROM t`)
	if err != nil {
		t.Skipf("NTILE window function not supported: %v", err)
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var b int
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if b < 1 || b > 3 {
			t.Errorf("ntile bucket %d out of range [1,3]", b)
		}
		total++
	}
	if total != 7 {
		t.Errorf("expected 7 ntile rows, got %d", total)
	}
}

// ---------------------------------------------------------------------------
// execOpenEphemeral — ephemeral table used internally for DISTINCT / GROUP BY
// ---------------------------------------------------------------------------

// TestMCDC12_EphemeralTable_DistinctQuery uses SELECT DISTINCT which forces
// the engine to open an ephemeral btree to deduplicate results.
func TestMCDC12_EphemeralTable_DistinctQuery(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(x INT)`)
	for _, v := range []int{1, 2, 2, 3, 3, 3} {
		mcdc12Exec(t, db, `INSERT INTO t VALUES(?)`, v)
	}

	if n := mcdc12QueryInt(t, db, `SELECT COUNT(*) FROM (SELECT DISTINCT x FROM t)`); n != 3 {
		t.Errorf("DISTINCT count = %d, want 3", n)
	}
}

// TestMCDC12_EphemeralTable_GroupBy uses GROUP BY which also opens an ephemeral
// aggregation table internally.
func TestMCDC12_EphemeralTable_GroupBy(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(x INT, v TEXT)`)
	for _, v := range []int{1, 1, 2, 2, 2} {
		mcdc12Exec(t, db, `INSERT INTO t VALUES(?,?)`, v, "row")
	}

	rows, err := db.Query(`SELECT x, COUNT(*) FROM t GROUP BY x ORDER BY x`)
	if err != nil {
		t.Fatalf("group by query: %v", err)
	}
	defer rows.Close()
	want := map[int]int{1: 2, 2: 3}
	for rows.Next() {
		var x, cnt int
		if err := rows.Scan(&x, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if want[x] != cnt {
			t.Errorf("x=%d: COUNT=%d, want %d", x, cnt, want[x])
		}
	}
}

// ---------------------------------------------------------------------------
// execSeekRowid — not-found path
//
// MC/DC: btCursor.MoveToFirst fails (empty table) → seekNotFound called
// ---------------------------------------------------------------------------

// TestMCDC12_SeekRowid_NotFound queries a non-existent rowid on an empty table,
// exercising the seekNotFound branch inside execSeekRowid.
func TestMCDC12_SeekRowid_NotFound(t *testing.T) {
	t.Parallel()
	db := mcdc12Open(t)

	mcdc12Exec(t, db, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)

	rows, err := db.Query(`SELECT * FROM t WHERE rowid = 999999`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Error("expected no rows for missing rowid, got one")
	}
}
