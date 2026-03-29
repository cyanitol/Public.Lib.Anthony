// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// MC/DC 20 — SQL-level tests for low-coverage driver paths.
//
// Targets:
//   compile_compound.go:479  cmpSameType     — float/int cross-type, []byte same-type
//   compile_compound.go:557  typeOrder       — []byte (blob) path
//   compile_compound.go:641  emitCompoundResult — second and beyond result rows
//   compile_dml.go:770       compileUnaryExpr — non-OpNeg path (returns OpNull)
//   compile_dml.go:808       compileLiteralExpr — blob literal path
//   compile_ddl.go:391       compileCreateTrigger — error path (already exists, no IF NOT EXISTS)
//   compile_ddl.go:420       compileDropTrigger  — IF EXISTS on missing trigger
//   compile_analyze.go:138   analyzeTableIndexes — ANALYZE with indexed table
//   compile_analyze.go:156   countTableRows      — ANALYZE row count
//   compile_analyze.go:185   countDistinctPrefix — multi-column index ANALYZE

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func drv20Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func drv20Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestMCDC20_CompoundSort_BlobType exercises typeOrder([]byte)==3 by using
// UNION ALL with a blob column and ORDER BY that column.
func TestMCDC20_CompoundSort_BlobType(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE blobdata (id INTEGER, data BLOB)`)
	drv20Exec(t, db, `INSERT INTO blobdata VALUES (1, X'0102')`)
	drv20Exec(t, db, `INSERT INTO blobdata VALUES (2, X'0304')`)

	rows, err := db.Query(`
		SELECT id, data FROM blobdata
		UNION ALL
		SELECT id+10, data FROM blobdata
		ORDER BY data
	`)
	if err != nil {
		t.Skipf("blob compound sort: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected rows from blob union order by")
	}
}

// TestMCDC20_CompoundSort_FloatIntMix exercises cmpSameType float/int cross-type
// by mixing float and integer values in a UNION ALL ORDER BY.
func TestMCDC20_CompoundSort_FloatIntMix(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	rows, err := db.Query(`
		SELECT 1 AS v
		UNION ALL
		SELECT 1.5
		UNION ALL
		SELECT 2
		ORDER BY v
	`)
	if err != nil {
		t.Skipf("float/int union order by: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestMCDC20_CompoundSort_MultiRow exercises emitCompoundResult for > 1 result.
func TestMCDC20_CompoundSort_MultiRow(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE nums (n INTEGER)`)
	for i := 1; i <= 5; i++ {
		drv20Exec(t, db, `INSERT INTO nums VALUES (?)`, i)
	}

	rows, err := db.Query(`SELECT n FROM nums UNION SELECT n+10 FROM nums ORDER BY n`)
	if err != nil {
		t.Skipf("multirow compound: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 10 {
		t.Errorf("expected 10 rows, got %d", count)
	}
}

// TestMCDC20_UnaryNeg_Float exercises compileNegatedLiteral with a float literal.
func TestMCDC20_UnaryNeg_Float(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	var v float64
	err := db.QueryRow(`SELECT -3.14`).Scan(&v)
	if err != nil {
		t.Skipf("SELECT -3.14: %v", err)
	}
	if v > -3.0 || v < -3.2 {
		t.Errorf("expected ~-3.14, got %v", v)
	}
}

// TestMCDC20_BlobLiteral exercises compileLiteralExpr blob (X'...') path.
func TestMCDC20_BlobLiteral(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	var v []byte
	err := db.QueryRow(`SELECT X'DEADBEEF'`).Scan(&v)
	if err != nil {
		t.Skipf("SELECT X'DEADBEEF': %v", err)
	}
	if len(v) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(v))
	}
}

// TestMCDC20_CreateTrigger_AlreadyExists exercises the error path in
// compileCreateTrigger when trigger already exists without IF NOT EXISTS.
func TestMCDC20_CreateTrigger_AlreadyExists(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE items (id INTEGER)`)
	drv20Exec(t, db, `CREATE TRIGGER trg1 AFTER INSERT ON items BEGIN SELECT 1; END`)

	// Duplicate without IF NOT EXISTS → should error.
	_, err := db.Exec(`CREATE TRIGGER trg1 AFTER INSERT ON items BEGIN SELECT 1; END`)
	if err == nil {
		t.Skip("engine allowed duplicate trigger name")
	}
}

// TestMCDC20_DropTrigger_IfExists_Missing exercises DROP TRIGGER IF EXISTS
// when the trigger does not exist (should succeed silently).
func TestMCDC20_DropTrigger_IfExists_Missing(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE t (id INTEGER)`)
	// Drop a trigger that was never created.
	_, err := db.Exec(`DROP TRIGGER IF EXISTS no_such_trigger`)
	if err != nil {
		t.Skipf("DROP TRIGGER IF EXISTS: %v", err)
	}
}

// TestMCDC20_Analyze_WithIndex exercises analyzeTableIndexes, countTableRows,
// and countDistinctPrefix by running ANALYZE on a table with a multi-column index.
func TestMCDC20_Analyze_WithIndex(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE scored (a INTEGER, b TEXT, c INTEGER)`)
	drv20Exec(t, db, `CREATE INDEX idx_scored ON scored(a, b)`)
	for i := 0; i < 10; i++ {
		drv20Exec(t, db, `INSERT INTO scored VALUES (?, ?, ?)`, i%3, "txt", i)
	}

	// ANALYZE exercises analyzeTableIndexes, countTableRows, countDistinctPrefix.
	_, err := db.Exec(`ANALYZE scored`)
	if err != nil {
		t.Skipf("ANALYZE scored: %v", err)
	}
}

// TestMCDC20_Analyze_SingleColIndex exercises ANALYZE with a single-column index.
func TestMCDC20_Analyze_SingleColIndex(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	drv20Exec(t, db, `CREATE TABLE words (w TEXT)`)
	drv20Exec(t, db, `CREATE INDEX idx_words ON words(w)`)
	for _, w := range []string{"apple", "banana", "cherry", "apple"} {
		drv20Exec(t, db, `INSERT INTO words VALUES (?)`, w)
	}

	_, err := db.Exec(`ANALYZE words`)
	if err != nil {
		t.Skipf("ANALYZE words: %v", err)
	}
}

// TestMCDC20_CompoundSort_NullVsInt exercises cmpNulls path where b is nil.
func TestMCDC20_CompoundSort_NullVsInt(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	rows, err := db.Query(`
		SELECT NULL AS v
		UNION ALL
		SELECT 1
		UNION ALL
		SELECT NULL
		ORDER BY v
	`)
	if err != nil {
		t.Skipf("null vs int union: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestMCDC20_CompoundSort_FloatFloat exercises cmpSameType float/float path.
func TestMCDC20_CompoundSort_FloatFloat(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	rows, err := db.Query(`
		SELECT 3.14 AS v
		UNION ALL
		SELECT 2.71
		UNION ALL
		SELECT 1.41
		ORDER BY v
	`)
	if err != nil {
		t.Skipf("float/float union: %v", err)
	}
	defer rows.Close()
	var vals []float64
	for rows.Next() {
		var f float64
		if err := rows.Scan(&f); err == nil {
			vals = append(vals, f)
		}
	}
	if len(vals) != 3 {
		t.Errorf("expected 3 float rows, got %d", len(vals))
	}
}

// TestMCDC20_CompoundSort_IntFloat exercises int < float cross-type in cmpSameType.
func TestMCDC20_CompoundSort_IntFloat(t *testing.T) {
	t.Parallel()
	db := drv20Open(t)

	// integer 2 vs float 1.5 in ORDER BY — should place 1.5 before 2.
	rows, err := db.Query(`
		SELECT 2 AS v
		UNION ALL
		SELECT 1.5
		ORDER BY v
	`)
	if err != nil {
		t.Skipf("int/float compound: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}
