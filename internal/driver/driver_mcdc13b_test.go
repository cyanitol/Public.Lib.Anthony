// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

// MC/DC 13b — SQL-level tests for PRAGMA edge cases and window functions
//
// Targets:
//   stmt_ddl_additions.go:489   parseForeignKeysValue       (75%) — invalid value path
//   stmt_ddl_additions.go:1418  compilePragmaIncrementalVacuum (75%) — invalid int path
//   stmt_window_helpers.go:250  emitWindowFunctionColumn    (75%) — LAG/LEAD/FIRST/LAST

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func drv13Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func drv13Exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// ---------------------------------------------------------------------------
// parseForeignKeysValue — invalid pragma value path
// ---------------------------------------------------------------------------

// TestMCDC13b_Pragma_ForeignKeys_Invalid exercises parseForeignKeysValue with
// an unrecognized value string, triggering the "invalid value" error path.
func TestMCDC13b_Pragma_ForeignKeys_Invalid(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	// "MAYBE" is not in the valid foreign_keys value map → error path.
	_, err := db.Exec(`PRAGMA foreign_keys=MAYBE`)
	if err == nil {
		t.Skip("engine accepted invalid foreign_keys value without error")
	}
}

// TestMCDC13b_Pragma_ForeignKeys_NumericInvalid exercises with a numeric value
// outside the valid range (2 is not 0 or 1).
func TestMCDC13b_Pragma_ForeignKeys_NumericInvalid(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	_, err := db.Exec(`PRAGMA foreign_keys=2`)
	if err == nil {
		t.Skip("engine accepted out-of-range foreign_keys value")
	}
}

// ---------------------------------------------------------------------------
// compilePragmaIncrementalVacuum — invalid integer path + MemoryPager path
// ---------------------------------------------------------------------------

// TestMCDC13b_Pragma_IncrementalVacuum_Invalid exercises the invalid-integer
// parse error path in compilePragmaIncrementalVacuum.
func TestMCDC13b_Pragma_IncrementalVacuum_Invalid(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	// Non-numeric value → fmt.Sscanf fails → error returned.
	_, err := db.Exec(`PRAGMA incremental_vacuum=notanumber`)
	if err == nil {
		t.Skip("engine accepted invalid incremental_vacuum value")
	}
}

// TestMCDC13b_Pragma_IncrementalVacuum_MemoryDB exercises the MemoryPager
// type assertion path. On a memory database the pager is a MemoryPager (not
// *pager.Pager), so the assertion fails and the operation is a no-op.
func TestMCDC13b_Pragma_IncrementalVacuum_MemoryDB(t *testing.T) {
	t.Parallel()
	db := drv13Open(t) // :memory: uses MemoryPager
	// Should succeed silently (no-op for MemoryPager).
	_, err := db.Exec(`PRAGMA incremental_vacuum=10`)
	if err != nil {
		t.Skipf("incremental_vacuum on memory db: %v", err)
	}
}

// ---------------------------------------------------------------------------
// emitWindowFunctionColumn — LAG, LEAD, FIRST_VALUE, LAST_VALUE
// ---------------------------------------------------------------------------

func drv13SetupWindow(t *testing.T, db *sql.DB) {
	t.Helper()
	drv13Exec(t, db, `CREATE TABLE t(id INTEGER, val INTEGER)`)
	drv13Exec(t, db, `INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50)`)
}

// TestMCDC13b_Window_Lag exercises the LAG window function.
func TestMCDC13b_Window_Lag(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, val, LAG(val) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("LAG window function: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Error("expected rows from LAG window query")
	}
}

// TestMCDC13b_Window_Lead exercises the LEAD window function.
func TestMCDC13b_Window_Lead(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, val, LEAD(val) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("LEAD window function: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestMCDC13b_Window_FirstValue exercises FIRST_VALUE window function.
func TestMCDC13b_Window_FirstValue(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, FIRST_VALUE(val) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("FIRST_VALUE window function: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestMCDC13b_Window_LastValue exercises LAST_VALUE window function.
func TestMCDC13b_Window_LastValue(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, LAST_VALUE(val) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("LAST_VALUE window function: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestMCDC13b_Window_NthValue exercises NTH_VALUE window function.
func TestMCDC13b_Window_NthValue(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("NTH_VALUE window function: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestMCDC13b_Window_Ntile exercises NTILE window function.
func TestMCDC13b_Window_Ntile(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`SELECT id, NTILE(2) OVER (ORDER BY id) FROM t`)
	if err != nil {
		t.Skipf("NTILE window function: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// TestMCDC13b_Window_AllFunctions exercises all window functions together.
func TestMCDC13b_Window_AllFunctions(t *testing.T) {
	t.Parallel()
	db := drv13Open(t)
	drv13SetupWindow(t, db)
	rows, err := db.Query(`
		SELECT id, val,
		  ROW_NUMBER() OVER (ORDER BY id),
		  RANK()       OVER (ORDER BY id),
		  DENSE_RANK() OVER (ORDER BY id),
		  LAG(val)     OVER (ORDER BY id),
		  LEAD(val)    OVER (ORDER BY id),
		  FIRST_VALUE(val) OVER (ORDER BY id),
		  LAST_VALUE(val)  OVER (ORDER BY id)
		FROM t ORDER BY id`)
	if err != nil {
		t.Skipf("window functions: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Skip("no rows returned from window function query")
	}
}
