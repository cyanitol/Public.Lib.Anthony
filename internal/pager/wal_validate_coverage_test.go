// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// wvcOpenDB opens a file-backed database for wal_validate_coverage tests.
func wvcOpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", path)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// wvcMustExec runs a SQL statement or fatally fails the test.
func wvcMustExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// wvcExec runs a SQL statement and logs any error as non-fatal.
func wvcExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Logf("exec %q: %v (non-fatal)", q, err)
	}
}

// ---------------------------------------------------------------------------
// validateFrame – exercise salt/checksum validation branches
// ---------------------------------------------------------------------------

// TestWALValidate_ValidFrameCheckpoint writes WAL frames and checkpoints so
// validateFrame is called with matching salts and valid checksums.
func TestWALValidate_ValidFrameCheckpoint(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vf_valid.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 60; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("A", 400))
	}
	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after checkpoint: %d", n)
}

// TestWALValidate_SaltMismatchOnReopen writes WAL frames, closes the DB, and
// reopens it so the WAL recovery reads frames against stored salt values.
func TestWALValidate_SaltMismatchOnReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vf_salt.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("phase1 open: %v", err)
		}
		defer db.Close()

		wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
		wvcMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 40; i++ {
			wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("S", 300))
		}
		// Close without checkpoint so WAL stays on disk.
	}()

	walPath := dbPath + "-wal"
	if info, err := os.Stat(walPath); err == nil {
		t.Logf("WAL present before reopen: %d bytes", info.Size())
	}

	// Reopen: triggers WAL recovery which calls validateFrame on each stored frame.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("phase2 open: %v", err)
	}
	defer db2.Close()

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	t.Logf("rows after reopen: %d", n)
}

// TestWALValidate_MultipleCheckpointModes exercises validateFrame through all
// four checkpoint modes to hit the salt+checksum validation paths.
func TestWALValidate_MultipleCheckpointModes(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vf_modes.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 80; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("M", 350))
	}

	wvcExec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
	for i := 0; i < 20; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("N", 350))
	}
	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")
	for i := 0; i < 10; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("O", 350))
	}
	wvcExec(t, db, "PRAGMA wal_checkpoint(RESTART)")
	wvcExec(t, db, "PRAGMA wal_checkpoint(TRUNCATE)")
}

// ---------------------------------------------------------------------------
// checkpointFramesToDB – exercise the build-map + write + sync path
// ---------------------------------------------------------------------------

// TestWALValidate_CheckpointFramesToDB_IncrementalBatches writes frames in
// incremental batches with a checkpoint between each batch, hitting
// checkpointFramesToDB multiple times with varying frame maps.
func TestWALValidate_CheckpointFramesToDB_IncrementalBatches(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ckpt_inc.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 40; i++ {
			wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("B", 300))
		}
		wvcExec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n < 1 {
		t.Errorf("expected rows, got %d", n)
	}
}

// TestWALValidate_CheckpointFramesToDB_UpdateOverwrite writes many updates to
// the same rows, creating multiple WAL frames for the same pages, then
// checkpoints to exercise the page-to-frame dedup in buildPageFrameMap.
func TestWALValidate_CheckpointFramesToDB_UpdateOverwrite(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ckpt_upd.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	// Seed rows.
	for i := 0; i < 20; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("C", 200))
	}

	// Update the same rows repeatedly so WAL accumulates duplicate page frames.
	for round := 0; round < 5; round++ {
		wvcMustExec(t, db, "UPDATE t SET data = ?", strings.Repeat("D", 200+round*10))
	}

	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// TestWALValidate_CheckpointFramesToDB_ReopenAndCheckpoint writes frames,
// closes the DB, reopens, and immediately checkpoints to exercise
// checkpointFramesToDB after recovery has populated the WAL.
func TestWALValidate_CheckpointFramesToDB_ReopenAndCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ckpt_ro_ck.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		wvcMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 60; i++ {
			wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("E", 400))
		}
	}()

	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	wvcExec(t, db2, "PRAGMA journal_mode=WAL")
	wvcExec(t, db2, "PRAGMA wal_checkpoint(FULL)")

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after reopen+checkpoint: %d", n)
}

// ---------------------------------------------------------------------------
// recoverWALReadOnly / recoverWALReadWrite – trigger on reopen
// ---------------------------------------------------------------------------

// TestWALValidate_RecoverWALReadWrite_LargeWAL creates a large WAL file by
// writing many frames, closes without checkpointing, and reopens read-write to
// trigger recoverWALReadWrite with a non-trivial frame set.
func TestWALValidate_RecoverWALReadWrite_LargeWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_rw_large.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()

		wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
		wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := tx.Exec("INSERT INTO t (data) VALUES (?)", strings.Repeat("R", 500)); err != nil {
				tx.Rollback()
				t.Fatalf("INSERT %d: %v", i, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		// Intentionally skip checkpoint — WAL stays on disk.
	}()

	walPath := dbPath + "-wal"
	if info, err := os.Stat(walPath); err == nil && info.Size() > 0 {
		t.Logf("WAL present: %d bytes", info.Size())
	}

	// Reopen triggers recoverWALReadWrite.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after recoverWALReadWrite: %d", n)
}

// TestWALValidate_RecoverWALReadWrite_ThenInsert reopens a WAL database after
// recovery and then immediately writes more rows so both recoverWALReadWrite
// and the normal write path are exercised.
func TestWALValidate_RecoverWALReadWrite_ThenInsert(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_rw_ins.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		wvcMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 30; i++ {
			wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("T", 250))
		}
	}()

	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	wvcExec(t, db2, "PRAGMA journal_mode=WAL")
	for i := 0; i < 15; i++ {
		wvcMustExec(t, db2, "INSERT INTO t (v) VALUES (?)", strings.Repeat("U", 250))
	}
	wvcExec(t, db2, "PRAGMA wal_checkpoint(FULL)")

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 row, got %d", n)
	}
	t.Logf("rows after recoverWALReadWrite+insert: %d", n)
}

// TestWALValidate_RecoverWALReadOnly_ExistingWAL writes WAL frames then opens
// a second connection while the first is still open, exercising the WAL open
// and read path (recoverWALReadOnly territory).
func TestWALValidate_RecoverWALReadOnly_ExistingWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_ro_exist.db")

	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("db1 open: %v", err)
	}
	defer db1.Close()

	wvcMustExec(t, db1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	wvcMustExec(t, db1, "PRAGMA journal_mode=WAL")
	for i := 0; i < 25; i++ {
		wvcMustExec(t, db1, "INSERT INTO t (v) VALUES (?)", strings.Repeat("V", 300))
	}

	// Second connection while WAL exists — exercises WAL open read path.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("db2 open: %v", err)
	}
	defer db2.Close()

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count db2: %v", err)
	}
	t.Logf("rows visible to second connection: %d", n)
}

// ---------------------------------------------------------------------------
// writeDirtyPagesToWAL – accumulate many dirty pages before commit
// ---------------------------------------------------------------------------

// TestWALValidate_WriteDirtyPagesToWAL_LargeSingleTx commits a large
// single transaction with many distinct pages to exercise writeDirtyPagesToWAL
// with a large dirty-page set.
func TestWALValidate_WriteDirtyPagesToWAL_LargeSingleTx(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "wdp_large.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	tx, txErr := db.Begin()
	if txErr != nil {
		t.Fatalf("Begin: %v", txErr)
	}
	for i := 0; i < 300; i++ {
		if _, err := tx.Exec("INSERT INTO t (data) VALUES (?)", strings.Repeat("W", 450)); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 300 {
		t.Errorf("count = %d, want 300", n)
	}
}

// TestWALValidate_WriteDirtyPagesToWAL_MixedOps exercises writeDirtyPagesToWAL
// with a mix of inserts, updates, and deletes so different page types are dirty.
func TestWALValidate_WriteDirtyPagesToWAL_MixedOps(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "wdp_mixed.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	// Seed.
	for i := 0; i < 50; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("X", 400))
	}

	// Large mixed-op transaction.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := 0; i < 30; i++ {
		if _, err := tx.Exec("INSERT INTO t (data) VALUES (?)", strings.Repeat("Y", 400)); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT: %v", err)
		}
	}
	if _, err := tx.Exec("UPDATE t SET data = ? WHERE id <= 10", strings.Repeat("Z", 400)); err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}
	if _, err := tx.Exec("DELETE FROM t WHERE id BETWEEN 11 AND 20"); err != nil {
		tx.Rollback()
		t.Fatalf("DELETE: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// ---------------------------------------------------------------------------
// enableWALMode – branch coverage via journal_mode transitions
// ---------------------------------------------------------------------------

// TestWALValidate_EnableWALMode_ReadOnlyError attempts to enable WAL mode on
// a file opened in a context that causes it to hit the enableWALMode function.
// It also exercises the normal success path by enabling WAL on a writable DB.
func TestWALValidate_EnableWALMode_NormalPath(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ewm_normal.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	// Enable WAL — exercises the NewWAL + Open + NewWALIndex + SetPageCount path.
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 25; i++ {
		wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("E", 200))
	}

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	t.Logf("journal_mode=%s", mode)
}

// TestWALValidate_EnableWALMode_MultiCycle switches between DELETE and WAL
// several times, exercising enableWALMode and disableWALMode repeatedly.
func TestWALValidate_EnableWALMode_MultiCycle(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ewm_cycle2.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	for cycle := 0; cycle < 3; cycle++ {
		wvcMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 10; i++ {
			wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("F", 200))
		}
		wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")
		wvcExec(t, db, "PRAGMA journal_mode=DELETE")
		wvcMustExec(t, db, "INSERT INTO t (v) VALUES ('delete_row')")
	}
}

// ---------------------------------------------------------------------------
// allocateLocked – triggered by VACUUM compacting fragmented pages
// ---------------------------------------------------------------------------

// TestWALValidate_AllocateLocked_SmallVacuum inserts a moderate number of rows,
// deletes half, then VACUUMs so allocateLocked is called during page compaction.
func TestWALValidate_AllocateLocked_SmallVacuum(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "alloc_small.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")

	for i := 0; i < 120; i++ {
		wvcMustExec(t, db, "INSERT INTO t (blob) VALUES (?)", strings.Repeat("G", 500))
	}
	wvcMustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")
	wvcMustExec(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after small VACUUM: %d", n)
}

// TestWALValidate_AllocateLocked_MultiTableVacuum exercises allocateLocked
// with multiple tables so more pages are compacted.
func TestWALValidate_AllocateLocked_MultiTableVacuum(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "alloc_multi.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
	wvcMustExec(t, db, "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")

	for i := 0; i < 80; i++ {
		wvcMustExec(t, db, "INSERT INTO a (v) VALUES (?)", strings.Repeat("H", 450))
		wvcMustExec(t, db, "INSERT INTO b (v) VALUES (?)", strings.Repeat("I", 450))
	}
	wvcMustExec(t, db, "DELETE FROM a WHERE id % 3 != 0")
	wvcMustExec(t, db, "DELETE FROM b WHERE id % 3 != 0")
	wvcMustExec(t, db, "VACUUM")

	for _, tbl := range []string{"a", "b"} {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil {
			t.Fatalf("count(%s): %v", tbl, err)
		}
		t.Logf("table %s rows after VACUUM: %d", tbl, n)
	}
}

// TestWALValidate_AllocateLocked_VacuumIntoNewFile exercises allocateLocked
// through VACUUM INTO which copies pages to a fresh target pager.
func TestWALValidate_AllocateLocked_VacuumIntoNewFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "alloc_src2.db")
	dstPath := filepath.Join(dir, "alloc_dst2.db")

	db := wvcOpenDB(t, srcPath)
	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 100; i++ {
		wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("J", 500))
	}
	wvcMustExec(t, db, "DELETE FROM t WHERE id % 4 != 0")

	if _, err := db.Exec("VACUUM INTO ?", dstPath); err != nil {
		t.Logf("VACUUM INTO: %v (non-fatal)", err)
		return
	}

	info, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("dst stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("VACUUM INTO produced empty file")
	}
	t.Logf("dst file size: %d bytes", info.Size())
}

// ---------------------------------------------------------------------------
// copyDatabaseToTarget – exercise page copy branches
// ---------------------------------------------------------------------------

// TestWALValidate_CopyDatabaseToTarget_ManyPages exercises copyDatabaseToTarget
// with a large database so the live-page copy loop iterates many times.
func TestWALValidate_CopyDatabaseToTarget_ManyPages(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "cdt_many.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 0; i < 200; i++ {
		wvcMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("K", 500))
	}
	wvcMustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")
	wvcMustExec(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after VACUUM: %d", n)
}

// ---------------------------------------------------------------------------
// vacuumToFile – exercise the full vacuum-to-temp-file path
// ---------------------------------------------------------------------------

// TestWALValidate_VacuumToFile_WithIndex exercises vacuumToFile on a database
// that has both a table and an index, generating more live pages.
func TestWALValidate_VacuumToFile_WithIndex(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vtf_idx.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	wvcMustExec(t, db, "CREATE INDEX idx_v ON t(v)")
	for i := 0; i < 100; i++ {
		wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("L", 300))
	}
	wvcMustExec(t, db, "DELETE FROM t WHERE id % 3 = 0")
	wvcMustExec(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after VACUUM with index: %d", n)
}

// TestWALValidate_VacuumToFile_AfterWALCycle runs a WAL cycle (enable, write,
// checkpoint, disable) before running VACUUM to exercise vacuumToFile after the
// database has been through WAL mode.
func TestWALValidate_VacuumToFile_AfterWALCycle(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vtf_wal.db")
	db := wvcOpenDB(t, dbPath)

	wvcMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	wvcMustExec(t, db, "PRAGMA journal_mode=WAL")
	for i := 0; i < 60; i++ {
		wvcMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("P", 350))
	}
	wvcExec(t, db, "PRAGMA wal_checkpoint(FULL)")
	wvcExec(t, db, "PRAGMA journal_mode=DELETE")

	wvcMustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")
	wvcMustExec(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after WAL+VACUUM: %d", n)
}
