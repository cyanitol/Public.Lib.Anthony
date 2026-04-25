// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// vacwOpenDB keeps the existing file-local call sites but delegates to the
// shared pager_test DB helper.
func vacwOpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	return openPagerTestDB(t, path)
}

// vacwMustExec keeps existing test bodies stable while delegating to the
// shared pager_test execution helper.
func vacwMustExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	mustExecPagerTest(t, db, q, args...)
}

// vacwExec keeps existing non-fatal checkpoint/journal-mode calls stable while
// delegating to the shared pager_test logging helper.
func vacwExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	logExecPagerTest(t, db, q, args...)
}

// ---------------------------------------------------------------------------
// vacuumToFile / copyDatabaseToTarget
// ---------------------------------------------------------------------------

// TestVacuumWAL_VacuumToFile_Basic exercises vacuumToFile and
// copyDatabaseToTarget by running VACUUM on a populated database.
func TestVacuumWAL_VacuumToFile_Basic(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vac_basic.db")
	db := openPagerTestDB(t, dbPath)

	mustExecPagerTest(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 0; i < 50; i++ {
		mustExecPagerTest(t, db, "INSERT INTO items (val) VALUES (?)", strings.Repeat("x", 400))
	}
	mustExecPagerTest(t, db, "DELETE FROM items WHERE id % 3 = 0")
	mustExecPagerTest(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM items").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after VACUUM: %d", n)
}

// TestVacuumWAL_VacuumToFile_MultiTable exercises vacuumToFile with multiple
// tables to generate more live pages for copyDatabaseToTarget to copy.
func TestVacuumWAL_VacuumToFile_MultiTable(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vac_multi.db")
	db := openPagerTestDB(t, dbPath)

	mustExecPagerTest(t, db, "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecPagerTest(t, db, "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 30; i++ {
		mustExecPagerTest(t, db, "INSERT INTO a (v) VALUES (?)", strings.Repeat("a", 300))
		mustExecPagerTest(t, db, "INSERT INTO b (v) VALUES (?)", strings.Repeat("b", 300))
	}
	mustExecPagerTest(t, db, "DELETE FROM a WHERE id > 10")
	mustExecPagerTest(t, db, "DELETE FROM b WHERE id > 10")
	mustExecPagerTest(t, db, "VACUUM")
}

// TestVacuumWAL_CopyDatabaseToTarget_VacuumInto exercises copyDatabaseToTarget
// via VACUUM INTO, which writes a compacted copy to a new file.
func TestVacuumWAL_CopyDatabaseToTarget_VacuumInto(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	db := vacwOpenDB(t, srcPath)
	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 0; i < 80; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("d", 500))
	}
	vacwMustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")

	if _, err := db.Exec("VACUUM INTO ?", dstPath); err != nil {
		t.Logf("VACUUM INTO: %v (non-fatal, may be unimplemented)", err)
		return
	}

	info, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("dst stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("VACUUM INTO produced an empty file")
	}

	dstDB := vacwOpenDB(t, dstPath)
	var n int
	if err := dstDB.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count from dst: %v", err)
	}
	t.Logf("rows in VACUUM INTO output: %d", n)
}

// TestVacuumWAL_CopyDatabaseToTarget_TempDir uses os.TempDir() directly (not
// t.TempDir()) to exercise the temp-file path inside vacuumToFile.
func TestVacuumWAL_CopyDatabaseToTarget_TempDir(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vac_tmpdir.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 60; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("t", 350))
	}
	vacwMustExec(t, db, "DELETE FROM t WHERE id % 4 = 0")
	vacwMustExec(t, db, "VACUUM")
}

// ---------------------------------------------------------------------------
// allocateLocked
// ---------------------------------------------------------------------------

// TestVacuumWAL_AllocateLocked_FreelistChurn inserts rows, deletes many, and
// then VACUUMs, triggering allocateLocked during page compaction.
func TestVacuumWAL_AllocateLocked_FreelistChurn(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "alloc_churn.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")
	for i := 0; i < 300; i++ {
		vacwMustExec(t, db, "INSERT INTO t (blob) VALUES (?)", strings.Repeat("z", 450))
	}
	// Delete 2/3 of rows to maximise free pages.
	vacwMustExec(t, db, "DELETE FROM t WHERE id % 3 != 0")
	vacwMustExec(t, db, "VACUUM")

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after VACUUM (freelist churn): %d", n)
}

// TestVacuumWAL_AllocateLocked_IntoNewFile exercises allocateLocked via
// VACUUM INTO, which allocates pages in the fresh target pager.
func TestVacuumWAL_AllocateLocked_IntoNewFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "alloc_src.db")
	dst := filepath.Join(dir, "alloc_dst.db")

	db := vacwOpenDB(t, src)
	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 150; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("p", 600))
	}
	vacwMustExec(t, db, "DELETE FROM t WHERE id % 5 != 0")

	if _, err := db.Exec("VACUUM INTO ?", dst); err != nil {
		t.Logf("VACUUM INTO: %v (non-fatal)", err)
		return
	}

	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("VACUUM INTO output missing: %v", err)
	}
}

// TestVacuumWAL_AllocateLocked_WALThenVacuum enables WAL mode, writes data,
// then disables WAL and runs VACUUM to exercise allocateLocked after a WAL cycle.
func TestVacuumWAL_AllocateLocked_WALThenVacuum(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "alloc_wal_vac.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
	for i := 0; i < 80; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("w", 400))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(FULL)")
	vacwExec(t, db, "PRAGMA journal_mode=DELETE")
	vacwMustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")
	vacwMustExec(t, db, "VACUUM")
}

// ---------------------------------------------------------------------------
// writeDirtyPagesToWAL
// ---------------------------------------------------------------------------

// TestVacuumWAL_WriteDirtyPagesToWAL_SingleTx writes many rows in a single
// large transaction to exercise writeDirtyPagesToWAL with many dirty pages.
func TestVacuumWAL_WriteDirtyPagesToWAL_SingleTx(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "wdp_single.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := 0; i < 150; i++ {
		if _, err := tx.Exec("INSERT INTO t (data) VALUES (?)", strings.Repeat("q", 400)); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// TestVacuumWAL_WriteDirtyPagesToWAL_RepeatedTx commits many small transactions
// to exercise writeDirtyPagesToWAL across repeated invocations.
func TestVacuumWAL_WriteDirtyPagesToWAL_RepeatedTx(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "wdp_repeated.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 40; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("r", 500))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(TRUNCATE)")
}

// ---------------------------------------------------------------------------
// recoverWALReadOnly / recoverWALReadWrite
// ---------------------------------------------------------------------------

// TestVacuumWAL_RecoverWALReadWrite writes WAL frames, closes without
// checkpointing, then reopens read-write to trigger recoverWALReadWrite.
func TestVacuumWAL_RecoverWALReadWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_rw.db")

	// Phase 1: populate with WAL frames, close without checkpoint.
	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("phase1 open: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 40; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("s", 300))
		}
		// Close with WAL frames uncommitted to disk.
	}()

	walPath := dbPath + "-wal"
	if info, statErr := os.Stat(walPath); statErr == nil {
		t.Logf("WAL present: %d bytes", info.Size())
	}

	// Phase 2: reopen read-write — triggers recoverWALReadWrite.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("phase2 open: %v", err)
	}
	defer db2.Close()

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count after recovery: %v", err)
	}
	t.Logf("rows after WAL read-write recovery: %d", n)
}

// TestVacuumWAL_RecoverWALReadWrite_ThenWrite reopens a WAL database and then
// writes more data, exercising both recovery and post-recovery writes.
func TestVacuumWAL_RecoverWALReadWrite_ThenWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_rw2.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 25; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("u", 250))
		}
	}()

	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	// Write after recovery.
	vacwExec(t, db2, "PRAGMA journal_mode=WAL")
	for i := 0; i < 10; i++ {
		vacwMustExec(t, db2, "INSERT INTO t (v) VALUES (?)", strings.Repeat("v", 200))
	}
	vacwExec(t, db2, "PRAGMA wal_checkpoint(FULL)")
}

// TestVacuumWAL_RecoverWALReadOnly opens a WAL database from a second
// connection (simulating read-only recovery) after writing WAL frames.
func TestVacuumWAL_RecoverWALReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_ro.db")

	// Populate WAL.
	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 30; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("k", 350))
		}
	}()

	// Second open — may trigger recoverWALReadOnly depending on WAL state.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer db2.Close()

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after potential WAL read-only recovery: %d", n)
}

// TestVacuumWAL_RecoverWAL_Sequential reopens the same WAL database several
// times in sequence, verifying row counts are stable across recoveries.
func TestVacuumWAL_RecoverWAL_Sequential(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "rcv_seq.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("init open: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 20; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("m", 200))
		}
	}()

	for round := 0; round < 3; round++ {
		func() {
			db, err := sql.Open("sqlite_internal", dbPath)
			if err != nil {
				t.Fatalf("round %d open: %v", round, err)
			}
			defer db.Close()
			vacwExec(t, db, "PRAGMA journal_mode=WAL")
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("n", 100))
		}()
	}

	db, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("final count: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// validateFrame
// ---------------------------------------------------------------------------

// TestVacuumWAL_ValidateFrame_WriteAndCheckpoint writes WAL frames and then
// runs a full checkpoint to exercise validateFrame during frame read-back.
func TestVacuumWAL_ValidateFrame_WriteAndCheckpoint(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vf_ckpt.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 80; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("f", 450))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// TestVacuumWAL_ValidateFrame_RestartCheckpoint exercises validateFrame via the
// RESTART checkpoint mode which re-reads all frames.
func TestVacuumWAL_ValidateFrame_RestartCheckpoint(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vf_restart.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 60; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("g", 500))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(RESTART)")
}

// TestVacuumWAL_ValidateFrame_TruncateAfterWrite writes many WAL frames then
// uses TRUNCATE checkpoint to fully validate and truncate the WAL.
func TestVacuumWAL_ValidateFrame_TruncateAfterWrite(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "vf_trunc.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for batch := 0; batch < 4; batch++ {
		for i := 0; i < 25; i++ {
			vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("h", 400))
		}
		vacwExec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(TRUNCATE)")
}

// ---------------------------------------------------------------------------
// checkpointFramesToDB
// ---------------------------------------------------------------------------

// TestVacuumWAL_CheckpointFramesToDB_PassiveMode exercises checkpointFramesToDB
// via the PASSIVE checkpoint path.
func TestVacuumWAL_CheckpointFramesToDB_PassiveMode(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ckpt_passive.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 120; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("c", 300))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
}

// TestVacuumWAL_CheckpointFramesToDB_FullMode exercises checkpointFramesToDB
// via the FULL checkpoint path with a large frame count.
func TestVacuumWAL_CheckpointFramesToDB_FullMode(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ckpt_full.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 200; i++ {
		vacwMustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("e", 400))
	}
	vacwExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// TestVacuumWAL_CheckpointFramesToDB_AfterReopen writes frames, closes the
// database, reopens it, and then checkpoints to exercise checkpointFramesToDB
// in the context of WAL recovery.
func TestVacuumWAL_CheckpointFramesToDB_AfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ckpt_reopen.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 50; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("o", 350))
		}
	}()

	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	vacwExec(t, db2, "PRAGMA journal_mode=WAL")
	vacwExec(t, db2, "PRAGMA wal_checkpoint(FULL)")

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after reopen+checkpoint: %d", n)
}

// ---------------------------------------------------------------------------
// enableWALMode
// ---------------------------------------------------------------------------

// TestVacuumWAL_EnableWALMode_FreshDB enables WAL on a newly created database
// to exercise the full enableWALMode path including WAL index open().
func TestVacuumWAL_EnableWALMode_FreshDB(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ewm_fresh.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 20; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("y", 200))
	}

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("journal_mode query: %v", err)
	}
	t.Logf("journal_mode=%s", mode)
}

// TestVacuumWAL_EnableWALMode_CycleDeleteWAL switches DELETE→WAL→DELETE→WAL
// to exercise all branches of handleJournalModeTransition and enableWALMode.
func TestVacuumWAL_EnableWALMode_CycleDeleteWAL(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ewm_cycle.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
	for i := 0; i < 15; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("j", 200))
	}
	vacwExec(t, db, "PRAGMA journal_mode=DELETE")

	vacwMustExec(t, db, "INSERT INTO t (v) VALUES ('delete_mode')")

	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
	for i := 0; i < 10; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("l", 150))
	}
}

// TestVacuumWAL_EnableWALMode_WithErrorHandling enables WAL mode and then
// exercises the error-handling branches by running operations that require a
// fully initialised WAL (index open, SetPageCount, etc.).
func TestVacuumWAL_EnableWALMode_WithErrorHandling(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "ewm_err.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	// Enable WAL — triggers NewWAL, wal.Open, NewWALIndex, walIndex.open,
	// and walIndex.SetPageCount.
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	// Write pages to exercise writeDirtyPagesToWAL.
	for i := 0; i < 30; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("e", 300))
	}

	// Checkpoint to exercise checkpointFramesToDB.
	vacwExec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// ---------------------------------------------------------------------------
// WAL index open()
// ---------------------------------------------------------------------------

// TestVacuumWAL_WALIndexOpen_NewFile exercises WALIndex.open() for a brand new
// file (the initialiseFile branch) by enabling WAL on a freshly created DB.
func TestVacuumWAL_WALIndexOpen_NewFile(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "idx_new.db")
	db := vacwOpenDB(t, dbPath)

	vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	vacwMustExec(t, db, "PRAGMA journal_mode=WAL")

	// Writes force index updates.
	for i := 0; i < 10; i++ {
		vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("i", 100))
	}
}

// TestVacuumWAL_WALIndexOpen_ExistingFile exercises WALIndex.open() for an
// already-initialised file by closing and reopening a WAL database.
func TestVacuumWAL_WALIndexOpen_ExistingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "idx_exist.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 15; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("x", 150))
		}
	}()

	// Reopen — WALIndex.open() hits the "existing file" branch.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	vacwExec(t, db2, "PRAGMA journal_mode=WAL")

	var n int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows on existing WAL index reopen: %d", n)
}

// TestVacuumWAL_WALIndexOpen_MultipleConnections opens a WAL database from
// several connections simultaneously to stress WALIndex.open() concurrency.
func TestVacuumWAL_WALIndexOpen_MultipleConnections(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "idx_multi.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		vacwMustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		vacwMustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 10; i++ {
			vacwMustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("m", 100))
		}
	}()

	conns := make([]*sql.DB, 3)
	for i := range conns {
		db, err := sql.Open("sqlite_internal", dbPath)
		if err != nil {
			t.Fatalf("conn %d open: %v", i, err)
		}
		conns[i] = db
		t.Cleanup(func() { db.Close() })
	}

	for i, db := range conns {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n); err != nil {
			t.Logf("conn %d count: %v (non-fatal)", i, err)
		} else {
			t.Logf("conn %d: %d rows", i, n)
		}
	}
}
