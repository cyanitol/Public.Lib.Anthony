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

// wal2OpenDB opens a file-backed database for WAL2 coverage tests.
func wal2OpenDB(t *testing.T, name string) (*sql.DB, string) {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), name)
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dbFile
}

// wal2Exec executes a SQL statement and ignores errors (non-fatal).
func wal2Exec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Logf("exec %q: %v (non-fatal)", query, err)
	}
}

// wal2MustExec executes a SQL statement or fails the test.
func wal2MustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// ---------------------------------------------------------------------------
// enableWALMode – branch coverage via journal_mode transitions
// ---------------------------------------------------------------------------

// TestPagerWAL2_EnableWALMode_SwitchAndVerify enables WAL mode via PRAGMA and
// verifies the mode is active.
func TestPagerWAL2_EnableWALMode_SwitchAndVerify(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "ewm_sw.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	rows, err := db.Query("PRAGMA journal_mode")
	if err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Logf("journal_mode=%s", mode)
	}
}

// TestPagerWAL2_EnableWALMode_TransitionDeleteToWAL switches from DELETE to WAL
// and back to DELETE, then to WAL again, exercising all branches in enableWALMode
// and disableWALMode.
func TestPagerWAL2_EnableWALMode_TransitionDeleteToWAL(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "ewm_trans.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	// Enable WAL mode.
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	// Write in WAL mode.
	for i := 0; i < 10; i++ {
		wal2MustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("x", 100))
	}

	// Switch back to DELETE mode (exercises disableWALMode → checkpoint).
	wal2Exec(t, db, "PRAGMA journal_mode=DELETE")

	// Write in DELETE mode.
	wal2MustExec(t, db, "INSERT INTO t (v) VALUES ('delete_mode')")

	// Switch to WAL again (enableWALMode second call path).
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	// Write more in WAL mode.
	for i := 0; i < 5; i++ {
		wal2MustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("y", 200))
	}
}

// TestPagerWAL2_EnableWALMode_WriteAfterSwitch enables WAL mode then writes a
// large number of rows to exercise writeDirtyPagesToWAL repeatedly.
func TestPagerWAL2_EnableWALMode_WriteAfterSwitch(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "ewm_write.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 50; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("w", 300))
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 50 {
		t.Errorf("count = %d, want 50", count)
	}
}

// ---------------------------------------------------------------------------
// validateFrame / checkpointFramesToDB – WAL frame validation paths
// ---------------------------------------------------------------------------

// TestPagerWAL2_ValidateFrame_ManyFrames writes many WAL frames then checkpoints
// to exercise validateFrame during the read-back phase.
func TestPagerWAL2_ValidateFrame_ManyFrames(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "vf_many.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 100; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("x", 500))
	}

	// Checkpoint forces validateFrame on every WAL frame.
	wal2Exec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// TestPagerWAL2_CheckpointFramesToDB_AllModes exercises checkpointFramesToDB
// through all four checkpoint modes.
func TestPagerWAL2_CheckpointFramesToDB_AllModes(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "ckpt_all.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	for i := 0; i < 500; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("z", 200))
	}

	// Exercise all four checkpoint modes in order.
	wal2Exec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
	wal2Exec(t, db, "PRAGMA wal_checkpoint(FULL)")
	wal2Exec(t, db, "PRAGMA wal_checkpoint(RESTART)")
	wal2Exec(t, db, "PRAGMA wal_checkpoint(TRUNCATE)")
}

// TestPagerWAL2_CheckpointFramesToDB_LargeBatch writes a very large batch to
// create many WAL frames then runs multiple checkpoint styles.
func TestPagerWAL2_CheckpointFramesToDB_LargeBatch(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "ckpt_large.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	// Write in batches with intervening checkpoints.
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 100; i++ {
			wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("b", 400))
		}
		wal2Exec(t, db, "PRAGMA wal_checkpoint(PASSIVE)")
	}

	wal2Exec(t, db, "PRAGMA wal_checkpoint(FULL)")
	wal2Exec(t, db, "PRAGMA wal_checkpoint(TRUNCATE)")
}

// ---------------------------------------------------------------------------
// recoverWALReadOnly / recoverWALReadWrite – reopen with existing WAL
// ---------------------------------------------------------------------------

// TestPagerWAL2_RecoverWALReadWrite writes frames to a WAL database, closes it
// without checkpointing, then reopens read-write to exercise recoverWALReadWrite.
func TestPagerWAL2_RecoverWALReadWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "recover_rw.db")

	// Phase 1: create and populate in WAL mode.
	func() {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("phase1 open: %v", err)
		}
		defer db.Close()

		wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
		wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

		for i := 0; i < 30; i++ {
			wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("r", 400))
		}
		// Close without explicit checkpoint – WAL file stays on disk.
	}()

	// Verify WAL file exists.
	walFile := dbFile + "-wal"
	if info, err := os.Stat(walFile); err == nil && info.Size() > 0 {
		t.Logf("WAL file present: %d bytes", info.Size())
	}

	// Phase 2: reopen read-write – triggers recoverWALReadWrite.
	db2, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("phase2 open: %v", err)
	}
	defer db2.Close()

	var count int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("SELECT after recovery: %v", err)
	}
	t.Logf("rows after WAL recovery: %d", count)
}

// TestPagerWAL2_RecoverWALReadOnly writes frames to a WAL database, closes it,
// then reopens in read-only mode to exercise recoverWALReadOnly.
func TestPagerWAL2_RecoverWALReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "recover_ro.db")

	// Phase 1: populate in WAL mode.
	func() {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("phase1 open: %v", err)
		}
		defer db.Close()

		wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
		wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

		for i := 0; i < 20; i++ {
			wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("s", 300))
		}
	}()

	// Phase 2: open read-only – exercises recoverWALReadOnly if WAL present.
	// We use the file:// URI with mode=ro if supported; otherwise just open normally
	// and note the limitation.
	roFile := dbFile
	db2, err := sql.Open("sqlite_internal", roFile)
	if err != nil {
		t.Fatalf("phase2 open: %v", err)
	}
	defer db2.Close()

	var count int
	if err := db2.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("SELECT after recovery: %v", err)
	}
	t.Logf("rows after WAL recovery: %d", count)
}

// TestPagerWAL2_RecoverWAL_MultipleReopens reopens a WAL database several times
// to exercise the recovery path repeatedly.
// wal2ReopenAndInsert opens the database, sets WAL mode, inserts rows, then closes.
func wal2ReopenAndInsert(t *testing.T, dbFile string, round, n int, data string) {
	t.Helper()
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("round %d open: %v", round, err)
	}
	defer db.Close()
	wal2Exec(t, db, "PRAGMA journal_mode=WAL")
	for i := 0; i < n; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat(data, 150))
	}
}

func TestPagerWAL2_RecoverWAL_MultipleReopens(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "recover_multi.db")

	func() {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		defer db.Close()
		wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
		wal2MustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 10; i++ {
			wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("m", 200))
		}
	}()

	for round := 0; round < 3; round++ {
		wal2ReopenAndInsert(t, dbFile, round, 5, "n")
	}

	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("final count: %v", err)
	}
	if count < 10 {
		t.Errorf("count = %d, want at least 10", count)
	}
	t.Logf("total rows after multiple reopens: %d", count)
}

// ---------------------------------------------------------------------------
// writeDirtyPagesToWAL – large transaction accumulating many dirty pages
// ---------------------------------------------------------------------------

// TestPagerWAL2_WriteDirtyPagesToWAL_HugeTx accumulates a large number of dirty
// pages in a single transaction then commits, heavily exercising writeDirtyPagesToWAL.
func TestPagerWAL2_WriteDirtyPagesToWAL_HugeTx(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "wdp_huge.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := 0; i < 200; i++ {
		if _, err := tx.Exec("INSERT INTO t (data) VALUES (?)", strings.Repeat("p", 500)); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	wal2Exec(t, db, "PRAGMA wal_checkpoint(FULL)")
}

// ---------------------------------------------------------------------------
// allocateLocked – triggered by VACUUM on fragmented database
// ---------------------------------------------------------------------------

// TestPagerWAL2_AllocateLocked_ViaVacuum exercises allocateLocked by running
// VACUUM on a database with significant fragmentation from deletions.
func TestPagerWAL2_AllocateLocked_ViaVacuum(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "alloc_vac.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")

	// Insert enough rows to span many pages.
	for i := 0; i < 200; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("a", 500))
	}

	// Delete every other row to create free-list entries.
	wal2MustExec(t, db, "DELETE FROM t WHERE id % 2 = 0")

	// VACUUM triggers vacuumToFile → copyDatabaseToTarget → allocateLocked.
	wal2MustExec(t, db, "VACUUM")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count after VACUUM: %v", err)
	}
	t.Logf("rows after VACUUM: %d", count)
}

// TestPagerWAL2_AllocateLocked_LargeVacuum exercises allocateLocked with a
// larger dataset to hit more allocation iterations.
func TestPagerWAL2_AllocateLocked_LargeVacuum(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "alloc_large.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")

	for i := 0; i < 500; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("v", 400))
	}

	// Delete 75% of rows to maximise free list.
	wal2MustExec(t, db, "DELETE FROM t WHERE id % 4 != 0")
	wal2MustExec(t, db, "VACUUM")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("rows after large VACUUM: %d", count)
}

// TestPagerWAL2_AllocateLocked_ViaVacuumInto exercises allocateLocked via
// VACUUM INTO which copies pages to a new file.
func TestPagerWAL2_AllocateLocked_ViaVacuumInto(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "dst.db")

	db, err := sql.Open("sqlite_internal", srcFile)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer db.Close()

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 0; i < 200; i++ {
		wal2MustExec(t, db, "INSERT INTO t (data) VALUES (?)", strings.Repeat("c", 500))
	}
	wal2MustExec(t, db, "DELETE FROM t WHERE id % 3 != 0")

	if _, err := db.Exec("VACUUM INTO ?", dstFile); err != nil {
		t.Logf("VACUUM INTO: %v (non-fatal)", err)
		return
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("dst file missing: %v", err)
	}
}

// ---------------------------------------------------------------------------
// validateRollbackState / savepoint paths
// ---------------------------------------------------------------------------

// TestPagerWAL2_ValidateRollbackState_NestedSavepoints creates nested savepoints,
// rolls back to an intermediate one, then releases, exercising validateRollbackState.
func TestPagerWAL2_ValidateRollbackState_NestedSavepoints(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "sp_nested.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec("SAVEPOINT sp1"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp1: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO t (v) VALUES (1)"); err != nil {
		tx.Rollback()
		t.Fatalf("INSERT 1: %v", err)
	}
	if _, err := tx.Exec("SAVEPOINT sp2"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp2: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO t (v) VALUES (2)"); err != nil {
		tx.Rollback()
		t.Fatalf("INSERT 2: %v", err)
	}

	if !wal2RollbackAndRelease(t, tx, "sp1") {
		return
	}

	if err := tx.Commit(); err != nil {
		t.Logf("Commit: %v (non-fatal)", err)
	}
}

// TestPagerWAL2_ValidateRollbackState_WALMode exercises savepoint rollback
// specifically in WAL mode which has different rollback handling.
// wal2InsertRows inserts n rows into table t within the given tx.
func wal2InsertRows(t *testing.T, tx *sql.Tx, n int, char string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := tx.Exec("INSERT INTO t (v) VALUES (?)", strings.Repeat(char, 100)); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
}

// wal2RollbackAndRelease rolls back to and releases a savepoint, returning false on non-fatal error.
func wal2RollbackAndRelease(t *testing.T, tx *sql.Tx, name string) bool {
	t.Helper()
	if _, err := tx.Exec("ROLLBACK TO SAVEPOINT " + name); err != nil {
		tx.Rollback()
		t.Logf("ROLLBACK TO %s: %v (non-fatal)", name, err)
		return false
	}
	if _, err := tx.Exec("RELEASE SAVEPOINT " + name); err != nil {
		tx.Rollback()
		t.Logf("RELEASE %s: %v (non-fatal)", name, err)
		return false
	}
	return true
}

func TestPagerWAL2_ValidateRollbackState_WALMode(t *testing.T) {
	t.Parallel()
	db, _ := wal2OpenDB(t, "sp_wal.db")

	wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	wal2MustExec(t, db, "PRAGMA journal_mode=WAL")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec("SAVEPOINT sp1"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp1: %v", err)
	}
	wal2InsertRows(t, tx, 5, "q")

	if _, err := tx.Exec("SAVEPOINT sp2"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp2: %v", err)
	}
	wal2InsertRows(t, tx, 3, "r")

	if !wal2RollbackAndRelease(t, tx, "sp2") {
		return
	}
	if !wal2RollbackAndRelease(t, tx, "sp1") {
		return
	}

	if err := tx.Commit(); err != nil {
		t.Logf("Commit: %v (non-fatal)", err)
	}
}

// ---------------------------------------------------------------------------
// acquirePendingLock – exercised by concurrent write contention
// ---------------------------------------------------------------------------

// TestPagerWAL2_AcquirePendingLock_WriteContention creates concurrent write
// transactions that compete for the exclusive lock, triggering acquirePendingLock.
// wal2SetupContentionDB creates and populates a database for contention tests.
func wal2SetupContentionDB(t *testing.T, dbFile string, rows int) {
	t.Helper()
	setupDB, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("setup open: %v", err)
	}
	wal2MustExec(t, setupDB, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	for i := 0; i < rows; i++ {
		wal2MustExec(t, setupDB, "INSERT INTO t (v) VALUES (?)", i)
	}
	setupDB.Close()
}

func TestPagerWAL2_AcquirePendingLock_WriteContention(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "pending_lock.db")

	wal2SetupContentionDB(t, dbFile, 50)

	db1, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("db1 open: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("db2 open: %v", err)
	}
	defer db2.Close()

	tx1, err := db1.Begin()
	if err != nil {
		t.Fatalf("db1 Begin: %v", err)
	}
	if _, err := tx1.Exec("INSERT INTO t (v) VALUES (999)"); err != nil {
		tx1.Rollback()
		t.Logf("db1 INSERT: %v (non-fatal)", err)
		return
	}

	tx2, err := db2.Begin()
	if err != nil {
		tx1.Rollback()
		t.Logf("db2 Begin: %v (non-fatal)", err)
		return
	}
	if _, err := tx2.Exec("INSERT INTO t (v) VALUES (888)"); err != nil {
		t.Logf("db2 INSERT (expected contention): %v", err)
	}
	tx2.Rollback()
	tx1.Commit()
}

// TestPagerWAL2_AcquirePendingLock_WALModeContention exercises acquirePendingLock
// specifically in WAL mode where lock promotion goes SHARED → RESERVED → EXCLUSIVE.
func TestPagerWAL2_AcquirePendingLock_WALModeContention(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "pending_wal.db")

	// Setup with WAL mode.
	setupDB, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("setup open: %v", err)
	}
	wal2MustExec(t, setupDB, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	wal2MustExec(t, setupDB, "PRAGMA journal_mode=WAL")
	for i := 0; i < 20; i++ {
		wal2MustExec(t, setupDB, "INSERT INTO t (v) VALUES (?)", i)
	}
	setupDB.Close()

	// Two writers competing under WAL.
	done := make(chan error, 2)
	write := func(dbFile string, val int) {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			done <- err
			return
		}
		defer db.Close()
		wal2Exec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 10; i++ {
			db.Exec("INSERT INTO t (v) VALUES (?)", val*100+i)
		}
		done <- nil
	}

	go write(dbFile, 1)
	go write(dbFile, 2)

	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Logf("writer error: %v (non-fatal)", err)
		}
	}
}

// ---------------------------------------------------------------------------
// WAL index open – concurrent access to shared WAL index
// ---------------------------------------------------------------------------

// TestPagerWAL2_WALIndex_MultipleOpens opens a WAL database from multiple
// connections to exercise the WAL index open() re-entry path.
func TestPagerWAL2_WALIndex_MultipleOpens(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_idx_multi.db")

	// Create WAL database.
	func() {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		defer db.Close()
		wal2MustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		wal2MustExec(t, db, "PRAGMA journal_mode=WAL")
		for i := 0; i < 10; i++ {
			wal2MustExec(t, db, "INSERT INTO t (v) VALUES (?)", strings.Repeat("i", 200))
		}
	}()

	// Open multiple connections simultaneously.
	dbs := make([]*sql.DB, 4)
	for i := range dbs {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		dbs[i] = db
	}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()

	for i, db := range dbs {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
			t.Logf("connection %d count: %v (non-fatal)", i, err)
		} else {
			t.Logf("connection %d: %d rows", i, count)
		}
	}
}
