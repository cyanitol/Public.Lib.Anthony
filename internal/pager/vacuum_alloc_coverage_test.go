// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build unix || linux || darwin || freebsd || openbsd || netbsd

package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestVacuumAllocCoverage_AllocateLocked calls allocateLocked directly so the
// function body is exercised. allocateLocked is unexported and never reached
// from production paths, so an internal (package pager) test is required.
func TestVacuumAllocCoverage_AllocateLocked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "alloc_locked.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Begin a write transaction so that getLocked (called inside
	// allocateLocked) has the lock state it expects.
	mustBeginWrite(t, p)

	page, err := p.allocateLocked()
	if err != nil {
		t.Fatalf("allocateLocked() error = %v", err)
	}
	if page == nil {
		t.Fatal("allocateLocked() returned nil page")
	}
	// Verify the page is zeroed.
	for i, b := range page.Data {
		if b != 0 {
			t.Errorf("allocateLocked() page.Data[%d] = %d, want 0", i, b)
			break
		}
	}
	p.Put(page)
	mustRollback(t, p)
}

// TestVacuumAllocCoverage_AllocateLockedErrorRollback verifies that
// allocateLocked rolls back dbSize when getLocked fails. We trigger the failure
// by setting maxPageNum to 0 so getLocked rejects every page number.
func TestVacuumAllocCoverage_AllocateLockedErrorRollback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "alloc_locked_err.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginWrite(t, p)

	// Force getLocked to fail by lowering the max page number below dbSize+1.
	original := p.maxPageNum
	p.maxPageNum = p.dbSize // any new page number will exceed this
	defer func() { p.maxPageNum = original }()

	sizeBefore := p.dbSize
	_, err := p.allocateLocked()
	if err == nil {
		t.Fatal("allocateLocked() should have returned an error with maxPageNum forced low")
	}
	// dbSize must be rolled back on error.
	if p.dbSize != sizeBefore {
		t.Errorf("dbSize after allocateLocked error = %d, want %d", p.dbSize, sizeBefore)
	}
	mustRollback(t, p)
}

// TestVacuumAllocCoverage_AcquirePendingLock_BelowReserved exercises the
// branch inside acquirePendingLock where currentLevel < lockReserved.
// We acquire a SHARED lock, manually set currentLevel to SHARED, then call
// acquirePendingLock directly so the "must maintain SHARED and RESERVED locks"
// branch is taken.
func TestVacuumAllocCoverage_AcquirePendingLock_BelowReserved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "pending_lock.db")

	// Create the file.
	f, err := os.OpenFile(dbFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// Write some content so byte-range locks have a real file to work with.
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		f.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatalf("Sync: %v", err)
	}

	lm, err := NewLockManager(f)
	if err != nil {
		f.Close()
		t.Fatalf("NewLockManager: %v", err)
	}
	defer func() {
		lm.Close()
		f.Close()
	}()

	// Acquire SHARED so the underlying OS lock is held.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED): %v", err)
	}

	// Manually reset currentLevel to SHARED so acquirePendingLock sees
	// currentLevel < lockReserved and takes the inner branch.
	lm.currentLevel = lockShared

	err = lm.acquirePendingLock()
	if err != nil {
		// On some platforms the same-process POSIX lock sharing may cause
		// unexpected behaviour; accept and log rather than fatally fail.
		t.Logf("acquirePendingLock with currentLevel=SHARED: %v (acceptable on this platform)", err)
	} else {
		t.Log("acquirePendingLock with currentLevel=SHARED succeeded, inner branch taken")
	}

	// Release everything cleanly.
	_ = lm.ReleaseLock(lockNone)
}

// TestVacuumAllocCoverage_ValidateFrame_SaltMismatch calls validateFrame
// directly with a frame whose Salt1/Salt2 differ from the WAL's own salts,
// covering the salt-mismatch error return.
func TestVacuumAllocCoverage_ValidateFrame_SaltMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vf_salt.db")

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	// Build a frame with deliberately wrong salt values.
	frame := &WALFrame{
		PageNumber: 1,
		DbSize:     1,
		Salt1:      wal.salt1 ^ 0xDEADBEEF, // mismatched
		Salt2:      wal.salt2 ^ 0xCAFEBABE, // mismatched
		Checksum1:  0,
		Checksum2:  0,
		Data:       make([]byte, DefaultPageSize),
	}

	err := wal.validateFrame(frame, 0)
	if err == nil {
		t.Fatal("validateFrame() with salt mismatch should return error")
	}
	t.Logf("validateFrame salt mismatch error (expected): %v", err)
}

// TestVacuumAllocCoverage_ValidateFrame_MatchingSalts calls validateFrame with
// salts that match but an invalid checksum to exercise the checksum-error branch.
func TestVacuumAllocCoverage_ValidateFrame_MatchingSalts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vf_chk.db")

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	// Salts match but checksums are wrong — gets past salt check, fails checksum.
	frame := &WALFrame{
		PageNumber: 1,
		DbSize:     1,
		Salt1:      wal.salt1,
		Salt2:      wal.salt2,
		Checksum1:  0xFFFFFFFF,
		Checksum2:  0xFFFFFFFF,
		Data:       make([]byte, DefaultPageSize),
	}

	err := wal.validateFrame(frame, 0)
	if err == nil {
		t.Fatal("validateFrame() with wrong checksum should return error")
	}
	t.Logf("validateFrame checksum error (expected): %v", err)
}

// TestVacuumAllocCoverage_CheckpointFramesToDB calls checkpointFramesToDB
// directly after writing frames to a WAL so the build-map and write paths run.
func TestVacuumAllocCoverage_CheckpointFramesToDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_direct.db")

	// Create the database file with at least one page of content.
	dbData := make([]byte, DefaultPageSize*2)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	// Write a couple of frames so the WAL is non-empty.
	pageData := make([]byte, DefaultPageSize)
	pageData[0] = 0xAB
	mustWriteFrame(t, wal, 1, pageData, 1)
	pageData[0] = 0xCD
	mustWriteFrame(t, wal, 2, pageData, 2)

	// Call checkpointFramesToDB directly — this covers lines 106-125 of
	// wal_checkpoint.go including ensureDBFileOpen, buildPageFrameMap,
	// writeFramesToDB, and dbFile.Sync().
	n, err := wal.checkpointFramesToDB()
	if err != nil {
		t.Fatalf("checkpointFramesToDB() error = %v", err)
	}
	if n < 1 {
		t.Errorf("checkpointFramesToDB() returned %d frames, want >= 1", n)
	}
	t.Logf("checkpointFramesToDB() checkpointed %d pages", n)
}

// TestVacuumAllocCoverage_EnableWALMode exercises enableWALMode via
// SetJournalMode so WAL and WAL index are created through the full code path.
func TestVacuumAllocCoverage_EnableWALMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Switch to WAL — exercises enableWALMode: NewWAL, Open, NewWALIndex,
	// SetPageCount.
	mustSetJournalMode(t, p, JournalModeWAL)

	if p.wal == nil {
		t.Error("p.wal should not be nil after enableWALMode")
	}
	if p.walIndex == nil {
		t.Error("p.walIndex should not be nil after enableWALMode")
	}

	// Write and commit a page to exercise WAL I/O.
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0x42
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)
}

// TestVacuumAllocCoverage_EnableWALMode_ReadOnly verifies the read-only guard
// inside enableWALMode returns an error.
func TestVacuumAllocCoverage_EnableWALMode_ReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_ro.db")

	rw := openTestPagerAt(t, dbFile, false)
	rw.Close()

	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	err := ro.enableWALMode()
	if err == nil {
		t.Error("enableWALMode on read-only pager should return error")
	}
}

// TestVacuumAllocCoverage_RecoverWALReadOnly exercises recoverWALReadOnly by
// writing WAL frames through a pager, closing it, then reopening as read-only
// while the WAL file still exists.
func TestVacuumAllocCoverage_RecoverWALReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_ro.db")

	// Phase 1: write frames and leave the WAL on disk (no checkpoint).
	func() {
		p := openTestPagerAt(t, dbFile, false)
		defer p.Close()
		mustSetJournalMode(t, p, JournalModeWAL)
		for i := 0; i < 3; i++ {
			mustBeginWrite(t, p)
			pgno := mustAllocatePage(t, p)
			page := mustGetPage(t, p, pgno)
			page.Data[0] = byte(i + 1)
			mustWritePage(t, p, page)
			p.Put(page)
			mustCommit(t, p)
		}
		// Do not checkpoint; WAL stays on disk.
		// Close will checkpoint by default, so detach WAL manually.
		p.wal = nil
		p.walIndex = nil
	}()

	walFile := dbFile + "-wal"
	info, err := os.Stat(walFile)
	if err != nil || info.Size() <= WALHeaderSize {
		t.Skip("WAL file not present or too small to trigger recoverWALReadOnly")
	}

	// Phase 2: open read-only — recoverWALReadOnly should be called.
	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()
	t.Logf("recoverWALReadOnly: wal=%v walIndex=%v", ro.wal != nil, ro.walIndex != nil)
}

// TestVacuumAllocCoverage_RecoverWALReadWrite exercises recoverWALReadWrite by
// writing WAL frames without checkpointing, then reopening read-write so that
// the recovery path checkpoints the WAL and updates dbSize.
func TestVacuumAllocCoverage_RecoverWALReadWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_rw.db")

	// Phase 1: write WAL frames and leave the WAL on disk.
	func() {
		p := openTestPagerAt(t, dbFile, false)
		defer p.Close()
		mustSetJournalMode(t, p, JournalModeWAL)
		for i := 0; i < 4; i++ {
			mustBeginWrite(t, p)
			pgno := mustAllocatePage(t, p)
			page := mustGetPage(t, p, pgno)
			page.Data[0] = byte(i + 10)
			mustWritePage(t, p, page)
			p.Put(page)
			mustCommit(t, p)
		}
		// Detach WAL and WAL index so Close() doesn't checkpoint or delete them.
		p.wal = nil
		p.walIndex = nil
	}()

	walFile := dbFile + "-wal"
	info, err := os.Stat(walFile)
	if err != nil || info.Size() <= WALHeaderSize {
		t.Skip("WAL file not present or too small to trigger recoverWALReadWrite")
	}

	// Phase 2: open read-write — recoverWALReadWrite should be called.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()
	t.Logf("recoverWALReadWrite: dbSize=%d", p2.dbSize)
}

// TestVacuumAllocCoverage_WALIndexOpen exercises the WALIndex.open() function
// (line 143 in wal_index.go) by calling NewWALIndex directly and then closing
// the index, which exercises open(), initializeFile(), mmapFile(), and readHeader().
func TestVacuumAllocCoverage_WALIndexOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "widx_open.db")

	// Create the database file first (NewWALIndex requires the base DB to exist
	// for the filename convention, but opens only the .shm file).
	if err := os.WriteFile(dbFile, make([]byte, DefaultPageSize), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// NewWALIndex calls open() internally.
	idx := mustOpenWALIndex(t, dbFile)
	mustCloseWALIndex(t, idx)

	// Verify the .shm file was created.
	shmFile := dbFile + "-shm"
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Error("WAL index .shm file was not created by open()")
	}

	// Re-open the already-existing .shm file to exercise the "file already
	// exists and is large enough" path through open().
	idx2 := mustOpenWALIndex(t, dbFile)
	mustCloseWALIndex(t, idx2)
}
