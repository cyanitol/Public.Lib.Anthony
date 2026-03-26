// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows && !js && !wasip1

package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSavepointCache2Coverage covers several low-coverage internal functions:
//   - releaseSavepoints (savepoint.go)
//   - rollbackJournal (pager.go)
//   - Put with nil page (pager.go)
//   - readFrameAtIndex (wal.go)
//   - initializeFile (wal_index.go)
//   - acquirePendingLock (lock_unix.go)
func TestSavepointCache2Coverage(t *testing.T) {
	// ---------------------------------------------------------------------------
	// releaseSavepoints — savepoint.go line 280
	// ---------------------------------------------------------------------------

	// exercises the early-return branch when index < 0.
	t.Run("ReleaseSavepointsNegativeIndex", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		mustBeginWrite(t, p)
		mustSavepoint(t, p, "a")
		mustSavepoint(t, p, "b")

		before := len(p.savepoints)
		p.releaseSavepoints(-1) // index < 0: early return, no change
		if len(p.savepoints) != before {
			t.Errorf("releaseSavepoints(-1) changed savepoints: got %d, want %d", len(p.savepoints), before)
		}
		p.Rollback()
	})

	// exercises the early-return branch when index >= len(savepoints).
	t.Run("ReleaseSavepointsOutOfRange", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		mustBeginWrite(t, p)
		mustSavepoint(t, p, "x")

		before := len(p.savepoints)
		p.releaseSavepoints(999) // index >= len: early return, no change
		if len(p.savepoints) != before {
			t.Errorf("releaseSavepoints(999) changed savepoints: got %d, want %d", len(p.savepoints), before)
		}
		p.Rollback()
	})

	// exercises the happy path: removing savepoints 0..index inclusive.
	t.Run("ReleaseSavepointsValid", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		mustBeginWrite(t, p)
		mustSavepoint(t, p, "s1")
		mustSavepoint(t, p, "s2")
		mustSavepoint(t, p, "s3")

		// Release index 1 (keeps only savepoints after index 1, i.e. one entry).
		p.releaseSavepoints(1)
		if len(p.savepoints) != 1 {
			t.Errorf("after releaseSavepoints(1): got %d savepoints, want 1", len(p.savepoints))
		}
		p.Rollback()
	})

	// ---------------------------------------------------------------------------
	// rollbackJournal — pager.go line 1121
	// ---------------------------------------------------------------------------

	// exercises the nil journalFile early-return branch.
	t.Run("RollbackJournalNilJournal", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		// journalFile is nil when no transaction has written anything.
		if err := p.rollbackJournal(); err != nil {
			t.Errorf("rollbackJournal with nil journalFile: expected nil, got %v", err)
		}
	})

	// exercises rollbackJournal through a real write-then-rollback cycle.
	t.Run("RollbackJournalWithData", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		p := openTestPagerAt(t, filepath.Join(dir, "rjwd.db"), false)
		defer p.Close()

		// Commit initial value.
		mustBeginWrite(t, p)
		pg := mustGetPage(t, p, 1)
		mustWritePage(t, p, pg)
		pg.Data[DatabaseHeaderSize] = 0xAA
		p.Put(pg)
		mustCommit(t, p)

		// Modify and rollback — exercises rollbackJournal with real journal entries.
		mustBeginWrite(t, p)
		pg2 := mustGetPage(t, p, 1)
		mustWritePage(t, p, pg2)
		pg2.Data[DatabaseHeaderSize] = 0xBB
		p.Put(pg2)
		if err := p.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		// Verify page was restored.
		mustBeginRead(t, p)
		pg3 := mustGetPage(t, p, 1)
		if pg3.Data[DatabaseHeaderSize] != 0xAA {
			t.Errorf("after rollback: got 0x%02X, want 0xAA", pg3.Data[DatabaseHeaderSize])
		}
		p.Put(pg3)
		mustEndRead(t, p)
	})

	// ---------------------------------------------------------------------------
	// Put — pager.go line 450
	// ---------------------------------------------------------------------------

	// exercises the nil guard in Pager.Put (uncovered nil branch).
	t.Run("PutNilPage", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		p.Put(nil) // must not panic
	})

	// exercises Put with a real page (Unref path).
	t.Run("PutValidPage", func(t *testing.T) {
		t.Parallel()
		p := openTestPager(t)
		mustBeginRead(t, p)
		pg := mustGetPage(t, p, 1)
		p.Put(pg)
		mustEndRead(t, p)
	})

	// ---------------------------------------------------------------------------
	// readFrameAtIndex — wal.go line 353
	// ---------------------------------------------------------------------------

	// exercises the out-of-range error branch of readFrameAtIndex.
	t.Run("ReadFrameAtIndexOutOfRange", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		dbFile := filepath.Join(dir, "rfi.db")
		if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
			t.Fatalf("create db file: %v", err)
		}

		wal := mustOpenWAL(t, dbFile, DefaultPageSize)
		defer wal.Close()

		// frameCount is 0 on a fresh WAL; any index is out of range.
		_, err := wal.readFrameAtIndex(0)
		if err == nil {
			t.Error("readFrameAtIndex(0) on empty WAL: expected error, got nil")
		}
	})

	// exercises the happy path by writing a frame first.
	t.Run("ReadFrameAtIndexValid", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		dbFile := filepath.Join(dir, "rfiv.db")
		if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
			t.Fatalf("create db file: %v", err)
		}

		wal := mustOpenWAL(t, dbFile, DefaultPageSize)
		defer wal.Close()

		data := make([]byte, DefaultPageSize)
		data[0] = 0x42
		mustWriteFrame(t, wal, 1, data, 1)

		frame, err := wal.readFrameAtIndex(0)
		if err != nil {
			t.Fatalf("readFrameAtIndex(0): %v", err)
		}
		if frame == nil {
			t.Fatal("readFrameAtIndex(0): got nil frame")
		}
		if frame.PageNumber != 1 {
			t.Errorf("frame.PageNumber = %d, want 1", frame.PageNumber)
		}
	})

	// ---------------------------------------------------------------------------
	// initializeFile — wal_index.go line 191
	// ---------------------------------------------------------------------------

	// exercises the success path of WALIndex.initializeFile directly.
	t.Run("InitializeFileSuccess", func(t *testing.T) {
		t.Parallel()
		filename := tempWALIndexFile(t)

		// Open normally (which already calls initializeFile internally).
		idx := mustOpenWALIndex(t, filename)
		defer idx.Close()

		// Call initializeFile again directly to cover its body a second time.
		minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)
		if err := idx.initializeFile(minSize); err != nil {
			t.Fatalf("initializeFile(%d): %v", minSize, err)
		}

		if idx.header == nil {
			t.Fatal("header is nil after initializeFile")
		}
		if idx.header.Version != WALIndexVersion {
			t.Errorf("header.Version = %d, want %d", idx.header.Version, WALIndexVersion)
		}
		if idx.header.IsInit != 1 {
			t.Errorf("header.IsInit = %d, want 1", idx.header.IsInit)
		}
	})

	// ---------------------------------------------------------------------------
	// acquirePendingLock — lock_unix.go line 251
	// ---------------------------------------------------------------------------

	// exercises the branch where currentLevel < lockReserved so acquirePendingLock
	// must also call acquireReservedLock (line 268-272 of lock_unix.go).
	t.Run("AcquirePendingLockFromShared", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "pending.db")
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatalf("open file: %v", err)
		}
		if _, err := f.Write(make([]byte, 4096)); err != nil {
			f.Close()
			t.Fatalf("write file: %v", err)
		}
		f.Sync()

		lm, err := NewLockManager(f)
		if err != nil {
			f.Close()
			t.Fatalf("NewLockManager: %v", err)
		}
		defer func() {
			lm.Close()
			f.Close()
		}()

		// Acquire only SHARED so currentLevel < lockReserved when we call
		// acquirePendingLock directly (the uncovered branch at line 268).
		if err := lm.AcquireLock(lockShared); err != nil {
			t.Fatalf("AcquireLock(SHARED): %v", err)
		}

		// currentLevel == SHARED (< lockReserved), so acquirePendingLock
		// enters the inner branch and calls acquireReservedLock as well.
		if err := lm.acquirePendingLock(); err != nil {
			t.Fatalf("acquirePendingLock from SHARED: %v", err)
		}

		// Release all locks cleanly.
		if err := lm.ReleaseLock(lockNone); err != nil {
			t.Fatalf("ReleaseLock(NONE): %v", err)
		}
	})
}
