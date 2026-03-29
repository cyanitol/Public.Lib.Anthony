// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows

package pager

// MC/DC test coverage for the pager package, batch 12.
// This file targets remaining uncovered branches in:
//   wal_checkpoint.go: checkpointRestart (reset WAL to beginning after checkpointing),
//                      restartWAL (truncates and rewrites WAL header),
//                      checkpointTruncate (removes WAL file completely).
//   journal.go:        updatePageCount (update page count in journal header).
//   vacuum.go:         createVacuumTempFile (different Vacuum paths).
//   pager.go:          initOrReadHeader (readExistingDatabase path when file already exists).

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// wal_checkpoint.go — checkpointTruncate
//
// Exercises the TRUNCATE checkpoint path through CheckpointMode.
// This is a second, independent test complementing the one in mcdc11.
// ---------------------------------------------------------------------------

func TestMCDC12_Checkpoint_Truncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_trunc.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Write data so the WAL has frames to checkpoint.
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0xAB
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// TRUNCATE checkpoint: copies frames to the DB then removes the WAL.
	if err := p.CheckpointMode(CheckpointTruncate); err != nil {
		t.Fatalf("CheckpointMode(CheckpointTruncate): %v", err)
	}

	// Verify the database is still readable after the checkpoint.
	mustBeginRead(t, p)
	rPage := mustGetPage(t, p, pgno)
	if rPage.Data[0] != 0xAB {
		t.Errorf("data after TRUNCATE checkpoint = 0x%X, want 0xAB", rPage.Data[0])
	}
	p.Put(rPage)
	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// wal_checkpoint.go — checkpointRestart / restartWAL
//
// Exercises the RESTART checkpoint path which, after writing all frames to
// the database, resets the WAL to the beginning via restartWAL.
// ---------------------------------------------------------------------------

func TestMCDC12_Checkpoint_Restart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_restart.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Write two pages to give the WAL multiple frames to checkpoint.
	mustBeginWrite(t, p)
	pgno1 := mustAllocatePage(t, p)
	page1 := mustGetPage(t, p, pgno1)
	page1.Data[0] = 0x11
	mustWritePage(t, p, page1)
	p.Put(page1)
	pgno2 := mustAllocatePage(t, p)
	page2 := mustGetPage(t, p, pgno2)
	page2.Data[0] = 0x22
	mustWritePage(t, p, page2)
	p.Put(page2)
	mustCommit(t, p)

	// RESTART checkpoint: copies WAL frames to the DB and resets the WAL.
	if err := p.CheckpointMode(CheckpointRestart); err != nil {
		t.Fatalf("CheckpointMode(CheckpointRestart): %v", err)
	}

	// After RESTART the WAL frame count should be reset to 0.
	if p.wal != nil && p.wal.frameCount != 0 {
		t.Errorf("expected WAL frameCount == 0 after RESTART checkpoint, got %d", p.wal.frameCount)
	}

	// The database content must be consistent.
	mustBeginRead(t, p)
	rp1 := mustGetPage(t, p, pgno1)
	if rp1.Data[0] != 0x11 {
		t.Errorf("page1 data after RESTART checkpoint = 0x%X, want 0x11", rp1.Data[0])
	}
	p.Put(rp1)
	rp2 := mustGetPage(t, p, pgno2)
	if rp2.Data[0] != 0x22 {
		t.Errorf("page2 data after RESTART checkpoint = 0x%X, want 0x22", rp2.Data[0])
	}
	p.Put(rp2)
	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// journal.go — updatePageCount
//
// updatePageCount writes the current page count into the journal header.
// It is called indirectly by WriteOriginal.  We open a journal directly
// and write multiple pages to cover the branch where the count is updated.
// ---------------------------------------------------------------------------

func TestMCDC12_Journal_UpdatePageCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "upd_pc.journal")

	j := NewJournal(jPath, 4096, 10)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	defer func() { _ = j.Close() }()

	page := make([]byte, 4096)

	// First WriteOriginal — pageCount becomes 1, updatePageCount is called.
	page[0] = 0x01
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal(1): %v", err)
	}
	if j.GetPageCount() != 1 {
		t.Errorf("after first write: pageCount = %d, want 1", j.GetPageCount())
	}

	// Second WriteOriginal — pageCount becomes 2, updatePageCount is called again.
	page[0] = 0x02
	if err := j.WriteOriginal(2, page); err != nil {
		t.Fatalf("WriteOriginal(2): %v", err)
	}
	if j.GetPageCount() != 2 {
		t.Errorf("after second write: pageCount = %d, want 2", j.GetPageCount())
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — createVacuumTempFile / Vacuum full path
//
// Exercises the Vacuum code path including createVacuumTempFile, vacuumToFile,
// copyDatabaseToTarget, and reloadDatabaseAfterVacuum on a simple file-based
// pager.
// ---------------------------------------------------------------------------

func TestMCDC12_Vacuum_BasicFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vac_basic.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Write a few pages so the vacuum has real data to compact.
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0xF1
	page.Data[1] = 0xF2
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Vacuum the file — exercises createVacuumTempFile, vacuumToFile, etc.
	if err := p.Vacuum(nil); err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}

	// The database must be usable after vacuum.
	mustBeginRead(t, p)
	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// pager.go — initOrReadHeader: readExistingDatabase path
//
// When a pager is opened on a non-empty file it calls readExistingDatabase
// (not initNewDatabase).  Create a pager, write data, close it, then reopen
// it so the "existing file" branch is exercised.
// ---------------------------------------------------------------------------

func TestMCDC12_InitOrReadHeader_ExistingDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "existing.db")

	// First open: creates and initialises the database.
	p1 := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, p1)
	pgno := mustAllocatePage(t, p1)
	page := mustGetPage(t, p1, pgno)
	page.Data[0] = 0xDE
	mustWritePage(t, p1, page)
	p1.Put(page)
	mustCommit(t, p1)
	p1.Close()

	// Second open: file is non-empty — readExistingDatabase is called.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()

	if p2.dbSize == 0 {
		t.Error("expected dbSize > 0 after reopening an existing database")
	}

	// Verify the previously written page is intact.
	mustBeginRead(t, p2)
	rPage := mustGetPage(t, p2, pgno)
	if rPage.Data[0] != 0xDE {
		t.Errorf("data after reopen = 0x%X, want 0xDE", rPage.Data[0])
	}
	p2.Put(rPage)
	mustEndRead(t, p2)
}
