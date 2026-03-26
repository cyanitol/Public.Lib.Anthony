// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// ============================================================================
// enableWALMode — cover the readOnly branch (line 313-315).
//
// Calling SetJournalMode(WAL) on a read-only pager routes through
// enableWALMode, which returns an error immediately when p.readOnly is true.
// This covers the uncovered "cannot enable WAL mode on read-only database"
// return path.
// ============================================================================

func TestWALMode2Coverage_EnableWAL_ReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "readonly.db")

	// Create the database first with a writable pager.
	rw := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, rw)
	page := mustGetPage(t, rw, 1)
	page.Data[DatabaseHeaderSize] = 0x01
	mustWritePage(t, rw, page)
	rw.Put(page)
	mustCommit(t, rw)
	rw.Close()

	// Open as read-only.
	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	if err := ro.SetJournalMode(JournalModeWAL); err == nil {
		t.Error("SetJournalMode(WAL) on read-only pager: expected error, got nil")
	}
}

// ============================================================================
// disableWALMode — cover the p.wal == nil early-return branch (line 347-349).
//
// disableWALMode returns nil immediately when p.wal is nil.  Calling it
// directly on a pager that was never put into WAL mode ensures p.wal is nil
// and exercises that branch.
// ============================================================================

func TestWALMode2Coverage_DisableWAL_NilWAL(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	// p is in default (delete) journal mode, so p.wal is nil.
	p.mu.Lock()
	err := p.disableWALMode()
	p.mu.Unlock()

	if err != nil {
		t.Errorf("disableWALMode with nil wal: unexpected error: %v", err)
	}
}

// ============================================================================
// enableWALMode then disableWALMode — full round-trip.
//
// Switching from delete → WAL → delete exercises both enableWALMode
// (success path) and disableWALMode (wal != nil path with Checkpoint+Delete).
// ============================================================================

func TestWALMode2Coverage_EnableThenDisable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_roundtrip.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Enable WAL mode.
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Write a frame so WAL has content to checkpoint.
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0xAB
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Disable WAL mode: exercises disableWALMode success path.
	if err := p.SetJournalMode(JournalModeDelete); err != nil {
		t.Fatalf("SetJournalMode(Delete) after WAL: %v", err)
	}
}

// ============================================================================
// enableWALMode — second enable on already-WAL pager is a no-op.
//
// Calling SetJournalMode(WAL) when already in WAL mode does not call
// enableWALMode again (handleJournalModeTransition skips it).  This test
// verifies the no-op path is safe and that the pager remains functional.
// ============================================================================

func TestWALMode2Coverage_EnableWAL_AlreadyWAL(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	mustSetJournalMode(t, p, JournalModeWAL)

	// Setting WAL again should be a no-op (no error).
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Errorf("SetJournalMode(WAL) when already WAL: unexpected error: %v", err)
	}
}
