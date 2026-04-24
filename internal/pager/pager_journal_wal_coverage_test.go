// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestPagerJournalWAL_SetUserVersion exercises Pager.SetUserVersion.
func TestPagerJournalWAL_SetUserVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "uv.db"), false)
	defer p.Close()

	if err := p.SetUserVersion(42); err != nil {
		t.Fatalf("SetUserVersion: %v", err)
	}
	if got := p.GetHeader().UserVersion; got != 42 {
		t.Errorf("UserVersion = %d, want 42", got)
	}
}

// TestPagerJournalWAL_SetSchemaCookie exercises Pager.SetSchemaCookie.
func TestPagerJournalWAL_SetSchemaCookie(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "sc.db"), false)
	defer p.Close()

	if err := p.SetSchemaCookie(99); err != nil {
		t.Fatalf("SetSchemaCookie: %v", err)
	}
	if got := p.GetHeader().SchemaCookie; got != 99 {
		t.Errorf("SchemaCookie = %d, want 99", got)
	}
}

// TestPagerJournalWAL_VerifyFreeList exercises Pager.VerifyFreeList.
func TestPagerJournalWAL_VerifyFreeList(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "fl.db"), false)
	defer p.Close()

	if err := p.VerifyFreeList(); err != nil {
		t.Fatalf("VerifyFreeList: %v", err)
	}
}

// TestPagerJournalWAL_CommitPhases exercises commitPhase0FlushFreeList and
// commitPhase3FinalizeJournal via a normal commit cycle.
func TestPagerJournalWAL_CommitPhases(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "phases.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)
}

// TestPagerJournalWAL_CommitPhasesPersist exercises commitPhase3FinalizeJournal
// in JournalModePersist (ZeroHeader path).
func TestPagerJournalWAL_CommitPhasesPersist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "persist.db"), false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModePersist)

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)
}

// TestPagerJournalWAL_CommitPhasesTruncate exercises commitPhase3FinalizeJournal
// in JournalModeTruncate.
func TestPagerJournalWAL_CommitPhasesTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "trunc.db"), false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeTruncate)

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)
}

// TestPagerJournalWAL_WriteDirtyPagesToWAL exercises writeDirtyPagesToWAL
// by committing in WAL mode.
func TestPagerJournalWAL_WriteDirtyPagesToWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "wal.db"), false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	pg.Data[DatabaseHeaderSize] = 0xAB
	p.Put(pg)
	mustCommit(t, p)
}

// TestPagerJournalWAL_AutoCheckpointWAL exercises autoCheckpointWAL by writing
// enough frames to meet the WALMinCheckpointFrames threshold.
func TestPagerJournalWAL_AutoCheckpointWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "autockpt.db"), false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)

	// Write pages normally to populate the WAL.
	for i := 0; i < 3; i++ {
		mustBeginWrite(t, p)
		pg := mustGetPage(t, p, 1)
		mustWritePage(t, p, pg)
		p.Put(pg)
		mustCommit(t, p)
	}

	// Now set frameCount to the threshold so the next commit triggers autoCheckpointWAL.
	p.mu.Lock()
	if p.wal != nil {
		p.wal.frameCount = WALMinCheckpointFrames
	}
	p.mu.Unlock()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)
}

// TestPagerJournalWAL_RollbackJournal exercises rollbackJournal and
// readJournalEntry by writing a page and rolling back.
func TestPagerJournalWAL_RollbackJournal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "rbj.db"), false)
	defer p.Close()

	// First commit to establish initial page state.
	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	pg.Data[DatabaseHeaderSize] = 0x11
	p.Put(pg)
	mustCommit(t, p)

	// Now modify and rollback — this exercises rollbackJournal / readJournalEntry.
	mustBeginWrite(t, p)
	pg2 := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg2)
	pg2.Data[DatabaseHeaderSize] = 0x22
	p.Put(pg2)
	mustRollback(t, p)
}

// TestPagerJournalWAL_ParseJournalEntryWithoutChecksum exercises
// parseJournalEntryWithoutChecksum by injecting a custom journal file that
// contains old-format entries (4+pageSize bytes, no trailing checksum).
func TestPagerJournalWAL_ParseJournalEntryWithoutChecksum(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "nocs.db")
	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	pageSize := p.PageSize()

	// Write initial page state so rollback has something to restore.
	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	// Build a raw journal file in the old format: 4-byte header, then
	// entries of exactly (4+pageSize) bytes — no checksum word.
	jFile := filepath.Join(dir, "nocs.db-journal")
	f, err := os.OpenFile(jFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("create journal: %v", err)
	}

	// 4-byte header: page size encoded big-endian.
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(pageSize))
	if _, err := f.Write(hdr); err != nil {
		t.Fatalf("write journal header: %v", err)
	}

	// One old-format entry: [4 bytes pgno][pageSize bytes data] — no checksum.
	entry := make([]byte, 4+pageSize)
	binary.BigEndian.PutUint32(entry[0:4], 1) // page 1
	if _, err := f.Write(entry); err != nil {
		t.Fatalf("write journal entry: %v", err)
	}
	f.Close()

	// Inject the journal file into the pager and force a write transaction
	// state so rollbackLocked will call rollbackJournal.
	jf, err := os.OpenFile(jFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open journal for inject: %v", err)
	}
	p.mu.Lock()
	p.journalFile = jf
	p.state = PagerStateWriterCachemod
	p.mu.Unlock()

	// Rollback should invoke rollbackJournal → readJournalEntry →
	// parseJournalEntryWithoutChecksum (n == 4+pageSize branch).
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback with old-format journal: %v", err)
	}
}

// TestPagerJournalWAL_JournalOpen exercises Journal.Open including the
// "already open" error path.
func TestPagerJournalWAL_JournalOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "j.journal")

	j := NewJournal(jPath, 4096, 1)

	// First Open should succeed.
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	defer j.Delete()

	// Second Open on an already-open journal should return an error.
	if err := j.Open(); err == nil {
		t.Error("Journal.Open on already-open journal: expected error, got nil")
	}
}

// TestPagerJournalWAL_JournalRestoreAllEntries exercises Journal.restoreAllEntries
// via Journal.Rollback.
func TestPagerJournalWAL_JournalRestoreAllEntries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "restore.db")

	// Create a real pager so we have a database file to restore to.
	p := openTestPagerAt(t, dbFile, false)

	// Write initial data.
	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	pg.Data[DatabaseHeaderSize] = 0xAA
	p.Put(pg)
	mustCommit(t, p)
	p.Close()

	// Create a journal with an entry for page 1 and call Rollback.
	jPath := filepath.Join(dir, "restore.db-journal")
	pageSize := 4096
	j := NewJournal(jPath, pageSize, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}

	// Write a valid journal entry for page 1.
	origData := make([]byte, pageSize)
	origData[DatabaseHeaderSize] = 0xBB
	if err := j.WriteOriginal(1, origData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// Re-open the pager so we have p.file to restore into.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()

	// Rollback exercises restoreAllEntries.
	if err := j.Rollback(p2); err != nil {
		t.Fatalf("Journal.Rollback: %v", err)
	}

	j.Delete()
}

// TestPagerJournalWAL_JournalZeroHeader exercises Journal.ZeroHeader.
func TestPagerJournalWAL_JournalZeroHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "zero.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Journal.Close: %v", err)
	}

	// ZeroHeader on an existing journal file.
	if err := j.ZeroHeader(); err != nil {
		t.Fatalf("Journal.ZeroHeader: %v", err)
	}

	// ZeroHeader on a non-existent file should be a no-op.
	j2 := NewJournal(filepath.Join(dir, "nonexistent.journal"), 4096, 1)
	if err := j2.ZeroHeader(); err != nil {
		t.Fatalf("Journal.ZeroHeader on missing file: %v", err)
	}

	_ = os.Remove(jPath)
}

// TestPagerJournalWAL_FlushAndEvictDirtyPages exercises
// MemoryPager.flushAndEvictDirtyPages by filling a tiny cache with dirty
// pages so that getLocked triggers the flush path.
// pjwAllocAndDirty allocates a page, gets it, marks it dirty, and puts it back.
func pjwAllocAndDirty(t *testing.T, mp *MemoryPager) Pgno {
	t.Helper()
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pg, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d): %v", pgno, err)
	}
	if err := mp.Write(pg); err != nil {
		t.Fatalf("Write(%d): %v", pgno, err)
	}
	mp.Put(pg)
	return pgno
}

func TestPagerJournalWAL_FlushAndEvictDirtyPages(t *testing.T) {
	t.Parallel()

	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	smallCache := NewPageCache(4096, 2)
	mp.mu.Lock()
	mp.cache = smallCache
	mp.mu.Unlock()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	pg1, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if err := mp.Write(pg1); err != nil {
		t.Fatalf("Write(1): %v", err)
	}
	mp.Put(pg1)

	pjwAllocAndDirty(t, mp)

	// Third page forces cache-full -> flushAndEvictDirtyPages.
	pgno3, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	pg3, err := mp.Get(pgno3)
	if err != nil {
		t.Fatalf("Get(%d): %v", pgno3, err)
	}
	mp.Put(pg3)

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// TestPagerJournalWAL_MemoryPagerSetUserVersion exercises MemoryPager.SetUserVersion.
func TestPagerJournalWAL_MemoryPagerSetUserVersion(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	if err := mp.SetUserVersion(7); err != nil {
		t.Fatalf("SetUserVersion: %v", err)
	}
	if got := mp.GetHeader().UserVersion; got != 7 {
		t.Errorf("UserVersion = %d, want 7", got)
	}
}

// TestPagerJournalWAL_MemoryPagerSetSchemaCookie exercises MemoryPager.SetSchemaCookie.
func TestPagerJournalWAL_MemoryPagerSetSchemaCookie(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	if err := mp.SetSchemaCookie(13); err != nil {
		t.Fatalf("SetSchemaCookie: %v", err)
	}
	if got := mp.GetHeader().SchemaCookie; got != 13 {
		t.Errorf("SchemaCookie = %d, want 13", got)
	}
}

// TestPagerJournalWAL_MemoryPagerVerifyFreeList exercises MemoryPager.VerifyFreeList.
func TestPagerJournalWAL_MemoryPagerVerifyFreeList(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	if err := mp.VerifyFreeList(); err != nil {
		t.Fatalf("VerifyFreeList: %v", err)
	}
}
