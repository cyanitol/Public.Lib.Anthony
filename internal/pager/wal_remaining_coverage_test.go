// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// --- WAL: multiple checkpoints ---

// TestWALRemaining_MultipleCheckpoints writes frames, checkpoints, writes more frames,
// then checkpoints again. Exercises the checkpoint sequence increment path in
// restartWAL and verifies the WAL remains functional across checkpoints.
func TestWALRemaining_MultipleCheckpoints(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "multi_ckpt.db")

	// Create backing db file large enough for two pages.
	dbData := make([]byte, DefaultPageSize*2)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	initialSeq := wal.checkpointSeq

	// Write frames for two pages.
	mustWriteFrame(t, wal, 1, makeTestPage(11, DefaultPageSize), 2)
	mustWriteFrame(t, wal, 2, makeTestPage(22, DefaultPageSize), 2)

	if wal.FrameCount() != 2 {
		t.Fatalf("expected 2 frames before first checkpoint, got %d", wal.FrameCount())
	}

	// Open the db file handle so checkpoint can write to it.
	dbF, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db file: %v", err)
	}
	defer dbF.Close()
	wal.dbFile = dbF

	// First checkpoint.
	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("first Checkpoint: %v", err)
	}

	if wal.FrameCount() != 0 {
		t.Errorf("expected 0 frames after first checkpoint, got %d", wal.FrameCount())
	}
	if wal.checkpointSeq <= initialSeq {
		t.Errorf("checkpointSeq should have incremented: was %d, now %d", initialSeq, wal.checkpointSeq)
	}

	seqAfterFirst := wal.checkpointSeq

	// Write more frames for a second checkpoint cycle.
	mustWriteFrame(t, wal, 1, makeTestPage(33, DefaultPageSize), 2)
	mustWriteFrame(t, wal, 2, makeTestPage(44, DefaultPageSize), 2)

	wal.dbFile = dbF
	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}

	if wal.FrameCount() != 0 {
		t.Errorf("expected 0 frames after second checkpoint, got %d", wal.FrameCount())
	}
	if wal.checkpointSeq <= seqAfterFirst {
		t.Errorf("checkpointSeq should have incremented again: was %d, now %d", seqAfterFirst, wal.checkpointSeq)
	}
}

// --- WAL: recovery after simulated crash ---

// TestWALRemaining_RecoveryAfterCrash creates a WAL file with committed frames
// on disk, then opens the database (simulating recovery after a crash). The
// recoverWALReadWrite path checkpoints the leftover WAL into the database.
func TestWALRemaining_RecoveryAfterCrash(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "crash.db")

	// Open pager and switch to WAL mode.
	p := openTestPager(t)
	// We need a file-backed pager for WAL recovery to work.
	p.Close()

	// Use a fresh file-backed pager.
	p2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := p2.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Write and commit a page.
	if err := p2.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	page, err := p2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	page.Data[DatabaseHeaderSize] = 0xBE
	if err := p2.Write(page); err != nil {
		t.Fatalf("Write: %v", err)
	}
	p2.Put(page)
	if err := p2.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Close WITHOUT checkpointing to leave WAL frames on disk.
	if err := p2.wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}
	p2.wal = nil
	if p2.walIndex != nil {
		p2.walIndex.Close()
		p2.walIndex = nil
	}
	p2.file.Close()

	// Now reopen the database — this triggers recoverWALIfExists → recoverWALReadWrite.
	p3, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer p3.Close()

	// After recovery the data written before the "crash" must be visible.
	if err := p3.BeginRead(); err != nil {
		t.Fatalf("BeginRead: %v", err)
	}
	recovered, err := p3.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after recovery: %v", err)
	}
	got := recovered.Data[DatabaseHeaderSize]
	p3.Put(recovered)
	if err := p3.EndRead(); err != nil {
		t.Fatalf("EndRead: %v", err)
	}

	if got != 0xBE {
		t.Errorf("recovery: expected byte 0xBE at DatabaseHeaderSize, got 0x%02X", got)
	}
}

// --- WAL: frames with different salt/checksum seeds (re-open after checkpoint) ---

// TestWALRemaining_ReopenAfterCheckpointNewSalt opens a WAL, writes frames,
// checkpoints (which calls restartWAL and generates new salts), writes more
// frames, then closes and reopens. The second open must successfully read the
// header with the new salt values — exercising readHeader + validateAllFrames
// with the post-checkpoint salt.
func TestWALRemaining_ReopenAfterCheckpointNewSalt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "salt.db")

	dbData := make([]byte, DefaultPageSize*3)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)

	mustWriteFrame(t, wal, 1, makeTestPage(10, DefaultPageSize), 3)
	mustWriteFrame(t, wal, 2, makeTestPage(20, DefaultPageSize), 3)

	dbF, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db file: %v", err)
	}
	wal.dbFile = dbF

	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	dbF.Close()
	wal.dbFile = nil

	// Write frames with the new post-checkpoint salt.
	mustWriteFrame(t, wal, 3, makeTestPage(30, DefaultPageSize), 3)

	salt1After := wal.salt1
	salt2After := wal.salt2

	wal.Close()

	// Reopen WAL — readHeader will restore the new salt values.
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("reopen WAL after checkpoint+new salt: %v", err)
	}
	defer wal2.Close()

	if wal2.salt1 != salt1After {
		t.Errorf("salt1 mismatch after reopen: want %d, got %d", salt1After, wal2.salt1)
	}
	if wal2.salt2 != salt2After {
		t.Errorf("salt2 mismatch after reopen: want %d, got %d", salt2After, wal2.salt2)
	}
	if wal2.FrameCount() != 1 {
		t.Errorf("expected 1 frame after reopen, got %d", wal2.FrameCount())
	}
}

// --- WAL: readFrameForChecksum (0% coverage) ---

// TestWALRemaining_ReadFrameForChecksum covers the readFrameForChecksum path
// (wal.go:703). This function is reached when validateFrameChecksum must read
// an intermediate frame from disk to compute a cumulative checksum. We write
// several frames, clear the full checksum cache, then read frame N — forcing
// calculateCumulativeChecksum to call readFrameForChecksum for the earlier frames.
func TestWALRemaining_ReadFrameForChecksum(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rfc.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	const numFrames = 5
	for i := 1; i <= numFrames; i++ {
		mustWriteFrame(t, wal, Pgno(i), makeTestPage(i*77, DefaultPageSize), uint32(i))
	}

	// Clear the entire cache. Now reading any frame other than frame 0 requires
	// readFrameForChecksum to be called for all predecessor frames.
	wal.checksumCache = make(map[uint32][2]uint32)

	// Reading the last frame forces readFrameForChecksum for frames 0..3.
	frame, err := wal.ReadFrame(uint32(numFrames - 1))
	if err != nil {
		t.Fatalf("ReadFrame(%d) after full cache clear: %v", numFrames-1, err)
	}
	if frame == nil {
		t.Fatal("ReadFrame returned nil")
	}
}

// --- WAL: validateFrame salt mismatch (validateFrameSalt error path) ---

// TestWALRemaining_ValidateFrameSaltMismatch exercises the validateFrameSalt
// error path inside validateAllFrames when the salt in a frame doesn't match
// the header. We write frames to a WAL then corrupt the frame salt on disk
// before reopening, expecting Open to recreate the WAL.
func TestWALRemaining_ValidateFrameSaltMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "saltmm.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	mustWriteFrame(t, wal, 1, makeTestPage(55, DefaultPageSize), 1)
	wal.Close()

	// Corrupt the salt1 field in the first frame header on disk.
	// Frame header starts at offset WALHeaderSize; Salt1 is at bytes 8-11.
	walFile := dbFile + "-wal"
	f, err := os.OpenFile(walFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}
	badSalt := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := f.WriteAt(badSalt, WALHeaderSize+8); err != nil {
		f.Close()
		t.Fatalf("corrupt salt: %v", err)
	}
	f.Close()

	// Reopen — validateAllFrames detects the salt mismatch, removes the WAL
	// and calls createNewWAL. Must not return an error.
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Open after salt corruption: %v", err)
	}
	defer wal2.Close()

	// The WAL was recreated so it should have 0 frames.
	if wal2.FrameCount() != 0 {
		t.Errorf("expected 0 frames after salt-corrupt reopen, got %d", wal2.FrameCount())
	}
}

// --- WAL: validateReadFrame — file nil and frame out of range ---

// TestWALRemaining_ValidateReadFrameErrors covers both branches in validateReadFrame:
// (1) WAL not open and (2) frame index out of range.
func TestWALRemaining_ValidateReadFrameErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vrf.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	// (1) WAL not open.
	wClosed := NewWAL(dbFile, DefaultPageSize)
	_, err := wClosed.ReadFrame(0)
	if err == nil {
		t.Error("ReadFrame on unopened WAL: expected error, got nil")
	}

	// (2) Frame out of range.
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()
	mustWriteFrame(t, wal, 1, makeTestPage(1, DefaultPageSize), 1)

	_, err = wal.ReadFrame(999)
	if err == nil {
		t.Error("ReadFrame(999) on WAL with 1 frame: expected error, got nil")
	}
}

// --- WAL: Delete with dbFile open ---

// TestWALRemaining_DeleteWithDBFileOpen exercises the wal.Delete path where
// both w.file and w.dbFile are non-nil, ensuring both are closed.
func TestWALRemaining_DeleteWithDBFileOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "del_dbf.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)

	// Attach an open db file handle.
	dbF, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db file: %v", err)
	}
	wal.dbFile = dbF

	if err := wal.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// WAL file should be gone.
	if _, statErr := os.Stat(dbFile + "-wal"); !os.IsNotExist(statErr) {
		t.Error("WAL file should have been deleted")
	}
	if wal.initialized {
		t.Error("WAL should not be initialized after Delete")
	}
}

// --- freelist Iterate: multiple trunk pages ---

// TestWALRemaining_FreelistIterateMultipleTrunks exercises the Iterate loop across
// multiple trunk pages (freelist.go:Iterate). Two trunk pages are wired together
// and Iterate must visit leaves from both.
func TestWALRemaining_FreelistIterateMultipleTrunks(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 → Trunk 3 → nil
	// Trunk 2 has two leaf pages: 10, 11
	mp.addTrunkPage(2, 3, []Pgno{10, 11})
	// Trunk 3 has one leaf page: 20
	mp.addTrunkPage(3, 0, []Pgno{20})

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 5 // 2 trunks + 3 leaves

	collected := make([]Pgno, 0, 5)
	err := fl.Iterate(func(pgno Pgno) bool {
		collected = append(collected, pgno)
		return true
	})
	if err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	// We expect exactly the 3 leaf pages from both trunks.
	if len(collected) != 3 {
		t.Errorf("expected 3 pages from Iterate, got %d: %v", len(collected), collected)
	}

	leafSet := map[Pgno]bool{10: true, 11: true, 20: true}
	for _, p := range collected {
		if !leafSet[p] {
			t.Errorf("unexpected page %d in Iterate results", p)
		}
	}
}

// TestWALRemaining_FreelistIterateEarlyStop verifies that returning false from
// the Iterate callback stops iteration immediately.
func TestWALRemaining_FreelistIterateEarlyStop(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	mp.addTrunkPage(2, 3, []Pgno{10, 11})
	mp.addTrunkPage(3, 0, []Pgno{20})

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 5

	count := 0
	err := fl.Iterate(func(pgno Pgno) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("Iterate (early stop): %v", err)
	}
	if count != 1 {
		t.Errorf("expected callback called once, got %d", count)
	}
}

// TestWALRemaining_FreelistIteratePendingThenOnDisk verifies that pending pages
// are visited before on-disk trunk pages.
func TestWALRemaining_FreelistIteratePendingThenOnDisk(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	mp.addTrunkPage(2, 0, []Pgno{30})

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 2
	fl.pendingFree = []Pgno{99}

	order := make([]Pgno, 0, 3)
	err := fl.Iterate(func(pgno Pgno) bool {
		order = append(order, pgno)
		return true
	})
	if err != nil {
		t.Fatalf("Iterate (pending then disk): %v", err)
	}

	if len(order) != 2 {
		t.Fatalf("expected 2 pages, got %d: %v", len(order), order)
	}
	// Pending (99) must appear before on-disk leaf (30).
	if order[0] != 99 {
		t.Errorf("expected first page 99 (pending), got %d", order[0])
	}
	if order[1] != 30 {
		t.Errorf("expected second page 30 (on-disk leaf), got %d", order[1])
	}
}

// --- freelist ReadTrunk: trunk with leaf pages ---

// TestWALRemaining_FreelistReadTrunkWithLeaves exercises ReadTrunk against a mock
// trunk page that has multiple leaf entries.
func TestWALRemaining_FreelistReadTrunkWithLeaves(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	mp.addTrunkPage(5, 7, []Pgno{100, 200, 300})

	fl := NewFreeList(mp)
	fl.firstTrunk = 5
	fl.totalFree = 4

	next, leaves, err := fl.ReadTrunk(5)
	if err != nil {
		t.Fatalf("ReadTrunk: %v", err)
	}
	if next != 7 {
		t.Errorf("expected next trunk 7, got %d", next)
	}
	if len(leaves) != 3 {
		t.Fatalf("expected 3 leaves, got %d", len(leaves))
	}
	expected := []Pgno{100, 200, 300}
	for i, want := range expected {
		if leaves[i] != want {
			t.Errorf("leaf[%d]: want %d, got %d", i, want, leaves[i])
		}
	}
}

// TestWALRemaining_FreelistReadTrunkZero covers the zero-page guard in ReadTrunk.
func TestWALRemaining_FreelistReadTrunkZero(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	fl := NewFreeList(mp)

	_, _, err := fl.ReadTrunk(0)
	if err != ErrInvalidTrunkPage {
		t.Errorf("ReadTrunk(0): expected ErrInvalidTrunkPage, got %v", err)
	}
}

// --- journal IsValid: valid and invalid states ---

// TestWALRemaining_JournalIsValidValid verifies IsValid returns true for a
// properly created and opened journal file.
func TestWALRemaining_JournalIsValidValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "valid.db-journal")

	j := mustOpenJournal(t, jPath, DefaultPageSize, 1)
	j.Close()

	// IsValid should return true: file exists, magic is correct, page size matches.
	valid, err := j.IsValid()
	if err != nil {
		t.Fatalf("IsValid (valid journal): %v", err)
	}
	if !valid {
		t.Error("IsValid: expected true for a valid journal, got false")
	}
}

// TestWALRemaining_JournalIsValidBadMagic writes a journal header with a
// wrong magic number — IsValid must return false, not an error.
func TestWALRemaining_JournalIsValidBadMagic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "badmagic.db-journal")

	// Write a header-sized block with a zeroed magic number.
	hdr := make([]byte, JournalHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], 0x00000000) // bad magic
	binary.BigEndian.PutUint32(hdr[20:24], uint32(DefaultPageSize))
	binary.BigEndian.PutUint32(hdr[24:28], JournalFormatVersion)
	if err := os.WriteFile(jPath, hdr, 0600); err != nil {
		t.Fatalf("write bad-magic journal: %v", err)
	}

	j := NewJournal(jPath, DefaultPageSize, 1)
	valid, err := j.IsValid()
	if err != nil {
		t.Fatalf("IsValid (bad magic): unexpected error: %v", err)
	}
	if valid {
		t.Error("IsValid: expected false for journal with bad magic, got true")
	}
}

// TestWALRemaining_JournalIsValidWrongPageSize exercises the page-size mismatch
// branch of validateHeader — IsValid must return false.
func TestWALRemaining_JournalIsValidWrongPageSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "wrongps.db-journal")

	// Create a valid journal but with a mismatched page size stored in header.
	hdr := make([]byte, JournalHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], JournalMagic)
	binary.BigEndian.PutUint32(hdr[20:24], uint32(512)) // wrong page size
	binary.BigEndian.PutUint32(hdr[24:28], JournalFormatVersion)
	if err := os.WriteFile(jPath, hdr, 0600); err != nil {
		t.Fatalf("write wrong-page-size journal: %v", err)
	}

	j := NewJournal(jPath, DefaultPageSize, 1)
	valid, err := j.IsValid()
	if err != nil {
		t.Fatalf("IsValid (wrong page size): unexpected error: %v", err)
	}
	if valid {
		t.Error("IsValid: expected false for journal with wrong page size, got true")
	}
}

// TestWALRemaining_JournalIsValidWrongVersion covers the format-version mismatch
// branch of validateHeader.
func TestWALRemaining_JournalIsValidWrongVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "wrongver.db-journal")

	hdr := make([]byte, JournalHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], JournalMagic)
	binary.BigEndian.PutUint32(hdr[20:24], uint32(DefaultPageSize))
	binary.BigEndian.PutUint32(hdr[24:28], 0x00000099) // bad version
	if err := os.WriteFile(jPath, hdr, 0600); err != nil {
		t.Fatalf("write wrong-version journal: %v", err)
	}

	j := NewJournal(jPath, DefaultPageSize, 1)
	valid, err := j.IsValid()
	if err != nil {
		t.Fatalf("IsValid (wrong version): unexpected error: %v", err)
	}
	if valid {
		t.Error("IsValid: expected false for journal with wrong format version, got true")
	}
}

// TestWALRemaining_JournalIsValidNoFile covers the not-exists branch: journalFileExists
// returns false, IsValid returns (false, nil) immediately.
func TestWALRemaining_JournalIsValidNoFile(t *testing.T) {
	t.Parallel()

	j := NewJournal("/nonexistent/path/test.db-journal", DefaultPageSize, 1)
	valid, err := j.IsValid()
	if err != nil {
		t.Fatalf("IsValid (no file): unexpected error: %v", err)
	}
	if valid {
		t.Error("IsValid: expected false when journal file does not exist")
	}
}

// --- journal Delete ---

// TestWALRemaining_JournalDeleteOpenFile exercises Delete when the journal file
// is currently open (j.file != nil). The file must be closed and removed.
func TestWALRemaining_JournalDeleteOpenFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "del_open.db-journal")

	j := mustOpenJournal(t, jPath, DefaultPageSize, 1)
	// j.file is non-nil here.

	if err := j.Delete(); err != nil {
		t.Fatalf("Delete (open file): %v", err)
	}

	if j.IsOpen() {
		t.Error("journal file should be closed after Delete")
	}
	if j.initialized {
		t.Error("journal should not be initialized after Delete")
	}
	if _, err := os.Stat(jPath); !os.IsNotExist(err) {
		t.Error("journal file should have been removed by Delete")
	}
}

// TestWALRemaining_JournalDeleteAlreadyClosed exercises Delete when j.file is nil
// but the journal file exists on disk. Must remove the file without error.
func TestWALRemaining_JournalDeleteAlreadyClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "del_closed.db-journal")

	j := mustOpenJournal(t, jPath, DefaultPageSize, 1)
	j.Close() // j.file = nil now, file still exists on disk

	if err := j.Delete(); err != nil {
		t.Fatalf("Delete (already closed): %v", err)
	}

	if _, err := os.Stat(jPath); !os.IsNotExist(err) {
		t.Error("journal file should have been removed by Delete")
	}
}

// TestWALRemaining_JournalDeleteNonExistent exercises Delete when neither the
// file handle nor the file on disk exists — os.IsNotExist must be swallowed.
func TestWALRemaining_JournalDeleteNonExistent(t *testing.T) {
	t.Parallel()

	j := NewJournal("/tmp/no_such_journal_xyz.db-journal", DefaultPageSize, 1)
	// j.file is nil and the file doesn't exist.

	if err := j.Delete(); err != nil {
		t.Fatalf("Delete (non-existent): expected nil, got %v", err)
	}
}

// --- journal restoreEntry ---

// TestWALRemaining_JournalRestoreEntryValid exercises the happy path of
// restoreEntry by manually constructing a valid journal file (header + entry
// with correct checksum) and calling Journal.Rollback() directly.
// The Journal.Rollback path seeks to JournalHeaderSize before reading entries,
// so the journal file must have the header at offset 0 and the entry at offset
// JournalHeaderSize.
func TestWALRemaining_JournalRestoreEntryValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "restore.db")
	jPath := filepath.Join(dir, "restore.db-journal")

	const pageSize = DefaultPageSize
	// Page 1 original content to be restored.
	originalData := make([]byte, pageSize)
	originalData[DatabaseHeaderSize] = 0xCC

	// Build the journal file manually so the entry sits at JournalHeaderSize.
	// Header layout (28 bytes):
	//   [0:4]   magic
	//   [4:8]   page count
	//   [8:12]  nonce
	//   [12:16] initial size
	//   [16:20] sector size
	//   [20:24] page size
	//   [24:28] format version
	const nonce = uint32(0x12345678)
	hdr := make([]byte, JournalHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], JournalMagic)
	binary.BigEndian.PutUint32(hdr[4:8], 1) // page count
	binary.BigEndian.PutUint32(hdr[8:12], nonce)
	binary.BigEndian.PutUint32(hdr[12:16], 1) // initial size
	binary.BigEndian.PutUint32(hdr[16:20], 512)
	binary.BigEndian.PutUint32(hdr[20:24], uint32(pageSize))
	binary.BigEndian.PutUint32(hdr[24:28], JournalFormatVersion)

	// Build entry: [4: pageNum][pageSize: data][4: checksum]
	// The checksum must match what Journal.calculateChecksum produces with the
	// same nonce. We build a throwaway Journal to calculate the checksum.
	jCalc := &Journal{pageSize: pageSize, nonce: nonce}
	checksum := jCalc.calculateChecksum(1, originalData)

	entry := make([]byte, 4+pageSize+4)
	binary.BigEndian.PutUint32(entry[0:4], 1) // page number 1
	copy(entry[4:4+pageSize], originalData)
	binary.BigEndian.PutUint32(entry[4+pageSize:], checksum)

	// Write header + entry to the journal file.
	jData := append(hdr, entry...)
	if err := os.WriteFile(jPath, jData, 0600); err != nil {
		t.Fatalf("write journal file: %v", err)
	}

	// Create a database file large enough for page 1 (offset 0).
	dbData := make([]byte, pageSize)
	dbData[DatabaseHeaderSize] = 0xFF // "dirty" value
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	// Open files and invoke Rollback.
	jf, err := os.OpenFile(jPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer jf.Close()

	dbF, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db file: %v", err)
	}
	defer dbF.Close()

	j := &Journal{
		filename: jPath,
		pageSize: pageSize,
		nonce:    nonce,
		file:     jf,
	}

	p := &Pager{
		file:     dbF,
		pageSize: pageSize,
	}

	if err := j.Rollback(p); err != nil {
		t.Fatalf("Journal.Rollback: %v", err)
	}

	// Verify restoreEntry wrote the original 0xCC back to the db file.
	buf := make([]byte, pageSize)
	if _, err := dbF.ReadAt(buf, 0); err != nil {
		t.Fatalf("read db file after rollback: %v", err)
	}
	got := buf[DatabaseHeaderSize]
	if got != 0xCC {
		t.Errorf("restoreEntry: expected 0xCC after rollback, got 0x%02X", got)
	}
}

// TestWALRemaining_JournalRestoreEntryChecksumMismatch injects a corrupted journal
// entry (bad checksum) and calls Rollback, exercising the checksum mismatch error
// branch in restoreEntry.
func TestWALRemaining_JournalRestoreEntryChecksumMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "badcksum.db-journal")
	dbFile := filepath.Join(dir, "badcksum.db")

	// Create a database file to write to.
	dbF, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("create db file: %v", err)
	}
	if err := dbF.Truncate(int64(DefaultPageSize)); err != nil {
		dbF.Close()
		t.Fatalf("truncate db file: %v", err)
	}
	dbF.Close()

	// Write a journal with a valid header and one entry, then corrupt the checksum.
	j := NewJournal(jPath, DefaultPageSize, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Open journal: %v", err)
	}
	pageData := makeTestPage(42, DefaultPageSize)
	if err := j.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}
	j.file.Close()
	j.file = nil

	// Corrupt the checksum at the end of the first entry.
	// Entry layout: [4: pageNum][pageSize: data][4: checksum]
	checksumOffset := int64(JournalHeaderSize) + int64(4) + int64(DefaultPageSize)
	jf, err := os.OpenFile(jPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open journal for corruption: %v", err)
	}
	badCksum := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := jf.WriteAt(badCksum, checksumOffset); err != nil {
		jf.Close()
		t.Fatalf("corrupt checksum: %v", err)
	}
	jf.Close()

	// Now build a minimal pager so Rollback can call restoreEntry.
	dbF2, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db file for pager: %v", err)
	}

	// Re-open the journal file for reading.
	jf2, err := os.Open(jPath)
	if err != nil {
		dbF2.Close()
		t.Fatalf("open journal for rollback: %v", err)
	}

	j2 := NewJournal(jPath, DefaultPageSize, 1)
	j2.file = jf2

	// Build a minimal Pager struct with enough fields set for restoreEntry.
	p := &Pager{
		file:     dbF2,
		pageSize: DefaultPageSize,
	}

	err = j2.Rollback(p)
	dbF2.Close()
	jf2.Close()

	if err == nil {
		t.Error("Rollback with corrupted checksum: expected error, got nil")
	}
}
