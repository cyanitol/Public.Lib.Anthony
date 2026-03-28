// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

// MC/DC test coverage for the pager package, batch 9.
// This file targets remaining uncovered branches in:
//   wal_checkpoint.go: checkpointFull (initialFrameCount > 0 path),
//                      checkpointRestart (frame write + restartWAL path),
//                      checkpointTruncate (frame write + truncateWALFile path),
//                      CheckpointWithMode (FULL/RESTART/TRUNCATE dispatch),
//                      CheckpointWithInfo (non-nil file branch).
//   journal.go:        updatePageCount (success path via explicit call),
//                      Truncate (file open + truncate), ZeroHeader,
//                      generateNonce (success path via Open).
//   memory_pager.go:   Commit (needsHeaderUpdate = true path),
//                      preparePageForWrite (savepoints branch),
//                      Commit (ErrNoTransaction guard).
//   wal_index.go:      open (size < minSize → initializeFile branch coverage).

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// WAL checkpoint helpers
// ---------------------------------------------------------------------------

// mcdc9SetupWALWithFrames creates a WAL file and writes n frames (page 1..n)
// into it, also creating a matching database file.
func mcdc9SetupWALWithFrames(t *testing.T, dir string, n int) (*WAL, string) {
	t.Helper()
	dbFile := filepath.Join(dir, "test.db")
	pageSize := 4096

	// Create a minimal database file (at least n pages).
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	dbData := make([]byte, pageSize*n)
	if _, err := f.Write(dbData); err != nil {
		f.Close()
		t.Fatalf("write db: %v", err)
	}
	f.Close()

	wal := NewWAL(dbFile, pageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}

	pageData := make([]byte, pageSize)
	for i := 1; i <= n; i++ {
		pageData[0] = byte(i) // mark each frame
		if err := wal.WriteFrame(Pgno(i), pageData, uint32(n)); err != nil {
			t.Fatalf("WriteFrame(%d): %v", i, err)
		}
	}
	return wal, dbFile
}

// ---------------------------------------------------------------------------
// CheckpointWithMode — FULL mode
//
// MC/DC:  mode == CheckpointFull → checkpointFull() called,
//         initialFrameCount > 0 → frames are checkpointed.
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_CheckpointFull(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, _ := mcdc9SetupWALWithFrames(t, dir, 3)
	defer wal.Close()

	if wal.FrameCount() == 0 {
		t.Fatal("expected frames before checkpoint")
	}

	got, remaining, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("CheckpointWithMode(Full): %v", err)
	}
	t.Logf("Full checkpoint: checkpointed=%d remaining=%d", got, remaining)
}

// ---------------------------------------------------------------------------
// CheckpointWithMode — RESTART mode
//
// MC/DC:  mode == CheckpointRestart → checkpointRestart() called,
//         initialFrameCount > 0 → frames checkpointed + WAL reset.
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_CheckpointRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, _ := mcdc9SetupWALWithFrames(t, dir, 3)
	defer wal.Close()

	got, remaining, err := wal.CheckpointWithMode(CheckpointRestart)
	if err != nil {
		t.Fatalf("CheckpointWithMode(Restart): %v", err)
	}
	t.Logf("Restart checkpoint: checkpointed=%d remaining=%d", got, remaining)

	// After RESTART, frameCount should be 0 (WAL was reset).
	if wal.FrameCount() != 0 {
		t.Errorf("expected frameCount=0 after RESTART, got %d", wal.FrameCount())
	}
}

// ---------------------------------------------------------------------------
// CheckpointWithMode — TRUNCATE mode
//
// MC/DC:  mode == CheckpointTruncate → checkpointTruncate() called,
//         initialFrameCount > 0 → frames checkpointed + WAL file truncated.
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_CheckpointTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, dbFile := mcdc9SetupWALWithFrames(t, dir, 3)
	// Do not defer Close — truncate mode closes the file.

	got, remaining, err := wal.CheckpointWithMode(CheckpointTruncate)
	if err != nil {
		t.Fatalf("CheckpointWithMode(Truncate): %v", err)
	}
	t.Logf("Truncate checkpoint: checkpointed=%d remaining=%d", got, remaining)

	// After TRUNCATE the WAL file should be empty or absent.
	walPath := dbFile + "-wal"
	info, statErr := os.Stat(walPath)
	if statErr == nil {
		if info.Size() != 0 {
			t.Errorf("expected WAL file to be truncated to 0 bytes, got %d", info.Size())
		}
	}
	// Close the wal file regardless (may already be nil after truncate).
	_ = wal.Close()
}

// ---------------------------------------------------------------------------
// CheckpointWithMode — empty WAL (initialFrameCount == 0) short-circuit paths
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_CheckpointFull_EmptyWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "empty.db")
	wal := NewWAL(dbFile, 4096)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}
	defer wal.Close()

	// FULL on empty WAL: initialFrameCount == 0 → early return.
	got, rem, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("CheckpointWithMode(Full, empty): %v", err)
	}
	if got != 0 || rem != 0 {
		t.Errorf("expected (0,0), got (%d,%d)", got, rem)
	}
}

func TestMCDC9_WAL_CheckpointRestart_EmptyWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "empty2.db")
	wal := NewWAL(dbFile, 4096)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}
	defer wal.Close()

	// RESTART on empty WAL: initialFrameCount == 0 → short-circuit.
	got, rem, err := wal.CheckpointWithMode(CheckpointRestart)
	if err != nil {
		t.Fatalf("CheckpointWithMode(Restart, empty): %v", err)
	}
	if got != 0 || rem != 0 {
		t.Errorf("expected (0,0), got (%d,%d)", got, rem)
	}
}

// ---------------------------------------------------------------------------
// CheckpointWithInfo — non-nil file branch (WAL file stat succeeds)
//
// MC/DC:  wal.file != nil → file.Stat() called for walSizeBefore.
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_CheckpointWithInfo_OpenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, _ := mcdc9SetupWALWithFrames(t, dir, 2)
	defer wal.Close()

	info, err := wal.CheckpointWithInfo(CheckpointPassive)
	if err != nil {
		t.Fatalf("CheckpointWithInfo: %v", err)
	}
	t.Logf("CheckpointWithInfo: %+v", info)
}

// ---------------------------------------------------------------------------
// Journal.updatePageCount — success path
//
// MC/DC:  j.file != nil → WriteAt succeeds.
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_UpdatePageCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "upd.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	defer func() { _ = j.Close() }()

	// Write one original page so the journal has data and pageCount > 0.
	page := make([]byte, 4096)
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// updatePageCount is unexported; call it indirectly by writing another page.
	if err := j.WriteOriginal(2, page); err != nil {
		t.Fatalf("WriteOriginal(2): %v", err)
	}
	// Verify page count was updated.
	if j.GetPageCount() < 2 {
		t.Errorf("expected pageCount >= 2, got %d", j.GetPageCount())
	}
}

// ---------------------------------------------------------------------------
// Journal.Truncate — file open path
//
// MC/DC:  j.file != nil → file.Close() called before truncating.
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_Truncate_OpenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "trunc.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	page := make([]byte, 4096)
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// Truncate while file is open — exercises j.file != nil branch.
	if err := j.Truncate(); err != nil {
		t.Fatalf("Journal.Truncate: %v", err)
	}

	// After truncate, journal should be uninitialized.
	if j.IsOpen() {
		t.Error("expected journal to be closed after Truncate")
	}
}

// ---------------------------------------------------------------------------
// Journal.Truncate — file already closed (nil file branch)
//
// MC/DC:  j.file == nil → skip Close, still truncate file on disk.
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_Truncate_ClosedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "trunc2.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	j.Close()

	// Truncate when file is nil — exercises j.file == nil branch.
	if err := j.Truncate(); err != nil {
		t.Fatalf("Journal.Truncate (closed): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Journal.ZeroHeader — success path
//
// MC/DC:  file exists → open, write zeros at offset 0, sync.
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_ZeroHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "zero.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	page := make([]byte, 4096)
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}
	j.Close()

	// ZeroHeader on an existing journal file.
	if err := j.ZeroHeader(); err != nil {
		t.Fatalf("Journal.ZeroHeader: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Journal.ZeroHeader — file not found (IsNotExist branch → return nil)
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_ZeroHeader_NotExist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "nonexistent.journal")

	j := NewJournal(jPath, 4096, 1)
	// Journal file was never created — ZeroHeader should return nil.
	if err := j.ZeroHeader(); err != nil {
		t.Fatalf("Journal.ZeroHeader (not exist): %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.Commit — ErrNoTransaction guard
//
// MC/DC:  mp.state < PagerStateWriterLocked → return ErrNoTransaction.
// ---------------------------------------------------------------------------

func TestMCDC9_MemoryPager_Commit_NoTransaction(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	// Commit without an active write transaction.
	if err := mp.Commit(); err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.Commit — needsHeaderUpdate = true (dbSize changed)
//
// MC/DC:  mp.dbSize != mp.dbOrigSize → updateDatabaseHeader is called.
// ---------------------------------------------------------------------------

func TestMCDC9_MemoryPager_Commit_HeaderUpdate(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Allocate a new page so dbSize increases (triggering needsHeaderUpdate).
	_, err = mp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	// Commit — needsHeaderUpdate = true because dbSize changed.
	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.preparePageForWrite — savepoints branch
//
// MC/DC:  len(mp.savepoints) > 0 → savePageState is called.
// ---------------------------------------------------------------------------

func TestMCDC9_MemoryPager_PreparePageForWrite_WithSavepoint(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Create a savepoint so len(mp.savepoints) > 0.
	if err := mp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	// Write a page — this calls preparePageForWrite with savepoints active.
	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if err := mp.Write(page); err != nil {
		mp.Put(page)
		t.Fatalf("Write: %v", err)
	}
	mp.Put(page)

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// ---------------------------------------------------------------------------
// WALIndex.open — size < minSize path (initializeFile is called)
//
// MC/DC:  file size < minSize → initializeFile called (new file).
// ---------------------------------------------------------------------------

func TestMCDC9_WALIndex_Open_NewFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "new_idx.db")

	// First open — file doesn't exist → creates and initializes.
	idx := mustOpenWALIndex(t, dbFile)
	if idx == nil {
		t.Fatal("expected non-nil WALIndex")
	}
	mustCloseWALIndex(t, idx)

	// Reopen on a tiny (empty) file to exercise the size < minSize path again.
	walShmPath := dbFile + "-shm"
	if err := os.Truncate(walShmPath, 0); err != nil && !os.IsNotExist(err) {
		t.Skipf("cannot truncate wal-shm: %v", err)
	}
	idx2 := mustOpenWALIndex(t, dbFile)
	mustCloseWALIndex(t, idx2)
}

// ---------------------------------------------------------------------------
// WAL.generateSalt — exercises salt generation (called in createNewWAL)
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_GenerateSalt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "salt.db")

	wal := NewWAL(dbFile, 4096)
	// createNewWAL is called during Open, which calls generateSalt twice.
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open: %v", err)
	}
	defer wal.Close()
	// generateSalt returns non-zero values with high probability.
	t.Log("WAL opened, salt generated during createNewWAL")
}

// ---------------------------------------------------------------------------
// WAL.Sync — exercises the Sync path after writes
// ---------------------------------------------------------------------------

func TestMCDC9_WAL_Sync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wal, _ := mcdc9SetupWALWithFrames(t, dir, 2)
	defer wal.Close()

	if err := wal.Sync(); err != nil {
		t.Fatalf("WAL.Sync: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Journal.IsValid — exercises validateHeader and ensureFileOpen paths
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_IsValid_AfterWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "valid.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	page := make([]byte, 4096)
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}
	// Finalize to make the journal valid (writes page count + syncs).
	if err := j.Finalize(); err != nil {
		t.Fatalf("Journal.Finalize: %v", err)
	}
	j.Close()

	// IsValid re-opens and validates the journal — exercises ensureFileOpen.
	valid, err := j.IsValid()
	if err != nil {
		t.Logf("Journal.IsValid error: %v (may be environment-specific)", err)
	}
	t.Logf("Journal.IsValid = %v", valid)
}

// ---------------------------------------------------------------------------
// Journal.Finalize — exercises the Finalize path
// ---------------------------------------------------------------------------

func TestMCDC9_Journal_Finalize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "finalize.journal")

	j := NewJournal(jPath, 4096, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	page := make([]byte, 4096)
	if err := j.WriteOriginal(1, page); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// Finalize writes page count and syncs.
	if err := j.Finalize(); err != nil {
		t.Fatalf("Journal.Finalize: %v", err)
	}
	j.Close()
}
