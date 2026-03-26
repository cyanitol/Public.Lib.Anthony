// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestWALRecoverReadOnly_OpenFails covers the wal.Open() error branch in
// recoverWALReadOnly (line 813-815). It calls the method directly on a Pager
// whose filename points into a non-existent directory so that WAL file
// creation fails.
func TestWALRecoverReadOnly_OpenFails(t *testing.T) {
	t.Parallel()

	// Point filename at a path whose parent directory does not exist.
	p := &Pager{
		filename: filepath.Join(t.TempDir(), "nosuchdir", "test.db"),
		pageSize: DefaultPageSize,
	}

	err := p.recoverWALReadOnly()
	if err == nil {
		t.Fatal("recoverWALReadOnly: expected error when WAL cannot be opened, got nil")
	}
}

// TestWALRecoverReadOnly_WALIndexFails covers the NewWALIndex() error branch in
// recoverWALReadOnly (lines 818-823). It creates a real WAL file, then makes the
// parent directory read-only so that the shared-memory (-shm) file cannot be
// created, forcing NewWALIndex to return an error.
func TestWALRecoverReadOnly_WALIndexFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod directory read-only not reliable on Windows")
	}
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv.db")

	// Write a minimal DB file so pager headers are present.
	dbData := make([]byte, DefaultPageSize)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	// Create and close a real WAL file with one frame so size > WALHeaderSize.
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	mustWriteFrame(t, wal, 1, make([]byte, DefaultPageSize), 1)
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// Make the directory read-only so the -shm file cannot be created.
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	t.Cleanup(func() {
		// Restore so TempDir cleanup can remove files.
		_ = os.Chmod(dir, 0700)
	})

	p := &Pager{
		filename: dbFile,
		pageSize: DefaultPageSize,
	}

	err := p.recoverWALReadOnly()
	if err == nil {
		t.Fatal("recoverWALReadOnly: expected error when WAL index cannot be opened, got nil")
	}
	// Ensure wal is cleaned up on failure (p.wal should be nil).
	if p.wal != nil {
		t.Error("recoverWALReadOnly: p.wal should be nil after NewWALIndex failure")
	}
}

// TestWALRecoverReadOnly_Success covers the success path of recoverWALReadOnly.
// It creates a WAL with frames, then calls the function directly and verifies
// that p.wal, p.walIndex, and p.journalMode are set correctly.
func TestWALRecoverReadOnly_Success(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_ro_ok.db")

	dbData := make([]byte, DefaultPageSize)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	mustWriteFrame(t, wal, 1, make([]byte, DefaultPageSize), 1)
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	p := &Pager{
		filename: dbFile,
		pageSize: DefaultPageSize,
	}

	if err := p.recoverWALReadOnly(); err != nil {
		t.Fatalf("recoverWALReadOnly: unexpected error: %v", err)
	}
	if p.wal == nil {
		t.Error("p.wal should not be nil after successful recoverWALReadOnly")
	}
	if p.walIndex == nil {
		t.Error("p.walIndex should not be nil after successful recoverWALReadOnly")
	}
	if p.journalMode != JournalModeWAL {
		t.Errorf("journalMode = %d, want JournalModeWAL (%d)", p.journalMode, JournalModeWAL)
	}
	// Cleanup.
	if p.wal != nil {
		p.wal.Close()
	}
	if p.walIndex != nil {
		p.walIndex.Close()
	}
}

// TestWALRecoverReadWrite_OpenFails covers the wal.Open() error branch in
// recoverWALReadWrite (lines 834-836). It calls the method directly on a Pager
// whose filename points into a non-existent directory so WAL creation fails.
func TestWALRecoverReadWrite_OpenFails(t *testing.T) {
	t.Parallel()

	p := &Pager{
		filename: filepath.Join(t.TempDir(), "nosuchdir", "test.db"),
		pageSize: DefaultPageSize,
	}

	err := p.recoverWALReadWrite()
	if err == nil {
		t.Fatal("recoverWALReadWrite: expected error when WAL cannot be opened, got nil")
	}
}

// TestWALRecoverReadWrite_CheckpointFails covers the wal.Checkpoint() error
// branch in recoverWALReadWrite (lines 842-845). It creates a WAL with frames,
// then provides a read-only p.file so the checkpoint write to the database file
// fails.
func TestWALRecoverReadWrite_CheckpointFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_rw_ckpt.db")

	// Write a real database page so the file exists and is readable.
	dbData := make([]byte, DefaultPageSize)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	// Create WAL with a frame so frameCount > 0 during checkpoint.
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	mustWriteFrame(t, wal, 1, make([]byte, DefaultPageSize), 1)
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// Open the db file read-only so writes during checkpoint fail.
	roFile, err := os.OpenFile(dbFile, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open db read-only: %v", err)
	}
	defer roFile.Close()

	p := &Pager{
		filename: dbFile,
		pageSize: DefaultPageSize,
		file:     roFile,
	}

	err = p.recoverWALReadWrite()
	if err == nil {
		t.Fatal("recoverWALReadWrite: expected error when checkpoint fails, got nil")
	}
}

// TestWALRecoverReadWrite_Success covers the full success path of
// recoverWALReadWrite: opens WAL, checkpoints frames into the db, closes WAL,
// and updates p.dbSize.
func TestWALRecoverReadWrite_Success(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_rw_ok.db")

	dbData := make([]byte, DefaultPageSize)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("create db file: %v", err)
	}

	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	mustWriteFrame(t, wal, 1, make([]byte, DefaultPageSize), 1)
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// Open the db file read-write so checkpoint can write to it.
	rwFile, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open db rw: %v", err)
	}
	defer rwFile.Close()

	p := &Pager{
		filename: dbFile,
		pageSize: DefaultPageSize,
		file:     rwFile,
	}

	if err := p.recoverWALReadWrite(); err != nil {
		t.Fatalf("recoverWALReadWrite: unexpected error: %v", err)
	}
	if p.dbSize == 0 {
		t.Error("p.dbSize should be > 0 after successful recoverWALReadWrite")
	}
}
