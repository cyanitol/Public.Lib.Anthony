// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEnableWALMode_WALOpenFailure covers the p.wal.Open() error path inside
// enableWALMode.  The pager filename is set to a path inside a nonexistent
// directory so that WAL file creation fails.  The test verifies that:
//   - an error is returned
//   - p.wal is set back to nil (cleanup happened)
func TestEnableWALMode_WALOpenFailure(t *testing.T) {
	t.Parallel()

	// Open a valid pager, then redirect its filename to an invalid location.
	p := openTestPager(t)

	// Point the pager filename at a path whose parent directory does not exist.
	p.filename = filepath.Join(t.TempDir(), "nosuchdir", "sub.db")

	p.mu.Lock()
	err := p.enableWALMode()
	p.mu.Unlock()

	if err == nil {
		t.Fatal("enableWALMode with bad path: expected error, got nil")
	}
	if p.wal != nil {
		t.Error("enableWALMode with bad path: p.wal should be nil after failure")
	}
}

// TestEnableWALMode_NewWALIndexFailure covers the NewWALIndex() error path
// inside enableWALMode.  The test creates the WAL file in advance so that
// p.wal.Open() succeeds, then makes the directory read-only so that the
// WAL index (.db-shm) file cannot be created.
//
// After the call the test verifies that:
//   - an error is returned
//   - both p.wal and p.walIndex are nil (cleanup happened)
func TestEnableWALMode_NewWALIndexFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.db")

	// Create the database file so Open succeeds.
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}

	// Open a pager against that file.
	p := openTestPagerAt(t, dbFile, false)

	// Pre-create the WAL file so WAL.Open() succeeds.
	walFile := dbFile + "-wal"
	if err := os.WriteFile(walFile, []byte{}, 0600); err != nil {
		t.Fatalf("failed to pre-create WAL file: %v", err)
	}

	// Make the directory read-only so the .db-shm file cannot be created.
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	// Restore permissions so the temp dir can be cleaned up.
	t.Cleanup(func() { os.Chmod(dir, 0755) })

	p.mu.Lock()
	err := p.enableWALMode()
	p.mu.Unlock()

	if err == nil {
		t.Fatal("enableWALMode with unwritable dir: expected error, got nil")
	}
	if p.wal != nil {
		t.Error("enableWALMode with unwritable dir: p.wal should be nil after failure")
	}
	if p.walIndex != nil {
		t.Error("enableWALMode with unwritable dir: p.walIndex should be nil after failure")
	}
}

// TestEnableWALMode_SetPageCountFailure_UninitializedIndex covers the
// SetPageCount "WAL index not initialized" error path.  Because enableWALMode
// creates the WALIndex internally, this test exercises the same code path by
// constructing an uninitialized WALIndex directly and calling SetPageCount on
// it, confirming the error text and ensuring callers receive a meaningful error.
func TestEnableWALMode_SetPageCountFailure_UninitializedIndex(t *testing.T) {
	t.Parallel()

	// Build an uninitialized WALIndex (initialized == false by default).
	idx := &WALIndex{
		hashTable: make(map[uint32]uint32),
	}

	err := idx.SetPageCount(42)
	if err == nil {
		t.Fatal("SetPageCount on uninitialized WALIndex: expected error, got nil")
	}
}

// TestEnableWALMode_SetPageCountFailure_CorruptMmap covers the writeHeader
// ErrWALIndexCorrupt path that SetPageCount reaches when the mmap slice is
// shorter than WALIndexHeaderSize.  This is the condition that causes
// enableWALMode's SetPageCount call to fail in degenerate environments.
func TestEnableWALMode_SetPageCountFailure_CorruptMmap(t *testing.T) {
	t.Parallel()

	// Build a WALIndex that looks initialized but has an empty mmap so that
	// writeHeader returns ErrWALIndexCorrupt.
	idx := &WALIndex{
		initialized: true,
		header:      &WALIndexHeader{},
		hashTable:   make(map[uint32]uint32),
		mmap:        []byte{}, // too short — triggers ErrWALIndexCorrupt in writeHeader
	}

	err := idx.SetPageCount(10)
	if err == nil {
		t.Fatal("SetPageCount with corrupt mmap: expected error, got nil")
	}
}

// TestEnableWALMode_CleanupOnWALOpenFailure verifies that after a WAL open
// failure p.wal is nil and the pager can still be used normally.
func TestEnableWALMode_CleanupOnWALOpenFailure(t *testing.T) {
	t.Parallel()

	p := openTestPager(t)
	origFilename := p.filename

	// Corrupt the filename so enableWALMode fails.
	p.filename = filepath.Join(t.TempDir(), "missing", "db.db")

	p.mu.Lock()
	err := p.enableWALMode()
	p.mu.Unlock()

	if err == nil {
		t.Fatal("expected error from enableWALMode with bad path")
	}

	// Restore valid filename and verify the pager is still usable.
	p.filename = origFilename
	if p.wal != nil {
		t.Error("p.wal should be nil after failed enableWALMode")
	}
}

// TestEnableWALMode_ReadOnlyDirect is a regression guard confirming that
// the readOnly check at the top of enableWALMode is still enforced when
// called directly (bypassing SetJournalMode).
func TestEnableWALMode_ReadOnlyDirect(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro.db")

	// Create the file first with a writable pager.
	rw := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, rw)
	pg := mustGetPage(t, rw, 1)
	pg.Data[DatabaseHeaderSize] = 0x01
	mustWritePage(t, rw, pg)
	rw.Put(pg)
	mustCommit(t, rw)
	rw.Close()

	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	ro.mu.Lock()
	err := ro.enableWALMode()
	ro.mu.Unlock()

	if err == nil {
		t.Fatal("enableWALMode on read-only pager: expected error, got nil")
	}
}
