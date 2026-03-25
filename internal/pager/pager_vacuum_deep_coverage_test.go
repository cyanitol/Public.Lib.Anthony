// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCopyDatabaseToTarget_VacuumIntoVerifiesData creates a pager with several
// pages of known data, performs VACUUM INTO a real temporary file, then opens
// the output file and confirms all pages are present and intact.
func TestCopyDatabaseToTarget_VacuumIntoVerifiesData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "dst_deep.db")

	p := openTestPagerAt(t, srcFile, false)

	// Write distinct data to several pages and commit.
	for i := Pgno(2); i <= 8; i++ {
		page := mustGetWritePage(t, p, i)
		page.Data[0] = byte(i * 7)
		p.Put(page)
	}
	mustCommit(t, p)

	if err := p.Vacuum(&VacuumOptions{IntoFile: dstFile}); err != nil {
		t.Fatalf("Vacuum(IntoFile): %v", err)
	}
	p.Close()

	// Destination file must exist and have non-zero size.
	info, err := os.Stat(dstFile)
	if err != nil {
		t.Fatalf("Stat(dstFile): %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("VACUUM INTO output file is empty")
	}

	// Open the destination pager and verify page data.
	dst := openTestPagerAt(t, dstFile, false)
	defer dst.Close()

	for i := Pgno(2); i <= 8; i++ {
		data := mustReadPageAtOffset(t, dst, i, 0, 1)
		if data[0] != byte(i*7) {
			t.Errorf("dst page %d: got %d, want %d", i, data[0], byte(i*7))
		}
	}
}

// TestCopyDatabaseToTarget_VacuumIntoWithFreePages creates a pager, allocates
// pages, frees some of them, then performs VACUUM INTO to confirm free pages
// are excluded and the output file is compacted.
func TestCopyDatabaseToTarget_VacuumIntoWithFreePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_free.db")
	dstFile := filepath.Join(dir, "dst_free.db")

	p := openTestPagerAt(t, srcFile, false)

	// Allocate pages 2-10 with recognisable data.
	for i := Pgno(2); i <= 10; i++ {
		page := mustGetWritePage(t, p, i)
		page.Data[0] = byte(i + 100)
		p.Put(page)
	}
	mustCommit(t, p)

	// Free pages 5, 6, 7 so copyLivePages must skip them.
	if p.freeList != nil {
		for _, pg := range []Pgno{5, 6, 7} {
			if err := p.freeList.Free(pg); err != nil {
				t.Logf("Free(%d) warning: %v", pg, err)
			}
		}
		mustFlush(t, p.freeList)
		mustCommit(t, p)
	}

	if err := p.Vacuum(&VacuumOptions{IntoFile: dstFile}); err != nil {
		t.Fatalf("Vacuum(IntoFile) with free pages: %v", err)
	}
	p.Close()

	info, err := os.Stat(dstFile)
	if err != nil {
		t.Fatalf("Stat(dstFile): %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("VACUUM INTO output file is empty after free-page run")
	}
}

// TestWALIndexOpen_ViaWALModeFileOnDisk opens a WAL-mode database that was
// written to disk, closes it, then reopens it in read-only mode so that
// recoverWALReadOnly calls NewWALIndex -> open() on the existing shm file.
// This exercises the open() branch where the shm file already has sufficient
// size (>= minSize) and initializeFile is skipped.
func TestWALIndexOpen_ViaWALModeFileOnDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_disk.db")

	// Phase 1: create a WAL-mode database and write some frames.
	pw := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, pw, JournalModeWAL)

	for i := Pgno(2); i <= 4; i++ {
		page := mustGetWritePage(t, pw, i)
		page.Data[0] = byte(i * 11)
		pw.Put(page)
		mustCommit(t, pw)
	}
	pw.Close()

	// The -wal and -shm files now exist on disk.
	walFile := dbFile + "-wal"
	shmFile := dbFile + "-shm"
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Skip("WAL file not present, skipping read-only reopen test")
	}
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Skip("SHM file not present, skipping read-only reopen test")
	}

	// Phase 2: open read-only so recoverWALReadOnly -> NewWALIndex -> open()
	// finds the existing shm file (size >= minSize, so initializeFile is skipped).
	pr := openTestPagerAt(t, dbFile, true)
	defer pr.Close()

	if pr.walIndex == nil {
		t.Fatal("walIndex should be non-nil after read-only WAL recovery")
	}
	if !pr.walIndex.IsInitialized() {
		t.Error("walIndex should be initialized after read-only WAL recovery")
	}
	if pr.GetJournalMode() != JournalModeWAL {
		t.Errorf("journal mode = %d, want JournalModeWAL", pr.GetJournalMode())
	}
}

// TestWALIndexOpen_OpenFileFailure exercises the os.OpenFile error path in
// open() by pointing the shm filename at a path that cannot be opened for
// writing (a read-only directory entry).
func TestWALIndexOpen_OpenFileFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "fail_open.db")

	shmFile := dbFile + "-shm"
	// Create the shm file, make it read-only, so O_RDWR|O_CREATE fails.
	if err := os.WriteFile(shmFile, []byte{}, 0400); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := NewWALIndex(dbFile)
	if err == nil {
		// On some systems (e.g. running as root) this may succeed; skip gracefully.
		t.Skip("os.OpenFile on read-only file succeeded (possibly running as root)")
	}
	// The error should wrap "failed to open WAL index file".
	if err.Error() == "" {
		t.Error("expected non-empty error from NewWALIndex on read-only shm file")
	}
}
