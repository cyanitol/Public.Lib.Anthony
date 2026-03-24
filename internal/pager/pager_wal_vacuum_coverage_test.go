// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALValidateFrameChecksum_ReadBackFrames exercises validateFrameChecksum
// by writing frames to a WAL through a WAL-mode pager, closing, and reopening.
// Recovery on open re-reads and validates every frame checksum.
func TestWALValidateFrameChecksum_ReadBackFrames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vfc.db")

	p := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p, JournalModeWAL)

	// Write several pages so multiple WAL frames are present.
	for i := 0; i < 5; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		for j := range page.Data {
			page.Data[j] = byte((i*17 + j) % 251)
		}
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}
	p.Close()

	// Reopen - recovery reads and validates every frame checksum.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()

	if p2.wal == nil {
		// WAL was checkpointed on close; re-enable to confirm frames readable.
		mustSetJournalMode(t, p2, JournalModeWAL)
	}
}

// TestWALValidateFrameChecksum_CheckpointAfterWrite exercises validateFrameChecksum
// via an explicit checkpoint after writing multiple frames.
func TestWALValidateFrameChecksum_CheckpointAfterWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vfc2.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	// Write 3 pages into the WAL.
	for i := 0; i < 3; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	// Checkpoint reads and validates each frame before copying to the database.
	mustCheckpoint(t, p)
}

// TestEnableWALMode_SwitchAndVerify exercises enableWALMode by calling
// SetJournalMode(WAL) and confirming the WAL and WAL index are created.
func TestEnableWALMode_SwitchAndVerify(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	if p.GetJournalMode() != JournalModeWAL {
		t.Errorf("GetJournalMode() = %d, want %d", p.GetJournalMode(), JournalModeWAL)
	}

	walFile := dbFile + "-wal"
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Error("WAL file was not created by enableWALMode")
	}

	shmFile := dbFile + "-shm"
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Error("WAL index file was not created by enableWALMode")
	}

	if p.wal == nil {
		t.Error("p.wal is nil after enableWALMode")
	}
	if p.walIndex == nil {
		t.Error("p.walIndex is nil after enableWALMode")
	}
}

// TestEnableWALMode_ReadOnlyRejected exercises the read-only guard in enableWALMode.
func TestEnableWALMode_ReadOnlyRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro.db")

	// Create the database file first.
	rw := openTestPagerAt(t, dbFile, false)
	rw.Close()

	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	if err := ro.SetJournalMode(JournalModeWAL); err == nil {
		t.Error("SetJournalMode(WAL) on read-only pager should return error")
	}
}

// TestEnableWALMode_WriteInWALMode exercises enableWALMode end-to-end by writing
// and reading a page after switching, confirming WAL I/O is live.
func TestEnableWALMode_WriteInWALMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm2.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	testData := []byte("wal mode enabled")
	pgno := walAllocWriteCommit(t, p, testData)

	page := mustGetPage(t, p, pgno)
	defer p.Put(page)
	if string(page.Data[:len(testData)]) != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", page.Data[:len(testData)], testData)
	}
}

// TestCopyDatabaseToTarget_ViaVacuum exercises copyDatabaseToTarget by running
// a VACUUM on a populated database and verifying data integrity afterwards.
func TestCopyDatabaseToTarget_ViaVacuum(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "cdtt.db")

	p := openTestPagerAt(t, dbFile, false)

	// Write several pages with recognisable data.
	for i := Pgno(2); i <= 6; i++ {
		mustWritePageAtOffset(t, p, i, 0, []byte{byte(i * 3)})
	}
	mustCommit(t, p)

	if err := p.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum(): %v", err)
	}

	// Verify data survived the copy.
	for i := Pgno(2); i <= 6; i++ {
		data := mustReadPageAtOffset(t, p, i, 0, 1)
		if data[0] != byte(i*3) {
			t.Errorf("page %d after vacuum: got %d, want %d", i, data[0], byte(i*3))
		}
	}

	p.Close()
}

// TestCopyDatabaseToTarget_IntoNewFile exercises copyDatabaseToTarget via VACUUM INTO,
// confirming that a separate target file receives all live pages.
func TestCopyDatabaseToTarget_IntoNewFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "dst.db")

	p := openTestPagerAt(t, srcFile, false)

	for i := Pgno(2); i <= 4; i++ {
		mustWritePageAtOffset(t, p, i, 0, []byte{byte(i + 10)})
	}
	mustCommit(t, p)

	opts := &VacuumOptions{IntoFile: dstFile}
	if err := p.Vacuum(opts); err != nil {
		t.Fatalf("Vacuum(Into): %v", err)
	}
	p.Close()

	if _, err := os.Stat(dstFile); os.IsNotExist(err) {
		t.Fatal("VACUUM INTO target file was not created")
	}

	dst := openTestPagerAt(t, dstFile, false)
	defer dst.Close()

	for i := Pgno(2); i <= 4; i++ {
		data := mustReadPageAtOffset(t, dst, i, 0, 1)
		if data[0] != byte(i+10) {
			t.Errorf("dst page %d: got %d, want %d", i, data[0], byte(i+10))
		}
	}
}

// TestWALIndexOpen_CreatesAndInitialises exercises the open() method of WALIndex
// by creating a new WAL index through NewWALIndex and checking initial state.
func TestWALIndexOpen_CreatesAndInitialises(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex(): %v", err)
	}
	defer idx.Close()

	if !idx.IsInitialized() {
		t.Error("WALIndex should be initialized after open()")
	}
	if idx.header == nil {
		t.Fatal("header should be non-nil after open()")
	}

	shmFile := filename + "-shm"
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Error("shm file should exist after open()")
	}
}

// TestWALIndexOpen_ViaSetJournalMode exercises open() indirectly through
// enableWALMode, which calls NewWALIndex internally.
func TestWALIndexOpen_ViaSetJournalMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wio.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)

	shmFile := dbFile + "-shm"
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Error("shm (WAL index) file should exist after SetJournalMode(WAL)")
	}

	if p.walIndex == nil {
		t.Error("walIndex should be non-nil after WAL mode is enabled")
	}
	if !p.walIndex.IsInitialized() {
		t.Error("walIndex should be initialized")
	}
}

// TestWALIndexOpen_ReopenExistingFile exercises the path in open() where the
// file already exists and is large enough, skipping initializeFile.
func TestWALIndexOpen_ReopenExistingFile(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx1, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() first open: %v", err)
	}
	mustInsertFrame(t, idx1, 1, 42)
	mustCloseWALIndex(t, idx1)

	// Reopen - open() should detect existing size and skip initializeFile.
	idx2, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() second open: %v", err)
	}
	defer idx2.Close()

	if !idx2.IsInitialized() {
		t.Error("reopened WALIndex should be initialized")
	}
}
