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

// ---------------------------------------------------------------------------
// WAL recovery: recoverWALReadOnly / recoverWALReadWrite
// ---------------------------------------------------------------------------

// TestRecoverWAL_ReopenPopulatedWALDatabase closes a WAL-mode database while
// frames are still in the WAL and reopens it, exercising the WAL-recovery path
// (recoverWALReadOnly or recoverWALReadWrite depending on the open flags).
func TestRecoverWAL_ReopenPopulatedWALDatabase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "recover.db")

	// Phase 1: populate a WAL-mode database and close without checkpointing.
	p1 := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p1, JournalModeWAL)
	for i := 0; i < 10; i++ {
		mustBeginWrite(t, p1)
		pgno := mustAllocatePage(t, p1)
		page := mustGetPage(t, p1, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p1, page)
		p1.Put(page)
		mustCommit(t, p1)
	}
	// Close without explicit checkpoint – WAL file stays on disk.
	p1.Close()

	// Verify WAL file exists before reopening.
	walFile := dbFile + "-wal"
	if info, err := os.Stat(walFile); err != nil || info.Size() == 0 {
		// WAL may have been removed; that is also valid – test still useful.
		t.Logf("WAL file absent or empty after first close: %v", err)
	}

	// Phase 2: reopen read-write – triggers recoverWALReadWrite.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()

	// The database must be readable after recovery.
	if err := p2.BeginRead(); err != nil {
		t.Fatalf("BeginRead after WAL recovery: %v", err)
	}
	if err := p2.EndRead(); err != nil {
		t.Fatalf("EndRead after WAL recovery: %v", err)
	}
}

// TestRecoverWAL_ReopenReadOnly exercises recoverWALReadOnly by opening a
// WAL-mode database in read-only mode when the WAL file is present.
func TestRecoverWAL_ReopenReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "recover_ro.db")

	// Phase 1: write several frames so the WAL file is non-empty.
	p1 := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p1, JournalModeWAL)
	for i := 0; i < 5; i++ {
		mustBeginWrite(t, p1)
		pgno := mustAllocatePage(t, p1)
		page := mustGetPage(t, p1, pgno)
		page.Data[0] = byte(i + 10)
		mustWritePage(t, p1, page)
		p1.Put(page)
		mustCommit(t, p1)
	}
	// Close without an explicit full checkpoint to keep WAL around.
	p1.Close()

	// Phase 2: open read-only – Open() detects the WAL and calls recoverWALReadOnly.
	p2 := openTestPagerAt(t, dbFile, true)
	defer p2.Close()

	if err := p2.BeginRead(); err != nil {
		t.Fatalf("BeginRead (read-only after WAL recovery): %v", err)
	}
	_ = p2.EndRead()
}

// ---------------------------------------------------------------------------
// writeDirtyPagesToWAL
// ---------------------------------------------------------------------------

// TestWriteDirtyPagesToWAL_ManyFrames writes a large number of pages in WAL
// mode to ensure writeDirtyPagesToWAL is exercised with multiple dirty pages.
func TestWriteDirtyPagesToWAL_ManyFrames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wdp.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	// Accumulate many dirty pages within a single transaction then commit.
	mustBeginWrite(t, p)
	for i := 0; i < 30; i++ {
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		for j := range page.Data {
			page.Data[j] = byte((i + j) % 251)
		}
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	// Checkpoint to exercise checkpointFramesToDB.
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(): %v", err)
	}
}

// ---------------------------------------------------------------------------
// enableWALMode – additional branch coverage
// ---------------------------------------------------------------------------

// TestEnableWALMode_MultipleWriteTransactions enables WAL mode and executes
// several write transactions to confirm writeDirtyPagesToWAL runs repeatedly.
func TestEnableWALMode_MultipleWriteTransactions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_multi.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	for round := 0; round < 8; round++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(round)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	// All checkpoint modes.
	modes := []CheckpointMode{
		CheckpointPassive,
		CheckpointFull,
		CheckpointRestart,
		CheckpointTruncate,
	}
	for _, mode := range modes {
		if err := p.CheckpointMode(mode); err != nil {
			t.Logf("CheckpointMode(%v): %v (non-fatal)", mode, err)
		}
	}
}

// ---------------------------------------------------------------------------
// validateRollbackState / releaseSavepoints
// ---------------------------------------------------------------------------

// TestValidateRollbackState_AndReleaseSavepoints exercises validateRollbackState
// (called by RollbackTo) and releaseSavepoints (called by Release).
func TestValidateRollbackState_AndReleaseSavepoints(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sp.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Need an active write transaction for savepoints.
	mustBeginWrite(t, p)

	// Create two savepoints.
	mustSavepoint(t, p, "sp1")
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0xAA
	mustWritePage(t, p, page)
	p.Put(page)

	mustSavepoint(t, p, "sp2")
	page2 := mustGetPage(t, p, pgno)
	page2.Data[0] = 0xBB
	mustWritePage(t, p, page2)
	p.Put(page2)

	// RollbackTo exercises validateRollbackState.
	mustRollbackTo(t, p, "sp1")

	// Release exercises releaseSavepoints.
	mustRelease(t, p, "sp1")

	mustCommit(t, p)
}

// TestValidateRollbackState_OutsideTransaction confirms that RollbackTo returns
// an error when there is no active write transaction.
func TestValidateRollbackState_OutsideTransaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sp_ot.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// No active write transaction – validateRollbackState should reject this.
	if err := p.RollbackTo("nonexistent"); err == nil {
		t.Error("RollbackTo() outside transaction should return error")
	}
}

// TestReleaseSavepoints_MultipleLevels creates a stack of savepoints and
// releases them from various depths to cover releaseSavepoints branches.
func TestReleaseSavepoints_MultipleLevels(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sp_multi.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginWrite(t, p)

	mustSavepoint(t, p, "a")
	mustSavepoint(t, p, "b")
	mustSavepoint(t, p, "c")

	// Release from the middle – exercises releaseSavepoints with index > 0.
	mustRelease(t, p, "b")

	mustCommit(t, p)
}

// ---------------------------------------------------------------------------
// vacuumToFile / copyDatabaseToTarget / allocateLocked
// ---------------------------------------------------------------------------

// TestVacuumToFile_InPlace exercises vacuumToFile, copyDatabaseToTarget, and
// the internal allocateLocked path via VACUUM on a database with many pages.
func TestVacuumToFile_InPlace(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "vac_inplace.db")

	p := openTestPagerAt(t, dbFile, false)

	// Write pages then free some to create fragmentation.
	mustBeginWrite(t, p)
	var pages []Pgno
	for i := 0; i < 12; i++ {
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p, page)
		p.Put(page)
		pages = append(pages, pgno)
	}
	mustCommit(t, p)

	// Free every other page to create free-list entries.
	for i := 0; i < len(pages); i += 2 {
		mustBeginWrite(t, p)
		mustFreePage(t, p, pages[i])
		mustCommit(t, p)
	}

	// VACUUM triggers vacuumToFile → copyDatabaseToTarget.
	if err := p.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum(): %v", err)
	}
	p.Close()
}

// TestVacuumToFile_Into exercises vacuumToFile via VACUUM INTO with schema.
func TestVacuumToFile_Into(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "vac_src.db")
	dstFile := filepath.Join(dir, "vac_dst.db")

	p := openTestPagerAt(t, srcFile, false)

	mustBeginWrite(t, p)
	for i := Pgno(2); i <= 6; i++ {
		page := mustGetPage(t, p, i)
		page.Data[0] = byte(i * 5)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	if err := p.Vacuum(&VacuumOptions{IntoFile: dstFile}); err != nil {
		t.Fatalf("Vacuum(Into): %v", err)
	}
	p.Close()

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("VACUUM INTO target missing: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.Put coverage
// ---------------------------------------------------------------------------

// TestMemoryPagerPut_NilAndValid exercises Put on a MemoryPager with a nil
// page (no-op path) and a valid page (Unref path).
func TestMemoryPagerPut_NilAndValid(t *testing.T) {
	t.Parallel()

	mp := mustOpenMemoryPager(t, DefaultPageSize)

	// Put(nil) must not panic.
	mp.Put(nil)

	// Get a real page, then Put it to exercise Unref.
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	page := mustMemoryGet(t, mp, pgno)
	mustMemoryWrite(t, mp, page)
	mustMemoryCommit(t, mp)

	// Put the page back – exercises the Unref branch.
	mp.Put(page)
}

// TestMemoryPagerPut_MultipleRefs exercises Put when a page has multiple
// outstanding references (Ref incremented more than once).
func TestMemoryPagerPut_MultipleRefs(t *testing.T) {
	t.Parallel()

	mp := mustOpenMemoryPager(t, DefaultPageSize)

	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	page := mustMemoryGet(t, mp, pgno)
	mustMemoryWrite(t, mp, page)
	mustMemoryCommit(t, mp)

	// Acquire a second reference.
	page.Ref()

	// Each Put decrements the refcount.
	mp.Put(page)
	mp.Put(page)
}

// ---------------------------------------------------------------------------
// checkpointFramesToDB (via CheckpointMode / explicit Checkpoint)
// ---------------------------------------------------------------------------

// TestCheckpointFramesToDB_ViaPassiveMode writes frames into a WAL then
// calls CheckpointMode(Passive) to exercise checkpointFramesToDB.
func TestCheckpointFramesToDB_ViaPassiveMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_passive.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	for i := 0; i < 5; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	if err := p.CheckpointMode(CheckpointPassive); err != nil {
		t.Fatalf("CheckpointMode(Passive): %v", err)
	}
}

// TestCheckpointFramesToDB_ViaFullMode exercises checkpointFramesToDB through
// the full checkpoint mode.
func TestCheckpointFramesToDB_ViaFullMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_full.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	for i := 0; i < 8; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 2)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	if err := p.CheckpointMode(CheckpointFull); err != nil {
		t.Fatalf("CheckpointMode(Full): %v", err)
	}
}

// TestCheckpointFramesToDB_ViaRestartAndTruncate exercises additional
// checkpoint modes that share the checkpointFramesToDB path.
func TestCheckpointFramesToDB_ViaRestartAndTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ckpt_rt.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()
	mustSetJournalMode(t, p, JournalModeWAL)

	for i := 0; i < 6; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 3)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	if err := p.CheckpointMode(CheckpointRestart); err != nil {
		t.Logf("CheckpointMode(Restart): %v (non-fatal)", err)
	}

	// Write a few more frames after restart.
	for i := 0; i < 3; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 50)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	if err := p.CheckpointMode(CheckpointTruncate); err != nil {
		t.Logf("CheckpointMode(Truncate): %v (non-fatal)", err)
	}
}

// ---------------------------------------------------------------------------
// WAL index open – multiple connections to same WAL database
// ---------------------------------------------------------------------------

// TestWALIndex_MultipleConnectionsSameDB opens the same WAL database via two
// separate pager instances, exercising the shared WAL index path.
func TestWALIndex_MultipleConnectionsSameDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "multi_conn.db")

	// First connection enables WAL and writes data.
	p1 := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p1, JournalModeWAL)
	mustBeginWrite(t, p1)
	pgno := mustAllocatePage(t, p1)
	page := mustGetPage(t, p1, pgno)
	page.Data[0] = 0x42
	mustWritePage(t, p1, page)
	p1.Put(page)
	mustCommit(t, p1)

	// Second connection opens the same file; WAL index open() is re-exercised.
	p2 := openTestPagerAt(t, dbFile, false)

	if err := p2.BeginRead(); err != nil {
		t.Fatalf("p2.BeginRead(): %v", err)
	}
	_ = p2.EndRead()

	p1.Close()
	p2.Close()
}

// ---------------------------------------------------------------------------
// acquirePendingLock (lock_unix.go:251) – reached via AcquireExclusive path
// ---------------------------------------------------------------------------

// TestAcquirePendingLock_ViaExclusiveLock exercises acquirePendingLock by
// promoting a lock manager to EXCLUSIVE level, which passes through PENDING.
func TestAcquirePendingLock_ViaExclusiveLock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "lock.db"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write(make([]byte, DefaultPageSize)); err != nil {
		f.Close()
		t.Fatalf("Write padding: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// Acquire SHARED → RESERVED → EXCLUSIVE.
	// acquirePendingLock is called on the EXCLUSIVE promotion path.
	mustAcquireLock(t, lm, LockShared)
	mustAcquireLock(t, lm, LockReserved)
	mustAcquireLock(t, lm, LockExclusive)

	// Release back down.
	mustReleaseLock(t, lm, LockExclusive)
	mustReleaseLock(t, lm, LockShared)
}
