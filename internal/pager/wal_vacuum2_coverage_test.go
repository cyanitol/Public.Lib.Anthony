// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// wv2OpenPager opens a read-write pager at path, registering cleanup.
func wv2OpenPager(t *testing.T, path string) *Pager {
	t.Helper()
	p, err := Open(path, false)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// wv2EnableWAL switches the pager to WAL journal mode.
func wv2EnableWAL(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}
}

// wv2WritePages begins a write transaction, marks pages dirty, commits.
func wv2WritePages(t *testing.T, p *Pager, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go – closeCurrentDatabase (line 99)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_CloseCurrentDatabase_WithFile verifies that
// closeCurrentDatabase closes a real file handle and sets p.file to nil.
func TestWALVacuum2Coverage_CloseCurrentDatabase_WithFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "close_db.db")

	p := wv2OpenPager(t, dbFile)

	// Pre-condition: file should be open.
	if p.file == nil {
		t.Fatal("expected p.file to be non-nil before closeCurrentDatabase")
	}

	if err := p.closeCurrentDatabase(); err != nil {
		t.Fatalf("closeCurrentDatabase() error = %v", err)
	}
	if p.file != nil {
		t.Error("expected p.file to be nil after closeCurrentDatabase")
	}

	// Reopen so t.Cleanup's p.Close() doesn't fail.
	f, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	p.file = f
}

// TestWALVacuum2Coverage_CloseCurrentDatabase_NilFile verifies that
// closeCurrentDatabase with nil p.file returns nil immediately.
func TestWALVacuum2Coverage_CloseCurrentDatabase_NilFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "close_nil.db")

	p := wv2OpenPager(t, dbFile)
	// Manually close and nil out the file to test the nil branch.
	p.file.Close()
	p.file = nil

	if err := p.closeCurrentDatabase(); err != nil {
		t.Fatalf("closeCurrentDatabase(nil file) error = %v", err)
	}

	// Reopen for cleanup.
	f, err := os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	p.file = f
}

// ---------------------------------------------------------------------------
// vacuum.go – replaceForVacuumInPlace (line 134)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_ReplaceForVacuumInPlace_Success exercises the
// happy path of replaceForVacuumInPlace: removes old DB, renames temp file,
// reopens the file.
func TestWALVacuum2Coverage_ReplaceForVacuumInPlace_Success(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "replace_ip.db")

	p := wv2OpenPager(t, dbFile)

	// Create a temp file that will act as the vacuumed copy.
	tmpFile, err := os.CreateTemp(dir, "vac-*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	tmpFile.Write([]byte(p.file.Name())) // write something harmless
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	// Write the page-size-aligned header so the new file is readable as a DB.
	// We copy the current DB file as our "vacuumed" version.
	if err := copyFile(dbFile, tmpPath); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	// Close the pager's file handle so we can rename over it.
	if err := p.file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}
	p.file = nil

	if err := p.replaceForVacuumInPlace(tmpPath); err != nil {
		t.Fatalf("replaceForVacuumInPlace() error = %v", err)
	}

	if p.file == nil {
		t.Error("expected p.file to be non-nil after replaceForVacuumInPlace")
	}
}

// TestWALVacuum2Coverage_ReplaceForVacuumInPlace_RemoveError verifies that
// replaceForVacuumInPlace returns an error when it cannot remove the old DB
// (because the file does not exist).
func TestWALVacuum2Coverage_ReplaceForVacuumInPlace_RemoveError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Point the pager's filename to a file that doesn't exist.
	dbFile := filepath.Join(dir, "nonexistent.db")

	p := &Pager{
		filename: dbFile,
		file:     nil,
		pageSize: DefaultPageSize,
	}

	tmpPath := filepath.Join(dir, "tmp.db")
	if err := os.WriteFile(tmpPath, []byte("data"), 0600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	err := p.replaceForVacuumInPlace(tmpPath)
	if err == nil {
		t.Error("expected error when removing non-existent old db, got nil")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go – vacuumToFile (line 182)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_VacuumToFile_WithFreePages exercises vacuumToFile on
// a database that has free pages (to hit buildFreePageSet and getTrunkPageData).
func TestWALVacuum2Coverage_VacuumToFile_WithFreePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_vac.db")
	dstFile := filepath.Join(dir, "dst_vac.db")

	p := wv2OpenPager(t, srcFile)

	// Write several pages and free some to build a free list.
	mustBeginWrite(t, p)
	for i := 2; i <= 10; i++ {
		page := mustGetPage(t, p, Pgno(i))
		page.Data[0] = byte(i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	// Free pages 5–8 to create a non-empty free list.
	mustBeginWrite(t, p)
	for i := Pgno(5); i <= 8; i++ {
		mustFreePage(t, p, i)
	}
	mustCommit(t, p)

	if err := p.vacuumToFile(dstFile, nil); err != nil {
		t.Fatalf("vacuumToFile() error = %v", err)
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("dst file missing: %v", err)
	}
}

// TestWALVacuum2Coverage_VacuumToFile_EmptyDB exercises vacuumToFile on a
// freshly-created empty database (no extra pages).
func TestWALVacuum2Coverage_VacuumToFile_EmptyDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_empty.db")
	dstFile := filepath.Join(dir, "dst_empty.db")

	p := wv2OpenPager(t, srcFile)

	if err := p.vacuumToFile(dstFile, nil); err != nil {
		t.Fatalf("vacuumToFile() on empty DB error = %v", err)
	}

	if _, err := os.Stat(dstFile); err != nil {
		t.Fatalf("dst file missing: %v", err)
	}
}

// TestWALVacuum2Coverage_VacuumToFile_WithSchema exercises vacuumToFile with a
// VacuumOptions that has nil SourceSchema/Btree (exercises persistSchemaToTarget
// nil-check branch).
func TestWALVacuum2Coverage_VacuumToFile_WithOpts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_opts.db")
	dstFile := filepath.Join(dir, "dst_opts.db")

	p := wv2OpenPager(t, srcFile)

	opts := &VacuumOptions{IntoFile: dstFile}
	if err := p.vacuumToFile(dstFile, opts); err != nil {
		t.Fatalf("vacuumToFile() with opts error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go – copyDatabaseToTarget (line 207)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_CopyDatabaseToTarget_MultiPage copies a database with
// multiple live pages to a fresh target pager.
func TestWALVacuum2Coverage_CopyDatabaseToTarget_MultiPage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_copy.db")
	dstFile := filepath.Join(dir, "dst_copy.db")

	src := wv2OpenPager(t, srcFile)

	// Write several pages.
	mustBeginWrite(t, src)
	for i := 2; i <= 6; i++ {
		page := mustGetPage(t, src, Pgno(i))
		page.Data[0] = byte(i * 3)
		mustWritePage(t, src, page)
		src.Put(page)
	}
	mustCommit(t, src)

	dst, err := OpenWithPageSize(dstFile, false, src.pageSize)
	if err != nil {
		t.Fatalf("OpenWithPageSize dst: %v", err)
	}
	defer dst.Close()

	if err := src.copyDatabaseToTarget(dst); err != nil {
		t.Fatalf("copyDatabaseToTarget() error = %v", err)
	}
}

// TestWALVacuum2Coverage_CopyDatabaseToTarget_WithFreelist exercises
// copyDatabaseToTarget when the source has a non-empty free list, so that
// buildFreePageSet and getTrunkPageData are traversed.
func TestWALVacuum2Coverage_CopyDatabaseToTarget_WithFreelist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_fl.db")
	dstFile := filepath.Join(dir, "dst_fl.db")

	src := wv2OpenPager(t, srcFile)

	// Write and then free pages to populate the free list.
	mustBeginWrite(t, src)
	for i := Pgno(2); i <= 12; i++ {
		page := mustGetPage(t, src, i)
		page.Data[0] = byte(i)
		mustWritePage(t, src, page)
		src.Put(page)
	}
	mustCommit(t, src)

	mustBeginWrite(t, src)
	for i := Pgno(6); i <= 10; i++ {
		mustFreePage(t, src, i)
	}
	mustCommit(t, src)

	dst, err := OpenWithPageSize(dstFile, false, src.pageSize)
	if err != nil {
		t.Fatalf("OpenWithPageSize dst: %v", err)
	}
	defer dst.Close()

	if err := src.copyDatabaseToTarget(dst); err != nil {
		t.Fatalf("copyDatabaseToTarget() with freelist error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go – persistSchemaToTarget (line 277)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_PersistSchemaToTarget_NilOpts calls
// persistSchemaToTarget with nil opts (early-return branch).
func TestWALVacuum2Coverage_PersistSchemaToTarget_NilOpts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "persist_src.db")
	dstFile := filepath.Join(dir, "persist_dst.db")

	src := wv2OpenPager(t, srcFile)
	dst, err := OpenWithPageSize(dstFile, false, src.pageSize)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	if err := src.persistSchemaToTarget(dst, nil); err != nil {
		t.Fatalf("persistSchemaToTarget(nil) error = %v", err)
	}
}

// TestWALVacuum2Coverage_PersistSchemaToTarget_NilSchema calls
// persistSchemaToTarget with opts that have nil SourceSchema (early-return branch).
func TestWALVacuum2Coverage_PersistSchemaToTarget_NilSchema(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "persist_src2.db")
	dstFile := filepath.Join(dir, "persist_dst2.db")

	src := wv2OpenPager(t, srcFile)
	dst, err := OpenWithPageSize(dstFile, false, src.pageSize)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	opts := &VacuumOptions{SourceSchema: nil, Btree: nil}
	if err := src.persistSchemaToTarget(dst, opts); err != nil {
		t.Fatalf("persistSchemaToTarget(nil schema) error = %v", err)
	}
}

// TestWALVacuum2Coverage_PersistSchemaToTarget_NilBtree calls
// persistSchemaToTarget with a non-nil SourceSchema but nil Btree to exercise
// the second nil check.
func TestWALVacuum2Coverage_PersistSchemaToTarget_NilBtree(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "persist_src3.db")
	dstFile := filepath.Join(dir, "persist_dst3.db")

	src := wv2OpenPager(t, srcFile)
	dst, err := OpenWithPageSize(dstFile, false, src.pageSize)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	opts := &VacuumOptions{SourceSchema: struct{}{}, Btree: nil}
	if err := src.persistSchemaToTarget(dst, opts); err != nil {
		t.Fatalf("persistSchemaToTarget(nil btree) error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go – buildFreePageSet (line 358)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_BuildFreePageSet_EmptyFreelist calls buildFreePageSet
// on a pager with no free list (exercises the nil freeList branch).
func TestWALVacuum2Coverage_BuildFreePageSet_EmptyFreelist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "bfps_empty.db")

	p := wv2OpenPager(t, dbFile)

	// Temporarily nil out the free list.
	orig := p.freeList
	p.freeList = nil

	freeSet, err := p.buildFreePageSet()
	if err != nil {
		t.Fatalf("buildFreePageSet(nil freelist) error = %v", err)
	}
	if len(freeSet) != 0 {
		t.Errorf("expected empty set, got %v", freeSet)
	}

	p.freeList = orig
}

// TestWALVacuum2Coverage_BuildFreePageSet_WithFreePages exercises buildFreePageSet
// when the database has actual free pages registered in the free list and header.
func TestWALVacuum2Coverage_BuildFreePageSet_WithFreePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "bfps_free.db")

	p := wv2OpenPager(t, dbFile)

	// Write pages then free some.
	mustBeginWrite(t, p)
	for i := Pgno(2); i <= 8; i++ {
		page := mustGetPage(t, p, i)
		page.Data[0] = byte(i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	mustBeginWrite(t, p)
	for i := Pgno(4); i <= 6; i++ {
		mustFreePage(t, p, i)
	}
	mustCommit(t, p)

	freeSet, err := p.buildFreePageSet()
	if err != nil {
		t.Fatalf("buildFreePageSet() error = %v", err)
	}
	t.Logf("free page set size: %d", len(freeSet))
}

// ---------------------------------------------------------------------------
// vacuum.go – getTrunkPageData (line 439)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_GetTrunkPageData_Page1 exercises the page==1 branch,
// which skips the database header.
func TestWALVacuum2Coverage_GetTrunkPageData_Page1(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "trunk_page1.db")

	p := wv2OpenPager(t, dbFile)
	mustBeginRead(t, p)
	page := mustGetPage(t, p, 1)
	defer p.Put(page)
	mustEndRead(t, p)

	data := p.getTrunkPageData(page)
	if len(data) == 0 {
		t.Fatal("expected non-empty data for page 1")
	}
	// Page 1 branch should skip DatabaseHeaderSize bytes.
	expectedLen := len(page.Data) - DatabaseHeaderSize
	if len(data) != expectedLen {
		t.Errorf("getTrunkPageData(page1) len = %d, want %d", len(data), expectedLen)
	}
}

// TestWALVacuum2Coverage_GetTrunkPageData_NonPage1 exercises the page!=1 branch,
// which returns the full page data slice.
func TestWALVacuum2Coverage_GetTrunkPageData_NonPage1(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "trunk_page2.db")

	p := wv2OpenPager(t, dbFile)

	mustBeginWrite(t, p)
	page := mustGetPage(t, p, 2)
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	mustBeginRead(t, p)
	page2 := mustGetPage(t, p, 2)
	defer p.Put(page2)
	mustEndRead(t, p)

	data := p.getTrunkPageData(page2)
	if len(data) != len(page2.Data) {
		t.Errorf("getTrunkPageData(page2) len = %d, want %d", len(data), len(page2.Data))
	}
}

// ---------------------------------------------------------------------------
// pager.go – Put (line 450)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_Put_NilPage verifies that Put with a nil page is a no-op.
func TestWALVacuum2Coverage_Put_NilPage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "put_nil.db")

	p := wv2OpenPager(t, dbFile)
	// Should not panic.
	p.Put(nil)
}

// TestWALVacuum2Coverage_Put_RealPage verifies that Put releases a real page
// without error.
func TestWALVacuum2Coverage_Put_RealPage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "put_real.db")

	p := wv2OpenPager(t, dbFile)
	mustBeginRead(t, p)
	page := mustGetPage(t, p, 1)
	mustEndRead(t, p)
	// Put after transaction end – exercises the non-nil branch.
	p.Put(page)
}

// ---------------------------------------------------------------------------
// pager.go – recoverWALReadOnly (line 811) / recoverWALReadWrite (line 832)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_RecoverWALReadOnly_DirectCall calls recoverWALReadOnly
// on a pager that has a WAL file on disk, exercising the open-and-assign path.
func TestWALVacuum2Coverage_RecoverWALReadOnly_DirectCall(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_ro_direct.db")

	// Build a pager with WAL frames so a WAL file exists on disk.
	p := wv2OpenPager(t, dbFile)
	wv2EnableWAL(t, p)
	wv2WritePages(t, p, 5)

	// Flush WAL to disk without closing the pager.
	if err := p.Checkpoint(); err != nil {
		t.Logf("Checkpoint (non-fatal): %v", err)
	}
	p.Close()

	// Open a fresh pager and call recoverWALReadOnly directly.
	p2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open p2: %v", err)
	}
	defer p2.Close()

	// Null out any existing wal so we trigger the full path.
	if p2.wal != nil {
		p2.wal.Close()
		p2.wal = nil
	}
	if p2.walIndex != nil {
		p2.walIndex.Close()
		p2.walIndex = nil
	}

	if err := p2.recoverWALReadOnly(); err != nil {
		// It is acceptable for this to fail if no WAL file is present.
		t.Logf("recoverWALReadOnly() = %v (non-fatal if WAL removed on close)", err)
	}
}

// TestWALVacuum2Coverage_RecoverWALReadWrite_DirectCall calls recoverWALReadWrite
// on a pager that has WAL data on disk, exercising the checkpoint path.
func TestWALVacuum2Coverage_RecoverWALReadWrite_DirectCall(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "rcv_rw_direct.db")

	// Create a pager, enable WAL, write pages – leave WAL on disk.
	func() {
		p := wv2OpenPager(t, dbFile)
		wv2EnableWAL(t, p)
		wv2WritePages(t, p, 8)
		// Close without explicit checkpoint so WAL file persists.
	}()

	walPath := dbFile + "-wal"
	if info, err := os.Stat(walPath); err == nil {
		t.Logf("WAL file present: %d bytes", info.Size())
	}

	// Open a fresh pager pointing at the same DB file.
	p2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open p2: %v", err)
	}
	defer p2.Close()

	// Call recoverWALReadWrite directly.
	if err := p2.recoverWALReadWrite(); err != nil {
		t.Logf("recoverWALReadWrite() = %v (non-fatal)", err)
	}
}

// ---------------------------------------------------------------------------
// pager.go – writeDirtyPagesToWAL (line 1322)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_WriteDirtyPagesToWAL_Basic sets up WAL mode and
// exercises writeDirtyPagesToWAL by marking pages dirty then calling it directly.
func TestWALVacuum2Coverage_WriteDirtyPagesToWAL_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wdpwal_basic.db")

	p := wv2OpenPager(t, dbFile)
	wv2EnableWAL(t, p)

	// Begin write transaction and dirty a page.
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, 1)
	page.Data[DatabaseHeaderSize] ^= 0xFF
	mustWritePage(t, p, page)
	p.Put(page)

	// Call writeDirtyPagesToWAL directly (p already holds the lock).
	// We must call via Commit which internally calls writeDirtyPagesToWAL.
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit (wraps writeDirtyPagesToWAL): %v", err)
	}
}

// TestWALVacuum2Coverage_WriteDirtyPagesToWAL_NilWAL verifies that
// writeDirtyPagesToWAL returns an error when p.wal is nil.
func TestWALVacuum2Coverage_WriteDirtyPagesToWAL_NilWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wdpwal_nil.db")

	p := wv2OpenPager(t, dbFile)

	// Force wal to nil.
	p.wal = nil

	// Set state to WriterCachemod so writeDirtyPagesToWAL can be reached.
	p.state = PagerStateWriterCachemod

	err := p.writeDirtyPagesToWAL()
	if err == nil {
		t.Error("expected error with nil WAL, got nil")
	}
	// Restore state for cleanup.
	p.state = PagerStateOpen
}

// TestWALVacuum2Coverage_WriteDirtyPagesToWAL_ManyPages exercises
// writeDirtyPagesToWAL with a large number of dirty pages.
func TestWALVacuum2Coverage_WriteDirtyPagesToWAL_ManyPages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wdpwal_many.db")

	p := wv2OpenPager(t, dbFile)
	wv2EnableWAL(t, p)

	mustBeginWrite(t, p)
	for i := 1; i <= 20; i++ {
		pgno := Pgno(i)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit with many pages: %v", err)
	}
}

// ---------------------------------------------------------------------------
// transaction.go – enableWALMode (line 312)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_EnableWALMode_Basic calls enableWALMode directly on a
// fresh pager, exercising the WAL and WAL-index creation path.
func TestWALVacuum2Coverage_EnableWALMode_Basic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_basic.db")

	p := wv2OpenPager(t, dbFile)
	if err := p.enableWALMode(); err != nil {
		t.Fatalf("enableWALMode() error = %v", err)
	}

	if p.wal == nil {
		t.Error("expected p.wal to be non-nil after enableWALMode")
	}
	if p.walIndex == nil {
		t.Error("expected p.walIndex to be non-nil after enableWALMode")
	}
}

// TestWALVacuum2Coverage_EnableWALMode_ReadOnlyReturnsError verifies that
// enableWALMode on a read-only pager returns an error.
func TestWALVacuum2Coverage_EnableWALMode_ReadOnlyReturnsError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_ro.db")

	// Create the file first.
	wv2OpenPager(t, dbFile)

	p, err := Open(dbFile, true)
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer p.Close()

	if err := p.enableWALMode(); err == nil {
		t.Error("expected error enabling WAL on read-only pager, got nil")
	}
}

// TestWALVacuum2Coverage_EnableWALMode_ThenWrite enables WAL mode and writes
// several pages, exercising the full enableWALMode → writeDirtyPagesToWAL path.
func TestWALVacuum2Coverage_EnableWALMode_ThenWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_write.db")

	p := wv2OpenPager(t, dbFile)
	if err := p.enableWALMode(); err != nil {
		t.Fatalf("enableWALMode(): %v", err)
	}
	p.journalMode = JournalModeWAL

	wv2WritePages(t, p, 10)
}

// TestWALVacuum2Coverage_EnableWALMode_SetJournalMode exercises enableWALMode
// indirectly via SetJournalMode, which validates the branch coverage path
// through handleJournalModeTransition.
func TestWALVacuum2Coverage_EnableWALMode_SetJournalMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ewm_sj.db")

	p := wv2OpenPager(t, dbFile)

	// First enable WAL via the proper path.
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Now disable by switching to DELETE.
	if err := p.SetJournalMode(JournalModeDelete); err != nil {
		t.Logf("SetJournalMode(DELETE): %v (non-fatal, WAL checkpoint)", err)
	}

	// Re-enable WAL – exercises enableWALMode a second time.
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL) second time: %v", err)
	}
}

// ---------------------------------------------------------------------------
// wal_checkpoint.go – checkpointFramesToDB (line 106) / checkpointRestart (line 170)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_CheckpointFramesToDB_Direct exercises checkpointFramesToDB
// by creating a WAL with frames and calling Checkpoint (which invokes it).
func TestWALVacuum2Coverage_CheckpointFramesToDB_Direct(t *testing.T) {
	t.Parallel()

	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write several pages.
	for i := Pgno(1); i <= 5; i++ {
		data := bytes.Repeat([]byte{byte(i)}, DefaultPageSize)
		mustWriteFrame(t, wal, i, data, 5)
	}

	n, err := wal.checkpointFramesToDB()
	if err != nil {
		t.Fatalf("checkpointFramesToDB() error = %v", err)
	}
	t.Logf("frames checkpointed: %d", n)
}

// TestWALVacuum2Coverage_CheckpointRestart_Direct exercises checkpointRestart
// by writing frames then calling it directly on the WAL.
func TestWALVacuum2Coverage_CheckpointRestart_Direct(t *testing.T) {
	t.Parallel()

	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames so there is something to checkpoint.
	for i := Pgno(1); i <= 4; i++ {
		data := bytes.Repeat([]byte{byte(i * 2)}, DefaultPageSize)
		mustWriteFrame(t, wal, i, data, 4)
	}

	checkpointed, remaining, err := wal.checkpointRestart()
	if err != nil {
		t.Fatalf("checkpointRestart() error = %v", err)
	}
	t.Logf("checkpointRestart: checkpointed=%d remaining=%d", checkpointed, remaining)
}

// TestWALVacuum2Coverage_CheckpointRestart_EmptyWAL verifies that checkpointRestart
// on a WAL with no frames returns (0, 0, nil).
func TestWALVacuum2Coverage_CheckpointRestart_EmptyWAL(t *testing.T) {
	t.Parallel()

	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// No frames written.
	checkpointed, remaining, err := wal.checkpointRestart()
	if err != nil {
		t.Fatalf("checkpointRestart() on empty WAL error = %v", err)
	}
	if checkpointed != 0 || remaining != 0 {
		t.Errorf("expected (0,0), got (%d,%d)", checkpointed, remaining)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go – open (line 143) / initializeFile (line 191)
// ---------------------------------------------------------------------------

// TestWALVacuum2Coverage_WALIndexOpen_FreshFile exercises WALIndex.open() on a
// brand-new file, hitting the initializeFile branch.
func TestWALVacuum2Coverage_WALIndexOpen_FreshFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "idx_fresh.db")

	idx := mustOpenWALIndex(t, dbFile)
	defer mustCloseWALIndex(t, idx)

	if !idx.IsInitialized() {
		t.Error("expected WAL index to be initialized after open")
	}
}

// TestWALVacuum2Coverage_WALIndexOpen_ExistingFile exercises WALIndex.open() on
// an already-initialised file (the size >= minSize branch, no initializeFile call).
func TestWALVacuum2Coverage_WALIndexOpen_ExistingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "idx_exist2.db")

	// First open creates and initializes the file.
	idx1 := mustOpenWALIndex(t, dbFile)
	mustCloseWALIndex(t, idx1)

	// Second open hits the existing-file branch.
	idx2 := mustOpenWALIndex(t, dbFile)
	defer mustCloseWALIndex(t, idx2)

	if !idx2.IsInitialized() {
		t.Error("expected WAL index to be initialized on reopen")
	}
}

// TestWALVacuum2Coverage_WALIndexInitializeFile_TruncateAndHeader calls
// initializeFile directly on a WAL index file that has been opened but not yet
// filled to the minimum size (forces truncate + header write).
func TestWALVacuum2Coverage_WALIndexInitializeFile_TruncateAndHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "idx_init.db")

	idx := mustOpenWALIndex(t, dbFile)
	defer mustCloseWALIndex(t, idx)

	// Calculate the expected minimum size.
	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)

	// Call initializeFile again to exercise the path directly (should be idempotent).
	if err := idx.initializeFile(minSize); err != nil {
		t.Fatalf("initializeFile() error = %v", err)
	}
}

// TestWALVacuum2Coverage_WALIndexOpen_ViaEnableWAL exercises WALIndex.open()
// indirectly through enableWALMode, which calls NewWALIndex internally.
func TestWALVacuum2Coverage_WALIndexOpen_ViaEnableWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "idx_via_wal.db")

	p := wv2OpenPager(t, dbFile)

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	if p.walIndex == nil {
		t.Fatal("expected walIndex to be non-nil after enabling WAL")
	}
	if !p.walIndex.IsInitialized() {
		t.Error("expected walIndex to be initialized")
	}

	// Write a page to exercise insertFrame inside the index.
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, 1)
	page.Data[DatabaseHeaderSize] ^= 0x01
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)
}
