// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// openTestPager opens a new pager for testing, returning it and a cleanup function.
func openTestPager(t *testing.T) *Pager {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// openTestPagerAt opens a pager at the given path.
func openTestPagerAt(t *testing.T, dbFile string, readOnly bool) *Pager {
	t.Helper()
	p, err := Open(dbFile, readOnly)
	if err != nil {
		t.Fatalf("Open(%q, readOnly=%v) error = %v", dbFile, readOnly, err)
	}
	return p
}

// openTestPagerSized opens a pager with a specific page size.
func openTestPagerSized(t *testing.T, pageSize int) *Pager {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), "test.db")
	p, err := OpenWithPageSize(dbFile, false, pageSize)
	if err != nil {
		t.Fatalf("OpenWithPageSize(%d) error = %v", pageSize, err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// openTestPagerSizedAt opens a pager with a specific page size at the given path.
func openTestPagerSizedAt(t *testing.T, dbFile string, readOnly bool, pageSize int) *Pager {
	t.Helper()
	p, err := OpenWithPageSize(dbFile, readOnly, pageSize)
	if err != nil {
		t.Fatalf("OpenWithPageSize(%q, %d) error = %v", dbFile, pageSize, err)
	}
	return p
}

// mustBeginWrite starts a write transaction or fails.
func mustBeginWrite(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite() error = %v", err)
	}
}

// mustBeginRead starts a read transaction or fails.
func mustBeginRead(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.BeginRead(); err != nil {
		t.Fatalf("BeginRead() error = %v", err)
	}
}

// mustEndRead ends a read transaction or fails.
func mustEndRead(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.EndRead(); err != nil {
		t.Fatalf("EndRead() error = %v", err)
	}
}

// mustCommit commits the current transaction or fails.
func mustCommit(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

// mustRollback rolls back the current transaction or fails.
func mustRollback(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
}

// mustGetPage gets a page or fails.
func mustGetPage(t *testing.T, p *Pager, pgno Pgno) *DbPage {
	t.Helper()
	page, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d) error = %v", pgno, err)
	}
	return page
}

// mustWritePage marks a page dirty or fails.
func mustWritePage(t *testing.T, p *Pager, page *DbPage) {
	t.Helper()
	if err := p.Write(page); err != nil {
		t.Fatalf("Write(page %d) error = %v", page.Pgno, err)
	}
}

// mustAllocatePage allocates a new page or fails.
func mustAllocatePage(t *testing.T, p *Pager) Pgno {
	t.Helper()
	pgno, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage() error = %v", err)
	}
	return pgno
}

// mustFreePage frees a page or fails.
func mustFreePage(t *testing.T, p *Pager, pgno Pgno) {
	t.Helper()
	if err := p.FreePage(pgno); err != nil {
		t.Fatalf("FreePage(%d) error = %v", pgno, err)
	}
}

// mustGetWritePage gets a page and marks it writable, or fails.
func mustGetWritePage(t *testing.T, p *Pager, pgno Pgno) *DbPage {
	t.Helper()
	page := mustGetPage(t, p, pgno)
	mustWritePage(t, p, page)
	return page
}

// mustSetJournalMode sets the journal mode or fails.
func mustSetJournalMode(t *testing.T, p *Pager, mode int) {
	t.Helper()
	if err := p.SetJournalMode(mode); err != nil {
		t.Fatalf("SetJournalMode(%d) error = %v", mode, err)
	}
}

// mustSavepoint creates a savepoint or fails.
func mustSavepoint(t *testing.T, p *Pager, name string) {
	t.Helper()
	if err := p.Savepoint(name); err != nil {
		t.Fatalf("Savepoint(%q) error = %v", name, err)
	}
}

// mustRollbackTo rolls back to a savepoint or fails.
func mustRollbackTo(t *testing.T, p *Pager, name string) {
	t.Helper()
	if err := p.RollbackTo(name); err != nil {
		t.Fatalf("RollbackTo(%q) error = %v", name, err)
	}
}

// mustRelease releases a savepoint or fails.
func mustRelease(t *testing.T, p *Pager, name string) {
	t.Helper()
	if err := p.Release(name); err != nil {
		t.Fatalf("Release(%q) error = %v", name, err)
	}
}

// mustCheckpoint checkpoints a WAL-mode pager or fails.
func mustCheckpoint(t *testing.T, p *Pager) {
	t.Helper()
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
}

// mustWriteFrame writes a WAL frame or fails.
func mustWriteFrame(t *testing.T, wal *WAL, pgno Pgno, data []byte, dbSize uint32) {
	t.Helper()
	if err := wal.WriteFrame(pgno, data, dbSize); err != nil {
		t.Fatalf("WriteFrame(%d) error = %v", pgno, err)
	}
}

// mustOpenWAL opens a WAL or fails.
func mustOpenWAL(t *testing.T, dbFile string, pageSize int) *WAL {
	t.Helper()
	wal := NewWAL(dbFile, pageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("WAL.Open() error = %v", err)
	}
	return wal
}

// mustOpenWALIndex opens a WAL index or fails.
func mustOpenWALIndex(t *testing.T, filename string) *WALIndex {
	t.Helper()
	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	return idx
}

// mustOpenJournal opens a journal or fails.
func mustOpenJournal(t *testing.T, path string, pageSize int, dbSize Pgno) *Journal {
	t.Helper()
	j := NewJournal(path, pageSize, dbSize)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open() error = %v", err)
	}
	return j
}

// mustAcquireLock acquires a lock or fails.
func mustAcquireLock(t *testing.T, lm *LockManager, level LockLevel) {
	t.Helper()
	if err := lm.AcquireLock(level); err != nil {
		t.Fatalf("AcquireLock(%v) error = %v", level, err)
	}
}

// mustReleaseLock releases a lock or fails.
func mustReleaseLock(t *testing.T, lm *LockManager, level LockLevel) {
	t.Helper()
	if err := lm.ReleaseLock(level); err != nil {
		t.Fatalf("ReleaseLock(%v) error = %v", level, err)
	}
}

// mustNewLockManager creates a lock manager or fails.
func mustNewLockManager(t *testing.T, f *os.File) *LockManager {
	t.Helper()
	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	return lm
}

// writeTestPages writes 'count' pages with pattern data to pager.
func writeTestPages(t *testing.T, p *Pager, count int) []Pgno {
	t.Helper()
	pages := make([]Pgno, 0, count)
	for i := 0; i < count; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		pages = append(pages, pgno)
		page := mustGetPage(t, p, pgno)
		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte((i + j) % 256)
		}
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}
	return pages
}

// allocateTestPagesRange creates pages in range [start, end] and writes them.
func allocateTestPagesRange(t *testing.T, p *Pager, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		page := mustGetPage(t, p, i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
}

// freeTestPagesRange frees pages in range [start, end] using a freelist.
func freeTestPagesRange(t *testing.T, fl *FreeList, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("Free(%d) error = %v", i, err)
		}
	}
}

// mustFlush flushes a freelist or fails.
func mustFlush(t *testing.T, fl *FreeList) {
	t.Helper()
	if err := fl.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

// createTestDBFile creates an empty db file and returns its path.
func createTestDBFile(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return dbFile
}

// createTestDBFileWithSize creates a db file of given size and returns its path.
func createTestDBFileWithSize(t *testing.T, size int) string {
	t.Helper()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")
	if err := os.WriteFile(dbFile, make([]byte, size), 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return dbFile
}

// walWriteAndCommit writes data to a page using WAL mode pager helper.
func walWriteAndCommit(t *testing.T, p *Pager, pgno Pgno, data []byte) {
	t.Helper()
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, pgno)
	copy(page.Data, data)
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)
}

// walAllocWriteCommit allocates a page, writes version data, and commits.
func walAllocWriteCommit(t *testing.T, p *Pager, version []byte) Pgno {
	t.Helper()
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	copy(page.Data, version)
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)
	return pgno
}

// mustAllocatePages allocates n pages and returns their page numbers.
func mustAllocatePages(t *testing.T, p *Pager, n int) []Pgno {
	t.Helper()
	pages := make([]Pgno, n)
	for i := 0; i < n; i++ {
		pages[i] = mustAllocatePage(t, p)
	}
	return pages
}

// mustGetWritePageData gets a page, marks it writable, sets byte 0, and puts it.
func mustGetWritePageData(t *testing.T, p *Pager, pgno Pgno, value byte) {
	t.Helper()
	page := mustGetWritePage(t, p, pgno)
	page.Data[0] = value
	p.Put(page)
}

// mustOpenPagerSized opens a pager with a given page size at the given path for testing.
func mustOpenPagerSized(t *testing.T, dbFile string, pageSize int) *Pager {
	t.Helper()
	p, err := OpenWithPageSize(dbFile, false, pageSize)
	if err != nil {
		t.Fatalf("OpenWithPageSize(%q, %d) error = %v", dbFile, pageSize, err)
	}
	return p
}

// mustInsertFrame inserts a frame into a WAL index or fails.
func mustInsertFrame(t *testing.T, idx *WALIndex, pgno, frameNo uint32) {
	t.Helper()
	if err := idx.InsertFrame(pgno, frameNo); err != nil {
		t.Fatalf("InsertFrame(%d, %d) error = %v", pgno, frameNo, err)
	}
}

// mustSetReadMark sets a read mark in a WAL index or fails.
func mustSetReadMark(t *testing.T, idx *WALIndex, reader int, frame uint32) {
	t.Helper()
	if err := idx.SetReadMark(reader, frame); err != nil {
		t.Fatalf("SetReadMark(%d, %d) error = %v", reader, frame, err)
	}
}

// mustCloseWALIndex closes a WAL index or fails.
func mustCloseWALIndex(t *testing.T, idx *WALIndex) {
	t.Helper()
	if err := idx.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// freeTestPagesRangePager frees pages in range [start, end] using a pager.
func freeTestPagesRangePager(t *testing.T, p *Pager, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		mustFreePage(t, p, i)
	}
}

// mustWritePageAtOffset writes data to a page at a given offset.
func mustWritePageAtOffset(t *testing.T, p *Pager, pgno Pgno, offset int, data []byte) {
	t.Helper()
	page := mustGetWritePage(t, p, pgno)
	if err := page.Write(offset, data); err != nil {
		t.Fatalf("page.Write(page %d, offset %d) error = %v", pgno, offset, err)
	}
	p.Put(page)
}

// mustReadPageAtOffset reads data from a page at a given offset.
func mustReadPageAtOffset(t *testing.T, p *Pager, pgno Pgno, offset, length int) []byte {
	t.Helper()
	page := mustGetPage(t, p, pgno)
	defer p.Put(page)
	data, err := page.Read(offset, length)
	if err != nil {
		t.Fatalf("page.Read(page %d, offset %d) error = %v", pgno, offset, err)
	}
	return data
}

// mustOpenJournalWrite opens a journal and writes original page data.
func mustOpenJournalWrite(t *testing.T, path string, pageSize int, dbSize Pgno, pgno uint32, data []byte) *Journal {
	t.Helper()
	j := mustOpenJournal(t, path, pageSize, dbSize)
	if err := j.WriteOriginal(pgno, data); err != nil {
		t.Fatalf("WriteOriginal(%d) error = %v", pgno, err)
	}
	return j
}

// mustCreateWritePages creates pages in [start, end], writes them, and commits.
func mustCreateWritePages(t *testing.T, p *Pager, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		page := mustGetPage(t, p, i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)
}

// mustFreePages frees pages in [start, end] using a FreeList.
func mustFreePages(t *testing.T, fl *FreeList, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("Free(%d) error = %v", i, err)
		}
	}
}

// walkTrunkChain walks the trunk chain and returns the count, with a safety limit.
func walkTrunkChain(t *testing.T, fl *FreeList, firstTrunk Pgno, limit int) int {
	t.Helper()
	count := 0
	current := firstTrunk
	for current != 0 && count < limit {
		next, leaves, err := fl.ReadTrunk(current)
		if err != nil {
			t.Fatalf("failed to read trunk %d: %v", current, err)
		}
		t.Logf("Trunk %d: next=%d, leaves=%d", current, next, len(leaves))
		if next == current {
			t.Fatal("trunk points to itself (infinite loop)")
		}
		count++
		current = next
	}
	return count
}

// mustWriteDataToPage gets a page, marks it writable, copies data at offset, and puts it back.
func mustWriteDataToPage(t *testing.T, p *Pager, pgno Pgno, offset int, data []byte) {
	t.Helper()
	page := mustGetWritePage(t, p, pgno)
	copy(page.Data[offset:offset+len(data)], data)
	p.Put(page)
}

// mustOpenSecondLockManager opens a second file handle on the same file and creates a LockManager.
func mustOpenSecondLockManager(t *testing.T, filename string) (*os.File, *LockManager) {
	t.Helper()
	f, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	lm, err := NewLockManager(f)
	if err != nil {
		f.Close()
		t.Fatalf("NewLockManager() error = %v", err)
	}
	t.Cleanup(func() {
		lm.Close()
		f.Close()
	})
	return f, lm
}

// mustAllocateAndCommit allocates n pages via pager.AllocatePage and commits.
func mustAllocateAndCommit(t *testing.T, p *Pager, n int) []Pgno {
	t.Helper()
	pages := make([]Pgno, n)
	for i := 0; i < n; i++ {
		pgno, err := p.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
		pages[i] = pgno
	}
	mustCommit(t, p)
	return pages
}

// mustWriteCommitAndVerify writes data to page 1, commits, then verifies after reopen.
func mustWriteCommitAndVerify(t *testing.T, p *Pager, offset int, data []byte) {
	t.Helper()
	page := mustGetWritePage(t, p, 1)
	copy(page.Data[offset:offset+len(data)], data)
	p.Put(page)
	mustCommit(t, p)
}

// mustModifyPage modifies a page's data at an offset and puts it back (already writable).
func mustModifyPage(t *testing.T, p *Pager, pgno Pgno, offset int, data []byte) {
	t.Helper()
	page := mustGetWritePage(t, p, pgno)
	copy(page.Data[offset:offset+len(data)], data)
	p.Put(page)
}

// mustOpenMemoryPager opens a memory pager or fails.
func mustOpenMemoryPager(t *testing.T, pageSize int) *MemoryPager {
	t.Helper()
	mp, err := OpenMemory(pageSize)
	if err != nil {
		t.Fatalf("OpenMemory(%d) error = %v", pageSize, err)
	}
	t.Cleanup(func() { mp.Close() })
	return mp
}

// mustMemoryWrite marks a memory pager page writable or fails.
func mustMemoryWrite(t *testing.T, mp *MemoryPager, page *DbPage) {
	t.Helper()
	if err := mp.Write(page); err != nil {
		t.Fatalf("MemoryPager.Write() error = %v", err)
	}
}

// mustMemoryGet gets a page from memory pager or fails.
func mustMemoryGet(t *testing.T, mp *MemoryPager, pgno Pgno) *DbPage {
	t.Helper()
	page, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("MemoryPager.Get(%d) error = %v", pgno, err)
	}
	return page
}

// mustMemoryCommit commits on a memory pager or fails.
func mustMemoryCommit(t *testing.T, mp *MemoryPager) {
	t.Helper()
	if err := mp.Commit(); err != nil {
		t.Fatalf("MemoryPager.Commit() error = %v", err)
	}
}

// mustMemoryAllocate allocates a page on a memory pager or fails.
func mustMemoryAllocate(t *testing.T, mp *MemoryPager) Pgno {
	t.Helper()
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("MemoryPager.AllocatePage() error = %v", err)
	}
	return pgno
}

// mustMemoryBeginWrite starts a write transaction on a memory pager or fails.
func mustMemoryBeginWrite(t *testing.T, mp *MemoryPager) {
	t.Helper()
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("MemoryPager.BeginWrite() error = %v", err)
	}
}

// mustMemoryRollback rolls back on a memory pager or fails.
func mustMemoryRollback(t *testing.T, mp *MemoryPager) {
	t.Helper()
	if err := mp.Rollback(); err != nil {
		t.Fatalf("MemoryPager.Rollback() error = %v", err)
	}
}

// mustMemorySavepoint creates a savepoint on a memory pager or fails.
func mustMemorySavepoint(t *testing.T, mp *MemoryPager, name string) {
	t.Helper()
	if err := mp.Savepoint(name); err != nil {
		t.Fatalf("MemoryPager.Savepoint(%q) error = %v", name, err)
	}
}

// mustMemoryRollbackTo rolls back to a savepoint on a memory pager or fails.
func mustMemoryRollbackTo(t *testing.T, mp *MemoryPager, name string) {
	t.Helper()
	if err := mp.RollbackTo(name); err != nil {
		t.Fatalf("MemoryPager.RollbackTo(%q) error = %v", name, err)
	}
}

// mustMemoryFreePage frees a page on a memory pager or fails.
func mustMemoryFreePage(t *testing.T, mp *MemoryPager, pgno Pgno) {
	t.Helper()
	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("MemoryPager.FreePage(%d) error = %v", pgno, err)
	}
}

// memoryWritePageData allocates or gets a page, writes data, and puts it back.
func memoryWritePageData(t *testing.T, mp *MemoryPager, pgno Pgno, data []byte) {
	t.Helper()
	page := mustMemoryGet(t, mp, pgno)
	mustMemoryWrite(t, mp, page)
	copy(page.Data, data)
	mp.Put(page)
}

// mustPagerWriteInitialData writes data to page 1 at DatabaseHeaderSize, commits, returns the pager.
func mustPagerWriteInitialData(t *testing.T, p *Pager, data []byte) {
	t.Helper()
	mustWriteDataToPage(t, p, 1, DatabaseHeaderSize, data)
	mustCommit(t, p)
}

// mustPagerModifyAndRollback modifies page 1 then rolls back.
func mustPagerModifyAndRollback(t *testing.T, p *Pager, data []byte) {
	t.Helper()
	mustModifyPage(t, p, 1, DatabaseHeaderSize, data)
	mustRollback(t, p)
}

// verifyLockHeld checks that lm.IsLockHeld returns expected for the given level.
func verifyLockHeld(t *testing.T, lm *LockManager, level LockLevel, expected bool) {
	t.Helper()
	if lm.IsLockHeld(level) != expected {
		t.Errorf("IsLockHeld(%v) = %v, want %v", level, !expected, expected)
	}
}

// mustPagerWriteAndCommitTwoRounds writes to page 1 twice with commits, returns initial change counter.
func mustPagerWriteAndCommitTwoRounds(t *testing.T, p *Pager) uint32 {
	t.Helper()
	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0xAB
	p.Put(page)
	mustCommit(t, p)
	initialCounter := p.GetHeader().FileChangeCounter
	page = mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0xCD
	p.Put(page)
	mustCommit(t, p)
	return initialCounter
}

// mustPagerWriteCommitRollbackVerify writes, commits, modifies, rolls back, then verifies restoration.
func mustPagerWriteCommitRollbackVerify(t *testing.T, p *Pager, original, modified []byte) {
	t.Helper()
	mustPagerWriteInitialData(t, p, original)
	mustModifyPage(t, p, 1, DatabaseHeaderSize, modified)
	mustRollback(t, p)
	page := mustGetPage(t, p, 1)
	defer p.Put(page)
	readData := page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(original)]
	if string(readData) != string(original) {
		t.Errorf("data after rollback: got %q, want %q", readData, original)
	}
}

// mustCreatePagesAndFreeRange creates pages [2..totalPages], commits, then frees [freeStart..freeEnd].
func mustCreatePagesAndFreeRange(t *testing.T, p *Pager, fl *FreeList, totalPages int, freeStart, freeEnd Pgno) {
	t.Helper()
	mustCreateWritePages(t, p, 2, Pgno(totalPages))
	mustFreePages(t, fl, freeStart, freeEnd)
}
