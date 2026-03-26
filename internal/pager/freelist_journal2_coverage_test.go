// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// --- FreeList verify error-path coverage ---

// mockPagerForVerify implements pagerInternal using a map of pre-built pages.
type mockPagerForVerify struct {
	pages    map[Pgno]*DbPage
	pageSize int
}

func (m *mockPagerForVerify) getLocked(pgno Pgno) (*DbPage, error) {
	p, ok := m.pages[pgno]
	if !ok {
		return nil, ErrPageNotFound
	}
	return p, nil
}

func (m *mockPagerForVerify) writeLocked(page *DbPage) error { return nil }
func (m *mockPagerForVerify) Put(page *DbPage)               {}
func (m *mockPagerForVerify) PageSize() int                  { return m.pageSize }

// newMockPager creates a mockPagerForVerify with the given page size.
func newMockPager(pageSize int) *mockPagerForVerify {
	return &mockPagerForVerify{
		pages:    make(map[Pgno]*DbPage),
		pageSize: pageSize,
	}
}

// addTrunkPage adds a properly formed trunk page to the mock.
// nextTrunk: next trunk page number (0 = last)
// leaves: slice of leaf page numbers stored in this trunk
func (m *mockPagerForVerify) addTrunkPage(pgno, nextTrunk Pgno, leaves []Pgno) {
	data := make([]byte, m.pageSize)
	binary.BigEndian.PutUint32(data[0:4], uint32(nextTrunk))
	binary.BigEndian.PutUint32(data[4:8], uint32(len(leaves)))
	for i, leaf := range leaves {
		offset := FreeListTrunkHeaderSize + i*4
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(leaf))
	}
	m.pages[pgno] = &DbPage{Pgno: pgno, Data: data}
}

// TestFreelistJournal2_VerifyDuplicateTrunk exercises the seen[trunkPgno] branch
// in verifyTrunkPage by creating a cycle in the trunk chain.
func TestFreelistJournal2_VerifyDuplicateTrunk(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 points back to itself as nextTrunk, creating a cycle.
	mp.addTrunkPage(2, 2, []Pgno{3})
	mp.pages[3] = &DbPage{Pgno: 3, Data: make([]byte, pageSize)}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 2 // trunk + leaf

	err := fl.Verify()
	if err == nil {
		t.Error("Verify: expected error for duplicate trunk page in cycle, got nil")
	}
	if err != ErrFreeListCorrupt {
		t.Errorf("Verify: expected ErrFreeListCorrupt, got %v", err)
	}
}

// TestFreelistJournal2_VerifyLeafCountOverflow exercises verifyLeafCount when
// the stored leaf count exceeds the maximum possible for the page size.
func TestFreelistJournal2_VerifyLeafCountOverflow(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Build a trunk page with an absurd leaf count (0xFFFFFFFF).
	data := make([]byte, pageSize)
	binary.BigEndian.PutUint32(data[0:4], 0) // no next trunk
	binary.BigEndian.PutUint32(data[4:8], 0xFFFFFFFF)
	mp.pages[2] = &DbPage{Pgno: 2, Data: data}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 1

	err := fl.Verify()
	if err == nil {
		t.Error("Verify: expected ErrFreeListCorrupt for oversized leaf count, got nil")
	}
	if err != ErrFreeListCorrupt {
		t.Errorf("Verify: expected ErrFreeListCorrupt, got %v", err)
	}
}

// TestFreelistJournal2_VerifyLeafPageZero exercises verifyLeafPage when a leaf
// page number stored in the trunk is 0 (invalid).
func TestFreelistJournal2_VerifyLeafPageZero(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 has one leaf whose page number is 0 (invalid).
	mp.addTrunkPage(2, 0, []Pgno{0})

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 2 // trunk + 1 leaf (even though leaf is invalid)

	err := fl.Verify()
	if err == nil {
		t.Error("Verify: expected ErrFreeListCorrupt for zero leaf page number, got nil")
	}
	if err != ErrFreeListCorrupt {
		t.Errorf("Verify: expected ErrFreeListCorrupt, got %v", err)
	}
}

// TestFreelistJournal2_VerifyDuplicateLeafPage exercises verifyLeafPage when
// the same leaf page appears more than once in the freelist.
func TestFreelistJournal2_VerifyDuplicateLeafPage(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 lists page 5 twice.
	mp.addTrunkPage(2, 0, []Pgno{5, 5})

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 3 // trunk + 2 "leaves" (one is a dup)

	err := fl.Verify()
	if err == nil {
		t.Error("Verify: expected ErrFreeListCorrupt for duplicate leaf page, got nil")
	}
	if err != ErrFreeListCorrupt {
		t.Errorf("Verify: expected ErrFreeListCorrupt, got %v", err)
	}
}

// TestFreelistJournal2_VerifyTotalCountMismatch exercises verifyTotalCount when
// the stored totalFree doesn't match the actual number of pages walked.
func TestFreelistJournal2_VerifyTotalCountMismatch(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 with leaf 3 — total walking gives 2 pages.
	mp.addTrunkPage(2, 0, []Pgno{3})
	mp.pages[3] = &DbPage{Pgno: 3, Data: make([]byte, pageSize)}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 99 // deliberately wrong

	err := fl.Verify()
	if err == nil {
		t.Error("Verify: expected ErrFreeListCorrupt for count mismatch, got nil")
	}
	if err != ErrFreeListCorrupt {
		t.Errorf("Verify: expected ErrFreeListCorrupt, got %v", err)
	}
}

// TestFreelistJournal2_ProcessTrunkPageFull exercises the "trunk is full" branch
// in processTrunkPage, which creates a new trunk when the current trunk is at
// capacity (leafCount == maxLeaves).
func TestFreelistJournal2_ProcessTrunkPageFull(t *testing.T) {
	t.Parallel()
	p, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	// Allocate enough real pages so the pager can serve getLocked calls.
	const totalPages = 30
	for i := Pgno(2); i <= totalPages; i++ {
		page, err := p.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if err := p.Write(page); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
		p.Put(page)
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Use a tiny page size via a separate mock so maxLeaves == 1.
	// With a 16-byte page: (16-8)/4 = 2 leaves max.
	// We'll fake a trunk with leafCount already at maxLeaves, then add one more pending page.
	const smallPageSize = 16
	mp := newMockPager(smallPageSize)

	// Trunk at page 5: maxLeaves = (16-8)/4 = 2. Set leafCount = 2 (full).
	data := make([]byte, smallPageSize)
	binary.BigEndian.PutUint32(data[0:4], 0)            // no next trunk
	binary.BigEndian.PutUint32(data[4:8], 2)            // leafCount = 2 (at max)
	binary.BigEndian.PutUint32(data[8:12], uint32(10))  // leaf 0 = page 10
	binary.BigEndian.PutUint32(data[12:16], uint32(11)) // leaf 1 = page 11
	mp.pages[5] = &DbPage{Pgno: 5, Data: data}
	// Page 7 exists so createNewTrunk can initialise it.
	mp.pages[7] = &DbPage{Pgno: 7, Data: make([]byte, smallPageSize)}

	fl := NewFreeList(mp)
	fl.firstTrunk = 5
	fl.totalFree = 3           // trunk + 2 leaves
	fl.pendingFree = []Pgno{7} // one pending page that needs a new trunk

	maxLeaves := FreeListMaxLeafPages(smallPageSize)
	if err := fl.processTrunkPage(maxLeaves); err != nil {
		t.Fatalf("processTrunkPage (full trunk): %v", err)
	}

	// After the call, a new trunk should have been created using page 7.
	if fl.firstTrunk != 7 {
		t.Errorf("expected new firstTrunk=7, got %d", fl.firstTrunk)
	}
	if len(fl.pendingFree) != 0 {
		t.Errorf("expected pendingFree to be empty, got %v", fl.pendingFree)
	}
}

// --- Journal restoreAllEntries error-path coverage ---

// TestFreelistJournal2_RestoreAllEntriesReadError exercises the non-EOF read
// error branch in restoreAllEntries by closing the underlying file mid-read.
func TestFreelistJournal2_RestoreAllEntriesReadError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "err.db")
	jPath := filepath.Join(dir, "err.db-journal")

	// Open pager and commit a page so the db file has content.
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}

	page, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if err := p.Write(page); err != nil {
		t.Fatalf("Write(1): %v", err)
	}
	page.Data[DatabaseHeaderSize] = 0xBE
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	pageSize := p.PageSize()
	dbSize := Pgno(p.PageCount())
	p.Close()

	// Build a journal with a full valid entry followed by truncated data.
	j := NewJournal(jPath, pageSize, dbSize)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	origData := make([]byte, pageSize)
	origData[DatabaseHeaderSize] = 0xBE
	if err := j.WriteOriginal(1, origData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// Append a partial (truncated) entry — not enough bytes to form a full record.
	// This hits the `n < entrySize` branch in restoreAllEntries.
	partial := make([]byte, 3) // far less than 4+pageSize+4
	// Write directly to the underlying file handle via the journal's file path.
	if err := j.Close(); err != nil {
		t.Fatalf("Close journal for append: %v", err)
	}
	f, err := os.OpenFile(jPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open journal for append: %v", err)
	}
	if _, err := f.Write(partial); err != nil {
		f.Close()
		t.Fatalf("append partial entry: %v", err)
	}
	f.Close()

	// Re-open journal for rollback — we need a Journal whose file is open.
	j2 := NewJournal(jPath, pageSize, dbSize)
	// Manually open it by re-reading — but Rollback needs j2.file open.
	// We replicate the internal open: set file field directly is not possible from
	// external package, so use the exported Open path. Open truncates, so instead
	// we open the file manually and call restoreAllEntries via Rollback.
	// Since we're in package pager (internal), we can set j2.file directly.
	j2.file, err = os.OpenFile(jPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open journal file for rollback: %v", err)
	}
	j2.initialized = true

	// Reopen db pager for rollback target.
	p2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer p2.Close()

	// Rollback should succeed: first entry is fully valid and gets applied;
	// the partial trailing data triggers the n < entrySize break (not an error).
	if err := j2.Rollback(p2); err != nil {
		t.Errorf("Rollback with partial trailing entry: unexpected error: %v", err)
	}
	j2.Delete()
}

// TestFreelistJournal2_RestoreAllEntriesChecksumError exercises the checksum
// mismatch error path inside restoreEntry (called from restoreAllEntries).
func TestFreelistJournal2_RestoreAllEntriesChecksumError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "cksum.db")
	jPath := filepath.Join(dir, "cksum.db-journal")

	// Create a db file with at least one page.
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open db: %v", err)
	}
	page, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if err := p.Write(page); err != nil {
		t.Fatalf("Write(1): %v", err)
	}
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	pageSize := p.PageSize()
	dbSize := Pgno(p.PageCount())
	p.Close()

	// Write a journal entry with a deliberately corrupted checksum.
	j := NewJournal(jPath, pageSize, dbSize)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	origData := make([]byte, pageSize)
	if err := j.WriteOriginal(1, origData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}
	// Corrupt the last 4 bytes (checksum) of the entry in the file.
	entrySize := int64(4 + pageSize + 4)
	corruptOffset := int64(JournalHeaderSize) + entrySize - 4
	badChecksum := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := j.file.WriteAt(badChecksum, corruptOffset); err != nil {
		t.Fatalf("corrupt checksum in journal: %v", err)
	}

	// Keep file open — seek back to after header before Rollback.
	if _, err := j.file.Seek(JournalHeaderSize, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	// Reopen pager for rollback.
	p2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer p2.Close()

	// Rollback should fail with a checksum mismatch error.
	err = j.Rollback(p2)
	if err == nil {
		t.Error("Rollback with corrupt checksum: expected error, got nil")
	}
	j.Delete()
}

// TestFreelistJournal2_VerifyLeafPagesMultiTrunk exercises verifyLeafPages across
// two trunk pages, ensuring both the happy path and the page-counting are
// exercised at higher depth than the existing tests.
func TestFreelistJournal2_VerifyLeafPagesMultiTrunk(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 2 → Trunk 3 (two-level chain), each with one valid leaf.
	mp.addTrunkPage(2, 3, []Pgno{10})
	mp.addTrunkPage(3, 0, []Pgno{11})
	mp.pages[10] = &DbPage{Pgno: 10, Data: make([]byte, pageSize)}
	mp.pages[11] = &DbPage{Pgno: 11, Data: make([]byte, pageSize)}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 4 // trunk2 + leaf10 + trunk3 + leaf11

	if err := fl.Verify(); err != nil {
		t.Errorf("Verify multi-trunk: unexpected error: %v", err)
	}
}
