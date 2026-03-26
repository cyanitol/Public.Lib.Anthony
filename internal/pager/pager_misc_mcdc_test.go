// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Condition: MemoryPager.Close — rollback guard
//   `mp.state >= PagerStateWriterLocked && mp.state < PagerStateError`
//
//   A = mp.state >= PagerStateWriterLocked
//   B = mp.state < PagerStateError
//
//   Rollback triggered when A && B is true.
//
//   Case 1 (A=F): state=PagerStateOpen → no rollback
//   Case 2 (A=T, B=F): state=PagerStateError → no rollback
//   Case 3 (A=T, B=T): state=PagerStateWriterLocked → rollback triggered
// ---------------------------------------------------------------------------

func TestMCDC_MemClose_StateOpen(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (state < WriterLocked) → Close succeeds without rollback
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.Close(); err != nil {
		t.Errorf("MCDC case1: Close on Open memory pager must succeed; got %v", err)
	}
}

func TestMCDC_MemClose_StateError(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (state=Error) → rollback guard skipped
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mp.mu.Lock()
	mp.state = PagerStateError
	mp.mu.Unlock()

	// Close should not panic; outcome is acceptable either way.
	_ = mp.Close()
}

func TestMCDC_MemClose_StateWriterLocked(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (state=WriterLocked) → rollback is triggered
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	// Close while in a write transaction should trigger rollback
	if err := mp.Close(); err != nil {
		t.Errorf("MCDC case3: Close with WriterLocked state must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.getLocked — invalid page number guard
//   `pgno == 0 || pgno > mp.maxPageNum`
//
//   A = pgno == 0
//   B = pgno > mp.maxPageNum
//
//   ErrInvalidPageNum returned when A || B is true.
//
//   Case 1 (A=T): pgno=0 → error
//   Case 2 (A=F, B=T): pgno > maxPageNum → error
//   Case 3 (A=F, B=F): valid pgno → no error
// ---------------------------------------------------------------------------

func TestMCDC_MemGetLocked_PgnoZero(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (pgno==0) → ErrInvalidPageNum
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	_, err := mp.Get(0)
	if err != ErrInvalidPageNum {
		t.Errorf("MCDC case1: pgno=0 must return ErrInvalidPageNum; got %v", err)
	}
}

func TestMCDC_MemGetLocked_PgnoExceedsMax(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (pgno > maxPageNum) → ErrInvalidPageNum
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	_, err := mp.Get(Pgno(mp.maxPageNum) + 1)
	if err != ErrInvalidPageNum {
		t.Errorf("MCDC case2: pgno > maxPageNum must return ErrInvalidPageNum; got %v", err)
	}
}

func TestMCDC_MemGetLocked_ValidPgno(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid pgno) → page returned without error
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	page, err := mp.Get(1)
	if err != nil {
		t.Errorf("MCDC case3: valid pgno=1 must not return error; got %v", err)
	}
	if page != nil {
		mp.Put(page)
	}
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.Commit — inline needsHeaderUpdate
//   `mp.dbSize != mp.dbOrigSize ||
//    mp.header.FreelistTrunk != uint32(mp.freeList.GetFirstTrunk()) ||
//    mp.header.FreelistCount != mp.freeList.GetTotalFree()`
//
//   A = dbSize changed
//   B = FreelistTrunk changed
//   C = FreelistCount changed
//
//   Header update triggered when A || B || C is true.
//
//   Case 1 (A=T): allocate a page → dbSize grows → header updated
//   Case 2 (A=F, B=F, C=T): only count differs → header updated
//   Case 3 (A=F, B=F, C=F): no changes → header not updated (change counter unchanged)
// ---------------------------------------------------------------------------

func TestMCDC_MemCommitHeader_DbSizeChanged(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → allocating a new page changes dbSize
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	_ = mustMemoryAllocate(t, mp)
	before := mp.header.FileChangeCounter
	mustMemoryCommit(t, mp)
	after := mp.header.FileChangeCounter
	if after <= before {
		t.Errorf("MCDC case1: dbSize change must trigger header update (change counter should increase); before=%d after=%d", before, after)
	}
}

func TestMCDC_MemCommitHeader_FreelistCountChanged(t *testing.T) {
	t.Parallel()
	// Case 2 (A=F, B=F, C=T): artificially make count differ only
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	// Arrange: keep dbSize == dbOrigSize and trunk unchanged, but alter header count
	mp.mu.Lock()
	mp.dbOrigSize = mp.dbSize
	mp.header.FreelistTrunk = uint32(mp.freeList.GetFirstTrunk())
	mp.header.FreelistCount++ // drift the count so C=T
	mp.mu.Unlock()
	before := mp.header.FileChangeCounter
	mustMemoryCommit(t, mp)
	after := mp.header.FileChangeCounter
	if after <= before {
		t.Errorf("MCDC case2: FreelistCount drift must trigger header update; before=%d after=%d", before, after)
	}
}

func TestMCDC_MemCommitHeader_NothingChanged(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=F → no header update (change counter unchanged)
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	// Align everything so no update is needed
	mp.mu.Lock()
	mp.dbOrigSize = mp.dbSize
	mp.header.FreelistTrunk = uint32(mp.freeList.GetFirstTrunk())
	mp.header.FreelistCount = mp.freeList.GetTotalFree()
	mp.mu.Unlock()
	before := mp.header.FileChangeCounter
	mustMemoryCommit(t, mp)
	after := mp.header.FileChangeCounter
	if after != before {
		t.Errorf("MCDC case3: no changes must not update header; before=%d after=%d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.FreePage — invalid page guard
//   `pgno == 0 || pgno > mp.dbSize`
//
//   A = pgno == 0
//   B = pgno > mp.dbSize
//
//   ErrInvalidPageNum returned when A || B is true.
//
//   Case 1 (A=T): pgno=0 → error
//   Case 2 (A=F, B=T): pgno > dbSize → error
//   Case 3 (A=F, B=F): valid pgno → no error from guard
// ---------------------------------------------------------------------------

func TestMCDC_MemFreePage_PgnoZero(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → ErrInvalidPageNum
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.FreePage(0); err != ErrInvalidPageNum {
		t.Errorf("MCDC case1: pgno=0 must return ErrInvalidPageNum; got %v", err)
	}
}

func TestMCDC_MemFreePage_PgnoExceedsDbSize(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (pgno > dbSize) → ErrInvalidPageNum
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	// dbSize starts at 1
	if err := mp.FreePage(Pgno(mp.dbSize + 1)); err != ErrInvalidPageNum {
		t.Errorf("MCDC case2: pgno > dbSize must return ErrInvalidPageNum; got %v", err)
	}
}

func TestMCDC_MemFreePage_ValidPgno(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid pgno within dbSize) → no error from guard
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	// pgno is now within dbSize
	if err := mp.FreePage(pgno); err != nil {
		t.Errorf("MCDC case3: valid pgno FreePage must succeed; got %v", err)
	}
	mustMemoryRollback(t, mp)
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.FreePage — auto-begin write guard
//   `mp.state == PagerStateOpen || mp.state == PagerStateReader`
//
//   A = mp.state == PagerStateOpen
//   B = mp.state == PagerStateReader
//
//   beginWriteTransaction called when A || B is true.
//
//   Case 1 (A=T): state=Open → write transaction auto-started
//   Case 2 (A=F, B=T): state=Reader → write transaction auto-started
//   Case 3 (A=F, B=F): state=WriterLocked → no-op (already in write txn)
// ---------------------------------------------------------------------------

func TestMCDC_MemFreePageAutoBegin_FromOpen(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (state=Open) → FreePage auto-starts write transaction
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)
	// Now state=Open; FreePage should auto-begin a write transaction
	if err := mp.FreePage(pgno); err != nil {
		t.Errorf("MCDC case1: FreePage from Open must succeed; got %v", err)
	}
	mustMemoryCommit(t, mp)
}

func TestMCDC_MemFreePageAutoBegin_FromReader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (state=Reader) → FreePage auto-promotes to writer
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)
	// Enter reader state
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead error = %v", err)
	}
	// FreePage should auto-start write transaction
	if err := mp.FreePage(pgno); err != nil {
		t.Errorf("MCDC case2: FreePage from Reader must succeed; got %v", err)
	}
	mustMemoryCommit(t, mp)
}

func TestMCDC_MemFreePageAutoBegin_AlreadyWriter(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (state=WriterLocked) → no re-entry, succeeds
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	// Still in write transaction; FreePage should work without auto-begin
	if err := mp.FreePage(pgno); err != nil {
		t.Errorf("MCDC case3: FreePage from WriterLocked must succeed; got %v", err)
	}
	mustMemoryRollback(t, mp)
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.ensureWriteTransaction
//   `mp.state == PagerStateOpen || mp.state == PagerStateReader`
//
//   A = mp.state == PagerStateOpen
//   B = mp.state == PagerStateReader
//
//   beginWriteTransaction called when A || B is true.
//
//   Case 1 (A=T): state=Open → beginWriteTransaction called
//   Case 2 (A=F, B=T): state=Reader → beginWriteTransaction called
//   Case 3 (A=F, B=F): state=WriterLocked → no-op
// ---------------------------------------------------------------------------

func TestMCDC_MemEnsureWriteTx_FromOpen(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (state=Open) → Write auto-starts write transaction
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	page := mustMemoryGet(t, mp, 1)
	if err := mp.Write(page); err != nil {
		t.Errorf("MCDC case1: Write from Open must succeed; got %v", err)
	}
	mp.Put(page)
	mustMemoryRollback(t, mp)
}

func TestMCDC_MemEnsureWriteTx_FromReader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (state=Reader) → beginWriteTransaction called
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead error = %v", err)
	}
	page := mustMemoryGet(t, mp, 1)
	if err := mp.Write(page); err != nil {
		t.Errorf("MCDC case2: Write from Reader must succeed; got %v", err)
	}
	mp.Put(page)
	mustMemoryRollback(t, mp)
}

func TestMCDC_MemEnsureWriteTx_AlreadyWriter(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (state=WriterLocked) → no-op, Write succeeds
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	page := mustMemoryGet(t, mp, 1)
	if err := mp.Write(page); err != nil {
		t.Errorf("MCDC case3: Write from WriterLocked must succeed; got %v", err)
	}
	mp.Put(page)
	mustMemoryRollback(t, mp)
}

// ---------------------------------------------------------------------------
// Condition: MemoryPager.Vacuum — VACUUM INTO guard
//   `opts != nil && opts.IntoFile != ""`
//
//   A = opts != nil
//   B = opts.IntoFile != ""
//
//   Returns an error (unsupported) when A && B is true; returns nil otherwise.
//
//   Case 1 (A=F): opts==nil → nil (no-op)
//   Case 2 (A=T, B=F): IntoFile=="" → nil (no-op)
//   Case 3 (A=T, B=T): IntoFile!="" → error (not supported)
// ---------------------------------------------------------------------------

func TestMCDC_MemVacuum_NilOpts(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (opts==nil) → nil
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.Vacuum(nil); err != nil {
		t.Errorf("MCDC case1: Vacuum(nil) must return nil; got %v", err)
	}
}

func TestMCDC_MemVacuum_EmptyIntoFile(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (IntoFile=="") → nil
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.Vacuum(&VacuumOptions{IntoFile: ""}); err != nil {
		t.Errorf("MCDC case2: Vacuum with empty IntoFile must return nil; got %v", err)
	}
}

func TestMCDC_MemVacuum_WithIntoFile(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (IntoFile!="") → error (unsupported for memory databases)
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	err := mp.Vacuum(&VacuumOptions{IntoFile: "/tmp/out.db"})
	if err == nil {
		t.Error("MCDC case3: Vacuum INTO on memory pager must return an error")
	}
}

// ---------------------------------------------------------------------------
// Condition: isValidPageSize — out-of-range guard
//   `size < MinPageSize || size > MaxPageSize`
//
//   A = size < MinPageSize   (below 512)
//   B = size > MaxPageSize   (above 65536)
//
//   Returns false when A || B is true (before power-of-2 check).
//
//   Case 1 (A=T): size=256 (< 512) → false
//   Case 2 (A=F, B=T): size=131072 (> 65536) → false
//   Case 3 (A=F, B=F): size=4096 (in range, power-of-2) → true
// ---------------------------------------------------------------------------

func TestMCDC_IsValidPageSize_TooSmall(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (size < MinPageSize) → false
	if isValidPageSize(256) {
		t.Error("MCDC case1: size=256 (< MinPageSize=512) must return false")
	}
}

func TestMCDC_IsValidPageSize_TooLarge(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (size > MaxPageSize) → false
	if isValidPageSize(131072) {
		t.Error("MCDC case2: size=131072 (> MaxPageSize=65536) must return false")
	}
}

func TestMCDC_IsValidPageSize_Valid(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (size in range and power-of-2) → true
	if !isValidPageSize(4096) {
		t.Error("MCDC case3: size=4096 must return true")
	}
}

// ---------------------------------------------------------------------------
// Condition: DatabaseHeader.validateSchemaAndEncoding — schema format range
//   `h.SchemaFormat < 1 || h.SchemaFormat > 4`
//
//   A = SchemaFormat < 1
//   B = SchemaFormat > 4
//
//   Returns an error when A || B is true.
//
//   Case 1 (A=T): SchemaFormat=0 → error
//   Case 2 (A=F, B=T): SchemaFormat=5 → error
//   Case 3 (A=F, B=F): SchemaFormat=4 → nil
// ---------------------------------------------------------------------------

func TestMCDC_ValidateSchemaFormat_TooLow(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (SchemaFormat < 1) → error
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 0
	h.TextEncoding = EncodingUTF8
	if err := h.validateSchemaAndEncoding(); err == nil {
		t.Error("MCDC case1: SchemaFormat=0 must return error")
	}
}

func TestMCDC_ValidateSchemaFormat_TooHigh(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (SchemaFormat > 4) → error
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 5
	h.TextEncoding = EncodingUTF8
	if err := h.validateSchemaAndEncoding(); err == nil {
		t.Error("MCDC case2: SchemaFormat=5 must return error")
	}
}

func TestMCDC_ValidateSchemaFormat_Valid(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (SchemaFormat in [1,4]) → nil
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 4
	h.TextEncoding = EncodingUTF8
	if err := h.validateSchemaAndEncoding(); err != nil {
		t.Errorf("MCDC case3: SchemaFormat=4 must not return error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: DatabaseHeader.validateSchemaAndEncoding — text encoding range
//   `h.TextEncoding < EncodingUTF8 || h.TextEncoding > EncodingUTF16BE`
//
//   A = TextEncoding < EncodingUTF8   (< 1)
//   B = TextEncoding > EncodingUTF16BE (> 3)
//
//   Returns an error when A || B is true.
//
//   Case 1 (A=T): TextEncoding=0 → error
//   Case 2 (A=F, B=T): TextEncoding=4 → error
//   Case 3 (A=F, B=F): TextEncoding=2 → nil
// ---------------------------------------------------------------------------

func TestMCDC_ValidateTextEncoding_TooLow(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (TextEncoding < EncodingUTF8=1) → error
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 4
	h.TextEncoding = 0
	if err := h.validateSchemaAndEncoding(); err == nil {
		t.Error("MCDC case1: TextEncoding=0 must return error")
	}
}

func TestMCDC_ValidateTextEncoding_TooHigh(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (TextEncoding > EncodingUTF16BE=3) → error
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 4
	h.TextEncoding = 4
	if err := h.validateSchemaAndEncoding(); err == nil {
		t.Error("MCDC case2: TextEncoding=4 must return error")
	}
}

func TestMCDC_ValidateTextEncoding_Valid(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (TextEncoding=EncodingUTF16LE=2) → nil
	h := NewDatabaseHeader(DefaultPageSize)
	h.SchemaFormat = 4
	h.TextEncoding = EncodingUTF16LE
	if err := h.validateSchemaAndEncoding(); err != nil {
		t.Errorf("MCDC case3: TextEncoding=2 must not return error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: PageCache.evictCleanPages — eviction eligibility
//   `page.IsClean() && page.GetRefCount() == 0`
//
//   A = page.IsClean()
//   B = page.GetRefCount() == 0
//
//   Page evicted when A && B is true; otherwise skipped.
//
//   Case 1 (A=F): dirty page → not evicted → ErrCacheFull when all dirty
//   Case 2 (A=T, B=F): clean page with active reference → not evicted
//   Case 3 (A=T, B=T): clean page with no references → evicted
// ---------------------------------------------------------------------------

func TestMCDC_EvictCleanPages_DirtyPage(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → dirty page is never evicted → cache full error when capacity is 1
	cache := NewPageCache(DefaultPageSize, 1)

	dirty := NewDbPage(1, DefaultPageSize)
	dirty.MakeDirty()
	// Force add to the map directly (bypassing Put's eviction path)
	cache.mu.Lock()
	cache.pages[1] = dirty
	cache.mu.Unlock()

	// Now try to evict — all pages are dirty, so evictCleanPages returns ErrCacheFull
	cache.mu.Lock()
	err := cache.evictCleanPages(1)
	cache.mu.Unlock()
	if err != ErrCacheFull {
		t.Errorf("MCDC case1: dirty-only cache must return ErrCacheFull; got %v", err)
	}
}

func TestMCDC_EvictCleanPages_CleanWithRef(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F → clean page with RefCount > 0 → not evicted → ErrCacheFull
	cache := NewPageCache(DefaultPageSize, 1)

	page := NewDbPage(2, DefaultPageSize)
	// page.IsClean() is true (PageFlagClean is set by default)
	// page.GetRefCount() == 1 (set by NewDbPage)
	cache.mu.Lock()
	cache.pages[2] = page
	cache.mu.Unlock()

	cache.mu.Lock()
	err := cache.evictCleanPages(1)
	cache.mu.Unlock()
	if err != ErrCacheFull {
		t.Errorf("MCDC case2: clean page with RefCount=1 must not be evicted; got err=%v", err)
	}
}

func TestMCDC_EvictCleanPages_CleanNoRef(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → clean page with RefCount=0 → evicted → nil
	cache := NewPageCache(DefaultPageSize, 1)

	page := NewDbPage(3, DefaultPageSize)
	// Drop reference so RefCount == 0
	page.Unref()
	cache.mu.Lock()
	cache.pages[3] = page
	cache.mu.Unlock()

	cache.mu.Lock()
	err := cache.evictCleanPages(1)
	cache.mu.Unlock()
	if err != nil {
		t.Errorf("MCDC case3: clean zero-ref page must be evicted; got err=%v", err)
	}
	// Verify it was actually removed
	cache.mu.RLock()
	_, stillPresent := cache.pages[3]
	cache.mu.RUnlock()
	if stillPresent {
		t.Error("MCDC case3: page should have been removed from cache after eviction")
	}
}

// ---------------------------------------------------------------------------
// Condition: DbPage.Write — bounds guard
//   `offset < 0 || offset+len(data) > len(p.Data)`
//
//   A = offset < 0
//   B = offset+len(data) > len(p.Data)
//
//   ErrInvalidOffset returned when A || B is true.
//
//   Case 1 (A=T): offset=-1 → error
//   Case 2 (A=F, B=T): offset valid but data extends past end → error
//   Case 3 (A=F, B=F): valid offset and length → nil
// ---------------------------------------------------------------------------

func TestMCDC_DbPageWrite_NegativeOffset(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (offset < 0) → ErrInvalidOffset
	page := NewDbPage(1, DefaultPageSize)
	data := []byte{0xAA}
	if err := page.Write(-1, data); err != ErrInvalidOffset {
		t.Errorf("MCDC case1: negative offset must return ErrInvalidOffset; got %v", err)
	}
}

func TestMCDC_DbPageWrite_PastEnd(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (write extends past page boundary) → ErrInvalidOffset
	page := NewDbPage(1, DefaultPageSize)
	data := make([]byte, 10)
	offset := DefaultPageSize - 5 // 5 bytes before end, but writing 10 bytes overflows
	if err := page.Write(offset, data); err != ErrInvalidOffset {
		t.Errorf("MCDC case2: write past end must return ErrInvalidOffset; got %v", err)
	}
}

func TestMCDC_DbPageWrite_ValidRange(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid offset and size) → nil
	page := NewDbPage(1, DefaultPageSize)
	data := []byte{0x01, 0x02, 0x03}
	if err := page.Write(0, data); err != nil {
		t.Errorf("MCDC case3: valid write must not return error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: DbPage.Read — bounds guard
//   `offset < 0 || offset+length > len(p.Data)`
//
//   A = offset < 0
//   B = offset+length > len(p.Data)
//
//   ErrInvalidOffset returned when A || B is true.
//
//   Case 1 (A=T): offset=-1 → error
//   Case 2 (A=F, B=T): read extends past page boundary → error
//   Case 3 (A=F, B=F): valid range → nil
// ---------------------------------------------------------------------------

func TestMCDC_DbPageRead_NegativeOffset(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (offset < 0) → ErrInvalidOffset
	page := NewDbPage(1, DefaultPageSize)
	if _, err := page.Read(-1, 4); err != ErrInvalidOffset {
		t.Errorf("MCDC case1: negative offset must return ErrInvalidOffset; got %v", err)
	}
}

func TestMCDC_DbPageRead_PastEnd(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (read extends past page boundary) → ErrInvalidOffset
	page := NewDbPage(1, DefaultPageSize)
	if _, err := page.Read(DefaultPageSize-5, 10); err != ErrInvalidOffset {
		t.Errorf("MCDC case2: read past end must return ErrInvalidOffset; got %v", err)
	}
}

func TestMCDC_DbPageRead_ValidRange(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid offset and length) → nil
	page := NewDbPage(1, DefaultPageSize)
	page.Data[0] = 0xBE
	data, err := page.Read(0, 4)
	if err != nil {
		t.Errorf("MCDC case3: valid read must not return error; got %v", err)
	}
	if data[0] != 0xBE {
		t.Errorf("MCDC case3: expected data[0]=0xBE; got 0x%02X", data[0])
	}
}

// ---------------------------------------------------------------------------
// Condition: FreeList.addPendingToTrunk — loop continuation guard
//   `leafCount < maxLeaves && len(fl.pendingFree) > 0`
//
//   A = leafCount < maxLeaves
//   B = len(fl.pendingFree) > 0
//
//   Loop continues when A && B is true.
//
//   Case 1 (A=F, B=T): trunk already full → loop body never entered
//   Case 2 (A=T, B=F): no pending pages → loop body never entered
//   Case 3 (A=T, B=T): space in trunk AND pending pages → loop entered
// ---------------------------------------------------------------------------

func TestMCDC_AddPendingToTrunk_TrunkFull(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (leafCount >= maxLeaves) → no pages added
	p := openTestPager(t)
	mustBeginWrite(t, p)

	fl := p.freeList
	maxLeaves := FreeListMaxLeafPages(p.pageSize)

	// Create enough pages so we can fill a trunk
	pages := make([]Pgno, maxLeaves+2)
	for i := range pages {
		pages[i] = mustAllocatePage(t, p)
	}
	mustCommit(t, p)
	mustBeginWrite(t, p)

	// Free enough pages to fill a trunk exactly
	for i := 0; i < maxLeaves; i++ {
		mustFreePage(t, p, pages[i])
	}
	// Flush to disk so trunk is written at full capacity
	if err := fl.Flush(); err != nil {
		t.Fatalf("Flush error = %v", err)
	}

	fl.mu.Lock()
	// Verify trunk is full: totalFree should equal maxLeaves
	if int(fl.totalFree) != maxLeaves {
		fl.mu.Unlock()
		t.Skipf("trunk not full after flush: totalFree=%d maxLeaves=%d", fl.totalFree, maxLeaves)
	}
	fl.mu.Unlock()

	// Now free one more page; addPendingToTrunk should see trunk full and create new trunk
	mustFreePage(t, p, pages[maxLeaves])
	if err := fl.Flush(); err != nil {
		t.Fatalf("MCDC case1: Flush with full trunk must succeed; got %v", err)
	}
	mustRollback(t, p)
}

func TestMCDC_AddPendingToTrunk_NoPendingPages(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (no pending pages) → loop not entered; flushPending is a no-op
	p := openTestPager(t)
	mustBeginWrite(t, p)

	fl := p.freeList
	fl.mu.Lock()
	fl.pendingFree = fl.pendingFree[:0]
	fl.mu.Unlock()

	// Flush with empty pending list → no-op, no error
	if err := fl.Flush(); err != nil {
		t.Errorf("MCDC case2: Flush with empty pending list must return nil; got %v", err)
	}
	mustRollback(t, p)
}

func TestMCDC_AddPendingToTrunk_SpaceAndPending(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (trunk has space and pending list is non-empty) → loop body entered
	p := openTestPager(t)
	mustBeginWrite(t, p)

	pgno1 := mustAllocatePage(t, p)
	pgno2 := mustAllocatePage(t, p)
	mustCommit(t, p)
	mustBeginWrite(t, p)

	// Free two pages → they go into pendingFree; flush merges them into the trunk
	mustFreePage(t, p, pgno1)
	mustFreePage(t, p, pgno2)

	fl := p.freeList
	fl.mu.Lock()
	pendingCount := len(fl.pendingFree)
	fl.mu.Unlock()

	if pendingCount < 2 {
		t.Skipf("expected ≥2 pending pages before flush; got %d", pendingCount)
	}

	// Flush exercises the loop body (space in trunk AND pages to add)
	if err := fl.Flush(); err != nil {
		t.Fatalf("MCDC case3: Flush with pending pages must succeed; got %v", err)
	}

	fl.mu.Lock()
	totalFree := fl.totalFree
	fl.mu.Unlock()

	if totalFree == 0 {
		t.Error("MCDC case3: pages should have been moved to on-disk free list")
	}
	mustRollback(t, p)
}
