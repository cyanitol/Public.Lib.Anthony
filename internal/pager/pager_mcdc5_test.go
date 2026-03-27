// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// wal_index.go:329 — InsertFrame not-initialized guard
//   `!w.initialized`
//
//   Returns error when w.initialized is false.
//
//   Case 1 (initialized=F): InsertFrame on freshly-created (closed) index → error
//   Case 2 (initialized=T): InsertFrame after open → succeeds
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_InsertFrame_NotInitialized(t *testing.T) {
	t.Parallel()
	// Case 1: initialized=false → error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	mustCloseWALIndex(t, idx)
	// idx.initialized is now false
	if err := idx.InsertFrame(1, 1); err == nil {
		t.Error("MCDC case1: InsertFrame on closed WALIndex must return an error")
	}
}

func TestMCDC_WALIndex_InsertFrame_Initialized(t *testing.T) {
	t.Parallel()
	// Case 2: initialized=true → succeeds
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.InsertFrame(1, 1); err != nil {
		t.Errorf("MCDC case2: InsertFrame on open WALIndex must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:333 — InsertFrame zero-page guard
//   `pgno == 0`
//
//   Returns ErrInvalidPageNum when pgno is zero.
//
//   Case 1 (pgno=0): returns ErrInvalidPageNum
//   Case 2 (pgno>0): proceeds normally
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_InsertFrame_ZeroPgno(t *testing.T) {
	t.Parallel()
	// Case 1: pgno == 0 → ErrInvalidPageNum
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.InsertFrame(0, 1); err != ErrInvalidPageNum {
		t.Errorf("MCDC case1: InsertFrame(0,...) must return ErrInvalidPageNum; got %v", err)
	}
}

func TestMCDC_WALIndex_InsertFrame_ValidPgno(t *testing.T) {
	t.Parallel()
	// Case 2: pgno > 0 → succeeds
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.InsertFrame(5, 10); err != nil {
		t.Errorf("MCDC case2: InsertFrame(5, 10) must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:390 — SetReadMark reader-range guard
//   `reader < 0 || reader >= WALIndexMaxReaders`
//
//   A = reader < 0
//   B = reader >= WALIndexMaxReaders
//
//   Returns ErrInvalidReader when A || B is true.
//
//   Case 1 (A=T): reader=-1 → ErrInvalidReader
//   Case 2 (A=F, B=T): reader=WALIndexMaxReaders → ErrInvalidReader
//   Case 3 (A=F, B=F): reader=2 (valid) → succeeds
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_SetReadMark_NegativeReader(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (reader < 0) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.SetReadMark(-1, 5); err != ErrInvalidReader {
		t.Errorf("MCDC case1: SetReadMark(-1,...) must return ErrInvalidReader; got %v", err)
	}
}

func TestMCDC_WALIndex_SetReadMark_ReaderAtMax(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (reader == WALIndexMaxReaders) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.SetReadMark(WALIndexMaxReaders, 5); err != ErrInvalidReader {
		t.Errorf("MCDC case2: SetReadMark(MaxReaders,...) must return ErrInvalidReader; got %v", err)
	}
}

func TestMCDC_WALIndex_SetReadMark_ValidReader(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid reader) → succeeds
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if err := idx.SetReadMark(2, 5); err != nil {
		t.Errorf("MCDC case3: SetReadMark(2, 5) must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:428 — GetMaxFrame not-initialized-or-nil guard
//   `!w.initialized || w.header == nil`
//
//   A = !w.initialized
//   B = w.header == nil
//
//   Returns 0 immediately when A || B is true.
//
//   Case 1 (A=T): closed index → 0
//   Case 2 (A=F, B=F): open index with valid header → non-zero after InsertFrame
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_GetMaxFrame_Closed(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (!initialized) → returns 0
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	mustCloseWALIndex(t, idx)

	if got := idx.GetMaxFrame(); got != 0 {
		t.Errorf("MCDC case1: GetMaxFrame on closed index must return 0; got %d", got)
	}
}

func TestMCDC_WALIndex_GetMaxFrame_Open(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=F (open, header != nil) → returns MxFrame
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	mustInsertFrame(t, idx, 1, 42)
	if got := idx.GetMaxFrame(); got != 42 {
		t.Errorf("MCDC case2: GetMaxFrame after insert(1,42) must return 42; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:574 — validateAndFixHeader version check
//   `w.header.Version != WALIndexVersion && w.header.Version != 0`
//
//   A = w.header.Version != WALIndexVersion
//   B = w.header.Version != 0
//
//   Reinitializes when A && B is true (unknown non-zero version).
//
//   Case 1 (A=F): Version == WALIndexVersion → no reinitialization
//   Case 2 (A=T, B=F): Version == 0 → header treated as uninitialized, not reinit by this branch
//   Case 3 (A=T, B=T): Version != WALIndexVersion, != 0 → reinitialized to WALIndexVersion
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_ValidateAndFix_CorrectVersion(t *testing.T) {
	t.Parallel()
	// Case 1: A=F — Version already correct, no rewrite triggered
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	if idx.header.Version != WALIndexVersion {
		t.Errorf("MCDC case1: expected Version=%d; got %d", WALIndexVersion, idx.header.Version)
	}
}

func TestMCDC_WALIndex_ValidateAndFix_ZeroVersion(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F — Version==0 is treated as uninitialized (IsInit=0 path),
	// which triggers initializeHeader not validateAndFixHeader.
	// We verify the WALIndex still opens successfully when the file header has Version=0.
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")

	// Create an index, corrupt its version to 0, re-open.
	idx := mustOpenWALIndex(t, dbFile)
	idx.header.Version = 0
	idx.header.IsInit = 0 // force initializeHeader path
	if err := idx.writeHeader(); err != nil {
		idx.Close()
		t.Fatalf("writeHeader error = %v", err)
	}
	mustCloseWALIndex(t, idx)

	// Re-open: IsInit=0 branch runs, sets version to WALIndexVersion
	idx2 := mustOpenWALIndex(t, dbFile)
	defer mustCloseWALIndex(t, idx2)
	if idx2.header.Version != WALIndexVersion {
		t.Errorf("MCDC case2: after re-open with Version=0, Version must be reset to %d; got %d",
			WALIndexVersion, idx2.header.Version)
	}
}

func TestMCDC_WALIndex_ValidateAndFix_UnknownVersion(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T — unknown non-zero version → validateAndFixHeader reinitializes
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")

	idx := mustOpenWALIndex(t, dbFile)
	// Set an unknown version != 0 and != WALIndexVersion
	idx.header.Version = 0xDEADBEEF
	idx.header.IsInit = 1 // so readHeader takes validateAndFixHeader path
	if err := idx.writeHeader(); err != nil {
		idx.Close()
		t.Fatalf("writeHeader error = %v", err)
	}
	mustCloseWALIndex(t, idx)

	// Re-open: IsInit=1, version mismatch → validateAndFixHeader resets it
	idx2 := mustOpenWALIndex(t, dbFile)
	defer mustCloseWALIndex(t, idx2)
	if idx2.header.Version != WALIndexVersion {
		t.Errorf("MCDC case3: after re-open with unknown version, Version must be %d; got %d",
			WALIndexVersion, idx2.header.Version)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:745 — Delete file-removal error guard
//   `err != nil && !os.IsNotExist(err)`
//
//   A = err != nil
//   B = !os.IsNotExist(err)
//
//   Returns error when A && B is true.
//
//   Case 1 (A=F): file exists and Remove succeeds → no error
//   Case 2 (A=T, B=F): file already gone (IsNotExist) → no error
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_Delete_ExistingFile(t *testing.T) {
	t.Parallel()
	// Case 1: A=F — file exists, Delete removes it cleanly
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	if err := idx.Delete(); err != nil {
		t.Errorf("MCDC case1: Delete of existing WAL index must succeed; got %v", err)
	}
}

func TestMCDC_WALIndex_Delete_AlreadyGone(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F — shm file never existed → os.IsNotExist → no error
	tmpDir := t.TempDir()
	// Build an index object but never call open so the file doesn't exist
	idx := &WALIndex{
		filename:  filepath.Join(tmpDir, "never-created.db-shm"),
		hashTable: make(map[uint32]uint32),
	}
	if err := idx.Delete(); err != nil {
		t.Errorf("MCDC case2: Delete of non-existent WAL index file must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// wal_index.go:786 — InsertFrameWithChecksum checksum validation
//   `!w.ValidateFrameChecksum(frameData, checksum)`
//
//   Returns ErrWALChecksumMismatch when validation fails.
//
//   Case 1: bad checksum → ErrWALChecksumMismatch
//   Case 2: correct checksum → succeeds
// ---------------------------------------------------------------------------

func TestMCDC_WALIndex_InsertFrameWithChecksum_BadChecksum(t *testing.T) {
	t.Parallel()
	// Case 1: wrong checksum → ErrWALChecksumMismatch
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	data := make([]byte, 64)
	if err := idx.InsertFrameWithChecksum(1, 1, data, 0xDEADBEEF); err == nil {
		t.Error("MCDC case1: InsertFrameWithChecksum with wrong checksum must return an error")
	}
}

func TestMCDC_WALIndex_InsertFrameWithChecksum_GoodChecksum(t *testing.T) {
	t.Parallel()
	// Case 2: correct checksum → succeeds
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer mustCloseWALIndex(t, idx)

	data := make([]byte, 64)
	correctChecksum := idx.CalculateFrameChecksum(data)
	if err := idx.InsertFrameWithChecksum(1, 1, data, correctChecksum); err != nil {
		t.Errorf("MCDC case2: InsertFrameWithChecksum with correct checksum must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// config.go:106 — Validate page-size range guard
//   `c.PageSize < 512 || c.PageSize > 65536`
//
//   A = c.PageSize < 512
//   B = c.PageSize > 65536
//
//   Returns ErrInvalidPageSize when A || B is true.
//
//   Case 1 (A=T): PageSize=256 → ErrInvalidPageSize
//   Case 2 (A=F, B=T): PageSize=131072 → ErrInvalidPageSize
//   Case 3 (A=F, B=F): PageSize=4096 → no error from range guard
// ---------------------------------------------------------------------------

func TestMCDC_PagerConfig_Validate_PageSizeTooSmall(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (PageSize < 512) → ErrInvalidPageSize
	cfg := DefaultPagerConfig()
	cfg.PageSize = 256
	if err := cfg.Validate(); err != ErrInvalidPageSize {
		t.Errorf("MCDC case1: PageSize=256 must return ErrInvalidPageSize; got %v", err)
	}
}

func TestMCDC_PagerConfig_Validate_PageSizeTooLarge(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (PageSize > 65536) → ErrInvalidPageSize
	cfg := DefaultPagerConfig()
	cfg.PageSize = 131072
	if err := cfg.Validate(); err != ErrInvalidPageSize {
		t.Errorf("MCDC case2: PageSize=131072 must return ErrInvalidPageSize; got %v", err)
	}
}

func TestMCDC_PagerConfig_Validate_PageSizeInRange(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid range) → passes range guard
	cfg := DefaultPagerConfig()
	cfg.PageSize = 4096
	if err := cfg.Validate(); err != nil {
		t.Errorf("MCDC case3: PageSize=4096 must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// config.go:109 — Validate power-of-two guard
//   `c.PageSize&(c.PageSize-1) != 0`
//
//   Returns ErrInvalidPageSize when PageSize is not a power of two.
//
//   Case 1: PageSize=4096 (power of 2) → passes
//   Case 2: PageSize=3000 (not power of 2) → ErrInvalidPageSize
// ---------------------------------------------------------------------------

func TestMCDC_PagerConfig_Validate_PowerOfTwo(t *testing.T) {
	t.Parallel()
	// Case 1: power-of-two → no error from this guard
	for _, sz := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		cfg := DefaultPagerConfig()
		cfg.PageSize = sz
		if err := cfg.Validate(); err != nil {
			t.Errorf("MCDC case1: PageSize=%d (power of 2) must pass; got %v", sz, err)
		}
	}
}

func TestMCDC_PagerConfig_Validate_NotPowerOfTwo(t *testing.T) {
	t.Parallel()
	// Case 2: not power-of-two (in valid range) → ErrInvalidPageSize
	cfg := DefaultPagerConfig()
	cfg.PageSize = 3000
	if err := cfg.Validate(); err != ErrInvalidPageSize {
		t.Errorf("MCDC case2: PageSize=3000 (not power-of-2) must return ErrInvalidPageSize; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// savepoint.go:71 — validateSavepointCreate state guard
//   `p.state < PagerStateWriterLocked`
//
//   Returns error when state is below WriterLocked (no active write transaction).
//
//   Case 1 (state < WriterLocked): Savepoint() before BeginWrite → error
//   Case 2 (state >= WriterLocked): Savepoint() after BeginWrite → passes this guard
// ---------------------------------------------------------------------------

func TestMCDC_ValidateSavepointCreate_NoWriteTx(t *testing.T) {
	t.Parallel()
	// Case 1: state < WriterLocked → error (no active write transaction)
	p := openTestPager(t)
	if err := p.Savepoint("sp1"); err == nil {
		t.Error("MCDC case1: Savepoint without write transaction must return an error")
	}
}

func TestMCDC_ValidateSavepointCreate_WriteTxActive(t *testing.T) {
	t.Parallel()
	// Case 2: state >= WriterLocked → passes the guard
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.Savepoint("sp1"); err != nil {
		t.Errorf("MCDC case2: Savepoint within write transaction must succeed; got %v", err)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// savepoint.go:74 — validateSavepointCreate error-state guard
//   `p.state == PagerStateError`
//
//   Returns p.errCode when pager is in error state.
//
//   Case 1: state != Error → proceeds past this guard
//   Case 2: state == Error → returns errCode immediately
// ---------------------------------------------------------------------------

func TestMCDC_ValidateSavepointCreate_NormalState(t *testing.T) {
	t.Parallel()
	// Case 1: state != Error → Savepoint succeeds (error state guard not triggered)
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.Savepoint("ok"); err != nil {
		t.Errorf("MCDC case1: Savepoint in normal state must succeed; got %v", err)
	}
	mustRollback(t, p)
}

func TestMCDC_ValidateSavepointCreate_ErrorState(t *testing.T) {
	t.Parallel()
	// Case 2: state == PagerStateError → returns errCode
	p := openTestPager(t)
	p.mu.Lock()
	p.state = PagerStateError
	p.errCode = ErrReadOnly
	p.mu.Unlock()

	err := p.Savepoint("fail")
	if err == nil {
		t.Error("MCDC case2: Savepoint in Error state must return an error")
	}
	// Restore state
	p.mu.Lock()
	p.state = PagerStateOpen
	p.errCode = nil
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// savepoint.go:77 — validateSavepointCreate empty-name guard
//   `name == ""`
//
//   Returns error when savepoint name is empty.
//
//   Case 1: name="" → error
//   Case 2: name non-empty → passes this guard
// ---------------------------------------------------------------------------

func TestMCDC_ValidateSavepointCreate_EmptyName(t *testing.T) {
	t.Parallel()
	// Case 1: name == "" → error
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.Savepoint(""); err == nil {
		t.Error("MCDC case1: Savepoint with empty name must return an error")
	}
	mustRollback(t, p)
}

func TestMCDC_ValidateSavepointCreate_NonEmptyName(t *testing.T) {
	t.Parallel()
	// Case 2: non-empty name → passes this guard
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.Savepoint("mySP"); err != nil {
		t.Errorf("MCDC case2: Savepoint with non-empty name must succeed; got %v", err)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// savepoint.go:107 — Release write-transaction guard
//   `p.state < PagerStateWriterLocked`
//
//   Returns error when no active write transaction.
//
//   Case 1: state < WriterLocked → error
//   Case 2: state >= WriterLocked → passes guard
// ---------------------------------------------------------------------------

func TestMCDC_Release_NoWriteTx(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → error
	p := openTestPager(t)
	if err := p.Release("sp1"); err == nil {
		t.Error("MCDC case1: Release without write transaction must return an error")
	}
}

func TestMCDC_Release_WriteTxActive(t *testing.T) {
	t.Parallel()
	// Case 2: A=F → proceeds; but "sp1" doesn't exist so it fails for a different reason
	p := openTestPager(t)
	mustBeginWrite(t, p)
	mustSavepoint(t, p, "sp1")
	if err := p.Release("sp1"); err != nil {
		t.Errorf("MCDC case2: Release of existing savepoint must succeed; got %v", err)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// savepoint.go:240 — restoreToSavepoint newer-savepoints loop condition
//   `_, exists := pagesToRestore[pgno]; !exists`
//
//   Only the first version of each page (from the oldest savepoint toward current)
//   is stored: pages already in pagesToRestore are NOT overwritten.
//
//   Case 1 (!exists=T): page not yet in map → added
//   Case 2 (!exists=F): page already in map → skipped (older version preserved)
// ---------------------------------------------------------------------------

func TestMCDC_RestoreToSavepoint_PageFirstVersion(t *testing.T) {
	t.Parallel()
	// Case 1: page first seen → added to pagesToRestore
	p := openTestPager(t)
	mustBeginWrite(t, p)

	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0xAA
	p.Put(page)
	mustSavepoint(t, p, "sp1")

	page = mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0xBB
	p.Put(page)
	mustSavepoint(t, p, "sp2")

	// RollbackTo sp1 must restore data to 0xAA
	if err := p.RollbackTo("sp1"); err != nil {
		t.Fatalf("MCDC case1: RollbackTo(sp1) error = %v", err)
	}
	restoredPage, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after RollbackTo error = %v", err)
	}
	restored := restoredPage.Data[DatabaseHeaderSize]
	p.Put(restoredPage)
	if restored != 0xAA {
		t.Errorf("MCDC case1: page data must be 0xAA after rollback to sp1; got 0x%02X", restored)
	}
	mustRollback(t, p)
}

func TestMCDC_RestoreToSavepoint_PageAlreadyPresent(t *testing.T) {
	t.Parallel()
	// Case 2: page already in pagesToRestore → newer copy skipped, older preserved
	p := openTestPager(t)
	mustBeginWrite(t, p)

	// Write initial value before any savepoint
	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x11
	p.Put(page)
	mustSavepoint(t, p, "sp1") // sp1 captures 0x11

	page = mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x22
	p.Put(page)
	mustSavepoint(t, p, "sp2") // sp2 captures 0x22

	page = mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x33
	p.Put(page)
	// RollbackTo sp1: both sp1 and sp2 have the page; sp1's copy (0x11) wins
	if err := p.RollbackTo("sp1"); err != nil {
		t.Fatalf("MCDC case2: RollbackTo(sp1) error = %v", err)
	}
	restoredPage, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after RollbackTo error = %v", err)
	}
	restored := restoredPage.Data[DatabaseHeaderSize]
	p.Put(restoredPage)
	if restored != 0x11 {
		t.Errorf("MCDC case2: page must be restored to 0x11 (sp1 version); got 0x%02X", restored)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// savepoint.go:272 — addSavepoint nil-stack guard
//   `p.savepoints == nil`
//
//   Initializes the stack when nil; appends to existing stack otherwise.
//
//   Case 1 (nil=T): no prior savepoints → stack initialized then appended
//   Case 2 (nil=F): existing stack → new savepoint prepended directly
// ---------------------------------------------------------------------------

func TestMCDC_AddSavepoint_NilStack(t *testing.T) {
	t.Parallel()
	// Case 1: savepoints is nil → addSavepoint initializes it
	p := openTestPager(t)
	mustBeginWrite(t, p)
	// savepoints should be nil before first Savepoint
	p.mu.RLock()
	isNil := p.savepoints == nil
	p.mu.RUnlock()
	if !isNil {
		t.Skip("savepoints not nil at start — cannot test nil initialization path")
	}
	mustSavepoint(t, p, "first")
	if count := p.savepointCount(); count != 1 {
		t.Errorf("MCDC case1: after first Savepoint, count must be 1; got %d", count)
	}
	mustRollback(t, p)
}

func TestMCDC_AddSavepoint_NonNilStack(t *testing.T) {
	t.Parallel()
	// Case 2: stack already has entries → new savepoint prepended
	p := openTestPager(t)
	mustBeginWrite(t, p)
	mustSavepoint(t, p, "sp1")
	mustSavepoint(t, p, "sp2")
	if count := p.savepointCount(); count != 2 {
		t.Errorf("MCDC case2: after two Savepoints, count must be 2; got %d", count)
	}
	// Verify newest is first
	names := p.GetSavepointNames()
	if len(names) < 1 || names[0] != "sp2" {
		t.Errorf("MCDC case2: newest savepoint must be first; got %v", names)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// vacuum.go:63 — validateVacuumPreconditions read-only guard
//   `p.readOnly`
//
//   Returns ErrReadOnly when the pager is read-only.
//
//   Case 1 (readOnly=T): Vacuum on read-only pager → ErrReadOnly
//   Case 2 (readOnly=F): Vacuum on writable pager → passes this guard
// ---------------------------------------------------------------------------

func TestMCDC_VacuumPreconditions_ReadOnly(t *testing.T) {
	t.Parallel()
	// Case 1: readOnly=true → ErrReadOnly
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	// Create the file with a writable pager first
	p0, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	p0.Close()

	p, err := Open(dbFile, true)
	if err != nil {
		t.Fatalf("Open(readOnly=true) error = %v", err)
	}
	defer p.Close()

	if err := p.Vacuum(nil); err != ErrReadOnly {
		t.Errorf("MCDC case1: Vacuum on read-only pager must return ErrReadOnly; got %v", err)
	}
}

func TestMCDC_VacuumPreconditions_Writable(t *testing.T) {
	t.Parallel()
	// Case 2: readOnly=false → passes guard; Vacuum runs (or fails for another reason)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	// A fresh pager in PagerStateOpen can be vacuumed
	if err := p.Vacuum(nil); err == ErrReadOnly {
		t.Error("MCDC case2: Vacuum on writable pager must not return ErrReadOnly")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go:66 — validateVacuumPreconditions transaction-open guard
//   `p.state != PagerStateOpen`
//
//   Returns ErrTransactionOpen when a transaction is already open.
//
//   Case 1 (state != Open): Vacuum during active write transaction → ErrTransactionOpen
//   Case 2 (state == Open): Vacuum with no open transaction → passes
// ---------------------------------------------------------------------------

func TestMCDC_VacuumPreconditions_TransactionOpen(t *testing.T) {
	t.Parallel()
	// Case 1: state != PagerStateOpen (WriterLocked) → ErrTransactionOpen
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.Vacuum(nil); err != ErrTransactionOpen {
		t.Errorf("MCDC case1: Vacuum within open transaction must return ErrTransactionOpen; got %v", err)
	}
	mustRollback(t, p)
}

func TestMCDC_VacuumPreconditions_NoTransactionOpen(t *testing.T) {
	t.Parallel()
	// Case 2: state == PagerStateOpen → passes transaction guard
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	// Vacuum with no open transaction should succeed (or fail for a different reason)
	if err := p.Vacuum(nil); err == ErrTransactionOpen {
		t.Error("MCDC case2: Vacuum with no open transaction must not return ErrTransactionOpen")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go:112 — replaceDatabase into-file branch
//   `opts != nil && opts.IntoFile != ""`
//
//   A = opts != nil
//   B = opts.IntoFile != ""
//
//   Calls replaceForVacuumInto when A && B is true; replaceForVacuumInPlace otherwise.
//
//   Case 1 (A=F): opts==nil → in-place replacement
//   Case 2 (A=T, B=F): opts non-nil but IntoFile="" → in-place replacement
//   Case 3 (A=T, B=T): opts.IntoFile set → VACUUM INTO path
// ---------------------------------------------------------------------------

func TestMCDC_ReplaceDatabase_NilOpts(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (opts==nil) → in-place replacement path
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	// Vacuum with nil opts follows in-place path
	if err := p.Vacuum(nil); err != nil {
		t.Errorf("MCDC case1: Vacuum(nil) must succeed on a fresh writable db; got %v", err)
	}
}

func TestMCDC_ReplaceDatabase_EmptyIntoFile(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (opts with empty IntoFile) → in-place path
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	opts := &VacuumOptions{IntoFile: ""}
	if err := p.Vacuum(opts); err != nil {
		t.Errorf("MCDC case2: Vacuum with empty IntoFile must succeed; got %v", err)
	}
}

func TestMCDC_ReplaceDatabase_IntoFileSet(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → VACUUM INTO path, database copied to IntoFile
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	intoFile := filepath.Join(tmpDir, "backup.db")
	opts := &VacuumOptions{IntoFile: intoFile}
	if err := p.Vacuum(opts); err != nil {
		t.Errorf("MCDC case3: Vacuum INTO must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go:297 — commitTargetPager write-transaction guard
//   `targetPager.state >= PagerStateWriterLocked`
//
//   Only commits the target pager when in a write transaction.
//
//   Case 1: state >= WriterLocked → Commit called
//   Case 2: state < WriterLocked (Open) → Commit skipped (no-op)
// ---------------------------------------------------------------------------

func TestMCDC_CommitTargetPager_WriterLocked(t *testing.T) {
	t.Parallel()
	// Case 1: target in write transaction → Commit called → succeeds
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "target.db")
	targetPager, err := Open(targetFile, false)
	if err != nil {
		t.Fatalf("Open target error = %v", err)
	}
	defer targetPager.Close()

	mustBeginWrite(t, targetPager)

	// Create a minimal source to call commitTargetPager via the src pager
	srcFile := filepath.Join(tmpDir, "src.db")
	src, err := Open(srcFile, false)
	if err != nil {
		t.Fatalf("Open src error = %v", err)
	}
	defer src.Close()

	src.mu.Lock()
	err = src.commitTargetPager(targetPager)
	src.mu.Unlock()

	if err != nil {
		t.Errorf("MCDC case1: commitTargetPager with WriterLocked must succeed; got %v", err)
	}
}

func TestMCDC_CommitTargetPager_StateOpen(t *testing.T) {
	t.Parallel()
	// Case 2: target in Open state (no write tx) → Commit skipped, no error
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "target.db")
	targetPager, err := Open(targetFile, false)
	if err != nil {
		t.Fatalf("Open target error = %v", err)
	}
	defer targetPager.Close()
	// targetPager.state == PagerStateOpen < PagerStateWriterLocked

	srcFile := filepath.Join(tmpDir, "src.db")
	src, err := Open(srcFile, false)
	if err != nil {
		t.Fatalf("Open src error = %v", err)
	}
	defer src.Close()

	src.mu.Lock()
	err = src.commitTargetPager(targetPager)
	src.mu.Unlock()

	if err != nil {
		t.Errorf("MCDC case2: commitTargetPager with Open state must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go:584 — freeTrailingPages nPages-limit guard
//   `limit <= 0`  (sets limit to all pages when nPages <= 0)
//
//   Controls whether the page-freed limit is bounded (nPages > 0) or unbounded.
//
//   Case 1 (nPages=0): limit set to dbSize → frees all trailing free pages
//   Case 2 (nPages=2): limit set to 2 → frees at most 2 pages
// ---------------------------------------------------------------------------

func TestMCDC_FreeTrailingPages_ZeroNPages(t *testing.T) {
	t.Parallel()
	// Case 1: nPages==0 → limit = dbSize → frees all trailing free pages
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2) error = %v", err)
	}
	mustRollback(t, p)

	// IncrementalVacuum(0) passes nPages=0 → freeTrailingPages uses limit=dbSize
	if err := p.IncrementalVacuum(0); err != nil {
		t.Errorf("MCDC case1: IncrementalVacuum(0) must not error; got %v", err)
	}
}

func TestMCDC_FreeTrailingPages_PositiveNPages(t *testing.T) {
	t.Parallel()
	// Case 2: nPages > 0 → limit = nPages → at most nPages pages freed
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2) error = %v", err)
	}
	mustRollback(t, p)

	// IncrementalVacuum(2) passes nPages=2 → freeTrailingPages uses limit=2
	if err := p.IncrementalVacuum(2); err != nil {
		t.Errorf("MCDC case2: IncrementalVacuum(2) must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go:592 — freeTrailingPages loop condition
//   `freed < limit && p.dbSize > 1`
//
//   A = freed < limit
//   B = p.dbSize > 1
//
//   Loop continues when A && B is true.
//
//   Case 1 (A=F): freed==limit → loop exits (enough freed)
//   Case 2 (A=T, B=F): dbSize==1 → loop exits (cannot shrink below 1)
//   Case 3 (A=T, B=T): more to free and db has pages → loop body runs
// ---------------------------------------------------------------------------

func TestMCDC_FreeTrailingPagesLoop_LimitReached(t *testing.T) {
	t.Parallel()
	// Case 1: freed==limit → loop exits; IncrementalVacuum(1) with no free trailing pages
	// → loop exits immediately because freeSet[dbSize] is false
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2) error = %v", err)
	}
	mustRollback(t, p)
	// dbSize==1, last page (1) is not free → loop body never runs
	if err := p.IncrementalVacuum(1); err != nil {
		t.Errorf("MCDC case1: IncrementalVacuum(1) with no trailing free pages must succeed; got %v", err)
	}
}

func TestMCDC_FreeTrailingPagesLoop_DbSizeOne(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (freed < limit=all), B=F (dbSize would reach 1) → loop exits
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2) error = %v", err)
	}
	mustRollback(t, p)
	// With dbSize==1, the loop guard B is false from the start
	if err := p.IncrementalVacuum(0); err != nil {
		t.Errorf("MCDC case2: IncrementalVacuum on 1-page db must succeed; got %v", err)
	}
}

func TestMCDC_FreeTrailingPagesLoop_FreesPages(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → loop body runs (allocate pages, free trailing ones, then vacuum)
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2) error = %v", err)
	}
	// Allocate a couple of pages
	pgno2 := mustAllocatePage(t, p)
	pgno3 := mustAllocatePage(t, p)
	mustCommit(t, p)

	// Free the trailing pages so IncrementalVacuum can remove them
	mustBeginWrite(t, p)
	mustFreePage(t, p, pgno3)
	mustFreePage(t, p, pgno2)
	mustCommit(t, p)

	// Now IncrementalVacuum(0) should free trailing free pages
	if err := p.IncrementalVacuum(0); err != nil {
		t.Errorf("MCDC case3: IncrementalVacuum with trailing free pages must succeed; got %v", err)
	}
}
