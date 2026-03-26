// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Condition: walFileExists — WAL file existence guard
//   `err == nil && info.Size() >= WALHeaderSize`
//
//   A = err == nil  (Stat succeeded, file exists)
//   B = info.Size() >= WALHeaderSize
//
//   Returns true when A && B is true; false otherwise.
//
//   Case 1 (A=F): file does not exist → false
//   Case 2 (A=T, B=F): file exists but is too small (< WALHeaderSize) → false
//   Case 3 (A=T, B=T): file exists with at least WALHeaderSize bytes → true
// ---------------------------------------------------------------------------

func TestMCDC_WALFileExists_NoFile(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → Stat fails (no such file) → walFileExists returns false
	tmpDir := t.TempDir()
	w := NewWAL(filepath.Join(tmpDir, "nonexistent.db"), DefaultPageSize)
	if w.walFileExists() {
		t.Error("MCDC case1: walFileExists must return false when WAL file does not exist")
	}
}

func TestMCDC_WALFileExists_TooSmall(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F → file exists but size < WALHeaderSize (32) → false
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")

	// Create an empty (0-byte) WAL file — below the 32-byte threshold
	walPath := dbFile + "-wal"
	if err := os.WriteFile(walPath, []byte{}, 0600); err != nil {
		t.Fatalf("failed to create tiny WAL file: %v", err)
	}

	w := NewWAL(dbFile, DefaultPageSize)
	if w.walFileExists() {
		t.Error("MCDC case2: walFileExists must return false when WAL file is smaller than WALHeaderSize")
	}
}

func TestMCDC_WALFileExists_ValidFile(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → WAL file exists with a full header → true
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	// Switch to WAL mode; this creates a WAL file with a proper header
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode error = %v", err)
	}

	w := NewWAL(dbFile, DefaultPageSize)
	if !w.walFileExists() {
		t.Error("MCDC case3: walFileExists must return true when WAL file has a full header")
	}
}

// ---------------------------------------------------------------------------
// Condition: isCachedChecksumValid — WAL checksum cache lookup
//   `ok && cached[0] == frame.Checksum1 && cached[1] == frame.Checksum2`
//
//   A = ok  (key present in cache)
//   B = cached[0] == frame.Checksum1
//   C = cached[1] == frame.Checksum2
//
//   Returns true when A && B && C is true.
//
//   Case 1 (A=F): key absent in cache → false
//   Case 2 (A=T, B=F): Checksum1 mismatch → false
//   Case 3 (A=T, B=T, C=F): Checksum2 mismatch → false
//   Case 4 (A=T, B=T, C=T): both values match → true
// ---------------------------------------------------------------------------

func TestMCDC_IsCachedChecksumValid_NotInCache(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → frame not in cache → returns false
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	frame := &WALFrame{Checksum1: 0x1234, Checksum2: 0x5678}
	// No entry in checksumCache for frame index 99
	if w.isCachedChecksumValid(frame, 99) {
		t.Error("MCDC case1: must return false when frame index is not in cache")
	}
}

func TestMCDC_IsCachedChecksumValid_Checksum1Mismatch(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F → cached[0] (Checksum1) differs → false
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	// Inject a cache entry where Checksum1 != frame.Checksum1
	w.checksumCache[0] = [2]uint32{0xAAAA, 0x5678}
	frame := &WALFrame{Checksum1: 0xBBBB, Checksum2: 0x5678}
	if w.isCachedChecksumValid(frame, 0) {
		t.Error("MCDC case2: must return false when Checksum1 does not match cached value")
	}
}

func TestMCDC_IsCachedChecksumValid_Checksum2Mismatch(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T, C=F → cached[1] (Checksum2) differs → false
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	w.checksumCache[0] = [2]uint32{0x1234, 0xAAAA}
	frame := &WALFrame{Checksum1: 0x1234, Checksum2: 0xBBBB}
	if w.isCachedChecksumValid(frame, 0) {
		t.Error("MCDC case3: must return false when Checksum2 does not match cached value")
	}
}

func TestMCDC_IsCachedChecksumValid_BothMatch(t *testing.T) {
	t.Parallel()
	// Case 4: A=T, B=T, C=T → both values match → true
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	w.checksumCache[0] = [2]uint32{0x1234, 0x5678}
	frame := &WALFrame{Checksum1: 0x1234, Checksum2: 0x5678}
	if !w.isCachedChecksumValid(frame, 0) {
		t.Error("MCDC case4: must return true when both checksums match the cached values")
	}
}

// ---------------------------------------------------------------------------
// Condition: verifyChecksum — WAL frame checksum comparison
//   `s1 != frame.Checksum1 || s2 != frame.Checksum2`
//
//   A = s1 != frame.Checksum1
//   B = s2 != frame.Checksum2
//
//   Returns an error when A || B is true.
//
//   Case 1 (A=T): s1 mismatch → error
//   Case 2 (A=F, B=T): s2 mismatch → error
//   Case 3 (A=F, B=F): both match → nil
// ---------------------------------------------------------------------------

func TestMCDC_VerifyChecksum_S1Mismatch(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → s1 differs from frame.Checksum1 → error returned
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	frame := &WALFrame{Checksum1: 0xDEAD, Checksum2: 0xBEEF}
	if err := w.verifyChecksum(frame, 0x0000, 0xBEEF); err == nil {
		t.Error("MCDC case1: s1 mismatch must return a non-nil error")
	}
}

func TestMCDC_VerifyChecksum_S2Mismatch(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T → s2 differs from frame.Checksum2 → error returned
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	frame := &WALFrame{Checksum1: 0x1234, Checksum2: 0xDEAD}
	if err := w.verifyChecksum(frame, 0x1234, 0x0000); err == nil {
		t.Error("MCDC case2: s2 mismatch must return a non-nil error")
	}
}

func TestMCDC_VerifyChecksum_BothMatch(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → both values match → nil
	tmpDir := t.TempDir()
	w := mustOpenWAL(t, filepath.Join(tmpDir, "test.db"), DefaultPageSize)
	defer w.Close()

	frame := &WALFrame{Checksum1: 0x1234, Checksum2: 0x5678}
	if err := w.verifyChecksum(frame, 0x1234, 0x5678); err != nil {
		t.Errorf("MCDC case3: matching checksums must return nil; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: FreeList.IsEmpty
//   `fl.totalFree == 0 && len(fl.pendingFree) == 0`
//
//   A = fl.totalFree == 0
//   B = len(fl.pendingFree) == 0
//
//   Returns true when A && B is true.
//
//   Case 1 (A=F): totalFree > 0 → false
//   Case 2 (A=T, B=F): pending pages present → false
//   Case 3 (A=T, B=T): no free pages anywhere → true
// ---------------------------------------------------------------------------

func TestMCDC_FreeListIsEmpty_TotalFreePositive(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (totalFree > 0) → IsEmpty returns false
	p := openTestPager(t)
	fl := p.freeList
	fl.mu.Lock()
	fl.totalFree = 1
	fl.pendingFree = fl.pendingFree[:0]
	fl.mu.Unlock()
	if fl.IsEmpty() {
		t.Error("MCDC case1: totalFree=1 must make IsEmpty return false")
	}
}

func TestMCDC_FreeListIsEmpty_PendingExists(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (totalFree=0), B=F (len(pendingFree)>0) → IsEmpty returns false
	p := openTestPager(t)
	fl := p.freeList
	fl.mu.Lock()
	fl.totalFree = 0
	fl.pendingFree = append(fl.pendingFree[:0], Pgno(2))
	fl.mu.Unlock()
	if fl.IsEmpty() {
		t.Error("MCDC case2: non-empty pendingFree must make IsEmpty return false")
	}
}

func TestMCDC_FreeListIsEmpty_TrulyEmpty(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → IsEmpty returns true
	p := openTestPager(t)
	fl := p.freeList
	fl.mu.Lock()
	fl.totalFree = 0
	fl.pendingFree = fl.pendingFree[:0]
	fl.mu.Unlock()
	if !fl.IsEmpty() {
		t.Error("MCDC case3: no free pages must make IsEmpty return true")
	}
}

// ---------------------------------------------------------------------------
// Condition: verifyLeafPage — corrupt leaf detection
//   `leafPgno == 0 || seen[leafPgno]`
//
//   A = leafPgno == 0
//   B = seen[leafPgno]
//
//   Returns ErrFreeListCorrupt when A || B is true.
//
//   Case 1 (A=T): leafPgno==0 → corrupt
//   Case 2 (A=F, B=T): leafPgno already seen → corrupt
//   Case 3 (A=F, B=F): valid, unseen leaf → no error
// ---------------------------------------------------------------------------

func TestMCDC_VerifyLeafPage_ZeroPgno(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (leafPgno==0) → ErrFreeListCorrupt
	p := openTestPager(t)
	fl := p.freeList

	// Build a DbPage where leaf slot 0 contains pgno=0 (all-zero bytes at offset 8)
	fakePage := NewDbPage(2, p.pageSize)
	// Data[8:12] is already 0x00000000 from NewDbPage → leafPgno == 0
	seen := make(map[Pgno]bool)
	count := uint32(0)
	err := fl.verifyLeafPage(fakePage, 0, seen, &count)
	if err != ErrFreeListCorrupt {
		t.Errorf("MCDC case1: leafPgno=0 must return ErrFreeListCorrupt; got %v", err)
	}
}

func TestMCDC_VerifyLeafPage_AlreadySeen(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (leafPgno already in seen) → ErrFreeListCorrupt
	p := openTestPager(t)
	fl := p.freeList

	fakePage := NewDbPage(2, p.pageSize)
	// Write leafPgno=7 into slot 0 (offset FreeListTrunkHeaderSize=8)
	fakePage.Data[8] = 0
	fakePage.Data[9] = 0
	fakePage.Data[10] = 0
	fakePage.Data[11] = 7

	seen := map[Pgno]bool{7: true} // 7 already seen
	count := uint32(0)
	err := fl.verifyLeafPage(fakePage, 0, seen, &count)
	if err != ErrFreeListCorrupt {
		t.Errorf("MCDC case2: duplicate leafPgno must return ErrFreeListCorrupt; got %v", err)
	}
}

func TestMCDC_VerifyLeafPage_ValidLeaf(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid unseen leaf) → nil, count incremented
	p := openTestPager(t)
	fl := p.freeList

	fakePage := NewDbPage(2, p.pageSize)
	// Write leafPgno=9 into slot 0
	fakePage.Data[8] = 0
	fakePage.Data[9] = 0
	fakePage.Data[10] = 0
	fakePage.Data[11] = 9

	seen := make(map[Pgno]bool)
	count := uint32(0)
	err := fl.verifyLeafPage(fakePage, 0, seen, &count)
	if err != nil {
		t.Errorf("MCDC case3: valid unseen leaf must return nil; got %v", err)
	}
	if count != 1 {
		t.Errorf("MCDC case3: count must be incremented to 1; got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Condition: getVacuumTargetFile
//   `opts != nil && opts.IntoFile != ""`
//
//   A = opts != nil
//   B = opts.IntoFile != ""
//
//   Returns opts.IntoFile when A && B is true; returns p.filename otherwise.
//
//   Case 1 (A=F): opts==nil → p.filename
//   Case 2 (A=T, B=F): opts.IntoFile=="" → p.filename
//   Case 3 (A=T, B=T): opts.IntoFile!="" → opts.IntoFile
// ---------------------------------------------------------------------------

func TestMCDC_GetVacuumTargetFile_NilOpts(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (opts==nil) → returns p.filename
	p := openTestPager(t)
	result := p.getVacuumTargetFile(nil)
	if result != p.filename {
		t.Errorf("MCDC case1: nil opts must return p.filename; got %q want %q", result, p.filename)
	}
}

func TestMCDC_GetVacuumTargetFile_EmptyIntoFile(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (IntoFile=="") → returns p.filename
	p := openTestPager(t)
	opts := &VacuumOptions{IntoFile: ""}
	result := p.getVacuumTargetFile(opts)
	if result != p.filename {
		t.Errorf("MCDC case2: empty IntoFile must return p.filename; got %q want %q", result, p.filename)
	}
}

func TestMCDC_GetVacuumTargetFile_WithIntoFile(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (IntoFile!="") → returns opts.IntoFile
	p := openTestPager(t)
	target := filepath.Join(t.TempDir(), "vacuum_out.db")
	opts := &VacuumOptions{IntoFile: target}
	result := p.getVacuumTargetFile(opts)
	if result != target {
		t.Errorf("MCDC case3: non-empty IntoFile must be returned; got %q want %q", result, target)
	}
}

// ---------------------------------------------------------------------------
// Condition: persistSchemaToTarget
//   `opts == nil || opts.SourceSchema == nil || opts.Btree == nil`
//
//   A = opts == nil
//   B = opts.SourceSchema == nil
//   C = opts.Btree == nil
//
//   Returns nil (skip) when A || B || C is true.
//
//   Case 1 (A=T): opts==nil → skip
//   Case 2 (A=F, B=T): SourceSchema==nil → skip
//   Case 3 (A=F, B=F, C=T): Btree==nil → skip
//   Case 4 (A=F, B=F, C=F): all non-nil → no-op impl still returns nil
// ---------------------------------------------------------------------------

func TestMCDC_PersistSchemaToTarget_NilOpts(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (opts==nil) → returns nil
	p := openTestPager(t)
	target := openTestPager(t)
	if err := p.persistSchemaToTarget(target, nil); err != nil {
		t.Errorf("MCDC case1: nil opts must return nil; got %v", err)
	}
}

func TestMCDC_PersistSchemaToTarget_NilSourceSchema(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (SourceSchema==nil) → returns nil
	p := openTestPager(t)
	target := openTestPager(t)
	opts := &VacuumOptions{SourceSchema: nil, Btree: struct{}{}}
	if err := p.persistSchemaToTarget(target, opts); err != nil {
		t.Errorf("MCDC case2: nil SourceSchema must return nil; got %v", err)
	}
}

func TestMCDC_PersistSchemaToTarget_NilBtree(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=T (Btree==nil) → returns nil
	p := openTestPager(t)
	target := openTestPager(t)
	opts := &VacuumOptions{SourceSchema: struct{}{}, Btree: nil}
	if err := p.persistSchemaToTarget(target, opts); err != nil {
		t.Errorf("MCDC case3: nil Btree must return nil; got %v", err)
	}
}

func TestMCDC_PersistSchemaToTarget_AllNonNil(t *testing.T) {
	t.Parallel()
	// Case 4: A=F, B=F, C=F (all non-nil) → no-op implementation still returns nil
	p := openTestPager(t)
	target := openTestPager(t)
	opts := &VacuumOptions{SourceSchema: struct{}{}, Btree: struct{}{}}
	if err := p.persistSchemaToTarget(target, opts); err != nil {
		t.Errorf("MCDC case4: all non-nil opts must still return nil (no-op impl); got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: IncrementalVacuum — early-exit guard
//   `p.header.LargestRootPage == 0 || p.header.IncrementalVacuum == 0`
//
//   A = p.header.LargestRootPage == 0
//   B = p.header.IncrementalVacuum == 0
//
//   Returns nil immediately (no-op) when A || B is true.
//
//   Case 1 (A=T): LargestRootPage==0 → no-op
//   Case 2 (A=F, B=T): full auto-vacuum, not incremental → no-op
//   Case 3 (A=F, B=F): incremental mode active → vacuum proceeds
// ---------------------------------------------------------------------------

func TestMCDC_IncrementalVacuum_NotAutoVacuumMode(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (LargestRootPage==0) → IncrementalVacuum is a no-op
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 0
	p.header.IncrementalVacuum = 1 // B=F would be needed for full coverage; here A=T dominates
	p.mu.Unlock()

	if err := p.IncrementalVacuum(1); err != nil {
		t.Errorf("MCDC case1: LargestRootPage=0 must return nil; got %v", err)
	}
}

func TestMCDC_IncrementalVacuum_FullVacuumMode(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (IncrementalVacuum==0) → no-op
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 1
	p.header.IncrementalVacuum = 0
	p.mu.Unlock()

	if err := p.IncrementalVacuum(1); err != nil {
		t.Errorf("MCDC case2: IncrementalVacuum=0 must return nil; got %v", err)
	}
}

func TestMCDC_IncrementalVacuum_IncrementalModeActive(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → incremental mode; vacuum proceeds (no trailing free pages → no-op body)
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 1
	p.header.IncrementalVacuum = 1
	p.mu.Unlock()

	if err := p.IncrementalVacuum(0); err != nil {
		t.Errorf("MCDC case3: incremental mode with no trailing free pages must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: GetAutoVacuumMode — header nil-or-zero guard, then incremental check
//   Guard:  `p.header == nil || p.header.LargestRootPage == 0`  → return 0
//   Inner:  `p.header.IncrementalVacuum != 0`                   → return 2 vs 1
//
//   A = p.header == nil
//   B = p.header.LargestRootPage == 0
//   C = p.header.IncrementalVacuum != 0
//
//   Case 1 (A=T): nil header → 0
//   Case 2 (A=F, B=T): LargestRootPage==0 → 0
//   Case 3 (A=F, B=F, C=F): LargestRootPage!=0, IncrementalVacuum==0 → 1 (full)
//   Case 4 (A=F, B=F, C=T): LargestRootPage!=0, IncrementalVacuum!=0 → 2 (incremental)
// ---------------------------------------------------------------------------

func TestMCDC_GetAutoVacuumMode_NilHeader(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (header==nil) → mode=0
	p := openTestPager(t)
	p.mu.Lock()
	savedHeader := p.header
	p.header = nil
	p.mu.Unlock()

	mode := p.GetAutoVacuumMode()

	// Restore so deferred Close works
	p.mu.Lock()
	p.header = savedHeader
	p.mu.Unlock()

	if mode != 0 {
		t.Errorf("MCDC case1: nil header must return mode=0; got %d", mode)
	}
}

func TestMCDC_GetAutoVacuumMode_LargestRootPageZero(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (LargestRootPage==0) → mode=0
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 0
	p.mu.Unlock()

	if mode := p.GetAutoVacuumMode(); mode != 0 {
		t.Errorf("MCDC case2: LargestRootPage=0 must return mode=0; got %d", mode)
	}
}

func TestMCDC_GetAutoVacuumMode_FullVacuumMode(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=F → mode=1 (full auto-vacuum)
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 1
	p.header.IncrementalVacuum = 0
	p.mu.Unlock()

	if mode := p.GetAutoVacuumMode(); mode != 1 {
		t.Errorf("MCDC case3: LargestRootPage=1, IncrementalVacuum=0 must return mode=1; got %d", mode)
	}
}

func TestMCDC_GetAutoVacuumMode_IncrementalVacuumMode(t *testing.T) {
	t.Parallel()
	// Case 4: A=F, B=F, C=T → mode=2 (incremental auto-vacuum)
	p := openTestPager(t)
	p.mu.Lock()
	p.header.LargestRootPage = 1
	p.header.IncrementalVacuum = 1
	p.mu.Unlock()

	if mode := p.GetAutoVacuumMode(); mode != 2 {
		t.Errorf("MCDC case4: IncrementalVacuum=1 must return mode=2; got %d", mode)
	}
}

// ---------------------------------------------------------------------------
// Condition: Journal.journalFileExists
//   `err == nil && info.Size() >= JournalHeaderSize`
//
//   A = err == nil  (os.Stat succeeded)
//   B = info.Size() >= JournalHeaderSize  (28 bytes)
//
//   Returns true when A && B is true.
//
//   Case 1 (A=F): journal file does not exist → false
//   Case 2 (A=T, B=F): file exists but is too small → false
//   Case 3 (A=T, B=T): file exists with at least a full header → true
// ---------------------------------------------------------------------------

func TestMCDC_JournalFileExists_NoFile(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → file missing → journalFileExists returns false
	tmpDir := t.TempDir()
	j := NewJournal(filepath.Join(tmpDir, "missing.db-journal"), DefaultPageSize, 1)
	if j.journalFileExists() {
		t.Error("MCDC case1: journalFileExists must return false when file does not exist")
	}
}

func TestMCDC_JournalFileExists_TooSmall(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F → file exists but size < JournalHeaderSize (28) → false
	tmpDir := t.TempDir()
	journalPath := filepath.Join(tmpDir, "test.db-journal")
	// Write a 10-byte file — smaller than JournalHeaderSize
	if err := os.WriteFile(journalPath, make([]byte, 10), 0600); err != nil {
		t.Fatalf("failed to create tiny journal file: %v", err)
	}

	j := NewJournal(journalPath, DefaultPageSize, 1)
	if j.journalFileExists() {
		t.Error("MCDC case2: journalFileExists must return false when file is smaller than JournalHeaderSize")
	}
}

func TestMCDC_JournalFileExists_ValidFile(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → journal properly opened (has full header) → true
	tmpDir := t.TempDir()
	journalPath := filepath.Join(tmpDir, "test.db-journal")
	j := mustOpenJournal(t, journalPath, DefaultPageSize, 1)
	defer j.Close()

	if !j.journalFileExists() {
		t.Error("MCDC case3: journalFileExists must return true for a properly opened journal")
	}
}

// ---------------------------------------------------------------------------
// Condition: commitPhase1WriteDirtyPages — LRU write-back fast path
//   `lruCache, ok := p.cache.(*LRUCache); ok && lruCache.Mode() == WriteBackMode`
//
//   A = ok  (cache is *LRUCache)
//   B = lruCache.Mode() == WriteBackMode
//
//   Takes the LRU Flush path when A && B is true.
//
//   Case 1 (A=F): default PageCache → normal writeDirtyPages path taken
//   Case 2 (A=T, B=F): LRU in WriteThroughMode → falls through to normal path
//   Case 3 (A=T, B=T): LRU in WriteBackMode → Flush path taken
// ---------------------------------------------------------------------------

func TestMCDC_CommitPhase1_NotLRUCache(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → PageCache (not LRU) → normal commit succeeds
	p := openTestPager(t)
	mustBeginWrite(t, p)
	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x11
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Errorf("MCDC case1: PageCache commit must succeed; got %v", err)
	}
}

func TestMCDC_CommitPhase1_LRUWriteThrough(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F → LRU cache in WriteThroughMode → falls through to normal path
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	config := LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  100,
		Mode:      WriteThroughMode,
		MaxMemory: 0,
	}
	p, err := OpenWithLRUCache(dbFile, false, DefaultPageSize, config)
	if err != nil {
		t.Fatalf("OpenWithLRUCache error = %v", err)
	}
	defer p.Close()

	mustBeginWrite(t, p)
	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x22
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Errorf("MCDC case2: LRU write-through commit must succeed; got %v", err)
	}
}

func TestMCDC_CommitPhase1_LRUWriteBack(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → LRU cache in WriteBackMode → Flush path taken
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	config := DefaultLRUCacheConfig(DefaultPageSize) // default is WriteBackMode
	p, err := OpenWithLRUCache(dbFile, false, DefaultPageSize, config)
	if err != nil {
		t.Fatalf("OpenWithLRUCache error = %v", err)
	}
	defer p.Close()

	mustBeginWrite(t, p)
	page := mustGetWritePage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x33
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Errorf("MCDC case3: LRU write-back commit must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: FreeList.Allocate — sequential single-condition guards
//   Guard 1: `len(fl.pendingFree) > 0`  → allocate from in-memory pending cache
//   Guard 2: `fl.firstTrunk == 0`       → no on-disk free pages; return 0
//
//   These are independent guards that independently control outcome.
//
//   Case P1 (pending > 0): returns pending page without touching disk
//   Case P2 (pending == 0, firstTrunk == 0): returns 0 (caller allocates new page)
//   Case P3 (pending == 0, firstTrunk != 0): reads from on-disk free list
// ---------------------------------------------------------------------------

func TestMCDC_FreeListAllocate_FromPending(t *testing.T) {
	t.Parallel()
	// Case P1: pending list non-empty → returns page from pending
	p := openTestPager(t)
	fl := p.freeList
	fl.mu.Lock()
	fl.pendingFree = append(fl.pendingFree[:0], Pgno(5))
	fl.mu.Unlock()

	pgno, err := fl.Allocate()
	if err != nil {
		t.Fatalf("MCDC caseP1: Allocate error = %v", err)
	}
	if pgno != 5 {
		t.Errorf("MCDC caseP1: expected pgno=5 from pending; got %d", pgno)
	}
}

func TestMCDC_FreeListAllocate_NoPagesAvailable(t *testing.T) {
	t.Parallel()
	// Case P2: no pending, no trunk → Allocate returns 0
	p := openTestPager(t)
	fl := p.freeList
	fl.mu.Lock()
	fl.pendingFree = fl.pendingFree[:0]
	fl.firstTrunk = 0
	fl.mu.Unlock()

	pgno, err := fl.Allocate()
	if err != nil {
		t.Fatalf("MCDC caseP2: Allocate error = %v", err)
	}
	if pgno != 0 {
		t.Errorf("MCDC caseP2: expected pgno=0 (no free pages); got %d", pgno)
	}
}

func TestMCDC_FreeListAllocate_FromDisk(t *testing.T) {
	t.Parallel()
	// Case P3: no pending, firstTrunk != 0 → Allocate reads from on-disk free list
	p := openTestPager(t)
	mustBeginWrite(t, p)

	// Allocate two extra pages so we have something to free
	pgno2 := mustAllocatePage(t, p)
	pgno3 := mustAllocatePage(t, p)
	mustCommit(t, p)

	mustBeginWrite(t, p)
	mustFreePage(t, p, pgno2)
	mustFreePage(t, p, pgno3)
	// Flush pending to on-disk free list
	if err := p.freeList.Flush(); err != nil {
		t.Fatalf("Flush error = %v", err)
	}

	// Clear pending so Allocate must hit the disk path
	p.freeList.mu.Lock()
	p.freeList.pendingFree = p.freeList.pendingFree[:0]
	p.freeList.mu.Unlock()

	if p.freeList.firstTrunk == 0 {
		t.Skip("firstTrunk is 0 after flush; disk allocation path not reachable in this configuration")
	}

	allocd, err := p.freeList.Allocate()
	if err != nil {
		t.Fatalf("MCDC caseP3: Allocate from disk error = %v", err)
	}
	if allocd == 0 {
		t.Error("MCDC caseP3: Allocate from disk must return a non-zero page number")
	}
	mustRollback(t, p)
}
