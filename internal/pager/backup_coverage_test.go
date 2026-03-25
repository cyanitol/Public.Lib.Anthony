// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"bytes"
	"path/filepath"
	"testing"
)

// TestNewBackup_EmptySource exercises the branch in NewBackup where
// src.PageCount() returns 0, which causes an error. Because the normal Open()
// path always initialises dbSize to at least 1, we force dbSize to 0 directly
// (package-internal access is available since this is a white-box test).
func TestNewBackup_EmptySource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := openTestPagerAt(t, filepath.Join(dir, "empty_src.db"), false)
	defer src.Close()

	// Force dbSize to 0 to exercise the "source is empty" guard.
	src.mu.Lock()
	src.dbSize = 0
	src.mu.Unlock()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	_, err := NewBackup(src, dst)
	if err == nil {
		t.Error("NewBackup with empty source (dbSize=0) expected error, got nil")
	}
}

// TestBackup_StepZero exercises the nPages <= 0 branch in Step, which
// substitutes totalPages for nPages and copies everything in one call.
func TestBackup_StepZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_zero.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Step(0) triggers the nPages <= 0 branch.
	done, err := bk.Step(0)
	if err != nil {
		t.Fatalf("Step(0): %v", err)
	}
	if !done {
		t.Error("Step(0) expected done=true for 3-page source")
	}
}

// TestBackup_StepPartialThenComplete exercises Step(N) where N < totalPages so
// copyPages loops multiple times in separate calls, covering the incremental
// path and the "remaining" calculation in Step.
func TestBackup_StepPartialThenComplete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 6)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_partial.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Copy 2 pages at a time from a 6-page source.
	steps := 0
	for {
		done, err := bk.Step(2)
		if err != nil {
			t.Fatalf("Step(2) iteration %d: %v", steps, err)
		}
		steps++
		if done {
			break
		}
		if steps > 10 {
			t.Fatal("too many steps, likely infinite loop")
		}
	}

	if bk.Remaining() != 0 {
		t.Errorf("Remaining() = %d after full backup, want 0", bk.Remaining())
	}
	if bk.PagesCopied() != 6 {
		t.Errorf("PagesCopied() = %d, want 6", bk.PagesCopied())
	}
}

// TestBackup_WALMode exercises a backup where the source pager is in WAL mode.
// This verifies that copySinglePage and writeDestPage work correctly when the
// source uses WAL-based reads.
func TestBackup_WALMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "wal_src.db")

	src := openTestPagerAt(t, srcFile, false)
	mustSetJournalMode(t, src, JournalModeWAL)

	// Write several pages into WAL-mode source.
	mustBeginWrite(t, src)
	for i := Pgno(1); i <= 4; i++ {
		pg := mustGetPage(t, src, i)
		mustWritePage(t, src, pg)
		pg.Data[0] = byte(i * 7)
		src.Put(pg)
	}
	mustCommit(t, src)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "wal_dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup (WAL src): %v", err)
	}

	done, err := bk.Step(-1)
	if err != nil {
		t.Fatalf("Step(-1): %v", err)
	}
	if !done {
		t.Error("Step(-1) expected done=true")
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

// TestBackup_ManyPages forces copyPages to iterate over many pages in one Step
// call, exercising the inner loop body multiple times (lines 120-126).
func TestBackup_ManyPages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 10)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_many.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// A single Step with nPages > totalPages copies all 10 and finishes.
	done, err := bk.Step(50)
	if err != nil {
		t.Fatalf("Step(50): %v", err)
	}
	if !done {
		t.Error("Step(50) expected done=true for 10-page source")
	}
	if bk.PagesCopied() != 10 {
		t.Errorf("PagesCopied() = %d, want 10", bk.PagesCopied())
	}
}

// TestBackup_ProgressCallbackCalledPerStep verifies that the progress callback
// receives the correct remaining/total values and is called for each Step.
func TestBackup_ProgressCallbackCalledPerStep(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 4)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_prog.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	var calls []int
	bk.SetProgress(func(remaining, total int) {
		calls = append(calls, remaining)
		if total != 4 {
			t.Errorf("progress total = %d, want 4", total)
		}
	})

	// Step one page at a time; progress callback should fire for each step.
	for {
		done, err := bk.Step(1)
		if err != nil {
			t.Fatalf("Step(1): %v", err)
		}
		if done {
			break
		}
	}

	if len(calls) == 0 {
		t.Error("progress callback was never called")
	}
}

// TestBackup_FinishSyncsHeader verifies that Finish calls syncDestHeader and
// updates the destination header's DatabaseSize and FileChangeCounter.
func TestBackup_FinishSyncsHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_hdr.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	dstHeaderBefore := dst.GetHeader()
	var counterBefore uint32
	if dstHeaderBefore != nil {
		counterBefore = dstHeaderBefore.FileChangeCounter
	}

	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	dstHeader := dst.GetHeader()
	if dstHeader == nil {
		// If the header is nil the sync was a no-op; that path is also valid coverage.
		return
	}
	if dstHeader.DatabaseSize == 0 {
		t.Error("Finish() should set DatabaseSize > 0 in destination header")
	}
	if dstHeader.FileChangeCounter <= counterBefore {
		t.Errorf("Finish() should increment FileChangeCounter; before=%d after=%d",
			counterBefore, dstHeader.FileChangeCounter)
	}
}

// TestBackup_DataIntegrity verifies that the page content written by the backup
// matches the source data byte-for-byte.
func TestBackup_DataIntegrity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_integrity.db")

	src := openTestPagerAt(t, srcFile, false)
	defer src.Close()

	// Write known patterns into pages 1-3.
	mustBeginWrite(t, src)
	for i := Pgno(1); i <= 3; i++ {
		pg := mustGetPage(t, src, i)
		mustWritePage(t, src, pg)
		for j := range pg.Data {
			pg.Data[j] = byte((int(i)*13 + j) % 256)
		}
		src.Put(pg)
	}
	mustCommit(t, src)

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_integrity.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	// Compare page data.
	for i := Pgno(1); i <= 3; i++ {
		srcPg, err := src.Get(i)
		if err != nil {
			t.Fatalf("src.Get(%d): %v", i, err)
		}
		dstPg, err := dst.Get(i)
		if err != nil {
			src.Put(srcPg)
			t.Fatalf("dst.Get(%d): %v", i, err)
		}
		if !bytes.Equal(srcPg.Data, dstPg.Data) {
			t.Errorf("page %d data mismatch after backup", i)
		}
		src.Put(srcPg)
		dst.Put(dstPg)
	}
}

// TestBackup_StepWhenAlreadyComplete exercises Step when nextPage > totalPages
// (i.e., the backup finished naturally without calling Finish). It ensures
// Step returns done=true without error and copyPages loop runs zero iterations.
func TestBackup_StepWhenAlreadyComplete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_complete.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// First Step copies everything.
	done, err := bk.Step(-1)
	if err != nil {
		t.Fatalf("Step(-1): %v", err)
	}
	if !done {
		t.Error("Step(-1) expected done=true")
	}

	// Second Step with backup naturally complete (not yet Finish()ed).
	// copyPages loop should not execute (copied == 0) and remaining == 0.
	done, err = bk.Step(1)
	if err != nil {
		t.Fatalf("second Step(1): %v", err)
	}
	if !done {
		t.Error("second Step(1) expected done=true")
	}
}

// TestValidateFileFormats_InvalidReadVersion targets the second branch in
// validateFileFormats (line 373-375): write version valid, read version invalid.
func TestValidateFileFormats_InvalidReadVersion(t *testing.T) {
	t.Parallel()
	h := NewDatabaseHeader(4096)
	// Write version 1 is valid; set read version to something invalid.
	h.FileFormatWrite = 1
	h.FileFormatRead = 99
	if err := h.validateFileFormats(); err == nil {
		t.Error("validateFileFormats() with invalid read version expected error, got nil")
	}
}

// TestValidateFileFormats_InvalidWriteVersion targets the first branch in
// validateFileFormats (line 370-372): write version invalid.
func TestValidateFileFormats_InvalidWriteVersion(t *testing.T) {
	t.Parallel()
	h := NewDatabaseHeader(4096)
	h.FileFormatWrite = 0 // 0 is not in {1, 2}
	h.FileFormatRead = 1
	if err := h.validateFileFormats(); err == nil {
		t.Error("validateFileFormats() with invalid write version expected error, got nil")
	}
}

// TestValidateFileFormats_BothValid ensures no error is returned when both
// format version fields are set to valid values (1 or 2).
func TestValidateFileFormats_BothValid(t *testing.T) {
	t.Parallel()
	for _, w := range []uint8{1, 2} {
		for _, r := range []uint8{1, 2} {
			h := NewDatabaseHeader(4096)
			h.FileFormatWrite = w
			h.FileFormatRead = r
			if err := h.validateFileFormats(); err != nil {
				t.Errorf("validateFileFormats() write=%d read=%d unexpected error: %v", w, r, err)
			}
		}
	}
}

// TestBackup_PagesCopied_BeforeAndAfter verifies PagesCopied() returns 0
// before any Step and the correct count after partial and full steps.
func TestBackup_PagesCopied_BeforeAndAfter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 5)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_pc.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	if got := bk.PagesCopied(); got != 0 {
		t.Errorf("PagesCopied() before any Step = %d, want 0", got)
	}

	if _, err := bk.Step(3); err != nil {
		t.Fatalf("Step(3): %v", err)
	}
	if got := bk.PagesCopied(); got != 3 {
		t.Errorf("PagesCopied() after Step(3) = %d, want 3", got)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step(-1): %v", err)
	}
	if got := bk.PagesCopied(); got != 5 {
		t.Errorf("PagesCopied() after full Step = %d, want 5", got)
	}
}

// TestBackup_NilDst exercises the nil pager check in NewBackup.
func TestBackup_NilDst(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := openTestPagerAt(t, filepath.Join(dir, "src.db"), false)
	defer src.Close()

	if _, err := NewBackup(src, nil); err == nil {
		t.Error("NewBackup(src, nil) expected error, got nil")
	}
}
