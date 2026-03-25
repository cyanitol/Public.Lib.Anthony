// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// TestBackup2_StepProgressZeroCopied exercises the branch in Step where
// copied == 0 (all pages already copied, nextPage > totalPages) but
// a progress callback is set. The callback must NOT be called in that case.
func TestBackup2_StepProgressZeroCopied(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_prog_zero.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Copy everything first.
	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step(-1): %v", err)
	}

	// Set a progress callback, then Step again when nothing remains to copy.
	// This exercises the `if b.progress != nil && copied > 0` branch with copied==0,
	// ensuring the callback is NOT invoked.
	called := false
	bk.SetProgress(func(remaining, total int) {
		called = true
	})

	done, err := bk.Step(1)
	if err != nil {
		t.Fatalf("second Step(1): %v", err)
	}
	if !done {
		t.Error("second Step(1) expected done=true when all pages already copied")
	}
	if called {
		t.Error("progress callback must not be called when copied==0")
	}
}

// TestBackup2_CopySinglePage_SrcError exercises the error path in copySinglePage
// when src.Get() fails. We manipulate totalPages to be larger than the actual
// source database so the backup tries to read a page that doesn't exist.
func TestBackup2_CopySinglePage_SrcError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_srcerr.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Inflate totalPages beyond what actually exists in src.
	// src has 2 pages; setting totalPages to a very large number forces
	// copySinglePage to call src.Get(pgno) for page 3 which does not exist,
	// triggering ErrInvalidPageNum (pgno > maxPageNum).
	bk.mu.Lock()
	bk.totalPages = Pgno(src.maxPageNum + 1)
	bk.nextPage = Pgno(src.maxPageNum + 1)
	bk.mu.Unlock()

	_, err = bk.Step(1)
	if err == nil {
		t.Error("Step expected error when src.Get fails for out-of-range page, got nil")
	}
}

// TestBackup2_WriteDestPage_WriteError exercises the error branch in writeDestPage
// when dst.Write() returns an error. We mark the destination as read-only after
// creating the Backup so that Write rejects the page.
func TestBackup2_WriteDestPage_WriteError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_writeerr.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Force the destination into a write transaction so Get succeeds,
	// then mark it read-only so Write fails.
	if err := dst.BeginWrite(); err != nil {
		t.Fatalf("dst.BeginWrite: %v", err)
	}
	dst.mu.Lock()
	dst.readOnly = true
	dst.mu.Unlock()

	_, err = bk.Step(1)
	if err == nil {
		t.Error("Step expected error when dst.Write fails (readOnly=true), got nil")
	}

	// Restore for cleanup.
	dst.mu.Lock()
	dst.readOnly = false
	dst.mu.Unlock()
	// Rollback so the pager is in a clean state for Close.
	_ = dst.Rollback()
}

// TestBackup2_SyncDestHeader_NilSrcHeader exercises the early-return branch in
// syncDestHeader where srcHeader is nil.
func TestBackup2_SyncDestHeader_NilSrcHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_nil_src_hdr.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	// Nil out the source header to trigger the first early-return in syncDestHeader.
	src.mu.Lock()
	src.header = nil
	src.mu.Unlock()

	// Finish calls syncDestHeader; with srcHeader==nil it returns nil immediately.
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish with nil src header: %v", err)
	}
}

// TestBackup2_SyncDestHeader_NilDstHeader exercises the second early-return branch
// in syncDestHeader where dstHeader is nil (srcHeader is non-nil).
func TestBackup2_SyncDestHeader_NilDstHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_nil_dst_hdr.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	// Keep src header intact but nil out the destination header.
	dst.mu.Lock()
	dst.header = nil
	dst.mu.Unlock()

	// Finish calls syncDestHeader; srcHeader is non-nil but dstHeader is nil,
	// so it returns nil at the second guard.
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish with nil dst header: %v", err)
	}
}

// TestBackup2_CopyPages_LoopSkipped exercises the copyPages branch where the
// loop body never executes because nextPage already exceeds totalPages when
// copyPages is first entered. This also covers the `remaining < 0` clamp in Step.
func TestBackup2_CopyPages_LoopSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_loopskip.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Move nextPage past totalPages so the loop in copyPages has zero iterations.
	bk.mu.Lock()
	bk.nextPage = bk.totalPages + 10
	bk.mu.Unlock()

	done, err := bk.Step(5)
	if err != nil {
		t.Fatalf("Step with nextPage>totalPages: %v", err)
	}
	if !done {
		t.Error("Step expected done=true when nextPage already past totalPages")
	}
	if bk.Remaining() != 0 {
		t.Errorf("Remaining() = %d, want 0", bk.Remaining())
	}
}

// TestBackup2_Step_NGtRemaining exercises the branch in Step where N > remaining
// pages. copyPages should stop after copying only the remaining pages (not N).
func TestBackup2_Step_NGtRemaining(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst_ngt.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Step with N=100 on a 3-page source: copyPages loop terminates after 3.
	done, err := bk.Step(100)
	if err != nil {
		t.Fatalf("Step(100) on 3-page src: %v", err)
	}
	if !done {
		t.Error("Step(100) expected done=true for 3-page source")
	}
	if bk.PagesCopied() != 3 {
		t.Errorf("PagesCopied() = %d, want 3", bk.PagesCopied())
	}
}
