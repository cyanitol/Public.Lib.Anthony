// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// --- Backup tests ---

func TestNewBackup_NilPagers(t *testing.T) {
	t.Parallel()
	if _, err := NewBackup(nil, nil); err == nil {
		t.Error("NewBackup(nil, nil) expected error, got nil")
	}
}

func TestNewBackup_ReadOnlyDst(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "dst.db")

	// Create and populate src.
	src := openTestPagerAt(t, srcFile, false)
	defer src.Close()

	mustBeginWrite(t, src)
	pg := mustGetPage(t, src, 1)
	mustWritePage(t, src, pg)
	src.Put(pg)
	mustCommit(t, src)

	// Create dst as writable first so the file exists, then close and reopen read-only.
	dst := openTestPagerAt(t, dstFile, false)
	mustBeginWrite(t, dst)
	pg2 := mustGetPage(t, dst, 1)
	mustWritePage(t, dst, pg2)
	dst.Put(pg2)
	mustCommit(t, dst)
	dst.Close()

	dstRO := openTestPagerAt(t, dstFile, true)
	defer dstRO.Close()

	if _, err := NewBackup(src, dstRO); err == nil {
		t.Error("NewBackup with read-only dst expected error, got nil")
	}
}

func TestNewBackup_PageSizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := openTestPagerSizedAt(t, filepath.Join(dir, "src.db"), false, 4096)
	defer src.Close()
	dst := openTestPagerSizedAt(t, filepath.Join(dir, "dst.db"), false, 8192)
	defer dst.Close()

	// Write at least one page to src.
	mustBeginWrite(t, src)
	pg := mustGetPage(t, src, 1)
	mustWritePage(t, src, pg)
	src.Put(pg)
	mustCommit(t, src)

	if _, err := NewBackup(src, dst); err == nil {
		t.Error("NewBackup with mismatched page sizes expected error, got nil")
	}
}

func TestNewBackup_NilSrc(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	if _, err := NewBackup(nil, dst); err == nil {
		t.Error("NewBackup(nil, dst) expected error, got nil")
	}
}

func backupSetupSrc(t *testing.T, dir string, pageCount int) *Pager {
	t.Helper()
	src := openTestPagerAt(t, filepath.Join(dir, "src.db"), false)
	mustBeginWrite(t, src)
	for i := Pgno(1); i <= Pgno(pageCount); i++ {
		pg := mustGetPage(t, src, i)
		mustWritePage(t, src, pg)
		pg.Data[0] = byte(i)
		src.Put(pg)
	}
	mustCommit(t, src)
	return src
}

func TestBackup_FullCopy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 5)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	done, err := bk.Step(-1)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !done {
		t.Error("Step(-1) expected done=true")
	}
}

func TestBackup_SetProgress(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	called := false
	bk.SetProgress(func(remaining, total int) {
		called = true
	})

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !called {
		t.Error("progress callback was not called")
	}
}

func TestBackup_IncrementalStep(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 4)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	// Copy one page at a time.
	for i := 0; i < 10; i++ {
		done, err := bk.Step(1)
		if err != nil {
			t.Fatalf("Step(%d): %v", i, err)
		}
		if done {
			break
		}
	}

	if bk.Remaining() != 0 {
		t.Errorf("Remaining() = %d, want 0", bk.Remaining())
	}
}

func TestBackup_Remaining_Total_PagesCopied(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 3)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	total := bk.Total()
	if total == 0 {
		t.Error("Total() should be > 0")
	}
	if bk.PagesCopied() != 0 {
		t.Errorf("PagesCopied() before Step = %d, want 0", bk.PagesCopied())
	}
	if bk.Remaining() != total {
		t.Errorf("Remaining() = %d, want %d", bk.Remaining(), total)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if bk.PagesCopied() != total {
		t.Errorf("PagesCopied() after full step = %d, want %d", bk.PagesCopied(), total)
	}
	if bk.Remaining() != 0 {
		t.Errorf("Remaining() after full step = %d, want 0", bk.Remaining())
	}
}

func TestBackup_Finish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}

	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Calling Finish again should return ErrBackupFinished.
	if err := bk.Finish(); err != ErrBackupFinished {
		t.Errorf("second Finish() error = %v, want ErrBackupFinished", err)
	}
}

func TestBackup_StepAfterFinish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	_, err = bk.Step(1)
	if err != ErrBackupFinished {
		t.Errorf("Step after Finish error = %v, want ErrBackupFinished", err)
	}
}

func TestBackup_RemainingAfterDone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := backupSetupSrc(t, dir, 2)
	defer src.Close()

	dst := openTestPagerAt(t, filepath.Join(dir, "dst.db"), false)
	defer dst.Close()

	bk, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup: %v", err)
	}
	if _, err := bk.Step(-1); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	if r := bk.Remaining(); r != 0 {
		t.Errorf("Remaining() after done = %d, want 0", r)
	}
}

// --- Vacuum auto-vacuum mode tests ---

func TestGetAutoVacuumMode_Default(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)
	mode := p.GetAutoVacuumMode()
	if mode != 0 {
		t.Errorf("GetAutoVacuumMode() = %d, want 0 (none)", mode)
	}
}

func TestSetAutoVacuumMode_None(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "av.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	if err := p.SetAutoVacuumMode(0); err != nil {
		t.Fatalf("SetAutoVacuumMode(0): %v", err)
	}
	if got := p.GetAutoVacuumMode(); got != 0 {
		t.Errorf("GetAutoVacuumMode() = %d, want 0", got)
	}
}

func TestSetAutoVacuumMode_Full(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "av_full.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	if err := p.SetAutoVacuumMode(1); err != nil {
		t.Fatalf("SetAutoVacuumMode(1): %v", err)
	}
	if got := p.GetAutoVacuumMode(); got != 1 {
		t.Errorf("GetAutoVacuumMode() = %d, want 1 (full)", got)
	}
}

func TestSetAutoVacuumMode_Incremental(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "av_incr.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2): %v", err)
	}
	if got := p.GetAutoVacuumMode(); got != 2 {
		t.Errorf("GetAutoVacuumMode() = %d, want 2 (incremental)", got)
	}
}

func TestSetAutoVacuumMode_Invalid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "av_inv.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	if err := p.SetAutoVacuumMode(99); err == nil {
		t.Error("SetAutoVacuumMode(99) expected error, got nil")
	}
}

func TestSetAutoVacuumMode_ReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create a writable database first.
	src := openTestPagerAt(t, filepath.Join(dir, "ro.db"), false)
	mustBeginWrite(t, src)
	pg := mustGetPage(t, src, 1)
	mustWritePage(t, src, pg)
	src.Put(pg)
	mustCommit(t, src)
	src.Close()

	ro := openTestPagerAt(t, filepath.Join(dir, "ro.db"), true)
	defer ro.Close()

	if err := ro.SetAutoVacuumMode(1); err != ErrReadOnly {
		t.Errorf("SetAutoVacuumMode on read-only = %v, want ErrReadOnly", err)
	}
}

func TestIncrementalVacuum_NotInMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "incvac.db"), false)
	defer p.Close()

	mustBeginWrite(t, p)
	pg := mustGetPage(t, p, 1)
	mustWritePage(t, p, pg)
	p.Put(pg)
	mustCommit(t, p)

	// Without incremental vacuum mode set, should be a no-op.
	if err := p.IncrementalVacuum(0); err != nil {
		t.Fatalf("IncrementalVacuum(0) without mode: %v", err)
	}
}

func TestIncrementalVacuum_InMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "incvac2.db"), false)
	defer p.Close()

	// Allocate a few pages and commit.
	mustBeginWrite(t, p)
	for i := Pgno(1); i <= 5; i++ {
		pg := mustGetPage(t, p, i)
		mustWritePage(t, p, pg)
		p.Put(pg)
	}
	mustCommit(t, p)

	// Enable incremental vacuum mode.
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2): %v", err)
	}

	// IncrementalVacuum with nPages=0 should run without error.
	if err := p.IncrementalVacuum(0); err != nil {
		t.Fatalf("IncrementalVacuum(0): %v", err)
	}
}

func TestIncrementalVacuum_ReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := openTestPagerAt(t, filepath.Join(dir, "iv_ro.db"), false)
	mustBeginWrite(t, src)
	pg := mustGetPage(t, src, 1)
	mustWritePage(t, src, pg)
	src.Put(pg)
	mustCommit(t, src)
	src.Close()

	ro := openTestPagerAt(t, filepath.Join(dir, "iv_ro.db"), true)
	defer ro.Close()

	if err := ro.IncrementalVacuum(0); err != ErrReadOnly {
		t.Errorf("IncrementalVacuum on read-only = %v, want ErrReadOnly", err)
	}
}

// TestIncrementalVacuum_FreesTrailingPages exercises updateHeaderAfterVacuum by
// arranging the freelist so that the last database page is a leaf entry.
// Step 1: free a middle page (e.g. page 3) so it becomes a trunk.
// Step 2: free the last page (page 6) so it becomes a leaf in the trunk.
// Step 3: IncrementalVacuum finds the last page as a leaf and frees it.
func TestIncrementalVacuum_FreesTrailingPages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := openTestPagerAt(t, filepath.Join(dir, "incvac3.db"), false)
	defer p.Close()

	// Allocate pages 1-6 and commit.
	mustBeginWrite(t, p)
	for i := Pgno(1); i <= 6; i++ {
		pg := mustGetPage(t, p, i)
		mustWritePage(t, p, pg)
		p.Put(pg)
	}
	mustCommit(t, p)

	// Free page 3 (middle page) to establish it as a trunk page.
	if err := p.FreePage(3); err != nil {
		t.Fatalf("FreePage(3): %v", err)
	}
	mustCommit(t, p)

	// Enable incremental vacuum mode (mode=2).
	if err := p.SetAutoVacuumMode(2); err != nil {
		t.Fatalf("SetAutoVacuumMode(2): %v", err)
	}

	// Free page 6 (last page) so it becomes a leaf in the existing trunk.
	// buildFreeSet iterates leaves, so page 6 will be in freeSet.
	if err := p.FreePage(6); err != nil {
		t.Fatalf("FreePage(6): %v", err)
	}
	mustCommit(t, p)

	// IncrementalVacuum: last page (6) is a leaf in freeSet, freeTrailingPages > 0,
	// so updateHeaderAfterVacuum is called.
	if err := p.IncrementalVacuum(1); err != nil {
		t.Fatalf("IncrementalVacuum(1): %v", err)
	}
}

func TestVacuumInto_BasicOperation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "into.db")

	p := openTestPagerAt(t, srcFile, false)
	defer p.Close()

	// Write several pages and commit.
	mustBeginWrite(t, p)
	for i := Pgno(1); i <= 3; i++ {
		pg := mustGetPage(t, p, i)
		mustWritePage(t, p, pg)
		pg.Data[0] = byte(i * 10)
		p.Put(pg)
	}
	mustCommit(t, p)

	opts := &VacuumOptions{IntoFile: dstFile}
	if err := p.Vacuum(opts); err != nil {
		t.Fatalf("Vacuum INTO: %v", err)
	}
}
