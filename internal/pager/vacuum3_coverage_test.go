// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// ============================================================================
// persistSchemaToTarget — cover the opts-with-non-nil-schema branch.
//
// persistSchemaToTarget returns nil in all cases.  The uncovered branch
// (line 282–287) is the code path where opts != nil && opts.SourceSchema != nil
// && opts.Btree != nil.  Calling the function directly with such opts covers
// that branch without needing a real schema or btree.
// ============================================================================

func TestVacuum3Coverage_PersistSchemaToTarget_NonNilOpts(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)
	target := openTestPager(t)
	defer target.Close()

	opts := &VacuumOptions{
		SourceSchema: struct{}{}, // non-nil value satisfies the opts.SourceSchema != nil check
		Btree:        struct{}{}, // non-nil value satisfies the opts.Btree != nil check
	}

	if err := p.persistSchemaToTarget(target, opts); err != nil {
		t.Errorf("persistSchemaToTarget with non-nil opts: unexpected error: %v", err)
	}
}

// ============================================================================
// closeCurrentDatabase — cover the p.file == nil early-return branch.
//
// closeCurrentDatabase returns nil immediately when p.file is already nil.
// In normal Vacuum flow p.file is always non-nil, so this branch is uncovered.
// ============================================================================

func TestVacuum3Coverage_CloseCurrentDatabase_NilFile(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	// Close the underlying file so p.file is nil, then call closeCurrentDatabase.
	p.mu.Lock()
	if p.file != nil {
		p.file.Close()
		p.file = nil
	}
	err := p.closeCurrentDatabase()
	p.mu.Unlock()

	if err != nil {
		t.Errorf("closeCurrentDatabase with nil file: unexpected error: %v", err)
	}
}

// ============================================================================
// vacuumToFile — cover the OpenWithPageSize error path.
//
// vacuumToFile opens a target pager as its first step.  Passing an invalid
// path (inside a nonexistent directory) forces OpenWithPageSize to return an
// error, exercising the early error-return on line 185-186.
// ============================================================================

func TestVacuum3Coverage_VacuumToFile_BadTargetPath(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	// Write a page so the pager has some data.
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, 1)
	page.Data[DatabaseHeaderSize] = 0x42
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Use a path inside a nonexistent directory so OpenWithPageSize fails.
	badPath := filepath.Join(t.TempDir(), "nonexistent_subdir", "vacuum.db")
	if err := p.vacuumToFile(badPath, nil); err == nil {
		t.Error("vacuumToFile with bad path: expected error, got nil")
	}
}

// ============================================================================
// replaceForVacuumInto — cover the VACUUM INTO code path.
//
// Calling Vacuum with opts.IntoFile set routes through replaceForVacuumInto
// (line 119) instead of replaceForVacuumInPlace (line 134), exercising:
//   - copyFile (temp → target)
//   - os.Remove (temp file)
//   - os.OpenFile (reopening source)
// ============================================================================

func TestVacuum3Coverage_VacuumInto(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	tgtFile := filepath.Join(dir, "tgt.db")

	p := openTestPagerAt(t, srcFile, false)
	defer p.Close()

	// Write a few pages so there is real data to copy.
	mustBeginWrite(t, p)
	for pgno := Pgno(2); pgno <= 5; pgno++ {
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(pgno)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	if err := p.Vacuum(&VacuumOptions{IntoFile: tgtFile}); err != nil {
		t.Fatalf("Vacuum INTO: unexpected error: %v", err)
	}

	// The target file must be openable.
	tgt := openTestPagerAt(t, tgtFile, true)
	tgt.Close()
}

// ============================================================================
// copyDatabaseToTarget — cover the normal path with multiple pages.
//
// copyDatabaseToTarget is called internally by vacuumToFile.  Exercising it
// with a source database that has several live pages ensures all three
// inner calls (copyHeader, copyPage1Content, copyLivePages) are reached and
// that the final "return nil" statement on line 224 is covered.
// ============================================================================

func TestVacuum3Coverage_CopyDatabaseToTarget_MultiPage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")

	p := openTestPagerAt(t, srcFile, false)
	defer p.Close()

	// Allocate several pages so copyLivePages has real work to do.
	mustBeginWrite(t, p)
	for i := 0; i < 8; i++ {
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	// Run Vacuum (which internally calls copyDatabaseToTarget) on the multi-page DB.
	if err := p.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum on multi-page DB: unexpected error: %v", err)
	}
}

// ============================================================================
// copyDatabaseToTarget — cover with free pages present.
//
// When a database has free pages, buildFreePageSet / collectFreePages is used
// by copyLivePages to skip those pages.  Creating and freeing several pages
// before calling Vacuum exercises more of the copy path.
// ============================================================================

func TestVacuum3Coverage_CopyDatabaseToTarget_WithFreePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src_free.db")

	p := openTestPagerAt(t, srcFile, false)
	defer p.Close()

	// Allocate pages.
	mustBeginWrite(t, p)
	var pages []Pgno
	for i := 0; i < 6; i++ {
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 10)
		mustWritePage(t, p, page)
		p.Put(page)
		pages = append(pages, pgno)
	}
	mustCommit(t, p)

	// Free some pages to create a non-empty freelist.
	mustBeginWrite(t, p)
	for _, pgno := range pages[2:4] {
		if err := p.FreePage(pgno); err != nil {
			t.Logf("FreePage(%d) warning: %v (continuing)", pgno, err)
		}
	}
	mustCommit(t, p)

	// Vacuum should compact, skipping the freed pages.
	if err := p.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum with free pages: unexpected error: %v", err)
	}
}
