// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

// MC/DC 15 — pager internal unit tests for low-coverage paths.
//
// Targets:
//   journal.go:82   Open            (78.6%) — "already open" error path
//   journal.go:401  updatePageCount (71.4%) — nil file error path
//   journal.go:127  WriteOriginal   (93.3%) — nil file error path
//   journal.go:233  Finalize        (90.9%) — nil file path
//   memory_pager.go:220  preparePageForWrite (71.4%) — page already writeable
//   memory_pager.go:253  Commit     (71.4%) — no dirty pages (writeDirtyPages with empty cache)
//   memory_pager.go:613  writePage  (80.0%) — page with data vs zero data
//   pager.go:952    beginWriteTransaction (78.6%) — ErrTransactionOpen path

import (
	"os"
	"testing"
)

// ---------------------------------------------------------------------------
// Journal.Open — "already open" error path
// ---------------------------------------------------------------------------

// TestMCDC15_Journal_Open_AlreadyOpen exercises the "journal already open" error.
func TestMCDC15_Journal_Open_AlreadyOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	j := NewJournal(dir+"/test.journal", DefaultPageSize, 1)

	if err := j.Open(); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer j.Close()

	// Second Open → should error.
	err := j.Open()
	if err == nil {
		t.Error("expected 'already open' error, got nil")
	}
}

// TestMCDC15_Journal_Open_BadPath exercises the file open failure path.
func TestMCDC15_Journal_Open_BadPath(t *testing.T) {
	t.Parallel()

	j := NewJournal("/nonexistent/path/test.journal", DefaultPageSize, 1)
	err := j.Open()
	if err == nil {
		t.Error("expected open failure for bad path, got nil")
	}
}

// ---------------------------------------------------------------------------
// Journal.updatePageCount — nil file path
// ---------------------------------------------------------------------------

// TestMCDC15_Journal_UpdatePageCount_NilFile exercises the "journal not open" path.
func TestMCDC15_Journal_UpdatePageCount_NilFile(t *testing.T) {
	t.Parallel()

	j := &Journal{pageSize: DefaultPageSize} // file is nil
	err := j.updatePageCount()
	if err == nil {
		t.Error("expected error for nil file in updatePageCount, got nil")
	}
}

// ---------------------------------------------------------------------------
// Journal.WriteOriginal — nil file path
// ---------------------------------------------------------------------------

// TestMCDC15_Journal_WriteOriginal_NilFile exercises the nil-file check in WriteOriginal.
func TestMCDC15_Journal_WriteOriginal_NilFile(t *testing.T) {
	t.Parallel()

	j := &Journal{pageSize: DefaultPageSize}
	err := j.WriteOriginal(1, make([]byte, DefaultPageSize))
	if err == nil {
		t.Error("expected error for nil file in WriteOriginal, got nil")
	}
}

// ---------------------------------------------------------------------------
// Journal.Finalize — nil file (no-op path)
// ---------------------------------------------------------------------------

// TestMCDC15_Journal_Finalize_NilFile exercises Finalize when file is nil.
func TestMCDC15_Journal_Finalize_NilFile(t *testing.T) {
	t.Parallel()

	j := &Journal{pageSize: DefaultPageSize}
	// Finalize on nil file should be a no-op without error.
	if err := j.Finalize(); err != nil {
		t.Errorf("unexpected error from Finalize on nil file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.preparePageForWrite — page already writeable
// ---------------------------------------------------------------------------

// TestMCDC15_MemPager_PreparePageForWrite_AlreadyWriteable exercises the
// "!page.IsWriteable()" false branch (line 222) — page is already writeable
// so journalPage is NOT called, but savePageState IS called via savepoints.
func TestMCDC15_MemPager_PreparePageForWrite_AlreadyWriteable(t *testing.T) {
	t.Parallel()

	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Create a savepoint to trigger savePageState path.
	if err := mp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer mp.Put(page)

	// First write: journals the page + saves savepoint state.
	if err := mp.Write(page); err != nil {
		t.Fatalf("first Write: %v", err)
	}

	// page is now writeable. Second Write: page.IsWriteable() is true,
	// so journalPage is skipped; savePageState is still called.
	if err := mp.Write(page); err != nil {
		t.Errorf("second Write (already writeable): %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Errorf("Commit: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.Commit — empty transaction (no dirty pages → needsHeaderUpdate false)
// ---------------------------------------------------------------------------

// TestMCDC15_MemPager_Commit_EmptyWrite exercises Commit after a write tx with
// no page modifications (WriterLocked but no dirty pages).
func TestMCDC15_MemPager_Commit_EmptyWrite(t *testing.T) {
	t.Parallel()

	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	// Begin write then commit without modifying any pages.
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := mp.Commit(); err != nil {
		t.Errorf("Commit (empty write tx): %v", err)
	}
}

// ---------------------------------------------------------------------------
// MemoryPager.writePage — page with actual data
// ---------------------------------------------------------------------------

// TestMCDC15_MemPager_WritePage_WithData exercises writePage by writing a
// page with non-zero content.
func TestMCDC15_MemPager_WritePage_WithData(t *testing.T) {
	t.Parallel()

	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer mp.Put(page)

	// Write some data to the page.
	if err := mp.Write(page); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for i := range page.Data {
		page.Data[i] = byte(i % 256)
	}

	// Commit forces writeDirtyPages → writePage with the modified data.
	if err := mp.Commit(); err != nil {
		t.Errorf("Commit with page data: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Pager.beginWriteTransaction — ErrTransactionOpen path
// ---------------------------------------------------------------------------

// TestMCDC15_Pager_BeginWrite_AlreadyOpen exercises the ErrTransactionOpen
// path in beginWriteTransaction (state >= WriterLocked).
// Uses a file-based pager so the real lock acquisition paths are exercised.
func TestMCDC15_Pager_BeginWrite_AlreadyOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := dir + "/test.db"

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create db file: %v", err)
	}
	f.Close()

	p, err := Open(path, false)
	if err != nil {
		t.Fatalf("Open pager: %v", err)
	}
	defer p.Close()

	// First write tx.
	if err := p.BeginWrite(); err != nil {
		t.Fatalf("first BeginWrite: %v", err)
	}

	// Second write tx while already in one → ErrTransactionOpen.
	err = p.BeginWrite()
	if err != ErrTransactionOpen {
		t.Errorf("expected ErrTransactionOpen, got %v", err)
	}

	// Rollback to clean up.
	_ = p.Rollback()
}
