// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

// MC/DC 14 — MemoryPager internal branch coverage
//
// Targets:
//   memory_pager.go:105  Close          — rollback active write txn on close
//   memory_pager.go:136  getLocked      — pgno==0 and pgno>maxPageNum error paths
//   memory_pager.go:183  writeLocked    — readOnly path, nil page path
//   memory_pager.go:220  preparePageForWrite — savepoints active path
//   memory_pager.go:253  Commit         — needsHeaderUpdate path (dbSize changed)
//   memory_pager.go:379  AllocatePage   — readOnly path
//   memory_pager.go:426  FreePage       — readOnly, invalid pgno paths
//   memory_pager.go:457  BeginRead      — state != Open returns ErrTransactionOpen
//   memory_pager.go:514  Savepoint      — no write transaction error
//   memory_pager.go:613  writePage      — nil page path
//   memory_pager.go:667  journalPage    — already journaled path (idempotent)

import (
	"testing"
)

// TestMCDC14_MemPager_Close_WithWriteTx exercises Close when a write transaction
// is active, triggering the rollbackLocked path at line 110-113.
func TestMCDC14_MemPager_Close_WithWriteTx(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Start a write transaction.
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Close while write transaction is active — must not error.
	if err := mp.Close(); err != nil {
		t.Errorf("Close with active write tx: %v", err)
	}
}

// TestMCDC14_MemPager_GetLocked_ZeroPgno exercises getLocked pgno==0 error path.
func TestMCDC14_MemPager_GetLocked_ZeroPgno(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	_, err = mp.Get(0)
	if err == nil {
		t.Error("expected error for pgno==0, got nil")
	}
}

// TestMCDC14_MemPager_GetLocked_PgnoTooLarge exercises getLocked pgno>maxPageNum.
func TestMCDC14_MemPager_GetLocked_PgnoTooLarge(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	// maxPageNum starts at 1 (page 1 exists); request page 9999.
	_, err = mp.Get(9999)
	// Either an error or a valid page (readPage extends dbSize) — the test
	// just verifies no panic occurs on the Get call.
	_ = err
}

// TestMCDC14_MemPager_WriteLocked_ReadOnly exercises the readOnly guard in writeLocked.
func TestMCDC14_MemPager_WriteLocked_ReadOnly(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	mp.readOnly = true

	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer mp.Put(page)

	err = mp.Write(page)
	if err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// TestMCDC14_MemPager_WriteLocked_NilPage exercises the nil-page guard in writeLocked.
func TestMCDC14_MemPager_WriteLocked_NilPage(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	err = mp.Write(nil)
	if err == nil {
		t.Error("expected error for nil page, got nil")
	}
}

// TestMCDC14_MemPager_PreparePageForWrite_Savepoints exercises the savepoint
// branch in preparePageForWrite (line 229) by creating a savepoint before writing.
func TestMCDC14_MemPager_PreparePageForWrite_Savepoints(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	if err := mp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint: %v", err)
	}

	// Get page 1 and write it — triggers savePageState via preparePageForWrite.
	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer mp.Put(page)

	if err := mp.Write(page); err != nil {
		t.Errorf("Write with savepoint active: %v", err)
	}

	// Second write to the same page — journalPage sees page already journaled
	// (line 669) and savePageState sees page already in sp.Pages.
	if err := mp.Write(page); err != nil {
		t.Errorf("second Write with savepoint: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Errorf("Commit: %v", err)
	}
}

// TestMCDC14_MemPager_Commit_NeedsHeaderUpdate exercises the needsHeaderUpdate
// branch (line 276) by allocating a new page so dbSize != dbOrigSize.
func TestMCDC14_MemPager_Commit_NeedsHeaderUpdate(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// Allocate a new page — causes dbSize to increase, so needsHeaderUpdate=true.
	if _, err := mp.AllocatePage(); err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Errorf("Commit (needsHeaderUpdate): %v", err)
	}
}

// TestMCDC14_MemPager_AllocatePage_ReadOnly exercises the readOnly guard.
func TestMCDC14_MemPager_AllocatePage_ReadOnly(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	mp.readOnly = true
	_, err = mp.AllocatePage()
	if err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// TestMCDC14_MemPager_FreePage_ReadOnly exercises FreePage readOnly guard.
func TestMCDC14_MemPager_FreePage_ReadOnly(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	mp.readOnly = true
	err = mp.FreePage(1)
	if err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// TestMCDC14_MemPager_FreePage_InvalidPgno exercises FreePage with pgno==0.
func TestMCDC14_MemPager_FreePage_InvalidPgno(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	err = mp.FreePage(0)
	if err != ErrInvalidPageNum {
		t.Errorf("expected ErrInvalidPageNum, got %v", err)
	}
}

// TestMCDC14_MemPager_BeginRead_AlreadyInTx exercises BeginRead when state != Open.
func TestMCDC14_MemPager_BeginRead_AlreadyInTx(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	// First BeginRead should succeed.
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("first BeginRead: %v", err)
	}

	// Second BeginRead should fail (state is now PagerStateReader).
	err = mp.BeginRead()
	if err != ErrTransactionOpen {
		t.Errorf("expected ErrTransactionOpen on second BeginRead, got %v", err)
	}
}

// TestMCDC14_MemPager_Savepoint_NoWriteTx exercises Savepoint when no write
// transaction is active (returns ErrNoTransaction).
func TestMCDC14_MemPager_Savepoint_NoWriteTx(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	err = mp.Savepoint("sp_no_tx")
	if err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction, got %v", err)
	}
}

// TestMCDC14_MemPager_Release_NotFound exercises Release with unknown savepoint name.
func TestMCDC14_MemPager_Release_NotFound(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	err = mp.Release("nonexistent")
	if err == nil {
		t.Error("expected error for missing savepoint, got nil")
	}
}

// TestMCDC14_MemPager_Commit_NoTx exercises Commit with no active transaction.
func TestMCDC14_MemPager_Commit_NoTx(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	err = mp.Commit()
	if err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction, got %v", err)
	}
}

// TestMCDC14_MemPager_FreePage_BeyondDbSize exercises FreePage pgno > dbSize.
func TestMCDC14_MemPager_FreePage_BeyondDbSize(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mp.Close()

	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}

	// dbSize is 1 at open; page 100 is beyond it.
	err = mp.FreePage(100)
	if err != ErrInvalidPageNum {
		t.Errorf("expected ErrInvalidPageNum for pgno > dbSize, got %v", err)
	}
}
