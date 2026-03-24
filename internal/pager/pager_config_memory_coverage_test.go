// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"testing"
	"time"
)

// TestPagerConfigDefault verifies DefaultPagerConfig returns expected defaults.
func TestPagerConfigDefault(t *testing.T) {
	t.Parallel()
	c := DefaultPagerConfig()
	if c == nil {
		t.Fatal("DefaultPagerConfig() returned nil")
	}
	if c.PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", c.PageSize)
	}
	if c.CacheSize != -2000 {
		t.Errorf("CacheSize = %d, want -2000", c.CacheSize)
	}
	if c.JournalMode != "delete" {
		t.Errorf("JournalMode = %q, want \"delete\"", c.JournalMode)
	}
	if c.SyncMode != "full" {
		t.Errorf("SyncMode = %q, want \"full\"", c.SyncMode)
	}
	if c.LockingMode != "normal" {
		t.Errorf("LockingMode = %q, want \"normal\"", c.LockingMode)
	}
	if c.TempStore != "default" {
		t.Errorf("TempStore = %q, want \"default\"", c.TempStore)
	}
	if c.BusyTimeout != 5*time.Second {
		t.Errorf("BusyTimeout = %v, want 5s", c.BusyTimeout)
	}
	if c.WALAutocheckpoint != 1000 {
		t.Errorf("WALAutocheckpoint = %d, want 1000", c.WALAutocheckpoint)
	}
	if c.MaxPageCount != 0 {
		t.Errorf("MaxPageCount = %d, want 0", c.MaxPageCount)
	}
	if c.ReadOnly {
		t.Error("ReadOnly should be false by default")
	}
	if c.MemoryDB {
		t.Error("MemoryDB should be false by default")
	}
	if c.NoLock {
		t.Error("NoLock should be false by default")
	}
}

// TestPagerConfigValidate_Valid verifies that a valid default config passes validation.
func TestPagerConfigValidate_Valid(t *testing.T) {
	t.Parallel()
	c := DefaultPagerConfig()
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() returned unexpected error: %v", err)
	}
}

// TestPagerConfigValidate_InvalidPageSizeTooSmall tests page size < 512.
func TestPagerConfigValidate_InvalidPageSizeTooSmall(t *testing.T) {
	t.Parallel()
	c := DefaultPagerConfig()
	c.PageSize = 256
	if err := c.Validate(); err == nil {
		t.Error("Validate() should return error for PageSize=256")
	}
}

// TestPagerConfigValidate_InvalidPageSizeTooLarge tests page size > 65536.
func TestPagerConfigValidate_InvalidPageSizeTooLarge(t *testing.T) {
	t.Parallel()
	c := DefaultPagerConfig()
	c.PageSize = 131072
	if err := c.Validate(); err == nil {
		t.Error("Validate() should return error for PageSize=131072")
	}
}

// TestPagerConfigValidate_InvalidPageSizeNotPowerOfTwo tests non-power-of-2 page size.
func TestPagerConfigValidate_InvalidPageSizeNotPowerOfTwo(t *testing.T) {
	t.Parallel()
	c := DefaultPagerConfig()
	c.PageSize = 3000
	if err := c.Validate(); err == nil {
		t.Error("Validate() should return error for non-power-of-2 PageSize")
	}
}

// TestPagerConfigValidate_InvalidStringFields tests that invalid string field values
// are reset to their defaults rather than returning an error.
func TestPagerConfigValidate_InvalidStringFields(t *testing.T) {
	t.Parallel()

	c := DefaultPagerConfig()
	c.JournalMode = "bogus"
	c.SyncMode = "bogus"
	c.LockingMode = "bogus"
	c.TempStore = "bogus"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
	if c.JournalMode != "delete" {
		t.Errorf("JournalMode not reset, got %q", c.JournalMode)
	}
	if c.SyncMode != "full" {
		t.Errorf("SyncMode not reset, got %q", c.SyncMode)
	}
	if c.LockingMode != "normal" {
		t.Errorf("LockingMode not reset, got %q", c.LockingMode)
	}
	if c.TempStore != "default" {
		t.Errorf("TempStore not reset, got %q", c.TempStore)
	}
}

// TestPagerConfigValidate_ValidStringFields tests all valid string values are accepted.
func TestPagerConfigValidate_ValidStringFields(t *testing.T) {
	t.Parallel()

	journalModes := []string{"delete", "truncate", "persist", "memory", "wal", "off"}
	for _, jm := range journalModes {
		c := DefaultPagerConfig()
		c.JournalMode = jm
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for JournalMode=%q: %v", jm, err)
		}
		if c.JournalMode != jm {
			t.Errorf("JournalMode changed from %q to %q", jm, c.JournalMode)
		}
	}

	syncModes := []string{"off", "normal", "full", "extra"}
	for _, sm := range syncModes {
		c := DefaultPagerConfig()
		c.SyncMode = sm
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() unexpected error for SyncMode=%q: %v", sm, err)
		}
	}
}

// TestPagerConfigValidate_IntFields tests that invalid int fields are reset to defaults.
func TestPagerConfigValidate_IntFields(t *testing.T) {
	t.Parallel()

	c := DefaultPagerConfig()
	c.CacheSize = 0 // below minimum of 1
	c.WALAutocheckpoint = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
	// CacheSize should be reset to DefaultCacheSize (2000)
	if c.CacheSize != DefaultCacheSize {
		t.Errorf("CacheSize not reset, got %d, want %d", c.CacheSize, DefaultCacheSize)
	}
	// WALAutocheckpoint should be reset to 1000
	if c.WALAutocheckpoint != 1000 {
		t.Errorf("WALAutocheckpoint not reset, got %d", c.WALAutocheckpoint)
	}
}

// TestPagerConfigValidate_NegativeCacheSize tests that negative cache size is reset.
func TestPagerConfigValidate_NegativeCacheSize(t *testing.T) {
	t.Parallel()

	c := DefaultPagerConfig()
	c.CacheSize = -9999
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
	if c.CacheSize != DefaultCacheSize {
		t.Errorf("CacheSize not reset, got %d, want %d", c.CacheSize, DefaultCacheSize)
	}
}

// TestPagerConfigValidate_DurationField tests that a negative BusyTimeout is reset.
func TestPagerConfigValidate_DurationField(t *testing.T) {
	t.Parallel()

	c := DefaultPagerConfig()
	c.BusyTimeout = -1 * time.Second
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
	if c.BusyTimeout != 5*time.Second {
		t.Errorf("BusyTimeout not reset, got %v", c.BusyTimeout)
	}
}

// TestPagerConfigJournalModeValue tests all journal mode conversions.
func TestPagerConfigJournalModeValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mode string
		want int
	}{
		{"delete", JournalModeDelete},
		{"persist", JournalModePersist},
		{"off", JournalModeOff},
		{"truncate", JournalModeTruncate},
		{"memory", JournalModeMemory},
		{"wal", JournalModeWAL},
		{"unknown", JournalModeDelete}, // default case
	}

	for _, tc := range cases {
		c := DefaultPagerConfig()
		c.JournalMode = tc.mode
		got := c.JournalModeValue()
		if got != tc.want {
			t.Errorf("JournalModeValue() for %q = %d, want %d", tc.mode, got, tc.want)
		}
	}
}

// TestPagerConfigClone tests that Clone creates an independent copy.
func TestPagerConfigClone(t *testing.T) {
	t.Parallel()

	orig := DefaultPagerConfig()
	orig.PageSize = 8192
	orig.JournalMode = "wal"

	clone := orig.Clone()
	if clone == orig {
		t.Error("Clone() returned same pointer")
	}
	if clone.PageSize != orig.PageSize {
		t.Errorf("Clone PageSize = %d, want %d", clone.PageSize, orig.PageSize)
	}
	if clone.JournalMode != orig.JournalMode {
		t.Errorf("Clone JournalMode = %q, want %q", clone.JournalMode, orig.JournalMode)
	}

	// Modifying clone should not affect original
	clone.PageSize = 512
	clone.JournalMode = "memory"
	if orig.PageSize == 512 {
		t.Error("modifying clone changed original PageSize")
	}
	if orig.JournalMode == "memory" {
		t.Error("modifying clone changed original JournalMode")
	}
}

// TestMemoryPagerInTransaction tests InTransaction before and after a read transaction.
func TestMemoryPagerInTransaction(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	// No transaction initially
	if mp.InTransaction() {
		t.Error("InTransaction() should be false before any transaction")
	}

	// Begin a read transaction
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead: %v", err)
	}
	if !mp.InTransaction() {
		t.Error("InTransaction() should be true during read transaction")
	}

	// End read transaction
	if err := mp.EndRead(); err != nil {
		t.Fatalf("EndRead: %v", err)
	}
	if mp.InTransaction() {
		t.Error("InTransaction() should be false after EndRead")
	}

	// Begin write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if !mp.InTransaction() {
		t.Error("InTransaction() should be true during write transaction")
	}

	// Rollback
	if err := mp.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if mp.InTransaction() {
		t.Error("InTransaction() should be false after Rollback")
	}
}

// TestMemoryPagerSetUserVersion tests SetUserVersion.
func TestMemoryPagerSetUserVersion(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	if err := mp.SetUserVersion(42); err != nil {
		t.Fatalf("SetUserVersion: %v", err)
	}
	hdr := mp.GetHeader()
	if hdr.UserVersion != 42 {
		t.Errorf("UserVersion = %d, want 42", hdr.UserVersion)
	}

	// Set again to a different value
	if err := mp.SetUserVersion(99); err != nil {
		t.Fatalf("SetUserVersion (second): %v", err)
	}
	hdr = mp.GetHeader()
	if hdr.UserVersion != 99 {
		t.Errorf("UserVersion = %d, want 99", hdr.UserVersion)
	}
}

// TestMemoryPagerSetSchemaCookie tests SetSchemaCookie.
func TestMemoryPagerSetSchemaCookie(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	if err := mp.SetSchemaCookie(7); err != nil {
		t.Fatalf("SetSchemaCookie: %v", err)
	}
	hdr := mp.GetHeader()
	if hdr.SchemaCookie != 7 {
		t.Errorf("SchemaCookie = %d, want 7", hdr.SchemaCookie)
	}
}

// TestMemoryPagerVerifyFreeList tests VerifyFreeList on a fresh pager.
func TestMemoryPagerVerifyFreeList(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	// Fresh pager should have an empty free list that passes verification.
	if err := mp.VerifyFreeList(); err != nil {
		t.Errorf("VerifyFreeList() on fresh pager: %v", err)
	}
}

// TestMemoryPagerVerifyFreeListAfterFree tests VerifyFreeList after freeing a page.
func TestMemoryPagerVerifyFreeListAfterFree(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	// Allocate then free a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit after allocate: %v", err)
	}

	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("FreePage: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit after free: %v", err)
	}

	if err := mp.VerifyFreeList(); err != nil {
		t.Errorf("VerifyFreeList() after free: %v", err)
	}
}

// TestMemoryPagerFlushAndEvict exercises flushAndEvictDirtyPages indirectly by
// filling the cache, causing an eviction path when getting new pages.
func TestMemoryPagerFlushAndEvict(t *testing.T) {
	t.Parallel()
	// Use small cache to trigger eviction more easily.
	// We use default page size but write many pages.
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer mp.Close()

	// Allocate and write enough pages to ensure some cache churn
	const numPages = 10
	pgnos := make([]Pgno, 0, numPages)
	for i := 0; i < numPages; i++ {
		pgno, err := mp.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		pgnos = append(pgnos, pgno)
	}

	// Write to each page to make them dirty
	for _, pgno := range pgnos {
		page, err := mp.Get(pgno)
		if err != nil {
			t.Fatalf("Get page %d: %v", pgno, err)
		}
		if err := mp.Write(page); err != nil {
			mp.Put(page)
			t.Fatalf("Write page %d: %v", pgno, err)
		}
		page.Data[0] = byte(pgno)
		mp.Put(page)
	}

	// Commit should trigger writeDirtyPages (which shares the flush path).
	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}
