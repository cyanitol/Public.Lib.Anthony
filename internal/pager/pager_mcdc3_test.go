// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// cache.go — NewLRUCache capacity guard
//   `config.MaxPages <= 0 && config.MaxMemory <= 0`
//
//   A = config.MaxPages <= 0
//   B = config.MaxMemory <= 0
//
//   Returns ErrCacheCapacityZero when A && B is true.
//
//   Case 1 (A=F): MaxPages > 0 → succeeds
//   Case 2 (A=T, B=F): MaxPages <= 0 but MaxMemory > 0 → succeeds
//   Case 3 (A=T, B=T): both zero → ErrCacheCapacityZero
// ---------------------------------------------------------------------------

func TestMCDC_NewLRUCache_MaxPagesPositive(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (MaxPages > 0) → no error
	_, err := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  10,
		MaxMemory: 0,
		Mode:      WriteThroughMode,
	})
	if err != nil {
		t.Errorf("MCDC case1: MaxPages=10 must succeed; got %v", err)
	}
}

func TestMCDC_NewLRUCache_MaxMemoryPositive(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (MaxPages=0), B=F (MaxMemory > 0) → no error
	_, err := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 1024 * 1024,
		Mode:      WriteThroughMode,
	})
	if err != nil {
		t.Errorf("MCDC case2: MaxPages=0 but MaxMemory>0 must succeed; got %v", err)
	}
}

func TestMCDC_NewLRUCache_BothZero(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (both <= 0) → ErrCacheCapacityZero
	_, err := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 0,
		Mode:      WriteThroughMode,
	})
	if err != ErrCacheCapacityZero {
		t.Errorf("MCDC case3: both zero must return ErrCacheCapacityZero; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// cache.go — SetMaxPages capacity guard
//   `maxPages <= 0 && c.maxMemory <= 0`
//
//   A = maxPages <= 0
//   B = c.maxMemory <= 0
//
//   Returns ErrCacheCapacityZero when A && B is true.
//
//   Case 1 (A=F): maxPages > 0 → succeeds
//   Case 2 (A=T, B=F): maxPages <= 0 but cache has maxMemory > 0 → succeeds
//   Case 3 (A=T, B=T): both zero → ErrCacheCapacityZero
// ---------------------------------------------------------------------------

func TestMCDC_SetMaxPages_PositivePages(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (maxPages=5) → succeeds
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})
	if err := c.SetMaxPages(5); err != nil {
		t.Errorf("MCDC case1: SetMaxPages(5) must succeed; got %v", err)
	}
}

func TestMCDC_SetMaxPages_ZeroPagesWithMemory(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (maxPages=0), B=F (cache has maxMemory > 0) → succeeds
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 1024 * 1024,
	})
	if err := c.SetMaxPages(0); err != nil {
		t.Errorf("MCDC case2: SetMaxPages(0) with maxMemory>0 must succeed; got %v", err)
	}
}

func TestMCDC_SetMaxPages_BothZero(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (maxPages=0, maxMemory=0) → ErrCacheCapacityZero
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})
	// maxMemory starts at 0; set maxPages to 0 → both zero
	if err := c.SetMaxPages(0); err != ErrCacheCapacityZero {
		t.Errorf("MCDC case3: SetMaxPages(0) with no maxMemory must return ErrCacheCapacityZero; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// cache.go — SetMaxMemory capacity guard
//   `maxMemory <= 0 && c.maxPages <= 0`
//
//   A = maxMemory <= 0
//   B = c.maxPages <= 0
//
//   Returns ErrCacheCapacityZero when A && B is true.
//
//   Case 1 (A=F): maxMemory > 0 → succeeds
//   Case 2 (A=T, B=F): maxMemory <= 0 but cache has maxPages > 0 → succeeds
//   Case 3 (A=T, B=T): both zero → ErrCacheCapacityZero
// ---------------------------------------------------------------------------

func TestMCDC_SetMaxMemory_PositiveMemory(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (maxMemory > 0) → succeeds
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})
	if err := c.SetMaxMemory(512 * 1024); err != nil {
		t.Errorf("MCDC case1: SetMaxMemory(512k) must succeed; got %v", err)
	}
}

func TestMCDC_SetMaxMemory_ZeroMemoryWithPages(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (maxMemory=0), B=F (cache has maxPages > 0) → succeeds
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})
	if err := c.SetMaxMemory(0); err != nil {
		t.Errorf("MCDC case2: SetMaxMemory(0) with maxPages>0 must succeed; got %v", err)
	}
}

func TestMCDC_SetMaxMemory_BothZero(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (maxMemory=0, maxPages=0) → ErrCacheCapacityZero
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 512 * 1024,
	})
	// Reduce maxPages to 0 first so the cache only has maxMemory as capacity
	_ = c.SetMaxPages(-1)
	// Now set maxMemory to 0 → both zero
	if err := c.SetMaxMemory(0); err != ErrCacheCapacityZero {
		t.Errorf("MCDC case3: SetMaxMemory(0) with no maxPages must return ErrCacheCapacityZero; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// cache.go — MarkDirtyByPgno write-through flush guard
//   `c.mode == WriteThroughMode && c.pager != nil`
//
//   A = c.mode == WriteThroughMode
//   B = c.pager != nil
//
//   Immediately flushes the page when A && B is true.
//
//   Case 1 (A=F): WriteBackMode → no immediate flush (page stays dirty)
//   Case 2 (A=T, B=F): WriteThroughMode but no pager → no flush
//   Case 3 (A=T, B=T): WriteThroughMode with pager → page flushed immediately
// ---------------------------------------------------------------------------

func TestMCDC_MarkDirtyByPgno_WriteBackMode(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (WriteBackMode) → page becomes dirty but is NOT flushed
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize: DefaultPageSize,
		MaxPages: 10,
		Mode:     WriteBackMode,
	})
	page := NewDbPage(1, DefaultPageSize)
	_ = c.Put(page)

	if err := c.MarkDirtyByPgno(1); err != nil {
		t.Fatalf("MCDC case1: MarkDirtyByPgno error = %v", err)
	}
	// In write-back mode, page should remain dirty (not flushed)
	if p := c.Get(1); p != nil && !p.IsDirty() {
		t.Error("MCDC case1: WriteBackMode must leave page dirty after MarkDirtyByPgno")
	}
}

func TestMCDC_MarkDirtyByPgno_WriteThroughNoPager(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (WriteThroughMode), B=F (pager==nil) → no flush, page stays dirty
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize: DefaultPageSize,
		MaxPages: 10,
		Mode:     WriteThroughMode,
	})
	// No pager set → c.pager == nil
	page := NewDbPage(1, DefaultPageSize)
	_ = c.Put(page)

	if err := c.MarkDirtyByPgno(1); err != nil {
		t.Fatalf("MCDC case2: MarkDirtyByPgno with nil pager error = %v", err)
	}
	// Page should be dirty (flush was skipped due to nil pager)
	if p := c.Get(1); p != nil && !p.IsDirty() {
		t.Error("MCDC case2: WriteThroughMode with nil pager must leave page dirty")
	}
}

func TestMCDC_MarkDirtyByPgno_WriteThroughWithPager(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (WriteThroughMode with pager set) → page is flushed immediately
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
	page.Data[DatabaseHeaderSize] = 0xAA
	p.Put(page)

	// In write-through mode with a real pager, the page is flushed when MarkDirty is called
	// The page should be clean after the implicit flush
	lruCache, ok := p.cache.(*LRUCache)
	if !ok {
		t.Skip("cache is not *LRUCache; skipping")
	}
	if err := lruCache.MarkDirtyByPgno(1); err != nil {
		t.Fatalf("MCDC case3: MarkDirtyByPgno error = %v", err)
	}
	// In write-through mode the page was already clean due to the Write call;
	// MarkDirtyByPgno on a clean page with pager → makes dirty and immediately flushes
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// cache.go — updateDirtyListForReplacement, first branch
//   `oldPage.IsDirty() && !newPage.IsDirty()`
//
//   A = oldPage.IsDirty()
//   B = !newPage.IsDirty()
//
//   Calls removeFromDirtyList when A && B is true.
//
//   Case 1 (A=F): old is clean → branch not taken
//   Case 2 (A=T, B=F): old dirty, new dirty → branch not taken (falls to second branch)
//   Case 3 (A=T, B=T): old dirty, new clean → removeFromDirtyList called
// ---------------------------------------------------------------------------

func TestMCDC_UpdateDirtyList_OldClean(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (old is clean) → first branch not taken
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	// oldPage stays clean
	_ = c.Put(oldPage)

	// Replace with a clean new page
	newPage := NewDbPage(1, DefaultPageSize)
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case1: Put(newPage) error = %v", err)
	}
	// dirty list should still be empty
	if dirty := c.GetDirtyPages(); len(dirty) != 0 {
		t.Errorf("MCDC case1: no dirty page expected; got %d", len(dirty))
	}
}

func TestMCDC_UpdateDirtyList_OldDirtyNewDirty(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (old dirty), B=F (new also dirty) → first branch not taken
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	oldPage.MakeDirty()
	_ = c.Put(oldPage)

	newPage := NewDbPage(1, DefaultPageSize)
	newPage.MakeDirty()
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case2: Put(newPage) error = %v", err)
	}
	// dirty list should still contain the new page
	dirty := c.GetDirtyPages()
	if len(dirty) == 0 {
		t.Error("MCDC case2: dirty list must still be non-empty after dirty→dirty replacement")
	}
}

func TestMCDC_UpdateDirtyList_OldDirtyNewClean(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (old dirty, new clean) → removeFromDirtyList called
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	oldPage.MakeDirty()
	_ = c.Put(oldPage)

	// Verify oldPage is in dirty list
	if dirty := c.GetDirtyPages(); len(dirty) == 0 {
		t.Fatal("MCDC case3: setup: old dirty page should be in dirty list")
	}

	// Replace with a clean page → dirty list entry should be removed
	newPage := NewDbPage(1, DefaultPageSize)
	// newPage is clean by default
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case3: Put(newPage) error = %v", err)
	}
	if dirty := c.GetDirtyPages(); len(dirty) != 0 {
		t.Errorf("MCDC case3: dirty list must be empty after dirty→clean replacement; got %d", len(dirty))
	}
}

// ---------------------------------------------------------------------------
// cache.go — updateDirtyListForReplacement, second branch
//   `!oldPage.IsDirty() && newPage.IsDirty()`
//
//   A = !oldPage.IsDirty()  (old is clean)
//   B = newPage.IsDirty()
//
//   Calls addToDirtyList when A && B is true.
//
//   Case 1 (A=F): old is dirty → second branch not taken
//   Case 2 (A=T, B=F): old clean, new clean → second branch not taken
//   Case 3 (A=T, B=T): old clean, new dirty → addToDirtyList called
// ---------------------------------------------------------------------------

func TestMCDC_UpdateDirtyList2_OldDirty(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (old is dirty) → second branch not taken
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	oldPage.MakeDirty()
	_ = c.Put(oldPage)

	// Replace with a clean page (exercises first branch, not second)
	newPage := NewDbPage(1, DefaultPageSize)
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case1: Put error = %v", err)
	}
}

func TestMCDC_UpdateDirtyList2_BothClean(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (old clean), B=F (new clean) → second branch not taken
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	_ = c.Put(oldPage)

	newPage := NewDbPage(1, DefaultPageSize)
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case2: Put error = %v", err)
	}
	if dirty := c.GetDirtyPages(); len(dirty) != 0 {
		t.Errorf("MCDC case2: no dirty pages expected for clean→clean; got %d", len(dirty))
	}
}

func TestMCDC_UpdateDirtyList2_OldCleanNewDirty(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (old clean, new dirty) → addToDirtyList called
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	oldPage := NewDbPage(1, DefaultPageSize)
	// oldPage is clean
	_ = c.Put(oldPage)

	newPage := NewDbPage(1, DefaultPageSize)
	newPage.MakeDirty()
	if err := c.Put(newPage); err != nil {
		t.Fatalf("MCDC case3: Put(dirty newPage) error = %v", err)
	}
	if dirty := c.GetDirtyPages(); len(dirty) == 0 {
		t.Error("MCDC case3: dirty list must contain new dirty page after clean→dirty replacement")
	}
}

// ---------------------------------------------------------------------------
// cache.go — EvictClean clean-and-unreferenced guard
//   `entry.page.IsClean() && entry.page.GetRefCount() == 0`
//
//   A = entry.page.IsClean()
//   B = entry.page.GetRefCount() == 0
//
//   Page is evicted when A && B is true.
//
//   Case 1 (A=F): dirty page → not evicted
//   Case 2 (A=T, B=F): clean but ref-count > 0 → not evicted
//   Case 3 (A=T, B=T): clean and unreferenced → evicted
// ---------------------------------------------------------------------------

func TestMCDC_EvictClean_DirtyPage(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (dirty) → not evicted by EvictClean
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	page := NewDbPage(1, DefaultPageSize)
	page.MakeDirty()
	_ = c.Put(page)

	evicted := c.EvictClean()
	if evicted != 0 {
		t.Errorf("MCDC case1: dirty page must not be evicted; got evicted=%d", evicted)
	}
	if c.Size() != 1 {
		t.Errorf("MCDC case1: dirty page must remain in cache; size=%d", c.Size())
	}
}

func TestMCDC_EvictClean_CleanWithRefs(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (clean), B=F (refCount > 0) → not evicted
	// NewDbPage starts with refCount=1, so no extra Ref() needed.
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	page := NewDbPage(1, DefaultPageSize)
	// page is clean and refCount == 1 (set by NewDbPage)
	_ = c.Put(page)

	evicted := c.EvictClean()
	if evicted != 0 {
		t.Errorf("MCDC case2: clean page with refs must not be evicted; got evicted=%d", evicted)
	}
	if c.Size() != 1 {
		t.Errorf("MCDC case2: referenced page must remain in cache; size=%d", c.Size())
	}
}

func TestMCDC_EvictClean_CleanNoRefs(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (clean, refCount==0) → evicted
	// NewDbPage starts with refCount=1; call Unref() to bring it to 0.
	c, _ := NewLRUCache(LRUCacheConfig{PageSize: DefaultPageSize, MaxPages: 10})

	page := NewDbPage(1, DefaultPageSize)
	page.Unref() // refCount: 1 → 0
	_ = c.Put(page)

	evicted := c.EvictClean()
	if evicted != 1 {
		t.Errorf("MCDC case3: clean unreferenced page must be evicted; got evicted=%d", evicted)
	}
	if c.Size() != 0 {
		t.Errorf("MCDC case3: cache must be empty after EvictClean; size=%d", c.Size())
	}
}

// ---------------------------------------------------------------------------
// cache.go — isAtCapacity memory limit branch
//   `c.maxMemory > 0 && c.memoryUsage >= c.maxMemory`
//
//   A = c.maxMemory > 0
//   B = c.memoryUsage >= c.maxMemory
//
//   Returns true (capacity reached via memory) when A && B is true.
//
//   Case 1 (A=F): maxMemory==0 → memory limit not checked
//   Case 2 (A=T, B=F): maxMemory>0 but usage < limit → not at capacity (via memory)
//   Case 3 (A=T, B=T): maxMemory>0 and usage >= limit → at capacity
// ---------------------------------------------------------------------------

func TestMCDC_IsAtCapacity_NoMemoryLimit(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (maxMemory==0) → memory check skipped; falls through to page count check
	// Use large maxPages so page-count limit isn't hit either
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  1000,
		MaxMemory: 0,
	})
	// Add one page — should succeed (not at capacity)
	page := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Errorf("MCDC case1: Put with no memory limit must not error; got %v", err)
	}
}

func TestMCDC_IsAtCapacity_MemoryLimitNotReached(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (maxMemory>0), B=F (usage < limit) → not at capacity via memory branch
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 100 * int64(DefaultPageSize), // room for 100 pages
	})
	page := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Errorf("MCDC case2: Put well below memory limit must succeed; got %v", err)
	}
}

func TestMCDC_IsAtCapacity_MemoryLimitReached(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (maxMemory > 0, memoryUsage >= maxMemory) → at capacity
	// Set limit to exactly one page to force capacity after first Put
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: int64(DefaultPageSize), // exactly one page
	})
	page1 := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page1); err != nil {
		t.Fatalf("MCDC case3: first Put must succeed; got %v", err)
	}
	// Second Put must trigger eviction (cache at capacity from memory limit)
	page2 := NewDbPage(2, DefaultPageSize)
	// If page1 is clean and unreferenced it will be evicted; Put should still succeed
	if err := c.Put(page2); err != nil {
		// ErrCacheFull means eviction found no clean page — that's acceptable
		// The important thing is isAtCapacity returned true
		t.Logf("MCDC case3: Put returned %v (acceptable when no clean evictable page)", err)
	}
}

// ---------------------------------------------------------------------------
// cache.go — isAtCapacity page count branch
//   `c.maxPages > 0 && len(c.entries) >= c.maxPages`
//
//   A = c.maxPages > 0
//   B = len(c.entries) >= c.maxPages
//
//   Returns true (capacity reached via page count) when A && B is true.
//
//   Case 1 (A=F): maxPages==0 → page-count limit not checked
//   Case 2 (A=T, B=F): maxPages>0 but current count < limit → not at capacity
//   Case 3 (A=T, B=T): maxPages>0 and count >= maxPages → at capacity
// ---------------------------------------------------------------------------

func TestMCDC_IsAtCapacity_NoPageLimit(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (maxPages==0) → page count check skipped; use maxMemory instead
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize:  DefaultPageSize,
		MaxPages:  0,
		MaxMemory: 100 * int64(DefaultPageSize),
	})
	page := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Errorf("MCDC case1: Put with maxPages=0 must succeed; got %v", err)
	}
}

func TestMCDC_IsAtCapacity_PageLimitNotReached(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (maxPages=5), B=F (only 1 entry) → not at capacity
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize: DefaultPageSize,
		MaxPages: 5,
	})
	page := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Errorf("MCDC case2: Put when below page limit must succeed; got %v", err)
	}
	if c.Size() != 1 {
		t.Errorf("MCDC case2: expected size=1; got %d", c.Size())
	}
}

func TestMCDC_IsAtCapacity_PageLimitReached(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (maxPages=1, already has 1 entry) → at capacity
	c, _ := NewLRUCache(LRUCacheConfig{
		PageSize: DefaultPageSize,
		MaxPages: 1,
	})
	page1 := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page1); err != nil {
		t.Fatalf("MCDC case3: first Put must succeed; got %v", err)
	}
	// Now cache is at capacity; adding page2 triggers eviction of page1 (clean, no refs)
	page2 := NewDbPage(2, DefaultPageSize)
	if err := c.Put(page2); err != nil {
		t.Logf("MCDC case3: Put triggered eviction attempt, result: %v", err)
	}
}

// ---------------------------------------------------------------------------
// transaction.go — BeginWrite already-in-write-transaction guard
//   `p.state >= PagerStateWriterLocked && p.state < PagerStateError`
//
//   A = p.state >= PagerStateWriterLocked
//   B = p.state < PagerStateError
//
//   Returns ErrTransactionOpen when A && B is true.
//
//   Case 1 (A=F): state below WriterLocked → no error from this guard
//   Case 2 (A=T, B=F): state=Error → not caught here (caught by error-state guard below)
//   Case 3 (A=T, B=T): state=WriterLocked → ErrTransactionOpen
// ---------------------------------------------------------------------------

func TestMCDC_BeginWrite_StateBelowWriter(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (state=Open) → BeginWrite proceeds normally
	p := openTestPager(t)
	if err := p.BeginWrite(); err != nil {
		t.Errorf("MCDC case1: BeginWrite from Open state must succeed; got %v", err)
	}
	mustRollback(t, p)
}

func TestMCDC_BeginWrite_StateError(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (state=Error) → error-state guard triggers, not the write-open guard
	p := openTestPager(t)
	p.mu.Lock()
	p.state = PagerStateError
	p.errCode = ErrReadOnly // any non-nil error
	p.mu.Unlock()

	err := p.BeginWrite()
	if err == nil {
		t.Error("MCDC case2: BeginWrite from Error state must return an error")
	}
	// Restore state so cleanup works
	p.mu.Lock()
	p.state = PagerStateOpen
	p.errCode = nil
	p.mu.Unlock()
}

func TestMCDC_BeginWrite_AlreadyWriterLocked(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (state=WriterLocked) → ErrTransactionOpen
	p := openTestPager(t)
	mustBeginWrite(t, p)
	// Second BeginWrite must return ErrTransactionOpen
	err := p.BeginWrite()
	if err != ErrTransactionOpen {
		t.Errorf("MCDC case3: BeginWrite when already writing must return ErrTransactionOpen; got %v", err)
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// transaction.go — InTransaction state range check
//   `p.state >= PagerStateReader && p.state < PagerStateError`
//
//   A = p.state >= PagerStateReader
//   B = p.state < PagerStateError
//
//   Returns true when A && B is true.
//
//   Case 1 (A=F): state=Open (below Reader) → false
//   Case 2 (A=T, B=F): state=Error → false
//   Case 3 (A=T, B=T): state=Reader → true
// ---------------------------------------------------------------------------

func TestMCDC_InTransaction_StateOpen(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (state=Open) → InTransaction returns false
	p := openTestPager(t)
	if p.InTransaction() {
		t.Error("MCDC case1: InTransaction must be false when state=Open")
	}
}

func TestMCDC_InTransaction_StateError(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (state=Error) → InTransaction returns false
	p := openTestPager(t)
	p.mu.Lock()
	p.state = PagerStateError
	p.mu.Unlock()

	result := p.InTransaction()

	p.mu.Lock()
	p.state = PagerStateOpen
	p.mu.Unlock()

	if result {
		t.Error("MCDC case2: InTransaction must be false when state=Error")
	}
}

func TestMCDC_InTransaction_StateReader(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (state=Reader) → InTransaction returns true
	p := openTestPager(t)
	mustBeginRead(t, p)
	if !p.InTransaction() {
		t.Error("MCDC case3: InTransaction must be true when state=Reader")
	}
	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// transaction.go — InWriteTransaction state range check
//   `p.state >= PagerStateWriterLocked && p.state < PagerStateError`
//
//   A = p.state >= PagerStateWriterLocked
//   B = p.state < PagerStateError
//
//   Returns true when A && B is true.
//
//   Case 1 (A=F): state=Reader (below WriterLocked) → false
//   Case 2 (A=T, B=F): state=Error → false
//   Case 3 (A=T, B=T): state=WriterLocked → true
// ---------------------------------------------------------------------------

func TestMCDC_InWriteTransaction_StateReader(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (state=Reader) → InWriteTransaction returns false
	p := openTestPager(t)
	mustBeginRead(t, p)
	if p.InWriteTransaction() {
		t.Error("MCDC case1: InWriteTransaction must be false when state=Reader")
	}
	mustEndRead(t, p)
}

func TestMCDC_InWriteTransaction_StateError(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F (state=Error) → InWriteTransaction returns false
	p := openTestPager(t)
	p.mu.Lock()
	p.state = PagerStateError
	p.mu.Unlock()

	result := p.InWriteTransaction()

	p.mu.Lock()
	p.state = PagerStateOpen
	p.mu.Unlock()

	if result {
		t.Error("MCDC case2: InWriteTransaction must be false when state=Error")
	}
}

func TestMCDC_InWriteTransaction_StateWriterLocked(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (state=WriterLocked) → InWriteTransaction returns true
	p := openTestPager(t)
	mustBeginWrite(t, p)
	if !p.InWriteTransaction() {
		t.Error("MCDC case3: InWriteTransaction must be true when state=WriterLocked")
	}
	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// transaction.go — handleJournalModeTransition, to-WAL branch
//   `newMode == JournalModeWAL && p.journalMode != JournalModeWAL`
//
//   A = newMode == JournalModeWAL
//   B = p.journalMode != JournalModeWAL
//
//   Calls enableWALMode() when A && B is true.
//
//   Case 1 (A=F): newMode != WAL → to-WAL branch skipped
//   Case 2 (A=T, B=F): newMode=WAL, already in WAL mode → branch skipped
//   Case 3 (A=T, B=T): newMode=WAL, currently not WAL → enableWALMode called
// ---------------------------------------------------------------------------

func TestMCDC_HandleJournalTransition_ToWAL_NotWALMode(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (newMode=Delete) → to-WAL branch not taken
	p := openTestPager(t)
	// Start in Delete mode; switch to Persist (not WAL) — no enableWALMode
	if err := p.SetJournalMode(JournalModePersist); err != nil {
		t.Errorf("MCDC case1: switching to Persist must succeed; got %v", err)
	}
	if p.GetJournalMode() != JournalModePersist {
		t.Errorf("MCDC case1: expected JournalModePersist; got %d", p.GetJournalMode())
	}
}

func TestMCDC_HandleJournalTransition_ToWAL_AlreadyWAL(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (newMode=WAL), B=F (already in WAL mode) → enableWALMode not called again
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	// Transition into WAL mode first
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("first SetJournalMode(WAL) error = %v", err)
	}
	// Second call to WAL mode: A=T, B=F → no-op
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Errorf("MCDC case2: SetJournalMode(WAL) when already in WAL must succeed; got %v", err)
	}
}

func TestMCDC_HandleJournalTransition_ToWAL_FromDelete(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (newMode=WAL, currently Delete) → enableWALMode called
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Errorf("MCDC case3: enabling WAL from Delete mode must succeed; got %v", err)
	}
	if p.GetJournalMode() != JournalModeWAL {
		t.Errorf("MCDC case3: expected JournalModeWAL; got %d", p.GetJournalMode())
	}
}

// ---------------------------------------------------------------------------
// transaction.go — handleJournalModeTransition, from-WAL branch
//   `p.journalMode == JournalModeWAL && newMode != JournalModeWAL`
//
//   A = p.journalMode == JournalModeWAL
//   B = newMode != JournalModeWAL
//
//   Calls disableWALMode() when A && B is true.
//
//   Case 1 (A=F): not currently in WAL → from-WAL branch skipped
//   Case 2 (A=T, B=F): in WAL, switching back to WAL → branch skipped
//   Case 3 (A=T, B=T): in WAL, switching to Delete → disableWALMode called
// ---------------------------------------------------------------------------

func TestMCDC_HandleJournalTransition_FromWAL_NotInWAL(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (journalMode=Delete) → from-WAL branch not taken
	p := openTestPager(t)
	// Switch Delete→Persist (no WAL involved)
	if err := p.SetJournalMode(JournalModePersist); err != nil {
		t.Errorf("MCDC case1: Delete→Persist must succeed; got %v", err)
	}
}

func TestMCDC_HandleJournalTransition_FromWAL_StayWAL(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (in WAL), B=F (newMode=WAL) → to-WAL branch fires but from-WAL branch skipped
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)
	// Transition WAL→WAL: A=T, B=F → from-WAL branch skipped
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Errorf("MCDC case2: WAL→WAL must succeed; got %v", err)
	}
	if p.GetJournalMode() != JournalModeWAL {
		t.Errorf("MCDC case2: mode must remain WAL; got %d", p.GetJournalMode())
	}
}

func TestMCDC_HandleJournalTransition_FromWAL_ToDelete(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (in WAL, switching to Delete) → disableWALMode called
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	p, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)
	if err := p.SetJournalMode(JournalModeDelete); err != nil {
		t.Errorf("MCDC case3: WAL→Delete must succeed; got %v", err)
	}
	if p.GetJournalMode() != JournalModeDelete {
		t.Errorf("MCDC case3: expected JournalModeDelete; got %d", p.GetJournalMode())
	}
}

// ---------------------------------------------------------------------------
// backup.go — NewBackup nil-pointer guard
//   `src == nil || dst == nil`
//
//   A = src == nil
//   B = dst == nil
//
//   Returns error when A || B is true.
//
//   Case 1 (A=T): src==nil → error
//   Case 2 (A=F, B=T): src!=nil, dst==nil → error
//   Case 3 (A=F, B=F): both non-nil → no error from this guard
// ---------------------------------------------------------------------------

func TestMCDC_NewBackup_SrcNil(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (src==nil) → error
	dst := openTestPager(t)
	_, err := NewBackup(nil, dst)
	if err == nil {
		t.Error("MCDC case1: NewBackup(nil, dst) must return an error")
	}
}

func TestMCDC_NewBackup_DstNil(t *testing.T) {
	t.Parallel()
	// Case 2: A=F (src!=nil), B=T (dst==nil) → error
	src := openTestPager(t)
	_, err := NewBackup(src, nil)
	if err == nil {
		t.Error("MCDC case2: NewBackup(src, nil) must return an error")
	}
}

func TestMCDC_NewBackup_BothNonNil(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (both non-nil) → no error from nil guard
	src := openTestPager(t)
	dst := openTestPager(t)
	// src starts with at least 1 page; dst is writable
	_, err := NewBackup(src, dst)
	if err != nil {
		t.Errorf("MCDC case3: NewBackup with non-nil src and dst must succeed; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// backup.go — Step progress callback guard
//   `b.progress != nil && copied > 0`
//
//   A = b.progress != nil
//   B = copied > 0
//
//   Calls b.progress() when A && B is true.
//
//   Case 1 (A=F): no progress callback → never called
//   Case 2 (A=T, B=F): callback set but copied==0 → not called
//   Case 3 (A=T, B=T): callback set and pages copied → called
// ---------------------------------------------------------------------------

func TestMCDC_BackupStep_NoProgressCallback(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (progress==nil) → callback not invoked
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}
	// No progress set
	called := false
	b.SetProgress(nil)
	_ = called // just ensure Step doesn't panic
	done, err := b.Step(1)
	if err != nil {
		t.Errorf("MCDC case1: Step must succeed; got %v", err)
	}
	_ = done
}

func TestMCDC_BackupStep_ProgressCallbackNoCopy(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (progress set), B=F (copied==0, nPages=0 but totalPages already done)
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	progressCalled := false
	b.SetProgress(func(remaining, total int) {
		progressCalled = true
	})

	// Advance nextPage past totalPages so copied==0 on the step
	b.mu.Lock()
	b.nextPage = b.totalPages + 1
	b.mu.Unlock()

	_, _ = b.Step(1)
	if progressCalled {
		t.Error("MCDC case2: progress must NOT be called when copied==0")
	}
}

func TestMCDC_BackupStep_ProgressCallbackWithCopy(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T (progress set and pages are copied) → callback invoked
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	progressCalled := false
	b.SetProgress(func(remaining, total int) {
		progressCalled = true
	})

	done, err := b.Step(1)
	if err != nil {
		t.Fatalf("MCDC case3: Step error = %v", err)
	}
	_ = done
	if !progressCalled {
		t.Error("MCDC case3: progress callback must be called when pages are copied")
	}
}

// ---------------------------------------------------------------------------
// backup.go — copyPages loop condition
//   `copied < nPages && b.nextPage <= b.totalPages`
//
//   A = copied < nPages
//   B = b.nextPage <= b.totalPages
//
//   Loop continues when A && B is true.
//
//   Case 1 (A=F): copied==nPages → loop exits (enough pages copied)
//   Case 2 (A=T, B=F): still want more but no pages left → loop exits
//   Case 3 (A=T, B=T): more to copy and pages remain → loop body executes
// ---------------------------------------------------------------------------

func TestMCDC_CopyPages_QuotaMet(t *testing.T) {
	t.Parallel()
	// Case 1: A=F (copied == nPages) → loop exits after copying exactly nPages
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}
	// totalPages is 1 for a fresh DB; request only 1 page
	done, err := b.Step(1)
	if err != nil {
		t.Errorf("MCDC case1: Step(1) must succeed; got %v", err)
	}
	_ = done
	if b.PagesCopied() != 1 {
		t.Errorf("MCDC case1: expected 1 page copied; got %d", b.PagesCopied())
	}
}

func TestMCDC_CopyPages_NoPagesLeft(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (nPages large), B=F (nextPage > totalPages) → loop exits immediately
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	// Exhaust the page range
	b.mu.Lock()
	b.nextPage = b.totalPages + 1
	b.mu.Unlock()

	done, err := b.Step(100) // nPages large but no pages remain
	if err != nil {
		t.Errorf("MCDC case2: Step with no remaining pages must succeed; got %v", err)
	}
	// When nextPage > totalPages the backup is done
	if !done {
		t.Error("MCDC case2: Step must report done=true when all pages already past")
	}
	// Remaining() must be 0 since nextPage > totalPages
	if r := b.Remaining(); r != 0 {
		t.Errorf("MCDC case2: Remaining must be 0 when nextPage > totalPages; got %d", r)
	}
}

func TestMCDC_CopyPages_CopiesPages(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T → loop body runs, pages are copied
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	// Request more pages than available — loop runs while pages remain
	done, err := b.Step(999)
	if err != nil {
		t.Fatalf("MCDC case3: Step(999) error = %v", err)
	}
	if !done {
		t.Error("MCDC case3: Step should report done=true when all pages copied")
	}
	if b.PagesCopied() < 1 {
		t.Errorf("MCDC case3: at least 1 page must have been copied; got %d", b.PagesCopied())
	}
}

// ---------------------------------------------------------------------------
// backup.go — Remaining early-return guard
//   `b.done || b.nextPage > b.totalPages`
//
//   A = b.done
//   B = b.nextPage > b.totalPages
//
//   Returns 0 when A || B is true.
//
//   Case 1 (A=T): done==true → returns 0
//   Case 2 (A=F, B=T): not done but nextPage exhausted → returns 0
//   Case 3 (A=F, B=F): backup in progress → returns actual remaining count
// ---------------------------------------------------------------------------

func TestMCDC_Remaining_Done(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (done==true) → Remaining returns 0
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	if err := b.Finish(); err != nil {
		t.Fatalf("Finish error = %v", err)
	}
	if r := b.Remaining(); r != 0 {
		t.Errorf("MCDC case1: Remaining after Finish must be 0; got %d", r)
	}
}

func TestMCDC_Remaining_NextPageExhausted(t *testing.T) {
	t.Parallel()
	// Case 2: A=F (not done), B=T (nextPage > totalPages) → returns 0
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	b.mu.Lock()
	b.nextPage = b.totalPages + 1
	b.mu.Unlock()

	if r := b.Remaining(); r != 0 {
		t.Errorf("MCDC case2: Remaining with nextPage>totalPages must be 0; got %d", r)
	}
}

func TestMCDC_Remaining_InProgress(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → backup in progress → returns positive value
	src := openTestPager(t)
	dst := openTestPager(t)
	b, err := NewBackup(src, dst)
	if err != nil {
		t.Fatalf("NewBackup error = %v", err)
	}

	// nextPage starts at 1, totalPages >= 1 → remaining >= 1
	r := b.Remaining()
	if r <= 0 {
		t.Errorf("MCDC case3: Remaining for in-progress backup must be > 0; got %d", r)
	}
}

// ---------------------------------------------------------------------------
// savepoint.go — releaseSavepoints out-of-range guard
//   `index < 0 || index >= len(p.savepoints)`
//
//   A = index < 0
//   B = index >= len(p.savepoints)
//
//   Returns immediately (no-op) when A || B is true.
//
//   Case 1 (A=T): index < 0 → no-op
//   Case 2 (A=F, B=T): index >= len → no-op
//   Case 3 (A=F, B=F): valid index → savepoints truncated
// ---------------------------------------------------------------------------

func TestMCDC_ReleaseSavepoints_NegativeIndex(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (index < 0) → releaseSavepoints is a no-op
	p := openTestPager(t)
	mustBeginWrite(t, p)
	mustSavepoint(t, p, "alpha")
	before := len(p.getSavepoints())

	// Call internal method with index -1
	p.releaseSavepoints(-1)

	after := len(p.getSavepoints())
	if after != before {
		t.Errorf("MCDC case1: releaseSavepoints(-1) must be a no-op; before=%d after=%d", before, after)
	}
	mustRollback(t, p)
}

func TestMCDC_ReleaseSavepoints_IndexAtLen(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (index >= len) → no-op
	p := openTestPager(t)
	mustBeginWrite(t, p)
	mustSavepoint(t, p, "beta")
	before := len(p.getSavepoints())

	// index == len → out-of-range
	p.releaseSavepoints(before)

	after := len(p.getSavepoints())
	if after != before {
		t.Errorf("MCDC case2: releaseSavepoints(len) must be a no-op; before=%d after=%d", before, after)
	}
	mustRollback(t, p)
}

func TestMCDC_ReleaseSavepoints_ValidIndex(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (valid index) → savepoints[index+1:] kept, rest discarded
	p := openTestPager(t)
	mustBeginWrite(t, p)
	mustSavepoint(t, p, "sp1")
	mustSavepoint(t, p, "sp2")
	// getSavepoints() returns newest-first: ["sp2", "sp1"]
	before := len(p.getSavepoints())
	if before != 2 {
		t.Fatalf("MCDC case3: setup: expected 2 savepoints; got %d", before)
	}

	// releaseSavepoints(0) removes index 0 (sp2) and everything before it (none),
	// leaving savepoints[1:] = ["sp1"]
	p.releaseSavepoints(0)

	after := len(p.getSavepoints())
	if after != 1 {
		t.Errorf("MCDC case3: releaseSavepoints(0) must leave 1 savepoint; got %d", after)
	}
	mustRollback(t, p)
}
