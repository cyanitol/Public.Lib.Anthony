// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"os"
	"testing"
)

// TestPagerWithLRUCache tests the pager using the LRU cache
func TestPagerWithLRUCache(t *testing.T) {
	t.Parallel()
	tmpFile := "/tmp/test_lru_pager.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	// Create pager with LRU cache
	cacheConfig := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 5,
		Mode:     WriteBackMode,
	}

	pager, err := OpenWithLRUCache(tmpFile, false, 4096, cacheConfig)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Get page 1 (header page)
	page1, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page 1: %v", err)
	}

	// Modify the page
	if err := pager.Write(page1); err != nil {
		t.Fatalf("failed to write page 1: %v", err)
	}

	page1.Data[100] = 0xAB
	pager.Put(page1)

	// Get a few more pages to test LRU
	for i := Pgno(2); i <= 6; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		pager.Put(page)
	}

	// Check cache statistics
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		hits, misses := lruCache.Stats()
		t.Logf("Cache stats: hits=%d, misses=%d, hit rate=%.2f%%", hits, misses, lruCache.HitRate())

		// Verify cache size is at or below max
		if lruCache.Size() > cacheConfig.MaxPages {
			t.Errorf("cache size %d exceeds max %d", lruCache.Size(), cacheConfig.MaxPages)
		}
	}

	// Commit the transaction
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

// TestPagerWithLRUCache_WriteThroughMode tests write-through mode
func TestPagerWithLRUCache_WriteThroughMode(t *testing.T) {
	t.Parallel()
	tmpFile := "/tmp/test_lru_writethrough.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	// Create pager with LRU cache in write-through mode
	cacheConfig := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
		Mode:     WriteThroughMode,
	}

	pager, err := OpenWithLRUCache(tmpFile, false, 4096, cacheConfig)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Verify mode
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		if lruCache.Mode() != WriteThroughMode {
			t.Error("expected write-through mode")
		}
	}
}

// TestPagerWithLRUCache_LargeWorkload tests LRU cache with many pages
func TestPagerWithLRUCache_LargeWorkload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping slow test in short mode")
	}

	tmpFile := "/tmp/test_lru_large.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	// Create pager with small LRU cache
	cacheConfig := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
		Mode:     WriteBackMode,
	}

	pager, err := OpenWithLRUCache(tmpFile, false, 4096, cacheConfig)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Access many pages (more than cache size)
	for i := Pgno(1); i <= 50; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		// Write to every 5th page
		if i%5 == 0 {
			if err := pager.Write(page); err != nil {
				t.Fatalf("failed to write page %d: %v", i, err)
			}
			page.Data[0] = byte(i)
		}

		pager.Put(page)
	}

	// Check statistics
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		hits, misses := lruCache.Stats()
		t.Logf("Cache stats after 50 accesses: hits=%d, misses=%d, hit rate=%.2f%%",
			hits, misses, lruCache.HitRate())

		// We expect some evictions to have occurred
		if lruCache.Size() > cacheConfig.MaxPages {
			t.Errorf("cache size %d exceeds max %d", lruCache.Size(), cacheConfig.MaxPages)
		}

		// Should have dirty pages
		dirty := lruCache.GetDirtyPages()
		t.Logf("Dirty pages: %d", len(dirty))
	}

	// Commit should flush all dirty pages
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// After commit, no dirty pages should remain
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		dirty := lruCache.GetDirtyPages()
		if len(dirty) != 0 {
			t.Errorf("expected 0 dirty pages after commit, got %d", len(dirty))
		}
	}
}

// TestPagerWithLRUCache_MemoryLimit tests memory-based eviction
func TestPagerWithLRUCache_MemoryLimit(t *testing.T) {
	t.Parallel()
	tmpFile := "/tmp/test_lru_memory.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	pageSize := 4096
	maxMemory := int64(pageSize * 5) // Only 5 pages worth of memory

	// Create pager with memory limit
	cacheConfig := LRUCacheConfig{
		PageSize:  pageSize,
		MaxMemory: maxMemory,
	}

	pager, err := OpenWithLRUCache(tmpFile, false, pageSize, cacheConfig)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Access 10 pages
	for i := Pgno(1); i <= 10; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		pager.Put(page)
	}

	// Check that memory usage is within limit
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		usage := lruCache.MemoryUsage()
		if usage > maxMemory {
			t.Errorf("memory usage %d exceeds limit %d", usage, maxMemory)
		}
		t.Logf("Memory usage: %d bytes (limit: %d bytes)", usage, maxMemory)
	}
}

// TestLRUCacheEvictionOrder tests that LRU eviction happens in correct order
func TestLRUCacheEvictionOrder(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 3)

	// Add pages 1, 2, 3
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.Unref() // Make evictable
		cache.Put(page)
	}

	// Access page 1 to make it most recently used
	cache.Get(1)

	// LRU order should be: 1, 3, 2
	order := cache.LRUOrder()
	expectedOrder := []Pgno{1, 3, 2}
	for i, pgno := range expectedOrder {
		if order[i] != pgno {
			t.Errorf("position %d: expected %d, got %d", i, pgno, order[i])
		}
	}

	// Add page 4 - should evict page 2 (least recently used)
	page4 := NewDbPage(4, 4096)
	page4.Unref()
	cache.Put(page4)

	// Page 2 should be evicted
	if cache.Contains(2) {
		t.Error("page 2 should have been evicted")
	}

	// Pages 1, 3, 4 should remain
	for _, pgno := range []Pgno{1, 3, 4} {
		if !cache.Contains(pgno) {
			t.Errorf("page %d should still be in cache", pgno)
		}
	}
}

// BenchmarkPagerWithLRUCache benchmarks pager with LRU cache
func BenchmarkPagerWithLRUCache(b *testing.B) {
	tmpFile := "/tmp/bench_lru_pager.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	cacheConfig := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 100,
		Mode:     WriteBackMode,
	}

	pager, err := OpenWithLRUCache(tmpFile, false, 4096, cacheConfig)
	if err != nil {
		b.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access pages in a pattern
		pgno := Pgno((i % 50) + 1)
		page, err := pager.Get(pgno)
		if err != nil {
			b.Fatalf("failed to get page: %v", err)
		}
		pager.Put(page)
	}
}

// BenchmarkPagerWithOldCache benchmarks pager with old PageCache
func BenchmarkPagerWithOldCache(b *testing.B) {
	tmpFile := "/tmp/bench_old_cache.db"
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	pager, err := OpenWithPageSize(tmpFile, false, 4096)
	if err != nil {
		b.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access pages in a pattern
		pgno := Pgno((i % 50) + 1)
		page, err := pager.Get(pgno)
		if err != nil {
			b.Fatalf("failed to get page: %v", err)
		}
		pager.Put(page)
	}
}
