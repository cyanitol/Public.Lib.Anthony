package pager

import (
	"errors"
	"sync"
	"testing"
)

func TestLRUCacheCreate(t *testing.T) {
	tests := []struct {
		name      string
		config    LRUCacheConfig
		wantErr   bool
		errString string
	}{
		{
			name: "valid config with max pages",
			config: LRUCacheConfig{
				PageSize: 4096,
				MaxPages: 100,
			},
			wantErr: false,
		},
		{
			name: "valid config with max memory",
			config: LRUCacheConfig{
				PageSize:  4096,
				MaxMemory: 1024 * 1024, // 1MB
			},
			wantErr: false,
		},
		{
			name: "valid config with both limits",
			config: LRUCacheConfig{
				PageSize:  4096,
				MaxPages:  100,
				MaxMemory: 1024 * 1024,
			},
			wantErr: false,
		},
		{
			name: "invalid config with zero capacity",
			config: LRUCacheConfig{
				PageSize:  4096,
				MaxPages:  0,
				MaxMemory: 0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewLRUCache(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cache == nil {
					t.Error("expected non-nil cache")
				}
			}
		})
	}
}

func TestLRUCacheBasicOperations(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Test empty cache
	if cache.Size() != 0 {
		t.Errorf("expected size 0, got %d", cache.Size())
	}

	// Test Get on empty cache
	page := cache.Get(1)
	if page != nil {
		t.Error("expected nil from empty cache")
	}

	// Test Put
	page1 := NewDbPage(1, 4096)
	err := cache.Put(page1)
	if err != nil {
		t.Errorf("unexpected error on Put: %v", err)
	}

	if cache.Size() != 1 {
		t.Errorf("expected size 1, got %d", cache.Size())
	}

	// Test Get
	retrieved := cache.Get(1)
	if retrieved == nil {
		t.Error("expected non-nil page")
	}
	if retrieved.Pgno != 1 {
		t.Errorf("expected page number 1, got %d", retrieved.Pgno)
	}

	// Test Contains
	if !cache.Contains(1) {
		t.Error("expected cache to contain page 1")
	}
	if cache.Contains(2) {
		t.Error("expected cache to not contain page 2")
	}

	// Test Remove
	cache.Remove(1)
	if cache.Size() != 0 {
		t.Errorf("expected size 0 after remove, got %d", cache.Size())
	}
	if cache.Contains(1) {
		t.Error("expected cache to not contain page 1 after remove")
	}
}

func TestLRUCacheLRUOrder(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add pages 1, 2, 3
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		cache.Put(page)
	}

	// Order should be 3, 2, 1 (most to least recently used)
	order := cache.LRUOrder()
	expected := []Pgno{3, 2, 1}
	if len(order) != len(expected) {
		t.Fatalf("expected %d pages, got %d", len(expected), len(order))
	}
	for i, pgno := range expected {
		if order[i] != pgno {
			t.Errorf("position %d: expected %d, got %d", i, pgno, order[i])
		}
	}

	// Access page 1 - should move to front
	cache.Get(1)
	order = cache.LRUOrder()
	expected = []Pgno{1, 3, 2}
	for i, pgno := range expected {
		if order[i] != pgno {
			t.Errorf("after Get(1) position %d: expected %d, got %d", i, pgno, order[i])
		}
	}

	// Touch page 2 - should move to front
	cache.Touch(2)
	order = cache.LRUOrder()
	expected = []Pgno{2, 1, 3}
	for i, pgno := range expected {
		if order[i] != pgno {
			t.Errorf("after Touch(2) position %d: expected %d, got %d", i, pgno, order[i])
		}
	}
}

func TestLRUCacheEviction(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 3)

	// Add 3 pages (at capacity)
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.Unref() // Decrease ref count so it can be evicted
		cache.Put(page)
	}

	if cache.Size() != 3 {
		t.Errorf("expected size 3, got %d", cache.Size())
	}

	// Add page 4 - should evict page 1 (least recently used)
	page4 := NewDbPage(4, 4096)
	page4.Unref()
	err := cache.Put(page4)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if cache.Size() != 3 {
		t.Errorf("expected size 3 after eviction, got %d", cache.Size())
	}

	// Page 1 should be evicted
	if cache.Contains(1) {
		t.Error("page 1 should have been evicted")
	}

	// Pages 2, 3, 4 should still be present
	for _, pgno := range []Pgno{2, 3, 4} {
		if !cache.Contains(pgno) {
			t.Errorf("page %d should still be in cache", pgno)
		}
	}
}

func TestLRUCacheEvictionSkipsDirty(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 3)

	// Add page 1 and mark it dirty
	page1 := NewDbPage(1, 4096)
	page1.Unref()
	page1.MakeDirty()
	cache.Put(page1)

	// Add pages 2 and 3
	for i := Pgno(2); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	// Try to add page 4 - should evict page 2 (not page 1 which is dirty)
	page4 := NewDbPage(4, 4096)
	page4.Unref()
	cache.Put(page4)

	// Page 1 should still be present (dirty, can't evict)
	if !cache.Contains(1) {
		t.Error("dirty page 1 should not have been evicted")
	}

	// Page 2 should be evicted (clean, least recently used of clean pages)
	if cache.Contains(2) {
		t.Error("page 2 should have been evicted")
	}
}

func TestLRUCacheEvictionSkipsReferenced(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 3)

	// Add page 1 and keep a reference
	page1 := NewDbPage(1, 4096)
	// Don't unref - keep reference count > 0
	cache.Put(page1)

	// Add pages 2 and 3 (unreferenced)
	for i := Pgno(2); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	// Try to add page 4
	page4 := NewDbPage(4, 4096)
	page4.Unref()
	cache.Put(page4)

	// Page 1 should still be present (has reference)
	if !cache.Contains(1) {
		t.Error("referenced page 1 should not have been evicted")
	}

	// Page 2 should be evicted
	if cache.Contains(2) {
		t.Error("page 2 should have been evicted")
	}
}

func TestLRUCacheDirtyList(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add some pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		if i%2 == 0 {
			page.MakeDirty()
		}
		cache.Put(page)
	}

	// Check dirty pages
	dirty := cache.GetDirtyPages()
	if len(dirty) != 2 {
		t.Errorf("expected 2 dirty pages, got %d", len(dirty))
	}

	// Mark page 3 as dirty through cache
	cache.MarkDirtyByPgno(3)
	dirty = cache.GetDirtyPages()
	if len(dirty) != 3 {
		t.Errorf("expected 3 dirty pages after MarkDirty, got %d", len(dirty))
	}

	// MakeClean should clear all dirty pages
	cache.MakeClean()
	dirty = cache.GetDirtyPages()
	if len(dirty) != 0 {
		t.Errorf("expected 0 dirty pages after MakeClean, got %d", len(dirty))
	}
}

func TestLRUCacheStats(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add a page
	page := NewDbPage(1, 4096)
	cache.Put(page)

	// Initial stats should be 0
	hits, misses := cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("expected 0 hits and misses, got %d hits and %d misses", hits, misses)
	}

	// Miss
	cache.Get(2)
	hits, misses = cache.Stats()
	if hits != 0 || misses != 1 {
		t.Errorf("expected 0 hits and 1 miss, got %d hits and %d misses", hits, misses)
	}

	// Hit
	cache.Get(1)
	hits, misses = cache.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("expected 1 hit and 1 miss, got %d hits and %d misses", hits, misses)
	}

	// Hit rate
	rate := cache.HitRate()
	expectedRate := 50.0
	if rate != expectedRate {
		t.Errorf("expected hit rate %.1f%%, got %.1f%%", expectedRate, rate)
	}

	// Reset stats
	cache.ResetStats()
	hits, misses = cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("expected 0 hits and misses after reset, got %d hits and %d misses", hits, misses)
	}
}

func TestLRUCacheMemoryUsage(t *testing.T) {
	pageSize := 4096
	cache := NewLRUCacheSimple(pageSize, 10)

	// Initial memory usage should be 0
	if cache.MemoryUsage() != 0 {
		t.Errorf("expected 0 memory usage, got %d", cache.MemoryUsage())
	}

	// Add 3 pages
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, pageSize)
		page.Unref()
		cache.Put(page)
	}

	expectedUsage := int64(3 * pageSize)
	if cache.MemoryUsage() != expectedUsage {
		t.Errorf("expected memory usage %d, got %d", expectedUsage, cache.MemoryUsage())
	}

	// Remove a page
	cache.Remove(1)
	expectedUsage = int64(2 * pageSize)
	if cache.MemoryUsage() != expectedUsage {
		t.Errorf("expected memory usage %d after remove, got %d", expectedUsage, cache.MemoryUsage())
	}

	// Clear cache
	cache.Clear()
	if cache.MemoryUsage() != 0 {
		t.Errorf("expected 0 memory usage after clear, got %d", cache.MemoryUsage())
	}
}

func TestLRUCachePeek(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add pages 1, 2, 3
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		cache.Put(page)
	}

	// Current order: 3, 2, 1
	order := cache.LRUOrder()
	if order[0] != 3 {
		t.Errorf("expected page 3 at front, got %d", order[0])
	}

	// Peek page 1 - should NOT change order
	page := cache.Peek(1)
	if page == nil {
		t.Error("expected non-nil page from Peek")
	}

	order = cache.LRUOrder()
	if order[0] != 3 {
		t.Errorf("Peek should not change order, expected page 3 at front, got %d", order[0])
	}
}

func TestLRUCacheShrink(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add 5 pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	if cache.Size() != 5 {
		t.Errorf("expected size 5, got %d", cache.Size())
	}

	// Shrink to 3
	evicted := cache.Shrink(3)
	if evicted != 2 {
		t.Errorf("expected 2 evicted, got %d", evicted)
	}
	if cache.Size() != 3 {
		t.Errorf("expected size 3 after shrink, got %d", cache.Size())
	}
}

func TestLRUCacheEvictClean(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add 5 pages - make 2 of them dirty
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		if i == 2 || i == 4 {
			page.MakeDirty()
		}
		cache.Put(page)
	}

	if cache.Size() != 5 {
		t.Errorf("expected size 5, got %d", cache.Size())
	}

	// Evict all clean pages
	evicted := cache.EvictClean()
	if evicted != 3 {
		t.Errorf("expected 3 evicted, got %d", evicted)
	}
	if cache.Size() != 2 {
		t.Errorf("expected size 2 after EvictClean, got %d", cache.Size())
	}

	// Only dirty pages should remain
	if !cache.Contains(2) || !cache.Contains(4) {
		t.Error("dirty pages should remain in cache")
	}
}

func TestLRUCacheSetMaxPages(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add 5 pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	// Reduce max pages to 3
	err := cache.SetMaxPages(3)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should have evicted down to at most 3 pages (could be less if eviction stopped early)
	if cache.Size() > 3 {
		t.Errorf("expected size <= 3 after SetMaxPages, got %d", cache.Size())
	}
}

func TestLRUCachePages(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add some pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		cache.Put(page)
	}

	pages := cache.Pages()
	if len(pages) != 5 {
		t.Errorf("expected 5 pages, got %d", len(pages))
	}

	// All pages should be in the list
	pageSet := make(map[Pgno]bool)
	for _, pgno := range pages {
		pageSet[pgno] = true
	}
	for i := Pgno(1); i <= 5; i++ {
		if !pageSet[i] {
			t.Errorf("page %d should be in pages list", i)
		}
	}
}

func TestLRUCacheUpdateExisting(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Add page 1
	page1 := NewDbPage(1, 4096)
	page1.Data[0] = 0xAA
	cache.Put(page1)

	// Update page 1 with new data
	page1New := NewDbPage(1, 4096)
	page1New.Data[0] = 0xBB
	cache.Put(page1New)

	// Cache should still have only 1 entry
	if cache.Size() != 1 {
		t.Errorf("expected size 1, got %d", cache.Size())
	}

	// Should get the new page
	retrieved := cache.Get(1)
	if retrieved.Data[0] != 0xBB {
		t.Errorf("expected data 0xBB, got 0x%X", retrieved.Data[0])
	}
}

func TestLRUCacheConcurrent(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 100)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pgno := Pgno(base*10 + j + 1)
				page := NewDbPage(pgno, 4096)
				page.Unref()
				cache.Put(page)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				pgno := Pgno(j%50 + 1)
				cache.Get(pgno)
			}
		}()
	}

	wg.Wait()

	// Should have at most 100 pages
	if cache.Size() > 100 {
		t.Errorf("cache size exceeded max: %d", cache.Size())
	}
}

func TestLRUCacheMemoryLimit(t *testing.T) {
	pageSize := 4096
	maxMemory := int64(pageSize * 3) // Only 3 pages worth

	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize:  pageSize,
		MaxMemory: maxMemory,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Add 5 pages - should only keep 3
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, pageSize)
		page.Unref()
		cache.Put(page)
	}

	if cache.Size() != 3 {
		t.Errorf("expected size 3 with memory limit, got %d", cache.Size())
	}

	if cache.MemoryUsage() > maxMemory {
		t.Errorf("memory usage %d exceeds limit %d", cache.MemoryUsage(), maxMemory)
	}
}

func BenchmarkLRUCacheGet(b *testing.B) {
	cache := NewLRUCacheSimple(4096, 1000)

	// Populate cache
	for i := Pgno(1); i <= 1000; i++ {
		page := NewDbPage(i, 4096)
		cache.Put(page)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pgno := Pgno(i%1000 + 1)
		cache.Get(pgno)
	}
}

func BenchmarkLRUCachePut(b *testing.B) {
	cache := NewLRUCacheSimple(4096, 1000)

	pages := make([]*DbPage, b.N)
	for i := 0; i < b.N; i++ {
		pages[i] = NewDbPage(Pgno(i+1), 4096)
		pages[i].Unref()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(pages[i])
	}
}

func BenchmarkLRUCacheEviction(b *testing.B) {
	cache := NewLRUCacheSimple(4096, 100)

	// Fill cache with pages that can be evicted
	for i := Pgno(1); i <= 100; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	pages := make([]*DbPage, b.N)
	for i := 0; i < b.N; i++ {
		pages[i] = NewDbPage(Pgno(i+101), 4096)
		pages[i].Unref()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(pages[i])
	}
}

// Mock PageWriter for testing
type mockPageWriter struct {
	writtenPages map[Pgno]*DbPage
	writeError   error
	mu           sync.Mutex
}

func newMockPageWriter() *mockPageWriter {
	return &mockPageWriter{
		writtenPages: make(map[Pgno]*DbPage),
	}
}

func (m *mockPageWriter) writePage(page *DbPage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeError != nil {
		return m.writeError
	}

	m.writtenPages[page.Pgno] = page
	return nil
}

func (m *mockPageWriter) getWrittenPage(pgno Pgno) *DbPage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writtenPages[pgno]
}

func (m *mockPageWriter) clearWritten() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writtenPages = make(map[Pgno]*DbPage)
}

func TestLRUCacheFlush(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	// Add some dirty pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		page.MakeDirty()
		cache.Put(page)
	}

	// Verify we have 5 dirty pages
	dirty := cache.GetDirtyPages()
	if len(dirty) != 5 {
		t.Errorf("expected 5 dirty pages, got %d", len(dirty))
	}

	// Flush the cache
	flushed, err := cache.Flush()
	if err != nil {
		t.Errorf("unexpected error during flush: %v", err)
	}
	if flushed != 5 {
		t.Errorf("expected to flush 5 pages, got %d", flushed)
	}

	// Verify all pages were written
	for i := Pgno(1); i <= 5; i++ {
		if writer.getWrittenPage(i) == nil {
			t.Errorf("page %d was not written", i)
		}
	}

	// Verify dirty list is now empty
	dirty = cache.GetDirtyPages()
	if len(dirty) != 0 {
		t.Errorf("expected 0 dirty pages after flush, got %d", len(dirty))
	}
}

func TestLRUCacheFlushPage(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	// Add pages
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.MakeDirty()
		cache.Put(page)
	}

	// Flush only page 2
	err := cache.FlushPage(2)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify page 2 was written
	if writer.getWrittenPage(2) == nil {
		t.Error("page 2 should have been written")
	}

	// Verify page 2 is clean
	page := cache.Get(2)
	if page.IsDirty() {
		t.Error("page 2 should be clean after flush")
	}

	// Verify pages 1 and 3 were not written
	if writer.getWrittenPage(1) != nil {
		t.Error("page 1 should not have been written")
	}
	if writer.getWrittenPage(3) != nil {
		t.Error("page 3 should not have been written")
	}
}

func TestLRUCacheFlushNoPager(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Try to flush without setting a pager
	_, err := cache.Flush()
	if err == nil {
		t.Error("expected error when flushing without pager")
	}
}

func TestLRUCacheWriteThroughMode(t *testing.T) {
	config := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
		Mode:     WriteThroughMode,
	}
	cache, err := NewLRUCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	writer := newMockPageWriter()
	cache.SetPager(writer)

	// Add a clean page
	page1 := NewDbPage(1, 4096)
	cache.Put(page1)

	// Mark it dirty - should flush immediately in write-through mode
	err = cache.MarkDirtyByPgno(1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify page was written immediately
	if writer.getWrittenPage(1) == nil {
		t.Error("page should have been written immediately in write-through mode")
	}

	// Verify page is clean again
	if cache.Get(1).IsDirty() {
		t.Error("page should be clean after write-through flush")
	}
}

func TestLRUCacheWriteBackMode(t *testing.T) {
	config := LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
		Mode:     WriteBackMode,
	}
	cache, err := NewLRUCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	writer := newMockPageWriter()
	cache.SetPager(writer)

	// Add a clean page
	page1 := NewDbPage(1, 4096)
	cache.Put(page1)

	// Mark it dirty - should NOT flush immediately in write-back mode
	err = cache.MarkDirtyByPgno(1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify page was NOT written yet
	if writer.getWrittenPage(1) != nil {
		t.Error("page should not be written in write-back mode until flush")
	}

	// Verify page is still dirty
	if !cache.Get(1).IsDirty() {
		t.Error("page should still be dirty in write-back mode")
	}

	// Now flush
	flushed, err := cache.Flush()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if flushed != 1 {
		t.Errorf("expected 1 page flushed, got %d", flushed)
	}

	// Now it should be written
	if writer.getWrittenPage(1) == nil {
		t.Error("page should be written after flush")
	}
}

func TestLRUCacheSetMode(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// Default should be write-back
	if cache.Mode() != WriteBackMode {
		t.Error("default mode should be write-back")
	}

	// Change to write-through
	cache.SetMode(WriteThroughMode)
	if cache.Mode() != WriteThroughMode {
		t.Error("mode should be write-through after SetMode")
	}
}

func TestLRUCacheEvictMethod(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 3)

	// Add 3 pages
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.Unref()
		cache.Put(page)
	}

	if cache.Size() != 3 {
		t.Errorf("expected size 3, got %d", cache.Size())
	}

	// Evict one page
	_, err := cache.Evict()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if cache.Size() != 2 {
		t.Errorf("expected size 2 after eviction, got %d", cache.Size())
	}
}

func TestLRUCacheFlushWithError(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	writer.writeError = errors.New("disk full")
	cache.SetPager(writer)

	// Add dirty pages
	for i := Pgno(1); i <= 3; i++ {
		page := NewDbPage(i, 4096)
		page.MakeDirty()
		cache.Put(page)
	}

	// Try to flush - should get error
	_, err := cache.Flush()
	if err == nil {
		t.Error("expected error during flush")
	}
}

// TestDefaultLRUCacheConfig tests the default config function
func TestDefaultLRUCacheConfig(t *testing.T) {
	config := DefaultLRUCacheConfig(4096)

	if config.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", config.PageSize)
	}

	if config.MaxPages <= 0 && config.MaxMemory <= 0 {
		t.Error("expected either MaxPages or MaxMemory to be set")
	}

	if config.Mode != WriteBackMode {
		t.Errorf("expected write-back mode, got %d", config.Mode)
	}
}

// TestLRUCacheSetMaxMemory tests the SetMaxMemory function
func TestLRUCacheSetMaxMemory(t *testing.T) {
	tests := []struct {
		name      string
		maxPages  int
		maxMemory int64
		newMemory int64
		wantErr   bool
	}{
		{
			name:      "valid memory limit",
			maxPages:  10,
			maxMemory: 100000,
			newMemory: 50000,
			wantErr:   false,
		},
		{
			name:      "increase memory limit",
			maxPages:  10,
			maxMemory: 50000,
			newMemory: 100000,
			wantErr:   false,
		},
		{
			name:      "zero memory with zero pages",
			maxPages:  0,
			maxMemory: 100000,
			newMemory: 0,
			wantErr:   true,
		},
		{
			name:      "zero memory with positive pages",
			maxPages:  10,
			maxMemory: 100000,
			newMemory: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := LRUCacheConfig{
				PageSize:  4096,
				MaxPages:  tt.maxPages,
				MaxMemory: tt.maxMemory,
			}
			cache, err := NewLRUCache(config)
			if err != nil {
				t.Fatalf("failed to create cache: %v", err)
			}

			// Add some pages
			for i := Pgno(1); i <= 5; i++ {
				page := NewDbPage(i, 4096)
				page.Unref() // Make evictable
				cache.Put(page)
			}

			err = cache.SetMaxMemory(tt.newMemory)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// Verify memory limit is respected
				if tt.newMemory > 0 {
					memUsage := cache.MemoryUsage()
					if memUsage > tt.newMemory {
						t.Errorf("memory usage %d exceeds limit %d", memUsage, tt.newMemory)
					}
				}
			}
		})
	}
}

// TestLRUCacheMarkDirty tests the MarkDirty function
func TestLRUCacheMarkDirty(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	t.Run("mark clean page dirty in write-back mode", func(t *testing.T) {
		cache.SetMode(WriteBackMode)
		page := NewDbPage(1, 4096)
		cache.Put(page)

		// Mark dirty - void function
		cache.MarkDirty(page)

		if !page.IsDirty() {
			t.Error("page should be dirty")
		}
	})

	t.Run("mark page dirty in write-through mode", func(t *testing.T) {
		cache.SetMode(WriteThroughMode)
		cache.Clear()

		page := NewDbPage(2, 4096)
		cache.Put(page)

		// Mark dirty should flush immediately
		cache.MarkDirty(page)

		// Page should be clean after write-through
		if page.IsDirty() {
			t.Error("page should be clean after write-through")
		}

		// Verify write happened
		if len(writer.writtenPages) != 1 {
			t.Errorf("expected 1 write, got %d", len(writer.writtenPages))
		}
	})

	t.Run("mark non-existent page dirty", func(t *testing.T) {
		cache.SetMode(WriteBackMode)
		page := NewDbPage(999, 4096)

		// Should not error for non-existent page
		cache.MarkDirty(page)
	})

	t.Run("mark dirty with write error", func(t *testing.T) {
		cache.SetMode(WriteThroughMode)
		cache.Clear()
		writer.writeError = errors.New("write failed")
		defer func() { writer.writeError = nil }()

		page := NewDbPage(3, 4096)
		cache.Put(page)

		// Mark dirty - will fail but function is void
		cache.MarkDirty(page)

		// Page should still be dirty since write failed
		if !page.IsDirty() {
			t.Error("page should still be dirty after failed write")
		}
	})
}

// TestLRUCacheEvictWithDirtyPages tests eviction with dirty pages
func TestLRUCacheEvictWithDirtyPages(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 5)

	// Fill cache with dirty pages
	for i := Pgno(1); i <= 5; i++ {
		page := NewDbPage(i, 4096)
		page.MakeDirty()
		page.Unref()
		cache.Put(page)
	}

	// Try to add another page - should fail because all are dirty
	page6 := NewDbPage(6, 4096)
	page6.Unref()
	err := cache.Put(page6)
	if err == nil {
		t.Error("expected error when cache full of dirty pages")
	}
}

// TestLRUCacheHitRateEdgeCases tests hit rate calculation edge cases
func TestLRUCacheHitRateEdgeCases(t *testing.T) {
	cache := NewLRUCacheSimple(4096, 10)

	// No accesses - should return 0.0
	hitRate := cache.HitRate()
	if hitRate != 0.0 {
		t.Errorf("expected 0.0 hit rate with no accesses, got %f", hitRate)
	}

	// Add a page - this counts as a miss
	page := NewDbPage(1, 4096)
	cache.Put(page)

	// Get triggers stats tracking
	cache.Get(999) // Miss

	// Check stats
	hits, misses := cache.Stats()
	if hits != 0 {
		t.Errorf("expected 0 hits, got %d", hits)
	}

	// Now get existing page - hit
	cache.Get(1)

	hits, _ = cache.Stats()
	if hits < 1 {
		t.Errorf("expected at least 1 hit, got %d", hits)
	}
	_ = misses // suppress unused warning
}
