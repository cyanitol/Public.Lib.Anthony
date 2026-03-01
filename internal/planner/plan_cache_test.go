// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
	"time"
)

// TestNewPlanCache tests the creation of a new plan cache.
func TestNewPlanCache(t *testing.T) {
	cache := NewPlanCache(50)
	if cache == nil {
		t.Fatal("NewPlanCache returned nil")
	}
	if cache.MaxSize() != 50 {
		t.Errorf("Expected max size 50, got %d", cache.MaxSize())
	}
	if cache.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", cache.Size())
	}
}

// TestNewPlanCacheDefaultSize tests that invalid size defaults to 100.
func TestNewPlanCacheDefaultSize(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		expected int
	}{
		{"zero size", 0, 100},
		{"negative size", -10, 100},
		{"valid size", 50, 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewPlanCache(tt.size)
			if cache.MaxSize() != tt.expected {
				t.Errorf("Expected max size %d, got %d", tt.expected, cache.MaxSize())
			}
		})
	}
}

// TestPlanCachePutGet tests basic put and get operations.
func TestPlanCachePutGet(t *testing.T) {
	cache := NewPlanCache(10)

	// Create a test plan
	plan := &WhereInfo{
		NOut: LogEst(100),
	}

	// Put the plan
	sql := "SELECT * FROM users WHERE id = ?"
	cache.Put(sql, plan)

	// Verify size
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	// Get the plan
	retrieved, found := cache.Get(sql)
	if !found {
		t.Fatal("Expected to find cached plan")
	}
	if retrieved != plan {
		t.Error("Retrieved plan does not match original")
	}
}

// TestPlanCacheMiss tests cache miss behavior.
func TestPlanCacheMiss(t *testing.T) {
	cache := NewPlanCache(10)

	// Try to get a plan that doesn't exist
	_, found := cache.Get("SELECT * FROM nonexistent")
	if found {
		t.Error("Expected cache miss for non-existent query")
	}

	stats := cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

// TestPlanCacheStats tests statistics tracking.
func TestPlanCacheStats(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	sql := "SELECT * FROM users WHERE id = ?"

	// Put a plan
	cache.Put(sql, plan)

	// Get it twice (two hits)
	cache.Get(sql)
	cache.Get(sql)

	// Try to get a non-existent plan (one miss)
	cache.Get("SELECT * FROM other")

	stats := cache.Stats()
	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.Size != 1 {
		t.Errorf("Expected size 1, got %d", stats.Size)
	}
}

// TestPlanCacheHitRate tests hit rate calculation.
func TestPlanCacheHitRate(t *testing.T) {
	cache := NewPlanCache(10)

	// No accesses yet
	if cache.HitRate() != 0 {
		t.Errorf("Expected hit rate 0 for empty cache, got %f", cache.HitRate())
	}

	plan := &WhereInfo{NOut: LogEst(100)}
	sql := "SELECT * FROM users WHERE id = ?"
	cache.Put(sql, plan)

	// 2 hits, 1 miss = 2/3 = 66.67%
	cache.Get(sql)
	cache.Get(sql)
	cache.Get("SELECT * FROM other")

	hitRate := cache.HitRate()
	expected := 66.66666666666666
	if hitRate < expected-0.01 || hitRate > expected+0.01 {
		t.Errorf("Expected hit rate ~66.67%%, got %f%%", hitRate)
	}
}

// TestPlanCacheLRUEviction tests LRU eviction strategy.
func TestPlanCacheLRUEviction(t *testing.T) {
	cache := NewPlanCache(3)

	plan := &WhereInfo{NOut: LogEst(100)}

	// Fill the cache
	cache.Put("SELECT 1", plan)
	time.Sleep(2 * time.Millisecond)
	cache.Put("SELECT 2", plan)
	time.Sleep(2 * time.Millisecond)
	cache.Put("SELECT 3", plan)

	if cache.Size() != 3 {
		t.Fatalf("Expected size 3, got %d", cache.Size())
	}

	// Access SELECT 1 to make it recently used
	cache.Get("SELECT 1")
	time.Sleep(2 * time.Millisecond)

	// Add a new entry, should evict SELECT 2 (oldest unused)
	cache.Put("SELECT 4", plan)

	if cache.Size() != 3 {
		t.Errorf("Expected size 3 after eviction, got %d", cache.Size())
	}

	// SELECT 1 should still be there
	if _, found := cache.Get("SELECT 1"); !found {
		t.Error("SELECT 1 should still be cached")
	}

	// SELECT 2 should be evicted
	if _, found := cache.Get("SELECT 2"); found {
		t.Error("SELECT 2 should have been evicted")
	}

	// SELECT 3 should still be there
	if _, found := cache.Get("SELECT 3"); !found {
		t.Error("SELECT 3 should still be cached")
	}

	// SELECT 4 should be there
	if _, found := cache.Get("SELECT 4"); !found {
		t.Error("SELECT 4 should be cached")
	}

	stats := cache.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", stats.Evictions)
	}
}

// TestPlanCacheSchemaVersion tests schema version tracking.
func TestPlanCacheSchemaVersion(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	sql := "SELECT * FROM users WHERE id = ?"

	// Put a plan
	cache.Put(sql, plan)

	// Should be able to retrieve it
	if _, found := cache.Get(sql); !found {
		t.Fatal("Expected to find cached plan")
	}

	// Increment schema version (simulating schema change)
	cache.InvalidateAll()

	// Should not find the plan anymore
	if _, found := cache.Get(sql); found {
		t.Error("Expected cache miss after schema change")
	}

	// Cache should be empty
	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after invalidation, got %d", cache.Size())
	}
}

// TestPlanCacheSetSchemaVersion tests setting schema version.
func TestPlanCacheSetSchemaVersion(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	cache.Put("SELECT 1", plan)

	// Set a different schema version
	cache.SetSchemaVersion(42)

	// Old plans should be invalidated
	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after schema version change, got %d", cache.Size())
	}

	// New plans should use the new version
	cache.Put("SELECT 2", plan)
	if cache.GetSchemaVersion() != 42 {
		t.Errorf("Expected schema version 42, got %d", cache.GetSchemaVersion())
	}

	// Setting the same version should not clear cache
	cache.SetSchemaVersion(42)
	if cache.Size() != 1 {
		t.Errorf("Expected size 1 after same schema version, got %d", cache.Size())
	}
}

// TestPlanCacheInvalidateTable tests table-specific invalidation.
func TestPlanCacheInvalidateTable(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	cache.Put("SELECT * FROM users", plan)
	cache.Put("SELECT * FROM posts", plan)

	if cache.Size() != 2 {
		t.Fatalf("Expected size 2, got %d", cache.Size())
	}

	// Invalidate all plans (schema version changes)
	cache.InvalidateTable("users")

	// All plans should be invalidated since schema version changed
	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after table invalidation, got %d", cache.Size())
	}
}

// TestPlanCacheClear tests clearing the cache.
func TestPlanCacheClear(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	cache.Put("SELECT 1", plan)
	cache.Put("SELECT 2", plan)
	cache.Get("SELECT 1")
	cache.Get("SELECT 3") // miss

	if cache.Size() != 2 {
		t.Fatalf("Expected size 2, got %d", cache.Size())
	}

	// Clear the cache
	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.Size())
	}

	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.Evictions != 0 {
		t.Error("Expected all stats to be zero after clear")
	}
}

// TestPlanCacheSetMaxSize tests changing the maximum cache size.
func TestPlanCacheSetMaxSize(t *testing.T) {
	cache := NewPlanCache(10)

	plan := &WhereInfo{NOut: LogEst(100)}
	for i := 0; i < 5; i++ {
		cache.Put("SELECT "+string(rune('0'+i)), plan)
	}

	if cache.Size() != 5 {
		t.Fatalf("Expected size 5, got %d", cache.Size())
	}

	// Reduce max size to 3, should evict 2 entries
	cache.SetMaxSize(3)

	if cache.MaxSize() != 3 {
		t.Errorf("Expected max size 3, got %d", cache.MaxSize())
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size 3 after reducing max size, got %d", cache.Size())
	}

	stats := cache.Stats()
	if stats.Evictions != 2 {
		t.Errorf("Expected 2 evictions, got %d", stats.Evictions)
	}

	// Increase max size
	cache.SetMaxSize(10)
	if cache.MaxSize() != 10 {
		t.Errorf("Expected max size 10, got %d", cache.MaxSize())
	}

	// Size should remain 3
	if cache.Size() != 3 {
		t.Errorf("Expected size 3 after increasing max size, got %d", cache.Size())
	}
}

// TestPlanCacheSetMaxSizeZero tests that zero or negative size defaults to 1.
func TestPlanCacheSetMaxSizeZero(t *testing.T) {
	cache := NewPlanCache(10)

	cache.SetMaxSize(0)
	if cache.MaxSize() != 1 {
		t.Errorf("Expected max size 1 for zero input, got %d", cache.MaxSize())
	}

	cache.SetMaxSize(-5)
	if cache.MaxSize() != 1 {
		t.Errorf("Expected max size 1 for negative input, got %d", cache.MaxSize())
	}
}

// TestPlanCacheHashSQL tests SQL hashing.
func TestPlanCacheHashSQL(t *testing.T) {
	// Same SQL should produce same hash
	sql1 := "SELECT * FROM users WHERE id = ?"
	hash1 := hashSQL(sql1)
	hash2 := hashSQL(sql1)

	if hash1 != hash2 {
		t.Error("Same SQL should produce same hash")
	}

	// Different SQL should produce different hash
	sql2 := "SELECT * FROM posts WHERE id = ?"
	hash3 := hashSQL(sql2)

	if hash1 == hash3 {
		t.Error("Different SQL should produce different hash")
	}

	// Hash should be a 64-character hex string (SHA-256)
	if len(hash1) != 64 {
		t.Errorf("Expected hash length 64, got %d", len(hash1))
	}
}

// TestPlanCacheConcurrentAccess tests thread-safe concurrent access.
func TestPlanCacheConcurrentAccess(t *testing.T) {
	cache := NewPlanCache(100)
	plan := &WhereInfo{NOut: LogEst(100)}

	// Run concurrent put and get operations
	done := make(chan bool)
	workers := 10
	operations := 100

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			for i := 0; i < operations; i++ {
				sql := "SELECT " + string(rune('0'+workerID))
				cache.Put(sql, plan)
				cache.Get(sql)
			}
			done <- true
		}(w)
	}

	// Wait for all workers to complete
	for w := 0; w < workers; w++ {
		<-done
	}

	// Cache should contain entries
	if cache.Size() == 0 {
		t.Error("Expected cache to contain entries after concurrent access")
	}
}

// TestPlanCacheLastUsedUpdate tests that LastUsed is updated on access.
func TestPlanCacheLastUsedUpdate(t *testing.T) {
	cache := NewPlanCache(10)
	plan := &WhereInfo{NOut: LogEst(100)}

	sql := "SELECT * FROM users"
	cache.Put(sql, plan)

	// Get the cached entry
	hash := hashSQL(sql)
	cache.mu.RLock()
	cached1 := cache.plans[hash]
	lastUsed1 := cached1.LastUsed
	cache.mu.RUnlock()

	time.Sleep(10 * time.Millisecond)

	// Access the plan
	cache.Get(sql)

	// Check that LastUsed was updated
	cache.mu.RLock()
	cached2 := cache.plans[hash]
	lastUsed2 := cached2.LastUsed
	cache.mu.RUnlock()

	if !lastUsed2.After(lastUsed1) {
		t.Error("Expected LastUsed to be updated after Get")
	}
}

// TestPlanCacheHitCount tests that hit count is incremented.
func TestPlanCacheHitCount(t *testing.T) {
	cache := NewPlanCache(10)
	plan := &WhereInfo{NOut: LogEst(100)}

	sql := "SELECT * FROM users"
	cache.Put(sql, plan)

	// Access the plan multiple times
	for i := 0; i < 5; i++ {
		cache.Get(sql)
	}

	// Check hit count
	hash := hashSQL(sql)
	cache.mu.RLock()
	cached := cache.plans[hash]
	hitCount := cached.HitCount
	cache.mu.RUnlock()

	if hitCount != 5 {
		t.Errorf("Expected hit count 5, got %d", hitCount)
	}
}

// TestPlanCacheStaleSchemaVersion tests that stale plans are not returned.
func TestPlanCacheStaleSchemaVersion(t *testing.T) {
	cache := NewPlanCache(10)
	plan := &WhereInfo{NOut: LogEst(100)}

	sql := "SELECT * FROM users"
	cache.Put(sql, plan)

	// Increment schema version without clearing cache
	cache.mu.Lock()
	cache.schemaVersion++
	cache.mu.Unlock()

	// Should get a cache miss due to stale schema version
	_, found := cache.Get(sql)
	if found {
		t.Error("Expected cache miss for stale schema version")
	}

	stats := cache.Stats()
	if stats.Misses == 0 {
		t.Error("Expected at least one miss for stale schema version")
	}
}

// TestPlanCacheMultipleEvictions tests multiple evictions.
func TestPlanCacheMultipleEvictions(t *testing.T) {
	cache := NewPlanCache(5)
	plan := &WhereInfo{NOut: LogEst(100)}

	// Add 10 entries (should cause 5 evictions)
	for i := 0; i < 10; i++ {
		cache.Put("SELECT "+string(rune('0'+i)), plan)
		time.Sleep(1 * time.Millisecond)
	}

	if cache.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cache.Size())
	}

	stats := cache.Stats()
	if stats.Evictions != 5 {
		t.Errorf("Expected 5 evictions, got %d", stats.Evictions)
	}
}

// BenchmarkPlanCachePut benchmarks cache put operations.
func BenchmarkPlanCachePut(b *testing.B) {
	cache := NewPlanCache(1000)
	plan := &WhereInfo{NOut: LogEst(100)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql := "SELECT " + string(rune('0'+(i%26)))
		cache.Put(sql, plan)
	}
}

// BenchmarkPlanCacheGet benchmarks cache get operations.
func BenchmarkPlanCacheGet(b *testing.B) {
	cache := NewPlanCache(1000)
	plan := &WhereInfo{NOut: LogEst(100)}

	// Populate cache
	for i := 0; i < 100; i++ {
		sql := "SELECT " + string(rune('0'+(i%26)))
		cache.Put(sql, plan)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql := "SELECT " + string(rune('0'+(i%26)))
		cache.Get(sql)
	}
}

// BenchmarkPlanCacheConcurrent benchmarks concurrent access.
func BenchmarkPlanCacheConcurrent(b *testing.B) {
	cache := NewPlanCache(1000)
	plan := &WhereInfo{NOut: LogEst(100)}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sql := "SELECT " + string(rune('0'+(i%26)))
			if i%2 == 0 {
				cache.Put(sql, plan)
			} else {
				cache.Get(sql)
			}
			i++
		}
	})
}
