// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package planner implements query plan caching for performance optimization.
package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// PlanCache is a thread-safe cache for optimized query plans.
// It caches plans by SQL hash and invalidates them on schema changes.
type PlanCache struct {
	mu            sync.RWMutex
	plans         map[string]*CachedPlan
	maxSize       int
	schemaVersion uint32
	stats         CacheStats
}

// CachedPlan represents a cached query plan with metadata.
type CachedPlan struct {
	SQL           string
	Hash          string
	Plan          *WhereInfo
	SchemaVersion uint32
	CreatedAt     time.Time
	LastUsed      time.Time
	HitCount      int64
}

// CacheStats tracks cache performance metrics.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Size      int
}

// NewPlanCache creates a new query plan cache with the specified maximum size.
// maxSize determines how many plans can be cached before eviction occurs.
func NewPlanCache(maxSize int) *PlanCache {
	if maxSize <= 0 {
		maxSize = 100 // Default cache size
	}
	return &PlanCache{
		plans:   make(map[string]*CachedPlan),
		maxSize: maxSize,
	}
}

// hashSQL computes a SHA-256 hash of the SQL query string.
// This is used as the cache key to uniquely identify queries.
func hashSQL(sql string) string {
	hash := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(hash[:])
}

// Get retrieves a cached plan for the given SQL query.
// Returns the cached plan and true if found and valid, nil and false otherwise.
func (c *PlanCache) Get(sql string) (*WhereInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hash := hashSQL(sql)
	cached, exists := c.plans[hash]
	if !exists {
		c.stats.Misses++
		return nil, false
	}

	// Check if the plan is still valid (schema hasn't changed)
	if cached.SchemaVersion != c.schemaVersion {
		// Plan is stale, will be cleaned up later
		c.stats.Misses++
		return nil, false
	}

	// Update statistics
	c.stats.Hits++
	cached.LastUsed = time.Now()
	cached.HitCount++

	return cached.Plan, true
}

// Put stores a query plan in the cache.
// If the cache is full, it evicts the least recently used plan.
func (c *PlanCache) Put(sql string, plan *WhereInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := hashSQL(sql)

	// Check if we need to evict an entry
	if len(c.plans) >= c.maxSize {
		c.evictLRU()
	}

	now := time.Now()
	c.plans[hash] = &CachedPlan{
		SQL:           sql,
		Hash:          hash,
		Plan:          plan,
		SchemaVersion: c.schemaVersion,
		CreatedAt:     now,
		LastUsed:      now,
		HitCount:      0,
	}

	c.stats.Size = len(c.plans)
}

// evictLRU removes the least recently used plan from the cache.
// Must be called with the lock held.
func (c *PlanCache) evictLRU() {
	var oldestHash string
	var oldestTime time.Time
	first := true

	for hash, cached := range c.plans {
		if first || cached.LastUsed.Before(oldestTime) {
			oldestHash = hash
			oldestTime = cached.LastUsed
			first = false
		}
	}

	if oldestHash != "" {
		delete(c.plans, oldestHash)
		c.stats.Evictions++
	}
}

// InvalidateAll invalidates all cached plans.
// This should be called when the database schema changes (CREATE/DROP/ALTER).
func (c *PlanCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemaVersion++
	c.plans = make(map[string]*CachedPlan)
	c.stats.Size = 0
}

// InvalidateTable invalidates all cached plans that reference a specific table.
// This is a more targeted invalidation for table-specific schema changes.
func (c *PlanCache) InvalidateTable(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Increment schema version to invalidate all plans
	// In a more sophisticated implementation, we could track which tables
	// each plan uses and only invalidate those that reference the changed table
	c.schemaVersion++

	// Remove stale plans
	for hash, cached := range c.plans {
		if cached.SchemaVersion != c.schemaVersion {
			delete(c.plans, hash)
		}
	}

	c.stats.Size = len(c.plans)
}

// SetSchemaVersion sets the current schema version.
// This should be called when the cache is initialized with the database's
// schema version (typically from the schema cookie).
func (c *PlanCache) SetSchemaVersion(version uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.schemaVersion != version {
		c.schemaVersion = version
		// Clear all cached plans since they're from a different schema version
		c.plans = make(map[string]*CachedPlan)
		c.stats.Size = 0
	}
}

// GetSchemaVersion returns the current schema version tracked by the cache.
func (c *PlanCache) GetSchemaVersion() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.schemaVersion
}

// Clear removes all cached plans and resets statistics.
func (c *PlanCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.plans = make(map[string]*CachedPlan)
	c.stats = CacheStats{}
}

// Stats returns a copy of the current cache statistics.
func (c *PlanCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := c.stats
	stats.Size = len(c.plans)
	return stats
}

// Size returns the current number of cached plans.
func (c *PlanCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.plans)
}

// MaxSize returns the maximum cache size.
func (c *PlanCache) MaxSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxSize
}

// SetMaxSize changes the maximum cache size.
// If the new size is smaller than the current number of cached plans,
// entries will be evicted using LRU policy.
func (c *PlanCache) SetMaxSize(maxSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if maxSize <= 0 {
		maxSize = 1
	}

	c.maxSize = maxSize

	// Evict entries if we're over the new limit
	for len(c.plans) > c.maxSize {
		c.evictLRU()
	}

	c.stats.Size = len(c.plans)
}

// HitRate returns the cache hit rate as a percentage (0-100).
// Returns 0 if there have been no cache accesses.
func (c *PlanCache) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.stats.Hits + c.stats.Misses
	if total == 0 {
		return 0
	}

	return (float64(c.stats.Hits) / float64(total)) * 100
}
