// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// StmtCacheEntry represents a cached prepared statement with its compiled VDBE.
type StmtCacheEntry struct {
	sql           string
	vdbe          *vdbe.VDBE
	schemaVersion uint64
	element       *list.Element // For LRU tracking
}

// StmtCache provides an LRU cache for compiled VDBE programs.
// It automatically invalidates entries when the schema changes.
type StmtCache struct {
	mu            sync.RWMutex
	capacity      int
	entries       map[string]*StmtCacheEntry
	lruList       *list.List
	schemaVersion uint64

	// Metrics
	hits   atomic.Uint64
	misses atomic.Uint64
}

// NewStmtCache creates a new statement cache with the given capacity.
// capacity specifies the maximum number of cached statements.
func NewStmtCache(capacity int) *StmtCache {
	if capacity <= 0 {
		capacity = 100 // Default capacity
	}

	return &StmtCache{
		capacity: capacity,
		entries:  make(map[string]*StmtCacheEntry, capacity),
		lruList:  list.New(),
	}
}

// Get retrieves a cached VDBE program for the given SQL statement.
// Returns a cloned VDBE if found, or nil if not found or if the entry is invalid due to schema changes.
// The returned VDBE is a clone that can be safely executed independently.
func (c *StmtCache) Get(sql string) *vdbe.VDBE {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[sql]
	if !ok {
		c.misses.Add(1)
		return nil
	}

	// Check if entry is still valid (schema hasn't changed)
	if entry.schemaVersion != c.schemaVersion {
		// Schema changed, invalidate this entry
		c.remove(sql)
		c.misses.Add(1)
		return nil
	}

	// Move to front of LRU list (most recently used)
	c.lruList.MoveToFront(entry.element)

	c.hits.Add(1)

	// Clone the VDBE so each execution gets its own instance
	return c.cloneVdbe(entry.vdbe)
}

// Put adds a compiled VDBE program to the cache.
// If the cache is full, it evicts the least recently used entry.
func (c *StmtCache) Put(sql string, vdbe *vdbe.VDBE) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if entry already exists
	if entry, ok := c.entries[sql]; ok {
		// Update existing entry
		entry.vdbe = vdbe
		entry.schemaVersion = c.schemaVersion
		c.lruList.MoveToFront(entry.element)
		return
	}

	// Evict LRU entry if cache is full
	if len(c.entries) >= c.capacity {
		c.evictLRU()
	}

	// Create new entry
	element := c.lruList.PushFront(sql)
	c.entries[sql] = &StmtCacheEntry{
		sql:           sql,
		vdbe:          vdbe,
		schemaVersion: c.schemaVersion,
		element:       element,
	}
}

// InvalidateAll invalidates all cached entries by incrementing the schema version.
// This should be called whenever the database schema changes (DDL operations).
func (c *StmtCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemaVersion++
	// We don't immediately remove entries - they'll be removed lazily on Get()
}

// Clear removes all entries from the cache.
func (c *StmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*StmtCacheEntry, c.capacity)
	c.lruList = list.New()
}

// SetSchemaVersion manually sets the schema version.
// This is useful for synchronizing the cache with external schema version tracking.
func (c *StmtCache) SetSchemaVersion(version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemaVersion = version
}

// GetMetrics returns cache hit and miss counts.
func (c *StmtCache) GetMetrics() (hits, misses uint64) {
	return c.hits.Load(), c.misses.Load()
}

// HitRate returns the cache hit rate as a percentage (0-100).
func (c *StmtCache) HitRate() float64 {
	hits := c.hits.Load()
	misses := c.misses.Load()

	total := hits + misses
	if total == 0 {
		return 0.0
	}

	return float64(hits) * 100.0 / float64(total)
}

// ResetMetrics resets the hit and miss counters.
func (c *StmtCache) ResetMetrics() {
	c.hits.Store(0)
	c.misses.Store(0)
}

// Size returns the current number of entries in the cache.
func (c *StmtCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// Capacity returns the maximum capacity of the cache.
func (c *StmtCache) Capacity() int {
	return c.capacity
}

// SetCapacity changes the cache capacity.
// If the new capacity is smaller, entries are evicted to fit.
func (c *StmtCache) SetCapacity(capacity int) {
	if capacity <= 0 {
		capacity = 100 // Default capacity
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.capacity = capacity

	// Evict entries if over capacity
	for len(c.entries) > c.capacity {
		c.evictLRU()
	}
}

// evictLRU removes the least recently used entry from the cache.
// Must be called with mu held.
func (c *StmtCache) evictLRU() {
	if c.lruList.Len() == 0 {
		return
	}

	// Get LRU element (back of list)
	element := c.lruList.Back()
	if element == nil {
		return
	}

	sql := element.Value.(string)
	c.remove(sql)
}

// remove removes an entry from the cache.
// Must be called with mu held.
func (c *StmtCache) remove(sql string) {
	entry, ok := c.entries[sql]
	if !ok {
		return
	}

	// Finalize the VDBE to release resources
	if entry.vdbe != nil {
		entry.vdbe.Finalize()
	}

	// Remove from map and LRU list
	delete(c.entries, sql)
	c.lruList.Remove(entry.element)
}

// cloneVdbe creates a deep copy of a VDBE program.
// This allows the cached VDBE to be reused while each execution gets its own instance.
func (c *StmtCache) cloneVdbe(original *vdbe.VDBE) *vdbe.VDBE {
	if original == nil {
		return nil
	}

	clone := vdbe.New()

	// Copy program instructions
	clone.Program = make([]*vdbe.Instruction, len(original.Program))
	for i, instr := range original.Program {
		// Create a copy of the instruction
		instrCopy := &vdbe.Instruction{
			Opcode:  instr.Opcode,
			P1:      instr.P1,
			P2:      instr.P2,
			P3:      instr.P3,
			P4:      instr.P4,
			P4Type:  instr.P4Type,
			P5:      instr.P5,
			Comment: instr.Comment,
		}
		clone.Program[i] = instrCopy
	}

	// Copy result columns
	if len(original.ResultCols) > 0 {
		clone.ResultCols = make([]string, len(original.ResultCols))
		copy(clone.ResultCols, original.ResultCols)
	}

	// Copy metadata
	clone.ReadOnly = original.ReadOnly
	clone.InTxn = original.InTxn
	clone.NumMem = original.NumMem
	clone.NumCursor = original.NumCursor

	return clone
}
