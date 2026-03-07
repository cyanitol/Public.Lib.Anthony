// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"errors"
	"sync"
)

// Cache errors
var (
	ErrCachePageNotFound = errors.New("page not found in cache")
	ErrCacheCapacityZero = errors.New("cache capacity must be greater than 0")
)

// CacheEntry represents a single entry in the LRU cache.
// It contains both the page and LRU list linkage.
type CacheEntry struct {
	page *DbPage

	// LRU list linkage (doubly-linked list)
	lruNext *CacheEntry
	lruPrev *CacheEntry
}

// LRUCache implements a Least Recently Used page cache.
// It maintains pages in order of access, evicting the least recently
// used pages when capacity is reached.
//
// The cache uses a combination of:
// - Hash map for O(1) lookups by page number
// - Doubly-linked list for O(1) LRU ordering updates
//
// Thread-safety is provided through a read-write mutex.
type LRUCache struct {
	// Map of page number to cache entry for O(1) lookups
	entries map[Pgno]*CacheEntry

	// LRU list head (most recently used)
	lruHead *CacheEntry

	// LRU list tail (least recently used)
	lruTail *CacheEntry

	// Head of dirty page list
	dirtyHead *DbPage

	// Page size in bytes
	pageSize int

	// Maximum number of pages to cache
	maxPages int

	// Current memory usage in bytes
	memoryUsage int64

	// Maximum memory usage in bytes (0 = unlimited, use maxPages)
	maxMemory int64

	// Write mode (write-through or write-back)
	mode CacheMode

	// Pager for flushing pages (optional)
	pager PageWriter

	// Statistics
	hits   int64
	misses int64

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// CacheMode defines the cache write mode
type CacheMode int

const (
	// WriteThroughMode - writes are immediately synced to disk
	WriteThroughMode CacheMode = iota
	// WriteBackMode - writes are batched and flushed later
	WriteBackMode
)

// LRUCacheConfig holds configuration options for the LRU cache.
type LRUCacheConfig struct {
	PageSize  int       // Size of each page in bytes
	MaxPages  int       // Maximum number of pages to cache
	MaxMemory int64     // Maximum memory usage in bytes (0 = use MaxPages)
	Mode      CacheMode // Write mode (write-through or write-back)
}

// DefaultLRUCacheConfig returns a default cache configuration.
func DefaultLRUCacheConfig(pageSize int) LRUCacheConfig {
	return LRUCacheConfig{
		PageSize:  pageSize,
		MaxPages:  DefaultCacheSize,
		MaxMemory: 0,             // Use MaxPages instead
		Mode:      WriteBackMode, // Default to write-back for better performance
	}
}

// PageWriter is an interface for writing pages to storage.
// This allows the cache to flush dirty pages without depending on the full Pager type.
type PageWriter interface {
	writePage(page *DbPage) error
}

// NewLRUCache creates a new LRU page cache with the given configuration.
func NewLRUCache(config LRUCacheConfig) (*LRUCache, error) {
	if config.MaxPages <= 0 && config.MaxMemory <= 0 {
		return nil, ErrCacheCapacityZero
	}

	return &LRUCache{
		entries:   make(map[Pgno]*CacheEntry),
		pageSize:  config.PageSize,
		maxPages:  config.MaxPages,
		maxMemory: config.MaxMemory,
		mode:      config.Mode,
	}, nil
}

// NewLRUCacheSimple creates a new LRU cache with default settings.
func NewLRUCacheSimple(pageSize, maxPages int) *LRUCache {
	cache, _ := NewLRUCache(LRUCacheConfig{
		PageSize: pageSize,
		MaxPages: maxPages,
		Mode:     WriteBackMode,
	})
	return cache
}

// Get retrieves a page from the cache.
// If found, the page is moved to the front of the LRU list (most recently used).
// Returns nil if the page is not in the cache.
func (c *LRUCache) Get(pgno Pgno) *DbPage {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[pgno]
	if !ok {
		c.misses++
		return nil
	}

	c.hits++

	// Move to front of LRU list (most recently used)
	c.moveToFront(entry)

	return entry.page
}

// Peek retrieves a page from the cache without updating LRU order.
// This is useful for checking if a page exists without affecting eviction priority.
func (c *LRUCache) Peek(pgno Pgno) *DbPage {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if entry, ok := c.entries[pgno]; ok {
		return entry.page
	}
	return nil
}

// Put adds a page to the cache.
// If the cache is full, the least recently used clean pages are evicted.
// If a page with the same number already exists, it is replaced.
func (c *LRUCache) Put(page *DbPage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.putLocked(page)
}

// putLocked adds a page to the cache (must hold lock).
func (c *LRUCache) putLocked(page *DbPage) error {
	if entry, ok := c.entries[page.Pgno]; ok {
		return c.updateExistingEntry(entry, page)
	}
	return c.addNewEntry(page)
}

// updateExistingEntry updates an existing cache entry.
func (c *LRUCache) updateExistingEntry(entry *CacheEntry, page *DbPage) error {
	oldPage := entry.page
	entry.page = page
	c.moveToFront(entry)
	c.updateDirtyListForReplacement(oldPage, page)
	return nil
}

// updateDirtyListForReplacement updates dirty list when replacing a page.
func (c *LRUCache) updateDirtyListForReplacement(oldPage, newPage *DbPage) {
	if oldPage.IsDirty() && !newPage.IsDirty() {
		c.removeFromDirtyList(oldPage)
	} else if !oldPage.IsDirty() && newPage.IsDirty() {
		c.addToDirtyList(newPage)
	}
}

// addNewEntry adds a new page to the cache.
func (c *LRUCache) addNewEntry(page *DbPage) error {
	if c.isAtCapacity() {
		if err := c.evictLRU(1); err != nil {
			return err
		}
	}

	entry := &CacheEntry{page: page}
	c.entries[page.Pgno] = entry
	c.addToFront(entry)
	c.memoryUsage += int64(c.pageSize)

	if page.IsDirty() {
		c.addToDirtyList(page)
	}

	return nil
}

// Remove removes a page from the cache.
func (c *LRUCache) Remove(pgno Pgno) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeLocked(pgno)
}

// removeLocked removes a page from the cache (must hold lock).
func (c *LRUCache) removeLocked(pgno Pgno) {
	entry, ok := c.entries[pgno]
	if !ok {
		return
	}

	// Remove from dirty list if present
	if entry.page.IsDirty() {
		c.removeFromDirtyList(entry.page)
	}

	// Remove from LRU list
	c.removeFromLRU(entry)

	// Remove from map
	delete(c.entries, pgno)

	// Update memory usage
	c.memoryUsage -= int64(c.pageSize)
}

// Clear removes all pages from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[Pgno]*CacheEntry)
	c.lruHead = nil
	c.lruTail = nil
	c.dirtyHead = nil
	c.memoryUsage = 0
}

// GetDirtyPages returns a list of all dirty pages in the cache.
func (c *LRUCache) GetDirtyPages() []*DbPage {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var dirty []*DbPage
	current := c.dirtyHead
	for current != nil {
		dirty = append(dirty, current)
		current = current.dirtyNext
	}

	return dirty
}

// MakeClean marks all pages as clean and clears the dirty list.
func (c *LRUCache) MakeClean() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear dirty list
	c.dirtyHead = nil

	// Mark all pages as clean
	for _, entry := range c.entries {
		entry.page.MakeClean()
		entry.page.dirtyNext = nil
		entry.page.dirtyPrev = nil
	}
}

// Size returns the number of pages in the cache.
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// MemoryUsage returns the current memory usage in bytes.
func (c *LRUCache) MemoryUsage() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.memoryUsage
}

// Stats returns cache statistics (hits, misses).
func (c *LRUCache) Stats() (hits, misses int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses
}

// HitRate returns the cache hit rate as a percentage (0-100).
func (c *LRUCache) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total) * 100
}

// ResetStats resets the cache statistics.
func (c *LRUCache) ResetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits = 0
	c.misses = 0
}

// SetMaxPages updates the maximum number of pages.
// May trigger eviction if the new limit is lower than current size.
func (c *LRUCache) SetMaxPages(maxPages int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if maxPages <= 0 && c.maxMemory <= 0 {
		return ErrCacheCapacityZero
	}

	c.maxPages = maxPages

	// Evict if necessary
	for c.isAtCapacity() {
		if err := c.evictLRU(1); err != nil {
			break // Stop if we can't evict more
		}
	}

	return nil
}

// SetMaxMemory updates the maximum memory usage.
// May trigger eviction if the new limit is lower than current usage.
func (c *LRUCache) SetMaxMemory(maxMemory int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if maxMemory <= 0 && c.maxPages <= 0 {
		return ErrCacheCapacityZero
	}

	c.maxMemory = maxMemory

	// Evict if necessary
	for c.isAtCapacity() {
		if err := c.evictLRU(1); err != nil {
			break // Stop if we can't evict more
		}
	}

	return nil
}

// Touch moves a page to the front of the LRU list without retrieving it.
// This is useful when a page is accessed but you already have a reference.
func (c *LRUCache) Touch(pgno Pgno) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[pgno]; ok {
		c.moveToFront(entry)
	}
}

// MarkDirtyByPgno marks a page as dirty by page number.
// In write-through mode, immediately flushes the page to disk.
func (c *LRUCache) MarkDirtyByPgno(pgno Pgno) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[pgno]
	if !ok {
		return nil // Page not in cache
	}

	if !entry.page.IsDirty() {
		entry.page.MakeDirty()
		c.addToDirtyList(entry.page)

		// In write-through mode, flush immediately
		if c.mode == WriteThroughMode && c.pager != nil {
			if err := c.pager.writePage(entry.page); err != nil {
				return err
			}
			entry.page.MakeClean()
			c.removeFromDirtyList(entry.page)
		}
	}

	return nil
}

// MarkDirty marks a page as dirty and adds it to the dirty list.
// This implements the PageCacheInterface.
func (c *LRUCache) MarkDirty(page *DbPage) {
	if page == nil {
		return
	}
	// Ignore error since interface method is void
	_ = c.MarkDirtyByPgno(page.Pgno)
}

// Contains returns true if the cache contains the given page number.
func (c *LRUCache) Contains(pgno Pgno) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.entries[pgno]
	return ok
}

// SetPager sets the pager for flushing dirty pages.
// This is needed for write-through mode.
func (c *LRUCache) SetPager(pager PageWriter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pager = pager
}

// Flush writes all dirty pages to disk.
// Returns the number of pages flushed and any error encountered.
func (c *LRUCache) Flush() (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pager == nil {
		return 0, errors.New("no pager set for cache flush")
	}

	flushed := 0
	var firstErr error

	// Flush all dirty pages
	current := c.dirtyHead
	for current != nil {
		next := current.dirtyNext

		if err := c.pager.writePage(current); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// Continue trying to flush other pages
		} else {
			flushed++
			// Mark page as clean but keep it in cache
			current.MakeClean()
			c.removeFromDirtyList(current)
		}

		current = next
	}

	return flushed, firstErr
}

// FlushPage writes a specific dirty page to disk.
// Returns an error if the page is not in cache or if write fails.
func (c *LRUCache) FlushPage(pgno Pgno) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pager == nil {
		return errors.New("no pager set for cache flush")
	}

	entry, ok := c.entries[pgno]
	if !ok {
		return ErrCachePageNotFound
	}

	if !entry.page.IsDirty() {
		return nil // Nothing to flush
	}

	if err := c.pager.writePage(entry.page); err != nil {
		return err
	}

	entry.page.MakeClean()
	c.removeFromDirtyList(entry.page)
	return nil
}

// Evict evicts the least recently used page from the cache.
// Only evicts clean pages with no references.
// Returns the page number that was evicted, or 0 if no page was evicted.
func (c *LRUCache) Evict() (Pgno, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.evictLRU(1)
	if err != nil {
		return 0, err
	}

	return 0, nil
}

// Mode returns the current cache write mode.
func (c *LRUCache) Mode() CacheMode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mode
}

// SetMode sets the cache write mode.
func (c *LRUCache) SetMode(mode CacheMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mode = mode
}

// Pages returns a slice of all page numbers in the cache.
func (c *LRUCache) Pages() []Pgno {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pages := make([]Pgno, 0, len(c.entries))
	for pgno := range c.entries {
		pages = append(pages, pgno)
	}
	return pages
}

// LRUOrder returns page numbers in LRU order (most to least recently used).
func (c *LRUCache) LRUOrder() []Pgno {
	c.mu.RLock()
	defer c.mu.RUnlock()

	order := make([]Pgno, 0, len(c.entries))
	entry := c.lruHead
	for entry != nil {
		order = append(order, entry.page.Pgno)
		entry = entry.lruNext
	}
	return order
}

// Shrink evicts pages until the cache is at or below the target size.
func (c *LRUCache) Shrink(targetSize int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := 0
	for len(c.entries) > targetSize {
		if err := c.evictLRU(1); err != nil {
			break
		}
		evicted++
	}
	return evicted
}

// EvictClean evicts all clean pages from the cache.
// Returns the number of pages evicted.
func (c *LRUCache) EvictClean() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := 0
	var toRemove []Pgno

	for pgno, entry := range c.entries {
		if entry.page.IsClean() && entry.page.GetRefCount() == 0 {
			toRemove = append(toRemove, pgno)
		}
	}

	for _, pgno := range toRemove {
		c.removeLocked(pgno)
		evicted++
	}

	return evicted
}

// --- Internal LRU list operations ---

// isAtCapacity returns true if the cache is at or over capacity.
func (c *LRUCache) isAtCapacity() bool {
	if c.maxMemory > 0 && c.memoryUsage >= c.maxMemory {
		return true
	}
	if c.maxPages > 0 && len(c.entries) >= c.maxPages {
		return true
	}
	return false
}

// moveToFront moves an entry to the front of the LRU list.
func (c *LRUCache) moveToFront(entry *CacheEntry) {
	if entry == c.lruHead {
		return // Already at front
	}

	// Remove from current position
	c.removeFromLRU(entry)

	// Add to front
	c.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list.
func (c *LRUCache) addToFront(entry *CacheEntry) {
	entry.lruPrev = nil
	entry.lruNext = c.lruHead

	if c.lruHead != nil {
		c.lruHead.lruPrev = entry
	}

	c.lruHead = entry

	if c.lruTail == nil {
		c.lruTail = entry
	}
}

// removeFromLRU removes an entry from the LRU list.
func (c *LRUCache) removeFromLRU(entry *CacheEntry) {
	if entry.lruPrev != nil {
		entry.lruPrev.lruNext = entry.lruNext
	} else {
		c.lruHead = entry.lruNext
	}

	if entry.lruNext != nil {
		entry.lruNext.lruPrev = entry.lruPrev
	} else {
		c.lruTail = entry.lruPrev
	}

	entry.lruPrev = nil
	entry.lruNext = nil
}

// evictLRU evicts up to n least recently used clean pages.
// Dirty pages and pages with references are skipped.
func (c *LRUCache) evictLRU(n int) error {
	evicted := 0
	entry := c.lruTail

	for entry != nil && evicted < n {
		prev := entry.lruPrev

		// Only evict clean pages with no references
		if entry.page.IsClean() && entry.page.GetRefCount() == 0 {
			pgno := entry.page.Pgno
			c.removeFromLRU(entry)
			delete(c.entries, pgno)
			c.memoryUsage -= int64(c.pageSize)
			evicted++
		}

		entry = prev
	}

	if evicted == 0 {
		return ErrCacheFull
	}

	return nil
}

// --- Dirty list operations ---

// addToDirtyList adds a page to the dirty page list.
func (c *LRUCache) addToDirtyList(page *DbPage) {
	// Remove from list if already present
	c.removeFromDirtyList(page)

	// Add to head of list
	page.dirtyNext = c.dirtyHead
	page.dirtyPrev = nil

	if c.dirtyHead != nil {
		c.dirtyHead.dirtyPrev = page
	}

	c.dirtyHead = page
}

// removeFromDirtyList removes a page from the dirty page list.
func (c *LRUCache) removeFromDirtyList(page *DbPage) {
	if page.dirtyPrev != nil {
		page.dirtyPrev.dirtyNext = page.dirtyNext
	} else if c.dirtyHead == page {
		c.dirtyHead = page.dirtyNext
	}

	if page.dirtyNext != nil {
		page.dirtyNext.dirtyPrev = page.dirtyPrev
	}

	page.dirtyNext = nil
	page.dirtyPrev = nil
}

// --- Interface compatibility with existing PageCache ---

// PageCacheInterface defines the interface that both the old PageCache
// and new LRUCache implement.
type PageCacheInterface interface {
	Get(pgno Pgno) *DbPage
	Put(page *DbPage) error
	Remove(pgno Pgno)
	Clear()
	GetDirtyPages() []*DbPage
	MakeClean()
	MarkDirty(page *DbPage)
	Size() int
}

// Verify that both caches implement the interface
var _ PageCacheInterface = (*PageCache)(nil)
var _ PageCacheInterface = (*LRUCache)(nil)
