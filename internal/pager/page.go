package pager

import (
	"sync"
	"sync/atomic"
)

// Pgno represents a page number in the database.
// Page numbers start at 1 (page 0 is reserved/invalid).
type Pgno uint32

// Page flags (based on PGHDR flags from SQLite)
const (
	// PageFlagClean indicates the page is not dirty (not modified).
	PageFlagClean = 0x001

	// PageFlagDirty indicates the page has been modified.
	PageFlagDirty = 0x002

	// PageFlagWriteable indicates the page is journaled and ready to modify.
	PageFlagWriteable = 0x004

	// PageFlagNeedSync indicates the rollback journal must be synced before
	// writing this page to the database.
	PageFlagNeedSync = 0x008

	// PageFlagDontWrite indicates the page should not be written to disk.
	PageFlagDontWrite = 0x010
)

// DbPage represents a single page in the database.
// This corresponds to the PgHdr structure in SQLite's C code.
type DbPage struct {
	// Page number (1-based)
	Pgno Pgno

	// Page data (actual content)
	Data []byte

	// Flags indicating page state
	Flags uint16

	// Reference count - number of active users of this page
	RefCount int64

	// Pager that owns this page
	pager *Pager

	// Dirty list linkage (for maintaining list of dirty pages)
	dirtyNext *DbPage
	dirtyPrev *DbPage

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewDbPage creates a new database page with the given page number and size.
func NewDbPage(pgno Pgno, pageSize int) *DbPage {
	return &DbPage{
		Pgno:     pgno,
		Data:     make([]byte, pageSize),
		Flags:    PageFlagClean,
		RefCount: 1, // Start with reference count of 1
	}
}

// IsDirty returns true if the page has been modified.
func (p *DbPage) IsDirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Flags&PageFlagDirty != 0
}

// IsClean returns true if the page has not been modified.
func (p *DbPage) IsClean() bool {
	return !p.IsDirty()
}

// GetPgno returns the page number.
// This implements the DbPageInterface used by btree's PagerAdapter.
func (p *DbPage) GetPgno() uint32 {
	return uint32(p.Pgno)
}

// IsWriteable returns true if the page is journaled and ready to be modified.
func (p *DbPage) IsWriteable() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Flags&PageFlagWriteable != 0
}

// MakeDirty marks the page as dirty (modified).
func (p *DbPage) MakeDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove clean flag and add dirty flag
	p.Flags &^= PageFlagClean
	p.Flags |= PageFlagDirty
}

// MakeClean marks the page as clean (not modified).
// This also clears the writeable flag so the page will be journaled again
// if modified in a future transaction.
func (p *DbPage) MakeClean() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove dirty and writeable flags, add clean flag
	p.Flags &^= PageFlagDirty | PageFlagWriteable
	p.Flags |= PageFlagClean
}

// MakeWriteable marks the page as writeable (journaled and ready to modify).
func (p *DbPage) MakeWriteable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Flags |= PageFlagWriteable
}

// SetDontWrite marks the page to not be written to disk.
func (p *DbPage) SetDontWrite() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Flags |= PageFlagDontWrite
}

// ShouldWrite returns true if the page should be written to disk.
func (p *DbPage) ShouldWrite() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Flags&PageFlagDontWrite == 0
}

// Ref increments the reference count for this page.
// The page cannot be evicted from cache while the reference count is > 0.
func (p *DbPage) Ref() {
	atomic.AddInt64(&p.RefCount, 1)
}

// Unref decrements the reference count for this page.
// When the reference count reaches 0, the page can be evicted from cache.
func (p *DbPage) Unref() {
	newCount := atomic.AddInt64(&p.RefCount, -1)
	if newCount < 0 {
		// This should never happen in correct usage
		atomic.StoreInt64(&p.RefCount, 0)
	}
}

// GetRefCount returns the current reference count.
func (p *DbPage) GetRefCount() int64 {
	return atomic.LoadInt64(&p.RefCount)
}

// GetData returns the page data.
// Callers should not modify the returned slice directly; use Write() instead.
func (p *DbPage) GetData() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Data
}

// Write writes data to the page at the specified offset.
// This marks the page as dirty and writeable.
func (p *DbPage) Write(offset int, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset < 0 || offset+len(data) > len(p.Data) {
		return ErrInvalidOffset
	}

	copy(p.Data[offset:], data)

	// Mark page as dirty and writeable
	p.Flags &^= PageFlagClean
	p.Flags |= PageFlagDirty | PageFlagWriteable

	return nil
}

// Read reads data from the page at the specified offset.
func (p *DbPage) Read(offset int, length int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset+length > len(p.Data) {
		return nil, ErrInvalidOffset
	}

	// Return a copy to prevent external modifications
	result := make([]byte, length)
	copy(result, p.Data[offset:offset+length])

	return result, nil
}

// Size returns the size of the page in bytes.
func (p *DbPage) Size() int {
	return len(p.Data)
}

// Zero zeroes out the entire page content.
func (p *DbPage) Zero() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.Data {
		p.Data[i] = 0
	}

	p.Flags &^= PageFlagClean
	p.Flags |= PageFlagDirty | PageFlagWriteable
}

// Clone creates a deep copy of the page.
func (p *DbPage) Clone() *DbPage {
	p.mu.RLock()
	defer p.mu.RUnlock()

	clone := &DbPage{
		Pgno:     p.Pgno,
		Data:     make([]byte, len(p.Data)),
		Flags:    p.Flags,
		RefCount: 1, // New page starts with reference count of 1
	}

	copy(clone.Data, p.Data)

	return clone
}

// PageCache represents a cache of database pages.
type PageCache struct {
	// Map of page number to page
	pages map[Pgno]*DbPage

	// Head of dirty page list
	dirtyHead *DbPage

	// Mutex for thread-safe operations
	mu sync.RWMutex

	// Maximum number of pages to cache
	maxPages int

	// Page size
	pageSize int
}

// NewPageCache creates a new page cache.
func NewPageCache(pageSize, maxPages int) *PageCache {
	return &PageCache{
		pages:    make(map[Pgno]*DbPage),
		maxPages: maxPages,
		pageSize: pageSize,
	}
}

// Get retrieves a page from the cache.
// Returns nil if the page is not in the cache.
func (c *PageCache) Get(pgno Pgno) *DbPage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pages[pgno]
}

// Put adds a page to the cache.
// If the cache is full, it may evict clean pages.
func (c *PageCache) Put(page *DbPage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict pages
	if len(c.pages) >= c.maxPages {
		if err := c.evictCleanPages(1); err != nil {
			return err
		}
	}

	c.pages[page.Pgno] = page

	// If the page is dirty, add it to the dirty list
	if page.IsDirty() {
		c.addToDirtyList(page)
	}

	return nil
}

// Remove removes a page from the cache.
func (c *PageCache) Remove(pgno Pgno) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if page, ok := c.pages[pgno]; ok {
		// Remove from dirty list if present
		if page.IsDirty() {
			c.removeFromDirtyList(page)
		}
		delete(c.pages, pgno)
	}
}

// Clear removes all pages from the cache.
func (c *PageCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pages = make(map[Pgno]*DbPage)
	c.dirtyHead = nil
}

// GetDirtyPages returns a list of all dirty pages.
func (c *PageCache) GetDirtyPages() []*DbPage {
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

// MakeClean marks all pages as clean.
func (c *PageCache) MakeClean() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear dirty list
	c.dirtyHead = nil

	// Mark all pages as clean
	for _, page := range c.pages {
		page.MakeClean()
	}
}

// MarkDirty marks a page as dirty and adds it to the dirty list.
func (c *PageCache) MarkDirty(page *DbPage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add to dirty list if the page is dirty
	if page.IsDirty() {
		c.addToDirtyList(page)
	}
}

// Size returns the number of pages in the cache.
func (c *PageCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pages)
}

// addToDirtyList adds a page to the dirty page list.
// Must be called with cache lock held.
func (c *PageCache) addToDirtyList(page *DbPage) {
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
// Must be called with cache lock held.
func (c *PageCache) removeFromDirtyList(page *DbPage) {
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

// evictCleanPages evicts up to n clean pages from the cache.
// Must be called with cache lock held.
func (c *PageCache) evictCleanPages(n int) error {
	evicted := 0

	for pgno, page := range c.pages {
		if evicted >= n {
			break
		}

		// Only evict clean pages with no references
		if page.IsClean() && page.GetRefCount() == 0 {
			delete(c.pages, pgno)
			evicted++
		}
	}

	if evicted == 0 {
		return ErrCacheFull
	}

	return nil
}
