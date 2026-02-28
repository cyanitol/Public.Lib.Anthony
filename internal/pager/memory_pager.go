package pager

import (
	"errors"
	"fmt"
	"sync"
)

// MemoryPager implements a fully in-memory pager that stores pages in a map.
// It supports all normal pager operations but doesn't write to any file.
// This is used for :memory: databases and provides the same interface as
// the file-based pager.
type MemoryPager struct {
	// Map of page number to page data
	pages map[Pgno][]byte

	// Database header
	header *DatabaseHeader

	// Free list manager
	freeList *FreeList

	// Page cache (same as file-based pager)
	cache PageCacheInterface

	// Current pager state
	state int

	// Page size in bytes
	pageSize int

	// Number of pages in the database
	dbSize Pgno

	// Original database size at start of transaction
	dbOrigSize Pgno

	// Maximum page number allowed
	maxPageNum Pgno

	// Read-only flag (for consistency, though memory DBs are rarely read-only)
	readOnly bool

	// Change counter done flag
	changeCountDone bool

	// Error code for error state
	errCode error

	// Savepoints for nested transaction support
	savepoints []*Savepoint

	// Journal for rollback (stored in memory)
	journalPages map[Pgno][]byte

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// OpenMemory creates a new in-memory pager.
// The pageSize parameter specifies the page size for the database.
func OpenMemory(pageSize int) (*MemoryPager, error) {
	if !isValidPageSize(pageSize) {
		return nil, ErrInvalidPageSize
	}

	mp := &MemoryPager{
		pages:        make(map[Pgno][]byte),
		journalPages: make(map[Pgno][]byte),
		pageSize:     pageSize,
		state:        PagerStateOpen,
		cache:        NewPageCache(pageSize, DefaultCacheSize),
		maxPageNum:   0x7FFFFFFF,
		readOnly:     false,
	}

	// Initialize header
	mp.header = NewDatabaseHeader(pageSize)
	mp.header.DatabaseSize = 0

	// Initialize free list
	mp.freeList = NewFreeList(pagerInternal(mp))

	// Create first page (page 1 with header)
	mp.dbSize = 1
	page1Data := make([]byte, pageSize)
	headerData := mp.header.Serialize()
	copy(page1Data, headerData)
	mp.pages[1] = page1Data

	return mp, nil
}

// Close closes the memory pager and releases all resources.
// For memory pagers, this destroys all data.
func (mp *MemoryPager) Close() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Rollback any active transaction
	if mp.state >= PagerStateWriterLocked && mp.state < PagerStateError {
		if err := mp.rollbackLocked(); err != nil {
			return err
		}
	}

	// Clear the cache
	mp.cache.Clear()

	// Clear all pages
	mp.pages = make(map[Pgno][]byte)
	mp.journalPages = make(map[Pgno][]byte)

	mp.state = PagerStateOpen

	return nil
}

// Get retrieves a page from the database.
func (mp *MemoryPager) Get(pgno Pgno) (*DbPage, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.getLocked(pgno)
}

// getLocked retrieves a page without acquiring the mutex.
func (mp *MemoryPager) getLocked(pgno Pgno) (*DbPage, error) {
	if pgno == 0 || pgno > mp.maxPageNum {
		return nil, ErrInvalidPageNum
	}

	// Check cache first
	if page := mp.cache.Get(pgno); page != nil {
		page.Ref()
		return page, nil
	}

	// Read from memory storage
	page, err := mp.readPage(pgno)
	if err != nil {
		return nil, err
	}

	// Add to cache
	if err := mp.cache.Put(page); err != nil {
		return nil, err
	}

	return page, nil
}

// Put releases a reference to a page.
func (mp *MemoryPager) Put(page *DbPage) {
	if page == nil {
		return
	}
	page.Unref()
}

// Write marks a page as writeable and journals it if necessary.
func (mp *MemoryPager) Write(page *DbPage) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.writeLocked(page)
}

// writeLocked writes a page without acquiring the mutex.
func (mp *MemoryPager) writeLocked(page *DbPage) error {
	if mp.readOnly {
		return ErrReadOnly
	}

	if page == nil {
		return errors.New("nil page")
	}

	// Ensure we have a write transaction
	if mp.state == PagerStateOpen || mp.state == PagerStateReader {
		if err := mp.beginWriteTransaction(); err != nil {
			return err
		}
	}

	// Journal the page if not already writeable
	if !page.IsWriteable() {
		if err := mp.journalPage(page); err != nil {
			return err
		}
	}

	// Handle savepoints
	if len(mp.savepoints) > 0 {
		if err := mp.savePageState(page); err != nil {
			return err
		}
	}

	page.MakeWriteable()
	page.MakeDirty()

	// Add page to cache's dirty list so it gets written during commit
	mp.cache.MarkDirty(page)

	// Advance state
	if mp.state == PagerStateWriterLocked {
		mp.state = PagerStateWriterCachemod
	}

	return nil
}

// Commit commits the current write transaction.
func (mp *MemoryPager) Commit() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.state < PagerStateWriterLocked {
		return ErrNoTransaction
	}

	// Flush pending free pages
	if err := mp.freeList.Flush(); err != nil {
		mp.state = PagerStateError
		mp.errCode = err
		return err
	}

	// Write all dirty pages to memory storage
	if err := mp.writeDirtyPages(); err != nil {
		mp.state = PagerStateError
		mp.errCode = err
		return err
	}

	// Update header if needed
	needsHeaderUpdate := mp.dbSize != mp.dbOrigSize ||
		mp.header.FreelistTrunk != uint32(mp.freeList.GetFirstTrunk()) ||
		mp.header.FreelistCount != mp.freeList.GetTotalFree()

	if needsHeaderUpdate {
		mp.updateDatabaseHeader()
	}

	// First write dirty pages, then clear cache
	// Clear the cache dirty flags
	mp.cache.MakeClean()

	// Clear savepoints
	mp.clearSavepointsLocked()

	// Clear journal
	mp.journalPages = make(map[Pgno][]byte)

	// Note: We do NOT clear the cache here for memory databases.
	// Unlike file-based databases, memory databases keep all data in mp.pages,
	// and clearing the cache would just cause unnecessary reloads on the next query.
	// The pages are already marked clean, so they will be properly journaled
	// in the next transaction.

	// Return to open state
	mp.state = PagerStateOpen
	mp.dbOrigSize = mp.dbSize

	return nil
}

// Rollback rolls back the current write transaction.
func (mp *MemoryPager) Rollback() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.rollbackLocked()
}

// rollbackLocked performs rollback with the lock already held.
func (mp *MemoryPager) rollbackLocked() error {
	if mp.state < PagerStateWriterLocked {
		return ErrNoTransaction
	}

	// Restore pages from journal
	for pgno, data := range mp.journalPages {
		pageCopy := make([]byte, mp.pageSize)
		copy(pageCopy, data)
		mp.pages[pgno] = pageCopy
	}

	// Clear the cache
	mp.cache.Clear()

	// Clear journal
	mp.journalPages = make(map[Pgno][]byte)

	// Restore original database size
	mp.dbSize = mp.dbOrigSize

	// Clear savepoints
	mp.clearSavepointsLocked()

	// Return to open state
	mp.state = PagerStateOpen

	return nil
}

// PageSize returns the page size of the database.
func (mp *MemoryPager) PageSize() int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.pageSize
}

// PageCount returns the number of pages in the database.
func (mp *MemoryPager) PageCount() Pgno {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.dbSize
}

// IsReadOnly returns true if the pager is read-only.
func (mp *MemoryPager) IsReadOnly() bool {
	return mp.readOnly
}

// GetHeader returns the database header.
func (mp *MemoryPager) GetHeader() *DatabaseHeader {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.header
}

// AllocatePage allocates a new page.
func (mp *MemoryPager) AllocatePage() (Pgno, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.readOnly {
		return 0, ErrReadOnly
	}

	// Ensure we have a write transaction
	if mp.state == PagerStateOpen || mp.state == PagerStateReader {
		if err := mp.beginWriteTransaction(); err != nil {
			return 0, err
		}
	}

	// Try to allocate from the free list first
	pgno, err := mp.freeList.Allocate()
	if err != nil {
		return 0, err
	}

	// If we got a free page, return it
	if pgno != 0 {
		return pgno, nil
	}

	// No free pages available - allocate new page at end
	mp.dbSize++
	newPgno := mp.dbSize
	mp.pages[newPgno] = make([]byte, mp.pageSize)
	return newPgno, nil
}

// FreePage adds a page to the free list for later reuse.
func (mp *MemoryPager) FreePage(pgno Pgno) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.readOnly {
		return ErrReadOnly
	}

	if pgno == 0 || pgno > mp.dbSize {
		return ErrInvalidPageNum
	}

	// Ensure we have a write transaction
	if mp.state == PagerStateOpen || mp.state == PagerStateReader {
		if err := mp.beginWriteTransaction(); err != nil {
			return err
		}
	}

	// Add to free list
	return mp.freeList.Free(pgno)
}

// GetFreePageCount returns the number of free pages in the database.
func (mp *MemoryPager) GetFreePageCount() uint32 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.freeList.Count()
}

// BeginRead starts a read transaction.
func (mp *MemoryPager) BeginRead() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.state != PagerStateOpen {
		return ErrTransactionOpen
	}

	mp.state = PagerStateReader
	return nil
}

// BeginWrite starts a write transaction.
func (mp *MemoryPager) BeginWrite() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.beginWriteTransaction()
}

// InWriteTransaction returns true if a write transaction is active.
func (mp *MemoryPager) InWriteTransaction() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.state >= PagerStateWriterLocked
}

// EndRead ends a read transaction.
func (mp *MemoryPager) EndRead() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.state == PagerStateReader {
		mp.state = PagerStateOpen
	}
	return nil
}

// Vacuum compacts the in-memory database. For memory databases, this is a no-op
// since there's no file to defragment.
func (mp *MemoryPager) Vacuum(opts *VacuumOptions) error {
	// For in-memory databases, VACUUM is essentially a no-op since there's no
	// file fragmentation. If VACUUM INTO is requested, we would need to write
	// to a file, but that's not supported for memory databases.
	if opts != nil && opts.IntoFile != "" {
		return fmt.Errorf("VACUUM INTO not supported for in-memory databases")
	}
	return nil
}

// Savepoint creates a savepoint for nested transaction support.
func (mp *MemoryPager) Savepoint(name string) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if mp.state < PagerStateWriterLocked {
		return ErrNoTransaction
	}

	sp := NewSavepoint(name)
	mp.savepoints = append(mp.savepoints, sp)
	return nil
}

// Release releases a savepoint.
func (mp *MemoryPager) Release(name string) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	idx := mp.findSavepoint(name)
	if idx < 0 {
		return errors.New("savepoint not found")
	}

	// Remove this savepoint and all newer ones
	mp.savepoints = mp.savepoints[:idx]
	return nil
}

// RollbackTo rolls back to a savepoint.
func (mp *MemoryPager) RollbackTo(name string) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	idx := mp.findSavepoint(name)
	if idx < 0 {
		return errors.New("savepoint not found")
	}

	sp := mp.savepoints[idx]

	// Restore pages from savepoint
	for pgno, data := range sp.Pages {
		pageCopy := make([]byte, mp.pageSize)
		copy(pageCopy, data)
		mp.pages[pgno] = pageCopy
	}

	// Clear cache to reload pages
	mp.cache.Clear()

	// Remove newer savepoints
	mp.savepoints = mp.savepoints[:idx]

	return nil
}

// --- Internal helper methods ---

// readPage reads a page from memory storage.
func (mp *MemoryPager) readPage(pgno Pgno) (*DbPage, error) {
	if pgno == 0 {
		return nil, ErrInvalidPageNum
	}

	page := NewDbPage(pgno, mp.pageSize)

	// Check if page exists in memory
	if data, ok := mp.pages[pgno]; ok {
		copy(page.Data, data)
	} else if pgno <= mp.dbSize {
		// Page is within database size but not yet allocated
		// Initialize with zeros (already done by NewDbPage)
		mp.pages[pgno] = make([]byte, mp.pageSize)
	} else {
		// Page is beyond database size, extend it
		mp.dbSize = pgno
		mp.pages[pgno] = make([]byte, mp.pageSize)
	}

	page.pager = (*Pager)(nil) // Memory pager doesn't use the file pager reference
	return page, nil
}

// writePage writes a page to memory storage.
func (mp *MemoryPager) writePage(page *DbPage) error {
	if page.Pgno == 0 {
		return ErrInvalidPageNum
	}

	if !page.ShouldWrite() {
		return nil
	}

	// Store a copy of the page data
	pageCopy := make([]byte, mp.pageSize)
	copy(pageCopy, page.Data)
	mp.pages[page.Pgno] = pageCopy

	// Extend database size if necessary
	if page.Pgno > mp.dbSize {
		mp.dbSize = page.Pgno
	}

	return nil
}

// writeDirtyPages writes all dirty pages to memory storage.
func (mp *MemoryPager) writeDirtyPages() error {
	dirtyPages := mp.cache.GetDirtyPages()

	for _, page := range dirtyPages {
		if err := mp.writePage(page); err != nil {
			return err
		}
	}

	mp.state = PagerStateWriterFinished
	return nil
}

// beginWriteTransaction starts a write transaction.
func (mp *MemoryPager) beginWriteTransaction() error {
	if mp.readOnly {
		return ErrReadOnly
	}

	// If already in a write transaction, just return success
	if mp.state >= PagerStateWriterLocked {
		return nil
	}

	mp.state = PagerStateWriterLocked
	mp.dbOrigSize = mp.dbSize

	return nil
}

// journalPage saves the current page state to the in-memory journal.
func (mp *MemoryPager) journalPage(page *DbPage) error {
	// Only journal if not already journaled
	if _, exists := mp.journalPages[page.Pgno]; exists {
		return nil
	}

	// Save current page data from memory storage (not from the page buffer)
	// This ensures we save the committed state, not the in-progress changes
	if data, ok := mp.pages[page.Pgno]; ok {
		dataCopy := make([]byte, mp.pageSize)
		copy(dataCopy, data)
		mp.journalPages[page.Pgno] = dataCopy
	} else {
		// Page doesn't exist yet, save empty page
		mp.journalPages[page.Pgno] = make([]byte, mp.pageSize)
	}

	return nil
}

// updateDatabaseHeader updates the database header in page 1.
func (mp *MemoryPager) updateDatabaseHeader() {
	mp.header.DatabaseSize = uint32(mp.dbSize)
	mp.header.FreelistTrunk = uint32(mp.freeList.GetFirstTrunk())
	mp.header.FreelistCount = mp.freeList.GetTotalFree()
	mp.header.FileChangeCounter++

	// Update page 1 with new header
	if page1Data, ok := mp.pages[1]; ok {
		headerData := mp.header.Serialize()
		copy(page1Data, headerData)
	}
}

// savePageState saves the page state to the current savepoint.
func (mp *MemoryPager) savePageState(page *DbPage) error {
	if len(mp.savepoints) == 0 {
		return nil
	}

	sp := mp.savepoints[len(mp.savepoints)-1]

	// Only save if not already saved in this savepoint
	if _, exists := sp.Pages[page.Pgno]; exists {
		return nil
	}

	// Save current page data from the page buffer itself, not from mp.pages
	// This is because in a transaction, the current state is in the cache/buffer,
	// not yet written to mp.pages until commit
	dataCopy := make([]byte, mp.pageSize)
	copy(dataCopy, page.Data)
	sp.Pages[page.Pgno] = dataCopy

	return nil
}

// clearSavepointsLocked clears all savepoints.
func (mp *MemoryPager) clearSavepointsLocked() {
	mp.savepoints = nil
}

// findSavepoint finds a savepoint by name.
func (mp *MemoryPager) findSavepoint(name string) int {
	for i, sp := range mp.savepoints {
		if sp.Name == name {
			return i
		}
	}
	return -1
}
