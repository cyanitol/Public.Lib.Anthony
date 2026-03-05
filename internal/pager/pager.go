// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Pager states (based on SQLite's pager states)
const (
	// PagerStateOpen - pager is open but no transaction is active
	PagerStateOpen = iota

	// PagerStateReader - read transaction is active
	PagerStateReader

	// PagerStateWriterLocked - write transaction started, locks acquired
	PagerStateWriterLocked

	// PagerStateWriterCachemod - write transaction, cache modified
	PagerStateWriterCachemod

	// PagerStateWriterDbmod - write transaction, database file modified
	PagerStateWriterDbmod

	// PagerStateWriterFinished - write transaction finished, ready to commit
	PagerStateWriterFinished

	// PagerStateError - error state
	PagerStateError
)

// Lock states
const (
	LockNone = iota
	LockShared
	LockReserved
	LockPending
	LockExclusive
)

// Journal modes
const (
	JournalModeDelete = iota
	JournalModePersist
	JournalModeOff
	JournalModeTruncate
	JournalModeMemory
	JournalModeWAL
)

// Default values
const (
	DefaultCacheSize = 2000 // Default number of pages to cache
)

// validTransitions defines valid state transitions for the pager
var validTransitions = map[int][]int{
	PagerStateOpen: {
		PagerStateReader,
		PagerStateWriterLocked,
	},
	PagerStateReader: {
		PagerStateOpen,
		PagerStateWriterLocked,
	},
	PagerStateWriterLocked: {
		PagerStateWriterCachemod,
		PagerStateWriterDbmod,
		PagerStateWriterFinished,
		PagerStateOpen,
		PagerStateError,
	},
	PagerStateWriterCachemod: {
		PagerStateWriterDbmod,
		PagerStateWriterFinished,
		PagerStateOpen,
		PagerStateError,
	},
	PagerStateWriterDbmod: {
		PagerStateWriterFinished,
		PagerStateOpen,
		PagerStateError,
	},
	PagerStateWriterFinished: {
		PagerStateOpen,
		PagerStateError,
	},
	PagerStateError: {
		PagerStateOpen,
	},
}

// Common errors
var (
	ErrInvalidPageSize        = errors.New("invalid page size")
	ErrInvalidPageNum         = errors.New("invalid page number")
	ErrInvalidOffset          = errors.New("invalid offset")
	ErrPageNotFound           = errors.New("page not found")
	ErrCacheFull              = errors.New("cache full")
	ErrReadOnly               = errors.New("pager is read-only")
	ErrNoTransaction          = errors.New("no transaction active")
	ErrTransactionOpen        = errors.New("transaction already open")
	ErrDatabaseLocked         = errors.New("database is locked")
	ErrDatabaseCorrupt        = errors.New("database file is corrupt")
	ErrDiskIO                 = errors.New("disk I/O error")
	ErrDiskFull               = errors.New("disk full")
	ErrInvalidStateTransition = errors.New("invalid pager state transition")
	ErrChecksumMismatch       = errors.New("checksum validation failed")
)

// Pager manages reading and writing pages from/to a database file.
// It implements page caching, journaling for atomic commits, and file locking.
type Pager struct {
	// File handle for the database file
	file *os.File

	// File handle for the journal file
	journalFile *os.File

	// Database filename
	filename string

	// Journal filename
	journalFilename string

	// Page cache (can be either PageCache or LRUCache)
	cache PageCacheInterface

	// Database header
	header *DatabaseHeader

	// Free list manager
	freeList *FreeList

	// Current pager state
	state int

	// Current lock state
	lockState int

	// Page size in bytes
	pageSize int

	// Number of pages in the database
	dbSize Pgno

	// Original database size at start of transaction
	dbOrigSize Pgno

	// Maximum page number allowed
	maxPageNum Pgno

	// Journal mode
	journalMode int

	// Read-only flag
	readOnly bool

	// Temporary file flag
	tempFile bool

	// Change counter done flag
	changeCountDone bool

	// Error code for error state
	errCode error

	// Savepoints for nested transaction support
	savepoints []*Savepoint

	// Busy handler for lock contention
	busyHandler BusyHandler

	// WAL instance (only used in WAL mode)
	wal *WAL

	// WAL index (only used in WAL mode)
	walIndex *WALIndex

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// Open opens a database file and creates a new Pager.
// If the file doesn't exist and readOnly is false, a new database will be created.
func Open(filename string, readOnly bool) (*Pager, error) {
	return OpenWithPageSize(filename, readOnly, DefaultPageSize)
}

// OpenWithPageSize opens a database file with a specific page size.
func OpenWithPageSize(filename string, readOnly bool, pageSize int) (*Pager, error) {
	if !isValidPageSize(pageSize) {
		return nil, ErrInvalidPageSize
	}
	pager := newPager(filename, pageSize, readOnly)
	if err := pager.openFile(readOnly); err != nil {
		return nil, err
	}
	if err := pager.initOrReadHeader(readOnly); err != nil {
		return nil, err
	}
	return pager, nil
}

// OpenWithLRUCache opens a database file with an LRU cache.
func OpenWithLRUCache(filename string, readOnly bool, pageSize int, cacheConfig LRUCacheConfig) (*Pager, error) {
	if !isValidPageSize(pageSize) {
		return nil, ErrInvalidPageSize
	}

	// Create pager with LRU cache
	pager := newPagerWithLRUCache(filename, pageSize, readOnly, cacheConfig)
	if err := pager.openFile(readOnly); err != nil {
		return nil, err
	}
	if err := pager.initOrReadHeader(readOnly); err != nil {
		return nil, err
	}

	// Set the pager reference in the cache for flushing
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		lruCache.SetPager(pager)
	}

	return pager, nil
}

// newPager creates a new Pager instance.
func newPager(filename string, pageSize int, readOnly bool) *Pager {
	pager := &Pager{
		filename:        filename,
		journalFilename: filename + "-journal",
		pageSize:        pageSize,
		journalMode:     JournalModeDelete,
		readOnly:        readOnly,
		state:           PagerStateOpen,
		lockState:       LockNone,
		cache:           NewPageCache(pageSize, DefaultCacheSize),
		maxPageNum:      0x7FFFFFFF,
	}
	// Initialize free list (will be loaded from header later)
	pager.freeList = NewFreeList(pager)
	return pager
}

// newPagerWithLRUCache creates a new Pager instance with an LRU cache.
func newPagerWithLRUCache(filename string, pageSize int, readOnly bool, cacheConfig LRUCacheConfig) *Pager {
	// Ensure page size matches
	cacheConfig.PageSize = pageSize

	lruCache, _ := NewLRUCache(cacheConfig)

	pager := &Pager{
		filename:        filename,
		journalFilename: filename + "-journal",
		pageSize:        pageSize,
		journalMode:     JournalModeDelete,
		readOnly:        readOnly,
		state:           PagerStateOpen,
		lockState:       LockNone,
		cache:           lruCache,
		maxPageNum:      0x7FFFFFFF,
	}
	// Initialize free list (will be loaded from header later)
	pager.freeList = NewFreeList(pager)
	return pager
}

// openFile opens the database file.
func (p *Pager) openFile(readOnly bool) error {
	var err error
	if readOnly {
		p.file, err = os.OpenFile(p.filename, os.O_RDONLY, 0)
	} else {
		p.file, err = os.OpenFile(p.filename, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}
	return nil
}

// initOrReadHeader initializes a new database or reads the header.
func (p *Pager) initOrReadHeader(readOnly bool) error {
	info, err := p.file.Stat()
	if err != nil {
		p.file.Close()
		return fmt.Errorf("failed to stat database file: %w", err)
	}
	if info.Size() == 0 {
		return p.initNewDatabase(readOnly)
	}
	return p.readExistingDatabase(info)
}

// initNewDatabase initializes a new empty database.
func (p *Pager) initNewDatabase(readOnly bool) error {
	if readOnly {
		p.file.Close()
		return errors.New("cannot create new database in read-only mode")
	}
	if err := p.initializeNewDatabase(); err != nil {
		p.file.Close()
		return err
	}
	return nil
}

// readExistingDatabase reads the header and calculates size.
func (p *Pager) readExistingDatabase(info os.FileInfo) error {
	if err := p.readHeader(); err != nil {
		p.file.Close()
		return err
	}
	p.dbSize = Pgno(info.Size() / int64(p.pageSize))
	p.dbOrigSize = p.dbSize

	// Check if WAL file exists and recover it
	if err := p.recoverWALIfExists(); err != nil {
		p.file.Close()
		return err
	}

	return nil
}

// Close closes the pager and releases all resources.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Rollback any active transaction
	if p.state >= PagerStateWriterLocked && p.state < PagerStateError {
		if err := p.rollbackLocked(); err != nil {
			return err
		}
	}

	// Clear the cache
	p.cache.Clear()

	// Close journal file if open
	if p.journalFile != nil {
		p.journalFile.Close()
		p.journalFile = nil
	}

	// Close database file
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			return err
		}
		p.file = nil
	}

	_ = p.setStateLocked(PagerStateOpen)
	p.lockState = LockNone

	return nil
}

// Get retrieves a page from the database.
// The returned page's reference count is incremented.
func (p *Pager) Get(pgno Pgno) (*DbPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.getLocked(pgno)
}

// getLocked retrieves a page without acquiring the mutex (must be called with lock held).
func (p *Pager) getLocked(pgno Pgno) (*DbPage, error) {
	if pgno == 0 || pgno > p.maxPageNum {
		return nil, ErrInvalidPageNum
	}

	// Check cache first - early return for common case
	if page := p.cache.Get(pgno); page != nil {
		page.Ref()
		return page, nil
	}

	// Not in cache - ensure we have proper lock
	if err := p.ensureSharedLock(); err != nil {
		return nil, err
	}

	// Try to load from WAL if in WAL mode
	if page, err := p.tryLoadFromWAL(pgno); err != nil {
		return nil, err
	} else if page != nil {
		return page, nil
	}

	// Load from disk and cache
	return p.loadPageFromDisk(pgno)
}

// ensureSharedLock ensures we have at least a shared lock for reading.
func (p *Pager) ensureSharedLock() error {
	if p.state == PagerStateOpen {
		return p.acquireSharedLock()
	}
	return nil
}

// tryLoadFromWAL attempts to load a page from the WAL if in WAL mode.
// Returns nil page if not in WAL mode or page not found in WAL.
func (p *Pager) tryLoadFromWAL(pgno Pgno) (*DbPage, error) {
	if p.journalMode != JournalModeWAL || p.wal == nil {
		return nil, nil
	}

	frame, err := p.wal.FindPage(pgno)
	if err != nil || frame == nil {
		return nil, nil
	}

	page := NewDbPage(pgno, p.pageSize)
	copy(page.Data, frame.Data)
	page.pager = p
	if err := p.cache.Put(page); err != nil {
		return nil, err
	}
	return page, nil
}

// loadPageFromDisk reads a page from disk and adds it to cache.
func (p *Pager) loadPageFromDisk(pgno Pgno) (*DbPage, error) {
	page, err := p.readPage(pgno)
	if err != nil {
		return nil, err
	}

	if err := p.cache.Put(page); err != nil {
		return nil, err
	}

	return page, nil
}

// Put releases a reference to a page.
func (p *Pager) Put(page *DbPage) {
	if page == nil {
		return
	}
	page.Unref()
}

// Write marks a page as writeable and journals it if necessary.
func (p *Pager) Write(page *DbPage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeLocked(page)
}

// writeLocked writes a page without acquiring the mutex (must be called with lock held).
func (p *Pager) writeLocked(page *DbPage) error {
	if p.readOnly {
		return ErrReadOnly
	}

	if page == nil {
		return errors.New("nil page")
	}

	if err := p.ensureWriteTransaction(); err != nil {
		return err
	}

	if err := p.preparePageForWrite(page); err != nil {
		return err
	}

	page.MakeWriteable()
	page.MakeDirty()
	p.advanceToWriterCachemod()

	// Notify cache that the page is now dirty
	if pageCache, ok := p.cache.(*PageCache); ok {
		pageCache.MarkDirty(page)
	}

	return nil
}

func (p *Pager) ensureWriteTransaction() error {
	if p.state == PagerStateOpen || p.state == PagerStateReader {
		return p.beginWriteTransaction()
	}
	return nil
}

func (p *Pager) preparePageForWrite(page *DbPage) error {
	if !page.IsWriteable() {
		if err := p.journalPage(page); err != nil {
			return err
		}
	}

	if len(p.savepoints) > 0 {
		return p.savePageState(page)
	}

	return nil
}

func (p *Pager) advanceToWriterCachemod() {
	if p.state == PagerStateWriterLocked {
		// Use validated state transition
		_ = p.setStateLocked(PagerStateWriterCachemod)
	}
}

// Commit commits the current write transaction.
func (p *Pager) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state < PagerStateWriterLocked {
		return ErrNoTransaction
	}

	if err := p.commitPhase0FlushFreeList(); err != nil {
		return err
	}

	if err := p.commitPhase1WriteDirtyPages(); err != nil {
		return err
	}

	if err := p.commitPhase2SyncDatabase(); err != nil {
		return err
	}

	if err := p.commitPhase3FinalizeJournal(); err != nil {
		return err
	}

	if err := p.commitPhase4UpdateHeader(); err != nil {
		return err
	}

	p.commitPhase5Cleanup()

	return nil
}

// commitPhase0FlushFreeList flushes pending free pages to disk.
func (p *Pager) commitPhase0FlushFreeList() error {
	if err := p.freeList.Flush(); err != nil {
		p.setErrorState(err)
		return err
	}
	return nil
}

// commitPhase1WriteDirtyPages writes all dirty pages to disk.
func (p *Pager) commitPhase1WriteDirtyPages() error {
	if lruCache, ok := p.cache.(*LRUCache); ok && lruCache.Mode() == WriteBackMode {
		if _, err := lruCache.Flush(); err != nil {
			p.setErrorState(err)
			return err
		}
		return nil
	}

	// In WAL mode, write to WAL instead of database file
	if p.journalMode == JournalModeWAL {
		if err := p.writeDirtyPagesToWAL(); err != nil {
			p.setErrorState(err)
			return err
		}
		return nil
	}

	if err := p.writeDirtyPages(); err != nil {
		p.setErrorState(err)
		return err
	}
	return nil
}

// commitPhase2SyncDatabase syncs the database file.
func (p *Pager) commitPhase2SyncDatabase() error {
	// In WAL mode, sync the WAL file instead of database file
	if p.journalMode == JournalModeWAL && p.wal != nil {
		if err := p.wal.Sync(); err != nil {
			p.setErrorState(err)
			return err
		}
		return nil
	}

	if err := p.file.Sync(); err != nil {
		p.setErrorState(err)
		return err
	}
	return nil
}

// commitPhase3FinalizeJournal deletes or truncates the journal.
func (p *Pager) commitPhase3FinalizeJournal() error {
	if err := p.finalizeJournal(); err != nil {
		p.setErrorState(err)
		return err
	}
	return nil
}

// commitPhase4UpdateHeader updates the database header if needed.
func (p *Pager) commitPhase4UpdateHeader() error {
	if !p.needsHeaderUpdate() {
		return nil
	}
	return p.updateDatabaseHeader()
}

// needsHeaderUpdate checks if the database header needs updating.
func (p *Pager) needsHeaderUpdate() bool {
	return p.dbSize != p.dbOrigSize ||
		p.header.FreelistTrunk != uint32(p.freeList.GetFirstTrunk()) ||
		p.header.FreelistCount != p.freeList.GetTotalFree()
}

// commitPhase5Cleanup cleans up after a successful commit.
func (p *Pager) commitPhase5Cleanup() {
	p.cache.MakeClean()
	p.clearSavepointsLocked()
	_ = p.setStateLocked(PagerStateOpen)
	p.lockState = LockNone
	p.dbOrigSize = p.dbSize
}

// Rollback rolls back the current write transaction.
func (p *Pager) Rollback() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.rollbackLocked()
}

// rollbackLocked performs rollback with the lock already held.
func (p *Pager) rollbackLocked() error {
	if p.state < PagerStateWriterLocked {
		return ErrNoTransaction
	}

	// Rollback using the journal if it exists
	if p.journalFile != nil {
		if err := p.rollbackJournal(); err != nil {
			p.state = PagerStateError
			p.errCode = err
			return err
		}
	}

	// Clear the cache
	p.cache.Clear()

	// Close and delete the journal
	if p.journalFile != nil {
		p.journalFile.Close()
		p.journalFile = nil
		os.Remove(p.journalFilename)
	}

	// Restore original database size
	p.dbSize = p.dbOrigSize

	// Clear savepoints
	p.clearSavepointsLocked()

	// Return to open state
	_ = p.setStateLocked(PagerStateOpen)
	p.lockState = LockNone

	return nil
}

// PageSize returns the page size of the database.
func (p *Pager) PageSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageSize
}

// PageCount returns the number of pages in the database.
func (p *Pager) PageCount() Pgno {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dbSize
}

// IsReadOnly returns true if the pager is read-only.
func (p *Pager) IsReadOnly() bool {
	return p.readOnly
}

// GetHeader returns the database header.
func (p *Pager) GetHeader() *DatabaseHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.header
}

// initializeNewDatabase initializes a new database file with a header.
func (p *Pager) initializeNewDatabase() error {
	p.header = NewDatabaseHeader(p.pageSize)
	p.header.DatabaseSize = 0

	// Write header to file
	headerData := p.header.Serialize()
	if _, err := p.file.WriteAt(headerData, 0); err != nil {
		return fmt.Errorf("failed to write database header: %w", err)
	}

	// Write empty page 1 (rest of first page after header)
	emptyPage := make([]byte, p.pageSize-DatabaseHeaderSize)
	if _, err := p.file.WriteAt(emptyPage, DatabaseHeaderSize); err != nil {
		return fmt.Errorf("failed to write first page: %w", err)
	}

	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync database file: %w", err)
	}

	p.dbSize = 1
	return nil
}

// readHeader reads the database header from the file.
func (p *Pager) readHeader() error {
	headerData := make([]byte, DatabaseHeaderSize)
	if _, err := p.file.ReadAt(headerData, 0); err != nil {
		return fmt.Errorf("failed to read database header: %w", err)
	}

	header, err := ParseDatabaseHeader(headerData)
	if err != nil {
		return err
	}

	if err := header.Validate(); err != nil {
		return err
	}

	p.header = header

	// Update page size if different from what was requested
	actualPageSize := header.GetPageSize()
	if actualPageSize != p.pageSize {
		p.pageSize = actualPageSize
		p.cache = NewPageCache(actualPageSize, DefaultCacheSize)
		p.freeList = NewFreeList(p)
	}

	// Initialize free list from header
	p.freeList.Initialize(Pgno(header.FreelistTrunk), header.FreelistCount)

	return nil
}

// recoverWALIfExists checks for an existing WAL file and recovers it if present.
// This ensures that uncommitted changes in the WAL are made visible when reopening a database.
func (p *Pager) recoverWALIfExists() error {
	walSize, err := p.checkWALFile()
	if err != nil {
		return err
	}

	// No WAL file or empty WAL - nothing to recover
	if walSize <= WALHeaderSize {
		return nil
	}

	// WAL file exists and has frames - recover based on mode
	if p.readOnly {
		return p.recoverWALReadOnly()
	}
	return p.recoverWALReadWrite()
}

// checkWALFile checks if a WAL file exists and returns its size.
// Returns 0 if the file doesn't exist.
func (p *Pager) checkWALFile() (int64, error) {
	walFilename := p.filename + "-wal"
	info, err := os.Stat(walFilename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	return info.Size(), nil
}

// recoverWALReadOnly opens the WAL for reading in read-only mode.
func (p *Pager) recoverWALReadOnly() error {
	p.wal = NewWAL(p.filename, p.pageSize)
	if err := p.wal.Open(); err != nil {
		return fmt.Errorf("failed to open WAL for reading: %w", err)
	}

	// Also open WAL index
	walIndex, err := NewWALIndex(p.filename)
	if err != nil {
		p.wal.Close()
		p.wal = nil
		return fmt.Errorf("failed to open WAL index: %w", err)
	}
	p.walIndex = walIndex

	// Set journal mode to WAL since we're using it
	p.journalMode = JournalModeWAL
	return nil
}

// recoverWALReadWrite checkpoints the WAL to persist data in read-write mode.
func (p *Pager) recoverWALReadWrite() error {
	wal := NewWAL(p.filename, p.pageSize)
	if err := wal.Open(); err != nil {
		return fmt.Errorf("failed to open WAL for recovery: %w", err)
	}

	// Set the database file handle so checkpoint can write to it
	wal.dbFile = p.file

	// Checkpoint the WAL to move data to the main database file
	if err := wal.Checkpoint(); err != nil {
		wal.Close()
		return fmt.Errorf("failed to checkpoint WAL during recovery: %w", err)
	}

	// Close the WAL (keeps the file for future use)
	wal.dbFile = nil // Don't close the main db file
	if err := wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL after recovery: %w", err)
	}

	// Re-read the database size after WAL recovery
	return p.updateDBSizeAfterRecovery()
}

// updateDBSizeAfterRecovery updates the database size after WAL recovery.
func (p *Pager) updateDBSizeAfterRecovery() error {
	fileInfo, err := p.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat database after WAL recovery: %w", err)
	}
	p.dbSize = Pgno(fileInfo.Size() / int64(p.pageSize))
	p.dbOrigSize = p.dbSize
	return nil
}

// readPage reads a page from the database file.
func (p *Pager) readPage(pgno Pgno) (*DbPage, error) {
	if pgno == 0 {
		return nil, ErrInvalidPageNum
	}

	page := NewDbPage(pgno, p.pageSize)

	offset := int64(pgno-1) * int64(p.pageSize)
	n, err := p.file.ReadAt(page.Data, offset)

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read page %d: %w", pgno, err)
	}

	// If we read less than a full page, it means we're reading beyond the end of the file
	if n < p.pageSize {
		// This is allowed - the page is just zero-filled
		if pgno > p.dbSize {
			// Extend the database size
			p.dbSize = pgno
		}
	}

	page.pager = p
	return page, nil
}

// writePage writes a page to the database file.
func (p *Pager) writePage(page *DbPage) error {
	if page.Pgno == 0 {
		return ErrInvalidPageNum
	}

	if !page.ShouldWrite() {
		return nil
	}

	offset := int64(page.Pgno-1) * int64(p.pageSize)
	if _, err := p.file.WriteAt(page.Data, offset); err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.Pgno, err)
	}

	// Extend database size if necessary
	if page.Pgno > p.dbSize {
		p.dbSize = page.Pgno
	}

	return nil
}

// writeDirtyPages writes all dirty pages to the database file.
func (p *Pager) writeDirtyPages() error {
	dirtyPages := p.cache.GetDirtyPages()

	for _, page := range dirtyPages {
		if err := p.writePage(page); err != nil {
			return err
		}
	}

	if err := p.setStateLocked(PagerStateWriterFinished); err != nil {
		return err
	}
	return nil
}

// acquireSharedLock acquires a shared lock on the database.
// If a busy handler is set, it will retry on lock contention.
func (p *Pager) acquireSharedLock() error {
	if p.lockState >= LockShared {
		return nil
	}

	// Use busy handler if available
	if p.busyHandler != nil {
		return p.acquireSharedLockWithRetry()
	}

	// Otherwise, try once without retry
	return p.tryAcquireSharedLock()
}

// beginWriteTransaction starts a write transaction.
func (p *Pager) beginWriteTransaction() error {
	if p.readOnly {
		return ErrReadOnly
	}

	if p.state >= PagerStateWriterLocked {
		return ErrTransactionOpen
	}

	// Acquire reserved lock (with busy handler retry if available)
	var err error
	if p.busyHandler != nil {
		err = p.acquireReservedLockWithRetry()
	} else {
		err = p.tryAcquireReservedLock()
	}

	if err != nil {
		return err
	}

	if err := p.setStateLocked(PagerStateWriterLocked); err != nil {
		return err
	}
	p.dbOrigSize = p.dbSize

	return nil
}

// journalPage writes a page to the journal file with checksum validation.
func (p *Pager) journalPage(page *DbPage) error {
	if p.journalMode == JournalModeOff || p.journalMode == JournalModeWAL {
		return nil
	}

	// Open journal file if not already open
	if p.journalFile == nil {
		if err := p.openJournal(); err != nil {
			return err
		}
	}

	// Write page number, data, and checksum to journal
	// Format: [4 bytes page number][pageSize bytes data][4 bytes checksum]
	journalEntry := make([]byte, 4+p.pageSize+4)

	// Write page number (big-endian)
	journalEntry[0] = byte(page.Pgno >> 24)
	journalEntry[1] = byte(page.Pgno >> 16)
	journalEntry[2] = byte(page.Pgno >> 8)
	journalEntry[3] = byte(page.Pgno)

	// Write page data
	copy(journalEntry[4:4+p.pageSize], page.Data)

	// Calculate and write checksum for the page data
	checksum := p.calculateChecksum(page.Data)
	binary.BigEndian.PutUint32(journalEntry[4+p.pageSize:], checksum)

	if _, err := p.journalFile.Write(journalEntry); err != nil {
		return fmt.Errorf("failed to journal page %d: %w", page.Pgno, err)
	}

	return nil
}

// openJournal opens the journal file for writing.
func (p *Pager) openJournal() error {
	var err error
	p.journalFile, err = os.OpenFile(
		p.journalFilename,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open journal file: %w", err)
	}

	// Write journal header (database page size)
	header := make([]byte, 4)
	header[0] = byte(p.pageSize >> 24)
	header[1] = byte(p.pageSize >> 16)
	header[2] = byte(p.pageSize >> 8)
	header[3] = byte(p.pageSize)

	if _, err := p.journalFile.Write(header); err != nil {
		return fmt.Errorf("failed to write journal header: %w", err)
	}

	return nil
}

// journalEntry represents a parsed journal entry.
type journalEntry struct {
	pgno     Pgno
	pageData []byte
	checksum uint32
	hasChecksum bool
}

// readJournalEntry reads and parses a single journal entry from the journal file.
func (p *Pager) readJournalEntry() (*journalEntry, error) {
	entry := make([]byte, 4+p.pageSize+4)
	n, err := p.journalFile.Read(entry)

	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read journal: %w", err)
	}

	// Check if we have a complete entry with checksum
	if n >= 4+p.pageSize+4 {
		return p.parseJournalEntryWithChecksum(entry), nil
	}

	// Check if we have old format without checksum
	if n == 4+p.pageSize {
		return p.parseJournalEntryWithoutChecksum(entry), nil
	}

	// Incomplete entry - end of journal
	return nil, io.EOF
}

// parseJournalEntryWithChecksum parses an entry that includes checksum.
func (p *Pager) parseJournalEntryWithChecksum(entry []byte) *journalEntry {
	pgno := Pgno(entry[0])<<24 | Pgno(entry[1])<<16 | Pgno(entry[2])<<8 | Pgno(entry[3])
	pageData := entry[4 : 4+p.pageSize]
	checksum := binary.BigEndian.Uint32(entry[4+p.pageSize:])

	return &journalEntry{
		pgno:        pgno,
		pageData:    pageData,
		checksum:    checksum,
		hasChecksum: true,
	}
}

// parseJournalEntryWithoutChecksum parses an entry without checksum (old format).
func (p *Pager) parseJournalEntryWithoutChecksum(entry []byte) *journalEntry {
	pgno := Pgno(entry[0])<<24 | Pgno(entry[1])<<16 | Pgno(entry[2])<<8 | Pgno(entry[3])
	pageData := entry[4 : 4+p.pageSize]

	return &journalEntry{
		pgno:        pgno,
		pageData:    pageData,
		hasChecksum: false,
	}
}

// restorePageFromJournal writes page data back to the database file.
func (p *Pager) restorePageFromJournal(entry *journalEntry) error {
	// Validate checksum if present
	if entry.hasChecksum && !p.validateJournalPage(entry.pageData, entry.checksum) {
		return fmt.Errorf("%w: page %d in journal has invalid checksum", ErrChecksumMismatch, entry.pgno)
	}

	// Write page data back to database
	offset := int64(entry.pgno-1) * int64(p.pageSize)
	if _, err := p.file.WriteAt(entry.pageData, offset); err != nil {
		return fmt.Errorf("failed to rollback page %d: %w", entry.pgno, err)
	}

	return nil
}

// rollbackJournal rolls back changes using the journal file with checksum validation.
func (p *Pager) rollbackJournal() error {
	if p.journalFile == nil {
		return nil
	}

	// Seek to beginning of journal (skip 4-byte header)
	if _, err := p.journalFile.Seek(4, 0); err != nil {
		return err
	}

	// Read and apply journal entries with checksum validation
	for {
		entry, err := p.readJournalEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := p.restorePageFromJournal(entry); err != nil {
			return err
		}
	}

	// Sync the database file
	return p.file.Sync()
}

// finalizeJournal finalizes the journal after a successful commit.
func (p *Pager) finalizeJournal() error {
	// In WAL mode, skip journal handling
	if p.journalMode == JournalModeWAL {
		return nil
	}

	if p.journalFile == nil {
		return nil
	}

	// Close the journal file
	if err := p.journalFile.Close(); err != nil {
		return err
	}
	p.journalFile = nil

	// Delete or truncate based on journal mode
	switch p.journalMode {
	case JournalModeDelete:
		return os.Remove(p.journalFilename)
	case JournalModeTruncate:
		return os.Truncate(p.journalFilename, 0)
	case JournalModePersist:
		// Zero the header to mark journal as invalid
		return p.zeroJournalHeader()
	}

	return nil
}

// zeroJournalHeader zeroes the journal header to mark it as invalid.
func (p *Pager) zeroJournalHeader() error {
	f, err := os.OpenFile(p.journalFilename, os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	zeros := make([]byte, 4)
	_, err = f.WriteAt(zeros, 0)
	return err
}

// updateDatabaseHeader updates the database size and free list info in the header.
func (p *Pager) updateDatabaseHeader() error {
	p.header.DatabaseSize = uint32(p.dbSize)
	p.header.FreelistTrunk = uint32(p.freeList.GetFirstTrunk())
	p.header.FreelistCount = p.freeList.GetTotalFree()
	p.header.FileChangeCounter++

	headerData := p.header.Serialize()
	if _, err := p.file.WriteAt(headerData, 0); err != nil {
		return fmt.Errorf("failed to update database header: %w", err)
	}

	return p.file.Sync()
}

// AllocatePage allocates a new page, first trying the free list,
// then allocating at the end of the file if no free pages are available.
// Returns the page number of the allocated page.
func (p *Pager) AllocatePage() (Pgno, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.readOnly {
		return 0, ErrReadOnly
	}

	// Ensure we have a write transaction
	if err := p.ensureWriteTransaction(); err != nil {
		return 0, err
	}

	// Try to allocate from the free list first
	pgno, err := p.freeList.Allocate()
	if err != nil {
		return 0, err
	}

	// If we got a free page, return it
	if pgno != 0 {
		return pgno, nil
	}

	// No free pages available - allocate new page at end of file
	p.dbSize++
	return p.dbSize, nil
}

// FreePage adds a page to the free list for later reuse.
func (p *Pager) FreePage(pgno Pgno) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.readOnly {
		return ErrReadOnly
	}

	if pgno == 0 || pgno > p.dbSize {
		return ErrInvalidPageNum
	}

	// Ensure we have a write transaction
	if err := p.ensureWriteTransaction(); err != nil {
		return err
	}

	// Add to free list
	return p.freeList.Free(pgno)
}

// GetFreePageCount returns the number of free pages in the database.
func (p *Pager) GetFreePageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.freeList.Count()
}

// getState returns the current pager state with proper synchronization.
func (p *Pager) getState() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// setState sets the pager state with proper synchronization and validation.
func (p *Pager) setState(newState int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.setStateLocked(newState)
}

// setStateLocked sets the pager state without acquiring the lock (must be called with lock held).
func (p *Pager) setStateLocked(newState int) error {
	if err := p.validateTransition(newState); err != nil {
		return err
	}
	p.state = newState
	return nil
}

// validateTransition validates that a state transition is allowed.
func (p *Pager) validateTransition(newState int) error {
	currentState := p.state
	validStates, exists := validTransitions[currentState]
	if !exists {
		return fmt.Errorf("%w: unknown current state %d", ErrInvalidStateTransition, currentState)
	}

	for _, validState := range validStates {
		if validState == newState {
			return nil
		}
	}

	return fmt.Errorf("%w: cannot transition from state %d to %d", ErrInvalidStateTransition, currentState, newState)
}

// validateJournalPage validates a journal page using CRC32 checksum.
func (p *Pager) validateJournalPage(data []byte, expectedChecksum uint32) bool {
	actualChecksum := crc32.ChecksumIEEE(data)
	return actualChecksum == expectedChecksum
}

// calculateChecksum calculates CRC32 checksum for data.
func (p *Pager) calculateChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// writeDirtyPagesToWAL writes all dirty pages to the WAL file.
func (p *Pager) writeDirtyPagesToWAL() error {
	if p.wal == nil {
		return errors.New("WAL not initialized")
	}

	if err := p.writePageFramesToWAL(); err != nil {
		return err
	}

	if err := p.setStateLocked(PagerStateWriterFinished); err != nil {
		return err
	}

	p.autoCheckpointWAL()
	return nil
}

func (p *Pager) writePageFramesToWAL() error {
	dirtyPages := p.cache.GetDirtyPages()
	for _, page := range dirtyPages {
		if err := p.writePageFrameToWAL(page); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) writePageFrameToWAL(page *DbPage) error {
	if err := p.wal.WriteFrame(page.Pgno, page.Data, uint32(p.dbSize)); err != nil {
		return fmt.Errorf("failed to write page %d to WAL: %w", page.Pgno, err)
	}

	if p.walIndex != nil {
		frameNo := p.wal.FrameCount() - 1
		if err := p.walIndex.InsertFrame(uint32(page.Pgno), frameNo); err != nil {
			return fmt.Errorf("failed to update WAL index for page %d: %w", page.Pgno, err)
		}
	}
	return nil
}

func (p *Pager) autoCheckpointWAL() {
	if p.wal.ShouldCheckpoint() {
		_ = p.wal.Checkpoint()
	}
}
