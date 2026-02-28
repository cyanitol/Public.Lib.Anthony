package pager

import (
	"encoding/binary"
	"errors"
	"sync"
)

// Free list errors
var (
	ErrFreeListCorrupt   = errors.New("free list is corrupt")
	ErrNoFreePages       = errors.New("no free pages available")
	ErrInvalidTrunkPage  = errors.New("invalid trunk page")
	ErrFreeListOverflow  = errors.New("free list overflow")
)

// Free list page structure constants (matching SQLite format)
const (
	// FreeListTrunkHeaderSize is the size of the trunk page header
	// [4 bytes: next trunk page] [4 bytes: number of leaf pages]
	FreeListTrunkHeaderSize = 8
)

// FreeListMaxLeafPages returns the maximum number of leaf page pointers per trunk page.
// Calculated as: (pageSize - 8) / 4
// For 4096 byte pages: (4096 - 8) / 4 = 1022 leaf pages per trunk
func FreeListMaxLeafPages(pageSize int) int {
	return (pageSize - FreeListTrunkHeaderSize) / 4
}

// pagerInternal interface for internal freelist operations.
// This allows the freelist to call pager methods without acquiring locks
// when already inside a pager method that holds the lock.
type pagerInternal interface {
	getLocked(pgno Pgno) (*DbPage, error)
	writeLocked(page *DbPage) error
	Put(page *DbPage)
	PageSize() int
}

// FreeList manages free (unused) pages in the database.
// It implements SQLite's free list format using a linked list of trunk pages,
// where each trunk page contains pointers to leaf pages that are free.
//
// Structure:
// - The database header contains the first trunk page number and total free page count
// - Each trunk page contains:
//   - Bytes 0-3: Next trunk page number (0 if this is the last trunk)
//   - Bytes 4-7: Number of leaf page pointers in this trunk
//   - Bytes 8+: Array of 4-byte leaf page numbers
//
// When allocating a page, we try to reuse from the free list first.
// When freeing a page, we add it to the free list.
type FreeList struct {
	// Pager reference for reading/writing pages
	pager pagerInternal

	// First trunk page number (0 if no free pages)
	firstTrunk Pgno

	// Total number of free pages
	totalFree uint32

	// In-memory cache of recently freed pages (for batching)
	pendingFree []Pgno

	// Maximum pending pages before flushing to disk
	maxPending int

	// Page size
	pageSize int

	// Mutex for thread-safety
	mu sync.Mutex
}

// NewFreeList creates a new FreeList manager.
func NewFreeList(pager pagerInternal) *FreeList {
	return &FreeList{
		pager:       pager,
		pageSize:    pager.PageSize(),
		pendingFree: make([]Pgno, 0, 64),
		maxPending:  64,
	}
}

// Initialize initializes the free list from the database header.
func (fl *FreeList) Initialize(firstTrunk Pgno, totalFree uint32) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	fl.firstTrunk = firstTrunk
	fl.totalFree = totalFree
}

// Count returns the total number of free pages.
func (fl *FreeList) Count() uint32 {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.totalFree + uint32(len(fl.pendingFree))
}

// IsEmpty returns true if there are no free pages available.
func (fl *FreeList) IsEmpty() bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.totalFree == 0 && len(fl.pendingFree) == 0
}

// Allocate allocates a free page for reuse.
// Returns 0 if no free pages are available (caller should allocate new page at end of file).
func (fl *FreeList) Allocate() (Pgno, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// First check pending free pages (in-memory cache)
	if len(fl.pendingFree) > 0 {
		pgno := fl.pendingFree[len(fl.pendingFree)-1]
		fl.pendingFree = fl.pendingFree[:len(fl.pendingFree)-1]
		return pgno, nil
	}

	// No pending pages - check on-disk free list
	if fl.firstTrunk == 0 {
		return 0, nil // No free pages available
	}

	return fl.allocateFromDisk()
}

// allocateFromDisk allocates a page from the on-disk free list.
func (fl *FreeList) allocateFromDisk() (Pgno, error) {
	// Read the first trunk page
	trunkPage, err := fl.pager.getLocked(fl.firstTrunk)
	if err != nil {
		return 0, err
	}
	defer fl.pager.Put(trunkPage)

	// Parse trunk page header
	nextTrunk := Pgno(binary.BigEndian.Uint32(trunkPage.Data[0:4]))
	leafCount := binary.BigEndian.Uint32(trunkPage.Data[4:8])

	var allocatedPage Pgno

	if leafCount > 0 {
		// Allocate from leaf pages in this trunk
		offset := FreeListTrunkHeaderSize + int(leafCount-1)*4
		allocatedPage = Pgno(binary.BigEndian.Uint32(trunkPage.Data[offset : offset+4]))

		// Mark page as writable and update leaf count
		if err := fl.pager.writeLocked(trunkPage); err != nil {
			return 0, err
		}
		binary.BigEndian.PutUint32(trunkPage.Data[4:8], leafCount-1)

	} else {
		// No leaf pages in this trunk - allocate the trunk page itself
		allocatedPage = fl.firstTrunk
		fl.firstTrunk = nextTrunk
	}

	fl.totalFree--
	return allocatedPage, nil
}

// Free adds a page to the free list.
func (fl *FreeList) Free(pgno Pgno) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// Add to pending list
	fl.pendingFree = append(fl.pendingFree, pgno)

	// Flush if pending list is full
	if len(fl.pendingFree) >= fl.maxPending {
		return fl.flushPending()
	}

	return nil
}

// FreeMultiple adds multiple pages to the free list.
func (fl *FreeList) FreeMultiple(pages []Pgno) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	for _, pgno := range pages {
		fl.pendingFree = append(fl.pendingFree, pgno)
		if len(fl.pendingFree) >= fl.maxPending {
			if err := fl.flushPending(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Flush writes all pending free pages to disk.
func (fl *FreeList) Flush() error {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.flushPending()
}

// flushPending writes pending free pages to the on-disk free list.
func (fl *FreeList) flushPending() error {
	if len(fl.pendingFree) == 0 {
		return nil
	}

	maxLeaves := FreeListMaxLeafPages(fl.pageSize)

	for len(fl.pendingFree) > 0 {
		if fl.firstTrunk == 0 {
			// No trunk pages exist - create one using first pending page
			if err := fl.createNewTrunk(); err != nil {
				return err
			}
			continue
		}

		// Read current trunk page
		trunkPage, err := fl.pager.getLocked(fl.firstTrunk)
		if err != nil {
			return err
		}

		leafCount := int(binary.BigEndian.Uint32(trunkPage.Data[4:8]))

		if leafCount < maxLeaves {
			// Add pending pages to this trunk
			if err := fl.pager.writeLocked(trunkPage); err != nil {
				fl.pager.Put(trunkPage)
				return err
			}

			// Add as many pending pages as will fit
			for leafCount < maxLeaves && len(fl.pendingFree) > 0 {
				pgno := fl.pendingFree[len(fl.pendingFree)-1]
				fl.pendingFree = fl.pendingFree[:len(fl.pendingFree)-1]

				offset := FreeListTrunkHeaderSize + leafCount*4
				binary.BigEndian.PutUint32(trunkPage.Data[offset:offset+4], uint32(pgno))
				leafCount++
				fl.totalFree++
			}

			binary.BigEndian.PutUint32(trunkPage.Data[4:8], uint32(leafCount))
			fl.pager.Put(trunkPage)
		} else {
			// Trunk is full - create a new trunk page
			fl.pager.Put(trunkPage)
			if err := fl.createNewTrunk(); err != nil {
				return err
			}
		}
	}

	return nil
}

// createNewTrunk creates a new trunk page using the first pending free page.
func (fl *FreeList) createNewTrunk() error {
	if len(fl.pendingFree) == 0 {
		return ErrNoFreePages
	}

	// Use the first pending page as the new trunk
	newTrunkPgno := fl.pendingFree[len(fl.pendingFree)-1]
	fl.pendingFree = fl.pendingFree[:len(fl.pendingFree)-1]

	// Read the page to initialize it as a trunk
	trunkPage, err := fl.pager.getLocked(newTrunkPgno)
	if err != nil {
		return err
	}

	if err := fl.pager.writeLocked(trunkPage); err != nil {
		fl.pager.Put(trunkPage)
		return err
	}

	// Initialize trunk header
	// Point to previous first trunk
	binary.BigEndian.PutUint32(trunkPage.Data[0:4], uint32(fl.firstTrunk))
	// No leaf pages yet
	binary.BigEndian.PutUint32(trunkPage.Data[4:8], 0)
	// Zero out the rest
	for i := FreeListTrunkHeaderSize; i < len(trunkPage.Data); i++ {
		trunkPage.Data[i] = 0
	}

	fl.pager.Put(trunkPage)

	// Update first trunk pointer
	fl.firstTrunk = newTrunkPgno
	fl.totalFree++ // The trunk page itself is counted as free

	return nil
}

// GetFirstTrunk returns the first trunk page number.
func (fl *FreeList) GetFirstTrunk() Pgno {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.firstTrunk
}

// GetTotalFree returns the total free count (for updating header).
func (fl *FreeList) GetTotalFree() uint32 {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.totalFree
}

// ReadTrunk reads and returns information about a trunk page.
func (fl *FreeList) ReadTrunk(trunkPgno Pgno) (nextTrunk Pgno, leaves []Pgno, err error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	if trunkPgno == 0 {
		return 0, nil, ErrInvalidTrunkPage
	}

	trunkPage, err := fl.pager.getLocked(trunkPgno)
	if err != nil {
		return 0, nil, err
	}
	defer fl.pager.Put(trunkPage)

	nextTrunk = Pgno(binary.BigEndian.Uint32(trunkPage.Data[0:4]))
	leafCount := binary.BigEndian.Uint32(trunkPage.Data[4:8])

	leaves = make([]Pgno, leafCount)
	for i := uint32(0); i < leafCount; i++ {
		offset := FreeListTrunkHeaderSize + int(i)*4
		leaves[i] = Pgno(binary.BigEndian.Uint32(trunkPage.Data[offset : offset+4]))
	}

	return nextTrunk, leaves, nil
}

// Iterate calls the callback function for each free page.
// This iterates through both pending and on-disk free pages.
func (fl *FreeList) Iterate(callback func(pgno Pgno) bool) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	// First iterate pending pages
	for _, pgno := range fl.pendingFree {
		if !callback(pgno) {
			return nil
		}
	}

	// Then iterate on-disk free list
	trunkPgno := fl.firstTrunk
	for trunkPgno != 0 {
		trunkPage, err := fl.pager.getLocked(trunkPgno)
		if err != nil {
			return err
		}

		nextTrunk := Pgno(binary.BigEndian.Uint32(trunkPage.Data[0:4]))
		leafCount := binary.BigEndian.Uint32(trunkPage.Data[4:8])

		// Visit leaf pages
		for i := uint32(0); i < leafCount; i++ {
			offset := FreeListTrunkHeaderSize + int(i)*4
			leafPgno := Pgno(binary.BigEndian.Uint32(trunkPage.Data[offset : offset+4]))
			if !callback(leafPgno) {
				fl.pager.Put(trunkPage)
				return nil
			}
		}

		fl.pager.Put(trunkPage)
		trunkPgno = nextTrunk
	}

	return nil
}

// Verify checks the integrity of the free list.
// Returns an error if the free list is corrupt.
func (fl *FreeList) Verify() error {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	seen := make(map[Pgno]bool)
	count := uint32(0)

	trunkPgno := fl.firstTrunk
	for trunkPgno != 0 {
		// Check for cycles
		if seen[trunkPgno] {
			return ErrFreeListCorrupt
		}
		seen[trunkPgno] = true
		count++ // Count the trunk page itself

		trunkPage, err := fl.pager.getLocked(trunkPgno)
		if err != nil {
			return err
		}

		nextTrunk := Pgno(binary.BigEndian.Uint32(trunkPage.Data[0:4]))
		leafCount := binary.BigEndian.Uint32(trunkPage.Data[4:8])

		maxLeaves := uint32(FreeListMaxLeafPages(fl.pageSize))
		if leafCount > maxLeaves {
			fl.pager.Put(trunkPage)
			return ErrFreeListCorrupt
		}

		// Verify leaf pages
		for i := uint32(0); i < leafCount; i++ {
			offset := FreeListTrunkHeaderSize + int(i)*4
			leafPgno := Pgno(binary.BigEndian.Uint32(trunkPage.Data[offset : offset+4]))

			if leafPgno == 0 {
				fl.pager.Put(trunkPage)
				return ErrFreeListCorrupt
			}

			if seen[leafPgno] {
				fl.pager.Put(trunkPage)
				return ErrFreeListCorrupt
			}
			seen[leafPgno] = true
			count++
		}

		fl.pager.Put(trunkPage)
		trunkPgno = nextTrunk
	}

	// Verify count matches
	if count != fl.totalFree {
		return ErrFreeListCorrupt
	}

	return nil
}

// Clear empties the free list (for testing or special operations).
func (fl *FreeList) Clear() {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	fl.firstTrunk = 0
	fl.totalFree = 0
	fl.pendingFree = fl.pendingFree[:0]
}

// FreeListInfo contains information about the free list state.
type FreeListInfo struct {
	FirstTrunk   Pgno
	TotalFree    uint32
	PendingFree  int
	TrunkCount   int
	LeafCount    int
}

// Info returns information about the free list state.
func (fl *FreeList) Info() (FreeListInfo, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	info := FreeListInfo{
		FirstTrunk:  fl.firstTrunk,
		TotalFree:   fl.totalFree,
		PendingFree: len(fl.pendingFree),
	}

	// Count trunk and leaf pages
	trunkPgno := fl.firstTrunk
	for trunkPgno != 0 {
		info.TrunkCount++

		trunkPage, err := fl.pager.getLocked(trunkPgno)
		if err != nil {
			return info, err
		}

		nextTrunk := Pgno(binary.BigEndian.Uint32(trunkPage.Data[0:4]))
		leafCount := binary.BigEndian.Uint32(trunkPage.Data[4:8])
		info.LeafCount += int(leafCount)

		fl.pager.Put(trunkPage)
		trunkPgno = nextTrunk
	}

	return info, nil
}
