package btree

import (
	"fmt"
	"sync"
)

// PageProvider is an interface for page access (can be pager or in-memory)
type PageProvider interface {
	GetPageData(pgno uint32) ([]byte, error)
	AllocatePageData() (uint32, []byte, error)
	MarkDirty(pgno uint32) error
}

// Btree represents a B-tree database file
type Btree struct {
	PageSize     uint32            // Size of each page in bytes
	UsableSize   uint32            // Usable bytes per page (pageSize - reserved)
	ReservedSize uint32            // Reserved bytes at end of each page
	Pages        map[uint32][]byte // In-memory page cache (pageNum -> page data)
	Provider     PageProvider      // Optional page provider (pager integration)
	mu           sync.RWMutex      // Protects Pages map
}

// BtShared represents shared B-tree state (in SQLite, multiple Btree handles can share this)
type BtShared struct {
	PageSize      uint32 // Total bytes on a page
	UsableSize    uint32 // Number of usable bytes on each page
	MaxLocal      uint16 // Maximum local payload in non-LEAFDATA tables
	MinLocal      uint16 // Minimum local payload in non-LEAFDATA tables
	MaxLeaf       uint16 // Maximum local payload in a LEAFDATA table
	MinLeaf       uint16 // Minimum local payload in a LEAFDATA table
	NumPages      uint32 // Number of pages in the database
	InTransaction bool   // True if in a transaction
}

// NewBtree creates a new B-tree instance
func NewBtree(pageSize uint32) *Btree {
	if pageSize == 0 {
		pageSize = 4096 // Default page size
	}

	return &Btree{
		PageSize:     pageSize,
		UsableSize:   pageSize, // No reserved space by default
		ReservedSize: 0,
		Pages:        make(map[uint32][]byte),
	}
}

// GetPage retrieves a page from the B-tree
func (bt *Btree) GetPage(pageNum uint32) ([]byte, error) {
	// Try in-memory cache first (read lock)
	bt.mu.RLock()
	if page, ok := bt.Pages[pageNum]; ok {
		bt.mu.RUnlock()
		return page, nil
	}
	bt.mu.RUnlock()

	// If we have a provider, try to get from there
	if bt.Provider != nil {
		data, err := bt.Provider.GetPageData(pageNum)
		if err != nil {
			return nil, err
		}
		// Cache it (write lock)
		bt.mu.Lock()
		bt.Pages[pageNum] = data
		bt.mu.Unlock()
		return data, nil
	}

	return nil, fmt.Errorf("page %d not found", pageNum)
}

// SetPage stores a page in the B-tree
func (bt *Btree) SetPage(pageNum uint32, data []byte) error {
	if uint32(len(data)) != bt.PageSize {
		return fmt.Errorf("page size mismatch: expected %d, got %d", bt.PageSize, len(data))
	}
	bt.mu.Lock()
	bt.Pages[pageNum] = data
	bt.mu.Unlock()

	// Mark as dirty if using a provider
	if bt.Provider != nil {
		bt.Provider.MarkDirty(pageNum)
	}
	return nil
}

// ParsePage parses a page and returns its header and cell information
func (bt *Btree) ParsePage(pageNum uint32) (*PageHeader, []*CellInfo, error) {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return nil, nil, err
	}

	// Parse page header
	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse page %d header: %w", pageNum, err)
	}

	// Parse cells
	cells := make([]*CellInfo, header.NumCells)
	for i := 0; i < int(header.NumCells); i++ {
		// Get cell pointer
		cellOffset, err := header.GetCellPointer(pageData, i)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get cell pointer %d: %w", i, err)
		}

		// Get cell data
		if int(cellOffset) >= len(pageData) {
			return nil, nil, fmt.Errorf("cell offset %d out of bounds", cellOffset)
		}
		cellData := pageData[cellOffset:]

		// Parse cell
		cellInfo, err := ParseCell(header.PageType, cellData, bt.UsableSize)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse cell %d: %w", i, err)
		}
		cells[i] = cellInfo
	}

	return header, cells, nil
}

// IteratePage iterates through all cells in a page, calling the visitor function for each
func (bt *Btree) IteratePage(pageNum uint32, visitor func(cellIndex int, cell *CellInfo) error) error {
	header, cells, err := bt.ParsePage(pageNum)
	if err != nil {
		return err
	}

	_ = header // May be used by visitor in the future

	for i, cell := range cells {
		if err := visitor(i, cell); err != nil {
			return err
		}
	}

	return nil
}

// String returns a string representation of the B-tree
func (bt *Btree) String() string {
	bt.mu.RLock()
	pageCount := len(bt.Pages)
	bt.mu.RUnlock()
	return fmt.Sprintf("Btree{pageSize=%d, usableSize=%d, pages=%d}",
		bt.PageSize, bt.UsableSize, pageCount)
}

// ClearCache clears the in-memory page cache.
// This should be called after a rollback to ensure cached pages are re-read from disk.
func (bt *Btree) ClearCache() {
	bt.mu.Lock()
	bt.Pages = make(map[uint32][]byte)
	bt.mu.Unlock()
}

// AllocatePage allocates a new page in the B-tree and returns its page number
func (bt *Btree) AllocatePage() (uint32, error) {
	// Use provider if available
	if bt.Provider != nil {
		pageNum, data, err := bt.Provider.AllocatePageData()
		if err != nil {
			return 0, err
		}
		bt.mu.Lock()
		bt.Pages[pageNum] = data
		bt.mu.Unlock()
		return pageNum, nil
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Find the next available page number
	pageNum := uint32(1)
	for {
		if _, ok := bt.Pages[pageNum]; !ok {
			// Found an unused page number
			break
		}
		pageNum++
		if pageNum == 0 {
			return 0, fmt.Errorf("page number overflow")
		}
	}

	// Create a new empty page
	page := make([]byte, bt.PageSize)
	bt.Pages[pageNum] = page

	return pageNum, nil
}

// CreateTable creates a new table B-tree and returns its root page number
func (bt *Btree) CreateTable() (rootPage uint32, err error) {
	// Allocate a new page for the table root
	rootPage, err = bt.AllocatePage()
	if err != nil {
		return 0, err
	}

	// Get the page data for initialization
	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		return 0, fmt.Errorf("failed to get allocated page: %w", err)
	}

	// Page 1 has a 100-byte database file header, so the page header starts at offset 100
	// For all other pages, the page header starts at offset 0
	headerOffset := 0
	if rootPage == 1 {
		headerOffset = FileHeaderSize
	}

	// Initialize the page as an empty leaf table page
	// Set page type to leaf table (0x0D)
	pageData[headerOffset+PageHeaderOffsetType] = PageTypeLeafTable

	// Initialize header fields
	// FirstFreeblock = 0 (2 bytes)
	pageData[headerOffset+PageHeaderOffsetFreeblock] = 0
	pageData[headerOffset+PageHeaderOffsetFreeblock+1] = 0

	// NumCells = 0 (2 bytes)
	pageData[headerOffset+PageHeaderOffsetNumCells] = 0
	pageData[headerOffset+PageHeaderOffsetNumCells+1] = 0

	// CellContentStart = 0 (2 bytes, 0 means end of page)
	pageData[headerOffset+PageHeaderOffsetCellStart] = 0
	pageData[headerOffset+PageHeaderOffsetCellStart+1] = 0

	// FragmentedBytes = 0 (1 byte)
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0

	return rootPage, nil
}

// DropTable drops a table B-tree by freeing all its pages
func (bt *Btree) DropTable(rootPage uint32) error {
	if rootPage == 0 {
		return fmt.Errorf("invalid root page 0")
	}

	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		return err
	}

	header, err := ParsePageHeader(pageData, rootPage)
	if err != nil {
		return err
	}

	if header.IsInterior {
		bt.dropInteriorChildren(pageData, header)
	}

	bt.mu.Lock()
	delete(bt.Pages, rootPage)
	bt.mu.Unlock()
	return nil
}

// dropInteriorChildren recursively drops all child pages of an interior page.
func (bt *Btree) dropInteriorChildren(pageData []byte, header *PageHeader) {
	for i := 0; i < int(header.NumCells); i++ {
		cellOffset, err := header.GetCellPointer(pageData, i)
		if err != nil {
			continue
		}

		cell, err := ParseCell(header.PageType, pageData[cellOffset:], bt.UsableSize)
		if err != nil {
			continue
		}

		if cell.ChildPage != 0 {
			bt.DropTable(cell.ChildPage)
		}
	}

	if header.RightChild != 0 {
		bt.DropTable(header.RightChild)
	}
}

// NewRowid generates a new unique rowid for a table
func (bt *Btree) NewRowid(rootPage uint32) (int64, error) {
	if rootPage == 0 {
		return 0, fmt.Errorf("invalid root page 0")
	}

	// Find the maximum rowid in the table
	cursor := NewCursor(bt, rootPage)
	if err := cursor.MoveToLast(); err != nil {
		// Empty table - return 1 as first rowid
		return 1, nil
	}

	maxRowid := cursor.GetKey()

	// Return next rowid
	return maxRowid + 1, nil
}
