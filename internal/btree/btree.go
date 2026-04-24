// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"errors"
	"fmt"
	"sync"
)

// Error definitions for page validation
var (
	ErrInvalidPageNumber = errors.New("invalid page number")
	ErrCorruptedPage     = errors.New("corrupted page")
	ErrInvalidPageType   = errors.New("invalid page type")
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

// validatePage validates a page's integrity before caching
func (bt *Btree) validatePage(data []byte, pageNum uint32) error {
	if err := bt.validatePageSize(data); err != nil {
		return err
	}
	if isUninitializedPage(data, pageNum) {
		return nil
	}
	headerOffset := pageHeaderOffset(pageNum)
	if data[headerOffset+PageHeaderOffsetType] == 0 {
		return nil
	}
	header, err := ParsePageHeader(data, pageNum)
	if err != nil {
		return fmt.Errorf("%w: failed to parse header: %v", ErrCorruptedPage, err)
	}
	if err := validatePageTypeForBtree(header.PageType); err != nil {
		return err
	}
	return validatePageStructure(header, data)
}

func isUninitializedPage(data []byte, pageNum uint32) bool {
	if pageNum == 1 {
		return isZeroBuffer(data[FileHeaderSize:])
	}
	return isZeroBuffer(data)
}

func pageHeaderOffset(pageNum uint32) int {
	if pageNum == 1 {
		return FileHeaderSize
	}
	return 0
}

// isZeroBuffer reports whether the buffer is entirely zeroed.
func isZeroBuffer(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// validatePageSize checks if the page data size matches expected size.
func (bt *Btree) validatePageSize(data []byte) error {
	if len(data) < int(bt.PageSize) {
		return fmt.Errorf("%w: page size mismatch (expected %d, got %d)",
			ErrCorruptedPage, bt.PageSize, len(data))
	}
	return nil
}

// validatePageTypeForBtree checks if the page type is valid for btree validation.
// Reuses the package-level validPageTypes map defined in page.go.
func validatePageTypeForBtree(pageType byte) error {
	if !validPageTypes[pageType] {
		return fmt.Errorf("%w: 0x%02x", ErrInvalidPageType, pageType)
	}
	return nil
}

// validatePageStructure validates cell pointer array and content area.
func validatePageStructure(header *PageHeader, data []byte) error {
	cellPtrArraySize := int(header.NumCells) * 2
	if header.CellPtrOffset+cellPtrArraySize > len(data) {
		return fmt.Errorf("%w: cell pointer array exceeds page bounds", ErrCorruptedPage)
	}

	cellContentStart := int(header.CellContentStart)
	if cellContentStart == 0 {
		cellContentStart = len(data)
	}
	if cellContentStart > len(data) {
		return fmt.Errorf("%w: invalid cell content start", ErrCorruptedPage)
	}

	cellPtrArrayEnd := header.CellPtrOffset + cellPtrArraySize
	if cellPtrArrayEnd > cellContentStart {
		return fmt.Errorf("%w: cell pointers overlap with cell content", ErrCorruptedPage)
	}

	return nil
}

// initEmptyPageHeader zeroes the B-tree page header fields and sets the page type.
// This consolidates the repeated header-init pattern used by CreateTable,
// CreateWithoutRowidTable, and ClearTableData.
func initEmptyPageHeader(pageData []byte, pageNum uint32, pageType byte) {
	headerOffset := 0
	if pageNum == 1 {
		headerOffset = FileHeaderSize
	}
	pageData[headerOffset+PageHeaderOffsetType] = pageType
	pageData[headerOffset+PageHeaderOffsetFreeblock] = 0
	pageData[headerOffset+PageHeaderOffsetFreeblock+1] = 0
	pageData[headerOffset+PageHeaderOffsetNumCells] = 0
	pageData[headerOffset+PageHeaderOffsetNumCells+1] = 0
	pageData[headerOffset+PageHeaderOffsetCellStart] = 0
	pageData[headerOffset+PageHeaderOffsetCellStart+1] = 0
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0
}

// GetPage retrieves a page from the B-tree
func (bt *Btree) GetPage(pageNum uint32) ([]byte, error) {
	// Validate page number
	if pageNum == 0 {
		return nil, ErrInvalidPageNumber
	}

	// Fast path: check cache with read lock
	bt.mu.RLock()
	if page, ok := bt.Pages[pageNum]; ok {
		bt.mu.RUnlock()
		return page, nil
	}
	bt.mu.RUnlock()

	// If we have a provider, load from storage
	if bt.Provider != nil {
		// Slow path: acquire write lock
		bt.mu.Lock()
		defer bt.mu.Unlock()

		// CRITICAL: Double-check after acquiring write lock
		// Another goroutine may have loaded the page
		if page, ok := bt.Pages[pageNum]; ok {
			return page, nil
		}

		// Load page from provider
		data, err := bt.Provider.GetPageData(pageNum)
		if err != nil {
			return nil, err
		}

		// Validate page before caching
		if err := bt.validatePage(data, pageNum); err != nil {
			return nil, err
		}

		// Cache the validated page
		bt.Pages[pageNum] = data
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

	// Initialize the page as an empty leaf table page
	initEmptyPageHeader(pageData, rootPage, PageTypeLeafTable)

	// Mark page dirty so pager persists initialization
	if bt.Provider != nil {
		if err := bt.Provider.MarkDirty(rootPage); err != nil {
			return 0, err
		}
	}

	return rootPage, nil
}

// CreateWithoutRowidTable creates a new WITHOUT ROWID table B-tree root.
func (bt *Btree) CreateWithoutRowidTable() (rootPage uint32, err error) {
	rootPage, err = bt.AllocatePage()
	if err != nil {
		return 0, err
	}

	pageData, err := bt.GetPage(rootPage)
	if err != nil {
		return 0, fmt.Errorf("failed to get allocated page: %w", err)
	}

	initEmptyPageHeader(pageData, rootPage, PageTypeLeafTableNoInt)

	if bt.Provider != nil {
		if err := bt.Provider.MarkDirty(rootPage); err != nil {
			return 0, err
		}
	}

	return rootPage, nil
}

// ClearTableData removes all rows from a table by reinitializing its root page as an empty leaf.
// Any child pages of an interior root are dropped. The root page itself is reused.
func (bt *Btree) ClearTableData(rootPage uint32) error {
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

	// Drop child pages if root is an interior node
	if header.IsInterior {
		bt.dropInteriorChildren(pageData, header)
	}

	// Reinitialize root page as empty leaf table
	initEmptyPageHeader(pageData, rootPage, PageTypeLeafTable)

	return nil
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
