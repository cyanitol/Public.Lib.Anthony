package btree

import (
	"encoding/binary"
	"fmt"
)

// Page type constants (first byte of page header)
const (
	PageTypeInteriorIndex = 0x02 // Interior index b-tree page
	PageTypeInteriorTable = 0x05 // Interior table b-tree page
	PageTypeLeafIndex     = 0x0a // Leaf index b-tree page
	PageTypeLeafTable     = 0x0d // Leaf table b-tree page
)

// Page type flags (bit flags in page type byte)
const (
	PTF_INTKEY   = 0x01 // True if table b-trees (integer key)
	PTF_ZERODATA = 0x02 // True for index b-trees (no data, only keys)
	PTF_LEAFDATA = 0x04 // True if data is stored in leaves
	PTF_LEAF     = 0x08 // True if this is a leaf page
)

// Page header offsets
const (
	PageHeaderOffsetType       = 0 // Page type (1 byte)
	PageHeaderOffsetFreeblock  = 1 // First freeblock offset (2 bytes)
	PageHeaderOffsetNumCells   = 3 // Number of cells (2 bytes)
	PageHeaderOffsetCellStart  = 5 // Start of cell content area (2 bytes)
	PageHeaderOffsetFragmented = 7 // Fragmented free bytes (1 byte)
	PageHeaderOffsetRightChild = 8 // Right-most child pointer (4 bytes, interior only)
)

// Header sizes
const (
	PageHeaderSizeLeaf     = 8   // Leaf pages: 8 bytes
	PageHeaderSizeInterior = 12  // Interior pages: 12 bytes (includes right child pointer)
	FileHeaderSize         = 100 // Database file header on page 1
)

// PageHeader represents the parsed header of a B-tree page
type PageHeader struct {
	PageType         byte   // Page type (0x02, 0x05, 0x0a, 0x0d)
	FirstFreeblock   uint16 // Offset to first freeblock (0 if none)
	NumCells         uint16 // Number of cells on this page
	CellContentStart uint16 // Start of cell content area
	FragmentedBytes  byte   // Number of fragmented free bytes
	RightChild       uint32 // Right-most child page number (interior pages only)

	// Derived properties
	IsLeaf        bool // True if this is a leaf page
	IsInterior    bool // True if this is an interior page
	IsTable       bool // True if this is a table b-tree (intkey)
	IsIndex       bool // True if this is an index b-tree (blob key)
	HeaderSize    int  // Size of page header (8 or 12 bytes)
	CellPtrOffset int  // Offset where cell pointer array starts
}

// ParsePageHeader parses the B-tree page header from raw page data
func ParsePageHeader(data []byte, pageNum uint32) (*PageHeader, error) {
	offset, err := validatePageData(data, pageNum)
	if err != nil {
		return nil, err
	}

	h := parseHeaderFields(data, offset)
	if err := finalizeHeader(h, data, offset); err != nil {
		return nil, err
	}
	return h, nil
}

func validatePageData(data []byte, pageNum uint32) (int, error) {
	if len(data) < PageHeaderSizeLeaf {
		return 0, fmt.Errorf("page data too small: %d bytes", len(data))
	}
	if pageNum == 1 {
		if len(data) < FileHeaderSize+PageHeaderSizeLeaf {
			return 0, fmt.Errorf("page 1 data too small: %d bytes", len(data))
		}
		return FileHeaderSize, nil
	}
	return 0, nil
}

func parseHeaderFields(data []byte, offset int) *PageHeader {
	h := &PageHeader{
		PageType:         data[offset+PageHeaderOffsetType],
		FirstFreeblock:   binary.BigEndian.Uint16(data[offset+PageHeaderOffsetFreeblock:]),
		NumCells:         binary.BigEndian.Uint16(data[offset+PageHeaderOffsetNumCells:]),
		CellContentStart: binary.BigEndian.Uint16(data[offset+PageHeaderOffsetCellStart:]),
		FragmentedBytes:  data[offset+PageHeaderOffsetFragmented],
	}
	h.IsLeaf = (h.PageType & PTF_LEAF) != 0
	h.IsInterior = !h.IsLeaf
	h.IsTable = (h.PageType & PTF_INTKEY) != 0
	h.IsIndex = !h.IsTable
	return h
}

func finalizeHeader(h *PageHeader, data []byte, offset int) error {
	if h.IsInterior {
		if len(data) < offset+PageHeaderSizeInterior {
			return fmt.Errorf("interior page data too small: %d bytes", len(data))
		}
		h.RightChild = binary.BigEndian.Uint32(data[offset+PageHeaderOffsetRightChild:])
		h.HeaderSize = PageHeaderSizeInterior
	} else {
		h.HeaderSize = PageHeaderSizeLeaf
	}
	h.CellPtrOffset = offset + h.HeaderSize

	return validatePageType(h.PageType)
}

var validPageTypes = map[byte]bool{
	PageTypeInteriorIndex: true,
	PageTypeInteriorTable: true,
	PageTypeLeafIndex:     true,
	PageTypeLeafTable:     true,
}

func validatePageType(pt byte) error {
	if !validPageTypes[pt] {
		return fmt.Errorf("invalid page type: 0x%02x", pt)
	}
	return nil
}

// GetCellPointer returns the offset of the i-th cell in the page
func (h *PageHeader) GetCellPointer(data []byte, cellIndex int) (uint16, error) {
	if cellIndex < 0 || cellIndex >= int(h.NumCells) {
		return 0, fmt.Errorf("cell index out of range: %d (max %d)", cellIndex, h.NumCells-1)
	}

	ptrOffset := h.CellPtrOffset + (cellIndex * 2)
	if ptrOffset+2 > len(data) {
		return 0, fmt.Errorf("cell pointer offset out of bounds: %d", ptrOffset)
	}

	return binary.BigEndian.Uint16(data[ptrOffset:]), nil
}

// GetCellPointers returns all cell pointers in the page
func (h *PageHeader) GetCellPointers(data []byte) ([]uint16, error) {
	pointers := make([]uint16, h.NumCells)
	for i := 0; i < int(h.NumCells); i++ {
		ptr, err := h.GetCellPointer(data, i)
		if err != nil {
			return nil, err
		}
		pointers[i] = ptr
	}
	return pointers, nil
}

// String returns a string representation of the page header
func (h *PageHeader) String() string {
	pageTypeStr := "unknown"
	switch h.PageType {
	case PageTypeInteriorIndex:
		pageTypeStr = "interior index"
	case PageTypeInteriorTable:
		pageTypeStr = "interior table"
	case PageTypeLeafIndex:
		pageTypeStr = "leaf index"
	case PageTypeLeafTable:
		pageTypeStr = "leaf table"
	}

	return fmt.Sprintf("PageHeader{type=%s, cells=%d, contentStart=%d, freeblock=%d, fragmented=%d}",
		pageTypeStr, h.NumCells, h.CellContentStart, h.FirstFreeblock, h.FragmentedBytes)
}

// BtreePage wraps a raw page buffer and provides write operations
type BtreePage struct {
	Data       []byte      // Raw page data
	PageNum    uint32      // Page number
	Header     *PageHeader // Parsed page header
	UsableSize uint32      // Usable bytes per page
}

// NewBtreePage creates a new BtreePage wrapper from raw page data
func NewBtreePage(pageNum uint32, data []byte, usableSize uint32) (*BtreePage, error) {
	header, err := ParsePageHeader(data, pageNum)
	if err != nil {
		return nil, err
	}

	return &BtreePage{
		Data:       data,
		PageNum:    pageNum,
		Header:     header,
		UsableSize: usableSize,
	}, nil
}

// InsertCell inserts a cell at the given index
func (p *BtreePage) InsertCell(idx int, cell []byte) error {
	if idx < 0 || idx > int(p.Header.NumCells) {
		return fmt.Errorf("invalid cell index: %d (max %d)", idx, p.Header.NumCells)
	}

	cellSize := len(cell)
	if cellSize < 4 {
		cellSize = 4 // Minimum cell size
	}

	// Allocate space for the cell
	cellOffset, err := p.AllocateSpace(cellSize)
	if err != nil {
		return err
	}

	// Copy cell data
	copy(p.Data[cellOffset:], cell)

	// Make room in cell pointer array
	cellPtrOffset := p.Header.CellPtrOffset + (idx * 2)
	numCellsAfter := int(p.Header.NumCells) - idx

	if numCellsAfter > 0 {
		// Shift cell pointers to make room
		src := p.Data[cellPtrOffset : cellPtrOffset+(numCellsAfter*2)]
		dst := p.Data[cellPtrOffset+2 : cellPtrOffset+2+(numCellsAfter*2)]
		copy(dst, src)
	}

	// Write new cell pointer
	binary.BigEndian.PutUint16(p.Data[cellPtrOffset:], uint16(cellOffset))

	// Update header
	p.Header.NumCells++
	binary.BigEndian.PutUint16(p.Data[p.Header.CellPtrOffset-5:], p.Header.NumCells)

	return nil
}

// DeleteCell deletes the cell at the given index
func (p *BtreePage) DeleteCell(idx int) error {
	if idx < 0 || idx >= int(p.Header.NumCells) {
		return fmt.Errorf("invalid cell index: %d (max %d)", idx, p.Header.NumCells-1)
	}

	// Get the cell pointer to delete
	cellPtrOffset := p.Header.CellPtrOffset + (idx * 2)

	// Remove cell pointer by shifting remaining pointers
	numCellsAfter := int(p.Header.NumCells) - idx - 1
	if numCellsAfter > 0 {
		src := p.Data[cellPtrOffset+2 : cellPtrOffset+2+(numCellsAfter*2)]
		dst := p.Data[cellPtrOffset : cellPtrOffset+(numCellsAfter*2)]
		copy(dst, src)
	}

	// Zero out the last cell pointer (optional, for cleanliness)
	lastPtrOffset := p.Header.CellPtrOffset + ((int(p.Header.NumCells) - 1) * 2)
	p.Data[lastPtrOffset] = 0
	p.Data[lastPtrOffset+1] = 0

	// Update header
	p.Header.NumCells--
	binary.BigEndian.PutUint16(p.Data[p.Header.CellPtrOffset-5:], p.Header.NumCells)

	// Note: The actual cell content is not removed here - it becomes fragmented space
	// Call Defragment() to reclaim the space

	return nil
}

// AllocateSpace allocates space for a cell of the given size
// Returns the offset where the cell should be written
func (p *BtreePage) AllocateSpace(size int) (offset int, err error) {
	// Calculate where cell content starts (or should start)
	cellContentStart := int(p.Header.CellContentStart)
	if cellContentStart == 0 {
		// 0 means end of page
		cellContentStart = int(p.UsableSize)
	}

	// Calculate space needed
	// We need space for the cell pointer (2 bytes) and the cell itself
	cellPtrArrayEnd := p.Header.CellPtrOffset + (int(p.Header.NumCells)+1)*2
	newCellContentStart := cellContentStart - size

	// Check if we have enough space
	if newCellContentStart < cellPtrArrayEnd {
		// Not enough space - need to defragment
		if err := p.Defragment(); err != nil {
			return 0, err
		}

		// Recalculate after defragmentation
		cellContentStart = int(p.Header.CellContentStart)
		if cellContentStart == 0 {
			cellContentStart = int(p.UsableSize)
		}
		newCellContentStart = cellContentStart - size

		// Check again
		if newCellContentStart < cellPtrArrayEnd {
			return 0, fmt.Errorf("page is full (need %d bytes, have %d)", size, cellContentStart-cellPtrArrayEnd)
		}
	}

	// Update cell content start in header
	p.Header.CellContentStart = uint16(newCellContentStart)
	binary.BigEndian.PutUint16(p.Data[p.Header.CellPtrOffset-3:], uint16(newCellContentStart))

	return newCellContentStart, nil
}

// Defragment defragments the page by compacting all cells
func (p *BtreePage) Defragment() error {
	if p.Header.NumCells == 0 {
		// Empty page - just reset content start
		p.Header.CellContentStart = 0
		binary.BigEndian.PutUint16(p.Data[p.Header.CellPtrOffset-3:], 0)
		return nil
	}

	// Get all cell pointers
	cellPointers, err := p.Header.GetCellPointers(p.Data)
	if err != nil {
		return err
	}

	// Parse all cells to get their sizes
	type cellData struct {
		offset int
		data   []byte
	}
	cells := make([]cellData, len(cellPointers))

	for i, ptr := range cellPointers {
		cellOffset := int(ptr)
		if cellOffset >= len(p.Data) {
			return fmt.Errorf("invalid cell offset: %d", cellOffset)
		}

		// Parse cell to determine size
		cell, err := ParseCell(p.Header.PageType, p.Data[cellOffset:], p.UsableSize)
		if err != nil {
			return err
		}

		cells[i] = cellData{
			offset: cellOffset,
			data:   p.Data[cellOffset : cellOffset+int(cell.CellSize)],
		}
	}

	// Compact cells from end of page backwards
	newContentStart := int(p.UsableSize)
	for i := len(cells) - 1; i >= 0; i-- {
		cellSize := len(cells[i].data)
		newContentStart -= cellSize

		// Copy cell to new location
		copy(p.Data[newContentStart:], cells[i].data)

		// Update cell pointer
		cellPtrOffset := p.Header.CellPtrOffset + (i * 2)
		binary.BigEndian.PutUint16(p.Data[cellPtrOffset:], uint16(newContentStart))
	}

	// Update header
	p.Header.CellContentStart = uint16(newContentStart)
	binary.BigEndian.PutUint16(p.Data[p.Header.CellPtrOffset-3:], uint16(newContentStart))

	// Clear fragmented bytes
	p.Header.FragmentedBytes = 0
	p.Data[p.Header.CellPtrOffset-1] = 0

	return nil
}

// FreeSpace returns the amount of free space on the page
func (p *BtreePage) FreeSpace() int {
	cellContentStart := int(p.Header.CellContentStart)
	if cellContentStart == 0 {
		cellContentStart = int(p.UsableSize)
	}

	cellPtrArrayEnd := p.Header.CellPtrOffset + (int(p.Header.NumCells) * 2)
	freeSpace := cellContentStart - cellPtrArrayEnd

	// Subtract space needed for one more cell pointer
	freeSpace -= 2

	if freeSpace < 0 {
		return 0
	}
	return freeSpace
}
