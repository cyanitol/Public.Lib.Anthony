package btree

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/security"
)

// CellInfo contains parsed information about a B-tree cell
type CellInfo struct {
	Key          int64  // The integer key for table b-trees, or payload size for index b-trees
	Payload      []byte // Pointer to start of payload data
	PayloadSize  uint32 // Total bytes of payload
	LocalPayload uint16 // Amount of payload stored locally (not in overflow pages)
	CellSize     uint16 // Total size of cell on the page
	OverflowPage uint32 // First overflow page number (0 if none)
	ChildPage    uint32 // Child page number (interior pages only)
}

// ParseCell parses a cell from a B-tree page
func ParseCell(pageType byte, cellData []byte, usableSize uint32) (*CellInfo, error) {
	switch pageType {
	case PageTypeLeafTable:
		return parseTableLeafCell(cellData, usableSize)
	case PageTypeInteriorTable:
		return parseTableInteriorCell(cellData)
	case PageTypeLeafIndex:
		return parseIndexLeafCell(cellData, usableSize)
	case PageTypeInteriorIndex:
		return parseIndexInteriorCell(cellData, usableSize)
	default:
		return nil, fmt.Errorf("invalid page type: 0x%02x", pageType)
	}
}

// parseTableLeafCell parses a table leaf cell
// Format: varint(payload_size), varint(rowid), payload
func parseTableLeafCell(cellData []byte, usableSize uint32) (*CellInfo, error) {
	if len(cellData) == 0 {
		return nil, fmt.Errorf("empty cell data")
	}
	info, offset, err := parseLeafCellHeader(cellData)
	if err != nil {
		return nil, err
	}
	return completeLeafCellParse(info, cellData, offset, usableSize)
}

// parseLeafCellHeader reads payload size and rowid from cell data.
func parseLeafCellHeader(cellData []byte) (*CellInfo, int, error) {
	info := &CellInfo{}
	offset := 0

	payloadSize64, n := GetVarint(cellData[offset:])
	if n == 0 {
		return nil, 0, fmt.Errorf("failed to read payload size")
	}
	if payloadSize64 > math.MaxUint32 {
		return nil, 0, security.ErrIntegerOverflow
	}
	info.PayloadSize = uint32(payloadSize64)
	offset += n

	rowid, n := GetVarint(cellData[offset:])
	if n == 0 {
		return nil, 0, fmt.Errorf("failed to read rowid")
	}
	if rowid > math.MaxInt64 {
		return nil, 0, security.ErrIntegerOverflow
	}
	info.Key = int64(rowid)
	offset += n

	return info, offset, nil
}

// completeLeafCellParse finishes parsing the cell with payload extraction.
func completeLeafCellParse(info *CellInfo, cellData []byte, offset int, usableSize uint32) (*CellInfo, error) {
	maxLocal := calculateMaxLocal(usableSize, true)
	minLocal := calculateMinLocal(usableSize, true)
	calculateCellSizeAndLocal(info, offset, maxLocal, minLocal, usableSize)

	if offset+int(info.LocalPayload) > len(cellData) {
		return nil, fmt.Errorf("cell data truncated")
	}
	info.Payload = cellData[offset : offset+int(info.LocalPayload)]

	return extractOverflowPage(info, cellData, offset, maxLocal)
}

// calculateCellSizeAndLocal sets LocalPayload and CellSize.
func calculateCellSizeAndLocal(info *CellInfo, offset int, maxLocal, minLocal, usableSize uint32) {
	if info.PayloadSize <= maxLocal {
		localPayload, err := security.SafeCastUint32ToUint16(info.PayloadSize)
		if err != nil {
			// If payload size doesn't fit in uint16, use maxLocal instead
			localPayload = uint16(maxLocal)
		}
		info.LocalPayload = localPayload

		cellSize, err := security.SafeCastIntToUint16(offset + int(info.PayloadSize))
		if err != nil {
			// Should not happen in practice, but handle defensively
			cellSize = 4
		}
		info.CellSize = cellSize
		if info.CellSize < 4 {
			info.CellSize = 4
		}
	} else {
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)
		cellSize, err := security.SafeCastIntToUint16(offset + int(info.LocalPayload) + 4)
		if err != nil {
			// Should not happen in practice, but handle defensively
			cellSize = uint16(offset) + info.LocalPayload + 4
		}
		info.CellSize = cellSize
	}
}

// extractOverflowPage reads the overflow page number if present.
func extractOverflowPage(info *CellInfo, cellData []byte, offset int, maxLocal uint32) (*CellInfo, error) {
	if info.PayloadSize <= maxLocal {
		return info, nil
	}
	overflowOffset := offset + int(info.LocalPayload)
	if overflowOffset+4 > len(cellData) {
		return nil, fmt.Errorf("overflow page number truncated")
	}
	info.OverflowPage = binary.BigEndian.Uint32(cellData[overflowOffset:])
	return info, nil
}

// parseTableInteriorCell parses a table interior cell
// Format: 4-byte child page number, varint(rowid)
func parseTableInteriorCell(cellData []byte) (*CellInfo, error) {
	if len(cellData) < 4 {
		return nil, fmt.Errorf("cell data too small for interior cell")
	}

	info := &CellInfo{}

	// Read child page number (4 bytes, big-endian)
	info.ChildPage = binary.BigEndian.Uint32(cellData[0:4])

	// Read rowid (varint)
	rowid, n := GetVarint(cellData[4:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read rowid")
	}
	if rowid > math.MaxInt64 {
		return nil, security.ErrIntegerOverflow
	}
	info.Key = int64(rowid)
	info.CellSize = uint16(4 + n)

	return info, nil
}

// parseIndexLeafCell parses an index leaf cell
// Format: varint(payload_size), payload
func parseIndexLeafCell(cellData []byte, usableSize uint32) (*CellInfo, error) {
	if len(cellData) == 0 {
		return nil, fmt.Errorf("empty cell data")
	}

	info := &CellInfo{}
	offset := 0

	// Read payload size (varint)
	payloadSize64, n := GetVarint(cellData[offset:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read payload size")
	}
	if payloadSize64 > math.MaxUint32 {
		return nil, security.ErrIntegerOverflow
	}
	info.PayloadSize = uint32(payloadSize64)
	if payloadSize64 > math.MaxInt64 {
		return nil, security.ErrIntegerOverflow
	}
	info.Key = int64(payloadSize64) // For index pages, key is payload size
	offset += n

	// Calculate local payload size
	maxLocal := calculateMaxLocal(usableSize, false)
	minLocal := calculateMinLocal(usableSize, false)

	if info.PayloadSize <= maxLocal {
		// Entire payload fits locally
		localPayload, err := security.SafeCastUint32ToUint16(info.PayloadSize)
		if err != nil {
			localPayload = uint16(maxLocal)
		}
		info.LocalPayload = localPayload

		cellSize, err := security.SafeCastIntToUint16(offset + int(info.PayloadSize))
		if err != nil {
			cellSize = 4
		}
		info.CellSize = cellSize
		if info.CellSize < 4 {
			info.CellSize = 4
		}
	} else {
		// Payload spills to overflow pages
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)
		cellSize, err := security.SafeCastIntToUint16(offset + int(info.LocalPayload) + 4)
		if err != nil {
			cellSize = uint16(offset) + info.LocalPayload + 4
		}
		info.CellSize = cellSize
	}

	// Extract payload pointer
	if offset+int(info.LocalPayload) > len(cellData) {
		return nil, fmt.Errorf("cell data truncated")
	}
	info.Payload = cellData[offset : offset+int(info.LocalPayload)]

	// Read overflow page if present
	if info.PayloadSize > maxLocal {
		overflowOffset := offset + int(info.LocalPayload)
		if overflowOffset+4 > len(cellData) {
			return nil, fmt.Errorf("overflow page number truncated")
		}
		info.OverflowPage = binary.BigEndian.Uint32(cellData[overflowOffset:])
	}

	return info, nil
}

// parseIndexInteriorCell parses an index interior cell
// Format: 4-byte child page number, varint(payload_size), payload
func parseIndexInteriorCell(cellData []byte, usableSize uint32) (*CellInfo, error) {
	if len(cellData) < 4 {
		return nil, fmt.Errorf("cell data too small for interior cell")
	}

	info := &CellInfo{}

	// Read child page number (4 bytes, big-endian)
	info.ChildPage = binary.BigEndian.Uint32(cellData[0:4])
	offset := 4

	// Read payload size (varint)
	payloadSize64, n := GetVarint(cellData[offset:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read payload size")
	}
	if payloadSize64 > math.MaxUint32 {
		return nil, security.ErrIntegerOverflow
	}
	info.PayloadSize = uint32(payloadSize64)
	if payloadSize64 > math.MaxInt64 {
		return nil, security.ErrIntegerOverflow
	}
	info.Key = int64(payloadSize64)
	offset += n

	// Calculate local payload size
	maxLocal := calculateMaxLocal(usableSize, false)
	minLocal := calculateMinLocal(usableSize, false)

	if info.PayloadSize <= maxLocal {
		// Entire payload fits locally
		localPayload, err := security.SafeCastUint32ToUint16(info.PayloadSize)
		if err != nil {
			localPayload = uint16(maxLocal)
		}
		info.LocalPayload = localPayload

		cellSize, err := security.SafeCastIntToUint16(offset + int(info.PayloadSize))
		if err != nil {
			cellSize = 4
		}
		info.CellSize = cellSize
		if info.CellSize < 4 {
			info.CellSize = 4
		}
	} else {
		// Payload spills to overflow pages
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)
		cellSize, err := security.SafeCastIntToUint16(offset + int(info.LocalPayload) + 4)
		if err != nil {
			cellSize = uint16(offset) + info.LocalPayload + 4
		}
		info.CellSize = cellSize
	}

	// Extract payload pointer
	if offset+int(info.LocalPayload) > len(cellData) {
		return nil, fmt.Errorf("cell data truncated")
	}
	info.Payload = cellData[offset : offset+int(info.LocalPayload)]

	// Read overflow page if present
	if info.PayloadSize > maxLocal {
		overflowOffset := offset + int(info.LocalPayload)
		if overflowOffset+4 > len(cellData) {
			return nil, fmt.Errorf("overflow page number truncated")
		}
		info.OverflowPage = binary.BigEndian.Uint32(cellData[overflowOffset:])
	}

	return info, nil
}

// calculateMaxLocal calculates the maximum amount of payload that can be stored locally
// Based on SQLite's usable page size and whether this is a table or index page
func calculateMaxLocal(usableSize uint32, isTable bool) uint32 {
	// Default values from SQLite: 64/255 for tables, 255/255 for indexes
	// maxLocal = usableSize - 35 (approximately)
	// For simplicity, using SQLite's calculation:
	// At least 4 cells must fit on a page, so maxLocal <= (usableSize-12)/4

	if isTable {
		// Table b-trees: max embedded payload fraction = 64/255
		return (usableSize - 35)
	}
	// Index b-trees: max embedded payload fraction = 255/255 (100%)
	return usableSize - 35
}

// calculateMinLocal calculates the minimum amount of payload that must be stored locally
func calculateMinLocal(usableSize uint32, isTable bool) uint32 {
	// SQLite uses: minLocal = (usableSize-12)*32/255 - 23
	// Validate that usableSize is large enough to avoid underflow
	if usableSize < security.MinUsableSize {
		// Return safe minimum if usableSize is too small
		return 0
	}

	// Prevent underflow: ensure (usableSize-12)*32/255 >= 23
	intermediate := (usableSize - 12) * 32 / 255
	if intermediate < 23 {
		return 0
	}

	return intermediate - 23
}

// calculateLocalPayload calculates how much payload to store locally when it overflows
func calculateLocalPayload(payloadSize uint32, minLocal, maxLocal, usableSize uint32) uint16 {
	// Validate usableSize to prevent underflow
	if usableSize < 4 {
		// Fallback to minLocal if usableSize is too small
		localPayload, err := security.SafeCastUint32ToUint16(minLocal)
		if err != nil {
			return 0
		}
		return localPayload
	}

	surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)

	if surplus <= maxLocal {
		localPayload, err := security.SafeCastUint32ToUint16(surplus)
		if err != nil {
			// If surplus doesn't fit in uint16, use minLocal instead
			localPayload, err = security.SafeCastUint32ToUint16(minLocal)
			if err != nil {
				return 0
			}
		}
		return localPayload
	}

	localPayload, err := security.SafeCastUint32ToUint16(minLocal)
	if err != nil {
		return 0
	}
	return localPayload
}

// String returns a string representation of the cell info
func (c *CellInfo) String() string {
	return fmt.Sprintf("CellInfo{key=%d, payloadSize=%d, localPayload=%d, cellSize=%d, overflow=%d, child=%d}",
		c.Key, c.PayloadSize, c.LocalPayload, c.CellSize, c.OverflowPage, c.ChildPage)
}

// EncodeTableLeafCell encodes a table leaf cell with the given rowid and payload
// Format: varint(payload_size), varint(rowid), payload, [overflow_page_number]
func EncodeTableLeafCell(rowid int64, payload []byte) []byte {
	payloadSize := uint64(len(payload))

	// Calculate buffer size
	// Max varint size is 9 bytes, so allocate enough space
	bufSize := 9 + 9 + len(payload) + 4 // varints + payload + potential overflow page
	buf := make([]byte, bufSize)
	offset := 0

	// Write payload size
	n := PutVarint(buf[offset:], payloadSize)
	offset += n

	// Write rowid
	n = PutVarint(buf[offset:], uint64(rowid))
	offset += n

	// Write payload
	copy(buf[offset:], payload)
	offset += len(payload)

	// Return the actual used portion
	return buf[:offset]
}

// EncodeTableInteriorCell encodes a table interior cell with the given child page and rowid
// Format: 4-byte child page number, varint(rowid)
func EncodeTableInteriorCell(childPage uint32, rowid int64) []byte {
	// Max size: 4 bytes (child page) + 9 bytes (varint rowid)
	buf := make([]byte, 13)
	offset := 0

	// Write child page number (4 bytes, big-endian)
	binary.BigEndian.PutUint32(buf[offset:], childPage)
	offset += 4

	// Write rowid
	n := PutVarint(buf[offset:], uint64(rowid))
	offset += n

	// Return the actual used portion
	return buf[:offset]
}

// EncodeIndexLeafCell encodes an index leaf cell with the given payload
// Format: varint(payload_size), payload, [overflow_page_number]
func EncodeIndexLeafCell(payload []byte) []byte {
	payloadSize := uint64(len(payload))

	// Calculate buffer size
	bufSize := 9 + len(payload) + 4 // varint + payload + potential overflow page
	buf := make([]byte, bufSize)
	offset := 0

	// Write payload size
	n := PutVarint(buf[offset:], payloadSize)
	offset += n

	// Write payload
	copy(buf[offset:], payload)
	offset += len(payload)

	return buf[:offset]
}

// EncodeIndexInteriorCell encodes an index interior cell with the given child page and payload
// Format: 4-byte child page number, varint(payload_size), payload, [overflow_page_number]
func EncodeIndexInteriorCell(childPage uint32, payload []byte) []byte {
	payloadSize := uint64(len(payload))

	// Calculate buffer size
	bufSize := 4 + 9 + len(payload) + 4 // child page + varint + payload + potential overflow
	buf := make([]byte, bufSize)
	offset := 0

	// Write child page number (4 bytes, big-endian)
	binary.BigEndian.PutUint32(buf[offset:], childPage)
	offset += 4

	// Write payload size
	n := PutVarint(buf[offset:], payloadSize)
	offset += n

	// Write payload
	copy(buf[offset:], payload)
	offset += len(payload)

	return buf[:offset]
}
