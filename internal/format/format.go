// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package format defines SQLite file format constants and structures.
//
// This package provides low-level SQLite database file format definitions including:
//   - Database header format and offsets
//   - Page type constants
//   - B-tree page header offsets
//   - Text encoding values
//   - Helper functions for validation
//
// These constants are used throughout the SQLite engine implementation.
package format

import (
	"encoding/binary"
	"fmt"
)

// SQLite file format constants
const (
	// HeaderSize is the database header size in bytes (first 100 bytes of the database file).
	HeaderSize = 100

	// MagicString is the magic header string for SQLite 3 database files.
	// Must be exactly 16 bytes including the null terminator.
	MagicString = "SQLite format 3\000"

	// DefaultPageSize is the default page size for new databases (4096 bytes).
	DefaultPageSize = 4096

	// MinPageSize is the minimum allowed page size (512 bytes).
	MinPageSize = 512

	// MaxPageSize is the maximum allowed page size (65536 bytes).
	MaxPageSize = 65536
)

// Header offsets - byte positions in the 100-byte database header
const (
	// OffsetMagic is the offset of the magic header string (16 bytes).
	OffsetMagic = 0

	// OffsetPageSize is the offset of the page size field (2 bytes big-endian).
	// A value of 1 represents 65536 bytes.
	OffsetPageSize = 16

	// OffsetWriteVersion is the file format write version (1 byte).
	// Valid values: 1 (legacy), 2 (WAL mode).
	OffsetWriteVersion = 18

	// OffsetReadVersion is the file format read version (1 byte).
	// Valid values: 1 (legacy), 2 (WAL mode).
	OffsetReadVersion = 19

	// OffsetReservedSpace is the reserved space at end of each page (1 byte).
	OffsetReservedSpace = 20

	// OffsetMaxPayloadFrac is the maximum embedded payload fraction (1 byte).
	// Must be 64.
	OffsetMaxPayloadFrac = 21

	// OffsetMinPayloadFrac is the minimum embedded payload fraction (1 byte).
	// Must be 32.
	OffsetMinPayloadFrac = 22

	// OffsetLeafPayloadFrac is the leaf payload fraction (1 byte).
	// Must be 32.
	OffsetLeafPayloadFrac = 23

	// OffsetFileChangeCounter is the file change counter (4 bytes big-endian).
	// Incremented whenever the database is modified.
	OffsetFileChangeCounter = 24

	// OffsetDatabaseSize is the database size in pages (4 bytes big-endian).
	OffsetDatabaseSize = 28

	// OffsetFirstFreelist is the first freelist trunk page (4 bytes big-endian).
	OffsetFirstFreelist = 32

	// OffsetFreelistCount is the total number of freelist pages (4 bytes big-endian).
	OffsetFreelistCount = 36

	// OffsetSchemaCookie is the schema cookie (4 bytes big-endian).
	// Incremented whenever the schema changes.
	OffsetSchemaCookie = 40

	// OffsetSchemaFormat is the schema format number (4 bytes big-endian).
	// Supported values: 1, 2, 3, 4.
	OffsetSchemaFormat = 44

	// OffsetDefaultCacheSize is the default page cache size (4 bytes big-endian).
	OffsetDefaultCacheSize = 48

	// OffsetLargestRootPage is the largest root b-tree page (4 bytes big-endian).
	// Only used for auto-vacuum and incremental-vacuum modes.
	OffsetLargestRootPage = 52

	// OffsetTextEncoding is the database text encoding (4 bytes big-endian).
	// 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be.
	OffsetTextEncoding = 56

	// OffsetUserVersion is the user version (4 bytes big-endian).
	// Set by user via PRAGMA user_version.
	OffsetUserVersion = 60

	// OffsetIncrVacuum is the incremental vacuum mode (4 bytes big-endian).
	// 0 = disabled, non-zero = enabled.
	OffsetIncrVacuum = 64

	// OffsetAppID is the application ID (4 bytes big-endian).
	// Set by user via PRAGMA application_id.
	OffsetAppID = 68

	// OffsetReserved is the reserved space (20 bytes, must be zero).
	OffsetReserved = 72

	// OffsetVersionValidFor is the version-valid-for number (4 bytes big-endian).
	OffsetVersionValidFor = 92

	// OffsetSQLiteVersion is the SQLite version number (4 bytes big-endian).
	OffsetSQLiteVersion = 96
)

// Text encodings - values for the OffsetTextEncoding field
const (
	// EncodingUTF8 indicates UTF-8 text encoding.
	EncodingUTF8 = 1

	// EncodingUTF16LE indicates UTF-16 little-endian text encoding.
	EncodingUTF16LE = 2

	// EncodingUTF16BE indicates UTF-16 big-endian text encoding.
	EncodingUTF16BE = 3
)

// Page types - first byte of B-tree page header
const (
	// PageTypeInteriorIndex is an interior index b-tree page (0x02).
	PageTypeInteriorIndex = 0x02

	// PageTypeInteriorTable is an interior table b-tree page (0x05).
	PageTypeInteriorTable = 0x05

	// PageTypeLeafIndex is a leaf index b-tree page (0x0a).
	PageTypeLeafIndex = 0x0a

	// PageTypeLeafTable is a leaf table b-tree page (0x0d).
	PageTypeLeafTable = 0x0d
)

// B-tree page header offsets
const (
	// BtreePageType is the page type (1 byte).
	BtreePageType = 0

	// BtreeFirstFreeblock is the first freeblock offset (2 bytes big-endian).
	BtreeFirstFreeblock = 1

	// BtreeCellCount is the number of cells (2 bytes big-endian).
	BtreeCellCount = 3

	// BtreeCellContentStart is the start of cell content area (2 bytes big-endian).
	BtreeCellContentStart = 5

	// BtreeFragmentedBytes is the number of fragmented free bytes (1 byte).
	BtreeFragmentedBytes = 7

	// BtreeRightmostPointer is the right-most child pointer (4 bytes big-endian).
	// Only present in interior pages (offset 8).
	BtreeRightmostPointer = 8
)

// B-tree page header sizes
const (
	// BtreeHeaderSizeLeaf is the size of a leaf page header (8 bytes).
	BtreeHeaderSizeLeaf = 8

	// BtreeHeaderSizeInterior is the size of an interior page header (12 bytes).
	// Includes the 4-byte right-most pointer.
	BtreeHeaderSizeInterior = 12
)

// Header represents the 100-byte SQLite database file header.
type Header struct {
	// Magic is the magic header string ("SQLite format 3\x00").
	Magic [16]byte

	// PageSize is the database page size in bytes.
	// Must be a power of 2 between 512 and 65536 inclusive, or 1 representing 65536.
	PageSize uint16

	// WriteVersion is the file format write version (1 or 2).
	WriteVersion uint8

	// ReadVersion is the file format read version (1 or 2).
	ReadVersion uint8

	// ReservedSpace is the number of bytes of unused space at the end of each page.
	ReservedSpace uint8

	// MaxPayloadFrac is the maximum embedded payload fraction (must be 64).
	MaxPayloadFrac uint8

	// MinPayloadFrac is the minimum embedded payload fraction (must be 32).
	MinPayloadFrac uint8

	// LeafPayloadFrac is the leaf payload fraction (must be 32).
	LeafPayloadFrac uint8

	// FileChangeCounter is incremented whenever the database file is modified.
	FileChangeCounter uint32

	// DatabaseSize is the size of the database file in pages.
	DatabaseSize uint32

	// FirstFreelist is the page number of the first freelist trunk page.
	FirstFreelist uint32

	// FreelistCount is the total number of freelist pages.
	FreelistCount uint32

	// SchemaCookie is incremented whenever the database schema changes.
	SchemaCookie uint32

	// SchemaFormat is the schema format number (1, 2, 3, or 4).
	SchemaFormat uint32

	// DefaultCacheSize is the suggested cache size in pages.
	DefaultCacheSize uint32

	// LargestRootPage is the largest root b-tree page number (for auto-vacuum).
	LargestRootPage uint32

	// TextEncoding is the database text encoding (1=UTF-8, 2=UTF-16le, 3=UTF-16be).
	TextEncoding uint32

	// UserVersion is a user-defined version number.
	UserVersion uint32

	// IncrVacuum is non-zero if incremental vacuum is enabled.
	IncrVacuum uint32

	// AppID is a user-defined application ID.
	AppID uint32

	// Reserved is 20 bytes of reserved space (must be zero).
	Reserved [20]byte

	// VersionValidFor is the version-valid-for number.
	VersionValidFor uint32

	// SQLiteVersion is the SQLite version number that wrote the database.
	SQLiteVersion uint32
}

// Parse parses the 100-byte database header from raw bytes.
func (h *Header) Parse(data []byte) error {
	if len(data) < HeaderSize {
		return fmt.Errorf("invalid header size: got %d, want %d", len(data), HeaderSize)
	}

	// Parse magic header
	copy(h.Magic[:], data[OffsetMagic:OffsetMagic+16])
	if string(h.Magic[:]) != MagicString {
		return fmt.Errorf("invalid magic header: got %q, want %q", h.Magic, MagicString)
	}

	// Parse page size
	pageSizeRaw := binary.BigEndian.Uint16(data[OffsetPageSize : OffsetPageSize+2])
	if pageSizeRaw == 1 {
		h.PageSize = 1 // Store as 1, GetPageSize() will return 65536
	} else {
		h.PageSize = pageSizeRaw
	}

	// Validate page size (use GetPageSize() to handle the special case)
	actualPageSize := h.GetPageSize()
	if !IsValidPageSize(actualPageSize) {
		return fmt.Errorf("invalid page size: %d", actualPageSize)
	}

	// Parse single-byte fields
	h.WriteVersion = data[OffsetWriteVersion]
	h.ReadVersion = data[OffsetReadVersion]
	h.ReservedSpace = data[OffsetReservedSpace]
	h.MaxPayloadFrac = data[OffsetMaxPayloadFrac]
	h.MinPayloadFrac = data[OffsetMinPayloadFrac]
	h.LeafPayloadFrac = data[OffsetLeafPayloadFrac]

	// Parse 32-bit fields
	h.FileChangeCounter = binary.BigEndian.Uint32(data[OffsetFileChangeCounter : OffsetFileChangeCounter+4])
	h.DatabaseSize = binary.BigEndian.Uint32(data[OffsetDatabaseSize : OffsetDatabaseSize+4])
	h.FirstFreelist = binary.BigEndian.Uint32(data[OffsetFirstFreelist : OffsetFirstFreelist+4])
	h.FreelistCount = binary.BigEndian.Uint32(data[OffsetFreelistCount : OffsetFreelistCount+4])
	h.SchemaCookie = binary.BigEndian.Uint32(data[OffsetSchemaCookie : OffsetSchemaCookie+4])
	h.SchemaFormat = binary.BigEndian.Uint32(data[OffsetSchemaFormat : OffsetSchemaFormat+4])
	h.DefaultCacheSize = binary.BigEndian.Uint32(data[OffsetDefaultCacheSize : OffsetDefaultCacheSize+4])
	h.LargestRootPage = binary.BigEndian.Uint32(data[OffsetLargestRootPage : OffsetLargestRootPage+4])
	h.TextEncoding = binary.BigEndian.Uint32(data[OffsetTextEncoding : OffsetTextEncoding+4])
	h.UserVersion = binary.BigEndian.Uint32(data[OffsetUserVersion : OffsetUserVersion+4])
	h.IncrVacuum = binary.BigEndian.Uint32(data[OffsetIncrVacuum : OffsetIncrVacuum+4])
	h.AppID = binary.BigEndian.Uint32(data[OffsetAppID : OffsetAppID+4])
	h.VersionValidFor = binary.BigEndian.Uint32(data[OffsetVersionValidFor : OffsetVersionValidFor+4])
	h.SQLiteVersion = binary.BigEndian.Uint32(data[OffsetSQLiteVersion : OffsetSQLiteVersion+4])

	// Parse reserved space
	copy(h.Reserved[:], data[OffsetReserved:OffsetReserved+20])

	return nil
}

// Serialize serializes the database header to 100 bytes.
func (h *Header) Serialize() []byte {
	data := make([]byte, HeaderSize)

	// Magic header
	copy(data[OffsetMagic:], h.Magic[:])

	// Page size (already stored as 1 if it represents 65536)
	binary.BigEndian.PutUint16(data[OffsetPageSize:], h.PageSize)

	// Single-byte fields
	data[OffsetWriteVersion] = h.WriteVersion
	data[OffsetReadVersion] = h.ReadVersion
	data[OffsetReservedSpace] = h.ReservedSpace
	data[OffsetMaxPayloadFrac] = h.MaxPayloadFrac
	data[OffsetMinPayloadFrac] = h.MinPayloadFrac
	data[OffsetLeafPayloadFrac] = h.LeafPayloadFrac

	// 32-bit fields
	binary.BigEndian.PutUint32(data[OffsetFileChangeCounter:], h.FileChangeCounter)
	binary.BigEndian.PutUint32(data[OffsetDatabaseSize:], h.DatabaseSize)
	binary.BigEndian.PutUint32(data[OffsetFirstFreelist:], h.FirstFreelist)
	binary.BigEndian.PutUint32(data[OffsetFreelistCount:], h.FreelistCount)
	binary.BigEndian.PutUint32(data[OffsetSchemaCookie:], h.SchemaCookie)
	binary.BigEndian.PutUint32(data[OffsetSchemaFormat:], h.SchemaFormat)
	binary.BigEndian.PutUint32(data[OffsetDefaultCacheSize:], h.DefaultCacheSize)
	binary.BigEndian.PutUint32(data[OffsetLargestRootPage:], h.LargestRootPage)
	binary.BigEndian.PutUint32(data[OffsetTextEncoding:], h.TextEncoding)
	binary.BigEndian.PutUint32(data[OffsetUserVersion:], h.UserVersion)
	binary.BigEndian.PutUint32(data[OffsetIncrVacuum:], h.IncrVacuum)
	binary.BigEndian.PutUint32(data[OffsetAppID:], h.AppID)
	binary.BigEndian.PutUint32(data[OffsetVersionValidFor:], h.VersionValidFor)
	binary.BigEndian.PutUint32(data[OffsetSQLiteVersion:], h.SQLiteVersion)

	// Reserved space
	copy(data[OffsetReserved:], h.Reserved[:])

	return data
}

// NewHeader creates a new database header with default values.
func NewHeader(pageSize int) *Header {
	// Handle special case where 65536 is stored as 1
	var pageSizeVal uint16
	if pageSize == MaxPageSize {
		pageSizeVal = 1
	} else {
		pageSizeVal = uint16(pageSize)
	}

	h := &Header{
		PageSize:        pageSizeVal,
		WriteVersion:    1,
		ReadVersion:     1,
		ReservedSpace:   0,
		MaxPayloadFrac:  64,
		MinPayloadFrac:  32,
		LeafPayloadFrac: 32,
		SchemaFormat:    4,
		TextEncoding:    EncodingUTF8,
		SQLiteVersion:   3051020, // SQLite version 3.51.2
	}

	copy(h.Magic[:], MagicString)

	return h
}

func (h *Header) validateVersions() error {
	validVersions := map[uint8]bool{1: true, 2: true}
	if !validVersions[h.WriteVersion] {
		return fmt.Errorf("invalid write version: %d", h.WriteVersion)
	}
	if !validVersions[h.ReadVersion] {
		return fmt.Errorf("invalid read version: %d", h.ReadVersion)
	}
	return nil
}

func (h *Header) validatePayloadFractions() error {
	type fracCheck struct {
		val      uint8
		expected uint8
		name     string
	}
	checks := []fracCheck{
		{h.MaxPayloadFrac, 64, "max payload fraction"},
		{h.MinPayloadFrac, 32, "min payload fraction"},
		{h.LeafPayloadFrac, 32, "leaf payload fraction"},
	}
	for _, c := range checks {
		if c.val != c.expected {
			return fmt.Errorf("invalid %s: %d", c.name, c.val)
		}
	}
	return nil
}

func (h *Header) validateSchemaAndEncoding() error {
	if h.SchemaFormat < 1 || h.SchemaFormat > 4 {
		return fmt.Errorf("invalid schema format: %d", h.SchemaFormat)
	}
	if h.TextEncoding < EncodingUTF8 || h.TextEncoding > EncodingUTF16BE {
		return fmt.Errorf("invalid text encoding: %d", h.TextEncoding)
	}
	return nil
}

func (h *Header) Validate() error {
	if string(h.Magic[:]) != MagicString {
		return fmt.Errorf("invalid magic header")
	}
	if pageSize := h.GetPageSize(); !IsValidPageSize(pageSize) {
		return fmt.Errorf("invalid page size: %d", pageSize)
	}
	if err := h.validateVersions(); err != nil {
		return err
	}
	if err := h.validatePayloadFractions(); err != nil {
		return err
	}
	return h.validateSchemaAndEncoding()
}

// GetPageSize returns the actual page size, handling the special case where
// a stored value of 1 means 65536.
func (h *Header) GetPageSize() int {
	if h.PageSize == 1 {
		return MaxPageSize
	}
	return int(h.PageSize)
}

// IsValidPageSize checks if a page size is valid.
// Valid page sizes are powers of 2 between 512 and 65536 inclusive.
func IsValidPageSize(size int) bool {
	if size < MinPageSize || size > MaxPageSize {
		return false
	}

	// Check if it's a power of 2
	return size&(size-1) == 0
}
