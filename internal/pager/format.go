// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package pager implements SQLite database file format parsing and page management.
//
// The pager is responsible for reading and writing pages from/to the database file,
// managing the page cache, and providing atomic commit/rollback through journaling.
package pager

import (
	"encoding/binary"
	"fmt"
)

// File format constants
const (
	// DatabaseHeaderSize is the size of the database file header (first 100 bytes).
	DatabaseHeaderSize = 100

	// DefaultPageSize is the default page size for new databases.
	DefaultPageSize = 4096

	// MinPageSize is the minimum allowed page size (512 bytes).
	MinPageSize = 512

	// MaxPageSize is the maximum allowed page size (65536 bytes).
	MaxPageSize = 65536

	// MagicHeaderString is the magic header string for SQLite 3 database files.
	// Must be exactly 16 bytes including the null terminator.
	MagicHeaderString = "SQLite format 3\x00"

	// MaxSectorSize is the maximum allowed sector size (64KiB).
	MaxSectorSize = 0x10000
)

// Database header byte offsets
const (
	// OffsetMagic is the offset of the magic header string (16 bytes).
	OffsetMagic = 0

	// OffsetPageSize is the offset of the page size field (2 bytes, big-endian).
	// The page size is stored as a 16-bit big-endian integer at offset 16.
	// A value of 1 represents 65536 bytes.
	OffsetPageSize = 16

	// OffsetFileFormatWrite is the file format write version (1 byte).
	OffsetFileFormatWrite = 18

	// OffsetFileFormatRead is the file format read version (1 byte).
	OffsetFileFormatRead = 19

	// OffsetReservedSpace is the reserved space at end of each page (1 byte).
	OffsetReservedSpace = 20

	// OffsetMaxPayloadFrac is the maximum embedded payload fraction (1 byte).
	OffsetMaxPayloadFrac = 21

	// OffsetMinPayloadFrac is the minimum embedded payload fraction (1 byte).
	OffsetMinPayloadFrac = 22

	// OffsetLeafPayloadFrac is the leaf payload fraction (1 byte).
	OffsetLeafPayloadFrac = 23

	// OffsetFileChangeCounter is the file change counter (4 bytes, big-endian).
	// Incremented whenever the database is modified.
	OffsetFileChangeCounter = 24

	// OffsetDatabaseSize is the database size in pages (4 bytes, big-endian).
	OffsetDatabaseSize = 28

	// OffsetFreelistTrunk is the first freelist trunk page (4 bytes, big-endian).
	OffsetFreelistTrunk = 32

	// OffsetFreelistCount is the total number of freelist pages (4 bytes, big-endian).
	OffsetFreelistCount = 36

	// OffsetSchemaCookie is the schema cookie (4 bytes, big-endian).
	// Incremented whenever the schema changes.
	OffsetSchemaCookie = 40

	// OffsetSchemaFormat is the schema format number (4 bytes, big-endian).
	// Supported values: 1, 2, 3, 4.
	OffsetSchemaFormat = 44

	// OffsetDefaultCacheSize is the default page cache size (4 bytes, big-endian).
	OffsetDefaultCacheSize = 48

	// OffsetLargestRootPage is the largest root b-tree page (4 bytes, big-endian).
	// Only used for auto-vacuum and incremental-vacuum modes.
	OffsetLargestRootPage = 52

	// OffsetTextEncoding is the database text encoding (4 bytes, big-endian).
	// 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be
	OffsetTextEncoding = 56

	// OffsetUserVersion is the user version (4 bytes, big-endian).
	// Set by user via PRAGMA user_version.
	OffsetUserVersion = 60

	// OffsetIncrementalVacuum is the incremental vacuum mode (4 bytes, big-endian).
	// 0 = disabled, non-zero = enabled.
	OffsetIncrementalVacuum = 64

	// OffsetApplicationID is the application ID (4 bytes, big-endian).
	// Set by user via PRAGMA application_id.
	OffsetApplicationID = 68

	// OffsetReserved is the reserved space (20 bytes, must be zero).
	OffsetReserved = 72

	// OffsetVersionValidFor is the version-valid-for number (4 bytes, big-endian).
	OffsetVersionValidFor = 92

	// OffsetSQLiteVersion is the SQLite version number (4 bytes, big-endian).
	OffsetSQLiteVersion = 96
)

// Text encoding values
const (
	// EncodingUTF8 indicates UTF-8 text encoding.
	EncodingUTF8 = 1

	// EncodingUTF16LE indicates UTF-16 little-endian text encoding.
	EncodingUTF16LE = 2

	// EncodingUTF16BE indicates UTF-16 big-endian text encoding.
	EncodingUTF16BE = 3
)

// DatabaseHeader represents the 100-byte header at the beginning of every SQLite database file.
type DatabaseHeader struct {
	// Magic is the magic header string ("SQLite format 3\x00")
	Magic [16]byte

	// PageSize is the database page size in bytes.
	// Must be a power of 2 between 512 and 65536 inclusive, or 1 representing 65536.
	PageSize uint16

	// FileFormatWrite is the file format write version (1 or 2).
	FileFormatWrite uint8

	// FileFormatRead is the file format read version (1 or 2).
	FileFormatRead uint8

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

	// FreelistTrunk is the page number of the first freelist trunk page.
	FreelistTrunk uint32

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

	// IncrementalVacuum is non-zero if incremental vacuum is enabled.
	IncrementalVacuum uint32

	// ApplicationID is a user-defined application ID.
	ApplicationID uint32

	// Reserved is 20 bytes of reserved space (must be zero).
	Reserved [20]byte

	// VersionValidFor is the version-valid-for number.
	VersionValidFor uint32

	// SQLiteVersion is the SQLite version number that wrote the database.
	SQLiteVersion uint32
}

// ParseDatabaseHeader parses the 100-byte database header from raw bytes.
func ParseDatabaseHeader(data []byte) (*DatabaseHeader, error) {
	if len(data) < DatabaseHeaderSize {
		return nil, fmt.Errorf("invalid header size: got %d, want %d", len(data), DatabaseHeaderSize)
	}

	header := &DatabaseHeader{}

	// Parse magic header
	copy(header.Magic[:], data[OffsetMagic:OffsetMagic+16])
	if string(header.Magic[:]) != MagicHeaderString {
		return nil, fmt.Errorf("invalid magic header: got %q, want %q", header.Magic, MagicHeaderString)
	}

	// Parse page size
	pageSizeRaw := binary.BigEndian.Uint16(data[OffsetPageSize : OffsetPageSize+2])
	if pageSizeRaw == 1 {
		// Special case: 1 represents 65536
		header.PageSize = 1 // Store as 1, GetPageSize() will return 65536
	} else {
		header.PageSize = pageSizeRaw
	}

	// Validate page size
	if !isValidPageSize(int(header.PageSize)) {
		return nil, fmt.Errorf("invalid page size: %d", header.PageSize)
	}

	// Parse single-byte fields
	header.FileFormatWrite = data[OffsetFileFormatWrite]
	header.FileFormatRead = data[OffsetFileFormatRead]
	header.ReservedSpace = data[OffsetReservedSpace]
	header.MaxPayloadFrac = data[OffsetMaxPayloadFrac]
	header.MinPayloadFrac = data[OffsetMinPayloadFrac]
	header.LeafPayloadFrac = data[OffsetLeafPayloadFrac]

	// Parse 32-bit fields
	header.FileChangeCounter = binary.BigEndian.Uint32(data[OffsetFileChangeCounter : OffsetFileChangeCounter+4])
	header.DatabaseSize = binary.BigEndian.Uint32(data[OffsetDatabaseSize : OffsetDatabaseSize+4])
	header.FreelistTrunk = binary.BigEndian.Uint32(data[OffsetFreelistTrunk : OffsetFreelistTrunk+4])
	header.FreelistCount = binary.BigEndian.Uint32(data[OffsetFreelistCount : OffsetFreelistCount+4])
	header.SchemaCookie = binary.BigEndian.Uint32(data[OffsetSchemaCookie : OffsetSchemaCookie+4])
	header.SchemaFormat = binary.BigEndian.Uint32(data[OffsetSchemaFormat : OffsetSchemaFormat+4])
	header.DefaultCacheSize = binary.BigEndian.Uint32(data[OffsetDefaultCacheSize : OffsetDefaultCacheSize+4])
	header.LargestRootPage = binary.BigEndian.Uint32(data[OffsetLargestRootPage : OffsetLargestRootPage+4])
	header.TextEncoding = binary.BigEndian.Uint32(data[OffsetTextEncoding : OffsetTextEncoding+4])
	header.UserVersion = binary.BigEndian.Uint32(data[OffsetUserVersion : OffsetUserVersion+4])
	header.IncrementalVacuum = binary.BigEndian.Uint32(data[OffsetIncrementalVacuum : OffsetIncrementalVacuum+4])
	header.ApplicationID = binary.BigEndian.Uint32(data[OffsetApplicationID : OffsetApplicationID+4])
	header.VersionValidFor = binary.BigEndian.Uint32(data[OffsetVersionValidFor : OffsetVersionValidFor+4])
	header.SQLiteVersion = binary.BigEndian.Uint32(data[OffsetSQLiteVersion : OffsetSQLiteVersion+4])

	// Parse reserved space
	copy(header.Reserved[:], data[OffsetReserved:OffsetReserved+20])

	return header, nil
}

// Serialize serializes the database header to 100 bytes.
func (h *DatabaseHeader) Serialize() []byte {
	data := make([]byte, DatabaseHeaderSize)

	// Magic header
	copy(data[OffsetMagic:], h.Magic[:])

	// Page size (special case: 65536 is stored as 1)
	pageSizeVal := h.PageSize
	if h.GetPageSize() == MaxPageSize {
		pageSizeVal = 1
	}
	binary.BigEndian.PutUint16(data[OffsetPageSize:], pageSizeVal)

	// Single-byte fields
	data[OffsetFileFormatWrite] = h.FileFormatWrite
	data[OffsetFileFormatRead] = h.FileFormatRead
	data[OffsetReservedSpace] = h.ReservedSpace
	data[OffsetMaxPayloadFrac] = h.MaxPayloadFrac
	data[OffsetMinPayloadFrac] = h.MinPayloadFrac
	data[OffsetLeafPayloadFrac] = h.LeafPayloadFrac

	// 32-bit fields
	binary.BigEndian.PutUint32(data[OffsetFileChangeCounter:], h.FileChangeCounter)
	binary.BigEndian.PutUint32(data[OffsetDatabaseSize:], h.DatabaseSize)
	binary.BigEndian.PutUint32(data[OffsetFreelistTrunk:], h.FreelistTrunk)
	binary.BigEndian.PutUint32(data[OffsetFreelistCount:], h.FreelistCount)
	binary.BigEndian.PutUint32(data[OffsetSchemaCookie:], h.SchemaCookie)
	binary.BigEndian.PutUint32(data[OffsetSchemaFormat:], h.SchemaFormat)
	binary.BigEndian.PutUint32(data[OffsetDefaultCacheSize:], h.DefaultCacheSize)
	binary.BigEndian.PutUint32(data[OffsetLargestRootPage:], h.LargestRootPage)
	binary.BigEndian.PutUint32(data[OffsetTextEncoding:], h.TextEncoding)
	binary.BigEndian.PutUint32(data[OffsetUserVersion:], h.UserVersion)
	binary.BigEndian.PutUint32(data[OffsetIncrementalVacuum:], h.IncrementalVacuum)
	binary.BigEndian.PutUint32(data[OffsetApplicationID:], h.ApplicationID)
	binary.BigEndian.PutUint32(data[OffsetVersionValidFor:], h.VersionValidFor)
	binary.BigEndian.PutUint32(data[OffsetSQLiteVersion:], h.SQLiteVersion)

	// Reserved space
	copy(data[OffsetReserved:], h.Reserved[:])

	return data
}

// NewDatabaseHeader creates a new database header with default values.
func NewDatabaseHeader(pageSize int) *DatabaseHeader {
	// SQLite stores page size 65536 as 1 (since 65536 doesn't fit in uint16)
	storedPageSize := uint16(pageSize)
	if pageSize == MaxPageSize {
		storedPageSize = 1
	}

	header := &DatabaseHeader{
		PageSize:        storedPageSize,
		FileFormatWrite: 1,
		FileFormatRead:  1,
		ReservedSpace:   0,
		MaxPayloadFrac:  64,
		MinPayloadFrac:  32,
		LeafPayloadFrac: 32,
		SchemaFormat:    4,
		TextEncoding:    EncodingUTF8,
		SQLiteVersion:   3051020, // SQLite version 3.51.2
	}

	copy(header.Magic[:], MagicHeaderString)

	return header
}

// isValidPageSize checks if a page size is valid.
// Valid page sizes are powers of 2 between 512 and 65536 inclusive.
// The special value 1 is also valid, representing 65536 (per SQLite file format).
func isValidPageSize(size int) bool {
	// Special case: 1 represents 65536 in SQLite
	if size == 1 {
		return true
	}

	if size < MinPageSize || size > MaxPageSize {
		return false
	}

	// Check if it's a power of 2
	return size&(size-1) == 0
}

// GetPageSize returns the actual page size, handling the special case where
// a stored value of 1 means 65536.
func (h *DatabaseHeader) GetPageSize() int {
	if h.PageSize == 1 {
		return MaxPageSize
	}
	return int(h.PageSize)
}

var validFileFormatVersions = map[uint8]bool{1: true, 2: true}

func (h *DatabaseHeader) validateMagicAndPageSize() error {
	if string(h.Magic[:]) != MagicHeaderString {
		return fmt.Errorf("invalid magic header")
	}
	pageSize := h.GetPageSize()
	if !isValidPageSize(pageSize) {
		return fmt.Errorf("invalid page size: %d", pageSize)
	}
	return nil
}

func (h *DatabaseHeader) validateFileFormats() error {
	if !validFileFormatVersions[h.FileFormatWrite] {
		return fmt.Errorf("invalid file format write version: %d", h.FileFormatWrite)
	}
	if !validFileFormatVersions[h.FileFormatRead] {
		return fmt.Errorf("invalid file format read version: %d", h.FileFormatRead)
	}
	return nil
}

func (h *DatabaseHeader) validatePayloadFractions() error {
	fractions := []struct {
		val      uint8
		expected uint8
		name     string
	}{
		{h.MaxPayloadFrac, 64, "max payload fraction"},
		{h.MinPayloadFrac, 32, "min payload fraction"},
		{h.LeafPayloadFrac, 32, "leaf payload fraction"},
	}
	for _, f := range fractions {
		if f.val != f.expected {
			return fmt.Errorf("invalid %s: %d", f.name, f.val)
		}
	}
	return nil
}

func (h *DatabaseHeader) validateSchemaAndEncoding() error {
	if h.SchemaFormat < 1 || h.SchemaFormat > 4 {
		return fmt.Errorf("invalid schema format: %d", h.SchemaFormat)
	}
	if h.TextEncoding < EncodingUTF8 || h.TextEncoding > EncodingUTF16BE {
		return fmt.Errorf("invalid text encoding: %d", h.TextEncoding)
	}
	return nil
}

func (h *DatabaseHeader) Validate() error {
	if err := h.validateMagicAndPageSize(); err != nil {
		return err
	}
	if err := h.validateFileFormats(); err != nil {
		return err
	}
	if err := h.validatePayloadFractions(); err != nil {
		return err
	}
	return h.validateSchemaAndEncoding()
}
