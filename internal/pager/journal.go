// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
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

// Journal header constants
const (
	// JournalHeaderSize is the size of the journal header in bytes
	JournalHeaderSize = 28

	// JournalMagic is the magic number at the start of a journal file
	JournalMagic = 0xd9d505f9

	// JournalFormatVersion is the journal format version
	JournalFormatVersion = 1
)

// Journal represents a rollback journal file.
// The journal stores original page data before modifications,
// allowing transactions to be rolled back.
type Journal struct {
	// File handle for the journal file
	file *os.File

	// Filename of the journal
	filename string

	// Page size of the database
	pageSize int

	// Number of pages written to the journal
	pageCount int

	// Database file size at start of transaction
	dbSize Pgno

	// Random nonce for this journal
	nonce uint32

	// Whether the journal has been initialized
	initialized bool

	// Whether the journal header has been synced
	// SCAFFOLDING: For atomic commit - tracks when header is safely on disk
	headerSynced bool

	// Mutex for thread-safe operations
	mu sync.Mutex
}

// JournalHeader represents the header of a journal file.
type JournalHeader struct {
	Magic         uint32 // Magic number
	PageCount     uint32 // Number of pages in the journal
	Nonce         uint32 // Random nonce
	InitialSize   uint32 // Initial database size in pages
	SectorSize    uint32 // Sector size (for atomic writes)
	PageSize      uint32 // Database page size
	FormatVersion uint32 // Journal format version
}

// NewJournal creates a new journal.
func NewJournal(filename string, pageSize int, dbSize Pgno) *Journal {
	return &Journal{
		filename: filename,
		pageSize: pageSize,
		dbSize:   dbSize,
		nonce:    generateNonce(),
	}
}

// Open opens or creates the journal file.
func (j *Journal) Open() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file != nil {
		return errors.New("journal already open")
	}

	var err error
	j.file, err = os.OpenFile(
		j.filename,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open journal file: %w", err)
	}

	// Write journal header
	if err := j.writeHeader(); err != nil {
		j.file.Close()
		j.file = nil
		return err
	}

	j.initialized = true
	return nil
}

// Close closes the journal file without deleting it.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file == nil {
		return nil
	}

	err := j.file.Close()
	j.file = nil
	return err
}

// WriteOriginal writes the original content of a page to the journal.
// This must be called before modifying the page.
func (j *Journal) WriteOriginal(pageNum uint32, data []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file == nil {
		return errors.New("journal not open")
	}

	if len(data) != j.pageSize {
		return fmt.Errorf("invalid page size: got %d, expected %d", len(data), j.pageSize)
	}

	// Journal entry format:
	// [4 bytes: page number]
	// [pageSize bytes: original page data]
	// [4 bytes: checksum]

	entry := make([]byte, 4+j.pageSize+4)

	// Write page number (big-endian)
	binary.BigEndian.PutUint32(entry[0:4], pageNum)

	// Write page data
	copy(entry[4:4+j.pageSize], data)

	// Calculate and write checksum
	checksum := j.calculateChecksum(pageNum, data)
	binary.BigEndian.PutUint32(entry[4+j.pageSize:], checksum)

	// Write entry to journal
	if _, err := j.file.Write(entry); err != nil {
		return fmt.Errorf("failed to write journal entry: %w", err)
	}

	j.pageCount++

	return nil
}

// Sync syncs the journal file to disk.
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file == nil {
		return errors.New("journal not open")
	}

	return j.file.Sync()
}

// Rollback applies the journal to rollback changes in the pager.
func (j *Journal) Rollback(pager *Pager) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return nil
	}
	if _, err := j.file.Seek(JournalHeaderSize, 0); err != nil {
		return fmt.Errorf("failed to seek in journal: %w", err)
	}
	if err := j.restoreAllEntries(pager); err != nil {
		return err
	}
	if err := pager.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync database after rollback: %w", err)
	}
	return nil
}

// restoreAllEntries reads and restores all journal entries.
func (j *Journal) restoreAllEntries(pager *Pager) error {
	entrySize := 4 + j.pageSize + 4
	for {
		entry := make([]byte, entrySize)
		n, err := j.file.Read(entry)
		if err == io.EOF || n < entrySize {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read journal entry: %w", err)
		}
		if err := j.restoreEntry(pager, entry); err != nil {
			return err
		}
	}
	return nil
}

// restoreEntry restores a single journal entry.
func (j *Journal) restoreEntry(pager *Pager, entry []byte) error {
	pageNum := binary.BigEndian.Uint32(entry[0:4])
	pageData := entry[4 : 4+j.pageSize]
	storedChecksum := binary.BigEndian.Uint32(entry[4+j.pageSize:])
	if storedChecksum != j.calculateChecksum(pageNum, pageData) {
		return fmt.Errorf("journal checksum mismatch for page %d", pageNum)
	}
	offset := int64(pageNum-1) * int64(j.pageSize)
	if _, err := pager.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to restore page %d: %w", pageNum, err)
	}
	return nil
}

// Finalize finalizes the journal after a successful commit.
// This deletes the journal file or truncates it based on journal mode.
func (j *Journal) Finalize() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return err
		}
		j.file = nil
	}

	// Delete the journal file
	if err := os.Remove(j.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete journal file: %w", err)
	}

	j.initialized = false
	j.pageCount = 0

	return nil
}

// Delete deletes the journal file.
func (j *Journal) Delete() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file != nil {
		j.file.Close()
		j.file = nil
	}

	if err := os.Remove(j.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete journal file: %w", err)
	}

	j.initialized = false
	j.pageCount = 0

	return nil
}

// Exists returns true if the journal file exists.
func (j *Journal) Exists() bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	_, err := os.Stat(j.filename)
	return err == nil
}

// IsValid checks if the journal file is valid and can be used for rollback.
func (j *Journal) IsValid() (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.journalFileExists() {
		return false, nil
	}
	cleanup, err := j.ensureFileOpen()
	if err != nil {
		return false, err
	}
	defer cleanup()
	return j.validateHeader()
}

// journalFileExists checks if the journal file exists and has content.
func (j *Journal) journalFileExists() bool {
	info, err := os.Stat(j.filename)
	return err == nil && info.Size() >= JournalHeaderSize
}

// ensureFileOpen opens the journal file if not already open, returning a cleanup function.
func (j *Journal) ensureFileOpen() (func(), error) {
	if j.file != nil {
		return func() {}, nil
	}
	var err error
	j.file, err = os.Open(j.filename)
	if err != nil {
		return nil, err
	}
	return func() {
		j.file.Close()
		j.file = nil
	}, nil
}

// validateHeader reads and validates the journal header.
func (j *Journal) validateHeader() (bool, error) {
	header, err := j.readHeader()
	if err != nil {
		return false, err
	}
	if header.Magic != JournalMagic {
		return false, nil
	}
	if int(header.PageSize) != j.pageSize {
		return false, nil
	}
	if header.FormatVersion != JournalFormatVersion {
		return false, nil
	}
	// Validate that page count is reasonable (not corrupted)
	// PageCount should not exceed file size bounds
	if j.file != nil {
		info, err := j.file.Stat()
		if err == nil {
			entrySize := int64(4 + j.pageSize + 4)
			maxPages := (info.Size() - JournalHeaderSize) / entrySize
			if int64(header.PageCount) > maxPages {
				return false, nil
			}
		}
	}
	return true, nil
}

// writeHeader writes the journal header to the file.
func (j *Journal) writeHeader() error {
	header := JournalHeader{
		Magic:         JournalMagic,
		PageCount:     0, // Will be updated as pages are written
		Nonce:         j.nonce,
		InitialSize:   uint32(j.dbSize),
		SectorSize:    512, // Default sector size
		PageSize:      uint32(j.pageSize),
		FormatVersion: JournalFormatVersion,
	}

	data := make([]byte, JournalHeaderSize)
	binary.BigEndian.PutUint32(data[0:4], header.Magic)
	binary.BigEndian.PutUint32(data[4:8], header.PageCount)
	binary.BigEndian.PutUint32(data[8:12], header.Nonce)
	binary.BigEndian.PutUint32(data[12:16], header.InitialSize)
	binary.BigEndian.PutUint32(data[16:20], header.SectorSize)
	binary.BigEndian.PutUint32(data[20:24], header.PageSize)
	binary.BigEndian.PutUint32(data[24:28], header.FormatVersion)

	if _, err := j.file.WriteAt(data, 0); err != nil {
		return fmt.Errorf("failed to write journal header: %w", err)
	}

	return nil
}

// readHeader reads the journal header from the file.
func (j *Journal) readHeader() (*JournalHeader, error) {
	data := make([]byte, JournalHeaderSize)

	if _, err := j.file.ReadAt(data, 0); err != nil {
		return nil, fmt.Errorf("failed to read journal header: %w", err)
	}

	header := &JournalHeader{
		Magic:         binary.BigEndian.Uint32(data[0:4]),
		PageCount:     binary.BigEndian.Uint32(data[4:8]),
		Nonce:         binary.BigEndian.Uint32(data[8:12]),
		InitialSize:   binary.BigEndian.Uint32(data[12:16]),
		SectorSize:    binary.BigEndian.Uint32(data[16:20]),
		PageSize:      binary.BigEndian.Uint32(data[20:24]),
		FormatVersion: binary.BigEndian.Uint32(data[24:28]),
	}

	return header, nil
}

// updatePageCount updates the page count in the journal header.
func (j *Journal) updatePageCount() error {
	if j.file == nil {
		return errors.New("journal not open")
	}

	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(j.pageCount))

	if _, err := j.file.WriteAt(data, 4); err != nil {
		return fmt.Errorf("failed to update page count: %w", err)
	}

	return nil
}

// calculateChecksum calculates a CRC32 checksum for a journal entry.
// Uses CRC32-C (Castagnoli) polynomial, which is what SQLite uses.
func (j *Journal) calculateChecksum(pageNum uint32, data []byte) uint32 {
	// Use CRC32-C (Castagnoli) table
	table := crc32.MakeTable(crc32.Castagnoli)

	// Create a buffer that includes page number, nonce, and data
	// This ensures the checksum is unique per page and per journal instance
	bufSize := 8 + len(data) // 4 bytes for pageNum + 4 bytes for nonce + data
	buf := make([]byte, bufSize)

	binary.BigEndian.PutUint32(buf[0:4], pageNum)
	binary.BigEndian.PutUint32(buf[4:8], j.nonce)
	copy(buf[8:], data)

	// Calculate CRC32-C checksum
	checksum := crc32.Checksum(buf, table)

	return checksum
}

// GetPageCount returns the number of pages in the journal.
func (j *Journal) GetPageCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.pageCount
}

// IsOpen returns true if the journal file is open.
func (j *Journal) IsOpen() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.file != nil
}

// generateNonce generates a random nonce for the journal.
func generateNonce() uint32 {
	// In a real implementation, this would use crypto/rand
	// For now, use a simple deterministic value
	return 0x12345678
}

// Truncate truncates the journal file to zero length.
// This is used in TRUNCATE journal mode.
func (j *Journal) Truncate() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return err
		}
		j.file = nil
	}

	if err := os.Truncate(j.filename, 0); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to truncate journal file: %w", err)
	}

	j.initialized = false
	j.pageCount = 0

	return nil
}

// ZeroHeader zeros the journal header to invalidate it.
// This is used in PERSIST journal mode.
func (j *Journal) ZeroHeader() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	f, err := os.OpenFile(j.filename, os.O_WRONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open journal for zeroing: %w", err)
	}
	defer f.Close()

	zeros := make([]byte, 4)
	if _, err := f.WriteAt(zeros, 0); err != nil {
		return fmt.Errorf("failed to zero journal header: %w", err)
	}

	return f.Sync()
}
