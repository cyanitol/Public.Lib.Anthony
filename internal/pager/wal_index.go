package pager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// WAL index constants
const (
	// WALIndexHeaderSize is the size of the WAL index header
	WALIndexHeaderSize = 136

	// WALIndexMaxReaders is the maximum number of concurrent readers
	WALIndexMaxReaders = 5

	// WALIndexHashTableSize is the number of hash table slots
	// SQLite uses 4096 slots per hash table region
	WALIndexHashTableSize = 4096

	// WALIndexMagic is the magic number for WAL index files
	WALIndexMagic = 0x377f0682

	// WALIndexVersion is the version number for the WAL index format
	WALIndexVersion = 3007000

	// WALIndexHashSlotSize is the size of each hash slot (8 bytes: 4 for pgno, 4 for frame)
	WALIndexHashSlotSize = 8
)

// Common errors
var (
	ErrWALIndexCorrupt = errors.New("WAL index is corrupt")
	ErrWALIndexLocked  = errors.New("WAL index is locked")
	ErrInvalidReader   = errors.New("invalid reader ID")
	ErrFrameNotFound   = errors.New("frame not found in WAL index")
)

// WALIndexHeader represents the header of the WAL index (shared memory).
// This structure is based on the wal-index header in SQLite.
type WALIndexHeader struct {
	// Version number - used to detect format changes
	Version uint32

	// Unused padding
	Unused uint32

	// Change counter - incremented on each write
	Change uint32

	// isInit flag - 1 if initialized, 0 otherwise
	IsInit uint8

	// bigEndCksum flag - 1 if checksums are big-endian
	BigEndCksum uint8

	// Page size in bytes
	PageSize uint16

	// mxFrame - maximum frame number in the log
	MxFrame uint32

	// nPage - number of pages in the database
	NPage uint32

	// aFrameCksum - checksum of frames
	AFrameCksum [2]uint32

	// aSalt - salt values for checksum
	ASalt [2]uint32

	// aCksum - checksum of this header
	ACksum [2]uint32

	// Read marks - mark the end of frames that each reader has consumed
	// Index 0 is unused, indices 1-5 are for readers
	ReadMark [WALIndexMaxReaders]uint32

	// Mutex/lock for header access
	mu sync.RWMutex
}

// WALHashSlot represents a single slot in the hash table
type WALHashSlot struct {
	PageNum  uint32 // Page number
	FrameNum uint32 // Frame number in WAL file
}

// WALIndex manages the WAL index (wal-index or shm file).
// The WAL index provides fast lookup of pages in the WAL file using a hash table.
// It's stored in shared memory to allow concurrent access by multiple processes.
type WALIndex struct {
	// File handle for the WAL index file (.db-shm)
	file *os.File

	// Filename of the WAL index
	filename string

	// Memory-mapped region (shared memory)
	mmap []byte

	// Header of the WAL index
	header *WALIndexHeader

	// Hash table for page number to frame number mapping
	// In a full implementation, this would be backed by the mmap region
	hashTable map[uint32]uint32

	// Page size of the database
	pageSize int

	// Whether the index has been initialized
	initialized bool

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewWALIndex creates or opens a WAL index file.
// The filename should be the database filename (the .db-shm extension will be added).
func NewWALIndex(filename string) (*WALIndex, error) {
	shmFilename := filename + "-shm"

	idx := &WALIndex{
		filename:  shmFilename,
		hashTable: make(map[uint32]uint32),
	}

	if err := idx.open(); err != nil {
		return nil, err
	}

	return idx, nil
}

// open opens or creates the WAL index file.
func (w *WALIndex) open() error {
	var err error

	// Open or create the WAL index file
	w.file, err = os.OpenFile(
		w.filename,
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open WAL index file: %w", err)
	}

	// Get file size
	info, err := w.file.Stat()
	if err != nil {
		w.file.Close()
		return fmt.Errorf("failed to stat WAL index file: %w", err)
	}

	// Calculate minimum size for WAL index (header + one hash table)
	minSize := int64(WALIndexHeaderSize + WALIndexHashTableSize*WALIndexHashSlotSize)

	// If file is too small or empty, initialize it
	if info.Size() < minSize {
		if err := w.initializeFile(minSize); err != nil {
			w.file.Close()
			return err
		}
	}

	// Memory-map the file
	if err := w.mmapFile(); err != nil {
		w.file.Close()
		return err
	}

	// Read the header
	if err := w.readHeader(); err != nil {
		w.Close()
		return err
	}

	w.initialized = true
	return nil
}

// initializeFile initializes a new WAL index file.
func (w *WALIndex) initializeFile(size int64) error {
	// Truncate to the desired size
	if err := w.file.Truncate(size); err != nil {
		return fmt.Errorf("failed to truncate WAL index file: %w", err)
	}

	// Create a new header
	w.header = &WALIndexHeader{
		Version:     WALIndexVersion,
		IsInit:      1,
		BigEndCksum: 0, // Use little-endian checksums
	}

	// Write the header directly to the file (mmap not set up yet)
	if err := w.writeHeaderToFile(); err != nil {
		return err
	}

	return nil
}

// writeHeaderToFile writes the header directly to the file (before mmap is established).
func (w *WALIndex) writeHeaderToFile() error {
	buf := make([]byte, WALIndexHeaderSize)
	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.Version)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.Unused)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.Change)
	offset += 4

	buf[offset] = w.header.IsInit
	offset++

	buf[offset] = w.header.BigEndCksum
	offset++

	binary.LittleEndian.PutUint16(buf[offset:offset+2], w.header.PageSize)
	offset += 2

	binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.MxFrame)
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.NPage)
	offset += 4

	// Write frame checksums
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.AFrameCksum[i])
		offset += 4
	}

	// Write salt values
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.ASalt[i])
		offset += 4
	}

	// Write checksums
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.ACksum[i])
		offset += 4
	}

	// Write read marks
	for i := 0; i < WALIndexMaxReaders; i++ {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], w.header.ReadMark[i])
		offset += 4
	}

	// Write to file at position 0
	_, err := w.file.WriteAt(buf, 0)
	if err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	return w.file.Sync()
}

// mmapFile memory-maps the WAL index file.
// In a production implementation, this would use syscall.Mmap for true shared memory.
// For simplicity, we use a basic file-backed approach.
func (w *WALIndex) mmapFile() error {
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file for mmap: %w", err)
	}

	size := int(info.Size())
	if size == 0 {
		return errors.New("cannot mmap empty file")
	}

	// Use syscall.Mmap for true memory mapping
	mmap, err := syscall.Mmap(
		int(w.file.Fd()),
		0,
		size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap WAL index file: %w", err)
	}

	w.mmap = mmap
	return nil
}

// Close closes the WAL index and releases resources.
func (w *WALIndex) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error

	// Unmap memory
	if w.mmap != nil {
		if err := syscall.Munmap(w.mmap); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to munmap: %w", err)
		}
		w.mmap = nil
	}

	// Close file
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close file: %w", err)
		}
		w.file = nil
	}

	w.initialized = false
	return firstErr
}

// InsertFrame adds a frame to the WAL index.
// This records that the given page number is stored in the given frame number.
func (w *WALIndex) InsertFrame(pgno uint32, frameNo uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return errors.New("WAL index not initialized")
	}

	if pgno == 0 {
		return ErrInvalidPageNum
	}

	// Update the hash table (in-memory for now)
	w.hashTable[pgno] = frameNo

	// Update header's max frame number
	w.header.mu.Lock()
	if frameNo > w.header.MxFrame {
		w.header.MxFrame = frameNo
	}
	w.header.Change++
	w.header.mu.Unlock()

	// Write the entry to the memory-mapped region
	if err := w.writeHashEntry(pgno, frameNo); err != nil {
		return err
	}

	return nil
}

// FindFrame finds the most recent frame number for a given page number.
// Returns the frame number or an error if not found.
func (w *WALIndex) FindFrame(pgno uint32) (uint32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.initialized {
		return 0, errors.New("WAL index not initialized")
	}

	if pgno == 0 {
		return 0, ErrInvalidPageNum
	}

	// Look up in hash table
	frameNo, ok := w.hashTable[pgno]
	if !ok {
		return 0, ErrFrameNotFound
	}

	return frameNo, nil
}

// SetReadMark sets the read mark for a given reader.
// The read mark indicates the last frame that the reader has consumed.
// Reader IDs are 0-4 (0 is reserved, 1-4 are for actual readers).
func (w *WALIndex) SetReadMark(reader int, frame uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return errors.New("WAL index not initialized")
	}

	if reader < 0 || reader >= WALIndexMaxReaders {
		return ErrInvalidReader
	}

	w.header.mu.Lock()
	w.header.ReadMark[reader] = frame
	w.header.Change++
	w.header.mu.Unlock()

	// Write the updated header to the mmap region
	return w.writeHeader()
}

// GetReadMark gets the read mark for a given reader.
func (w *WALIndex) GetReadMark(reader int) (uint32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.initialized {
		return 0, errors.New("WAL index not initialized")
	}

	if reader < 0 || reader >= WALIndexMaxReaders {
		return 0, ErrInvalidReader
	}

	w.header.mu.RLock()
	mark := w.header.ReadMark[reader]
	w.header.mu.RUnlock()

	return mark, nil
}

// GetMaxFrame returns the maximum frame number in the WAL.
func (w *WALIndex) GetMaxFrame() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.initialized || w.header == nil {
		return 0
	}

	w.header.mu.RLock()
	defer w.header.mu.RUnlock()
	return w.header.MxFrame
}

// GetPageCount returns the number of pages in the database.
func (w *WALIndex) GetPageCount() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.initialized || w.header == nil {
		return 0
	}

	w.header.mu.RLock()
	defer w.header.mu.RUnlock()
	return w.header.NPage
}

// SetPageCount sets the number of pages in the database.
func (w *WALIndex) SetPageCount(nPage uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return errors.New("WAL index not initialized")
	}

	w.header.mu.Lock()
	w.header.NPage = nPage
	w.header.Change++
	w.header.mu.Unlock()

	return w.writeHeader()
}

// Clear clears all entries from the WAL index.
// This is called when the WAL is checkpointed and reset.
func (w *WALIndex) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return errors.New("WAL index not initialized")
	}

	// Clear the hash table
	w.hashTable = make(map[uint32]uint32)

	// Reset header
	w.header.mu.Lock()
	w.header.MxFrame = 0
	w.header.Change++
	for i := range w.header.ReadMark {
		w.header.ReadMark[i] = 0
	}
	w.header.mu.Unlock()

	// Clear the mmap region (zero out the hash table)
	if err := w.clearHashTable(); err != nil {
		return err
	}

	return w.writeHeader()
}

// readHeader reads the WAL index header from the memory-mapped region.
func (w *WALIndex) readHeader() error {
	if len(w.mmap) < WALIndexHeaderSize {
		return ErrWALIndexCorrupt
	}

	w.header = &WALIndexHeader{}
	w.readHeaderFields()

	// If not initialized, initialize now
	if w.header.IsInit == 0 {
		return w.initializeHeader()
	}

	// Validate and possibly reinitialize header
	if err := w.validateAndFixHeader(); err != nil {
		return err
	}

	// Load hash table from mmap
	return w.loadHashTable()
}

// readHeaderFields reads all header fields from mmap and returns final offset.
func (w *WALIndex) readHeaderFields() int {
	offset := 0
	w.header.Version = binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
	offset += 4

	w.header.Unused = binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
	offset += 4

	w.header.Change = binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
	offset += 4

	w.header.IsInit = w.mmap[offset]
	offset++

	w.header.BigEndCksum = w.mmap[offset]
	offset++

	w.header.PageSize = binary.LittleEndian.Uint16(w.mmap[offset : offset+2])
	offset += 2

	w.header.MxFrame = binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
	offset += 4

	w.header.NPage = binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
	offset += 4

	offset = readUint32Array(w.mmap, offset, w.header.AFrameCksum[:])
	offset = readUint32Array(w.mmap, offset, w.header.ASalt[:])
	offset = readUint32Array(w.mmap, offset, w.header.ACksum[:])
	offset = readUint32Array(w.mmap, offset, w.header.ReadMark[:])

	return offset
}

// readUint32Array reads multiple uint32 values from mmap at the given offset.
func readUint32Array(mmap []byte, offset int, dest []uint32) int {
	for i := range dest {
		dest[i] = binary.LittleEndian.Uint32(mmap[offset : offset+4])
		offset += 4
	}
	return offset
}

// initializeHeader initializes a new WAL index header.
func (w *WALIndex) initializeHeader() error {
	w.header.Version = WALIndexVersion
	w.header.IsInit = 1
	w.header.BigEndCksum = 0
	return w.writeHeader()
}

// validateAndFixHeader validates the header version and reinitializes if needed.
func (w *WALIndex) validateAndFixHeader() error {
	if w.header.Version != WALIndexVersion && w.header.Version != 0 {
		// Version mismatch - reinitialize
		w.header.Version = WALIndexVersion
		w.header.IsInit = 1
		return w.writeHeader()
	}
	return nil
}

// writeHeader writes the WAL index header to the memory-mapped region.
func (w *WALIndex) writeHeader() error {
	if len(w.mmap) < WALIndexHeaderSize {
		return ErrWALIndexCorrupt
	}

	offset := 0

	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.Version)
	offset += 4

	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.Unused)
	offset += 4

	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.Change)
	offset += 4

	w.mmap[offset] = w.header.IsInit
	offset++

	w.mmap[offset] = w.header.BigEndCksum
	offset++

	binary.LittleEndian.PutUint16(w.mmap[offset:offset+2], w.header.PageSize)
	offset += 2

	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.MxFrame)
	offset += 4

	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.NPage)
	offset += 4

	// Write frame checksums
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.AFrameCksum[i])
		offset += 4
	}

	// Write salt values
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.ASalt[i])
		offset += 4
	}

	// Write checksums
	for i := 0; i < 2; i++ {
		binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.ACksum[i])
		offset += 4
	}

	// Write read marks
	for i := 0; i < WALIndexMaxReaders; i++ {
		binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], w.header.ReadMark[i])
		offset += 4
	}

	// Sync to disk
	if err := w.syncMmap(); err != nil {
		return err
	}

	return nil
}

// writeHashEntry writes a hash table entry to the memory-mapped region.
func (w *WALIndex) writeHashEntry(pgno, frameNo uint32) error {
	if len(w.mmap) < WALIndexHeaderSize {
		return ErrWALIndexCorrupt
	}

	// Calculate hash slot
	hash := w.hashFunction(pgno)

	// Offset in mmap: header + hash_slot_index * slot_size
	offset := WALIndexHeaderSize + int(hash)*WALIndexHashSlotSize

	if offset+WALIndexHashSlotSize > len(w.mmap) {
		return ErrWALIndexCorrupt
	}

	// Write page number and frame number
	binary.LittleEndian.PutUint32(w.mmap[offset:offset+4], pgno)
	binary.LittleEndian.PutUint32(w.mmap[offset+4:offset+8], frameNo)

	return nil
}

// loadHashTable loads the hash table from the memory-mapped region.
func (w *WALIndex) loadHashTable() error {
	if len(w.mmap) < WALIndexHeaderSize {
		return ErrWALIndexCorrupt
	}

	// Clear existing hash table
	w.hashTable = make(map[uint32]uint32)

	// Read all hash slots
	for i := 0; i < WALIndexHashTableSize; i++ {
		offset := WALIndexHeaderSize + i*WALIndexHashSlotSize

		if offset+WALIndexHashSlotSize > len(w.mmap) {
			break
		}

		pgno := binary.LittleEndian.Uint32(w.mmap[offset : offset+4])
		frameNo := binary.LittleEndian.Uint32(w.mmap[offset+4 : offset+8])

		// Only add valid entries (pgno != 0)
		if pgno != 0 {
			w.hashTable[pgno] = frameNo
		}
	}

	return nil
}

// clearHashTable clears all hash table entries in the memory-mapped region.
func (w *WALIndex) clearHashTable() error {
	if len(w.mmap) < WALIndexHeaderSize {
		return ErrWALIndexCorrupt
	}

	// Zero out the hash table region
	hashTableStart := WALIndexHeaderSize
	hashTableSize := WALIndexHashTableSize * WALIndexHashSlotSize
	hashTableEnd := hashTableStart + hashTableSize

	if hashTableEnd > len(w.mmap) {
		hashTableEnd = len(w.mmap)
	}

	for i := hashTableStart; i < hashTableEnd; i++ {
		w.mmap[i] = 0
	}

	return w.syncMmap()
}

// hashFunction computes a hash for a page number.
// This is a simple hash function for distributing pages across slots.
func (w *WALIndex) hashFunction(pgno uint32) uint32 {
	// Simple modulo hash - in production, use a better hash function
	return pgno % WALIndexHashTableSize
}

// syncMmap syncs the memory-mapped region to disk.
func (w *WALIndex) syncMmap() error {
	if w.mmap == nil {
		return errors.New("mmap not initialized")
	}

	// Use msync to flush changes to disk
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&w.mmap[0])),
		uintptr(len(w.mmap)),
		uintptr(syscall.MS_SYNC),
	)

	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}

	return nil
}

// Delete deletes the WAL index file.
func (w *WALIndex) Delete() error {
	// Close first (which takes its own lock)
	if err := w.Close(); err != nil {
		return err
	}

	// Delete the file
	if err := os.Remove(w.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete WAL index file: %w", err)
	}

	return nil
}

// GetChangeCounter returns the change counter from the header.
// The change counter is incremented on each write to detect concurrent modifications.
func (w *WALIndex) GetChangeCounter() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.initialized || w.header == nil {
		return 0
	}

	w.header.mu.RLock()
	defer w.header.mu.RUnlock()
	return w.header.Change
}

// IsInitialized returns true if the WAL index has been initialized.
func (w *WALIndex) IsInitialized() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.initialized
}
