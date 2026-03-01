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

// WAL constants matching SQLite's WAL format
const (
	// WALHeaderSize is the size of the WAL header (32 bytes)
	WALHeaderSize = 32

	// WALFrameHeaderSize is the size of the frame header (24 bytes)
	WALFrameHeaderSize = 24

	// WALMagic is the magic number for WAL files (0x377f0682 or 0x377f0683)
	// 0x377f0682 = big-endian format
	// 0x377f0683 = little-endian format (we use big-endian)
	WALMagic = 0x377f0682

	// WALFormatVersion is the WAL file format version
	WALFormatVersion = 3007000

	// WALMinCheckpointFrames is the minimum number of frames before checkpoint
	WALMinCheckpointFrames = 1000
)

// WAL represents a Write-Ahead Log file.
// The WAL allows concurrent readers while writing by appending changes
// to a separate log file instead of modifying the database directly.
type WAL struct {
	// File handle for the WAL file
	file *os.File

	// Filename of the WAL file
	filename string

	// Database filename
	dbFilename string

	// Page size of the database
	pageSize int

	// Number of frames written to the WAL
	frameCount uint32

	// Checkpoint sequence number
	checkpointSeq uint32

	// Salt values for checksum calculation
	salt1 uint32
	salt2 uint32

	// Whether the WAL has been initialized
	initialized bool

	// Read lock for concurrent access
	mu sync.RWMutex

	// Database file handle (needed for checkpoint)
	dbFile *os.File

	// Last frame checksums for cumulative calculation
	lastChecksum1 uint32
	lastChecksum2 uint32

	// Checksum cache to avoid recalculating for sequential reads
	// Maps frame number to its checksums (s1, s2)
	checksumCache map[uint32][2]uint32
}

// WALHeader represents the 32-byte header at the beginning of a WAL file.
// Format matches SQLite WAL specification exactly.
type WALHeader struct {
	Magic         uint32 // Magic number: 0x377f0682 (big-endian)
	Version       uint32 // File format version: 3007000
	PageSize      uint32 // Database page size
	CheckpointSeq uint32 // Checkpoint sequence number
	Salt1         uint32 // Random salt-1
	Salt2         uint32 // Random salt-2
	Checksum1     uint32 // First checksum value
	Checksum2     uint32 // Second checksum value
}

// WALFrame represents a single frame in the WAL file.
// A frame consists of a 24-byte header followed by page data.
type WALFrame struct {
	PageNumber uint32 // Page number
	DbSize     uint32 // Database size in pages after this frame
	Salt1      uint32 // Copy of salt1 from WAL header
	Salt2      uint32 // Copy of salt2 from WAL header
	Checksum1  uint32 // Cumulative checksum 1
	Checksum2  uint32 // Cumulative checksum 2
	Data       []byte // Page data (pageSize bytes)
}

// NewWAL creates a new WAL instance.
// The WAL file is named dbFilename + "-wal".
func NewWAL(dbFilename string, pageSize int) *WAL {
	return &WAL{
		filename:      dbFilename + "-wal",
		dbFilename:    dbFilename,
		pageSize:      pageSize,
		salt1:         generateSalt(),
		salt2:         generateSalt(),
		checksumCache: make(map[uint32][2]uint32),
	}
}

// Open opens or creates the WAL file.
// If the file already exists and is valid, it will be opened for append.
// Otherwise, a new WAL file is created with a fresh header.
func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return errors.New("WAL already open")
	}

	// Check if WAL file exists
	exists := w.walFileExists()

	// Open WAL file
	var err error
	if exists {
		w.file, err = os.OpenFile(w.filename, os.O_RDWR, 0600)
		if err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}

		// Read and validate existing header
		if err := w.readHeader(); err != nil {
			w.file.Close()
			w.file = nil
			// If header is invalid, remove and recreate
			os.Remove(w.filename)
			return w.createNewWAL()
		}

		// Validate all frames and build checksum cache
		if err := w.validateAllFrames(); err != nil {
			w.file.Close()
			w.file = nil
			// If validation fails, remove and recreate
			os.Remove(w.filename)
			return w.createNewWAL()
		}
	} else {
		return w.createNewWAL()
	}

	w.initialized = true
	return nil
}

// createNewWAL creates a new WAL file with a fresh header.
func (w *WAL) createNewWAL() error {
	var err error
	w.file, err = os.OpenFile(
		w.filename,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	// Generate new salt values
	w.salt1 = generateSalt()
	w.salt2 = generateSalt()
	w.frameCount = 0
	w.checkpointSeq++
	w.lastChecksum1 = 0
	w.lastChecksum2 = 0
	w.checksumCache = make(map[uint32][2]uint32)

	// Write WAL header
	if err := w.writeHeader(); err != nil {
		w.file.Close()
		w.file = nil
		return err
	}

	w.initialized = true
	return nil
}

// Close closes the WAL file without checkpointing.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	return err
}

// WriteFrame writes a page to the WAL as a new frame.
// pgno is the page number, data is the page content.
// dbSize is the database size in pages after this write.
func (w *WAL) WriteFrame(pgno Pgno, data []byte, dbSize uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL not open")
	}

	if len(data) != w.pageSize {
		return fmt.Errorf("invalid page size: got %d, expected %d", len(data), w.pageSize)
	}

	if pgno == 0 {
		return ErrInvalidPageNum
	}

	// Create frame
	frame := &WALFrame{
		PageNumber: uint32(pgno),
		DbSize:     dbSize,
		Salt1:      w.salt1,
		Salt2:      w.salt2,
		Data:       data,
	}

	// Calculate checksums (cumulative)
	w.calculateFrameChecksum(frame)

	// Serialize and write frame
	if err := w.writeFrameData(frame); err != nil {
		return err
	}

	w.frameCount++
	return nil
}

// ReadFrame reads a frame from the WAL by frame number (0-indexed).
// Returns nil if the frame doesn't exist.
func (w *WAL) ReadFrame(frameNo uint32) (*WALFrame, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.file == nil {
		return nil, errors.New("WAL not open")
	}

	if frameNo >= w.frameCount {
		return nil, fmt.Errorf("frame %d out of range (total: %d)", frameNo, w.frameCount)
	}

	// Calculate offset: header + (frameNo * (frame header + page data))
	offset := int64(WALHeaderSize) + int64(frameNo)*(int64(WALFrameHeaderSize)+int64(w.pageSize))

	// Read frame header
	headerData := make([]byte, WALFrameHeaderSize)
	if _, err := w.file.ReadAt(headerData, offset); err != nil {
		return nil, fmt.Errorf("failed to read frame header: %w", err)
	}

	// Parse frame header
	frame := &WALFrame{
		PageNumber: binary.BigEndian.Uint32(headerData[0:4]),
		DbSize:     binary.BigEndian.Uint32(headerData[4:8]),
		Salt1:      binary.BigEndian.Uint32(headerData[8:12]),
		Salt2:      binary.BigEndian.Uint32(headerData[12:16]),
		Checksum1:  binary.BigEndian.Uint32(headerData[16:20]),
		Checksum2:  binary.BigEndian.Uint32(headerData[20:24]),
		Data:       make([]byte, w.pageSize),
	}

	// Read page data
	dataOffset := offset + int64(WALFrameHeaderSize)
	if _, err := w.file.ReadAt(frame.Data, dataOffset); err != nil {
		return nil, fmt.Errorf("failed to read frame data: %w", err)
	}

	// Validate salt values
	if frame.Salt1 != w.salt1 || frame.Salt2 != w.salt2 {
		return nil, fmt.Errorf("frame salt mismatch")
	}

	// Validate checksum
	if err := w.validateFrameChecksum(frame, frameNo); err != nil {
		return nil, fmt.Errorf("frame checksum validation failed: %w", err)
	}

	return frame, nil
}

// FindPage searches the WAL for the most recent frame containing the given page.
// Returns nil if the page is not in the WAL.
func (w *WAL) FindPage(pgno Pgno) (*WALFrame, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.file == nil {
		return nil, errors.New("WAL not open")
	}

	// Search backwards from the most recent frame
	// This finds the latest version of the page
	for i := int32(w.frameCount) - 1; i >= 0; i-- {
		frame, err := w.readFrameAtIndex(uint32(i))
		if err != nil {
			return nil, err
		}

		if frame.PageNumber == uint32(pgno) {
			return frame, nil
		}
	}

	return nil, nil
}

// readFrameAtIndex reads a frame without lock (internal helper).
func (w *WAL) readFrameAtIndex(frameNo uint32) (*WALFrame, error) {
	if frameNo >= w.frameCount {
		return nil, fmt.Errorf("frame %d out of range", frameNo)
	}

	offset := int64(WALHeaderSize) + int64(frameNo)*(int64(WALFrameHeaderSize)+int64(w.pageSize))

	headerData := make([]byte, WALFrameHeaderSize)
	if _, err := w.file.ReadAt(headerData, offset); err != nil {
		return nil, fmt.Errorf("failed to read frame header: %w", err)
	}

	frame := &WALFrame{
		PageNumber: binary.BigEndian.Uint32(headerData[0:4]),
		DbSize:     binary.BigEndian.Uint32(headerData[4:8]),
		Salt1:      binary.BigEndian.Uint32(headerData[8:12]),
		Salt2:      binary.BigEndian.Uint32(headerData[12:16]),
		Checksum1:  binary.BigEndian.Uint32(headerData[16:20]),
		Checksum2:  binary.BigEndian.Uint32(headerData[20:24]),
		Data:       make([]byte, w.pageSize),
	}

	dataOffset := offset + int64(WALFrameHeaderSize)
	if _, err := w.file.ReadAt(frame.Data, dataOffset); err != nil {
		return nil, fmt.Errorf("failed to read frame data: %w", err)
	}

	return frame, nil
}

// Checkpoint copies all frames from the WAL back to the database file.
// After a successful checkpoint, the WAL is truncated/reset.
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL not open")
	}

	if w.frameCount == 0 {
		// Nothing to checkpoint
		return nil
	}

	// Open database file if not already open
	if err := w.ensureDBFileOpen(); err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}

	// Build map of page number to latest frame index
	pageFrames, err := w.buildPageFrameMap()
	if err != nil {
		return err
	}

	// Write each page to the database and sync
	if err := w.writeFramesToDB(pageFrames); err != nil {
		return err
	}

	// Sync database file
	if err := w.dbFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync database: %w", err)
	}

	// Truncate and reset WAL
	return w.restartWAL()
}

// Sync syncs the WAL file to disk.
func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.file == nil {
		return errors.New("WAL not open")
	}

	return w.file.Sync()
}

// FrameCount returns the number of frames in the WAL.
func (w *WAL) FrameCount() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.frameCount
}

// ShouldCheckpoint returns true if a checkpoint should be performed.
func (w *WAL) ShouldCheckpoint() bool {
	return w.frameCount >= WALMinCheckpointFrames
}

// Delete deletes the WAL file.
func (w *WAL) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		w.file.Close()
		w.file = nil
	}

	if w.dbFile != nil {
		w.dbFile.Close()
		w.dbFile = nil
	}

	if err := os.Remove(w.filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete WAL file: %w", err)
	}

	w.initialized = false
	w.frameCount = 0

	return nil
}

// walFileExists checks if the WAL file exists.
func (w *WAL) walFileExists() bool {
	info, err := os.Stat(w.filename)
	return err == nil && info.Size() >= WALHeaderSize
}

// writeHeader writes the WAL header to the file.
func (w *WAL) writeHeader() error {
	header := &WALHeader{
		Magic:         WALMagic,
		Version:       WALFormatVersion,
		PageSize:      uint32(w.pageSize),
		CheckpointSeq: w.checkpointSeq,
		Salt1:         w.salt1,
		Salt2:         w.salt2,
	}

	// Calculate checksums for header
	w.calculateHeaderChecksum(header)

	// Serialize header
	data := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(data[0:4], header.Magic)
	binary.BigEndian.PutUint32(data[4:8], header.Version)
	binary.BigEndian.PutUint32(data[8:12], header.PageSize)
	binary.BigEndian.PutUint32(data[12:16], header.CheckpointSeq)
	binary.BigEndian.PutUint32(data[16:20], header.Salt1)
	binary.BigEndian.PutUint32(data[20:24], header.Salt2)
	binary.BigEndian.PutUint32(data[24:28], header.Checksum1)
	binary.BigEndian.PutUint32(data[28:32], header.Checksum2)

	// Write header at offset 0
	if _, err := w.file.WriteAt(data, 0); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	return nil
}

// readHeader reads and validates the WAL header.
func (w *WAL) readHeader() error {
	data := make([]byte, WALHeaderSize)

	if _, err := w.file.ReadAt(data, 0); err != nil {
		return fmt.Errorf("failed to read WAL header: %w", err)
	}

	header := &WALHeader{
		Magic:         binary.BigEndian.Uint32(data[0:4]),
		Version:       binary.BigEndian.Uint32(data[4:8]),
		PageSize:      binary.BigEndian.Uint32(data[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(data[12:16]),
		Salt1:         binary.BigEndian.Uint32(data[16:20]),
		Salt2:         binary.BigEndian.Uint32(data[20:24]),
		Checksum1:     binary.BigEndian.Uint32(data[24:28]),
		Checksum2:     binary.BigEndian.Uint32(data[28:32]),
	}

	// Validate magic number
	if header.Magic != WALMagic {
		return fmt.Errorf("invalid WAL magic: 0x%x", header.Magic)
	}

	// Validate page size
	if int(header.PageSize) != w.pageSize {
		return fmt.Errorf("page size mismatch: got %d, expected %d", header.PageSize, w.pageSize)
	}

	// Validate header checksum
	if err := w.validateHeaderChecksum(header); err != nil {
		return fmt.Errorf("header checksum validation failed: %w", err)
	}

	// Restore state from header
	w.salt1 = header.Salt1
	w.salt2 = header.Salt2
	w.checkpointSeq = header.CheckpointSeq

	// Count frames in WAL
	w.frameCount = 0
	info, err := w.file.Stat()
	if err != nil {
		return err
	}

	fileSize := info.Size()
	if fileSize > WALHeaderSize {
		frameSize := int64(WALFrameHeaderSize + w.pageSize)
		w.frameCount = uint32((fileSize - WALHeaderSize) / frameSize)
	}

	return nil
}

// writeFrameData writes a frame to the WAL file.
func (w *WAL) writeFrameData(frame *WALFrame) error {
	// Serialize frame header
	headerData := make([]byte, WALFrameHeaderSize)
	binary.BigEndian.PutUint32(headerData[0:4], frame.PageNumber)
	binary.BigEndian.PutUint32(headerData[4:8], frame.DbSize)
	binary.BigEndian.PutUint32(headerData[8:12], frame.Salt1)
	binary.BigEndian.PutUint32(headerData[12:16], frame.Salt2)
	binary.BigEndian.PutUint32(headerData[16:20], frame.Checksum1)
	binary.BigEndian.PutUint32(headerData[20:24], frame.Checksum2)

	// Seek to end of file
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek WAL: %w", err)
	}

	// Write frame header
	if _, err := w.file.Write(headerData); err != nil {
		return fmt.Errorf("failed to write frame header: %w", err)
	}

	// Write page data
	if _, err := w.file.Write(frame.Data); err != nil {
		return fmt.Errorf("failed to write frame data: %w", err)
	}

	return nil
}

// calculateHeaderChecksum calculates the checksums for the WAL header.
// Uses the same algorithm as SQLite.
func (w *WAL) calculateHeaderChecksum(header *WALHeader) {
	// Create data array for checksum calculation (first 24 bytes)
	data := make([]byte, 24)
	binary.BigEndian.PutUint32(data[0:4], header.Magic)
	binary.BigEndian.PutUint32(data[4:8], header.Version)
	binary.BigEndian.PutUint32(data[8:12], header.PageSize)
	binary.BigEndian.PutUint32(data[12:16], header.CheckpointSeq)
	binary.BigEndian.PutUint32(data[16:20], header.Salt1)
	binary.BigEndian.PutUint32(data[20:24], header.Salt2)

	// Calculate checksums using SQLite algorithm
	s1, s2 := walChecksum(data, 0, 0)
	header.Checksum1 = s1
	header.Checksum2 = s2
}

// validateHeaderChecksum validates the checksum of the WAL header.
func (w *WAL) validateHeaderChecksum(header *WALHeader) error {
	// Create data array for checksum calculation (first 24 bytes)
	data := make([]byte, 24)
	binary.BigEndian.PutUint32(data[0:4], header.Magic)
	binary.BigEndian.PutUint32(data[4:8], header.Version)
	binary.BigEndian.PutUint32(data[8:12], header.PageSize)
	binary.BigEndian.PutUint32(data[12:16], header.CheckpointSeq)
	binary.BigEndian.PutUint32(data[16:20], header.Salt1)
	binary.BigEndian.PutUint32(data[20:24], header.Salt2)

	// Calculate checksums using SQLite algorithm
	s1, s2 := walChecksum(data, 0, 0)

	// Compare calculated checksum with stored checksum
	if s1 != header.Checksum1 || s2 != header.Checksum2 {
		return fmt.Errorf("checksum mismatch: expected (%d, %d), got (%d, %d)",
			header.Checksum1, header.Checksum2, s1, s2)
	}

	return nil
}

// calculateFrameChecksum calculates the checksums for a WAL frame.
// This is cumulative - each frame's checksum depends on previous checksums.
func (w *WAL) calculateFrameChecksum(frame *WALFrame) {
	// Build frame header data (first 16 bytes)
	headerData := make([]byte, 16)
	binary.BigEndian.PutUint32(headerData[0:4], frame.PageNumber)
	binary.BigEndian.PutUint32(headerData[4:8], frame.DbSize)
	binary.BigEndian.PutUint32(headerData[8:12], frame.Salt1)
	binary.BigEndian.PutUint32(headerData[12:16], frame.Salt2)

	// Start with previous checksums (cumulative calculation)
	s1 := w.lastChecksum1
	s2 := w.lastChecksum2

	// Checksum the frame header
	s1, s2 = walChecksum(headerData, s1, s2)

	// Checksum the page data
	s1, s2 = walChecksum(frame.Data, s1, s2)

	frame.Checksum1 = s1
	frame.Checksum2 = s2

	// Update last checksums for next frame
	w.lastChecksum1 = s1
	w.lastChecksum2 = s2
}

// validateFrameChecksum validates the checksum of a WAL frame.
// The checksum is cumulative, so we use a cache to avoid
// recalculating checksums for frames we've already validated.
func (w *WAL) validateFrameChecksum(frame *WALFrame, frameNo uint32) error {
	// Check if we have a cached checksum for this frame
	if cached, ok := w.checksumCache[frameNo]; ok {
		if cached[0] == frame.Checksum1 && cached[1] == frame.Checksum2 {
			return nil // Already validated
		}
	}

	// Calculate expected checksum from the beginning or from last cached frame
	var s1, s2 uint32
	startFrame := uint32(0)

	// Find the most recent cached frame before this one
	for i := int32(frameNo) - 1; i >= 0; i-- {
		if cached, ok := w.checksumCache[uint32(i)]; ok {
			s1 = cached[0]
			s2 = cached[1]
			startFrame = uint32(i) + 1
			break
		}
	}

	// Calculate checksum from startFrame to frameNo
	for i := startFrame; i <= frameNo; i++ {
		var currentFrame *WALFrame
		var err error

		if i == frameNo {
			// Use the frame we already have
			currentFrame = frame
		} else {
			// Read previous frame for checksum calculation
			offset := int64(WALHeaderSize) + int64(i)*(int64(WALFrameHeaderSize)+int64(w.pageSize))
			headerData := make([]byte, WALFrameHeaderSize)
			if _, err := w.file.ReadAt(headerData, offset); err != nil {
				return fmt.Errorf("failed to read frame %d header: %w", i, err)
			}

			currentFrame = &WALFrame{
				PageNumber: binary.BigEndian.Uint32(headerData[0:4]),
				DbSize:     binary.BigEndian.Uint32(headerData[4:8]),
				Salt1:      binary.BigEndian.Uint32(headerData[8:12]),
				Salt2:      binary.BigEndian.Uint32(headerData[12:16]),
				Data:       make([]byte, w.pageSize),
			}

			dataOffset := offset + int64(WALFrameHeaderSize)
			if _, err = w.file.ReadAt(currentFrame.Data, dataOffset); err != nil {
				return fmt.Errorf("failed to read frame %d data: %w", i, err)
			}
		}

		// Build frame header for checksum (first 16 bytes)
		headerData := make([]byte, 16)
		binary.BigEndian.PutUint32(headerData[0:4], currentFrame.PageNumber)
		binary.BigEndian.PutUint32(headerData[4:8], currentFrame.DbSize)
		binary.BigEndian.PutUint32(headerData[8:12], currentFrame.Salt1)
		binary.BigEndian.PutUint32(headerData[12:16], currentFrame.Salt2)

		// Checksum the frame header
		s1, s2 = walChecksum(headerData, s1, s2)

		// Checksum the page data
		s1, s2 = walChecksum(currentFrame.Data, s1, s2)

		// Cache the checksum for this frame
		w.checksumCache[i] = [2]uint32{s1, s2}
	}

	// Compare calculated checksum with stored checksum
	if s1 != frame.Checksum1 || s2 != frame.Checksum2 {
		return fmt.Errorf("checksum mismatch: expected (%d, %d), got (%d, %d)",
			frame.Checksum1, frame.Checksum2, s1, s2)
	}

	return nil
}

// validateAllFrames validates all frames in the WAL on open.
// This builds the checksum cache and sets lastChecksum for writing.
func (w *WAL) validateAllFrames() error {
	if w.frameCount == 0 {
		return nil
	}

	var s1, s2 uint32

	for i := uint32(0); i < w.frameCount; i++ {
		offset := int64(WALHeaderSize) + int64(i)*(int64(WALFrameHeaderSize)+int64(w.pageSize))

		headerData := make([]byte, WALFrameHeaderSize)
		if _, err := w.file.ReadAt(headerData, offset); err != nil {
			return fmt.Errorf("failed to read frame %d header: %w", i, err)
		}

		frame := &WALFrame{
			PageNumber: binary.BigEndian.Uint32(headerData[0:4]),
			DbSize:     binary.BigEndian.Uint32(headerData[4:8]),
			Salt1:      binary.BigEndian.Uint32(headerData[8:12]),
			Salt2:      binary.BigEndian.Uint32(headerData[12:16]),
			Checksum1:  binary.BigEndian.Uint32(headerData[16:20]),
			Checksum2:  binary.BigEndian.Uint32(headerData[20:24]),
			Data:       make([]byte, w.pageSize),
		}

		dataOffset := offset + int64(WALFrameHeaderSize)
		if _, err := w.file.ReadAt(frame.Data, dataOffset); err != nil {
			return fmt.Errorf("failed to read frame %d data: %w", i, err)
		}

		// Validate salt
		if frame.Salt1 != w.salt1 || frame.Salt2 != w.salt2 {
			return fmt.Errorf("frame %d salt mismatch", i)
		}

		// Build frame header for checksum
		checksumData := make([]byte, 16)
		binary.BigEndian.PutUint32(checksumData[0:4], frame.PageNumber)
		binary.BigEndian.PutUint32(checksumData[4:8], frame.DbSize)
		binary.BigEndian.PutUint32(checksumData[8:12], frame.Salt1)
		binary.BigEndian.PutUint32(checksumData[12:16], frame.Salt2)

		// Checksum the frame header
		s1, s2 = walChecksum(checksumData, s1, s2)

		// Checksum the page data
		s1, s2 = walChecksum(frame.Data, s1, s2)

		// Validate checksum
		if s1 != frame.Checksum1 || s2 != frame.Checksum2 {
			return fmt.Errorf("frame %d checksum mismatch: expected (%d, %d), got (%d, %d)",
				i, frame.Checksum1, frame.Checksum2, s1, s2)
		}

		// Cache the checksum
		w.checksumCache[i] = [2]uint32{s1, s2}
	}

	// Set last checksums for writing new frames
	w.lastChecksum1 = s1
	w.lastChecksum2 = s2

	return nil
}

// walChecksum implements the SQLite WAL checksum algorithm.
// This is a simple running checksum over 32-bit big-endian integers.
func walChecksum(data []byte, s1, s2 uint32) (uint32, uint32) {
	// Process data in 8-byte chunks (two 32-bit values)
	for i := 0; i+7 < len(data); i += 8 {
		s1 += binary.BigEndian.Uint32(data[i:i+4]) + s2
		s2 += binary.BigEndian.Uint32(data[i+4:i+8]) + s1
	}

	// Handle remaining bytes if any
	remaining := len(data) % 8
	if remaining >= 4 {
		s1 += binary.BigEndian.Uint32(data[len(data)-remaining : len(data)-remaining+4])
	}

	return s1, s2
}

// generateSalt generates a random salt value for the WAL.
// In production, this should use crypto/rand for security.
func generateSalt() uint32 {
	// Use CRC32 of current nonce counter for deterministic but varied salts
	// In production, replace with crypto/rand
	nonce := generateNonce()
	return crc32.ChecksumIEEE([]byte{
		byte(nonce >> 24),
		byte(nonce >> 16),
		byte(nonce >> 8),
		byte(nonce),
	})
}
