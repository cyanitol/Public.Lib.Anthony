package vdbe

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"unsafe"
)

// SorterConfig holds configuration for sorter memory limits and spill behavior.
type SorterConfig struct {
	// MaxMemoryBytes is the maximum memory the sorter can use before spilling to disk.
	// Default: 10 MB (10 * 1024 * 1024 bytes)
	MaxMemoryBytes int64

	// TempDir is the directory for temporary spill files.
	// If empty, uses os.TempDir()
	TempDir string

	// EnableSpill determines whether spill-to-disk is enabled.
	// If false, sorter will use unlimited memory.
	EnableSpill bool
}

// DefaultSorterConfig returns the default sorter configuration.
func DefaultSorterConfig() *SorterConfig {
	return &SorterConfig{
		MaxMemoryBytes: 10 * 1024 * 1024, // 10 MB
		TempDir:        "",                // Use os.TempDir()
		EnableSpill:    true,
	}
}

// SpilledRun represents a sorted run that has been written to disk.
type SpilledRun struct {
	FilePath string // Path to the temporary file
	File     *os.File // Open file handle
	NumRows  int    // Number of rows in this run
}

// SorterWithSpill extends the basic Sorter with spill-to-disk capability.
type SorterWithSpill struct {
	*Sorter                       // Embed the base Sorter
	Config          *SorterConfig // Configuration
	currentMemBytes int64         // Current memory usage in bytes
	spilledRuns     []*SpilledRun // List of spilled runs on disk
	spillCounter    int           // Counter for unique spill file names
}

// NewSorterWithSpill creates a new sorter with spill-to-disk support.
func NewSorterWithSpill(keyCols []int, desc []bool, collations []string, numCols int, config *SorterConfig) *SorterWithSpill {
	if config == nil {
		config = DefaultSorterConfig()
	}

	return &SorterWithSpill{
		Sorter:          NewSorter(keyCols, desc, collations, numCols),
		Config:          config,
		currentMemBytes: 0,
		spilledRuns:     make([]*SpilledRun, 0),
		spillCounter:    0,
	}
}

// NewSorterWithSpillAndRegistry creates a sorter with spill support and custom collation registry.
func NewSorterWithSpillAndRegistry(keyCols []int, desc []bool, collations []string, numCols int, registry interface{}, config *SorterConfig) *SorterWithSpill {
	if config == nil {
		config = DefaultSorterConfig()
	}

	return &SorterWithSpill{
		Sorter:          NewSorterWithRegistry(keyCols, desc, collations, numCols, registry),
		Config:          config,
		currentMemBytes: 0,
		spilledRuns:     make([]*SpilledRun, 0),
		spillCounter:    0,
	}
}

// estimateRowMemory estimates the memory used by a single row.
func (s *SorterWithSpill) estimateRowMemory(row []*Mem) int64 {
	var size int64
	for _, mem := range row {
		// Base overhead for Mem struct
		size += 64 // Approximate size of Mem struct

		// Add size of variable-length data
		if mem.flags&(MemStr|MemBlob) != 0 {
			size += int64(len(mem.z))
		}
	}
	return size
}

// Insert adds a row to the sorter, spilling to disk if necessary.
func (s *SorterWithSpill) Insert(row []*Mem) error {
	// If spill is disabled, use base implementation
	if !s.Config.EnableSpill {
		s.Sorter.Insert(row)
		return nil
	}

	// Estimate memory for this row
	rowSize := s.estimateRowMemory(row)

	// Check if we need to spill before adding this row
	if s.currentMemBytes+rowSize > s.Config.MaxMemoryBytes && len(s.Rows) > 0 {
		if err := s.spillCurrentRun(); err != nil {
			return fmt.Errorf("failed to spill to disk: %w", err)
		}
	}

	// Add row to in-memory buffer
	s.Sorter.Insert(row)
	s.currentMemBytes += rowSize

	return nil
}

// spillCurrentRun sorts the current in-memory rows and writes them to a temporary file.
func (s *SorterWithSpill) spillCurrentRun() error {
	if len(s.Rows) == 0 {
		return nil
	}

	// Sort the current batch
	s.Sorter.Sort()

	// Create temporary file
	tempDir := s.Config.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	s.spillCounter++
	fileName := fmt.Sprintf("anthony_sorter_spill_%d_%d.tmp", os.Getpid(), s.spillCounter)
	filePath := filepath.Join(tempDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create spill file: %w", err)
	}

	// Write rows to file
	numRows := len(s.Rows)
	if err := s.writeRunToFile(file, s.Rows); err != nil {
		file.Close()
		os.Remove(filePath)
		return fmt.Errorf("failed to write spill file: %w", err)
	}

	// Create spilled run record
	spilledRun := &SpilledRun{
		FilePath: filePath,
		File:     nil, // Will be opened during merge
		NumRows:  numRows,
	}
	s.spilledRuns = append(s.spilledRuns, spilledRun)

	// Close the file for now (we'll reopen during merge)
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close spill file: %w", err)
	}

	// Clear in-memory rows and reset memory counter
	for _, row := range s.Rows {
		for _, mem := range row {
			if mem != nil {
				PutMem(mem)
			}
		}
	}
	s.Rows = s.Rows[:0]
	s.currentMemBytes = 0
	s.Sorted = false

	return nil
}

// writeRunToFile writes sorted rows to a file.
// Format: [numRows:8][row1_len:4][row1_data][row2_len:4][row2_data]...
func (s *SorterWithSpill) writeRunToFile(file *os.File, rows [][]*Mem) error {
	// Write number of rows
	if err := binary.Write(file, binary.LittleEndian, int64(len(rows))); err != nil {
		return err
	}

	// Write each row
	for _, row := range rows {
		// Serialize row
		rowData, err := s.serializeRow(row)
		if err != nil {
			return fmt.Errorf("failed to serialize row: %w", err)
		}

		// Write row length
		if err := binary.Write(file, binary.LittleEndian, int32(len(rowData))); err != nil {
			return err
		}

		// Write row data
		if _, err := file.Write(rowData); err != nil {
			return err
		}
	}

	return nil
}

// serializeRow converts a row of Mem cells to bytes.
func (s *SorterWithSpill) serializeRow(row []*Mem) ([]byte, error) {
	var buf []byte

	// Write number of columns
	numCols := int32(len(row))
	colsBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(colsBuf, uint32(numCols))
	buf = append(buf, colsBuf...)

	// Write each cell
	for _, mem := range row {
		cellData, err := s.serializeMem(mem)
		if err != nil {
			return nil, err
		}
		buf = append(buf, cellData...)
	}

	return buf, nil
}

// serializeMem converts a Mem cell to bytes.
// Format: [flags:2][data_len:4][data]
func (s *SorterWithSpill) serializeMem(mem *Mem) ([]byte, error) {
	var buf []byte

	// Write flags (2 bytes)
	flagsBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(flagsBuf, uint16(mem.flags))
	buf = append(buf, flagsBuf...)

	// Write data based on type
	switch {
	case mem.flags&MemNull != 0:
		// NULL: no data
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, 0)
		buf = append(buf, lenBuf...)

	case mem.flags&MemInt != 0:
		// Integer: 8 bytes
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, 8)
		buf = append(buf, lenBuf...)

		intBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(intBuf, uint64(mem.i))
		buf = append(buf, intBuf...)

	case mem.flags&MemReal != 0:
		// Real: 8 bytes
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, 8)
		buf = append(buf, lenBuf...)

		realBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(realBuf, binary.LittleEndian.Uint64((*[8]byte)(unsafe.Pointer(&mem.r))[:]))
		buf = append(buf, realBuf...)

	case mem.flags&(MemStr|MemBlob) != 0:
		// String or Blob: variable length
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(mem.z)))
		buf = append(buf, lenBuf...)

		buf = append(buf, mem.z...)

	default:
		// Undefined or other
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, 0)
		buf = append(buf, lenBuf...)
	}

	return buf, nil
}

// readRunFromFile reads a sorted run from a file.
func (s *SorterWithSpill) readRunFromFile(file *os.File) ([][]*Mem, error) {
	// Read number of rows
	var numRows int64
	if err := binary.Read(file, binary.LittleEndian, &numRows); err != nil {
		return nil, err
	}

	rows := make([][]*Mem, 0, numRows)

	// Read each row
	for i := int64(0); i < numRows; i++ {
		// Read row length
		var rowLen int32
		if err := binary.Read(file, binary.LittleEndian, &rowLen); err != nil {
			return nil, err
		}

		// Read row data
		rowData := make([]byte, rowLen)
		if _, err := io.ReadFull(file, rowData); err != nil {
			return nil, err
		}

		// Deserialize row
		row, err := s.deserializeRow(rowData)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %w", err)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// deserializeRow converts bytes back to a row of Mem cells.
func (s *SorterWithSpill) deserializeRow(data []byte) ([]*Mem, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("row data too short")
	}

	// Read number of columns
	numCols := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	row := make([]*Mem, numCols)
	for i := uint32(0); i < numCols; i++ {
		mem, bytesRead, err := s.deserializeMem(data[offset:])
		if err != nil {
			return nil, err
		}
		row[i] = mem
		offset += bytesRead
	}

	return row, nil
}

// deserializeMem converts bytes back to a Mem cell.
func (s *SorterWithSpill) deserializeMem(data []byte) (*Mem, int, error) {
	if len(data) < 6 {
		return nil, 0, fmt.Errorf("mem data too short")
	}

	// Read flags
	flags := MemFlags(binary.LittleEndian.Uint16(data[0:2]))

	// Read data length
	dataLen := binary.LittleEndian.Uint32(data[2:6])
	offset := 6

	if len(data) < int(uint32(offset)+dataLen) {
		return nil, 0, fmt.Errorf("mem data truncated")
	}

	mem := GetMem()
	mem.flags = flags

	// Read data based on type
	switch {
	case flags&MemNull != 0:
		mem.SetNull()

	case flags&MemInt != 0:
		if dataLen != 8 {
			return nil, 0, fmt.Errorf("invalid integer data length")
		}
		val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		mem.SetInt(val)

	case flags&MemReal != 0:
		if dataLen != 8 {
			return nil, 0, fmt.Errorf("invalid real data length")
		}
		bits := binary.LittleEndian.Uint64(data[offset : offset+8])
		val := *(*float64)(unsafe.Pointer(&bits))
		mem.SetReal(val)

	case flags&(MemStr|MemBlob) != 0:
		valueData := make([]byte, dataLen)
		copy(valueData, data[offset:offset+int(dataLen)])

		if flags&MemStr != 0 {
			mem.SetStr(string(valueData))
		} else {
			mem.SetBlob(valueData)
		}

	default:
		// Undefined or other
		mem.flags = MemUndefined
	}

	totalBytes := offset + int(dataLen)
	return mem, totalBytes, nil
}

// Sort performs the final sort, merging all spilled runs if necessary.
func (s *SorterWithSpill) Sort() error {
	// If no spilled runs, just sort in-memory
	if len(s.spilledRuns) == 0 {
		s.Sorter.Sort()
		// Set current to -1 so first Next() call advances to row 0
		s.Current = -1
		return nil
	}

	// Spill current in-memory data if any
	if len(s.Rows) > 0 {
		if err := s.spillCurrentRun(); err != nil {
			return err
		}
	}

	// Merge all spilled runs
	if err := s.mergeSpilledRuns(); err != nil {
		return err
	}

	s.Sorted = true
	// Set current to -1 so first Next() call advances to row 0
	s.Current = -1
	return nil
}

// mergeSpilledRuns performs a k-way merge of all spilled runs.
func (s *SorterWithSpill) mergeSpilledRuns() error {
	if len(s.spilledRuns) == 0 {
		return nil
	}

	// Open all spilled run files
	readers := make([]*runReader, 0, len(s.spilledRuns))
	for _, run := range s.spilledRuns {
		file, err := os.Open(run.FilePath)
		if err != nil {
			s.closeReaders(readers)
			return fmt.Errorf("failed to open spill file: %w", err)
		}

		rows, err := s.readRunFromFile(file)
		if err != nil {
			file.Close()
			s.closeReaders(readers)
			return fmt.Errorf("failed to read spill file: %w", err)
		}

		readers = append(readers, &runReader{
			file:    file,
			rows:    rows,
			current: 0,
		})
	}

	// Perform k-way merge
	s.Rows = s.mergeRuns(readers)
	s.Sorted = true

	// Close and delete all spill files
	s.closeReaders(readers)
	s.cleanupSpillFiles()

	return nil
}

// runReader tracks state for reading from a spilled run.
type runReader struct {
	file    *os.File
	rows    [][]*Mem
	current int
}

// hasMore returns true if the reader has more rows.
func (r *runReader) hasMore() bool {
	return r.current < len(r.rows)
}

// peek returns the current row without advancing.
func (r *runReader) peek() []*Mem {
	if r.hasMore() {
		return r.rows[r.current]
	}
	return nil
}

// next advances to the next row.
func (r *runReader) next() {
	r.current++
}

// mergeRuns performs k-way merge of multiple sorted runs.
func (s *SorterWithSpill) mergeRuns(readers []*runReader) [][]*Mem {
	result := make([][]*Mem, 0)

	// Create a min-heap of readers
	heap := &mergeHeap{
		sorter:  s.Sorter,
		readers: readers,
	}

	// Initialize heap with first row from each reader
	for _, reader := range readers {
		if reader.hasMore() {
			heap.items = append(heap.items, reader)
		}
	}
	sort.Sort(heap)

	// Merge loop
	for len(heap.items) > 0 {
		// Get reader with smallest row
		reader := heap.items[0]
		row := reader.peek()

		// Add to result
		result = append(result, row)

		// Advance reader
		reader.next()

		// Update heap
		if reader.hasMore() {
			heap.items[0] = reader
			heap.down(0)
		} else {
			heap.items = heap.items[1:]
		}
	}

	return result
}

// mergeHeap is a min-heap of run readers for k-way merge.
type mergeHeap struct {
	sorter  *Sorter
	readers []*runReader
	items   []*runReader
}

func (h *mergeHeap) Len() int {
	return len(h.items)
}

func (h *mergeHeap) Less(i, j int) bool {
	row1 := h.items[i].peek()
	row2 := h.items[j].peek()
	return h.sorter.compareRows(row1, row2) < 0
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) down(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}

		j := left
		right := left + 1
		if right < n && h.Less(right, left) {
			j = right
		}

		if !h.Less(j, i) {
			break
		}

		h.Swap(i, j)
		i = j
	}
}

// closeReaders closes all run readers.
func (s *SorterWithSpill) closeReaders(readers []*runReader) {
	for _, reader := range readers {
		if reader.file != nil {
			reader.file.Close()
		}
	}
}

// cleanupSpillFiles removes all temporary spill files.
func (s *SorterWithSpill) cleanupSpillFiles() {
	for _, run := range s.spilledRuns {
		if run.FilePath != "" {
			os.Remove(run.FilePath)
		}
	}
	s.spilledRuns = s.spilledRuns[:0]
}

// Close releases all resources and removes temporary files.
func (s *SorterWithSpill) Close() {
	s.cleanupSpillFiles()
	s.Sorter.Close()
}

// GetMemoryUsage returns the current memory usage in bytes.
func (s *SorterWithSpill) GetMemoryUsage() int64 {
	return s.currentMemBytes
}

// GetNumSpilledRuns returns the number of spilled runs.
func (s *SorterWithSpill) GetNumSpilledRuns() int {
	return len(s.spilledRuns)
}
