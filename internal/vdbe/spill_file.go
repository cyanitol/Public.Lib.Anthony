// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !js && !wasip1

package vdbe

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// newDefaultSpillBackend returns nil on native platforms because the sorter
// uses direct file I/O methods rather than the SpillBackend interface.
func newDefaultSpillBackend() SpillBackend {
	return nil
}

// createSpillFilePath generates a unique temporary file path for spilling.
func (s *SorterWithSpill) createSpillFilePath() (string, error) {
	tempDir := s.Config.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	s.spillCounter++
	fileName := fmt.Sprintf("anthony_sorter_spill_%d_%d.tmp", os.Getpid(), s.spillCounter)
	return filepath.Join(tempDir, fileName), nil
}

// doSpillCurrentRun writes the current sorted in-memory run to a temp file.
func (s *SorterWithSpill) doSpillCurrentRun(numRows int) error {
	filePath, err := s.createSpillFilePath()
	if err != nil {
		return err
	}
	return s.writeAndRecordSpill(filePath, numRows)
}

// writeAndRecordSpill writes rows to a spill file and records the spilled run.
func (s *SorterWithSpill) writeAndRecordSpill(filePath string, numRows int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create spill file: %w", err)
	}

	if err := s.writeRunToFile(file, s.Rows); err != nil {
		file.Close()
		os.Remove(filePath)
		return fmt.Errorf("failed to write spill file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close spill file: %w", err)
	}

	s.spilledRuns = append(s.spilledRuns, &SpilledRun{
		FilePath: filePath,
		NumRows:  numRows,
	})

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

// mergeSpilledRuns performs a k-way merge of all spilled runs.
func (s *SorterWithSpill) mergeSpilledRuns() error {
	if len(s.spilledRuns) == 0 {
		return nil
	}

	readers := make([]*runReader, 0, len(s.spilledRuns))
	for _, run := range s.spilledRuns {
		file, err := os.Open(run.FilePath)
		if err != nil {
			return fmt.Errorf("failed to open spill file: %w", err)
		}

		rows, err := s.readRunFromFile(file)
		file.Close() // close immediately after reading all rows
		if err != nil {
			return fmt.Errorf("failed to read spill file: %w", err)
		}

		readers = append(readers, &runReader{
			rows:    rows,
			current: 0,
		})
	}

	// Perform k-way merge
	s.Rows = s.mergeRuns(readers)
	s.Sorted = true

	// Remove all spill files
	s.cleanupSpillFiles()

	return nil
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

// closeReaders is a no-op on the file backend since files are closed after reading.
func (s *SorterWithSpill) closeReaders(_ []*runReader) {}
