// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build js || wasip1

package vdbe

import (
	"fmt"
)

// memSpillBackend is an in-memory SpillBackend for WASM targets where
// filesystem access is unavailable. It stores serialized runs as byte slices.
// Total storage is capped at maxMemSpillBytes to prevent unbounded growth.
type memSpillBackend struct {
	runs      [][]byte
	totalSize int64
}

const maxMemSpillBytes = 64 * 1024 * 1024 // 64 MB cap

// WriteRun appends a serialized run and returns its index as the run ID.
func (b *memSpillBackend) WriteRun(data []byte) (int, error) {
	if b.totalSize+int64(len(data)) > maxMemSpillBytes {
		return 0, fmt.Errorf("spill memory cap exceeded (%d bytes)", maxMemSpillBytes)
	}
	id := len(b.runs)
	cp := make([]byte, len(data))
	copy(cp, data)
	b.runs = append(b.runs, cp)
	b.totalSize += int64(len(data))
	return id, nil
}

// ReadRun returns the serialized run for the given ID.
func (b *memSpillBackend) ReadRun(id int) ([]byte, error) {
	if id < 0 || id >= len(b.runs) {
		return nil, fmt.Errorf("invalid run ID %d", id)
	}
	return b.runs[id], nil
}

// Close releases all stored run data.
func (b *memSpillBackend) Close() error {
	b.runs = nil
	b.totalSize = 0
	return nil
}

// newDefaultSpillBackend returns an in-memory backend for WASM targets.
func newDefaultSpillBackend() SpillBackend {
	return &memSpillBackend{}
}

// doSpillCurrentRun serializes the current sorted run and stores it via the backend.
func (s *SorterWithSpill) doSpillCurrentRun(numRows int) error {
	backend := s.Config.Backend
	if backend == nil {
		backend = &memSpillBackend{}
		s.Config.Backend = backend
	}

	data, err := s.serializeRows(s.Rows)
	if err != nil {
		return fmt.Errorf("failed to serialize spill run: %w", err)
	}

	runID, err := backend.WriteRun(data)
	if err != nil {
		return fmt.Errorf("failed to write spill run: %w", err)
	}

	s.spilledRuns = append(s.spilledRuns, &SpilledRun{
		NumRows: numRows,
		runID:   runID,
	})

	return nil
}

// mergeSpilledRuns performs a k-way merge reading runs from the in-memory backend.
func (s *SorterWithSpill) mergeSpilledRuns() error {
	if len(s.spilledRuns) == 0 {
		return nil
	}

	backend := s.Config.Backend
	if backend == nil {
		return fmt.Errorf("no spill backend configured")
	}

	readers := make([]*runReader, 0, len(s.spilledRuns))
	for _, run := range s.spilledRuns {
		data, err := backend.ReadRun(run.runID)
		if err != nil {
			return fmt.Errorf("failed to read spill run %d: %w", run.runID, err)
		}

		rows, err := s.deserializeRows(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize spill run %d: %w", run.runID, err)
		}

		readers = append(readers, &runReader{
			rows:    rows,
			current: 0,
		})
	}

	s.Rows = s.mergeRuns(readers)
	s.Sorted = true

	s.cleanupSpillFiles()

	return nil
}

// cleanupSpillFiles closes the backend and clears the spilled runs list.
func (s *SorterWithSpill) cleanupSpillFiles() {
	if s.Config.Backend != nil {
		s.Config.Backend.Close()
		s.Config.Backend = newDefaultSpillBackend()
	}
	s.spilledRuns = s.spilledRuns[:0]
}

// closeReaders is a no-op on the memory backend.
func (s *SorterWithSpill) closeReaders(_ []*runReader) {}
