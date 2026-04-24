// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// openBenchWAL creates a temporary WAL for benchmarks.
func openBenchWAL(b *testing.B) (wal *WAL, cleanup func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "wal_bench_*")
	if err != nil {
		b.Fatalf("MkdirTemp() error = %v", err)
	}
	dbFile := filepath.Join(dir, "bench.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		os.RemoveAll(dir)
		b.Fatalf("WriteFile() error = %v", err)
	}
	w := NewWAL(dbFile, DefaultPageSize)
	if err := w.Open(); err != nil {
		os.RemoveAll(dir)
		b.Fatalf("WAL.Open() error = %v", err)
	}
	return w, func() {
		w.Close()
		os.RemoveAll(dir)
	}
}

// BenchmarkWALWriteFrame measures sequential WAL frame writes.
func BenchmarkWALWriteFrame(b *testing.B) {
	wal, cleanup := openBenchWAL(b)
	defer cleanup()

	pageData := make([]byte, DefaultPageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pgno := Pgno(i%1000 + 1)
		if err := wal.WriteFrame(pgno, pageData, uint32(i+1)); err != nil {
			b.Fatalf("WriteFrame(%d) error = %v", i, err)
		}
	}
}

// BenchmarkWALReadFrame measures reading frames after writing them.
func BenchmarkWALReadFrame(b *testing.B) {
	wal, cleanup := openBenchWAL(b)
	defer cleanup()

	const frameCount = 100
	pageData := make([]byte, DefaultPageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}
	for i := 0; i < frameCount; i++ {
		if err := wal.WriteFrame(Pgno(i+1), pageData, uint32(i+1)); err != nil {
			b.Fatalf("WriteFrame setup(%d) error = %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := uint32(i % frameCount)
		if _, err := wal.ReadFrame(idx); err != nil {
			b.Fatalf("ReadFrame(%d) error = %v", idx, err)
		}
	}
}

// BenchmarkWALChecksumValidation measures checksum validation overhead on reads
// by writing frames, closing and reopening (to force cache rebuild), then reading.
// benchSetupWALWithFrames creates a WAL with frameCount frames and returns a reopened WAL.
func benchSetupWALWithFrames(b *testing.B, frameCount int) *WAL {
	b.Helper()
	dir, err := os.MkdirTemp("", "wal_checksum_bench_*")
	if err != nil {
		b.Fatalf("MkdirTemp() error = %v", err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	dbFile := filepath.Join(dir, "bench.db")
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		b.Fatalf("WriteFile() error = %v", err)
	}

	w := NewWAL(dbFile, DefaultPageSize)
	if err := w.Open(); err != nil {
		b.Fatalf("WAL.Open() error = %v", err)
	}
	for i := 0; i < frameCount; i++ {
		if err := w.WriteFrame(Pgno(i+1), makeTestPage(i*7, DefaultPageSize), uint32(i+1)); err != nil {
			w.Close()
			b.Fatalf("WriteFrame(%d) error = %v", i, err)
		}
	}
	w.Close()

	w2 := NewWAL(dbFile, DefaultPageSize)
	if err := w2.Open(); err != nil {
		b.Fatalf("WAL.Open() (reopen) error = %v", err)
	}
	return w2
}

func BenchmarkWALChecksumValidation(b *testing.B) {
	const frameCount = 50
	w2 := benchSetupWALWithFrames(b, frameCount)
	defer w2.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := uint32(i % frameCount)
		if _, err := w2.ReadFrame(idx); err != nil {
			b.Fatalf("ReadFrame(%d) error = %v", idx, err)
		}
	}
}
