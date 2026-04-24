// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"math/rand"
	"sync"
	"testing"
)

// TestWALConcurrentReads verifies that multiple goroutines can simultaneously
// read frames from the WAL without race conditions or checksum errors.
func TestWALConcurrentReads(t *testing.T) {
	t.Parallel()

	const numFrames = 20
	const numGoroutines = 8

	dbFile := createTestDBFile(t)
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	// Write 20 frames with unique page numbers and deterministic data.
	frames := make([][]byte, numFrames)
	for i := 0; i < numFrames; i++ {
		data := makeTestPage(i*13+7, DefaultPageSize)
		frames[i] = data
		mustWriteFrame(t, wal, Pgno(i+1), data, uint32(i+1))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*numFrames)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readAllFrames(wal, numFrames, frames, errCh)
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// readAllFrames reads all numFrames from wal and sends any errors to errCh.
func readAllFrames(wal *WAL, numFrames int, expected [][]byte, errCh chan<- error) {
	for i := 0; i < numFrames; i++ {
		frame, err := wal.ReadFrame(uint32(i))
		if err != nil {
			errCh <- err
			continue
		}
		if !bytesEqual(frame.Data, expected[i]) {
			errCh <- newDataMismatchError(i)
			continue
		}
		if frame.PageNumber != uint32(i+1) {
			errCh <- newPageNumberError(i, frame.PageNumber)
		}
	}
}

// newDataMismatchError creates an error for a data mismatch at a frame index.
func newDataMismatchError(frameIdx int) error {
	return &walTestError{msg: "data mismatch", frameIdx: frameIdx}
}

// newPageNumberError creates an error for a page number mismatch.
func newPageNumberError(frameIdx int, got uint32) error {
	return &walTestError{msg: "page number mismatch", frameIdx: frameIdx, extra: got}
}

// walTestError is a simple error type for WAL test failures.
type walTestError struct {
	msg      string
	frameIdx int
	extra    uint32
}

func (e *walTestError) Error() string {
	if e.extra != 0 {
		return e.msg + " at frame " + itoa(e.frameIdx)
	}
	return e.msg + " at frame " + itoa(e.frameIdx)
}

// itoa converts a non-negative int to a decimal string without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// TestWALSequentialWriteRead writes 50 frames sequentially, then reads them
// back in random order, verifying data, page number, and checksum correctness.
// This specifically exercises the checksumCache fix for many frames.
func TestWALSequentialWriteRead(t *testing.T) {
	t.Parallel()

	const numFrames = 50

	dbFile := createTestDBFile(t)
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	// Write 50 frames with deterministic data.
	frames := make([][]byte, numFrames)
	for i := 0; i < numFrames; i++ {
		data := makeTestPage(i*17+3, DefaultPageSize)
		frames[i] = data
		mustWriteFrame(t, wal, Pgno(i+1), data, uint32(i+1))
	}

	// Build a shuffled read order.
	order := shuffledIndices(numFrames)

	// Read frames back in shuffled order and verify.
	for _, i := range order {
		frame, err := wal.ReadFrame(uint32(i))
		if err != nil {
			t.Errorf("ReadFrame(%d) error = %v", i, err)
			continue
		}
		if frame.PageNumber != uint32(i+1) {
			t.Errorf("frame %d: got page number %d, want %d", i, frame.PageNumber, i+1)
		}
		if !bytesEqual(frame.Data, frames[i]) {
			t.Errorf("frame %d: data mismatch", i)
		}
	}
}

// shuffledIndices returns a deterministically shuffled slice of [0, n).
func shuffledIndices(n int) []int {
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	r := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic seed for tests
	r.Shuffle(n, func(i, j int) { indices[i], indices[j] = indices[j], indices[i] })
	return indices
}

// TestWALCheckpointAfterManyFrames writes 100 frames, checkpoints, verifies the
// WAL is empty, then writes 10 more frames and reads them back correctly.
// This tests WAL restart after checkpoint.
// walWriteFramesBatch writes numFrames with deterministic data based on seedMul and seedAdd.
func walWriteFramesBatch(t *testing.T, w *WAL, numFrames int, seedMul, seedAdd int) [][]byte {
	t.Helper()
	result := make([][]byte, numFrames)
	for i := 0; i < numFrames; i++ {
		data := makeTestPage(i*seedMul+seedAdd, DefaultPageSize)
		result[i] = data
		mustWriteFrame(t, w, Pgno(i+1), data, uint32(i+1))
	}
	return result
}

// walVerifyFramesBatch reads numFrames from the WAL and verifies data and page numbers.
func walVerifyFramesBatch(t *testing.T, w *WAL, expected [][]byte) {
	t.Helper()
	for i := 0; i < len(expected); i++ {
		frame, err := w.ReadFrame(uint32(i))
		if err != nil {
			t.Errorf("ReadFrame(%d) error = %v", i, err)
			continue
		}
		if frame.PageNumber != uint32(i+1) {
			t.Errorf("frame %d: got page number %d, want %d", i, frame.PageNumber, i+1)
		}
		if !bytesEqual(frame.Data, expected[i]) {
			t.Errorf("frame %d: data mismatch", i)
		}
	}
}

func TestWALCheckpointAfterManyFrames(t *testing.T) {
	t.Parallel()

	dbFile := createTestDBFileWithSize(t, DefaultPageSize*100)
	wal := mustOpenWAL(t, dbFile, DefaultPageSize)
	defer wal.Close()

	walWriteFramesBatch(t, wal, 100, 11, 5)

	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if wal.frameCount != 0 {
		t.Errorf("expected frameCount=0 after checkpoint, got %d", wal.frameCount)
	}

	postData := walWriteFramesBatch(t, wal, 10, 23, 1)
	walVerifyFramesBatch(t, wal, postData)
}
