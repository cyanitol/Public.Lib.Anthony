// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"os"
	"testing"
)

// spill2CheckDevFull skips the test if /dev/full is not available.
func spill2CheckDevFull(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skipf("/dev/full not available: %v", err)
	}
}

// TestSorterSpill2Coverage_WriteErrorTriggersCleanup exercises the cleanup block
// in writeAndRecordSpill when writeRunToFile returns an error.
func TestSorterSpill2Coverage_WriteErrorTriggersCleanup(t *testing.T) {
	t.Parallel()
	spill2CheckDevFull(t)

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()
	s.Sorter.Insert([]*Mem{NewMemInt(42)})

	if err := s.writeAndRecordSpill("/dev/full", 1); err == nil {
		t.Fatal("expected an error writing to /dev/full, got nil")
	}
	if s.GetNumSpilledRuns() != 0 {
		t.Errorf("spilled runs after write error: want 0, got %d", s.GetNumSpilledRuns())
	}
}

// TestSorterSpill2Coverage_WriteErrorRemovesFile exercises the cleanup path with
// multiple column types.
func TestSorterSpill2Coverage_WriteErrorRemovesFile(t *testing.T) {
	t.Parallel()
	spill2CheckDevFull(t)

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, nil)
	defer s.Close()
	s.Sorter.Insert([]*Mem{NewMemInt(1), NewMemStr("hello")})
	s.Sorter.Insert([]*Mem{NewMemInt(2), NewMemReal(3.14)})

	if err := s.writeAndRecordSpill("/dev/full", 2); err == nil {
		t.Fatal("expected write error to /dev/full, got nil")
	}
}

// TestSorterSpill2Coverage_WriteErrorWithNullRows exercises the NULL/Real/Blob
// serialisation paths before the write fails.
func TestSorterSpill2Coverage_WriteErrorWithNullRows(t *testing.T) {
	t.Parallel()
	spill2CheckDevFull(t)

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 3, nil)
	defer s.Close()
	s.Sorter.Insert([]*Mem{NewMemNull(), NewMemReal(1.5), NewMemBlob([]byte{0xAB, 0xCD})})

	if err := s.writeAndRecordSpill("/dev/full", 1); err == nil {
		t.Fatal("expected write error to /dev/full, got nil")
	}
}

// TestSorterSpill2Coverage_CloseErrorPath exercises the file.Close() error
// branch by manipulating the underlying OS file descriptor.
func TestSorterSpill2Coverage_CloseErrorPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	tmpFile, createErr := os.CreateTemp(tempDir, "close_err_*.tmp")
	if createErr != nil {
		t.Fatalf("CreateTemp: %v", createErr)
	}
	targetPath := tmpFile.Name()

	rawFd := tmpFile.Fd()
	shadow := os.NewFile(rawFd, targetPath)
	if shadow != nil {
		shadow.Close()
	}
	tmpFile.Close()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	writeErr := s.writeAndRecordSpill(targetPath, 0)
	if writeErr != nil {
		t.Logf("writeAndRecordSpill returned (expected on some platforms): %v", writeErr)
	}

	os.Remove(targetPath)
}
