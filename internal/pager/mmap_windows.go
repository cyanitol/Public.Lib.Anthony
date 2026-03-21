// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build windows

package pager

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	_PAGE_READWRITE = 0x04
	_FILE_MAP_WRITE = 0x02
)

// platformMmap memory-maps a file using Windows APIs.
func platformMmap(f *os.File, size int) ([]byte, error) {
	handle := windows.Handle(f.Fd())

	// CreateFileMapping
	mapHandle, err := windows.CreateFileMapping(
		handle,
		nil, // default security
		_PAGE_READWRITE,
		0,            // high 32 bits of size
		uint32(size), // low 32 bits of size
		nil,          // name (anonymous)
	)
	if err != nil {
		return nil, fmt.Errorf("CreateFileMapping: %w", err)
	}

	// MapViewOfFile
	addr, err := windows.MapViewOfFile(
		mapHandle,
		_FILE_MAP_WRITE,
		0, 0, // offset
		uintptr(size),
	)
	if err != nil {
		windows.CloseHandle(mapHandle)
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	// Close the mapping handle — the view keeps it alive.
	windows.CloseHandle(mapHandle)

	// Create a byte slice backed by the mapped memory.
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}

// platformMunmap unmaps a memory-mapped region on Windows.
func platformMunmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0]))); err != nil {
		return fmt.Errorf("UnmapViewOfFile: %w", err)
	}
	return nil
}

// platformMsync flushes a memory-mapped region to disk on Windows.
func platformMsync(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := windows.FlushViewOfFile(uintptr(unsafe.Pointer(&data[0])), uintptr(len(data))); err != nil {
		return fmt.Errorf("FlushViewOfFile: %w", err)
	}
	return nil
}
