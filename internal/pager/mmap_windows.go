// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build windows

package pager

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Windows mmap via CreateFileMapping + MapViewOfFile.

var (
	modkernel32            = syscall.NewLazyDLL("kernel32.dll")
	procCreateFileMappingW = modkernel32.NewProc("CreateFileMappingW")
	procMapViewOfFile      = modkernel32.NewProc("MapViewOfFile")
	procUnmapViewOfFile    = modkernel32.NewProc("UnmapViewOfFile")
	procFlushViewOfFile    = modkernel32.NewProc("FlushViewOfFile")
)

const (
	_PAGE_READWRITE = 0x04
	_FILE_MAP_WRITE = 0x02
	_INVALID_HANDLE = ^syscall.Handle(0)
)

// platformMmap memory-maps a file using Windows APIs.
func platformMmap(f *os.File, size int) ([]byte, error) {
	handle := syscall.Handle(f.Fd())

	// CreateFileMapping
	mapHandle, _, err := procCreateFileMappingW.Call(
		uintptr(handle),
		0, // default security
		uintptr(_PAGE_READWRITE),
		0,             // high 32 bits of size
		uintptr(size), // low 32 bits of size
		0,             // name (anonymous)
	)
	if mapHandle == 0 {
		return nil, fmt.Errorf("CreateFileMapping: %w", err)
	}

	// MapViewOfFile
	addr, _, err := procMapViewOfFile.Call(
		mapHandle,
		uintptr(_FILE_MAP_WRITE),
		0, 0, // offset
		uintptr(size),
	)
	if addr == 0 {
		syscall.CloseHandle(syscall.Handle(mapHandle))
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	// Close the mapping handle — the view keeps it alive.
	syscall.CloseHandle(syscall.Handle(mapHandle))

	// Create a byte slice backed by the mapped memory.
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}

// platformMunmap unmaps a memory-mapped region on Windows.
func platformMunmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ret, _, err := procUnmapViewOfFile.Call(uintptr(unsafe.Pointer(&data[0])))
	if ret == 0 {
		return fmt.Errorf("UnmapViewOfFile: %w", err)
	}
	return nil
}

// platformMsync flushes a memory-mapped region to disk on Windows.
func platformMsync(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ret, _, err := procFlushViewOfFile.Call(
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
	)
	if ret == 0 {
		return fmt.Errorf("FlushViewOfFile: %w", err)
	}
	return nil
}
