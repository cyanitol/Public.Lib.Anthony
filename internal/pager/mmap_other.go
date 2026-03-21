// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !(unix || linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris || aix || windows)

package pager

import "os"

// Fallback mmap implementation for platforms without mmap support (e.g. WASM).
// Uses a heap-allocated byte slice with file I/O instead of memory mapping.

// platformMmap reads the file into a heap-allocated byte slice.
func platformMmap(f *os.File, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := f.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// platformMunmap is a no-op for the fallback implementation.
func platformMunmap(data []byte) error {
	return nil
}

// platformMsync is a no-op for the fallback implementation.
// Writes are flushed via the file handle directly.
func platformMsync(data []byte) error {
	return nil
}
