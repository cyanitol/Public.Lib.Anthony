// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build unix || linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris || aix

package pager

import (
	"os"

	"golang.org/x/sys/unix"
)

// platformMmap memory-maps a file using Unix mmap.
func platformMmap(f *os.File, size int) ([]byte, error) {
	return unix.Mmap(
		int(f.Fd()),
		0,
		size,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
}

// platformMunmap unmaps a memory-mapped region.
func platformMunmap(data []byte) error {
	return unix.Munmap(data)
}

// platformMsync flushes a memory-mapped region to disk.
func platformMsync(data []byte) error {
	return unix.Msync(data, unix.MS_SYNC)
}
