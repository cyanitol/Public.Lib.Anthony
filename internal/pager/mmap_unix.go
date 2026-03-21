// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build unix || linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris || aix

package pager

import (
	"os"
	"syscall"
)

// platformMmap memory-maps a file using Unix mmap.
func platformMmap(f *os.File, size int) ([]byte, error) {
	return syscall.Mmap(
		int(f.Fd()),
		0,
		size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
}

// platformMunmap unmaps a memory-mapped region.
func platformMunmap(data []byte) error {
	return syscall.Munmap(data)
}
