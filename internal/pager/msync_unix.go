// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build linux || darwin || freebsd || openbsd || dragonfly

package pager

import (
	"fmt"
	"syscall"
	"unsafe"
)

// platformMsync flushes a memory-mapped region to disk.
func platformMsync(data []byte) error {
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}
	return nil
}
