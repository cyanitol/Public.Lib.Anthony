// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build netbsd

package pager

import (
	"fmt"
	"syscall"
	"unsafe"
)

// SYS_MSYNC is NetBSD's __msync13 syscall number (277).
// Go's stdlib syscall package does not define this for NetBSD.
const _SYS_MSYNC = 277

// platformMsync flushes a memory-mapped region to disk.
func platformMsync(data []byte) error {
	_, _, errno := syscall.Syscall(
		_SYS_MSYNC,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}
	return nil
}
