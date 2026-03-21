// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build solaris || aix

package pager

// platformMsync is a no-op on platforms without a usable raw msync syscall.
// WAL integrity relies on fdatasync via the file handle.
func platformMsync(data []byte) error {
	return nil
}
