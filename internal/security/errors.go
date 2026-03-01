// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package security

import "errors"

var (
	ErrNullByte         = errors.New("security: path contains null byte")
	ErrTraversal        = errors.New("security: path traversal attempt detected")
	ErrEscapesSandbox   = errors.New("security: path escapes sandbox")
	ErrNotInAllowlist   = errors.New("security: path not in allowed directories")
	ErrSymlink          = errors.New("security: symlinks not allowed")
	ErrAbsolutePath     = errors.New("security: absolute paths not allowed")
	ErrIntegerOverflow  = errors.New("security: integer overflow detected")
	ErrIntegerUnderflow = errors.New("security: integer underflow detected")
	ErrBufferOverflow   = errors.New("security: buffer overflow")
)
