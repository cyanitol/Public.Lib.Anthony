// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package security

import (
	"math"
)

// SafeCastUint32ToUint16 safely casts uint32 to uint16 with bounds checking
func SafeCastUint32ToUint16(v uint32) (uint16, error) {
	if v > math.MaxUint16 {
		return 0, ErrIntegerOverflow
	}
	return uint16(v), nil
}

// SafeCastInt64ToInt32 safely casts int64 to int32 with bounds checking
func SafeCastInt64ToInt32(v int64) (int32, error) {
	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0, ErrIntegerOverflow
	}
	return int32(v), nil
}

// SafeAddUint32 safely adds two uint32 values with overflow detection
func SafeAddUint32(a, b uint32) (uint32, error) {
	if a > math.MaxUint32-b {
		return 0, ErrIntegerOverflow
	}
	return a + b, nil
}

// SafeSubUint32 safely subtracts two uint32 values with underflow detection
func SafeSubUint32(a, b uint32) (uint32, error) {
	if b > a {
		return 0, ErrIntegerUnderflow
	}
	return a - b, nil
}

// SafeCastIntToUint16 safely casts int to uint16 with bounds checking
func SafeCastIntToUint16(v int) (uint16, error) {
	if v < 0 || v > math.MaxUint16 {
		return 0, ErrIntegerOverflow
	}
	return uint16(v), nil
}

// SafeCastIntToUint32 safely casts int to uint32 with bounds checking
func SafeCastIntToUint32(v int) (uint32, error) {
	if v < 0 {
		return 0, ErrIntegerOverflow
	}
	// On 64-bit systems, int can be larger than uint32
	if int64(v) > math.MaxUint32 {
		return 0, ErrIntegerOverflow
	}
	return uint32(v), nil
}

// SafeMultiplyUint32 safely multiplies two uint32 values with overflow detection.
func SafeMultiplyUint32(a, b uint32) (uint32, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	result := a * b
	if result/a != b {
		return 0, ErrIntegerOverflow
	}
	return result, nil
}

// ValidateBufferAccess validates that an access at offset with given length is
// within the bounds of the buffer, preventing buffer overflow.
func ValidateBufferAccess(buf []byte, offset, length int) error {
	if offset < 0 || length < 0 {
		return ErrBufferOverflow
	}
	if offset > len(buf) || length > len(buf)-offset {
		return ErrBufferOverflow
	}
	return nil
}

// ValidateExpressionDepth checks that an expression depth is within limits.
func ValidateExpressionDepth(config *SecurityConfig, depth int) error {
	if config == nil {
		config = DefaultSecurityConfig()
	}
	if depth > config.MaxExpressionDepth {
		return ErrExprDepthExceeds
	}
	return nil
}

// SanitizeIdentifier validates an identifier for dangerous characters.
// Returns the identifier unchanged if valid, or an error if it contains
// null bytes or control characters.
func SanitizeIdentifier(ident string) (string, error) {
	if len(ident) == 0 {
		return "", ErrEmptyIdentifier
	}
	for _, char := range ident {
		if isControlChar(char) {
			return "", ErrNullByte
		}
	}
	return ident, nil
}

// ValidateUsableSize checks that usableSize is large enough for B-tree operations
// Minimum usable size is 35 bytes (based on SQLite's calculation requirements)
const MinUsableSize = 35

func ValidateUsableSize(usableSize uint32) error {
	if usableSize < MinUsableSize {
		return ErrIntegerUnderflow
	}
	return nil
}
