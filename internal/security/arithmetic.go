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

// ValidateUsableSize checks that usableSize is large enough for B-tree operations
// Minimum usable size is 35 bytes (based on SQLite's calculation requirements)
const MinUsableSize = 35

func ValidateUsableSize(usableSize uint32) error {
	if usableSize < MinUsableSize {
		return ErrIntegerUnderflow
	}
	return nil
}
