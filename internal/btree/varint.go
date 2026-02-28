package btree

// Variable-length integer encoding/decoding (SQLite format)
// Based on SQLite's varint implementation

// PutVarint writes a 64-bit unsigned integer to p and returns the number of bytes written.
// The integer is encoded as a variable-length integer using SQLite's encoding:
// - Lower 7 bits of each byte are used for data
// - High bit (0x80) set on all bytes except the last
// - Most significant byte first (big-endian)
// - Maximum of 9 bytes (last byte uses all 8 bits)
// Returns 0 if buffer is too small.
func PutVarint(p []byte, v uint64) int {
	// Validate minimum buffer length
	if len(p) == 0 {
		return 0
	}

	if v <= 0x7f {
		p[0] = byte(v & 0x7f)
		return 1
	}

	if len(p) < 2 {
		return 0
	}

	if v <= 0x3fff {
		p[0] = byte((v>>7)&0x7f) | 0x80
		p[1] = byte(v & 0x7f)
		return 2
	}

	return putVarint64(p, v)
}

// putVarint64 handles the general case of encoding a 64-bit varint
func putVarint64(p []byte, v uint64) int {
	// Validate buffer length for worst case (9 bytes)
	if len(p) < 9 {
		return 0
	}

	if v&(uint64(0xff000000)<<32) != 0 {
		// 9-byte case: all 8 bits of the 9th byte are used
		p[8] = byte(v)
		v >>= 8
		for i := 7; i >= 0; i-- {
			p[i] = byte((v & 0x7f) | 0x80)
			v >>= 7
		}
		return 9
	}

	// Build varint in forward order
	// Count how many 7-bit groups we need
	n := 1 // At least one byte needed
	temp := v >> 7
	for temp > 0 {
		n++
		temp >>= 7
	}

	// Encode from most significant to least significant
	for i := n - 1; i >= 0; i-- {
		shift := uint(i * 7)
		b := byte((v >> shift) & 0x7f)
		if i > 0 {
			b |= 0x80 // Set continuation bit for all except last byte
		}
		p[n-1-i] = b
	}
	return n
}

func decodeShortVarint(p []byte) (uint64, int) {
	const SLOT_2_0 = 0x001fc07f

	a := uint32(p[0])<<14 | uint32(p[2])
	b := uint32(p[1])

	if a&0x80 == 0 {
		return uint64((b&0x7f)<<7 | a&SLOT_2_0), 3
	}

	if len(p) < 4 {
		return 0, 0
	}

	b = (b&0x7f)<<14 | uint32(p[3])
	if b&0x80 == 0 {
		return uint64((a&SLOT_2_0)<<7 | b&SLOT_2_0), 4
	}

	return 0, 0
}

func decodeMultiByteVarint(p []byte) (uint64, int) {
	// This function should only be called when len(p) >= 9 is verified
	// by the caller (GetVarint), but we add an extra safety check
	if len(p) < 9 {
		return 0, 0
	}

	var v uint64
	for i := 0; i < 8; i++ {
		v = (v << 7) | uint64(p[i]&0x7f)
		if p[i]&0x80 == 0 {
			return v, i + 1
		}
	}
	return (v << 8) | uint64(p[8]), 9
}

// GetVarint reads a 64-bit variable-length integer from p and returns
// the value and the number of bytes read.
func GetVarint(p []byte) (uint64, int) {
	// Validate minimum buffer length
	if len(p) == 0 {
		return 0, 0
	}

	if p[0] < 0x80 {
		return uint64(p[0]), 1
	}

	if len(p) > 1 && p[1] < 0x80 {
		return (uint64(p[0]&0x7f) << 7) | uint64(p[1]), 2
	}

	if len(p) < 3 {
		return 0, 0
	}

	if v, n := decodeShortVarint(p); n > 0 {
		return v, n
	}

	if len(p) < 9 {
		return 0, 0
	}

	return decodeMultiByteVarint(p)
}

// GetVarint32 reads a 32-bit variable-length integer from p and returns
// the value and the number of bytes read. If the varint is larger than
// 32 bits, it returns 0xffffffff.
func GetVarint32(p []byte) (uint32, int) {
	if v, n, ok := tryFastBtreeVarint32(p); ok {
		return v, n
	}
	return slowBtreeVarint32(p)
}

func tryFastBtreeVarint32(p []byte) (uint32, int, bool) {
	if len(p) > 0 && p[0] < 0x80 {
		return uint32(p[0]), 1, true
	}
	if len(p) > 1 && p[1] < 0x80 {
		return (uint32(p[0]&0x7f) << 7) | uint32(p[1]), 2, true
	}
	if len(p) > 2 && p[2] < 0x80 {
		return (uint32(p[0]&0x7f) << 14) | (uint32(p[1]&0x7f) << 7) | uint32(p[2]), 3, true
	}
	return 0, 0, false
}

func slowBtreeVarint32(p []byte) (uint32, int) {
	v64, n := GetVarint(p)
	if n > 3 && n <= 9 {
		if v64 > 0xffffffff {
			return 0xffffffff, n
		}
		return uint32(v64), n
	}
	return 0, 0
}

// varintLenThresholds defines the upper bounds for each varint size.
var varintLenThresholds = [8]uint64{
	0x7f, 0x3fff, 0x1fffff, 0xfffffff,
	0x7ffffffff, 0x3ffffffffff, 0x1ffffffffffff, 0xffffffffffffff,
}

// VarintLen returns the number of bytes required to encode v as a varint
func VarintLen(v uint64) int {
	for i, thresh := range varintLenThresholds {
		if v <= thresh {
			return i + 1
		}
	}
	return 9
}
