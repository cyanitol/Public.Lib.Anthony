// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package utf

import "encoding/binary"

// Encoding represents the byte order of UTF-16 encoded text.
type Encoding byte

const (
	// UTF8 encoding
	UTF8 Encoding = 1

	// UTF16LE is little-endian UTF-16
	UTF16LE Encoding = 2

	// UTF16BE is big-endian UTF-16
	UTF16BE Encoding = 3
)

// BOM represents byte order marks for UTF-16
const (
	BOM_LE_FIRST  = 0xFF // Little-endian BOM first byte
	BOM_LE_SECOND = 0xFE // Little-endian BOM second byte
	BOM_BE_FIRST  = 0xFE // Big-endian BOM first byte
	BOM_BE_SECOND = 0xFF // Big-endian BOM second byte
)

// UTF-16 surrogate pair constants
const (
	SurrogateMin     = 0xD800
	SurrogateMax     = 0xDFFF
	HighSurrogateMin = 0xD800
	HighSurrogateMax = 0xDBFF
	LowSurrogateMin  = 0xDC00
	LowSurrogateMax  = 0xDFFF
	SurrogateOffset  = 0x10000
)

// EncodeUTF16LE encodes a rune into UTF-16 little-endian format.
// It returns the encoded bytes.
func EncodeUTF16LE(buf []byte, r rune) int {
	if r <= 0xFFFF {
		buf[0] = byte(r)
		buf[1] = byte(r >> 8)
		return 2
	}

	// Encode as surrogate pair
	r -= SurrogateOffset
	high := uint16(HighSurrogateMin + (r >> 10))
	low := uint16(LowSurrogateMin + (r & 0x3FF))

	buf[0] = byte(high)
	buf[1] = byte(high >> 8)
	buf[2] = byte(low)
	buf[3] = byte(low >> 8)
	return 4
}

// EncodeUTF16BE encodes a rune into UTF-16 big-endian format.
// It returns the encoded bytes.
func EncodeUTF16BE(buf []byte, r rune) int {
	if r <= 0xFFFF {
		buf[0] = byte(r >> 8)
		buf[1] = byte(r)
		return 2
	}

	// Encode as surrogate pair
	r -= SurrogateOffset
	high := uint16(HighSurrogateMin + (r >> 10))
	low := uint16(LowSurrogateMin + (r & 0x3FF))

	buf[0] = byte(high >> 8)
	buf[1] = byte(high)
	buf[2] = byte(low >> 8)
	buf[3] = byte(low)
	return 4
}

// DecodeUTF16LE decodes a UTF-16 little-endian sequence to a rune.
// It returns the rune and the number of bytes consumed (2 or 4).
func DecodeUTF16LE(data []byte) (r rune, size int) {
	if len(data) < 2 {
		return ReplacementChar, 0
	}

	c := uint32(data[0]) | uint32(data[1])<<8

	// Check if it's a high surrogate
	if c >= HighSurrogateMin && c <= HighSurrogateMax {
		if len(data) < 4 {
			return ReplacementChar, 2
		}

		c2 := uint32(data[2]) | uint32(data[3])<<8
		if c2 >= LowSurrogateMin && c2 <= LowSurrogateMax {
			// Valid surrogate pair
			r = rune(((c & 0x3FF) << 10) + (c2 & 0x3FF) + SurrogateOffset)
			return r, 4
		}

		// Invalid surrogate pair
		return ReplacementChar, 2
	}

	// Single UTF-16 code unit
	return rune(c), 2
}

// DecodeUTF16BE decodes a UTF-16 big-endian sequence to a rune.
// It returns the rune and the number of bytes consumed (2 or 4).
func DecodeUTF16BE(data []byte) (r rune, size int) {
	if len(data) < 2 {
		return ReplacementChar, 0
	}

	c := uint32(data[0])<<8 | uint32(data[1])

	// Check if it's a high surrogate
	if c >= HighSurrogateMin && c <= HighSurrogateMax {
		if len(data) < 4 {
			return ReplacementChar, 2
		}

		c2 := uint32(data[2])<<8 | uint32(data[3])
		if c2 >= LowSurrogateMin && c2 <= LowSurrogateMax {
			// Valid surrogate pair
			r = rune(((c & 0x3FF) << 10) + (c2 & 0x3FF) + SurrogateOffset)
			return r, 4
		}

		// Invalid surrogate pair
		return ReplacementChar, 2
	}

	// Single UTF-16 code unit
	return rune(c), 2
}

// UTF16ToUTF8 converts UTF-16 encoded data to UTF-8.
// enc specifies whether the input is UTF16LE or UTF16BE.
func UTF16ToUTF8(data []byte, enc Encoding) []byte {
	if len(data) == 0 {
		return nil
	}

	// Estimate output size (max growth: UTF-16 to UTF-8 is 2x)
	result := make([]byte, 0, len(data)*2)

	i := 0
	for i < len(data) {
		var r rune
		var size int

		if enc == UTF16LE {
			r, size = DecodeUTF16LE(data[i:])
		} else {
			r, size = DecodeUTF16BE(data[i:])
		}

		if size == 0 {
			break
		}

		result = AppendRune(result, r)
		i += size
	}

	return result
}

// UTF8ToUTF16 converts UTF-8 encoded data to UTF-16.
// enc specifies whether the output should be UTF16LE or UTF16BE.
func UTF8ToUTF16(data []byte, enc Encoding) []byte {
	if len(data) == 0 {
		return nil
	}

	// Estimate output size
	result := make([]byte, 0, len(data)*2)
	buf := make([]byte, 4)

	i := 0
	for i < len(data) {
		r, size := DecodeRune(data[i:])
		if size == 0 {
			break
		}

		var n int
		if enc == UTF16LE {
			n = EncodeUTF16LE(buf, r)
		} else {
			n = EncodeUTF16BE(buf, r)
		}

		result = append(result, buf[:n]...)
		i += size
	}

	return result
}

// DetectBOM checks for a byte order mark at the beginning of data.
// It returns the detected encoding and true if a BOM was found.
// If no BOM is found, it returns UTF8 and false.
func DetectBOM(data []byte) (enc Encoding, hasBOM bool) {
	if len(data) < 2 {
		return UTF8, false
	}

	if data[0] == BOM_BE_FIRST && data[1] == BOM_BE_SECOND {
		return UTF16BE, true
	}

	if data[0] == BOM_LE_FIRST && data[1] == BOM_LE_SECOND {
		return UTF16LE, true
	}

	return UTF8, false
}

// StripBOM removes the byte order mark from the beginning of data if present.
// It returns the data without BOM and the detected encoding.
func StripBOM(data []byte) ([]byte, Encoding) {
	enc, hasBOM := DetectBOM(data)
	if hasBOM {
		return data[2:], enc
	}
	return data, UTF8
}

// SwapEndian swaps the byte order of UTF-16 encoded data in place.
// This is used to convert between UTF-16LE and UTF-16BE.
func SwapEndian(data []byte) {
	for i := 0; i < len(data)-1; i += 2 {
		data[i], data[i+1] = data[i+1], data[i]
	}
}

// UTF16CharCount returns the number of UTF-16 characters in the given byte slice.
// Surrogate pairs count as a single character.
func UTF16CharCount(data []byte, enc Encoding, nChar int) int {
	if len(data) < 2 {
		return 0
	}

	count := 0
	i := 0

	for i < len(data)-1 && count < nChar {
		c := readUint16(data[i:], enc)
		i += 2

		if isHighSurrogate(c) && i < len(data)-1 && isLowSurrogate(readUint16(data[i:], enc)) {
			i += 2
		}

		count++
	}

	return count
}

func readUint16(data []byte, enc Encoding) uint16 {
	if enc == UTF16LE {
		return binary.LittleEndian.Uint16(data)
	}
	return binary.BigEndian.Uint16(data)
}

func isHighSurrogate(c uint16) bool {
	return c >= HighSurrogateMin && c < LowSurrogateMin
}

func isLowSurrogate(c uint16) bool {
	return c >= LowSurrogateMin && c < SurrogateMax+1
}

// UTF16ByteLen returns the number of bytes needed to encode nChar UTF-16 characters
// from the given data.
func UTF16ByteLen(data []byte, enc Encoding, nChar int) int {
	if len(data) < 2 || nChar <= 0 {
		return 0
	}

	count := 0
	i := 0

	for i < len(data)-1 && count < nChar {
		c := readUint16(data[i:], enc)
		i += 2

		if isHighSurrogate(c) && i < len(data)-1 && isLowSurrogate(readUint16(data[i:], enc)) {
			i += 2
		}

		count++
	}

	return i
}
