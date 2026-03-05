// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package utf provides UTF-8 and UTF-16 encoding/decoding utilities for SQLite.
//
// This package implements the core UTF functionality needed by the SQLite engine,
// including character encoding conversion, validation, and string operations.
package utf

import (
	"unicode/utf8"
)

// Rune representing an invalid UTF-8 sequence
const (
	// ReplacementChar is used to replace invalid UTF-8 sequences
	ReplacementChar = '\uFFFD'

	// MaxRune is the maximum valid Unicode code point
	MaxRune = '\U0010FFFF'
)

// utf8Trans1 is a lookup table used to help decode the first byte of
// a multi-byte UTF8 character. This matches the sqlite3Utf8Trans1 table.
var utf8Trans1 = [64]byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
}

// AppendRune appends the UTF-8 encoding of r to buf and returns the extended buffer.
// This follows SQLite's WRITE_UTF8 macro logic.
func AppendRune(buf []byte, r rune) []byte {
	if r < 0x00080 {
		return append(buf, byte(r))
	} else if r < 0x00800 {
		return append(buf,
			0xC0|byte(r>>6),
			0x80|byte(r&0x3F))
	} else if r < 0x10000 {
		return append(buf,
			0xE0|byte(r>>12),
			0x80|byte((r>>6)&0x3F),
			0x80|byte(r&0x3F))
	} else {
		return append(buf,
			0xF0|byte(r>>18),
			0x80|byte((r>>12)&0x3F),
			0x80|byte((r>>6)&0x3F),
			0x80|byte(r&0x3F))
	}
}

// EncodeRune writes into buf the UTF-8 encoding of the rune.
// It returns the number of bytes written.
// buf must be at least 4 bytes long.
func EncodeRune(buf []byte, r rune) int {
	if r < 0x00080 {
		buf[0] = byte(r)
		return 1
	}
	if r < 0x00800 {
		buf[0] = 0xC0 | byte(r>>6)
		buf[1] = 0x80 | byte(r&0x3F)
		return 2
	}
	if r < 0x10000 {
		buf[0] = 0xE0 | byte(r>>12)
		buf[1] = 0x80 | byte((r>>6)&0x3F)
		buf[2] = 0x80 | byte(r&0x3F)
		return 3
	}
	buf[0] = 0xF0 | byte(r>>18)
	buf[1] = 0x80 | byte((r>>12)&0x3F)
	buf[2] = 0x80 | byte((r>>6)&0x3F)
	buf[3] = 0x80 | byte(r&0x3F)
	return 4
}

// DecodeRune decodes a single UTF-8 encoded rune from the start of data.
// It returns the rune and the number of bytes consumed.
// Invalid UTF-8 sequences are replaced with ReplacementChar.
//
// This follows SQLite's utf8Read logic with these rules:
// - Never allow 7-bit characters (0x00-0x7f) to be encoded as multi-byte
// - Never allow UTF-16 surrogate values (0xD800-0xDFFF)
// - Replace invalid sequences with 0xFFFD
func DecodeRune(data []byte) (r rune, size int) {
	if len(data) == 0 {
		return 0, 0
	}
	c := uint32(data[0])
	if c < 0xC0 {
		return rune(c), 1
	}
	return decodeMultiByte(data, c)
}

// decodeMultiByte handles multi-byte UTF-8 sequences.
func decodeMultiByte(data []byte, c uint32) (rune, int) {
	c = uint32(utf8Trans1[c-0xC0])
	size := 1
	for size < len(data) && (data[size]&0xC0) == 0x80 {
		c = (c << 6) + uint32(data[size]&0x3F)
		size++
	}
	if isInvalidUTF8(c) {
		return ReplacementChar, size
	}
	return rune(c), size
}

// isInvalidUTF8 checks if a decoded value is invalid UTF-8.
func isInvalidUTF8(c uint32) bool {
	return c < 0x80 || (c&0xFFFFF800) == 0xD800 || (c&0xFFFFFFFE) == 0xFFFE
}

// DecodeRuneLimited decodes a single UTF-8 rune from data with a maximum
// of n bytes. Returns the rune and the number of bytes used (1-4).
// This is less strict than DecodeRune and doesn't validate invalid UTF-8.
func DecodeRuneLimited(data []byte, n int) (r rune, size int) {
	if n <= 0 || len(data) == 0 {
		return 0, 0
	}

	c := uint32(data[0])
	if c < 0xC0 {
		return rune(c), 1
	}

	return decodeMultiByteLimited(data, c, n)
}

// decodeMultiByteLimited decodes a multi-byte UTF-8 sequence with a limit.
func decodeMultiByteLimited(data []byte, c uint32, n int) (rune, int) {
	c = uint32(utf8Trans1[c-0xC0])
	if n > 4 {
		n = 4
	}

	size := 1
	for size < n && size < len(data) && (data[size]&0xC0) == 0x80 {
		c = (c << 6) + uint32(data[size]&0x3F)
		size++
	}

	return rune(c), size
}

// CharCount returns the number of UTF-8 encoded characters in the string.
// If nByte < 0, counts until the first 0x00 byte.
// If nByte >= 0, counts characters in the first nByte bytes or until 0x00.
func CharCount(s string, nByte int) int {
	data := []byte(s)
	limit := computeCharCountLimit(len(data), nByte)
	return countUTF8Chars(data, limit)
}

// computeCharCountLimit computes the limit for character counting.
func computeCharCountLimit(dataLen, nByte int) int {
	if nByte < 0 {
		return dataLen
	}
	if nByte > dataLen {
		return dataLen
	}
	return nByte
}

// countUTF8Chars counts UTF-8 characters up to limit or null byte.
func countUTF8Chars(data []byte, limit int) int {
	count := 0
	i := 0
	for i < limit && data[i] != 0 {
		i += skipUTF8Sequence(data[i:], limit-i)
		count++
	}
	return count
}

// skipUTF8Sequence skips a single UTF-8 character sequence.
func skipUTF8Sequence(data []byte, limit int) int {
	if data[0] < 0xC0 {
		return 1
	}

	i := 1
	for i < limit && i < len(data) && (data[i]&0xC0) == 0x80 {
		i++
	}
	return i
}

// CharCountBytes is like CharCount but works with byte slices.
func CharCountBytes(data []byte, nByte int) int {
	limit := computeCharCountLimit(len(data), nByte)
	return countUTF8Chars(data, limit)
}

// ValidateUTF8 validates that data contains valid UTF-8.
// It returns true if valid, false otherwise.
func ValidateUTF8(data []byte) bool {
	return utf8.Valid(data)
}

// ToValidUTF8 converts data to valid UTF-8 by replacing invalid sequences.
// This is used to clean up potentially malformed UTF-8 strings.
func ToValidUTF8(data []byte) []byte {
	if utf8.Valid(data) {
		return data
	}

	result := make([]byte, 0, len(data))
	for len(data) > 0 {
		r, size := DecodeRune(data)
		result = AppendRune(result, r)
		if size == 0 {
			break
		}
		data = data[size:]
	}
	return result
}

// UpperToLower maps ASCII uppercase letters to lowercase.
// Non-ASCII characters are unchanged.
var UpperToLower [256]byte

func init() {
	// Initialize UpperToLower table
	for i := 0; i < 256; i++ {
		UpperToLower[i] = byte(i)
	}
	for i := 'A'; i <= 'Z'; i++ {
		UpperToLower[i] = byte(i - 'A' + 'a')
	}
}

// ToUpper converts a rune to uppercase (ASCII only for NOCASE collation)
func ToUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 'a' + 'A'
	}
	return r
}

// ToLower converts a rune to lowercase (ASCII only for NOCASE collation)
func ToLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r - 'A' + 'a'
	}
	return r
}

// IsSpace returns true if the byte is an ASCII whitespace character.
func IsSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// IsDigit returns true if the byte is an ASCII digit.
func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// IsXDigit returns true if the byte is an ASCII hexadecimal digit.
func IsXDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// HexToInt converts a hexadecimal character to its integer value.
// The input must be a valid hex character (0-9, a-f, A-F).
func HexToInt(h byte) byte {
	// This works because:
	// '0'-'9': h += 9*(1&(h>>6))  -> adds 0
	// 'A'-'F': h += 9*(1&(h>>6))  -> adds 9, so 'A'(65) -> 10
	// 'a'-'f': h += 9*(1&(h>>6))  -> adds 9, so 'a'(97) -> 10
	h += 9 * (1 & (h >> 6))
	return h & 0xf
}

// FoldCase performs case folding on a string for case-insensitive comparison.
// This only folds ASCII characters (A-Z -> a-z).
func FoldCase(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		result[i] = UpperToLower[s[i]]
	}
	return string(result)
}

// EqualFold reports whether s and t are equal under Unicode case-folding,
// but only for ASCII characters (SQLite NOCASE collation behavior).
func EqualFold(s, t string) bool {
	if len(s) != len(t) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if UpperToLower[s[i]] != UpperToLower[t[i]] {
			return false
		}
	}
	return true
}

// RuneLen returns the number of bytes required to encode the rune in UTF-8.
func RuneLen(r rune) int {
	if r < 0x80 {
		return 1
	} else if r < 0x800 {
		return 2
	} else if r < 0x10000 {
		return 3
	}
	return 4
}

// FullRune reports whether the bytes in p begin with a full UTF-8 encoding of a rune.
func FullRune(p []byte) bool {
	if len(p) == 0 {
		return false
	}

	c := p[0]
	if c < 0x80 {
		return true
	}
	if c < 0xC0 {
		return false // continuation byte
	}

	// Determine required length
	var need int
	if c < 0xE0 {
		need = 2
	} else if c < 0xF0 {
		need = 3
	} else {
		need = 4
	}

	return len(p) >= need
}
