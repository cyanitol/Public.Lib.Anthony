// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package utf

import (
	"bytes"
	"testing"
)

// ---------------------------------------------------------------------------
// Condition: DecodeUTF16LE — high-surrogate range guard
//   utf16.go: `c >= HighSurrogateMin && c <= HighSurrogateMax`
//
//   A = c >= HighSurrogateMin   (0xD800)
//   B = c <= HighSurrogateMax   (0xDBFF)
//
//   Surrogate-pair path taken only when A && B is true.
//
//   Case 1 (A=F): c < 0xD800 → plain BMP character returned
//   Case 2 (A=T, B=F): c > 0xDBFF (low-surrogate range) → NOT a high surrogate
//   Case 3 (A=T, B=T): valid high surrogate → surrogate-pair path entered
// ---------------------------------------------------------------------------

func TestMCDC_DecodeUTF16LE_HighSurrogate_BelowRange(t *testing.T) {
	// Case 1: A=F — c=0xD7FF, below HighSurrogateMin
	// LE encoding of 0xD7FF: [0xFF, 0xD7]
	data := []byte{0xFF, 0xD7}
	r, size := DecodeUTF16LE(data)
	if size != 2 || r != 0xD7FF {
		t.Errorf("MCDC case1: 0xD7FF LE: want rune=0xD7FF size=2; got rune=%U size=%d", r, size)
	}
}

func TestMCDC_DecodeUTF16LE_HighSurrogate_LowSurrogateCode(t *testing.T) {
	// Case 2: A=T, B=F — c=0xDC00 (LowSurrogateMin), above HighSurrogateMax
	// LE encoding: [0x00, 0xDC]
	// Not a valid high surrogate → treated as plain code unit
	data := []byte{0x00, 0xDC}
	r, size := DecodeUTF16LE(data)
	if size != 2 || r != 0xDC00 {
		t.Errorf("MCDC case2: 0xDC00 LE: want rune=0xDC00 size=2; got rune=%U size=%d", r, size)
	}
}

func TestMCDC_DecodeUTF16LE_HighSurrogate_ValidPair(t *testing.T) {
	// Case 3: A=T, B=T — c=0xD800 + low surrogate 0xDC00 → U+10000
	// LE: high=[0x00, 0xD8], low=[0x00, 0xDC]
	data := []byte{0x00, 0xD8, 0x00, 0xDC}
	r, size := DecodeUTF16LE(data)
	if size != 4 || r != 0x10000 {
		t.Errorf("MCDC case3: surrogate pair: want rune=U+10000 size=4; got rune=%U size=%d", r, size)
	}
}

// ---------------------------------------------------------------------------
// Condition: DecodeUTF16LE — low-surrogate validity check (inner guard)
//   utf16.go: `c2 >= LowSurrogateMin && c2 <= LowSurrogateMax`
//
//   A = c2 >= LowSurrogateMin   (0xDC00)
//   B = c2 <= LowSurrogateMax   (0xDFFF)
//
//   Valid surrogate pair only when A && B is true.
//
//   Case 1 (A=F): c2 < 0xDC00 → invalid pair → ReplacementChar
//   Case 2 (A=T, B=T): valid low surrogate → decoded rune returned
// ---------------------------------------------------------------------------

func TestMCDC_DecodeUTF16LE_LowSurrogate_BelowRange(t *testing.T) {
	// Case 1: A=F — c2=0xD800 (another high surrogate, not a low surrogate)
	// LE: high=[0x00, 0xD8], second=[0x00, 0xD8]
	data := []byte{0x00, 0xD8, 0x00, 0xD8}
	r, size := DecodeUTF16LE(data)
	if r != ReplacementChar {
		t.Errorf("MCDC case1: invalid low-surrogate must return ReplacementChar; got rune=%U size=%d", r, size)
	}
}

func TestMCDC_DecodeUTF16LE_LowSurrogate_ValidRange(t *testing.T) {
	// Case 2: A=T, B=T — c2=0xDFFF (top of low surrogate range)
	// high=0xD800 [0x00, 0xD8], low=0xDFFF [0xFF, 0xDF]
	// decoded rune: ((0xD800&0x3FF)<<10)+(0xDFFF&0x3FF)+0x10000 = 0+0x3FF+0x10000 = 0x103FF
	data := []byte{0x00, 0xD8, 0xFF, 0xDF}
	r, size := DecodeUTF16LE(data)
	if size != 4 || r != 0x103FF {
		t.Errorf("MCDC case2: top of low-surrogate range: want U+103FF size=4; got rune=%U size=%d", r, size)
	}
}

// ---------------------------------------------------------------------------
// Condition: DecodeUTF16BE — high-surrogate range guard
//   utf16.go: `c >= HighSurrogateMin && c <= HighSurrogateMax`
//
//   A = c >= HighSurrogateMin
//   B = c <= HighSurrogateMax
//
//   Case 1 (A=F): plain BMP character
//   Case 2 (A=T, B=F): low-surrogate code unit → not a high surrogate
//   Case 3 (A=T, B=T): valid high surrogate entered
// ---------------------------------------------------------------------------

func TestMCDC_DecodeUTF16BE_HighSurrogate_BelowRange(t *testing.T) {
	// Case 1: A=F — c=0x0041 ('A')
	// BE encoding: [0x00, 0x41]
	data := []byte{0x00, 0x41}
	r, size := DecodeUTF16BE(data)
	if size != 2 || r != 'A' {
		t.Errorf("MCDC case1: 'A' BE: want rune='A' size=2; got rune=%U size=%d", r, size)
	}
}

func TestMCDC_DecodeUTF16BE_HighSurrogate_LowCode(t *testing.T) {
	// Case 2: A=T, B=F — c=0xDC00 (above HighSurrogateMax 0xDBFF)
	// BE: [0xDC, 0x00]
	data := []byte{0xDC, 0x00}
	r, size := DecodeUTF16BE(data)
	if size != 2 || r != 0xDC00 {
		t.Errorf("MCDC case2: 0xDC00 BE: want rune=0xDC00 size=2; got rune=%U size=%d", r, size)
	}
}

func TestMCDC_DecodeUTF16BE_HighSurrogate_ValidPair(t *testing.T) {
	// Case 3: A=T, B=T — valid surrogate pair BE: high=0xD800, low=0xDC00 → U+10000
	// BE: [0xD8, 0x00, 0xDC, 0x00]
	data := []byte{0xD8, 0x00, 0xDC, 0x00}
	r, size := DecodeUTF16BE(data)
	if size != 4 || r != 0x10000 {
		t.Errorf("MCDC case3: BE surrogate pair: want U+10000 size=4; got rune=%U size=%d", r, size)
	}
}

// ---------------------------------------------------------------------------
// Condition: EncodeUTF16LE — `r <= 0xFFFF`
//   (single sub-condition distinguishing BMP vs surrogate-pair encoding)
//
//   Case 1 (A=T): BMP character → 2 bytes
//   Case 2 (A=F): supplementary character → 4 bytes (surrogate pair)
// ---------------------------------------------------------------------------

func TestMCDC_EncodeUTF16LE_BMP(t *testing.T) {
	// Case 1: A=T — U+00E9 (é)
	buf := make([]byte, 4)
	n := EncodeUTF16LE(buf, 0x00E9)
	if n != 2 || buf[0] != 0xE9 || buf[1] != 0x00 {
		t.Errorf("MCDC case1: BMP LE: want 2 bytes [0xE9 0x00]; got n=%d buf=%v", n, buf[:n])
	}
}

func TestMCDC_EncodeUTF16LE_Supplementary(t *testing.T) {
	// Case 2: A=F — U+1F600 (😀)
	buf := make([]byte, 4)
	n := EncodeUTF16LE(buf, 0x1F600)
	if n != 4 {
		t.Errorf("MCDC case2: supplementary LE: want 4 bytes; got n=%d", n)
	}
	// Round-trip verify
	r, size := DecodeUTF16LE(buf[:4])
	if r != 0x1F600 || size != 4 {
		t.Errorf("MCDC case2: round-trip: want U+1F600 size=4; got rune=%U size=%d", r, size)
	}
}

// ---------------------------------------------------------------------------
// Condition: EncodeUTF16BE — `r <= 0xFFFF`
//
//   Case 1 (A=T): BMP character → 2 bytes
//   Case 2 (A=F): supplementary character → 4 bytes
// ---------------------------------------------------------------------------

func TestMCDC_EncodeUTF16BE_BMP(t *testing.T) {
	// Case 1: A=T — U+00E9
	buf := make([]byte, 4)
	n := EncodeUTF16BE(buf, 0x00E9)
	if n != 2 || buf[0] != 0x00 || buf[1] != 0xE9 {
		t.Errorf("MCDC case1: BMP BE: want 2 bytes [0x00 0xE9]; got n=%d buf=%v", n, buf[:n])
	}
}

func TestMCDC_EncodeUTF16BE_Supplementary(t *testing.T) {
	// Case 2: A=F — U+1F600
	buf := make([]byte, 4)
	n := EncodeUTF16BE(buf, 0x1F600)
	if n != 4 {
		t.Errorf("MCDC case2: supplementary BE: want 4 bytes; got n=%d", n)
	}
	r, size := DecodeUTF16BE(buf[:4])
	if r != 0x1F600 || size != 4 {
		t.Errorf("MCDC case2: round-trip: want U+1F600 size=4; got rune=%U size=%d", r, size)
	}
}

// ---------------------------------------------------------------------------
// Condition: isHighSurrogate
//   utf16.go: `c >= HighSurrogateMin && c < LowSurrogateMin`
//
//   A = c >= HighSurrogateMin  (0xD800)
//   B = c < LowSurrogateMin    (0xDC00)
//
//   Case 1 (A=F): c < 0xD800 → false
//   Case 2 (A=T, B=F): c >= 0xDC00 → false (low surrogate)
//   Case 3 (A=T, B=T): 0xD800 <= c < 0xDC00 → true
// ---------------------------------------------------------------------------

func TestMCDC_isHighSurrogate_BelowMin(t *testing.T) {
	// Case 1: A=F
	if isHighSurrogate(0xD7FF) {
		t.Error("MCDC case1: 0xD7FF must not be a high surrogate")
	}
}

func TestMCDC_isHighSurrogate_LowSurrogateValue(t *testing.T) {
	// Case 2: A=T, B=F — c=0xDC00 >= HighSurrogateMin but >= LowSurrogateMin
	if isHighSurrogate(0xDC00) {
		t.Error("MCDC case2: 0xDC00 must not be a high surrogate")
	}
}

func TestMCDC_isHighSurrogate_ValidHigh(t *testing.T) {
	// Case 3: A=T, B=T
	if !isHighSurrogate(0xD800) {
		t.Error("MCDC case3: 0xD800 must be a high surrogate")
	}
	if !isHighSurrogate(0xDBFF) {
		t.Error("MCDC case3b: 0xDBFF must be a high surrogate")
	}
}

// ---------------------------------------------------------------------------
// Condition: isLowSurrogate
//   utf16.go: `c >= LowSurrogateMin && c < SurrogateMax+1`
//
//   A = c >= LowSurrogateMin  (0xDC00)
//   B = c < SurrogateMax+1    (0xE000)
//
//   Case 1 (A=F): c < 0xDC00 → false
//   Case 2 (A=T, B=F): c >= 0xE000 → false
//   Case 3 (A=T, B=T): 0xDC00 <= c <= 0xDFFF → true
// ---------------------------------------------------------------------------

func TestMCDC_isLowSurrogate_BelowMin(t *testing.T) {
	// Case 1: A=F
	if isLowSurrogate(0xDBFF) {
		t.Error("MCDC case1: 0xDBFF must not be a low surrogate")
	}
}

func TestMCDC_isLowSurrogate_AboveMax(t *testing.T) {
	// Case 2: A=T, B=F — c=0xE000
	if isLowSurrogate(0xE000) {
		t.Error("MCDC case2: 0xE000 must not be a low surrogate")
	}
}

func TestMCDC_isLowSurrogate_ValidLow(t *testing.T) {
	// Case 3: A=T, B=T
	if !isLowSurrogate(0xDC00) {
		t.Error("MCDC case3: 0xDC00 must be a low surrogate")
	}
	if !isLowSurrogate(0xDFFF) {
		t.Error("MCDC case3b: 0xDFFF must be a low surrogate")
	}
}

// ---------------------------------------------------------------------------
// Condition: DetectBOM — BE BOM guard
//   utf16.go: `data[0] == BOM_BE_FIRST && data[1] == BOM_BE_SECOND`
//
//   A = data[0] == BOM_BE_FIRST   (0xFE)
//   B = data[1] == BOM_BE_SECOND  (0xFF)
//
//   Detects UTF-16BE BOM only when A && B is true.
//
//   Case 1 (A=F): first byte not 0xFE → not BE BOM
//   Case 2 (A=T, B=F): first byte 0xFE, second not 0xFF → not BE BOM
//   Case 3 (A=T, B=T): both match → UTF16BE detected
// ---------------------------------------------------------------------------

func TestMCDC_DetectBOM_BE_FirstByteWrong(t *testing.T) {
	// Case 1: A=F
	enc, hasBOM := DetectBOM([]byte{0x00, 0xFF})
	if hasBOM || enc != UTF8 {
		t.Errorf("MCDC case1: wrong first byte must not detect BE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

func TestMCDC_DetectBOM_BE_SecondByteWrong(t *testing.T) {
	// Case 2: A=T, B=F — first=0xFE but second != 0xFF
	// Note: 0xFE 0x00 is not LE BOM either (LE is FF FE), so result is UTF8
	enc, hasBOM := DetectBOM([]byte{0xFE, 0x00})
	if hasBOM {
		t.Errorf("MCDC case2: 0xFE 0x00 must not detect BE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

func TestMCDC_DetectBOM_BE_BothMatch(t *testing.T) {
	// Case 3: A=T, B=T → UTF16BE
	enc, hasBOM := DetectBOM([]byte{0xFE, 0xFF})
	if !hasBOM || enc != UTF16BE {
		t.Errorf("MCDC case3: 0xFE 0xFF must detect UTF16BE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

// ---------------------------------------------------------------------------
// Condition: DetectBOM — LE BOM guard
//   utf16.go: `data[0] == BOM_LE_FIRST && data[1] == BOM_LE_SECOND`
//
//   A = data[0] == BOM_LE_FIRST   (0xFF)
//   B = data[1] == BOM_LE_SECOND  (0xFE)
//
//   Case 1 (A=F): first byte not 0xFF → not LE BOM
//   Case 2 (A=T, B=F): first byte 0xFF, second not 0xFE → not LE BOM
//   Case 3 (A=T, B=T): both match → UTF16LE detected
// ---------------------------------------------------------------------------

func TestMCDC_DetectBOM_LE_FirstByteWrong(t *testing.T) {
	// Case 1: A=F — 0x00 0xFE
	enc, hasBOM := DetectBOM([]byte{0x00, 0xFE})
	if hasBOM || enc != UTF8 {
		t.Errorf("MCDC case1: wrong first byte must not detect LE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

func TestMCDC_DetectBOM_LE_SecondByteWrong(t *testing.T) {
	// Case 2: A=T, B=F — first=0xFF, second != 0xFE
	// 0xFF 0x00 is not a BOM
	enc, hasBOM := DetectBOM([]byte{0xFF, 0x00})
	if hasBOM {
		t.Errorf("MCDC case2: 0xFF 0x00 must not detect LE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

func TestMCDC_DetectBOM_LE_BothMatch(t *testing.T) {
	// Case 3: A=T, B=T → UTF16LE
	enc, hasBOM := DetectBOM([]byte{0xFF, 0xFE})
	if !hasBOM || enc != UTF16LE {
		t.Errorf("MCDC case3: 0xFF 0xFE must detect UTF16LE BOM; got enc=%v hasBOM=%v", enc, hasBOM)
	}
}

// ---------------------------------------------------------------------------
// Condition: GetVarint — 1-byte fast path
//   varint.go: `buf[0] < 0x80`
//
//   A = buf[0] < 0x80  (high bit not set → 1-byte varint)
//
//   Case 1 (A=T): buf[0]=0x7F → value=0x7F, bytes=1
//   Case 2 (A=F): buf[0]=0x80 → falls through to longer path
// ---------------------------------------------------------------------------

func TestMCDC_GetVarint_OneByte(t *testing.T) {
	// Case 1: A=T
	v, n := GetVarint([]byte{0x7F})
	if v != 0x7F || n != 1 {
		t.Errorf("MCDC case1: 1-byte varint: want v=0x7F n=1; got v=%d n=%d", v, n)
	}
}

func TestMCDC_GetVarint_NotOneByte(t *testing.T) {
	// Case 2: A=F — buf[0] has high bit set → 2+ byte varint
	// 0x81, 0x00 encodes value 128
	v, n := GetVarint([]byte{0x81, 0x00})
	if n < 2 {
		t.Errorf("MCDC case2: multi-byte varint must read >1 byte; got n=%d v=%d", n, v)
	}
}

// ---------------------------------------------------------------------------
// Condition: GetVarint — 2-byte fast path
//   varint.go: `len(buf) >= 2 && buf[1] < 0x80`
//
//   A = len(buf) >= 2
//   B = buf[1] < 0x80  (second byte has high bit clear → end of varint)
//
//   Case 1 (A=F): buf length < 2 after 1-byte check fails → falls to slow path
//   Case 2 (A=T, B=F): second byte has high bit set → not 2-byte varint
//   Case 3 (A=T, B=T): valid 2-byte varint
// ---------------------------------------------------------------------------

func TestMCDC_GetVarint_TwoByte_TooShort(t *testing.T) {
	// Case 1: A=F — only 1 byte with high bit set → slow path returns 0,0
	v, n := GetVarint([]byte{0x80})
	if n != 0 {
		t.Errorf("MCDC case1: single continuation byte must fail; got v=%d n=%d", v, n)
	}
}

func TestMCDC_GetVarint_TwoByte_SecondByteContinues(t *testing.T) {
	// Case 2: A=T, B=F — both bytes have high bit set → 3+ byte varint
	// 0x81, 0x80, 0x00 encodes a 3-byte varint
	v, n := GetVarint([]byte{0x81, 0x80, 0x00})
	if n != 3 {
		t.Errorf("MCDC case2: 3-byte varint: want n=3; got n=%d v=%d", n, v)
	}
}

func TestMCDC_GetVarint_TwoByte_Valid(t *testing.T) {
	// Case 3: A=T, B=T — valid 2-byte varint: 0x81 0x00 = 128
	v, n := GetVarint([]byte{0x81, 0x00})
	if v != 128 || n != 2 {
		t.Errorf("MCDC case3: 2-byte varint 128: want v=128 n=2; got v=%d n=%d", v, n)
	}
}

// ---------------------------------------------------------------------------
// Condition: PutVarint — first fast path `v <= 0x7f`
//
//   A = v <= 0x7f
//
//   Case 1 (A=T): v=0 → 1 byte output
//   Case 2 (A=F): v=0x80 → 2+ byte output
// ---------------------------------------------------------------------------

func TestMCDC_PutVarint_SmallValue(t *testing.T) {
	// Case 1: A=T
	buf := make([]byte, 9)
	n := PutVarint(buf, 0x7F)
	if n != 1 || buf[0] != 0x7F {
		t.Errorf("MCDC case1: v=0x7F → want 1 byte 0x7F; got n=%d buf[0]=%x", n, buf[0])
	}
}

func TestMCDC_PutVarint_LargerValue(t *testing.T) {
	// Case 2: A=F — v=0x80 (128) falls to second check or slow path
	buf := make([]byte, 9)
	n := PutVarint(buf, 0x80)
	if n < 2 {
		t.Errorf("MCDC case2: v=0x80 must need >1 byte; got n=%d", n)
	}
	// Verify round-trip
	v, k := GetVarint(buf[:n])
	if v != 0x80 || k != n {
		t.Errorf("MCDC case2: round-trip: want v=128 n=%d; got v=%d n=%d", n, v, k)
	}
}

// ---------------------------------------------------------------------------
// Condition: PutVarint — second fast path `v <= 0x3fff`
//
//   A = v <= 0x3fff
//
//   Case 1 (A=T): v=0x100 → 2 bytes output (0x80 < v <= 0x3fff)
//   Case 2 (A=F): v=0x4000 → slow path (3+ bytes)
// ---------------------------------------------------------------------------

func TestMCDC_PutVarint_TwoByteRange(t *testing.T) {
	// Case 1: A=T — v=0x100 is in [0x80, 0x3fff]
	buf := make([]byte, 9)
	n := PutVarint(buf, 0x100)
	if n != 2 {
		t.Errorf("MCDC case1: v=0x100 must need 2 bytes; got n=%d", n)
	}
	v, k := GetVarint(buf[:n])
	if v != 0x100 || k != 2 {
		t.Errorf("MCDC case1: round-trip: want v=256 n=2; got v=%d n=%d", v, k)
	}
}

func TestMCDC_PutVarint_ThreePlusByteRange(t *testing.T) {
	// Case 2: A=F — v=0x4000 needs 3 bytes
	buf := make([]byte, 9)
	n := PutVarint(buf, 0x4000)
	if n < 3 {
		t.Errorf("MCDC case2: v=0x4000 must need ≥3 bytes; got n=%d", n)
	}
	v, k := GetVarint(buf[:n])
	if v != 0x4000 || k != n {
		t.Errorf("MCDC case2: round-trip: want v=0x4000; got v=%d n=%d", v, k)
	}
}

// ---------------------------------------------------------------------------
// Condition: VarintLen — 9-byte path `v&(uint64(0xff000000)<<32) != 0`
//
//   A = v&(uint64(0xff000000)<<32) != 0  (high byte of 64-bit value is set)
//
//   Case 1 (A=T): very large value needs 9 bytes
//   Case 2 (A=F): value fits in fewer bytes
// ---------------------------------------------------------------------------

func TestMCDC_VarintLen_NineByte(t *testing.T) {
	// Case 1: A=T — value with top 8 bits set needs 9 bytes
	const nineByteVal = uint64(0xFF00000000000000)
	n := VarintLen(nineByteVal)
	if n != 9 {
		t.Errorf("MCDC case1: large value must need 9 bytes; got n=%d", n)
	}
	// Also verify PutVarint/GetVarint consistency
	buf := make([]byte, 9)
	written := PutVarint(buf, nineByteVal)
	if written != 9 {
		t.Errorf("MCDC case1: PutVarint must write 9 bytes; got %d", written)
	}
	v, k := GetVarint(buf[:written])
	if v != nineByteVal || k != 9 {
		t.Errorf("MCDC case1: round-trip: want v=%x n=9; got v=%x n=%d", nineByteVal, v, k)
	}
}

func TestMCDC_VarintLen_FewerThanNineBytes(t *testing.T) {
	// Case 2: A=F — v=0x3FFF needs only 2 bytes
	n := VarintLen(0x3FFF)
	if n != 2 {
		t.Errorf("MCDC case2: v=0x3FFF must need 2 bytes; got n=%d", n)
	}
}

// ---------------------------------------------------------------------------
// Condition: UTF16ToUTF8 — encoding dispatch `enc == UTF16LE`
//   utf16.go: `if enc == UTF16LE { DecodeUTF16LE } else { DecodeUTF16BE }`
//
//   A = enc == UTF16LE
//
//   Case 1 (A=T): LE encoding → DecodeUTF16LE used
//   Case 2 (A=F): BE encoding → DecodeUTF16BE used
//
//   Verified by checking that the same bytes are decoded differently by each path.
// ---------------------------------------------------------------------------

func TestMCDC_UTF16ToUTF8_LEPath(t *testing.T) {
	// Case 1: A=T — 'A' in LE = [0x41, 0x00]
	data := []byte{0x41, 0x00}
	result := UTF16ToUTF8(data, UTF16LE)
	if !bytes.Equal(result, []byte{'A'}) {
		t.Errorf("MCDC case1: UTF16LE 'A': want [0x41]; got %v", result)
	}
}

func TestMCDC_UTF16ToUTF8_BEPath(t *testing.T) {
	// Case 2: A=F — 'A' in BE = [0x00, 0x41]
	data := []byte{0x00, 0x41}
	result := UTF16ToUTF8(data, UTF16BE)
	if !bytes.Equal(result, []byte{'A'}) {
		t.Errorf("MCDC case2: UTF16BE 'A': want [0x41]; got %v", result)
	}
}
