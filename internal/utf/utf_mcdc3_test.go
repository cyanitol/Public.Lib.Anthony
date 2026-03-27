// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package utf

import (
	"testing"
)

// ---------------------------------------------------------------------------
// StrNICmp (collation.go:222) — 91.7%
// Uncovered: len(a)==0 || len(b)==0 early-return path.
// ---------------------------------------------------------------------------

func TestStrNICmp_EmptyA(t *testing.T) {
	result := StrNICmp("", "abc", 3)
	if result >= 0 {
		t.Errorf("StrNICmp empty a: want negative, got %d", result)
	}
}

func TestStrNICmp_EmptyB(t *testing.T) {
	result := StrNICmp("abc", "", 3)
	if result <= 0 {
		t.Errorf("StrNICmp empty b: want positive, got %d", result)
	}
}

func TestStrNICmp_BothEmpty(t *testing.T) {
	result := StrNICmp("", "", 5)
	if result != 0 {
		t.Errorf("StrNICmp both empty: want 0, got %d", result)
	}
}

// Cover the n <= limit → return 0 branch (n equals or less than min of lens).
func TestStrNICmp_NLessLimit(t *testing.T) {
	// Compare first 2 bytes of "abc" and "abX" — they agree up to n=2
	result := StrNICmp("abc", "abX", 2)
	if result != 0 {
		t.Errorf("StrNICmp n=2 equal prefix: want 0, got %d", result)
	}
}

// ---------------------------------------------------------------------------
// likeImpl / likeMatchPercent (collation.go:268 / 325) — 92.3% / 91.7%
// Uncovered: likeMatchPercent — si >= len(str) break path (% at end of exhausted str).
// ---------------------------------------------------------------------------

func TestLike_PercentAtEnd_EmptyStr(t *testing.T) {
	// Pattern "abc%" against "abc" — % matches empty suffix
	if !Like("abc%", "abc", 0) {
		t.Error("Like 'abc%' vs 'abc': expected true")
	}
}

func TestLike_PercentMiddle_NoMatch(t *testing.T) {
	if Like("a%z", "abc", 0) {
		t.Error("Like 'a%z' vs 'abc': expected false")
	}
}

// ---------------------------------------------------------------------------
// likeAdvanceOneRune (collation.go:347) — 83.3%
// Uncovered: ssize == 0 return -1 path (invalid rune at position).
// Simulate by providing a byte that won't decode as a full rune
// when our DecodeRune implementation returns 0 for empty slice.
// ---------------------------------------------------------------------------

func TestLikeAdvanceOneRune_Empty(t *testing.T) {
	result := likeAdvanceOneRune([]byte{}, 0)
	if result != -1 {
		t.Errorf("likeAdvanceOneRune empty: want -1, got %d", result)
	}
}

func TestLikeAdvanceOneRune_ValidRune(t *testing.T) {
	result := likeAdvanceOneRune([]byte("A"), 0)
	if result != 1 {
		t.Errorf("likeAdvanceOneRune 'A': want 1, got %d", result)
	}
}

func TestLikeAdvanceOneRune_AtEnd(t *testing.T) {
	// si >= len(str)
	result := likeAdvanceOneRune([]byte("A"), 1)
	if result != -1 {
		t.Errorf("likeAdvanceOneRune si>=len: want -1, got %d", result)
	}
}

// ---------------------------------------------------------------------------
// likeMatchLiteral (collation.go:361) — 90.9%
// Uncovered: ssize == 0 return false path (decode fails).
// ---------------------------------------------------------------------------

func TestLikeMatchLiteral_SiAtEnd(t *testing.T) {
	// si >= len(str) → returns ok=false
	newPI, newSI, ok := likeMatchLiteral([]byte("a"), []byte(""), 'a', 1, 0, 0, false)
	if ok {
		t.Error("likeMatchLiteral si>=len: expected ok=false")
	}
	_ = newPI
	_ = newSI
}

func TestLikeMatchLiteral_Mismatch(t *testing.T) {
	_, _, ok := likeMatchLiteral([]byte("a"), []byte("b"), 'a', 1, 0, 0, false)
	if ok {
		t.Error("likeMatchLiteral mismatch: expected ok=false")
	}
}

func TestLikeMatchLiteral_Match(t *testing.T) {
	_, _, ok := likeMatchLiteral([]byte("a"), []byte("a"), 'a', 1, 0, 0, false)
	if !ok {
		t.Error("likeMatchLiteral match: expected ok=true")
	}
}

// ---------------------------------------------------------------------------
// match / matchStar / matchQuestion / matchCharClass / matchRange /
// parseClassEntry (collation.go:401-538) — 90.0% / 92.3% / 87.5% / 92.3% / 83.3% / 90.0%
// ---------------------------------------------------------------------------

// matchQuestion — si >= len(str) return false.
func TestGlob_QuestionAtEnd(t *testing.T) {
	// Pattern "?" against empty string → false
	if Glob("?", "") {
		t.Error("Glob '?' vs '': expected false")
	}
}

// matchCharClass — si >= len(str) return false.
func TestGlob_CharClassAtEnd(t *testing.T) {
	if Glob("[a]", "") {
		t.Error("Glob '[a]' vs '': expected false")
	}
}

// matchCharClass — pi >= len(pattern) return false.
func TestGlob_CharClassOpenUnclosed(t *testing.T) {
	// Unclosed '[' at end of pattern
	if Glob("[", "a") {
		t.Error("Glob '[' vs 'a': expected false")
	}
}

// matchRange — covers the range check path (sc in range).
func TestGlob_CharClassRange_Match(t *testing.T) {
	if !Glob("[a-z]", "m") {
		t.Error("Glob '[a-z]' vs 'm': expected true")
	}
}

func TestGlob_CharClassRange_NoMatch(t *testing.T) {
	if Glob("[a-z]", "M") {
		t.Error("Glob '[a-z]' vs 'M': expected false (case-sensitive)")
	}
}

// matchRange — csize2 == 0 → return false, false (truncated range).
func TestGlob_CharClassRange_Truncated(t *testing.T) {
	// Pattern "[a-" is truncated — matchRange will get empty tail
	if Glob("[a-", "b") {
		t.Error("Glob '[a-' truncated: expected false")
	}
}

// matchCharClass — invert (^) path.
func TestGlob_CharClassInvert_Match(t *testing.T) {
	if !Glob("[^b]", "a") {
		t.Error("Glob '[^b]' vs 'a': expected true")
	}
}

func TestGlob_CharClassInvert_NoMatch(t *testing.T) {
	if Glob("[^a]", "a") {
		t.Error("Glob '[^a]' vs 'a': expected false")
	}
}

// matchStar — star at end matches anything.
func TestGlob_StarAtEnd(t *testing.T) {
	if !Glob("abc*", "abcdef") {
		t.Error("Glob 'abc*' vs 'abcdef': expected true")
	}
}

// matchStar — star, si >= len(str) break path.
func TestGlob_StarNoMatch(t *testing.T) {
	if Glob("a*z", "abc") {
		t.Error("Glob 'a*z' vs 'abc': expected false")
	}
}

// match — si < len(str) at end of pattern (literal mismatch short).
func TestGlob_LiteralMismatch(t *testing.T) {
	if Glob("abc", "ab") {
		t.Error("Glob 'abc' vs 'ab': expected false")
	}
}

// ---------------------------------------------------------------------------
// UTF8ToUTF16 (utf16.go:175) — 93.8%
// Uncovered: else branch (UTF16BE encoding).
// ---------------------------------------------------------------------------

func TestUTF8ToUTF16_BE(t *testing.T) {
	// "A" = U+0041; BE encoding: [0x00, 0x41]
	input := []byte("A")
	result := UTF8ToUTF16(input, UTF16BE)
	if len(result) != 2 {
		t.Errorf("UTF8ToUTF16 BE 'A': want 2 bytes, got %d", len(result))
	}
	if result[0] != 0x00 || result[1] != 0x41 {
		t.Errorf("UTF8ToUTF16 BE 'A': want [0x00, 0x41], got %v", result)
	}
}

func TestUTF8ToUTF16_LE(t *testing.T) {
	// "A" = U+0041; LE encoding: [0x41, 0x00]
	input := []byte("A")
	result := UTF8ToUTF16(input, UTF16LE)
	if len(result) != 2 {
		t.Errorf("UTF8ToUTF16 LE 'A': want 2 bytes, got %d", len(result))
	}
	if result[0] != 0x41 || result[1] != 0x00 {
		t.Errorf("UTF8ToUTF16 LE 'A': want [0x41, 0x00], got %v", result)
	}
}

func TestUTF8ToUTF16_SurrogatePair_BE(t *testing.T) {
	// U+1F600 (emoji) requires surrogate pair in UTF-16
	input := []byte("\U0001F600")
	result := UTF8ToUTF16(input, UTF16BE)
	if len(result) != 4 {
		t.Errorf("UTF8ToUTF16 BE emoji: want 4 bytes, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// utf16CharByteLen (utf16.go:301) — 83.3%
// Uncovered: surrogate pair path (isHighSurrogate && isLowSurrogate → return 4).
// ---------------------------------------------------------------------------

func TestUtf16CharByteLen_SurrogatePair_LE(t *testing.T) {
	// High surrogate 0xD800 LE: [0x00, 0xD8], Low surrogate 0xDC00 LE: [0x00, 0xDC]
	data := []byte{0x00, 0xD8, 0x00, 0xDC}
	n := utf16CharByteLen(data, UTF16LE)
	if n != 4 {
		t.Errorf("utf16CharByteLen surrogate pair LE: want 4, got %d", n)
	}
}

func TestUtf16CharByteLen_BMP_LE(t *testing.T) {
	// 'A' = 0x0041 LE: [0x41, 0x00]
	data := []byte{0x41, 0x00}
	n := utf16CharByteLen(data, UTF16LE)
	if n != 2 {
		t.Errorf("utf16CharByteLen BMP LE: want 2, got %d", n)
	}
}

func TestUtf16CharByteLen_TooShort(t *testing.T) {
	// Less than 2 bytes → return 0
	data := []byte{0x41}
	n := utf16CharByteLen(data, UTF16LE)
	if n != 0 {
		t.Errorf("utf16CharByteLen too short: want 0, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// ToValidUTF8 (utf8.go:211) — 90.0%
// Uncovered: size == 0 break path (DecodeRune returns 0 for empty remaining).
// ---------------------------------------------------------------------------

func TestToValidUTF8_AlreadyValid(t *testing.T) {
	input := []byte("hello")
	result := ToValidUTF8(input)
	if string(result) != "hello" {
		t.Errorf("ToValidUTF8 valid: want 'hello', got %s", string(result))
	}
}

func TestToValidUTF8_InvalidSequence(t *testing.T) {
	// 0xFF is not valid UTF-8
	input := []byte{0x68, 0xFF, 0x65} // h <invalid> e
	result := ToValidUTF8(input)
	if len(result) == 0 {
		t.Error("ToValidUTF8 invalid seq: expected non-empty result")
	}
}

func TestToValidUTF8_MultibyteValid(t *testing.T) {
	input := []byte("héllo")
	result := ToValidUTF8(input)
	if string(result) != "héllo" {
		t.Errorf("ToValidUTF8 multibyte: want 'héllo', got %s", string(result))
	}
}
