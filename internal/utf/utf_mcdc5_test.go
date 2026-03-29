// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package utf

import (
	"testing"
)

// ---------------------------------------------------------------------------
// likeAdvanceOneRune (collation.go:347) — MC/DC
// Conditions:
//   A: si >= len(str)   → return -1
//   B: ssize == 0       → return -1
//   else                → return si+ssize
// ---------------------------------------------------------------------------

func TestMCDC5_LikeAdvanceOneRune_SiAtEnd(t *testing.T) {
	t.Parallel()
	// A=true: si >= len(str) → -1
	got := likeAdvanceOneRune([]byte("abc"), 3)
	if got != -1 {
		t.Errorf("want -1, got %d", got)
	}
}

func TestMCDC5_LikeAdvanceOneRune_ASCII(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid ASCII → si+1
	got := likeAdvanceOneRune([]byte("abc"), 0)
	if got != 1 {
		t.Errorf("want 1, got %d", got)
	}
}

func TestMCDC5_LikeAdvanceOneRune_Multibyte(t *testing.T) {
	t.Parallel()
	// A=false, B=false: multi-byte UTF-8 'é' (U+00E9) = 2 bytes
	str := []byte("é")
	got := likeAdvanceOneRune(str, 0)
	if got != 2 {
		t.Errorf("want 2 for 'é', got %d", got)
	}
}

func TestMCDC5_LikeAdvanceOneRune_ThreeByteRune(t *testing.T) {
	t.Parallel()
	// A=false, B=false: 3-byte UTF-8 '中' (U+4E2D)
	str := []byte("中")
	got := likeAdvanceOneRune(str, 0)
	if got != 3 {
		t.Errorf("want 3 for '中', got %d", got)
	}
}

// ---------------------------------------------------------------------------
// matchRange (collation.go:515) — MC/DC
// Conditions:
//   A: csize2 == 0          → return false, false
//   B: sc >= cc             (low bound)
//   C: sc <= cc2            (high bound)
// matched = B && C
// ---------------------------------------------------------------------------

func TestMCDC5_MatchRange_TruncatedPattern(t *testing.T) {
	t.Parallel()
	// A=true: no character after '-' → pattern "[a-" → false
	if Glob("[a-", "b") {
		t.Error("truncated range: want false")
	}
}

func TestMCDC5_MatchRange_InRange(t *testing.T) {
	t.Parallel()
	// B=true, C=true: 'm' in [a-z]
	if !Glob("[a-z]", "m") {
		t.Error("[a-z] vs 'm': want true")
	}
}

func TestMCDC5_MatchRange_BelowRange(t *testing.T) {
	t.Parallel()
	// B=false, C=true: 'A' not in [a-z] (below 'a')
	if Glob("[a-z]", "A") {
		t.Error("[a-z] vs 'A': want false (case-sensitive)")
	}
}

func TestMCDC5_MatchRange_AboveRange(t *testing.T) {
	t.Parallel()
	// B=true, C=false: '{' (ASCII 123) above 'z' (ASCII 122)
	if Glob("[a-z]", "{") {
		t.Error("[a-z] vs '{': want false")
	}
}

func TestMCDC5_MatchRange_MultibyteBounds(t *testing.T) {
	t.Parallel()
	// Multi-byte range: 'é' (U+00E9) in [à-ö] (U+00E0 - U+00F6)
	if !Glob("[à-ö]", "é") {
		t.Error("[à-ö] vs 'é': want true")
	}
}

func TestMCDC5_MatchRange_ReversedBounds(t *testing.T) {
	t.Parallel()
	// Reversed bounds [z-a]: sc='m' >= 'z' is false → no match
	if Glob("[z-a]", "m") {
		t.Error("[z-a] vs 'm': want false (reversed bounds)")
	}
}

// ---------------------------------------------------------------------------
// matchQuestion (collation.go:456) — MC/DC
// Conditions:
//   A: g.si >= len(g.str)   → false
//   B: ssize == 0           → false
//   else                    → advance and return true
// ---------------------------------------------------------------------------

func TestMCDC5_MatchQuestion_EmptyStr(t *testing.T) {
	t.Parallel()
	// A=true: empty string
	if Glob("?", "") {
		t.Error("'?' vs '': want false")
	}
}

func TestMCDC5_MatchQuestion_SingleASCII(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid ASCII char
	if !Glob("?", "x") {
		t.Error("'?' vs 'x': want true")
	}
}

func TestMCDC5_MatchQuestion_MultibyteSingleChar(t *testing.T) {
	t.Parallel()
	// A=false, B=false: multi-byte char counts as one
	if !Glob("?", "中") {
		t.Error("'?' vs '中': want true")
	}
}

func TestMCDC5_MatchQuestion_ExactlyOneChar(t *testing.T) {
	t.Parallel()
	// ? must match exactly one character, not more
	if !Glob("h?llo", "hello") {
		t.Error("'h?llo' vs 'hello': want true")
	}
}

func TestMCDC5_MatchQuestion_TwoCharsNoMatch(t *testing.T) {
	t.Parallel()
	// ? matches only one character
	if Glob("h?lo", "hello") {
		t.Error("'h?lo' vs 'hello': want false (? matches only 1 char)")
	}
}

// ---------------------------------------------------------------------------
// match / matchStar (collation.go:401 / 434) — MC/DC
// Conditions for matchStar:
//   A: g.pi >= len(g.pattern) after advancing → return true (star at end)
//   B: globImpl matches at current position   → return true
//   C: g.si >= len(g.str)                     → break
// ---------------------------------------------------------------------------

func TestMCDC5_MatchStar_AtEnd(t *testing.T) {
	t.Parallel()
	// A=true: star at end of pattern → matches any suffix
	if !Glob("abc*", "abcxyz") {
		t.Error("'abc*' vs 'abcxyz': want true")
	}
}

func TestMCDC5_MatchStar_MatchesEmpty(t *testing.T) {
	t.Parallel()
	// Star can match zero characters
	if !Glob("abc*", "abc") {
		t.Error("'abc*' vs 'abc': want true")
	}
}

func TestMCDC5_MatchStar_MiddleNoMatch(t *testing.T) {
	t.Parallel()
	// B=false for all positions, C=true at end → false
	if Glob("a*z", "abc") {
		t.Error("'a*z' vs 'abc': want false")
	}
}

func TestMCDC5_MatchStar_MultibyteSuffix(t *testing.T) {
	t.Parallel()
	// Star matches multi-byte suffix
	if !Glob("h*", "héllo") {
		t.Error("'h*' vs 'héllo': want true")
	}
}

func TestMCDC5_Match_PatternLongerThanStr(t *testing.T) {
	t.Parallel()
	// si < len(str) at end of pattern → false
	if Glob("abcd", "abc") {
		t.Error("'abcd' vs 'abc': want false")
	}
}

func TestMCDC5_Match_StrLongerThanPattern(t *testing.T) {
	t.Parallel()
	// Pattern consumed but str has more → si < len(str) → false
	if Glob("abc", "abcd") {
		t.Error("'abc' vs 'abcd': want false")
	}
}

// ---------------------------------------------------------------------------
// parseClassEntry (collation.go:525) — MC/DC
// Conditions:
//   A: csize == 0   → done=true
//   B: cc == ']'    → done=true
//   C: isCharRange()→ matchRange path vs direct equality
// ---------------------------------------------------------------------------

func TestMCDC5_ParseClassEntry_ClosingBracket(t *testing.T) {
	t.Parallel()
	// B=true: empty class [] → never matches any char
	if Glob("[]", "a") {
		t.Error("'[]' vs 'a': want false")
	}
}

func TestMCDC5_ParseClassEntry_DirectMatch(t *testing.T) {
	t.Parallel()
	// C=false: single char in class, direct equality
	if !Glob("[a]", "a") {
		t.Error("'[a]' vs 'a': want true")
	}
}

func TestMCDC5_ParseClassEntry_DirectNoMatch(t *testing.T) {
	t.Parallel()
	// C=false: single char in class, no match
	if Glob("[a]", "b") {
		t.Error("'[a]' vs 'b': want false")
	}
}

func TestMCDC5_ParseClassEntry_RangePath(t *testing.T) {
	t.Parallel()
	// C=true: isCharRange() → matchRange path
	if !Glob("[b-d]", "c") {
		t.Error("'[b-d]' vs 'c': want true")
	}
}

func TestMCDC5_ParseClassEntry_MultipleEntries(t *testing.T) {
	t.Parallel()
	// Multiple entries: [aeiou] matches vowels
	for _, tc := range []struct{ pat, str string; want bool }{
		{"[aeiou]", "e", true},
		{"[aeiou]", "b", false},
	} {
		got := Glob(tc.pat, tc.str)
		if got != tc.want {
			t.Errorf("Glob(%q, %q) = %v, want %v", tc.pat, tc.str, got, tc.want)
		}
	}
}

func TestMCDC5_ParseClassEntry_InvertedMultiple(t *testing.T) {
	t.Parallel()
	// Inverted class [^aeiou] → consonants match
	if !Glob("[^aeiou]", "b") {
		t.Error("'[^aeiou]' vs 'b': want true")
	}
	if Glob("[^aeiou]", "e") {
		t.Error("'[^aeiou]' vs 'e': want false")
	}
}

// ---------------------------------------------------------------------------
// likeMatchPercent (collation.go:325) — MC/DC
// Conditions:
//   A: pi >= len(pattern)     → return true (% at end)
//   B: likeImpl matches       → return true
//   C: si >= len(str) in loop → break
// ---------------------------------------------------------------------------

func TestMCDC5_LikeMatchPercent_AtEnd(t *testing.T) {
	t.Parallel()
	// A=true: % at end of pattern → matches anything
	if !Like("abc%", "abcdef", 0) {
		t.Error("'abc%' vs 'abcdef': want true")
	}
}

func TestMCDC5_LikeMatchPercent_Alone(t *testing.T) {
	t.Parallel()
	// A=true: % is the entire pattern
	if !Like("%", "anything", 0) {
		t.Error("'%' vs 'anything': want true")
	}
}

func TestMCDC5_LikeMatchPercent_EmptyStrMatch(t *testing.T) {
	t.Parallel()
	// % matches empty string too
	if !Like("%", "", 0) {
		t.Error("'%' vs '': want true")
	}
}

func TestMCDC5_LikeMatchPercent_MiddleMatch(t *testing.T) {
	t.Parallel()
	// B=true at some suffix
	if !Like("h%lo", "hello", 0) {
		t.Error("'h%lo' vs 'hello': want true")
	}
}

func TestMCDC5_LikeMatchPercent_MiddleNoMatch(t *testing.T) {
	t.Parallel()
	// B=false at all positions
	if Like("h%z", "hello", 0) {
		t.Error("'h%z' vs 'hello': want false")
	}
}

func TestMCDC5_LikeMatchPercent_MultibyteMid(t *testing.T) {
	t.Parallel()
	// Multi-byte: é is 2 bytes; % must advance over it
	if !Like("h%llo", "héllo", 0) {
		t.Error("'h%%llo' vs 'héllo': want true")
	}
}

// ---------------------------------------------------------------------------
// likeMatchLiteral (collation.go:361) — MC/DC
// Conditions:
//   A: si >= len(str)  → false
//   B: ssize == 0      → false
//   C: pc != sc        → false
//   noCase path
// ---------------------------------------------------------------------------

func TestMCDC5_LikeMatchLiteral_NoCaseMatch(t *testing.T) {
	t.Parallel()
	// noCase=true: 'A' pattern matches 'a' in string
	if !Like("A", "a", 0) {
		t.Error("Like 'A' vs 'a' (noCase): want true")
	}
}

func TestMCDC5_LikeMatchLiteral_NoCaseMismatch(t *testing.T) {
	t.Parallel()
	// noCase=true but different letter
	if Like("A", "b", 0) {
		t.Error("Like 'A' vs 'b' (noCase): want false")
	}
}

func TestMCDC5_LikeMatchLiteral_CaseSensitiveMatch(t *testing.T) {
	t.Parallel()
	// LikeCase: exact case match
	if !LikeCase("a", "a", 0) {
		t.Error("LikeCase 'a' vs 'a': want true")
	}
}

func TestMCDC5_LikeMatchLiteral_CaseSensitiveMismatch(t *testing.T) {
	t.Parallel()
	// LikeCase: 'A' != 'a'
	if LikeCase("A", "a", 0) {
		t.Error("LikeCase 'A' vs 'a': want false")
	}
}

func TestMCDC5_LikeMatchLiteral_MultibyteLiteral(t *testing.T) {
	t.Parallel()
	// Multi-byte literal match: '中' matches '中'
	if !Like("中国", "中国", 0) {
		t.Error("Like '中国' vs '中国': want true")
	}
}

// ---------------------------------------------------------------------------
// likeImpl (collation.go:268) — MC/DC
// Conditions covering the escape character path and underscore path
// ---------------------------------------------------------------------------

func TestMCDC5_LikeImpl_EscapeChar(t *testing.T) {
	t.Parallel()
	// Escape '!' makes '%' literal
	if !Like("a!%b", "a%b", '!') {
		t.Error("Like with escape '!': want true")
	}
}

func TestMCDC5_LikeImpl_EscapeAtEnd(t *testing.T) {
	t.Parallel()
	// Escape at end of pattern → false (invalid pattern)
	if Like("abc!", "abcd", '!') {
		t.Error("Like escape at end: want false")
	}
}

func TestMCDC5_LikeImpl_UnderscoreMatch(t *testing.T) {
	t.Parallel()
	// _ matches exactly one character
	if !Like("h_llo", "hello", 0) {
		t.Error("Like 'h_llo' vs 'hello': want true")
	}
}

func TestMCDC5_LikeImpl_UnderscoreEmptyStr(t *testing.T) {
	t.Parallel()
	// _ at end against empty remainder → false
	if Like("abc_", "abc", 0) {
		t.Error("Like 'abc_' vs 'abc': want false")
	}
}

func TestMCDC5_LikeImpl_PatternExhaustedStrRemains(t *testing.T) {
	t.Parallel()
	// Pattern consumed, string has extra chars → false
	if Like("ab", "abc", 0) {
		t.Error("Like 'ab' vs 'abc': want false")
	}
}

// ---------------------------------------------------------------------------
// ToValidUTF8 (utf8.go:211) — MC/DC
// Conditions:
//   A: utf8.Valid(data) → return data unchanged
//   B: size == 0 (inner break)
// ---------------------------------------------------------------------------

func TestMCDC5_ToValidUTF8_AlreadyValid(t *testing.T) {
	t.Parallel()
	// A=true: fast path returns same slice
	input := []byte("hello world")
	result := ToValidUTF8(input)
	if string(result) != "hello world" {
		t.Errorf("want 'hello world', got %q", string(result))
	}
}

func TestMCDC5_ToValidUTF8_ValidMultibyte(t *testing.T) {
	t.Parallel()
	// A=true: multi-byte valid UTF-8
	input := []byte("héllo 中文")
	result := ToValidUTF8(input)
	if string(result) != "héllo 中文" {
		t.Errorf("want 'héllo 中文', got %q", string(result))
	}
}

func TestMCDC5_ToValidUTF8_SingleInvalidByte(t *testing.T) {
	t.Parallel()
	// A=false: 0xFF is invalid; replaced with replacement char
	input := []byte{0xFF}
	result := ToValidUTF8(input)
	if len(result) == 0 {
		t.Error("want non-empty result for invalid byte")
	}
}

func TestMCDC5_ToValidUTF8_MixedValidInvalid(t *testing.T) {
	t.Parallel()
	// A=false: h + invalid + e → h + replacement + e
	input := []byte{0x68, 0xFF, 0x65} // h <invalid> e
	result := ToValidUTF8(input)
	// Should have 'h', replacement char (3 bytes U+FFFD), 'e'
	if len(result) < 3 {
		t.Errorf("expected at least 3 bytes, got %d", len(result))
	}
}

func TestMCDC5_ToValidUTF8_SurrogatePair(t *testing.T) {
	t.Parallel()
	// Surrogate pair bytes are invalid UTF-8 (0xED 0xA0 0x80 = U+D800 encoded)
	input := []byte{0xED, 0xA0, 0x80}
	result := ToValidUTF8(input)
	if len(result) == 0 {
		t.Error("want non-empty result for surrogate pair bytes")
	}
}

func TestMCDC5_ToValidUTF8_OverlongEncoding(t *testing.T) {
	t.Parallel()
	// Overlong encoding of NUL (0xC0 0x80) is invalid
	input := []byte{0xC0, 0x80}
	result := ToValidUTF8(input)
	if len(result) == 0 {
		t.Error("want non-empty result for overlong encoding")
	}
}

// ---------------------------------------------------------------------------
// UTF8ToUTF16 (utf16.go:175) — MC/DC
// Conditions:
//   A: len(data) == 0       → return nil
//   B: enc == UTF16LE path vs UTF16BE path
//   C: surrogate pair needed (r > 0xFFFF)
// ---------------------------------------------------------------------------

func TestMCDC5_UTF8ToUTF16_Empty(t *testing.T) {
	t.Parallel()
	// A=true: empty input → nil
	result := UTF8ToUTF16([]byte{}, UTF16LE)
	if result != nil {
		t.Errorf("want nil for empty input, got %v", result)
	}
}

func TestMCDC5_UTF8ToUTF16_ASCIILittleEndian(t *testing.T) {
	t.Parallel()
	// B=LE: 'A' → [0x41, 0x00]
	result := UTF8ToUTF16([]byte("A"), UTF16LE)
	if len(result) != 2 || result[0] != 0x41 || result[1] != 0x00 {
		t.Errorf("LE 'A': want [0x41, 0x00], got %v", result)
	}
}

func TestMCDC5_UTF8ToUTF16_ASCIIBigEndian(t *testing.T) {
	t.Parallel()
	// B=BE: 'A' → [0x00, 0x41]
	result := UTF8ToUTF16([]byte("A"), UTF16BE)
	if len(result) != 2 || result[0] != 0x00 || result[1] != 0x41 {
		t.Errorf("BE 'A': want [0x00, 0x41], got %v", result)
	}
}

func TestMCDC5_UTF8ToUTF16_ChineseLE(t *testing.T) {
	t.Parallel()
	// 3-byte UTF-8 '中' (U+4E2D) → single UTF-16 code unit LE: [0x2D, 0x4E]
	result := UTF8ToUTF16([]byte("中"), UTF16LE)
	if len(result) != 2 {
		t.Errorf("LE '中': want 2 bytes, got %d", len(result))
	}
	if result[0] != 0x2D || result[1] != 0x4E {
		t.Errorf("LE '中': want [0x2D, 0x4E], got %v", result)
	}
}

func TestMCDC5_UTF8ToUTF16_SurrogatePairLE(t *testing.T) {
	t.Parallel()
	// C=true: U+1F600 (😀) requires surrogate pair → 4 bytes LE
	result := UTF8ToUTF16([]byte("\U0001F600"), UTF16LE)
	if len(result) != 4 {
		t.Errorf("LE emoji: want 4 bytes, got %d", len(result))
	}
}

func TestMCDC5_UTF8ToUTF16_SurrogatePairBE(t *testing.T) {
	t.Parallel()
	// C=true, B=BE: U+1F600 → 4 bytes BE
	result := UTF8ToUTF16([]byte("\U0001F600"), UTF16BE)
	if len(result) != 4 {
		t.Errorf("BE emoji: want 4 bytes, got %d", len(result))
	}
}

func TestMCDC5_UTF8ToUTF16_MultipleChars(t *testing.T) {
	t.Parallel()
	// "AB" → 4 bytes LE
	result := UTF8ToUTF16([]byte("AB"), UTF16LE)
	if len(result) != 4 {
		t.Errorf("LE 'AB': want 4 bytes, got %d", len(result))
	}
}

func TestMCDC5_UTF8ToUTF16_BMPCharBE(t *testing.T) {
	t.Parallel()
	// U+00E9 ('é') → BMP → 2 bytes BE: [0x00, 0xE9]
	result := UTF8ToUTF16([]byte("é"), UTF16BE)
	if len(result) != 2 {
		t.Errorf("BE 'é': want 2 bytes, got %d", len(result))
	}
	if result[0] != 0x00 || result[1] != 0xE9 {
		t.Errorf("BE 'é': want [0x00, 0xE9], got %v", result)
	}
}
