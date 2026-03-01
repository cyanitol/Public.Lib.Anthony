// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package utf

import "bytes"

// CollationType represents the collation sequence type.
type CollationType int

const (
	// BINARY performs byte-by-byte comparison
	BINARY CollationType = iota

	// NOCASE performs case-insensitive comparison for ASCII characters (A-Z = a-z)
	NOCASE

	// RTRIM ignores trailing spaces during comparison
	RTRIM
)

// Collation represents a collation function.
type Collation struct {
	Type CollationType
	Name string
}

// BuiltinCollations are the standard SQLite collations.
var BuiltinCollations = map[string]Collation{
	"BINARY": {Type: BINARY, Name: "BINARY"},
	"NOCASE": {Type: NOCASE, Name: "NOCASE"},
	"RTRIM":  {Type: RTRIM, Name: "RTRIM"},
}

// Compare compares two strings using the specified collation.
// Returns:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func (c Collation) Compare(a, b string) int {
	switch c.Type {
	case BINARY:
		return CompareBinary(a, b)
	case NOCASE:
		return CompareNoCase(a, b)
	case RTRIM:
		return CompareRTrim(a, b)
	default:
		return CompareBinary(a, b)
	}
}

// CompareBytes compares two byte slices using the specified collation.
func (c Collation) CompareBytes(a, b []byte) int {
	switch c.Type {
	case BINARY:
		return bytes.Compare(a, b)
	case NOCASE:
		return CompareNoCaseBytes(a, b)
	case RTRIM:
		return CompareRTrimBytes(a, b)
	default:
		return bytes.Compare(a, b)
	}
}

// CompareBinary performs byte-by-byte comparison.
func CompareBinary(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// CompareNoCase performs case-insensitive comparison for ASCII characters.
// This matches SQLite's NOCASE collation which only folds ASCII A-Z to a-z.
func CompareNoCase(a, b string) int {
	aBytes := []byte(a)
	bBytes := []byte(b)

	minLen := len(aBytes)
	if len(bBytes) < minLen {
		minLen = len(bBytes)
	}

	for i := 0; i < minLen; i++ {
		ca := UpperToLower[aBytes[i]]
		cb := UpperToLower[bBytes[i]]

		if ca != cb {
			if ca < cb {
				return -1
			}
			return 1
		}
	}

	// If all compared bytes are equal, compare lengths
	if len(aBytes) < len(bBytes) {
		return -1
	}
	if len(aBytes) > len(bBytes) {
		return 1
	}
	return 0
}

// CompareNoCaseBytes performs case-insensitive comparison on byte slices.
func CompareNoCaseBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		ca := UpperToLower[a[i]]
		cb := UpperToLower[b[i]]

		if ca != cb {
			if ca < cb {
				return -1
			}
			return 1
		}
	}

	// If all compared bytes are equal, compare lengths
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// CompareRTrim compares strings while ignoring trailing spaces.
func CompareRTrim(a, b string) int {
	// Remove trailing spaces
	a = rtrimSpaces(a)
	b = rtrimSpaces(b)
	return CompareBinary(a, b)
}

// CompareRTrimBytes compares byte slices while ignoring trailing spaces.
func CompareRTrimBytes(a, b []byte) int {
	// Remove trailing spaces
	a = rtrimSpacesBytes(a)
	b = rtrimSpacesBytes(b)
	return bytes.Compare(a, b)
}

// rtrimSpaces removes trailing spaces from a string.
func rtrimSpaces(s string) string {
	i := len(s)
	for i > 0 && s[i-1] == ' ' {
		i--
	}
	return s[:i]
}

// rtrimSpacesBytes removes trailing spaces from a byte slice.
func rtrimSpacesBytes(s []byte) []byte {
	i := len(s)
	for i > 0 && s[i-1] == ' ' {
		i--
	}
	return s[:i]
}

func strICmpEmpty(a, b string) (int, bool) {
	if a == "" || b == "" {
		return len(a) - len(b), true
	}
	return 0, false
}

func strICmpScan(a, b []byte) int {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	for i := 0; i < limit; i++ {
		diff := int(UpperToLower[a[i]]) - int(UpperToLower[b[i]])
		if diff != 0 {
			return diff
		}
	}
	return strNICmpResult(len(a), len(b))
}

func StrICmp(a, b string) int {
	if v, ok := strICmpEmpty(a, b); ok {
		return v
	}
	return strICmpScan([]byte(a), []byte(b))
}

func strNICmpLimit(n, aLen, bLen int) int {
	if aLen < n {
		n = aLen
	}
	if bLen < n {
		n = bLen
	}
	return n
}

func strNICmpResult(aLen, bLen int) int {
	if aLen < bLen {
		return -1
	}
	if aLen > bLen {
		return 1
	}
	return 0
}

// StrNICmp performs case-insensitive string comparison up to n bytes.
func StrNICmp(a, b string, n int) int {
	if len(a) == 0 || len(b) == 0 {
		return len(a) - len(b)
	}

	aBytes := []byte(a)
	bBytes := []byte(b)
	limit := strNICmpLimit(n, len(aBytes), len(bBytes))

	for i := 0; i < limit; i++ {
		diff := int(UpperToLower[aBytes[i]]) - int(UpperToLower[bBytes[i]])
		if diff != 0 {
			return diff
		}
	}

	if n <= limit {
		return 0
	}
	return strNICmpResult(len(aBytes)-limit, len(bBytes)-limit)
}

// StrIHash computes an 8-bit hash of a string that is insensitive to case differences.
func StrIHash(s string) byte {
	var h byte
	for i := 0; i < len(s); i++ {
		h += UpperToLower[s[i]]
	}
	return h
}

// Like implements the SQL LIKE operator.
// pattern is the pattern to match against (may contain % and _ wildcards)
// str is the string to test
// escape is the escape character (0 if none)
func Like(pattern, str string, escape rune) bool {
	return likeImpl([]byte(pattern), []byte(str), escape, true)
}

// LikeCase implements the SQL LIKE operator with case-sensitivity.
func LikeCase(pattern, str string, escape rune) bool {
	return likeImpl([]byte(pattern), []byte(str), escape, false)
}

// likeImpl implements the LIKE matching algorithm.
// If noCase is true, performs case-insensitive matching for ASCII characters.
func likeImpl(pattern, str []byte, escape rune, noCase bool) bool {
	pi := 0
	si := 0

	for pi < len(pattern) {
		pc, psize := DecodeRune(pattern[pi:])
		if psize == 0 {
			break
		}

		newPI, newSI, done, ok := likeProcessChar(pattern, str, pi, si, pc, psize, escape, noCase)
		if done {
			return ok
		}
		if !ok {
			return false
		}
		pi, si = newPI, newSI
	}

	return si >= len(str)
}

func likeProcessChar(pattern, str []byte, pi, si int, pc rune, psize int, escape rune, noCase bool) (newPI, newSI int, done, ok bool) {
	if escape != 0 && pc == escape {
		pc, psize, pi = likeConsumeEscape(pattern, pi, psize)
		if psize == 0 {
			return 0, 0, true, false
		}
	} else if pc == '%' {
		return 0, 0, true, likeMatchPercent(pattern, str, pi+psize, si, escape, noCase)
	} else if pc == '_' {
		newSI := likeAdvanceOneRune(str, si)
		if newSI < 0 {
			return 0, 0, true, false
		}
		return pi + psize, newSI, false, true
	}

	newPI, newSI, ok = likeMatchLiteral(pattern, str, pc, psize, pi, si, noCase)
	return newPI, newSI, false, ok
}

// likeConsumeEscape advances past an escape character in the pattern and
// returns the escaped rune, its size, and the new pattern index.
// If the escape appears at the end of the pattern, size is returned as 0.
func likeConsumeEscape(pattern []byte, pi, psize int) (pc rune, size, newPI int) {
	newPI = pi + psize
	if newPI >= len(pattern) {
		return 0, 0, newPI
	}
	r, sz := DecodeRune(pattern[newPI:])
	return r, sz, newPI
}

// likeMatchPercent handles a '%' wildcard by trying to match the remainder of
// the pattern against every suffix of str starting at si.
func likeMatchPercent(pattern, str []byte, pi, si int, escape rune, noCase bool) bool {
	if pi >= len(pattern) {
		return true
	}
	for si <= len(str) {
		if likeImpl(pattern[pi:], str[si:], escape, noCase) {
			return true
		}
		if si >= len(str) {
			break
		}
		_, ssize := DecodeRune(str[si:])
		if ssize == 0 {
			break
		}
		si += ssize
	}
	return false
}

// likeAdvanceOneRune returns the string index after consuming exactly one rune
// from str at position si. Returns -1 if str is exhausted or the rune is invalid.
func likeAdvanceOneRune(str []byte, si int) int {
	if si >= len(str) {
		return -1
	}
	_, ssize := DecodeRune(str[si:])
	if ssize == 0 {
		return -1
	}
	return si + ssize
}

// likeMatchLiteral compares a single literal pattern rune against the current
// string rune, applying case-folding when noCase is true.
// Returns the updated pi and si on success, or ok=false on mismatch.
func likeMatchLiteral(pattern, str []byte, pc rune, psize, pi, si int, noCase bool) (newPI, newSI int, ok bool) {
	if si >= len(str) {
		return pi, si, false
	}
	sc, ssize := DecodeRune(str[si:])
	if ssize == 0 {
		return pi, si, false
	}
	if noCase {
		pc = ToLower(pc)
		sc = ToLower(sc)
	}
	if pc != sc {
		return pi, si, false
	}
	return pi + psize, si + ssize, true
}

// Glob implements the SQL GLOB operator.
// pattern is the pattern to match against (may contain * and ? wildcards)
// str is the string to test
func Glob(pattern, str string) bool {
	return globImpl([]byte(pattern), []byte(str))
}

// globState holds the state for glob pattern matching.
type globState struct {
	pattern []byte
	str     []byte
	pi      int // pattern index
	si      int // string index
}

// globImpl implements the GLOB matching algorithm (case-sensitive).
func globImpl(pattern, str []byte) bool {
	state := &globState{pattern: pattern, str: str}
	return state.match()
}

// match performs the main glob matching loop.
func (g *globState) match() bool {
	for g.pi < len(g.pattern) {
		pc, psize := DecodeRune(g.pattern[g.pi:])
		if psize == 0 {
			break
		}
		done, ok := g.matchRune(pc, psize)
		if done {
			return ok
		}
		if !ok {
			return false
		}
	}
	return g.si >= len(g.str)
}

// matchRune handles a single pattern rune. Returns (done, result).
// done=true means matching is complete, done=false means continue.
func (g *globState) matchRune(pc rune, psize int) (done, ok bool) {
	switch pc {
	case '*':
		return true, g.matchStar(psize)
	case '?':
		return false, g.matchQuestion(psize)
	case '[':
		return false, g.matchCharClass(psize)
	default:
		return false, g.matchLiteral(pc, psize)
	}
}

// matchStar handles the '*' wildcard (zero or more characters).
func (g *globState) matchStar(psize int) bool {
	g.pi += psize
	if g.pi >= len(g.pattern) {
		return true
	}
	for g.si <= len(g.str) {
		if globImpl(g.pattern[g.pi:], g.str[g.si:]) {
			return true
		}
		if g.si >= len(g.str) {
			break
		}
		_, ssize := DecodeRune(g.str[g.si:])
		if ssize == 0 {
			break
		}
		g.si += ssize
	}
	return false
}

// matchQuestion handles the '?' wildcard (exactly one character).
func (g *globState) matchQuestion(psize int) bool {
	if g.si >= len(g.str) {
		return false
	}
	_, ssize := DecodeRune(g.str[g.si:])
	if ssize == 0 {
		return false
	}
	g.si += ssize
	g.pi += psize
	return true
}

// matchCharClass handles '[...]' character classes.
func (g *globState) matchCharClass(psize int) bool {
	g.pi += psize
	if g.pi >= len(g.pattern) || g.si >= len(g.str) {
		return false
	}

	sc, ssize := DecodeRune(g.str[g.si:])
	if ssize == 0 {
		return false
	}

	invert, matched := g.parseCharClass(sc)
	if invert {
		matched = !matched
	}
	if !matched {
		return false
	}
	g.si += ssize
	return true
}

// parseCharClass parses a character class and checks if sc matches.
func (g *globState) parseCharClass(sc rune) (invert, matched bool) {
	invert = g.consumeInvert()
	for g.pi < len(g.pattern) {
		hit, done := g.parseClassEntry(sc)
		if done {
			break
		}
		if hit {
			matched = true
		}
	}
	return invert, matched
}

func (g *globState) consumeInvert() bool {
	if g.pi < len(g.pattern) && g.pattern[g.pi] == '^' {
		g.pi++
		return true
	}
	return false
}

func (g *globState) matchRange(sc, cc rune) (matched, ok bool) {
	g.pi++
	cc2, csize2 := DecodeRune(g.pattern[g.pi:])
	if csize2 == 0 {
		return false, false
	}
	g.pi += csize2
	return sc >= cc && sc <= cc2, true
}

func (g *globState) parseClassEntry(sc rune) (matched, done bool) {
	cc, csize := DecodeRune(g.pattern[g.pi:])
	if csize == 0 {
		return false, true
	}
	g.pi += csize
	if cc == ']' {
		return false, true
	}
	if g.isCharRange() {
		hit, ok := g.matchRange(sc, cc)
		return hit, !ok
	}
	return sc == cc, false
}

// isCharRange checks if the current position is a character range (a-z).
func (g *globState) isCharRange() bool {
	return g.pi < len(g.pattern) && g.pattern[g.pi] == '-' && g.pi+1 < len(g.pattern)
}

// matchLiteral handles literal character matching.
func (g *globState) matchLiteral(pc rune, psize int) bool {
	if g.si >= len(g.str) {
		return false
	}
	sc, ssize := DecodeRune(g.str[g.si:])
	if ssize == 0 || pc != sc {
		return false
	}
	g.pi += psize
	g.si += ssize
	return true
}
