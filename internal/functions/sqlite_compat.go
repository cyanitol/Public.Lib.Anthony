// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

// sqliteCompatVersion is the SQLite version this implementation targets.
const sqliteCompatVersion = "3.46.0"

// RegisterSQLiteCompatFunctions registers SQLite compatibility functions.
func RegisterSQLiteCompatFunctions(r *Registry) {
	r.Register(NewScalarFunc("sqlite_version", 0, sqliteVersionFunc))
	r.Register(NewScalarFunc("soundex", 1, soundexFunc))

	// Connection-state functions: these are placeholder registrations so the function
	// registry recognizes the names. The actual values are injected by the VDBE at
	// execution time from connection state.
	r.Register(NewScalarFunc("last_insert_rowid", 0, connStatePlaceholder))
	r.Register(NewScalarFunc("changes", 0, connStatePlaceholder))
	r.Register(NewScalarFunc("total_changes", 0, connStatePlaceholder))
}

// connStatePlaceholder is a placeholder for connection-state functions.
// The VDBE intercepts these before they reach this code.
func connStatePlaceholder(args []Value) (Value, error) {
	return NewIntValue(0), nil
}

// sqliteVersionFunc implements sqlite_version()
// Returns the SQLite version string this implementation targets.
func sqliteVersionFunc(args []Value) (Value, error) {
	return NewTextValue(sqliteCompatVersion), nil
}

// soundexMap maps uppercase letters to their Soundex digit.
var soundexMap = map[rune]byte{
	'B': '1', 'F': '1', 'P': '1', 'V': '1',
	'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
	'D': '3', 'T': '3',
	'L': '4',
	'M': '5', 'N': '5',
	'R': '6',
}

// soundexFunc implements soundex(X)
// Returns the Soundex encoding of string X.
func soundexFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewTextValue("?000"), nil
	}

	s := strings.TrimSpace(args[0].AsString())
	if s == "" {
		return NewTextValue("?000"), nil
	}

	return NewTextValue(computeSoundex(s)), nil
}

// soundexFindFirstLetter finds the first letter in s and returns it uppercased
// along with the remainder of the string. Returns (0, "") if no letter found.
func soundexFindFirstLetter(s string) (rune, string) {
	for i, r := range s {
		if unicode.IsLetter(r) {
			return unicode.ToUpper(r), s[i+len(string(r)):]
		}
	}
	return 0, ""
}

// soundexUpdateCode updates the lastCode tracking based on the current character.
// Returns the new lastCode value.
func soundexUpdateCode(upper rune, code byte, hasCode bool, lastCode byte) byte {
	if hasCode {
		return code
	}
	if upper != 'H' && upper != 'W' {
		return 0 // vowels and non-letters reset the duplicate tracking
	}
	return lastCode
}

// computeSoundex computes the Soundex encoding for a non-empty string.
func computeSoundex(s string) string {
	firstLetter, rest := soundexFindFirstLetter(s)
	if firstLetter == 0 {
		return "?000"
	}

	var result [4]byte
	result[0] = byte(firstLetter)
	pos := 1
	lastCode := soundexMap[firstLetter]

	for _, r := range rest {
		if pos >= 4 {
			break
		}
		upper := unicode.ToUpper(r)
		code, ok := soundexMap[upper]
		if ok && code != lastCode {
			result[pos] = code
			pos++
		}
		lastCode = soundexUpdateCode(upper, code, ok, lastCode)
	}

	for pos < 4 {
		result[pos] = '0'
		pos++
	}

	return string(result[:])
}

// logVariadicFunc implements log(X) and log(B, X)
// With 1 argument: returns natural log of X (same as ln(X))
// With 2 arguments: returns log base B of X (= ln(X)/ln(B))
func logVariadicFunc(args []Value) (Value, error) {
	if len(args) == 1 {
		return logOneArg(args[0])
	}
	if len(args) == 2 {
		return logTwoArgs(args[0], args[1])
	}
	return nil, fmt.Errorf("log() requires 1 or 2 arguments (%d given)", len(args))
}

// logOneArg implements log(X) - natural logarithm.
func logOneArg(x Value) (Value, error) {
	if x.IsNull() {
		return NewNullValue(), nil
	}
	val := x.AsFloat64()
	if val <= 0 {
		return NewFloatValue(math.NaN()), nil
	}
	return NewFloatValue(math.Log(val)), nil
}

// logTwoArgs implements log(B, X) - logarithm base B of X.
func logTwoArgs(base, x Value) (Value, error) {
	if base.IsNull() || x.IsNull() {
		return NewNullValue(), nil
	}
	b := base.AsFloat64()
	val := x.AsFloat64()
	if b <= 0 || b == 1 || val <= 0 {
		return NewFloatValue(math.NaN()), nil
	}
	return NewFloatValue(math.Log(val) / math.Log(b)), nil
}
