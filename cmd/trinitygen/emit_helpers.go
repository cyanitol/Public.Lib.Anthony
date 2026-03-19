// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

package main

import (
	"fmt"
	"strings"
	"unicode"
)

// formatGoValue converts a Go value to its literal representation for codegen.
func formatGoValue(v interface{}) string {
	if v == nil {
		return "nil"
	}
	switch val := v.(type) {
	case int64:
		return fmt.Sprintf("int64(%d)", val)
	case float64:
		return fmt.Sprintf("%v", val)
	case string:
		return fmt.Sprintf("%q", val)
	case bool:
		return fmt.Sprintf("%t", val)
	}
	return fmt.Sprintf("%v", v)
}

// sanitizeName converts a string to a valid Go identifier component.
func sanitizeName(s string) string {
	var b strings.Builder
	for i, r := range s {
		writeRuneForIdent(&b, r, i)
	}
	return finalizeSanitizedName(b.String())
}

// writeRuneForIdent writes a rune to the builder if it's a valid identifier character.
func writeRuneForIdent(b *strings.Builder, r rune, pos int) {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		b.WriteRune(r)
	} else if (r == '_' || r == '-' || r == ' ') && pos > 0 {
		b.WriteRune('_')
	}
}

// finalizeSanitizedName handles edge cases for empty or digit-starting names.
func finalizeSanitizedName(result string) string {
	if len(result) == 0 {
		return "unnamed"
	}
	if unicode.IsDigit(rune(result[0])) {
		return "t_" + result
	}
	return result
}

// assignReqID generates a REQ-ID string for a test case.
func assignReqID(module string, seq int, desc string) string {
	return fmt.Sprintf("REQ-%s-%03d_%s",
		strings.ToUpper(module), seq, sanitizeName(desc))
}
