// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
)

// Affinity represents SQLite type affinity.
// We re-export the type from expr package for convenience.
type Affinity = expr.Affinity

// Affinity constants for schema use.
const (
	AffinityNone    = expr.AFF_NONE
	AffinityText    = expr.AFF_TEXT
	AffinityNumeric = expr.AFF_NUMERIC
	AffinityInteger = expr.AFF_INTEGER
	AffinityReal    = expr.AFF_REAL
	AffinityBlob    = expr.AFF_BLOB
)

// DetermineAffinity determines the type affinity from a column type name.
//
// SQLite type affinity rules (from https://sqlite.org/datatype3.html):
// 1. If the type contains "INT" -> INTEGER affinity
// 2. If the type contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity
// 3. If the type contains "BLOB" or no type specified -> BLOB affinity
// 4. If the type contains "REAL", "FLOA", or "DOUB" -> REAL affinity
// 5. Otherwise -> NUMERIC affinity
//
// Examples:
//   - "INTEGER", "BIGINT", "INT2" -> INTEGER
//   - "VARCHAR(100)", "CHARACTER(20)", "TEXT" -> TEXT
//   - "BLOB", "" -> BLOB
//   - "REAL", "DOUBLE", "FLOAT" -> REAL
//   - "NUMERIC", "DECIMAL(10,2)", "BOOLEAN", "DATE" -> NUMERIC
func DetermineAffinity(typeName string) Affinity {
	if typeName == "" {
		return AffinityBlob
	}
	upper := strings.ToUpper(typeName)
	return determineAffinityFromUpper(upper)
}

func determineAffinityFromUpper(upper string) Affinity {
	if strings.Contains(upper, "INT") {
		return AffinityInteger
	}
	if containsAny(upper, "CHAR", "CLOB", "TEXT") {
		return AffinityText
	}
	if strings.Contains(upper, "BLOB") {
		return AffinityBlob
	}
	if containsAny(upper, "REAL", "FLOA", "DOUB") {
		return AffinityReal
	}
	return AffinityNumeric
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// IsNumericAffinity returns true if the affinity is numeric (NUMERIC, INTEGER, or REAL).
func IsNumericAffinity(aff Affinity) bool {
	return aff == AffinityNumeric || aff == AffinityInteger || aff == AffinityReal
}

// AffinityName returns the canonical name for an affinity.
func AffinityName(aff Affinity) string {
	switch aff {
	case AffinityNone:
		return "NONE"
	case AffinityText:
		return "TEXT"
	case AffinityNumeric:
		return "NUMERIC"
	case AffinityInteger:
		return "INTEGER"
	case AffinityReal:
		return "REAL"
	case AffinityBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}
