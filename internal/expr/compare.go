// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"bytes"
	"math"
	"strconv"
	"strings"
)

// CompareResult represents the result of a comparison.
type CompareResult int

const (
	CmpLess    CompareResult = -1
	CmpEqual   CompareResult = 0
	CmpGreater CompareResult = 1
	CmpNull    CompareResult = 2 // Either operand is NULL
)

// CollSeq represents a collation sequence for string comparison.
type CollSeq struct {
	Name    string
	Compare func(a, b string) int
}

// Standard collation sequences
var (
	CollSeqBinary = &CollSeq{
		Name:    "BINARY",
		Compare: compareBinary,
	}

	CollSeqNoCase = &CollSeq{
		Name:    "NOCASE",
		Compare: compareNoCase,
	}

	CollSeqRTrim = &CollSeq{
		Name:    "RTRIM",
		Compare: compareRTrim,
	}
)

var collSeqByName = map[string]*CollSeq{
	"BINARY": CollSeqBinary,
	"NOCASE": CollSeqNoCase,
	"RTRIM":  CollSeqRTrim,
}

func collSeqFromName(name string) *CollSeq {
	if cs, ok := collSeqByName[strings.ToUpper(name)]; ok {
		return cs
	}
	return CollSeqBinary
}

func collSeqFromCollateOp(e *Expr) *CollSeq {
	return collSeqFromName(e.CollSeq)
}

func collSeqFromColumn(e *Expr) *CollSeq {
	if e.Op != OpColumn || e.CollSeq == "" {
		return nil
	}
	cs := collSeqFromName(e.CollSeq)
	if cs == CollSeqBinary {
		return nil
	}
	return cs
}

func nextCollSeqExpr(e *Expr) *Expr {
	if e.HasProperty(EP_Collate) && e.Left != nil && e.Left.HasProperty(EP_Collate) {
		return e.Left
	}
	return nil
}

// compareBinary performs binary (byte-by-byte) comparison.
func compareBinary(a, b string) int {
	return strings.Compare(a, b)
}

// compareNoCase performs case-insensitive comparison.
func compareNoCase(a, b string) int {
	return strings.Compare(strings.ToUpper(a), strings.ToUpper(b))
}

// compareRTrim performs comparison with trailing spaces ignored.
func compareRTrim(a, b string) int {
	a = strings.TrimRight(a, " ")
	b = strings.TrimRight(b, " ")
	return strings.Compare(a, b)
}

// GetCollSeq returns the collation sequence for an expression.
// Returns CollSeqBinary if no specific collation is set.
func GetCollSeq(e *Expr) *CollSeq {
	for e != nil {
		if e.Op == OpCollate {
			return collSeqFromCollateOp(e)
		}
		if next := nextCollSeqExpr(e); next != nil {
			e = next
			continue
		}
		if cs := collSeqFromColumn(e); cs != nil {
			return cs
		}
		break
	}
	return CollSeqBinary
}

// getExplicitCollation returns collation if expression has EP_Collate property.
func getExplicitCollation(e *Expr) *CollSeq {
	if e != nil && e.HasProperty(EP_Collate) {
		return GetCollSeq(e)
	}
	return nil
}

// getImplicitCollation returns non-binary collation from expression.
func getImplicitCollation(e *Expr) *CollSeq {
	if e == nil {
		return nil
	}
	coll := GetCollSeq(e)
	if coll != CollSeqBinary {
		return coll
	}
	return nil
}

// GetBinaryCompareCollSeq returns the collation for a binary comparison.
// Left operand takes precedence over right operand.
func GetBinaryCompareCollSeq(left, right *Expr) *CollSeq {
	// Check for explicit collation (EP_Collate flag)
	if coll := getExplicitCollation(left); coll != nil {
		return coll
	}
	if coll := getExplicitCollation(right); coll != nil {
		return coll
	}
	// Check for implicit non-binary collation
	if coll := getImplicitCollation(left); coll != nil {
		return coll
	}
	if right != nil {
		return GetCollSeq(right)
	}
	return CollSeqBinary
}

// intToCompareResult converts a three-way integer (negative/zero/positive) to
// a CompareResult, following the same convention used by strings.Compare and
// bytes.Compare.
func intToCompareResult(n int) CompareResult {
	if n < 0 {
		return CmpLess
	}
	if n > 0 {
		return CmpGreater
	}
	return CmpEqual
}

// compareIntegers compares two int64 values.
func compareIntegers(l, r int64) CompareResult {
	if l < r {
		return CmpLess
	}
	if l > r {
		return CmpGreater
	}
	return CmpEqual
}

// toFloat64 widens an int64 or float64 to float64.
// The caller must ensure v is one of those two types.
func toFloat64(v interface{}) float64 {
	if i, ok := v.(int64); ok {
		return float64(i)
	}
	return v.(float64)
}

// compareNumerics compares two values that are each int64 or float64.
func compareNumerics(left, right interface{}) CompareResult {
	lf := toFloat64(left)
	rf := toFloat64(right)
	if math.IsNaN(lf) || math.IsNaN(rf) {
		return CmpNull
	}
	if lf < rf {
		return CmpLess
	}
	if lf > rf {
		return CmpGreater
	}
	return CmpEqual
}

// compareStrings compares two strings using the supplied collation sequence.
func compareStrings(l, r string, coll *CollSeq) CompareResult {
	if coll == nil {
		coll = CollSeqBinary
	}
	return intToCompareResult(coll.Compare(l, r))
}

// compareBlobs compares two byte slices.
func compareBlobs(l, r []byte) CompareResult {
	return intToCompareResult(bytes.Compare(l, r))
}

// isNumeric reports whether a value is an int64 or float64.
func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int64, float64:
		return true
	}
	return false
}

// CompareValues compares two values according to SQLite semantics.
// Returns CmpLess, CmpEqual, CmpGreater, or CmpNull.
func CompareValues(left, right interface{}, aff Affinity, coll *CollSeq) CompareResult {
	if left == nil || right == nil {
		return CmpNull
	}

	left = ApplyAffinity(left, aff)
	right = ApplyAffinity(right, aff)

	if result, ok := compareSameType(left, right, coll); ok {
		return result
	}

	if isNumeric(left) && isNumeric(right) {
		return compareNumerics(left, right)
	}

	return intToCompareResult(valueType(left) - valueType(right))
}

func compareSameType(left, right interface{}, coll *CollSeq) (CompareResult, bool) {
	if lv, ok := left.(int64); ok {
		if rv, ok := right.(int64); ok {
			return compareIntegers(lv, rv), true
		}
	}
	if lv, ok := left.(string); ok {
		if rv, ok := right.(string); ok {
			return compareStrings(lv, rv, coll), true
		}
	}
	if lv, ok := left.([]byte); ok {
		if rv, ok := right.([]byte); ok {
			return compareBlobs(lv, rv), true
		}
	}
	return CmpNull, false
}

// valueType returns a type order for mixed comparisons.
func valueType(v interface{}) int {
	switch v.(type) {
	case nil:
		return 0
	case int64:
		return 1
	case float64:
		return 2
	case string:
		return 3
	case []byte:
		return 4
	default:
		return 5
	}
}

// comparisonPredicate maps each standard comparison OpCode to a predicate that
// accepts a non-NULL CompareResult and returns the boolean outcome.
var comparisonPredicate = map[OpCode]func(CompareResult) bool{
	OpEq: func(c CompareResult) bool { return c == CmpEqual },
	OpNe: func(c CompareResult) bool { return c != CmpEqual },
	OpLt: func(c CompareResult) bool { return c == CmpLess },
	OpLe: func(c CompareResult) bool { return c == CmpLess || c == CmpEqual },
	OpGt: func(c CompareResult) bool { return c == CmpGreater },
	OpGe: func(c CompareResult) bool { return c == CmpGreater || c == CmpEqual },
}

// evaluateIs implements the IS operator (NULL-safe equality).
func evaluateIs(left, right interface{}, cmp CompareResult) interface{} {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	return cmp == CmpEqual
}

// evaluateIsNot implements the IS NOT operator (NULL-safe inequality).
func evaluateIsNot(left, right interface{}, cmp CompareResult) interface{} {
	if left == nil && right == nil {
		return false
	}
	if left == nil || right == nil {
		return true
	}
	return cmp != CmpEqual
}

// EvaluateComparison evaluates a comparison expression.
// Returns true, false, or nil (for NULL result).
func EvaluateComparison(op OpCode, left, right interface{}, aff Affinity, coll *CollSeq) interface{} {
	cmp := CompareValues(left, right, aff, coll)

	if op == OpIs {
		return evaluateIs(left, right, cmp)
	}
	if op == OpIsNot {
		return evaluateIsNot(left, right, cmp)
	}

	// All remaining operators propagate NULL.
	if cmp == CmpNull {
		return nil
	}

	pred, ok := comparisonPredicate[op]
	if !ok {
		return nil
	}
	return pred(cmp)
}

// EvaluateLike evaluates the LIKE operator.
// pattern: the LIKE pattern (may contain % and _)
// str: the string to match
// escape: the escape character (0 for none)
func EvaluateLike(pattern, str string, escape rune) bool {
	return matchLike(pattern, str, escape, false)
}

// EvaluateGlob evaluates the GLOB operator.
// pattern: the GLOB pattern (may contain * and ?)
// str: the string to match
func EvaluateGlob(pattern, str string) bool {
	return matchLike(pattern, str, 0, true)
}

// matchLike implements LIKE and GLOB pattern matching.
// isGlob: true for GLOB (case-sensitive), false for LIKE (case-insensitive)
func matchLike(pattern, str string, escape rune, isGlob bool) bool {
	// Convert to runes for proper Unicode handling
	pRunes := []rune(pattern)
	sRunes := []rune(str)

	return matchLikeRunes(pRunes, sRunes, escape, isGlob, 0, 0)
}

// stepResult carries the outcome of processing a single pattern character.
// When done is true, the caller must return result immediately.
// Otherwise newPi and newSi are the updated indices to continue from.
type stepResult struct {
	newPi, newSi int
	done         bool
	result       bool
}

// stepEscape handles an escape character in the pattern.
func stepEscape(pattern, str []rune, isGlob bool, pi, si int) stepResult {
	pi++ // skip the escape rune itself
	if pi >= len(pattern) {
		return stepResult{done: true, result: false}
	}
	if si >= len(str) {
		return stepResult{done: true, result: false}
	}
	if !matchChar(pattern[pi], str[si], isGlob) {
		return stepResult{done: true, result: false}
	}
	return stepResult{newPi: pi + 1, newSi: si + 1}
}

// stepMultiWildcard handles % (LIKE) or * (GLOB): matches zero or more chars.
func stepMultiWildcard(pattern, str []rune, escape rune, isGlob bool, pi, si int) stepResult {
	pi++ // consume the wildcard
	if pi >= len(pattern) {
		// Trailing wildcard — matches everything remaining.
		return stepResult{done: true, result: true}
	}
	for si <= len(str) {
		if matchLikeRunes(pattern, str, escape, isGlob, pi, si) {
			return stepResult{done: true, result: true}
		}
		si++
	}
	return stepResult{done: true, result: false}
}

// stepSingleWildcard handles _ (LIKE) or ? (GLOB): matches exactly one char.
func stepSingleWildcard(str []rune, pi, si int) stepResult {
	if si >= len(str) {
		return stepResult{done: true, result: false}
	}
	return stepResult{newPi: pi + 1, newSi: si + 1}
}

// stepLiteral handles a literal character match.
func stepLiteral(pattern, str []rune, isGlob bool, pi, si int) stepResult {
	if si >= len(str) {
		return stepResult{done: true, result: false}
	}
	if !matchChar(pattern[pi], str[si], isGlob) {
		return stepResult{done: true, result: false}
	}
	return stepResult{newPi: pi + 1, newSi: si + 1}
}

// isMultiWildcard reports whether pc is the multi-character wildcard for the
// current mode (% for LIKE, * for GLOB).
func isMultiWildcard(pc rune, isGlob bool) bool {
	return (isGlob && pc == '*') || (!isGlob && pc == '%')
}

// isSingleWildcard reports whether pc is the single-character wildcard for the
// current mode (_ for LIKE, ? for GLOB).
func isSingleWildcard(pc rune, isGlob bool) bool {
	return (isGlob && pc == '?') || (!isGlob && pc == '_')
}

// matchLikeRunes performs recursive pattern matching.
func matchLikeRunes(pattern, str []rune, escape rune, isGlob bool, pi, si int) bool {
	for pi < len(pattern) {
		pc := pattern[pi]

		var step stepResult
		switch {
		case escape != 0 && pc == escape:
			step = stepEscape(pattern, str, isGlob, pi, si)
		case isMultiWildcard(pc, isGlob):
			step = stepMultiWildcard(pattern, str, escape, isGlob, pi, si)
		case isSingleWildcard(pc, isGlob):
			step = stepSingleWildcard(str, pi, si)
		default:
			step = stepLiteral(pattern, str, isGlob, pi, si)
		}

		if step.done {
			return step.result
		}
		pi, si = step.newPi, step.newSi
	}

	return si >= len(str)
}

// matchChar checks if two characters match.
// For GLOB, comparison is case-sensitive.
// For LIKE, comparison is case-insensitive.
func matchChar(pattern, str rune, isGlob bool) bool {
	if isGlob {
		return pattern == str
	}
	// Case-insensitive for LIKE
	return strings.EqualFold(string(pattern), string(str))
}

// EvaluateBetween evaluates the BETWEEN operator.
// value BETWEEN low AND high
func EvaluateBetween(value, low, high interface{}, aff Affinity, coll *CollSeq) interface{} {
	// value >= low AND value <= high
	cmpLow := CompareValues(value, low, aff, coll)
	cmpHigh := CompareValues(value, high, aff, coll)

	if cmpLow == CmpNull || cmpHigh == CmpNull {
		return nil
	}

	return (cmpLow == CmpGreater || cmpLow == CmpEqual) &&
		(cmpHigh == CmpLess || cmpHigh == CmpEqual)
}

// EvaluateIn evaluates the IN operator.
// value IN (list...)
func EvaluateIn(value interface{}, list []interface{}, aff Affinity, coll *CollSeq) interface{} {
	if value == nil {
		return nil
	}

	hasNull := false
	for _, item := range list {
		if item == nil {
			hasNull = true
			continue
		}

		cmp := CompareValues(value, item, aff, coll)
		if cmp == CmpEqual {
			return true
		}
	}

	// If we found a NULL in the list and no match, result is NULL
	if hasNull {
		return nil
	}

	return false
}

// CoerceToNumeric attempts to convert a value to numeric.
// Returns the numeric value, or the original value if not convertible.
func CoerceToNumeric(v interface{}) interface{} {
	switch val := v.(type) {
	case int64, float64:
		return val
	case string:
		// Try integer first
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		// Try float
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
		// Not numeric
		return val
	default:
		return val
	}
}

// coerceStringToInt attempts to convert a string to integer.
func coerceStringToInt(val string) (int64, bool) {
	if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return int64(f), true
	}
	return 0, false
}

// coerceBoolToInt converts a boolean to integer (0 or 1).
func coerceBoolToInt(val bool) (int64, bool) {
	if val {
		return 1, true
	}
	return 0, true
}

// CoerceToInteger attempts to convert a value to integer.
func CoerceToInteger(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case float64:
		return int64(val), true
	case string:
		return coerceStringToInt(val)
	case bool:
		return coerceBoolToInt(val)
	default:
		return 0, false
	}
}

// CoerceToBoolean converts a value to boolean.
// SQLite treats 0 as false, non-zero as true.
func CoerceToBoolean(v interface{}) bool {
	if v == nil {
		return false
	}

	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0.0
	case string:
		// Try to parse as number
		if i, ok := CoerceToInteger(val); ok {
			return i != 0
		}
		// Non-numeric strings are considered false
		return false
	default:
		return false
	}
}
