// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type numericOperands struct {
	li, ri   int64
	lf, rf   float64
	lis, ris bool
}

func parseNumericOperands(left, right interface{}) (numericOperands, bool) {
	if left == nil || right == nil {
		return numericOperands{}, false
	}
	ln := CoerceToNumeric(left)
	rn := CoerceToNumeric(right)
	li, lis := ln.(int64)
	ri, ris := rn.(int64)
	lf, lisF := ln.(float64)
	rf, risF := rn.(float64)
	if !lis && !lisF {
		return numericOperands{}, false
	}
	if !ris && !risF {
		return numericOperands{}, false
	}
	return numericOperands{li, ri, lf, rf, lis, ris}, true
}

var arithmeticDispatch = map[OpCode]func(numericOperands) interface{}{
	OpPlus:      func(o numericOperands) interface{} { return add(o.li, o.lis, o.ri, o.ris, o.lf, o.rf) },
	OpMinus:     func(o numericOperands) interface{} { return subtract(o.li, o.lis, o.ri, o.ris, o.lf, o.rf) },
	OpMultiply:  func(o numericOperands) interface{} { return multiply(o.li, o.lis, o.ri, o.ris, o.lf, o.rf) },
	OpDivide:    func(o numericOperands) interface{} { return divide(o.li, o.lis, o.ri, o.ris, o.lf, o.rf) },
	OpRemainder: func(o numericOperands) interface{} { return remainder(o.li, o.lis, o.ri, o.ris, o.lf, o.rf) },
}

func EvaluateArithmetic(op OpCode, left, right interface{}) interface{} {
	ops, ok := parseNumericOperands(left, right)
	if !ok {
		return nil
	}
	fn, ok := arithmeticDispatch[op]
	if !ok {
		return nil
	}
	return fn(ops)
}

func toFloat(isInt bool, i int64, f float64) float64 {
	if isInt {
		return float64(i)
	}
	return f
}

func addOverflows(li, ri, result int64) bool {
	return (li > 0 && ri > 0 && result < 0) || (li < 0 && ri < 0 && result > 0)
}

func subtractOverflows(li, ri, result int64) bool {
	return (li > 0 && ri < 0 && result < 0) || (li < 0 && ri > 0 && result > 0)
}

// add performs addition.
func add(li int64, lis bool, ri int64, ris bool, lf, rf float64) interface{} {
	if lis && ris {
		result := li + ri
		if addOverflows(li, ri, result) {
			return float64(li) + float64(ri)
		}
		return result
	}
	return toFloat(lis, li, lf) + toFloat(ris, ri, rf)
}

// subtract performs subtraction.
func subtract(li int64, lis bool, ri int64, ris bool, lf, rf float64) interface{} {
	if lis && ris {
		result := li - ri
		if subtractOverflows(li, ri, result) {
			return float64(li) - float64(ri)
		}
		return result
	}
	return toFloat(lis, li, lf) - toFloat(ris, ri, rf)
}

// multiply performs multiplication.
func multiply(li int64, lis bool, ri int64, ris bool, lf, rf float64) interface{} {
	if lis && ris {
		result := li * ri
		check := float64(li) * float64(ri)
		if float64(result) != check || math.IsInf(check, 0) {
			return check
		}
		return result
	}
	return toFloat(lis, li, lf) * toFloat(ris, ri, rf)
}

func isDivideByZero(ris bool, ri int64, rf float64) bool {
	if ris {
		return ri == 0
	}
	return rf == 0.0
}

func resolveFloatVal(isInt bool, i int64, f float64) float64 {
	if isInt {
		return float64(i)
	}
	return f
}

func divideIntegers(li, ri int64) interface{} {
	if ri == -1 && li == math.MinInt64 {
		return float64(li) / float64(ri)
	}
	return li / ri
}

func divideFloats(lf, rf float64) interface{} {
	result := lf / rf
	if math.IsInf(result, 0) {
		return nil
	}
	return result
}

func divide(li int64, lis bool, ri int64, ris bool, lf, rf float64) interface{} {
	if isDivideByZero(ris, ri, rf) {
		return nil
	}
	if lis && ris {
		return divideIntegers(li, ri)
	}
	return divideFloats(resolveFloatVal(lis, li, lf), resolveFloatVal(ris, ri, rf))
}

// remainder performs modulo operation.
func remainder(li int64, lis bool, ri int64, ris bool, lf, rf float64) interface{} {
	if isModZero(ri, ris, rf) {
		return nil
	}
	if lis && ris {
		return li % ri
	}
	return math.Mod(resolveFloatVal(lis, li, lf), resolveFloatVal(ris, ri, rf))
}

// isModZero checks if the right operand is zero for modulo.
func isModZero(ri int64, ris bool, rf float64) bool {
	return (ris && ri == 0) || (!ris && rf == 0.0)
}

// EvaluateUnary evaluates a unary arithmetic expression.
func EvaluateUnary(op OpCode, operand interface{}) interface{} {
	if operand == nil {
		return nil
	}

	switch op {
	case OpNegate:
		return negate(operand)
	case OpUnaryPlus:
		// Unary plus: convert to numeric but don't change sign
		return CoerceToNumeric(operand)
	case OpBitNot:
		return bitNot(operand)
	default:
		return nil
	}
}

// negate performs unary negation.
func negate(v interface{}) interface{} {
	switch val := v.(type) {
	case int64:
		// Check for overflow (negating MinInt64)
		if val == math.MinInt64 {
			return -float64(val)
		}
		return -val
	case float64:
		return -val
	case string:
		// Try to parse as number
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			if i == math.MinInt64 {
				return -float64(i)
			}
			return -i
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return -f
		}
		// Non-numeric string becomes 0
		return int64(0)
	default:
		return int64(0)
	}
}

// bitNot performs bitwise NOT.
func bitNot(v interface{}) interface{} {
	i, ok := CoerceToInteger(v)
	if !ok {
		return int64(0)
	}
	return ^i
}

// bitwiseLShift performs a left-shift, clamping out-of-range shifts to zero.
func bitwiseLShift(left, right int64) int64 {
	if right < 0 || right >= 64 {
		return 0
	}
	return left << uint(right)
}

// bitwiseRShift performs a right-shift, propagating the sign bit for
// out-of-range shifts (matches SQLite semantics).
func bitwiseRShift(left, right int64) int64 {
	if right < 0 || right >= 64 {
		if left < 0 {
			return -1
		}
		return 0
	}
	return left >> uint(right)
}

// bitwiseDispatch maps each bitwise OpCode to its two-operand int64 function.
var bitwiseDispatch = map[OpCode]func(int64, int64) int64{
	OpBitAnd: func(l, r int64) int64 { return l & r },
	OpBitOr:  func(l, r int64) int64 { return l | r },
	OpBitXor: func(l, r int64) int64 { return l ^ r },
	OpLShift: bitwiseLShift,
	OpRShift: bitwiseRShift,
}

// EvaluateBitwise evaluates a bitwise operation.
func EvaluateBitwise(op OpCode, left, right interface{}) interface{} {
	if left == nil || right == nil {
		return nil
	}

	leftInt, leftOk := CoerceToInteger(left)
	rightInt, rightOk := CoerceToInteger(right)

	if !leftOk || !rightOk {
		return nil
	}

	fn, ok := bitwiseDispatch[op]
	if !ok {
		return nil
	}

	return fn(leftInt, rightInt)
}

// EvaluateConcat evaluates string concatenation.
func EvaluateConcat(left, right interface{}) interface{} {
	// NULL propagation
	if left == nil || right == nil {
		return nil
	}

	// Convert both operands to strings
	leftStr := valueToString(left)
	rightStr := valueToString(right)

	return leftStr + rightStr
}

// valueToString converts a value to string for concatenation.
func valueToString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		// Use SQLite's float formatting
		return formatFloat(val)
	case []byte:
		return string(val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatFloat formats a float like SQLite does.
func formatFloat(f float64) string {
	// SQLite uses a specific format for floats
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Inf"
	}
	if math.IsInf(f, -1) {
		return "-Inf"
	}

	// Use 'g' format but ensure enough precision
	s := strconv.FormatFloat(f, 'g', -1, 64)

	// If there's no decimal point and no exponent, add .0
	if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
		s += ".0"
	}

	return s
}

// EvaluateLogical evaluates a logical operation.
func EvaluateLogical(op OpCode, left, right interface{}) interface{} {
	switch op {
	case OpAnd:
		return evaluateAnd(left, right)
	case OpOr:
		return evaluateOr(left, right)
	case OpNot:
		return evaluateNot(left)
	default:
		return nil
	}
}

// evaluateAnd evaluates logical AND with NULL handling.
// SQLite three-valued logic:
//
//	true AND true = true
//	true AND false = false
//	true AND NULL = NULL
//	false AND anything = false
//	NULL AND false = false
//	NULL AND true = NULL
//	NULL AND NULL = NULL
func evaluateAnd(left, right interface{}) interface{} {
	// Handle NULL cases first
	leftIsNull := left == nil
	rightIsNull := right == nil

	// If left is definitely false (not NULL), result is false
	if !leftIsNull && !CoerceToBoolean(left) {
		return false
	}
	// If right is definitely false (not NULL), result is false
	if !rightIsNull && !CoerceToBoolean(right) {
		return false
	}

	// At this point, both are either true or NULL
	// If either is NULL, result is NULL
	if leftIsNull || rightIsNull {
		return nil
	}

	// Both are true
	return true
}

// evaluateOr evaluates logical OR with NULL handling.
// SQLite three-valued logic:
//
//	false OR false = false
//	false OR true = true
//	false OR NULL = NULL
//	true OR anything = true
//	NULL OR true = true
//	NULL OR false = NULL
//	NULL OR NULL = NULL
func evaluateOr(left, right interface{}) interface{} {
	// Handle NULL cases first
	leftIsNull := left == nil
	rightIsNull := right == nil

	// If left is definitely true (not NULL), result is true
	if !leftIsNull && CoerceToBoolean(left) {
		return true
	}
	// If right is definitely true (not NULL), result is true
	if !rightIsNull && CoerceToBoolean(right) {
		return true
	}

	// At this point, both are either false or NULL
	// If either is NULL, result is NULL
	if leftIsNull || rightIsNull {
		return nil
	}

	// Both are false
	return false
}

// evaluateNot evaluates logical NOT.
func evaluateNot(operand interface{}) interface{} {
	if operand == nil {
		return nil
	}

	return !CoerceToBoolean(operand)
}

// castToInteger casts a value to int64 for AFF_INTEGER.
func castToInteger(value interface{}) interface{} {
	i, ok := CoerceToInteger(value)
	if ok {
		return i
	}
	return int64(0)
}

// castToReal casts a value to float64 for AFF_REAL.
func castToReal(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0.0
		}
		return f
	default:
		return 0.0
	}
}

// castToNumeric casts a value to the best numeric type for AFF_NUMERIC.
func castToNumeric(value interface{}) interface{} {
	if i, ok := CoerceToInteger(value); ok {
		return i
	}
	s, ok := value.(string)
	if ok {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return value
}

// castToBlob casts a value to []byte for AFF_BLOB.
func castToBlob(value interface{}) interface{} {
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte(valueToString(value))
	}
}

// castDispatch maps each Affinity constant to its CAST conversion function.
// AFF_TEXT is handled inline via valueToString; AFF_NONE and unknowns pass
// the value through unchanged.
var castDispatch = map[Affinity]func(interface{}) interface{}{
	AFF_INTEGER: castToInteger,
	AFF_REAL:    castToReal,
	AFF_NUMERIC: castToNumeric,
	AFF_BLOB:    castToBlob,
}

// EvaluateCast performs type casting.
func EvaluateCast(value interface{}, targetType string) interface{} {
	if value == nil {
		return nil
	}

	aff := AffinityFromType(targetType)

	if aff == AFF_TEXT {
		return valueToString(value)
	}

	if cast, ok := castDispatch[aff]; ok {
		return cast(value)
	}

	return value
}
