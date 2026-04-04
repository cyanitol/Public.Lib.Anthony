// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
)

// newMathFunc creates a single-arg math function with null handling.
func newMathFunc(fn func(float64) float64) func([]Value) (Value, error) {
	return func(args []Value) (Value, error) {
		if args[0].IsNull() {
			return NewNullValue(), nil
		}
		return NewFloatValue(fn(args[0].AsFloat64())), nil
	}
}

// newMathFuncWithCheck creates a single-arg math function with a domain check.
// If invalid returns true for the input, NaN is returned.
func newMathFuncWithCheck(fn func(float64) float64, invalid func(float64) bool) func([]Value) (Value, error) {
	return func(args []Value) (Value, error) {
		if args[0].IsNull() {
			return NewNullValue(), nil
		}
		v := args[0].AsFloat64()
		if invalid(v) {
			return NewFloatValue(math.NaN()), nil
		}
		return NewFloatValue(fn(v)), nil
	}
}

// Single-arg math functions created via factories.
// Named variables are kept for backward compatibility with tests.
var (
	ceilFunc    = newMathFunc(math.Ceil)
	floorFunc   = newMathFunc(math.Floor)
	expFunc     = newMathFunc(math.Exp)
	sinFunc     = newMathFunc(math.Sin)
	cosFunc     = newMathFunc(math.Cos)
	tanFunc     = newMathFunc(math.Tan)
	atanFunc    = newMathFunc(math.Atan)
	sinhFunc    = newMathFunc(math.Sinh)
	coshFunc    = newMathFunc(math.Cosh)
	tanhFunc    = newMathFunc(math.Tanh)
	asinhFunc   = newMathFunc(math.Asinh)
	radiansFunc = newMathFunc(func(x float64) float64 { return x * math.Pi / 180.0 })
	degreesFunc = newMathFunc(func(x float64) float64 { return x * 180.0 / math.Pi })

	sqrtFunc  = newMathFuncWithCheck(math.Sqrt, func(v float64) bool { return v < 0 })
	lnFunc    = newMathFuncWithCheck(math.Log, func(v float64) bool { return v <= 0 })
	log10Func = newMathFuncWithCheck(math.Log10, func(v float64) bool { return v <= 0 })
	log2Func  = newMathFuncWithCheck(math.Log2, func(v float64) bool { return v <= 0 })
	asinFunc  = newMathFuncWithCheck(math.Asin, func(v float64) bool { return v < -1 || v > 1 })
	acosFunc  = newMathFuncWithCheck(math.Acos, func(v float64) bool { return v < -1 || v > 1 })
	acoshFunc = newMathFuncWithCheck(math.Acosh, func(v float64) bool { return v < 1 })
	atanhFunc = newMathFuncWithCheck(math.Atanh, func(v float64) bool { return v <= -1 || v >= 1 })
)

// RegisterMathFunctions registers all math functions.
func RegisterMathFunctions(r *Registry) {
	r.Register(NewScalarFunc("abs", 1, absFunc))
	// Note: min/max are registered as aggregate functions in RegisterAggregateFunctions
	// SQLite uses aggregate min/max by default; scalar versions would need different names
	r.Register(NewScalarFunc("round", -1, roundFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("trunc", -1, truncFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("random", 0, randomFunc))
	r.Register(NewScalarFunc("randomblob", 1, randomblobFunc))

	// Extended math functions
	r.Register(NewScalarFunc("ceil", 1, ceilFunc))
	r.Register(NewScalarFunc("ceiling", 1, ceilFunc))
	r.Register(NewScalarFunc("floor", 1, floorFunc))
	r.Register(NewScalarFunc("sqrt", 1, sqrtFunc))
	r.Register(NewScalarFunc("power", 2, powerFunc))
	r.Register(NewScalarFunc("pow", 2, powerFunc))
	r.Register(NewScalarFunc("exp", 1, expFunc))
	r.Register(NewScalarFunc("ln", 1, lnFunc))
	r.Register(NewScalarFunc("log", -1, logVariadicFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("log10", 1, log10Func))
	r.Register(NewScalarFunc("log2", 1, log2Func))

	// Trigonometric functions
	r.Register(NewScalarFunc("sin", 1, sinFunc))
	r.Register(NewScalarFunc("cos", 1, cosFunc))
	r.Register(NewScalarFunc("tan", 1, tanFunc))
	r.Register(NewScalarFunc("asin", 1, asinFunc))
	r.Register(NewScalarFunc("acos", 1, acosFunc))
	r.Register(NewScalarFunc("atan", 1, atanFunc))
	r.Register(NewScalarFunc("atan2", 2, atan2Func))

	// Hyperbolic functions
	r.Register(NewScalarFunc("sinh", 1, sinhFunc))
	r.Register(NewScalarFunc("cosh", 1, coshFunc))
	r.Register(NewScalarFunc("tanh", 1, tanhFunc))
	r.Register(NewScalarFunc("asinh", 1, asinhFunc))
	r.Register(NewScalarFunc("acosh", 1, acoshFunc))
	r.Register(NewScalarFunc("atanh", 1, atanhFunc))

	// Other functions
	r.Register(NewScalarFunc("sign", 1, signFunc))
	r.Register(NewScalarFunc("mod", 2, modFunc))
	r.Register(NewScalarFunc("pi", 0, piFunc))
	r.Register(NewScalarFunc("radians", 1, radiansFunc))
	r.Register(NewScalarFunc("degrees", 1, degreesFunc))
}

// absFunc implements abs(X)
// Returns the absolute value of X
func absFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	switch args[0].Type() {
	case TypeInteger:
		val := args[0].AsInt64()
		if val < 0 {
			// Check for overflow (most negative int64)
			if val == math.MinInt64 {
				return nil, fmt.Errorf("integer overflow")
			}
			return NewIntValue(-val), nil
		}
		return NewIntValue(val), nil

	case TypeFloat:
		return NewFloatValue(math.Abs(args[0].AsFloat64())), nil

	default:
		// Try to convert to number
		f := args[0].AsFloat64()
		return NewFloatValue(math.Abs(f)), nil
	}
}

func roundParsePrecision(args []Value) (int64, bool, error) {
	if len(args) < 1 || len(args) > 2 {
		return 0, false, fmt.Errorf("round() requires 1 or 2 arguments")
	}
	if len(args) == 1 {
		return 0, true, nil
	}
	if args[1].IsNull() {
		return 0, false, nil
	}
	p := args[1].AsInt64()
	if p > 30 {
		p = 30
	}
	if p < 0 {
		p = 0
	}
	return p, true, nil
}

func roundIsPassthrough(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value) >= 4503599627370496.0
}

func roundToIntValue(rounded float64) Value {
	if rounded >= float64(math.MinInt64) && rounded <= float64(math.MaxInt64) {
		return NewIntValue(int64(rounded))
	}
	return NewFloatValue(rounded)
}

func roundFunc(args []Value) (Value, error) {
	precision, ok, err := roundParsePrecision(args)
	if err != nil {
		return nil, err
	}
	if !ok || args[0].IsNull() {
		return NewNullValue(), nil
	}
	value := args[0].AsFloat64()
	if roundIsPassthrough(value) {
		return NewFloatValue(value), nil
	}
	if precision == 0 {
		return roundToIntValue(math.Round(value)), nil
	}
	multiplier := math.Pow(10, float64(precision))
	return NewFloatValue(math.Round(value*multiplier) / multiplier), nil
}

// truncFunc implements trunc(X) and trunc(X,Y)
// Truncates X toward zero, optionally to Y decimal places.
func truncFunc(args []Value) (Value, error) {
	precision, ok, err := roundParsePrecision(args)
	if err != nil {
		return nil, fmt.Errorf("trunc() requires 1 or 2 arguments")
	}
	if !ok || args[0].IsNull() {
		return NewNullValue(), nil
	}
	value := args[0].AsFloat64()
	if roundIsPassthrough(value) {
		return NewFloatValue(value), nil
	}
	if precision == 0 {
		return roundToIntValue(math.Trunc(value)), nil
	}
	multiplier := math.Pow(10, float64(precision))
	return NewFloatValue(math.Trunc(value*multiplier) / multiplier), nil
}

// randomFunc implements random()
// Returns a pseudo-random integer between -9223372036854775808 and +9223372036854775807
func randomFunc(args []Value) (Value, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("failed to generate random number: %w", err)
	}

	// Convert to int64 - intentionally allows values > MaxInt64 for full random range
	r := int64(binary.LittleEndian.Uint64(buf[:]))

	// Prevent returning the most negative value to avoid abs() issues
	if r < 0 {
		r = -(r & math.MaxInt64)
	}

	return NewIntValue(r), nil
}

// randomblobFunc implements randomblob(N)
// Returns a blob of N random bytes
func randomblobFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	n := args[0].AsInt64()
	if n < 1 {
		n = 1
	}
	if n > maxBlobSize {
		return nil, fmt.Errorf("randomblob(%d) exceeds maximum blob size of %d bytes", n, maxBlobSize)
	}

	blob := make([]byte, n)
	if _, err := rand.Read(blob); err != nil {
		return nil, fmt.Errorf("failed to generate random blob: %w", err)
	}

	return NewBlobValue(blob), nil
}

// powerFunc implements power(X, Y) / pow(X, Y)
func powerFunc(args []Value) (Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	base := args[0].AsFloat64()
	exponent := args[1].AsFloat64()

	return NewFloatValue(math.Pow(base, exponent)), nil
}

// atan2Func implements atan2(Y, X)
func atan2Func(args []Value) (Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	y := args[0].AsFloat64()
	x := args[1].AsFloat64()

	return NewFloatValue(math.Atan2(y, x)), nil
}

// signFunc implements sign(X)
// Returns -1, 0, or +1 depending on the sign of X
func signFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	value := args[0].AsFloat64()
	if value > 0 {
		return NewIntValue(1), nil
	} else if value < 0 {
		return NewIntValue(-1), nil
	}
	return NewIntValue(0), nil
}

// modFunc implements mod(X, Y)
// Returns X % Y
func modFunc(args []Value) (Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	y := args[1].AsInt64()
	if y == 0 {
		return NewNullValue(), nil // Division by zero returns NULL
	}

	x := args[0].AsInt64()
	return NewIntValue(x % y), nil
}

// piFunc implements pi()
// Returns the value of π
func piFunc(args []Value) (Value, error) {
	return NewFloatValue(math.Pi), nil
}
