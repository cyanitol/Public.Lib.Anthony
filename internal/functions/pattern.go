// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/utf"
)

// RegisterPatternFunctions registers LIKE and GLOB pattern matching functions.
func RegisterPatternFunctions(r *Registry) {
	r.Register(NewScalarFunc("like", -1, likeFunc)) // 2 or 3 args
	r.Register(NewScalarFunc("glob", 2, globFunc))  // 2 args
}

// likeFunc implements the SQL LIKE operator as a function.
// Args: like(value, pattern) or like(value, pattern, escape)
// Returns 1 if value matches pattern, 0 if not, NULL if either is NULL.
func likeFunc(args []Value) (Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("like() takes 2 or 3 arguments (%d given)", len(args))
	}

	// NULL propagation: if either operand is NULL, result is NULL
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	value := args[0].AsString()
	pattern := args[1].AsString()

	var escape rune
	if len(args) == 3 && !args[2].IsNull() {
		escStr := args[2].AsString()
		if len(escStr) > 0 {
			runes := []rune(escStr)
			escape = runes[0]
		}
	}

	if utf.Like(pattern, value, escape) {
		return NewIntValue(1), nil
	}
	return NewIntValue(0), nil
}

// globFunc implements the SQL GLOB operator as a function.
// Args: glob(value, pattern)
// Returns 1 if value matches pattern, 0 if not, NULL if either is NULL.
func globFunc(args []Value) (Value, error) {
	// NULL propagation
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	value := args[0].AsString()
	pattern := args[1].AsString()

	if utf.Glob(pattern, value) {
		return NewIntValue(1), nil
	}
	return NewIntValue(0), nil
}
