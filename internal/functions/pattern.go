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

	if hasNullPatternArg(args[:2]) {
		return NewNullValue(), nil
	}

	value := args[0].AsString()
	pattern := args[1].AsString()
	return patternMatchValue(utf.Like(pattern, value, patternEscape(args))), nil
}

// globFunc implements the SQL GLOB operator as a function.
// Args: glob(value, pattern)
// Returns 1 if value matches pattern, 0 if not, NULL if either is NULL.
func globFunc(args []Value) (Value, error) {
	if hasNullPatternArg(args) {
		return NewNullValue(), nil
	}

	value := args[0].AsString()
	pattern := args[1].AsString()
	return patternMatchValue(utf.Glob(pattern, value)), nil
}

func hasNullPatternArg(args []Value) bool {
	for _, arg := range args {
		if arg.IsNull() {
			return true
		}
	}
	return false
}

func patternEscape(args []Value) rune {
	if len(args) != 3 || args[2].IsNull() {
		return 0
	}
	escStr := args[2].AsString()
	if len(escStr) == 0 {
		return 0
	}
	return []rune(escStr)[0]
}

func patternMatchValue(matched bool) Value {
	if matched {
		return NewIntValue(1)
	}
	return NewIntValue(0)
}
