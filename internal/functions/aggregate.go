// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RegisterAggregateFunctions registers all aggregate functions.
func RegisterAggregateFunctions(r *Registry) {
	r.Register(&CountFunc{})
	r.Register(&CountStarFunc{})
	r.Register(&SumFunc{})
	r.Register(&TotalFunc{})
	r.Register(&AvgFunc{})
	r.Register(&MinFunc{})
	r.Register(&MaxFunc{})
	r.Register(&GroupConcatFunc{})
	r.Register(&JSONGroupArrayFunc{})
	r.Register(&JSONGroupObjectFunc{})
}

// toFloat64 converts a non-null Value to float64, preferring the integer
// representation for TypeInteger values to avoid precision loss.
func toFloat64(v Value) float64 {
	if v.Type() == TypeInteger {
		return float64(v.AsInt64())
	}
	return v.AsFloat64()
}

// Resettable defines the interface for types that can be reset.
type Resettable interface {
	Reset()
}

// finalizeAndReset is a helper that calls Reset on the resettable type and returns the result.
func finalizeAndReset(r Resettable, result Value) (Value, error) {
	r.Reset()
	return result, nil
}

// CountFunc implements count(X)
type CountFunc struct {
	count int64
}

func (f *CountFunc) Name() string { return "count" }
func (f *CountFunc) NumArgs() int { return 1 }
func (f *CountFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("count() is an aggregate function")
}

func (f *CountFunc) Step(args []Value) error {
	// Count non-NULL values
	if len(args) > 0 && !args[0].IsNull() {
		f.count++
	}
	return nil
}

func (f *CountFunc) Final() (Value, error) {
	return finalizeAndReset(f, NewIntValue(f.count))
}

func (f *CountFunc) Reset() {
	f.count = 0
}

// CountStarFunc implements count(*)
type CountStarFunc struct {
	count int64
}

func (f *CountStarFunc) Name() string { return "count(*)" }
func (f *CountStarFunc) NumArgs() int { return 0 }
func (f *CountStarFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("count(*) is an aggregate function")
}

func (f *CountStarFunc) Step(args []Value) error {
	// Count all rows
	f.count++
	return nil
}

func (f *CountStarFunc) Final() (Value, error) {
	return finalizeAndReset(f, NewIntValue(f.count))
}

func (f *CountStarFunc) Reset() {
	f.count = 0
}

// SumFunc implements sum(X)
type SumFunc struct {
	hasValue bool
	intSum   int64
	floatSum float64
	isFloat  bool
}

func (f *SumFunc) Name() string { return "sum" }
func (f *SumFunc) NumArgs() int { return 1 }
func (f *SumFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("sum() is an aggregate function")
}

func (f *SumFunc) Step(args []Value) error {
	if args[0].IsNull() {
		return nil
	}
	f.hasValue = true
	f.addValue(args[0])
	return nil
}

func (f *SumFunc) addValue(v Value) {
	switch v.Type() {
	case TypeInteger:
		f.addInteger(v.AsInt64())
	case TypeFloat:
		f.addFloat(v.AsFloat64())
	default:
		f.addFloat(v.AsFloat64())
	}
}

func (f *SumFunc) addInteger(val int64) {
	if f.isFloat {
		f.floatSum += float64(val)
		return
	}
	newSum := f.intSum + val
	if (val > 0 && newSum < f.intSum) || (val < 0 && newSum > f.intSum) {
		f.floatSum = float64(f.intSum) + float64(val)
		f.isFloat = true
	} else {
		f.intSum = newSum
	}
}

func (f *SumFunc) addFloat(val float64) {
	if !f.isFloat {
		f.floatSum = float64(f.intSum)
		f.isFloat = true
	}
	f.floatSum += val
}

func (f *SumFunc) Final() (Value, error) {
	if !f.hasValue {
		return NewNullValue(), nil
	}

	var result Value
	if f.isFloat {
		result = NewFloatValue(f.floatSum)
	} else {
		result = NewIntValue(f.intSum)
	}

	f.Reset()
	return result, nil
}

func (f *SumFunc) Reset() {
	f.hasValue = false
	f.intSum = 0
	f.floatSum = 0.0
	f.isFloat = false
}

// TotalFunc implements total(X)
// Like sum() but returns 0.0 instead of NULL for empty set
type TotalFunc struct {
	sum float64
}

func (f *TotalFunc) Name() string { return "total" }
func (f *TotalFunc) NumArgs() int { return 1 }
func (f *TotalFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("total() is an aggregate function")
}

func (f *TotalFunc) Step(args []Value) error {
	if args[0].IsNull() {
		return nil
	}
	f.sum += toFloat64(args[0])
	return nil
}

func (f *TotalFunc) Final() (Value, error) {
	return finalizeAndReset(f, NewFloatValue(f.sum))
}

func (f *TotalFunc) Reset() {
	f.sum = 0.0
}

// AvgFunc implements avg(X)
type AvgFunc struct {
	count int64
	sum   float64
}

func (f *AvgFunc) Name() string { return "avg" }
func (f *AvgFunc) NumArgs() int { return 1 }
func (f *AvgFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("avg() is an aggregate function")
}

func (f *AvgFunc) Step(args []Value) error {
	if args[0].IsNull() {
		return nil
	}
	f.count++
	f.sum += toFloat64(args[0])
	return nil
}

func (f *AvgFunc) Final() (Value, error) {
	if f.count == 0 {
		return finalizeAndReset(f, NewNullValue())
	}
	return finalizeAndReset(f, NewFloatValue(f.sum/float64(f.count)))
}

func (f *AvgFunc) Reset() {
	f.count = 0
	f.sum = 0.0
}

// MinFunc implements min(X)
type MinFunc struct {
	hasValue bool
	minValue Value
}

func (f *MinFunc) Name() string { return "min" }
func (f *MinFunc) NumArgs() int { return 1 }
func (f *MinFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("min() is an aggregate function")
}

func (f *MinFunc) Step(args []Value) error {
	if args[0].IsNull() {
		return nil
	}

	if !f.hasValue {
		f.minValue = args[0]
		f.hasValue = true
	} else {
		if compareValues(args[0], f.minValue) < 0 {
			f.minValue = args[0]
		}
	}

	return nil
}

func (f *MinFunc) Final() (Value, error) {
	if !f.hasValue {
		return finalizeAndReset(f, NewNullValue())
	}
	return finalizeAndReset(f, f.minValue)
}

func (f *MinFunc) Reset() {
	f.hasValue = false
	f.minValue = nil
}

// MaxFunc implements max(X)
type MaxFunc struct {
	hasValue bool
	maxValue Value
}

func (f *MaxFunc) Name() string { return "max" }
func (f *MaxFunc) NumArgs() int { return 1 }
func (f *MaxFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("max() is an aggregate function")
}

func (f *MaxFunc) Step(args []Value) error {
	if args[0].IsNull() {
		return nil
	}

	if !f.hasValue {
		f.maxValue = args[0]
		f.hasValue = true
	} else {
		if compareValues(args[0], f.maxValue) > 0 {
			f.maxValue = args[0]
		}
	}

	return nil
}

func (f *MaxFunc) Final() (Value, error) {
	if !f.hasValue {
		return finalizeAndReset(f, NewNullValue())
	}
	return finalizeAndReset(f, f.maxValue)
}

func (f *MaxFunc) Reset() {
	f.hasValue = false
	f.maxValue = nil
}

// GroupConcatFunc implements group_concat(X [, Y])
type GroupConcatFunc struct {
	values    []string
	separator string
	hasSep    bool
}

func (f *GroupConcatFunc) Name() string { return "group_concat" }
func (f *GroupConcatFunc) NumArgs() int { return -1 } // 1 or 2 args
func (f *GroupConcatFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("group_concat() is an aggregate function")
}

func (f *GroupConcatFunc) Step(args []Value) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("group_concat() requires 1 or 2 arguments")
	}

	if args[0].IsNull() {
		return nil
	}

	f.initializeSeparator(args)
	f.values = append(f.values, args[0].AsString())
	return nil
}

// initializeSeparator sets the separator on first call
func (f *GroupConcatFunc) initializeSeparator(args []Value) {
	if f.hasSep {
		return
	}

	if len(args) == 2 && !args[1].IsNull() {
		f.separator = args[1].AsString()
	} else {
		f.separator = ","
	}
	f.hasSep = true
}

func (f *GroupConcatFunc) Final() (Value, error) {
	if len(f.values) == 0 {
		return finalizeAndReset(f, NewNullValue())
	}
	return finalizeAndReset(f, NewTextValue(strings.Join(f.values, f.separator)))
}

func (f *GroupConcatFunc) Reset() {
	f.values = nil
	f.separator = ","
	f.hasSep = false
}

// JSONGroupArrayFunc implements json_group_array(X)
type JSONGroupArrayFunc struct {
	values []interface{}
}

func (f *JSONGroupArrayFunc) Name() string { return "json_group_array" }
func (f *JSONGroupArrayFunc) NumArgs() int { return 1 }
func (f *JSONGroupArrayFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("json_group_array() is an aggregate function")
}

func (f *JSONGroupArrayFunc) Step(args []Value) error {
	if len(args) < 1 {
		return fmt.Errorf("json_group_array() requires 1 argument")
	}
	f.values = append(f.values, valueToJSONInterface(args[0]))
	return nil
}

func (f *JSONGroupArrayFunc) Final() (Value, error) {
	if f.values == nil {
		f.values = []interface{}{}
	}
	return finalizeAndReset(f, marshalJSONValue(f.values))
}

func (f *JSONGroupArrayFunc) Reset() {
	f.values = nil
}

// JSONGroupObjectFunc implements json_group_object(key, value)
type JSONGroupObjectFunc struct {
	keys   []string
	values []interface{}
}

func (f *JSONGroupObjectFunc) Name() string { return "json_group_object" }
func (f *JSONGroupObjectFunc) NumArgs() int { return 2 }
func (f *JSONGroupObjectFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("json_group_object() is an aggregate function")
}

func (f *JSONGroupObjectFunc) Step(args []Value) error {
	if len(args) < 2 {
		return fmt.Errorf("json_group_object() requires 2 arguments")
	}
	if args[0].IsNull() {
		return nil // skip rows with NULL keys
	}
	f.keys = append(f.keys, args[0].AsString())
	f.values = append(f.values, valueToJSONInterface(args[1]))
	return nil
}

func (f *JSONGroupObjectFunc) Final() (Value, error) {
	obj := buildJSONObject(f.keys, f.values)
	return finalizeAndReset(f, marshalJSONValue(obj))
}

func (f *JSONGroupObjectFunc) Reset() {
	f.keys = nil
	f.values = nil
}

// valueToJSONInterface converts a Value to a Go interface{} for JSON marshaling.
func valueToJSONInterface(v Value) interface{} {
	if v.IsNull() {
		return nil
	}
	switch v.Type() {
	case TypeInteger:
		return v.AsInt64()
	case TypeFloat:
		return v.AsFloat64()
	case TypeBlob:
		return string(v.AsBlob())
	default:
		return v.AsString()
	}
}

// buildJSONObject constructs a map from parallel key/value slices.
func buildJSONObject(keys []string, values []interface{}) map[string]interface{} {
	obj := make(map[string]interface{}, len(keys))
	for i, k := range keys {
		obj[k] = values[i]
	}
	return obj
}

// marshalJSONValue marshals a value to JSON text, returning "[]" on error.
func marshalJSONValue(v interface{}) Value {
	data, err := json.Marshal(v)
	if err != nil {
		return NewTextValue("[]")
	}
	return NewTextValue(string(data))
}

// makeMinMaxScalar creates a scalar min/max function parameterized by comparison direction.
// cmpDir should be -1 for min (pick smaller) or +1 for max (pick larger).
func makeMinMaxScalar(name string, cmpDir int) func([]Value) (Value, error) {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("%s() requires at least 1 argument", name)
		}
		var best Value
		for _, arg := range args {
			if arg.IsNull() {
				continue
			}
			if best == nil || compareValues(arg, best)*cmpDir > 0 {
				best = arg
			}
		}
		if best == nil {
			return NewNullValue(), nil
		}
		return best, nil
	}
}

var (
	minScalarFunc = makeMinMaxScalar("min", -1)
	maxScalarFunc = makeMinMaxScalar("max", 1)
)
