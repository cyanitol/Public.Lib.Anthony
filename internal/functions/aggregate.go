package functions

import (
	"fmt"
	"math"
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
	result := NewIntValue(f.count)
	f.Reset()
	return result, nil
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
	result := NewIntValue(f.count)
	f.Reset()
	return result, nil
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

	switch args[0].Type() {
	case TypeInteger:
		f.sum += float64(args[0].AsInt64())
	case TypeFloat:
		f.sum += args[0].AsFloat64()
	default:
		f.sum += args[0].AsFloat64()
	}

	return nil
}

func (f *TotalFunc) Final() (Value, error) {
	result := NewFloatValue(f.sum)
	f.Reset()
	return result, nil
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

	switch args[0].Type() {
	case TypeInteger:
		f.sum += float64(args[0].AsInt64())
	case TypeFloat:
		f.sum += args[0].AsFloat64()
	default:
		f.sum += args[0].AsFloat64()
	}

	return nil
}

func (f *AvgFunc) Final() (Value, error) {
	if f.count == 0 {
		return NewNullValue(), nil
	}

	result := NewFloatValue(f.sum / float64(f.count))
	f.Reset()
	return result, nil
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
		return NewNullValue(), nil
	}

	result := f.minValue
	f.Reset()
	return result, nil
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
		return NewNullValue(), nil
	}

	result := f.maxValue
	f.Reset()
	return result, nil
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

	// Set separator from second argument (only first time)
	if len(args) == 2 && !f.hasSep {
		if !args[1].IsNull() {
			f.separator = args[1].AsString()
		} else {
			f.separator = ","
		}
		f.hasSep = true
	} else if !f.hasSep {
		f.separator = ","
		f.hasSep = true
	}

	f.values = append(f.values, args[0].AsString())
	return nil
}

func (f *GroupConcatFunc) Final() (Value, error) {
	if len(f.values) == 0 {
		return NewNullValue(), nil
	}

	result := NewTextValue(strings.Join(f.values, f.separator))
	f.Reset()
	return result, nil
}

func (f *GroupConcatFunc) Reset() {
	f.values = nil
	f.separator = ","
	f.hasSep = false
}

// Scalar versions of min/max for non-aggregate use
// These are registered separately and handle multiple arguments

// minScalarFunc implements min(X1, X2, ..., XN) as scalar function
func minScalarFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("min() requires at least 1 argument")
	}

	var minVal Value
	hasValue := false

	for _, arg := range args {
		if arg.IsNull() {
			continue
		}

		if !hasValue {
			minVal = arg
			hasValue = true
		} else {
			if compareValues(arg, minVal) < 0 {
				minVal = arg
			}
		}
	}

	if !hasValue {
		return NewNullValue(), nil
	}

	return minVal, nil
}

// maxScalarFunc implements max(X1, X2, ..., XN) as scalar function
func maxScalarFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("max() requires at least 1 argument")
	}

	var maxVal Value
	hasValue := false

	for _, arg := range args {
		if arg.IsNull() {
			continue
		}

		if !hasValue {
			maxVal = arg
			hasValue = true
		} else {
			if compareValues(arg, maxVal) > 0 {
				maxVal = arg
			}
		}
	}

	if !hasValue {
		return NewNullValue(), nil
	}

	return maxVal, nil
}

// init registers scalar versions of min/max
func init() {
	// These will be registered by the registry when needed
}

// Helper to check for NaN
func isNaN(f float64) bool {
	return math.IsNaN(f)
}

// Helper to check for infinity
func isInf(f float64) bool {
	return math.IsInf(f, 0)
}
