// Package functions implements SQLite's built-in SQL functions.
package functions

import (
	"fmt"
)

// Value represents a SQL value with its type.
type Value interface {
	// Type returns the type of the value
	Type() ValueType

	// AsInt64 returns the value as int64
	AsInt64() int64

	// AsFloat64 returns the value as float64
	AsFloat64() float64

	// AsString returns the value as string
	AsString() string

	// AsBlob returns the value as byte slice
	AsBlob() []byte

	// IsNull returns true if the value is NULL
	IsNull() bool

	// Bytes returns the number of bytes in the value
	Bytes() int
}

// ValueType represents SQL value types.
type ValueType int

const (
	TypeNull ValueType = iota
	TypeInteger
	TypeFloat
	TypeText
	TypeBlob
)

// String returns the string representation of the type
func (t ValueType) String() string {
	switch t {
	case TypeNull:
		return "null"
	case TypeInteger:
		return "integer"
	case TypeFloat:
		return "real"
	case TypeText:
		return "text"
	case TypeBlob:
		return "blob"
	default:
		return "unknown"
	}
}

// Function is the interface for all SQL functions.
type Function interface {
	// Name returns the function name
	Name() string

	// NumArgs returns the number of arguments (-1 for variadic)
	NumArgs() int

	// Call executes the function with the given arguments
	Call(args []Value) (Value, error)
}

// AggregateFunction is the interface for aggregate SQL functions.
type AggregateFunction interface {
	Function

	// Step processes one row of data
	Step(args []Value) error

	// Final returns the final aggregate result
	Final() (Value, error)

	// Reset resets the aggregate state
	Reset()
}

// ScalarFunc is a simple scalar function implementation.
type ScalarFunc struct {
	name    string
	numArgs int
	fn      func(args []Value) (Value, error)
}

// NewScalarFunc creates a new scalar function.
func NewScalarFunc(name string, numArgs int, fn func(args []Value) (Value, error)) *ScalarFunc {
	return &ScalarFunc{
		name:    name,
		numArgs: numArgs,
		fn:      fn,
	}
}

func (f *ScalarFunc) Name() string {
	return f.name
}

func (f *ScalarFunc) NumArgs() int {
	return f.numArgs
}

func (f *ScalarFunc) Call(args []Value) (Value, error) {
	if f.numArgs >= 0 && len(args) != f.numArgs {
		return nil, fmt.Errorf("%s() takes exactly %d arguments (%d given)", f.name, f.numArgs, len(args))
	}
	return f.fn(args)
}

// SimpleValue is a basic implementation of the Value interface.
type SimpleValue struct {
	typ    ValueType
	intVal int64
	fltVal float64
	strVal string
	blbVal []byte
}

// NewNullValue creates a NULL value
func NewNullValue() Value {
	return &SimpleValue{typ: TypeNull}
}

// NewIntValue creates an integer value
func NewIntValue(v int64) Value {
	return &SimpleValue{typ: TypeInteger, intVal: v}
}

// NewFloatValue creates a float value
func NewFloatValue(v float64) Value {
	return &SimpleValue{typ: TypeFloat, fltVal: v}
}

// NewTextValue creates a text value
func NewTextValue(v string) Value {
	return &SimpleValue{typ: TypeText, strVal: v}
}

// NewBlobValue creates a blob value
func NewBlobValue(v []byte) Value {
	return &SimpleValue{typ: TypeBlob, blbVal: v}
}

func (v *SimpleValue) Type() ValueType {
	return v.typ
}

func (v *SimpleValue) AsInt64() int64 {
	switch v.typ {
	case TypeInteger:
		return v.intVal
	case TypeFloat:
		return int64(v.fltVal)
	case TypeText:
		// Parse string to int
		var i int64
		fmt.Sscanf(v.strVal, "%d", &i)
		return i
	default:
		return 0
	}
}

func (v *SimpleValue) AsFloat64() float64 {
	switch v.typ {
	case TypeFloat:
		return v.fltVal
	case TypeInteger:
		return float64(v.intVal)
	case TypeText:
		// Parse string to float
		var f float64
		fmt.Sscanf(v.strVal, "%f", &f)
		return f
	default:
		return 0.0
	}
}

func (v *SimpleValue) AsString() string {
	switch v.typ {
	case TypeText:
		return v.strVal
	case TypeInteger:
		return fmt.Sprintf("%d", v.intVal)
	case TypeFloat:
		return fmt.Sprintf("%g", v.fltVal)
	case TypeBlob:
		return string(v.blbVal)
	default:
		return ""
	}
}

func (v *SimpleValue) AsBlob() []byte {
	switch v.typ {
	case TypeBlob:
		return v.blbVal
	case TypeText:
		return []byte(v.strVal)
	default:
		return nil
	}
}

func (v *SimpleValue) IsNull() bool {
	return v.typ == TypeNull
}

func (v *SimpleValue) Bytes() int {
	switch v.typ {
	case TypeText:
		return len(v.strVal)
	case TypeBlob:
		return len(v.blbVal)
	case TypeInteger:
		return 8 // int64 size
	case TypeFloat:
		return 8 // float64 size
	default:
		return 0
	}
}

// Registry holds all registered functions.
type Registry struct {
	functions map[string]Function
}

// NewRegistry creates a new function registry.
func NewRegistry() *Registry {
	return &Registry{
		functions: make(map[string]Function),
	}
}

// Register registers a function.
func (r *Registry) Register(fn Function) {
	r.functions[fn.Name()] = fn
}

// Lookup finds a function by name.
func (r *Registry) Lookup(name string) (Function, bool) {
	fn, ok := r.functions[name]
	return fn, ok
}

// GetAllFunctions returns all registered functions.
func (r *Registry) GetAllFunctions() []Function {
	result := make([]Function, 0, len(r.functions))
	for _, fn := range r.functions {
		result = append(result, fn)
	}
	return result
}

// DefaultRegistry returns a registry with all standard SQLite functions.
func DefaultRegistry() *Registry {
	r := NewRegistry()

	// Register scalar functions
	RegisterScalarFunctions(r)

	// Register aggregate functions
	RegisterAggregateFunctions(r)

	// Register date/time functions
	RegisterDateTimeFunctions(r)

	// Register math functions
	RegisterMathFunctions(r)

	return r
}
