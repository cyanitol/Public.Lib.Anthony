package functions

import (
	"fmt"
)

// UserFunction is the interface for user-defined scalar functions.
// Users implement this interface to create custom SQL functions.
type UserFunction interface {
	// Invoke executes the function with the given arguments.
	// Returns the result value and any error encountered.
	Invoke(args []Value) (Value, error)
}

// UserAggregateFunction is the interface for user-defined aggregate functions.
// Users implement this interface to create custom aggregate SQL functions.
type UserAggregateFunction interface {
	// Step processes one row of data during aggregation.
	// This is called once per row in the group.
	Step(args []Value) error

	// Final returns the final aggregate result after all Step calls.
	// This is called once at the end of the aggregation.
	Final() (Value, error)

	// Reset resets the aggregate state for reuse.
	// This is called to prepare the function for a new aggregation.
	Reset()
}

// FunctionConfig holds configuration for a user-defined function.
type FunctionConfig struct {
	// Name is the function name used in SQL
	Name string

	// NumArgs is the number of arguments the function accepts.
	// Use -1 for variadic functions (any number of arguments).
	NumArgs int

	// Deterministic indicates whether the function always returns
	// the same result for the same inputs. Deterministic functions
	// can be optimized by the query planner.
	Deterministic bool
}

// userScalarFunc wraps a UserFunction to implement the Function interface.
type userScalarFunc struct {
	config FunctionConfig
	fn     UserFunction
}

// NewUserScalarFunc creates a Function from a UserFunction.
func NewUserScalarFunc(config FunctionConfig, fn UserFunction) Function {
	return &userScalarFunc{
		config: config,
		fn:     fn,
	}
}

func (f *userScalarFunc) Name() string {
	return f.config.Name
}

func (f *userScalarFunc) NumArgs() int {
	return f.config.NumArgs
}

func (f *userScalarFunc) Call(args []Value) (Value, error) {
	if f.config.NumArgs >= 0 && len(args) != f.config.NumArgs {
		return nil, fmt.Errorf("%s() takes exactly %d arguments (%d given)",
			f.config.Name, f.config.NumArgs, len(args))
	}
	return f.fn.Invoke(args)
}

// IsDeterministic returns whether the function is deterministic.
func (f *userScalarFunc) IsDeterministic() bool {
	return f.config.Deterministic
}

// userAggregateFunc wraps a UserAggregateFunction to implement the AggregateFunction interface.
type userAggregateFunc struct {
	config FunctionConfig
	fn     UserAggregateFunction
}

// NewUserAggregateFunc creates an AggregateFunction from a UserAggregateFunction.
func NewUserAggregateFunc(config FunctionConfig, fn UserAggregateFunction) AggregateFunction {
	return &userAggregateFunc{
		config: config,
		fn:     fn,
	}
}

func (f *userAggregateFunc) Name() string {
	return f.config.Name
}

func (f *userAggregateFunc) NumArgs() int {
	return f.config.NumArgs
}

func (f *userAggregateFunc) Call(args []Value) (Value, error) {
	return nil, fmt.Errorf("%s() is an aggregate function", f.config.Name)
}

func (f *userAggregateFunc) Step(args []Value) error {
	if f.config.NumArgs >= 0 && len(args) != f.config.NumArgs {
		return fmt.Errorf("%s() takes exactly %d arguments (%d given)",
			f.config.Name, f.config.NumArgs, len(args))
	}
	return f.fn.Step(args)
}

func (f *userAggregateFunc) Final() (Value, error) {
	return f.fn.Final()
}

func (f *userAggregateFunc) Reset() {
	f.fn.Reset()
}

// IsDeterministic returns whether the function is deterministic.
func (f *userAggregateFunc) IsDeterministic() bool {
	return f.config.Deterministic
}

// RegisterScalarFunction registers a user-defined scalar function.
// The function can be called in SQL queries after registration.
//
// Parameters:
//   - r: The registry to register the function in
//   - config: Configuration for the function (name, arg count, determinism)
//   - fn: The UserFunction implementation
//
// Example:
//
//	type MyFunc struct{}
//	func (f *MyFunc) Invoke(args []Value) (Value, error) {
//	    return NewIntValue(42), nil
//	}
//	RegisterScalarFunction(registry, FunctionConfig{
//	    Name: "my_func",
//	    NumArgs: 0,
//	    Deterministic: true,
//	}, &MyFunc{})
func RegisterScalarFunction(r *Registry, config FunctionConfig, fn UserFunction) error {
	if config.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if fn == nil {
		return fmt.Errorf("function implementation cannot be nil")
	}

	wrapped := NewUserScalarFunc(config, fn)
	r.RegisterUser(wrapped, config.NumArgs)
	return nil
}

// RegisterAggregateFunction registers a user-defined aggregate function.
// The function can be used in GROUP BY queries and aggregations.
//
// Parameters:
//   - r: The registry to register the function in
//   - config: Configuration for the function (name, arg count, determinism)
//   - fn: The UserAggregateFunction implementation
//
// Example:
//
//	type MyAvg struct { sum float64; count int }
//	func (f *MyAvg) Step(args []Value) error {
//	    if !args[0].IsNull() {
//	        f.sum += args[0].AsFloat64()
//	        f.count++
//	    }
//	    return nil
//	}
//	func (f *MyAvg) Final() (Value, error) {
//	    if f.count == 0 { return NewNullValue(), nil }
//	    return NewFloatValue(f.sum / float64(f.count)), nil
//	}
//	func (f *MyAvg) Reset() { f.sum = 0; f.count = 0 }
//	RegisterAggregateFunction(registry, FunctionConfig{
//	    Name: "my_avg",
//	    NumArgs: 1,
//	    Deterministic: true,
//	}, &MyAvg{})
func RegisterAggregateFunction(r *Registry, config FunctionConfig, fn UserAggregateFunction) error {
	if config.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if fn == nil {
		return fmt.Errorf("function implementation cannot be nil")
	}

	wrapped := NewUserAggregateFunc(config, fn)
	r.RegisterUser(wrapped, config.NumArgs)
	return nil
}

// UnregisterFunction removes a function from the registry.
// This allows unregistering both built-in and user-defined functions.
//
// Parameters:
//   - r: The registry to unregister from
//   - name: The function name to remove
//   - numArgs: The number of arguments (-1 for variadic). This is needed
//     for function overloading support.
//
// Returns true if a function was removed, false if not found.
func UnregisterFunction(r *Registry, name string, numArgs int) bool {
	return r.Unregister(name, numArgs)
}
