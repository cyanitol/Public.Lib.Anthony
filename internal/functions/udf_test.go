// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"fmt"
	"strings"
	"testing"
)

// Test scalar UDF implementations

// doubleFunc doubles an integer value
type doubleFunc struct{}

func (f *doubleFunc) Invoke(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}
	return NewIntValue(args[0].AsInt64() * 2), nil
}

// addFunc adds two numbers
type addFunc struct{}

func (f *addFunc) Invoke(args []Value) (Value, error) {
	if args[0].IsNull() || args[1].IsNull() {
		return NewNullValue(), nil
	}

	// If either is float, return float
	if args[0].Type() == TypeFloat || args[1].Type() == TypeFloat {
		return NewFloatValue(args[0].AsFloat64() + args[1].AsFloat64()), nil
	}

	return NewIntValue(args[0].AsInt64() + args[1].AsInt64()), nil
}

// concatFunc concatenates string arguments
type concatFunc struct{}

func (f *concatFunc) Invoke(args []Value) (Value, error) {
	var sb strings.Builder
	for _, arg := range args {
		if !arg.IsNull() {
			sb.WriteString(arg.AsString())
		}
	}
	return NewTextValue(sb.String()), nil
}

// Test aggregate UDF implementations

// productFunc multiplies values together
type productFunc struct {
	product  int64
	hasValue bool
}

func (f *productFunc) Step(args []Value) error {
	if !args[0].IsNull() {
		if !f.hasValue {
			f.product = 1
			f.hasValue = true
		}
		f.product *= args[0].AsInt64()
	}
	return nil
}

func (f *productFunc) Final() (Value, error) {
	if !f.hasValue {
		return NewNullValue(), nil
	}
	return NewIntValue(f.product), nil
}

func (f *productFunc) Reset() {
	f.product = 1
	f.hasValue = false
}

// customSumFunc sums values with custom state
type customSumFunc struct {
	sum      float64
	count    int
	hasValue bool
}

func (f *customSumFunc) Step(args []Value) error {
	if !args[0].IsNull() {
		f.sum += args[0].AsFloat64()
		f.count++
		f.hasValue = true
	}
	return nil
}

func (f *customSumFunc) Final() (Value, error) {
	if !f.hasValue {
		return NewNullValue(), nil
	}
	return NewFloatValue(f.sum), nil
}

func (f *customSumFunc) Reset() {
	f.sum = 0
	f.count = 0
	f.hasValue = false
}

// Test basic scalar function registration and invocation

func TestRegisterScalarFunction(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:          "double",
		NumArgs:       1,
		Deterministic: true,
	}

	err := RegisterScalarFunction(r, config, &doubleFunc{})
	if err != nil {
		t.Fatalf("Failed to register function: %v", err)
	}

	// Look up the function
	fn, ok := r.LookupWithArgs("double", 1)
	if !ok {
		t.Fatal("Function not found after registration")
	}

	// Test invocation
	args := []Value{NewIntValue(21)}
	result, err := fn.Call(args)
	if err != nil {
		t.Fatalf("Function call failed: %v", err)
	}

	if result.Type() != TypeInteger {
		t.Errorf("Expected integer result, got %v", result.Type())
	}

	if result.AsInt64() != 42 {
		t.Errorf("Expected 42, got %d", result.AsInt64())
	}
}

func TestRegisterScalarFunctionWithNullInput(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "double",
		NumArgs: 1,
	}

	RegisterScalarFunction(r, config, &doubleFunc{})
	fn, _ := r.LookupWithArgs("double", 1)

	// Test with NULL input
	args := []Value{NewNullValue()}
	result, err := fn.Call(args)
	if err != nil {
		t.Fatalf("Function call failed: %v", err)
	}

	if !result.IsNull() {
		t.Error("Expected NULL result for NULL input")
	}
}

func TestRegisterScalarFunctionVariadic(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "concat",
		NumArgs: -1, // variadic
	}

	err := RegisterScalarFunction(r, config, &concatFunc{})
	if err != nil {
		t.Fatalf("Failed to register variadic function: %v", err)
	}

	fn, ok := r.LookupWithArgs("concat", 3)
	if !ok {
		t.Fatal("Variadic function not found")
	}

	// Test with multiple arguments
	args := []Value{
		NewTextValue("Hello"),
		NewTextValue(" "),
		NewTextValue("World"),
	}

	result, err := fn.Call(args)
	if err != nil {
		t.Fatalf("Function call failed: %v", err)
	}

	if result.AsString() != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", result.AsString())
	}
}

// Test aggregate function registration and usage

func TestRegisterAggregateFunction(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:          "product",
		NumArgs:       1,
		Deterministic: true,
	}

	err := RegisterAggregateFunction(r, config, &productFunc{})
	if err != nil {
		t.Fatalf("Failed to register aggregate function: %v", err)
	}

	fn, ok := r.LookupWithArgs("product", 1)
	if !ok {
		t.Fatal("Aggregate function not found after registration")
	}

	// Verify it's an aggregate function
	aggFn, ok := fn.(AggregateFunction)
	if !ok {
		t.Fatal("Function is not an AggregateFunction")
	}

	// Test aggregation
	values := []int64{2, 3, 4}
	for _, v := range values {
		err := aggFn.Step([]Value{NewIntValue(v)})
		if err != nil {
			t.Fatalf("Step failed: %v", err)
		}
	}

	result, err := aggFn.Final()
	if err != nil {
		t.Fatalf("Final failed: %v", err)
	}

	expected := int64(24) // 2 * 3 * 4
	if result.AsInt64() != expected {
		t.Errorf("Expected %d, got %d", expected, result.AsInt64())
	}
}

func TestAggregateWithNullValues(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "product",
		NumArgs: 1,
	}

	RegisterAggregateFunction(r, config, &productFunc{})
	fn, _ := r.LookupWithArgs("product", 1)
	aggFn := fn.(AggregateFunction)

	// Mix of values and NULLs
	values := []Value{
		NewIntValue(2),
		NewNullValue(),
		NewIntValue(5),
		NewNullValue(),
	}

	for _, v := range values {
		aggFn.Step([]Value{v})
	}

	result, err := aggFn.Final()
	if err != nil {
		t.Fatalf("Final failed: %v", err)
	}

	expected := int64(10) // 2 * 5, NULLs ignored
	if result.AsInt64() != expected {
		t.Errorf("Expected %d, got %d", expected, result.AsInt64())
	}
}

func TestAggregateAllNullValues(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "product",
		NumArgs: 1,
	}

	RegisterAggregateFunction(r, config, &productFunc{})
	fn, _ := r.LookupWithArgs("product", 1)
	aggFn := fn.(AggregateFunction)

	// All NULL values
	for i := 0; i < 3; i++ {
		aggFn.Step([]Value{NewNullValue()})
	}

	result, err := aggFn.Final()
	if err != nil {
		t.Fatalf("Final failed: %v", err)
	}

	if !result.IsNull() {
		t.Error("Expected NULL result for all NULL inputs")
	}
}

func TestAggregateReset(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "custom_sum",
		NumArgs: 1,
	}

	RegisterAggregateFunction(r, config, &customSumFunc{})
	fn, _ := r.LookupWithArgs("custom_sum", 1)
	aggFn := fn.(AggregateFunction)

	// First aggregation
	aggFn.Step([]Value{NewFloatValue(1.5)})
	aggFn.Step([]Value{NewFloatValue(2.5)})
	result1, _ := aggFn.Final()

	if result1.AsFloat64() != 4.0 {
		t.Errorf("First aggregation: expected 4.0, got %f", result1.AsFloat64())
	}

	// Reset should be called by Final, but test explicit reset
	aggFn.Reset()

	// Second aggregation with fresh state
	aggFn.Step([]Value{NewFloatValue(10.0)})
	result2, _ := aggFn.Final()

	if result2.AsFloat64() != 10.0 {
		t.Errorf("Second aggregation: expected 10.0, got %f", result2.AsFloat64())
	}
}

// Test function overloading

func TestFunctionOverloading(t *testing.T) {
	r := NewRegistry()

	// Register add function with 2 args
	config2 := FunctionConfig{
		Name:    "add",
		NumArgs: 2,
	}
	RegisterScalarFunction(r, config2, &addFunc{})

	// Register concat function with variadic args (can act as "add" for strings)
	configVar := FunctionConfig{
		Name:    "add",
		NumArgs: -1,
	}
	RegisterScalarFunction(r, configVar, &concatFunc{})

	// Lookup with 2 args should find the exact match
	fn2, ok := r.LookupWithArgs("add", 2)
	if !ok {
		t.Fatal("Function with 2 args not found")
	}

	result, _ := fn2.Call([]Value{NewIntValue(3), NewIntValue(4)})
	if result.AsInt64() != 7 {
		t.Errorf("Expected 7, got %d", result.AsInt64())
	}

	// Lookup with different arg count should find variadic
	fn3, ok := r.LookupWithArgs("add", 3)
	if !ok {
		t.Fatal("Variadic function not found")
	}

	result3, _ := fn3.Call([]Value{
		NewTextValue("a"),
		NewTextValue("b"),
		NewTextValue("c"),
	})
	if result3.AsString() != "abc" {
		t.Errorf("Expected 'abc', got '%s'", result3.AsString())
	}
}

// Test user function priority over built-ins

// userLengthFunc is a test implementation that returns a fixed value
type userLengthFunc struct{}

func (f *userLengthFunc) Invoke(args []Value) (Value, error) {
	return NewIntValue(200), nil // User function returns 200
}

func TestUserFunctionPriority(t *testing.T) {
	r := NewRegistry()

	// Register a built-in "length" function
	r.Register(NewScalarFunc("length", 1, func(args []Value) (Value, error) {
		return NewIntValue(100), nil // Built-in returns 100
	}))

	// Register user-defined "length" function
	userLength := &userLengthFunc{}

	config := FunctionConfig{
		Name:    "length",
		NumArgs: 1,
	}

	RegisterScalarFunction(r, config, userLength)

	// Lookup should find user function first
	fn, ok := r.LookupWithArgs("length", 1)
	if !ok {
		t.Fatal("Function not found")
	}

	// The user function implementation
	_, ok = fn.(*userScalarFunc)
	if !ok {
		t.Error("Expected user function, got built-in")
	}
}

// Test unregister function

func TestUnregisterFunction(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "double",
		NumArgs: 1,
	}

	RegisterScalarFunction(r, config, &doubleFunc{})

	// Verify it's registered
	_, ok := r.LookupWithArgs("double", 1)
	if !ok {
		t.Fatal("Function not found after registration")
	}

	// Unregister it
	removed := UnregisterFunction(r, "double", 1)
	if !removed {
		t.Error("Expected function to be removed")
	}

	// Verify it's gone
	_, ok = r.LookupWithArgs("double", 1)
	if ok {
		t.Error("Function still found after unregistration")
	}
}

func TestUnregisterNonExistentFunction(t *testing.T) {
	r := NewRegistry()

	removed := UnregisterFunction(r, "nonexistent", 1)
	if removed {
		t.Error("Expected false for non-existent function")
	}
}

func TestUnregisterVariadicFunction(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "concat",
		NumArgs: -1,
	}

	RegisterScalarFunction(r, config, &concatFunc{})

	// Verify it's registered
	_, ok := r.LookupWithArgs("concat", 3)
	if !ok {
		t.Fatal("Variadic function not found")
	}

	// Unregister with -1 for variadic
	removed := UnregisterFunction(r, "concat", -1)
	if !removed {
		t.Error("Expected variadic function to be removed")
	}

	// Verify it's gone
	_, ok = r.LookupWithArgs("concat", 3)
	if ok {
		t.Error("Variadic function still found after unregistration")
	}
}

// Test argument validation

func TestScalarFunctionArgValidation(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "double",
		NumArgs: 1,
	}

	RegisterScalarFunction(r, config, &doubleFunc{})
	fn, _ := r.LookupWithArgs("double", 1)

	// Try calling with wrong number of args
	_, err := fn.Call([]Value{NewIntValue(1), NewIntValue(2)})
	if err == nil {
		t.Error("Expected error for wrong number of arguments")
	}

	if !strings.Contains(err.Error(), "takes exactly 1 arguments") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestAggregateFunctionArgValidation(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "product",
		NumArgs: 1,
	}

	RegisterAggregateFunction(r, config, &productFunc{})
	fn, _ := r.LookupWithArgs("product", 1)
	aggFn := fn.(AggregateFunction)

	// Try calling Step with wrong number of args
	err := aggFn.Step([]Value{NewIntValue(1), NewIntValue(2)})
	if err == nil {
		t.Error("Expected error for wrong number of arguments")
	}

	if !strings.Contains(err.Error(), "takes exactly 1 arguments") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// Test error cases

func TestRegisterScalarFunctionEmptyName(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "",
		NumArgs: 1,
	}

	err := RegisterScalarFunction(r, config, &doubleFunc{})
	if err == nil {
		t.Error("Expected error for empty function name")
	}
}

func TestRegisterScalarFunctionNilImplementation(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "test",
		NumArgs: 1,
	}

	err := RegisterScalarFunction(r, config, nil)
	if err == nil {
		t.Error("Expected error for nil function implementation")
	}
}

func TestRegisterAggregateFunctionEmptyName(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "",
		NumArgs: 1,
	}

	err := RegisterAggregateFunction(r, config, &productFunc{})
	if err == nil {
		t.Error("Expected error for empty function name")
	}
}

func TestRegisterAggregateFunctionNilImplementation(t *testing.T) {
	r := NewRegistry()

	config := FunctionConfig{
		Name:    "test",
		NumArgs: 1,
	}

	err := RegisterAggregateFunction(r, config, nil)
	if err == nil {
		t.Error("Expected error for nil function implementation")
	}
}

// Test deterministic flag

func TestDeterministicFlag(t *testing.T) {
	r := NewRegistry()

	configDet := FunctionConfig{
		Name:          "det_func",
		NumArgs:       1,
		Deterministic: true,
	}

	RegisterScalarFunction(r, configDet, &doubleFunc{})
	fn, _ := r.LookupWithArgs("det_func", 1)

	// Check if we can access deterministic flag
	if udf, ok := fn.(*userScalarFunc); ok {
		if !udf.IsDeterministic() {
			t.Error("Expected function to be deterministic")
		}
	} else {
		t.Error("Expected userScalarFunc type")
	}

	// Test non-deterministic
	configNonDet := FunctionConfig{
		Name:          "nondet_func",
		NumArgs:       1,
		Deterministic: false,
	}

	RegisterScalarFunction(r, configNonDet, &doubleFunc{})
	fn2, _ := r.LookupWithArgs("nondet_func", 1)

	if udf, ok := fn2.(*userScalarFunc); ok {
		if udf.IsDeterministic() {
			t.Error("Expected function to be non-deterministic")
		}
	}
}

// Test multiple functions in registry

func TestMultipleFunctionsInRegistry(t *testing.T) {
	r := NewRegistry()

	// Register multiple functions
	RegisterScalarFunction(r, FunctionConfig{Name: "double", NumArgs: 1}, &doubleFunc{})
	RegisterScalarFunction(r, FunctionConfig{Name: "add", NumArgs: 2}, &addFunc{})
	RegisterScalarFunction(r, FunctionConfig{Name: "concat", NumArgs: -1}, &concatFunc{})
	RegisterAggregateFunction(r, FunctionConfig{Name: "product", NumArgs: 1}, &productFunc{})

	// Verify all are accessible
	if _, ok := r.LookupWithArgs("double", 1); !ok {
		t.Error("double not found")
	}
	if _, ok := r.LookupWithArgs("add", 2); !ok {
		t.Error("add not found")
	}
	if _, ok := r.LookupWithArgs("concat", 5); !ok {
		t.Error("concat not found")
	}
	if _, ok := r.LookupWithArgs("product", 1); !ok {
		t.Error("product not found")
	}

	// Verify GetAllFunctions returns them all
	allFuncs := r.GetAllFunctions()
	if len(allFuncs) < 4 {
		t.Errorf("Expected at least 4 functions, got %d", len(allFuncs))
	}
}

// Benchmark tests

func BenchmarkScalarFunctionCall(b *testing.B) {
	r := NewRegistry()
	config := FunctionConfig{Name: "double", NumArgs: 1}
	RegisterScalarFunction(r, config, &doubleFunc{})
	fn, _ := r.LookupWithArgs("double", 1)
	args := []Value{NewIntValue(21)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn.Call(args)
	}
}

func BenchmarkAggregateFunctionStep(b *testing.B) {
	r := NewRegistry()
	config := FunctionConfig{Name: "product", NumArgs: 1}
	RegisterAggregateFunction(r, config, &productFunc{})
	fn, _ := r.LookupWithArgs("product", 1)
	aggFn := fn.(AggregateFunction)
	args := []Value{NewIntValue(2)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggFn.Reset()
		aggFn.Step(args)
		aggFn.Final()
	}
}

// Example error-returning function

type errorFunc struct{}

func (f *errorFunc) Invoke(args []Value) (Value, error) {
	return nil, fmt.Errorf("intentional error")
}

func TestErrorReturningFunction(t *testing.T) {
	r := NewRegistry()
	config := FunctionConfig{Name: "error_func", NumArgs: 0}
	RegisterScalarFunction(r, config, &errorFunc{})
	fn, _ := r.LookupWithArgs("error_func", 0)

	_, err := fn.Call([]Value{})
	if err == nil {
		t.Error("Expected error from function")
	}

	if err.Error() != "intentional error" {
		t.Errorf("Unexpected error message: %v", err)
	}
}
