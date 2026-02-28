package functions

import (
	"fmt"
	"math"
	"testing"
)

// Test helper to create test values
func testInt(v int64) Value {
	return NewIntValue(v)
}

func testFloat(v float64) Value {
	return NewFloatValue(v)
}

func testText(v string) Value {
	return NewTextValue(v)
}

func testBlob(v []byte) Value {
	return NewBlobValue(v)
}

func testNull() Value {
	return NewNullValue()
}

// Test String Functions

func TestLength(t *testing.T) {
	tests := []struct {
		input    Value
		expected int64
	}{
		{testText("hello"), 5},
		{testText("世界"), 2}, // UTF-8 characters
		{testText(""), 0},
		{testBlob([]byte{1, 2, 3}), 3},
		{testInt(12345), 8}, // int64 size
	}

	for _, test := range tests {
		result, err := lengthFunc([]Value{test.input})
		if err != nil {
			t.Errorf("lengthFunc failed: %v", err)
			continue
		}
		if result.AsInt64() != test.expected {
			t.Errorf("lengthFunc(%v) = %d, want %d", test.input, result.AsInt64(), test.expected)
		}
	}
}

func TestSubstr(t *testing.T) {
	tests := []struct {
		str      Value
		start    Value
		length   Value
		expected string
	}{
		{testText("hello"), testInt(1), testInt(2), "he"},
		{testText("hello"), testInt(2), testInt(3), "ell"},
		{testText("hello"), testInt(-2), testInt(2), "lo"},
		{testText("hello"), testInt(1), testInt(100), "hello"},
		{testText("世界你好"), testInt(1), testInt(2), "世界"},
	}

	for _, test := range tests {
		var result Value
		var err error
		if test.length != nil {
			result, err = substrFunc([]Value{test.str, test.start, test.length})
		} else {
			result, err = substrFunc([]Value{test.str, test.start})
		}

		if err != nil {
			t.Errorf("substrFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("substrFunc(%v, %v, %v) = %s, want %s",
				test.str, test.start, test.length, result.AsString(), test.expected)
		}
	}
}

func TestUpper(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{testText("hello"), "HELLO"},
		{testText("Hello World"), "HELLO WORLD"},
		{testText("123abc"), "123ABC"},
	}

	for _, test := range tests {
		result, err := upperFunc([]Value{test.input})
		if err != nil {
			t.Errorf("upperFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("upperFunc(%v) = %s, want %s", test.input, result.AsString(), test.expected)
		}
	}
}

func TestLower(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{testText("HELLO"), "hello"},
		{testText("Hello World"), "hello world"},
		{testText("123ABC"), "123abc"},
	}

	for _, test := range tests {
		result, err := lowerFunc([]Value{test.input})
		if err != nil {
			t.Errorf("lowerFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("lowerFunc(%v) = %s, want %s", test.input, result.AsString(), test.expected)
		}
	}
}

func TestReplace(t *testing.T) {
	tests := []struct {
		str      Value
		old      Value
		new      Value
		expected string
	}{
		{testText("hello world"), testText("world"), testText("there"), "hello there"},
		{testText("aaa"), testText("a"), testText("b"), "bbb"},
		{testText("test"), testText("x"), testText("y"), "test"},
	}

	for _, test := range tests {
		result, err := replaceFunc([]Value{test.str, test.old, test.new})
		if err != nil {
			t.Errorf("replaceFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("replaceFunc(%v, %v, %v) = %s, want %s",
				test.str, test.old, test.new, result.AsString(), test.expected)
		}
	}
}

func TestInstr(t *testing.T) {
	tests := []struct {
		haystack Value
		needle   Value
		expected int64
	}{
		{testText("hello world"), testText("world"), 7},
		{testText("hello"), testText("x"), 0},
		{testText("hello"), testText(""), 1},
		{testText("世界你好"), testText("你"), 3},
	}

	for _, test := range tests {
		result, err := instrFunc([]Value{test.haystack, test.needle})
		if err != nil {
			t.Errorf("instrFunc failed: %v", err)
			continue
		}
		if result.AsInt64() != test.expected {
			t.Errorf("instrFunc(%v, %v) = %d, want %d",
				test.haystack, test.needle, result.AsInt64(), test.expected)
		}
	}
}

func TestHex(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{testBlob([]byte{0x12, 0x34, 0xAB, 0xCD}), "1234ABCD"},
		{testText("hello"), "68656C6C6F"},
		{testBlob([]byte{}), ""},
	}

	for _, test := range tests {
		result, err := hexFunc([]Value{test.input})
		if err != nil {
			t.Errorf("hexFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("hexFunc(%v) = %s, want %s", test.input, result.AsString(), test.expected)
		}
	}
}

func TestQuote(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{testInt(42), "42"},
		{testFloat(3.14), "3.14"},
		{testText("hello"), "'hello'"},
		{testText("it's"), "'it''s'"},
		{testNull(), "NULL"},
	}

	for _, test := range tests {
		result, err := quoteFunc([]Value{test.input})
		if err != nil {
			t.Errorf("quoteFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("quoteFunc(%v) = %s, want %s", test.input, result.AsString(), test.expected)
		}
	}
}

// Test Type Functions

func TestTypeof(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{testInt(42), "integer"},
		{testFloat(3.14), "real"},
		{testText("hello"), "text"},
		{testBlob([]byte{1, 2}), "blob"},
		{testNull(), "null"},
	}

	for _, test := range tests {
		result, err := typeofFunc([]Value{test.input})
		if err != nil {
			t.Errorf("typeofFunc failed: %v", err)
			continue
		}
		if result.AsString() != test.expected {
			t.Errorf("typeofFunc(%v) = %s, want %s", test.input, result.AsString(), test.expected)
		}
	}
}

func TestCoalesce(t *testing.T) {
	tests := []struct {
		args     []Value
		expected Value
	}{
		{[]Value{testNull(), testInt(42), testInt(100)}, testInt(42)},
		{[]Value{testInt(1), testInt(2)}, testInt(1)},
		{[]Value{testNull(), testNull(), testText("hello")}, testText("hello")},
		{[]Value{testNull(), testNull()}, testNull()},
	}

	for _, test := range tests {
		result, err := coalesceFunc(test.args)
		if err != nil {
			t.Errorf("coalesceFunc failed: %v", err)
			continue
		}

		if result.IsNull() != test.expected.IsNull() {
			t.Errorf("coalesceFunc null mismatch")
			continue
		}

		if !result.IsNull() && result.AsString() != test.expected.AsString() {
			t.Errorf("coalesceFunc(...) = %v, want %v", result, test.expected)
		}
	}
}

func TestNullif(t *testing.T) {
	tests := []struct {
		x        Value
		y        Value
		expected Value
	}{
		{testInt(42), testInt(42), testNull()},
		{testInt(42), testInt(100), testInt(42)},
		{testText("hello"), testText("hello"), testNull()},
		{testText("hello"), testText("world"), testText("hello")},
	}

	for _, test := range tests {
		result, err := nullifFunc([]Value{test.x, test.y})
		if err != nil {
			t.Errorf("nullifFunc failed: %v", err)
			continue
		}

		if result.IsNull() != test.expected.IsNull() {
			t.Errorf("nullifFunc(%v, %v) null mismatch", test.x, test.y)
			continue
		}

		if !result.IsNull() && result.AsInt64() != test.expected.AsInt64() {
			t.Errorf("nullifFunc(%v, %v) = %v, want %v", test.x, test.y, result, test.expected)
		}
	}
}

// Test Math Functions

func TestAbs(t *testing.T) {
	tests := []struct {
		input    Value
		expected Value
	}{
		{testInt(-42), testInt(42)},
		{testInt(42), testInt(42)},
		{testFloat(-3.14), testFloat(3.14)},
		{testFloat(3.14), testFloat(3.14)},
	}

	for _, test := range tests {
		result, err := absFunc([]Value{test.input})
		if err != nil {
			t.Errorf("absFunc failed: %v", err)
			continue
		}

		if result.Type() != test.expected.Type() {
			t.Errorf("absFunc type mismatch")
			continue
		}

		if result.Type() == TypeInteger {
			if result.AsInt64() != test.expected.AsInt64() {
				t.Errorf("absFunc(%v) = %d, want %d",
					test.input, result.AsInt64(), test.expected.AsInt64())
			}
		} else {
			if result.AsFloat64() != test.expected.AsFloat64() {
				t.Errorf("absFunc(%v) = %f, want %f",
					test.input, result.AsFloat64(), test.expected.AsFloat64())
			}
		}
	}
}

func TestRound(t *testing.T) {
	tests := []struct {
		value     Value
		precision Value
		expected  float64
	}{
		{testFloat(3.14159), testInt(2), 3.14},
		{testFloat(3.14159), testInt(0), 3.0},
		{testFloat(2.5), testInt(0), 3.0},
		{testFloat(-2.5), testInt(0), -3.0},
	}

	for _, test := range tests {
		result, err := roundFunc([]Value{test.value, test.precision})
		if err != nil {
			t.Errorf("roundFunc failed: %v", err)
			continue
		}

		got := result.AsFloat64()
		if math.Abs(got-test.expected) > 0.0001 {
			t.Errorf("roundFunc(%v, %v) = %f, want %f",
				test.value, test.precision, got, test.expected)
		}
	}
}

func TestPower(t *testing.T) {
	tests := []struct {
		base     Value
		exponent Value
		expected float64
	}{
		{testFloat(2), testFloat(3), 8.0},
		{testFloat(10), testFloat(2), 100.0},
		{testFloat(2), testFloat(-1), 0.5},
	}

	for _, test := range tests {
		result, err := powerFunc([]Value{test.base, test.exponent})
		if err != nil {
			t.Errorf("powerFunc failed: %v", err)
			continue
		}

		got := result.AsFloat64()
		if math.Abs(got-test.expected) > 0.0001 {
			t.Errorf("powerFunc(%v, %v) = %f, want %f",
				test.base, test.exponent, got, test.expected)
		}
	}
}

func TestSqrt(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{testFloat(4), 2.0},
		{testFloat(9), 3.0},
		{testFloat(2), math.Sqrt(2)},
	}

	for _, test := range tests {
		result, err := sqrtFunc([]Value{test.input})
		if err != nil {
			t.Errorf("sqrtFunc failed: %v", err)
			continue
		}

		got := result.AsFloat64()
		if math.Abs(got-test.expected) > 0.0001 {
			t.Errorf("sqrtFunc(%v) = %f, want %f", test.input, got, test.expected)
		}
	}
}

// Test Aggregate Functions

func TestCountAggregate(t *testing.T) {
	f := &CountFunc{}

	values := []Value{
		testInt(1),
		testInt(2),
		testNull(),
		testInt(3),
	}

	for _, v := range values {
		if err := f.Step([]Value{v}); err != nil {
			t.Errorf("CountFunc.Step failed: %v", err)
		}
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("CountFunc.Final failed: %v", err)
	}

	// Should count only non-NULL values
	if result.AsInt64() != 3 {
		t.Errorf("CountFunc result = %d, want 3", result.AsInt64())
	}
}

func TestSumAggregate(t *testing.T) {
	f := &SumFunc{}

	values := []Value{
		testInt(10),
		testInt(20),
		testNull(),
		testInt(30),
	}

	for _, v := range values {
		if err := f.Step([]Value{v}); err != nil {
			t.Errorf("SumFunc.Step failed: %v", err)
		}
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("SumFunc.Final failed: %v", err)
	}

	if result.AsInt64() != 60 {
		t.Errorf("SumFunc result = %d, want 60", result.AsInt64())
	}
}

func TestAvgAggregate(t *testing.T) {
	f := &AvgFunc{}

	values := []Value{
		testInt(10),
		testInt(20),
		testNull(),
		testInt(30),
	}

	for _, v := range values {
		if err := f.Step([]Value{v}); err != nil {
			t.Errorf("AvgFunc.Step failed: %v", err)
		}
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("AvgFunc.Final failed: %v", err)
	}

	expected := 20.0
	got := result.AsFloat64()
	if math.Abs(got-expected) > 0.0001 {
		t.Errorf("AvgFunc result = %f, want %f", got, expected)
	}
}

func TestMinMaxAggregate(t *testing.T) {
	minFunc := &MinFunc{}
	maxFunc := &MaxFunc{}

	values := []Value{
		testInt(30),
		testInt(10),
		testNull(),
		testInt(20),
	}

	for _, v := range values {
		minFunc.Step([]Value{v})
		maxFunc.Step([]Value{v})
	}

	minResult, _ := minFunc.Final()
	maxResult, _ := maxFunc.Final()

	if minResult.AsInt64() != 10 {
		t.Errorf("MinFunc result = %d, want 10", minResult.AsInt64())
	}

	if maxResult.AsInt64() != 30 {
		t.Errorf("MaxFunc result = %d, want 30", maxResult.AsInt64())
	}
}

func TestGroupConcatAggregate(t *testing.T) {
	f := &GroupConcatFunc{}

	values := []Value{
		testText("hello"),
		testText("world"),
		testNull(),
		testText("test"),
	}

	for _, v := range values {
		if err := f.Step([]Value{v}); err != nil {
			t.Errorf("GroupConcatFunc.Step failed: %v", err)
		}
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("GroupConcatFunc.Final failed: %v", err)
	}

	expected := "hello,world,test"
	if result.AsString() != expected {
		t.Errorf("GroupConcatFunc result = %s, want %s", result.AsString(), expected)
	}
}

// Test Registry

func TestRegistry(t *testing.T) {
	r := DefaultRegistry()

	// Test that functions are registered
	funcs := []string{
		"length", "upper", "lower", "substr", "replace",
		"abs", "round", "sqrt",
		"count", "sum", "avg", "min", "max",
		"date", "time", "datetime", "julianday",
	}

	for _, name := range funcs {
		if _, ok := r.Lookup(name); !ok {
			t.Errorf("Function %s not found in registry", name)
		}
	}
}

func TestScalarFuncExecution(t *testing.T) {
	r := DefaultRegistry()

	// Test length function through registry
	lenFunc, ok := r.Lookup("length")
	if !ok {
		t.Fatal("length function not found")
	}

	result, err := lenFunc.Call([]Value{testText("hello")})
	if err != nil {
		t.Errorf("Failed to call length: %v", err)
	}

	if result.AsInt64() != 5 {
		t.Errorf("length('hello') = %d, want 5", result.AsInt64())
	}
}

// TestValueTypeStringUnknown tests ValueType.String for unknown type
func TestValueTypeStringUnknown(t *testing.T) {
	var vt ValueType = 99 // Invalid type
	result := vt.String()
	if result != "unknown" {
		t.Errorf("Unknown ValueType.String() = %q, want 'unknown'", result)
	}
}

// TestScalarFuncCallError tests ScalarFunc.Call with function that returns error
func TestScalarFuncCallError(t *testing.T) {
	sf := NewScalarFunc("test", 1, func(args []Value) (Value, error) {
		return NewNullValue(), fmt.Errorf("test error")
	})

	_, err := sf.Call([]Value{NewIntValue(1)})
	if err == nil {
		t.Error("Call() should propagate error from function")
	}
}

// TestValueAsInt64NullValue tests AsInt64 on null value
func TestValueAsInt64NullValue(t *testing.T) {
	v := NewNullValue()
	result := v.AsInt64()
	if result != 0 {
		t.Errorf("NULL AsInt64() = %d, want 0", result)
	}
}

// TestValueAsStringNonTextValue tests AsString on non-text value
func TestValueAsStringNonTextValue(t *testing.T) {
	// Test with blob - returns string representation
	v := NewBlobValue([]byte{1, 2, 3})
	result := v.AsString()
	if result != string([]byte{1, 2, 3}) {
		t.Errorf("Blob AsString() = %q, want byte string", result)
	}

	// Test with integer
	v = NewIntValue(42)
	result = v.AsString()
	if result != "42" {
		t.Errorf("Integer AsString() = %q, want '42'", result)
	}
}

// TestValueAsBlobNonBlobValue tests AsBlob on non-blob value
func TestValueAsBlobNonBlobValue(t *testing.T) {
	// Test with integer
	v := NewIntValue(42)
	result := v.AsBlob()
	if result != nil {
		t.Errorf("Integer AsBlob() = %v, want nil", result)
	}

	// Test with null
	v = NewNullValue()
	result = v.AsBlob()
	if result != nil {
		t.Errorf("NULL AsBlob() = %v, want nil", result)
	}
}

// TestValueBytesForDifferentTypes tests Bytes on different value types
func TestValueBytesForDifferentTypes(t *testing.T) {
	// Test with text
	v := NewTextValue("hello")
	result := v.Bytes()
	if result != 5 {
		t.Errorf("Text Bytes() = %d, want 5", result)
	}

	// Test with integer (should return 8 for int64 size)
	v = NewIntValue(42)
	result = v.Bytes()
	if result != 8 {
		t.Errorf("Integer Bytes() = %d, want 8", result)
	}

	// Test with float (should return 8 for float64 size)
	v = NewFloatValue(3.14)
	result = v.Bytes()
	if result != 8 {
		t.Errorf("Float Bytes() = %d, want 8", result)
	}

	// Test with NULL (should return 0)
	v = NewNullValue()
	result = v.Bytes()
	if result != 0 {
		t.Errorf("NULL Bytes() = %d, want 0", result)
	}
}

// TestRegistryLookupNonExistent tests Lookup with non-existent function
func TestRegistryLookupNonExistent(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Lookup("nonexistent")
	if ok {
		t.Error("Lookup() should return false for non-existent function")
	}
}

// TestRegistryLookupWithArgsNoMatch tests LookupWithArgs with no matching overload
func TestRegistryLookupWithArgsNoMatch(t *testing.T) {
	r := NewRegistry()
	// Register a user function with specific arg count
	testFunc := NewScalarFunc("test", 2, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})
	r.RegisterUser(testFunc, 2)

	// Lookup with matching arg count should work
	_, ok := r.LookupWithArgs("test", 2)
	if !ok {
		t.Error("LookupWithArgs() should find function with matching arg count")
	}

	// Try to look up with different arg count when no variadic version exists
	_, ok = r.LookupWithArgs("nonexistent", 5)
	if ok {
		t.Error("LookupWithArgs() should return false for non-existent function")
	}
}

// TestRegistryUnregisterNonExistent tests Unregister with non-existent function
func TestRegistryUnregisterNonExistent(t *testing.T) {
	r := NewRegistry()
	// Unregistering non-existent function should not panic
	result := r.Unregister("nonexistent", 1)
	if result {
		t.Error("Unregister() should return false for non-existent function")
	}
}

// TestRegistryGetAllFunctionsEmpty tests GetAllFunctions on empty registry
func TestRegistryGetAllFunctionsEmpty(t *testing.T) {
	r := NewRegistry()
	funcs := r.GetAllFunctions()
	if len(funcs) != 0 {
		t.Errorf("GetAllFunctions() on empty registry = %d functions, want 0", len(funcs))
	}
}

// TestValueAsInt64WithFloat tests AsInt64 on float value
func TestValueAsInt64WithFloat(t *testing.T) {
	v := NewFloatValue(42.7)
	result := v.AsInt64()
	if result != 42 {
		t.Errorf("Float AsInt64() = %d, want 42", result)
	}
}

// TestValueAsStringWithFloat tests AsString on float value
func TestValueAsStringWithFloat(t *testing.T) {
	v := NewFloatValue(3.14)
	result := v.AsString()
	if result != "3.14" {
		t.Errorf("Float AsString() = %q, want '3.14'", result)
	}
}

// TestRegistryLookupUserFunction tests user function priority
func TestRegistryLookupUserFunction(t *testing.T) {
	r := NewRegistry()

	// Register a built-in
	r.Register(NewScalarFunc("test", 1, func(args []Value) (Value, error) {
		return NewIntValue(1), nil
	}))

	// Register a user function with same name (should have priority)
	userFunc := NewScalarFunc("test", 1, func(args []Value) (Value, error) {
		return NewIntValue(2), nil
	})
	r.RegisterUser(userFunc, 1)

	// Lookup should find user function via LookupWithArgs
	fn, ok := r.LookupWithArgs("test", 1)
	if !ok {
		t.Fatal("LookupWithArgs() should find test function")
	}

	result, err := fn.Call([]Value{NewIntValue(0)})
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if result.AsInt64() != 2 {
		t.Errorf("User function should be used, got result = %d, want 2", result.AsInt64())
	}
}

// TestRegistryUnregisterVariadic tests unregistering variadic function
func TestRegistryUnregisterVariadic(t *testing.T) {
	r := NewRegistry()

	// Register a variadic function
	varFunc := NewScalarFunc("vfunc", -1, func(args []Value) (Value, error) {
		return NewIntValue(int64(len(args))), nil
	})
	r.RegisterUser(varFunc, -1)

	// Verify it's there
	_, ok := r.Lookup("vfunc")
	if !ok {
		t.Error("Variadic function should be registered")
	}

	// Unregister it
	removed := r.Unregister("vfunc", -1)
	if !removed {
		t.Error("Unregister() should return true")
	}

	// Verify it's gone
	_, ok = r.Lookup("vfunc")
	if ok {
		t.Error("Variadic function should be unregistered")
	}
}

// TestRegistryGetAllFunctionsWithUserFuncs tests GetAllFunctions with user functions
func TestRegistryGetAllFunctionsWithUserFuncs(t *testing.T) {
	r := NewRegistry()

	// Register some functions
	r.Register(NewScalarFunc("builtin", 1, func(args []Value) (Value, error) {
		return NewIntValue(1), nil
	}))

	userFunc := NewScalarFunc("user", 1, func(args []Value) (Value, error) {
		return NewIntValue(2), nil
	})
	r.RegisterUser(userFunc, 1)

	funcs := r.GetAllFunctions()
	if len(funcs) < 1 {
		t.Errorf("GetAllFunctions() = %d functions, want at least 1", len(funcs))
	}
}

// TestRegistryLookupVariadicFallback tests variadic fallback in Lookup
func TestRegistryLookupVariadicFallback(t *testing.T) {
	r := NewRegistry()

	// Only register variadic version
	varFunc := NewScalarFunc("vfunc", -1, func(args []Value) (Value, error) {
		return NewIntValue(int64(len(args))), nil
	})
	r.RegisterUser(varFunc, -1)

	// Lookup without args should find variadic
	fn, ok := r.Lookup("vfunc")
	if !ok {
		t.Error("Lookup() should find variadic function")
	}

	// Should work with any number of args
	result, err := fn.Call([]Value{NewIntValue(1), NewIntValue(2), NewIntValue(3)})
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if result.AsInt64() != 3 {
		t.Errorf("Variadic function result = %d, want 3", result.AsInt64())
	}
}
