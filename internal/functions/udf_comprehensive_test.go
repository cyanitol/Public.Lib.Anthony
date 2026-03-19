// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// simpleUserFunc is a test implementation of UserFunction
type simpleUserFunc struct {
	value int64
}

func (f *simpleUserFunc) Invoke(args []Value) (Value, error) {
	return NewIntValue(f.value), nil
}

// TestUserScalarFunc_NumArgs tests NumArgs method for user scalar functions
func TestUserScalarFunc_NumArgs(t *testing.T) {
	config := FunctionConfig{
		Name:          "test",
		NumArgs:       3,
		Deterministic: true,
	}
	fn := NewUserScalarFunc(config, &simpleUserFunc{value: 42})

	got := fn.NumArgs()
	if got != 3 {
		t.Errorf("NumArgs() = %d, want 3", got)
	}
}

// TestUserScalarFunc_IsDeterministic tests IsDeterministic method
func TestUserScalarFunc_IsDeterministic(t *testing.T) {
	tests := []struct {
		name            string
		isDeterministic bool
	}{
		{
			name:            "deterministic",
			isDeterministic: true,
		},
		{
			name:            "non-deterministic",
			isDeterministic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := FunctionConfig{
				Name:          "test",
				NumArgs:       1,
				Deterministic: tt.isDeterministic,
			}
			fn := NewUserScalarFunc(config, &simpleUserFunc{value: 42})

			// Cast to the concrete type to access IsDeterministic
			if userFn, ok := fn.(*userScalarFunc); ok {
				got := userFn.IsDeterministic()
				if got != tt.isDeterministic {
					t.Errorf("IsDeterministic() = %v, want %v", got, tt.isDeterministic)
				}
			} else {
				t.Error("Expected *userScalarFunc type")
			}
		})
	}
}

// simpleUserAggregateFunc is a test implementation of UserAggregateFunction
type simpleUserAggregateFunc struct {
	sum int64
}

func (f *simpleUserAggregateFunc) Step(args []Value) error {
	if !args[0].IsNull() {
		f.sum += args[0].AsInt64()
	}
	return nil
}

func (f *simpleUserAggregateFunc) Final() (Value, error) {
	return NewIntValue(f.sum), nil
}

func (f *simpleUserAggregateFunc) Reset() {
	f.sum = 0
}

// TestUserAggregateFunc_NumArgs tests NumArgs method for user aggregate functions
func TestUserAggregateFunc_NumArgs(t *testing.T) {
	config := FunctionConfig{
		Name:          "test",
		NumArgs:       2,
		Deterministic: true,
	}
	fn := NewUserAggregateFunc(config, &simpleUserAggregateFunc{})

	got := fn.NumArgs()
	if got != 2 {
		t.Errorf("NumArgs() = %d, want 2", got)
	}
}

// TestUserAggregateFunc_Call tests Call method for user aggregate functions
func TestUserAggregateFunc_Call(t *testing.T) {
	config := FunctionConfig{
		Name:          "test",
		NumArgs:       1,
		Deterministic: true,
	}
	fn := NewUserAggregateFunc(config, &simpleUserAggregateFunc{})

	_, err := fn.Call([]Value{})
	if err == nil {
		t.Error("Call() expected error for aggregate function, got nil")
	}
}

// TestUserAggregateFunc_IsDeterministic tests IsDeterministic method
func TestUserAggregateFunc_IsDeterministic(t *testing.T) {
	tests := []struct {
		name            string
		isDeterministic bool
	}{
		{
			name:            "deterministic",
			isDeterministic: true,
		},
		{
			name:            "non-deterministic",
			isDeterministic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := FunctionConfig{
				Name:          "test",
				NumArgs:       1,
				Deterministic: tt.isDeterministic,
			}
			fn := NewUserAggregateFunc(config, &simpleUserAggregateFunc{})

			// Cast to the concrete type to access IsDeterministic
			if userFn, ok := fn.(*userAggregateFunc); ok {
				got := userFn.IsDeterministic()
				if got != tt.isDeterministic {
					t.Errorf("IsDeterministic() = %v, want %v", got, tt.isDeterministic)
				}
			} else {
				t.Error("Expected *userAggregateFunc type")
			}
		})
	}
}

// TestUserAggregateFunc_Complete tests complete lifecycle of user aggregate function
func TestUserAggregateFunc_Complete(t *testing.T) {
	config := FunctionConfig{
		Name:          "my_sum",
		NumArgs:       1,
		Deterministic: true,
	}
	fn := NewUserAggregateFunc(config, &simpleUserAggregateFunc{})

	if fn.Name() != "my_sum" {
		t.Errorf("Name() = %q, want \"my_sum\"", fn.Name())
	}

	stepValues := []Value{NewIntValue(10), NewIntValue(20), NewNullValue(), NewIntValue(30)}
	stepAll(t, fn, stepValues)

	assertFinalValue(t, fn, 60)

	fn.Reset()
	stepAll(t, fn, []Value{NewIntValue(5)})
	assertFinalValue(t, fn, 5)
}

func stepAll(t *testing.T, fn interface{ Step([]Value) error }, values []Value) {
	t.Helper()
	for _, v := range values {
		if err := fn.Step([]Value{v}); err != nil {
			t.Fatalf("Step() error = %v", err)
		}
	}
}

func assertFinalValue(t *testing.T, fn interface{ Final() (Value, error) }, want int64) {
	t.Helper()
	result, err := fn.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}
	if result.AsInt64() != want {
		t.Errorf("Final() = %d, want %d", result.AsInt64(), want)
	}
}
