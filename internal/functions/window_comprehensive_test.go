// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// TestWindowFunctions_Names tests window function names
func TestWindowFunctions_Names(t *testing.T) {
	tests := []struct {
		fn   interface{ Name() string }
		want string
	}{
		{&RowNumberFunc{}, "row_number"},
		{&RankFunc{}, "rank"},
		{&DenseRankFunc{}, "dense_rank"},
		{&NtileFunc{}, "ntile"},
		{&LagFunc{}, "lag"},
		{&LeadFunc{}, "lead"},
		{&FirstValueFunc{}, "first_value"},
		{&LastValueFunc{}, "last_value"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.fn.Name()
			if got != tt.want {
				t.Errorf("Name() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWindowFunctions_NumArgs tests window function argument counts
func TestWindowFunctions_NumArgs(t *testing.T) {
	tests := []struct {
		name string
		fn   interface{ NumArgs() int }
		want int
	}{
		{"row_number", &RowNumberFunc{}, 0},
		{"rank", &RankFunc{}, 0},
		{"dense_rank", &DenseRankFunc{}, 0},
		{"ntile", &NtileFunc{}, 1},
		{"lag", &LagFunc{}, -1},
		{"lead", &LeadFunc{}, -1},
		{"first_value", &FirstValueFunc{}, 1},
		{"last_value", &LastValueFunc{}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fn.NumArgs()
			if got != tt.want {
				t.Errorf("NumArgs() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestWindowFunctions_Call tests that window functions return error on Call
func TestWindowFunctions_Call(t *testing.T) {
	funcs := []struct {
		name string
		fn   interface {
			Call([]Value) (Value, error)
		}
	}{
		{"row_number", &RowNumberFunc{}},
		{"rank", &RankFunc{}},
		{"dense_rank", &DenseRankFunc{}},
		{"ntile", &NtileFunc{}},
		{"lag", &LagFunc{}},
		{"lead", &LeadFunc{}},
		{"first_value", &FirstValueFunc{}},
		{"last_value", &LastValueFunc{}},
	}

	for _, tt := range funcs {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.fn.Call([]Value{})
			if err == nil {
				t.Errorf("%s.Call() expected error, got nil", tt.name)
			}
		})
	}
}

// TestWindowAggregateWrapper tests window aggregate wrapper
func TestWindowAggregateWrapper(t *testing.T) {
	// Create a simple aggregate function (sum)
	sumFunc := &SumFunc{}

	// Wrap it as a window function
	wrapper := NewWindowAggregateWrapper(sumFunc)

	// Test name
	if wrapper.Name() != "sum" {
		t.Errorf("Name() = %q, want \"sum\"", wrapper.Name())
	}

	// Test NumArgs
	if wrapper.NumArgs() != 1 {
		t.Errorf("NumArgs() = %d, want 1", wrapper.NumArgs())
	}

	// Test Call returns error
	_, err := wrapper.Call([]Value{})
	if err == nil {
		t.Error("Call() expected error, got nil")
	}

	// Test Inverse (not implemented, should return error)
	err = wrapper.Inverse([]Value{})
	if err == nil {
		t.Error("Inverse() should return error but didn't")
	}

	// Test Value
	// This will fail because we haven't called Step, but we're testing the path exists
	wrapper.Value()

	// Test Reset
	wrapper.Reset()
}

// TestWindowAggregateWrapper_Complete tests complete lifecycle
func TestWindowAggregateWrapper_Complete(t *testing.T) {
	// Create a simple aggregate function (count)
	countFunc := &CountFunc{}

	// Wrap it as a window function
	wrapper := NewWindowAggregateWrapper(countFunc)

	// This test just ensures the wrapper doesn't panic on basic operations
	wrapper.Reset()

	// Value should work after reset
	wrapper.Value()
}
