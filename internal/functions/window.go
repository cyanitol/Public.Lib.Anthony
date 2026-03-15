// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"fmt"
)

// RegisterWindowFunctions registers all window functions.
func RegisterWindowFunctions(r *Registry) {
	// ROW_NUMBER, RANK, DENSE_RANK, NTILE are implemented as opcodes
	// but we register placeholder functions for completeness
	r.Register(&RowNumberFunc{})
	r.Register(&RankFunc{})
	r.Register(&DenseRankFunc{})
	r.Register(&NtileFunc{})
	r.Register(&LagFunc{})
	r.Register(&LeadFunc{})
	r.Register(&FirstValueFunc{})
	r.Register(&LastValueFunc{})
	r.Register(&NthValueFunc{})
}

// RowNumberFunc implements ROW_NUMBER() window function
// This is primarily handled by opcode, but we provide the interface
type RowNumberFunc struct{}

func (f *RowNumberFunc) Name() string { return "row_number" }
func (f *RowNumberFunc) NumArgs() int { return 0 }
func (f *RowNumberFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("row_number() is a window function")
}

// RankFunc implements RANK() window function
type RankFunc struct{}

func (f *RankFunc) Name() string { return "rank" }
func (f *RankFunc) NumArgs() int { return 0 }
func (f *RankFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("rank() is a window function")
}

// DenseRankFunc implements DENSE_RANK() window function
type DenseRankFunc struct{}

func (f *DenseRankFunc) Name() string { return "dense_rank" }
func (f *DenseRankFunc) NumArgs() int { return 0 }
func (f *DenseRankFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("dense_rank() is a window function")
}

// NtileFunc implements NTILE() window function
type NtileFunc struct{}

func (f *NtileFunc) Name() string { return "ntile" }
func (f *NtileFunc) NumArgs() int { return 1 }
func (f *NtileFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("ntile() is a window function")
}

// LagFunc implements LAG() window function
type LagFunc struct{}

func (f *LagFunc) Name() string { return "lag" }
func (f *LagFunc) NumArgs() int { return -1 } // 1-3 args
func (f *LagFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("lag() is a window function")
}

// LeadFunc implements LEAD() window function
type LeadFunc struct{}

func (f *LeadFunc) Name() string { return "lead" }
func (f *LeadFunc) NumArgs() int { return -1 } // 1-3 args
func (f *LeadFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("lead() is a window function")
}

// FirstValueFunc implements FIRST_VALUE() window function
type FirstValueFunc struct{}

func (f *FirstValueFunc) Name() string { return "first_value" }
func (f *FirstValueFunc) NumArgs() int { return 1 }
func (f *FirstValueFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("first_value() is a window function")
}

// LastValueFunc implements LAST_VALUE() window function
type LastValueFunc struct{}

func (f *LastValueFunc) Name() string { return "last_value" }
func (f *LastValueFunc) NumArgs() int { return 1 }
func (f *LastValueFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("last_value() is a window function")
}

// NthValueFunc implements NTH_VALUE() window function
type NthValueFunc struct{}

func (f *NthValueFunc) Name() string { return "nth_value" }
func (f *NthValueFunc) NumArgs() int { return 2 }
func (f *NthValueFunc) Call([]Value) (Value, error) {
	return nil, fmt.Errorf("nth_value() is a window function")
}

// WindowAggregateWrapper wraps an aggregate function for use in window context
// This allows aggregate functions like SUM, AVG, MIN, MAX to work with window frames
type WindowAggregateWrapper struct {
	aggFunc AggregateFunction
	name    string
}

// NewWindowAggregateWrapper creates a wrapper for an aggregate function
func NewWindowAggregateWrapper(aggFunc AggregateFunction) *WindowAggregateWrapper {
	return &WindowAggregateWrapper{
		aggFunc: aggFunc,
		name:    aggFunc.Name(),
	}
}

func (w *WindowAggregateWrapper) Name() string {
	return w.name
}

func (w *WindowAggregateWrapper) NumArgs() int {
	return w.aggFunc.NumArgs()
}

func (w *WindowAggregateWrapper) Call(args []Value) (Value, error) {
	return w.aggFunc.Call(args)
}

func (w *WindowAggregateWrapper) Inverse(args []Value) error {
	// For simple case, we don't support inverse - would need full recalculation
	// More sophisticated implementations would track state for efficient updates
	return fmt.Errorf("inverse not implemented for %s", w.name)
}

func (w *WindowAggregateWrapper) Value() (Value, error) {
	return w.aggFunc.Final()
}

func (w *WindowAggregateWrapper) Reset() {
	w.aggFunc.Reset()
}
