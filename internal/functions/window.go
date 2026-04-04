// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"fmt"
)

// windowErrFunc returns an error indicating the function is a window function.
func windowErrFunc(name string) func([]Value) (Value, error) {
	return func([]Value) (Value, error) {
		return nil, fmt.Errorf("%s() is a window function", name)
	}
}

// RowNumberFunc implements ROW_NUMBER() window function placeholder.
type RowNumberFunc struct{}

func (f *RowNumberFunc) Name() string                  { return "row_number" }
func (f *RowNumberFunc) NumArgs() int                  { return 0 }
func (f *RowNumberFunc) Call(a []Value) (Value, error) { return windowErrFunc("row_number")(a) }

// RankFunc implements RANK() window function placeholder.
type RankFunc struct{}

func (f *RankFunc) Name() string                  { return "rank" }
func (f *RankFunc) NumArgs() int                  { return 0 }
func (f *RankFunc) Call(a []Value) (Value, error) { return windowErrFunc("rank")(a) }

// DenseRankFunc implements DENSE_RANK() window function placeholder.
type DenseRankFunc struct{}

func (f *DenseRankFunc) Name() string                  { return "dense_rank" }
func (f *DenseRankFunc) NumArgs() int                  { return 0 }
func (f *DenseRankFunc) Call(a []Value) (Value, error) { return windowErrFunc("dense_rank")(a) }

// NtileFunc implements NTILE() window function placeholder.
type NtileFunc struct{}

func (f *NtileFunc) Name() string                  { return "ntile" }
func (f *NtileFunc) NumArgs() int                  { return 1 }
func (f *NtileFunc) Call(a []Value) (Value, error) { return windowErrFunc("ntile")(a) }

// PercentRankFunc implements PERCENT_RANK() window function placeholder.
type PercentRankFunc struct{}

func (f *PercentRankFunc) Name() string                  { return "percent_rank" }
func (f *PercentRankFunc) NumArgs() int                  { return 0 }
func (f *PercentRankFunc) Call(a []Value) (Value, error) { return windowErrFunc("percent_rank")(a) }

// CumeDistFunc implements CUME_DIST() window function placeholder.
type CumeDistFunc struct{}

func (f *CumeDistFunc) Name() string                  { return "cume_dist" }
func (f *CumeDistFunc) NumArgs() int                  { return 0 }
func (f *CumeDistFunc) Call(a []Value) (Value, error) { return windowErrFunc("cume_dist")(a) }

// LagFunc implements LAG() window function placeholder.
type LagFunc struct{}

func (f *LagFunc) Name() string                  { return "lag" }
func (f *LagFunc) NumArgs() int                  { return -1 }
func (f *LagFunc) Call(a []Value) (Value, error) { return windowErrFunc("lag")(a) }

// LeadFunc implements LEAD() window function placeholder.
type LeadFunc struct{}

func (f *LeadFunc) Name() string                  { return "lead" }
func (f *LeadFunc) NumArgs() int                  { return -1 }
func (f *LeadFunc) Call(a []Value) (Value, error) { return windowErrFunc("lead")(a) }

// FirstValueFunc implements FIRST_VALUE() window function placeholder.
type FirstValueFunc struct{}

func (f *FirstValueFunc) Name() string                  { return "first_value" }
func (f *FirstValueFunc) NumArgs() int                  { return 1 }
func (f *FirstValueFunc) Call(a []Value) (Value, error) { return windowErrFunc("first_value")(a) }

// LastValueFunc implements LAST_VALUE() window function placeholder.
type LastValueFunc struct{}

func (f *LastValueFunc) Name() string                  { return "last_value" }
func (f *LastValueFunc) NumArgs() int                  { return 1 }
func (f *LastValueFunc) Call(a []Value) (Value, error) { return windowErrFunc("last_value")(a) }

// NthValueFunc implements NTH_VALUE() window function placeholder.
type NthValueFunc struct{}

func (f *NthValueFunc) Name() string                  { return "nth_value" }
func (f *NthValueFunc) NumArgs() int                  { return 2 }
func (f *NthValueFunc) Call(a []Value) (Value, error) { return windowErrFunc("nth_value")(a) }

// RegisterWindowFunctions registers all window functions.
func RegisterWindowFunctions(r *Registry) {
	r.Register(&RowNumberFunc{})
	r.Register(&RankFunc{})
	r.Register(&DenseRankFunc{})
	r.Register(&NtileFunc{})
	r.Register(&PercentRankFunc{})
	r.Register(&CumeDistFunc{})
	r.Register(&LagFunc{})
	r.Register(&LeadFunc{})
	r.Register(&FirstValueFunc{})
	r.Register(&LastValueFunc{})
	r.Register(&NthValueFunc{})
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
