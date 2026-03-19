// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import "fmt"

// generateSeriesFunc implements generate_series(start, stop[, step]) table-valued function.
type generateSeriesFunc struct{}

func (f *generateSeriesFunc) Name() string { return "generate_series" }
func (f *generateSeriesFunc) NumArgs() int { return -1 } // 1-3 args

func (f *generateSeriesFunc) Call(args []Value) (Value, error) {
	return nil, fmt.Errorf("generate_series() is a table-valued function")
}

func (f *generateSeriesFunc) Columns() []string {
	return []string{"value", "start", "stop", "step"}
}

// Open implements the TableValuedFunction interface for generate_series.
func (f *generateSeriesFunc) Open(args []Value) ([][]Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, fmt.Errorf("generate_series() requires 1 to 3 arguments")
	}

	start, stop, step, err := parseSeriesArgs(args)
	if err != nil {
		return nil, err
	}

	return buildSeriesRows(start, stop, step), nil
}

// parseSeriesArgs extracts start, stop, step from the function arguments.
func parseSeriesArgs(args []Value) (int64, int64, int64, error) {
	var start, stop, step int64

	switch len(args) {
	case 1:
		// generate_series(stop) — start=0, step=1
		stop = args[0].AsInt64()
		step = 1
	case 2:
		// generate_series(start, stop) — step=1
		start = args[0].AsInt64()
		stop = args[1].AsInt64()
		step = 1
	case 3:
		start = args[0].AsInt64()
		stop = args[1].AsInt64()
		step = args[2].AsInt64()
	}

	if step == 0 {
		return 0, 0, 0, fmt.Errorf("generate_series() step must not be zero")
	}

	return start, stop, step, nil
}

// buildSeriesRows generates the result rows for the given start, stop, step.
func buildSeriesRows(start, stop, step int64) [][]Value {
	var rows [][]Value

	startVal := NewIntValue(start)
	stopVal := NewIntValue(stop)
	stepVal := NewIntValue(step)

	for v := start; (step > 0 && v <= stop) || (step < 0 && v >= stop); v += step {
		row := []Value{NewIntValue(v), startVal, stopVal, stepVal}
		rows = append(rows, row)
	}

	return rows
}

// RegisterGenerateSeriesFunctions registers the generate_series TVF.
func RegisterGenerateSeriesFunctions(r *Registry) {
	r.Register(&generateSeriesFunc{})
}
