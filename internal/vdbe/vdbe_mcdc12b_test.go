// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

// MC/DC 12b — internal unit tests for exec.go and functions.go low-coverage paths.
//
// Targets:
//   exec.go:3325  addRowidToValues          — nil Ctx error path
//   exec.go:2922  getWritableBtreeCursor    — nil BtreeCursor path
//   exec.go:6510  getWindowState            — not-found path (idx beyond map)
//   functions.go:346 createAggregateInstance — count/sum/avg/max/min/unknown

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
)

// ---------------------------------------------------------------------------
// addRowidToValues — nil Ctx error path
//
// MC/DC: v.Ctx == nil → return fmt.Errorf("no schema available")
// ---------------------------------------------------------------------------

// TestMCDC12b_AddRowidToValues_NilCtx calls addRowidToValues with Ctx == nil
// and expects an error.
func TestMCDC12b_AddRowidToValues_NilCtx(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.Ctx = nil

	err := vm.addRowidToValues("t", 1, map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for nil Ctx in addRowidToValues, got nil")
	}
}

// ---------------------------------------------------------------------------
// getWritableBtreeCursor — nil BtreeCursor path
//
// MC/DC: cursor.BtreeCursor is nil → "invalid btree cursor for insert"
// ---------------------------------------------------------------------------

// TestMCDC12b_GetWritableBtreeCursor_NilBtree creates a cursor with a nil
// BtreeCursor field and verifies getWritableBtreeCursor returns an error.
func TestMCDC12b_GetWritableBtreeCursor_NilBtree(t *testing.T) {
	t.Parallel()

	vm := New()
	vm.AllocMemory(4)
	vm.AllocCursors(2)

	// Create a writable cursor with a nil BtreeCursor.
	vm.Cursors[0] = &Cursor{
		Writable:    true,
		BtreeCursor: nil,
	}

	_, _, err := vm.getWritableBtreeCursor(0)
	if err == nil {
		t.Fatal("expected error for nil BtreeCursor in getWritableBtreeCursor, got nil")
	}
}

// ---------------------------------------------------------------------------
// getWindowState — not-found path
//
// MC/DC: WindowStates[idx] not found → error "window state %d not found"
// ---------------------------------------------------------------------------

// TestMCDC12b_GetWindowState_NotFound calls getWindowState with an index that
// does not exist in WindowStates, expecting an error.
func TestMCDC12b_GetWindowState_NotFound(t *testing.T) {
	t.Parallel()

	vm := New()
	// WindowStates is initialised as an empty map by New().
	_, err := vm.getWindowState(9999)
	if err == nil {
		t.Fatal("expected error for missing window state idx=9999, got nil")
	}
}

// ---------------------------------------------------------------------------
// createAggregateInstance — various function names
//
// MC/DC: reflection-based copy returns a fresh instance with zeroed state.
// ---------------------------------------------------------------------------

// mcdc12bLookupAgg is a local helper that retrieves a named aggregate function
// from the default registry and returns a fresh instance via createAggregateInstance.
func mcdc12bLookupAgg(t *testing.T, name string) functions.AggregateFunction {
	t.Helper()
	fc := NewFunctionContext()
	fn, ok := fc.registry.Lookup(name)
	if !ok {
		// min/max may be registered under a builtin-only path.
		fn, ok = fc.registry.LookupBuiltin(name)
	}
	if !ok {
		t.Skipf("aggregate function %q not found in registry", name)
	}
	aggFn, ok := fn.(functions.AggregateFunction)
	if !ok {
		t.Skipf("function %q is not an AggregateFunction", name)
	}
	return createAggregateInstance(aggFn)
}

// TestMCDC12b_CreateAggregateInstance_Count creates a fresh "count" instance
// and verifies Final() returns 0 (no steps taken).
func TestMCDC12b_CreateAggregateInstance_Count(t *testing.T) {
	t.Parallel()

	agg := mcdc12bLookupAgg(t, "count")
	result, err := agg.Final()
	if err != nil {
		t.Fatalf("Final() on fresh count: %v", err)
	}
	if result.AsInt64() != 0 {
		t.Errorf("fresh count Final() = %d, want 0", result.AsInt64())
	}
}

// TestMCDC12b_CreateAggregateInstance_Sum creates a fresh "sum" instance and
// verifies the aggregate starts at zero.
func TestMCDC12b_CreateAggregateInstance_Sum(t *testing.T) {
	t.Parallel()

	agg := mcdc12bLookupAgg(t, "sum")
	result, err := agg.Final()
	if err != nil {
		t.Fatalf("Final() on fresh sum: %v", err)
	}
	// sum of no rows is NULL; accept NULL or 0.
	if result != nil && !result.IsNull() && result.AsFloat64() != 0 {
		t.Errorf("fresh sum Final() = %v, want 0 or NULL", result)
	}
}

// TestMCDC12b_CreateAggregateInstance_Avg creates a fresh "avg" instance.
func TestMCDC12b_CreateAggregateInstance_Avg(t *testing.T) {
	t.Parallel()

	agg := mcdc12bLookupAgg(t, "avg")
	result, err := agg.Final()
	if err != nil {
		t.Fatalf("Final() on fresh avg: %v", err)
	}
	// avg of no rows is NULL.
	if result != nil && !result.IsNull() {
		t.Logf("avg of no rows returned %v (expected NULL)", result)
	}
}

// TestMCDC12b_CreateAggregateInstance_Max creates a fresh "max" instance and
// steps one value, verifying the result.
func TestMCDC12b_CreateAggregateInstance_Max(t *testing.T) {
	t.Parallel()

	agg := mcdc12bLookupAgg(t, "max")
	val := functions.NewIntValue(42)
	if err := agg.Step([]functions.Value{val}); err != nil {
		t.Fatalf("Step() on max: %v", err)
	}
	result, err := agg.Final()
	if err != nil {
		t.Fatalf("Final() on max: %v", err)
	}
	if result.AsInt64() != 42 {
		t.Errorf("max Final() = %d, want 42", result.AsInt64())
	}
}

// TestMCDC12b_CreateAggregateInstance_Min creates a fresh "min" instance and
// steps one value, verifying the result.
func TestMCDC12b_CreateAggregateInstance_Min(t *testing.T) {
	t.Parallel()

	agg := mcdc12bLookupAgg(t, "min")
	val := functions.NewIntValue(7)
	if err := agg.Step([]functions.Value{val}); err != nil {
		t.Fatalf("Step() on min: %v", err)
	}
	result, err := agg.Final()
	if err != nil {
		t.Fatalf("Final() on min: %v", err)
	}
	if result.AsInt64() != 7 {
		t.Errorf("min Final() = %d, want 7", result.AsInt64())
	}
}

// TestMCDC12b_CreateAggregateInstance_Unknown uses the fallback path by passing
// a custom aggregate whose reflection-created copy is the same type.
func TestMCDC12b_CreateAggregateInstance_Unknown(t *testing.T) {
	t.Parallel()

	// Use the minimalAgg type defined in functions_mem_pool_coverage_test.go.
	// Since that is in the same package we can use it directly.
	original := &minimalAgg{}
	_ = original.Step(nil)
	_ = original.Step(nil)
	// original.count == 2

	fresh := createAggregateInstance(original)
	result, err := fresh.Final()
	if err != nil {
		t.Fatalf("Final() on fresh unknown agg: %v", err)
	}
	// Fresh copy must have count == 0.
	if result.AsInt64() != 0 {
		t.Errorf("fresh unknown agg count = %d, want 0", result.AsInt64())
	}
}
