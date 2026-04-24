// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC for: len(args) < 1 || len(args) > 3  (generateSeriesFunc.Open arg guard)
//
// Source: generate_series.go:22
// Conditions: A=(len(args)<1), B=(len(args)>3)
// Short-circuit OR: A=T → error immediately; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {0-arg (A=T,B=F) → error, 1-arg (A=F,B=F) → valid}
//	B: {4-arg (A=F,B=T) → error, 2-arg (A=F,B=F) → valid}
//
// ---------------------------------------------------------------------------
func TestMCDC_GenerateSeries_Open_ArgGuard(t *testing.T) {
	t.Parallel()

	f := &generateSeriesFunc{}

	// A=T B=F: 0 args → error (len<1)
	t.Run("A=T: 0-arg → error", func(t *testing.T) {
		t.Parallel()
		_, err := f.Open([]Value{})
		if err == nil {
			t.Error("Open(0 args) expected error, got nil")
		}
	})

	// A=F B=T: 4 args → error (len>3)
	t.Run("A=F B=T: 4-arg → error", func(t *testing.T) {
		t.Parallel()
		_, err := f.Open([]Value{
			NewIntValue(1), NewIntValue(5), NewIntValue(1), NewIntValue(0),
		})
		if err == nil {
			t.Error("Open(4 args) expected error, got nil")
		}
	})

	// A=F B=F: 1 arg → valid (generate_series(stop))
	t.Run("A=F B=F: 1-arg → valid", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewIntValue(3)})
		if err != nil {
			t.Fatalf("Open(1 arg) unexpected error: %v", err)
		}
		// generate_series(3) → 0,1,2,3 = 4 rows
		if len(rows) != 4 {
			t.Errorf("Open(3) = %d rows, want 4", len(rows))
		}
	})

	// A=F B=F: 2 args → valid (generate_series(start, stop))
	t.Run("A=F B=F: 2-arg → valid", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewIntValue(1), NewIntValue(3)})
		if err != nil {
			t.Fatalf("Open(2 args) unexpected error: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Open(1,3) = %d rows, want 3", len(rows))
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: (step > 0 && v <= stop) || (step < 0 && v >= stop)
//
// Source: generate_series.go:69  (buildSeriesRows loop condition)
// Conditions: A=(step>0), B=(v<=stop), C=(step<0), D=(v>=stop)
// Full compound: (A && B) || (C && D)
// Note: A and C are mutually exclusive (step != 0), so only 3 effective independent
// conditions. MC/DC requires N+1=5 test cases covering each sub-condition flip.
//
// Cases:
//  1. A=T B=T C=F D=*: positive step, v in range → loop body executes
//  2. A=T B=F C=F D=*: positive step, v past stop → loop exits
//  3. A=F B=* C=T D=T: negative step, v in range → loop body executes
//  4. A=F B=* C=T D=F: negative step, v past stop → loop exits
//  5. A=F B=* C=F D=*: step=0 path is a parse error (step==0 branch)
//
// ---------------------------------------------------------------------------
func TestMCDC_BuildSeriesRows_LoopCondition(t *testing.T) {
	t.Parallel()

	// Case 1: A=T B=T — positive step, stop >= start → rows produced
	t.Run("A=T B=T: positive step, stop>=start → rows", func(t *testing.T) {
		t.Parallel()
		rows := buildSeriesRows(1, 3, 1)
		if len(rows) != 3 {
			t.Errorf("buildSeriesRows(1,3,1) = %d rows, want 3", len(rows))
		}
		// First value should be 1
		if rows[0][0].AsInt64() != 1 {
			t.Errorf("first value = %d, want 1", rows[0][0].AsInt64())
		}
	})

	// Case 2: A=T B=F — positive step, stop < start → empty (loop never enters)
	t.Run("A=T B=F: positive step, stop<start → empty", func(t *testing.T) {
		t.Parallel()
		rows := buildSeriesRows(5, 3, 1)
		if len(rows) != 0 {
			t.Errorf("buildSeriesRows(5,3,1) = %d rows, want 0", len(rows))
		}
	})

	// Case 3: C=T D=T — negative step, stop <= start → rows produced
	t.Run("C=T D=T: negative step, start>=stop → rows", func(t *testing.T) {
		t.Parallel()
		rows := buildSeriesRows(3, 1, -1)
		if len(rows) != 3 {
			t.Errorf("buildSeriesRows(3,1,-1) = %d rows, want 3", len(rows))
		}
		// First value should be 3
		if rows[0][0].AsInt64() != 3 {
			t.Errorf("first value = %d, want 3", rows[0][0].AsInt64())
		}
	})

	// Case 4: C=T D=F — negative step, start < stop → empty
	t.Run("C=T D=F: negative step, start<stop → empty", func(t *testing.T) {
		t.Parallel()
		rows := buildSeriesRows(1, 5, -1)
		if len(rows) != 0 {
			t.Errorf("buildSeriesRows(1,5,-1) = %d rows, want 0", len(rows))
		}
	})

	// Boundary: positive step, single element (start == stop)
	t.Run("A=T B=T boundary: start==stop → 1 row", func(t *testing.T) {
		t.Parallel()
		rows := buildSeriesRows(7, 7, 2)
		if len(rows) != 1 {
			t.Errorf("buildSeriesRows(7,7,2) = %d rows, want 1", len(rows))
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: step == 0  (parseSeriesArgs step-zero guard)
//
// Source: generate_series.go:54
// Condition: A=(step==0)
// A=T → error; A=F → valid.
//
// ---------------------------------------------------------------------------
func TestMCDC_ParseSeriesArgs_StepZero(t *testing.T) {
	t.Parallel()

	// A=T: step=0 → error
	t.Run("A=T: step=0 → error", func(t *testing.T) {
		t.Parallel()
		_, _, _, err := parseSeriesArgs([]Value{
			NewIntValue(1), NewIntValue(5), NewIntValue(0),
		})
		if err == nil {
			t.Error("parseSeriesArgs with step=0 expected error, got nil")
		}
	})

	// A=F: step=1 → valid
	t.Run("A=F: step=1 → valid", func(t *testing.T) {
		t.Parallel()
		start, stop, step, err := parseSeriesArgs([]Value{
			NewIntValue(1), NewIntValue(5), NewIntValue(1),
		})
		if err != nil {
			t.Fatalf("parseSeriesArgs(1,5,1) unexpected error: %v", err)
		}
		if start != 1 || stop != 5 || step != 1 {
			t.Errorf("got start=%d stop=%d step=%d, want 1 5 1", start, stop, step)
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) < 1 || len(args) > 2  (jsonEachFunc.Open arg guard)
//
// Source: json_table.go:51
// Conditions: A=(len(args)<1), B=(len(args)>2)
// Short-circuit OR: A=T → error; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {0-arg (A=T,B=F) → error, 1-arg (A=F,B=F) → valid}
//	B: {3-arg (A=F,B=T) → error, 2-arg (A=F,B=F) → valid}
//
// ---------------------------------------------------------------------------
func TestMCDC_JSONEachFunc_Open_ArgGuard(t *testing.T) {
	t.Parallel()

	f := &jsonEachFunc{}

	// A=T B=F: 0 args → error
	t.Run("A=T: 0-arg → error", func(t *testing.T) {
		t.Parallel()
		_, err := f.Open([]Value{})
		if err == nil {
			t.Error("json_each Open(0 args) expected error, got nil")
		}
	})

	// A=F B=T: 3 args → error
	t.Run("A=F B=T: 3-arg → error", func(t *testing.T) {
		t.Parallel()
		_, err := f.Open([]Value{
			NewTextValue(`[1,2]`), NewTextValue(`$`), NewTextValue(`extra`),
		})
		if err == nil {
			t.Error("json_each Open(3 args) expected error, got nil")
		}
	})

	// A=F B=F: 1 arg → valid
	t.Run("A=F B=F: 1-arg → valid", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewTextValue(`[10,20]`)})
		if err != nil {
			t.Fatalf("json_each Open(1 arg) unexpected error: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("json_each([10,20]) = %d rows, want 2", len(rows))
		}
	})

	// A=F B=F: 2 args → valid
	t.Run("A=F B=F: 2-arg → valid", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{
			NewTextValue(`{"a":1,"b":2}`), NewTextValue(`$`),
		})
		if err != nil {
			t.Fatalf("json_each Open(2 args) unexpected error: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("json_each(obj,$) = %d rows, want 2", len(rows))
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (jsonEachFunc.Open path guard)
//
// Source: json_table.go:61
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// Conjunction &&: both must be true to use the path argument.
// Independent-effect pairs:
//
//	A: {1-arg (A=F,B=*) → root path used, 2-arg non-null (A=T,B=T) → path applied}
//	B: {2-arg null (A=T,B=F) → root path used, 2-arg non-null (A=T,B=T) → path applied}
//
// ---------------------------------------------------------------------------
// openJSONEach is a helper that calls jsonEachFunc.Open and fails on error.
func openJSONEach(t *testing.T, f *jsonEachFunc, args []Value) [][]Value {
	t.Helper()
	rows, err := f.Open(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return rows
}

func TestMCDC_JSONEachFunc_Open_PathGuard(t *testing.T) {
	t.Parallel()

	f := &jsonEachFunc{}
	jsonStr := `{"a":[1,2],"b":3}`

	// A=F: 1-arg → root path "$/a" not extracted, iterates top-level object keys
	t.Run("A=F: 1-arg → root path, top-level keys", func(t *testing.T) {
		t.Parallel()
		rows := openJSONEach(t, f, []Value{NewTextValue(jsonStr)})
		if len(rows) != 2 {
			t.Errorf("1-arg json_each rows = %d, want 2", len(rows))
		}
	})

	// A=T B=F: 2-arg with null path → root path used (same as 1-arg case)
	t.Run("A=T B=F: 2-arg null path → root path used", func(t *testing.T) {
		t.Parallel()
		rows := openJSONEach(t, f, []Value{NewTextValue(jsonStr), NewNullValue()})
		if len(rows) != 2 {
			t.Errorf("2-arg null-path json_each rows = %d, want 2", len(rows))
		}
	})

	// A=T B=T: 2-arg with non-null path → path $.a applied, iterates [1,2]
	t.Run("A=T B=T: 2-arg non-null path → path applied", func(t *testing.T) {
		t.Parallel()
		rows := openJSONEach(t, f, []Value{NewTextValue(jsonStr), NewTextValue("$.a")})
		if len(rows) != 2 {
			t.Errorf("2-arg path=$.a json_each rows = %d, want 2", len(rows))
		}
		// Verify values are 1 and 2
		if rows[0][1].AsInt64() != 1 || rows[1][1].AsInt64() != 2 {
			t.Errorf("path $.a values = %v %v, want 1 2",
				rows[0][1].AsInt64(), rows[1][1].AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (jsonTreeFunc.Open path guard)
//
// Source: json_table.go:191
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// Same structure as jsonEachFunc path guard but for json_tree.
//
// ---------------------------------------------------------------------------
func TestMCDC_JSONTreeFunc_Open_PathGuard(t *testing.T) {
	t.Parallel()

	f := &jsonTreeFunc{}
	jsonStr := `{"x":{"y":1}}`

	// A=F: 1-arg → full tree walk from root
	t.Run("A=F: 1-arg → full tree from root", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewTextValue(jsonStr)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Root obj + key x + nested obj + key y = 4 rows
		if len(rows) < 3 {
			t.Errorf("1-arg json_tree rows = %d, want >= 3", len(rows))
		}
	})

	// A=T B=F: 2-arg null path → full tree from root (same as 1-arg)
	t.Run("A=T B=F: 2-arg null path → full tree", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewTextValue(jsonStr), NewNullValue()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rows) < 3 {
			t.Errorf("2-arg null-path json_tree rows = %d, want >= 3", len(rows))
		}
	})

	// A=T B=T: 2-arg non-null path → subtree rooted at $.x
	t.Run("A=T B=T: 2-arg path=$.x → subtree", func(t *testing.T) {
		t.Parallel()
		rows, err := f.Open([]Value{NewTextValue(jsonStr), NewTextValue("$.x")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// $.x subtree: {y:1} + key y = 2 rows
		if len(rows) != 2 {
			t.Errorf("2-arg path=$.x json_tree rows = %d, want 2", len(rows))
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: f.config.NumArgs >= 0 && len(args) != f.config.NumArgs
//
// Source: udf.go:70  (userScalarFunc.Call arg count guard)
// Conditions: A=(f.config.NumArgs>=0), B=(len(args)!=f.config.NumArgs)
// Conjunction &&: both must be true to return an error.
// Independent-effect pairs:
//
//	A: {NumArgs=-1 (A=F) → passes through, NumArgs=1 (A=T,B=T) → error}
//	B: {NumArgs=1,1-arg (A=T,B=F) → no error, NumArgs=1,0-arg (A=T,B=T) → error}
//
// Exercised via NewUserScalarFunc + Call.
//
// ---------------------------------------------------------------------------
func TestMCDC_UserScalarFunc_Call_ArgCountGuard(t *testing.T) {
	t.Parallel()

	// A helper UDF that returns 42 for any number of args
	impl := &fixedReturnFunc{val: NewIntValue(42)}

	// A=F: variadic (NumArgs=-1) → any arg count passes through, no error
	t.Run("A=F: variadic NumArgs=-1 → no arg count error", func(t *testing.T) {
		t.Parallel()
		fn := NewUserScalarFunc(FunctionConfig{Name: "testfn", NumArgs: -1}, impl)
		result, err := fn.Call([]Value{NewIntValue(1), NewIntValue(2)})
		if err != nil {
			t.Errorf("variadic Call() unexpected error: %v", err)
		}
		if result.AsInt64() != 42 {
			t.Errorf("variadic Call() = %d, want 42", result.AsInt64())
		}
	})

	// A=T B=F: NumArgs=1, 1-arg passed → no arg count error
	t.Run("A=T B=F: NumArgs=1, correct arg count → no error", func(t *testing.T) {
		t.Parallel()
		fn := NewUserScalarFunc(FunctionConfig{Name: "testfn", NumArgs: 1}, impl)
		result, err := fn.Call([]Value{NewIntValue(7)})
		if err != nil {
			t.Errorf("1-arg Call() unexpected error: %v", err)
		}
		if result.AsInt64() != 42 {
			t.Errorf("1-arg Call() = %d, want 42", result.AsInt64())
		}
	})

	// A=T B=T: NumArgs=1, 0 args passed → error
	t.Run("A=T B=T: NumArgs=1, wrong arg count → error", func(t *testing.T) {
		t.Parallel()
		fn := NewUserScalarFunc(FunctionConfig{Name: "testfn", NumArgs: 1}, impl)
		_, err := fn.Call([]Value{})
		if err == nil {
			t.Error("wrong-arg-count Call() expected error, got nil")
		}
	})
}

// fixedReturnFunc is a trivial UserFunction that returns a preset value.
type fixedReturnFunc struct {
	val Value
}

func (f *fixedReturnFunc) Invoke(args []Value) (Value, error) {
	return f.val, nil
}

// ---------------------------------------------------------------------------
// MC/DC for: f.config.NumArgs >= 0 && len(args) != f.config.NumArgs
//
// Source: udf.go:109  (userAggregateFunc.Step arg count guard)
// Same compound condition but on the aggregate Step path.
// Conditions: A=(f.config.NumArgs>=0), B=(len(args)!=f.config.NumArgs)
//
// ---------------------------------------------------------------------------
func TestMCDC_UserAggregateFunc_Step_ArgCountGuard(t *testing.T) {
	t.Parallel()

	impl := &sumAggImpl{}

	// A=F: variadic (NumArgs=-1) → any arg count passes through
	t.Run("A=F: variadic NumArgs=-1 → no arg count error", func(t *testing.T) {
		t.Parallel()
		fn := NewUserAggregateFunc(FunctionConfig{Name: "myagg", NumArgs: -1}, impl)
		af := fn.(AggregateFunction)
		err := af.Step([]Value{NewIntValue(1), NewIntValue(2)})
		if err != nil {
			t.Errorf("variadic Step() unexpected error: %v", err)
		}
	})

	// A=T B=F: NumArgs=1, 1-arg passed → no error
	t.Run("A=T B=F: NumArgs=1, correct count → no error", func(t *testing.T) {
		t.Parallel()
		impl2 := &sumAggImpl{}
		fn := NewUserAggregateFunc(FunctionConfig{Name: "myagg", NumArgs: 1}, impl2)
		af := fn.(AggregateFunction)
		err := af.Step([]Value{NewIntValue(5)})
		if err != nil {
			t.Errorf("1-arg Step() unexpected error: %v", err)
		}
	})

	// A=T B=T: NumArgs=1, 2-args passed → error
	t.Run("A=T B=T: NumArgs=1, wrong count → error", func(t *testing.T) {
		t.Parallel()
		impl3 := &sumAggImpl{}
		fn := NewUserAggregateFunc(FunctionConfig{Name: "myagg", NumArgs: 1}, impl3)
		af := fn.(AggregateFunction)
		err := af.Step([]Value{NewIntValue(1), NewIntValue(2)})
		if err == nil {
			t.Error("wrong-arg-count Step() expected error, got nil")
		}
	})
}

// sumAggImpl is a minimal UserAggregateFunction for testing.
type sumAggImpl struct {
	total int64
}

func (s *sumAggImpl) Step(args []Value) error {
	for _, a := range args {
		s.total += a.AsInt64()
	}
	return nil
}

func (s *sumAggImpl) Final() (Value, error) {
	return NewIntValue(s.total), nil
}

func (s *sumAggImpl) Reset() {
	s.total = 0
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() || args[1].IsNull() || args[2].IsNull()
//
// Source: scalar.go:301  (replaceFunc: 3-operand OR null guard)
// Conditions: A=(args[0].IsNull()), B=(args[1].IsNull()), C=(args[2].IsNull())
// Short-circuit OR: A=T short-circuits; B only when A=F; C only when A=F,B=F.
// Independent-effect pairs:
//
//	A: {null arg0 (A=T) → NULL, non-null arg0 (A=F,B=F,C=F) → text}
//	B: {null arg1 (A=F,B=T) → NULL, non-null arg1 (A=F,B=F,C=F) → text}
//	C: {null arg2 (A=F,B=F,C=T) → NULL, non-null all (A=F,B=F,C=F) → text}
//
// ---------------------------------------------------------------------------
func TestMCDC_ReplaceFunc_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  string
	}{
		// A=T: arg0 null → NULL
		{
			name:     "A=T: null arg0 → NULL",
			args:     []Value{NewNullValue(), NewTextValue("a"), NewTextValue("b")},
			wantNull: true,
		},
		// A=F B=T: arg1 null → NULL
		{
			name:     "A=F B=T: null arg1 → NULL",
			args:     []Value{NewTextValue("hello"), NewNullValue(), NewTextValue("b")},
			wantNull: true,
		},
		// A=F B=F C=T: arg2 null → NULL
		{
			name:     "A=F B=F C=T: null arg2 → NULL",
			args:     []Value{NewTextValue("hello"), NewTextValue("l"), NewNullValue()},
			wantNull: true,
		},
		// A=F B=F C=F: no nulls → replacement result
		{
			name:     "A=F B=F C=F: no nulls → replaced string",
			args:     []Value{NewTextValue("hello"), NewTextValue("l"), NewTextValue("r")},
			wantNull: false,
			wantVal:  "herro",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := replaceFunc(tt.args)
			if err != nil {
				t.Fatalf("replaceFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("replaceFunc() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsString() != tt.wantVal {
				t.Errorf("replaceFunc() = %q, want %q", result.AsString(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].Type() == TypeBlob && args[1].Type() == TypeBlob
//
// Source: scalar.go:325  (instrFunc blob dispatch)
// Conditions: A=(args[0].Type()==TypeBlob), B=(args[1].Type()==TypeBlob)
// Conjunction &&: both must be true to take the blob path.
// Independent-effect pairs:
//
//	A: {text haystack (A=F) → text path, blob haystack (A=T,B=T) → blob path}
//	B: {blob haystack + text needle (A=T,B=F) → text path, both blob (A=T,B=T) → blob path}
//
// ---------------------------------------------------------------------------
func TestMCDC_InstrFunc_BlobDispatch(t *testing.T) {
	t.Parallel()

	// A=F B=F: both text → text-based search
	t.Run("A=F B=F: text haystack text needle → text search", func(t *testing.T) {
		t.Parallel()
		result, err := instrFunc([]Value{
			NewTextValue("hello world"),
			NewTextValue("world"),
		})
		if err != nil {
			t.Fatalf("instrFunc() unexpected error: %v", err)
		}
		if result.AsInt64() != 7 {
			t.Errorf("instrFunc(text,text) = %d, want 7", result.AsInt64())
		}
	})

	// A=T B=F: blob haystack, text needle → text path (not blob path)
	// instrFunc falls through to instrTextSearch when B=F
	t.Run("A=T B=F: blob haystack text needle → text path", func(t *testing.T) {
		t.Parallel()
		result, err := instrFunc([]Value{
			NewBlobValue([]byte("hello")),
			NewTextValue("ll"),
		})
		if err != nil {
			t.Fatalf("instrFunc() unexpected error: %v", err)
		}
		// text path: "hello" contains "ll" at position 3
		if result.AsInt64() != 3 {
			t.Errorf("instrFunc(blob,text) = %d, want 3", result.AsInt64())
		}
	})

	// A=T B=T: both blob → blob search
	t.Run("A=T B=T: both blob → blob search", func(t *testing.T) {
		t.Parallel()
		result, err := instrFunc([]Value{
			NewBlobValue([]byte{0x01, 0x02, 0x03, 0x04}),
			NewBlobValue([]byte{0x03, 0x04}),
		})
		if err != nil {
			t.Fatalf("instrFunc() unexpected error: %v", err)
		}
		// bytes {0x03,0x04} start at offset 2 (1-indexed → 3)
		if result.AsInt64() != 3 {
			t.Errorf("instrFunc(blob,blob) = %d, want 3", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: !args[1].IsNull()  AND  prob < 0.0 || prob > 1.0
//
// Source: scalar.go:647-651  (likelihoodFunc probability guard)
// First condition: A=(!args[1].IsNull()) — null skips range check.
// Second condition (nested): B=(prob<0.0), C=(prob>1.0) — short-circuit OR.
//
// Independent-effect pairs for outer A:
//
//	A=T,B=F,C=F: non-null valid probability → no error
//	A=F: null probability → no error (check skipped)
//
// Independent-effect pairs for inner B||C (when A=T):
//
//	B=T: prob=-0.1 → error
//	C=T: prob=1.1 → error
//	B=F,C=F: prob=0.5 → no error
//
// ---------------------------------------------------------------------------
func TestMCDC_LikelihoodFunc_ProbabilityGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   Value
		prob    Value
		wantErr bool
	}{
		// A=F: null probability → no validation, no error
		{
			name:    "A=F: null prob → no error",
			value:   NewIntValue(1),
			prob:    NewNullValue(),
			wantErr: false,
		},
		// A=T B=F C=F: valid prob 0.5 → no error
		{
			name:    "A=T B=F C=F: prob=0.5 → valid",
			value:   NewIntValue(1),
			prob:    NewFloatValue(0.5),
			wantErr: false,
		},
		// A=T B=T: prob<0 → error
		{
			name:    "A=T B=T: prob=-0.1 → error",
			value:   NewIntValue(1),
			prob:    NewFloatValue(-0.1),
			wantErr: true,
		},
		// A=T B=F C=T: prob>1 → error
		{
			name:    "A=T B=F C=T: prob=1.1 → error",
			value:   NewIntValue(1),
			prob:    NewFloatValue(1.1),
			wantErr: true,
		},
		// A=T B=F C=F boundary: prob=0.0 exactly → valid
		{
			name:    "A=T B=F C=F: prob=0.0 → valid",
			value:   NewTextValue("x"),
			prob:    NewFloatValue(0.0),
			wantErr: false,
		},
		// A=T B=F C=F boundary: prob=1.0 exactly → valid
		{
			name:    "A=T B=F C=F: prob=1.0 → valid",
			value:   NewTextValue("x"),
			prob:    NewFloatValue(1.0),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := likelihoodFunc([]Value{tt.value, tt.prob})
			if (err != nil) != tt.wantErr {
				t.Errorf("likelihoodFunc() error=%v, wantErr=%v", err, tt.wantErr)
			}
			if err == nil && !tt.wantErr {
				// likelihoodFunc is a pass-through: should return args[0]
				if result.AsInt64() != tt.value.AsInt64() && result.AsString() != tt.value.AsString() {
					t.Errorf("likelihoodFunc() = %v, want %v", result, tt.value)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: (val > 0 && newSum < f.intSum) || (val < 0 && newSum > f.intSum)
//
// Source: aggregate.go:136  (SumFunc.addInteger integer overflow detection)
// Conditions: A=(val>0), B=(newSum<f.intSum), C=(val<0), D=(newSum>f.intSum)
// A and C are mutually exclusive (val cannot be both >0 and <0).
// Full compound: (A && B) || (C && D)
//
// Cases:
//  1. A=T B=T: positive overflow → isFloat set to true
//  2. A=T B=F: positive add, no overflow → stays integer
//  3. C=T D=T: negative overflow → isFloat set
//  4. C=T D=F: negative add, no overflow → stays integer
//  5. A=F C=F (val=0): zero add → no overflow check needed
//
// ---------------------------------------------------------------------------
// sumFinalResult calls Final on a SumFunc and asserts no error, returning the result.
func sumFinalResult(t *testing.T, f *SumFunc) Value {
	t.Helper()
	result, err := f.Final()
	if err != nil {
		t.Fatalf("SumFunc.Final() error: %v", err)
	}
	return result
}

// assertSumType checks that the SumFunc result has the expected type.
func assertSumType(t *testing.T, f *SumFunc, wantType ValueType) Value {
	t.Helper()
	result := sumFinalResult(t, f)
	if result.Type() != wantType {
		t.Errorf("got Type=%v, want %v", result.Type(), wantType)
	}
	return result
}

func TestMCDC_SumFunc_AddInteger_OverflowGuard(t *testing.T) {
	t.Parallel()

	t.Run("positive overflow → float", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(math.MaxInt64)})
		_ = f.Step([]Value{NewIntValue(1)})
		assertSumType(t, f, TypeFloat)
	})

	t.Run("normal positive add → integer 300", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(100)})
		_ = f.Step([]Value{NewIntValue(200)})
		result := assertSumType(t, f, TypeInteger)
		if result.AsInt64() != 300 {
			t.Errorf("sum = %d, want 300", result.AsInt64())
		}
	})

	t.Run("negative overflow → float", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(math.MinInt64)})
		_ = f.Step([]Value{NewIntValue(-1)})
		assertSumType(t, f, TypeFloat)
	})

	t.Run("normal negative add → integer -80", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(-50)})
		_ = f.Step([]Value{NewIntValue(-30)})
		result := assertSumType(t, f, TypeInteger)
		if result.AsInt64() != -80 {
			t.Errorf("sum = %d, want -80", result.AsInt64())
		}
	})

	t.Run("zero add → integer 10", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(10)})
		_ = f.Step([]Value{NewIntValue(0)})
		result := assertSumType(t, f, TypeInteger)
		if result.AsInt64() != 10 {
			t.Errorf("sum = %d, want 10", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: numArgs < 0  (Registry.RegisterUser: variadic vs fixed)
//
// Source: functions.go:295
// Condition: A=(numArgs<0)
// A=T → stores in variadicUser map; A=F → stores in userFuncs map.
// Observable effect: Lookup vs LookupWithArgs behavior differs.
//
// ---------------------------------------------------------------------------
func TestMCDC_Registry_RegisterUser_VariadicVsFixed(t *testing.T) {
	t.Parallel()

	impl := &fixedReturnFunc{val: NewIntValue(99)}

	// A=T: numArgs=-1 → variadic registration → found by Lookup
	t.Run("A=T: variadic registered → found by Lookup", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		fn := NewUserScalarFunc(FunctionConfig{Name: "varfn", NumArgs: -1}, impl)
		r.RegisterUser(fn, -1)

		got, ok := r.Lookup("varfn")
		if !ok {
			t.Error("Lookup('varfn') not found after variadic registration")
		}
		if got == nil {
			t.Error("Lookup('varfn') returned nil function")
		}
	})

	// A=F: numArgs=2 → fixed registration → found by LookupWithArgs
	t.Run("A=F: fixed (2-arg) registered → found by LookupWithArgs", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		fn := NewUserScalarFunc(FunctionConfig{Name: "fixfn", NumArgs: 2}, impl)
		r.RegisterUser(fn, 2)

		got, ok := r.LookupWithArgs("fixfn", 2)
		if !ok {
			t.Error("LookupWithArgs('fixfn', 2) not found after fixed registration")
		}
		if got == nil {
			t.Error("LookupWithArgs('fixfn', 2) returned nil function")
		}

		// Should NOT be found by Lookup (only checks variadic + builtins)
		_, okLookup := r.Lookup("fixfn")
		if okLookup {
			t.Error("fixed-arg fn should not appear in Lookup (only variadic+builtin)")
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: numArgs < 0  (Registry.Unregister: variadic vs fixed branch)
//
// Source: functions.go:312-318
// Condition: A=(numArgs<0)
// A=T → deletes from variadicUser map; A=F → deletes from userFuncs map.
//
// ---------------------------------------------------------------------------
func TestMCDC_Registry_Unregister_VariadicVsFixed(t *testing.T) {
	t.Parallel()

	impl := &fixedReturnFunc{val: NewIntValue(7)}

	// A=T: unregister variadic → removed from variadicUser
	t.Run("A=T: unregister variadic → true, then not found", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		fn := NewUserScalarFunc(FunctionConfig{Name: "varun", NumArgs: -1}, impl)
		r.RegisterUser(fn, -1)

		removed := r.Unregister("varun", -1)
		if !removed {
			t.Error("Unregister variadic returned false, want true")
		}
		_, ok := r.Lookup("varun")
		if ok {
			t.Error("variadic fn still found after Unregister")
		}
	})

	// A=T: unregister variadic that doesn't exist → false
	t.Run("A=T: unregister nonexistent variadic → false", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		removed := r.Unregister("nosuchfn", -1)
		if removed {
			t.Error("Unregister nonexistent variadic returned true, want false")
		}
	})

	// A=F: unregister fixed-arg → removed from userFuncs
	t.Run("A=F: unregister fixed → true, then not found", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		fn := NewUserScalarFunc(FunctionConfig{Name: "fixun", NumArgs: 1}, impl)
		r.RegisterUser(fn, 1)

		removed := r.Unregister("fixun", 1)
		if !removed {
			t.Error("Unregister fixed returned false, want true")
		}
		_, ok := r.LookupWithArgs("fixun", 1)
		if ok {
			t.Error("fixed fn still found after Unregister")
		}
	})

	// A=F: unregister fixed that doesn't exist → false
	t.Run("A=F: unregister nonexistent fixed → false", func(t *testing.T) {
		t.Parallel()
		r := NewRegistry()
		removed := r.Unregister("nosuchfn", 2)
		if removed {
			t.Error("Unregister nonexistent fixed returned true, want false")
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) > pathIndex && !args[pathIndex].IsNull()
//
// Source: json.go:97  (applyPathIfPresent compound guard)
// Conditions: A=(len(args)>pathIndex), B=(!args[pathIndex].IsNull())
// Conjunction &&: both must be true to apply the path.
// Independent-effect pairs:
//
//	A: {1-arg (A=F) → no path, 2-arg non-null (A=T,B=T) → path applied}
//	B: {2-arg null (A=T,B=F) → no path, 2-arg non-null (A=T,B=T) → path applied}
//
// Exercised via jsonArrayLengthFunc (which calls applyPathIfPresent).
//
// ---------------------------------------------------------------------------
func TestMCDC_ApplyPathIfPresent_Guard(t *testing.T) {
	t.Parallel()

	jsonStr := `{"items":[1,2,3]}`

	// A=F: 1-arg → no path arg → returns length of root (not array → NULL)
	t.Run("A=F: 1-arg → no path applied", func(t *testing.T) {
		t.Parallel()
		result, err := jsonArrayLengthFunc([]Value{NewTextValue(`[10,20,30]`)})
		if err != nil {
			t.Fatalf("jsonArrayLengthFunc() unexpected error: %v", err)
		}
		if result.AsInt64() != 3 {
			t.Errorf("jsonArrayLengthFunc([10,20,30]) = %d, want 3", result.AsInt64())
		}
	})

	// A=T B=F: 2-arg null path → path not applied, uses root
	t.Run("A=T B=F: 2-arg null path → root used", func(t *testing.T) {
		t.Parallel()
		// root is an object, not an array → NULL length
		result, err := jsonArrayLengthFunc([]Value{
			NewTextValue(jsonStr),
			NewNullValue(),
		})
		if err != nil {
			t.Fatalf("jsonArrayLengthFunc() unexpected error: %v", err)
		}
		// Root is an object → json_array_length returns NULL
		if !result.IsNull() {
			t.Errorf("jsonArrayLengthFunc(obj, null) should be NULL for object root, got %v", result.AsInt64())
		}
	})

	// A=T B=T: 2-arg non-null path "$.items" → path applied, returns length of $.items array
	t.Run("A=T B=T: 2-arg non-null path → path applied", func(t *testing.T) {
		t.Parallel()
		result, err := jsonArrayLengthFunc([]Value{
			NewTextValue(jsonStr),
			NewTextValue("$.items"),
		})
		if err != nil {
			t.Fatalf("jsonArrayLengthFunc() unexpected error: %v", err)
		}
		if result.AsInt64() != 3 {
			t.Errorf("jsonArrayLengthFunc(obj, '$.items') = %d, want 3", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: value < -1 || value > 1  (asinFunc / acosFunc domain guard)
//
// Source: math.go:330  (asinFunc), math.go:345 (acosFunc)
// Conditions: A=(value<-1), B=(value>1)
// Short-circuit OR: A=T → NaN; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {value=-2 (A=T) → NaN, value=0 (A=F,B=F) → valid}
//	B: {value=2 (A=F,B=T) → NaN, value=0 (A=F,B=F) → valid}
//
// ---------------------------------------------------------------------------
func TestMCDC_AsinFunc_DomainGuard_Extra(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   float64
		wantNaN bool
	}{
		// A=T: value < -1 → NaN
		{name: "A=T: -2 → NaN", input: -2.0, wantNaN: true},
		// A=F B=T: value > 1 → NaN
		{name: "A=F B=T: 2 → NaN", input: 2.0, wantNaN: true},
		// A=F B=F: value in [-1,1] → valid
		{name: "A=F B=F: 0 → valid", input: 0.0, wantNaN: false},
		// Boundary: value=-1 exactly → valid (asin(-1) = -pi/2)
		{name: "boundary: -1 → valid", input: -1.0, wantNaN: false},
		// Boundary: value=1 exactly → valid (asin(1) = pi/2)
		{name: "boundary: 1 → valid", input: 1.0, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := asinFunc([]Value{NewFloatValue(tt.input)})
			if err != nil {
				t.Fatalf("asinFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("asinFunc(%v) IsNaN=%v, want %v", tt.input, gotNaN, tt.wantNaN)
			}
		})
	}
}

func TestMCDC_AcosFunc_DomainGuard_Extra(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   float64
		wantNaN bool
	}{
		// A=T: value < -1 → NaN
		{name: "A=T: -1.5 → NaN", input: -1.5, wantNaN: true},
		// A=F B=T: value > 1 → NaN
		{name: "A=F B=T: 1.5 → NaN", input: 1.5, wantNaN: true},
		// A=F B=F: value in [-1,1] → valid
		{name: "A=F B=F: 0.5 → valid", input: 0.5, wantNaN: false},
		// Boundary: -1 → valid
		{name: "boundary: -1 → valid", input: -1.0, wantNaN: false},
		// Boundary: 1 → valid
		{name: "boundary: 1 → valid", input: 1.0, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := acosFunc([]Value{NewFloatValue(tt.input)})
			if err != nil {
				t.Fatalf("acosFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("acosFunc(%v) IsNaN=%v, want %v", tt.input, gotNaN, tt.wantNaN)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: value <= -1 || value >= 1  (atanhFunc domain guard)
//
// Source: math.go:429
// Conditions: A=(value<=-1), B=(value>=1)
// Short-circuit OR: A=T → NaN; B only when A=F.
// Independent-effect pairs:
//
//	A: {value=-1 (A=T) → NaN, value=0 (A=F,B=F) → valid}
//	B: {value=1 (A=F,B=T) → NaN, value=0 (A=F,B=F) → valid}
//
// ---------------------------------------------------------------------------
func TestMCDC_AtanhFunc_DomainGuard_Extra(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   float64
		wantNaN bool
	}{
		// A=T: value <= -1 (boundary) → NaN
		{name: "A=T: -1 → NaN", input: -1.0, wantNaN: true},
		// A=T: value < -1 → NaN
		{name: "A=T: -2 → NaN", input: -2.0, wantNaN: true},
		// A=F B=T: value >= 1 (boundary) → NaN
		{name: "A=F B=T: 1 → NaN", input: 1.0, wantNaN: true},
		// A=F B=T: value > 1 → NaN
		{name: "A=F B=T: 2 → NaN", input: 2.0, wantNaN: true},
		// A=F B=F: value in (-1,1) → valid
		{name: "A=F B=F: 0 → valid", input: 0.0, wantNaN: false},
		{name: "A=F B=F: 0.5 → valid", input: 0.5, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := atanhFunc([]Value{NewFloatValue(tt.input)})
			if err != nil {
				t.Fatalf("atanhFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("atanhFunc(%v) IsNaN=%v, want %v", tt.input, gotNaN, tt.wantNaN)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() && args[1].IsNull()  (nullifFunc both-null guard)
//
// Source: scalar.go:517
// Conditions: A=(args[0].IsNull()), B=(args[1].IsNull())
// Conjunction &&: both null → return NULL.
// Then: A || B (either null) → return args[0].
// Independent-effect pairs for &&:
//
//	A: {non-null arg0 (A=F) → not taken, null arg0 with null arg1 (A=T,B=T)}
//	B: {null arg0 with non-null arg1 (A=T,B=F) → second guard, both null (A=T,B=T)}
//
// ---------------------------------------------------------------------------
// callNullif invokes nullifFunc and fails the test on error.
func callNullif(t *testing.T, args []Value) Value {
	t.Helper()
	result, err := nullifFunc(args)
	if err != nil {
		t.Fatalf("nullifFunc() error: %v", err)
	}
	return result
}

func TestMCDC_NullifFunc_BothNullGuard(t *testing.T) {
	t.Parallel()

	// A=T B=T: both null → NULL (equal nulls)
	t.Run("A=T B=T: both null → NULL", func(t *testing.T) {
		t.Parallel()
		result := callNullif(t, []Value{NewNullValue(), NewNullValue()})
		if !result.IsNull() {
			t.Errorf("nullif(NULL,NULL) should be NULL, got %v", result.AsString())
		}
	})

	// A=T B=F: arg0 null, arg1 non-null → arg0 returned (not equal)
	t.Run("A=T B=F: arg0 null, arg1 non-null → arg0 (NULL)", func(t *testing.T) {
		t.Parallel()
		result := callNullif(t, []Value{NewNullValue(), NewIntValue(5)})
		if !result.IsNull() {
			t.Errorf("nullif(NULL,5) should be NULL (returns arg0), got %v", result.AsString())
		}
	})

	// A=F B=T: arg0 non-null, arg1 null → arg0 returned
	t.Run("A=F B=T: arg0 non-null, arg1 null → arg0", func(t *testing.T) {
		t.Parallel()
		result := callNullif(t, []Value{NewIntValue(3), NewNullValue()})
		if result.IsNull() || result.AsInt64() != 3 {
			t.Errorf("nullif(3,NULL) should return 3, got %v", result.AsString())
		}
	})

	// A=F B=F: both non-null, equal → NULL
	t.Run("A=F B=F: both non-null equal → NULL", func(t *testing.T) {
		t.Parallel()
		result := callNullif(t, []Value{NewIntValue(5), NewIntValue(5)})
		if !result.IsNull() {
			t.Errorf("nullif(5,5) should be NULL, got %v", result.AsString())
		}
	})

	// A=F B=F: both non-null, not equal → arg0
	t.Run("A=F B=F: both non-null not equal → arg0", func(t *testing.T) {
		t.Parallel()
		result := callNullif(t, []Value{NewIntValue(3), NewIntValue(5)})
		if result.IsNull() || result.AsInt64() != 3 {
			t.Errorf("nullif(3,5) should return 3, got %v", result.AsString())
		}
	})
}
