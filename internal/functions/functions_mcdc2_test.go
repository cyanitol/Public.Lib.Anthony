// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() || args[1].IsNull()  (likeFunc null guard)
// Conditions: A=(args[0].IsNull()), B=(args[1].IsNull())
// Short-circuit OR: A=T short-circuits; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {null-value, "hello"}   — B=F held constant
//	B: {"hello", null-value}   — A=F held constant
//
// ---------------------------------------------------------------------------
func TestMCDC_LikeFunc_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		// A=T B=F: value is NULL → result is NULL
		{
			name:     "A=T B=F: value null → NULL",
			args:     []Value{NewNullValue(), NewTextValue("%lo")},
			wantNull: true,
		},
		// A=F B=T: pattern is NULL → result is NULL
		{
			name:     "A=F B=T: pattern null → NULL",
			args:     []Value{NewTextValue("hello"), NewNullValue()},
			wantNull: true,
		},
		// A=F B=F: neither null → integer result
		{
			name:     "A=F B=F: both non-null → integer 0 or 1",
			args:     []Value{NewTextValue("hello"), NewTextValue("%lo")},
			wantNull: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := likeFunc(tt.args)
			if err != nil {
				t.Fatalf("likeFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("likeFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 3 && !args[2].IsNull()  (likeFunc escape character)
// Conditions: A=(len(args)==3), B=(!args[2].IsNull())
// Independent-effect pairs:
//
//	A: {2-arg call, 3-arg call with non-null escape}  — B=T when 3-arg
//	B: {3-arg with null escape, 3-arg with non-null}  — A=T held constant
//
// ---------------------------------------------------------------------------
func TestMCDC_LikeFunc_EscapeGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []Value
		wantMatch  int64 // 1=match, 0=no match
		escapeUsed bool  // whether the escape path is exercised
	}{
		// A=F B=T (vacuously): 2-arg call → no escape character → default matching
		{
			name:       "A=F: 2-arg no escape",
			args:       []Value{NewTextValue("a%b"), NewTextValue("a%b")},
			wantMatch:  1,
			escapeUsed: false,
		},
		// A=T B=F: 3-arg with null escape → escape character not used (default matching)
		{
			name:       "A=T B=F: 3-arg null escape → default match",
			args:       []Value{NewTextValue("a%b"), NewTextValue("a%b"), NewNullValue()},
			wantMatch:  1,
			escapeUsed: false,
		},
		// A=T B=T: 3-arg with non-null escape → escape character used
		// "a\%b" with escape='\' means literal '%' in pattern, so "a%b" matches "a%b"
		{
			name:       "A=T B=T: 3-arg non-null escape → escape used",
			args:       []Value{NewTextValue("a%b"), NewTextValue(`a\%b`), NewTextValue(`\`)},
			wantMatch:  1,
			escapeUsed: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := likeFunc(tt.args)
			if err != nil {
				t.Fatalf("likeFunc() unexpected error: %v", err)
			}
			if result.AsInt64() != tt.wantMatch {
				t.Errorf("likeFunc() = %d, want %d", result.AsInt64(), tt.wantMatch)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() || args[1].IsNull()  (globFunc null guard)
// Conditions: A=(args[0].IsNull()), B=(args[1].IsNull())
// Independent-effect pairs:
//
//	A: {null-value, "hello"}   — B=F held
//	B: {"hello", null-value}   — A=F held
//
// ---------------------------------------------------------------------------
func TestMCDC_GlobFunc_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		// A=T B=F: value null → NULL
		{
			name:     "A=T B=F: value null → NULL",
			args:     []Value{NewNullValue(), NewTextValue("h*")},
			wantNull: true,
		},
		// A=F B=T: pattern null → NULL
		{
			name:     "A=F B=T: pattern null → NULL",
			args:     []Value{NewTextValue("hello"), NewNullValue()},
			wantNull: true,
		},
		// A=F B=F: neither null → integer result
		{
			name:     "A=F B=F: both non-null → integer",
			args:     []Value{NewTextValue("hello"), NewTextValue("h*")},
			wantNull: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := globFunc(tt.args)
			if err != nil {
				t.Fatalf("globFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("globFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) > pathIndex && !args[pathIndex].IsNull()
//
//	(applyPathIfPresent in json.go)
//
// Conditions: A=(len(args)>pathIndex), B=(!args[pathIndex].IsNull())
// Independent-effect pairs:
//
//	A: {1-arg to jsonArrayLengthFunc, 2-arg with non-null path}  — B=T when arg present
//	B: {2-arg with null path, 2-arg with non-null path}          — A=T held
//
// Exercised via jsonArrayLengthFunc (pathIndex=1).
// ---------------------------------------------------------------------------
func TestMCDC_ApplyPathIfPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []Value
		wantLen int64
	}{
		// A=F: only 1 arg → path not applied → returns top-level array length
		{
			name:    "A=F: 1-arg no path → top-level length",
			args:    []Value{NewTextValue(`[1,2,3]`)},
			wantLen: 3,
		},
		// A=T B=F: 2-arg with null path → path not applied → returns top-level length
		{
			name:    "A=T B=F: 2-arg null path → top-level length",
			args:    []Value{NewTextValue(`[1,2,3]`), NewNullValue()},
			wantLen: 3,
		},
		// A=T B=T: 2-arg with non-null path → path applied → nested array length
		{
			name:    "A=T B=T: 2-arg non-null path → nested length",
			args:    []Value{NewTextValue(`{"a":[10,20]}`), NewTextValue(`$.a`)},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonArrayLengthFunc(tt.args)
			if err != nil {
				t.Fatalf("jsonArrayLengthFunc() unexpected error: %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonArrayLengthFunc() returned NULL, want %d", tt.wantLen)
			}
			if result.AsInt64() != tt.wantLen {
				t.Errorf("jsonArrayLengthFunc() = %d, want %d", result.AsInt64(), tt.wantLen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (jsonTypeWithPath)
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// Independent-effect pairs:
//
//	A: {1-arg, 2-arg non-null}  — B=T when 2-arg
//	B: {2-arg null, 2-arg non-null}  — A=T held
//
// Exercised via jsonTypeFunc.
// ---------------------------------------------------------------------------
func TestMCDC_JSONTypeWithPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantType string
	}{
		// A=F: 1-arg → path not applied → type of root
		{
			name:     "A=F: 1-arg → type of root object",
			args:     []Value{NewTextValue(`{"x":42}`)},
			wantType: "object",
		},
		// A=T B=F: 2-arg with null path → path not applied → type of root
		{
			name:     "A=T B=F: 2-arg null path → type of root",
			args:     []Value{NewTextValue(`{"x":42}`), NewNullValue()},
			wantType: "object",
		},
		// A=T B=T: 2-arg with non-null path → path applied → type of nested value
		{
			name:     "A=T B=T: 2-arg non-null path → type of nested integer",
			args:     []Value{NewTextValue(`{"x":42}`), NewTextValue(`$.x`)},
			wantType: "integer",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonTypeFunc(tt.args)
			if err != nil {
				t.Fatalf("jsonTypeFunc() unexpected error: %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonTypeFunc() returned NULL")
			}
			if result.AsString() != tt.wantType {
				t.Errorf("jsonTypeFunc() = %q, want %q", result.AsString(), tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: f == float64(int64(f))  (convertFloat64ToText)
// Conditions: A=(f==float64(int64(f)))  — single boolean; two cases flip it.
// True  → integer formatting (no decimal point)
// False → float formatting  (decimal point preserved)
//
// Exercised via jsonExtractTextFunc on a JSON number field.
// ---------------------------------------------------------------------------
func TestMCDC_ConvertFloat64ToText_IntegerCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		json string
		path string
		want string
	}{
		// A=T: value is whole number → formatted as integer string
		{
			name: "A=T: whole number 42 → \"42\"",
			json: `{"n":42}`,
			path: "$.n",
			want: "42",
		},
		// A=F: value has fractional part → formatted with decimal
		{
			name: "A=F: fractional 3.14 → \"3.14\"",
			json: `{"n":3.14}`,
			path: "$.n",
			want: "3.14",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonExtractTextFunc([]Value{
				NewTextValue(tt.json),
				NewTextValue(tt.path),
			})
			if err != nil {
				t.Fatalf("jsonExtractTextFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("jsonExtractTextFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (jsonEachFunc.Open path guard)
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// Independent-effect pairs:
//
//	A: {1-arg, 2-arg non-null}  — B=T in 2-arg case
//	B: {2-arg null, 2-arg non-null}  — A=T held
//
// ---------------------------------------------------------------------------
func TestMCDC_JSONEachOpen_PathGuard(t *testing.T) {
	t.Parallel()

	fn := &jsonEachFunc{}

	tests := []struct {
		name     string
		args     []Value
		wantRows int
	}{
		// A=F: 1-arg → no path filtering → all top-level entries
		{
			name:     "A=F: 1-arg → iterate root object keys",
			args:     []Value{NewTextValue(`{"a":1,"b":2}`)},
			wantRows: 2,
		},
		// A=T B=F: 2-arg null path → no path applied → all entries
		{
			name:     "A=T B=F: 2-arg null path → root object",
			args:     []Value{NewTextValue(`{"a":1,"b":2}`), NewNullValue()},
			wantRows: 2,
		},
		// A=T B=T: 2-arg non-null path → path applied → nested array
		{
			name:     "A=T B=T: 2-arg path $.arr → nested array of 3",
			args:     []Value{NewTextValue(`{"arr":[10,20,30]}`), NewTextValue("$.arr")},
			wantRows: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rows, err := fn.Open(tt.args)
			if err != nil {
				t.Fatalf("jsonEachFunc.Open() unexpected error: %v", err)
			}
			if len(rows) != tt.wantRows {
				t.Errorf("jsonEachFunc.Open() rows = %d, want %d", len(rows), tt.wantRows)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (jsonTreeFunc.Open path guard)
// Same structure as jsonEachFunc; tested separately because it's a different function.
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// ---------------------------------------------------------------------------
func TestMCDC_JSONTreeOpen_PathGuard(t *testing.T) {
	t.Parallel()

	fn := &jsonTreeFunc{}

	tests := []struct {
		name        string
		args        []Value
		wantMinRows int // json_tree includes the root node itself
	}{
		// A=F: 1-arg → no path filter → root + all descendants
		// {"a":1,"b":2} → root(object), a(integer), b(integer) = 3 rows
		{
			name:        "A=F: 1-arg → root + all descendants",
			args:        []Value{NewTextValue(`{"a":1,"b":2}`)},
			wantMinRows: 3,
		},
		// A=T B=F: 2-arg null path → same as 1-arg
		{
			name:        "A=T B=F: 2-arg null path → root + all descendants",
			args:        []Value{NewTextValue(`{"a":1,"b":2}`), NewNullValue()},
			wantMinRows: 3,
		},
		// A=T B=T: 2-arg non-null path → path applied → only subtree at $.a (scalar)
		// $.a is integer 1 → just 1 row (the root of the subtree)
		{
			name:        "A=T B=T: 2-arg path $.a → subtree of scalar = 1 row",
			args:        []Value{NewTextValue(`{"a":1,"b":2}`), NewTextValue("$.a")},
			wantMinRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rows, err := fn.Open(tt.args)
			if err != nil {
				t.Fatalf("jsonTreeFunc.Open() unexpected error: %v", err)
			}
			if len(rows) < tt.wantMinRows {
				t.Errorf("jsonTreeFunc.Open() rows = %d, want >= %d", len(rows), tt.wantMinRows)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) > 0 && !args[0].IsNull()  (CountFunc.Step)
// Conditions: A=(len(args)>0), B=(!args[0].IsNull())
// Independent-effect pairs:
//
//	A: {0-arg Step (no-op), 1-arg non-null Step}  — B=T when arg present & non-null
//	B: {1-arg null, 1-arg non-null}               — A=T held constant
//
// ---------------------------------------------------------------------------
func TestMCDC_CountFunc_Step_Guard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stepArgs  [][]Value // sequence of Step calls
		wantCount int64
	}{
		// A=F: Step called with 0 args → count not incremented
		{
			name:      "A=F: 0-arg step → count stays 0",
			stepArgs:  [][]Value{{}},
			wantCount: 0,
		},
		// A=T B=F: Step with null value → not counted
		{
			name:      "A=T B=F: null arg → count stays 0",
			stepArgs:  [][]Value{{NewNullValue()}},
			wantCount: 0,
		},
		// A=T B=T: Step with non-null value → counted
		{
			name:      "A=T B=T: non-null arg → count = 1",
			stepArgs:  [][]Value{{NewIntValue(42)}},
			wantCount: 1,
		},
		// Mixed: null then non-null → count = 1
		{
			name:      "mixed: null then non-null → count = 1",
			stepArgs:  [][]Value{{NewNullValue()}, {NewIntValue(7)}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &CountFunc{}
			for _, args := range tt.stepArgs {
				if err := f.Step(args); err != nil {
					t.Fatalf("CountFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("CountFunc.Final() error: %v", err)
			}
			if result.AsInt64() != tt.wantCount {
				t.Errorf("CountFunc count = %d, want %d", result.AsInt64(), tt.wantCount)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) == 2 && !args[1].IsNull()  (initializeSeparator)
// Conditions: A=(len(args)==2), B=(!args[1].IsNull())
// Independent-effect pairs:
//
//	A: {1-arg step, 2-arg non-null}  — B=T in 2-arg case
//	B: {2-arg null, 2-arg non-null}  — A=T held
//
// Exercised via GroupConcatFunc.
// ---------------------------------------------------------------------------
func TestMCDC_GroupConcatInitSeparator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stepCalls [][]Value // sequence of Step invocations
		wantSep   string    // separator used in output (deduced from joined result)
		wantOut   string    // expected Final() string
	}{
		// A=F: 1-arg steps → default separator ","
		{
			name: "A=F: 1-arg → default comma separator",
			stepCalls: [][]Value{
				{NewTextValue("x")},
				{NewTextValue("y")},
			},
			wantOut: "x,y",
		},
		// A=T B=F: 2-arg with null separator → default separator ","
		{
			name: "A=T B=F: 2-arg null sep → default comma",
			stepCalls: [][]Value{
				{NewTextValue("x"), NewNullValue()},
				{NewTextValue("y"), NewNullValue()},
			},
			wantOut: "x,y",
		},
		// A=T B=T: 2-arg with non-null separator → custom separator used
		{
			name: "A=T B=T: 2-arg non-null sep → custom separator |",
			stepCalls: [][]Value{
				{NewTextValue("x"), NewTextValue("|")},
				{NewTextValue("y"), NewTextValue("|")},
			},
			wantOut: "x|y",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &GroupConcatFunc{}
			for _, args := range tt.stepCalls {
				if err := f.Step(args); err != nil {
					t.Fatalf("GroupConcatFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("GroupConcatFunc.Final() error: %v", err)
			}
			if result.AsString() != tt.wantOut {
				t.Errorf("GroupConcatFunc output = %q, want %q", result.AsString(), tt.wantOut)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: (val > 0 && newSum < f.intSum) || (val < 0 && newSum > f.intSum)
//
//	(SumFunc.addInteger overflow detection)
//
// Conditions (4 total, split across two AND-groups in an OR):
//
//	A=(val>0), B=(newSum<f.intSum) — left AND group
//	C=(val<0), D=(newSum>f.intSum) — right AND group
//
// The overall expression is (A&&B)||(C&&D).
// MC/DC requires: for each sub-condition, a pair where flipping it alone flips the result.
//
// Independent-effect pairs:
//
//	A: positive overflow (A=T,B=T → overflow) vs. A=F,C=F (no overflow, val=0)
//	B: positive overflow (A=T,B=T) vs. A=T,B=F (val>0 but no overflow)
//	C: negative overflow (C=T,D=T → overflow) vs. C=F,A=F (val=0)
//	D: negative overflow (C=T,D=T) vs. C=T,D=F (val<0 but no overflow)
//
// Exercised via SumFunc.Step with int64 values, inspecting isFloat state.
// ---------------------------------------------------------------------------
func TestMCDC_SumFunc_IntegerOverflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initial   int64 // first value to accumulate (sets intSum)
		addVal    int64 // second value — triggers overflow check
		wantFloat bool  // true → overflow happened, isFloat set
	}{
		// A=T B=T: positive val + overflow (val>0, newSum wraps negative)
		// math.MaxInt64 + 1 overflows → newSum < intSum
		{
			name:      "A=T B=T: positive overflow → isFloat",
			initial:   math.MaxInt64,
			addVal:    1,
			wantFloat: true,
		},
		// A=T B=F: positive val but no overflow (val>0, newSum >= intSum)
		{
			name:      "A=T B=F: positive no overflow → stays int",
			initial:   1,
			addVal:    2,
			wantFloat: false,
		},
		// A=F C=F: val=0 → no overflow possible
		{
			name:      "A=F C=F: val=0 → stays int",
			initial:   100,
			addVal:    0,
			wantFloat: false,
		},
		// C=T D=T: negative val + underflow (val<0, newSum wraps positive)
		// math.MinInt64 + (-1) overflows → newSum > intSum
		{
			name:      "C=T D=T: negative overflow → isFloat",
			initial:   math.MinInt64,
			addVal:    -1,
			wantFloat: true,
		},
		// C=T D=F: negative val but no underflow (val<0, newSum <= intSum)
		{
			name:      "C=T D=F: negative no overflow → stays int",
			initial:   10,
			addVal:    -3,
			wantFloat: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &SumFunc{}
			// Seed the accumulator with the initial int value
			if err := f.Step([]Value{NewIntValue(tt.initial)}); err != nil {
				t.Fatalf("SumFunc.Step(initial) error: %v", err)
			}
			// Now add the value that triggers (or doesn't trigger) overflow
			if err := f.Step([]Value{NewIntValue(tt.addVal)}); err != nil {
				t.Fatalf("SumFunc.Step(addVal) error: %v", err)
			}
			if f.isFloat != tt.wantFloat {
				t.Errorf("SumFunc.isFloat = %v, want %v (initial=%d, addVal=%d)",
					f.isFloat, tt.wantFloat, tt.initial, tt.addVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() || args[1].IsNull()  (powerFunc / atan2Func / modFunc)
// Conditions: A=(args[0].IsNull()), B=(args[1].IsNull())
// Independent-effect pairs:
//
//	A: {null,non-null}    — B=F held
//	B: {non-null,null}    — A=F held
//
// ---------------------------------------------------------------------------
func TestMCDC_PowerFunc_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  float64
	}{
		// A=T B=F: base is null → NULL
		{
			name:     "A=T B=F: base null → NULL",
			args:     []Value{NewNullValue(), NewFloatValue(2)},
			wantNull: true,
		},
		// A=F B=T: exponent is null → NULL
		{
			name:     "A=F B=T: exponent null → NULL",
			args:     []Value{NewFloatValue(2), NewNullValue()},
			wantNull: true,
		},
		// A=F B=F: neither null → 2^3 = 8
		{
			name:     "A=F B=F: both non-null → 2^3",
			args:     []Value{NewFloatValue(2), NewFloatValue(3)},
			wantNull: false,
			wantVal:  8,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := powerFunc(tt.args)
			if err != nil {
				t.Fatalf("powerFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("powerFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsFloat64() != tt.wantVal {
				t.Errorf("powerFunc() = %v, want %v", result.AsFloat64(), tt.wantVal)
			}
		})
	}
}

func TestMCDC_Atan2Func_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		// A=T B=F: y is null → NULL
		{name: "A=T B=F: y null → NULL", args: []Value{NewNullValue(), NewFloatValue(1)}, wantNull: true},
		// A=F B=T: x is null → NULL
		{name: "A=F B=T: x null → NULL", args: []Value{NewFloatValue(1), NewNullValue()}, wantNull: true},
		// A=F B=F: both non-null → result
		{name: "A=F B=F: both non-null → result", args: []Value{NewFloatValue(1), NewFloatValue(1)}, wantNull: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := atan2Func(tt.args)
			if err != nil {
				t.Fatalf("atan2Func() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("atan2Func() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

func TestMCDC_ModFunc_NullPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  int64
	}{
		// A=T B=F: x is null → NULL
		{name: "A=T B=F: x null → NULL", args: []Value{NewNullValue(), NewIntValue(3)}, wantNull: true},
		// A=F B=T: y is null → NULL
		{name: "A=F B=T: y null → NULL", args: []Value{NewIntValue(7), NewNullValue()}, wantNull: true},
		// A=F B=F: both non-null, y≠0 → 7 % 3 = 1
		{name: "A=F B=F: both non-null → 7%3=1", args: []Value{NewIntValue(7), NewIntValue(3)}, wantNull: false, wantVal: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := modFunc(tt.args)
			if err != nil {
				t.Fatalf("modFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("modFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsInt64() != tt.wantVal {
				t.Errorf("modFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) < 1 || len(args) > 2  (roundParsePrecision)
// Conditions: A=(len(args)<1), B=(len(args)>2)
// Short-circuit OR: A=T short-circuits; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {0-arg, 1-arg}        — B=F held (0-arg makes A=T, 1-arg makes A=F B=F)
//	B: {3-arg, 1-arg}        — A=F held (3-arg makes B=T, 1-arg makes both F)
//
// Exercised via roundFunc.
// ---------------------------------------------------------------------------
func TestMCDC_RoundParsePrecision_ArgCountGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []Value
		wantErr bool
		wantNil bool // true if result is NULL (valid path)
	}{
		// A=T: 0-arg → error
		{
			name:    "A=T: 0-arg → error",
			args:    []Value{},
			wantErr: true,
		},
		// A=F B=F: 1-arg → valid → round(3.7) = 4
		{
			name:    "A=F B=F: 1-arg → valid",
			args:    []Value{NewFloatValue(3.7)},
			wantErr: false,
		},
		// A=F B=F: 2-arg → valid → round(3.456, 2) = 3.46
		{
			name:    "A=F B=F: 2-arg → valid",
			args:    []Value{NewFloatValue(3.456), NewIntValue(2)},
			wantErr: false,
		},
		// A=F B=T: 3-arg → error
		{
			name:    "A=F B=T: 3-arg → error",
			args:    []Value{NewFloatValue(3.7), NewIntValue(1), NewIntValue(1)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := roundFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("roundFunc() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: !ok || args[0].IsNull()  (roundFunc / truncFunc early-return guard)
// Conditions: A=(!ok), B=(args[0].IsNull())
// Short-circuit OR: A=T short-circuits; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {2-arg with null precision (ok=false), 1-arg valid (ok=true,B=F)}
//	B: {1-arg with null value (ok=true, B=T), 1-arg valid (ok=true, B=F)}
//
// ---------------------------------------------------------------------------
func TestMCDC_RoundFunc_NullGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  float64
	}{
		// A=T: 2-arg where precision arg is null → ok=false → returns NULL
		{
			name:     "A=T: null precision → ok=false → NULL",
			args:     []Value{NewFloatValue(3.7), NewNullValue()},
			wantNull: true,
		},
		// A=F B=T: 1-arg where value is null → ok=true, B=T → NULL
		{
			name:     "A=F B=T: null value → NULL",
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
		// A=F B=F: 1-arg valid → ok=true, B=F → rounded result
		{
			name:     "A=F B=F: valid → rounded",
			args:     []Value{NewFloatValue(3.7)},
			wantNull: false,
			wantVal:  4,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := roundFunc(tt.args)
			if err != nil {
				t.Fatalf("roundFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("roundFunc() IsNull() = %v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsFloat64() != tt.wantVal {
				t.Errorf("roundFunc() = %v, want %v", result.AsFloat64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: value < -1 || value > 1  (asinFunc / acosFunc domain guard)
// Conditions: A=(value < -1), B=(value > 1)
// Short-circuit OR: A=T short-circuits; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {-2, 0}    — B=F held (both have B=F when A flips)
//	B: {2, 0}     — A=F held
//
// ---------------------------------------------------------------------------
func TestMCDC_AsinFunc_DomainGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     float64
		wantNaN bool
	}{
		// A=T B=F: value < -1 → NaN
		{name: "A=T B=F: val=-2 → NaN", val: -2, wantNaN: true},
		// A=F B=T: value > 1 → NaN
		{name: "A=F B=T: val=2 → NaN", val: 2, wantNaN: true},
		// A=F B=F: value in domain → valid result
		{name: "A=F B=F: val=0 → valid", val: 0, wantNaN: false},
		// Boundary: value=-1 → valid (asin(-1) = -π/2)
		{name: "A=F B=F: val=-1 boundary → valid", val: -1, wantNaN: false},
		// Boundary: value=1 → valid (asin(1) = π/2)
		{name: "A=F B=F: val=1 boundary → valid", val: 1, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := asinFunc([]Value{NewFloatValue(tt.val)})
			if err != nil {
				t.Fatalf("asinFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("asinFunc(%v) IsNaN=%v, want %v", tt.val, gotNaN, tt.wantNaN)
			}
		})
	}
}

func TestMCDC_AcosFunc_DomainGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     float64
		wantNaN bool
	}{
		{name: "A=T B=F: val=-2 → NaN", val: -2, wantNaN: true},
		{name: "A=F B=T: val=2 → NaN", val: 2, wantNaN: true},
		{name: "A=F B=F: val=0 → valid", val: 0, wantNaN: false},
		{name: "A=F B=F: val=-1 boundary → valid", val: -1, wantNaN: false},
		{name: "A=F B=F: val=1 boundary → valid", val: 1, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := acosFunc([]Value{NewFloatValue(tt.val)})
			if err != nil {
				t.Fatalf("acosFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("acosFunc(%v) IsNaN=%v, want %v", tt.val, gotNaN, tt.wantNaN)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: value <= -1 || value >= 1  (atanhFunc domain guard)
// Conditions: A=(value<=-1), B=(value>=1)
// Short-circuit OR: A=T short-circuits.
// Independent-effect pairs:
//
//	A: {-1, 0}    — B=F held (B is false for both when value <= 0 and >= -1)
//	B: {1, 0}     — A=F held
//
// ---------------------------------------------------------------------------
func TestMCDC_AtanhFunc_DomainGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     float64
		wantNaN bool
	}{
		// A=T B=F: value=-1 → NaN (at boundary)
		{name: "A=T B=F: val=-1 → NaN", val: -1, wantNaN: true},
		// A=T B=F: value=-2 → NaN (below boundary)
		{name: "A=T B=F: val=-2 → NaN", val: -2, wantNaN: true},
		// A=F B=T: value=1 → NaN (at boundary)
		{name: "A=F B=T: val=1 → NaN", val: 1, wantNaN: true},
		// A=F B=T: value=2 → NaN (above boundary)
		{name: "A=F B=T: val=2 → NaN", val: 2, wantNaN: true},
		// A=F B=F: value in open domain → valid
		{name: "A=F B=F: val=0 → valid", val: 0, wantNaN: false},
		{name: "A=F B=F: val=0.5 → valid", val: 0.5, wantNaN: false},
		{name: "A=F B=F: val=-0.5 → valid", val: -0.5, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := atanhFunc([]Value{NewFloatValue(tt.val)})
			if err != nil {
				t.Fatalf("atanhFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("atanhFunc(%v) IsNaN=%v, want %v", tt.val, gotNaN, tt.wantNaN)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value) >= 4503599627370496.0
//
//	(roundIsPassthrough)
//
// Conditions: A=(IsNaN), B=(IsInf), C=(|value|>=2^52)
// Short-circuit OR: A=T → skip; B only if A=F; C only if A,B both F.
// Independent-effect pairs:
//
//	A: {NaN, 1.0}                   — B=F, C=F held for 1.0
//	B: {+Inf, 1.0}                  — A=F, C=F held for 1.0
//	C: {2^52, 1.0}                  — A=F, B=F held
//
// Exercised via roundFunc (passthrough → returns the value unchanged as float).
// ---------------------------------------------------------------------------
func TestMCDC_RoundIsPassthrough(t *testing.T) {
	t.Parallel()

	const bigVal = 4503599627370496.0 // 2^52

	tests := []struct {
		name            string
		val             float64
		wantPassthrough bool
	}{
		// A=T: NaN → passthrough
		{name: "A=T: NaN → passthrough", val: math.NaN(), wantPassthrough: true},
		// A=F B=T: +Inf → passthrough
		{name: "A=F B=T: +Inf → passthrough", val: math.Inf(1), wantPassthrough: true},
		// A=F B=T: -Inf → passthrough
		{name: "A=F B=T: -Inf → passthrough", val: math.Inf(-1), wantPassthrough: true},
		// A=F B=F C=T: |value|=2^52 exactly → passthrough
		{name: "A=F B=F C=T: val=2^52 → passthrough", val: bigVal, wantPassthrough: true},
		// A=F B=F C=T: value > 2^52 → passthrough
		{name: "A=F B=F C=T: val=2^52+1 → passthrough", val: bigVal + 1, wantPassthrough: true},
		// A=F B=F C=F: normal value → not passthrough (rounding applied)
		{name: "A=F B=F C=F: val=1.5 → not passthrough", val: 1.5, wantPassthrough: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := roundIsPassthrough(tt.val)
			if got != tt.wantPassthrough {
				t.Errorf("roundIsPassthrough(%v) = %v, want %v", tt.val, got, tt.wantPassthrough)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: rounded >= float64(math.MinInt64) && rounded <= float64(math.MaxInt64)
//
//	(roundToIntValue)
//
// Conditions: A=(rounded>=MinInt64_f), B=(rounded<=MaxInt64_f)
// Independent-effect pairs:
//
//	A: {MinInt64_f, -(MaxInt64_f+1e10)} — B=T held; flipping A flips result
//	B: {0.0, MaxInt64_f+1e10}           — A=T held; flipping B flips result
//
// When A&&B → returns IntValue; otherwise → FloatValue.
// ---------------------------------------------------------------------------
func TestMCDC_RoundToIntValue(t *testing.T) {
	t.Parallel()

	minI64f := float64(math.MinInt64)
	maxI64f := float64(math.MaxInt64)
	// Values safely outside range (not representable as int64)
	belowMin := minI64f * 2 // definitely < MinInt64 float representation
	aboveMax := maxI64f * 2 // definitely > MaxInt64 float representation

	tests := []struct {
		name      string
		val       float64
		wantIsInt bool // true → result should be TypeInteger
	}{
		// A=T B=T: value in [MinInt64, MaxInt64] → IntValue
		{name: "A=T B=T: 0.0 → IntValue", val: 0.0, wantIsInt: true},
		{name: "A=T B=T: MinInt64 → IntValue", val: minI64f, wantIsInt: true},
		{name: "A=T B=T: MaxInt64 → IntValue (approx)", val: maxI64f, wantIsInt: true},
		// A=F B=T: value below MinInt64 → FloatValue (A flips result)
		{name: "A=F B=T: below MinInt64 → FloatValue", val: belowMin, wantIsInt: false},
		// A=T B=F: value above MaxInt64 → FloatValue (B flips result)
		{name: "A=T B=F: above MaxInt64 → FloatValue", val: aboveMax, wantIsInt: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := roundToIntValue(tt.val)
			gotIsInt := result.Type() == TypeInteger
			if gotIsInt != tt.wantIsInt {
				t.Errorf("roundToIntValue(%v) type=%v (isInt=%v), want isInt=%v",
					tt.val, result.Type(), gotIsInt, tt.wantIsInt)
			}
		})
	}
}
