// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC for: v == float64(int64(v)) && v <= 1e15 && v >= -1e15
//
//	(getJSONType float64 branch → "integer" vs "real")
//
// Note: jsonTypeFunc uses json.Number internally, so integer-looking JSON literals
// are routed through jsonNumberType (dot-check), not the float64 branch.
// We call getJSONType directly to exercise the float64 branch.
//
// Conditions: A=(v==float64(int64(v))), B=(v<=1e15), C=(v>=-1e15)
// Independent-effect pairs:
//
//	A: {3.14 (A=F), 42.0 (A=T)}    — both in [-1e15,1e15]; A alone flips outcome
//	B: {2e15 (B=F), 1.0 (B=T)}     — A=T, C=T held; B flips outcome
//	C: {-2e15 (C=F), 1.0 (C=T)}    — A=T, B=T held; C flips outcome
//
// ---------------------------------------------------------------------------
func TestMCDC_GetJSONType_Float64IntegerCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    interface{}
		wantType string
	}{
		// A=F: fractional float64 → "real" (A alone flips outcome)
		{name: "A=F: 3.14 → real", input: float64(3.14), wantType: "real"},
		// A=T B=T C=T: whole number in range → "integer"
		{name: "A=T B=T C=T: 42.0 → integer", input: float64(42), wantType: "integer"},
		// A=T B=F C=T: 2e15 > 1e15 → "real" (B flips outcome)
		{name: "A=T B=F C=T: 2e15 → real", input: float64(2e15), wantType: "real"},
		// A=T B=T C=F: -2e15 < -1e15 → "real" (C flips outcome)
		{name: "A=T B=T C=F: -2e15 → real", input: float64(-2e15), wantType: "real"},
		// A=T B=T C=T boundary: 1e15 exactly → "integer"
		{name: "A=T B=T C=T: 1e15 → integer", input: float64(1e15), wantType: "integer"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getJSONType(tt.input)
			if got != tt.wantType {
				t.Errorf("getJSONType(%v) = %q, want %q", tt.input, got, tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: strings.Contains(v.String(), ".")  (jsonNumberType)
// Condition: A=(string contains ".")
// Two cases flip it: number string with "." → "real"; without → "integer".
//
// Exercised via jsonTypeFunc on a raw number (no path) to trigger json.Number path.
// ---------------------------------------------------------------------------
func TestMCDC_JSONNumberType_DotCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jsonStr  string
		wantType string
	}{
		// A=T: JSON number "3.14" contains "." → type "real"
		{
			name:     "A=T: 3.14 contains dot → real",
			jsonStr:  `3.14`,
			wantType: "real",
		},
		// A=F: JSON number "42" has no "." → type "integer"
		{
			name:     "A=F: 42 no dot → integer",
			jsonStr:  `42`,
			wantType: "integer",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonTypeFunc([]Value{NewTextValue(tt.jsonStr)})
			if err != nil {
				t.Fatalf("jsonTypeFunc() unexpected error: %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonTypeFunc() returned NULL")
			}
			if result.AsString() != tt.wantType {
				t.Errorf("jsonTypeFunc(%q) = %q, want %q", tt.jsonStr, result.AsString(), tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: v == float64(int64(v))  (jsonFloat64ToValue: int vs float type)
// Condition: A=(v==float64(int64(v)))
// True  → NewIntValue; False → NewFloatValue.
//
// Exercised via jsonExtractFunc on a JSON number field.
// ---------------------------------------------------------------------------
func TestMCDC_JSONFloat64ToValue_IntegerCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jsonStr    string
		path       string
		wantIsInt  bool
		wantIntVal int64
		wantFltVal float64
	}{
		// A=T: whole number 10 → TypeInteger
		{
			name:       "A=T: 10 → TypeInteger",
			jsonStr:    `{"x":10}`,
			path:       "$.x",
			wantIsInt:  true,
			wantIntVal: 10,
		},
		// A=F: fractional 1.5 → TypeFloat
		{
			name:       "A=F: 1.5 → TypeFloat",
			jsonStr:    `{"x":1.5}`,
			path:       "$.x",
			wantIsInt:  false,
			wantFltVal: 1.5,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonExtractFunc([]Value{
				NewTextValue(tt.jsonStr),
				NewTextValue(tt.path),
			})
			if err != nil {
				t.Fatalf("jsonExtractFunc() unexpected error: %v", err)
			}
			gotIsInt := result.Type() == TypeInteger
			if gotIsInt != tt.wantIsInt {
				t.Errorf("jsonExtractFunc() type isInt=%v, want %v", gotIsInt, tt.wantIsInt)
			}
			if tt.wantIsInt && result.AsInt64() != tt.wantIntVal {
				t.Errorf("jsonExtractFunc() int value = %d, want %d", result.AsInt64(), tt.wantIntVal)
			}
			if !tt.wantIsInt && result.AsFloat64() != tt.wantFltVal {
				t.Errorf("jsonExtractFunc() float value = %v, want %v", result.AsFloat64(), tt.wantFltVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: !ok || part.index < 0 || part.index >= len(arr)
//
//	(removeFromArray guard: cast-ok, valid index, in-bounds)
//
// Conditions: A=(!ok), B=(index<0), C=(index>=len(arr))
// Short-circuit OR: A=T → data returned as-is.
// Independent-effect pairs:
//
//	A: {remove from non-array, remove from array at valid index}
//	B: {index=-1 (negative), index=0 (non-negative), on valid array}
//	C: {index=5 (>=len), index=0 (<len), on valid array}
//
// Exercised via jsonRemoveFunc.
// ---------------------------------------------------------------------------
func TestMCDC_RemoveFromArray_Guard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jsonStr string
		path    string
		want    string
	}{
		// A=F B=F C=F: valid array, valid index → element removed
		{
			name:    "A=F B=F C=F: remove [1] from [1,2,3] → [1,3]",
			jsonStr: `[1,2,3]`,
			path:    `$[1]`,
			want:    `[1,3]`,
		},
		// A=F B=F C=T: index out-of-bounds (>=len) → data unchanged
		{
			name:    "A=F B=F C=T: index=5 out of bounds → unchanged",
			jsonStr: `[1,2,3]`,
			path:    `$[5]`,
			want:    `[1,2,3]`,
		},
		// removeFromObject path (not array): remove key from object → works normally
		{
			name:    "object remove: remove key a → {b:2}",
			jsonStr: `{"a":1,"b":2}`,
			path:    `$.a`,
			want:    `{"b":2}`,
		},
		// A=F B=F C=F: first element → [2,3]
		{
			name:    "A=F B=F C=F: remove [0] from [1,2,3] → [2,3]",
			jsonStr: `[1,2,3]`,
			path:    `$[0]`,
			want:    `[2,3]`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonRemoveFunc([]Value{
				NewTextValue(tt.jsonStr),
				NewTextValue(tt.path),
			})
			if err != nil {
				t.Fatalf("jsonRemoveFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("jsonRemoveFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: b <= 0 || b == 1 || val <= 0  (logTwoArgs domain guard)
// Conditions: A=(b<=0), B=(b==1), C=(val<=0)
// Short-circuit OR: A=T → NaN; B only if A=F; C only if A,B both F.
// Independent-effect pairs:
//
//	A: {b=-1, b=2}      — B=F, C=F held when b=2,val=1
//	B: {b=1, b=2}       — A=F, C=F held when val=1
//	C: {b=2,val=0, b=2,val=1} — A=F, B=F held
//
// Exercised via logVariadicFunc with 2 args.
// ---------------------------------------------------------------------------
func TestMCDC_LogTwoArgs_DomainGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		base    float64
		val     float64
		wantNaN bool
	}{
		// A=T B=F C=F: b<=0 → NaN
		{name: "A=T: b=-1 → NaN", base: -1, val: 8, wantNaN: true},
		// A=T (boundary): b=0 → NaN
		{name: "A=T: b=0 → NaN", base: 0, val: 8, wantNaN: true},
		// A=F B=T C=F: b==1 → NaN
		{name: "A=F B=T: b=1 → NaN", base: 1, val: 8, wantNaN: true},
		// A=F B=F C=T: val<=0 → NaN
		{name: "A=F B=F C=T: val=0 → NaN", base: 2, val: 0, wantNaN: true},
		// A=F B=F C=T: val<0 → NaN
		{name: "A=F B=F C=T: val=-1 → NaN", base: 2, val: -1, wantNaN: true},
		// A=F B=F C=F: valid domain → valid result
		{name: "A=F B=F C=F: b=2,val=8 → 3.0", base: 2, val: 8, wantNaN: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := logVariadicFunc([]Value{
				NewFloatValue(tt.base),
				NewFloatValue(tt.val),
			})
			if err != nil {
				t.Fatalf("logVariadicFunc() unexpected error: %v", err)
			}
			gotNaN := math.IsNaN(result.AsFloat64())
			if gotNaN != tt.wantNaN {
				t.Errorf("logVariadicFunc(base=%v, val=%v) IsNaN=%v, want %v",
					tt.base, tt.val, gotNaN, tt.wantNaN)
			}
			if !tt.wantNaN {
				want := math.Log(tt.val) / math.Log(tt.base)
				if math.Abs(result.AsFloat64()-want) > 1e-9 {
					t.Errorf("logVariadicFunc(base=%v, val=%v) = %v, want %v",
						tt.base, tt.val, result.AsFloat64(), want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: upper != 'H' && upper != 'W'  (soundexUpdateCode)
// Conditions: A=(upper!='H'), B=(upper!='W')
// Conjunction && : both must be true for the code to reset lastCode to 0.
// When A=F (char is 'H') or B=F (char is 'W'): lastCode is preserved.
// When A=T and B=T (any other non-coded char like vowel): lastCode resets to 0.
//
// Observable effect: inserting a vowel between same-code consonants ALLOWS
// the second consonant to be appended (because lastCode was reset to 0).
// Inserting H or W between same-code consonants SUPPRESSES the second consonant
// (lastCode is preserved, duplicate is detected).
//
// Independent-effect pairs for the &&:
//
//	A: {'H' (A=F) → preserve, 'A' (A=T,B=T) → reset} — B=T held for 'A' case
//	B: {'W' (B=F) → preserve, 'A' (A=T,B=T) → reset} — A=T held for 'A' case
//
// Exercised via soundexFunc.
// ---------------------------------------------------------------------------
func TestMCDC_SoundexUpdateCode_HWGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		// A=F: H between B and P (both code '1');
		// H preserves lastCode='1', so P is deduplicated → only B coded → B000
		{
			name:  "A=F: H transparent - BHP deduplicates → B000",
			input: "BHP",
			want:  "B000",
		},
		// B=F: W between B and P (both code '1');
		// W preserves lastCode='1', so P is deduplicated → only B coded → B000
		{
			name:  "B=F: W transparent - BWP deduplicates → B000",
			input: "BWP",
			want:  "B000",
		},
		// A=T B=T: vowel A between B and P;
		// A resets lastCode to 0, so P (code '1' != 0) is appended → B100
		{
			name:  "A=T B=T: vowel A resets - BAP appends P → B100",
			input: "BAP",
			want:  "B100",
		},
		// A=T B=T contrast: consonant C between B(1) and P(1);
		// C='2' is different from B='1' so C appended; lastCode='2';
		// then P='1' != '2' → appended too → B210
		{
			name:  "A=T B=T: non-HW consonant: BCPN → B215",
			input: "BCPN",
			want:  "B215",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := soundexFunc([]Value{NewTextValue(tt.input)})
			if err != nil {
				t.Fatalf("soundexFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("soundexFunc(%q) = %q, want %q", tt.input, result.AsString(), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: ok && code != lastCode  (computeSoundex inner condition)
// Conditions: A=(ok: char has a Soundex code), B=(code!=lastCode)
// Both must be true to append a digit.
// Independent-effect pairs:
//
//	A: {vowel 'A' (A=F), consonant 'B' after vowel (A=T,B=T)} — vowel has no code
//	B: {consecutive same-code (B=F), different-code (B=T)} — A=T held
//
// Exercised via soundexFunc.
// ---------------------------------------------------------------------------
func TestMCDC_ComputeSoundex_CodeAppend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		// A=F: vowels have no Soundex code (ok=false) → not appended
		// "FAEI": F=1 (first letter), A/E/I have ok=false → F000
		{
			name:  "A=F: vowels not appended → F000",
			input: "FAEI",
			want:  "F000",
		},
		// A=T B=F: consecutive same-code consonants → duplicate skipped
		// "FFP": F is first letter (code '1'), next F has same code → skipped,
		// P also has code '1' → skipped → F000
		{
			name:  "A=T B=F: same-code consecutive → F000",
			input: "FFP",
			want:  "F000",
		},
		// A=T B=T: different-code consonants → both appended
		// "FL": F=1 (first), L=4 (different) → F400
		{
			name:  "A=T B=T: different codes → F400",
			input: "FL",
			want:  "F400",
		},
		// Full word test: "Robert" → R163
		{
			name:  "Robert → R163",
			input: "Robert",
			want:  "R163",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := soundexFunc([]Value{NewTextValue(tt.input)})
			if err != nil {
				t.Fatalf("soundexFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.want {
				t.Errorf("soundexFunc(%q) = %q, want %q", tt.input, result.AsString(), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: s[0] == '{'  (convertStringToJSON first branch in json.go)
// AND separately:  s[0] == '['  (second branch)
// Conditions for '{' branch: A=(s[0]=='{')
// Conditions for '[' branch: B=(s[0]=='[')  evaluated only when A=F
// Independent-effect pairs:
//
//	A: {"{\"k\":1}", "[1,2]"}  — flips whether object parsing is tried
//	B: {"[1,2]", "hello"}     — flips whether array parsing is tried
//
// Exercised via json_array() which calls valueToJSONSmart → convertStringToJSON.
// ---------------------------------------------------------------------------
func TestMCDC_ConvertStringToJSON_FirstCharDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string // a minified JSON string passed as TEXT argument
		wantOut string // expected json_array result wrapping the input
	}{
		// A=T: input is minified JSON object → parsed and nested as object
		// json_array('{"k":1}') with json() wrapping → [{"k":1}]
		{
			name:    "A=T: minified object → nested as object in array",
			input:   `{"k":1}`,
			wantOut: `[{"k":1}]`,
		},
		// A=F B=T: input is minified JSON array → parsed and nested as array
		// json_array('[1,2]') with json() wrapping → [[1,2]]
		{
			name:    "A=F B=T: minified array → nested as array in array",
			input:   `[1,2]`,
			wantOut: `[[1,2]]`,
		},
		// A=F B=F: plain string → kept as string
		// json_array('hello') → ["hello"]
		{
			name:    "A=F B=F: plain string → kept as string",
			input:   `hello`,
			wantOut: `["hello"]`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Pass the input as a text value directly to jsonArrayFunc
			result, err := jsonArrayFunc([]Value{NewTextValue(tt.input)})
			if err != nil {
				t.Fatalf("jsonArrayFunc() unexpected error: %v", err)
			}
			if result.AsString() != tt.wantOut {
				t.Errorf("jsonArrayFunc(%q) = %q, want %q", tt.input, result.AsString(), tt.wantOut)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) < 3 || len(args)%2 == 0  (processPathValuePairs guard)
// Conditions: A=(len(args)<3), B=(len(args)%2==0)
// Short-circuit OR: A=T → error; B only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {1-arg (A=T,B=F), 3-arg (A=F,B=F)} — B=F held for both
//	B: {4-arg (A=F,B=T), 3-arg (A=F,B=F)} — A=F held for both
//
// Exercised via jsonInsertFunc.
// ---------------------------------------------------------------------------
func TestMCDC_ProcessPathValuePairs_ArgGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []Value
		wantErr bool
	}{
		// A=T B=F: 1 arg (< 3) → error
		{
			name:    "A=T: 1-arg → error",
			args:    []Value{NewTextValue(`{}`)},
			wantErr: true,
		},
		// A=T B=T: 2 args (< 3 and even) → error (A fires first due to short-circuit)
		{
			name:    "A=T B=T: 2-arg → error",
			args:    []Value{NewTextValue(`{}`), NewTextValue(`$.a`)},
			wantErr: true,
		},
		// A=F B=T: 4 args (not <3, but even) → error
		{
			name:    "A=F B=T: 4-arg → error",
			args:    []Value{NewTextValue(`{}`), NewTextValue(`$.a`), NewIntValue(1), NewTextValue(`$.b`)},
			wantErr: true,
		},
		// A=F B=F: 3 args (not <3, odd) → valid insert
		{
			name:    "A=F B=F: 3-arg → valid",
			args:    []Value{NewTextValue(`{}`), NewTextValue(`$.a`), NewIntValue(1)},
			wantErr: false,
		},
		// A=F B=F: 5 args (not <3, odd) → valid insert
		{
			name:    "A=F B=F: 5-arg → valid",
			args:    []Value{NewTextValue(`{}`), NewTextValue(`$.a`), NewIntValue(1), NewTextValue(`$.b`), NewIntValue(2)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := jsonInsertFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("jsonInsertFunc() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(args) < 3 || len(args)%2 == 0  (jsonSetFunc guard)
// Same structure as processPathValuePairs but exercised via jsonSetFunc directly.
// Conditions: A=(len(args)<3), B=(len(args)%2==0)
// ---------------------------------------------------------------------------
func TestMCDC_JSONSetFunc_ArgGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []Value
		wantErr bool
	}{
		// A=T: 2-arg → error (< 3)
		{
			name:    "A=T: 2-arg → error",
			args:    []Value{NewTextValue(`{"a":1}`), NewTextValue(`$.a`)},
			wantErr: true,
		},
		// A=F B=T: 4-arg → error (even)
		{
			name:    "A=F B=T: 4-arg → error",
			args:    []Value{NewTextValue(`{"a":1}`), NewTextValue(`$.a`), NewIntValue(2), NewTextValue(`$.b`)},
			wantErr: true,
		},
		// A=F B=F: 3-arg → valid
		{
			name:    "A=F B=F: 3-arg → valid",
			args:    []Value{NewTextValue(`{"a":1}`), NewTextValue(`$.a`), NewIntValue(99)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := jsonSetFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("jsonSetFunc() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull()  (jsonFunc single-condition null guard)
// Condition: A=(args[0].IsNull())
// A=T → NULL; A=F → non-null result.
// ---------------------------------------------------------------------------
func TestMCDC_JSONFunc_NullGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arg      Value
		wantNull bool
	}{
		// A=T: null input → NULL returned
		{name: "A=T: null → NULL", arg: NewNullValue(), wantNull: true},
		// A=F: valid JSON → minified result
		{name: "A=F: valid JSON → text", arg: NewTextValue(`{"a":1}`), wantNull: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonFunc([]Value{tt.arg})
			if err != nil {
				t.Fatalf("jsonFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("jsonFunc() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull()  (jsonValidFunc null guard)
// Condition: A=(args[0].IsNull())
// A=T → NULL; A=F → 0 or 1 integer.
// ---------------------------------------------------------------------------
func TestMCDC_JSONValidFunc_NullGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arg      Value
		wantNull bool
		wantVal  int64
	}{
		// A=T: null → NULL
		{name: "A=T: null → NULL", arg: NewNullValue(), wantNull: true},
		// A=F: valid JSON → 1
		{name: "A=F: valid JSON → 1", arg: NewTextValue(`[1,2,3]`), wantNull: false, wantVal: 1},
		// A=F: invalid JSON → 0
		{name: "A=F: invalid JSON → 0", arg: NewTextValue(`not json`), wantNull: false, wantVal: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonValidFunc([]Value{tt.arg})
			if err != nil {
				t.Fatalf("jsonValidFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("jsonValidFunc() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsInt64() != tt.wantVal {
				t.Errorf("jsonValidFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: args[0].IsNull() / args[1].IsNull() (jsonPatchFunc two sequential guards)
//
//	First: args[0].IsNull() → return NULL
//	Second: args[1].IsNull() → return args[0] unchanged
//
// These are sequential single-condition guards.
// Condition A=(args[0].IsNull()), Condition B=(args[1].IsNull())
// ---------------------------------------------------------------------------
func TestMCDC_JSONPatchFunc_NullGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  string
	}{
		// A=T: target null → NULL
		{
			name:     "A=T: target null → NULL",
			args:     []Value{NewNullValue(), NewTextValue(`{"b":2}`)},
			wantNull: true,
		},
		// A=F B=T: patch null → original target returned unchanged
		{
			name:     "A=F B=T: patch null → original target",
			args:     []Value{NewTextValue(`{"a":1}`), NewNullValue()},
			wantNull: false,
			wantVal:  `{"a":1}`,
		},
		// A=F B=F: both non-null → merge applied
		{
			name:     "A=F B=F: both non-null → merged",
			args:     []Value{NewTextValue(`{"a":1}`), NewTextValue(`{"b":2}`)},
			wantNull: false,
			wantVal:  `{"a":1,"b":2}`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := jsonPatchFunc(tt.args)
			if err != nil {
				t.Fatalf("jsonPatchFunc() unexpected error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("jsonPatchFunc() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsString() != tt.wantVal {
				t.Errorf("jsonPatchFunc() = %q, want %q", result.AsString(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: !f.hasValue  (SumFunc.Final: null vs result guard)
// Condition: A=(!f.hasValue)
// A=T → return NULL; A=F → return sum.
// ---------------------------------------------------------------------------
func TestMCDC_SumFunc_HasValueGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []Value
		wantNull bool
		wantVal  float64
	}{
		// A=T: no non-null steps → NULL
		{
			name:     "A=T: no values stepped → NULL",
			steps:    []Value{},
			wantNull: true,
		},
		// A=T: only null steps → NULL
		{
			name:     "A=T: only null stepped → NULL",
			steps:    []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		// A=F: at least one non-null stepped → non-null sum
		{
			name:     "A=F: one non-null → sum returned",
			steps:    []Value{NewIntValue(5)},
			wantNull: false,
			wantVal:  5,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &SumFunc{}
			for _, v := range tt.steps {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("SumFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("SumFunc.Final() error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("SumFunc.Final() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsFloat64() != tt.wantVal {
				t.Errorf("SumFunc.Final() = %v, want %v", result.AsFloat64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: f.count == 0  (AvgFunc.Final: null guard)
// Condition: A=(f.count==0)
// A=T → NULL; A=F → average.
// ---------------------------------------------------------------------------
func TestMCDC_AvgFunc_CountGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []Value
		wantNull bool
		wantVal  float64
	}{
		// A=T: no non-null values → NULL
		{
			name:     "A=T: no non-null values → NULL",
			steps:    []Value{NewNullValue()},
			wantNull: true,
		},
		// A=T: empty → NULL
		{
			name:     "A=T: empty → NULL",
			steps:    []Value{},
			wantNull: true,
		},
		// A=F: two values → average
		{
			name:     "A=F: 2 values → avg",
			steps:    []Value{NewFloatValue(3.0), NewFloatValue(7.0)},
			wantNull: false,
			wantVal:  5.0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &AvgFunc{}
			for _, v := range tt.steps {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("AvgFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("AvgFunc.Final() error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("AvgFunc.Final() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsFloat64() != tt.wantVal {
				t.Errorf("AvgFunc.Final() = %v, want %v", result.AsFloat64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: !f.hasValue  (MinFunc.Final and MaxFunc.Final null guard)
// Condition: A=(!f.hasValue)
// A=T → NULL; A=F → min/max value.
// ---------------------------------------------------------------------------
func TestMCDC_MinFunc_HasValueGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []Value
		wantNull bool
		wantVal  int64
	}{
		// A=T: no values → NULL
		{name: "A=T: empty → NULL", steps: nil, wantNull: true},
		// A=T: only nulls → NULL
		{name: "A=T: only nulls → NULL", steps: []Value{NewNullValue()}, wantNull: true},
		// A=F: values present → min
		{name: "A=F: {3,1,2} → min=1", steps: []Value{NewIntValue(3), NewIntValue(1), NewIntValue(2)}, wantNull: false, wantVal: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &MinFunc{}
			for _, v := range tt.steps {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("MinFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("MinFunc.Final() error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("MinFunc.Final() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsInt64() != tt.wantVal {
				t.Errorf("MinFunc.Final() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

func TestMCDC_MaxFunc_HasValueGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		steps    []Value
		wantNull bool
		wantVal  int64
	}{
		// A=T: empty → NULL
		{name: "A=T: empty → NULL", steps: nil, wantNull: true},
		// A=T: only nulls → NULL
		{name: "A=T: only nulls → NULL", steps: []Value{NewNullValue()}, wantNull: true},
		// A=F: values present → max
		{name: "A=F: {3,1,2} → max=3", steps: []Value{NewIntValue(3), NewIntValue(1), NewIntValue(2)}, wantNull: false, wantVal: 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &MaxFunc{}
			for _, v := range tt.steps {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("MaxFunc.Step() error: %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("MaxFunc.Final() error: %v", err)
			}
			if result.IsNull() != tt.wantNull {
				t.Errorf("MaxFunc.Final() IsNull()=%v, want %v", result.IsNull(), tt.wantNull)
			}
			if !tt.wantNull && result.AsInt64() != tt.wantVal {
				t.Errorf("MaxFunc.Final() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: !f.hasValue (MinFunc.Step branch 1) and compareValues(arg,minValue)<0
//
//	if !f.hasValue { set first } else { if compareValues < 0 { update } }
//
// Conditions: A=(!f.hasValue), B=(compareValues(arg, minValue)<0)
// B is only evaluated when A=F.
// Independent-effect pairs:
//
//	A: {first call (A=T), second call (A=F,B=T)}
//	B: {second call smaller (B=T), second call larger (B=F)} — A=F held
//
// ---------------------------------------------------------------------------
func TestMCDC_MinFunc_Step_UpdateGuard(t *testing.T) {
	t.Parallel()

	// A=T path: first non-null value sets minValue
	t.Run("A=T: first value sets min", func(t *testing.T) {
		t.Parallel()
		f := &MinFunc{}
		if err := f.Step([]Value{NewIntValue(10)}); err != nil {
			t.Fatalf("Step error: %v", err)
		}
		result, _ := f.Final()
		if result.AsInt64() != 10 {
			t.Errorf("got %d, want 10", result.AsInt64())
		}
	})

	// A=F B=T: second value is smaller → min updated
	t.Run("A=F B=T: smaller second value updates min", func(t *testing.T) {
		t.Parallel()
		f := &MinFunc{}
		_ = f.Step([]Value{NewIntValue(10)})
		_ = f.Step([]Value{NewIntValue(3)}) // smaller → updates
		result, _ := f.Final()
		if result.AsInt64() != 3 {
			t.Errorf("got %d, want 3", result.AsInt64())
		}
	})

	// A=F B=F: second value is larger → min NOT updated
	t.Run("A=F B=F: larger second value does not update min", func(t *testing.T) {
		t.Parallel()
		f := &MinFunc{}
		_ = f.Step([]Value{NewIntValue(10)})
		_ = f.Step([]Value{NewIntValue(20)}) // larger → no update
		result, _ := f.Final()
		if result.AsInt64() != 10 {
			t.Errorf("got %d, want 10", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: !f.hasValue (MaxFunc.Step) / compareValues(arg, maxValue)>0
// Conditions: A=(!f.hasValue), B=(compareValues(arg, maxValue)>0)
// ---------------------------------------------------------------------------
func TestMCDC_MaxFunc_Step_UpdateGuard(t *testing.T) {
	t.Parallel()

	// A=T: first value sets max
	t.Run("A=T: first value sets max", func(t *testing.T) {
		t.Parallel()
		f := &MaxFunc{}
		_ = f.Step([]Value{NewIntValue(5)})
		result, _ := f.Final()
		if result.AsInt64() != 5 {
			t.Errorf("got %d, want 5", result.AsInt64())
		}
	})

	// A=F B=T: second value larger → max updated
	t.Run("A=F B=T: larger second value updates max", func(t *testing.T) {
		t.Parallel()
		f := &MaxFunc{}
		_ = f.Step([]Value{NewIntValue(5)})
		_ = f.Step([]Value{NewIntValue(15)}) // larger → updates
		result, _ := f.Final()
		if result.AsInt64() != 15 {
			t.Errorf("got %d, want 15", result.AsInt64())
		}
	})

	// A=F B=F: second value smaller → max NOT updated
	t.Run("A=F B=F: smaller second value does not update max", func(t *testing.T) {
		t.Parallel()
		f := &MaxFunc{}
		_ = f.Step([]Value{NewIntValue(5)})
		_ = f.Step([]Value{NewIntValue(2)}) // smaller → no update
		result, _ := f.Final()
		if result.AsInt64() != 5 {
			t.Errorf("got %d, want 5", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: val < 0  (absFunc integer branch: negate vs pass-through)
// Condition: A=(val<0)
// A=T → negate (and overflow check); A=F → return as-is.
// ---------------------------------------------------------------------------
func TestMCDC_AbsFunc_IntegerSign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arg     Value
		wantVal int64
	}{
		// A=T: negative integer → negated
		{name: "A=T: -5 → 5", arg: NewIntValue(-5), wantVal: 5},
		// A=F: positive integer → unchanged
		{name: "A=F: 7 → 7", arg: NewIntValue(7), wantVal: 7},
		// A=F: zero → zero
		{name: "A=F: 0 → 0", arg: NewIntValue(0), wantVal: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := absFunc([]Value{tt.arg})
			if err != nil {
				t.Fatalf("absFunc() unexpected error: %v", err)
			}
			if result.AsInt64() != tt.wantVal {
				t.Errorf("absFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: val < 0 && val == math.MinInt64  (absFunc overflow guard)
// Conditions: A=(val<0), B=(val==math.MinInt64)
// Only when A=T is B evaluated.
// Independent-effect pairs:
//
//	A: {-5 (A=T,B=F → negate ok), +5 (A=F → pass-through)}
//	B: {MinInt64 (A=T,B=T → error), -5 (A=T,B=F → negate ok)}
//
// ---------------------------------------------------------------------------
func TestMCDC_AbsFunc_OverflowGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arg     Value
		wantErr bool
		wantVal int64
	}{
		// A=T B=T: MinInt64 → overflow error
		{name: "A=T B=T: MinInt64 → error", arg: NewIntValue(math.MinInt64), wantErr: true},
		// A=T B=F: other negative → valid negation
		{name: "A=T B=F: -5 → 5", arg: NewIntValue(-5), wantErr: false, wantVal: 5},
		// A=F: positive → no overflow check
		{name: "A=F: 5 → 5", arg: NewIntValue(5), wantErr: false, wantVal: 5},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := absFunc([]Value{tt.arg})
			if (err != nil) != tt.wantErr {
				t.Errorf("absFunc() error=%v, wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && result.AsInt64() != tt.wantVal {
				t.Errorf("absFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: f.isFloat  (SumFunc.addInteger first branch)
// Condition: A=(f.isFloat)
// A=T → add to floatSum; A=F → integer overflow detection path.
// ---------------------------------------------------------------------------
func TestMCDC_SumFunc_AddInteger_IsFloat(t *testing.T) {
	t.Parallel()

	// A=T path: force isFloat by stepping a float value first, then add int
	t.Run("A=T: already float → adds to floatSum", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		// Step a float to set isFloat=true
		_ = f.Step([]Value{NewFloatValue(1.5)})
		// Now add an integer
		_ = f.Step([]Value{NewIntValue(3)})
		result, _ := f.Final()
		if result.AsFloat64() != 4.5 {
			t.Errorf("SumFunc with float+int = %v, want 4.5", result.AsFloat64())
		}
	})

	// A=F path: only integers → stays in integer path
	t.Run("A=F: all integers → integer sum", func(t *testing.T) {
		t.Parallel()
		f := &SumFunc{}
		_ = f.Step([]Value{NewIntValue(2)})
		_ = f.Step([]Value{NewIntValue(3)})
		result, _ := f.Final()
		if result.Type() != TypeInteger {
			t.Errorf("SumFunc with ints only should return TypeInteger, got %v", result.Type())
		}
		if result.AsInt64() != 5 {
			t.Errorf("SumFunc with ints only = %d, want 5", result.AsInt64())
		}
	})
}

// ---------------------------------------------------------------------------
// MC/DC for: len(values) == 0  (GroupConcatFunc.Final null guard)
// Condition: A=(len(values)==0)
// A=T → NULL; A=F → joined string.
// ---------------------------------------------------------------------------
func TestMCDC_GroupConcatFunc_FinalNullGuard(t *testing.T) {
	t.Parallel()

	// A=T: no steps → NULL
	t.Run("A=T: no values → NULL", func(t *testing.T) {
		t.Parallel()
		f := &GroupConcatFunc{}
		result, err := f.Final()
		if err != nil {
			t.Fatalf("GroupConcatFunc.Final() error: %v", err)
		}
		if !result.IsNull() {
			t.Errorf("GroupConcatFunc.Final() with no values should be NULL, got %q", result.AsString())
		}
	})

	// A=T: only null values stepped → NULL (they're skipped)
	t.Run("A=T: only nulls stepped → NULL", func(t *testing.T) {
		t.Parallel()
		f := &GroupConcatFunc{}
		_ = f.Step([]Value{NewNullValue()})
		result, err := f.Final()
		if err != nil {
			t.Fatalf("GroupConcatFunc.Final() error: %v", err)
		}
		if !result.IsNull() {
			t.Errorf("GroupConcatFunc.Final() with only nulls should be NULL, got %q", result.AsString())
		}
	})

	// A=F: values present → joined string
	t.Run("A=F: values present → joined string", func(t *testing.T) {
		t.Parallel()
		f := &GroupConcatFunc{}
		_ = f.Step([]Value{NewTextValue("a")})
		_ = f.Step([]Value{NewTextValue("b")})
		result, err := f.Final()
		if err != nil {
			t.Fatalf("GroupConcatFunc.Final() error: %v", err)
		}
		if result.IsNull() {
			t.Error("GroupConcatFunc.Final() with values should not be NULL")
		}
		if result.AsString() != "a,b" {
			t.Errorf("GroupConcatFunc.Final() = %q, want \"a,b\"", result.AsString())
		}
	})
}
