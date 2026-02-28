package functions

import (
	"testing"
)

// TestSubstrFunc_EdgeCases tests edge cases for substr function
func TestSubstrFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name: "blob substr",
			args: []Value{NewBlobValue([]byte("hello")), NewIntValue(2), NewIntValue(3)},
			want: "ell",
		},
		{
			name: "blob substr to end",
			args: []Value{NewBlobValue([]byte("hello")), NewIntValue(3)},
			want: "llo",
		},
		{
			name: "blob negative start",
			args: []Value{NewBlobValue([]byte("hello")), NewIntValue(-2)},
			want: "lo",
		},
		{
			name: "blob negative start with length",
			args: []Value{NewBlobValue([]byte("hello")), NewIntValue(-3), NewIntValue(2)},
			want: "ll",
		},
		{
			name: "text negative start overflow",
			args: []Value{NewTextValue("hello"), NewIntValue(-10), NewIntValue(2)},
			want: "",
		},
		{
			name: "text negative length",
			args: []Value{NewTextValue("hello"), NewIntValue(4), NewIntValue(-2)},
			want: "el",
		},
		{
			name: "text negative length larger than start",
			args: []Value{NewTextValue("hello"), NewIntValue(2), NewIntValue(-5)},
			want: "h",
		},
		{
			name:     "null length argument",
			args:     []Value{NewTextValue("hello"), NewIntValue(1), NewNullValue()},
			wantNull: true,
		},
		{
			name:     "null start argument",
			args:     []Value{NewTextValue("hello"), NewNullValue()},
			wantNull: true,
		},
		{
			name: "zero start position returns full string",
			args: []Value{NewTextValue("hello"), NewIntValue(0)},
			want: "hello",
		},
		{
			name: "start beyond length",
			args: []Value{NewTextValue("hello"), NewIntValue(100), NewIntValue(5)},
			want: "",
		},
		{
			name:    "invalid number of args",
			args:    []Value{NewTextValue("hello")},
			wantErr: true,
		},
		{
			name:    "too many args",
			args:    []Value{NewTextValue("hello"), NewIntValue(1), NewIntValue(2), NewIntValue(3)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := substrFunc(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("substrFunc() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("substrFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("substrFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("substrFunc() returned NULL")
			}

			var got string
			if result.Type() == TypeBlob {
				got = string(result.AsBlob())
			} else {
				got = result.AsString()
			}

			if got != tt.want {
				t.Errorf("substrFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestInstrFunc_EdgeCases tests edge cases for instr function
func TestInstrFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     int64
		wantNull bool
	}{
		{
			name: "find in middle",
			args: []Value{NewTextValue("hello world"), NewTextValue("world")},
			want: 7,
		},
		{
			name: "find at beginning",
			args: []Value{NewTextValue("hello world"), NewTextValue("hello")},
			want: 1,
		},
		{
			name: "not found",
			args: []Value{NewTextValue("hello world"), NewTextValue("xyz")},
			want: 0,
		},
		{
			name: "empty needle",
			args: []Value{NewTextValue("hello"), NewTextValue("")},
			want: 1,
		},
		{
			name: "blob search",
			args: []Value{NewBlobValue([]byte("hello world")), NewBlobValue([]byte("world"))},
			want: 7,
		},
		{
			name: "blob not found",
			args: []Value{NewBlobValue([]byte("hello world")), NewBlobValue([]byte("xyz"))},
			want: 0,
		},
		{
			name: "utf-8 aware search",
			args: []Value{NewTextValue("hello 世界"), NewTextValue("世界")},
			want: 7,
		},
		{
			name:     "null haystack",
			args:     []Value{NewNullValue(), NewTextValue("test")},
			wantNull: true,
		},
		{
			name:     "null needle",
			args:     []Value{NewTextValue("test"), NewNullValue()},
			wantNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := instrFunc(tt.args)
			if err != nil {
				t.Fatalf("instrFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("instrFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("instrFunc() returned NULL")
			}
			got := result.AsInt64()
			if got != tt.want {
				t.Errorf("instrFunc() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestUnhexFunc_EdgeCases tests edge cases for unhex function
func TestUnhexFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     []byte
		wantNull bool
		wantErr  bool
	}{
		{
			name: "simple hex",
			args: []Value{NewTextValue("48656C6C6F")},
			want: []byte("Hello"),
		},
		{
			name: "lowercase hex",
			args: []Value{NewTextValue("48656c6c6f")},
			want: []byte("Hello"),
		},
		{
			name: "hex with ignored chars",
			args: []Value{NewTextValue("48-65-6C-6C-6F"), NewTextValue("-")},
			want: []byte("Hello"),
		},
		{
			name: "hex with multiple ignored chars",
			args: []Value{NewTextValue("48:65:6C:6C:6F"), NewTextValue(": ")},
			want: []byte("Hello"),
		},
		{
			name:     "invalid hex",
			args:     []Value{NewTextValue("GG")},
			wantNull: true,
		},
		{
			name:     "odd length hex",
			args:     []Value{NewTextValue("123")},
			wantNull: true,
		},
		{
			name:     "null input",
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:    "too many args",
			args:    []Value{NewTextValue("48"), NewTextValue("-"), NewTextValue("extra")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unhexFunc(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("unhexFunc() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unhexFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("unhexFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("unhexFunc() returned NULL")
			}
			got := result.AsBlob()
			if string(got) != string(tt.want) {
				t.Errorf("unhexFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTrimFunc_EdgeCases tests edge cases for trim functions
func TestTrimFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		fn       func([]Value) (Value, error)
		fnName   string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:   "trim null input",
			fn:     trimFunc,
			fnName: "trim",
			args:   []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:   "trim null cutset",
			fn:     trimFunc,
			fnName: "trim",
			args:   []Value{NewTextValue("  hello  "), NewNullValue()},
			want:   "hello",
		},
		{
			name:   "ltrim null input",
			fn:     ltrimFunc,
			fnName: "ltrim",
			args:   []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:   "ltrim null cutset",
			fn:     ltrimFunc,
			fnName: "ltrim",
			args:   []Value{NewTextValue("  hello  "), NewNullValue()},
			want:   "hello  ",
		},
		{
			name:   "rtrim null input",
			fn:     rtrimFunc,
			fnName: "rtrim",
			args:   []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:   "rtrim null cutset",
			fn:     rtrimFunc,
			fnName: "rtrim",
			args:   []Value{NewTextValue("  hello  "), NewNullValue()},
			want:   "  hello",
		},
		{
			name:    "trim too many args",
			fn:      trimFunc,
			fnName:  "trim",
			args:    []Value{NewTextValue("test"), NewTextValue(" "), NewTextValue("extra")},
			wantErr: true,
		},
		{
			name:    "ltrim too many args",
			fn:      ltrimFunc,
			fnName:  "ltrim",
			args:    []Value{NewTextValue("test"), NewTextValue(" "), NewTextValue("extra")},
			wantErr: true,
		},
		{
			name:    "rtrim too many args",
			fn:      rtrimFunc,
			fnName:  "rtrim",
			args:    []Value{NewTextValue("test"), NewTextValue(" "), NewTextValue("extra")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%s() expected error, got nil", tt.fnName)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s() error = %v", tt.fnName, err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("%s() = %v, want NULL", tt.fnName, result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("%s() returned NULL", tt.fnName)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("%s() = %q, want %q", tt.fnName, got, tt.want)
			}
		})
	}
}

// TestUpperLowerFunc_EdgeCases tests edge cases for upper/lower functions
func TestUpperLowerFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		fn       func([]Value) (Value, error)
		fnName   string
		input    Value
		want     string
		wantNull bool
	}{
		{
			name:     "upper null",
			fn:       upperFunc,
			fnName:   "upper",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:   "upper empty string",
			fn:     upperFunc,
			fnName: "upper",
			input:  NewTextValue(""),
			want:   "",
		},
		{
			name:   "upper mixed case",
			fn:     upperFunc,
			fnName: "upper",
			input:  NewTextValue("HeLLo WoRLd"),
			want:   "HELLO WORLD",
		},
		{
			name:     "lower null",
			fn:       lowerFunc,
			fnName:   "lower",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:   "lower empty string",
			fn:     lowerFunc,
			fnName: "lower",
			input:  NewTextValue(""),
			want:   "",
		},
		{
			name:   "lower mixed case",
			fn:     lowerFunc,
			fnName: "lower",
			input:  NewTextValue("HeLLo WoRLd"),
			want:   "hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn([]Value{tt.input})
			if err != nil {
				t.Fatalf("%s() error = %v", tt.fnName, err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("%s() = %v, want NULL", tt.fnName, result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("%s() returned NULL", tt.fnName)
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("%s() = %q, want %q", tt.fnName, got, tt.want)
			}
		})
	}
}

// TestReplaceFunc_EdgeCases tests edge cases for replace function
func TestReplaceFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
	}{
		{
			name: "replace with null replacement",
			args: []Value{NewTextValue("hello world"), NewTextValue("world"), NewNullValue()},
			want: "hello ",
		},
		{
			name: "replace empty pattern",
			args: []Value{NewTextValue("hello"), NewTextValue(""), NewTextValue("x")},
			want: "hello",
		},
		{
			name:     "null input",
			args:     []Value{NewNullValue(), NewTextValue("x"), NewTextValue("y")},
			wantNull: true,
		},
		{
			name:     "null pattern",
			args:     []Value{NewTextValue("hello"), NewNullValue(), NewTextValue("y")},
			wantNull: true,
		},
		{
			name: "multiple replacements",
			args: []Value{NewTextValue("hello hello"), NewTextValue("hello"), NewTextValue("hi")},
			want: "hi hi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := replaceFunc(tt.args)
			if err != nil {
				t.Fatalf("replaceFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("replaceFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("replaceFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("replaceFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestLengthFunc_EdgeCases tests edge cases for length function
func TestLengthFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		want     int64
		wantNull bool
	}{
		{
			name:     "null input",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:  "empty string",
			input: NewTextValue(""),
			want:  0,
		},
		{
			name:  "utf-8 string",
			input: NewTextValue("世界"),
			want:  2,
		},
		{
			name:  "blob",
			input: NewBlobValue([]byte("hello")),
			want:  5,
		},
		{
			name:  "integer",
			input: NewIntValue(12345),
			want:  8, // Length of the bytes representation
		},
		{
			name:  "float",
			input: NewFloatValue(3.14),
			want:  8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lengthFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("lengthFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("lengthFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("lengthFunc() returned NULL")
			}
			got := result.AsInt64()
			if got != tt.want {
				t.Errorf("lengthFunc() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestHexFunc_EdgeCases tests edge cases for hex function
func TestHexFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		want     string
		wantNull bool
	}{
		{
			name:     "null input",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:  "empty blob",
			input: NewBlobValue([]byte{}),
			want:  "",
		},
		{
			name:  "text to hex",
			input: NewTextValue("Hello"),
			want:  "48656C6C6F",
		},
		{
			name:  "blob to hex",
			input: NewBlobValue([]byte{0xFF, 0x00, 0xAB}),
			want:  "FF00AB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hexFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("hexFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("hexFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("hexFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("hexFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestUnicodeFunc_EdgeCases tests edge cases for unicode function
func TestUnicodeFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		want     int64
		wantNull bool
	}{
		{
			name:     "null input",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:     "empty string",
			input:    NewTextValue(""),
			wantNull: true,
		},
		{
			name:  "ascii character",
			input: NewTextValue("A"),
			want:  65,
		},
		{
			name:  "utf-8 character",
			input: NewTextValue("世"),
			want:  19990,
		},
		{
			name:  "emoji",
			input: NewTextValue("😀"),
			want:  128512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unicodeFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("unicodeFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("unicodeFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("unicodeFunc() returned NULL")
			}
			got := result.AsInt64()
			if got != tt.want {
				t.Errorf("unicodeFunc() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestCharFunc_EdgeCases tests edge cases for char function
func TestCharFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		args  []Value
		want  string
	}{
		{
			name: "single character",
			args: []Value{NewIntValue(65)},
			want: "A",
		},
		{
			name: "multiple characters",
			args: []Value{NewIntValue(72), NewIntValue(101), NewIntValue(108), NewIntValue(108), NewIntValue(111)},
			want: "Hello",
		},
		{
			name: "with null values",
			args: []Value{NewIntValue(72), NewNullValue(), NewIntValue(105)},
			want: "Hi",
		},
		{
			name: "invalid code point negative",
			args: []Value{NewIntValue(-1)},
			want: "�",
		},
		{
			name: "invalid code point too large",
			args: []Value{NewIntValue(0x110000)},
			want: "�",
		},
		{
			name: "emoji",
			args: []Value{NewIntValue(128512)},
			want: "😀",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := charFunc(tt.args)
			if err != nil {
				t.Fatalf("charFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Fatalf("charFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("charFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestCoalesceFunc_EdgeCases tests edge cases for coalesce function
func TestCoalesceFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     Value
		wantNull bool
		wantErr  bool
	}{
		{
			name: "first non-null",
			args: []Value{NewNullValue(), NewIntValue(42), NewIntValue(100)},
			want: NewIntValue(42),
		},
		{
			name: "all null",
			args: []Value{NewNullValue(), NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name: "first is non-null",
			args: []Value{NewIntValue(42), NewNullValue()},
			want: NewIntValue(42),
		},
		{
			name:    "no arguments",
			args:    []Value{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := coalesceFunc(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("coalesceFunc() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("coalesceFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("coalesceFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("coalesceFunc() returned NULL")
			}
			if !valuesEqual(result, tt.want) {
				t.Errorf("coalesceFunc() = %v, want %v", result, tt.want)
			}
		})
	}
}

// TestNullifFunc_EdgeCases tests edge cases for nullif function
func TestNullifFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantX    bool
	}{
		{
			name:     "both null - equal",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:  "first null, second not - not equal",
			args:  []Value{NewNullValue(), NewIntValue(42)},
			wantX: true,
		},
		{
			name:  "second null, first not - not equal",
			args:  []Value{NewIntValue(42), NewNullValue()},
			wantX: true,
		},
		{
			name:     "equal integers",
			args:     []Value{NewIntValue(42), NewIntValue(42)},
			wantNull: true,
		},
		{
			name:  "different integers",
			args:  []Value{NewIntValue(42), NewIntValue(100)},
			wantX: true,
		},
		{
			name:     "equal strings",
			args:     []Value{NewTextValue("hello"), NewTextValue("hello")},
			wantNull: true,
		},
		{
			name:  "different strings",
			args:  []Value{NewTextValue("hello"), NewTextValue("world")},
			wantX: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := nullifFunc(tt.args)
			if err != nil {
				t.Fatalf("nullifFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("nullifFunc() = %v, want NULL", result)
				}
				return
			}
			if tt.wantX {
				// Should return the first argument
				if !valuesEqual(result, tt.args[0]) {
					t.Errorf("nullifFunc() = %v, want %v", result, tt.args[0])
				}
			}
		})
	}
}

// TestIifFunc_EdgeCases tests edge cases for iif function
func TestIifFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantTrue bool
	}{
		{
			name:     "null condition - false",
			args:     []Value{NewNullValue(), NewIntValue(1), NewIntValue(2)},
			wantTrue: false,
		},
		{
			name:     "integer zero - false",
			args:     []Value{NewIntValue(0), NewIntValue(1), NewIntValue(2)},
			wantTrue: false,
		},
		{
			name:     "integer non-zero - true",
			args:     []Value{NewIntValue(42), NewIntValue(1), NewIntValue(2)},
			wantTrue: true,
		},
		{
			name:     "float zero - false",
			args:     []Value{NewFloatValue(0.0), NewIntValue(1), NewIntValue(2)},
			wantTrue: false,
		},
		{
			name:     "float non-zero - true",
			args:     []Value{NewFloatValue(3.14), NewIntValue(1), NewIntValue(2)},
			wantTrue: true,
		},
		{
			name:     "text numeric zero - false",
			args:     []Value{NewTextValue("0"), NewIntValue(1), NewIntValue(2)},
			wantTrue: false,
		},
		{
			name:     "text numeric non-zero - true",
			args:     []Value{NewTextValue("42"), NewIntValue(1), NewIntValue(2)},
			wantTrue: true,
		},
		{
			name:     "text non-numeric - false",
			args:     []Value{NewTextValue("hello"), NewIntValue(1), NewIntValue(2)},
			wantTrue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := iifFunc(tt.args)
			if err != nil {
				t.Fatalf("iifFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Fatalf("iifFunc() returned NULL")
			}
			expectedIdx := 2
			if tt.wantTrue {
				expectedIdx = 1
			}
			if !valuesEqual(result, tt.args[expectedIdx]) {
				t.Errorf("iifFunc() = %v, want %v", result, tt.args[expectedIdx])
			}
		})
	}
}

// TestZeroblobFunc_EdgeCases tests edge cases for zeroblob function
func TestZeroblobFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		wantLen  int
		wantNull bool
	}{
		{
			name:     "null input",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:    "zero length",
			input:   NewIntValue(0),
			wantLen: 0,
		},
		{
			name:    "negative length",
			input:   NewIntValue(-5),
			wantLen: 0,
		},
		{
			name:    "positive length",
			input:   NewIntValue(10),
			wantLen: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := zeroblobFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("zeroblobFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("zeroblobFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("zeroblobFunc() returned NULL")
			}
			got := result.AsBlob()
			if len(got) != tt.wantLen {
				t.Errorf("zeroblobFunc() length = %d, want %d", len(got), tt.wantLen)
			}
			// Verify all bytes are zero
			for i, b := range got {
				if b != 0 {
					t.Errorf("zeroblobFunc() byte[%d] = %d, want 0", i, b)
				}
			}
		})
	}
}

// TestCompareValues_EdgeCases tests edge cases for compareValues function
func TestCompareValues_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		a    Value
		b    Value
		want int
	}{
		{
			name: "both null",
			a:    NewNullValue(),
			b:    NewNullValue(),
			want: 0,
		},
		{
			name: "first null",
			a:    NewNullValue(),
			b:    NewIntValue(42),
			want: -1,
		},
		{
			name: "second null",
			a:    NewIntValue(42),
			b:    NewNullValue(),
			want: 1,
		},
		{
			name: "different types",
			a:    NewIntValue(1),
			b:    NewTextValue("1"),
			want: int(TypeInteger) - int(TypeText),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			if (got < 0 && tt.want >= 0) || (got > 0 && tt.want <= 0) || (got == 0 && tt.want != 0) {
				t.Errorf("compareValues() = %d, want %d", got, tt.want)
			}
		})
	}
}

// Helper function to compare values
func valuesEqual(a, b Value) bool {
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() || b.IsNull() {
		return false
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Type() {
	case TypeInteger:
		return a.AsInt64() == b.AsInt64()
	case TypeFloat:
		return a.AsFloat64() == b.AsFloat64()
	case TypeText:
		return a.AsString() == b.AsString()
	case TypeBlob:
		return string(a.AsBlob()) == string(b.AsBlob())
	}
	return false
}
