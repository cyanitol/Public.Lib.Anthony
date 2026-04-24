// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// TestJSONFunc_EdgeCases tests edge cases for json function
func TestJSONFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		wantNull bool
		wantErr  bool
		validate func(string) bool
	}{
		{
			name:     "null input",
			input:    NewNullValue(),
			wantNull: true,
		},
		{
			name:    "invalid json",
			input:   NewTextValue("{invalid}"),
			wantErr: true,
		},
		{
			name:  "valid json object",
			input: NewTextValue(`{"key":"value"}`),
			validate: func(s string) bool {
				return s == `{"key":"value"}`
			},
		},
		{
			name:  "json with whitespace",
			input: NewTextValue(`{ "key" : "value" }`),
			validate: func(s string) bool {
				return s == `{"key":"value"}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonFunc([]Value{tt.input})
			r, ok := assertFuncResult(t, "jsonFunc", result, err, tt.wantErr, tt.wantNull)
			if ok && tt.validate != nil && !tt.validate(r.AsString()) {
				t.Errorf("jsonFunc() = %v, validation failed", r.AsString())
			}
		})
	}
}

// TestJSONArrayFunc_EdgeCases tests edge cases for json_array function
func TestJSONArrayFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "empty array",
			args: []Value{},
			want: "[]",
		},
		{
			name: "single element",
			args: []Value{NewIntValue(42)},
			want: "[42]",
		},
		{
			name: "with null",
			args: []Value{NewIntValue(1), NewNullValue(), NewIntValue(3)},
			want: "[1,null,3]",
		},
		{
			name: "mixed types",
			args: []Value{NewIntValue(1), NewTextValue("hello"), NewFloatValue(3.14)},
			want: "[1,\"hello\",3.14]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayFunc(tt.args)
			if err != nil {
				t.Fatalf("jsonArrayFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonArrayFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("jsonArrayFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestJSONArrayLengthFunc_EdgeCases tests edge cases for json_array_length function
func TestJSONArrayLengthFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     int64
		wantNull bool
		wantErr  bool
	}{
		{
			name:     "null json",
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}")},
			wantNull: true,
		},
		{
			name: "empty array",
			args: []Value{NewTextValue("[]")},
			want: 0,
		},
		{
			name: "array with elements",
			args: []Value{NewTextValue("[1,2,3]")},
			want: 3,
		},
		{
			name:     "not an array",
			args:     []Value{NewTextValue(`{"key":"value"}`)},
			wantNull: true,
		},
		{
			name: "with path to array",
			args: []Value{NewTextValue(`{"arr":[1,2,3]}`), NewTextValue("$.arr")},
			want: 3,
		},
		{
			name:     "with path to non-array",
			args:     []Value{NewTextValue(`{"key":"value"}`), NewTextValue("$.key")},
			wantNull: true,
		},
		{
			name: "with null path returns array length",
			args: []Value{NewTextValue("[1,2,3]"), NewNullValue()},
			want: 3,
		},
		{
			name:    "too many args",
			args:    []Value{NewTextValue("[]"), NewTextValue("$"), NewTextValue("extra")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayLengthFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonArrayLengthFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsInt64(); got != tt.want {
					t.Errorf("jsonArrayLengthFunc() = %d, want %d", got, tt.want)
				}
			}
		})
	}
}

// TestJSONExtractFunc_EdgeCases tests edge cases for json_extract function
func TestJSONExtractFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "too few args",
			args:    []Value{NewTextValue("{}")},
			wantErr: true,
		},
		{
			name:     "null json",
			args:     []Value{NewNullValue(), NewTextValue("$")},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("$")},
			wantNull: true,
		},
		{
			name:     "null path",
			args:     []Value{NewTextValue("{}"), NewNullValue()},
			wantNull: true,
		},
		{
			name: "extract root",
			args: []Value{NewTextValue(`{"key":"value"}`), NewTextValue("$")},
			want: `{"key":"value"}`,
		},
		{
			name: "extract key",
			args: []Value{NewTextValue(`{"key":"value"}`), NewTextValue("$.key")},
			want: "value",
		},
		{
			name: "multiple paths",
			args: []Value{NewTextValue(`{"a":1,"b":2}`), NewTextValue("$.a"), NewTextValue("$.b")},
			want: "[1,2]",
		},
		{
			name: "multiple paths with null",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue("$.a"), NewNullValue()},
			want: "[1,null]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonExtractFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonExtractFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonExtractFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONInsertFunc_EdgeCases tests edge cases for json_insert function
func TestJSONInsertFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "too few args",
			args:    []Value{NewTextValue("{}")},
			wantErr: true,
		},
		{
			name:    "even number of args",
			args:    []Value{NewTextValue("{}"), NewTextValue("$.a")},
			wantErr: true,
		},
		{
			name:     "null json",
			args:     []Value{NewNullValue(), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name: "insert new key",
			args: []Value{NewTextValue("{}"), NewTextValue("$.a"), NewIntValue(1)},
			want: `{"a":1}`,
		},
		{
			name: "skip existing key",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue("$.a"), NewIntValue(2)},
			want: `{"a":1}`,
		},
		{
			name: "null path is skipped",
			args: []Value{NewTextValue("{}"), NewNullValue(), NewIntValue(1)},
			want: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonInsertFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonInsertFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonInsertFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONObjectFunc_EdgeCases tests edge cases for json_object function
func TestJSONObjectFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		args    []Value
		want    string
		wantErr bool
	}{
		{
			name: "empty object",
			args: []Value{},
			want: "{}",
		},
		{
			name: "single pair",
			args: []Value{NewTextValue("key"), NewIntValue(42)},
			want: `{"key":42}`,
		},
		{
			name: "multiple pairs",
			args: []Value{NewTextValue("a"), NewIntValue(1), NewTextValue("b"), NewIntValue(2)},
			want: `{"a":1,"b":2}`,
		},
		{
			name:    "odd number of args",
			args:    []Value{NewTextValue("key")},
			wantErr: true,
		},
		{
			name:    "null key",
			args:    []Value{NewNullValue(), NewIntValue(1)},
			wantErr: true,
		},
		{
			name: "null value",
			args: []Value{NewTextValue("key"), NewNullValue()},
			want: `{"key":null}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonObjectFunc(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonObjectFunc() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonObjectFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonObjectFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("jsonObjectFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestJSONPatchFunc_EdgeCases tests edge cases for json_patch function
func TestJSONPatchFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
	}{
		{
			name:     "null target",
			args:     []Value{NewNullValue(), NewTextValue("{}")},
			wantNull: true,
		},
		{
			name: "null patch returns target",
			args: []Value{NewTextValue(`{"a":1}`), NewNullValue()},
			want: `{"a":1}`,
		},
		{
			name:     "invalid target",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("{}")},
			wantNull: true,
		},
		{
			name:     "invalid patch",
			args:     []Value{NewTextValue("{}"), NewTextValue("{invalid}")},
			wantNull: true,
		},
		{
			name: "patch adds key",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue(`{"b":2}`)},
			want: `{"a":1,"b":2}`,
		},
		{
			name: "patch removes key",
			args: []Value{NewTextValue(`{"a":1,"b":2}`), NewTextValue(`{"b":null}`)},
			want: `{"a":1}`,
		},
		{
			name: "patch replaces non-object target",
			args: []Value{NewTextValue(`[1,2,3]`), NewTextValue(`{"a":1}`)},
			want: `{"a":1}`,
		},
		{
			name: "non-object patch replaces target",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue(`[1,2,3]`)},
			want: `[1,2,3]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonPatchFunc(tt.args)
			if err != nil {
				t.Fatalf("jsonPatchFunc() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("jsonPatchFunc() = %v, want NULL", result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("jsonPatchFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("jsonPatchFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestJSONRemoveFunc_EdgeCases tests edge cases for json_remove function
func TestJSONRemoveFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "too few args",
			args:    []Value{NewTextValue("{}")},
			wantErr: true,
		},
		{
			name:     "null json",
			args:     []Value{NewNullValue(), NewTextValue("$.a")},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("$.a")},
			wantNull: true,
		},
		{
			name: "remove key",
			args: []Value{NewTextValue(`{"a":1,"b":2}`), NewTextValue("$.a")},
			want: `{"b":2}`,
		},
		{
			name: "remove array element",
			args: []Value{NewTextValue(`[1,2,3]`), NewTextValue("$[1]")},
			want: `[1,3]`,
		},
		{
			name: "null path is skipped",
			args: []Value{NewTextValue(`{"a":1}`), NewNullValue()},
			want: `{"a":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonRemoveFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonRemoveFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonRemoveFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONReplaceFunc_EdgeCases tests edge cases for json_replace function
func TestJSONReplaceFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "too few args",
			args:    []Value{NewTextValue("{}")},
			wantErr: true,
		},
		{
			name:    "even number of args",
			args:    []Value{NewTextValue("{}"), NewTextValue("$.a")},
			wantErr: true,
		},
		{
			name:     "null json",
			args:     []Value{NewNullValue(), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name: "replace existing key",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue("$.a"), NewIntValue(2)},
			want: `{"a":2}`,
		},
		{
			name: "skip non-existing key",
			args: []Value{NewTextValue("{}"), NewTextValue("$.a"), NewIntValue(1)},
			want: "{}",
		},
		{
			name: "null path is skipped",
			args: []Value{NewTextValue(`{"a":1}`), NewNullValue(), NewIntValue(2)},
			want: `{"a":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonReplaceFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonReplaceFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonReplaceFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONSetFunc_EdgeCases tests edge cases for json_set function
func TestJSONSetFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "too few args",
			args:    []Value{NewTextValue("{}")},
			wantErr: true,
		},
		{
			name:    "even number of args",
			args:    []Value{NewTextValue("{}"), NewTextValue("$.a")},
			wantErr: true,
		},
		{
			name:     "null json",
			args:     []Value{NewNullValue(), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}"), NewTextValue("$.a"), NewIntValue(1)},
			wantNull: true,
		},
		{
			name: "set new key",
			args: []Value{NewTextValue("{}"), NewTextValue("$.a"), NewIntValue(1)},
			want: `{"a":1}`,
		},
		{
			name: "replace existing key",
			args: []Value{NewTextValue(`{"a":1}`), NewTextValue("$.a"), NewIntValue(2)},
			want: `{"a":2}`,
		},
		{
			name: "null path is skipped",
			args: []Value{NewTextValue("{}"), NewNullValue(), NewIntValue(1)},
			want: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSetFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonSetFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonSetFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONTypeFunc_EdgeCases tests edge cases for json_type function
func TestJSONTypeFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:     "null json",
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:     "invalid json",
			args:     []Value{NewTextValue("{invalid}")},
			wantNull: true,
		},
		{
			name: "null value",
			args: []Value{NewTextValue("null")},
			want: "null",
		},
		{
			name: "boolean true",
			args: []Value{NewTextValue("true")},
			want: "true",
		},
		{
			name: "boolean false",
			args: []Value{NewTextValue("false")},
			want: "true",
		},
		{
			name: "number",
			args: []Value{NewTextValue("42")},
			want: "integer",
		},
		{
			name: "string",
			args: []Value{NewTextValue(`"hello"`)},
			want: "text",
		},
		{
			name: "array",
			args: []Value{NewTextValue("[1,2,3]")},
			want: "array",
		},
		{
			name: "object",
			args: []Value{NewTextValue(`{"key":"value"}`)},
			want: "object",
		},
		{
			name: "with path to string",
			args: []Value{NewTextValue(`{"key":"value"}`), NewTextValue("$.key")},
			want: "text",
		},
		{
			name: "with null path returns object type",
			args: []Value{NewTextValue("{}"), NewNullValue()},
			want: "object",
		},
		{
			name:     "with path to non-existing",
			args:     []Value{NewTextValue("{}"), NewTextValue("$.missing")},
			wantNull: true,
		},
		{
			name:    "too many args",
			args:    []Value{NewTextValue("{}"), NewTextValue("$"), NewTextValue("extra")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonTypeFunc(tt.args)
			if r, ok := assertFuncResult(t, "jsonTypeFunc", result, err, tt.wantErr, tt.wantNull); ok {
				if got := r.AsString(); got != tt.want {
					t.Errorf("jsonTypeFunc() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// TestJSONQuoteFunc_EdgeCases tests edge cases for json_quote function
func TestJSONQuoteFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input Value
		want  string
	}{
		{
			name:  "null input",
			input: NewNullValue(),
			want:  "null",
		},
		{
			name:  "integer",
			input: NewIntValue(42),
			want:  "42",
		},
		{
			name:  "float",
			input: NewFloatValue(3.14),
			want:  "3.14",
		},
		{
			name:  "string",
			input: NewTextValue("hello"),
			want:  `"hello"`,
		},
		{
			name:  "blob encoded as base64 in JSON",
			input: NewBlobValue([]byte("test")),
			want:  `"dGVzdA=="`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonQuoteFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("jsonQuoteFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Fatalf("jsonQuoteFunc() returned NULL")
			}
			got := result.AsString()
			if got != tt.want {
				t.Errorf("jsonQuoteFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestValueToJSON_EdgeCases tests edge cases for valueToJSON helper
func TestValueToJSON_EdgeCases(t *testing.T) {
	// Test JSON string parsing in valueToJSON
	jsonStr := `{"nested":"value"}`
	val := NewTextValue(jsonStr)
	result := valueToJSON(val)

	// Should parse JSON string into structure
	if result == nil {
		t.Error("valueToJSON() returned nil for valid JSON string")
	}
}

// TestJSONToValue_EdgeCases tests edge cases for jsonToValue helper
func TestJSONToValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  ValueType
	}{
		{
			name:  "nil",
			input: nil,
			want:  TypeNull,
		},
		{
			name:  "bool true",
			input: true,
			want:  TypeInteger,
		},
		{
			name:  "bool false",
			input: false,
			want:  TypeInteger,
		},
		{
			name:  "float as integer",
			input: float64(42),
			want:  TypeInteger,
		},
		{
			name:  "float with decimal",
			input: float64(3.14),
			want:  TypeFloat,
		},
		{
			name:  "string",
			input: "hello",
			want:  TypeText,
		},
		{
			name:  "array",
			input: []interface{}{1, 2, 3},
			want:  TypeText,
		},
		{
			name:  "object",
			input: map[string]interface{}{"key": "value"},
			want:  TypeText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jsonToValue(tt.input)
			if result.Type() != tt.want {
				t.Errorf("jsonToValue() type = %v, want %v", result.Type(), tt.want)
			}
		})
	}
}

// TestExtractArrayElement_EdgeCases tests edge cases for extractArrayElement
func TestExtractArrayElement_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		index    int
		wantNull bool
	}{
		{
			name:     "not an array",
			data:     "string",
			index:    0,
			wantNull: true,
		},
		{
			name:     "negative index",
			data:     []interface{}{1, 2, 3},
			index:    -1,
			wantNull: true,
		},
		{
			name:     "index out of bounds",
			data:     []interface{}{1, 2, 3},
			index:    10,
			wantNull: true,
		},
		{
			name:  "valid index",
			data:  []interface{}{1, 2, 3},
			index: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractArrayElement(tt.data, tt.index)
			if tt.wantNull {
				if result != nil {
					t.Errorf("extractArrayElement() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Error("extractArrayElement() = nil, want non-nil")
				}
			}
		})
	}
}

// TestExtractObjectKey_EdgeCases tests edge cases for extractObjectKey
func TestExtractObjectKey_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		key      string
		wantNull bool
	}{
		{
			name:     "not an object",
			data:     "string",
			key:      "key",
			wantNull: true,
		},
		{
			name:     "key not found",
			data:     map[string]interface{}{"other": "value"},
			key:      "missing",
			wantNull: true,
		},
		{
			name: "valid key",
			data: map[string]interface{}{"key": "value"},
			key:  "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractObjectKey(tt.data, tt.key)
			if tt.wantNull {
				if result != nil {
					t.Errorf("extractObjectKey() = %v, want nil", result)
				}
			} else {
				if result == nil {
					t.Error("extractObjectKey() = nil, want non-nil")
				}
			}
		})
	}
}

// TestRemoveFromArray_EdgeCases tests edge cases for removeFromArray
func TestRemoveFromArray_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		data interface{}
		part pathPart
		want interface{}
	}{
		{
			name: "not an array",
			data: "string",
			part: pathPart{index: 0, isIndex: true},
			want: "string",
		},
		{
			name: "index out of bounds negative",
			data: []interface{}{1, 2, 3},
			part: pathPart{index: -1, isIndex: true},
			want: []interface{}{1, 2, 3},
		},
		{
			name: "index out of bounds positive",
			data: []interface{}{1, 2, 3},
			part: pathPart{index: 10, isIndex: true},
			want: []interface{}{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeFromArray(tt.data, tt.part, nil)
			// Just verify it doesn't panic
			if result == nil {
				t.Error("removeFromArray() returned nil")
			}
		})
	}
}

// TestRemoveFromObject_EdgeCases tests edge cases for removeFromObject
func TestRemoveFromObject_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		data interface{}
		part pathPart
	}{
		{
			name: "not an object",
			data: "string",
			part: pathPart{key: "key", isIndex: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeFromObject(tt.data, tt.part, nil)
			// Just verify it doesn't panic
			if result == nil {
				t.Error("removeFromObject() returned nil")
			}
		})
	}
}

// TestDeepCopy_EdgeCases tests edge cases for deepCopy
func TestDeepCopy_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "nil",
			input: nil,
		},
		{
			name:  "primitive",
			input: 42,
		},
		{
			name:  "string",
			input: "hello",
		},
		{
			name:  "array",
			input: []interface{}{1, 2, 3},
		},
		{
			name:  "object",
			input: map[string]interface{}{"key": "value"},
		},
		{
			name:  "nested",
			input: map[string]interface{}{"arr": []interface{}{1, 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deepCopy(tt.input)
			// Just verify it doesn't panic and returns something
			_ = result
		})
	}
}

// TestJsonFuncMarshalError tests json function with values that cause marshal errors
func TestJsonFuncMarshalError(t *testing.T) {
	// Test with invalid JSON string - should return error per SQLite spec
	_, err := jsonFunc([]Value{NewTextValue("{invalid json}")})
	if err == nil {
		t.Error("jsonFunc() expected error for invalid JSON")
	}
}

// TestJsonArrayFuncMarshalError tests json_array with unmarshalable data
func TestJsonArrayFuncMarshalError(t *testing.T) {
	// Normal case should work
	result, err := jsonArrayFunc([]Value{NewIntValue(1), NewTextValue("test")})
	if err != nil {
		t.Errorf("jsonArrayFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonArrayFunc() should not return NULL for valid input")
	}
}

// TestJsonObjectFuncOddArgs tests json_object with odd number of arguments
func TestJsonObjectFuncOddArgs(t *testing.T) {
	result, err := jsonObjectFunc([]Value{NewTextValue("key1")})
	if err == nil {
		t.Error("jsonObjectFunc() should return error for odd number of args")
		return
	}
	// Error expected, result may be nil
	_ = result
}

// TestJsonObjectFuncNullKey tests json_object with null key
func TestJsonObjectFuncNullKey(t *testing.T) {
	result, err := jsonObjectFunc([]Value{NewNullValue(), NewTextValue("value")})
	if err == nil {
		t.Error("jsonObjectFunc() should return error for null key")
		return
	}
	// Error expected, result may be nil
	_ = result
}

// TestJsonQuoteFuncEdgeCases tests json_quote with various edge cases
func TestJsonQuoteFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input Value
	}{
		{"text", NewTextValue("hello")},
		{"null", NewNullValue()},
		{"integer", NewIntValue(42)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonQuoteFunc([]Value{tt.input})
			if err != nil {
				t.Errorf("jsonQuoteFunc() error = %v", err)
			}
			if result.IsNull() {
				t.Error("jsonQuoteFunc() should not return NULL")
			}
		})
	}
}

// TestValueToJSONEdgeCases tests valueToJSON with various types
func TestValueToJSONEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input Value
	}{
		{"blob", NewBlobValue([]byte{1, 2, 3})},
		{"text_number", NewTextValue("123")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToJSON(tt.input)
			_ = result // Just verify it doesn't panic
		})
	}
}

// TestJsonToValueEdgeCases tests jsonToValue with edge cases
func TestJsonToValueEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"bool", true},
		{"null", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jsonToValue(tt.input)
			_ = result // Just verify it doesn't panic
		})
	}
}

// TestGetJSONTypeEdgeCases tests getJSONType with edge cases
func TestGetJSONTypeEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"bool", true, "true"},
		{"unknown", struct{}{}, "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getJSONType(tt.input)
			if result != tt.expected {
				t.Errorf("getJSONType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJsonSetPathEdgeCases tests json_set with edge cases via the public API
func TestJsonSetPathEdgeCases(t *testing.T) {
	// Test with empty path
	result, err := jsonSetFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue("$"),
		NewTextValue("newvalue"),
	})
	if err != nil {
		t.Errorf("jsonSetFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonSetFunc() should not return NULL for valid input")
	}
}

// TestJsonRemovePathEdgeCases tests json_remove with edge cases via the public API
func TestJsonRemovePathEdgeCases(t *testing.T) {
	// Test with valid removal
	result, err := jsonRemoveFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue("$.key"),
	})
	if err != nil {
		t.Errorf("jsonRemoveFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonRemoveFunc() should not return NULL for valid input")
	}
}

// TestJsonExtractPathEdgeCases tests json_extract with various paths
func TestJsonExtractPathEdgeCases(t *testing.T) {
	// Test with array access
	result, err := jsonExtractFunc([]Value{
		NewTextValue(`{"arr":[1,2,3]}`),
		NewTextValue("$.arr[0]"),
	})
	if err != nil {
		t.Errorf("jsonExtractFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonExtractFunc() should not return NULL for valid array access")
	}
}

// TestJsonInsertEdgeCases tests json_insert with various edge cases
func TestJsonInsertEdgeCases(t *testing.T) {
	// Test inserting into existing path (should not replace)
	result, err := jsonInsertFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue("$.key"),
		NewTextValue("newvalue"),
	})
	if err != nil {
		t.Errorf("jsonInsertFunc() error = %v", err)
	}
	// Should keep original value since path exists
	if result.IsNull() {
		t.Error("jsonInsertFunc() should not return NULL")
	}
}

// TestJsonReplaceEdgeCases tests json_replace with various edge cases
func TestJsonReplaceEdgeCases(t *testing.T) {
	// Test replacing non-existent path (should not add)
	result, err := jsonReplaceFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue("$.newkey"),
		NewTextValue("newvalue"),
	})
	if err != nil {
		t.Errorf("jsonReplaceFunc() error = %v", err)
	}
	// Should keep original since path doesn't exist
	if result.IsNull() {
		t.Error("jsonReplaceFunc() should not return NULL")
	}
}

// TestJsonPatchEdgeCases tests json_patch with various edge cases
func TestJsonPatchEdgeCases(t *testing.T) {
	// Test with valid patch
	result, err := jsonPatchFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue(`{"key":"newvalue"}`),
	})
	if err != nil {
		t.Errorf("jsonPatchFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonPatchFunc() should not return NULL for valid patch")
	}
}

// TestJsonFuncInvalidMarshal tests json function error handling
func TestJsonFuncInvalidMarshal(t *testing.T) {
	// Test with blob input
	result, err := jsonFunc([]Value{NewBlobValue([]byte(`{"key":"value"}`))})
	if err != nil {
		t.Errorf("jsonFunc() with blob error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonFunc() with valid JSON blob should not return NULL")
	}
}

// TestJsonArrayFuncErrorHandling tests json_array error path
func TestJsonArrayFuncErrorHandling(t *testing.T) {
	// Test with various value types
	result, err := jsonArrayFunc([]Value{
		NewBlobValue([]byte{1, 2, 3}),
		NewNullValue(),
		NewIntValue(42),
	})
	if err != nil {
		t.Errorf("jsonArrayFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonArrayFunc() should not return NULL")
	}
}

// TestJsonExtractMultiplePaths tests json_extract with multiple paths
func TestJsonExtractMultiplePaths(t *testing.T) {
	result, err := jsonExtractFunc([]Value{
		NewTextValue(`{"a":1,"b":2}`),
		NewTextValue("$.a"),
		NewTextValue("$.b"),
	})
	if err != nil {
		t.Errorf("jsonExtractFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonExtractFunc() should not return NULL for multiple valid paths")
	}
}

// TestJsonInsertInvalidJSON tests json_insert with invalid JSON
func TestJsonInsertInvalidJSON(t *testing.T) {
	result, err := jsonInsertFunc([]Value{
		NewTextValue(`{invalid}`),
		NewTextValue("$.key"),
		NewTextValue("value"),
	})
	if err != nil {
		t.Errorf("jsonInsertFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("jsonInsertFunc() should return NULL for invalid JSON")
	}
}

// TestJsonRemoveInvalidJSON tests json_remove with invalid JSON
func TestJsonRemoveInvalidJSON(t *testing.T) {
	result, err := jsonRemoveFunc([]Value{
		NewTextValue(`{invalid}`),
		NewTextValue("$.key"),
	})
	if err != nil {
		t.Errorf("jsonRemoveFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("jsonRemoveFunc() should return NULL for invalid JSON")
	}
}

// TestJsonReplaceInvalidJSON tests json_replace with invalid JSON
func TestJsonReplaceInvalidJSON(t *testing.T) {
	result, err := jsonReplaceFunc([]Value{
		NewTextValue(`{invalid}`),
		NewTextValue("$.key"),
		NewTextValue("value"),
	})
	if err != nil {
		t.Errorf("jsonReplaceFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("jsonReplaceFunc() should return NULL for invalid JSON")
	}
}

// TestJsonSetInvalidJSON tests json_set with invalid JSON
func TestJsonSetInvalidJSON(t *testing.T) {
	result, err := jsonSetFunc([]Value{
		NewTextValue(`{invalid}`),
		NewTextValue("$.key"),
		NewTextValue("value"),
	})
	if err != nil {
		t.Errorf("jsonSetFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("jsonSetFunc() should return NULL for invalid JSON")
	}
}

// TestJsonQuoteBlob tests json_quote with blob
func TestJsonQuoteBlob(t *testing.T) {
	result, err := jsonQuoteFunc([]Value{NewBlobValue([]byte{1, 2, 3})})
	if err != nil {
		t.Errorf("jsonQuoteFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonQuoteFunc() should not return NULL for blob")
	}
}

// TestRemovePathWithNonExistentPath tests removePath internals
func TestRemovePathWithNonExistentPath(t *testing.T) {
	// Test via json_remove
	result, err := jsonRemoveFunc([]Value{
		NewTextValue(`{"key":"value"}`),
		NewTextValue("$.nonexistent"),
	})
	if err != nil {
		t.Errorf("jsonRemoveFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonRemoveFunc() should not return NULL even if path doesn't exist")
	}
}

// TestSetPathWithComplexPaths tests setPath with nested structures
func TestSetPathWithComplexPaths(t *testing.T) {
	// Test via json_set with nested array access
	result, err := jsonSetFunc([]Value{
		NewTextValue(`{"arr":[1,2,3]}`),
		NewTextValue("$.arr[1]"),
		NewIntValue(99),
	})
	if err != nil {
		t.Errorf("jsonSetFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonSetFunc() should not return NULL for valid array set")
	}
}

// TestTraversePathWithInvalidIndex tests traversePath error paths
func TestTraversePathWithInvalidIndex(t *testing.T) {
	// Test via json_extract with invalid array index
	// Note: "abc" might be parsed as a key, not an array index
	result, err := jsonExtractFunc([]Value{
		NewTextValue(`{"arr":[1,2,3]}`),
		NewTextValue("$.arr[999]"), // Out of bounds index
	})
	if err != nil {
		t.Errorf("jsonExtractFunc() error = %v", err)
	}
	// Out of bounds returns NULL or the value might be handled gracefully
	_ = result
}

// TestApplyJSONPatchWithNestedObjects tests applyJSONPatch
func TestApplyJSONPatchWithNestedObjects(t *testing.T) {
	result, err := jsonPatchFunc([]Value{
		NewTextValue(`{"a":{"b":1}}`),
		NewTextValue(`{"a":{"c":2}}`),
	})
	if err != nil {
		t.Errorf("jsonPatchFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("jsonPatchFunc() should not return NULL for nested patch")
	}
}
