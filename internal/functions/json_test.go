// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"encoding/json"
	"testing"
)

// Test json() - Validate and minify JSON
func TestJSONFunc(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		expected string
		isNull   bool
	}{
		{
			name:     "valid object",
			input:    NewTextValue(`{"name": "John", "age": 30}`),
			expected: `{"age":30,"name":"John"}`,
		},
		{
			name:     "valid array",
			input:    NewTextValue(`[1, 2, 3]`),
			expected: `[1,2,3]`,
		},
		{
			name:     "with whitespace",
			input:    NewTextValue(`  {  "x"  :  1  }  `),
			expected: `{"x":1}`,
		},
		{
			name:   "invalid JSON",
			input:  NewTextValue(`{invalid}`),
			isNull: true,
		},
		{
			name:   "NULL input",
			input:  NewNullValue(),
			isNull: true,
		},
		{
			name:     "nested object",
			input:    NewTextValue(`{"a":{"b":{"c":1}}}`),
			expected: `{"a":{"b":{"c":1}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.isNull {
				if !result.IsNull() {
					t.Errorf("expected NULL, got %v", result.AsString())
				}
			} else {
				if result.IsNull() {
					t.Errorf("expected %s, got NULL", tt.expected)
				} else if result.AsString() != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result.AsString())
				}
			}
		})
	}
}

// Test json_array() - Create JSON array
func TestJSONArrayFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
	}{
		{
			name:     "empty array",
			inputs:   []Value{},
			expected: `[]`,
		},
		{
			name:     "single value",
			inputs:   []Value{NewIntValue(1)},
			expected: `[1]`,
		},
		{
			name:     "multiple values",
			inputs:   []Value{NewIntValue(1), NewTextValue("hello"), NewFloatValue(3.14)},
			expected: `[1,"hello",3.14]`,
		},
		{
			name:     "with NULL",
			inputs:   []Value{NewIntValue(1), NewNullValue(), NewIntValue(3)},
			expected: `[1,null,3]`,
		},
		{
			name:     "nested JSON",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue(`[1,2,3]`)},
			expected: `[{"x":1},[1,2,3]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_array_length() - Get array length
func TestJSONArrayLengthFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected int64
		isNull   bool
	}{
		{
			name:     "simple array",
			inputs:   []Value{NewTextValue(`[1,2,3]`)},
			expected: 3,
		},
		{
			name:     "empty array",
			inputs:   []Value{NewTextValue(`[]`)},
			expected: 0,
		},
		{
			name:   "not an array",
			inputs: []Value{NewTextValue(`{"x":1}`)},
			isNull: true,
		},
		{
			name:     "nested array with path",
			inputs:   []Value{NewTextValue(`{"a":[1,2,3]}`), NewTextValue("$.a")},
			expected: 3,
		},
		{
			name:   "invalid path",
			inputs: []Value{NewTextValue(`{"a":[1,2,3]}`), NewTextValue("$.b")},
			isNull: true,
		},
		{
			name:   "NULL input",
			inputs: []Value{NewNullValue()},
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayLengthFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.isNull {
				if !result.IsNull() {
					t.Errorf("expected NULL, got %d", result.AsInt64())
				}
			} else {
				if result.AsInt64() != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, result.AsInt64())
				}
			}
		})
	}
}

// Test json_extract() - Extract value from path
func TestJSONExtractFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
		isNull   bool
	}{
		{
			name:     "extract root",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$")},
			expected: `{"x":1}`,
		},
		{
			name:     "extract key",
			inputs:   []Value{NewTextValue(`{"x":1,"y":2}`), NewTextValue("$.x")},
			expected: `1`,
		},
		{
			name:     "extract nested",
			inputs:   []Value{NewTextValue(`{"a":{"b":{"c":1}}}`), NewTextValue("$.a.b.c")},
			expected: `1`,
		},
		{
			name:     "extract array element",
			inputs:   []Value{NewTextValue(`[1,2,3]`), NewTextValue("$[1]")},
			expected: `2`,
		},
		{
			name:     "extract nested array",
			inputs:   []Value{NewTextValue(`{"a":[1,2,3]}`), NewTextValue("$.a[2]")},
			expected: `3`,
		},
		{
			name:   "non-existent path",
			inputs: []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y")},
			isNull: true,
		},
		{
			name:     "multiple paths",
			inputs:   []Value{NewTextValue(`{"x":1,"y":2}`), NewTextValue("$.x"), NewTextValue("$.y")},
			expected: `[1,2]`,
		},
		{
			name:     "extract string",
			inputs:   []Value{NewTextValue(`{"name":"John"}`), NewTextValue("$.name")},
			expected: `John`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonExtractFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.isNull {
				if !result.IsNull() {
					t.Errorf("expected NULL, got %v", result.AsString())
				}
			} else {
				if result.AsString() != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result.AsString())
				}
			}
		})
	}
}

// Test json_insert() - Insert value at path
func TestJSONInsertFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
	}{
		{
			name:     "insert new key",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y"), NewIntValue(2)},
			expected: `{"x":1,"y":2}`,
		},
		{
			name:     "don't replace existing",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.x"), NewIntValue(2)},
			expected: `{"x":1}`,
		},
		{
			name:     "insert nested",
			inputs:   []Value{NewTextValue(`{"a":{}}`), NewTextValue("$.a.b"), NewIntValue(1)},
			expected: `{"a":{"b":1}}`,
		},
		{
			name:     "multiple inserts",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y"), NewIntValue(2), NewTextValue("$.z"), NewIntValue(3)},
			expected: `{"x":1,"y":2,"z":3}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonInsertFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_object() - Create JSON object
func TestJSONObjectFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
		hasError bool
	}{
		{
			name:     "empty object",
			inputs:   []Value{},
			expected: `{}`,
		},
		{
			name:     "single pair",
			inputs:   []Value{NewTextValue("x"), NewIntValue(1)},
			expected: `{"x":1}`,
		},
		{
			name:     "multiple pairs",
			inputs:   []Value{NewTextValue("x"), NewIntValue(1), NewTextValue("y"), NewTextValue("hello")},
			expected: `{"x":1,"y":"hello"}`,
		},
		{
			name:     "with NULL value",
			inputs:   []Value{NewTextValue("x"), NewNullValue()},
			expected: `{"x":null}`,
		},
		{
			name:     "NULL key error",
			inputs:   []Value{NewNullValue(), NewIntValue(1)},
			hasError: true,
		},
		{
			name:     "odd args error",
			inputs:   []Value{NewTextValue("x")},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonObjectFunc(tt.inputs)

			if tt.hasError {
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_patch() - Apply JSON patch
func TestJSONPatchFunc(t *testing.T) {
	tests := []struct {
		name     string
		target   Value
		patch    Value
		expected string
	}{
		{
			name:     "add field",
			target:   NewTextValue(`{"x":1}`),
			patch:    NewTextValue(`{"y":2}`),
			expected: `{"x":1,"y":2}`,
		},
		{
			name:     "replace field",
			target:   NewTextValue(`{"x":1}`),
			patch:    NewTextValue(`{"x":2}`),
			expected: `{"x":2}`,
		},
		{
			name:     "delete field",
			target:   NewTextValue(`{"x":1,"y":2}`),
			patch:    NewTextValue(`{"y":null}`),
			expected: `{"x":1}`,
		},
		{
			name:     "nested merge",
			target:   NewTextValue(`{"a":{"b":1,"c":2}}`),
			patch:    NewTextValue(`{"a":{"b":3}}`),
			expected: `{"a":{"b":3,"c":2}}`,
		},
		{
			name:     "NULL patch returns original",
			target:   NewTextValue(`{"x":1}`),
			patch:    NewNullValue(),
			expected: `{"x":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonPatchFunc([]Value{tt.target, tt.patch})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_remove() - Remove path
func TestJSONRemoveFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
	}{
		{
			name:     "remove key",
			inputs:   []Value{NewTextValue(`{"x":1,"y":2}`), NewTextValue("$.y")},
			expected: `{"x":1}`,
		},
		{
			name:     "remove nested",
			inputs:   []Value{NewTextValue(`{"a":{"b":1,"c":2}}`), NewTextValue("$.a.b")},
			expected: `{"a":{"c":2}}`,
		},
		{
			name:     "remove array element",
			inputs:   []Value{NewTextValue(`[1,2,3]`), NewTextValue("$[1]")},
			expected: `[1,3]`,
		},
		{
			name:     "remove multiple",
			inputs:   []Value{NewTextValue(`{"x":1,"y":2,"z":3}`), NewTextValue("$.x"), NewTextValue("$.z")},
			expected: `{"y":2}`,
		},
		{
			name:     "remove non-existent",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y")},
			expected: `{"x":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonRemoveFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_replace() - Replace at path
func TestJSONReplaceFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
	}{
		{
			name:     "replace existing",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.x"), NewIntValue(2)},
			expected: `{"x":2}`,
		},
		{
			name:     "don't insert new",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y"), NewIntValue(2)},
			expected: `{"x":1}`,
		},
		{
			name:     "replace nested",
			inputs:   []Value{NewTextValue(`{"a":{"b":1}}`), NewTextValue("$.a.b"), NewIntValue(2)},
			expected: `{"a":{"b":2}}`,
		},
		{
			name:     "replace array element",
			inputs:   []Value{NewTextValue(`[1,2,3]`), NewTextValue("$[1]"), NewIntValue(99)},
			expected: `[1,99,3]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonReplaceFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_set() - Set value at path
func TestJSONSetFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
	}{
		{
			name:     "set existing",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.x"), NewIntValue(2)},
			expected: `{"x":2}`,
		},
		{
			name:     "set new",
			inputs:   []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y"), NewIntValue(2)},
			expected: `{"x":1,"y":2}`,
		},
		{
			name:     "set nested new",
			inputs:   []Value{NewTextValue(`{}`), NewTextValue("$.a.b.c"), NewIntValue(1)},
			expected: `{"a":{"b":{"c":1}}}`,
		},
		{
			name:     "set array element",
			inputs:   []Value{NewTextValue(`[1,2,3]`), NewTextValue("$[1]"), NewIntValue(99)},
			expected: `[1,99,3]`,
		},
		{
			name:     "multiple sets",
			inputs:   []Value{NewTextValue(`{}`), NewTextValue("$.x"), NewIntValue(1), NewTextValue("$.y"), NewIntValue(2)},
			expected: `{"x":1,"y":2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSetFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test json_type() - Get JSON type
func TestJSONTypeFunc(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []Value
		expected string
		isNull   bool
	}{
		{
			name:     "object type",
			inputs:   []Value{NewTextValue(`{"x":1}`)},
			expected: "object",
		},
		{
			name:     "array type",
			inputs:   []Value{NewTextValue(`[1,2,3]`)},
			expected: "array",
		},
		{
			name:     "text type",
			inputs:   []Value{NewTextValue(`"hello"`)},
			expected: "text",
		},
		{
			name:     "integer type",
			inputs:   []Value{NewTextValue(`42`)},
			expected: "integer",
		},
		{
			name:     "null type",
			inputs:   []Value{NewTextValue(`null`)},
			expected: "null",
		},
		{
			name:     "type with path",
			inputs:   []Value{NewTextValue(`{"x":[1,2,3]}`), NewTextValue("$.x")},
			expected: "array",
		},
		{
			name:   "non-existent path",
			inputs: []Value{NewTextValue(`{"x":1}`), NewTextValue("$.y")},
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonTypeFunc(tt.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.isNull {
				if !result.IsNull() {
					t.Errorf("expected NULL, got %v", result.AsString())
				}
			} else {
				if result.AsString() != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result.AsString())
				}
			}
		})
	}
}

// Test json_valid() - Check if valid JSON
func TestJSONValidFunc(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		expected int64
	}{
		{
			name:     "valid object",
			input:    NewTextValue(`{"x":1}`),
			expected: 1,
		},
		{
			name:     "valid array",
			input:    NewTextValue(`[1,2,3]`),
			expected: 1,
		},
		{
			name:     "valid string",
			input:    NewTextValue(`"hello"`),
			expected: 1,
		},
		{
			name:     "valid number",
			input:    NewTextValue(`42`),
			expected: 1,
		},
		{
			name:     "valid null",
			input:    NewTextValue(`null`),
			expected: 1,
		},
		{
			name:     "invalid JSON",
			input:    NewTextValue(`{invalid}`),
			expected: 0,
		},
		{
			name:     "empty string",
			input:    NewTextValue(``),
			expected: 0,
		},
		{
			name:     "NULL input",
			input:    NewNullValue(),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonValidFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsInt64() != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result.AsInt64())
			}
		})
	}
}

// Test json_quote() - Quote value as JSON string
func TestJSONQuoteFunc(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		expected string
	}{
		{
			name:     "quote string",
			input:    NewTextValue("hello"),
			expected: `"hello"`,
		},
		{
			name:     "quote number",
			input:    NewIntValue(42),
			expected: `42`,
		},
		{
			name:     "quote float",
			input:    NewFloatValue(3.14),
			expected: `3.14`,
		},
		{
			name:     "quote NULL",
			input:    NewNullValue(),
			expected: `null`,
		},
		{
			name:     "quote with escapes",
			input:    NewTextValue(`hello"world`),
			expected: `"hello\"world"`,
		},
		{
			name:     "quote object",
			input:    NewTextValue(`{"x":1}`),
			expected: `{"x":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonQuoteFunc([]Value{tt.input})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.AsString() != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result.AsString())
			}
		})
	}
}

// Test path parsing
func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected []pathPart
	}{
		{
			name: "simple key",
			path: ".x",
			expected: []pathPart{
				{key: "x", isIndex: false},
			},
		},
		{
			name: "nested keys",
			path: ".a.b.c",
			expected: []pathPart{
				{key: "a", isIndex: false},
				{key: "b", isIndex: false},
				{key: "c", isIndex: false},
			},
		},
		{
			name: "array index",
			path: "[0]",
			expected: []pathPart{
				{index: 0, isIndex: true},
			},
		},
		{
			name: "key with array",
			path: ".a[0]",
			expected: []pathPart{
				{key: "a", isIndex: false},
				{index: 0, isIndex: true},
			},
		},
		{
			name: "complex path",
			path: ".a.b[2].c",
			expected: []pathPart{
				{key: "a", isIndex: false},
				{key: "b", isIndex: false},
				{index: 2, isIndex: true},
				{key: "c", isIndex: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePath(tt.path)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d parts, got %d", len(tt.expected), len(result))
				return
			}

			for i, part := range result {
				exp := tt.expected[i]
				if part.isIndex != exp.isIndex {
					t.Errorf("part %d: expected isIndex=%v, got %v", i, exp.isIndex, part.isIndex)
				}
				if part.isIndex {
					if part.index != exp.index {
						t.Errorf("part %d: expected index=%d, got %d", i, exp.index, part.index)
					}
				} else {
					if part.key != exp.key {
						t.Errorf("part %d: expected key=%s, got %s", i, exp.key, part.key)
					}
				}
			}
		})
	}
}

// Test value to JSON conversion
func TestValueToJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    Value
		expected interface{}
	}{
		{
			name:     "integer",
			input:    NewIntValue(42),
			expected: int64(42),
		},
		{
			name:     "float",
			input:    NewFloatValue(3.14),
			expected: 3.14,
		},
		{
			name:     "text",
			input:    NewTextValue("hello"),
			expected: "hello",
		},
		{
			name:     "null",
			input:    NewNullValue(),
			expected: nil,
		},
		{
			name:     "JSON object as text",
			input:    NewTextValue(`{"x":1}`),
			expected: map[string]interface{}{"x": float64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToJSON(tt.input)

			// Compare based on type
			switch exp := tt.expected.(type) {
			case nil:
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
			case int64:
				if r, ok := result.(int64); !ok || r != exp {
					t.Errorf("expected %d, got %v", exp, result)
				}
			case float64:
				if r, ok := result.(float64); !ok || r != exp {
					t.Errorf("expected %f, got %v", exp, result)
				}
			case string:
				if r, ok := result.(string); !ok || r != exp {
					t.Errorf("expected %s, got %v", exp, result)
				}
			}
		})
	}
}

// Test JSON functions registration
func TestJSONFunctionsRegistration(t *testing.T) {
	r := NewRegistry()
	RegisterJSONFunctions(r)

	expectedFunctions := []string{
		"json",
		"json_array",
		"json_array_length",
		"json_extract",
		"json_insert",
		"json_object",
		"json_patch",
		"json_remove",
		"json_replace",
		"json_set",
		"json_type",
		"json_valid",
		"json_quote",
	}

	for _, name := range expectedFunctions {
		if _, ok := r.Lookup(name); !ok {
			t.Errorf("function %s not registered", name)
		}
	}
}

// Test edge cases
func TestJSONEdgeCases(t *testing.T) {
	t.Run("deep nesting", func(t *testing.T) {
		deep := `{"a":{"b":{"c":{"d":{"e":{"f":1}}}}}}`
		result, err := jsonExtractFunc([]Value{
			NewTextValue(deep),
			NewTextValue("$.a.b.c.d.e.f"),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.AsString() != "1" {
			t.Errorf("expected 1, got %s", result.AsString())
		}
	})

	t.Run("large array", func(t *testing.T) {
		arr := make([]Value, 100)
		for i := range arr {
			arr[i] = NewIntValue(int64(i))
		}
		result, err := jsonArrayFunc(arr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify it's a valid JSON array
		var parsed []interface{}
		if err := json.Unmarshal([]byte(result.AsString()), &parsed); err != nil {
			t.Errorf("result is not valid JSON: %v", err)
		}
		if len(parsed) != 100 {
			t.Errorf("expected 100 elements, got %d", len(parsed))
		}
	})

	t.Run("unicode handling", func(t *testing.T) {
		input := NewTextValue("你好世界")
		result, err := jsonQuoteFunc([]Value{input})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify it's valid JSON
		var parsed string
		if err := json.Unmarshal([]byte(result.AsString()), &parsed); err != nil {
			t.Errorf("result is not valid JSON: %v", err)
		}
		if parsed != "你好世界" {
			t.Errorf("unicode not preserved: got %s", parsed)
		}
	})

	t.Run("empty object operations", func(t *testing.T) {
		result, err := jsonSetFunc([]Value{
			NewTextValue(`{}`),
			NewTextValue("$.a"),
			NewIntValue(1),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.AsString() != `{"a":1}` {
			t.Errorf("expected {\"a\":1}, got %s", result.AsString())
		}
	})
}

// Benchmark tests
func BenchmarkJSONFunc(b *testing.B) {
	input := NewTextValue(`{"name":"John","age":30,"city":"New York"}`)
	for i := 0; i < b.N; i++ {
		jsonFunc([]Value{input})
	}
}

func BenchmarkJSONExtract(b *testing.B) {
	input := NewTextValue(`{"a":{"b":{"c":1}}}`)
	path := NewTextValue("$.a.b.c")
	for i := 0; i < b.N; i++ {
		jsonExtractFunc([]Value{input, path})
	}
}

func BenchmarkJSONSet(b *testing.B) {
	input := NewTextValue(`{"x":1}`)
	path := NewTextValue("$.y")
	value := NewIntValue(2)
	for i := 0; i < b.N; i++ {
		jsonSetFunc([]Value{input, path, value})
	}
}
