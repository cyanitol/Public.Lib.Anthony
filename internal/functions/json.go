// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// RegisterJSONFunctions registers all JSON functions.
func RegisterJSONFunctions(r *Registry) {
	r.Register(NewScalarFunc("json", 1, jsonFunc))
	r.Register(NewScalarFunc("json_array", -1, jsonArrayFunc))
	r.Register(NewScalarFunc("json_array_length", -1, jsonArrayLengthFunc)) // 1 or 2 args
	r.Register(NewScalarFunc("json_extract", -1, jsonExtractFunc))          // 2+ args
	r.Register(NewScalarFunc("json_insert", -1, jsonInsertFunc))            // 3+ args (odd)
	r.Register(NewScalarFunc("json_object", -1, jsonObjectFunc))            // even args
	r.Register(NewScalarFunc("json_patch", 2, jsonPatchFunc))
	r.Register(NewScalarFunc("json_remove", -1, jsonRemoveFunc)) // 2+ args
	r.Register(NewScalarFunc("json_replace", -1, jsonReplaceFunc))
	r.Register(NewScalarFunc("json_set", -1, jsonSetFunc))
	r.Register(NewScalarFunc("json_type", -1, jsonTypeFunc))   // 1 or 2 args
	r.Register(NewScalarFunc("json_valid", 1, jsonValidFunc))
	r.Register(NewScalarFunc("json_quote", 1, jsonQuoteFunc))
}

// jsonFunc implements json(X)
// Validates and minifies JSON
func jsonFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()

	// Validate and minify by unmarshaling and marshaling
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil // Invalid JSON returns NULL
	}

	minified, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(minified)), nil
}

// jsonArrayFunc implements json_array(X1, X2, ..., XN)
// Creates a JSON array from arguments
func jsonArrayFunc(args []Value) (Value, error) {
	arr := make([]interface{}, len(args))

	for i, arg := range args {
		// Use smart version to detect JSON-typed values from json()
		arr[i] = valueToJSONSmart(arg)
	}

	result, err := json.Marshal(arr)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonArrayLengthFunc implements json_array_length(X [, path])
// Returns the length of a JSON array
func jsonArrayLengthFunc(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("json_array_length() requires 1 or 2 arguments")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// If path is provided, extract the value at that path
	if len(args) == 2 && !args[1].IsNull() {
		path := args[1].AsString()
		data = extractPath(data, path)
		if data == nil {
			return NewNullValue(), nil
		}
	}

	// Check if it's an array
	arr, ok := data.([]interface{})
	if !ok {
		return NewNullValue(), nil
	}

	return NewIntValue(int64(len(arr))), nil
}

// jsonExtractFunc implements json_extract(X, path1, path2, ...)
// Extracts values from JSON at given paths
func jsonExtractFunc(args []Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("json_extract() requires at least 2 arguments")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// If only one path, return single value
	if len(args) == 2 {
		if args[1].IsNull() {
			return NewNullValue(), nil
		}
		path := args[1].AsString()
		result := extractPath(data, path)
		return jsonToValue(result), nil
	}

	// Multiple paths: return array of results
	results := make([]interface{}, len(args)-1)
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			results[i-1] = nil
		} else {
			path := args[i].AsString()
			results[i-1] = extractPath(data, path)
		}
	}

	jsonResult, err := json.Marshal(results)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(jsonResult)), nil
}

// jsonInsertFunc implements json_insert(X, path1, value1, path2, value2, ...)
// Inserts values into JSON (only if path doesn't exist)
func jsonInsertFunc(args []Value) (Value, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, fmt.Errorf("json_insert() requires odd number of arguments (at least 3)")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// Process path-value pairs
	for i := 1; i < len(args); i += 2 {
		if args[i].IsNull() {
			continue
		}
		path := args[i].AsString()
		// Use smart version to support both string and JSON values
		value := valueToJSONSmart(args[i+1])

		// Only insert if path doesn't exist
		if extractPath(data, path) == nil {
			data = setPath(data, path, value)
		}
	}

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonObjectFunc implements json_object(key1, value1, key2, value2, ...)
// Creates a JSON object from key-value pairs
func jsonObjectFunc(args []Value) (Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("json_object() requires even number of arguments")
	}

	obj := make(map[string]interface{})

	for i := 0; i < len(args); i += 2 {
		if args[i].IsNull() {
			return nil, fmt.Errorf("json_object() keys cannot be NULL")
		}

		key := args[i].AsString()
		// Use smart version to detect JSON-typed values
		value := valueToJSONSmart(args[i+1])
		obj[key] = value
	}

	result, err := json.Marshal(obj)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonPatchFunc implements json_patch(X, Y)
// Applies RFC 7396 JSON Merge Patch
func jsonPatchFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewNullValue(), nil
	}
	if args[1].IsNull() {
		return args[0], nil // NULL patch returns original
	}

	targetStr := args[0].AsString()
	patchStr := args[1].AsString()

	var target interface{}
	var patch interface{}

	if err := json.Unmarshal([]byte(targetStr), &target); err != nil {
		return NewNullValue(), nil
	}
	if err := json.Unmarshal([]byte(patchStr), &patch); err != nil {
		return NewNullValue(), nil
	}

	result := applyJSONPatch(target, patch)

	jsonResult, err := json.Marshal(result)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(jsonResult)), nil
}

// jsonRemoveFunc implements json_remove(X, path1, path2, ...)
// Removes values from JSON at given paths
func jsonRemoveFunc(args []Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("json_remove() requires at least 2 arguments")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// Remove each path
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			continue
		}
		path := args[i].AsString()
		data = removePath(data, path)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonReplaceFunc implements json_replace(X, path1, value1, path2, value2, ...)
// Replaces values in JSON (only if path exists)
func jsonReplaceFunc(args []Value) (Value, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, fmt.Errorf("json_replace() requires odd number of arguments (at least 3)")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// Process path-value pairs
	for i := 1; i < len(args); i += 2 {
		if args[i].IsNull() {
			continue
		}
		path := args[i].AsString()
		// Use smart version to support both string and JSON values
		value := valueToJSONSmart(args[i+1])

		// Only replace if path exists
		if extractPath(data, path) != nil {
			data = setPath(data, path, value)
		}
	}

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonSetFunc implements json_set(X, path1, value1, path2, value2, ...)
// Sets values in JSON (creates or replaces)
func jsonSetFunc(args []Value) (Value, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, fmt.Errorf("json_set() requires odd number of arguments (at least 3)")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// Process path-value pairs
	for i := 1; i < len(args); i += 2 {
		if args[i].IsNull() {
			continue
		}
		path := args[i].AsString()
		// Use smart version for set - need to support both string and JSON values
		value := valueToJSONSmart(args[i+1])
		data = setPath(data, path, value)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// jsonTypeFunc implements json_type(X [, path])
// Returns the JSON type of a value
func jsonTypeFunc(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("json_type() requires 1 or 2 arguments")
	}

	if args[0].IsNull() {
		return NewNullValue(), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewNullValue(), nil
	}

	// If path is provided, extract the value at that path
	if len(args) == 2 && !args[1].IsNull() {
		path := args[1].AsString()
		data = extractPath(data, path)
		if data == nil {
			return NewNullValue(), nil
		}
	}

	return NewTextValue(getJSONType(data)), nil
}

// jsonValidFunc implements json_valid(X)
// Returns 1 if X is valid JSON, 0 otherwise
func jsonValidFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewIntValue(0), nil
	}

	jsonStr := args[0].AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return NewIntValue(0), nil
	}

	return NewIntValue(1), nil
}

// jsonQuoteFunc implements json_quote(X)
// Quotes a value as a JSON string
func jsonQuoteFunc(args []Value) (Value, error) {
	if args[0].IsNull() {
		return NewTextValue("null"), nil
	}

	value := valueToJSON(args[0])

	// Marshal the value to get proper JSON representation
	result, err := json.Marshal(value)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// Helper functions

// valueToJSON converts a SQL Value to a Go interface for JSON marshaling
func valueToJSON(v Value) interface{} {
	if v.IsNull() {
		return nil
	}

	switch v.Type() {
	case TypeInteger:
		return v.AsInt64()
	case TypeFloat:
		return v.AsFloat64()
	case TypeText:
		// Always return text as a string, never parse as JSON
		// SQLite uses subtypes to distinguish JSON text from plain text,
		// but we don't have that mechanism. For json_array/json_object/json_set,
		// the caller should explicitly use json() to parse JSON strings.
		return v.AsString()
	case TypeBlob:
		return v.AsBlob()
	default:
		return nil
	}
}

// valueToJSONSmart is like valueToJSON but attempts to detect JSON-typed text values.
// It checks if a text value is minified JSON (no extra whitespace) which indicates
// it came from json() function. This is a heuristic workaround for lack of subtypes.
func valueToJSONSmart(v Value) interface{} {
	if v.IsNull() {
		return nil
	}

	switch v.Type() {
	case TypeInteger:
		return v.AsInt64()
	case TypeFloat:
		return v.AsFloat64()
	case TypeText:
		s := v.AsString()
		// Check if this looks like it could be JSON output from json() function
		// We parse JSON objects {} or arrays/objects with specific characteristics
		if len(s) > 0 && s[0] == '{' {
			// Objects are more likely from json() - parse them
			var data interface{}
			if err := json.Unmarshal([]byte(s), &data); err == nil {
				if _, ok := data.(map[string]interface{}); ok {
					// Check if minified (indicates it's from json())
					minified, err := json.Marshal(data)
					if err == nil && string(minified) == s {
						return data
					}
				}
			}
		} else if len(s) > 0 && s[0] == '[' {
			// For arrays: only parse if they contain strings or nested structures
			// Arrays with only numbers like [97,96] are treated as string literals
			// This is a heuristic since we lack SQLite's subtype system
			var data interface{}
			if err := json.Unmarshal([]byte(s), &data); err == nil {
				if arr, ok := data.([]interface{}); ok {
					// Check if array contains strings or complex types
					hasStringOrComplex := false
					for _, item := range arr {
						switch item.(type) {
						case string, map[string]interface{}, []interface{}:
							hasStringOrComplex = true
							break
						}
					}

					if hasStringOrComplex {
						// Contains non-numeric types - parse as JSON
						minified, err := json.Marshal(data)
						if err == nil && string(minified) == s {
							return data
						}
					}
				}
			}
		}
		// Otherwise it's a plain string
		return s
	case TypeBlob:
		return v.AsBlob()
	default:
		return nil
	}
}

// jsonToValue converts a JSON value to a SQL Value
// For simple types (string, number, bool, null), returns the value directly
// For complex types (object, array), returns as JSON string
func jsonToValue(data interface{}) Value {
	if data == nil {
		return NewNullValue()
	}

	switch v := data.(type) {
	case bool:
		if v {
			return NewIntValue(1)
		}
		return NewIntValue(0)
	case float64:
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return NewIntValue(int64(v))
		}
		return NewFloatValue(v)
	case string:
		return NewTextValue(v)
	case []interface{}, map[string]interface{}:
		// Return as JSON string
		jsonBytes, _ := json.Marshal(v)
		return NewTextValue(string(jsonBytes))
	default:
		return NewNullValue()
	}
}

// getJSONType returns the SQLite JSON type string for a value
func getJSONType(data interface{}) string {
	if data == nil {
		return "null"
	}

	switch data.(type) {
	case bool:
		return "true" // SQLite uses "true" for both true and false
	case float64:
		return "integer" // JSON numbers are treated as integers or reals
	case string:
		return "text"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "null"
	}
}

// extractPath extracts a value from JSON using a JSONPath-like syntax
// Simplified version supporting $, $.key, $[0], $.key[0], etc.
func extractPath(data interface{}, path string) interface{} {
	normalizedPath := normalizeJSONPath(path)
	if normalizedPath == "" {
		return data
	}

	parts := parsePath(normalizedPath)
	return traversePath(data, parts)
}

// normalizeJSONPath removes the leading $ from a JSON path
func normalizeJSONPath(path string) string {
	if path == "" || path == "$" {
		return ""
	}

	// Remove leading $ if present
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}

	return path
}

// traversePath traverses a JSON structure following the given path parts
func traversePath(data interface{}, parts []pathPart) interface{} {
	current := data

	for _, part := range parts {
		if current == nil {
			return nil
		}

		if part.isIndex {
			current = extractArrayElement(current, part.index)
		} else {
			current = extractObjectKey(current, part.key)
		}

		if current == nil {
			return nil
		}
	}

	return current
}

// extractArrayElement extracts an element from a JSON array
func extractArrayElement(data interface{}, index int) interface{} {
	arr, ok := data.([]interface{})
	if !ok {
		return nil
	}
	if index < 0 || index >= len(arr) {
		return nil
	}
	return arr[index]
}

// extractObjectKey extracts a value from a JSON object by key
func extractObjectKey(data interface{}, key string) interface{} {
	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}
	val, exists := obj[key]
	if !exists {
		return nil
	}
	return val
}

// setPath sets a value in JSON at the given path
func setPath(data interface{}, path string, value interface{}) interface{} {
	if path == "" || path == "$" {
		return value
	}

	// Remove leading $ if present
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}

	if path == "" {
		return value
	}

	parts := parsePath(path)
	if len(parts) == 0 {
		return value
	}

	// Deep copy the data structure
	dataCopy := deepCopy(data)

	// Navigate to parent and set value
	return setPathRecursive(dataCopy, parts, value)
}

func setPathRecursive(data interface{}, parts []pathPart, value interface{}) interface{} {
	if len(parts) == 0 {
		return value
	}

	part := parts[0]
	remaining := parts[1:]

	if part.isIndex {
		// Array access
		arr, ok := data.([]interface{})
		if !ok {
			// Create new array if needed
			arr = make([]interface{}, 0)
		}

		// Ensure array is large enough
		for len(arr) <= part.index {
			arr = append(arr, nil)
		}

		if len(remaining) == 0 {
			arr[part.index] = value
		} else {
			arr[part.index] = setPathRecursive(arr[part.index], remaining, value)
		}
		return arr
	} else {
		// Object access
		obj, ok := data.(map[string]interface{})
		if !ok {
			// Create new object if needed
			obj = make(map[string]interface{})
		}

		if len(remaining) == 0 {
			obj[part.key] = value
		} else {
			obj[part.key] = setPathRecursive(obj[part.key], remaining, value)
		}
		return obj
	}
}

// removePath removes a value from JSON at the given path
func removePath(data interface{}, path string) interface{} {
	if path == "" || path == "$" {
		return nil
	}

	// Remove leading $ if present
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}

	if path == "" {
		return nil
	}

	parts := parsePath(path)
	if len(parts) == 0 {
		return data
	}

	// Deep copy the data structure
	dataCopy := deepCopy(data)

	// Navigate to parent and remove
	return removePathRecursive(dataCopy, parts)
}

func removePathRecursive(data interface{}, parts []pathPart) interface{} {
	if len(parts) == 0 {
		return data
	}

	part := parts[0]
	remaining := parts[1:]

	if part.isIndex {
		return removeFromArray(data, part, remaining)
	}
	return removeFromObject(data, part, remaining)
}

func removeFromArray(data interface{}, part pathPart, remaining []pathPart) interface{} {
	arr, ok := data.([]interface{})
	if !ok || part.index < 0 || part.index >= len(arr) {
		return data
	}

	if len(remaining) == 0 {
		// Remove this element
		return append(arr[:part.index], arr[part.index+1:]...)
	}
	arr[part.index] = removePathRecursive(arr[part.index], remaining)
	return arr
}

func removeFromObject(data interface{}, part pathPart, remaining []pathPart) interface{} {
	obj, ok := data.(map[string]interface{})
	if !ok {
		return data
	}

	if len(remaining) == 0 {
		delete(obj, part.key)
		return obj
	}
	if val, exists := obj[part.key]; exists {
		obj[part.key] = removePathRecursive(val, remaining)
	}
	return obj
}

// pathPart represents a single part of a JSON path
type pathPart struct {
	key     string
	index   int
	isIndex bool
}

// parsePath parses a JSON path into parts
func parsePath(path string) []pathPart {
	var parts []pathPart
	var current strings.Builder
	inBracket := false

	for i := 0; i < len(path); i++ {
		ch := path[i]
		inBracket = handlePathCharacter(ch, &current, &parts, inBracket)
	}

	// Add any remaining content as a key part
	addRemainingKeyPart(&current, &parts)

	return parts
}

// handlePathCharacter processes a single character in the path parsing
func handlePathCharacter(ch byte, current *strings.Builder, parts *[]pathPart, inBracket bool) bool {
	switch ch {
	case '.':
		return handleDotCharacter(current, parts, inBracket)
	case '[':
		return handleOpenBracket(current, parts)
	case ']':
		return handleCloseBracket(current, parts, inBracket)
	default:
		current.WriteByte(ch)
		return inBracket
	}
}

// handleDotCharacter handles the '.' character in path parsing
func handleDotCharacter(current *strings.Builder, parts *[]pathPart, inBracket bool) bool {
	if inBracket {
		current.WriteByte('.')
	} else {
		addRemainingKeyPart(current, parts)
	}
	return inBracket
}

// handleOpenBracket handles the '[' character in path parsing
func handleOpenBracket(current *strings.Builder, parts *[]pathPart) bool {
	addRemainingKeyPart(current, parts)
	return true // entering bracket context
}

// handleCloseBracket handles the ']' character in path parsing
func handleCloseBracket(current *strings.Builder, parts *[]pathPart, inBracket bool) bool {
	if inBracket {
		addIndexPart(current, parts)
		return false // exiting bracket context
	}
	return inBracket
}

// addRemainingKeyPart adds any remaining content as a key part
func addRemainingKeyPart(current *strings.Builder, parts *[]pathPart) {
	if current.Len() > 0 {
		*parts = append(*parts, pathPart{key: current.String(), isIndex: false})
		current.Reset()
	}
}

// addIndexPart adds an index part from the current content
func addIndexPart(current *strings.Builder, parts *[]pathPart) {
	idx, err := strconv.Atoi(current.String())
	if err == nil {
		*parts = append(*parts, pathPart{index: idx, isIndex: true})
	}
	current.Reset()
}

// applyJSONPatch applies RFC 7396 JSON Merge Patch
func applyJSONPatch(target, patch interface{}) interface{} {
	patchMap, ok := patch.(map[string]interface{})
	if !ok {
		// If patch is not an object, it replaces the target
		return patch
	}

	targetMap, ok := target.(map[string]interface{})
	if !ok {
		// If target is not an object, patch replaces it
		targetMap = make(map[string]interface{})
	}

	// Create a new map for the result
	result := make(map[string]interface{})

	// Copy existing target values
	for k, v := range targetMap {
		result[k] = v
	}

	// Apply patch
	for k, v := range patchMap {
		if v == nil {
			// null in patch means delete
			delete(result, k)
		} else if vMap, ok := v.(map[string]interface{}); ok {
			// Recursively merge objects
			if targetVal, exists := result[k]; exists {
				result[k] = applyJSONPatch(targetVal, vMap)
			} else {
				result[k] = v
			}
		} else {
			// Replace value
			result[k] = v
		}
	}

	return result
}

// deepCopy creates a deep copy of a JSON-compatible data structure
func deepCopy(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = deepCopy(val)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = deepCopy(val)
		}
		return result
	default:
		// Primitive types are copied by value
		// Marshal and unmarshal to ensure deep copy
		b, _ := json.Marshal(v)
		var result interface{}
		json.Unmarshal(b, &result)
		return result
	}
}
