// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
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
	r.Register(NewScalarFunc("json_extract_text", 2, jsonExtractTextFunc))  // For ->> operator
	r.Register(NewScalarFunc("json_insert", -1, jsonInsertFunc))            // 3+ args (odd)
	r.Register(NewScalarFunc("json_object", -1, jsonObjectFunc))            // even args
	r.Register(NewScalarFunc("json_patch", 2, jsonPatchFunc))
	r.Register(NewScalarFunc("json_remove", -1, jsonRemoveFunc)) // 2+ args
	r.Register(NewScalarFunc("json_replace", -1, jsonReplaceFunc))
	r.Register(NewScalarFunc("json_set", -1, jsonSetFunc))
	r.Register(NewScalarFunc("json_type", -1, jsonTypeFunc)) // 1 or 2 args
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
		return nil, fmt.Errorf("malformed JSON")
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

	return marshalJSONPreserveFloats(arr)
}

// jsonArrayLengthFunc implements json_array_length(X [, path])
// Returns the length of a JSON array
func jsonArrayLengthFunc(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("json_array_length() requires 1 or 2 arguments")
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	data = applyPathIfPresent(data, args, 1)
	return getArrayLength(data)
}

// parseJSONArg parses the first argument as JSON
func parseJSONArg(arg Value) (interface{}, error) {
	if arg.IsNull() {
		return nil, fmt.Errorf("null argument")
	}

	jsonStr := arg.AsString()
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, err
	}
	return data, nil
}

// applyPathIfPresent applies a path if the argument at the given index is present
func applyPathIfPresent(data interface{}, args []Value, pathIndex int) interface{} {
	if len(args) > pathIndex && !args[pathIndex].IsNull() {
		path := args[pathIndex].AsString()
		return extractPath(data, path)
	}
	return data
}

// getArrayLength returns the length of a JSON array
func getArrayLength(data interface{}) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

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

	data, err := parseJSONArg(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	// If only one path, return single value
	if len(args) == 2 {
		return extractSinglePath(data, args[1])
	}

	// Multiple paths: return array of results
	return extractMultiplePaths(data, args[1:])
}

// extractSinglePath extracts a single path from JSON data
func extractSinglePath(data interface{}, pathArg Value) (Value, error) {
	if pathArg.IsNull() {
		return NewNullValue(), nil
	}
	path := pathArg.AsString()
	result := extractPath(data, path)
	return jsonToValue(result), nil
}

// extractMultiplePaths extracts multiple paths from JSON data and returns as array
func extractMultiplePaths(data interface{}, pathArgs []Value) (Value, error) {
	results := make([]interface{}, len(pathArgs))
	for i, pathArg := range pathArgs {
		results[i] = extractPathOrNil(data, pathArg)
	}

	jsonResult, err := json.Marshal(results)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(jsonResult)), nil
}

// extractPathOrNil extracts a path or returns nil for null arguments
func extractPathOrNil(data interface{}, pathArg Value) interface{} {
	if pathArg.IsNull() {
		return nil
	}
	path := pathArg.AsString()
	return extractPath(data, path)
}

// jsonExtractTextFunc implements the ->> operator (json_extract returning text)
// Like json_extract but always returns unquoted text value
func jsonExtractTextFunc(args []Value) (Value, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("json_extract_text() requires 2 arguments")
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	result := applyPathIfPresent(data, args, 1)
	return convertResultToText(result)
}

// textConverter is a function that converts a specific type to text
type textConverter func(interface{}) (Value, error)

// textConverters maps type names to their converter functions
var textConverters = map[string]textConverter{
	"string":  convertStringToText,
	"float64": convertFloat64ToText,
	"bool":    convertBoolToText,
}

// convertResultToText converts a JSON result to a text value
func convertResultToText(result interface{}) (Value, error) {
	if result == nil {
		return NewNullValue(), nil
	}

	// Try to find a specific converter
	typeName := fmt.Sprintf("%T", result)
	if converter, ok := textConverters[typeName]; ok {
		return converter(result)
	}

	// Default: marshal as JSON for complex types
	return marshalAsJSONText(result)
}

// convertStringToText converts a string to text
func convertStringToText(v interface{}) (Value, error) {
	return NewTextValue(v.(string)), nil
}

// convertFloat64ToText converts a float64 to text
func convertFloat64ToText(v interface{}) (Value, error) {
	f := v.(float64)
	if f == float64(int64(f)) {
		return NewTextValue(strconv.FormatInt(int64(f), 10)), nil
	}
	return NewTextValue(strconv.FormatFloat(f, 'f', -1, 64)), nil
}

// convertBoolToText converts a bool to text
func convertBoolToText(v interface{}) (Value, error) {
	if v.(bool) {
		return NewTextValue("true"), nil
	}
	return NewTextValue("false"), nil
}

// marshalAsJSONText marshals a value as JSON text
func marshalAsJSONText(result interface{}) (Value, error) {
	jsonResult, err := json.Marshal(result)
	if err != nil {
		return NewNullValue(), nil
	}
	return NewTextValue(string(jsonResult)), nil
}

// jsonInsertFunc implements json_insert(X, path1, value1, path2, value2, ...)
// Inserts values into JSON (only if path doesn't exist)
func jsonInsertFunc(args []Value) (Value, error) {
	return processPathValuePairs(args, "json_insert", shouldInsertPath)
}

// shouldInsertPath checks if a path should be inserted (path doesn't exist)
func shouldInsertPath(data interface{}, path string) bool {
	return extractPath(data, path) == nil
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

	return marshalJSONPreserveFloats(obj)
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
	return processPathValuePairs(args, "json_replace", shouldReplacePath)
}

// shouldReplacePath checks if a path should be replaced (path exists)
func shouldReplacePath(data interface{}, path string) bool {
	return extractPath(data, path) != nil
}

// pathConditionFunc is a function that checks whether a path should be modified
type pathConditionFunc func(data interface{}, path string) bool

// processPathValuePairs processes path-value pairs with a condition function
func processPathValuePairs(args []Value, funcName string, shouldSet pathConditionFunc) (Value, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, fmt.Errorf("%s() requires odd number of arguments (at least 3)", funcName)
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	data = applyPathValuePairs(data, args[1:], shouldSet)

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// applyPathValuePairs applies path-value pairs with a condition check
func applyPathValuePairs(data interface{}, pathValueArgs []Value, shouldSet pathConditionFunc) interface{} {
	for i := 0; i < len(pathValueArgs); i += 2 {
		if pathValueArgs[i].IsNull() {
			continue
		}
		path := pathValueArgs[i].AsString()
		value := valueToJSONSmart(pathValueArgs[i+1])

		if shouldSet(data, path) {
			data = setPath(data, path, value)
		}
	}
	return data
}

// jsonSetFunc implements json_set(X, path1, value1, path2, value2, ...)
// Sets values in JSON (creates or replaces)
func jsonSetFunc(args []Value) (Value, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, fmt.Errorf("json_set() requires odd number of arguments (at least 3)")
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	data = applySetPathPairs(data, args[1:])

	result, err := json.Marshal(data)
	if err != nil {
		return NewNullValue(), nil
	}

	return NewTextValue(string(result)), nil
}

// applySetPathPairs applies all path-value pairs for json_set
func applySetPathPairs(data interface{}, pathValueArgs []Value) interface{} {
	for i := 0; i < len(pathValueArgs); i += 2 {
		if pathValueArgs[i].IsNull() {
			continue
		}
		path := pathValueArgs[i].AsString()
		value := valueToJSONSmart(pathValueArgs[i+1])
		data = setPath(data, path, value)
	}
	return data
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

	data, err := parseJSONArgUseNumber(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	return jsonTypeWithPath(data, args)
}

// parseJSONArgUseNumber parses a JSON argument using json.Number to preserve number format
func parseJSONArgUseNumber(arg Value) (interface{}, error) {
	jsonStr := arg.AsString()
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	var data interface{}
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

// jsonTypeWithPath applies path if present and returns the JSON type
func jsonTypeWithPath(data interface{}, args []Value) (Value, error) {
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
		return NewNullValue(), nil
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

	value := valueToJSONSmart(args[0])

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
		return jsonFloat(v.AsFloat64())
	case TypeText:
		return convertStringToJSON(v.AsString())
	case TypeBlob:
		return v.AsBlob()
	default:
		return nil
	}
}

// convertStringToJSON attempts to parse a string as JSON if it looks like JSON.
// Returns the parsed JSON structure or the original string.
func convertStringToJSON(s string) interface{} {
	if len(s) == 0 {
		return s
	}

	// Try parsing as JSON object
	if s[0] == '{' {
		if parsed := tryParseJSONObject(s); parsed != nil {
			return parsed
		}
		return s
	}

	// Try parsing as JSON array
	if s[0] == '[' {
		if parsed := tryParseJSONArray(s); parsed != nil {
			return parsed
		}
		return s
	}

	// Plain string
	return s
}

// tryParseJSONObject attempts to parse a string as a JSON object.
// Returns the parsed object if it's minified JSON, nil otherwise.
func tryParseJSONObject(s string) interface{} {
	var data interface{}
	if err := json.Unmarshal([]byte(s), &data); err != nil {
		return nil
	}

	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	// Check if minified (indicates it's from json())
	if isMinifiedJSON(data, s) {
		return obj
	}

	return nil
}

// tryParseJSONArray attempts to parse a string as a JSON array.
// Returns the parsed array if it's minified JSON (indicating it came from json()).
func tryParseJSONArray(s string) interface{} {
	var data interface{}
	if err := json.Unmarshal([]byte(s), &data); err != nil {
		return nil
	}

	arr, ok := data.([]interface{})
	if !ok {
		return nil
	}

	// Check if minified (indicates it's from json())
	if isMinifiedJSON(data, s) {
		return arr
	}

	return nil
}

// isMinifiedJSON checks if the string representation matches the minified JSON.
func isMinifiedJSON(data interface{}, s string) bool {
	minified, err := json.Marshal(data)
	if err != nil {
		return false
	}
	return string(minified) == s
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
		return jsonBoolToValue(v)
	case float64:
		return jsonFloat64ToValue(v)
	case string:
		return NewTextValue(v)
	case []interface{}, map[string]interface{}:
		return jsonComplexToValue(v)
	default:
		return NewNullValue()
	}
}

// jsonBoolToValue converts a JSON boolean to a SQL Value
func jsonBoolToValue(v bool) Value {
	if v {
		return NewIntValue(1)
	}
	return NewIntValue(0)
}

// jsonFloat64ToValue converts a JSON number to a SQL Value
func jsonFloat64ToValue(v float64) Value {
	if v == float64(int64(v)) {
		return NewIntValue(int64(v))
	}
	return NewFloatValue(v)
}

// jsonComplexToValue converts JSON objects/arrays to SQL Values (as JSON strings)
func jsonComplexToValue(v interface{}) Value {
	jsonBytes, _ := json.Marshal(v)
	return NewTextValue(string(jsonBytes))
}

// getJSONType returns the SQLite JSON type string for a value
func getJSONType(data interface{}) string {
	if data == nil {
		return "null"
	}

	switch v := data.(type) {
	case bool:
		return "true" // SQLite uses "true" for both true and false
	case json.Number:
		return jsonNumberType(v)
	case float64:
		if v == float64(int64(v)) && v <= 1e15 && v >= -1e15 {
			return "integer"
		}
		return "real"
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

// jsonNumberType returns "integer" or "real" for a json.Number.
func jsonNumberType(v json.Number) string {
	if strings.Contains(v.String(), ".") {
		return "real"
	}
	return "integer"
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
	path = strings.TrimPrefix(path, "$")

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
	path = strings.TrimPrefix(path, "$")

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
		return setPathInArray(data, part, remaining, value)
	}
	return setPathInObject(data, part, remaining, value)
}

// setPathInArray sets a value in a JSON array at the specified path
func setPathInArray(data interface{}, part pathPart, remaining []pathPart, value interface{}) interface{} {
	arr, ok := data.([]interface{})
	if !ok {
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
}

// setPathInObject sets a value in a JSON object at the specified path
func setPathInObject(data interface{}, part pathPart, remaining []pathPart, value interface{}) interface{} {
	obj, ok := data.(map[string]interface{})
	if !ok {
		obj = make(map[string]interface{})
	}

	if len(remaining) == 0 {
		obj[part.key] = value
	} else {
		obj[part.key] = setPathRecursive(obj[part.key], remaining, value)
	}
	return obj
}

// removePath removes a value from JSON at the given path
func removePath(data interface{}, path string) interface{} {
	if path == "" || path == "$" {
		return nil
	}

	// Remove leading $ if present
	path = strings.TrimPrefix(path, "$")

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
		return patch
	}

	targetMap := ensureTargetMap(target)
	result := copyMap(targetMap)
	return mergePatchIntoResult(result, patchMap)
}

// ensureTargetMap converts target to a map or returns an empty map
func ensureTargetMap(target interface{}) map[string]interface{} {
	if targetMap, ok := target.(map[string]interface{}); ok {
		return targetMap
	}
	return make(map[string]interface{})
}

// copyMap creates a shallow copy of a map
func copyMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	return result
}

// mergePatchIntoResult applies patch entries to the result map
func mergePatchIntoResult(result, patchMap map[string]interface{}) interface{} {
	for k, v := range patchMap {
		if v == nil {
			delete(result, k)
		} else if vMap, ok := v.(map[string]interface{}); ok {
			result[k] = mergeObjectPatch(result[k], vMap)
		} else {
			result[k] = v
		}
	}
	return result
}

// mergeObjectPatch recursively merges object patches
func mergeObjectPatch(targetVal interface{}, vMap map[string]interface{}) interface{} {
	if _, exists := targetVal.(map[string]interface{}); exists {
		return applyJSONPatch(targetVal, vMap)
	}
	return vMap
}

// jsonFloat wraps float64 to preserve decimal point in JSON output (e.g. 3.0 not 3)
type jsonFloat float64

// MarshalJSON outputs the float with a decimal point when it has no fractional part.
func (f jsonFloat) MarshalJSON() ([]byte, error) {
	v := float64(f)
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return []byte(s), nil
}

// marshalJSONPreserveFloats marshals a value, preserving float64 decimal points.
func marshalJSONPreserveFloats(data interface{}) (Value, error) {
	wrapped := wrapFloats(data)
	result, err := json.Marshal(wrapped)
	if err != nil {
		return NewNullValue(), nil
	}
	return NewTextValue(string(result)), nil
}

// wrapFloats recursively processes nested structures to preserve jsonFloat wrappers.
// SQL REAL values are already wrapped as jsonFloat by valueToJSONSmart;
// float64 from JSON parsing are left as-is to marshal as integers.
func wrapFloats(data interface{}) interface{} {
	switch v := data.(type) {
	case float64:
		return v
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, elem := range v {
			out[i] = wrapFloats(elem)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, val := range v {
			out[k] = wrapFloats(val)
		}
		return out
	default:
		return data
	}
}

// deepCopy creates a deep copy of a JSON-compatible data structure.
// Primitive types (string, float64, bool, json.Number) are immutable
// and returned directly without marshaling overhead.
func deepCopy(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{}, len(v))
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
		// Primitive types (string, float64, bool, json.Number) are
		// immutable in Go and safe to share without copying.
		return data
	}
}
