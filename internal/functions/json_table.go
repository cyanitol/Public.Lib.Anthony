// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// TableValuedFunction is the interface for functions that return multiple rows.
// These are used in FROM clauses (e.g., json_each, json_tree).
type TableValuedFunction interface {
	Function

	// Open initializes the table-valued function with arguments and returns rows.
	Open(args []Value) ([][]Value, error)

	// Columns returns the column names for the result set.
	Columns() []string
}

// JSONEachRow represents a single row returned by json_each.
type JSONEachRow struct {
	Key     Value
	Value   Value
	Type    Value
	Atom    Value
	ID      Value
	Parent  Value
	FullKey Value
	Path    Value
}

// jsonEachFunc implements json_each(json, path?) table-valued function.
type jsonEachFunc struct{}

func (f *jsonEachFunc) Name() string { return "json_each" }
func (f *jsonEachFunc) NumArgs() int { return -1 } // 1 or 2 args

func (f *jsonEachFunc) Call(args []Value) (Value, error) {
	return nil, fmt.Errorf("json_each() is a table-valued function")
}

func (f *jsonEachFunc) Columns() []string {
	return []string{"key", "value", "type", "atom", "id", "parent", "fullkey", "path"}
}

// Open implements the TableValuedFunction interface for json_each.
func (f *jsonEachFunc) Open(args []Value) ([][]Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("json_each() requires 1 or 2 arguments")
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return nil, nil
	}

	rootPath := "$"
	if len(args) == 2 && !args[1].IsNull() {
		rootPath = args[1].AsString()
		data = extractPath(data, rootPath)
		if data == nil {
			return nil, nil
		}
	}

	return jsonEachRows(data, rootPath), nil
}

// jsonEachRows generates rows for json_each from a JSON value.
func jsonEachRows(data interface{}, rootPath string) [][]Value {
	var rows [][]Value
	id := 0

	switch v := data.(type) {
	case []interface{}:
		rows = appendArrayRows(v, rootPath, &id)
	case map[string]interface{}:
		rows = appendObjectRows(v, rootPath, &id)
	default:
		// Scalar value: return single row
		rows = append(rows, makeEachRow(nil, data, rootPath, rootPath, &id, nil))
	}

	return rows
}

// appendArrayRows generates rows for each element in a JSON array.
func appendArrayRows(arr []interface{}, rootPath string, id *int) [][]Value {
	rows := make([][]Value, 0, len(arr))
	for i, elem := range arr {
		fullkey := fmt.Sprintf("%s[%d]", rootPath, i)
		row := makeEachRow(NewIntValue(int64(i)), elem, fullkey, rootPath, id, nil)
		rows = append(rows, row)
	}
	return rows
}

// appendObjectRows generates rows for each key-value pair in a JSON object.
func appendObjectRows(obj map[string]interface{}, rootPath string, id *int) [][]Value {
	rows := make([][]Value, 0, len(obj))
	for key, val := range obj {
		fullkey := rootPath + "." + key
		row := makeEachRow(NewTextValue(key), val, fullkey, rootPath, id, nil)
		rows = append(rows, row)
	}
	return rows
}

// makeEachRow creates a single row for json_each output.
func makeEachRow(key Value, val interface{}, fullkey, path string, id *int, parent *int) []Value {
	*id++
	currentID := *id

	valValue := jsonToValue(val)
	typeStr := getJSONType(val)
	atom := getAtomValue(val)

	parentVal := Value(NewNullValue())
	if parent != nil {
		parentVal = NewIntValue(int64(*parent))
	}

	if key == nil {
		key = NewNullValue()
	}

	return []Value{
		key,
		valValue,
		NewTextValue(typeStr),
		atom,
		NewIntValue(int64(currentID)),
		parentVal,
		NewTextValue(fullkey),
		NewTextValue(path),
	}
}

// getAtomValue returns the atom representation of a JSON value.
// For objects and arrays, atom is NULL. For scalars, it's the value itself.
func getAtomValue(val interface{}) Value {
	switch val.(type) {
	case []interface{}, map[string]interface{}:
		return NewNullValue()
	default:
		return jsonToValue(val)
	}
}

// jsonTreeFunc implements json_tree(json, path?) table-valued function.
type jsonTreeFunc struct{}

func (f *jsonTreeFunc) Name() string { return "json_tree" }
func (f *jsonTreeFunc) NumArgs() int { return -1 } // 1 or 2 args

func (f *jsonTreeFunc) Call(args []Value) (Value, error) {
	return nil, fmt.Errorf("json_tree() is a table-valued function")
}

func (f *jsonTreeFunc) Columns() []string {
	return []string{"key", "value", "type", "atom", "id", "parent", "fullkey", "path"}
}

// Open implements the TableValuedFunction interface for json_tree.
func (f *jsonTreeFunc) Open(args []Value) ([][]Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("json_tree() requires 1 or 2 arguments")
	}

	data, err := parseJSONArg(args[0])
	if err != nil {
		return nil, nil
	}

	rootPath := "$"
	if len(args) == 2 && !args[1].IsNull() {
		rootPath = args[1].AsString()
		data = extractPath(data, rootPath)
		if data == nil {
			return nil, nil
		}
	}

	var rows [][]Value
	id := 0
	jsonTreeWalk(data, nil, rootPath, rootPath, &id, nil, &rows)
	return rows, nil
}

// jsonTreeWalk recursively walks a JSON structure and appends rows.
func jsonTreeWalk(data interface{}, key Value, fullkey, path string, id *int, parent *int, rows *[][]Value) {
	*id++
	currentID := *id

	row := makeTreeRow(key, data, fullkey, path, currentID, parent)
	*rows = append(*rows, row)

	walkChildren(data, fullkey, id, &currentID, rows)
}

// makeTreeRow creates a single row for json_tree output.
func makeTreeRow(key Value, data interface{}, fullkey, path string, id int, parent *int) []Value {
	valValue := marshalValueForTree(data)
	typeStr := getJSONType(data)
	atom := getAtomValue(data)

	parentVal := Value(NewNullValue())
	if parent != nil {
		parentVal = NewIntValue(int64(*parent))
	}

	if key == nil {
		key = NewNullValue()
	}

	return []Value{
		key, valValue, NewTextValue(typeStr), atom,
		NewIntValue(int64(id)), parentVal,
		NewTextValue(fullkey), NewTextValue(path),
	}
}

// marshalValueForTree converts a JSON value to a Value, marshaling complex types.
func marshalValueForTree(data interface{}) Value {
	switch v := data.(type) {
	case []interface{}, map[string]interface{}:
		b, _ := json.Marshal(v)
		return NewTextValue(string(b))
	default:
		return jsonToValue(data)
	}
}

// walkChildren recursively walks children of arrays and objects.
func walkChildren(data interface{}, parentPath string, id, parentID *int, rows *[][]Value) {
	switch v := data.(type) {
	case []interface{}:
		for i, elem := range v {
			childKey := NewIntValue(int64(i))
			childPath := fmt.Sprintf("%s[%d]", parentPath, i)
			jsonTreeWalk(elem, childKey, childPath, parentPath, id, parentID, rows)
		}
	case map[string]interface{}:
		for key, val := range v {
			childKey := NewTextValue(key)
			childPath := parentPath + "." + key
			jsonTreeWalk(val, childKey, childPath, parentPath, id, parentID, rows)
		}
	}
}

// jsonValueToString converts a Go value to its JSON string representation.
func jsonValueToString(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case string:
		return strconv.Quote(val)
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// RegisterJSONTableFunctions registers table-valued JSON functions.
func RegisterJSONTableFunctions(r *Registry) {
	r.Register(&jsonEachFunc{})
	r.Register(&jsonTreeFunc{})
}
