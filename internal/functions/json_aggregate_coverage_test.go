// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions_test

import (
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// aggregate.go: marshalJSONValue (75.0%)
// Exercised via json_group_array and json_group_object with various value types.
// ---------------------------------------------------------------------------

// TestJSONAggregateCoverage_GroupArrayIntegers verifies json_group_array with integer values.
func TestJSONAggregateCoverage_GroupArrayIntegers(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_ints (v INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_ints VALUES (10), (20), (30)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(v) FROM jac_ints`)
	if isNull {
		t.Fatal("json_group_array(integer) returned NULL")
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(got), &arr); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d in %q", len(arr), got)
	}
}

// TestJSONAggregateCoverage_GroupArrayFloats verifies json_group_array with float values,
// exercising the float branch of valueToJSONInterface.
func TestJSONAggregateCoverage_GroupArrayFloats(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_floats (v REAL)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_floats VALUES (1.5), (2.5), (3.5)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(v) FROM jac_floats`)
	if isNull {
		t.Fatal("json_group_array(float) returned NULL")
	}
	if got == "" {
		t.Fatal("expected non-empty result")
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(got), &arr); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d in %q", len(arr), got)
	}
}

// TestJSONAggregateCoverage_GroupArrayMixed verifies json_group_array with a mix
// of integer, float, text, and NULL values, covering multiple branches.
func TestJSONAggregateCoverage_GroupArrayMixed(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_mixed (v)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_mixed VALUES (1), (2.5), ('hello'), (NULL)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(v) FROM jac_mixed`)
	if isNull {
		t.Fatal("json_group_array(mixed) returned NULL")
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(got), &arr); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(arr) != 4 {
		t.Errorf("expected 4 elements, got %d in %q", len(arr), got)
	}
}

// TestJSONAggregateCoverage_GroupArrayEmpty verifies json_group_array with no rows
// returns an empty JSON array via the nil-values branch.
func TestJSONAggregateCoverage_GroupArrayEmpty(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_empty (v INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(v) FROM jac_empty`)
	if isNull {
		t.Fatal("json_group_array on empty table returned NULL")
	}
	if got != "[]" {
		t.Errorf("expected '[]', got %q", got)
	}
}

// TestJSONAggregateCoverage_GroupObjectIntValue verifies json_group_object marshals
// integer values correctly (exercises the integer branch in valueToJSONInterface).
func TestJSONAggregateCoverage_GroupObjectIntValue(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_obj_int (k TEXT, v INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_obj_int VALUES ('a', 100), ('b', 200)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_object(k, v) FROM jac_obj_int`)
	if isNull {
		t.Fatal("json_group_object(integer values) returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(m) != 2 {
		t.Errorf("expected 2 keys, got %d in %q", len(m), got)
	}
}

// TestJSONAggregateCoverage_GroupObjectFloatValue verifies json_group_object marshals
// float values correctly (exercises the float branch in valueToJSONInterface).
func TestJSONAggregateCoverage_GroupObjectFloatValue(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_obj_float (k TEXT, v REAL)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_obj_float VALUES ('x', 3.14), ('y', 2.71)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_object(k, v) FROM jac_obj_float`)
	if isNull {
		t.Fatal("json_group_object(float values) returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if _, ok := m["x"]; !ok {
		t.Errorf("expected key 'x' in result %q", got)
	}
}

// TestJSONAggregateCoverage_GroupObjectNullValue verifies json_group_object with NULL
// values includes null in the JSON output.
func TestJSONAggregateCoverage_GroupObjectNullValue(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_obj_null (k TEXT, v TEXT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_obj_null VALUES ('present', 'yes'), ('absent', NULL)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_object(k, v) FROM jac_obj_null`)
	if isNull {
		t.Fatal("json_group_object(null value) returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if _, ok := m["absent"]; !ok {
		t.Errorf("expected 'absent' key in result %q", got)
	}
}

// TestJSONAggregateCoverage_GroupObjectSkipsNullKey verifies that json_group_object
// skips rows where the key is NULL.
func TestJSONAggregateCoverage_GroupObjectSkipsNullKey(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_obj_nullkey (k TEXT, v INTEGER)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_obj_nullkey VALUES ('real', 1), (NULL, 2)`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_object(k, v) FROM jac_obj_nullkey`)
	if isNull {
		t.Fatal("json_group_object(null key) returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(m) != 1 {
		t.Errorf("expected 1 key (null key skipped), got %d in %q", len(m), got)
	}
	if _, ok := m["real"]; !ok {
		t.Errorf("expected key 'real' in result %q", got)
	}
}

// ---------------------------------------------------------------------------
// json.go: marshalAsJSONText (75.0%)
// Triggered via json_extract_text when extracting complex types (object/array).
// ---------------------------------------------------------------------------

// TestJSONAggregateCoverage_MarshalAsJSONText_NestedArray verifies marshalAsJSONText
// is called when json_extract_text returns a nested array value.
func TestJSONAggregateCoverage_MarshalAsJSONText_NestedArray(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db,
		`SELECT json_extract_text('{"data":[10,20,30]}', '$.data')`)
	if isNull {
		t.Fatal("expected non-NULL result for nested array extraction")
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(got), &arr); err != nil {
		t.Fatalf("result not valid JSON array: %v (got %q)", err, got)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d in %q", len(arr), got)
	}
}

// TestJSONAggregateCoverage_MarshalAsJSONText_NestedObject verifies marshalAsJSONText
// is called when json_extract_text returns a nested object value.
func TestJSONAggregateCoverage_MarshalAsJSONText_NestedObject(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db,
		`SELECT json_extract_text('{"user":{"name":"alice","age":30}}', '$.user')`)
	if isNull {
		t.Fatal("expected non-NULL result for nested object extraction")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON object: %v (got %q)", err, got)
	}
	if _, ok := m["name"]; !ok {
		t.Errorf("expected 'name' key in result %q", got)
	}
}

// TestJSONAggregateCoverage_MarshalAsJSONText_DeepNesting verifies marshalAsJSONText
// with deeply nested JSON structures.
func TestJSONAggregateCoverage_MarshalAsJSONText_DeepNesting(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db,
		`SELECT json_extract_text('{"a":{"b":{"c":[1,2,3]}}}', '$.a')`)
	if isNull {
		t.Fatal("expected non-NULL result for deep nested extraction")
	}

	if got == "" {
		t.Fatal("expected non-empty result")
	}
	// Result should be the JSON representation of the sub-object.
	if !strings.HasPrefix(got, "{") {
		t.Errorf("expected JSON object result, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// json.go: isMinifiedJSON (75.0%)
// Triggered via json functions that call convertStringToJSON, which calls
// tryParseJSONObject / tryParseJSONArray → isMinifiedJSON.
// ---------------------------------------------------------------------------

// TestJSONAggregateCoverage_IsMinifiedJSON_MinifiedObject verifies isMinifiedJSON
// returns true for a minified JSON object (so the parsed form is returned).
// This is exercised when json_set or json_insert processes a JSON string value.
func TestJSONAggregateCoverage_IsMinifiedJSON_MinifiedObject(t *testing.T) {
	db := openTestDB(t)

	// json_set with a JSON object value exercises convertStringToJSON →
	// tryParseJSONObject → isMinifiedJSON (true path).
	got, isNull := queryOneString(t, db,
		`SELECT json_set('{"a":1}', '$.b', json('{"x":2}'))`)
	if isNull {
		t.Fatal("json_set returned NULL")
	}
	if got == "" {
		t.Fatal("expected non-empty JSON result")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
}

// TestJSONAggregateCoverage_IsMinifiedJSON_MinifiedArray verifies isMinifiedJSON
// returns true for a minified JSON array (so tryParseJSONArray returns the parsed form).
func TestJSONAggregateCoverage_IsMinifiedJSON_MinifiedArray(t *testing.T) {
	db := openTestDB(t)

	// json_set with a JSON array value exercises convertStringToJSON →
	// tryParseJSONArray → isMinifiedJSON (true path).
	got, isNull := queryOneString(t, db,
		`SELECT json_set('{"items":[]}', '$.items', json('[1,2,3]'))`)
	if isNull {
		t.Fatal("json_set with array value returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
}

// TestJSONAggregateCoverage_IsMinifiedJSON_NonMinifiedObject verifies isMinifiedJSON
// returns false for a pretty-printed (non-minified) JSON object, causing
// tryParseJSONObject to return nil and the original string to be kept.
func TestJSONAggregateCoverage_IsMinifiedJSON_NonMinifiedObject(t *testing.T) {
	db := openTestDB(t)

	// Passing a pretty-printed JSON object as a value means isMinifiedJSON
	// returns false, so convertStringToJSON returns the string as-is.
	// json_set will then treat it as a plain string value.
	got, isNull := queryOneString(t, db,
		`SELECT json_type(json_set('{"a":1}', '$.b', '{ "x": 2 }'), '$.b')`)
	if isNull {
		t.Fatal("json_type returned NULL")
	}
	// The pretty-printed JSON is stored as a plain text string, so json_type
	// for that key should be "text".
	if got != "text" {
		t.Errorf("expected 'text' for non-minified JSON value, got %q", got)
	}
}

// TestJSONAggregateCoverage_GroupArrayWithBlob verifies json_group_array with BLOB
// values, exercising the blob branch of valueToJSONInterface.
func TestJSONAggregateCoverage_GroupArrayWithBlob(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_blob (v BLOB)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_blob VALUES (X'414243')`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(v) FROM jac_blob`)
	if isNull {
		t.Fatal("json_group_array(blob) returned NULL")
	}

	var arr []interface{}
	if err := json.Unmarshal([]byte(got), &arr); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if len(arr) != 1 {
		t.Errorf("expected 1 element, got %d in %q", len(arr), got)
	}
}

// TestJSONAggregateCoverage_GroupObjectWithText verifies json_group_object with
// text values, exercising the default branch of valueToJSONInterface.
func TestJSONAggregateCoverage_GroupObjectWithText(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`CREATE TABLE jac_obj_text (k TEXT, v TEXT)`)
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jac_obj_text VALUES ('greeting', 'hello'), ('farewell', 'bye')`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_object(k, v) FROM jac_obj_text`)
	if isNull {
		t.Fatal("json_group_object(text values) returned NULL")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatalf("result not valid JSON: %v (got %q)", err, got)
	}
	if m["greeting"] != "hello" {
		t.Errorf("expected greeting='hello', got %q in %q", m["greeting"], got)
	}
}
