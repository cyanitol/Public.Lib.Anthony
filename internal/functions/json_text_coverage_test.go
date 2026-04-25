// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions_test

import (
	"database/sql"
	"testing"
)

// TestJSONTextExtractString verifies json_extract_text returns a plain string.
func TestJSONTextExtractString(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_extract_text('{"a":"hello"}', '$.a')`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

// TestJSONTextExtractNumber verifies an integer JSON value is returned as its text representation.
func TestJSONTextExtractNumber(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_extract_text('{"n":42}', '$.n')`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != "42" {
		t.Errorf("got %q, want %q", got, "42")
	}
}

// TestJSONTextExtractBool verifies a boolean JSON value is returned as text.
func TestJSONTextExtractBool(t *testing.T) {
	db := openTestDB(t)

	for _, tc := range []struct {
		json string
		want string
	}{
		{`{"b":true}`, "true"},
		{`{"b":false}`, "false"},
	} {
		got, isNull := queryOneString(t, db, `SELECT json_extract_text('`+tc.json+`', '$.b')`)
		if isNull {
			t.Errorf("json=%q: expected non-NULL result", tc.json)
			continue
		}
		if got != tc.want {
			t.Errorf("json=%q: got %q, want %q", tc.json, got, tc.want)
		}
	}
}

// TestJSONTextExtractFloat verifies a float JSON value is returned as its decimal text.
func TestJSONTextExtractFloat(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_extract_text('{"f":3.14}', '$.f')`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != "3.14" {
		t.Errorf("got %q, want %q", got, "3.14")
	}
}

// TestJSONTextExtractObject verifies a nested object is marshalled back to JSON text.
func TestJSONTextExtractObject(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_extract_text('{"obj":{"x":1}}', '$.obj')`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	// The result must be valid JSON representing the object.
	if got != `{"x":1}` {
		t.Errorf("got %q, want %q", got, `{"x":1}`)
	}
}

// TestJSONTextExtractNullJSON verifies that a NULL JSON input yields NULL.
func TestJSONTextExtractNullJSON(t *testing.T) {
	db := openTestDB(t)
	row := db.QueryRow(`SELECT json_extract_text(NULL, '$.a')`)
	var s sql.NullString
	if err := row.Scan(&s); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if s.Valid {
		t.Errorf("expected NULL, got %q", s.String)
	}
}

// TestJSONTextExtractMissingPath verifies that a non-existent path yields NULL.
func TestJSONTextExtractMissingPath(t *testing.T) {
	db := openTestDB(t)
	row := db.QueryRow(`SELECT json_extract_text('{"a":1}', '$.z')`)
	var s sql.NullString
	if err := row.Scan(&s); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if s.Valid {
		t.Errorf("expected NULL for missing path, got %q", s.String)
	}
}

// TestJSONExtractMultiplePaths verifies extractMultiplePaths via json_extract with
// multiple path arguments, which returns a JSON array.
func TestJSONExtractMultiplePaths(t *testing.T) {
	db := openTestDB(t)
	// json_extract with 3+ args exercises extractMultiplePaths (line 150).
	got, isNull := queryOneString(t, db, `SELECT json_extract('{"a":1,"b":2}', '$.a', '$.b')`)
	if isNull {
		t.Fatal("expected non-NULL result for multi-path extract")
	}
	if got != `[1,2]` {
		t.Errorf("got %q, want %q", got, `[1,2]`)
	}
}

// TestJSONExtractMultiplePathsWithNull verifies extractMultiplePaths handles null path args.
func TestJSONExtractMultiplePathsWithNull(t *testing.T) {
	db := openTestDB(t)
	// A null path arg should produce null in the result array.
	got, isNull := queryOneString(t, db, `SELECT json_extract('{"a":1}', '$.a', NULL)`)
	if isNull {
		t.Fatal("expected non-NULL result array")
	}
	if got != `[1,null]` {
		t.Errorf("got %q, want %q", got, `[1,null]`)
	}
}

// TestJSONFloatMarshalWithDecimal exercises jsonFloat.MarshalJSON for a value
// that already has a fractional part (covers the branch where "." is present).
func TestJSONFloatMarshalWithDecimal(t *testing.T) {
	db := openTestDB(t)
	// CAST AS REAL produces a jsonFloat-wrapped value via valueToJSONSmart.
	got, isNull := queryOneString(t, db, `SELECT json_array(CAST(3.14 AS REAL))`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != `[3.14]` {
		t.Errorf("got %q, want %q", got, `[3.14]`)
	}
}

// TestJSONFloatMarshalWholeNumber exercises jsonFloat.MarshalJSON for a whole-number
// float, covering the branch that appends ".0".
func TestJSONFloatMarshalWholeNumber(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_array(CAST(3.0 AS REAL))`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != `[3.0]` {
		t.Errorf("got %q, want %q", got, `[3.0]`)
	}
}

// TestMarshalJSONPreserveFloatsViaArray exercises marshalJSONPreserveFloats through
// json_array with mixed value types.
func TestMarshalJSONPreserveFloatsViaArray(t *testing.T) {
	db := openTestDB(t)
	got, isNull := queryOneString(t, db, `SELECT json_array(1, 'hello', CAST(2.5 AS REAL))`)
	if isNull {
		t.Fatal("expected non-NULL result")
	}
	if got != `[1,"hello",2.5]` {
		t.Errorf("got %q, want %q", got, `[1,"hello",2.5]`)
	}
}
