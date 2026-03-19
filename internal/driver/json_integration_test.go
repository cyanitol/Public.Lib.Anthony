// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// jsonCompareResult compares a query result against an expected value of type int64 or string.
func jsonCompareResult(t *testing.T, result, expected interface{}) {
	t.Helper()
	switch exp := expected.(type) {
	case int64:
		if r, ok := result.(int64); !ok || r != exp {
			t.Errorf("expected %d, got %v (type: %T)", exp, result, result)
		}
	case string:
		if r, ok := result.(string); !ok || r != exp {
			t.Errorf("expected %s, got %v (type: %T)", exp, result, result)
		}
	default:
		t.Errorf("unexpected expected type: %T", exp)
	}
}

// TestJSONFunctionsIntegration tests JSON functions via SQL queries
func TestJSONFunctionsIntegration(t *testing.T) {
	db, err := sql.Open("sqlite_internal", t.TempDir()+"/test_json.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		// json_valid tests
		{
			name:     "json_valid with valid object",
			query:    `SELECT json_valid('{"a":1}')`,
			expected: int64(1),
		},
		{
			name:     "json_valid with invalid JSON",
			query:    `SELECT json_valid('{invalid}')`,
			expected: int64(0),
		},
		{
			name:     "json_valid with valid array",
			query:    `SELECT json_valid('[1,2,3]')`,
			expected: int64(1),
		},

		// json_type tests
		{
			name:     "json_type integer",
			query:    `SELECT json_type('123')`,
			expected: "integer",
		},
		{
			name:     "json_type object",
			query:    `SELECT json_type('{"a":1}')`,
			expected: "object",
		},
		{
			name:     "json_type array",
			query:    `SELECT json_type('[1,2,3]')`,
			expected: "array",
		},
		{
			name:     "json_type text",
			query:    `SELECT json_type('"hello"')`,
			expected: "text",
		},
		{
			name:     "json_type null",
			query:    `SELECT json_type('null')`,
			expected: "null",
		},

		// json_extract tests
		{
			name:     "json_extract simple key",
			query:    `SELECT json_extract('{"a":1}', '$.a')`,
			expected: int64(1),
		},
		{
			name:     "json_extract nested",
			query:    `SELECT json_extract('{"a":{"b":{"c":42}}}', '$.a.b.c')`,
			expected: int64(42),
		},
		{
			name:     "json_extract array element",
			query:    `SELECT json_extract('[1,2,3]', '$[1]')`,
			expected: int64(2),
		},
		{
			name:     "json_extract string value",
			query:    `SELECT json_extract('{"name":"John"}', '$.name')`,
			expected: "John",
		},

		// json() tests
		{
			name:     "json minify object",
			query:    `SELECT json('{"a": 1, "b": 2}')`,
			expected: `{"a":1,"b":2}`,
		},
		{
			name:     "json minify array",
			query:    `SELECT json('[1, 2, 3]')`,
			expected: `[1,2,3]`,
		},

		// json_array tests
		{
			name:     "json_array empty",
			query:    `SELECT json_array()`,
			expected: `[]`,
		},
		{
			name:     "json_array single value",
			query:    `SELECT json_array(1)`,
			expected: `[1]`,
		},
		{
			name:     "json_array multiple values",
			query:    `SELECT json_array(1, 'hello', 3.14)`,
			expected: `[1,"hello",3.14]`,
		},

		// json_object tests
		{
			name:     "json_object empty",
			query:    `SELECT json_object()`,
			expected: `{}`,
		},
		{
			name:     "json_object single pair",
			query:    `SELECT json_object('x', 1)`,
			expected: `{"x":1}`,
		},
		{
			name:     "json_object multiple pairs",
			query:    `SELECT json_object('x', 1, 'y', 'hello')`,
			expected: `{"x":1,"y":"hello"}`,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			jsonCompareResult(t, result, tt.expected)
		})
	}
}

// TestJSONFunctionsNULL tests NULL handling in JSON functions
func TestJSONFunctionsNULL(t *testing.T) {
	db, err := sql.Open("sqlite_internal", t.TempDir()+"/test_json_null.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testsNull := []struct {
		name  string
		query string
	}{
		{
			name:  "json_extract with non-existent path returns NULL",
			query: `SELECT json_extract('{"a":1}', '$.b')`,
		},
		{
			name:  "json_type with invalid JSON returns NULL",
			query: `SELECT json_type('{invalid}')`,
		},
	}

	for _, tt := range testsNull {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if result != nil {
				t.Errorf("expected NULL, got %v (type: %T)", result, result)
			}
		})
	}

	// json() with invalid input returns error per SQLite spec
	t.Run("json with invalid JSON returns error", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow(`SELECT json('{invalid}')`).Scan(&result)
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})
}

// jsonValidateAllRows queries rows with (id, valid) columns and checks valid=1.
func jsonValidateAllRows(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, valid int64
		if err := rows.Scan(&id, &valid); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if valid != 1 {
			t.Errorf("row %d: expected valid=1, got %d", id, valid)
		}
	}
}

// jsonSetupUsersTable creates and populates a users table with JSON data.
func jsonSetupUsersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE users (id INTEGER, data TEXT)`,
		`INSERT INTO users VALUES (1, '{"name":"Alice","age":30}')`,
		`INSERT INTO users VALUES (2, '{"name":"Bob","age":25}')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("failed to exec %q: %v", s, err)
		}
	}
}

// TestJSONFunctionsInTable tests JSON functions with table data
func TestJSONFunctionsInTable(t *testing.T) {
	db, err := sql.Open("sqlite_internal", t.TempDir()+"/test_json_table.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	jsonSetupUsersTable(t, db)

	t.Run("extract name from JSON", func(t *testing.T) {
		var name string
		if err := db.QueryRow(`SELECT json_extract(data, '$.name') FROM users WHERE id = 1`).Scan(&name); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if name != "Alice" {
			t.Errorf("expected Alice, got %s", name)
		}
	})

	t.Run("extract age from JSON", func(t *testing.T) {
		var age int64
		if err := db.QueryRow(`SELECT json_extract(data, '$.age') FROM users WHERE id = 2`).Scan(&age); err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if age != 25 {
			t.Errorf("expected 25, got %d", age)
		}
	})

	t.Run("validate all JSON data", func(t *testing.T) {
		jsonValidateAllRows(t, db, `SELECT id, json_valid(data) FROM users ORDER BY id`)
	})
}

// TestJSONFunctionsComplex tests more complex JSON operations
func TestJSONFunctionsComplex(t *testing.T) {
	db, err := sql.Open("sqlite_internal", t.TempDir()+"/test_json_complex.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("nested object extraction", func(t *testing.T) {
		query := `SELECT json_extract('{"user":{"profile":{"email":"test@example.com"}}}', '$.user.profile.email')`
		var email string
		err := db.QueryRow(query).Scan(&email)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if email != "test@example.com" {
			t.Errorf("expected test@example.com, got %s", email)
		}
	})

	t.Run("array within object", func(t *testing.T) {
		query := `SELECT json_extract('{"items":[10,20,30]}', '$.items[1]')`
		var value int64
		err := db.QueryRow(query).Scan(&value)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if value != 20 {
			t.Errorf("expected 20, got %d", value)
		}
	})

	t.Run("create complex object", func(t *testing.T) {
		query := `SELECT json_object('id', 1, 'items', json_array(1, 2, 3), 'active', 1)`
		var result string
		err := db.QueryRow(query).Scan(&result)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Verify it's valid JSON
		valid_query := fmt.Sprintf(`SELECT json_valid('%s')`, result)
		var valid int64
		err = db.QueryRow(valid_query).Scan(&valid)
		if err != nil {
			t.Fatalf("validation query failed: %v", err)
		}

		if valid != 1 {
			t.Errorf("expected valid JSON, got result: %s", result)
		}
	})
}
