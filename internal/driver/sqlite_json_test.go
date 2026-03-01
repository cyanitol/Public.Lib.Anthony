// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteJSON tests SQLite JSON functions
// Converted from contrib/sqlite/sqlite-src-3510200/test/json*.test
func TestSQLiteJSON(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "json_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		// json_array() tests (json101.test:18-20)
		{
			name:  "json_array_simple",
			query: "SELECT json_array(1,2.5,null,'hello')",
			want:  `[1,2.5,null,"hello"]`,
		},
		// json101.test:21-24
		{
			name:  "json_array_string_literal",
			query: `SELECT json_array(1,'{"abc":2.5,"def":null,"ghi":hello}',99)`,
			want:  `[1,"{\"abc\":2.5,\"def\":null,\"ghi\":hello}",99]`,
		},
		// json101.test:25-28
		{
			name:  "json_array_json_object",
			query: `SELECT json_array(1,json('{"abc":2.5,"def":null,"ghi":"hello"}'),99)`,
			want:  `[1,{"abc":2.5,"def":null,"ghi":"hello"},99]`,
		},
		// json101.test:29-32
		{
			name:  "json_array_nested_json_object",
			query: "SELECT json_array(1,json_object('abc',2.5,'def',null,'ghi','hello'),99)",
			want:  `[1,{"abc":2.5,"def":null,"ghi":"hello"},99]`,
		},
		// json101.test:42-52
		{
			name:  "json_array_large",
			query: "SELECT json_array(-9223372036854775808,9223372036854775807,0,1,-1,0.0,1.0,-1.0,-1e99,+2e100,'one','two','three',4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,NULL,21,22,23,24,25,26,27,28,29,30,31,'abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ',99)",
			want:  `[-9223372036854775808,9223372036854775807,0,1,-1,0.0,1.0,-1.0,-1.0e+99,2.0e+100,"one","two","three",4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,null,21,22,23,24,25,26,27,28,29,30,31,"abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ",99]`,
		},

		// json_object() tests (json101.test:65-67)
		{
			name:  "json_object_simple",
			query: "SELECT json_object('a',1,'b',2.5,'c',null,'d','String Test')",
			want:  `{"a":1,"b":2.5,"c":null,"d":"String Test"}`,
		},
		// json101.test:77-79
		{
			name:  "json_object_nested_array",
			query: "SELECT json_object('a',json_array('xyx',77,4.5),'x',2.5)",
			want:  `{"a":["xyx",77,4.5],"x":2.5}`,
		},
		// json101.test:89-91
		{
			name:    "json_object_odd_args",
			query:   "SELECT json_object('a',1,'b')",
			wantErr: true,
		},

		// json_extract() tests (json102.test:139-140)
		{
			name:  "json_extract_root",
			query: `SELECT json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$')`,
			want:  `{"a":2,"c":[4,5,{"f":7}]}`,
		},
		// json102.test:150-152
		{
			name:  "json_extract_array",
			query: `SELECT json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.c')`,
			want:  `[4,5,{"f":7}]`,
		},
		// json102.test:162-164
		{
			name:  "json_extract_array_index",
			query: `SELECT json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.c[2]')`,
			want:  `{"f":7}`,
		},
		// json102.test:174-176
		{
			name:  "json_extract_nested",
			query: `SELECT json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.c[2].f')`,
			want:  "7",
		},
		// json102.test:180-182
		{
			name:  "json_extract_multiple",
			query: `SELECT json_extract('{"a":2,"c":[4,5],"f":7}','$.c','$.a')`,
			want:  `[[4,5],2]`,
		},
		// json102.test:192-194
		{
			name:  "json_extract_missing",
			query: `SELECT json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.x')`,
			want:  "",
		},

		// json_insert() tests (json102.test:210-212)
		{
			name:  "json_insert_existing",
			query: `SELECT json_insert('{"a":2,"c":4}', '$.a', 99)`,
			want:  `{"a":2,"c":4}`,
		},
		// json102.test:222-224
		{
			name:  "json_insert_new",
			query: `SELECT json_insert('{"a":2,"c":4}', '$.e', 99)`,
			want:  `{"a":2,"c":4,"e":99}`,
		},

		// json_replace() tests (json102.test:234-236)
		{
			name:  "json_replace_existing",
			query: `SELECT json_replace('{"a":2,"c":4}', '$.a', 99)`,
			want:  `{"a":99,"c":4}`,
		},
		// json102.test:246-248
		{
			name:  "json_replace_missing",
			query: `SELECT json_replace('{"a":2,"c":4}', '$.e', 99)`,
			want:  `{"a":2,"c":4}`,
		},

		// json_set() tests (json102.test:258-260)
		{
			name:  "json_set_existing",
			query: `SELECT json_set('{"a":2,"c":4}', '$.a', 99)`,
			want:  `{"a":99,"c":4}`,
		},
		// json102.test:270-272
		{
			name:  "json_set_new",
			query: `SELECT json_set('{"a":2,"c":4}', '$.e', 99)`,
			want:  `{"a":2,"c":4,"e":99}`,
		},
		// json102.test:282-284
		{
			name:  "json_set_string_value",
			query: `SELECT json_set('{"a":2,"c":4}', '$.c', '[97,96]')`,
			want:  `{"a":2,"c":"[97,96]"}`,
		},
		// json102.test:294-296
		{
			name:  "json_set_json_value",
			query: `SELECT json_set('{"a":2,"c":4}', '$.c', json('[97,96]'))`,
			want:  `{"a":2,"c":[97,96]}`,
		},

		// json_remove() tests (json102.test:366-368)
		{
			name:  "json_remove_array_element",
			query: `SELECT json_remove('[0,1,2,3,4]','$[2]')`,
			want:  `[0,1,3,4]`,
		},
		// json102.test:378-380
		{
			name:  "json_remove_multiple",
			query: `SELECT json_remove('[0,1,2,3,4]','$[2]','$[0]')`,
			want:  `[1,3,4]`,
		},
		// json102.test:390-392
		{
			name:  "json_remove_order_matters",
			query: `SELECT json_remove('[0,1,2,3,4]','$[0]','$[2]')`,
			want:  `[1,2,4]`,
		},
		// json102.test:426-428
		{
			name:  "json_remove_object_key",
			query: `SELECT json_remove('{"x":25,"y":42}','$.y')`,
			want:  `{"x":25}`,
		},

		// json_type() tests (json102.test:450-452)
		{
			name:  "json_type_object",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}')`,
			want:  "object",
		},
		// json102.test:462-464
		{
			name:  "json_type_array",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a')`,
			want:  "array",
		},
		// json102.test:468-470
		{
			name:  "json_type_integer",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[0]')`,
			want:  "integer",
		},
		// json102.test:474-476
		{
			name:  "json_type_real",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[1]')`,
			want:  "real",
		},
		// json102.test:480-482
		{
			name:  "json_type_true",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[2]')`,
			want:  "true",
		},
		// json102.test:486-488
		{
			name:  "json_type_false",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[3]')`,
			want:  "false",
		},
		// json102.test:492-494
		{
			name:  "json_type_null",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[4]')`,
			want:  "null",
		},
		// json102.test:498-500
		{
			name:  "json_type_text",
			query: `SELECT json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[5]')`,
			want:  "text",
		},

		// json_valid() tests (json102.test:510-512)
		{
			name:  "json_valid_true",
			query: `SELECT json_valid('{"x":35}')`,
			want:  "1",
		},
		// json102.test:513-515
		{
			name:  "json_valid_false",
			query: `SELECT json_valid('{"x":35')`,
			want:  "0",
		},
		// json101.test:395-397
		{
			name:  "json_valid_trailing_comma_object",
			query: `SELECT json_valid('{"a":55,"b":72,}')`,
			want:  "0",
		},
		// json101.test:413-415
		{
			name:  "json_valid_no_trailing_comma",
			query: `SELECT json_valid('{"a":55,"b":72}')`,
			want:  "1",
		},

		// json_quote() tests (json101.test:481-483)
		{
			name:  "json_quote_string",
			query: `SELECT json_quote('abc"xyz')`,
			want:  `"abc\"xyz"`,
		},
		// json101.test:484-486
		{
			name:  "json_quote_number",
			query: `SELECT json_quote(3.14159)`,
			want:  "3.14159",
		},
		// json101.test:487-489
		{
			name:  "json_quote_integer",
			query: `SELECT json_quote(12345)`,
			want:  "12345",
		},
		// json101.test:490-492
		{
			name:  "json_quote_null",
			query: `SELECT json_quote(null)`,
			want:  "null",
		},

		// json_array_length() tests (json102.test:99-101)
		{
			name:  "json_array_length_simple",
			query: `SELECT json_array_length('[1,2,3,4]')`,
			want:  "4",
		},
		// json102.test:111-113
		{
			name:  "json_array_length_with_path",
			query: `SELECT json_array_length('[1,2,3,4]', '$')`,
			want:  "4",
		},
		// json102.test:117-119
		{
			name:  "json_array_length_scalar",
			query: `SELECT json_array_length('[1,2,3,4]', '$[2]')`,
			want:  "0",
		},
		// json102.test:123-125
		{
			name:  "json_array_length_object",
			query: `SELECT json_array_length('{"one":[1,2,3]}')`,
			want:  "0",
		},

		// Operator tests (json102.test:795-797)
		{
			name:  "json_arrow_operator_string",
			query: `SELECT '{"1":"one","2":"two","3":"three"}'->>'2'`,
			want:  "two",
		},
		// json102.test:802-804
		{
			name:  "json_arrow_operator_array_string",
			query: `SELECT '["zero","one","two"]'->>'1'`,
			want:  "",
		},
		// json102.test:805-807
		{
			name:  "json_arrow_operator_array_int",
			query: `SELECT '["zero","one","two"]'->>1`,
			want:  "one",
		},

		// json_group_array() tests (json103.test:17-26)
		{
			name:  "json_group_array_empty",
			query: "CREATE TABLE t1(a,b,c); SELECT json_group_array(a) FROM t1 WHERE a<0",
			want:  "[]",
		},
		// json103.test:38-40
		{
			name:  "json_group_array_basic",
			query: "CREATE TABLE t2(a); INSERT INTO t2 VALUES(1),(2),(3); SELECT json_group_array(a) FROM t2",
			want:  "[1,2,3]",
		},

		// json_group_object() tests (json103.test:42-44)
		{
			name:  "json_group_object_empty",
			query: "CREATE TABLE t3(c,a); SELECT json_group_object(c,a) FROM t3 WHERE a<0",
			want:  "{}",
		},
		// json103.test:49-52
		{
			name:  "json_group_object_basic",
			query: "CREATE TABLE t4(c,a); INSERT INTO t4 VALUES('x',1),('y',2); SELECT json_group_object(c,a) FROM t4",
			want:  `{"x":1,"y":2}`,
		},

		// Additional edge cases
		{
			name:  "json_empty_object",
			query: `SELECT json('{}')`,
			want:  "{}",
		},
		{
			name:  "json_empty_array",
			query: `SELECT json('[]')`,
			want:  "[]",
		},
		{
			name:  "json_nested_objects",
			query: `SELECT json('{"a":{"b":{"c":1}}}')`,
			want:  `{"a":{"b":{"c":1}}}`,
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := ""
			if result.Valid {
				got = result.String
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSQLiteJSONExtract tests json_extract with complex paths
func TestSQLiteJSONExtract(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "json_extract_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with complex JSON (json101.test:193-321)
	_, err = db.Exec(`
		CREATE TABLE j2(id INTEGER PRIMARY KEY, json, src);
		INSERT INTO j2(id,json,src) VALUES(1,'{
			"firstName": "John",
			"lastName": "Smith",
			"isAlive": true,
			"age": 25,
			"address": {
				"streetAddress": "21 2nd Street",
				"city": "New York",
				"state": "NY",
				"postalCode": "10021-3100"
			},
			"phoneNumbers": [
				{
					"type": "home",
					"number": "212 555-1234"
				},
				{
					"type": "office",
					"number": "646 555-4567"
				}
			],
			"children": [],
			"spouse": null
		}','test');
	`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	tests := []struct {
		name  string
		path  string
		want  string
		valid bool
	}{
		{
			name:  "extract_firstName",
			path:  "$.firstName",
			want:  "John",
			valid: true,
		},
		{
			name:  "extract_age",
			path:  "$.age",
			want:  "25",
			valid: true,
		},
		{
			name:  "extract_nested_city",
			path:  "$.address.city",
			want:  "New York",
			valid: true,
		},
		{
			name:  "extract_array_element",
			path:  "$.phoneNumbers[0].number",
			want:  "212 555-1234",
			valid: true,
		},
		{
			name:  "extract_boolean",
			path:  "$.isAlive",
			want:  "1",
			valid: true,
		},
		{
			name:  "extract_null_field",
			path:  "$.spouse",
			want:  "",
			valid: false,
		},
		{
			name:  "extract_empty_array",
			path:  "$.children",
			want:  "[]",
			valid: true,
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			query := "SELECT json_extract(json, ?) FROM j2 WHERE id = 1"
			err := db.QueryRow(query, tt.path).Scan(&result)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Valid != tt.valid {
				t.Errorf("validity: got %v, want %v", result.Valid, tt.valid)
			}

			if result.Valid && result.String != tt.want {
				t.Errorf("value: got %q, want %q", result.String, tt.want)
			}
		})
	}
}
