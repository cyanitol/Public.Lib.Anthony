// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import "testing"

func TestJSONArrowOperators(t *testing.T) {
	tests := []sqlTestCase{
		{
			name:     "arrow_extract_integer",
			query:    `SELECT '{"a":1}' -> '$.a'`,
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name:     "arrow_extract_string",
			query:    `SELECT '{"a":"hello"}' -> '$.a'`,
			wantRows: [][]interface{}{{"hello"}},
		},
		{
			name:     "double_arrow_extract_text",
			query:    `SELECT '{"a":"hello"}' ->> '$.a'`,
			wantRows: [][]interface{}{{"hello"}},
		},
		{
			name:     "double_arrow_extract_integer",
			query:    `SELECT '{"a":1}' ->> '$.a'`,
			wantRows: [][]interface{}{{"1"}},
		},
		{
			name:  "arrow_with_table_column",
			setup: []string{
				"CREATE TABLE t1(data TEXT)",
				`INSERT INTO t1 VALUES ('{"name":"Alice","age":30}')`,
			},
			query:    `SELECT data -> '$.name' FROM t1`,
			wantRows: [][]interface{}{{"Alice"}},
		},
		{
			name:  "double_arrow_with_table_column",
			setup: []string{
				"CREATE TABLE t2(data TEXT)",
				`INSERT INTO t2 VALUES ('{"name":"Alice","age":30}')`,
			},
			query:    `SELECT data ->> '$.name' FROM t2`,
			wantRows: [][]interface{}{{"Alice"}},
		},
		{
			name:  "double_arrow_numeric_from_table",
			setup: []string{
				"CREATE TABLE t3(data TEXT)",
				`INSERT INTO t3 VALUES ('{"name":"Alice","age":30}')`,
			},
			query:    `SELECT data ->> '$.age' FROM t3`,
			wantRows: [][]interface{}{{"30"}},
		},
		{
			name:     "arrow_nested_path",
			query:    `SELECT '{"a":{"b":42}}' -> '$.a.b'`,
			wantRows: [][]interface{}{{int64(42)}},
		},
		{
			name:     "double_arrow_nested_path",
			query:    `SELECT '{"a":{"b":"deep"}}' ->> '$.a.b'`,
			wantRows: [][]interface{}{{"deep"}},
		},
		{
			name:     "arrow_null_value",
			query:    `SELECT '{"a":null}' ->> '$.a'`,
			wantRows: [][]interface{}{{nil}},
		},
		{
			name:     "arrow_missing_key",
			query:    `SELECT '{"a":1}' ->> '$.b'`,
			wantRows: [][]interface{}{{nil}},
		},
	}

	runSQLTestsFreshDB(t, tests)
}
