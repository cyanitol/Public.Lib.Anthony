// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import "testing"

// TestAlterGen tests ALTER TABLE operations through the driver layer.
func TestAlterGen(t *testing.T) {
	tests := buildAlterGenTests()
	runSQLTestsFreshDB(t, tests)
}

func buildAlterGenTests() []sqlTestCase {
	var tests []sqlTestCase
	tests = append(tests, alterRenameTableTests()...)
	tests = append(tests, alterRenameColumnTests()...)
	tests = append(tests, alterDropColumnTests()...)
	tests = append(tests, alterDropColumnErrorTests()...)
	tests = append(tests, alterWithIndexTests()...)
	tests = append(tests, alterSqliteMasterTests()...)
	return tests
}

func alterRenameTableTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "rename_table_basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1, 'hello')",
				"INSERT INTO t1 VALUES(2, 'world')",
			},
			exec:  "ALTER TABLE t1 RENAME TO t2",
			query: "SELECT a, b FROM t2 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "hello"},
				{int64(2), "world"},
			},
		},
		{
			name: "rename_table_data_preserved",
			setup: []string{
				"CREATE TABLE src(x INTEGER, y INTEGER, z TEXT)",
				"INSERT INTO src VALUES(10, 20, 'abc')",
				"INSERT INTO src VALUES(30, 40, 'def')",
			},
			exec:  "ALTER TABLE src RENAME TO dst",
			query: "SELECT x, y, z FROM dst ORDER BY x",
			wantRows: [][]interface{}{
				{int64(10), int64(20), "abc"},
				{int64(30), int64(40), "def"},
			},
		},
		{
			name: "rename_table_with_index",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE INDEX idx_t1_b ON t1(b)",
				"INSERT INTO t1 VALUES(1, 'x')",
				"INSERT INTO t1 VALUES(2, 'y')",
			},
			exec:  "ALTER TABLE t1 RENAME TO t2",
			query: "SELECT a, b FROM t2 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "x"},
				{int64(2), "y"},
			},
		},
	}
}

func alterRenameColumnTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "rename_column_basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1, 'hello')",
				"INSERT INTO t1 VALUES(2, 'world')",
			},
			exec:  "ALTER TABLE t1 RENAME COLUMN b TO c",
			query: "SELECT a, c FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "hello"},
				{int64(2), "world"},
			},
		},
		{
			name: "rename_column_data_preserved",
			setup: []string{
				"CREATE TABLE items(id INTEGER, name TEXT, price INTEGER)",
				"INSERT INTO items VALUES(1, 'widget', 100)",
				"INSERT INTO items VALUES(2, 'gadget', 200)",
			},
			exec:  "ALTER TABLE items RENAME COLUMN price TO cost",
			query: "SELECT id, name, cost FROM items ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), "widget", int64(100)},
				{int64(2), "gadget", int64(200)},
			},
		},
		{
			name: "rename_column_duplicate_error",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
			},
			exec:    "ALTER TABLE t1 RENAME COLUMN b TO a",
			wantErr: true,
			errLike: "already exists",
		},
		{
			name: "rename_column_not_found_error",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
			},
			exec:    "ALTER TABLE t1 RENAME COLUMN z TO w",
			wantErr: true,
			errLike: "not found",
		},
	}
}

func alterDropColumnTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "drop_column_basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT, c REAL)",
				"INSERT INTO t1 VALUES(1, 'hello', 3.14)",
				"INSERT INTO t1 VALUES(2, 'world', 2.72)",
			},
			exec:  "ALTER TABLE t1 DROP COLUMN c",
			query: "SELECT a, b FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "hello"},
				{int64(2), "world"},
			},
		},
		{
			name: "drop_middle_column",
			skip: "DROP COLUMN does not rebuild table data; existing rows retain old column layout",
			setup: []string{
				"CREATE TABLE t1(x INTEGER, y TEXT, z INTEGER)",
				"INSERT INTO t1 VALUES(1, 'mid', 100)",
			},
			exec:  "ALTER TABLE t1 DROP COLUMN y",
			query: "SELECT x, z FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(100)},
			},
		},
	}
}

func alterDropColumnErrorTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "drop_last_column_error",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
			},
			exec:    "ALTER TABLE t1 DROP COLUMN a",
			wantErr: true,
			errLike: "last column",
		},
		{
			name: "drop_nonexistent_column_error",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
			},
			exec:    "ALTER TABLE t1 DROP COLUMN z",
			wantErr: true,
			errLike: "not found",
		},
	}
}

func alterWithIndexTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "rename_column_with_index_query_works",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT, c INTEGER)",
				"CREATE INDEX idx_t1_b ON t1(b)",
				"INSERT INTO t1 VALUES(1, 'alpha', 10)",
				"INSERT INTO t1 VALUES(2, 'beta', 20)",
			},
			exec:  "ALTER TABLE t1 RENAME COLUMN b TO label",
			query: "SELECT a, label, c FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), "alpha", int64(10)},
				{int64(2), "beta", int64(20)},
			},
		},
	}
}

func alterSqliteMasterTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "rename_table_updates_schema",
			setup: []string{
				"CREATE TABLE original(a INTEGER, b TEXT)",
			},
			exec:  "ALTER TABLE original RENAME TO renamed",
			query: "SELECT name FROM sqlite_master WHERE type='table' AND name='renamed'",
			wantRows: [][]interface{}{
				{"renamed"},
			},
			skip: "sqlite_master query not yet supported for in-memory schema",
		},
		{
			name: "rename_column_schema_visible",
			setup: []string{
				"CREATE TABLE t1(old_name INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1, 'x')",
			},
			exec:  "ALTER TABLE t1 RENAME COLUMN old_name TO new_name",
			query: "SELECT new_name, b FROM t1",
			wantRows: [][]interface{}{
				{int64(1), "x"},
			},
		},
		{
			name: "drop_column_schema_visible",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT, c REAL)",
				"INSERT INTO t1 VALUES(1, 'x', 1.5)",
			},
			exec:  "ALTER TABLE t1 DROP COLUMN c",
			query: "SELECT a, b FROM t1",
			wantRows: [][]interface{}{
				{int64(1), "x"},
			},
		},
	}
}
