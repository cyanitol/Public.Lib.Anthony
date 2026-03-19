// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

func TestNullsFirstLastOrderBy(t *testing.T) {
	tests := []sqlTestCase{
		{
			name: "ASC NULLS FIRST - NULLs appear first",
			setup: []string{
				"CREATE TABLE t1 (val INTEGER)",
				"INSERT INTO t1 VALUES (3)",
				"INSERT INTO t1 VALUES (NULL)",
				"INSERT INTO t1 VALUES (1)",
				"INSERT INTO t1 VALUES (NULL)",
				"INSERT INTO t1 VALUES (2)",
			},
			query: "SELECT val FROM t1 ORDER BY val NULLS FIRST",
			wantRows: [][]interface{}{
				{nil},
				{nil},
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		{
			name: "ASC NULLS LAST - NULLs appear last",
			setup: []string{
				"CREATE TABLE t2 (val INTEGER)",
				"INSERT INTO t2 VALUES (3)",
				"INSERT INTO t2 VALUES (NULL)",
				"INSERT INTO t2 VALUES (1)",
				"INSERT INTO t2 VALUES (NULL)",
				"INSERT INTO t2 VALUES (2)",
			},
			query: "SELECT val FROM t2 ORDER BY val NULLS LAST",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{nil},
				{nil},
			},
		},
		{
			name: "DESC NULLS FIRST - NULLs appear first",
			setup: []string{
				"CREATE TABLE t3 (val INTEGER)",
				"INSERT INTO t3 VALUES (3)",
				"INSERT INTO t3 VALUES (NULL)",
				"INSERT INTO t3 VALUES (1)",
				"INSERT INTO t3 VALUES (NULL)",
				"INSERT INTO t3 VALUES (2)",
			},
			query: "SELECT val FROM t3 ORDER BY val DESC NULLS FIRST",
			wantRows: [][]interface{}{
				{nil},
				{nil},
				{int64(3)},
				{int64(2)},
				{int64(1)},
			},
		},
		{
			name: "DESC NULLS LAST - NULLs appear last",
			setup: []string{
				"CREATE TABLE t4 (val INTEGER)",
				"INSERT INTO t4 VALUES (3)",
				"INSERT INTO t4 VALUES (NULL)",
				"INSERT INTO t4 VALUES (1)",
				"INSERT INTO t4 VALUES (NULL)",
				"INSERT INTO t4 VALUES (2)",
			},
			query: "SELECT val FROM t4 ORDER BY val DESC NULLS LAST",
			wantRows: [][]interface{}{
				{int64(3)},
				{int64(2)},
				{int64(1)},
				{nil},
				{nil},
			},
		},
		{
			name: "Default ASC - NULLs first (SQLite default)",
			setup: []string{
				"CREATE TABLE t5 (val INTEGER)",
				"INSERT INTO t5 VALUES (2)",
				"INSERT INTO t5 VALUES (NULL)",
				"INSERT INTO t5 VALUES (1)",
			},
			query: "SELECT val FROM t5 ORDER BY val",
			wantRows: [][]interface{}{
				{nil},
				{int64(1)},
				{int64(2)},
			},
		},
		{
			name: "Default DESC - NULLs last (SQLite default)",
			setup: []string{
				"CREATE TABLE t6 (val INTEGER)",
				"INSERT INTO t6 VALUES (2)",
				"INSERT INTO t6 VALUES (NULL)",
				"INSERT INTO t6 VALUES (1)",
			},
			query: "SELECT val FROM t6 ORDER BY val DESC",
			wantRows: [][]interface{}{
				{int64(2)},
				{int64(1)},
				{nil},
			},
		},
	}

	runSQLTestsFreshDB(t, tests)
}
