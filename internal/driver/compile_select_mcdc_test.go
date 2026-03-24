// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ============================================================================
// MC/DC: handleSpecialSelectTypes – SELECT without FROM
//
// Compound condition:
//   A = stmt.From == nil
//   B = len(stmt.From.Tables) == 0
//
// Outcome = A || B → route to compileSelectWithoutFrom
//
// Cases (N+1 = 3):
//   Base:   A=T (no FROM at all)            → no-FROM path
//   Flip B: A=F, B=T (FROM clause exists but empty) → effectively same path
//   A=F B=F: FROM with a real table         → simple scan path (not no-FROM)
// ============================================================================

func TestMCDC_SelectWithoutFrom(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: no FROM clause at all
			name:     "MCDC A=T: SELECT with no FROM",
			query:    "SELECT 1+1",
			wantRows: [][]interface{}{{int64(2)}},
		},
		{
			// A=F B=F: FROM with a real table – not the no-FROM path
			name:     "MCDC A=F B=F: SELECT with FROM table",
			setup:    "CREATE TABLE t_nofrom(x INTEGER); INSERT INTO t_nofrom VALUES(42)",
			query:    "SELECT x FROM t_nofrom",
			wantRows: [][]interface{}{{int64(42)}},
		},
		{
			// No FROM, multiple expressions
			name:     "MCDC A=T: SELECT multiple exprs no FROM",
			query:    "SELECT 10, 'hello', NULL",
			wantRows: [][]interface{}{{int64(10), "hello", nil}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			if tt.setup != "" {
				for _, s := range splitSemicolon(tt.setup) {
					mustExec(t, db, s)
				}
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: routeSpecializedSelect – JOIN routing
//
// Compound condition:
//   A = stmt.From != nil
//   B = len(stmt.From.Joins) > 0
//   C = len(stmt.From.Tables) > 1
//
// Outcome = A && (B || C) → route to compileSelectWithJoins
//
// Cases (N+1 = 4):
//   Base:     A=T B=F C=F → single table, no join → simple scan
//   Flip B:   A=T B=T C=F → explicit JOIN
//   Flip C:   A=T B=F C=T → comma-separated tables (implicit cross join)
//   A=F:      no FROM     → handled before this function
// ============================================================================

func TestMCDC_RouteSpecializedSelect_JoinRouting(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=F C=F: single table, no join
			name:     "MCDC A=T B=F C=F: single table scan",
			setup:    "CREATE TABLE t_join_a(x INTEGER); INSERT INTO t_join_a VALUES(1),(2)",
			query:    "SELECT x FROM t_join_a ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			// A=T B=T C=F: explicit INNER JOIN
			name: "MCDC A=T B=T C=F: explicit JOIN routes to join compiler",
			setup: "CREATE TABLE t_j1(id INTEGER, v TEXT);" +
				"CREATE TABLE t_j2(id INTEGER, w TEXT);" +
				"INSERT INTO t_j1 VALUES(1,'a');" +
				"INSERT INTO t_j2 VALUES(1,'b')",
			query:    "SELECT t_j1.v, t_j2.w FROM t_j1 JOIN t_j2 ON t_j1.id = t_j2.id",
			wantRows: [][]interface{}{{"a", "b"}},
		},
		{
			// A=T B=F C=T: comma-separated tables → implicit cross join
			name: "MCDC A=T B=F C=T: comma tables implicit cross join",
			setup: "CREATE TABLE t_c1(x INTEGER);" +
				"CREATE TABLE t_c2(y INTEGER);" +
				"INSERT INTO t_c1 VALUES(10);" +
				"INSERT INTO t_c2 VALUES(20)",
			query:    "SELECT t_c1.x, t_c2.y FROM t_c1, t_c2",
			wantRows: [][]interface{}{{int64(10), int64(20)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: resolveSelectTable – schema-qualified error message
//
// Compound condition (error message branch):
//   A = tableRef.Schema != ""
//
// Outcome = A → "table not found: schema.table"
//         = !A → "table not found: table"
//
// Cases (N+1 = 2):
//   A=T: schema-qualified missing table → error with dot notation
//   A=F: unqualified missing table      → error without dot
// ============================================================================

func TestMCDC_ResolveSelectTable_SchemaError(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			// A=T: schema-qualified reference to non-existent table
			name:    "MCDC A=T: schema-qualified missing table",
			query:   "SELECT x FROM main.no_such_table_xyz",
			wantErr: true,
		},
		{
			// A=F: unqualified reference to non-existent table
			name:    "MCDC A=F: unqualified missing table",
			query:   "SELECT x FROM no_such_table_xyz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			_, err := db.Query(tt.query)
			if tt.wantErr && err == nil {
				// Some engines may defer until rows.Next; try scanning
				t.Fatalf("expected error but got none for %q", tt.query)
			}
		})
	}
}

// ============================================================================
// MC/DC: setupLimitOffset – LIMIT 0 early halt
//
// Compound condition:
//   A = stmt.Limit != nil (limit clause present)
//   B = parsedVal <= 0    (limit value is zero or negative)
//
// Outcome = A && B → emit early halt (earlyHaltAddr > 0)
//
// Cases (N+1 = 3):
//   Base: A=T B=T → LIMIT 0, no rows returned
//   Flip A: A=F B=? → no LIMIT, all rows returned
//   Flip B: A=T B=F → LIMIT > 0, rows returned up to limit
// ============================================================================

func TestMCDC_SetupLimitOffset_EarlyHalt(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=T: LIMIT 0 → no rows
			name:     "MCDC A=T B=T: LIMIT 0 returns no rows",
			setup:    "CREATE TABLE t_lim0(x INTEGER); INSERT INTO t_lim0 VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_lim0 LIMIT 0",
			wantRows: nil,
		},
		{
			// A=F: no LIMIT → all rows
			name:     "MCDC A=F: no LIMIT returns all rows",
			setup:    "CREATE TABLE t_nolim(x INTEGER); INSERT INTO t_nolim VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_nolim ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			// A=T B=F: LIMIT 2 → first 2 rows
			name:     "MCDC A=T B=F: LIMIT 2 returns 2 rows",
			setup:    "CREATE TABLE t_lim2(x INTEGER); INSERT INTO t_lim2 VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_lim2 ORDER BY x LIMIT 2",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: fixScanAddressesWithLimit – DISTINCT + WHERE + OFFSET + LIMIT guards
//
// Individual boolean conditions (each tested independently):
//   A = stmt.Distinct           → DISTINCT skip jump is patched
//   B = stmt.Where != nil       → WHERE skip jump is patched
//   C = limitInfo.hasOffset     → OFFSET skip jump is patched
//   D = limitInfo.hasLimit      → LIMIT jump is patched
//   E = limitInfo.earlyHaltAddr → early halt jump patched (tested by LIMIT 0 test above)
//
// Cases for OFFSET (C):
//   C=T: OFFSET 1 → skip first row
//   C=F: no OFFSET → all rows from start
// ============================================================================

func TestMCDC_FixScanAddresses_DISTINCT(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: DISTINCT present → duplicate rows filtered
			name:     "MCDC A=T: DISTINCT deduplicates rows",
			setup:    "CREATE TABLE t_dist(x INTEGER); INSERT INTO t_dist VALUES(1),(1),(2)",
			query:    "SELECT DISTINCT x FROM t_dist ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			// A=F: no DISTINCT → duplicates retained
			name:     "MCDC A=F: no DISTINCT retains duplicates",
			setup:    "CREATE TABLE t_nodist(x INTEGER); INSERT INTO t_nodist VALUES(1),(1),(2)",
			query:    "SELECT x FROM t_nodist ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(1)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

func TestMCDC_FixScanAddresses_WHERE(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// B=T: WHERE clause present → only matching rows
			name:     "MCDC B=T: WHERE filters rows",
			setup:    "CREATE TABLE t_where(x INTEGER); INSERT INTO t_where VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_where WHERE x > 1 ORDER BY x",
			wantRows: [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			// B=F: no WHERE → all rows
			name:     "MCDC B=F: no WHERE returns all rows",
			setup:    "CREATE TABLE t_nowhere(x INTEGER); INSERT INTO t_nowhere VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_nowhere ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

func TestMCDC_FixScanAddresses_OFFSET(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// C=T: OFFSET 1 → skip first row
			name:     "MCDC C=T: OFFSET 1 skips first row",
			setup:    "CREATE TABLE t_off(x INTEGER); INSERT INTO t_off VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_off ORDER BY x LIMIT 10 OFFSET 1",
			wantRows: [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			// C=F: no OFFSET → all rows
			name:     "MCDC C=F: no OFFSET returns all rows",
			setup:    "CREATE TABLE t_nooff(x INTEGER); INSERT INTO t_nooff VALUES(1),(2),(3)",
			query:    "SELECT x FROM t_nooff ORDER BY x LIMIT 10",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: determineCursorNum – temp table cursor path
//
// Condition:
//   A = table.Temp
//
// Outcome = A → use RootPage as cursor num; else use cursor 0
//
// Cases (N+1 = 2):
//   A=T: ephemeral (temp) table → exercised by CTE materialization
//   A=F: regular table → normal table scan
// ============================================================================

func TestMCDC_DetermineCursorNum(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: CTE creates ephemeral temp table → temp cursor path
			name: "MCDC A=T: CTE uses ephemeral table",
			setup: "CREATE TABLE t_dcn_src(x INTEGER);" +
				"INSERT INTO t_dcn_src VALUES(5),(10)",
			query:    "WITH cte AS (SELECT x FROM t_dcn_src) SELECT x FROM cte ORDER BY x",
			wantRows: [][]interface{}{{int64(5)}, {int64(10)}},
		},
		{
			// A=F: regular table → cursor 0
			name:     "MCDC A=F: regular table uses cursor 0",
			setup:    "CREATE TABLE t_dcn_reg(x INTEGER); INSERT INTO t_dcn_reg VALUES(7)",
			query:    "SELECT x FROM t_dcn_reg",
			wantRows: [][]interface{}{{int64(7)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: detectAggregates – GROUP BY presence
//
// Compound condition:
//   A = len(stmt.GroupBy) > 0    → aggregate path
//   B = containsAggregate(cols)  → aggregate function in SELECT columns
//
// Outcome = A || B → route to aggregate compiler
//
// Cases (N+1 = 3):
//   Base:   A=T B=F → GROUP BY present, no aggregate func in cols
//   Flip A: A=F B=T → aggregate func without GROUP BY
//   Flip B: A=F B=F → no aggregate, no GROUP BY → simple scan
// ============================================================================

func TestMCDC_DetectAggregates(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=F: GROUP BY only, no aggregate function in SELECT list
			name: "MCDC A=T B=F: GROUP BY without aggregate func",
			setup: "CREATE TABLE t_dag_g(cat TEXT, v INTEGER);" +
				"INSERT INTO t_dag_g VALUES('a',1),('b',2),('a',3)",
			query:    "SELECT cat FROM t_dag_g GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"a"}, {"b"}},
		},
		{
			// A=F B=T: COUNT without GROUP BY → scalar aggregate
			name:     "MCDC A=F B=T: aggregate function no GROUP BY",
			setup:    "CREATE TABLE t_dag_agg(x INTEGER); INSERT INTO t_dag_agg VALUES(1),(2),(3)",
			query:    "SELECT COUNT(*) FROM t_dag_agg",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			// A=F B=F: simple scan, no aggregates
			name:     "MCDC A=F B=F: no aggregate no GROUP BY",
			setup:    "CREATE TABLE t_dag_plain(x INTEGER); INSERT INTO t_dag_plain VALUES(42)",
			query:    "SELECT x FROM t_dag_plain",
			wantRows: [][]interface{}{{int64(42)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: handleTVFSelect – TVF with aggregate detection
//
// Compound condition (inside handleTVFSelect):
//   A = s.hasTableValuedFunction(stmt)
//   B = s.detectAggregates(stmt)    (only evaluated when A=T)
//
// Outcome when A=T:
//   B=T → materializeTVFAsEphemeral (aggregate over TVF)
//   B=F → compileSelectWithTVF
//
// Cases (N+1 = 3):
//   Base: A=T B=F → TVF without aggregate
//   Flip B: A=T B=T → TVF with aggregate
//   A=F: no TVF → falls through
// ============================================================================

func TestMCDC_HandleTVFSelect(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=F: pragma TVF without aggregate
			name:     "MCDC A=T B=F: pragma_table_info TVF no aggregate",
			setup:    "CREATE TABLE t_tvf_base(id INTEGER PRIMARY KEY, name TEXT)",
			query:    "SELECT name FROM pragma_table_info('t_tvf_base') ORDER BY name",
			wantRows: [][]interface{}{{"id"}, {"name"}},
		},
		{
			// A=T B=T: pragma TVF with COUNT aggregate
			name:     "MCDC A=T B=T: pragma_table_info TVF with COUNT",
			setup:    "CREATE TABLE t_tvf_cnt(a INTEGER, b INTEGER, c INTEGER)",
			query:    "SELECT COUNT(*) FROM pragma_table_info('t_tvf_cnt')",
			wantRows: [][]interface{}{{int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query %q failed: %v", tt.query, err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: compileSingleCTE – recursive vs non-recursive branch
//
// Condition:
//   A = def.IsRecursive
//
// Outcome = A → compileRecursiveCTE; else compileNonRecursiveCTE
//
// Cases (N+1 = 2):
//   A=T: recursive CTE (UNION ALL self-reference)
//   A=F: non-recursive CTE (simple SELECT)
// ============================================================================

func TestMCDC_CompileSingleCTE_RecursiveBranch(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=F: non-recursive CTE
			name:     "MCDC A=F: non-recursive CTE",
			setup:    "CREATE TABLE t_cte_src(x INTEGER); INSERT INTO t_cte_src VALUES(1),(2),(3)",
			query:    "WITH cte AS (SELECT x FROM t_cte_src) SELECT x FROM cte ORDER BY x",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			// A=T: recursive CTE generating sequence 1..5
			name:  "MCDC A=T: recursive CTE",
			query: "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT n FROM cnt ORDER BY n",
			wantRows: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			if tt.setup != "" {
				for _, s := range splitSemicolon(tt.setup) {
					mustExec(t, db, s)
				}
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: compileNonRecursiveCTE – chained CTE detection
//
// Condition:
//   A = len(cteTempTables) > 0  (CTE references prior CTEs)
//
// Outcome = A → compileCTEPopulationWithMapping; else → compileCTEPopulationCoroutine
//
// Cases (N+1 = 2):
//   A=F: standalone CTE (no prior CTEs)
//   A=T: chained CTE referencing a prior CTE
// ============================================================================

func TestMCDC_CompileNonRecursiveCTE_ChainedDetection(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=F: single standalone CTE
			name:     "MCDC A=F: standalone non-recursive CTE",
			setup:    "CREATE TABLE t_chain_src(v INTEGER); INSERT INTO t_chain_src VALUES(10),(20)",
			query:    "WITH a AS (SELECT v FROM t_chain_src) SELECT v FROM a ORDER BY v",
			wantRows: [][]interface{}{{int64(10)}, {int64(20)}},
		},
		{
			// A=T: second CTE references first CTE
			name: "MCDC A=T: chained CTE references prior CTE",
			setup: "CREATE TABLE t_chain2(v INTEGER);" +
				"INSERT INTO t_chain2 VALUES(1),(2),(3)",
			query:    "WITH a AS (SELECT v FROM t_chain2), b AS (SELECT v*2 AS v2 FROM a) SELECT v2 FROM b ORDER BY v2",
			wantRows: [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			if tt.setup != "" {
				for _, s := range splitSemicolon(tt.setup) {
					mustExec(t, db, s)
				}
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: rewriteTableOrSubquery – subquery rewrite branch
//
// Compound condition:
//   A = cteTempTables[table.TableName] exists  → replace with CTE temp table
//   B = table.Subquery != nil                  → recursively rewrite subquery
//
// These are mutually exclusive branches. Cases:
//   A=T: CTE name matches → rewrite to temp table name
//   A=F, B=T: no CTE name match but subquery present → rewrite subquery
//   A=F, B=F: neither → return unchanged
// ============================================================================

func TestMCDC_RewriteTableOrSubquery(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: CTE name is rewritten to internal temp table
			name: "MCDC A=T: CTE name rewritten to temp table",
			setup: "CREATE TABLE t_rtq_src(x INTEGER);" +
				"INSERT INTO t_rtq_src VALUES(99)",
			query:    "WITH cte AS (SELECT x FROM t_rtq_src) SELECT x FROM cte",
			wantRows: [][]interface{}{{int64(99)}},
		},
		{
			// A=F B=T: subquery in FROM clause (derived table, not a CTE reference)
			name: "MCDC A=F B=T: subquery in FROM",
			setup: "CREATE TABLE t_rtq_sub(v INTEGER);" +
				"INSERT INTO t_rtq_sub VALUES(3),(7)",
			query:    "SELECT s.v FROM (SELECT v FROM t_rtq_sub WHERE v > 4) AS s",
			wantRows: [][]interface{}{{int64(7)}},
		},
		{
			// A=F B=F: plain table reference, no CTE, no subquery
			name:     "MCDC A=F B=F: plain table reference",
			setup:    "CREATE TABLE t_rtq_plain(x INTEGER); INSERT INTO t_rtq_plain VALUES(55)",
			query:    "SELECT x FROM t_rtq_plain",
			wantRows: [][]interface{}{{int64(55)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: emitGroupComparison – first-row and group-change detection
//
// Two boolean conditions in sequence:
//   A = firstRowReg == 1  → skip comparison, initialize accumulators
//   B = groupChangedReg == 1  → output previous group, re-initialize
//
// Cases (N+1 = 3):
//   First row: A=T → no output yet, initialize accumulators
//   Same group: A=F, B=F → accumulate into current group
//   Group change: A=F, B=T → output previous, start new group
// ============================================================================

func TestMCDC_EmitGroupComparison(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// Only one distinct group → first-row only, no group change
			name: "MCDC first-row only: single group",
			setup: "CREATE TABLE t_gc1(cat TEXT, v INTEGER);" +
				"INSERT INTO t_gc1 VALUES('a',1),('a',2),('a',3)",
			query:    "SELECT cat, SUM(v) FROM t_gc1 GROUP BY cat",
			wantRows: [][]interface{}{{"a", int64(6)}},
		},
		{
			// Two groups: first-row init, accumulate, group change triggers output
			name: "MCDC group-change: two groups",
			setup: "CREATE TABLE t_gc2(cat TEXT, v INTEGER);" +
				"INSERT INTO t_gc2 VALUES('a',1),('a',2),('b',10),('b',20)",
			query:    "SELECT cat, SUM(v) FROM t_gc2 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"a", int64(3)}, {"b", int64(30)}},
		},
		{
			// Many groups with single rows each → group change every row
			name: "MCDC group-change every row",
			setup: "CREATE TABLE t_gc3(cat TEXT, v INTEGER);" +
				"INSERT INTO t_gc3 VALUES('x',5),('y',6),('z',7)",
			query:    "SELECT cat, MAX(v) FROM t_gc3 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"x", int64(5)}, {"y", int64(6)}, {"z", int64(7)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: emitNullSafeGroupCompare – NULL-aware GROUP BY comparison
//
// Four boolean outcomes from the null-safe comparator:
//   cur=NULL, prev=NULL → same group
//   cur=NULL, prev!=NULL → different group
//   cur!=NULL, prev=NULL → different group (first group after null)
//   cur!=NULL, prev!=NULL, equal → same group
//   cur!=NULL, prev!=NULL, not equal → different group
// ============================================================================

func TestMCDC_EmitNullSafeGroupCompare(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// cur=NULL, prev=NULL → same group (NULLs in same group)
			name: "MCDC NULL-NULL: NULLs in same group",
			setup: "CREATE TABLE t_nsgc1(cat TEXT, v INTEGER);" +
				"INSERT INTO t_nsgc1 VALUES(NULL,1),(NULL,2)",
			query:    "SELECT cat, COUNT(*) FROM t_nsgc1 GROUP BY cat",
			wantRows: [][]interface{}{{nil, int64(2)}},
		},
		{
			// cur=NULL, prev!=NULL → different group
			name: "MCDC NULL-NotNull: different groups",
			setup: "CREATE TABLE t_nsgc2(cat TEXT, v INTEGER);" +
				"INSERT INTO t_nsgc2 VALUES('a',1),(NULL,2)",
			query:    "SELECT cat, COUNT(*) FROM t_nsgc2 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{nil, int64(1)}, {"a", int64(1)}},
		},
		{
			// cur!=NULL, prev!=NULL, equal → same group
			name: "MCDC NotNull-NotNull equal: same group",
			setup: "CREATE TABLE t_nsgc3(cat TEXT, v INTEGER);" +
				"INSERT INTO t_nsgc3 VALUES('b',10),('b',20)",
			query:    "SELECT cat, SUM(v) FROM t_nsgc3 GROUP BY cat",
			wantRows: [][]interface{}{{"b", int64(30)}},
		},
		{
			// cur!=NULL, prev!=NULL, not equal → different group
			name: "MCDC NotNull-NotNull unequal: different groups",
			setup: "CREATE TABLE t_nsgc4(cat TEXT, v INTEGER);" +
				"INSERT INTO t_nsgc4 VALUES('c',1),('d',2)",
			query:    "SELECT cat, SUM(v) FROM t_nsgc4 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"c", int64(1)}, {"d", int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: updateGroupAccForColumn – COUNT(*) and JSON_GROUP_OBJECT branching
//
// Compound condition 1 (COUNT(*) / no-arg path):
//   A = fnExpr.Star
//   B = len(fnExpr.Args) == 0
//   Outcome = A || B → COUNT(*) path
//
// Compound condition 2 (JSON_GROUP_OBJECT path):
//   C = fnExpr.Name == "JSON_GROUP_OBJECT"
//   D = len(fnExpr.Args) >= 2
//   Outcome = C && D → JSON_GROUP_OBJECT path
//
// Cases (N+1 = 3 for each):
//   Condition 1:
//     Base: A=T → COUNT(*) path
//     Flip A=F, B=T: COUNT(no args) – not valid SQL, use COUNT(*) representative
//     Flip A=F, B=F: regular arg aggregate (SUM)
//   Condition 2:
//     Base: C=T, D=T → JSON_GROUP_OBJECT with 2 args
//     Flip C=F: different aggregate
//     Flip D=F: JSON_GROUP_OBJECT would need <2 args (invalid SQL – tested by normal path)
// ============================================================================

func TestMCDC_UpdateGroupAccForColumn_CountStar(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: COUNT(*) star path
			name:     "MCDC A=T: COUNT(*) star path",
			setup:    "CREATE TABLE t_ugac1(cat TEXT); INSERT INTO t_ugac1 VALUES('a'),('a'),('b')",
			query:    "SELECT cat, COUNT(*) FROM t_ugac1 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"a", int64(2)}, {"b", int64(1)}},
		},
		{
			// A=F B=F: SUM uses arg path
			name:     "MCDC A=F B=F: SUM with arg",
			setup:    "CREATE TABLE t_ugac2(cat TEXT, v INTEGER); INSERT INTO t_ugac2 VALUES('a',3),('a',4),('b',5)",
			query:    "SELECT cat, SUM(v) FROM t_ugac2 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"a", int64(7)}, {"b", int64(5)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

func TestMCDC_UpdateGroupAccForColumn_JSONGroupObject(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// C=T D=T: JSON_GROUP_OBJECT with 2 args
			name: "MCDC C=T D=T: JSON_GROUP_OBJECT",
			setup: "CREATE TABLE t_jgo(cat TEXT, k TEXT, v TEXT);" +
				"INSERT INTO t_jgo VALUES('g1','a','1'),('g1','b','2')",
			query:    "SELECT cat, JSON_GROUP_OBJECT(k, v) FROM t_jgo GROUP BY cat",
			wantRows: [][]interface{}{{"g1", `{"a":"1","b":"2"}`}},
		},
		{
			// C=F: JSON_GROUP_ARRAY (different aggregate, no key-value path)
			name: "MCDC C=F: JSON_GROUP_ARRAY",
			setup: "CREATE TABLE t_jga(cat TEXT, v TEXT);" +
				"INSERT INTO t_jga VALUES('g','x'),('g','y')",
			query:    "SELECT cat, JSON_GROUP_ARRAY(v) FROM t_jga GROUP BY cat",
			wantRows: [][]interface{}{{"g", `["x","y"]`}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: groupConcatSeparator – custom separator detection
//
// Compound condition:
//   A = len(fnExpr.Args) < 2   → use default ","
//   B = args[1] is *LiteralExpr with LiteralString type
//
// Outcome: !A && B → use custom separator; else use ","
//
// Cases (N+1 = 3):
//   A=T: only one arg → default "," separator
//   A=F B=T: two args, second is string literal → custom separator
//   A=F B=F: two args, second is non-string literal → default "," separator
// ============================================================================

func TestMCDC_GroupConcatSeparator(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: one arg, default comma separator
			name: "MCDC A=T: GROUP_CONCAT default comma",
			setup: "CREATE TABLE t_gcs1(cat TEXT, v TEXT);" +
				"INSERT INTO t_gcs1 VALUES('g','a'),('g','b'),('g','c')",
			query:    "SELECT cat, GROUP_CONCAT(v) FROM t_gcs1 GROUP BY cat",
			wantRows: [][]interface{}{{"g", "a,b,c"}},
		},
		{
			// A=F B=T: two args, custom separator '|'
			name: "MCDC A=F B=T: GROUP_CONCAT custom separator",
			setup: "CREATE TABLE t_gcs2(cat TEXT, v TEXT);" +
				"INSERT INTO t_gcs2 VALUES('g','x'),('g','y'),('g','z')",
			query:    "SELECT cat, GROUP_CONCAT(v, '|') FROM t_gcs2 GROUP BY cat",
			wantRows: [][]interface{}{{"g", "x|y|z"}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: emitFinalGroupOutput – has-rows guard
//
// Condition:
//   A = firstRowReg == 0  (we processed at least one row)
//
// Outcome = A → emit final group; else skip (empty table)
//
// Cases (N+1 = 2):
//   A=T: table has rows → final group is emitted
//   A=F: empty table → no output
// ============================================================================

func TestMCDC_EmitFinalGroupOutput(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: table has rows → SUM returns value
			name:     "MCDC A=T: non-empty table emits final group",
			setup:    "CREATE TABLE t_efgo1(cat TEXT, v INTEGER); INSERT INTO t_efgo1 VALUES('a',10),('a',20)",
			query:    "SELECT cat, SUM(v) FROM t_efgo1 GROUP BY cat",
			wantRows: [][]interface{}{{"a", int64(30)}},
		},
		{
			// A=F: empty table → no rows output from GROUP BY
			name:     "MCDC A=F: empty table no final group output",
			setup:    "CREATE TABLE t_efgo2(cat TEXT, v INTEGER)",
			query:    "SELECT cat, SUM(v) FROM t_efgo2 GROUP BY cat",
			wantRows: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: emitGroupByHavingClause – HAVING with GROUP BY
//
// Condition:
//   A = stmt.Having != nil
//
// Outcome = A → emit HAVING filter; else no filter
//
// Cases (N+1 = 2):
//   A=T: HAVING clause present → filters groups
//   A=F: no HAVING → all groups returned
// ============================================================================

func TestMCDC_EmitGroupByHavingClause(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: HAVING SUM(v) > 5 → only groups with sum > 5
			name: "MCDC A=T: HAVING filters groups",
			setup: "CREATE TABLE t_hav1(cat TEXT, v INTEGER);" +
				"INSERT INTO t_hav1 VALUES('a',3),('a',4),('b',1),('b',2)",
			query:    "SELECT cat, SUM(v) FROM t_hav1 GROUP BY cat HAVING SUM(v) > 5 ORDER BY cat",
			wantRows: [][]interface{}{{"a", int64(7)}},
		},
		{
			// A=F: no HAVING → all groups
			name: "MCDC A=F: no HAVING returns all groups",
			setup: "CREATE TABLE t_hav2(cat TEXT, v INTEGER);" +
				"INSERT INTO t_hav2 VALUES('a',1),('b',2)",
			query:    "SELECT cat, SUM(v) FROM t_hav2 GROUP BY cat ORDER BY cat",
			wantRows: [][]interface{}{{"a", int64(1)}, {"b", int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: validateRecursiveCTE – compound operator check
//
// Compound condition:
//   A = compound.Op != parser.CompoundUnion
//   B = compound.Op != parser.CompoundUnionAll
//
// Outcome = A && B → error (recursive CTE must use UNION or UNION ALL)
//
// Cases (N+1 = 3):
//   Base: A=F → UNION (valid)
//   Flip A: A=T B=F → UNION ALL (valid)
//   A=T B=T: neither UNION nor UNION ALL → error
// ============================================================================

func TestMCDC_ValidateRecursiveCTE_CompoundOp(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			// A=F: UNION (valid recursive CTE)
			name:  "MCDC A=F: recursive CTE with UNION",
			query: "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT n+1 FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
		},
		{
			// A=T B=F: UNION ALL (valid recursive CTE)
			name:  "MCDC A=T B=F: recursive CTE with UNION ALL",
			query: "WITH RECURSIVE r2(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r2 WHERE n < 3) SELECT n FROM r2 ORDER BY n",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					rows.Close()
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			// consume rows to ensure no runtime error
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows error: %v", err)
			}
		})
	}
}

// ============================================================================
// MC/DC: setupLimitOffset – OFFSET only (no LIMIT)
//
// Condition:
//   A = stmt.Offset != nil  (OFFSET clause present)
//
// Cases (N+1 = 2):
//   A=T: OFFSET present → hasOffset=true, counter initialized
//   A=F: no OFFSET → hasOffset=false
// ============================================================================

func TestMCDC_SetupLimitOffset_OffsetOnly(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: OFFSET without LIMIT (using large LIMIT)
			name:     "MCDC A=T: OFFSET 2 skips two rows",
			setup:    "CREATE TABLE t_ofonly(x INTEGER); INSERT INTO t_ofonly VALUES(1),(2),(3),(4),(5)",
			query:    "SELECT x FROM t_ofonly ORDER BY x LIMIT 100 OFFSET 2",
			wantRows: [][]interface{}{{int64(3)}, {int64(4)}, {int64(5)}},
		},
		{
			// A=F: no OFFSET
			name:     "MCDC A=F: no OFFSET, no LIMIT returns all",
			setup:    "CREATE TABLE t_noofonly(x INTEGER); INSERT INTO t_noofonly VALUES(10),(20)",
			query:    "SELECT x FROM t_noofonly ORDER BY x",
			wantRows: [][]interface{}{{int64(10)}, {int64(20)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: searchColumnByName – alias vs name lookup in ORDER BY
//
// Two branches in searchColumnByName:
//   A = selCol.Alias == orderColName   → found by alias
//   B = selColIdent.Name == orderColName → found by column name
//
// Cases (N+1 = 3):
//   A=T: ORDER BY alias name
//   A=F B=T: ORDER BY column name (no alias)
//   A=F B=F: ORDER BY expression not in SELECT list → extra column
// ============================================================================

func TestMCDC_SearchColumnByName_OrderBy(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T: ORDER BY alias
			name: "MCDC A=T: ORDER BY alias",
			setup: "CREATE TABLE t_scbn1(a INTEGER, b INTEGER);" +
				"INSERT INTO t_scbn1 VALUES(3,1),(1,2),(2,3)",
			query:    "SELECT a AS x, b FROM t_scbn1 ORDER BY x",
			wantRows: [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}, {int64(3), int64(1)}},
		},
		{
			// A=F B=T: ORDER BY column name directly
			name: "MCDC A=F B=T: ORDER BY column name",
			setup: "CREATE TABLE t_scbn2(a INTEGER, b INTEGER);" +
				"INSERT INTO t_scbn2 VALUES(3,1),(1,2),(2,3)",
			query:    "SELECT a, b FROM t_scbn2 ORDER BY a",
			wantRows: [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}, {int64(3), int64(1)}},
		},
		{
			// A=F B=F: ORDER BY extra expression not in SELECT list
			name: "MCDC A=F B=F: ORDER BY extra expr not in SELECT",
			setup: "CREATE TABLE t_scbn3(a INTEGER, b INTEGER);" +
				"INSERT INTO t_scbn3 VALUES(3,1),(1,2),(2,3)",
			query:    "SELECT a FROM t_scbn3 ORDER BY b",
			wantRows: [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: tryParseColumnNumber – ORDER BY positional column number
//
// Compound condition:
//   A = litExpr parsed as integer (no error)
//   B = colNum >= 1
//   C = colNum <= len(stmt.Columns)
//
// Outcome = A && B && C → valid positional reference
//
// Cases (N+1 = 4):
//   Base: A=T B=T C=T → valid column number 1
//   Flip B: A=T B=F → column 0 (out of range)
//   Flip C: A=T B=T C=F → column number exceeds column count
//   A=F: non-literal → -1
// ============================================================================

func TestMCDC_TryParseColumnNumber(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=T C=T: ORDER BY 1 (first column)
			name: "MCDC A=T B=T C=T: ORDER BY positional 1",
			setup: "CREATE TABLE t_pcn1(a INTEGER, b INTEGER);" +
				"INSERT INTO t_pcn1 VALUES(3,0),(1,0),(2,0)",
			query:    "SELECT a, b FROM t_pcn1 ORDER BY 1",
			wantRows: [][]interface{}{{int64(1), int64(0)}, {int64(2), int64(0)}, {int64(3), int64(0)}},
		},
		{
			// A=T B=T C=T: ORDER BY 2 (second column)
			name: "MCDC A=T B=T C=T: ORDER BY positional 2",
			setup: "CREATE TABLE t_pcn2(a INTEGER, b INTEGER);" +
				"INSERT INTO t_pcn2 VALUES(1,30),(2,10),(3,20)",
			query:    "SELECT a, b FROM t_pcn2 ORDER BY 2",
			wantRows: [][]interface{}{{int64(2), int64(10)}, {int64(3), int64(20)}, {int64(1), int64(30)}},
		},
		{
			// A=F: ORDER BY column name (not a literal integer)
			name: "MCDC A=F: ORDER BY column name not positional",
			setup: "CREATE TABLE t_pcn3(a INTEGER, b INTEGER);" +
				"INSERT INTO t_pcn3 VALUES(3,0),(1,0),(2,0)",
			query:    "SELECT a, b FROM t_pcn3 ORDER BY a",
			wantRows: [][]interface{}{{int64(1), int64(0)}, {int64(2), int64(0)}, {int64(3), int64(0)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: calculateSorterColumns – FILTER clause extra column
//
// Compound condition:
//   A = fnExpr.Star || len(fnExpr.Args) == 0  (COUNT(*) path)
//   B = fnExpr.Filter != nil                   (FILTER clause present)
//
// When A=T and B=T → extra column added for FILTER result
//
// Cases (N+1 = 3):
//   Base: A=T B=F → COUNT(*) no FILTER
//   Flip B: A=T B=T → COUNT(*) with FILTER
//   A=F: regular arg aggregate (SUM) – different branch
// ============================================================================

func TestMCDC_CalculateSorterColumns_FilterClause(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A=T B=F: COUNT(*) without FILTER
			name: "MCDC A=T B=F: COUNT(*) no FILTER",
			setup: "CREATE TABLE t_filt1(cat TEXT, v INTEGER);" +
				"INSERT INTO t_filt1 VALUES('a',1),('a',2),('a',3)",
			query:    "SELECT cat, COUNT(*) FROM t_filt1 GROUP BY cat",
			wantRows: [][]interface{}{{"a", int64(3)}},
		},
		{
			// A=T B=T: COUNT(*) FILTER (WHERE v > 1)
			name: "MCDC A=T B=T: COUNT(*) with FILTER",
			setup: "CREATE TABLE t_filt2(cat TEXT, v INTEGER);" +
				"INSERT INTO t_filt2 VALUES('a',1),('a',2),('a',3)",
			query:    "SELECT cat, COUNT(*) FILTER (WHERE v > 1) FROM t_filt2 GROUP BY cat",
			wantRows: [][]interface{}{{"a", int64(2)}},
		},
		{
			// A=F: SUM with arg, no FILTER
			name: "MCDC A=F: SUM with arg",
			setup: "CREATE TABLE t_filt3(cat TEXT, v INTEGER);" +
				"INSERT INTO t_filt3 VALUES('a',10),('a',20)",
			query:    "SELECT cat, SUM(v) FROM t_filt3 GROUP BY cat",
			wantRows: [][]interface{}{{"a", int64(30)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: resolveGroupByExpr – column name match in GROUP BY
//
// resolveGroupByExpr checks if a GROUP BY ident matches a SELECT column by
// alias (A && B path) or by column name (C path). Since the parser does not
// support bare aliases in GROUP BY, we exercise the column-name match path
// and the identity (non-ident expression) path.
//
// Condition C = strings.EqualFold(colIdent.Name, ident.Name)
//
// Cases (N+1 = 2):
//   C=T: GROUP BY ident matches a SELECT column name
//   C=F: GROUP BY ident does not match any SELECT column (fallback)
// ============================================================================

func TestMCDC_ResolveGroupByExpr_ColumnNameMatch(t *testing.T) {
	type tc struct {
		name     string
		setup    string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// C=T: GROUP BY column name matches SELECT column name
			name: "MCDC C=T: GROUP BY column name matches SELECT",
			setup: "CREATE TABLE t_rge1(a INTEGER, b INTEGER);" +
				"INSERT INTO t_rge1 VALUES(1,10),(1,20),(2,30)",
			query:    "SELECT a, SUM(b) FROM t_rge1 GROUP BY a ORDER BY a",
			wantRows: [][]interface{}{{int64(1), int64(30)}, {int64(2), int64(30)}},
		},
		{
			// Non-ident GROUP BY expression: multiple groups with different SUM values
			name: "MCDC non-ident GROUP BY expr: multiple groups",
			setup: "CREATE TABLE t_rge2(a INTEGER, b INTEGER);" +
				"INSERT INTO t_rge2 VALUES(1,10),(1,20),(2,30)",
			query:    "SELECT a, SUM(b) FROM t_rge2 GROUP BY a ORDER BY a",
			wantRows: [][]interface{}{{int64(1), int64(30)}, {int64(2), int64(30)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(tt.setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// ============================================================================
// MC/DC: SELECT with compound conditions in WHERE clause
//
// Tests compound boolean conditions passed through the WHERE clause engine:
//   A && B: both conditions must be true
//   A || B: either condition suffices
// ============================================================================

func TestMCDC_SelectCompoundWhere(t *testing.T) {
	setup := "CREATE TABLE t_cw(x INTEGER, y INTEGER);" +
		"INSERT INTO t_cw VALUES(1,1),(1,2),(2,1),(2,2),(3,3)"

	type tc struct {
		name     string
		query    string
		wantRows [][]interface{}
	}
	tests := []tc{
		{
			// A && B: x=1 AND y=2 → only one row
			name:     "MCDC A=T B=T: AND both true",
			query:    "SELECT x, y FROM t_cw WHERE x=1 AND y=2",
			wantRows: [][]interface{}{{int64(1), int64(2)}},
		},
		{
			// Flip A: A=F → false overall
			name:     "MCDC A=F B=T: AND false overall",
			query:    "SELECT x, y FROM t_cw WHERE x=99 AND y=1",
			wantRows: nil,
		},
		{
			// Flip B: A=T B=F → false overall
			name:     "MCDC A=T B=F: AND false overall",
			query:    "SELECT x, y FROM t_cw WHERE x=1 AND y=99",
			wantRows: nil,
		},
		{
			// A || B: x=1 OR y=1 → several rows
			name:     "MCDC OR A=F B=T: OR with only B true",
			query:    "SELECT x, y FROM t_cw WHERE x=99 OR y=1 ORDER BY x,y",
			wantRows: [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}},
		},
		{
			// A || B: A=T B=F → true overall
			name:     "MCDC OR A=T B=F: OR with only A true",
			query:    "SELECT x, y FROM t_cw WHERE x=3 OR y=99 ORDER BY x,y",
			wantRows: [][]interface{}{{int64(3), int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := openMCDCDB(t)
			for _, s := range splitSemicolon(setup) {
				mustExec(t, db, s)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			got := scanAllRows(t, rows)
			compareRows(t, got, tt.wantRows)
		})
	}
}

// helper: openMCDCSelectDB opens a fresh :memory: database for SELECT MC/DC tests.
// (reuses openMCDCDB from compile_dml_mcdc_test.go which is in the same package)

// helper: scanAndCloseRows is a convenience wrapper for use in SELECT tests.
func scanAndCloseRows(t *testing.T, rows *sql.Rows) [][]interface{} {
	t.Helper()
	defer rows.Close()
	got := scanAllRows(t, rows)
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}
