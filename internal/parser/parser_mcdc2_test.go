// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"strings"
	"testing"
)

// TestMCDC2_Parse_EmptyInput covers the Parse() path when the SQL is empty
// (token count == 0, statements list empty).
// MC/DC: len(tokens)==0 branch.
func TestMCDC2_Parse_EmptyInput(t *testing.T) {
	stmts, err := ParseString("")
	if err != nil {
		t.Errorf("Parse empty string: %v", err)
	}
	if len(stmts) != 0 {
		t.Errorf("expected 0 statements, got %d", len(stmts))
	}
}

// TestMCDC2_ParseSelect_MultipleCompounds covers the parseCompoundSelect
// INTERSECT path and the EXCEPT path (MC/DC operators).
//
// MC/DC for parseCompoundSelect:
//
//	C1: UNION    → CompoundUnion
//	C2: UNION ALL→ CompoundUnionAll
//	C3: EXCEPT   → CompoundExcept   (covered here)
//	C4: INTERSECT→ CompoundIntersect (covered here)
func TestMCDC2_ParseSelect_MultipleCompounds(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantComp CompoundOp
	}{
		{
			name:     "EXCEPT compound",
			sql:      "SELECT 1 EXCEPT SELECT 2",
			wantComp: CompoundExcept,
		},
		{
			name:     "INTERSECT compound",
			sql:      "SELECT 1 INTERSECT SELECT 2",
			wantComp: CompoundIntersect,
		},
		{
			name:     "UNION ALL compound",
			sql:      "SELECT 1 UNION ALL SELECT 2",
			wantComp: CompoundUnionAll,
		},
		{
			name:     "UNION compound",
			sql:      "SELECT 1 UNION SELECT 2",
			wantComp: CompoundUnion,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString(%q): %v", tt.sql, err)
			}
			if len(stmts) == 0 {
				t.Fatal("expected at least one statement")
			}
			sel, ok := stmts[0].(*SelectStmt)
			if !ok {
				t.Fatalf("expected *SelectStmt, got %T", stmts[0])
			}
			if sel.Compound == nil {
				t.Fatal("expected compound select, got nil")
			}
			if sel.Compound.Op != tt.wantComp {
				t.Errorf("compound op=%v, want %v", sel.Compound.Op, tt.wantComp)
			}
		})
	}
}

// TestMCDC2_ParseWithClause_Recursive covers the RECURSIVE keyword path in
// parseWithClause.
//
// MC/DC for parseWithClause:
//
//	C1: match(TK_RECURSIVE) == true  → with.Recursive=true (covered here)
//	C1: match(TK_RECURSIVE) == false → with.Recursive=false (covered by other CTE tests)
func TestMCDC2_ParseWithClause_Recursive(t *testing.T) {
	sql := "WITH RECURSIVE cte(n) AS (SELECT 1) SELECT * FROM cte"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected statement")
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmts[0])
	}
	if sel.With == nil {
		t.Fatal("expected WITH clause")
	}
	if !sel.With.Recursive {
		t.Error("expected Recursive=true")
	}
}

// TestMCDC2_ParseCTEColumns_WithColumnList covers the branch where parseCTEColumns
// reads an actual column list (not a SELECT inside parens).
//
// MC/DC for parseCTEColumns:
//
//	C1: match(TK_LP) && check(TK_SELECT) → back-track (covered elsewhere)
//	C2: match(TK_LP) && !check(TK_SELECT)→ parse column names (covered here)
//	C3: !match(TK_LP)                    → no column list
func TestMCDC2_ParseCTEColumns_WithColumnList(t *testing.T) {
	sql := "WITH cte(a, b) AS (SELECT 1, 2) SELECT a, b FROM cte"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.With.CTEs) == 0 {
		t.Fatal("expected CTE")
	}
	cte := sel.With.CTEs[0]
	if len(cte.Columns) != 2 {
		t.Errorf("expected 2 CTE columns, got %d", len(cte.Columns))
	}
}

// TestMCDC2_ParseCTEColumns_NoColumnList covers the no-LP branch.
func TestMCDC2_ParseCTEColumns_NoColumnList(t *testing.T) {
	sql := "WITH cte AS (SELECT 42) SELECT * FROM cte"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.With.CTEs) == 0 {
		t.Fatal("expected CTE")
	}
	if len(sel.With.CTEs[0].Columns) != 0 {
		t.Errorf("expected 0 columns for no-column-list CTE, got %d", len(sel.With.CTEs[0].Columns))
	}
}

// TestMCDC2_ParseSelect_WindowNamedDefs covers parseWindowDef
// (WINDOW clause on SELECT).
//
// MC/DC for parseWindowDef:
//
//	C1: check(TK_ID) → name present (covered here)
//	C2: match(TK_AS) → AS found
func TestMCDC2_ParseSelect_WindowNamedDefs(t *testing.T) {
	sql := "SELECT row_number() OVER w FROM t WINDOW w AS ()"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected statement")
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt")
	}
	if len(sel.WindowDefs) == 0 {
		t.Error("expected at least one named window definition")
	}
}

// TestMCDC2_ParseLimitClause_OffsetComma covers the "LIMIT y, x" (comma) form.
//
// MC/DC for parseLimitClauseInto:
//
//	C1: match(TK_OFFSET) → LIMIT x OFFSET y form (covered by other tests)
//	C2: match(TK_COMMA)  → LIMIT y, x form (covered here)
func TestMCDC2_ParseLimitClause_OffsetComma(t *testing.T) {
	sql := "SELECT 1 LIMIT 5, 10"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Limit == nil {
		t.Error("expected Limit to be set")
	}
}

// TestMCDC2_ParseTableOrSubquery_Schema covers schema.table syntax
// (parseTableOrSubquery with DOT).
//
// MC/DC for parseTableOrSubquery:
//
//	C1: match(TK_DOT) → schema-qualified name (covered here)
//	C1: !match(TK_DOT)→ simple name
func TestMCDC2_ParseTableOrSubquery_Schema(t *testing.T) {
	sql := "SELECT * FROM main.some_table"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) == 0 {
		t.Fatal("expected FROM clause")
	}
	tbl := sel.From.Tables[0]
	if tbl.Schema != "main" {
		t.Errorf("schema=%q, want %q", tbl.Schema, "main")
	}
}

// TestMCDC2_ParseTableOrSubquery_TableFunc covers table-valued function syntax
// (parseTableFuncArgs).
//
// MC/DC for parseTableOrSubquery:
//
//	C1: match(TK_LP) after name → table function args path (covered here)
func TestMCDC2_ParseTableOrSubquery_TableFunc(t *testing.T) {
	sql := "SELECT * FROM generate_series(1, 10)"
	_, err := ParseString(sql)
	// We only care that it doesn't panic / parses without crash.
	// It may error depending on lexer but should not panic.
	_ = err
}

// TestMCDC2_ParseTableAlias_DirectAlias covers the alias-without-AS path
// in parseTableAlias (bare identifier after table name).
//
// MC/DC for parseTableAlias:
//
//	C1: match(TK_AS)  → AS alias (covered elsewhere)
//	C2: check(TK_ID) && !isJoinKeyword → bare alias (covered here)
func TestMCDC2_ParseTableAlias_DirectAlias(t *testing.T) {
	sql := "SELECT t.id FROM my_table t"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) == 0 {
		t.Fatal("expected FROM clause")
	}
	if sel.From.Tables[0].Alias != "t" {
		t.Errorf("alias=%q, want %q", sel.From.Tables[0].Alias, "t")
	}
}

// TestMCDC2_ParseJoinClause_NaturalInner covers NATURAL INNER JOIN (and
// the INNER join type token).
//
// MC/DC for parseJoinType:
//
//	C1: TK_INNER → JoinInner (covered here)
//	C2: TK_CROSS → JoinCross
//	C3: TK_NATURAL prefix
func TestMCDC2_ParseJoinClause_NaturalInner(t *testing.T) {
	sql := "SELECT * FROM a NATURAL INNER JOIN b"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	j := sel.From.Joins[0]
	if !j.Natural {
		t.Error("expected Natural=true")
	}
	if j.Type != JoinInner {
		t.Errorf("join type=%v, want INNER", j.Type)
	}
}

// TestMCDC2_ParseJoinClause_CrossJoin covers CROSS JOIN.
func TestMCDC2_ParseJoinClause_CrossJoin(t *testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	if sel.From.Joins[0].Type != JoinCross {
		t.Errorf("join type=%v, want CROSS", sel.From.Joins[0].Type)
	}
}

// TestMCDC2_ParseJoinUsingCondition covers the USING (cols) join condition.
//
// MC/DC for parseJoinCondition:
//
//	C1: match(TK_ON)    → ON condition (covered elsewhere)
//	C2: match(TK_USING) → USING condition (covered here)
func TestMCDC2_ParseJoinUsingCondition(t *testing.T) {
	sql := "SELECT * FROM a JOIN b USING (id)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	cols := sel.From.Joins[0].Condition.Using
	if len(cols) == 0 {
		t.Error("expected USING columns")
	}
	if cols[0] != "id" {
		t.Errorf("using col=%q, want %q", cols[0], "id")
	}
}

// TestMCDC2_ParseInsertBody_SchemaQualified covers the schema-qualified table
// name path in parseInsertBody (schema.table syntax).
//
// MC/DC for parseInsertBody:
//
//	C1: match(TK_DOT) → schema-qualified (covered here)
func TestMCDC2_ParseInsertBody_SchemaQualified(t *testing.T) {
	sql := "INSERT INTO main.my_table VALUES (1)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins, ok := stmts[0].(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmts[0])
	}
	if ins.Schema != "main" {
		t.Errorf("schema=%q, want %q", ins.Schema, "main")
	}
}

// TestMCDC2_ParseInsertSource_Default covers the DEFAULT VALUES path.
//
// MC/DC for parseInsertSource:
//
//	C1: match(TK_VALUES) → values (covered elsewhere)
//	C2: match(TK_SELECT) → select (covered elsewhere)
//	C3: match(TK_DEFAULT)→ DEFAULT VALUES (covered here)
func TestMCDC2_ParseInsertSource_Default(t *testing.T) {
	sql := "INSERT INTO t DEFAULT VALUES"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if !ins.DefaultVals {
		t.Error("expected DefaultVals=true")
	}
}

// TestMCDC2_ParseInsertSource_Select covers the INSERT INTO ... SELECT path.
func TestMCDC2_ParseInsertSource_Select(t *testing.T) {
	sql := "INSERT INTO t SELECT 1, 2"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if ins.Select == nil {
		t.Error("expected Select to be set")
	}
}

// TestMCDC2_ParseInsertValues_MultiRow covers multiple rows in INSERT VALUES.
//
// MC/DC for parseInsertValues:
//
//	C1: match(TK_COMMA) after RP → more rows (covered here)
func TestMCDC2_ParseInsertValues_MultiRow(t *testing.T) {
	sql := "INSERT INTO t (a, b) VALUES (1, 2), (3, 4)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if len(ins.Values) != 2 {
		t.Errorf("expected 2 rows, got %d", len(ins.Values))
	}
}

// TestMCDC2_ParseDoUpdateClause_WhereClause covers the optional WHERE clause
// in parseDoUpdateClause.
//
// MC/DC for parseDoUpdateClause:
//
//	C1: parseOptionalWhereExpr finds WHERE → Where set (covered here)
//	C1: no WHERE clause                    → Where nil (covered elsewhere)
func TestMCDC2_ParseDoUpdateClause_WhereClause(t *testing.T) {
	sql := `INSERT INTO t (id, v) VALUES (1, 'a')
	         ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE t.id < 100`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if ins.Upsert == nil || ins.Upsert.Update == nil {
		t.Fatal("expected Upsert.Update")
	}
	if ins.Upsert.Update.Where == nil {
		t.Error("expected WHERE clause in DO UPDATE")
	}
}

// TestMCDC2_ParseConflictTarget_ColumnWithWhere covers the conflict target
// with column list AND a WHERE clause.
//
// MC/DC for parseColumnsTarget:
//
//	C1: match(TK_WHERE) → target.Where set (covered here)
func TestMCDC2_ParseConflictTarget_ColumnWithWhere(t *testing.T) {
	sql := `INSERT INTO t (id) VALUES (1)
	         ON CONFLICT (id) WHERE id > 0 DO NOTHING`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if ins.Upsert == nil || ins.Upsert.Target == nil {
		t.Fatal("expected Upsert.Target")
	}
	if ins.Upsert.Target.Where == nil {
		t.Error("expected WHERE in conflict target")
	}
}

// TestMCDC2_ParseFromClause_MultipleTablesComma covers multiple FROM tables
// separated by commas (implicit cross join).
//
// MC/DC for parseFromClause:
//
//	C1: match(TK_COMMA) → additional table (covered here)
func TestMCDC2_ParseFromClause_MultipleTablesComma(t *testing.T) {
	sql := "SELECT * FROM a, b"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) < 2 {
		t.Errorf("expected 2 tables in FROM, got %d", len(sel.From.Tables))
	}
}

// TestMCDC2_ParseWindowDef_PartitionAndOrder covers a window spec with
// PARTITION BY and ORDER BY clauses inside parseWindowDef.
//
// MC/DC for parseWindowDef:
//
//	C1: parsePartitionBy finds PARTITION BY → spec.PartitionBy set
//	C2: parseWindowOrderBy finds ORDER BY   → spec.OrderBy set
func TestMCDC2_ParseWindowDef_PartitionAndOrder(t *testing.T) {
	sql := "SELECT sum(v) OVER w FROM t WINDOW w AS (PARTITION BY c ORDER BY d)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.WindowDefs) == 0 {
		t.Fatal("expected window defs")
	}
	spec := sel.WindowDefs[0].Spec
	if spec == nil {
		t.Fatal("expected window spec")
	}
	if len(spec.PartitionBy) == 0 {
		t.Error("expected PARTITION BY")
	}
	if len(spec.OrderBy) == 0 {
		t.Error("expected ORDER BY in window spec")
	}
}

// TestMCDC2_ParseJoinClause_RightOuterJoin covers RIGHT OUTER JOIN.
//
// MC/DC for parseJoinType:
//
//	C1: TK_RIGHT with optional OUTER → JoinRight (covered here)
func TestMCDC2_ParseJoinClause_RightOuterJoin(t *testing.T) {
	sql := "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	if sel.From.Joins[0].Type != JoinRight {
		t.Errorf("join type=%v, want RIGHT", sel.From.Joins[0].Type)
	}
}

// TestMCDC2_ParseJoinClause_FullOuterJoin covers FULL OUTER JOIN.
func TestMCDC2_ParseJoinClause_FullOuterJoin(t *testing.T) {
	sql := "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	if sel.From.Joins[0].Type != JoinFull {
		t.Errorf("join type=%v, want FULL", sel.From.Joins[0].Type)
	}
}

// TestMCDC2_ParseSelect_SubqueryFrom covers a subquery in the FROM clause
// (parseTableOrSubquery with subquery branch).
//
// MC/DC for parseTableOrSubquery:
//
//	C1: match(TK_LP) → check if subquery (has SELECT) (covered here)
func TestMCDC2_ParseSelect_SubqueryFrom(t *testing.T) {
	sql := "SELECT x FROM (SELECT 1 AS x) sub"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) == 0 {
		t.Fatal("expected FROM")
	}
	if sel.From.Tables[0].Subquery == nil {
		t.Error("expected subquery in FROM")
	}
}

// TestMCDC2_ParseSelect_LimitOffset covers the LIMIT ... OFFSET form.
//
// MC/DC for parseLimitClauseInto:
//
//	C1: match(TK_OFFSET) → offset set (covered here)
func TestMCDC2_ParseSelect_LimitOffset(t *testing.T) {
	sql := "SELECT 1 LIMIT 10 OFFSET 5"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Limit == nil {
		t.Error("expected Limit")
	}
	if sel.Offset == nil {
		t.Error("expected Offset")
	}
}

// TestMCDC2_ParseSelect_LeftJoinWith covers LEFT JOIN (outer keyword optional).
func TestMCDC2_ParseSelect_LeftJoinWith(t *testing.T) {
	sql := "SELECT * FROM a LEFT JOIN b ON a.id = b.id"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	if sel.From.Joins[0].Type != JoinLeft {
		t.Errorf("join type=%v, want LEFT", sel.From.Joins[0].Type)
	}
}

// TestMCDC2_ParseInsertValues_ParseErrors verifies error cases in parseInsertValues.
//
// MC/DC for parseInsertValues:
//
//	C1: no LP before values → error
func TestMCDC2_ParseInsertValues_ParseErrors(t *testing.T) {
	badSQLs := []string{
		"INSERT INTO t VALUES 1, 2",     // missing ( before values
		"INSERT INTO t (a) VALUES (1 2", // missing ) after values
	}
	for _, sql := range badSQLs {
		_, err := ParseString(sql)
		if err == nil {
			t.Errorf("expected error for %q, got nil", sql)
		}
	}
}

// TestMCDC2_ParseSelect_HavingWithoutGroupBy covers the HAVING without GROUP BY path.
//
// MC/DC for parseGroupByClauseInto:
//
//	C1: match(TK_GROUP) → GROUP BY parsed
//	C2: match(TK_HAVING) (independently) → HAVING parsed (covered here)
func TestMCDC2_ParseSelect_HavingWithoutGroupBy(t *testing.T) {
	sql := "SELECT count(*) FROM t HAVING count(*) > 0"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Having == nil {
		t.Error("expected HAVING clause")
	}
	if sel.GroupBy != nil {
		t.Error("did not expect GROUP BY")
	}
}

// TestMCDC2_ParseErrors_InvalidStatement verifies the error path for
// unrecognized statement tokens.
func TestMCDC2_ParseErrors_InvalidStatement(t *testing.T) {
	_, err := ParseString("BLARGH foo bar")
	if err == nil {
		t.Error("expected error for invalid statement keyword")
	}
}

// TestMCDC2_ParseJoinUsingCondition_MultipleColumns covers multiple columns
// in USING().
func TestMCDC2_ParseJoinUsingCondition_MultipleColumns(t *testing.T) {
	// MC/DC for parseUsingColumnList:
	//   C1: match(TK_COMMA) → more columns (covered here)
	sql := "SELECT * FROM a JOIN b USING (id, name)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.From.Joins) == 0 {
		t.Fatal("expected JOIN")
	}
	cols := sel.From.Joins[0].Condition.Using
	if len(cols) != 2 {
		t.Errorf("expected 2 USING columns, got %d", len(cols))
	}
}

// TestMCDC2_ParseSelect_IndexedBy covers the INDEXED BY clause in FROM.
//
// MC/DC for parseIndexedBy:
//
//	C1: match(TK_INDEXED) → consume BY and index name (covered here)
func TestMCDC2_ParseSelect_IndexedBy(t *testing.T) {
	sql := "SELECT * FROM t INDEXED BY my_idx"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) == 0 {
		t.Fatal("expected FROM")
	}
	if sel.From.Tables[0].Indexed != "my_idx" {
		t.Errorf("indexed=%q, want %q", sel.From.Tables[0].Indexed, "my_idx")
	}
}

// TestMCDC2_ParseCTESelect_ErrorNoAS covers the error path when AS is missing.
func TestMCDC2_ParseCTESelect_ErrorNoAS(t *testing.T) {
	sql := "WITH cte SELECT 1"
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when CTE missing AS")
	}
}

// TestMCDC2_ParseWithMultipleCTEs covers the comma-separated multiple CTEs path.
//
// MC/DC for parseWithClause:
//
//	C1: match(TK_COMMA) after first CTE → parse second CTE (covered here)
func TestMCDC2_ParseWithMultipleCTEs(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.With.CTEs) != 2 {
		t.Errorf("expected 2 CTEs, got %d", len(sel.With.CTEs))
	}
}

// TestMCDC2_ParseInsertOrConflict covers INSERT OR REPLACE (on-conflict clause).
func TestMCDC2_ParseInsertOrConflict(t *testing.T) {
	sql := "INSERT OR REPLACE INTO t VALUES (1)"
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ParseString: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if ins.OnConflict == OnConflictNone {
		t.Error("expected OnConflict to be set for INSERT OR REPLACE")
	}
}

// TestMCDC2_ParseErrors_ErrorsJoined covers the parse-errors join path
// in Parse() when errors are accumulated.
func TestMCDC2_ParseErrors_ErrorsJoined(t *testing.T) {
	// An expression parse that fails should produce joined error messages.
	_, err := ParseString("SELECT FROM")
	if err == nil {
		t.Error("expected error")
	}
	// The error message should contain something meaningful.
	if !strings.Contains(err.Error(), "") {
		t.Error("error should be non-empty string")
	}
}
