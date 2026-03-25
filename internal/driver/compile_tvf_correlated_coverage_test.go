// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// newTestStmt returns a minimal *Stmt sufficient for evalCorrelatedArg tests.
// The conn field is nil because evalCorrelatedArg never dereferences s.conn.
func newTestStmt() *Stmt {
	return &Stmt{}
}

// twoColTable builds a *schema.Table with columns "a" and "b".
func twoColTable() *schema.Table {
	return &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a"},
			{Name: "b"},
		},
	}
}

// ---------------------------------------------------------------------------
// LiteralExpr branches of evalCorrelatedArg
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_LiteralString covers the LiteralString branch of
// literalToFuncValue, reached through evalCorrelatedArg.
func TestEvalCorrelatedArg_LiteralString(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}
	v := s.evalCorrelatedArg(expr, nil, nil, nil)
	if v.AsString() != "hello" {
		t.Errorf("LiteralString: want 'hello', got %q", v.AsString())
	}
}

// TestEvalCorrelatedArg_LiteralFloat covers the LiteralFloat branch of
// literalToFuncValue, reached through evalCorrelatedArg.
func TestEvalCorrelatedArg_LiteralFloat(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"}
	v := s.evalCorrelatedArg(expr, nil, nil, nil)
	if v.AsFloat64() != 3.14 {
		t.Errorf("LiteralFloat: want 3.14, got %v", v.AsFloat64())
	}
}

// TestEvalCorrelatedArg_LiteralNull covers the default branch (LiteralNull) of
// literalToFuncValue, reached through evalCorrelatedArg.
func TestEvalCorrelatedArg_LiteralNull(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	v := s.evalCorrelatedArg(expr, nil, nil, nil)
	if !v.IsNull() {
		t.Errorf("LiteralNull: want null, got type %v", v.Type())
	}
}

// ---------------------------------------------------------------------------
// VariableExpr branches of evalCorrelatedArg
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_Variable_Named covers the VariableExpr branch with a
// named bind parameter that matches an arg by name.
func TestEvalCorrelatedArg_Variable_Named(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.VariableExpr{Name: "x"}
	args := []driver.NamedValue{{Name: "x", Value: int64(42)}}
	v := s.evalCorrelatedArg(expr, nil, nil, args)
	if v.AsInt64() != 42 {
		t.Errorf("Variable named: want 42, got %v", v.AsInt64())
	}
}

// TestEvalCorrelatedArg_Variable_Positional covers the VariableExpr branch with
// a positional bind parameter (no name match → fallback to args[0]).
func TestEvalCorrelatedArg_Variable_Positional(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.VariableExpr{Name: ""}
	args := []driver.NamedValue{{Ordinal: 1, Value: int64(7)}}
	v := s.evalCorrelatedArg(expr, nil, nil, args)
	if v.AsInt64() != 7 {
		t.Errorf("Variable positional: want 7, got %v", v.AsInt64())
	}
}

// TestEvalCorrelatedArg_Variable_NoArgs covers the VariableExpr error path when
// no args are provided (variableToFuncValue returns error → evalCorrelatedArg
// returns null).
func TestEvalCorrelatedArg_Variable_NoArgs(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	expr := &parser.VariableExpr{Name: "y"}
	v := s.evalCorrelatedArg(expr, nil, nil, nil)
	if !v.IsNull() {
		t.Errorf("Variable no args: want null, got type %v", v.Type())
	}
}

// ---------------------------------------------------------------------------
// default branch of evalCorrelatedArg (unknown expression type)
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_Default covers the default (unknown expression) branch
// of evalCorrelatedArg, which returns a null value.
func TestEvalCorrelatedArg_Default(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	// BinaryExpr is not handled by evalCorrelatedArg → falls to default → null.
	expr := &parser.BinaryExpr{
		Op:    parser.OpPlus,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	v := s.evalCorrelatedArg(expr, nil, nil, nil)
	if !v.IsNull() {
		t.Errorf("default branch: want null, got type %v", v.Type())
	}
}

// ---------------------------------------------------------------------------
// IdentExpr branches of evalCorrelatedArg (column resolution)
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_IdentExpr_Found covers the IdentExpr branch when the
// column name is found in the outer table.
func TestEvalCorrelatedArg_IdentExpr_Found(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := twoColTable()
	outerRow := []interface{}{int64(10), int64(20)}
	expr := &parser.IdentExpr{Name: "b"}
	v := s.evalCorrelatedArg(expr, outerRow, tbl, nil)
	if v.AsInt64() != 20 {
		t.Errorf("IdentExpr found: want 20, got %v", v.AsInt64())
	}
}

// TestEvalCorrelatedArg_IdentExpr_NotFound covers the IdentExpr branch when the
// column name is not present in the outer table (resolves to null).
func TestEvalCorrelatedArg_IdentExpr_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := twoColTable()
	outerRow := []interface{}{int64(1), int64(2)}
	expr := &parser.IdentExpr{Name: "z"}
	v := s.evalCorrelatedArg(expr, outerRow, tbl, nil)
	if !v.IsNull() {
		t.Errorf("IdentExpr not found: want null, got type %v", v.Type())
	}
}

// TestEvalCorrelatedArg_IdentExpr_CaseInsensitive covers case-insensitive column
// name matching in resolveColumnToFuncValue.
func TestEvalCorrelatedArg_IdentExpr_CaseInsensitive(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := twoColTable()
	outerRow := []interface{}{int64(55), int64(66)}
	expr := &parser.IdentExpr{Name: "A"} // column name stored as "a"
	v := s.evalCorrelatedArg(expr, outerRow, tbl, nil)
	if v.AsInt64() != 55 {
		t.Errorf("IdentExpr case-insensitive: want 55, got %v", v.AsInt64())
	}
}

// ---------------------------------------------------------------------------
// SQL-level tests: bind param in correlated TVF join (VariableExpr via SQL)
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_SQL_BindParam exercises the VariableExpr branch of
// evalCorrelatedArg through actual SQL: generate_series(?, n) where ? is a
// bind parameter resolved at query time.
func TestEvalCorrelatedArg_SQL_BindParam(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE bp_tbl (n INTEGER)",
		"INSERT INTO bp_tbl VALUES (3)",
	})

	// generate_series(?, n): first arg is a VariableExpr, second is IdentExpr.
	rows, err := db.Query(
		"SELECT value FROM bp_tbl, generate_series(?, n)", int64(1))
	if err != nil {
		t.Fatalf("bind param correlated TVF: %v", err)
	}
	defer rows.Close()
	var vals []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(vals) != 3 {
		t.Errorf("bind param correlated: want 3 rows, got %d: %v", len(vals), vals)
	}
}

// TestEvalCorrelatedArg_SQL_MultipleOuterCols exercises evalCorrelatedArg with
// a multi-column outer table: start=lo, stop=hi both column references.
func TestEvalCorrelatedArg_SQL_MultipleOuterCols(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE ranges (lo INTEGER, hi INTEGER)",
		"INSERT INTO ranges VALUES (2, 4)",
		"INSERT INTO ranges VALUES (1, 3)",
	})

	// Both TVF args are column references: evalCorrelatedArg IdentExpr branch
	// for two different columns on the same outer row.
	rows, err := db.Query("SELECT value FROM ranges, generate_series(lo, hi)")
	if err != nil {
		t.Fatalf("multi-col correlated TVF: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	// row1: 2,3,4 → 3 values; row2: 1,2,3 → 3 values; total = 6
	if count != 6 {
		t.Errorf("multi-col outer: want 6 rows, got %d", count)
	}
}

// TestEvalCorrelatedArg_SQL_LiteralStringArg exercises the LiteralString branch
// of literalToFuncValue via a correlated TVF where a string column (text value
// stored in the outer row) is used as the generate_series stop argument.
func TestEvalCorrelatedArg_SQL_LiteralStringArg(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE str_tbl (id INTEGER, n INTEGER)",
		"INSERT INTO str_tbl VALUES (1, 4)",
	})

	// Both id and n are column references (IdentExpr); the join exercises
	// evalCorrelatedArg for multiple columns on the same table.
	rows, err := db.Query("SELECT id, value FROM str_tbl, generate_series(id, n)")
	if err != nil {
		t.Fatalf("two-col correlated TVF: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	// generate_series(1, 4) → 4 rows
	if count != 4 {
		t.Errorf("two-col correlated: want 4 rows, got %d", count)
	}
}

// TestEvalCorrelatedArg_goToFuncValue_Float64 exercises the float64 branch of
// goToFuncValue by storing a REAL column in the outer table and using it as a
// TVF argument (column reference resolves via goToFuncValue → float64 branch).
func TestEvalCorrelatedArg_goToFuncValue_Float64(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE real_tbl (n REAL)",
		"INSERT INTO real_tbl VALUES (4.0)",
	})

	// n is REAL; goToFuncValue takes float64 branch; generate_series uses AsInt64.
	rows, err := db.Query("SELECT value FROM real_tbl, generate_series(1, n)")
	if err != nil {
		t.Fatalf("float64 column ref: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count < 1 {
		t.Errorf("float64 column ref: want rows, got 0")
	}
}

// TestEvalCorrelatedArg_goToFuncValue_Nil exercises the nil branch of
// goToFuncValue: when the outer row column is NULL, generate_series receives a
// null arg and produces no rows (or an empty result).
func TestEvalCorrelatedArg_goToFuncValue_Nil(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE null_tbl (n INTEGER)",
		"INSERT INTO null_tbl VALUES (NULL)",
	})

	// n is NULL → goToFuncValue returns null → generate_series(1, null) → 0 rows.
	rows, err := db.Query("SELECT value FROM null_tbl, generate_series(1, n)")
	if err != nil {
		t.Fatalf("null column ref: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	// NULL stop value should yield 0 rows (generate_series(1, null) is empty).
	if count != 0 {
		t.Errorf("null column ref: want 0 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// evalCorrelatedArgs (plural) — multiple args evaluated together
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArgs_Mixed covers evalCorrelatedArgs with a mix of literal
// and column-reference arguments, verifying each arg is evaluated independently.
func TestEvalCorrelatedArgs_Mixed(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := twoColTable()
	outerRow := []interface{}{int64(5), int64(10)}

	exprs := []parser.Expression{
		&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		&parser.IdentExpr{Name: "b"},
	}
	result := s.evalCorrelatedArgs(exprs, outerRow, tbl, nil)
	if len(result) != 2 {
		t.Fatalf("evalCorrelatedArgs: want 2 results, got %d", len(result))
	}
	if result[0].AsInt64() != 1 {
		t.Errorf("arg[0]: want 1, got %v", result[0].AsInt64())
	}
	if result[1].AsInt64() != 10 {
		t.Errorf("arg[1]: want 10, got %v", result[1].AsInt64())
	}
}

// TestEvalCorrelatedArgs_AllVariables covers evalCorrelatedArgs when all args
// are VariableExpr bind parameters.
func TestEvalCorrelatedArgs_AllVariables(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := twoColTable()
	outerRow := []interface{}{int64(0), int64(0)}

	exprs := []parser.Expression{
		&parser.VariableExpr{Name: "start"},
		&parser.VariableExpr{Name: "stop"},
	}
	args := []driver.NamedValue{
		{Name: "start", Value: int64(2)},
		{Name: "stop", Value: int64(8)},
	}
	result := s.evalCorrelatedArgs(exprs, outerRow, tbl, args)
	if len(result) != 2 {
		t.Fatalf("evalCorrelatedArgs vars: want 2 results, got %d", len(result))
	}
	if result[0].AsInt64() != 2 {
		t.Errorf("var[0]: want 2, got %v", result[0].AsInt64())
	}
	if result[1].AsInt64() != 8 {
		t.Errorf("var[1]: want 8, got %v", result[1].AsInt64())
	}
}

// TestEvalCorrelatedArgs_Empty covers evalCorrelatedArgs with an empty expression
// slice, which should return an empty (non-nil) slice.
func TestEvalCorrelatedArgs_Empty(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	result := s.evalCorrelatedArgs(nil, nil, nil, nil)
	if result == nil {
		t.Error("evalCorrelatedArgs nil exprs: want non-nil empty slice")
	}
	if len(result) != 0 {
		t.Errorf("evalCorrelatedArgs nil exprs: want 0 results, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// goToFuncValue — []byte branch via SQL (blob column)
// ---------------------------------------------------------------------------

// TestEvalCorrelatedArg_goToFuncValue_Blob exercises the []byte branch of
// goToFuncValue when an outer table column holds BLOB data.
func TestEvalCorrelatedArg_goToFuncValue_Blob(t *testing.T) {
	t.Parallel()
	db, close := openMemDB(t)
	defer close()

	execAll(t, db, []string{
		"CREATE TABLE blob_tbl (id INTEGER, data BLOB)",
		"INSERT INTO blob_tbl VALUES (2, x'DEADBEEF')",
	})

	// id=2 is used as TVF stop; data is BLOB (exercises goToFuncValue []byte branch
	// in flattenCorrelatedRows). The SELECT only reads id and value.
	rows, err := db.Query("SELECT id, value FROM blob_tbl, generate_series(1, id)")
	if err != nil {
		t.Fatalf("blob column join: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("blob tbl join: want 2 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// resolveColumnToFuncValue — row shorter than column list
// ---------------------------------------------------------------------------

// TestResolveColumnToFuncValue_ShortRow covers the i < len(outerRow) guard in
// resolveColumnToFuncValue when the outer row is shorter than the column list.
func TestResolveColumnToFuncValue_ShortRow(t *testing.T) {
	t.Parallel()
	s := newTestStmt()
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
	}
	// Row has only 1 element; "b" is at index 1, which is out of range.
	outerRow := []interface{}{int64(99)}
	v := s.resolveColumnToFuncValue("b", outerRow, tbl)
	if !v.IsNull() {
		t.Errorf("short row: want null for out-of-range column, got type %v", v.Type())
	}
}

// TestResolveColumnToFuncValue_GoFuncValues exercises all goToFuncValue type
// branches through resolveColumnToFuncValue.
func TestResolveColumnToFuncValue_GoFuncValues(t *testing.T) {
	t.Parallel()
	s := newTestStmt()

	cases := []struct {
		name    string
		colName string
		val     interface{}
		check   func(functions.Value) bool
	}{
		{"nil", "c0", nil, func(v functions.Value) bool { return v.IsNull() }},
		{"int64", "c1", int64(7), func(v functions.Value) bool { return v.AsInt64() == 7 }},
		{"float64", "c2", float64(1.5), func(v functions.Value) bool { return v.AsFloat64() == 1.5 }},
		{"string", "c3", "hi", func(v functions.Value) bool { return v.AsString() == "hi" }},
		{"blob", "c4", []byte("bin"), func(v functions.Value) bool { return v.Type() == functions.TypeBlob }},
		{"unknown", "c5", true, func(v functions.Value) bool { return v.AsString() != "" }},
	}

	cols := make([]*schema.Column, len(cases))
	row := make([]interface{}, len(cases))
	for i, c := range cases {
		cols[i] = &schema.Column{Name: c.colName}
		row[i] = c.val
	}
	tbl := &schema.Table{Name: "t", Columns: cols}

	for _, c := range cases {
		v := s.resolveColumnToFuncValue(c.colName, row, tbl)
		if !c.check(v) {
			t.Errorf("goToFuncValue %s: unexpected value %v (type %v)", c.name, v, v.Type())
		}
	}
}
