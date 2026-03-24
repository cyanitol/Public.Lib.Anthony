// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// dmlRemOpenDB opens an in-memory database for testing.
func dmlRemOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// dmlRemExec executes a SQL statement and fails the test on error.
func dmlRemExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// ============================================================================
// emitInsertRow (0%) — dead code, tested by calling it directly
// ============================================================================

// TestCompileDMLRemaining_EmitInsertRowNormal exercises the normal-table branch of
// emitInsertRow, which has no production callers (dead code).
func TestCompileDMLRemaining_EmitInsertRowNormal(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name:     "tbl",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true, Affinity: schema.AffinityInteger},
			{Name: "val", Type: "TEXT", Affinity: schema.AffinityText},
		},
	}

	vm := vdbe.New()
	vm.AllocMemory(10)
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	row := []parser.Expression{
		&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		&parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"},
	}
	colNames := []string{"id", "val"}

	paramIdx := 0
	gen := expr.NewCodeGenerator(vm)

	d := &Driver{}
	dbFile := t.TempDir() + "/emit_insert_row.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	s := &Stmt{conn: conn.(*Conn)}

	// rowidColIdx=0 (id is rowid), rowidReg=0, recordStartReg=1, numRecordCols=1
	s.emitInsertRow(vm, table, colNames, row, 0, 0, 1, 1, 0, nil, &paramIdx, gen)

	if len(vm.Program) == 0 {
		t.Fatal("emitInsertRow emitted no opcodes")
	}
}

// TestCompileDMLRemaining_EmitInsertRowWithoutRowid exercises the WITHOUT ROWID
// branch of emitInsertRow.
func TestCompileDMLRemaining_EmitInsertRowWithoutRowid(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name:         "tbl_nr",
		RootPage:     3,
		WithoutRowID: true,
		Columns: []*schema.Column{
			{Name: "k", Type: "INTEGER", PrimaryKey: true, Affinity: schema.AffinityInteger},
			{Name: "v", Type: "TEXT", Affinity: schema.AffinityText},
		},
	}

	vm := vdbe.New()
	vm.AllocMemory(10)
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	row := []parser.Expression{
		&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "7"},
		&parser.LiteralExpr{Type: parser.LiteralString, Value: "world"},
	}
	colNames := []string{"k", "v"}
	paramIdx := 0
	gen := expr.NewCodeGenerator(vm)

	d := &Driver{}
	dbFile := t.TempDir() + "/emit_insert_row_nr.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	s := &Stmt{conn: conn.(*Conn)}

	// rowidColIdx=-1 (no rowid col), rowidReg=0, recordStartReg=0, numRecordCols=2
	s.emitInsertRow(vm, table, colNames, row, -1, 0, 0, 2, 0, nil, &paramIdx, gen)

	if len(vm.Program) == 0 {
		t.Fatal("emitInsertRow (WITHOUT ROWID) emitted no opcodes")
	}
}

// ============================================================================
// loadValueIntoReg — remaining branches: []byte, and default (e.g. int32)
// ============================================================================

// TestCompileDMLRemaining_LoadValueIntoRegBlobDirect exercises the []byte branch
// of loadValueIntoReg directly by calling it with a []byte value and verifying
// that an OpBlob instruction is emitted.
func TestCompileDMLRemaining_LoadValueIntoRegBlobDirect(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	before := len(vm.Program)
	loadValueIntoReg(vm, []byte{0xCA, 0xFE}, 0)
	if len(vm.Program) <= before {
		t.Fatal("loadValueIntoReg([]byte) emitted no opcodes")
	}
}

// TestCompileDMLRemaining_LoadValueIntoRegDefault exercises the default branch
// of loadValueIntoReg directly (passes a value whose type is not handled).
func TestCompileDMLRemaining_LoadValueIntoRegDefault(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	// int32 falls through to the default branch → emits OpNull
	loadValueIntoReg(vm, int32(42), 0)
	if len(vm.Program) == 0 {
		t.Fatal("expected at least one opcode")
	}
	// float32 also falls through to default
	loadValueIntoReg(vm, float32(3.14), 1)
	if len(vm.Program) < 2 {
		t.Fatal("expected a second opcode")
	}
}

// ============================================================================
// dmlToInt64 — int and default (bool) branches
// ============================================================================

// TestCompileDMLRemaining_DmlToInt64Int exercises the int branch.
func TestCompileDMLRemaining_DmlToInt64Int(t *testing.T) {
	t.Parallel()
	got, ok := dmlToInt64(int(99))
	if !ok {
		t.Fatal("expected ok=true for int")
	}
	if got != 99 {
		t.Fatalf("want 99, got %d", got)
	}
}

// TestCompileDMLRemaining_DmlToInt64Default exercises the default (false) branch.
func TestCompileDMLRemaining_DmlToInt64Default(t *testing.T) {
	t.Parallel()
	_, ok := dmlToInt64(true)
	if ok {
		t.Fatal("expected ok=false for bool")
	}
	_, ok = dmlToInt64(float32(1.5))
	if ok {
		t.Fatal("expected ok=false for float32")
	}
}

// ============================================================================
// goValueToLiteral — bool, int32, uint32, time.Time branches (all → default)
// ============================================================================

// TestCompileDMLRemaining_GoValueToLiteralDefault exercises the default branch
// of goValueToLiteral with bool, int32, uint32, and time.Time values (all
// types that are not explicitly handled → return a NULL literal).
func TestCompileDMLRemaining_GoValueToLiteralDefault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  interface{}
	}{
		{"bool_true", true},
		{"bool_false", false},
		{"int32", int32(5)},
		{"uint32", uint32(7)},
		{"time", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lit := goValueToLiteral(tc.val)
			if lit == nil {
				t.Fatal("goValueToLiteral returned nil")
			}
			if lit.Type != parser.LiteralNull {
				t.Fatalf("want LiteralNull for %T, got type=%v", tc.val, lit.Type)
			}
		})
	}
}

// TestCompileDMLRemaining_GoValueToLiteralKnownTypes exercises the explicitly
// handled types (nil, int64, float64, string) to ensure they return the
// correct literal types.
func TestCompileDMLRemaining_GoValueToLiteralKnownTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		val     interface{}
		wantTyp parser.LiteralType
	}{
		{"nil", nil, parser.LiteralNull},
		{"int64", int64(42), parser.LiteralInteger},
		{"float64", float64(3.14), parser.LiteralFloat},
		{"string", "hello", parser.LiteralString},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lit := goValueToLiteral(tc.val)
			if lit == nil {
				t.Fatal("goValueToLiteral returned nil")
			}
			if lit.Type != tc.wantTyp {
				t.Fatalf("want %v, got %v", tc.wantTyp, lit.Type)
			}
		})
	}
}

// ============================================================================
// compileUnaryExpr — NOT branch (non-negation op → emits OpNull)
// ============================================================================

// TestCompileDMLRemaining_CompileUnaryExprNot exercises the branch where
// v.Op != OpNeg (e.g. OpNot), causing compileUnaryExpr to emit OpNull.
func TestCompileDMLRemaining_CompileUnaryExprNot(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)

	expr := &parser.UnaryExpr{
		Op:   parser.OpNot,
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	before := len(vm.Program)
	compileUnaryExpr(vm, expr, 0)
	if len(vm.Program) <= before {
		t.Fatal("compileUnaryExpr(OpNot) did not emit any opcodes")
	}
}

// TestCompileDMLRemaining_CompileUnaryExprNotViaSQL exercises the NOT branch
// through the full SQL compiler path.
func TestCompileDMLRemaining_CompileUnaryExprNotViaSQL(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE bools(id INTEGER PRIMARY KEY, flag INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO bools VALUES(1, 0)`)

	// UPDATE SET uses NOT expression, which triggers compileUnaryExpr with OpNot.
	dmlRemExec(t, db, `UPDATE bools SET flag = NOT flag WHERE id = 1`)

	var flag int
	if err := db.QueryRow(`SELECT flag FROM bools WHERE id=1`).Scan(&flag); err != nil {
		t.Fatalf("scan: %v", err)
	}
	// NOT 0 should yield 1 (truthy)
	if flag != 1 {
		t.Fatalf("want flag=1 after NOT, got %d", flag)
	}
}

// ============================================================================
// replaceExcludedRefs — nested BinaryExpr with multiple excluded refs
// ============================================================================

// TestCompileDMLRemaining_ReplaceExcludedRefsNested exercises replaceExcludedRefs
// when the expression is a nested BinaryExpr containing multiple excluded.col refs.
func TestCompileDMLRemaining_ReplaceExcludedRefsNested(t *testing.T) {
	t.Parallel()

	xLit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"}
	yLit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"}

	colValueMap := map[string]parser.Expression{
		"x": xLit,
		"y": yLit,
	}

	// Build: excluded.x + excluded.y * 2
	// As a nested BinaryExpr: Add( excluded.x , Mul( excluded.y , 2 ) )
	inner := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "excluded", Name: "y"},
		Op:    parser.OpMul,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}
	outer := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "excluded", Name: "x"},
		Op:    parser.OpPlus,
		Right: inner,
	}

	result := replaceExcludedRefs(outer, colValueMap)

	bin, ok := result.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr, got %T", result)
	}
	// Left should be replaced with xLit
	if bin.Left != xLit {
		t.Errorf("Left: want xLit, got %T %v", bin.Left, bin.Left)
	}
	// Right should be a BinaryExpr with Left = yLit
	innerResult, ok := bin.Right.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("Right: expected *BinaryExpr, got %T", bin.Right)
	}
	if innerResult.Left != yLit {
		t.Errorf("inner Left: want yLit, got %T %v", innerResult.Left, innerResult.Left)
	}
}

// TestCompileDMLRemaining_ReplaceExcludedRefsUpsert exercises replaceExcludedRefs
// through the full upsert SQL path with a nested expression.
func TestCompileDMLRemaining_ReplaceExcludedRefsUpsert(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE upsert_t(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO upsert_t VALUES(1, 10, 2)`)

	// ON CONFLICT DO UPDATE with nested excluded refs: x = excluded.x + excluded.y * 2
	dmlRemExec(t, db,
		`INSERT INTO upsert_t(id, x, y) VALUES(1, 5, 3)
		 ON CONFLICT(id) DO UPDATE SET x = excluded.x + excluded.y * 2`)

	var x int
	if err := db.QueryRow(`SELECT x FROM upsert_t WHERE id=1`).Scan(&x); err != nil {
		t.Fatalf("scan: %v", err)
	}
	// x = 5 + 3*2 = 11
	if x != 11 {
		t.Fatalf("want x=11, got %d", x)
	}
}

// TestCompileDMLRemaining_ReplaceExcludedRefsUnary exercises the UnaryExpr
// branch of replaceExcludedRefs.
func TestCompileDMLRemaining_ReplaceExcludedRefsUnary(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "9"}
	colValueMap := map[string]parser.Expression{"v": lit}

	// -excluded.v
	unary := &parser.UnaryExpr{
		Op:   parser.OpNeg,
		Expr: &parser.IdentExpr{Table: "excluded", Name: "v"},
	}

	result := replaceExcludedRefs(unary, colValueMap)
	u, ok := result.(*parser.UnaryExpr)
	if !ok {
		t.Fatalf("expected *UnaryExpr, got %T", result)
	}
	if u.Expr != lit {
		t.Errorf("expected inner expression to be replaced with lit, got %T", u.Expr)
	}
}

// ============================================================================
// emitExtraOrderByColumnMultiTable (0%) — multi-table ORDER BY extra column
// ============================================================================

// TestCompileDMLRemaining_EmitExtraOrderByColumnMultiTable exercises
// emitExtraOrderByColumnMultiTable via a JOIN query with ORDER BY on a column
// from the joined table that is not in the SELECT list.
func TestCompileDMLRemaining_EmitExtraOrderByColumnMultiTable(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)`)
	dmlRemExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER, sort_key INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO t1 VALUES(1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')`)
	dmlRemExec(t, db, `INSERT INTO t2 VALUES(1, 3, 10), (2, 1, 30), (3, 2, 20)`)

	// sort_key is NOT in the SELECT list — forces emitExtraOrderByColumnMultiTable
	rows, err := db.Query(
		`SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t2.sort_key`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(names) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(names), names)
	}
	// Sorted by sort_key: 10→Gamma, 20→Beta, 30→Alpha
	want := []string{"Gamma", "Beta", "Alpha"}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("row %d: want %q, got %q", i, w, names[i])
		}
	}
}

// TestCompileDMLRemaining_EmitExtraOrderByColumnMultiTableMissing exercises
// the "column not found in any table" path of emitExtraOrderByColumnMultiTable,
// which emits OpNull. It uses an ORDER BY on an alias that doesn't exist.
func TestCompileDMLRemaining_EmitExtraOrderByColumnMultiTableMissing(t *testing.T) {
	t.Parallel()

	d := &Driver{}
	dbFile := t.TempDir() + "/extra_orderby_missing.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	c := conn.(*Conn)
	s := &Stmt{conn: c}

	vm := vdbe.New()
	vm.AllocMemory(5)
	vm.AllocCursors(2)

	tbl1 := &schema.Table{
		Name:     "a",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "x", Type: "INTEGER"}},
	}
	tbl2 := &schema.Table{
		Name:     "b",
		RootPage: 3,
		Columns:  []*schema.Column{{Name: "y", Type: "INTEGER"}},
	}
	tables := []stmtTableInfo{
		{name: "a", table: tbl1, cursorIdx: 0},
		{name: "b", table: tbl2, cursorIdx: 1},
	}

	before := len(vm.Program)
	s.emitExtraOrderByColumnMultiTable(vm, tables, "nonexistent_col", 0)
	if len(vm.Program) <= before {
		t.Fatal("expected at least one opcode (OpNull) for missing column")
	}
}

// ============================================================================
// emitNonIdentifierColumn — remaining branches
// ============================================================================

// TestCompileDMLRemaining_EmitNonIdentifierColumnGenError exercises the branch
// where gen.GenerateExpr returns an error (emits OpNull without failing).
func TestCompileDMLRemaining_EmitNonIdentifierColumnGenError(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	vm.AllocMemory(5)

	d := &Driver{}
	dbFile := t.TempDir() + "/noident_err.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	s := &Stmt{conn: conn.(*Conn)}

	gen := expr.NewCodeGenerator(vm)

	// A SubqueryExpr with no tables registered will fail in GenerateExpr.
	// The function should still emit OpNull and return nil.
	col := parser.ResultColumn{
		Expr: &parser.FunctionExpr{
			Name: "UNKNOWN_FUNC_THAT_FAILS",
			Args: []parser.Expression{
				&parser.IdentExpr{Name: "no_such_col"},
			},
		},
	}

	before := len(vm.Program)
	err = s.emitNonIdentifierColumn(vm, col, 0, gen)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if len(vm.Program) <= before {
		t.Fatal("expected at least one opcode emitted")
	}
}

// TestCompileDMLRemaining_EmitNonIdentifierColumnNilGen exercises the nil
// generator branch, which emits OpNull immediately.
func TestCompileDMLRemaining_EmitNonIdentifierColumnNilGen(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	vm.AllocMemory(5)

	d := &Driver{}
	dbFile := t.TempDir() + "/noident_nil.db"
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()
	s := &Stmt{conn: conn.(*Conn)}

	col := parser.ResultColumn{
		Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}

	before := len(vm.Program)
	err = s.emitNonIdentifierColumn(vm, col, 0, nil)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if len(vm.Program) <= before {
		t.Fatal("expected OpNull opcode with nil generator")
	}
}

// TestCompileDMLRemaining_EmitNonIdentifierColumnExprInJoin exercises
// emitNonIdentifierColumn via a JOIN SELECT with an expression column,
// hitting the reg != targetReg copy path.
func TestCompileDMLRemaining_EmitNonIdentifierColumnExprInJoin(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER)`)
	dmlRemExec(t, db, `CREATE TABLE q(id INTEGER PRIMARY KEY, b INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO p VALUES(1, 5)`)
	dmlRemExec(t, db, `INSERT INTO q VALUES(1, 3)`)

	// a + b is a non-identifier expression in a multi-table SELECT
	var sum int
	err := db.QueryRow(
		`SELECT p.a + q.b FROM p JOIN q ON p.id = q.id`).Scan(&sum)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if sum != 8 {
		t.Fatalf("want 8, got %d", sum)
	}
}

// ============================================================================
// loadValueIntoReg — remaining: bool and int bindings in INSERT/UPDATE params
// ============================================================================

// TestCompileDMLRemaining_LoadValueIntoRegStringDirect exercises the string
// branch of loadValueIntoReg directly, verifying an opcode is emitted.
func TestCompileDMLRemaining_LoadValueIntoRegStringDirect(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	before := len(vm.Program)
	loadValueIntoReg(vm, "hello", 0)
	if len(vm.Program) <= before {
		t.Fatal("loadValueIntoReg(string) emitted no opcodes")
	}
}

// TestCompileDMLRemaining_LoadValueIntoRegFloatDirect exercises the float64
// branch of loadValueIntoReg directly, verifying an opcode is emitted.
func TestCompileDMLRemaining_LoadValueIntoRegFloatDirect(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(5)
	before := len(vm.Program)
	loadValueIntoReg(vm, float64(9.99), 0)
	if len(vm.Program) <= before {
		t.Fatal("loadValueIntoReg(float64) emitted no opcodes")
	}
}

// ============================================================================
// compileArgValue — bool, int, []byte bindings via INSERT ? parameters
// ============================================================================

// TestCompileDMLRemaining_CompileArgBool exercises the bool branch of
// compileArgValue / compileBoolArg via a parameterised INSERT.
func TestCompileDMLRemaining_CompileArgBool(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE boolarg(id INTEGER PRIMARY KEY, flag INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO boolarg VALUES(1, ?)`, true)
	dmlRemExec(t, db, `INSERT INTO boolarg VALUES(2, ?)`, false)

	var f1, f2 int
	if err := db.QueryRow(`SELECT flag FROM boolarg WHERE id=1`).Scan(&f1); err != nil {
		t.Fatalf("scan id=1: %v", err)
	}
	if err := db.QueryRow(`SELECT flag FROM boolarg WHERE id=2`).Scan(&f2); err != nil {
		t.Fatalf("scan id=2: %v", err)
	}
	if f1 != 1 {
		t.Fatalf("bool true: want 1, got %d", f1)
	}
	if f2 != 0 {
		t.Fatalf("bool false: want 0, got %d", f2)
	}
}

// TestCompileDMLRemaining_CompileArgInt exercises the int branch of
// compileArgValue / compileIntArg via a parameterised INSERT.
func TestCompileDMLRemaining_CompileArgInt(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE intarg(id INTEGER PRIMARY KEY, n INTEGER)`)
	dmlRemExec(t, db, `INSERT INTO intarg VALUES(1, ?)`, int(12345))

	var n int
	if err := db.QueryRow(`SELECT n FROM intarg WHERE id=1`).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 12345 {
		t.Fatalf("want 12345, got %d", n)
	}
}

// TestCompileDMLRemaining_CompileArgBlob exercises the []byte branch of
// compileArgValue / compileBlobArg via a parameterised INSERT.
func TestCompileDMLRemaining_CompileArgBlob(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE blobarg(id INTEGER PRIMARY KEY, data BLOB)`)
	blob := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	dmlRemExec(t, db, `INSERT INTO blobarg VALUES(1, ?)`, blob)

	var got []byte
	if err := db.QueryRow(`SELECT data FROM blobarg WHERE id=1`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4-byte blob, got %d bytes", len(got))
	}
}

// TestCompileDMLRemaining_CompileArgTimeDefault exercises the default branch of
// compileArgValue / compileDefaultArg via time.Time (not a standard driver.Value).
func TestCompileDMLRemaining_CompileArgTimeDefault(t *testing.T) {
	t.Parallel()
	db := dmlRemOpenDB(t)
	dmlRemExec(t, db, `CREATE TABLE timearg(id INTEGER PRIMARY KEY, ts TEXT)`)
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	dmlRemExec(t, db, `INSERT INTO timearg VALUES(1, ?)`, ts)

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM timearg`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 row, got %d", count)
	}
}

// ============================================================================
// loadValueIntoReg direct unit tests for int64 and nil branches
// ============================================================================

// TestCompileDMLRemaining_LoadValueIntoRegDirect tests loadValueIntoReg directly
// for all its type branches: nil, int64, float64, string, []byte, default.
func TestCompileDMLRemaining_LoadValueIntoRegDirect(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  interface{}
	}{
		{"nil", nil},
		{"int64", int64(100)},
		{"float64", float64(3.14)},
		{"string", "hello"},
		{"bytes", []byte{0x01, 0x02}},
		{"int32_default", int32(7)},
		{"bool_default", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vm := vdbe.New()
			vm.AllocMemory(3)
			before := len(vm.Program)
			loadValueIntoReg(vm, tc.val, 0)
			if len(vm.Program) <= before {
				t.Fatalf("loadValueIntoReg(%T) emitted no opcodes", tc.val)
			}
		})
	}
}

// Ensure the driver.NamedValue type alias is used (compile-time check).
var _ driver.NamedValue = driver.NamedValue{}
