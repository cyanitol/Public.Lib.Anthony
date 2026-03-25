// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// ParseStatement is a convenience wrapper used by these tests.
func mustParseOne(t *testing.T, sql string) Statement {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse(%q) unexpected error: %v", sql, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("Parse(%q): expected 1 statement, got %d", sql, len(stmts))
	}
	return stmts[0]
}

func mustParseErr(t *testing.T, sql string) {
	t.Helper()
	p := NewParser(sql)
	_, err := p.Parse()
	if err == nil {
		t.Fatalf("Parse(%q): expected error but got none", sql)
	}
}

// TestParserFKUpsert groups all coverage-boosting subtests under a single top-level name
// so that `-run TestParserFKUpsert` exercises all of them.
func TestParserFKUpsert(t *testing.T) {
	t.Parallel()
	t.Run("FKTableLevelAllActions", testFKTableLevelAllActions)
	t.Run("FKColumnLevelRestrict", testFKColumnLevelRestrict)
	t.Run("FKColumnLevelSetNull", testFKColumnLevelSetNull)
	t.Run("FKColumnLevelSetDefault", testFKColumnLevelSetDefault)
	t.Run("FKColumnLevelNoAction", testFKColumnLevelNoAction)
	t.Run("FKOnClauseUpdateBranch", testFKOnClauseUpdateBranch)
	t.Run("FKOnClauseErrorBranch", testFKOnClauseErrorBranch)
	t.Run("ColumnCheckConstraint", testColumnCheckConstraint)
	t.Run("ColumnCheckConstraintError", testColumnCheckConstraintError)
	t.Run("TableUniqueOnConflictReplace", testTableUniqueOnConflictReplace)
	t.Run("CreateVirtualTableWithStringArg", testCreateVirtualTableWithStringArg)
	t.Run("CreateVirtualTableNoArgs", testCreateVirtualTableNoArgs)
	t.Run("CreateVirtualTableModuleArgError", testCreateVirtualTableModuleArgError)
	t.Run("CreateTableAsSelect", testCreateTableAsSelect)
	t.Run("CreateTableAsSelectError", testCreateTableAsSelectError)
	t.Run("UpsertDoUpdateWhereExpr", testUpsertDoUpdateWhereExpr)
	t.Run("UpsertConstraintTargetErrorNoCONSTRAINT", testUpsertConstraintTargetErrorNoCONSTRAINT)
	t.Run("UpsertConstraintTargetErrorNoName", testUpsertConstraintTargetErrorNoName)
	t.Run("UpdateFromClause", testUpdateFromClause)
	t.Run("UpdateFromClauseError", testUpdateFromClauseError)
	t.Run("DeleteReturningMultipleColumns", testDeleteReturningMultipleColumns)
	t.Run("UpdateReturningClause", testUpdateReturningClause)
	t.Run("InsertReturningClause", testInsertReturningClause)
	t.Run("ParseSetAssignmentsMultiple", testParseSetAssignmentsMultiple)
	t.Run("ParseSetAssignmentsError", testParseSetAssignmentsError)
	t.Run("ParseColumnOrConstraintFallback", testParseColumnOrConstraintFallback)
}

// --- applyConstraintReferences + parseFKOnClause: table-level FK with ON DELETE CASCADE ON UPDATE SET NULL ---

func testFKTableLevelAllActions(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE orders (
		id INTEGER,
		user_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
	)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if len(stmt.Constraints) != 1 {
		t.Fatalf("expected 1 table constraint, got %d", len(stmt.Constraints))
	}
	fkc := stmt.Constraints[0].ForeignKey
	if fkc == nil {
		t.Fatal("expected ForeignKeyTableConstraint")
	}
	if fkc.ForeignKey.OnDelete != FKActionCascade {
		t.Errorf("OnDelete: got %v, want FKActionCascade", fkc.ForeignKey.OnDelete)
	}
	if fkc.ForeignKey.OnUpdate != FKActionSetNull {
		t.Errorf("OnUpdate: got %v, want FKActionSetNull", fkc.ForeignKey.OnUpdate)
	}
}

// --- applyConstraintReferences: column-level FK with ON DELETE RESTRICT ---

func testFKColumnLevelRestrict(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE items (id INTEGER REFERENCES products(id) ON DELETE RESTRICT)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	col := stmt.Columns[0]
	if len(col.Constraints) != 1 {
		t.Fatalf("expected 1 column constraint, got %d", len(col.Constraints))
	}
	fk := col.Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint on column")
	}
	if fk.Table != "products" {
		t.Errorf("FK table: got %q, want %q", fk.Table, "products")
	}
	if len(fk.Columns) != 1 || fk.Columns[0] != "id" {
		t.Errorf("FK columns: got %v, want [id]", fk.Columns)
	}
	if fk.OnDelete != FKActionRestrict {
		t.Errorf("OnDelete: got %v, want FKActionRestrict", fk.OnDelete)
	}
}

// --- parseFKOnClause: ON UPDATE SET NULL branch ---

func testFKColumnLevelSetNull(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (ref_id INTEGER REFERENCES parent(id) ON DELETE SET NULL)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnDelete != FKActionSetNull {
		t.Errorf("OnDelete: got %v, want FKActionSetNull", fk.OnDelete)
	}
}

// --- parseFKOnClause: ON UPDATE SET DEFAULT branch ---

func testFKColumnLevelSetDefault(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (ref_id INTEGER REFERENCES parent(id) ON UPDATE SET DEFAULT)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnUpdate != FKActionSetDefault {
		t.Errorf("OnUpdate: got %v, want FKActionSetDefault", fk.OnUpdate)
	}
}

// --- parseFKOnClause: ON DELETE NO ACTION branch ---

func testFKColumnLevelNoAction(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (ref_id INTEGER REFERENCES parent(id) ON DELETE NO ACTION)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnDelete != FKActionNoAction {
		t.Errorf("OnDelete: got %v, want FKActionNoAction", fk.OnDelete)
	}
}

// --- parseFKOnClause: ON UPDATE branch (separate from ON DELETE) ---

func testFKOnClauseUpdateBranch(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (fk INTEGER REFERENCES p(id) ON UPDATE CASCADE)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnUpdate != FKActionCascade {
		t.Errorf("OnUpdate: got %v, want FKActionCascade", fk.OnUpdate)
	}
}

// --- parseFKOnClause: error branch (neither DELETE nor UPDATE) ---

func testFKOnClauseErrorBranch(t *testing.T) {
	t.Parallel()
	// "ON INSERT" is not valid; triggers the error path in parseFKOnClause
	mustParseErr(t, `CREATE TABLE t (fk INTEGER REFERENCES p(id) ON INSERT CASCADE)`)
}

// --- applyConstraintCheck: column-level CHECK ---

func testColumnCheckConstraint(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE products (price REAL CHECK(price > 0))`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	col := stmt.Columns[0]
	if len(col.Constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(col.Constraints))
	}
	c := col.Constraints[0]
	if c.Type != ConstraintCheck {
		t.Errorf("constraint type: got %v, want ConstraintCheck", c.Type)
	}
	if c.Check == nil {
		t.Error("expected non-nil Check expression")
	}
}

// --- applyConstraintCheck: error path (missing open paren) ---

func testColumnCheckConstraintError(t *testing.T) {
	t.Parallel()
	mustParseErr(t, `CREATE TABLE t (x INTEGER CHECK x > 0)`)
}

// --- parseConstraintTarget (table UNIQUE with ON CONFLICT REPLACE) ---

func testTableUniqueOnConflictReplace(t *testing.T) {
	t.Parallel()
	// This exercises the table-level UNIQUE constraint; SQLite syntax does not
	// put ON CONFLICT on table-level UNIQUE the same way, but the column-level
	// UNIQUE does not surface parseConstraintTarget. Instead exercise the upsert
	// ON CONFLICT ON CONSTRAINT path which is parseConstraintTarget's real home.
	sql := `INSERT INTO t(a) VALUES(1) ON CONFLICT ON CONSTRAINT my_uq DO NOTHING`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	if stmt.Upsert.Target == nil {
		t.Fatal("expected conflict target")
	}
	if stmt.Upsert.Target.ConstraintName != "my_uq" {
		t.Errorf("ConstraintName: got %q, want %q", stmt.Upsert.Target.ConstraintName, "my_uq")
	}
}

// --- parseCreateVirtualTable + parseModuleArgs: string literal arg ---

func testCreateVirtualTableWithStringArg(t *testing.T) {
	t.Parallel()
	// fts5 with a plain string literal argument exercises the TK_STRING branch
	// inside parseModuleArgs.
	sql := `CREATE VIRTUAL TABLE docs USING fts5(title, 'body')`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	stmt, ok := stmts[0].(*CreateVirtualTableStmt)
	if !ok {
		t.Fatalf("expected *CreateVirtualTableStmt, got %T", stmts[0])
	}
	if stmt.Module != "fts5" {
		t.Errorf("Module: got %q, want fts5", stmt.Module)
	}
	if len(stmt.Args) != 2 {
		t.Errorf("expected 2 module args, got %d", len(stmt.Args))
	}
}

// --- parseCreateVirtualTable: no args at all (no parens) ---

func testCreateVirtualTableNoArgs(t *testing.T) {
	t.Parallel()
	sql := `CREATE VIRTUAL TABLE t USING mymodule`
	stmt := mustParseOne(t, sql).(*CreateVirtualTableStmt)
	if stmt.Module != "mymodule" {
		t.Errorf("Module: got %q, want mymodule", stmt.Module)
	}
	if len(stmt.Args) != 0 {
		t.Errorf("expected 0 args, got %d", len(stmt.Args))
	}
}

// --- parseModuleArgs: invalid token triggers error ---

func testCreateVirtualTableModuleArgError(t *testing.T) {
	t.Parallel()
	// A numeric literal is neither TK_ID nor TK_STRING, so parseModuleArgs errors.
	mustParseErr(t, `CREATE VIRTUAL TABLE t USING fts5(123)`)
}

// --- parseCreateTableAsSelect ---

func testCreateTableAsSelect(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE summary AS SELECT id, name FROM source WHERE active = 1`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if stmt.Name != "summary" {
		t.Errorf("Name: got %q, want summary", stmt.Name)
	}
	if stmt.Select == nil {
		t.Error("expected non-nil Select")
	}
}

// --- parseCreateTableAsSelect: error when non-SELECT follows AS ---

func testCreateTableAsSelectError(t *testing.T) {
	t.Parallel()
	mustParseErr(t, `CREATE TABLE t AS INSERT INTO other VALUES(1)`)
}

// --- parseOptionalWhereExpr in ON CONFLICT DO UPDATE ---

func testUpsertDoUpdateWhereExpr(t *testing.T) {
	t.Parallel()
	sql := `INSERT INTO t(a,b) VALUES(1,2) ON CONFLICT(a) DO UPDATE SET b=excluded.b WHERE b > 0`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert")
	}
	update := stmt.Upsert.Update
	if update == nil {
		t.Fatal("expected DoUpdateClause")
	}
	if update.Where == nil {
		t.Error("expected WHERE expression in DO UPDATE")
	}
}

// --- parseConstraintTarget: error path when ON is not followed by CONSTRAINT ---

func testUpsertConstraintTargetErrorNoCONSTRAINT(t *testing.T) {
	t.Parallel()
	// "ON CONFLICT ON NOTHING" — ON is consumed by the upsert parser which then
	// calls parseConstraintTarget; NOTHING is not TK_CONSTRAINT.
	mustParseErr(t, `INSERT INTO t(a) VALUES(1) ON CONFLICT ON NOTHING DO NOTHING`)
}

// --- parseConstraintTarget: error path when CONSTRAINT is not followed by an identifier ---

func testUpsertConstraintTargetErrorNoName(t *testing.T) {
	t.Parallel()
	// After "ON CONSTRAINT" the parser expects a plain identifier.
	mustParseErr(t, `INSERT INTO t(a) VALUES(1) ON CONFLICT ON CONSTRAINT 123 DO NOTHING`)
}

// --- parseUpdateFromClause ---

func testUpdateFromClause(t *testing.T) {
	t.Parallel()
	sql := `UPDATE t SET a = s.a, b = s.b FROM src s WHERE t.id = s.id`
	stmt := mustParseOne(t, sql).(*UpdateStmt)
	if stmt.From == nil {
		t.Error("expected non-nil From clause")
	}
	if len(stmt.Sets) != 2 {
		t.Errorf("expected 2 SET assignments, got %d", len(stmt.Sets))
	}
}

// --- parseUpdateFromClause: FROM with subquery ---

func testUpdateFromClauseError(t *testing.T) {
	t.Parallel()
	// FROM without a table name should produce an error.
	mustParseErr(t, `UPDATE t SET a=1 FROM WHERE x=1`)
}

// --- parseReturningClause: DELETE RETURNING multiple columns ---

func testDeleteReturningMultipleColumns(t *testing.T) {
	t.Parallel()
	sql := `DELETE FROM t WHERE id > 10 RETURNING id, name`
	stmt := mustParseOne(t, sql).(*DeleteStmt)
	if len(stmt.Returning) != 2 {
		t.Errorf("expected 2 returning columns, got %d", len(stmt.Returning))
	}
}

// --- parseReturningClause: UPDATE RETURNING ---

func testUpdateReturningClause(t *testing.T) {
	t.Parallel()
	sql := `UPDATE t SET status = 'done' WHERE id = 1 RETURNING id, status`
	stmt := mustParseOne(t, sql).(*UpdateStmt)
	if len(stmt.Returning) != 2 {
		t.Errorf("expected 2 returning columns, got %d", len(stmt.Returning))
	}
}

// --- parseReturningClause: INSERT RETURNING ---

func testInsertReturningClause(t *testing.T) {
	t.Parallel()
	sql := `INSERT INTO t(a, b) VALUES(1, 2) RETURNING id, a`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if len(stmt.Returning) != 2 {
		t.Errorf("expected 2 returning columns, got %d", len(stmt.Returning))
	}
}

// --- parseSetAssignments: multiple assignments in DO UPDATE ---

func testParseSetAssignmentsMultiple(t *testing.T) {
	t.Parallel()
	sql := `INSERT INTO t(a,b,c) VALUES(1,2,3) ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=excluded.c`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	update := stmt.Upsert.Update
	if update == nil {
		t.Fatal("expected DoUpdateClause")
	}
	if len(update.Sets) != 2 {
		t.Errorf("expected 2 SET assignments, got %d", len(update.Sets))
	}
	if update.Sets[0].Column != "b" {
		t.Errorf("first SET column: got %q, want b", update.Sets[0].Column)
	}
	if update.Sets[1].Column != "c" {
		t.Errorf("second SET column: got %q, want c", update.Sets[1].Column)
	}
}

// --- parseSetAssignments: error path (non-identifier where column expected) ---

func testParseSetAssignmentsError(t *testing.T) {
	t.Parallel()
	// In DO UPDATE SET, expecting a column name but getting a number triggers error.
	mustParseErr(t, `INSERT INTO t(a) VALUES(1) ON CONFLICT DO UPDATE SET 1=2`)
}

// --- parseColumnOrConstraint: fallback path when both column and constraint parse fail ---

func testParseColumnOrConstraintFallback(t *testing.T) {
	t.Parallel()
	// A raw number in column position is neither a valid column def nor a table
	// constraint, exercising the colErr-returning fallback branch.
	mustParseErr(t, `CREATE TABLE t (123 INTEGER)`)
}
