// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestParserFK2 groups coverage-boosting subtests for the remaining low-coverage
// functions: applyConstraintReferences, parseFKOnClause, parseColumnOrConstraint,
// parseConstraintTarget, parseOptionalWhereExpr, and parseSetAssignments.
func TestParserFK2(t *testing.T) {
	t.Parallel()
	t.Run("FKColumnNoColumnList", testFK2ColumnNoColumnList)
	t.Run("FKColumnMultiColTable", testFK2ColumnMultiColTable)
	t.Run("FKOnDeleteSetNull", testFK2OnDeleteSetNull)
	t.Run("FKOnDeleteSetDefault", testFK2OnDeleteSetDefault)
	t.Run("FKOnUpdateSetDefault", testFK2OnUpdateSetDefault)
	t.Run("ConstraintTargetNoTarget", testFK2ConstraintTargetNoTarget)
	t.Run("ConstraintTargetOnConflict", testFK2ConstraintTargetOnConflict)
	t.Run("OptionalWhereExprAbsent", testFK2OptionalWhereExprAbsent)
	t.Run("OptionalWhereExprPresent", testFK2OptionalWhereExprPresent)
	t.Run("SetAssignmentsSubquery", testFK2SetAssignmentsSubquery)
	t.Run("SetAssignmentsMultipleUpdate", testFK2SetAssignmentsMultipleUpdate)
	t.Run("MultiColFKTableConstraint", testFK2ColumnMultiColTable)
}

// testFK2ColumnNoColumnList exercises the applyConstraintReferences branch
// where no column list follows the referenced table name (no parentheses).
func testFK2ColumnNoColumnList(t *testing.T) {
	t.Parallel()
	// REFERENCES without a column list exercises the branch where p.match(TK_LP)
	// is false and Columns stays nil.
	sql := `CREATE TABLE t (a INTEGER REFERENCES parent ON DELETE CASCADE)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if len(stmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(stmt.Columns))
	}
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.Table != "parent" {
		t.Errorf("FK table: got %q, want %q", fk.Table, "parent")
	}
	if len(fk.Columns) != 0 {
		t.Errorf("expected no FK columns, got %v", fk.Columns)
	}
	if fk.OnDelete != FKActionCascade {
		t.Errorf("OnDelete: got %v, want FKActionCascade", fk.OnDelete)
	}
}

// testFK2ColumnMultiColTable exercises applyConstraintReferences with a
// table-level multi-column FK (both FK column list and referenced column list).
func testFK2ColumnMultiColTable(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES other(x, y) ON DELETE CASCADE)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	if len(stmt.Constraints) != 1 {
		t.Fatalf("expected 1 table constraint, got %d", len(stmt.Constraints))
	}
	fkc := stmt.Constraints[0].ForeignKey
	if fkc == nil {
		t.Fatal("expected ForeignKeyTableConstraint")
	}
	if len(fkc.Columns) != 2 {
		t.Errorf("expected 2 FK source columns, got %d", len(fkc.Columns))
	}
	if len(fkc.ForeignKey.Columns) != 2 {
		t.Errorf("expected 2 FK ref columns, got %d", len(fkc.ForeignKey.Columns))
	}
	if fkc.ForeignKey.OnDelete != FKActionCascade {
		t.Errorf("OnDelete: got %v, want FKActionCascade", fkc.ForeignKey.OnDelete)
	}
}

// testFK2OnDeleteSetNull exercises parseFKOnClause ON DELETE SET NULL on a
// column-level REFERENCES constraint.
func testFK2OnDeleteSetNull(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INTEGER REFERENCES other(id) ON DELETE SET NULL ON UPDATE SET DEFAULT)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnDelete != FKActionSetNull {
		t.Errorf("OnDelete: got %v, want FKActionSetNull", fk.OnDelete)
	}
	if fk.OnUpdate != FKActionSetDefault {
		t.Errorf("OnUpdate: got %v, want FKActionSetDefault", fk.OnUpdate)
	}
}

// testFK2OnDeleteSetDefault exercises parseFKOnClause ON DELETE SET DEFAULT.
func testFK2OnDeleteSetDefault(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INTEGER REFERENCES other(id) ON DELETE SET DEFAULT)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnDelete != FKActionSetDefault {
		t.Errorf("OnDelete: got %v, want FKActionSetDefault", fk.OnDelete)
	}
}

// testFK2OnUpdateSetDefault exercises parseFKOnClause ON UPDATE SET DEFAULT.
func testFK2OnUpdateSetDefault(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INTEGER REFERENCES other(id) ON UPDATE SET DEFAULT)`
	stmt := mustParseOne(t, sql).(*CreateTableStmt)
	fk := stmt.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnUpdate != FKActionSetDefault {
		t.Errorf("OnUpdate: got %v, want FKActionSetDefault", fk.OnUpdate)
	}
}

// testFK2ConstraintTargetNoTarget exercises parseOptionalWhereExpr and the
// ON CONFLICT path without a conflict target (DO NOTHING, no target).
func testFK2ConstraintTargetNoTarget(t *testing.T) {
	t.Parallel()
	// No conflict target at all — exercises the branch in parseConflictTarget
	// where neither TK_ON nor TK_LP is present.
	sql := `INSERT INTO t(a) VALUES(1) ON CONFLICT DO NOTHING`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	if stmt.Upsert.Target != nil {
		t.Errorf("expected nil conflict target, got %v", stmt.Upsert.Target)
	}
}

// testFK2ConstraintTargetOnConflict exercises parseConstraintTarget via the
// ON CONFLICT ON CONSTRAINT syntax.
func testFK2ConstraintTargetOnConflict(t *testing.T) {
	t.Parallel()
	sql := `INSERT INTO t(a) VALUES(1) ON CONFLICT ON CONSTRAINT uq_a DO NOTHING`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	if stmt.Upsert.Target == nil {
		t.Fatal("expected conflict target")
	}
	if stmt.Upsert.Target.ConstraintName != "uq_a" {
		t.Errorf("ConstraintName: got %q, want uq_a", stmt.Upsert.Target.ConstraintName)
	}
}

// testFK2OptionalWhereExprAbsent exercises parseOptionalWhereExpr when no
// WHERE clause is present (the non-matching branch).
func testFK2OptionalWhereExprAbsent(t *testing.T) {
	t.Parallel()
	// ON CONFLICT DO UPDATE with no WHERE — parseOptionalWhereExpr returns early.
	sql := `INSERT INTO t(a) VALUES(1) ON CONFLICT(a) DO UPDATE SET a=excluded.a`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	update := stmt.Upsert.Update
	if update == nil {
		t.Fatal("expected DoUpdateClause")
	}
	if update.Where != nil {
		t.Error("expected nil WHERE in DO UPDATE (no WHERE clause given)")
	}
}

// testFK2OptionalWhereExprPresent exercises parseOptionalWhereExpr when a
// WHERE clause is present (the matching branch).
func testFK2OptionalWhereExprPresent(t *testing.T) {
	t.Parallel()
	sql := `INSERT INTO t(a) VALUES(1) ON CONFLICT(a) DO UPDATE SET a=excluded.a WHERE a IS NOT NULL`
	stmt := mustParseOne(t, sql).(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	update := stmt.Upsert.Update
	if update == nil {
		t.Fatal("expected DoUpdateClause")
	}
	if update.Where == nil {
		t.Error("expected non-nil WHERE in DO UPDATE")
	}
}

// testFK2SetAssignmentsSubquery exercises parseSetAssignments (via parseUpdateAssignments)
// where the assigned value is a scalar subquery.
func testFK2SetAssignmentsSubquery(t *testing.T) {
	t.Parallel()
	sql := `UPDATE t SET a = (SELECT max(id) FROM other)`
	stmt := mustParseOne(t, sql).(*UpdateStmt)
	if len(stmt.Sets) != 1 {
		t.Fatalf("expected 1 SET assignment, got %d", len(stmt.Sets))
	}
	if stmt.Sets[0].Column != "a" {
		t.Errorf("column: got %q, want a", stmt.Sets[0].Column)
	}
	if stmt.Sets[0].Value == nil {
		t.Error("expected non-nil subquery value")
	}
}

// testFK2SetAssignmentsMultipleUpdate exercises parseUpdateAssignments with
// multiple column assignments in a plain UPDATE statement.
func testFK2SetAssignmentsMultipleUpdate(t *testing.T) {
	t.Parallel()
	sql := `UPDATE t SET a = 1, b = 2`
	stmt := mustParseOne(t, sql).(*UpdateStmt)
	if len(stmt.Sets) != 2 {
		t.Fatalf("expected 2 SET assignments, got %d", len(stmt.Sets))
	}
	if stmt.Sets[0].Column != "a" {
		t.Errorf("first column: got %q, want a", stmt.Sets[0].Column)
	}
	if stmt.Sets[1].Column != "b" {
		t.Errorf("second column: got %q, want b", stmt.Sets[1].Column)
	}
}
