// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// newTestExecutor creates a TriggerExecutor with the given OLD/NEW rows.
func newTestExecutor(oldRow, newRow map[string]interface{}) *TriggerExecutor {
	ctx := &TriggerContext{
		Schema:    schema.NewSchema(),
		TableName: "test",
		OldRow:    oldRow,
		NewRow:    newRow,
	}
	return NewTriggerExecutor(ctx)
}

// --- substituteExpression / handleIdentExpr ---

func TestHandleIdentExpr_Unqualified(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.IdentExpr{Name: "id"}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expr {
		t.Errorf("expected same pointer for unqualified ident, got %v", result)
	}
}

func TestHandleIdentExpr_NewRef(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"name": "Alice"})
	expr := &parser.IdentExpr{Table: "NEW", Name: "name"}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lit, ok := result.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr, got %T", result)
	}
	if lit.Value != "Alice" {
		t.Errorf("expected 'Alice', got %q", lit.Value)
	}
}

func TestHandleIdentExpr_OldRef(t *testing.T) {
	te := newTestExecutor(map[string]interface{}{"id": int64(42)}, nil)
	expr := &parser.IdentExpr{Table: "OLD", Name: "id"}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lit, ok := result.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr, got %T", result)
	}
	if lit.Value != "42" {
		t.Errorf("expected '42', got %q", lit.Value)
	}
}

func TestHandleIdentExpr_NewNil(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.IdentExpr{Table: "NEW", Name: "id"}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error when NEW is nil")
	}
}

func TestHandleIdentExpr_OldNil(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.IdentExpr{Table: "OLD", Name: "id"}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error when OLD is nil")
	}
}

func TestHandleIdentExpr_ColumnMissingNew(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"name": "Bob"})
	expr := &parser.IdentExpr{Table: "NEW", Name: "nonexistent"}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error for missing column in NEW")
	}
}

func TestHandleIdentExpr_ColumnMissingOld(t *testing.T) {
	te := newTestExecutor(map[string]interface{}{"name": "Bob"}, nil)
	expr := &parser.IdentExpr{Table: "OLD", Name: "nonexistent"}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error for missing column in OLD")
	}
}

func TestHandleIdentExpr_OtherQualifier(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.IdentExpr{Table: "t", Name: "col"}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expr {
		t.Error("non-OLD/NEW qualifier should return original expression")
	}
}

// --- handleBinaryExpr ---

func TestHandleBinaryExpr_SubstitutesOperands(t *testing.T) {
	te := newTestExecutor(
		map[string]interface{}{"x": int64(10)},
		map[string]interface{}{"x": int64(20)},
	)
	expr := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "OLD", Name: "x"},
		Op:    parser.OpPlus,
		Right: &parser.IdentExpr{Table: "NEW", Name: "x"},
	}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bin, ok := result.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", result)
	}
	leftLit, ok := bin.Left.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected left to be LiteralExpr, got %T", bin.Left)
	}
	if leftLit.Value != "10" {
		t.Errorf("expected left '10', got %q", leftLit.Value)
	}
	rightLit, ok := bin.Right.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected right to be LiteralExpr, got %T", bin.Right)
	}
	if rightLit.Value != "20" {
		t.Errorf("expected right '20', got %q", rightLit.Value)
	}
}

func TestHandleBinaryExpr_ErrorOnLeft(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "NEW", Name: "x"},
		Op:    parser.OpPlus,
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error propagated from left operand substitution")
	}
}

func TestHandleBinaryExpr_ErrorOnRight(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"x": int64(5)})
	expr := &parser.BinaryExpr{
		Left:  &parser.IdentExpr{Table: "NEW", Name: "x"},
		Op:    parser.OpPlus,
		Right: &parser.IdentExpr{Table: "OLD", Name: "y"}, // OLD is nil
	}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error propagated from right operand substitution")
	}
}

// --- handleUnaryExpr ---

func TestHandleUnaryExpr_Negation(t *testing.T) {
	te := newTestExecutor(map[string]interface{}{"val": int64(7)}, nil)
	expr := &parser.UnaryExpr{
		Op:   parser.OpNeg,
		Expr: &parser.IdentExpr{Table: "OLD", Name: "val"},
	}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	unary, ok := result.(*parser.UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", result)
	}
	inner, ok := unary.Expr.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected inner LiteralExpr, got %T", unary.Expr)
	}
	if inner.Value != "7" {
		t.Errorf("expected '7', got %q", inner.Value)
	}
}

func TestHandleUnaryExpr_Error(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.UnaryExpr{
		Op:   parser.OpNeg,
		Expr: &parser.IdentExpr{Table: "OLD", Name: "val"},
	}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error propagated from unary operand")
	}
}

// --- handleFunctionExpr ---

func TestHandleFunctionExpr_SubstitutesArgs(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"name": "hello"})
	expr := &parser.FunctionExpr{
		Name: "upper",
		Args: []parser.Expression{
			&parser.IdentExpr{Table: "NEW", Name: "name"},
		},
	}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fn, ok := result.(*parser.FunctionExpr)
	if !ok {
		t.Fatalf("expected FunctionExpr, got %T", result)
	}
	if fn.Name != "upper" {
		t.Errorf("expected function name 'upper', got %q", fn.Name)
	}
	lit, ok := fn.Args[0].(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected arg to be LiteralExpr, got %T", fn.Args[0])
	}
	if lit.Value != "hello" {
		t.Errorf("expected 'hello', got %q", lit.Value)
	}
}

func TestHandleFunctionExpr_NoArgs(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.FunctionExpr{
		Name: "now",
		Args: nil,
	}
	result, err := te.substituteExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fn, ok := result.(*parser.FunctionExpr)
	if !ok {
		t.Fatalf("expected FunctionExpr, got %T", result)
	}
	if len(fn.Args) != 0 {
		t.Errorf("expected no args, got %d", len(fn.Args))
	}
}

func TestHandleFunctionExpr_Error(t *testing.T) {
	te := newTestExecutor(nil, nil)
	expr := &parser.FunctionExpr{
		Name: "upper",
		Args: []parser.Expression{
			&parser.IdentExpr{Table: "NEW", Name: "name"}, // NEW is nil
		},
	}
	_, err := te.substituteExpression(expr)
	if err == nil {
		t.Error("expected error propagated from function argument substitution")
	}
}

// --- valueToLiteralExpr ---

func TestValueToLiteralExpr_Types(t *testing.T) {
	te := newTestExecutor(nil, nil)

	cases := []struct {
		value    interface{}
		wantType parser.LiteralType
		wantVal  string
	}{
		{nil, parser.LiteralNull, "NULL"},
		{int64(99), parser.LiteralInteger, "99"},
		{float64(3.14), parser.LiteralFloat, "3.14"},
		{"hello", parser.LiteralString, "hello"},
		{true, parser.LiteralInteger, "1"},
		{false, parser.LiteralInteger, "0"},
		{[]byte("blob"), parser.LiteralNull, "NULL"}, // unknown type -> NULL
	}

	for _, tc := range cases {
		lit := te.valueToLiteralExpr(tc.value)
		if lit.Type != tc.wantType {
			t.Errorf("value %v: want type %v, got %v", tc.value, tc.wantType, lit.Type)
		}
		if lit.Value != tc.wantVal {
			t.Errorf("value %v: want value %q, got %q", tc.value, tc.wantVal, lit.Value)
		}
	}
}

// --- substituteExpression with nil ---

func TestSubstituteExpression_Nil(t *testing.T) {
	te := newTestExecutor(nil, nil)
	result, err := te.substituteExpression(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestSubstituteExpression_Literal(t *testing.T) {
	te := newTestExecutor(nil, nil)
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "5"}
	result, err := te.substituteExpression(lit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != lit {
		t.Error("literal expressions should be returned unchanged")
	}
}

// --- substituteInUpdate ---

func TestSubstituteInUpdate_WithWhere(t *testing.T) {
	te := newTestExecutor(
		map[string]interface{}{"id": int64(1)},
		map[string]interface{}{"status": "active"},
	)
	stmt := &parser.UpdateStmt{
		Table: "logs",
		Sets: []parser.Assignment{
			{Column: "status", Value: &parser.IdentExpr{Table: "NEW", Name: "status"}},
		},
		Where: &parser.IdentExpr{Table: "OLD", Name: "id"},
	}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	upd, ok := result.(*parser.UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", result)
	}
	setLit, ok := upd.Sets[0].Value.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected SET value to be LiteralExpr, got %T", upd.Sets[0].Value)
	}
	if setLit.Value != "active" {
		t.Errorf("expected 'active', got %q", setLit.Value)
	}
	whereLit, ok := upd.Where.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected WHERE to be LiteralExpr, got %T", upd.Where)
	}
	if whereLit.Value != "1" {
		t.Errorf("expected '1', got %q", whereLit.Value)
	}
}

func TestSubstituteInUpdate_NoWhere(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"val": int64(0)})
	stmt := &parser.UpdateStmt{
		Table: "t",
		Sets: []parser.Assignment{
			{Column: "val", Value: &parser.IdentExpr{Table: "NEW", Name: "val"}},
		},
	}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	upd := result.(*parser.UpdateStmt)
	if upd.Where != nil {
		t.Error("expected nil WHERE clause")
	}
}

func TestSubstituteInUpdate_SetError(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.UpdateStmt{
		Table: "t",
		Sets: []parser.Assignment{
			{Column: "x", Value: &parser.IdentExpr{Table: "NEW", Name: "x"}},
		},
	}
	_, err := te.SubstituteOldNewReferences(stmt)
	if err == nil {
		t.Error("expected error when NEW is nil")
	}
}

func TestSubstituteInUpdate_WhereError(t *testing.T) {
	te := newTestExecutor(nil, map[string]interface{}{"x": int64(1)})
	stmt := &parser.UpdateStmt{
		Table: "t",
		Sets: []parser.Assignment{
			{Column: "x", Value: &parser.IdentExpr{Table: "NEW", Name: "x"}},
		},
		Where: &parser.IdentExpr{Table: "OLD", Name: "id"}, // OLD is nil
	}
	_, err := te.SubstituteOldNewReferences(stmt)
	if err == nil {
		t.Error("expected error when OLD is nil in WHERE")
	}
}

// --- substituteInDelete ---

func TestSubstituteInDelete_WithWhere(t *testing.T) {
	te := newTestExecutor(map[string]interface{}{"id": int64(5)}, nil)
	stmt := &parser.DeleteStmt{
		Table: "logs",
		Where: &parser.IdentExpr{Table: "OLD", Name: "id"},
	}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	del, ok := result.(*parser.DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", result)
	}
	lit, ok := del.Where.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected WHERE to be LiteralExpr, got %T", del.Where)
	}
	if lit.Value != "5" {
		t.Errorf("expected '5', got %q", lit.Value)
	}
}

func TestSubstituteInDelete_NoWhere(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.DeleteStmt{Table: "logs"}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	del := result.(*parser.DeleteStmt)
	if del.Where != nil {
		t.Error("expected nil WHERE")
	}
}

func TestSubstituteInDelete_WhereError(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.DeleteStmt{
		Table: "logs",
		Where: &parser.IdentExpr{Table: "OLD", Name: "id"},
	}
	_, err := te.SubstituteOldNewReferences(stmt)
	if err == nil {
		t.Error("expected error when OLD is nil")
	}
}

// --- substituteInSelect ---

func TestSubstituteInSelect_WithWhere(t *testing.T) {
	te := newTestExecutor(map[string]interface{}{"id": int64(3)}, nil)
	stmt := &parser.SelectStmt{
		Where: &parser.IdentExpr{Table: "OLD", Name: "id"},
	}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sel, ok := result.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", result)
	}
	lit, ok := sel.Where.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected WHERE to be LiteralExpr, got %T", sel.Where)
	}
	if lit.Value != "3" {
		t.Errorf("expected '3', got %q", lit.Value)
	}
}

func TestSubstituteInSelect_NoWhere(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.SelectStmt{}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sel := result.(*parser.SelectStmt)
	if sel.Where != nil {
		t.Error("expected nil WHERE")
	}
}

func TestSubstituteInSelect_WhereError(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.SelectStmt{
		Where: &parser.IdentExpr{Table: "NEW", Name: "id"},
	}
	_, err := te.SubstituteOldNewReferences(stmt)
	if err == nil {
		t.Error("expected error when NEW is nil")
	}
}

// --- ExecuteTriggersForInsert / Update / Delete with real triggers ---

func makeSchemaWithTable(name string, cols ...string) *schema.Schema {
	sch := schema.NewSchema()
	columns := make([]*schema.Column, len(cols))
	for i, c := range cols {
		columns[i] = &schema.Column{Name: c}
	}
	sch.Tables[name] = &schema.Table{Name: name, Columns: columns}
	return sch
}

func addTrigger(sch *schema.Schema, trig *schema.Trigger) {
	if sch.Triggers == nil {
		sch.Triggers = make(map[string]*schema.Trigger)
	}
	sch.Triggers[trig.Name] = trig
}

// TestExecuteTriggersForInsert_WithTrigger exercises the path where a trigger
// exists, fires, and its body is executed (insert body triggers the vm.Run path).
func TestExecuteTriggersForInsert_WithTrigger(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_ins",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{&parser.InsertStmt{Table: "audit"}},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		NewRow:    map[string]interface{}{"id": int64(1), "name": "Alice"},
	}
	// Error is acceptable (audit table doesn't exist in btree), just must not panic.
	_ = ExecuteTriggersForInsert(ctx)
}

// TestExecuteTriggersForUpdate_WithTrigger exercises BEFORE UPDATE trigger.
func TestExecuteTriggersForUpdate_WithTrigger(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_upd",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerUpdate,
		Body:   []parser.Statement{&parser.UpdateStmt{Table: "audit"}},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		OldRow:    map[string]interface{}{"id": int64(1), "name": "Old"},
		NewRow:    map[string]interface{}{"id": int64(1), "name": "New"},
	}
	_ = ExecuteTriggersForUpdate(ctx, []string{"name"})
}

// TestExecuteTriggersForDelete_WithTrigger exercises BEFORE DELETE trigger.
func TestExecuteTriggersForDelete_WithTrigger(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_del",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerDelete,
		Body:   []parser.Statement{&parser.DeleteStmt{Table: "audit"}},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		OldRow:    map[string]interface{}{"id": int64(1), "name": "Alice"},
	}
	_ = ExecuteTriggersForDelete(ctx)
}

// TestExecuteTriggersForInsert_WithSubstitution exercises trigger body
// that contains OLD/NEW substitution (an INSERT body with VALUES using NEW.id).
func TestExecuteTriggersForInsert_WithSubstitution(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_ins_subst",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerInsert,
		Body: []parser.Statement{
			&parser.InsertStmt{
				Table:   "audit",
				Columns: []string{"uid"},
				Values: [][]parser.Expression{
					{&parser.IdentExpr{Table: "NEW", Name: "id"}},
				},
			},
		},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		NewRow:    map[string]interface{}{"id": int64(7), "name": "Bob"},
	}
	// Substitution should succeed; vm.Run may fail due to missing btree — that is fine.
	_ = ExecuteAfterInsertTriggers(ctx)
}

// TestExecuteTriggersForUpdate_WithSubstitution exercises UPDATE trigger body
// that uses NEW.name and OLD.id in expressions.
func TestExecuteTriggersForUpdate_WithSubstitution(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_upd_subst",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerUpdate,
		Body: []parser.Statement{
			&parser.UpdateStmt{
				Table: "audit",
				Sets: []parser.Assignment{
					{Column: "new_name", Value: &parser.IdentExpr{Table: "NEW", Name: "name"}},
				},
				Where: &parser.BinaryExpr{
					Left:  &parser.IdentExpr{Table: "OLD", Name: "id"},
					Op:    parser.OpEq,
					Right: &parser.IdentExpr{Table: "NEW", Name: "id"},
				},
			},
		},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		OldRow:    map[string]interface{}{"id": int64(1), "name": "Old"},
		NewRow:    map[string]interface{}{"id": int64(1), "name": "New"},
	}
	_ = ExecuteAfterUpdateTriggers(ctx, []string{"name"})
}

// TestExecuteTriggersForDelete_WithSubstitution exercises DELETE trigger body
// that uses OLD.id in the WHERE clause.
func TestExecuteTriggersForDelete_WithSubstitution(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_del_subst",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerDelete,
		Body: []parser.Statement{
			&parser.DeleteStmt{
				Table: "audit",
				Where: &parser.IdentExpr{Table: "OLD", Name: "id"},
			},
		},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		OldRow:    map[string]interface{}{"id": int64(2), "name": "Carol"},
	}
	_ = ExecuteAfterDeleteTriggers(ctx)
}

// TestExecuteTriggersForInsert_SelectBody exercises SELECT body in trigger
// which is dispatched through compileAndExecuteStatement.
func TestExecuteTriggersForInsert_SelectBody(t *testing.T) {
	sch := makeSchemaWithTable("users", "id")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_sel",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerInsert,
		Body: []parser.Statement{
			&parser.SelectStmt{
				Where: &parser.IdentExpr{Table: "NEW", Name: "id"},
			},
		},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		NewRow:    map[string]interface{}{"id": int64(1)},
	}
	_ = ExecuteAfterInsertTriggers(ctx)
}

// TestExecuteTriggers_WhenClause exercises the ShouldExecuteTrigger path
// where a WHEN clause filters out the trigger.
func TestExecuteTriggers_WhenClauseFalse(t *testing.T) {
	sch := makeSchemaWithTable("users", "id")
	addTrigger(sch, &schema.Trigger{
		Name:   "trg_when_false",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		When: &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: "0", // WHEN 0 — never fires
		},
		Body: []parser.Statement{&parser.InsertStmt{Table: "audit"}},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		NewRow:    map[string]interface{}{"id": int64(1)},
	}
	err := ExecuteTriggersForInsert(ctx)
	if err != nil {
		t.Errorf("unexpected error with WHEN 0 trigger: %v", err)
	}
}

// TestExecuteTriggers_UnqualifiedUpdateOf covers UpdateOf column filtering path.
func TestExecuteTriggers_UpdateOfFilter(t *testing.T) {
	sch := makeSchemaWithTable("users", "id", "name", "email")
	addTrigger(sch, &schema.Trigger{
		Name:     "trg_upd_of",
		Table:    "users",
		Timing:   parser.TriggerBefore,
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{"email"}, // only fires when email is updated
		Body:     []parser.Statement{&parser.InsertStmt{Table: "audit"}},
	})
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "users",
		OldRow:    map[string]interface{}{"id": int64(1), "name": "Old", "email": "old@x"},
		NewRow:    map[string]interface{}{"id": int64(1), "name": "New", "email": "old@x"},
	}
	// Updating only "name" — trigger should be skipped (no error from body execution).
	err := ExecuteTriggersForUpdate(ctx, []string{"name"})
	if err != nil {
		t.Errorf("unexpected error when UPDATE OF filter skips trigger: %v", err)
	}
}

// TestSubstituteOldNewReferences_Unsupported exercises the default branch
// in SubstituteOldNewReferences (e.g. CreateTableStmt).
func TestSubstituteOldNewReferences_Unsupported(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.CreateTableStmt{Name: "x"}
	result, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != stmt {
		t.Error("unsupported statement type should be returned unchanged")
	}
}
