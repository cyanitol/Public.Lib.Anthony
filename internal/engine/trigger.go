// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package engine provides trigger execution logic for the Anthony SQLite clone.
package engine

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/types"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TriggerContext holds the context needed for trigger execution.
type TriggerContext struct {
	Schema    *schema.Schema
	Pager     interface{} // pager.Pager interface
	Btree     interface{} // btree.BTree interface
	OldRow    map[string]interface{}
	NewRow    map[string]interface{}
	TableName string
}

// TriggerExecutor handles trigger execution.
type TriggerExecutor struct {
	ctx *TriggerContext
}

// NewTriggerExecutor creates a new trigger executor.
func NewTriggerExecutor(ctx *TriggerContext) *TriggerExecutor {
	return &TriggerExecutor{ctx: ctx}
}

// ExecuteBeforeTriggers executes all BEFORE triggers for the given event.
// Returns an error if any trigger fails.
func (te *TriggerExecutor) ExecuteBeforeTriggers(event parser.TriggerEvent, updatedColumns []string) error {
	timing := parser.TriggerBefore
	triggers := te.ctx.Schema.GetTableTriggers(te.ctx.TableName, &timing, &event)

	for _, trigger := range triggers {
		// Check if trigger matches the updated columns (for UPDATE events)
		if !trigger.MatchesUpdateColumns(updatedColumns) {
			continue
		}

		// Check WHEN clause
		shouldExecute, err := trigger.ShouldExecuteTrigger(te.ctx.OldRow, te.ctx.NewRow)
		if err != nil {
			return fmt.Errorf("error evaluating WHEN clause for trigger %s: %w", trigger.Name, err)
		}
		if !shouldExecute {
			continue
		}

		// Execute trigger body
		if err := te.executeTriggerBody(trigger); err != nil {
			return fmt.Errorf("error executing BEFORE trigger %s: %w", trigger.Name, err)
		}
	}

	return nil
}

// ExecuteAfterTriggers executes all AFTER triggers for the given event.
// Returns an error if any trigger fails.
func (te *TriggerExecutor) ExecuteAfterTriggers(event parser.TriggerEvent, updatedColumns []string) error {
	timing := parser.TriggerAfter
	triggers := te.ctx.Schema.GetTableTriggers(te.ctx.TableName, &timing, &event)

	for _, trigger := range triggers {
		// Check if trigger matches the updated columns (for UPDATE events)
		if !trigger.MatchesUpdateColumns(updatedColumns) {
			continue
		}

		// Check WHEN clause
		shouldExecute, err := trigger.ShouldExecuteTrigger(te.ctx.OldRow, te.ctx.NewRow)
		if err != nil {
			return fmt.Errorf("error evaluating WHEN clause for trigger %s: %w", trigger.Name, err)
		}
		if !shouldExecute {
			continue
		}

		// Execute trigger body
		if err := te.executeTriggerBody(trigger); err != nil {
			return fmt.Errorf("error executing AFTER trigger %s: %w", trigger.Name, err)
		}
	}

	return nil
}

// ExecuteInsteadOfTriggers executes all INSTEAD OF triggers for the given event.
// Returns an error if any trigger fails.
// INSTEAD OF triggers are typically used with views.
func (te *TriggerExecutor) ExecuteInsteadOfTriggers(event parser.TriggerEvent, updatedColumns []string) error {
	timing := parser.TriggerInsteadOf
	triggers := te.ctx.Schema.GetTableTriggers(te.ctx.TableName, &timing, &event)

	for _, trigger := range triggers {
		// Check if trigger matches the updated columns (for UPDATE events)
		if !trigger.MatchesUpdateColumns(updatedColumns) {
			continue
		}

		// Check WHEN clause
		shouldExecute, err := trigger.ShouldExecuteTrigger(te.ctx.OldRow, te.ctx.NewRow)
		if err != nil {
			return fmt.Errorf("error evaluating WHEN clause for trigger %s: %w", trigger.Name, err)
		}
		if !shouldExecute {
			continue
		}

		// Execute trigger body
		if err := te.executeTriggerBody(trigger); err != nil {
			return fmt.Errorf("error executing INSTEAD OF trigger %s: %w", trigger.Name, err)
		}
	}

	return nil
}

// executeTriggerBody executes the statements in a trigger's body.
func (te *TriggerExecutor) executeTriggerBody(trigger *schema.Trigger) error {
	// For each statement in the trigger body, we need to:
	// 1. Parse/compile it (it's already parsed as part of trigger definition)
	// 2. Execute it with OLD and NEW pseudo-records in context
	// 3. Handle any errors

	for i, stmt := range trigger.Body {
		if err := te.executeStatement(stmt, trigger); err != nil {
			return fmt.Errorf("error executing statement %d in trigger %s: %w", i+1, trigger.Name, err)
		}
	}

	return nil
}

// executeStatement executes a single statement from a trigger body.
func (te *TriggerExecutor) executeStatement(stmt parser.Statement, trigger *schema.Trigger) error {
	// Create a VDBE for this statement
	vm := vdbe.New()

	// Type assert Btree to the typed interface
	var btreeAccess types.BtreeAccess
	if te.ctx.Btree != nil {
		if bt, ok := te.ctx.Btree.(types.BtreeAccess); ok {
			btreeAccess = bt
		}
	}

	vm.Ctx = &vdbe.VDBEContext{
		Btree:  btreeAccess,
		Pager:  te.ctx.Pager,
		Schema: te.ctx.Schema,
	}

	// Compile the statement
	// Note: In a full implementation, we'd need to substitute OLD and NEW references
	// with actual values from te.ctx.OldRow and te.ctx.NewRow

	// For now, we'll compile and execute the statement as-is
	// A complete implementation would need to:
	// 1. Walk the AST and replace OLD.column with actual old values
	// 2. Walk the AST and replace NEW.column with actual new values
	// 3. Then compile and execute

	if err := te.compileAndExecuteStatement(vm, stmt); err != nil {
		return err
	}

	return nil
}

// compileAndExecuteStatement compiles and executes a trigger statement.
func (te *TriggerExecutor) compileAndExecuteStatement(vm *vdbe.VDBE, stmt parser.Statement) error {
	// Substitute OLD and NEW references in the statement
	substitutedStmt, err := te.SubstituteOldNewReferences(stmt)
	if err != nil {
		return fmt.Errorf("failed to substitute OLD/NEW references: %w", err)
	}

	// For basic statements, we can compile them directly
	switch s := substitutedStmt.(type) {
	case *parser.InsertStmt:
		return te.executeInsert(vm, s)
	case *parser.UpdateStmt:
		return te.executeUpdate(vm, s)
	case *parser.DeleteStmt:
		return te.executeDelete(vm, s)
	case *parser.SelectStmt:
		// SELECT statements in triggers are allowed (for side effects)
		// but their results are discarded
		return te.executeSelect(vm, s)
	default:
		// For other statement types, we'll skip them for now
		return fmt.Errorf("unsupported statement type in trigger: %T", stmt)
	}
}

// SubstituteOldNewReferences walks the statement AST and replaces OLD.col and NEW.col references with actual values.
// This function creates a modified copy of the statement with OLD and NEW references replaced by literal values.
// This is exported so it can be used by the driver package during trigger compilation.
func (te *TriggerExecutor) SubstituteOldNewReferences(stmt parser.Statement) (parser.Statement, error) {
	// For each statement type, walk the AST and substitute OLD/NEW references
	switch s := stmt.(type) {
	case *parser.InsertStmt:
		return te.substituteInInsert(s)
	case *parser.UpdateStmt:
		return te.substituteInUpdate(s)
	case *parser.DeleteStmt:
		return te.substituteInDelete(s)
	case *parser.SelectStmt:
		return te.substituteInSelect(s)
	default:
		// For unsupported statement types, return unchanged
		return stmt, nil
	}
}

// substituteInInsert substitutes OLD/NEW references in INSERT statements.
func (te *TriggerExecutor) substituteInInsert(stmt *parser.InsertStmt) (parser.Statement, error) {
	// Create a copy of the INSERT statement
	newStmt := *stmt

	// Substitute values in VALUES clause
	if len(stmt.Values) > 0 {
		newStmt.Values = make([][]parser.Expression, len(stmt.Values))
		for i, row := range stmt.Values {
			newStmt.Values[i] = make([]parser.Expression, len(row))
			for j, expr := range row {
				substitutedExpr, err := te.substituteExpression(expr)
				if err != nil {
					return nil, err
				}
				newStmt.Values[i][j] = substitutedExpr
			}
		}
	}

	return &newStmt, nil
}

// substituteInUpdate substitutes OLD/NEW references in UPDATE statements.
func (te *TriggerExecutor) substituteInUpdate(stmt *parser.UpdateStmt) (parser.Statement, error) {
	// Create a copy of the UPDATE statement
	newStmt := *stmt

	// Substitute in SET clauses
	newStmt.Sets = make([]parser.Assignment, len(stmt.Sets))
	for i, assign := range stmt.Sets {
		substitutedValue, err := te.substituteExpression(assign.Value)
		if err != nil {
			return nil, err
		}
		newStmt.Sets[i] = parser.Assignment{
			Column: assign.Column,
			Value:  substitutedValue,
		}
	}

	// Substitute in WHERE clause
	if stmt.Where != nil {
		substitutedWhere, err := te.substituteExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = substitutedWhere
	}

	return &newStmt, nil
}

// substituteInDelete substitutes OLD/NEW references in DELETE statements.
func (te *TriggerExecutor) substituteInDelete(stmt *parser.DeleteStmt) (parser.Statement, error) {
	// Create a copy of the DELETE statement
	newStmt := *stmt

	// Substitute in WHERE clause
	if stmt.Where != nil {
		substitutedWhere, err := te.substituteExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = substitutedWhere
	}

	return &newStmt, nil
}

// substituteInSelect substitutes OLD/NEW references in SELECT statements.
func (te *TriggerExecutor) substituteInSelect(stmt *parser.SelectStmt) (parser.Statement, error) {
	// Create a copy of the SELECT statement
	newStmt := *stmt

	// Substitute in WHERE clause
	if stmt.Where != nil {
		substitutedWhere, err := te.substituteExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = substitutedWhere
	}

	// Note: A full implementation would also substitute in:
	// - Column expressions
	// - HAVING clause
	// - ORDER BY expressions
	// For now, we handle the most common case (WHERE clause)

	return &newStmt, nil
}

// substituteExpression recursively substitutes OLD/NEW references in an expression.
func (te *TriggerExecutor) substituteExpression(expr parser.Expression) (parser.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.IdentExpr:
		return te.handleIdentExpr(e)
	case *parser.BinaryExpr:
		return te.handleBinaryExpr(e)
	case *parser.UnaryExpr:
		return te.handleUnaryExpr(e)
	case *parser.FunctionExpr:
		return te.handleFunctionExpr(e)
	default:
		// For other expression types (literals, etc.), return unchanged
		return expr, nil
	}
}

// handleIdentExpr handles identifier expressions (OLD.col or NEW.col).
func (te *TriggerExecutor) handleIdentExpr(e *parser.IdentExpr) (parser.Expression, error) {
	// Check if this is an OLD.col or NEW.col reference
	if e.Table != "" {
		return te.substituteIdentExpr(e)
	}
	// Unqualified column reference - leave as is
	return e, nil
}

// handleBinaryExpr handles binary expressions.
func (te *TriggerExecutor) handleBinaryExpr(e *parser.BinaryExpr) (parser.Expression, error) {
	// Recursively substitute in left and right operands
	leftSubst, err := te.substituteExpression(e.Left)
	if err != nil {
		return nil, err
	}
	rightSubst, err := te.substituteExpression(e.Right)
	if err != nil {
		return nil, err
	}

	// Create a new BinaryExpr with substituted operands
	return &parser.BinaryExpr{
		Left:  leftSubst,
		Op:    e.Op,
		Right: rightSubst,
	}, nil
}

// handleUnaryExpr handles unary expressions.
func (te *TriggerExecutor) handleUnaryExpr(e *parser.UnaryExpr) (parser.Expression, error) {
	// Recursively substitute in the operand
	exprSubst, err := te.substituteExpression(e.Expr)
	if err != nil {
		return nil, err
	}
	return &parser.UnaryExpr{
		Op:   e.Op,
		Expr: exprSubst,
	}, nil
}

// handleFunctionExpr handles function expressions.
func (te *TriggerExecutor) handleFunctionExpr(e *parser.FunctionExpr) (parser.Expression, error) {
	// Recursively substitute in function arguments
	newArgs := make([]parser.Expression, len(e.Args))
	for i, arg := range e.Args {
		argSubst, err := te.substituteExpression(arg)
		if err != nil {
			return nil, err
		}
		newArgs[i] = argSubst
	}
	return &parser.FunctionExpr{
		Name: e.Name,
		Args: newArgs,
		Star: e.Star,
	}, nil
}

// substituteIdentExpr substitutes an identifier expression (OLD.col or NEW.col) with a literal value.
func (te *TriggerExecutor) substituteIdentExpr(expr *parser.IdentExpr) (parser.Expression, error) {
	qualifier := strings.ToLower(expr.Table)
	colName := strings.ToLower(expr.Name)

	var value interface{}
	var found bool

	switch qualifier {
	case "new":
		if te.ctx.NewRow == nil {
			return nil, fmt.Errorf("NEW is not available in this trigger context")
		}
		value, found = te.ctx.NewRow[colName]
		if !found {
			return nil, fmt.Errorf("column not found in NEW: %s", expr.Name)
		}

	case "old":
		if te.ctx.OldRow == nil {
			return nil, fmt.Errorf("OLD is not available in this trigger context")
		}
		value, found = te.ctx.OldRow[colName]
		if !found {
			return nil, fmt.Errorf("column not found in OLD: %s", expr.Name)
		}

	default:
		// Not an OLD/NEW reference, return unchanged
		return expr, nil
	}

	// Convert the value to a LiteralExpr
	return te.valueToLiteralExpr(value), nil
}

// valueToLiteralExpr converts a Go value to a parser.LiteralExpr.
func (te *TriggerExecutor) valueToLiteralExpr(value interface{}) *parser.LiteralExpr {
	if value == nil {
		return &parser.LiteralExpr{
			Type:  parser.LiteralNull,
			Value: "NULL",
		}
	}

	switch v := value.(type) {
	case int64:
		return &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: fmt.Sprintf("%d", v),
		}
	case float64:
		return &parser.LiteralExpr{
			Type:  parser.LiteralFloat,
			Value: fmt.Sprintf("%g", v),
		}
	case string:
		return &parser.LiteralExpr{
			Type:  parser.LiteralString,
			Value: v,
		}
	case bool:
		if v {
			return &parser.LiteralExpr{
				Type:  parser.LiteralInteger,
				Value: "1",
			}
		}
		return &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: "0",
		}
	default:
		// For unknown types, return NULL
		return &parser.LiteralExpr{
			Type:  parser.LiteralNull,
			Value: "NULL",
		}
	}
}

// Helper functions for executing different statement types
// These are simplified versions - a full implementation would use the proper compiler

func (te *TriggerExecutor) executeInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt) error {
	// Simplified INSERT execution
	// In a real implementation, this would use the full INSERT compiler
	// with OLD/NEW substitution
	if err := vm.Run(); err != nil {
		return fmt.Errorf("INSERT execution failed: %w", err)
	}
	return nil
}

func (te *TriggerExecutor) executeUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt) error {
	// Simplified UPDATE execution
	if err := vm.Run(); err != nil {
		return fmt.Errorf("UPDATE execution failed: %w", err)
	}
	return nil
}

func (te *TriggerExecutor) executeDelete(vm *vdbe.VDBE, stmt *parser.DeleteStmt) error {
	// Simplified DELETE execution
	if err := vm.Run(); err != nil {
		return fmt.Errorf("DELETE execution failed: %w", err)
	}
	return nil
}

func (te *TriggerExecutor) executeSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt) error {
	// SELECT in trigger - execute but discard results
	if err := vm.Run(); err != nil {
		return fmt.Errorf("SELECT execution failed: %w", err)
	}
	return nil
}

// PrepareOldRow prepares the OLD pseudo-record for trigger execution.
// For INSERT triggers, OLD is not available.
// For UPDATE and DELETE triggers, OLD contains the row before the operation.
func PrepareOldRow(table *schema.Table, rowData map[string]interface{}) map[string]interface{} {
	if rowData == nil {
		return nil
	}

	oldRow := make(map[string]interface{})
	for _, col := range table.Columns {
		if val, ok := rowData[col.Name]; ok {
			oldRow[col.Name] = val
		}
	}
	return oldRow
}

// PrepareNewRow prepares the NEW pseudo-record for trigger execution.
// For DELETE triggers, NEW is not available.
// For INSERT and UPDATE triggers, NEW contains the row after the operation.
func PrepareNewRow(table *schema.Table, rowData map[string]interface{}) map[string]interface{} {
	if rowData == nil {
		return nil
	}

	newRow := make(map[string]interface{})
	for _, col := range table.Columns {
		if val, ok := rowData[col.Name]; ok {
			newRow[col.Name] = val
		}
	}
	return newRow
}

// ExecuteTriggersForInsert is a convenience function that executes all triggers for an INSERT operation.
func ExecuteTriggersForInsert(ctx *TriggerContext) error {
	executor := NewTriggerExecutor(ctx)

	// Execute BEFORE INSERT triggers
	if err := executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil); err != nil {
		return err
	}

	// Note: The actual INSERT happens between BEFORE and AFTER triggers
	// This is handled by the caller

	return nil
}

// ExecuteAfterInsertTriggers executes AFTER INSERT triggers.
func ExecuteAfterInsertTriggers(ctx *TriggerContext) error {
	executor := NewTriggerExecutor(ctx)
	return executor.ExecuteAfterTriggers(parser.TriggerInsert, nil)
}

// ExecuteTriggersForUpdate executes triggers for an UPDATE operation.
func ExecuteTriggersForUpdate(ctx *TriggerContext, updatedColumns []string) error {
	executor := NewTriggerExecutor(ctx)

	// Execute BEFORE UPDATE triggers
	if err := executor.ExecuteBeforeTriggers(parser.TriggerUpdate, updatedColumns); err != nil {
		return err
	}

	return nil
}

// ExecuteAfterUpdateTriggers executes AFTER UPDATE triggers.
func ExecuteAfterUpdateTriggers(ctx *TriggerContext, updatedColumns []string) error {
	executor := NewTriggerExecutor(ctx)
	return executor.ExecuteAfterTriggers(parser.TriggerUpdate, updatedColumns)
}

// ExecuteTriggersForDelete executes triggers for a DELETE operation.
func ExecuteTriggersForDelete(ctx *TriggerContext) error {
	executor := NewTriggerExecutor(ctx)

	// Execute BEFORE DELETE triggers
	if err := executor.ExecuteBeforeTriggers(parser.TriggerDelete, nil); err != nil {
		return err
	}

	return nil
}

// ExecuteAfterDeleteTriggers executes AFTER DELETE triggers.
func ExecuteAfterDeleteTriggers(ctx *TriggerContext) error {
	executor := NewTriggerExecutor(ctx)
	return executor.ExecuteAfterTriggers(parser.TriggerDelete, nil)
}
