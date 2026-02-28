// Package engine provides trigger execution logic for the Anthony SQLite clone.
package engine

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// TriggerContext holds the context needed for trigger execution.
type TriggerContext struct {
	Schema  *schema.Schema
	Pager   interface{} // pager.Pager interface
	Btree   interface{} // btree.BTree interface
	OldRow  map[string]interface{}
	NewRow  map[string]interface{}
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
	vm.Ctx = &vdbe.VDBEContext{
		Btree:  te.ctx.Btree,
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
	substitutedStmt, err := te.substituteOldNewReferences(stmt)
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

// substituteOldNewReferences walks the statement AST and replaces OLD.col and NEW.col
// references with the actual values from the trigger context.
func (te *TriggerExecutor) substituteOldNewReferences(stmt parser.Statement) (parser.Statement, error) {
	// This is a simplified implementation that handles basic cases
	// A full implementation would need to walk the entire AST and replace
	// all OLD.column and NEW.column references with literal values

	// For now, we'll document that OLD/NEW substitution happens here
	// The actual implementation would need an AST visitor pattern

	// TODO: Implement full AST traversal and substitution
	// For the current implementation, we'll return the statement as-is
	// and let the executor handle OLD/NEW references at evaluation time

	return stmt, nil
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
