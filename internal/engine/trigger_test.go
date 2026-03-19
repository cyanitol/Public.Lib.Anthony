// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestNewTriggerExecutor tests creating a new trigger executor
func TestNewTriggerExecutor(t *testing.T) {
	ctx := &TriggerContext{
		Schema:    schema.NewSchema(),
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	if executor == nil {
		t.Fatal("NewTriggerExecutor() returned nil")
	}
	if executor.ctx != ctx {
		t.Error("NewTriggerExecutor() did not set context correctly")
	}
}

// TestPrepareOldRow tests preparing OLD row
func TestPrepareOldRow(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	rowData := map[string]interface{}{
		"id":   1,
		"name": "Alice",
	}

	oldRow := PrepareOldRow(table, rowData)
	if oldRow == nil {
		t.Fatal("PrepareOldRow() returned nil")
	}
	if oldRow["id"] != 1 {
		t.Errorf("oldRow[id] = %v, want 1", oldRow["id"])
	}
	if oldRow["name"] != "Alice" {
		t.Errorf("oldRow[name] = %v, want Alice", oldRow["name"])
	}
}

// TestPrepareOldRowNil tests preparing OLD row with nil data
func TestPrepareOldRowNil(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{{Name: "id"}},
	}

	oldRow := PrepareOldRow(table, nil)
	if oldRow != nil {
		t.Errorf("PrepareOldRow(nil) = %v, want nil", oldRow)
	}
}

// TestPrepareOldRowMissingColumns tests OLD row with missing columns
func TestPrepareOldRowMissingColumns(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	rowData := map[string]interface{}{
		"id": 1,
		// name is missing
	}

	oldRow := PrepareOldRow(table, rowData)
	if oldRow == nil {
		t.Fatal("PrepareOldRow() returned nil")
	}
	if _, ok := oldRow["name"]; ok {
		t.Error("oldRow should not contain name column")
	}
}

// TestPrepareNewRow tests preparing NEW row
func TestPrepareNewRow(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	rowData := map[string]interface{}{
		"id":   2,
		"name": "Bob",
	}

	newRow := PrepareNewRow(table, rowData)
	if newRow == nil {
		t.Fatal("PrepareNewRow() returned nil")
	}
	if newRow["id"] != 2 {
		t.Errorf("newRow[id] = %v, want 2", newRow["id"])
	}
	if newRow["name"] != "Bob" {
		t.Errorf("newRow[name] = %v, want Bob", newRow["name"])
	}
}

// TestPrepareNewRowNil tests preparing NEW row with nil data
func TestPrepareNewRowNil(t *testing.T) {
	table := &schema.Table{
		Columns: []*schema.Column{{Name: "id"}},
	}

	newRow := PrepareNewRow(table, nil)
	if newRow != nil {
		t.Errorf("PrepareNewRow(nil) = %v, want nil", newRow)
	}
}

// TestExecuteTriggersForInsert tests executing INSERT triggers
func TestExecuteTriggersForInsert(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow: map[string]interface{}{
			"id":   1,
			"name": "Test",
		},
	}

	// Should not error even with no triggers
	err := ExecuteTriggersForInsert(ctx)
	if err != nil {
		t.Errorf("ExecuteTriggersForInsert() returned error: %v", err)
	}
}

// TestExecuteAfterInsertTriggers tests executing AFTER INSERT triggers
func TestExecuteAfterInsertTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow: map[string]interface{}{
			"id": 1,
		},
	}

	err := ExecuteAfterInsertTriggers(ctx)
	if err != nil {
		t.Errorf("ExecuteAfterInsertTriggers() returned error: %v", err)
	}
}

// TestExecuteTriggersForUpdate tests executing UPDATE triggers
func TestExecuteTriggersForUpdate(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow: map[string]interface{}{
			"id":   1,
			"name": "Old",
		},
		NewRow: map[string]interface{}{
			"id":   1,
			"name": "New",
		},
	}

	updatedCols := []string{"name"}
	err := ExecuteTriggersForUpdate(ctx, updatedCols)
	if err != nil {
		t.Errorf("ExecuteTriggersForUpdate() returned error: %v", err)
	}
}

// TestExecuteAfterUpdateTriggers tests executing AFTER UPDATE triggers
func TestExecuteAfterUpdateTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
		NewRow:    map[string]interface{}{"id": 1},
	}

	updatedCols := []string{"name"}
	err := ExecuteAfterUpdateTriggers(ctx, updatedCols)
	if err != nil {
		t.Errorf("ExecuteAfterUpdateTriggers() returned error: %v", err)
	}
}

// TestExecuteTriggersForDelete tests executing DELETE triggers
func TestExecuteTriggersForDelete(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow: map[string]interface{}{
			"id":   1,
			"name": "Test",
		},
	}

	err := ExecuteTriggersForDelete(ctx)
	if err != nil {
		t.Errorf("ExecuteTriggersForDelete() returned error: %v", err)
	}
}

// TestExecuteAfterDeleteTriggers tests executing AFTER DELETE triggers
func TestExecuteAfterDeleteTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
	}

	err := ExecuteAfterDeleteTriggers(ctx)
	if err != nil {
		t.Errorf("ExecuteAfterDeleteTriggers() returned error: %v", err)
	}
}

// TestExecuteBeforeTriggers tests executing BEFORE triggers
func TestExecuteBeforeTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	err := executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteBeforeTriggers() returned error: %v", err)
	}
}

// TestExecuteAfterTriggers tests executing AFTER triggers
func TestExecuteAfterTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	err := executor.ExecuteAfterTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteAfterTriggers() returned error: %v", err)
	}
}

// TestExecuteInsteadOfTriggers tests executing INSTEAD OF triggers
func TestExecuteInsteadOfTriggers(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	err := executor.ExecuteInsteadOfTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteInsteadOfTriggers() returned error: %v", err)
	}
}

// TestExecuteTriggerBodyEmpty tests executing trigger with empty body
func TestExecuteTriggerBodyEmpty(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	trigger := &schema.Trigger{
		Name: "test_trigger",
		Body: []parser.Statement{},
	}

	err := executor.executeTriggerBody(trigger)
	if err != nil {
		t.Errorf("executeTriggerBody() with empty body returned error: %v", err)
	}
}

// TestSubstituteOldNewReferences tests substituting OLD/NEW references
func TestSubstituteOldNewReferences(t *testing.T) {
	sch := schema.NewSchema()
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	stmt := &parser.InsertStmt{Table: "test"}

	// Should not error - creates a copy with OLD/NEW refs substituted
	result, err := executor.SubstituteOldNewReferences(stmt)
	if err != nil {
		t.Errorf("SubstituteOldNewReferences() returned error: %v", err)
	}
	if result == nil {
		t.Error("SubstituteOldNewReferences() should return a non-nil statement")
	}
}

// TestExecuteInsertInTrigger tests executing INSERT in trigger
func TestExecuteInsertInTrigger(t *testing.T) {
	// Skip test - requires full database setup
	t.Skip("Requires full database setup")
}

// TestExecuteUpdateInTrigger tests executing UPDATE in trigger
func TestExecuteUpdateInTrigger(t *testing.T) {
	// Skip test - requires full database setup
	t.Skip("Requires full database setup")
}

// TestExecuteDeleteInTrigger tests executing DELETE in trigger
func TestExecuteDeleteInTrigger(t *testing.T) {
	// Skip test - requires full database setup
	t.Skip("Requires full database setup")
}

// TestExecuteSelectInTrigger tests executing SELECT in trigger
func TestExecuteSelectInTrigger(t *testing.T) {
	// Skip test - requires full database setup
	t.Skip("Requires full database setup")
}

// TestCompileAndExecuteStatementUnsupported tests unsupported statement type
func TestCompileAndExecuteStatementUnsupported(t *testing.T) {
	// Skip test - requires full database setup
	t.Skip("Requires full database setup")
}
