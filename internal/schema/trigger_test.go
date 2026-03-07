// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package schema

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

func TestCreateTrigger(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a table first
	s.Tables["users"] = &Table{Name: "users"}

	// Create a trigger
	stmt := &parser.CreateTriggerStmt{
		Name:       "audit_users",
		Table:      "users",
		Timing:     parser.TriggerAfter,
		Event:      parser.TriggerInsert,
		ForEachRow: true,
		Body: []parser.Statement{
			&parser.SelectStmt{},
		},
	}

	trigger, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger() error = %v", err)
	}

	if trigger.Name != "audit_users" {
		t.Errorf("trigger.Name = %q, want %q", trigger.Name, "audit_users")
	}
	if trigger.Table != "users" {
		t.Errorf("trigger.Table = %q, want %q", trigger.Table, "users")
	}
	if trigger.Timing != parser.TriggerAfter {
		t.Error("trigger.Timing should be AFTER")
	}
	if trigger.Event != parser.TriggerInsert {
		t.Error("trigger.Event should be INSERT")
	}
	if !trigger.ForEachRow {
		t.Error("trigger.ForEachRow should be true")
	}

	// Verify trigger is in schema
	if _, ok := s.GetTrigger("audit_users"); !ok {
		t.Error("trigger not found in schema")
	}
}

func TestCreateTriggerWithNilStatement(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	_, err := s.CreateTrigger(nil)
	if err == nil {
		t.Error("Expected error for nil statement")
	}
}

func TestCreateTriggerOnNonexistentTable(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateTriggerStmt{
		Name:   "my_trigger",
		Table:  "nonexistent",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerUpdate,
	}

	_, err := s.CreateTrigger(stmt)
	if err == nil {
		t.Error("Expected error creating trigger on nonexistent table")
	}
}

func TestCreateTriggerIfNotExists(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["users"] = &Table{Name: "users"}

	stmt := &parser.CreateTriggerStmt{
		Name:   "my_trigger",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerDelete,
	}

	// First creation should succeed
	_, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("First CreateTrigger() error = %v", err)
	}

	// Second creation without IF NOT EXISTS should fail
	_, err = s.CreateTrigger(stmt)
	if err == nil {
		t.Error("Expected error creating duplicate trigger")
	}

	// Second creation with IF NOT EXISTS should succeed
	stmt.IfNotExists = true
	trigger, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger with IF NOT EXISTS error = %v", err)
	}
	if trigger == nil {
		t.Error("Expected existing trigger, got nil")
	}
}

func TestCreateTriggerWithUpdateOf(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["users"] = &Table{Name: "users"}

	stmt := &parser.CreateTriggerStmt{
		Name:     "update_name_trigger",
		Table:    "users",
		Timing:   parser.TriggerAfter,
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{"name", "email"},
	}

	trigger, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger() error = %v", err)
	}

	if len(trigger.UpdateOf) != 2 {
		t.Fatalf("trigger has %d UpdateOf columns, want 2", len(trigger.UpdateOf))
	}
	if trigger.UpdateOf[0] != "name" || trigger.UpdateOf[1] != "email" {
		t.Error("UpdateOf columns not set correctly")
	}
}

func TestCreateTriggerWithWhen(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["users"] = &Table{Name: "users"}

	whenExpr := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.IdentExpr{Name: "age"},
		Right: &parser.LiteralExpr{Value: "18"},
	}

	stmt := &parser.CreateTriggerStmt{
		Name:   "adult_users_trigger",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		When:   whenExpr,
	}

	trigger, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger() error = %v", err)
	}

	if trigger.When == nil {
		t.Error("trigger.When should not be nil")
	}
}

func TestCreateTemporaryTrigger(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Tables["users"] = &Table{Name: "users"}

	stmt := &parser.CreateTriggerStmt{
		Name:   "temp_trigger",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerDelete,
		Temp:   true,
	}

	trigger, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger() error = %v", err)
	}

	if !trigger.Temp {
		t.Error("trigger should be temporary")
	}
}

func TestDropTrigger(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Triggers["my_trigger"] = &Trigger{Name: "my_trigger"}

	err := s.DropTrigger("my_trigger")
	if err != nil {
		t.Fatalf("DropTrigger() error = %v", err)
	}

	if _, ok := s.GetTrigger("my_trigger"); ok {
		t.Error("Trigger still exists after drop")
	}
}

func TestDropTriggerCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Triggers["MyTrigger"] = &Trigger{Name: "MyTrigger"}

	err := s.DropTrigger("mytrigger")
	if err != nil {
		t.Fatalf("DropTrigger() error = %v", err)
	}

	if _, ok := s.GetTrigger("MyTrigger"); ok {
		t.Error("Trigger still exists after drop")
	}
}

func TestDropTriggerNonexistent(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	err := s.DropTrigger("nonexistent")
	if err == nil {
		t.Error("Expected error dropping nonexistent trigger")
	}
}

func TestGetTrigger(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Triggers["TestTrigger"] = &Trigger{Name: "TestTrigger"}

	// Test case-insensitive lookup
	tests := []string{"TestTrigger", "testtrigger", "TESTTRIGGER", "tEsTtRiGgEr"}
	for _, name := range tests {
		name := name
		t.Run(name, func(t *testing.T) {
			trigger, ok := s.GetTrigger(name)
			if !ok {
				t.Errorf("GetTrigger(%q) not found", name)
			}
			if trigger.Name != "TestTrigger" {
				t.Errorf("GetTrigger(%q) returned wrong trigger: %q", name, trigger.Name)
			}
		})
	}

	// Test non-existent trigger
	_, ok := s.GetTrigger("nonexistent")
	if ok {
		t.Error("GetTrigger should return false for nonexistent trigger")
	}
}

func TestGetTableTriggers(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Triggers["trigger1"] = &Trigger{
		Name:   "trigger1",
		Table:  "users",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
	}
	s.Triggers["trigger2"] = &Trigger{
		Name:   "trigger2",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerUpdate,
	}
	s.Triggers["trigger3"] = &Trigger{
		Name:   "trigger3",
		Table:  "orders",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerDelete,
	}

	// Get all triggers for users table
	triggers := s.GetTableTriggers("users", nil, nil)
	if len(triggers) != 2 {
		t.Fatalf("GetTableTriggers() returned %d triggers, want 2", len(triggers))
	}

	// Filter by timing
	timing := parser.TriggerBefore
	triggers = s.GetTableTriggers("users", &timing, nil)
	if len(triggers) != 1 {
		t.Fatalf("GetTableTriggers() with timing filter returned %d triggers, want 1", len(triggers))
	}
	if triggers[0].Name != "trigger1" {
		t.Errorf("Expected trigger1, got %s", triggers[0].Name)
	}

	// Filter by event
	event := parser.TriggerUpdate
	triggers = s.GetTableTriggers("users", nil, &event)
	if len(triggers) != 1 {
		t.Fatalf("GetTableTriggers() with event filter returned %d triggers, want 1", len(triggers))
	}
	if triggers[0].Name != "trigger2" {
		t.Errorf("Expected trigger2, got %s", triggers[0].Name)
	}

	// Filter by both timing and event
	timing = parser.TriggerAfter
	event = parser.TriggerUpdate
	triggers = s.GetTableTriggers("users", &timing, &event)
	if len(triggers) != 1 {
		t.Fatalf("GetTableTriggers() with both filters returned %d triggers, want 1", len(triggers))
	}
}

func TestGetTableTriggersCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	s.Triggers["trigger1"] = &Trigger{
		Name:  "trigger1",
		Table: "Users",
	}

	triggers := s.GetTableTriggers("users", nil, nil)
	if len(triggers) != 1 {
		t.Errorf("GetTableTriggers() should be case-insensitive, got %d triggers", len(triggers))
	}
}

func TestListTriggers(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Empty schema
	triggers := s.ListTriggers()
	if len(triggers) != 0 {
		t.Errorf("ListTriggers() returned %d triggers, want 0", len(triggers))
	}

	// Add triggers
	s.Triggers["trigger1"] = &Trigger{Name: "trigger1"}
	s.Triggers["trigger2"] = &Trigger{Name: "trigger2"}
	s.Triggers["trigger3"] = &Trigger{Name: "trigger3"}

	triggers = s.ListTriggers()
	if len(triggers) != 3 {
		t.Fatalf("ListTriggers() returned %d triggers, want 3", len(triggers))
	}
}

func TestShouldExecuteTriggerNoWhen(t *testing.T) {
	t.Parallel()
	trigger := &Trigger{
		Name: "my_trigger",
		When: nil,
	}

	should, err := trigger.ShouldExecuteTrigger(nil, nil)
	if err != nil {
		t.Fatalf("ShouldExecuteTrigger() error = %v", err)
	}
	if !should {
		t.Error("Trigger without WHEN clause should always execute")
	}
}

func TestShouldExecuteTriggerWithWhen(t *testing.T) {
	t.Parallel()
	// WHEN NEW.age > 18
	whenExpr := &parser.BinaryExpr{
		Op: parser.OpGt,
		Left: &parser.IdentExpr{
			Table: "NEW",
			Name:  "age",
		},
		Right: &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: "18",
		},
	}

	trigger := &Trigger{
		Name: "my_trigger",
		When: whenExpr,
	}

	// Test with NEW.age = 25 (should execute)
	newRow := map[string]interface{}{"age": int64(25)}
	should, err := trigger.ShouldExecuteTrigger(nil, newRow)
	if err != nil {
		t.Fatalf("ShouldExecuteTrigger() error = %v", err)
	}
	if !should {
		t.Error("Trigger should execute when NEW.age > 18")
	}

	// Test with NEW.age = 15 (should not execute)
	newRow = map[string]interface{}{"age": int64(15)}
	should, err = trigger.ShouldExecuteTrigger(nil, newRow)
	if err != nil {
		t.Fatalf("ShouldExecuteTrigger() error = %v", err)
	}
	if should {
		t.Error("Trigger should not execute when NEW.age <= 18")
	}
}

func TestShouldExecuteTriggerLogicalOperators(t *testing.T) {
	t.Parallel()
	// WHEN NEW.age > 18 AND NEW.active = 1
	whenExpr := &parser.BinaryExpr{
		Op: parser.OpAnd,
		Left: &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Table: "NEW", Name: "age"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
		},
		Right: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Table: "NEW", Name: "active"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	trigger := &Trigger{When: whenExpr}

	// Both conditions true
	newRow := map[string]interface{}{"age": int64(25), "active": int64(1)}
	should, _ := trigger.ShouldExecuteTrigger(nil, newRow)
	if !should {
		t.Error("Should execute when both conditions are true")
	}

	// First condition false
	newRow = map[string]interface{}{"age": int64(15), "active": int64(1)}
	should, _ = trigger.ShouldExecuteTrigger(nil, newRow)
	if should {
		t.Error("Should not execute when first condition is false")
	}

	// Second condition false
	newRow = map[string]interface{}{"age": int64(25), "active": int64(0)}
	should, _ = trigger.ShouldExecuteTrigger(nil, newRow)
	if should {
		t.Error("Should not execute when second condition is false")
	}
}

func TestShouldExecuteTriggerOrOperator(t *testing.T) {
	t.Parallel()
	// WHEN NEW.age < 18 OR NEW.age > 65
	whenExpr := &parser.BinaryExpr{
		Op: parser.OpOr,
		Left: &parser.BinaryExpr{
			Op:    parser.OpLt,
			Left:  &parser.IdentExpr{Table: "NEW", Name: "age"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
		},
		Right: &parser.BinaryExpr{
			Op:    parser.OpGt,
			Left:  &parser.IdentExpr{Table: "NEW", Name: "age"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "65"},
		},
	}

	trigger := &Trigger{When: whenExpr}

	// First condition true
	newRow := map[string]interface{}{"age": int64(15)}
	should, _ := trigger.ShouldExecuteTrigger(nil, newRow)
	if !should {
		t.Error("Should execute when first OR condition is true")
	}

	// Second condition true
	newRow = map[string]interface{}{"age": int64(70)}
	should, _ = trigger.ShouldExecuteTrigger(nil, newRow)
	if !should {
		t.Error("Should execute when second OR condition is true")
	}

	// Neither condition true
	newRow = map[string]interface{}{"age": int64(30)}
	should, _ = trigger.ShouldExecuteTrigger(nil, newRow)
	if should {
		t.Error("Should not execute when neither OR condition is true")
	}
}

func TestShouldExecuteTriggerWithOLD(t *testing.T) {
	t.Parallel()
	// WHEN OLD.status != NEW.status
	whenExpr := &parser.BinaryExpr{
		Op:    parser.OpNe,
		Left:  &parser.IdentExpr{Table: "OLD", Name: "status"},
		Right: &parser.IdentExpr{Table: "NEW", Name: "status"},
	}

	trigger := &Trigger{When: whenExpr}

	// Different status
	oldRow := map[string]interface{}{"status": "active"}
	newRow := map[string]interface{}{"status": "inactive"}
	should, _ := trigger.ShouldExecuteTrigger(oldRow, newRow)
	if !should {
		t.Error("Should execute when status changed")
	}

	// Same status
	oldRow = map[string]interface{}{"status": "active"}
	newRow = map[string]interface{}{"status": "active"}
	should, _ = trigger.ShouldExecuteTrigger(oldRow, newRow)
	if should {
		t.Error("Should not execute when status unchanged")
	}
}

func TestShouldExecuteTriggerNullComparison(t *testing.T) {
	t.Parallel()
	// WHEN NEW.age > 18
	whenExpr := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.IdentExpr{Table: "NEW", Name: "age"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
	}

	trigger := &Trigger{When: whenExpr}

	// NULL value
	newRow := map[string]interface{}{"age": nil}
	should, _ := trigger.ShouldExecuteTrigger(nil, newRow)
	if should {
		t.Error("NULL comparisons should return false")
	}
}

func TestShouldExecuteTriggerStringComparison(t *testing.T) {
	t.Parallel()
	// WHEN NEW.name = 'John'
	whenExpr := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Table: "NEW", Name: "name"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "John"},
	}

	trigger := &Trigger{When: whenExpr}

	newRow := map[string]interface{}{"name": "John"}
	should, _ := trigger.ShouldExecuteTrigger(nil, newRow)
	if !should {
		t.Error("Should execute when name = 'John'")
	}

	newRow = map[string]interface{}{"name": "Jane"}
	should, _ = trigger.ShouldExecuteTrigger(nil, newRow)
	if should {
		t.Error("Should not execute when name != 'John'")
	}
}

func TestMatchesUpdateColumns(t *testing.T) {
	t.Parallel()
	// INSERT trigger - should always match
	trigger := &Trigger{
		Event: parser.TriggerInsert,
	}
	if !trigger.MatchesUpdateColumns([]string{"any", "columns"}) {
		t.Error("INSERT trigger should match any columns")
	}

	// UPDATE trigger with no UPDATE OF clause - should match all updates
	trigger = &Trigger{
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{},
	}
	if !trigger.MatchesUpdateColumns([]string{"name", "email"}) {
		t.Error("UPDATE trigger without UPDATE OF should match all updates")
	}

	// UPDATE OF trigger - should match when specified column is updated
	trigger = &Trigger{
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{"name", "email"},
	}
	if !trigger.MatchesUpdateColumns([]string{"name"}) {
		t.Error("UPDATE OF trigger should match when name is updated")
	}
	if !trigger.MatchesUpdateColumns([]string{"email"}) {
		t.Error("UPDATE OF trigger should match when email is updated")
	}
	if !trigger.MatchesUpdateColumns([]string{"name", "age"}) {
		t.Error("UPDATE OF trigger should match when name is updated (even with other columns)")
	}

	// Should not match when different columns are updated
	if trigger.MatchesUpdateColumns([]string{"age", "status"}) {
		t.Error("UPDATE OF trigger should not match when different columns are updated")
	}
}

func TestMatchesUpdateColumnsCaseInsensitive(t *testing.T) {
	t.Parallel()
	trigger := &Trigger{
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{"Name", "Email"},
	}

	if !trigger.MatchesUpdateColumns([]string{"name"}) {
		t.Error("UPDATE OF matching should be case-insensitive")
	}
	if !trigger.MatchesUpdateColumns([]string{"EMAIL"}) {
		t.Error("UPDATE OF matching should be case-insensitive")
	}
}

func TestEvaluateLiteralAsBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		literal  *parser.LiteralExpr
		expected bool
	}{
		{"integer zero", &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}, false},
		{"integer non-zero", &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}, true},
		{"NULL", &parser.LiteralExpr{Type: parser.LiteralNull}, false},
		{"string", &parser.LiteralExpr{Type: parser.LiteralString, Value: "test"}, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateLiteralAsBool(tt.literal)
			if err != nil {
				t.Fatalf("evaluateLiteralAsBool() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("evaluateLiteralAsBool() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCompareValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		op       parser.BinaryOp
		expected bool
	}{
		{"int64 equals", int64(10), int64(10), parser.OpEq, true},
		{"int64 not equals", int64(10), int64(20), parser.OpNe, true},
		{"int64 less than", int64(10), int64(20), parser.OpLt, true},
		{"int64 less or equal", int64(10), int64(10), parser.OpLe, true},
		{"int64 greater than", int64(20), int64(10), parser.OpGt, true},
		{"int64 greater or equal", int64(20), int64(20), parser.OpGe, true},
		{"string equals", "abc", "abc", parser.OpEq, true},
		{"string not equals", "abc", "def", parser.OpNe, true},
		{"float64 equals", float64(1.5), float64(1.5), parser.OpEq, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result, err := compareValues(tt.left, tt.right, tt.op)
			if err != nil {
				t.Fatalf("compareValues() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("compareValues() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"nil", nil, false},
		{"bool true", true, true},
		{"bool false", false, false},
		{"int64 zero", int64(0), false},
		{"int64 non-zero", int64(5), true},
		{"float64 zero", float64(0.0), false},
		{"float64 non-zero", float64(1.5), true},
		{"empty string", "", false},
		{"non-empty string", "test", true},
		{"other type", struct{}{}, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			result := toBool(tt.value)
			if result != tt.expected {
				t.Errorf("toBool(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestEvaluateIdentExprDirect(t *testing.T) {
	t.Parallel()
	expr := &parser.IdentExpr{Name: "status"}
	newRow := map[string]interface{}{"status": int64(1)}

	result, err := evaluateIdentExpr(expr, nil, newRow)
	if err != nil {
		t.Fatalf("evaluateIdentExpr() error = %v", err)
	}
	if !result {
		t.Error("Expected true for status = 1")
	}

	newRow = map[string]interface{}{"status": int64(0)}
	result, err = evaluateIdentExpr(expr, nil, newRow)
	if err != nil {
		t.Fatalf("evaluateIdentExpr() error = %v", err)
	}
	if result {
		t.Error("Expected false for status = 0")
	}
}

func TestResolveIdentValueUnqualified(t *testing.T) {
	t.Parallel()
	expr := &parser.IdentExpr{Name: "age"}

	// Try NEW first
	newRow := map[string]interface{}{"age": int64(25)}
	oldRow := map[string]interface{}{"age": int64(20)}
	val, err := resolveIdentValue(expr, oldRow, newRow)
	if err != nil {
		t.Fatalf("resolveIdentValue() error = %v", err)
	}
	if val != int64(25) {
		t.Errorf("resolveIdentValue() = %v, want 25 (should prefer NEW)", val)
	}

	// Only OLD available
	val, err = resolveIdentValue(expr, oldRow, nil)
	if err != nil {
		t.Fatalf("resolveIdentValue() error = %v", err)
	}
	if val != int64(20) {
		t.Errorf("resolveIdentValue() = %v, want 20", val)
	}

	// Column not found
	expr = &parser.IdentExpr{Name: "nonexistent"}
	_, err = resolveIdentValue(expr, oldRow, newRow)
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

func TestResolveQualifiedValueErrors(t *testing.T) {
	t.Parallel()
	// NEW not available
	expr := &parser.IdentExpr{Table: "NEW", Name: "age"}
	_, err := resolveQualifiedValue(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when NEW is not available")
	}

	// OLD not available
	expr = &parser.IdentExpr{Table: "OLD", Name: "age"}
	_, err = resolveQualifiedValue(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when OLD is not available")
	}

	// Column not found in NEW
	newRow := map[string]interface{}{"name": "test"}
	expr = &parser.IdentExpr{Table: "NEW", Name: "nonexistent"}
	_, err = resolveQualifiedValue(expr, nil, newRow)
	if err == nil {
		t.Error("Expected error for nonexistent column in NEW")
	}

	// Column not found in OLD
	oldRow := map[string]interface{}{"name": "test"}
	expr = &parser.IdentExpr{Table: "OLD", Name: "nonexistent"}
	_, err = resolveQualifiedValue(expr, oldRow, nil)
	if err == nil {
		t.Error("Expected error for nonexistent column in OLD")
	}

	// Invalid qualifier
	expr = &parser.IdentExpr{Table: "INVALID", Name: "age"}
	newRow = map[string]interface{}{"age": int64(25)}
	_, err = resolveQualifiedValue(expr, nil, newRow)
	if err == nil {
		t.Error("Expected error for invalid qualifier")
	}
}

func TestParseLiteralValueFloat(t *testing.T) {
	t.Parallel()
	expr := &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"}
	val := parseLiteralValue(expr)
	if val == nil {
		t.Error("Expected non-nil value for float literal")
	}
}

func TestCompareLessThanFloat(t *testing.T) {
	t.Parallel()
	result := compareLessThan(float64(1.5), float64(2.5))
	if !result {
		t.Error("1.5 < 2.5 should be true")
	}

	result = compareLessThan(float64(2.5), float64(1.5))
	if result {
		t.Error("2.5 < 1.5 should be false")
	}
}

func TestCompareLessThanString(t *testing.T) {
	t.Parallel()
	result := compareLessThan("abc", "def")
	if !result {
		t.Error("'abc' < 'def' should be true")
	}

	result = compareLessThan("def", "abc")
	if result {
		t.Error("'def' < 'abc' should be false")
	}
}

func TestCompareLessThanIncompatibleTypes(t *testing.T) {
	t.Parallel()
	result := compareLessThan(int64(10), "string")
	if result {
		t.Error("Incompatible types should return false")
	}

	result = compareLessThan(true, int64(10))
	if result {
		t.Error("Incompatible types should return false")
	}
}

func TestCompareEqualBool(t *testing.T) {
	t.Parallel()
	result := compareEqual(true, true)
	if !result {
		t.Error("true == true should be true")
	}

	result = compareEqual(true, false)
	if result {
		t.Error("true == false should be false")
	}
}

func TestCompareEqualFloat(t *testing.T) {
	t.Parallel()
	result := compareEqual(float64(3.14), float64(3.14))
	if !result {
		t.Error("3.14 == 3.14 should be true")
	}

	result = compareEqual(float64(3.14), float64(2.71))
	if result {
		t.Error("3.14 == 2.71 should be false")
	}
}

func TestCompareEqualIncompatibleTypes(t *testing.T) {
	t.Parallel()
	result := compareEqual(int64(10), float64(10.0))
	if result {
		t.Error("int64 and float64 should not be equal (incompatible types)")
	}

	result = compareEqual(int64(10), "10")
	if result {
		t.Error("int64 and string should not be equal")
	}
}

func TestEvaluateWhenClauseLiteral(t *testing.T) {
	t.Parallel()
	// Literal integer
	literalExpr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	result, err := evaluateWhenClause(literalExpr, nil, nil)
	if err != nil {
		t.Fatalf("evaluateWhenClause() error = %v", err)
	}
	if !result {
		t.Error("Non-zero literal should evaluate to true")
	}

	// Literal zero
	literalExpr = &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}
	result, err = evaluateWhenClause(literalExpr, nil, nil)
	if err != nil {
		t.Fatalf("evaluateWhenClause() error = %v", err)
	}
	if result {
		t.Error("Zero literal should evaluate to false")
	}
}

func TestEvaluateWhenClauseUnsupportedOp(t *testing.T) {
	t.Parallel()
	// Binary expression with unsupported operator
	expr := &parser.BinaryExpr{
		Op:    parser.OpMul, // Arithmetic operator, not logical/comparison
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
	}

	_, err := evaluateBinaryExpr(expr, nil, nil)
	if err == nil {
		t.Error("Expected error for unsupported binary operator")
	}
}

func TestEvaluateExprValueUnsupportedType(t *testing.T) {
	t.Parallel()
	// Case expression (not supported in WHEN clause evaluation)
	expr := &parser.CaseExpr{}
	_, err := evaluateExprValue(expr, nil, nil)
	if err == nil {
		t.Error("Expected error for unsupported expression type")
	}
}

func TestCompareNonNullValuesUnsupportedOp(t *testing.T) {
	t.Parallel()
	_, err := compareNonNullValues(int64(1), int64(2), parser.OpMul)
	if err == nil {
		t.Error("Expected error for unsupported comparison operator")
	}
}

func TestCreateTriggerInitializesMap(t *testing.T) {
	t.Parallel()
	s := NewSchema()
	s.Triggers = nil // Set to nil to test initialization

	s.Tables["users"] = &Table{Name: "users"}

	stmt := &parser.CreateTriggerStmt{
		Name:   "my_trigger",
		Table:  "users",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerInsert,
	}

	_, err := s.CreateTrigger(stmt)
	if err != nil {
		t.Fatalf("CreateTrigger() error = %v", err)
	}

	if s.Triggers == nil {
		t.Error("Triggers map should be initialized")
	}
}

func TestShouldExecuteTriggerError(t *testing.T) {
	t.Parallel()
	// WHEN with identifier that can't be resolved
	trigger := &Trigger{
		When: &parser.IdentExpr{
			Table: "INVALID",
			Name:  "col",
		},
	}

	_, err := trigger.ShouldExecuteTrigger(nil, nil)
	if err == nil {
		t.Error("Expected error for invalid identifier in WHEN clause")
	}
}

func TestEvaluateWhenClauseUnsupportedExpr(t *testing.T) {
	t.Parallel()
	// Unsupported expression type
	expr := &parser.UnaryExpr{Op: parser.OpNot}
	result, err := evaluateWhenClause(expr, nil, nil)
	// Should default to true for unsupported types
	if err != nil {
		t.Fatalf("evaluateWhenClause() error = %v", err)
	}
	if !result {
		t.Error("Unsupported expression should default to true")
	}
}

func TestEvaluateLogicalOpErrors(t *testing.T) {
	t.Parallel()
	// Error in left operand
	expr := &parser.BinaryExpr{
		Op: parser.OpAnd,
		Left: &parser.IdentExpr{
			Table: "INVALID",
			Name:  "col",
		},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}

	_, err := evaluateLogicalOp(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when left operand fails")
	}

	// Error in right operand
	expr = &parser.BinaryExpr{
		Op:   parser.OpAnd,
		Left: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.IdentExpr{
			Table: "INVALID",
			Name:  "col",
		},
	}

	_, err = evaluateLogicalOp(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when right operand fails")
	}
}

func TestEvaluateComparisonOpErrors(t *testing.T) {
	t.Parallel()
	// Error in left operand
	expr := &parser.BinaryExpr{
		Op: parser.OpEq,
		Left: &parser.IdentExpr{
			Table: "INVALID",
			Name:  "col",
		},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}

	_, err := evaluateComparisonOp(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when left value evaluation fails")
	}

	// Error in right operand
	expr = &parser.BinaryExpr{
		Op:   parser.OpEq,
		Left: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.IdentExpr{
			Table: "INVALID",
			Name:  "col",
		},
	}

	_, err = evaluateComparisonOp(expr, nil, nil)
	if err == nil {
		t.Error("Expected error when right value evaluation fails")
	}
}

func TestEvaluateIdentExprValueQualified(t *testing.T) {
	t.Parallel()
	expr := &parser.IdentExpr{
		Table: "NEW",
		Name:  "age",
	}
	newRow := map[string]interface{}{"age": int64(25)}

	val, err := evaluateIdentExprValue(expr, nil, newRow)
	if err != nil {
		t.Fatalf("evaluateIdentExprValue() error = %v", err)
	}
	if val != int64(25) {
		t.Errorf("evaluateIdentExprValue() = %v, want 25", val)
	}
}

func TestParseLiteralValueNull(t *testing.T) {
	t.Parallel()
	expr := &parser.LiteralExpr{Type: parser.LiteralNull}
	val := parseLiteralValue(expr)
	if val != nil {
		t.Error("NULL literal should return nil")
	}
}

func TestParseLiteralValueDefault(t *testing.T) {
	t.Parallel()
	expr := &parser.LiteralExpr{Type: parser.LiteralBlob, Value: "x'00FF'"}
	val := parseLiteralValue(expr)
	if val == nil {
		t.Error("Expected non-nil value for blob literal")
	}
}

func TestEvaluateIdentExprError(t *testing.T) {
	t.Parallel()
	expr := &parser.IdentExpr{Name: "nonexistent"}
	_, err := evaluateIdentExpr(expr, nil, nil)
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}
