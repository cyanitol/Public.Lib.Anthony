// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package schema provides trigger management for the Anthony SQLite clone.
package schema

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// Trigger represents a database trigger definition.
type Trigger struct {
	Name       string               // Trigger name
	Table      string               // Table name this trigger is attached to
	Timing     parser.TriggerTiming // BEFORE, AFTER, or INSTEAD OF
	Event      parser.TriggerEvent  // INSERT, UPDATE, or DELETE
	UpdateOf   []string             // Column names for UPDATE OF (nil if not UPDATE OF)
	ForEachRow bool                 // True if FOR EACH ROW specified
	When       parser.Expression    // Optional WHEN condition
	Body       []parser.Statement   // Trigger body statements
	SQL        string               // Original CREATE TRIGGER statement
	Temp       bool                 // True for temporary triggers
}

// CreateTrigger creates a trigger from a CREATE TRIGGER statement.
func (s *Schema) CreateTrigger(stmt *parser.CreateTriggerStmt) (*Trigger, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if trigger already exists
	lowerName := strings.ToLower(stmt.Name)
	for triggerName := range s.Triggers {
		if strings.ToLower(triggerName) == lowerName {
			if stmt.IfNotExists {
				return s.Triggers[triggerName], nil
			}
			return nil, fmt.Errorf("trigger already exists: %s", stmt.Name)
		}
	}

	// Check if table exists (tables or views)
	if !s.tableExistsLocked(stmt.Table) && !s.viewExistsLocked(stmt.Table) {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// INSTEAD OF triggers are only valid on views, not tables
	if stmt.Timing == parser.TriggerInsteadOf && !s.viewExistsLocked(stmt.Table) {
		return nil, fmt.Errorf("cannot create INSTEAD OF trigger on table: %s", stmt.Table)
	}

	// Create the trigger
	trigger := &Trigger{
		Name:       stmt.Name,
		Table:      stmt.Table,
		Timing:     stmt.Timing,
		Event:      stmt.Event,
		UpdateOf:   stmt.UpdateOf,
		ForEachRow: stmt.ForEachRow,
		When:       stmt.When,
		Body:       stmt.Body,
		SQL:        stmt.String(),
		Temp:       stmt.Temp,
	}

	// Initialize Triggers map if not already done
	if s.Triggers == nil {
		s.Triggers = make(map[string]*Trigger)
	}

	s.Triggers[stmt.Name] = trigger
	return trigger, nil
}

// DropTrigger removes a trigger from the schema.
func (s *Schema) DropTrigger(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lowerName := strings.ToLower(name)

	// Find the actual trigger name (case-insensitive)
	for triggerName := range s.Triggers {
		if strings.ToLower(triggerName) == lowerName {
			delete(s.Triggers, triggerName)
			return nil
		}
	}

	return fmt.Errorf("trigger not found: %s", name)
}

// GetTrigger retrieves a trigger by name.
// Returns the trigger and true if found, nil and false otherwise.
func (s *Schema) GetTrigger(name string) (*Trigger, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lowerName := strings.ToLower(name)
	for triggerName, trigger := range s.Triggers {
		if strings.ToLower(triggerName) == lowerName {
			return trigger, true
		}
	}
	return nil, false
}

// GetTableTriggers returns all triggers for a given table and event.
// If event is nil, returns all triggers for the table.
// timing and event can be used to filter triggers.
func (s *Schema) GetTableTriggers(tableName string, timing *parser.TriggerTiming, event *parser.TriggerEvent) []*Trigger {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var triggers []*Trigger
	lowerTableName := strings.ToLower(tableName)

	for _, trigger := range s.Triggers {
		if strings.ToLower(trigger.Table) != lowerTableName {
			continue
		}

		// Filter by timing if specified
		if timing != nil && trigger.Timing != *timing {
			continue
		}

		// Filter by event if specified
		if event != nil && trigger.Event != *event {
			continue
		}

		triggers = append(triggers, trigger)
	}

	return triggers
}

// ListTriggers returns a sorted list of all trigger names.
func (s *Schema) ListTriggers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.Triggers))
	for name := range s.Triggers {
		names = append(names, name)
	}
	return names
}

// ShouldExecuteTrigger determines if a trigger should execute based on its WHEN clause.
// oldRow and newRow represent the OLD and NEW pseudo-records available in trigger context.
// Returns true if the trigger should execute, false otherwise.
func (t *Trigger) ShouldExecuteTrigger(oldRow, newRow map[string]interface{}) (bool, error) {
	// If no WHEN clause, always execute
	if t.When == nil {
		return true, nil
	}

	// Evaluate WHEN expression with OLD and NEW context
	result, err := evaluateWhenClause(t.When, oldRow, newRow)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate WHEN clause: %w", err)
	}

	return result, nil
}

// evaluateWhenClause evaluates a WHEN clause expression with OLD and NEW row context.
// This is a simplified evaluator that handles basic comparison expressions.
func evaluateWhenClause(expr parser.Expression, oldRow, newRow map[string]interface{}) (bool, error) {
	if expr == nil {
		return true, nil
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return evaluateBinaryExpr(e, oldRow, newRow)
	case *parser.LiteralExpr:
		return evaluateLiteralAsBool(e)
	case *parser.IdentExpr:
		// Column reference - need to resolve from OLD or NEW
		return evaluateIdentExpr(e, oldRow, newRow)
	default:
		// For unsupported expression types, default to true to avoid breaking triggers
		// A full implementation would support all expression types
		return true, nil
	}
}

// evaluateBinaryExpr evaluates a binary expression (AND, OR, comparisons, etc.)
func evaluateBinaryExpr(expr *parser.BinaryExpr, oldRow, newRow map[string]interface{}) (bool, error) {
	// Handle logical operators (AND, OR) specially since they need boolean evaluation
	if expr.Op == parser.OpAnd || expr.Op == parser.OpOr {
		return evaluateLogicalOp(expr, oldRow, newRow)
	}

	if isComparisonOp(expr.Op) {
		return evaluateComparisonOp(expr, oldRow, newRow)
	}

	return false, fmt.Errorf("unsupported binary operator in WHEN clause: %v", expr.Op)
}

// evaluateLogicalOp evaluates AND/OR logical operations.
func evaluateLogicalOp(expr *parser.BinaryExpr, oldRow, newRow map[string]interface{}) (bool, error) {
	left, err := evaluateWhenClause(expr.Left, oldRow, newRow)
	if err != nil {
		return false, err
	}
	right, err := evaluateWhenClause(expr.Right, oldRow, newRow)
	if err != nil {
		return false, err
	}

	if expr.Op == parser.OpAnd {
		return left && right, nil
	}
	return left || right, nil
}

// isComparisonOp checks if an operator is a comparison operator.
func isComparisonOp(op parser.BinaryOp) bool {
	return op == parser.OpEq || op == parser.OpNe || op == parser.OpLt ||
		op == parser.OpLe || op == parser.OpGt || op == parser.OpGe
}

// evaluateComparisonOp evaluates comparison operations.
func evaluateComparisonOp(expr *parser.BinaryExpr, oldRow, newRow map[string]interface{}) (bool, error) {
	leftVal, err := evaluateExprValue(expr.Left, oldRow, newRow)
	if err != nil {
		return false, err
	}
	rightVal, err := evaluateExprValue(expr.Right, oldRow, newRow)
	if err != nil {
		return false, err
	}
	return compareValues(leftVal, rightVal, expr.Op)
}

// evaluateExprValue extracts the actual value from an expression
func evaluateExprValue(expr parser.Expression, oldRow, newRow map[string]interface{}) (interface{}, error) {
	if litExpr, ok := expr.(*parser.LiteralExpr); ok {
		return parseLiteralValue(litExpr), nil
	}

	if identExpr, ok := expr.(*parser.IdentExpr); ok {
		return evaluateIdentExprValue(identExpr, oldRow, newRow)
	}

	return nil, fmt.Errorf("unsupported expression type in WHEN clause: %T", expr)
}

// evaluateIdentExprValue evaluates an identifier expression to get its value.
func evaluateIdentExprValue(e *parser.IdentExpr, oldRow, newRow map[string]interface{}) (interface{}, error) {
	if e.Table != "" {
		return resolveQualifiedValue(e, oldRow, newRow)
	}
	return resolveIdentValue(e, oldRow, newRow)
}

// resolveIdentValue resolves an identifier (column name) to its value
func resolveIdentValue(expr *parser.IdentExpr, oldRow, newRow map[string]interface{}) (interface{}, error) {
	name := strings.ToLower(expr.Name)

	// Try NEW first, then OLD
	if newRow != nil {
		if val, ok := newRow[name]; ok {
			return val, nil
		}
	}
	if oldRow != nil {
		if val, ok := oldRow[name]; ok {
			return val, nil
		}
	}

	return nil, fmt.Errorf("column not found: %s", expr.Name)
}

// resolveQualifiedValue resolves a qualified identifier (NEW.col or OLD.col)
func resolveQualifiedValue(expr *parser.IdentExpr, oldRow, newRow map[string]interface{}) (interface{}, error) {
	qualifier := strings.ToLower(expr.Table)
	colName := strings.ToLower(expr.Name)

	switch qualifier {
	case "new":
		if newRow == nil {
			return nil, fmt.Errorf("NEW is not available in this trigger context")
		}
		if val, ok := newRow[colName]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("column not found in NEW: %s", expr.Name)
	case "old":
		if oldRow == nil {
			return nil, fmt.Errorf("OLD is not available in this trigger context")
		}
		if val, ok := oldRow[colName]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("column not found in OLD: %s", expr.Name)
	default:
		return nil, fmt.Errorf("invalid qualifier in trigger WHEN clause: %s (expected OLD or NEW)", expr.Table)
	}
}

// parseLiteralValue parses a literal expression to its Go value
func parseLiteralValue(expr *parser.LiteralExpr) interface{} {
	switch expr.Type {
	case parser.LiteralInteger:
		// Simple integer parsing
		var val int64
		fmt.Sscanf(expr.Value, "%d", &val)
		return val
	case parser.LiteralFloat:
		var val float64
		fmt.Sscanf(expr.Value, "%f", &val)
		return val
	case parser.LiteralString:
		return expr.Value
	case parser.LiteralNull:
		return nil
	default:
		return expr.Value
	}
}

// compareValues compares two values using the given operator
func compareValues(left, right interface{}, op parser.BinaryOp) (bool, error) {
	// Handle NULL comparisons
	if left == nil || right == nil {
		// NULL comparisons with standard operators return false (except IS/IS NOT which we don't handle here)
		return false, nil
	}

	return compareNonNullValues(left, right, op)
}

// compareNonNullValues compares two non-NULL values using the given operator
func compareNonNullValues(left, right interface{}, op parser.BinaryOp) (bool, error) {
	switch op {
	case parser.OpEq:
		return compareEqual(left, right), nil
	case parser.OpNe:
		return !compareEqual(left, right), nil
	case parser.OpLt:
		return compareLessThan(left, right), nil
	case parser.OpLe:
		return compareLessThanOrEqual(left, right), nil
	case parser.OpGt:
		return compareGreaterThan(left, right), nil
	case parser.OpGe:
		return compareGreaterThanOrEqual(left, right), nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %v", op)
	}
}

// compareLessThanOrEqual checks if left <= right
func compareLessThanOrEqual(left, right interface{}) bool {
	return compareLessThan(left, right) || compareEqual(left, right)
}

// compareGreaterThan checks if left > right
func compareGreaterThan(left, right interface{}) bool {
	return !compareLessThan(left, right) && !compareEqual(left, right)
}

// compareGreaterThanOrEqual checks if left >= right
func compareGreaterThanOrEqual(left, right interface{}) bool {
	return !compareLessThan(left, right)
}

// compareEqual compares two values for equality
func compareEqual(left, right interface{}) bool {
	if compareInt64Equal(left, right) {
		return true
	}
	if compareFloat64Equal(left, right) {
		return true
	}
	if compareStringEqual(left, right) {
		return true
	}
	if compareBoolEqual(left, right) {
		return true
	}
	return false
}

// compareInt64Equal checks if both values are int64 and equal
func compareInt64Equal(left, right interface{}) bool {
	l, okL := left.(int64)
	r, okR := right.(int64)
	return okL && okR && l == r
}

// compareFloat64Equal checks if both values are float64 and equal
func compareFloat64Equal(left, right interface{}) bool {
	l, okL := left.(float64)
	r, okR := right.(float64)
	return okL && okR && l == r
}

// compareStringEqual checks if both values are string and equal
func compareStringEqual(left, right interface{}) bool {
	l, okL := left.(string)
	r, okR := right.(string)
	return okL && okR && l == r
}

// compareBoolEqual checks if both values are bool and equal
func compareBoolEqual(left, right interface{}) bool {
	l, okL := left.(bool)
	r, okR := right.(bool)
	return okL && okR && l == r
}

// compareLessThan compares if left < right
func compareLessThan(left, right interface{}) bool {
	switch l := left.(type) {
	case int64:
		r, ok := right.(int64)
		return ok && l < r
	case float64:
		r, ok := right.(float64)
		return ok && l < r
	case string:
		r, ok := right.(string)
		return ok && l < r
	default:
		return false
	}
}

// evaluateLiteralAsBool converts a literal to a boolean
func evaluateLiteralAsBool(expr *parser.LiteralExpr) (bool, error) {
	switch expr.Type {
	case parser.LiteralInteger:
		return expr.Value != "0", nil
	case parser.LiteralNull:
		return false, nil
	default:
		return true, nil
	}
}

// evaluateIdentExpr evaluates an identifier expression as a boolean
func evaluateIdentExpr(expr *parser.IdentExpr, oldRow, newRow map[string]interface{}) (bool, error) {
	val, err := resolveIdentValue(expr, oldRow, newRow)
	if err != nil {
		return false, err
	}
	return toBool(val), nil
}

// toBool converts a value to boolean
func toBool(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	default:
		return true
	}
}

// MatchesUpdateColumns checks if this trigger should fire for an UPDATE
// that modifies the given columns.
// Returns true if:
// - This is not an UPDATE trigger, OR
// - This is an UPDATE trigger with no UPDATE OF clause, OR
// - This is an UPDATE OF trigger and at least one of the specified columns is being updated
func (t *Trigger) MatchesUpdateColumns(updatedColumns []string) bool {
	// Not an UPDATE trigger - always matches
	if t.Event != parser.TriggerUpdate {
		return true
	}

	// UPDATE trigger with no UPDATE OF clause - matches all updates
	if len(t.UpdateOf) == 0 {
		return true
	}

	// UPDATE OF trigger - check if any specified column is being updated
	lowerUpdatedCols := make(map[string]bool)
	for _, col := range updatedColumns {
		lowerUpdatedCols[strings.ToLower(col)] = true
	}

	for _, col := range t.UpdateOf {
		if lowerUpdatedCols[strings.ToLower(col)] {
			return true
		}
	}

	return false
}
