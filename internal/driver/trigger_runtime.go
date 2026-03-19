// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TriggerRuntime implements vdbe.TriggerCompilerInterface.
// It compiles and executes trigger body statements at VDBE runtime
// when actual row data is available.
type TriggerRuntime struct {
	conn *Conn
	// recursionDepth tracks nesting to prevent infinite trigger recursion.
	recursionDepth int
}

// maxTriggerDepth limits trigger recursion to prevent stack overflow.
const maxTriggerDepth = 32

// NewTriggerRuntime creates a new trigger runtime for the given connection.
func NewTriggerRuntime(conn *Conn) *TriggerRuntime {
	return &TriggerRuntime{conn: conn}
}

// ExecuteTriggers implements vdbe.TriggerCompilerInterface.
// It finds, compiles, and executes matching triggers.
func (tr *TriggerRuntime) ExecuteTriggers(
	tableName string, event int, timing int,
	triggerRow *vdbe.TriggerRowData, updatedCols []string,
) error {
	if tr.recursionDepth >= maxTriggerDepth {
		return fmt.Errorf("trigger recursion depth exceeded (max %d)", maxTriggerDepth)
	}

	triggers := tr.findMatchingTriggers(tableName, event, timing)
	if len(triggers) == 0 {
		return nil
	}

	tr.recursionDepth++
	defer func() { tr.recursionDepth-- }()

	for _, trigger := range triggers {
		if err := tr.executeSingleTrigger(trigger, triggerRow, updatedCols); err != nil {
			return err
		}
	}
	return nil
}

// findMatchingTriggers returns triggers matching the given table, event, and timing.
func (tr *TriggerRuntime) findMatchingTriggers(
	tableName string, event int, timing int,
) []*schema.Trigger {
	parserTiming := convertTiming(timing)
	parserEvent := convertEvent(event)
	return tr.conn.schema.GetTableTriggers(tableName, &parserTiming, &parserEvent)
}

// executeSingleTrigger checks the WHEN clause and executes a single trigger.
func (tr *TriggerRuntime) executeSingleTrigger(
	trigger *schema.Trigger,
	triggerRow *vdbe.TriggerRowData,
	updatedCols []string,
) error {
	if !trigger.MatchesUpdateColumns(updatedCols) {
		return nil
	}

	oldRow, newRow := extractRowMaps(triggerRow)

	shouldExec, err := trigger.ShouldExecuteTrigger(oldRow, newRow)
	if err != nil {
		return fmt.Errorf("trigger %s WHEN clause: %w", trigger.Name, err)
	}
	if !shouldExec {
		return nil
	}

	return tr.executeTriggerBody(trigger, triggerRow)
}

// executeTriggerBody executes each statement in a trigger's body.
func (tr *TriggerRuntime) executeTriggerBody(
	trigger *schema.Trigger, triggerRow *vdbe.TriggerRowData,
) error {
	for i, stmt := range trigger.Body {
		if err := tr.executeTriggerStatement(stmt, triggerRow, trigger.Name, i); err != nil {
			return err
		}
	}
	return nil
}

// executeTriggerStatement compiles and executes a single trigger body statement.
func (tr *TriggerRuntime) executeTriggerStatement(
	stmt parser.Statement,
	triggerRow *vdbe.TriggerRowData,
	triggerName string, stmtIdx int,
) error {
	// Substitute OLD/NEW references with actual values
	substituted, err := tr.substituteReferences(stmt, triggerRow)
	if err != nil {
		return fmt.Errorf("trigger %s stmt %d: %w", triggerName, stmtIdx+1, err)
	}

	// Compile and execute the substituted statement
	return tr.compileAndExecute(substituted, triggerName, stmtIdx)
}

// substituteReferences replaces OLD.col and NEW.col with literal values.
func (tr *TriggerRuntime) substituteReferences(
	stmt parser.Statement, triggerRow *vdbe.TriggerRowData,
) (parser.Statement, error) {
	if triggerRow == nil {
		return stmt, nil
	}

	sub := &triggerSubstitutor{
		oldRow: triggerRow.OldRow,
		newRow: triggerRow.NewRow,
	}

	return sub.substituteStatement(stmt)
}

// compileAndExecute compiles a trigger body statement and runs it.
func (tr *TriggerRuntime) compileAndExecute(
	stmt parser.Statement, triggerName string, stmtIdx int,
) error {
	// Create a temporary Stmt to compile the trigger body statement
	triggerStmt := &Stmt{
		conn: tr.conn,
		ast:  stmt,
	}

	vm := triggerStmt.newVDBE()

	// Set up the trigger runtime on the sub-VM so recursive triggers work
	vm.Ctx.TriggerCompiler = tr

	compiled, err := triggerStmt.dispatchCompile(vm, nil)
	if err != nil {
		return fmt.Errorf("trigger %s stmt %d compile: %w", triggerName, stmtIdx+1, err)
	}

	// Run the compiled program
	return compiled.Run()
}

// extractRowMaps extracts oldRow and newRow maps from TriggerRowData.
func extractRowMaps(triggerRow *vdbe.TriggerRowData) (map[string]interface{}, map[string]interface{}) {
	if triggerRow == nil {
		return nil, nil
	}
	return triggerRow.OldRow, triggerRow.NewRow
}

// convertTiming converts an int timing value to parser.TriggerTiming.
func convertTiming(timing int) parser.TriggerTiming {
	switch timing {
	case 1:
		return parser.TriggerAfter
	default:
		return parser.TriggerBefore
	}
}

// convertEvent converts an int event value to parser.TriggerEvent.
func convertEvent(event int) parser.TriggerEvent {
	switch event {
	case 1:
		return parser.TriggerUpdate
	case 2:
		return parser.TriggerDelete
	default:
		return parser.TriggerInsert
	}
}

// ============================================================================
// Trigger AST Substitution
// ============================================================================

// triggerSubstitutor replaces OLD/NEW column references with literal values.
type triggerSubstitutor struct {
	oldRow map[string]interface{}
	newRow map[string]interface{}
}

// substituteStatement dispatches substitution by statement type.
func (s *triggerSubstitutor) substituteStatement(stmt parser.Statement) (parser.Statement, error) {
	switch st := stmt.(type) {
	case *parser.InsertStmt:
		return s.substituteInsert(st)
	case *parser.UpdateStmt:
		return s.substituteUpdate(st)
	case *parser.DeleteStmt:
		return s.substituteDelete(st)
	case *parser.SelectStmt:
		return s.substituteSelect(st)
	default:
		return stmt, nil
	}
}

// substituteInsert substitutes references in an INSERT statement.
func (s *triggerSubstitutor) substituteInsert(stmt *parser.InsertStmt) (*parser.InsertStmt, error) {
	newStmt := *stmt
	if len(stmt.Values) > 0 {
		newStmt.Values = make([][]parser.Expression, len(stmt.Values))
		for i, row := range stmt.Values {
			newRow, err := s.substituteExprList(row)
			if err != nil {
				return nil, err
			}
			newStmt.Values[i] = newRow
		}
	}
	return &newStmt, nil
}

// substituteUpdate substitutes references in an UPDATE statement.
func (s *triggerSubstitutor) substituteUpdate(stmt *parser.UpdateStmt) (*parser.UpdateStmt, error) {
	newStmt := *stmt
	newStmt.Sets = make([]parser.Assignment, len(stmt.Sets))
	for i, assign := range stmt.Sets {
		val, err := s.substituteExpr(assign.Value)
		if err != nil {
			return nil, err
		}
		newStmt.Sets[i] = parser.Assignment{Column: assign.Column, Value: val}
	}
	if stmt.Where != nil {
		where, err := s.substituteExpr(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = where
	}
	return &newStmt, nil
}

// substituteDelete substitutes references in a DELETE statement.
func (s *triggerSubstitutor) substituteDelete(stmt *parser.DeleteStmt) (*parser.DeleteStmt, error) {
	newStmt := *stmt
	if stmt.Where != nil {
		where, err := s.substituteExpr(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = where
	}
	return &newStmt, nil
}

// substituteSelect substitutes references in a SELECT statement.
func (s *triggerSubstitutor) substituteSelect(stmt *parser.SelectStmt) (*parser.SelectStmt, error) {
	newStmt := *stmt
	if stmt.Where != nil {
		where, err := s.substituteExpr(stmt.Where)
		if err != nil {
			return nil, err
		}
		newStmt.Where = where
	}
	return &newStmt, nil
}

// substituteExprList substitutes OLD/NEW references in a list of expressions.
func (s *triggerSubstitutor) substituteExprList(exprs []parser.Expression) ([]parser.Expression, error) {
	result := make([]parser.Expression, len(exprs))
	for i, expr := range exprs {
		sub, err := s.substituteExpr(expr)
		if err != nil {
			return nil, err
		}
		result[i] = sub
	}
	return result, nil
}

// substituteExpr recursively replaces OLD/NEW references in an expression.
func (s *triggerSubstitutor) substituteExpr(expr parser.Expression) (parser.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.IdentExpr:
		return s.substituteIdent(e)
	case *parser.BinaryExpr:
		return s.substituteBinary(e)
	case *parser.UnaryExpr:
		return s.substituteUnary(e)
	case *parser.FunctionExpr:
		return s.substituteFunction(e)
	case *parser.RaiseExpr:
		return e, nil // RAISE expressions don't need substitution
	default:
		return expr, nil
	}
}

// substituteIdent handles OLD.col and NEW.col identifier substitution.
func (s *triggerSubstitutor) substituteIdent(e *parser.IdentExpr) (parser.Expression, error) {
	qualifier := strings.ToUpper(e.Table)
	if qualifier != "OLD" && qualifier != "NEW" {
		return e, nil
	}

	row := s.oldRow
	if qualifier == "NEW" {
		row = s.newRow
	}
	if row == nil {
		return nil, fmt.Errorf("%s is not available in this trigger context", qualifier)
	}

	val, found := row[e.Name]
	if !found {
		// Try case-insensitive lookup
		val, found = caseInsensitiveLookup(row, e.Name)
		if !found {
			return nil, fmt.Errorf("column %s not found in %s", e.Name, qualifier)
		}
	}

	return valueToLiteral(val), nil
}

// substituteBinary substitutes in a binary expression.
func (s *triggerSubstitutor) substituteBinary(e *parser.BinaryExpr) (parser.Expression, error) {
	left, err := s.substituteExpr(e.Left)
	if err != nil {
		return nil, err
	}
	right, err := s.substituteExpr(e.Right)
	if err != nil {
		return nil, err
	}
	return &parser.BinaryExpr{Left: left, Op: e.Op, Right: right}, nil
}

// substituteUnary substitutes in a unary expression.
func (s *triggerSubstitutor) substituteUnary(e *parser.UnaryExpr) (parser.Expression, error) {
	operand, err := s.substituteExpr(e.Expr)
	if err != nil {
		return nil, err
	}
	return &parser.UnaryExpr{Op: e.Op, Expr: operand}, nil
}

// substituteFunction substitutes in function arguments.
func (s *triggerSubstitutor) substituteFunction(e *parser.FunctionExpr) (parser.Expression, error) {
	newArgs, err := s.substituteExprList(e.Args)
	if err != nil {
		return nil, err
	}
	return &parser.FunctionExpr{
		Name: e.Name, Args: newArgs, Distinct: e.Distinct, Star: e.Star,
		Filter: e.Filter, Over: e.Over,
	}, nil
}

// caseInsensitiveLookup finds a value in a map with case-insensitive key matching.
func caseInsensitiveLookup(m map[string]interface{}, key string) (interface{}, bool) {
	lower := strings.ToLower(key)
	for k, v := range m {
		if strings.ToLower(k) == lower {
			return v, true
		}
	}
	return nil, false
}

// valueToLiteral converts a Go value to a parser.LiteralExpr.
func valueToLiteral(val interface{}) *parser.LiteralExpr {
	if val == nil {
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	}
	switch v := val.(type) {
	case int64:
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: fmt.Sprintf("%d", v)}
	case int:
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: fmt.Sprintf("%d", v)}
	case float64:
		return &parser.LiteralExpr{Type: parser.LiteralFloat, Value: fmt.Sprintf("%g", v)}
	case string:
		return &parser.LiteralExpr{Type: parser.LiteralString, Value: v}
	case bool:
		if v {
			return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
		}
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}
	default:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	}
}

// tableHasTriggers checks if a table has any triggers defined.
func (s *Stmt) tableHasTriggers(tableName string) bool {
	triggers := s.conn.schema.GetTableTriggers(tableName, nil, nil)
	return len(triggers) > 0
}
