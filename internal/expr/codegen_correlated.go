// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// outerRef describes a reference from a subquery to an outer scope column.
type outerRef struct {
	Table  string // outer table alias (e.g. "a")
	Column string // column name (e.g. "age")
}

// findOuterRefs walks a subquery AST to find column references that belong
// to the outer query scope (i.e., their Table qualifier is known in the outer
// cursorMap but not in the subquery's FROM clause). Returns deduplicated refs.
func (g *CodeGenerator) findOuterRefs(stmt *parser.SelectStmt) []outerRef {
	subTables := collectSubqueryTables(stmt)
	var refs []outerRef
	seen := map[outerRef]bool{}

	walkExpr(stmt.Where, func(e parser.Expression) {
		ident, ok := e.(*parser.IdentExpr)
		if !ok || ident.Table == "" {
			return
		}
		if _, inOuter := g.cursorMap[ident.Table]; !inOuter {
			return
		}
		if subTables[ident.Table] {
			return
		}
		ref := outerRef{Table: ident.Table, Column: ident.Name}
		if !seen[ref] {
			seen[ref] = true
			refs = append(refs, ref)
		}
	})
	return refs
}

// collectSubqueryTables returns a set of table names/aliases defined in a
// subquery's FROM clause.
func collectSubqueryTables(stmt *parser.SelectStmt) map[string]bool {
	tables := map[string]bool{}
	if stmt.From == nil {
		return tables
	}
	for _, t := range stmt.From.Tables {
		if t.Alias != "" {
			tables[t.Alias] = true
		}
		if t.TableName != "" {
			tables[t.TableName] = true
		}
	}
	return tables
}

// walkExpr traverses an expression tree calling fn on each node.
func walkExpr(e parser.Expression, fn func(parser.Expression)) {
	if e == nil {
		return
	}
	fn(e)
	for _, child := range exprChildren(e) {
		walkExpr(child, fn)
	}
}

// exprChildren returns the child expressions of an AST node.
func exprChildren(e parser.Expression) []parser.Expression {
	switch v := e.(type) {
	case *parser.BinaryExpr:
		return []parser.Expression{v.Left, v.Right}
	case *parser.UnaryExpr:
		return []parser.Expression{v.Expr}
	case *parser.ParenExpr:
		return []parser.Expression{v.Expr}
	case *parser.FunctionExpr:
		return v.Args
	case *parser.InExpr:
		return append([]parser.Expression{v.Expr}, v.Values...)
	case *parser.BetweenExpr:
		return []parser.Expression{v.Expr, v.Lower, v.Upper}
	case *parser.CaseExpr:
		return caseExprChildren(v)
	case *parser.CastExpr:
		return []parser.Expression{v.Expr}
	case *parser.CollateExpr:
		return []parser.Expression{v.Expr}
	default:
		return nil
	}
}

// caseExprChildren returns children of a CASE expression.
func caseExprChildren(v *parser.CaseExpr) []parser.Expression {
	children := []parser.Expression{v.Expr}
	for _, w := range v.WhenClauses {
		children = append(children, w.Condition, w.Result)
	}
	return append(children, v.ElseClause)
}

// emitCorrelatedExists generates OpCorrelatedExists for a correlated EXISTS.
// It loads outer column values into binding registers and emits the opcode.
func (g *CodeGenerator) emitCorrelatedExists(e *parser.ExistsExpr, refs []outerRef) (int, error) {
	firstBindReg, err := g.emitOuterBindings(refs)
	if err != nil {
		return 0, err
	}
	resultReg := g.AllocReg()

	notFlag := uint16(0)
	if e.Not {
		notFlag = 1
	}

	callback := g.buildExistsCallback(e.Select, refs)
	addr := g.vdbe.AddOpWithP4Callback(
		vdbe.OpCorrelatedExists, resultReg, firstBindReg, len(refs), callback,
	)
	g.vdbe.Program[addr].P5 = notFlag
	g.vdbe.SetComment(addr, "Correlated EXISTS subquery")
	return resultReg, nil
}

// emitCorrelatedScalar generates OpCorrelatedScalar for a correlated scalar
// subquery. It loads outer column values into binding registers.
func (g *CodeGenerator) emitCorrelatedScalar(e *parser.SubqueryExpr, refs []outerRef) (int, error) {
	firstBindReg, err := g.emitOuterBindings(refs)
	if err != nil {
		return 0, err
	}
	resultReg := g.AllocReg()

	callback := g.buildScalarCallback(e.Select, refs)
	addr := g.vdbe.AddOpWithP4Callback(
		vdbe.OpCorrelatedScalar, resultReg, firstBindReg, len(refs), callback,
	)
	g.vdbe.SetComment(addr, "Correlated scalar subquery")
	return resultReg, nil
}

// emitOuterBindings loads the outer column values into consecutive registers.
// Returns the first register index.
func (g *CodeGenerator) emitOuterBindings(refs []outerRef) (int, error) {
	firstReg := g.nextReg
	for _, ref := range refs {
		reg := g.AllocReg()
		cursor, ok := g.cursorMap[ref.Table]
		if !ok {
			return 0, fmt.Errorf("outer table %s not found", ref.Table)
		}
		colIdx, isRowid, err := g.lookupColumnInfo(ref.Table, ref.Column)
		if err != nil {
			return 0, err
		}
		g.emitColumnOpcode(cursor, colIdx, isRowid, reg)
	}
	return firstReg, nil
}

// buildExistsCallback builds the CorrelatedExistsFunc closure for runtime eval.
func (g *CodeGenerator) buildExistsCallback(
	selectStmt *parser.SelectStmt, refs []outerRef,
) vdbe.CorrelatedExistsFunc {
	executor := g.subqueryExecutor
	return func(bindings []interface{}) (bool, error) {
		rewritten := rewriteOuterRefs(selectStmt, refs, bindings)
		rows, err := executor(rewritten)
		if err != nil {
			return false, err
		}
		return len(rows) > 0, nil
	}
}

// buildScalarCallback builds the CorrelatedScalarFunc closure for runtime eval.
func (g *CodeGenerator) buildScalarCallback(
	selectStmt *parser.SelectStmt, refs []outerRef,
) vdbe.CorrelatedScalarFunc {
	executor := g.subqueryExecutor
	return func(bindings []interface{}) (interface{}, error) {
		rewritten := rewriteOuterRefs(selectStmt, refs, bindings)
		rows, err := executor(rewritten)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 || len(rows[0]) == 0 {
			return nil, nil
		}
		return rows[0][0], nil
	}
}

// rewriteOuterRefs creates a shallow copy of the SELECT statement with outer
// column references replaced by literal values from the binding slice.
func rewriteOuterRefs(
	stmt *parser.SelectStmt, refs []outerRef, values []interface{},
) *parser.SelectStmt {
	refMap := buildRefMap(refs, values)
	stmtCopy := *stmt
	stmtCopy.Where = rewriteExpr(stmt.Where, refMap)
	return &stmtCopy
}

// buildRefMap creates a lookup from "table.column" to literal value.
func buildRefMap(refs []outerRef, values []interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(refs))
	for i, ref := range refs {
		m[ref.Table+"."+ref.Column] = values[i]
	}
	return m
}

// rewriteExpr recursively replaces outer column references with literals.
func rewriteExpr(e parser.Expression, refMap map[string]interface{}) parser.Expression {
	if e == nil {
		return nil
	}
	switch v := e.(type) {
	case *parser.IdentExpr:
		return rewriteIdent(v, refMap)
	case *parser.BinaryExpr:
		return rewriteBinary(v, refMap)
	case *parser.UnaryExpr:
		return rewriteUnary(v, refMap)
	case *parser.ParenExpr:
		return rewriteParen(v, refMap)
	default:
		return e
	}
}

func rewriteIdent(v *parser.IdentExpr, refMap map[string]interface{}) parser.Expression {
	if v.Table == "" {
		return v
	}
	if val, ok := refMap[v.Table+"."+v.Name]; ok {
		return valueToLiteralExpr(val)
	}
	return v
}

func rewriteBinary(v *parser.BinaryExpr, refMap map[string]interface{}) parser.Expression {
	newLeft := rewriteExpr(v.Left, refMap)
	newRight := rewriteExpr(v.Right, refMap)
	if newLeft == v.Left && newRight == v.Right {
		return v
	}
	cp := *v
	cp.Left = newLeft
	cp.Right = newRight
	return &cp
}

func rewriteUnary(v *parser.UnaryExpr, refMap map[string]interface{}) parser.Expression {
	newExpr := rewriteExpr(v.Expr, refMap)
	if newExpr == v.Expr {
		return v
	}
	cp := *v
	cp.Expr = newExpr
	return &cp
}

func rewriteParen(v *parser.ParenExpr, refMap map[string]interface{}) parser.Expression {
	newE := rewriteExpr(v.Expr, refMap)
	if newE == v.Expr {
		return v
	}
	cp := *v
	cp.Expr = newE
	return &cp
}

// valueToLiteralExpr converts a Go value to a parser LiteralExpr.
func valueToLiteralExpr(val interface{}) *parser.LiteralExpr {
	switch v := val.(type) {
	case nil:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	case int64:
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: fmt.Sprintf("%d", v)}
	case float64:
		return &parser.LiteralExpr{Type: parser.LiteralFloat, Value: fmt.Sprintf("%g", v)}
	case string:
		return &parser.LiteralExpr{Type: parser.LiteralString, Value: v}
	default:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	}
}
