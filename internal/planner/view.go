// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ExpandView expands a view reference into its underlying SELECT statement.
// This is called when a view is referenced in a FROM clause.
func ExpandView(view *schema.View, alias string) (*parser.SelectStmt, error) {
	if view == nil {
		return nil, fmt.Errorf("nil view")
	}

	if view.Select == nil {
		return nil, fmt.Errorf("view %s has no SELECT statement", view.Name)
	}

	// Create a copy of the view's SELECT statement
	// This prevents modifications to the original view definition
	expandedSelect := copySelectStmt(view.Select)

	// If the view has explicit column names, we may need to rename columns
	// in the expanded SELECT to match the view's column names
	if len(view.Columns) > 0 {
		expandedSelect = renameViewColumns(expandedSelect, view.Columns)
	}

	return expandedSelect, nil
}

// copySelectStmt creates a deep copy of a SELECT statement.
// This is necessary because we may modify the statement during view expansion.
func copySelectStmt(stmt *parser.SelectStmt) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}

	// Create a shallow copy
	copy := *stmt

	// Copy slices to prevent shared references
	copyResultColumns(&copy, stmt)
	copyGroupByClause(&copy, stmt)
	copyOrderByClause(&copy, stmt)
	copyFromClause(&copy, stmt)

	return &copy
}

// copyResultColumns copies result columns from source to destination.
func copyResultColumns(dst, src *parser.SelectStmt) {
	if len(src.Columns) > 0 {
		dst.Columns = make([]parser.ResultColumn, len(src.Columns))
		copy(dst.Columns, src.Columns)
	}
}

// copyGroupByClause copies GROUP BY expressions from source to destination.
func copyGroupByClause(dst, src *parser.SelectStmt) {
	if len(src.GroupBy) > 0 {
		dst.GroupBy = make([]parser.Expression, len(src.GroupBy))
		copy(dst.GroupBy, src.GroupBy)
	}
}

// copyOrderByClause copies ORDER BY terms from source to destination.
func copyOrderByClause(dst, src *parser.SelectStmt) {
	if len(src.OrderBy) > 0 {
		dst.OrderBy = make([]parser.OrderingTerm, len(src.OrderBy))
		copy(dst.OrderBy, src.OrderBy)
	}
}

// copyFromClause copies FROM clause including tables and joins from source to destination.
func copyFromClause(dst, src *parser.SelectStmt) {
	if src.From != nil {
		fromCopy := *src.From
		copyFromTables(&fromCopy, src.From)
		copyFromJoins(&fromCopy, src.From)
		dst.From = &fromCopy
	}
}

// copyFromTables copies table references from source FROM clause to destination.
func copyFromTables(dst, src *parser.FromClause) {
	if len(src.Tables) > 0 {
		dst.Tables = make([]parser.TableOrSubquery, len(src.Tables))
		copy(dst.Tables, src.Tables)
	}
}

// copyFromJoins copies JOIN clauses from source FROM clause to destination.
func copyFromJoins(dst, src *parser.FromClause) {
	if len(src.Joins) > 0 {
		dst.Joins = make([]parser.JoinClause, len(src.Joins))
		copy(dst.Joins, src.Joins)
	}
}

// renameViewColumns applies explicit column names from a view definition
// to the expanded SELECT statement.
func renameViewColumns(stmt *parser.SelectStmt, viewColumns []string) *parser.SelectStmt {
	if stmt == nil || len(viewColumns) == 0 {
		return stmt
	}

	// Apply aliases to result columns based on view's column names
	for i, col := range viewColumns {
		if i < len(stmt.Columns) {
			stmt.Columns[i].Alias = col
		}
	}

	return stmt
}

// IsViewReference checks if a table reference might be a view.
// This is a helper function for the query planner.
func IsViewReference(tableName string, s *schema.Schema) bool {
	_, exists := s.GetView(tableName)
	return exists
}

// ExpandViewsInSelect recursively expands all view references in a SELECT statement.
// For simple views (single table with optional WHERE), it flattens them into the outer query.
// For complex views (joins, aggregates), it replaces them with subqueries.
func ExpandViewsInSelect(stmt *parser.SelectStmt, s *schema.Schema) (*parser.SelectStmt, error) {
	return flattenViewsInSelect(stmt, s, 0)
}

// flattenViewsInSelect attempts to flatten simple views into the outer query.
func flattenViewsInSelect(stmt *parser.SelectStmt, s *schema.Schema, depth int) (*parser.SelectStmt, error) {
	if stmt == nil || stmt.From == nil {
		return stmt, nil
	}

	// Prevent infinite recursion
	if depth > 100 {
		return nil, fmt.Errorf("view expansion depth limit exceeded")
	}

	// Make a copy of the statement to avoid mutating the original
	result := copySelectStmt(stmt)

	// Process each table in the FROM clause
	if err := processFromTablesForFlattening(result, s, depth); err != nil {
		return nil, err
	}

	return result, nil
}

// processFromTablesForFlattening processes FROM clause tables, flattening views where possible.
func processFromTablesForFlattening(stmt *parser.SelectStmt, s *schema.Schema, depth int) error {
	for i := range stmt.From.Tables {
		table := &stmt.From.Tables[i]

		// Skip if it's already a subquery
		if table.Subquery != nil {
			continue
		}

		// Check if this is a view reference
		view, exists := s.GetView(table.TableName)
		if !exists {
			continue
		}

		// Process the view (flatten or expand as subquery)
		if err := processViewInFromClause(stmt, i, view, table, s, depth); err != nil {
			return err
		}
	}
	return nil
}

// processViewInFromClause handles a view reference by either flattening or expanding as subquery.
func processViewInFromClause(stmt *parser.SelectStmt, tableIdx int, view *schema.View, table *parser.TableOrSubquery, s *schema.Schema, depth int) error {
	if canFlattenView(view) {
		return flattenViewInPlace(stmt, tableIdx, view, s, depth)
	}
	return expandViewAsSubquery(table, view, s, depth)
}

// flattenViewInPlace flattens a simple view directly into the statement.
func flattenViewInPlace(stmt *parser.SelectStmt, tableIdx int, view *schema.View, s *schema.Schema, depth int) error {
	flattenedStmt, err := flattenSimpleView(stmt, tableIdx, view, s, depth)
	if err != nil {
		return err
	}
	*stmt = *flattenedStmt
	return nil
}

// expandViewAsSubquery expands a complex view as a subquery.
func expandViewAsSubquery(table *parser.TableOrSubquery, view *schema.View, s *schema.Schema, depth int) error {
	expandedSelect, err := ExpandView(view, table.Alias)
	if err != nil {
		return fmt.Errorf("failed to expand view %s: %w", table.TableName, err)
	}

	// Recursively flatten views in the expanded select
	expandedSelect, err = flattenViewsInSelect(expandedSelect, s, depth+1)
	if err != nil {
		return err
	}

	table.Subquery = expandedSelect
	if table.Alias == "" {
		table.Alias = table.TableName
	}
	table.TableName = ""
	return nil
}

// canFlattenView checks if a view can be flattened into the outer query.
// A view can be flattened if it's a simple SELECT from a single table.
func canFlattenView(view *schema.View) bool {
	if view == nil || view.Select == nil {
		return false
	}

	sel := view.Select

	return hasValidFromClauseForFlattening(sel) &&
		hasNoComplexFeatures(sel) &&
		hasNoExplicitColumns(view)
}

// hasValidFromClauseForFlattening checks if FROM clause allows flattening.
func hasValidFromClauseForFlattening(sel *parser.SelectStmt) bool {
	// Must have a FROM clause with exactly one table (no subqueries)
	if sel.From == nil || len(sel.From.Tables) != 1 {
		return false
	}

	// No subquery in FROM
	if sel.From.Tables[0].Subquery != nil {
		return false
	}

	// No JOINs
	if len(sel.From.Joins) > 0 {
		return false
	}

	return true
}

// hasNoComplexFeatures checks if the SELECT has no features that prevent flattening.
func hasNoComplexFeatures(sel *parser.SelectStmt) bool {
	// No GROUP BY, HAVING
	if len(sel.GroupBy) > 0 || sel.Having != nil {
		return false
	}

	// No LIMIT/OFFSET
	if sel.Limit != nil || sel.Offset != nil {
		return false
	}

	// No compound SELECT (UNION, etc)
	if sel.Compound != nil {
		return false
	}

	return true
}

// hasNoExplicitColumns checks if view has no explicit column names.
func hasNoExplicitColumns(view *schema.View) bool {
	// We can flatten views with explicit column names as long as we
	// properly apply the column name mapping during flattening
	return true
}

// flattenSimpleView flattens a simple view into the outer query.
func flattenSimpleView(outer *parser.SelectStmt, tableIdx int, view *schema.View, s *schema.Schema, depth int) (*parser.SelectStmt, error) {
	viewSelect, underlyingTable, err := resolveViewSelect(view, s, depth)
	if err != nil {
		return nil, err
	}

	replaceViewWithTable(outer, tableIdx, underlyingTable)

	if len(view.Columns) > 0 {
		if err := applyViewColumnMapping(outer, view, viewSelect); err != nil {
			return nil, err
		}
	}

	handleSelectStar(outer, viewSelect)
	mergeWhereClauses(outer, viewSelect)

	return outer, nil
}

// resolveViewSelect resolves a view's SELECT, recursively flattening nested views
func resolveViewSelect(view *schema.View, s *schema.Schema, depth int) (*parser.SelectStmt, string, error) {
	viewSelect := view.Select
	underlyingTable := viewSelect.From.Tables[0].TableName

	innerView, exists := s.GetView(underlyingTable)
	if !exists || !canFlattenView(innerView) {
		return viewSelect, underlyingTable, nil
	}

	flattenedInner, err := flattenSimpleView(viewSelect, 0, innerView, s, depth+1)
	if err != nil {
		return nil, "", err
	}
	return flattenedInner, flattenedInner.From.Tables[0].TableName, nil
}

// replaceViewWithTable replaces a view reference with its underlying table
func replaceViewWithTable(outer *parser.SelectStmt, tableIdx int, underlyingTable string) {
	outer.From.Tables[tableIdx].TableName = underlyingTable
	outer.From.Tables[tableIdx].Subquery = nil
}

// handleSelectStar replaces SELECT * with the view's columns
func handleSelectStar(outer, viewSelect *parser.SelectStmt) {
	if isSelectStar(outer) {
		outer.Columns = viewSelect.Columns
	}
}

// mergeWhereClauses combines the outer and view WHERE clauses
func mergeWhereClauses(outer, viewSelect *parser.SelectStmt) {
	if viewSelect.Where == nil {
		return
	}

	if outer.Where != nil {
		outer.Where = &parser.BinaryExpr{
			Left:  outer.Where,
			Op:    parser.OpAnd,
			Right: viewSelect.Where,
		}
	} else {
		outer.Where = viewSelect.Where
	}
}

// applyViewColumnMapping maps column references from the view's explicit column
// names to the underlying table's column names.
func applyViewColumnMapping(outer *parser.SelectStmt, view *schema.View, viewSelect *parser.SelectStmt) error {
	columnMap := buildColumnMap(view, viewSelect)

	rewriteSelectColumns(outer, columnMap)
	rewriteWhereClause(outer, columnMap)
	rewriteOrderByClause(outer, columnMap)
	rewriteGroupByClause(outer, columnMap)
	rewriteHavingClause(outer, columnMap)

	return nil
}

// buildColumnMap creates a mapping from view column names to underlying column expressions.
func buildColumnMap(view *schema.View, viewSelect *parser.SelectStmt) map[string]parser.Expression {
	columnMap := make(map[string]parser.Expression)
	for i, viewColName := range view.Columns {
		if i < len(viewSelect.Columns) {
			columnMap[viewColName] = viewSelect.Columns[i].Expr
		}
	}
	return columnMap
}

// rewriteSelectColumns rewrites column references in SELECT columns and preserves aliases.
func rewriteSelectColumns(outer *parser.SelectStmt, columnMap map[string]parser.Expression) {
	for i := range outer.Columns {
		origName := extractOriginalName(&outer.Columns[i])
		outer.Columns[i].Expr = rewriteColumnReferences(outer.Columns[i].Expr, columnMap)
		preserveColumnAlias(&outer.Columns[i], origName)
	}
}

// extractOriginalName extracts the original column name from a result column.
func extractOriginalName(col *parser.ResultColumn) string {
	if ident, ok := col.Expr.(*parser.IdentExpr); ok && ident.Table == "" {
		return ident.Name
	}
	return ""
}

// preserveColumnAlias sets the alias if needed to preserve the original column name.
func preserveColumnAlias(col *parser.ResultColumn, origName string) {
	if origName != "" && col.Alias == "" {
		col.Alias = origName
	}
}

// rewriteWhereClause rewrites column references in the WHERE clause.
func rewriteWhereClause(outer *parser.SelectStmt, columnMap map[string]parser.Expression) {
	if outer.Where != nil {
		outer.Where = rewriteColumnReferences(outer.Where, columnMap)
	}
}

// rewriteOrderByClause rewrites column references in the ORDER BY clause.
func rewriteOrderByClause(outer *parser.SelectStmt, columnMap map[string]parser.Expression) {
	for i := range outer.OrderBy {
		outer.OrderBy[i].Expr = rewriteColumnReferences(outer.OrderBy[i].Expr, columnMap)
	}
}

// rewriteGroupByClause rewrites column references in the GROUP BY clause.
func rewriteGroupByClause(outer *parser.SelectStmt, columnMap map[string]parser.Expression) {
	for i := range outer.GroupBy {
		outer.GroupBy[i] = rewriteColumnReferences(outer.GroupBy[i], columnMap)
	}
}

// rewriteHavingClause rewrites column references in the HAVING clause.
func rewriteHavingClause(outer *parser.SelectStmt, columnMap map[string]parser.Expression) {
	if outer.Having != nil {
		outer.Having = rewriteColumnReferences(outer.Having, columnMap)
	}
}

// copyExpression creates a deep copy of an expression to avoid shared references.
func copyExpression(expr parser.Expression) parser.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *parser.IdentExpr:
		return copyIdentExpr(e)
	case *parser.LiteralExpr:
		return copyLiteralExpr(e)
	case *parser.BinaryExpr:
		return copyBinaryExpr(e)
	case *parser.UnaryExpr:
		return copyUnaryExpr(e)
	case *parser.FunctionExpr:
		return copyFunctionExpr(e)
	default:
		return expr
	}
}

// copyIdentExpr creates a copy of an IdentExpr.
func copyIdentExpr(e *parser.IdentExpr) parser.Expression {
	copy := *e
	return &copy
}

// copyLiteralExpr creates a copy of a LiteralExpr.
func copyLiteralExpr(e *parser.LiteralExpr) parser.Expression {
	copy := *e
	return &copy
}

// copyBinaryExpr creates a deep copy of a BinaryExpr.
func copyBinaryExpr(e *parser.BinaryExpr) parser.Expression {
	copy := *e
	copy.Left = copyExpression(e.Left)
	copy.Right = copyExpression(e.Right)
	return &copy
}

// copyUnaryExpr creates a deep copy of a UnaryExpr.
func copyUnaryExpr(e *parser.UnaryExpr) parser.Expression {
	copy := *e
	copy.Expr = copyExpression(e.Expr)
	return &copy
}

// copyFunctionExpr creates a deep copy of a FunctionExpr.
func copyFunctionExpr(e *parser.FunctionExpr) parser.Expression {
	copy := *e
	copy.Args = make([]parser.Expression, len(e.Args))
	for i, arg := range e.Args {
		copy.Args[i] = copyExpression(arg)
	}
	return &copy
}

// rewriteColumnReferences recursively rewrites column references using the mapping.
func rewriteColumnReferences(expr parser.Expression, columnMap map[string]parser.Expression) parser.Expression {
	if expr == nil {
		return nil
	}
	return dispatchRewrite(expr, columnMap)
}

// dispatchRewrite dispatches to the appropriate rewriter based on expression type.
func dispatchRewrite(expr parser.Expression, columnMap map[string]parser.Expression) parser.Expression {
	// Handle IdentExpr specially as it's the most common case
	if e, ok := expr.(*parser.IdentExpr); ok {
		return rewriteIdentExpr(e, columnMap)
	}

	// Handle BinaryExpr as second most common case
	if e, ok := expr.(*parser.BinaryExpr); ok {
		return rewriteBinaryExpr(e, columnMap)
	}

	// Handle remaining expression types
	return dispatchComplexRewrite(expr, columnMap)
}

// dispatchComplexRewrite handles less common expression types.
func dispatchComplexRewrite(expr parser.Expression, columnMap map[string]parser.Expression) parser.Expression {
	switch e := expr.(type) {
	case *parser.UnaryExpr:
		return rewriteUnaryExpr(e, columnMap)
	case *parser.FunctionExpr:
		return rewriteFunctionExpr(e, columnMap)
	case *parser.CaseExpr:
		return rewriteCaseExpr(e, columnMap)
	default:
		return dispatchWrapperRewrite(expr, columnMap)
	}
}

// dispatchWrapperRewrite handles wrapper expression types (CAST, COLLATE, Paren).
func dispatchWrapperRewrite(expr parser.Expression, columnMap map[string]parser.Expression) parser.Expression {
	switch e := expr.(type) {
	case *parser.CastExpr:
		return rewriteCastExpr(e, columnMap)
	case *parser.CollateExpr:
		return rewriteCollateExpr(e, columnMap)
	case *parser.ParenExpr:
		return rewriteParenExpr(e, columnMap)
	default:
		// For other expression types (literals, etc.), return as-is
		return expr
	}
}

// rewriteIdentExpr rewrites identifier expressions using the column map.
func rewriteIdentExpr(e *parser.IdentExpr, columnMap map[string]parser.Expression) parser.Expression {
	// Check if this is a view column that needs to be mapped
	if mapped, ok := columnMap[e.Name]; ok {
		// Return a copy of the mapped expression to avoid shared references
		return copyExpression(mapped)
	}
	return e
}

// rewriteBinaryExpr rewrites binary expressions recursively.
func rewriteBinaryExpr(e *parser.BinaryExpr, columnMap map[string]parser.Expression) parser.Expression {
	e.Left = rewriteColumnReferences(e.Left, columnMap)
	e.Right = rewriteColumnReferences(e.Right, columnMap)
	return e
}

// rewriteUnaryExpr rewrites unary expressions recursively.
func rewriteUnaryExpr(e *parser.UnaryExpr, columnMap map[string]parser.Expression) parser.Expression {
	e.Expr = rewriteColumnReferences(e.Expr, columnMap)
	return e
}

// rewriteFunctionExpr rewrites function expressions recursively.
func rewriteFunctionExpr(e *parser.FunctionExpr, columnMap map[string]parser.Expression) parser.Expression {
	for i := range e.Args {
		e.Args[i] = rewriteColumnReferences(e.Args[i], columnMap)
	}
	return e
}

// rewriteCaseExpr rewrites CASE expressions recursively.
func rewriteCaseExpr(e *parser.CaseExpr, columnMap map[string]parser.Expression) parser.Expression {
	for i := range e.WhenClauses {
		e.WhenClauses[i].Condition = rewriteColumnReferences(e.WhenClauses[i].Condition, columnMap)
		e.WhenClauses[i].Result = rewriteColumnReferences(e.WhenClauses[i].Result, columnMap)
	}
	e.ElseClause = rewriteColumnReferences(e.ElseClause, columnMap)
	return e
}

// rewriteCastExpr rewrites CAST expressions recursively.
func rewriteCastExpr(e *parser.CastExpr, columnMap map[string]parser.Expression) parser.Expression {
	e.Expr = rewriteColumnReferences(e.Expr, columnMap)
	return e
}

// rewriteCollateExpr rewrites COLLATE expressions recursively.
func rewriteCollateExpr(e *parser.CollateExpr, columnMap map[string]parser.Expression) parser.Expression {
	e.Expr = rewriteColumnReferences(e.Expr, columnMap)
	return e
}

// rewriteParenExpr rewrites parenthesized expressions recursively.
func rewriteParenExpr(e *parser.ParenExpr, columnMap map[string]parser.Expression) parser.Expression {
	e.Expr = rewriteColumnReferences(e.Expr, columnMap)
	return e
}

// isSelectStar checks if a SELECT statement uses SELECT *.
func isSelectStar(stmt *parser.SelectStmt) bool {
	if len(stmt.Columns) == 1 {
		col := stmt.Columns[0]
		if col.Star && col.Table == "" {
			return true
		}
	}
	return false
}

// expandViewsInSelectWithDepth handles recursive view expansion with depth tracking to prevent infinite loops.
func expandViewsInSelectWithDepth(stmt *parser.SelectStmt, s *schema.Schema, depth int) (*parser.SelectStmt, error) {
	if stmt == nil || stmt.From == nil {
		return stmt, nil
	}

	// Prevent infinite recursion from circular view references
	if depth > 100 {
		return nil, fmt.Errorf("view expansion depth limit exceeded (possible circular reference)")
	}

	// Process each table in the FROM clause
	if err := expandViewsInFromTables(stmt.From.Tables, s, depth); err != nil {
		return nil, err
	}

	// Process JOINs as well
	if err := expandViewsInJoins(stmt.From.Joins, s, depth); err != nil {
		return nil, err
	}

	return stmt, nil
}

// expandViewsInFromTables expands views in FROM clause tables.
func expandViewsInFromTables(tables []parser.TableOrSubquery, s *schema.Schema, depth int) error {
	for i := range tables {
		table := &tables[i]

		// If it's already a subquery, recursively expand views within it
		if table.Subquery != nil {
			expandedSubquery, err := expandViewsInSelectWithDepth(table.Subquery, s, depth+1)
			if err != nil {
				return err
			}
			table.Subquery = expandedSubquery
			continue
		}

		// Check if this is a view reference
		if view, exists := s.GetView(table.TableName); exists {
			if err := expandViewReference(table, view, s, depth); err != nil {
				return err
			}
		}
	}
	return nil
}

// expandViewsInJoins expands views in JOIN clauses.
func expandViewsInJoins(joins []parser.JoinClause, s *schema.Schema, depth int) error {
	for i := range joins {
		join := &joins[i]

		// If it's already a subquery, recursively expand views within it
		if join.Table.Subquery != nil {
			expandedSubquery, err := expandViewsInSelectWithDepth(join.Table.Subquery, s, depth+1)
			if err != nil {
				return err
			}
			join.Table.Subquery = expandedSubquery
			continue
		}

		// Check if this is a view reference
		if view, exists := s.GetView(join.Table.TableName); exists {
			if err := expandViewReference(&join.Table, view, s, depth); err != nil {
				return err
			}
		}
	}
	return nil
}

// expandViewReference expands a single view reference into a subquery.
func expandViewReference(table *parser.TableOrSubquery, view *schema.View, s *schema.Schema, depth int) error {
	// Expand the view
	expandedSelect, err := ExpandView(view, table.Alias)
	if err != nil {
		return fmt.Errorf("failed to expand view %s: %w", table.TableName, err)
	}

	// Recursively expand views within the expanded view
	expandedSelect, err = expandViewsInSelectWithDepth(expandedSelect, s, depth+1)
	if err != nil {
		return err
	}

	// Replace the table reference with a subquery
	table.Subquery = expandedSelect

	// If no alias was specified, use the view name as the alias
	if table.Alias == "" {
		table.Alias = table.TableName
	}

	// Clear the table name since it's now a subquery
	table.TableName = ""
	return nil
}
