package planner

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
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
	if len(stmt.Columns) > 0 {
		copy.Columns = make([]parser.ResultColumn, len(stmt.Columns))
		for i := range stmt.Columns {
			copy.Columns[i] = stmt.Columns[i]
		}
	}

	if len(stmt.GroupBy) > 0 {
		copy.GroupBy = make([]parser.Expression, len(stmt.GroupBy))
		for i := range stmt.GroupBy {
			copy.GroupBy[i] = stmt.GroupBy[i]
		}
	}

	if len(stmt.OrderBy) > 0 {
		copy.OrderBy = make([]parser.OrderingTerm, len(stmt.OrderBy))
		for i := range stmt.OrderBy {
			copy.OrderBy[i] = stmt.OrderBy[i]
		}
	}

	// Copy FROM clause if present
	if stmt.From != nil {
		fromCopy := *stmt.From
		if len(stmt.From.Tables) > 0 {
			fromCopy.Tables = make([]parser.TableOrSubquery, len(stmt.From.Tables))
			for i := range stmt.From.Tables {
				fromCopy.Tables[i] = stmt.From.Tables[i]
			}
		}
		if len(stmt.From.Joins) > 0 {
			fromCopy.Joins = make([]parser.JoinClause, len(stmt.From.Joins))
			for i := range stmt.From.Joins {
				fromCopy.Joins[i] = stmt.From.Joins[i]
			}
		}
		copy.From = &fromCopy
	}

	return &copy
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
	for i := range result.From.Tables {
		table := &result.From.Tables[i]

		// Skip if it's already a subquery
		if table.Subquery != nil {
			continue
		}

		// Check if this is a view reference
		view, exists := s.GetView(table.TableName)
		if !exists {
			continue
		}

		// Check if this view can be flattened (simple single-table view)
		if canFlattenView(view) {
			// Flatten the view
			flattenedStmt, err := flattenSimpleView(result, i, view, s, depth)
			if err != nil {
				return nil, err
			}
			result = flattenedStmt
		} else {
			// Fall back to subquery for complex views
			expandedSelect, err := ExpandView(view, table.Alias)
			if err != nil {
				return nil, fmt.Errorf("failed to expand view %s: %w", table.TableName, err)
			}

			// Recursively flatten views in the expanded select
			expandedSelect, err = flattenViewsInSelect(expandedSelect, s, depth+1)
			if err != nil {
				return nil, err
			}

			table.Subquery = expandedSelect
			if table.Alias == "" {
				table.Alias = table.TableName
			}
			table.TableName = ""
		}
	}

	return result, nil
}

// canFlattenView checks if a view can be flattened into the outer query.
// A view can be flattened if it's a simple SELECT from a single table.
func canFlattenView(view *schema.View) bool {
	if view == nil || view.Select == nil {
		return false
	}

	sel := view.Select

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

	// Don't flatten views with explicit column names
	// because flattening loses the column name mapping
	if len(view.Columns) > 0 {
		return false
	}

	return true
}

// flattenSimpleView flattens a simple view into the outer query.
func flattenSimpleView(outer *parser.SelectStmt, tableIdx int, view *schema.View, s *schema.Schema, depth int) (*parser.SelectStmt, error) {
	viewSelect := view.Select

	// Get the underlying table name from the view
	underlyingTable := viewSelect.From.Tables[0].TableName

	// Check if the underlying table is also a view (recursive view resolution)
	if innerView, exists := s.GetView(underlyingTable); exists {
		if canFlattenView(innerView) {
			// Recursively flatten
			flattenedInner, err := flattenSimpleView(viewSelect, 0, innerView, s, depth+1)
			if err != nil {
				return nil, err
			}
			viewSelect = flattenedInner
			underlyingTable = viewSelect.From.Tables[0].TableName
		}
	}

	// Replace the view reference with the underlying table
	outer.From.Tables[tableIdx].TableName = underlyingTable
	outer.From.Tables[tableIdx].Subquery = nil

	// Handle SELECT * from the view - replace with view's columns
	if isSelectStar(outer) {
		outer.Columns = viewSelect.Columns
	}

	// Merge WHERE clauses
	if viewSelect.Where != nil {
		if outer.Where != nil {
			// Combine with AND
			outer.Where = &parser.BinaryExpr{
				Left:  outer.Where,
				Op:    parser.OpAnd,
				Right: viewSelect.Where,
			}
		} else {
			outer.Where = viewSelect.Where
		}
	}

	return outer, nil
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
