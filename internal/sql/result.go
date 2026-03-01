// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"fmt"
)

// ResultColumn represents a column in a SELECT result set.
type ResultColumn struct {
	Expr     *Expr  // Expression that generates the column value
	Name     string // Column name (AS clause or derived)
	Table    string // Source table name (for metadata)
	Database string // Source database name (for metadata)
	OrigCol  string // Original column name (for metadata)
	DeclType string // Declared type (for metadata)
}

// ExprListItem represents an item in an expression list.
type ExprListItem struct {
	Expr       *Expr  // The expression
	Name       string // Name assigned via AS clause or generated
	SortOrder  int    // ASCENDING or DESCENDING for ORDER BY
	OrderByCol int    // Index into result set for ORDER BY optimization
	Done       bool   // Processing complete flag
	BUsed      bool   // Used flag for nested queries
	BSorterRef bool   // Use sorter-reference optimization
	BNoExpand  bool   // Do not expand * or table.*
	BUsingTerm bool   // Part of USING clause
}

// ExprList represents a list of expressions (for SELECT, ORDER BY, GROUP BY).
type ExprList struct {
	Items []ExprListItem
}

// ResultCompiler handles compilation of result column expressions.
type ResultCompiler struct {
	parse *Parse
}

// NewResultCompiler creates a new result compiler.
func NewResultCompiler(parse *Parse) *ResultCompiler {
	return &ResultCompiler{parse: parse}
}

// NewExprList creates a new expression list.
func NewExprList() *ExprList {
	return &ExprList{
		Items: make([]ExprListItem, 0),
	}
}

// Len returns the number of items in the expression list.
func (el *ExprList) Len() int {
	if el == nil {
		return 0
	}
	return len(el.Items)
}

// Get returns the item at the given index.
func (el *ExprList) Get(idx int) *ExprListItem {
	if el == nil || idx < 0 || idx >= len(el.Items) {
		return nil
	}
	return &el.Items[idx]
}

// Append adds an item to the expression list.
func (el *ExprList) Append(item ExprListItem) {
	if el != nil {
		el.Items = append(el.Items, item)
	}
}

// ExpandResultColumns expands * and table.* wildcards in SELECT list.
func (rc *ResultCompiler) ExpandResultColumns(sel *Select) error {
	if sel.EList == nil {
		return fmt.Errorf("nil expression list")
	}

	expanded := NewExprList()

	for i := 0; i < sel.EList.Len(); i++ {
		item := sel.EList.Get(i)
		if err := rc.expandItem(sel, item, expanded); err != nil {
			return err
		}
	}

	sel.EList = expanded
	return nil
}

// expandItem expands a single result item, handling wildcards.
func (rc *ResultCompiler) expandItem(sel *Select, item *ExprListItem, expanded *ExprList) error {
	if item.Expr.Op == TK_ASTERISK {
		return rc.expandStar(sel, expanded)
	}
	if rc.isTableStar(item.Expr) {
		return rc.expandTableStar(sel, item.Expr.Left, expanded)
	}
	expanded.Append(*item)
	return nil
}

// isTableStar returns true if expr is a table.* wildcard.
func (rc *ResultCompiler) isTableStar(expr *Expr) bool {
	return expr.Op == TK_DOT && expr.Right != nil && expr.Right.Op == TK_ASTERISK
}

// expandStar expands * to all columns from all tables in FROM clause.
func (rc *ResultCompiler) expandStar(sel *Select, result *ExprList) error {
	if sel.Src == nil || sel.Src.Len() == 0 {
		return fmt.Errorf("* used with no tables in FROM clause")
	}

	for i := 0; i < sel.Src.Len(); i++ {
		srcItem := sel.Src.Get(i)
		if srcItem.Table == nil {
			continue
		}

		table := srcItem.Table
		for colIdx := 0; colIdx < table.NumColumns; colIdx++ {
			col := table.GetColumn(colIdx)

			// Create column reference expression
			expr := &Expr{
				Op:     TK_COLUMN,
				Table:  srcItem.Cursor,
				Column: colIdx,
			}

			// Generate column name
			name := col.Name
			if sel.Src.Len() > 1 {
				// Multiple tables - use table.column format
				name = fmt.Sprintf("%s.%s", table.Name, col.Name)
			}

			result.Append(ExprListItem{
				Expr: expr,
				Name: name,
			})
		}
	}

	return nil
}

// expandTableStar expands table.* to all columns from specific table.
func (rc *ResultCompiler) expandTableStar(sel *Select, tableExpr *Expr, result *ExprList) error {
	if tableExpr == nil || tableExpr.Op != TK_ID {
		return fmt.Errorf("invalid table reference in table.*")
	}
	srcItem := rc.findTableByName(sel, tableExpr.StringValue)
	if srcItem == nil {
		return fmt.Errorf("table %s not found in FROM clause", tableExpr.StringValue)
	}
	appendTableColumns(srcItem, result)
	return nil
}

// findTableByName finds a table in the FROM clause by name or alias.
func (rc *ResultCompiler) findTableByName(sel *Select, tableName string) *SrcListItem {
	if sel.Src == nil {
		return nil
	}
	for i := 0; i < sel.Src.Len(); i++ {
		item := sel.Src.Get(i)
		if item.Table != nil && item.Table.Name == tableName {
			return item
		}
		if item.Alias == tableName {
			return item
		}
	}
	return nil
}

// appendTableColumns adds all columns from a table to the result list.
func appendTableColumns(srcItem *SrcListItem, result *ExprList) {
	table := srcItem.Table
	for colIdx := 0; colIdx < table.NumColumns; colIdx++ {
		col := table.GetColumn(colIdx)
		expr := &Expr{Op: TK_COLUMN, Table: srcItem.Cursor, Column: colIdx}
		result.Append(ExprListItem{Expr: expr, Name: col.Name})
	}
}

// GenerateColumnNames generates column names for the result set.
// This implements the SQLite column naming rules.
func (rc *ResultCompiler) GenerateColumnNames(sel *Select) error {
	vdbe := rc.parse.GetVdbe()
	if vdbe == nil {
		return fmt.Errorf("no VDBE available")
	}

	if sel.EList == nil {
		return fmt.Errorf("nil expression list")
	}

	nCol := sel.EList.Len()
	vdbe.SetNumCols(nCol)

	for i := 0; i < nCol; i++ {
		item := sel.EList.Get(i)
		name := rc.computeColumnName(sel, item, i)

		// Set column name in VDBE
		vdbe.SetColName(i, name)

		// Set declared type if available
		if item.Expr != nil {
			declType := rc.computeDeclaredType(sel, item.Expr)
			if declType != "" {
				vdbe.SetColDeclType(i, declType)
			}
		}
	}

	return nil
}

// computeColumnName determines the column name for a result column.
func (rc *ResultCompiler) computeColumnName(sel *Select, item *ExprListItem, idx int) string {
	// If AS clause is present, use it
	if item.Name != "" && !item.BNoExpand {
		return item.Name
	}

	// If expression is a simple column reference, use column name
	if item.Expr != nil && item.Expr.Op == TK_COLUMN {
		if item.Expr.ColumnRef != nil {
			return item.Expr.ColumnRef.Name
		}
	}

	// If expression is an identifier, use it
	if item.Expr != nil && item.Expr.Op == TK_ID {
		return item.Expr.StringValue
	}

	// Default: generate column name
	return fmt.Sprintf("column%d", idx+1)
}

// computeDeclaredType determines the declared type for a result column.
func (rc *ResultCompiler) computeDeclaredType(sel *Select, expr *Expr) string {
	if expr == nil {
		return ""
	}

	switch expr.Op {
	case TK_COLUMN:
		// Get type from source table column
		if expr.ColumnRef != nil {
			return expr.ColumnRef.DeclType
		}
		return ""

	case TK_INTEGER:
		return "INTEGER"

	case TK_FLOAT:
		return "REAL"

	case TK_STRING, TK_BLOB:
		return "TEXT"

	case TK_NULL:
		return ""

	default:
		// For complex expressions, type inference would be needed
		return ""
	}
}

// ResolveResultColumns resolves column references in result expressions.
// This binds column names to actual table columns.
func (rc *ResultCompiler) ResolveResultColumns(sel *Select) error {
	if sel.EList == nil {
		return nil
	}

	for i := 0; i < sel.EList.Len(); i++ {
		item := sel.EList.Get(i)
		if item.Expr == nil {
			continue
		}

		if err := rc.resolveExprColumns(sel, item.Expr); err != nil {
			return fmt.Errorf("column %d: %w", i+1, err)
		}
	}

	return nil
}

// resolveExprColumns recursively resolves column references in an expression.
func (rc *ResultCompiler) resolveExprColumns(sel *Select, expr *Expr) error {
	if expr == nil {
		return nil
	}
	switch expr.Op {
	case TK_COLUMN:
		return rc.resolveColumnRef(sel, expr)
	case TK_DOT:
		return rc.resolveDotExpr(sel, expr)
	default:
		return rc.resolveChildExprs(sel, expr)
	}
}

// resolveDotExpr resolves a table.column qualified reference.
func (rc *ResultCompiler) resolveDotExpr(sel *Select, expr *Expr) error {
	if expr.Left != nil && expr.Right != nil {
		return rc.resolveQualifiedColumn(sel, expr)
	}
	return nil
}

// resolveChildExprs recursively resolves column references in child expressions.
func (rc *ResultCompiler) resolveChildExprs(sel *Select, expr *Expr) error {
	if expr.Left != nil {
		if err := rc.resolveExprColumns(sel, expr.Left); err != nil {
			return err
		}
	}
	if expr.Right != nil {
		if err := rc.resolveExprColumns(sel, expr.Right); err != nil {
			return err
		}
	}
	return nil
}

// resolveColumnRef resolves an unqualified column reference.
func (rc *ResultCompiler) resolveColumnRef(sel *Select, expr *Expr) error {
	if expr.Op != TK_COLUMN {
		return nil
	}

	// Column reference by name
	colName := expr.StringValue
	if colName == "" {
		return nil // Already resolved (has cursor/column index)
	}

	// Search for column in FROM clause tables
	if sel.Src != nil {
		for i := 0; i < sel.Src.Len(); i++ {
			srcItem := sel.Src.Get(i)
			if srcItem.Table == nil {
				continue
			}

			table := srcItem.Table
			for colIdx := 0; colIdx < table.NumColumns; colIdx++ {
				col := table.GetColumn(colIdx)
				if col.Name == colName {
					// Found it - bind to this column
					expr.Table = srcItem.Cursor
					expr.Column = colIdx
					expr.ColumnRef = col
					return nil
				}
			}
		}
	}

	return fmt.Errorf("no such column: %s", colName)
}

// resolveQualifiedColumn resolves a qualified column reference (table.column).
func (rc *ResultCompiler) resolveQualifiedColumn(sel *Select, expr *Expr) error {
	if expr.Op != TK_DOT {
		return nil
	}

	tableName, colName, err := extractQualifiedNames(expr)
	if err != nil {
		return err
	}

	srcItem := findTableInSrc(sel.Src, tableName)
	if srcItem == nil {
		return fmt.Errorf("no such table: %s", tableName)
	}

	col, colIdx := findColumnInTable(srcItem.Table, colName)
	if col == nil {
		return fmt.Errorf("no such column: %s.%s", tableName, colName)
	}

	expr.Op = TK_COLUMN
	expr.Table = srcItem.Cursor
	expr.Column = colIdx
	expr.ColumnRef = col
	expr.Left = nil
	expr.Right = nil
	return nil
}

func extractQualifiedNames(expr *Expr) (string, string, error) {
	if expr.Left == nil || expr.Left.Op != TK_ID {
		return "", "", fmt.Errorf("invalid table reference")
	}
	if expr.Right == nil || expr.Right.Op != TK_ID {
		return "", "", fmt.Errorf("invalid column reference")
	}
	return expr.Left.StringValue, expr.Right.StringValue, nil
}

func findTableInSrc(src *SrcList, tableName string) *SrcListItem {
	if src == nil {
		return nil
	}
	for i := 0; i < src.Len(); i++ {
		item := src.Get(i)
		if item.Table == nil {
			continue
		}
		if item.Table.Name == tableName || item.Alias == tableName {
			return item
		}
	}
	return nil
}

func findColumnInTable(table *Table, colName string) (*Column, int) {
	for colIdx := 0; colIdx < table.NumColumns; colIdx++ {
		col := table.GetColumn(colIdx)
		if col.Name == colName {
			return col, colIdx
		}
	}
	return nil, -1
}

// affinityFromOp maps expression operators to their affinities.
var affinityFromOp = map[int]Affinity{
	TK_INTEGER: SQLITE_AFF_INTEGER,
	TK_FLOAT:   SQLITE_AFF_REAL,
	TK_STRING:  SQLITE_AFF_TEXT,
	TK_BLOB:    SQLITE_AFF_BLOB,
	TK_NULL:    SQLITE_AFF_NONE,
}

// ComputeColumnAffinity determines the affinity of a result column.
func (rc *ResultCompiler) ComputeColumnAffinity(expr *Expr) Affinity {
	if expr == nil {
		return SQLITE_AFF_BLOB
	}
	if expr.Op == TK_COLUMN {
		return columnRefAffinity(expr)
	}
	if aff, ok := affinityFromOp[expr.Op]; ok {
		return aff
	}
	if expr.Left != nil {
		return rc.ComputeColumnAffinity(expr.Left)
	}
	return SQLITE_AFF_BLOB
}

// columnRefAffinity returns the affinity for a column reference expression.
func columnRefAffinity(expr *Expr) Affinity {
	if expr.ColumnRef != nil {
		return expr.ColumnRef.Affinity
	}
	return SQLITE_AFF_BLOB
}
