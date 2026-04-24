// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ExplainNode represents a node in the query plan tree.
// This mirrors SQLite's EXPLAIN QUERY PLAN output format.
type ExplainNode struct {
	ID            int
	Parent        int
	NotUsed       int
	Detail        string
	Children      []*ExplainNode
	Level         int
	EstimatedRows int64
	EstimatedCost float64
}

// ExplainPlan represents a complete query execution plan.
type ExplainPlan struct {
	Roots  []*ExplainNode
	nextID int
}

// NewExplainPlan creates a new empty explain plan.
func NewExplainPlan() *ExplainPlan {
	return &ExplainPlan{Roots: make([]*ExplainNode, 0)}
}

// AddNode adds a new node to the plan and returns it.
func (p *ExplainPlan) AddNode(parent *ExplainNode, detail string) *ExplainNode {
	node := &ExplainNode{
		ID:       p.nextID,
		Parent:   -1,
		Detail:   detail,
		Children: make([]*ExplainNode, 0),
	}
	p.nextID++

	if parent == nil {
		p.Roots = append(p.Roots, node)
	} else {
		node.Parent = parent.ID
		node.Level = parent.Level + 1
		parent.Children = append(parent.Children, node)
	}
	return node
}

// AddNodeWithCost adds a new node with cost estimates to the plan and returns it.
func (p *ExplainPlan) AddNodeWithCost(parent *ExplainNode, detail string, rows int64, cost float64) *ExplainNode {
	node := p.AddNode(parent, detail)
	node.EstimatedRows = rows
	node.EstimatedCost = cost
	return node
}

// FormatAsTable formats the explain plan in SQLite's table format.
// Output format: id | parent | notused | detail
func (p *ExplainPlan) FormatAsTable() [][]interface{} {
	rows := make([][]interface{}, 0)
	var traverse func(*ExplainNode)
	traverse = func(node *ExplainNode) {
		detail := strings.Repeat("  ", node.Level) + node.Detail
		rows = append(rows, []interface{}{node.ID, node.Parent, node.NotUsed, detail})
		for _, child := range node.Children {
			traverse(child)
		}
	}
	for _, root := range p.Roots {
		traverse(root)
	}
	return rows
}

// FormatAsText formats the explain plan as human-readable text.
func (p *ExplainPlan) FormatAsText() string {
	var sb strings.Builder
	var traverse func(*ExplainNode)
	traverse = func(node *ExplainNode) {
		sb.WriteString(strings.Repeat("  ", node.Level))
		sb.WriteString(node.Detail)
		sb.WriteByte('\n')
		for _, child := range node.Children {
			traverse(child)
		}
	}
	for _, root := range p.Roots {
		traverse(root)
	}
	return sb.String()
}

// explainCtx holds context for plan generation.
type explainCtx struct {
	plan      *ExplainPlan
	schema    *schema.Schema
	subqCount int
}

// GenerateExplain generates an explain plan for a SQL statement.
func GenerateExplain(stmt parser.Statement) (*ExplainPlan, error) {
	return GenerateExplainWithSchema(stmt, nil)
}

// GenerateExplainWithSchema generates an explain plan with schema info.
func GenerateExplainWithSchema(stmt parser.Statement, sch *schema.Schema) (*ExplainPlan, error) {
	ctx := &explainCtx{plan: NewExplainPlan(), schema: sch}
	return ctx.generate(stmt)
}

func (c *explainCtx) generate(stmt parser.Statement) (*ExplainPlan, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		c.explainSelect(nil, s)
		return c.plan, nil
	case *parser.InsertStmt:
		c.explainInsert(s)
		return c.plan, nil
	case *parser.UpdateStmt:
		c.explainUpdate(s)
		return c.plan, nil
	case *parser.DeleteStmt:
		c.explainDelete(s)
		return c.plan, nil
	default:
		return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
	}
}

// explainSelect generates EXPLAIN nodes for a SELECT statement.
func (c *explainCtx) explainSelect(parent *ExplainNode, stmt *parser.SelectStmt) {
	// Handle compound SELECT (UNION, INTERSECT, EXCEPT)
	if stmt.Compound != nil {
		c.explainCompound(parent, stmt)
		return
	}
	c.explainSingleSelect(parent, stmt)
}

// explainCompound generates EXPLAIN for UNION/INTERSECT/EXCEPT.
func (c *explainCtx) explainCompound(parent *ExplainNode, stmt *parser.SelectStmt) {
	c.subqCount++
	n := c.subqCount
	opName := stmt.Compound.Op.String()
	root := c.plan.AddNode(parent, fmt.Sprintf("COMPOUND SUBQUERY %d (%s)", n, opName))
	c.explainSelect(root, stmt.Compound.Left)
	c.explainSelect(root, stmt.Compound.Right)
}

// explainSingleSelect handles a non-compound SELECT.
func (c *explainCtx) explainSingleSelect(parent *ExplainNode, stmt *parser.SelectStmt) {
	// Determine the main table and add scan/search node
	tableName := extractMainTableName(stmt)

	// Check for FROM-clause subqueries
	if hasFromSubqueries(stmt) {
		c.explainFromSubqueries(parent, stmt)
		return
	}

	// Emit scan node for the main table
	scanNode := c.emitScanNode(parent, tableName, stmt.Where)

	// Emit join nodes
	if stmt.From != nil {
		for _, join := range stmt.From.Joins {
			c.emitJoinScanNode(scanNode, join)
		}
	}

	// Check for WHERE subqueries (correlated scalar subqueries)
	c.checkWhereSubqueries(scanNode, stmt.Where)

	// Emit GROUP BY (explicit or implicit via aggregate functions)
	if len(stmt.GroupBy) > 0 || detectAggregates(stmt) {
		c.plan.AddNode(scanNode, "USE TEMP B-TREE FOR GROUP BY")
	}

	// Emit DISTINCT
	if stmt.Distinct {
		c.plan.AddNode(scanNode, "USE TEMP B-TREE FOR DISTINCT")
	}

	// Emit ORDER BY
	if len(stmt.OrderBy) > 0 {
		c.plan.AddNode(scanNode, "USE TEMP B-TREE FOR ORDER BY")
	}
}

// emitScanNode emits a SCAN TABLE or SEARCH TABLE node.
func (c *explainCtx) emitScanNode(parent *ExplainNode, tableName string, where parser.Expression) *ExplainNode {
	if tableName == "" {
		return parent
	}
	detail := c.formatScanDetail(tableName, where)
	return c.plan.AddNode(parent, detail)
}

// emitJoinScanNode emits a scan node for a joined table.
func (c *explainCtx) emitJoinScanNode(parent *ExplainNode, join parser.JoinClause) {
	joinTableName := join.Table.TableName
	if join.Table.Subquery != nil {
		c.subqCount++
		subNode := c.plan.AddNode(parent, fmt.Sprintf("SUBQUERY %d", c.subqCount))
		c.explainSelect(subNode, join.Table.Subquery)
		return
	}
	var joinWhere parser.Expression
	if join.Condition.On != nil {
		joinWhere = join.Condition.On
	}
	c.emitScanNode(parent, joinTableName, joinWhere)
}

// formatScanDetail produces the SCAN/SEARCH detail string for a table.
func (c *explainCtx) formatScanDetail(tableName string, where parser.Expression) string {
	if where == nil {
		return fmt.Sprintf("SCAN TABLE %s", tableName)
	}

	// Check for rowid lookup
	if colName, isRowid := c.isRowidLookup(tableName, where); isRowid {
		return fmt.Sprintf("SEARCH TABLE %s USING INTEGER PRIMARY KEY (rowid=?)", tableName)
	} else if colName != "" {
		// Try to find an index
		if idx := c.findIndexForColumn(tableName, colName); idx != nil {
			return c.formatIndexDetail(tableName, idx, colName, where)
		}
	}

	return fmt.Sprintf("SCAN TABLE %s", tableName)
}

// isRowidAlias reports whether name is one of the built-in rowid aliases.
func isRowidAlias(name string) bool {
	lower := strings.ToLower(name)
	return lower == "rowid" || lower == "oid" || lower == "_rowid_"
}

// isRowidLookup checks if the WHERE clause is a rowid/INTEGER PRIMARY KEY lookup.
func (c *explainCtx) isRowidLookup(tableName string, where parser.Expression) (string, bool) {
	colName := extractEqColumnName(where)
	if colName == "" {
		return "", false
	}

	return colName, isRowidAlias(colName) || c.isIntegerPrimaryKeyAlias(tableName, colName)
}

func (c *explainCtx) isIntegerPrimaryKeyAlias(tableName, colName string) bool {
	if c.schema == nil {
		return false
	}
	tbl, ok := c.schema.GetTable(tableName)
	if !ok {
		return false
	}
	for _, col := range tbl.Columns {
		if strings.EqualFold(col.Name, colName) && col.PrimaryKey && isIntegerAffinity(col.Type) {
			return true
		}
	}
	return false
}

func isIntegerAffinity(colType string) bool {
	upper := strings.ToUpper(colType)
	return upper == "INTEGER" || upper == "INT"
}

// findIndexForColumn looks up the best index for a column in the schema.
func (c *explainCtx) findIndexForColumn(tableName, colName string) *schema.Index {
	if c.schema == nil {
		return nil
	}
	indexes := c.schema.GetTableIndexes(tableName)
	for _, idx := range indexes {
		if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], colName) {
			return idx
		}
	}
	return nil
}

// formatIndexDetail formats an index usage detail string.
func (c *explainCtx) formatIndexDetail(tableName string, idx *schema.Index, colName string, where parser.Expression) string {
	isEq := isEqualityWhere(where)
	if isEq {
		return fmt.Sprintf("SEARCH TABLE %s USING INDEX %s (%s=?)", tableName, idx.Name, colName)
	}
	return fmt.Sprintf("SEARCH TABLE %s USING INDEX %s (%s>?)", tableName, idx.Name, colName)
}

// explainFromSubqueries handles FROM-clause subqueries.
func (c *explainCtx) explainFromSubqueries(parent *ExplainNode, stmt *parser.SelectStmt) {
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			c.subqCount++
			subNode := c.plan.AddNode(parent, fmt.Sprintf("SUBQUERY %d", c.subqCount))
			c.explainSelect(subNode, table.Subquery)
		} else if table.TableName != "" {
			c.emitScanNode(parent, table.TableName, stmt.Where)
		}
	}
}

// checkWhereSubqueries checks for subqueries in WHERE clause.
func (c *explainCtx) checkWhereSubqueries(parent *ExplainNode, where parser.Expression) {
	if where == nil || parent == nil {
		return
	}
	c.walkExprForSubqueries(parent, where)
}

// walkExprForSubqueries walks an expression tree looking for subqueries.
func (c *explainCtx) walkExprForSubqueries(parent *ExplainNode, expr parser.Expression) {
	if expr == nil {
		return
	}
	if sel := subqueryExprSelect(expr); sel != nil {
		c.emitCorrelatedScalarSubquery(parent, sel)
		return
	}
	for _, child := range subqueryChildExprs(expr) {
		c.walkExprForSubqueries(parent, child)
	}
}

func (c *explainCtx) emitCorrelatedScalarSubquery(parent *ExplainNode, sel *parser.SelectStmt) {
	c.subqCount++
	sub := c.plan.AddNode(parent, fmt.Sprintf("CORRELATED SCALAR SUBQUERY %d", c.subqCount))
	c.explainSelect(sub, sel)
}

func subqueryExprSelect(expr parser.Expression) *parser.SelectStmt {
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		return e.Select
	case *parser.ExistsExpr:
		return e.Select
	case *parser.InExpr:
		return e.Select
	default:
		return nil
	}
}

func subqueryChildExprs(expr parser.Expression) []parser.Expression {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return []parser.Expression{e.Left, e.Right}
	case *parser.UnaryExpr:
		return []parser.Expression{e.Expr}
	case *parser.ParenExpr:
		return []parser.Expression{e.Expr}
	default:
		return nil
	}
}

// explainInsert generates EXPLAIN nodes for INSERT.
func (c *explainCtx) explainInsert(stmt *parser.InsertStmt) {
	if stmt.Select != nil {
		scanNode := c.plan.AddNode(nil, fmt.Sprintf("INSERT INTO %s", stmt.Table))
		c.explainSelect(scanNode, stmt.Select)
		return
	}
	c.plan.AddNode(nil, fmt.Sprintf("INSERT INTO %s", stmt.Table))
}

// explainUpdate generates EXPLAIN nodes for UPDATE.
func (c *explainCtx) explainUpdate(stmt *parser.UpdateStmt) {
	root := c.plan.AddNode(nil, fmt.Sprintf("UPDATE %s", stmt.Table))
	detail := c.formatScanDetail(stmt.Table, stmt.Where)
	c.plan.AddNode(root, detail)
}

// explainDelete generates EXPLAIN nodes for DELETE.
func (c *explainCtx) explainDelete(stmt *parser.DeleteStmt) {
	root := c.plan.AddNode(nil, fmt.Sprintf("DELETE FROM %s", stmt.Table))
	detail := c.formatScanDetail(stmt.Table, stmt.Where)
	c.plan.AddNode(root, detail)
}

// extractEqColumnName extracts the column name from a simple equality/comparison WHERE clause.
func extractEqColumnName(where parser.Expression) string {
	binExpr, ok := where.(*parser.BinaryExpr)
	if !ok {
		return ""
	}
	if ident, ok := binExpr.Left.(*parser.IdentExpr); ok {
		if isIndexableOp(binExpr.Op) {
			return ident.Name
		}
	}
	return ""
}

// isEqualityWhere checks if the WHERE clause is an equality comparison.
func isEqualityWhere(where parser.Expression) bool {
	if binExpr, ok := where.(*parser.BinaryExpr); ok {
		return binExpr.Op == parser.OpEq
	}
	return false
}

// isIndexableOp checks if an operator can use an index.
func isIndexableOp(op parser.BinaryOp) bool {
	return op == parser.OpEq || op == parser.OpLt ||
		op == parser.OpGt || op == parser.OpLe ||
		op == parser.OpGe
}

// extractMainTableName extracts the main table name from a SELECT statement.
func extractMainTableName(stmt *parser.SelectStmt) string {
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		tableName := stmt.From.Tables[0].TableName
		if tableName == "" && stmt.From.Tables[0].Subquery != nil {
			return "subquery"
		}
		return tableName
	}
	return ""
}

// hasFromSubqueries checks if a SELECT has subqueries in the FROM clause.
func hasFromSubqueries(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			return true
		}
	}
	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			return true
		}
	}
	return false
}

// =====================================================================
// Index candidate analysis (used by other planner components)
// =====================================================================

// IndexCandidate represents a potential index that could be used for a query.
type IndexCandidate struct {
	IndexName     string
	TableName     string
	Columns       []string
	IsUnique      bool
	IsCovering    bool
	HasEquality   bool
	EstimatedRows int64
	EstimatedCost float64
}

// findBestIndex analyzes the WHERE clause and available indexes.
func findBestIndex(tableName string, where parser.Expression, sch *schema.Schema) *IndexCandidate {
	return findBestIndexWithColumns(tableName, where, sch, nil)
}

// findBestIndexWithColumns analyzes WHERE and indexes with covering index detection.
func findBestIndexWithColumns(tableName string, where parser.Expression, sch *schema.Schema, neededColumns []string) *IndexCandidate {
	if sch == nil || where == nil {
		return nil
	}
	indexes := sch.GetTableIndexes(tableName)
	colName := extractEqColumnName(where)
	if colName == "" {
		return nil
	}
	return selectBestIndex(tableName, where, indexes, colName, neededColumns)
}

// selectBestIndex finds the best index from available indexes for the given column.
func selectBestIndex(tableName string, where parser.Expression, indexes []*schema.Index, colName string, neededColumns []string) *IndexCandidate {
	var best *IndexCandidate
	estimator := NewCostEstimator()
	for _, idx := range indexes {
		if len(idx.Columns) == 0 || !strings.EqualFold(idx.Columns[0], colName) {
			continue
		}
		hasEq := isEqualityWhere(where)
		covering := isIndexCoveringCols(idx, neededColumns)
		rows, cost := estimator.EstimateIndexScan(idx.Name, idx.Unique, covering, hasEq)
		cand := &IndexCandidate{
			IndexName: idx.Name, TableName: tableName, Columns: idx.Columns,
			IsUnique: idx.Unique, IsCovering: covering, HasEquality: hasEq,
			EstimatedRows: rows, EstimatedCost: cost,
		}
		if best == nil || cand.EstimatedCost < best.EstimatedCost {
			best = cand
		}
	}
	return best
}

// isIndexCoveringCols checks if an index covers all needed columns.
func isIndexCoveringCols(idx *schema.Index, neededColumns []string) bool {
	if len(neededColumns) == 0 {
		return false
	}
	m := make(map[string]bool)
	for _, col := range idx.Columns {
		m[strings.ToLower(col)] = true
	}
	for _, needed := range neededColumns {
		lower := strings.ToLower(needed)
		if lower != "rowid" && lower != "oid" && lower != "_rowid_" && !m[lower] {
			return false
		}
	}
	return true
}

// formatIndexScan formats an index scan description.
func formatIndexScan(candidate *IndexCandidate) string {
	if candidate.IsUnique && candidate.HasEquality {
		return fmt.Sprintf("SEARCH TABLE %s USING INDEX %s (%s=?)",
			candidate.TableName, candidate.IndexName, candidate.Columns[0])
	}
	if candidate.IsCovering {
		return fmt.Sprintf("SCAN TABLE %s USING COVERING INDEX %s",
			candidate.TableName, candidate.IndexName)
	}
	op := "SEARCH"
	if !candidate.HasEquality {
		op = "SCAN"
	}
	return fmt.Sprintf("%s TABLE %s USING INDEX %s", op, candidate.TableName, candidate.IndexName)
}

// detectAggregates checks if a SELECT contains aggregate functions.
func detectAggregates(stmt *parser.SelectStmt) bool {
	for _, col := range stmt.Columns {
		if isAggregateExpr(col.Expr) {
			return true
		}
	}
	return false
}

// isAggregateExpr checks if an expression is an aggregate function.
func isAggregateExpr(expr parser.Expression) bool {
	if expr == nil {
		return false
	}
	fnExpr, ok := expr.(*parser.FunctionExpr)
	if !ok {
		return false
	}
	aggFuncs := map[string]bool{
		"COUNT": true, "SUM": true, "AVG": true,
		"MIN": true, "MAX": true, "TOTAL": true,
		"GROUP_CONCAT": true,
	}
	return aggFuncs[fnExpr.Name]
}

// =====================================================================
// CostEstimator (used by planner and explain)
// =====================================================================

// CostEstimator provides cost estimation for query operations.
type CostEstimator struct {
	defaultTableRows int64
	defaultIndexRows int64
	schemaInfo       *schema.Schema
}

// NewCostEstimator creates a new cost estimator with default values.
func NewCostEstimator() *CostEstimator {
	return &CostEstimator{
		defaultTableRows: 1000000,
		defaultIndexRows: 100000,
	}
}

// NewCostEstimatorWithSchema creates a cost estimator with schema info.
func NewCostEstimatorWithSchema(sch *schema.Schema) *CostEstimator {
	ce := NewCostEstimator()
	ce.schemaInfo = sch
	return ce
}

// getTableRowCount returns the row count for a table from statistics or defaults.
func (ce *CostEstimator) getTableRowCount(tableName string) int64 {
	if ce.schemaInfo != nil {
		if table, ok := ce.schemaInfo.GetTable(tableName); ok {
			if stats := table.GetTableStats(); stats != nil && stats.RowCount > 0 {
				return stats.RowCount
			}
		}
	}
	return ce.defaultTableRows
}

// EstimateTableScan estimates the cost of a full table scan.
func (ce *CostEstimator) EstimateTableScan(tableName string, hasWhere bool) (int64, float64) {
	totalRows := ce.getTableRowCount(tableName)
	rows := totalRows
	cost := float64(totalRows)
	if hasWhere {
		rows = totalRows / 10
	}
	return rows, cost
}

// EstimateIndexScan estimates the cost of an index scan.
func (ce *CostEstimator) EstimateIndexScan(indexName string, isUnique, isCovering, hasEquality bool) (int64, float64) {
	if isUnique && hasEquality {
		return 1, 10.0
	}
	var rows int64
	var cost float64
	if hasEquality {
		rows = ce.defaultIndexRows / 100
		cost = float64(rows) * 0.5
	} else {
		rows = ce.defaultIndexRows / 10
		cost = float64(rows) * 0.7
	}
	if isCovering {
		cost *= 0.8
	}
	return rows, cost
}

// EstimateJoinCost estimates the cost of joining two tables.
func (ce *CostEstimator) EstimateJoinCost(leftRows, rightRows int64, joinType parser.JoinType, hasIndex bool) (int64, float64) {
	var rows int64
	switch joinType {
	case parser.JoinCross:
		rows = leftRows * rightRows
	case parser.JoinInner:
		rows = (leftRows * rightRows) / 10
	case parser.JoinLeft:
		rows = leftRows
	case parser.JoinRight:
		rows = rightRows
	case parser.JoinFull:
		rows = leftRows + rightRows
	default:
		rows = leftRows
	}
	var cost float64
	if hasIndex {
		cost = float64(leftRows) * (10.0 + float64(rightRows)*0.01)
	} else {
		cost = float64(leftRows) * float64(rightRows)
	}
	return rows, cost
}

// EstimateAggregateCost estimates the cost of aggregation.
func (ce *CostEstimator) EstimateAggregateCost(inputRows, numGroups int64) (int64, float64) {
	if numGroups == 0 {
		numGroups = 1000
		if inputRows > 10000 {
			numGroups = inputRows / 100
		}
	}
	return numGroups, float64(inputRows) * 1.2
}

// EstimateSortCost estimates the cost of sorting.
func (ce *CostEstimator) EstimateSortCost(inputRows int64) (int64, float64) {
	if inputRows <= 0 {
		return 0, 0
	}
	return inputRows, float64(inputRows) * 1.5
}

// =====================================================================
// Backward-compatible functions used by existing tests
// =====================================================================

// formatJoinType returns the string representation of a join type.
func formatJoinType(joinType parser.JoinType) string {
	switch joinType {
	case parser.JoinLeft:
		return "LEFT JOIN"
	case parser.JoinRight:
		return "RIGHT JOIN"
	case parser.JoinFull:
		return "FULL JOIN"
	case parser.JoinCross:
		return "CROSS JOIN"
	default:
		return "INNER JOIN"
	}
}

// formatTableScan formats a table scan description (backward-compat, without TABLE keyword).
func formatTableScan(tableName string, where parser.Expression, isWrite bool) string {
	if tableName == "" {
		tableName = "?"
	}
	if where == nil {
		return fmt.Sprintf("SCAN %s", tableName)
	}
	return formatTableScanWithWhere(tableName, where)
}

// formatTableScanWithWhere formats a table scan description when WHERE clause exists.
func formatTableScanWithWhere(tableName string, where parser.Expression) string {
	colName := extractEqColumnName(where)
	if colName != "" {
		return fmt.Sprintf("SEARCH %s USING INDEX (POSSIBLE %s)", tableName, colName)
	}
	return fmt.Sprintf("SCAN %s", tableName)
}

// analyzeIndexability determines if a WHERE expression is indexable.
func analyzeIndexability(where parser.Expression) (bool, string) {
	binExpr, ok := where.(*parser.BinaryExpr)
	if !ok {
		return false, ""
	}
	ident, ok := binExpr.Left.(*parser.IdentExpr)
	if !ok {
		return false, ""
	}
	indexable := isIndexableOp(binExpr.Op)
	return indexable, ident.Name
}

// isIndexCovering determines if an index covers all needed columns.
func isIndexCovering(idx *schema.Index, neededColumns []string) bool {
	return isIndexCoveringCols(idx, neededColumns)
}

// mergeSubplan merges a subplan tree into the parent plan (used externally).
func mergeSubplan(parent *ExplainNode, subRoot *ExplainNode, plan *ExplainPlan) {
	newNode := &ExplainNode{
		ID:       plan.nextID,
		Parent:   parent.ID,
		Detail:   subRoot.Detail,
		Children: make([]*ExplainNode, 0),
		Level:    parent.Level + 1,
	}
	plan.nextID++
	parent.Children = append(parent.Children, newNode)
	for _, child := range subRoot.Children {
		mergeSubplan(newNode, child, plan)
	}
}
