// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// ExplainNode represents a node in the query plan tree.
// This mirrors SQLite's EXPLAIN QUERY PLAN output format.
type ExplainNode struct {
	// ID is the node identifier (unique within the plan)
	ID int

	// Parent is the ID of the parent node (-1 for root nodes)
	Parent int

	// NotUsed is reserved for compatibility (always 0)
	NotUsed int

	// Detail is the textual description of this plan step
	Detail string

	// Children are the child nodes in the plan tree
	Children []*ExplainNode

	// Level is the nesting level (0 for root, 1 for children, etc.)
	Level int

	// Cost estimates (for enhanced EXPLAIN QUERY PLAN)
	EstimatedRows int64   // Estimated number of rows this operation will produce
	EstimatedCost float64 // Relative cost estimate (higher = more expensive)
}

// ExplainPlan represents a complete query execution plan.
type ExplainPlan struct {
	// Roots are the top-level nodes in the plan
	Roots []*ExplainNode

	// nextID is used for generating unique node IDs
	nextID int
}

// NewExplainPlan creates a new empty explain plan.
func NewExplainPlan() *ExplainPlan {
	return &ExplainPlan{
		Roots:  make([]*ExplainNode, 0),
		nextID: 0,
	}
}

// AddNode adds a new node to the plan and returns it.
func (p *ExplainPlan) AddNode(parent *ExplainNode, detail string) *ExplainNode {
	node := &ExplainNode{
		ID:            p.nextID,
		Parent:        -1,
		NotUsed:       0,
		Detail:        detail,
		Children:      make([]*ExplainNode, 0),
		Level:         0,
		EstimatedRows: 0,
		EstimatedCost: 0,
	}
	p.nextID++

	if parent == nil {
		// Root node
		p.Roots = append(p.Roots, node)
	} else {
		// Child node
		node.Parent = parent.ID
		node.Level = parent.Level + 1
		parent.Children = append(parent.Children, node)
	}

	return node
}

// AddNodeWithCost adds a new node with cost estimates to the plan and returns it.
func (p *ExplainPlan) AddNodeWithCost(parent *ExplainNode, detail string, estimatedRows int64, estimatedCost float64) *ExplainNode {
	node := p.AddNode(parent, detail)
	node.EstimatedRows = estimatedRows
	node.EstimatedCost = estimatedCost
	return node
}

// FormatAsTable formats the explain plan in SQLite's table format.
// Output format: id | parent | notused | detail
func (p *ExplainPlan) FormatAsTable() [][]interface{} {
	rows := make([][]interface{}, 0)

	// Traverse the tree in depth-first order
	var traverse func(*ExplainNode)
	traverse = func(node *ExplainNode) {
		// Add indentation to detail based on level
		detail := strings.Repeat("  ", node.Level) + node.Detail

		row := []interface{}{
			node.ID,
			node.Parent,
			node.NotUsed,
			detail,
		}
		rows = append(rows, row)

		// Recursively traverse children
		for _, child := range node.Children {
			traverse(child)
		}
	}

	// Process all root nodes
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
		// Add indentation based on level
		indent := strings.Repeat("  ", node.Level)
		sb.WriteString(fmt.Sprintf("%s%s\n", indent, node.Detail))

		// Recursively traverse children
		for _, child := range node.Children {
			traverse(child)
		}
	}

	// Process all root nodes
	for _, root := range p.Roots {
		traverse(root)
	}

	return sb.String()
}

// GenerateExplain generates an explain plan for a SQL statement.
// This is the main entry point for EXPLAIN QUERY PLAN functionality.
func GenerateExplain(stmt parser.Statement) (*ExplainPlan, error) {
	return GenerateExplainWithSchema(stmt, nil)
}

// GenerateExplainWithSchema generates an explain plan with schema information for better cost estimates.
func GenerateExplainWithSchema(stmt parser.Statement, schemaInfo *schema.Schema) (*ExplainPlan, error) {
	plan := NewExplainPlan()

	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return generateExplainSelect(plan, s)
	case *parser.InsertStmt:
		return generateExplainInsert(plan, s)
	case *parser.UpdateStmt:
		return generateExplainUpdate(plan, s)
	case *parser.DeleteStmt:
		return generateExplainDelete(plan, s)
	default:
		return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
	}
}

// generateExplainSelect generates an explain plan for a SELECT statement.
func generateExplainSelect(plan *ExplainPlan, stmt *parser.SelectStmt) (*ExplainPlan, error) {
	features := analyzeSelectFeatures(stmt)
	tableName := extractMainTableName(stmt)

	if features.hasSubqueries {
		return generateExplainWithSubqueries(plan, stmt)
	}

	if features.hasJoins {
		return generateExplainWithJoins(plan, stmt, tableName)
	}

	return generateExplainSimpleSelect(plan, stmt, tableName, features)
}

// selectFeatures represents features detected in a SELECT statement.
type selectFeatures struct {
	hasJoins      bool
	hasSubqueries bool
	hasAggregates bool
	hasOrderBy    bool
}

// analyzeSelectFeatures analyzes a SELECT statement for various features.
func analyzeSelectFeatures(stmt *parser.SelectStmt) selectFeatures {
	return selectFeatures{
		hasJoins:      stmt.From != nil && len(stmt.From.Joins) > 0,
		hasSubqueries: hasFromSubqueries(stmt),
		hasAggregates: detectAggregates(stmt),
		hasOrderBy:    len(stmt.OrderBy) > 0,
	}
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

// generateExplainWithSubqueries handles EXPLAIN for queries with subqueries.
func generateExplainWithSubqueries(plan *ExplainPlan, stmt *parser.SelectStmt) (*ExplainPlan, error) {
	root := plan.AddNode(nil, "COMPOUND QUERY")
	for i, table := range stmt.From.Tables {
		if table.Subquery != nil {
			subNode := plan.AddNode(root, fmt.Sprintf("SUBQUERY %d", i+1))
			subPlan, err := generateExplainSelect(NewExplainPlan(), table.Subquery)
			if err == nil && len(subPlan.Roots) > 0 {
				for _, subRoot := range subPlan.Roots {
					mergeSubplan(subNode, subRoot, plan)
				}
			}
		}
	}
	return plan, nil
}

// generateExplainWithJoins handles EXPLAIN for queries with JOINs.
func generateExplainWithJoins(plan *ExplainPlan, stmt *parser.SelectStmt, tableName string) (*ExplainPlan, error) {
	estimator := NewCostEstimator()
	root := plan.AddNode(nil, "QUERY PLAN")

	// Estimate main table scan
	leftRows, leftCost := estimator.EstimateTableScan(tableName, stmt.Where != nil)
	scanDetail := formatTableScanWithCost(tableName, stmt.Where, false, leftRows, leftCost)
	mainScan := plan.AddNodeWithCost(root, scanDetail, leftRows, leftCost)

	currentRows := leftRows
	totalCost := leftCost

	for _, join := range stmt.From.Joins {
		// Estimate right table scan
		rightRows, rightCost := estimator.EstimateTableScan(join.Table.TableName, join.Condition.On != nil)

		// Estimate join cost
		hasIndex := false // Simplified: assume no index
		joinRows, joinCost := estimator.EstimateJoinCost(currentRows, rightRows, join.Type, hasIndex)

		joinType := formatJoinType(join.Type)
		joinDetail := fmt.Sprintf("%s (cost=%.2f rows=%d)", joinType, joinCost, joinRows)
		joinNode := plan.AddNodeWithCost(mainScan, joinDetail, joinRows, joinCost)

		scanDetail := formatTableScanWithCost(join.Table.TableName, join.Condition.On, false, rightRows, rightCost)
		plan.AddNodeWithCost(joinNode, scanDetail, rightRows, rightCost)

		currentRows = joinRows
		totalCost += joinCost + rightCost
	}

	return plan, nil
}

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

// generateExplainSimpleSelect handles EXPLAIN for simple SELECT queries.
func generateExplainSimpleSelect(plan *ExplainPlan, stmt *parser.SelectStmt, tableName string, features selectFeatures) (*ExplainPlan, error) {
	estimator := NewCostEstimator()

	root := plan.AddNode(nil, "QUERY PLAN")

	// Estimate table scan cost
	estimatedRows, estimatedCost := estimator.EstimateTableScan(tableName, stmt.Where != nil)
	scanDetail := formatTableScanWithCost(tableName, stmt.Where, false, estimatedRows, estimatedCost)
	scanNode := plan.AddNodeWithCost(root, scanDetail, estimatedRows, estimatedCost)

	if features.hasAggregates {
		aggRows, aggCost := estimator.EstimateAggregateCost(estimatedRows, 0)
		aggDetail := fmt.Sprintf("USE TEMP B-TREE FOR GROUP BY (cost=%.2f rows=%d)", aggCost, aggRows)
		plan.AddNodeWithCost(scanNode, aggDetail, aggRows, aggCost)
		estimatedRows = aggRows
		estimatedCost += aggCost
	}

	if features.hasOrderBy {
		addOrderByNodeWithCost(plan, scanNode, stmt.OrderBy, estimatedRows, estimator)
	}

	return plan, nil
}

// addOrderByNode adds an ORDER BY node to the explain plan.
// SCAFFOLDING: Simplified version without cost estimates, for basic EXPLAIN.
func addOrderByNode(plan *ExplainPlan, parent *ExplainNode, orderBy []parser.OrderingTerm) {
	var orderCols []string
	for _, term := range orderBy {
		if ident, ok := term.Expr.(*parser.IdentExpr); ok {
			orderCols = append(orderCols, ident.Name)
		}
	}
	if len(orderCols) > 0 {
		sortDetail := fmt.Sprintf("USE TEMP B-TREE FOR ORDER BY (%s)", strings.Join(orderCols, ", "))
		plan.AddNode(parent, sortDetail)
	}
}

// addOrderByNodeWithCost adds an ORDER BY node with cost estimates to the explain plan.
func addOrderByNodeWithCost(plan *ExplainPlan, parent *ExplainNode, orderBy []parser.OrderingTerm, inputRows int64, estimator *CostEstimator) {
	var orderCols []string
	for _, term := range orderBy {
		if ident, ok := term.Expr.(*parser.IdentExpr); ok {
			orderCols = append(orderCols, ident.Name)
		}
	}
	if len(orderCols) > 0 {
		sortRows, sortCost := estimator.EstimateSortCost(inputRows)
		sortDetail := fmt.Sprintf("USE TEMP B-TREE FOR ORDER BY (%s) (cost=%.2f rows=%d)",
			strings.Join(orderCols, ", "), sortCost, sortRows)
		plan.AddNodeWithCost(parent, sortDetail, sortRows, sortCost)
	}
}

// generateExplainInsert generates an explain plan for an INSERT statement.
func generateExplainInsert(plan *ExplainPlan, stmt *parser.InsertStmt) (*ExplainPlan, error) {
	_ = NewCostEstimator() // Reserved for future cost estimation
	root := plan.AddNode(nil, "QUERY PLAN")

	// Estimate insert cost
	numRows := int64(1) // Single row insert by default
	if stmt.Select != nil {
		numRows = 1000 // Estimate for INSERT...SELECT
	} else if len(stmt.Values) > 0 {
		numRows = int64(len(stmt.Values))
	}

	insertCost := float64(numRows) * 2.0 // Cost per row insert
	detail := fmt.Sprintf("INSERT INTO %s (cost=%.2f rows=%d)", stmt.Table, insertCost, numRows)
	insertNode := plan.AddNodeWithCost(root, detail, numRows, insertCost)

	// Check if INSERT...SELECT
	if stmt.Select != nil {
		selectPlan, err := generateExplainSelect(NewExplainPlan(), stmt.Select)
		if err == nil && len(selectPlan.Roots) > 0 {
			for _, subRoot := range selectPlan.Roots {
				mergeSubplan(insertNode, subRoot, plan)
			}
		}
	}

	return plan, nil
}

// generateExplainUpdate generates an explain plan for an UPDATE statement.
func generateExplainUpdate(plan *ExplainPlan, stmt *parser.UpdateStmt) (*ExplainPlan, error) {
	estimator := NewCostEstimator()
	root := plan.AddNode(nil, "QUERY PLAN")

	// Estimate scan cost
	scanRows, scanCost := estimator.EstimateTableScan(stmt.Table, stmt.Where != nil)

	// Estimate update cost (scan + write)
	updateCost := scanCost + float64(scanRows)*1.5 // Additional cost for writing
	detail := fmt.Sprintf("UPDATE %s (cost=%.2f rows=%d)", stmt.Table, updateCost, scanRows)
	updateNode := plan.AddNodeWithCost(root, detail, scanRows, updateCost)

	// Add scan information
	scanDetail := formatTableScanWithCost(stmt.Table, stmt.Where, true, scanRows, scanCost)
	plan.AddNodeWithCost(updateNode, scanDetail, scanRows, scanCost)

	return plan, nil
}

// generateExplainDelete generates an explain plan for a DELETE statement.
func generateExplainDelete(plan *ExplainPlan, stmt *parser.DeleteStmt) (*ExplainPlan, error) {
	estimator := NewCostEstimator()
	root := plan.AddNode(nil, "QUERY PLAN")

	// Estimate scan cost
	scanRows, scanCost := estimator.EstimateTableScan(stmt.Table, stmt.Where != nil)

	// Estimate delete cost (scan + delete)
	deleteCost := scanCost + float64(scanRows)*1.0 // Additional cost for deletion
	detail := fmt.Sprintf("DELETE FROM %s (cost=%.2f rows=%d)", stmt.Table, deleteCost, scanRows)
	deleteNode := plan.AddNodeWithCost(root, detail, scanRows, deleteCost)

	// Add scan information
	scanDetail := formatTableScanWithCost(stmt.Table, stmt.Where, true, scanRows, scanCost)
	plan.AddNodeWithCost(deleteNode, scanDetail, scanRows, scanCost)

	return plan, nil
}

// formatTableScan formats a table scan description.
func formatTableScan(tableName string, where parser.Expression, isWrite bool) string {
	if tableName == "" {
		tableName = "?"
	}

	// Check for index usage (simplified - real implementation would analyze WHERE clause)
	if where == nil {
		return fmt.Sprintf("SCAN %s", tableName)
	}

	return formatTableScanWithWhere(tableName, where)
}

// formatTableScanWithCost formats a table scan description with cost estimates.
func formatTableScanWithCost(tableName string, where parser.Expression, isWrite bool, estimatedRows int64, estimatedCost float64) string {
	if tableName == "" {
		tableName = "?"
	}

	baseScan := formatTableScan(tableName, where, isWrite)
	return fmt.Sprintf("%s (cost=%.2f rows=%d)", baseScan, estimatedCost, estimatedRows)
}

// formatTableScanWithWhere formats a table scan description when WHERE clause exists
func formatTableScanWithWhere(tableName string, where parser.Expression) string {
	indexable, colName := analyzeIndexability(where)

	if indexable && colName != "" {
		// Assume we might use an index on this column
		return fmt.Sprintf("SEARCH %s USING INDEX (POSSIBLE %s)", tableName, colName)
	}

	// Full scan with WHERE clause
	return fmt.Sprintf("SCAN %s", tableName)
}

// formatIndexScan formats an index scan description.
// SCAFFOLDING: For detailed EXPLAIN output with index information.
func formatIndexScan(candidate *IndexCandidate) string {
	if candidate.IsUnique && candidate.HasEquality {
		return fmt.Sprintf("SEARCH %s USING INDEX %s (%s=?)",
			candidate.TableName, candidate.IndexName, candidate.Columns[0])
	}

	if candidate.IsCovering {
		return fmt.Sprintf("SCAN %s USING COVERING INDEX %s",
			candidate.TableName, candidate.IndexName)
	}

	operation := "SEARCH"
	if !candidate.HasEquality {
		operation = "SCAN"
	}

	return fmt.Sprintf("%s %s USING INDEX %s",
		operation, candidate.TableName, candidate.IndexName)
}

// analyzeIndexability determines if a WHERE expression is indexable
func analyzeIndexability(where parser.Expression) (bool, string) {
	binExpr, ok := where.(*parser.BinaryExpr)
	if !ok {
		return false, ""
	}

	ident, ok := binExpr.Left.(*parser.IdentExpr)
	if !ok {
		return false, ""
	}

	// Simple heuristic: = and < > operators are indexable
	indexable := isIndexableOperator(binExpr.Op)
	return indexable, ident.Name
}

// isIndexableOperator checks if an operator can use an index
func isIndexableOperator(op parser.BinaryOp) bool {
	return op == parser.OpEq || op == parser.OpLt ||
		op == parser.OpGt || op == parser.OpLe ||
		op == parser.OpGe
}

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

// findBestIndex analyzes the WHERE clause and available indexes to find the best index.
// Returns nil if no suitable index is found (table scan should be used).
func findBestIndex(tableName string, where parser.Expression, schemaInfo *schema.Schema) *IndexCandidate {
	return findBestIndexWithColumns(tableName, where, schemaInfo, nil)
}

// findBestIndexWithColumns analyzes the WHERE clause and available indexes to find the best index.
// The neededColumns parameter specifies which columns are needed by the query for covering index detection.
// Returns nil if no suitable index is found (table scan should be used).
func findBestIndexWithColumns(tableName string, where parser.Expression, schemaInfo *schema.Schema, neededColumns []string) *IndexCandidate {
	if !canUseIndex(schemaInfo, where) {
		return nil
	}

	indexes := schemaInfo.GetTableIndexes(tableName)
	indexable, colName := analyzeIndexability(where)
	if !indexable || colName == "" {
		return nil
	}

	return selectBestIndex(tableName, where, indexes, colName, neededColumns)
}

// canUseIndex checks if index usage is possible given schema and WHERE clause.
func canUseIndex(schemaInfo *schema.Schema, where parser.Expression) bool {
	return schemaInfo != nil && where != nil
}

// selectBestIndex finds the best index from available indexes for the given column.
func selectBestIndex(tableName string, where parser.Expression, indexes []*schema.Index, colName string, neededColumns []string) *IndexCandidate {
	var bestCandidate *IndexCandidate
	estimator := NewCostEstimator()

	for _, idx := range indexes {
		candidate := evaluateIndex(tableName, where, idx, colName, neededColumns, estimator)
		if candidate != nil {
			bestCandidate = selectLowerCostCandidate(bestCandidate, candidate)
		}
	}

	return bestCandidate
}

// evaluateIndex evaluates a single index and returns a candidate if usable.
func evaluateIndex(tableName string, where parser.Expression, idx *schema.Index, colName string, neededColumns []string, estimator *CostEstimator) *IndexCandidate {
	if !isIndexUsable(idx, colName) {
		return nil
	}

	hasEquality := detectEqualityCondition(where)
	isCovering := isIndexCovering(idx, neededColumns)
	rows, cost := estimator.EstimateIndexScan(idx.Name, idx.Unique, isCovering, hasEquality)

	return &IndexCandidate{
		IndexName:     idx.Name,
		TableName:     tableName,
		Columns:       idx.Columns,
		IsUnique:      idx.Unique,
		IsCovering:    isCovering,
		HasEquality:   hasEquality,
		EstimatedRows: rows,
		EstimatedCost: cost,
	}
}

// isIndexUsable checks if an index can be used for the given column.
func isIndexUsable(idx *schema.Index, colName string) bool {
	return len(idx.Columns) > 0 && idx.Columns[0] == colName
}

// detectEqualityCondition checks if the WHERE clause uses an equality operator.
func detectEqualityCondition(where parser.Expression) bool {
	if binExpr, ok := where.(*parser.BinaryExpr); ok {
		return binExpr.Op == parser.OpEq
	}
	return false
}

// selectLowerCostCandidate returns the candidate with the lower cost.
func selectLowerCostCandidate(current, candidate *IndexCandidate) *IndexCandidate {
	if current == nil || candidate.EstimatedCost < current.EstimatedCost {
		return candidate
	}
	return current
}

// isIndexCovering determines if an index covers all needed columns.
// A covering index contains all columns needed by the query, so no table lookup is required.
func isIndexCovering(idx *schema.Index, neededColumns []string) bool {
	if len(neededColumns) == 0 {
		return false
	}

	indexColumns := buildIndexColumnMap(idx)

	for _, needed := range neededColumns {
		if !isColumnAvailable(needed, indexColumns) {
			return false
		}
	}

	return true
}

// buildIndexColumnMap creates a map of columns available in an index.
func buildIndexColumnMap(idx *schema.Index) map[string]bool {
	indexColumns := make(map[string]bool)
	for _, col := range idx.Columns {
		indexColumns[strings.ToLower(col)] = true
	}
	return indexColumns
}

// isColumnAvailable checks if a column is available (either in index or is rowid).
func isColumnAvailable(column string, indexColumns map[string]bool) bool {
	neededLower := strings.ToLower(column)
	if isRowidColumn(neededLower) {
		return true
	}
	return indexColumns[neededLower]
}

// isRowidColumn checks if a column name is a rowid alias.
func isRowidColumn(column string) bool {
	return column == "rowid" || column == "oid" || column == "_rowid_"
}

// mergeSubplan merges a subplan tree into the parent plan.
func mergeSubplan(parent *ExplainNode, subRoot *ExplainNode, plan *ExplainPlan) {
	// Create a new node with adjusted IDs and levels
	newNode := &ExplainNode{
		ID:       plan.nextID,
		Parent:   parent.ID,
		NotUsed:  0,
		Detail:   subRoot.Detail,
		Children: make([]*ExplainNode, 0),
		Level:    parent.Level + 1,
	}
	plan.nextID++

	parent.Children = append(parent.Children, newNode)

	// Recursively merge children
	for _, child := range subRoot.Children {
		mergeSubplan(newNode, child, plan)
	}
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

// CostEstimator provides cost estimation for query operations.
type CostEstimator struct {
	// Default heuristic values when statistics are unavailable
	defaultTableRows int64
	defaultIndexRows int64

	// Schema information for better estimates
	schemaInfo *schema.Schema
}

// NewCostEstimator creates a new cost estimator with default values.
func NewCostEstimator() *CostEstimator {
	return &CostEstimator{
		defaultTableRows: 1000000, // Assume 1M rows by default
		defaultIndexRows: 100000,  // Assume 100K rows for index scans
		schemaInfo:       nil,
	}
}

// NewCostEstimatorWithSchema creates a new cost estimator with schema information.
func NewCostEstimatorWithSchema(schemaInfo *schema.Schema) *CostEstimator {
	ce := NewCostEstimator()
	ce.schemaInfo = schemaInfo
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
func (ce *CostEstimator) EstimateTableScan(tableName string, hasWhere bool) (rows int64, cost float64) {
	// Get actual or estimated row count
	totalRows := ce.getTableRowCount(tableName)
	rows = totalRows

	// Base cost is proportional to number of rows
	// Full table scan = 1.0 cost per row
	cost = float64(totalRows)

	// WHERE clause reduces estimated rows but still scans all
	if hasWhere {
		rows = totalRows / 10 // Assume WHERE filters 90% of rows
		// Cost remains the same (still need to scan all rows)
	}

	return rows, cost
}

// EstimateIndexScan estimates the cost of an index scan.
func (ce *CostEstimator) EstimateIndexScan(indexName string, isUnique bool, isCovering bool, hasEquality bool) (rows int64, cost float64) {
	// Unique index with equality: expect 1 row
	if isUnique && hasEquality {
		rows = 1
		cost = 10.0 // Log(N) probes = ~10 for 1M rows
		return
	}

	// Non-unique index with equality: expect ~1% of rows
	if hasEquality {
		rows = ce.defaultIndexRows / 100
		cost = float64(rows) * 0.5 // Index scan is cheaper than table scan
	} else {
		// Range scan: expect ~10% of rows
		rows = ce.defaultIndexRows / 10
		cost = float64(rows) * 0.7
	}

	// Covering index is cheaper (no table lookup needed)
	if isCovering {
		cost = cost * 0.8
	}

	return rows, cost
}

// EstimateJoinCost estimates the cost of joining two tables.
func (ce *CostEstimator) EstimateJoinCost(leftRows, rightRows int64, joinType parser.JoinType, hasIndexOnRight bool) (rows int64, cost float64) {
	// Estimate result rows based on join type
	switch joinType {
	case parser.JoinCross:
		// Cartesian product
		rows = leftRows * rightRows
	case parser.JoinInner:
		// Assume 10% selectivity for join condition
		rows = (leftRows * rightRows) / 10
	case parser.JoinLeft, parser.JoinRight:
		// At least as many as the outer table
		rows = leftRows
		if joinType == parser.JoinRight {
			rows = rightRows
		}
	case parser.JoinFull:
		// Sum of both tables (worst case)
		rows = leftRows + rightRows
	default:
		rows = leftRows
	}

	// Cost calculation
	if hasIndexOnRight {
		// Nested loop with index: O(N * log M)
		cost = float64(leftRows) * (10.0 + float64(rightRows)*0.01)
	} else {
		// Nested loop without index: O(N * M)
		cost = float64(leftRows) * float64(rightRows)
	}

	return rows, cost
}

// EstimateAggregateCost estimates the cost of aggregation (GROUP BY, DISTINCT).
func (ce *CostEstimator) EstimateAggregateCost(inputRows int64, numGroups int64) (rows int64, cost float64) {
	if numGroups == 0 {
		// Estimate number of groups as sqrt(input rows)
		numGroups = int64(1000) // Conservative estimate
		if inputRows > 10000 {
			numGroups = inputRows / 100
		}
	}

	rows = numGroups
	// Aggregation requires sorting or hashing: O(N log N)
	cost = float64(inputRows) * 1.2

	return rows, cost
}

// EstimateSortCost estimates the cost of sorting (ORDER BY).
func (ce *CostEstimator) EstimateSortCost(inputRows int64) (rows int64, cost float64) {
	rows = inputRows
	// Sorting cost: O(N log N)
	if inputRows > 0 {
		cost = float64(inputRows) * 1.5
	}
	return rows, cost
}
