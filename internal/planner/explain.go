package planner

import (
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
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
		ID:       p.nextID,
		Parent:   -1,
		NotUsed:  0,
		Detail:   detail,
		Children: make([]*ExplainNode, 0),
		Level:    0,
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
	root := plan.AddNode(nil, "QUERY PLAN")
	scanDetail := formatTableScan(tableName, stmt.Where, false)
	mainScan := plan.AddNode(root, scanDetail)

	for _, join := range stmt.From.Joins {
		joinType := formatJoinType(join.Type)
		joinNode := plan.AddNode(mainScan, joinType)
		scanDetail := formatTableScan(join.Table.TableName, join.Condition.On, false)
		plan.AddNode(joinNode, scanDetail)
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
	root := plan.AddNode(nil, "QUERY PLAN")
	scanDetail := formatTableScan(tableName, stmt.Where, false)
	scanNode := plan.AddNode(root, scanDetail)

	if features.hasAggregates {
		plan.AddNode(scanNode, "USE TEMP B-TREE FOR GROUP BY")
	}

	if features.hasOrderBy {
		addOrderByNode(plan, scanNode, stmt.OrderBy)
	}

	return plan, nil
}

// addOrderByNode adds an ORDER BY node to the explain plan.
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

// generateExplainInsert generates an explain plan for an INSERT statement.
func generateExplainInsert(plan *ExplainPlan, stmt *parser.InsertStmt) (*ExplainPlan, error) {
	root := plan.AddNode(nil, "QUERY PLAN")

	detail := fmt.Sprintf("INSERT INTO %s", stmt.Table)
	insertNode := plan.AddNode(root, detail)

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
	root := plan.AddNode(nil, "QUERY PLAN")

	detail := fmt.Sprintf("UPDATE %s", stmt.Table)
	updateNode := plan.AddNode(root, detail)

	// Add scan information
	scanDetail := formatTableScan(stmt.Table, stmt.Where, true)
	plan.AddNode(updateNode, scanDetail)

	return plan, nil
}

// generateExplainDelete generates an explain plan for a DELETE statement.
func generateExplainDelete(plan *ExplainPlan, stmt *parser.DeleteStmt) (*ExplainPlan, error) {
	root := plan.AddNode(nil, "QUERY PLAN")

	detail := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	deleteNode := plan.AddNode(root, detail)

	// Add scan information
	scanDetail := formatTableScan(stmt.Table, stmt.Where, true)
	plan.AddNode(deleteNode, scanDetail)

	return plan, nil
}

// formatTableScan formats a table scan description.
func formatTableScan(tableName string, where parser.Expression, isWrite bool) string {
	if tableName == "" {
		tableName = "?"
	}

	// Check for index usage (simplified - real implementation would analyze WHERE clause)
	hasWhere := where != nil

	if hasWhere {
		// Try to determine if an index could be used
		indexable := false
		var colName string

		if binExpr, ok := where.(*parser.BinaryExpr); ok {
			if ident, ok := binExpr.Left.(*parser.IdentExpr); ok {
				colName = ident.Name
				// Simple heuristic: = and < > operators are indexable
				if binExpr.Op == parser.OpEq || binExpr.Op == parser.OpLt ||
					binExpr.Op == parser.OpGt || binExpr.Op == parser.OpLe ||
					binExpr.Op == parser.OpGe {
					indexable = true
				}
			}
		}

		if indexable && colName != "" {
			// Assume we might use an index on this column
			return fmt.Sprintf("SEARCH %s USING INDEX (POSSIBLE %s)", tableName, colName)
		}

		// Full scan with WHERE clause
		return fmt.Sprintf("SCAN %s", tableName)
	}

	// Full table scan
	return fmt.Sprintf("SCAN %s", tableName)
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
