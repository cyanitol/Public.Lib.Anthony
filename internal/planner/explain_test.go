// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// TestExplainPlanBasic tests basic explain plan creation.
func TestExplainPlanBasic(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "QUERY PLAN")
	if root == nil {
		t.Fatal("Failed to create root node")
	}

	if root.ID != 0 {
		t.Errorf("Expected root ID to be 0, got %d", root.ID)
	}

	if root.Parent != -1 {
		t.Errorf("Expected root Parent to be -1, got %d", root.Parent)
	}

	if root.Level != 0 {
		t.Errorf("Expected root Level to be 0, got %d", root.Level)
	}

	if root.Detail != "QUERY PLAN" {
		t.Errorf("Expected Detail 'QUERY PLAN', got '%s'", root.Detail)
	}
}

// TestExplainPlanChildren tests adding child nodes.
func TestExplainPlanChildren(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "QUERY PLAN")
	child1 := plan.AddNode(root, "SCAN table1")
	_ = plan.AddNode(root, "SCAN table2")

	if child1.Parent != root.ID {
		t.Errorf("Expected child1 Parent to be %d, got %d", root.ID, child1.Parent)
	}

	if child1.Level != 1 {
		t.Errorf("Expected child1 Level to be 1, got %d", child1.Level)
	}

	if len(root.Children) != 2 {
		t.Errorf("Expected root to have 2 children, got %d", len(root.Children))
	}
}

// TestExplainPlanFormatAsTable tests table formatting.
func TestExplainPlanFormatAsTable(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "QUERY PLAN")
	_ = plan.AddNode(root, "SCAN table1")

	rows := plan.FormatAsTable()

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Check first row (root)
	row0 := rows[0]
	if row0[0].(int) != 0 {
		t.Errorf("Expected row 0 ID to be 0, got %d", row0[0])
	}
	if row0[1].(int) != -1 {
		t.Errorf("Expected row 0 Parent to be -1, got %d", row0[1])
	}
	if !strings.Contains(row0[3].(string), "QUERY PLAN") {
		t.Errorf("Expected row 0 Detail to contain 'QUERY PLAN', got '%s'", row0[3])
	}

	// Check second row (child with indentation)
	row1 := rows[1]
	if row1[0].(int) != 1 {
		t.Errorf("Expected row 1 ID to be 1, got %d", row1[0])
	}
	if row1[1].(int) != 0 {
		t.Errorf("Expected row 1 Parent to be 0, got %d", row1[1])
	}
	detail := row1[3].(string)
	if !strings.HasPrefix(detail, "  ") {
		t.Errorf("Expected row 1 Detail to be indented, got '%s'", detail)
	}
	if !strings.Contains(detail, "SCAN table1") {
		t.Errorf("Expected row 1 Detail to contain 'SCAN table1', got '%s'", detail)
	}
}

// TestExplainPlanFormatAsText tests text formatting.
func TestExplainPlanFormatAsText(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "QUERY PLAN")
	plan.AddNode(root, "SCAN table1")

	text := plan.FormatAsText()

	if !strings.Contains(text, "QUERY PLAN") {
		t.Errorf("Expected text to contain 'QUERY PLAN', got:\n%s", text)
	}

	if !strings.Contains(text, "SCAN table1") {
		t.Errorf("Expected text to contain 'SCAN table1', got:\n%s", text)
	}

	// Check indentation
	lines := strings.Split(text, "\n")
	if len(lines) < 2 {
		t.Errorf("Expected at least 2 lines, got %d", len(lines))
	}

	if strings.HasPrefix(lines[0], " ") {
		t.Errorf("Expected first line not to be indented, got '%s'", lines[0])
	}

	if len(lines) > 1 && !strings.HasPrefix(lines[1], "  ") {
		t.Errorf("Expected second line to be indented, got '%s'", lines[1])
	}
}

// TestGenerateExplainSimpleSelect tests explain generation for simple SELECT.
func TestGenerateExplainSimpleSelect(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Expected plan to be non-nil")
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions the table name
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "users") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention 'users' table")
	}
}

// TestGenerateExplainSelectWithWhere tests explain with WHERE clause.
func TestGenerateExplainSelectWithWhere(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions SCAN or SEARCH
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "SCAN") || strings.Contains(detail, "SEARCH") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention SCAN or SEARCH")
	}
}

// TestGenerateExplainSelectWithOrderBy tests explain with ORDER BY.
func TestGenerateExplainSelectWithOrderBy(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: &parser.IdentExpr{Name: "name"}, Asc: true},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions ORDER BY
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "ORDER BY") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention ORDER BY")
	}
}

// TestGenerateExplainSelectWithJoin tests explain with JOIN.
func TestGenerateExplainSelectWithJoin(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
			Joins: []parser.JoinClause{
				{
					Type:  parser.JoinInner,
					Table: parser.TableOrSubquery{TableName: "orders"},
					Condition: parser.JoinCondition{
						On: &parser.BinaryExpr{
							Op:    parser.OpEq,
							Left:  &parser.IdentExpr{Table: "users", Name: "id"},
							Right: &parser.IdentExpr{Table: "orders", Name: "user_id"},
						},
					},
				},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 2 {
		t.Errorf("Expected at least 2 rows for join, got %d", len(rows))
	}

	// Check that it mentions both tables
	foundUsers := false
	foundOrders := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "users") {
			foundUsers = true
		}
		if strings.Contains(detail, "orders") {
			foundOrders = true
		}
	}

	if !foundUsers {
		t.Errorf("Expected plan to mention 'users' table")
	}
	if !foundOrders {
		t.Errorf("Expected plan to mention 'orders' table")
	}
}

// TestGenerateExplainSelectWithAggregates tests explain with aggregate functions.
func TestGenerateExplainSelectWithAggregates(t *testing.T) {
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.FunctionExpr{Name: "COUNT", Star: true}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions GROUP BY or aggregation
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "GROUP BY") || strings.Contains(detail, "TEMP") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention GROUP BY or temp table for aggregation")
	}
}

// TestGenerateExplainInsert tests explain for INSERT.
func TestGenerateExplainInsert(t *testing.T) {
	stmt := &parser.InsertStmt{
		Table:   "users",
		Columns: []string{"name", "email"},
		Values: [][]parser.Expression{
			{
				&parser.LiteralExpr{Type: parser.LiteralString, Value: "John"},
				&parser.LiteralExpr{Type: parser.LiteralString, Value: "john@example.com"},
			},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions INSERT
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "INSERT") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention INSERT")
	}
}

// TestGenerateExplainUpdate tests explain for UPDATE.
func TestGenerateExplainUpdate(t *testing.T) {
	stmt := &parser.UpdateStmt{
		Table: "users",
		Sets: []parser.Assignment{
			{Column: "name", Value: &parser.LiteralExpr{Type: parser.LiteralString, Value: "Jane"}},
		},
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions UPDATE
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "UPDATE") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention UPDATE")
	}
}

// TestGenerateExplainDelete tests explain for DELETE.
func TestGenerateExplainDelete(t *testing.T) {
	stmt := &parser.DeleteStmt{
		Table: "users",
		Where: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "id"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}

	plan, err := GenerateExplain(stmt)
	if err != nil {
		t.Fatalf("GenerateExplain failed: %v", err)
	}

	rows := plan.FormatAsTable()
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(rows))
	}

	// Check that it mentions DELETE
	found := false
	for _, row := range rows {
		detail := row[3].(string)
		if strings.Contains(detail, "DELETE") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected plan to mention DELETE")
	}
}

// TestIsAggregateExpr tests aggregate function detection.
func TestIsAggregateExpr(t *testing.T) {
	tests := []struct {
		name     string
		expr     parser.Expression
		expected bool
	}{
		{
			name:     "COUNT",
			expr:     &parser.FunctionExpr{Name: "COUNT"},
			expected: true,
		},
		{
			name:     "SUM",
			expr:     &parser.FunctionExpr{Name: "SUM"},
			expected: true,
		},
		{
			name:     "AVG",
			expr:     &parser.FunctionExpr{Name: "AVG"},
			expected: true,
		},
		{
			name:     "MIN",
			expr:     &parser.FunctionExpr{Name: "MIN"},
			expected: true,
		},
		{
			name:     "MAX",
			expr:     &parser.FunctionExpr{Name: "MAX"},
			expected: true,
		},
		{
			name:     "TOTAL",
			expr:     &parser.FunctionExpr{Name: "TOTAL"},
			expected: true,
		},
		{
			name:     "GROUP_CONCAT",
			expr:     &parser.FunctionExpr{Name: "GROUP_CONCAT"},
			expected: true,
		},
		{
			name:     "UPPER (non-aggregate)",
			expr:     &parser.FunctionExpr{Name: "UPPER"},
			expected: false,
		},
		{
			name:     "identifier",
			expr:     &parser.IdentExpr{Name: "column"},
			expected: false,
		},
		{
			name:     "nil",
			expr:     nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAggregateExpr(tt.expr)
			if result != tt.expected {
				t.Errorf("isAggregateExpr(%v) = %v, expected %v", tt.name, result, tt.expected)
			}
		})
	}
}

// TestDetectAggregates tests aggregate detection in SELECT.
func TestDetectAggregates(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.SelectStmt
		expected bool
	}{
		{
			name: "SELECT with COUNT",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.FunctionExpr{Name: "COUNT"}},
				},
			},
			expected: true,
		},
		{
			name: "SELECT with SUM",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.FunctionExpr{Name: "SUM"}},
				},
			},
			expected: true,
		},
		{
			name: "SELECT without aggregates",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.IdentExpr{Name: "name"}},
				},
			},
			expected: false,
		},
		{
			name: "SELECT with non-aggregate function",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.FunctionExpr{Name: "UPPER"}},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectAggregates(tt.stmt)
			if result != tt.expected {
				t.Errorf("detectAggregates() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestHasFromSubqueries tests subquery detection.
func TestHasFromSubqueries(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.SelectStmt
		expected bool
	}{
		{
			name: "No subqueries",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
				},
			},
			expected: false,
		},
		{
			name: "WITH subquery in FROM",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{Subquery: &parser.SelectStmt{}},
					},
				},
			},
			expected: true,
		},
		{
			name: "Subquery in JOIN",
			stmt: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{Subquery: &parser.SelectStmt{}}},
					},
				},
			},
			expected: true,
		},
		{
			name:     "No FROM clause",
			stmt:     &parser.SelectStmt{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasFromSubqueries(tt.stmt)
			if result != tt.expected {
				t.Errorf("hasFromSubqueries() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestFormatTableScan tests table scan formatting.
func TestFormatTableScan(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		where     parser.Expression
		isWrite   bool
		expected  string
	}{
		{
			name:      "Simple scan without WHERE",
			tableName: "users",
			where:     nil,
			isWrite:   false,
			expected:  "SCAN users",
		},
		{
			name:      "Scan with equality WHERE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "id"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			isWrite:  false,
			expected: "SEARCH",
		},
		{
			name:      "Scan with range WHERE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpGt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
			},
			isWrite:  false,
			expected: "SEARCH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTableScan(tt.tableName, tt.where, tt.isWrite)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("formatTableScan() = '%s', expected to contain '%s'", result, tt.expected)
			}
		})
	}
}
