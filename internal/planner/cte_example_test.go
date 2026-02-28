package planner_test

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
)

// Example_simpleCTE demonstrates using a simple CTE.
func Example_simpleCTE() {
	sql := "WITH users_cte AS (SELECT id, name FROM users WHERE active = 1) SELECT * FROM users_cte"

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	fmt.Printf("CTEs defined: %d\n", len(ctx.CTEs))
	fmt.Printf("Is recursive: %v\n", ctx.IsRecursive)

	// Output:
	// CTEs defined: 1
	// Is recursive: false
}

// Example_multipleCTEs demonstrates using multiple CTEs with dependencies.
func Example_multipleCTEs() {
	sql := `WITH
		active_users AS (SELECT id, name FROM users WHERE active = 1),
		user_orders AS (SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users))
	SELECT * FROM user_orders`

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	fmt.Printf("CTEs defined: %d\n", len(ctx.CTEs))
	fmt.Printf("CTE order: %v\n", ctx.CTEOrder)

	// Check dependencies
	userOrdersDef, _ := ctx.GetCTE("user_orders")
	fmt.Printf("user_orders depends on: %v\n", userOrdersDef.DependsOn)

	// Output:
	// CTEs defined: 2
	// CTE order: [active_users user_orders]
	// user_orders depends on: [active_users]
}

// Example_recursiveCTE demonstrates a recursive CTE.
func Example_recursiveCTE() {
	sql := `WITH RECURSIVE cnt(n) AS (
		SELECT 1
		UNION ALL
		SELECT n+1 FROM cnt WHERE n < 10
	) SELECT * FROM cnt`

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	fmt.Printf("Is recursive: %v\n", ctx.IsRecursive)

	cntDef, _ := ctx.GetCTE("cnt")
	fmt.Printf("cnt is recursive: %v\n", cntDef.IsRecursive)
	fmt.Printf("cnt columns: %v\n", cntDef.Columns)

	// Output:
	// Is recursive: true
	// cnt is recursive: true
	// cnt columns: [n]
}

// Example_cteWithPlanner demonstrates integrating CTEs with the query planner.
func Example_cteWithPlanner() {
	sql := "WITH cte AS (SELECT 1 AS x, 2 AS y) SELECT * FROM cte WHERE x > 0"

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	// Create planner and set CTE context
	queryPlanner := planner.NewPlanner()
	queryPlanner.SetCTEContext(ctx)

	// Expand CTE to table
	cteTable, err := ctx.ExpandCTE("cte", 0)
	if err != nil {
		fmt.Printf("Expand error: %v\n", err)
		return
	}

	fmt.Printf("CTE expanded to table: %s\n", cteTable.Name)
	fmt.Printf("Table has %d columns\n", len(cteTable.Columns))

	// Output:
	// CTE expanded to table: cte
	// Table has 2 columns
}

// Example_hierarchicalQuery demonstrates a hierarchical recursive CTE.
func Example_hierarchicalQuery() {
	sql := `WITH RECURSIVE org_chart(id, name, manager_id, level) AS (
		SELECT id, name, manager_id, 0
		FROM employees
		WHERE manager_id IS NULL
		UNION ALL
		SELECT e.id, e.name, e.manager_id, oc.level + 1
		FROM employees e
		JOIN org_chart oc ON e.manager_id = oc.id
	) SELECT * FROM org_chart ORDER BY level`

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	orgChartDef, _ := ctx.GetCTE("org_chart")
	fmt.Printf("org_chart is recursive: %v\n", orgChartDef.IsRecursive)
	fmt.Printf("org_chart columns: %v\n", orgChartDef.Columns)

	// Output:
	// org_chart is recursive: true
	// org_chart columns: [id name manager_id level]
}

// Example_cteValidation demonstrates CTE validation.
func Example_cteValidation() {
	sql := `WITH
		a AS (SELECT * FROM users),
		b AS (SELECT * FROM a),
		c AS (SELECT * FROM b)
	SELECT * FROM c`

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	// Validate CTEs
	err = ctx.ValidateCTEs()
	if err != nil {
		fmt.Printf("Validation error: %v\n", err)
		return
	}

	fmt.Printf("CTE validation passed\n")
	fmt.Printf("Dependency order: %v\n", ctx.CTEOrder)

	// Output:
	// CTE validation passed
	// Dependency order: [a b c]
}

// Example_cteMaterialization demonstrates CTE materialization.
func Example_cteMaterialization() {
	sql := "WITH expensive_cte AS (SELECT * FROM large_table WHERE complex_condition = 1) SELECT * FROM expensive_cte"

	// Parse the SQL
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	selectStmt := stmts[0].(*parser.SelectStmt)

	// Create CTE context
	ctx, err := planner.NewCTEContext(selectStmt.With)
	if err != nil {
		fmt.Printf("CTE context error: %v\n", err)
		return
	}

	// Materialize the CTE
	mat, err := ctx.MaterializeCTE("expensive_cte")
	if err != nil {
		fmt.Printf("Materialization error: %v\n", err)
		return
	}

	fmt.Printf("Materialized CTE: %s\n", mat.Name)
	fmt.Printf("Temp table: %s\n", mat.TempTable)
	fmt.Printf("Is recursive: %v\n", mat.IsRecursive)

	// Output:
	// Materialized CTE: expensive_cte
	// Temp table: _cte_expensive_cte
	// Is recursive: false
}
